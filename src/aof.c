/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"
#include "bio.h"
#include "rio.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/mman.h>


/* ----------------------------------------------------------------------------
 * AOF rewrite buffer implementation.
 *
 * The following code implement a simple buffer used in order to accumulate
 * changes while the background process is rewriting the AOF file.
 *
 * We only need to append, but can't just use realloc with a large block
 * because 'huge' reallocs are not always handled as one could expect
 * (via remapping of pages at OS level) but may involve copying data.
 *
 * For this reason we use a list of blocks, every block is
 * AOF_RW_BUF_BLOCK_SIZE bytes.
 * ------------------------------------------------------------------------- */

#define AOF_RW_BUF_BLOCK_SIZE (1024*1024*10)    /* 10 MB per block */

typedef struct aofrwblock {
    unsigned long used, free;
    char buf[AOF_RW_BUF_BLOCK_SIZE];
} aofrwblock;

/* This function free the old AOF rewrite buffer if needed, and initialize
 * a fresh new one. It tests for server.aof_rewrite_buf_blocks equal to NULL
 * so can be used for the first initialization as well. */
void aofRewriteBufferReset(void) {
    if (server.aof_rewrite_buf_blocks)
        listRelease(server.aof_rewrite_buf_blocks, PM_TRANS_RAM);

    server.aof_rewrite_buf_blocks = listCreate(PM_TRANS_RAM);
    listSetFreeMethod(server.aof_rewrite_buf_blocks,zfree_trans);
}

/* Return the current size of the AOF rewrite buffer. */
unsigned long aofRewriteBufferSize(void) {
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;

    if (block == NULL) return 0;
    unsigned long size =
        (listLength(server.aof_rewrite_buf_blocks)-1) * AOF_RW_BUF_BLOCK_SIZE;
    size += block->used;
    return size;
}

/* Append data to the AOF rewrite buffer, allocating new blocks if needed. */
void aofRewriteBufferAppend(unsigned char *s, unsigned long len) {
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;

    while(len) {
        /* If we already got at least an allocated block, try appending
         * at least some piece into it. */
        if (block) {
            unsigned long thislen = (block->free < len) ? block->free : len;
            if (thislen) {  /* The current block is not already full. */
                memcpy(block->buf+block->used, s, thislen);
                block->used += thislen;
                block->free -= thislen;
                s += thislen;
                len -= thislen;
            }
        }

        if (len) { /* First block to allocate, or need another block. */
            int numblocks;

            block = zmalloc(sizeof(*block));
            block->free = AOF_RW_BUF_BLOCK_SIZE;
            block->used = 0;
            listAddNodeTail(server.aof_rewrite_buf_blocks,block,PM_TRANS_RAM);

            /* Log every time we cross more 10 or 100 blocks, respectively
             * as a notice or warning. */
            numblocks = listLength(server.aof_rewrite_buf_blocks);
            if (((numblocks+1) % 10) == 0) {
                int level = ((numblocks+1) % 100) == 0 ? REDIS_WARNING :
                                                         REDIS_NOTICE;
                redisLog(level,"Background AOF buffer size: %lu MB",
                    aofRewriteBufferSize()/(1024*1024));
            }
        }
    }
}

/* Write the buffer (possibly composed of multiple blocks) into the specified
 * fd. If a short write or any other error happens -1 is returned,
 * otherwise the number of bytes written is returned. */
ssize_t aofRewriteBufferWrite(int fd) {
    listNode *ln;
    listIter li;
    ssize_t count = 0;

    listRewind(server.aof_rewrite_buf_blocks,&li);
    while((ln = listNext(&li))) {
        aofrwblock *block = listNodeValue(ln);
        ssize_t nwritten;

        if (block->used) {
            nwritten = write(fd,block->buf,block->used);
            if (nwritten != (ssize_t)block->used) {
                if (nwritten == 0) errno = EIO;
                return -1;
            }
            count += nwritten;
        }
    }
    return count;
}

/* mmap'ed variant of the function above.
 * Write the buffer (possibly composed of multiple blocks) into the specified
 * fd. If no short write or any other error happens -1 is returned,
 * otherwise the number of bytes written is returned. */
ssize_t aofRewriteBufferWriteMapped(int aof_fd, void **aof_mapped_ptr, size_t *aof_mapped_size, size_t *aof_current_size) {
    listNode *ln;
    listIter li;
    ssize_t count = 0;
    size_t aof_new_size;

    listRewind(server.aof_rewrite_buf_blocks,&li);
    while((ln = listNext(&li))) {
        aofrwblock *block = listNodeValue(ln);

        if (block->used) {
            aof_new_size = *aof_current_size + block->used;

            if (extendAppendOnlyFile(aof_fd, aof_mapped_ptr, aof_mapped_size, aof_new_size) != REDIS_OK) {
                return -1;
            }

            memcpy((char *)*aof_mapped_ptr + *aof_current_size, block->buf, block->used);
            count += block->used;
            *aof_current_size += block->used;
        }
    }
    return count;
}

/* ----------------------------------------------------------------------------
 * AOF file implementation
 * ------------------------------------------------------------------------- */

/* Starts a background task that performs fsync() against the specified
 * file descriptor (the one of the AOF file) in another thread. */
void aof_background_fsync(int fd) {
    bioCreateBackgroundJob(REDIS_BIO_AOF_FSYNC,(void*)(long)fd,NULL,NULL);
}

void aof_background_msync(void *ptr, size_t size) {
    bioCreateBackgroundJob(REDIS_BIO_AOF_MSYNC, ptr, (void *)size, NULL);
}




int extendAppendOnlyFile(int aof_fd, void **aof_mapped_ptr, size_t *aof_mapped_size, size_t aof_new_size)
{
    if (*aof_mapped_size < aof_new_size) {
        /* round-up to the multiply of block size */
        aof_new_size = aof_new_size + server.aof_mmap_block_size;
        aof_new_size = (aof_new_size / server.aof_mmap_block_size) * server.aof_mmap_block_size;

        if (ftruncate(aof_fd, aof_new_size) < 0) {
            redisLog(REDIS_WARNING, "Could not truncate append-only file. Redis may refuse "
                                    "to load the AOF the next time it starts. "
                                    "ftruncate: %s", strerror(errno));
            return REDIS_ERR;
        }

        if (*aof_mapped_ptr == NULL) {
            /* READ+WRITE mode to allow loading data from AOF */
            *aof_mapped_ptr = mmap(NULL, aof_new_size, PROT_READ|PROT_WRITE, MAP_SHARED, aof_fd, 0);
            if (*aof_mapped_ptr == MAP_FAILED) {
                redisLog(REDIS_WARNING, "Can't mmap the append-only file: %s", strerror(errno));
                return REDIS_ERR;
            }
        } else {
            *aof_mapped_ptr = mremap(*aof_mapped_ptr, *aof_mapped_size, aof_new_size, MREMAP_MAYMOVE);
            if (*aof_mapped_ptr == MAP_FAILED) {
                redisLog(REDIS_WARNING, "Can't remap the append-only file: %s", strerror(errno));
                return REDIS_ERR;
            }
        }

        *aof_mapped_size = aof_new_size;
    }
    return REDIS_OK;
}



/* Called when the user switches from "appendonly yes" to "appendonly no"
 * at runtime using the CONFIG command. */
void stopAppendOnly(void) {
    redisAssert(server.aof_state != REDIS_AOF_OFF);
    flushAppendOnlyFile(1);
    aof_fsync(server.aof_fd);
    close(server.aof_fd);

    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = REDIS_AOF_OFF;
    /* rewrite operation in progress? kill it, wait child exit */
    if (server.aof_child_pid != -1) {
        int statloc;

        redisLog(REDIS_NOTICE,"Killing running AOF rewrite child: %ld",
            (long) server.aof_child_pid);
        if (kill(server.aof_child_pid,SIGUSR1) != -1)
            wait3(&statloc,0,NULL);
        /* reset the buffer accumulating changes while the child saves */
        aofRewriteBufferReset();
        aofRemoveTempFile(server.aof_child_pid);
        server.aof_child_pid = -1;
        server.aof_rewrite_time_start = -1;
    }
}

/* Called when the user switches from "appendonly no" to "appendonly yes"
 * at runtime using the CONFIG command. */
int startAppendOnly(void) {
    server.aof_last_fsync = server.unixtime;
    server.aof_fd = open(server.aof_filename,O_WRONLY|O_APPEND|O_CREAT,0644);
    redisAssert(server.aof_state == REDIS_AOF_OFF);
    if (server.aof_fd == -1) {
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't open the append only file: %s",strerror(errno));
        return REDIS_ERR;
    }
    if (rewriteAppendOnlyFileBackground() == REDIS_ERR) {
        close(server.aof_fd);
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
        return REDIS_ERR;
    }
    /* We correctly switched on AOF, now wait for the rewrite to be complete
     * in order to append data on disk. */
    server.aof_state = REDIS_AOF_WAIT_REWRITE;
    return REDIS_OK;
}

/* Write the append only file buffer on disk.
 *
 * Since we are required to write the AOF before replying to the client,
 * and the only way the client socket can get a write is entering when the
 * the event loop, we accumulate all the AOF writes in a memory
 * buffer and write it on disk using this function just before entering
 * the event loop again.
 *
 * About the 'force' argument:
 *
 * When the fsync policy is set to 'everysec' we may delay the flush if there
 * is still an fsync() going on in the background thread, since for instance
 * on Linux write(2) will be blocked by the background fsync anyway.
 * When this happens we remember that there is some aof buffer to be
 * flushed ASAP, and will try to do that in the serverCron() function.
 *
 * However if force is set to 1 we'll write regardless of the background
 * fsync. */
#define AOF_WRITE_LOG_ERROR_RATE 30 /* Seconds between errors logging. */
void flushAppendOnlyFile(int force) {
    ssize_t nwritten;
    int sync_in_progress = 0;
    mstime_t latency;

    if (sdslen(server.aof_buf) == 0) {
        redisLog(REDIS_DEBUG,"AOF: Nothing to flush");
        return;
    }

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC) {
        if (server.aof_mmap) {
            sync_in_progress = bioPendingJobsOfType(REDIS_BIO_AOF_FSYNC) != 0;
        } else {
            sync_in_progress = bioPendingJobsOfType(REDIS_BIO_AOF_MSYNC) != 0;
        }
    }

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force) {
        /* With this append fsync policy we do background fsyncing.
         * If the fsync is still in progress we can try to delay
         * the write for a couple of seconds. */
        if (sync_in_progress) {
            if (server.aof_flush_postponed_start == 0) {
                /* No previous write postponing, remember that we are
                 * postponing the flush and return. */
                server.aof_flush_postponed_start = server.unixtime;
                return;
            } else if (server.unixtime - server.aof_flush_postponed_start < 2) {
                /* We were already waiting for fsync to finish, but for less
                 * than two seconds this is still ok. Postpone again. */
                return;
            }
            /* Otherwise fall trough, and go write since we can't wait
             * over two seconds. */
            server.aof_delayed_fsync++;
            redisLog(REDIS_NOTICE,"Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
        }
    }
    /* We want to perform a single write. This should be guaranteed atomic
     * at least if the filesystem we are writing is a real physical one.
     * While this will save us against the server being killed I don't think
     * there is much to do about the whole server stopping for power problems
     * or alike */
    if (server.aof_mmap) {
        size_t aof_new_size = server.aof_current_size + sdslen(server.aof_buf);
        if (extendAppendOnlyFile(server.aof_fd, &server.aof_mapped_ptr, &server.aof_mapped_size, aof_new_size) != REDIS_OK) {
            exit(1);
        }

        memcpy((char *)server.aof_mapped_ptr + server.aof_current_size, server.aof_buf, sdslen(server.aof_buf));
        nwritten = sdslen(server.aof_buf);
    } else {
        latencyStartMonitor(latency);
        nwritten = write(server.aof_fd,server.aof_buf,sdslen(server.aof_buf));
        latencyEndMonitor(latency);
    }
    /* We want to capture different events for delayed writes:
     * when the delay happens with a pending fsync, or with a saving child
     * active, and when the above two conditions are missing.
     * We also use an additional event name to save all samples which is
     * useful for graphing / monitoring purposes. */
    if (sync_in_progress) {
        latencyAddSampleIfNeeded("aof-write-pending-fsync",latency);
    } else if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) {
        latencyAddSampleIfNeeded("aof-write-active-child",latency);
    } else {
        latencyAddSampleIfNeeded("aof-write-alone",latency);
    }
    latencyAddSampleIfNeeded("aof-write",latency);

    /* We performed the write so reset the postponed flush sentinel to zero. */
    server.aof_flush_postponed_start = 0;
    if (nwritten != (signed)sdslen(server.aof_buf)) {
        static time_t last_write_error_log = 0;
        int can_log = 0;

        /* Limit logging rate to 1 line per AOF_WRITE_LOG_ERROR_RATE seconds. */
        if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
            can_log = 1;
            last_write_error_log = server.unixtime;
        }

        /* Log the AOF write error and record the error code. */
        if (nwritten == -1) {
            if (can_log) {
                redisLog(REDIS_WARNING,"Error writing to the AOF file: %s",
                    strerror(errno));
                server.aof_last_write_errno = errno;
            }
        } else {
            if (can_log) {
                redisLog(REDIS_WARNING,"Short write while writing to "
                                       "the AOF file: (nwritten=%lld, "
                                       "expected=%lld)",
                                       (long long)nwritten,
                                       (long long)sdslen(server.aof_buf));
            }

            if (ftruncate(server.aof_fd, server.aof_current_size) == -1) {
                if (can_log) {
                    redisLog(REDIS_WARNING, "Could not remove short write "
                             "from the append-only file.  Redis may refuse "
                             "to load the AOF the next time it starts.  "
                             "ftruncate: %s", strerror(errno));
                }
            } else {
                /* If the ftruncate() succeeded we can set nwritten to
                 * -1 since there is no longer partial data into the AOF. */
                nwritten = -1;
            }
            server.aof_last_write_errno = ENOSPC;
        }

        /* Handle the AOF write error. */
        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            /* We can't recover when the fsync policy is ALWAYS since the
             * reply for the client is already in the output buffers, and we
             * have the contract with the user that on acknowledged write data
             * is synced on disk. */
            redisLog(REDIS_WARNING,"Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        } else {
            /* Recover from failed write leaving data into the buffer. However
             * set an error to stop accepting writes as long as the error
             * condition is not cleared. */
            server.aof_last_write_status = REDIS_ERR;

            /* Trim the sds buffer if there was a partial write, and there
             * was no way to undo it with ftruncate(2). */
            if (nwritten > 0) {
                server.aof_current_size += nwritten;
                sdsrange(server.aof_buf,nwritten,-1);
            }
            return; /* We'll try again on the next call... */
        }
    } else {
        /* Successful write(2). If AOF was in error state, restore the
         * OK state and log the event. */
        if (server.aof_last_write_status == REDIS_ERR) {
            redisLog(REDIS_WARNING,
                "AOF write error looks solved, Redis can write again.");
            server.aof_last_write_status = REDIS_OK;
        }
    }
    server.aof_current_size += nwritten;

    /* Re-use AOF buffer when it is small enough. The maximum comes from the
     * arena size of 4k minus some overhead (but is otherwise arbitrary). */
    if ((sdslen(server.aof_buf)+sdsavail(server.aof_buf)) < 4000) {
        sdsclear(server.aof_buf);
    } else {
        sdsfree(server.aof_buf,PM_TRANS_RAM);
        server.aof_buf = sdsempty(PM_TRANS_RAM);
    }

    /* Don't fsync if no-appendfsync-on-rewrite is set to yes and there are
     * children doing I/O in the background. */
    if (server.aof_no_fsync_on_rewrite &&
        (server.aof_child_pid != -1 || server.rdb_child_pid != -1))
            return;

    /* Perform the fsync if needed. */
    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        /* aof_fsync is defined as fdatasync() for Linux in order to avoid
         * flushing metadata. */
         if (server.aof_mmap) {
            /* TODO: don't sync the whole mapped region. */
            redisMsync(server.aof_mapped_ptr, server.aof_mapped_size, MS_SYNC);
        } else {
            redisLog(REDIS_NOTICE,"Calling fsync() on the AOF after flushing the accumulated commands.");
            latencyStartMonitor(latency);
            aof_fsync(server.aof_fd); /* Let's try to get this data on the disk */
            latencyEndMonitor(latency);
            latencyAddSampleIfNeeded("aof-fsync-always",latency);
	}
        server.aof_last_fsync = server.unixtime;
    } else if ((server.aof_fsync == AOF_FSYNC_EVERYSEC &&
                server.unixtime > server.aof_last_fsync)) {
        if (!sync_in_progress) {
            if (server.aof_mmap) {
                /* TODO: don't sync the whole mapped region. */
                redisLog(REDIS_VERBOSE,"Scheduling background msync() on the AOF file.");
                aof_background_msync(server.aof_mapped_ptr, server.aof_mapped_size);
            } else {
                redisLog(REDIS_VERBOSE,"Scheduling background fsync() on the AOF file.");
                aof_background_fsync(server.aof_fd);
            }
        }
        server.aof_last_fsync = server.unixtime;
    }
}

sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
    char buf[32];
    int len, j;
    robj *o;

    buf[0] = '*';
    len = 1+ll2string(buf+1,sizeof(buf)-1,argc);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst,buf,len);

    for (j = 0; j < argc; j++) {
        o = getDecodedObject(argv[j],PM_TRANS_RAM);
        buf[0] = '$';
        len = 1+ll2string(buf+1,sizeof(buf)-1,sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst,buf,len);
        dst = sdscatlen(dst,o->ptr,sdslen(o->ptr));
        dst = sdscatlen(dst,"\r\n",2);
        decrRefCount(o,PM_TRANS_RAM);
    }
    return dst;
}

/* Create the sds representation of an PEXPIREAT command, using
 * 'seconds' as time to live and 'cmd' to understand what command
 * we are translating into a PEXPIREAT.
 *
 * This command is used in order to translate EXPIRE and PEXPIRE commands
 * into PEXPIREAT command so that we retain precision in the append only
 * file, and the time is always absolute and not relative. */
sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds) {
    long long when;
    robj *argv[3];

    /* Make sure we can use strtol */
    seconds = getDecodedObject(seconds,PM_TRANS_RAM);
    when = strtoll(seconds->ptr,NULL,10);
    /* Convert argument into milliseconds for EXPIRE, SETEX, EXPIREAT */
    if (cmd->proc == expireCommand || cmd->proc == setexCommand ||
        cmd->proc == expireatCommand)
    {
        when *= 1000;
    }
    /* Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX */
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand)
    {
        when += mstime();
    }
    decrRefCount(seconds,PM_TRANS_RAM);

    argv[0] = createStringObject("PEXPIREAT",9,PM_TRANS_RAM);
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(when,PM_TRANS_RAM);
    buf = catAppendOnlyGenericCommand(buf, 3, argv);
    decrRefCount(argv[0],PM_TRANS_RAM);
    decrRefCount(argv[2],PM_TRANS_RAM);
    return buf;
}

void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc) {
    sds buf = sdsempty(PM_TRANS_RAM);
    robj *tmpargv[3];

    /* The DB this command was targeting is not the same as the last command
     * we appended. To issue a SELECT command is needed. */
    if (dictid != server.aof_selected_db) {
        char seldb[64];

        snprintf(seldb,sizeof(seldb),"%d",dictid);
        buf = sdscatprintf(buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);
        server.aof_selected_db = dictid;
    }

    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == expireatCommand) {
        /* Translate EXPIRE/PEXPIRE/EXPIREAT into PEXPIREAT */
        buf = catAppendOnlyExpireAtCommand(buf,cmd,argv[1],argv[2]);
    } else if (cmd->proc == setexCommand || cmd->proc == psetexCommand) {
        /* Translate SETEX/PSETEX to SET and PEXPIREAT */
        tmpargv[0] = createStringObject("SET",3,PM_TRANS_RAM);
        tmpargv[1] = argv[1];
        tmpargv[2] = argv[3];
        buf = catAppendOnlyGenericCommand(buf,3,tmpargv);
        decrRefCount(tmpargv[0],PM_TRANS_RAM);
        buf = catAppendOnlyExpireAtCommand(buf,cmd,argv[1],argv[2]);
    } else {
        /* All the other commands don't need translation or need the
         * same translation already operated in the command vector
         * for the replication itself. */
        buf = catAppendOnlyGenericCommand(buf,argc,argv);
    }

    /* Append to the AOF buffer. This will be flushed on disk just before
     * of re-entering the event loop, so before the client will get a
     * positive reply about the operation performed. */
    if (server.aof_state == REDIS_AOF_ON) {
        if (server.aof_mmap && server.aof_mmap_direct) {
            size_t aof_new_size = server.aof_current_size + sdslen(buf);

            if (extendAppendOnlyFile(server.aof_fd, &server.aof_mapped_ptr, &server.aof_mapped_size, aof_new_size) != REDIS_OK) {
                exit(1);
            }

            memcpy((char *)server.aof_mapped_ptr + server.aof_current_size, buf, sdslen(buf));
            server.aof_current_size += sdslen(buf);
        } else {
            server.aof_buf = sdscatlen(server.aof_buf,buf,sdslen(buf));
        }
    }

    /* If aof_mmap_rewrite is enabled, the background append buffer is not used */
    if (!(server.aof_mmap && server.aof_mmap_rewrite)) {
        /* If a background append only file rewriting is in progress we want to
         * accumulate the differences between the child DB and the current one
         * in a buffer, so that when the child process will do its work we
         * can append the differences to the new append only file. */
        if (server.aof_child_pid != -1)
            aofRewriteBufferAppend((unsigned char*)buf,sdslen(buf));
    }

    sdsfree(buf,PM_TRANS_RAM);
}

/* ----------------------------------------------------------------------------
 * AOF loading
 * ------------------------------------------------------------------------- */

/* In Redis commands are always executed in the context of a client, so in
 * order to load the append only file we need to create a fake client. */
struct redisClient *createFakeClient(void) {
    struct redisClient *c = zmalloc(sizeof(*c));

    selectDb(c,0);
    c->fd = -1;
    c->name = NULL;
    c->querybuf = sdsempty(PM_TRANS_RAM);
    c->querybuf_peak = 0;
    c->argc = 0;
    c->argv = NULL;
    c->bufpos = 0;
    c->flags = 0;
    /* We set the fake client as a slave waiting for the synchronization
     * so that Redis will not try to send replies to this client. */
    c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
    c->reply = listCreate(PM_TRANS_RAM);
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    c->watched_keys = listCreate(PM_TRANS_RAM);
    c->peerid = NULL;
    listSetFreeMethod(c->reply,decrRefCountVoid);
    listSetDupMethod(c->reply,dupClientReplyValue);
    initClientMultiState(c);
    return c;
}

void freeFakeClientArgv(struct redisClient *c) {
    int j;

    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j],PM_TRANS_RAM);
    zfree(c->argv);
}

void freeFakeClient(struct redisClient *c) {
    sdsfree(c->querybuf,PM_TRANS_RAM);
    listRelease(c->reply,PM_TRANS_RAM);
    listRelease(c->watched_keys,PM_TRANS_RAM);
    freeClientMultiState(c);
    zfree(c);
}

/* Replay the append log file. On success REDIS_OK is returned. On non fatal
 * error (the append only file is zero-length) REDIS_ERR is returned. On
 * fatal error an error message is logged and the program exists. */
int loadAppendOnlyFile(char *filename) {
    struct redisClient *fakeClient;
    FILE *fp = fopen(filename,"r");
    struct redis_stat sb;
    int old_aof_state = server.aof_state;
    long loops = 0;
    off_t valid_up_to = 0; /* Offset of the latest well-formed command loaded. */

    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        fclose(fp);
        redisLog(REDIS_WARNING, "Empty AOF file (size == 0)");
        return REDIS_ERR;
    }

    if (fp == NULL) {
        redisLog(REDIS_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }

    /* Temporarily disable AOF, to prevent EXEC from feeding a MULTI
     * to the same file we're about to read. */
    server.aof_state = REDIS_AOF_OFF;

    fakeClient = createFakeClient();
    startLoading(fp);

    while(1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[128];
        sds argsds;
        struct redisCommand *cmd;

        /* Serve the clients from time to time */
        if (!(loops++ % 1000)) {
            loadingProgress(ftello(fp));
            processEventsWhileBlocked();
        }

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            if (feof(fp))
                break;
            else
                goto readerr;
        }

        if (buf[0] == '#') {
            /* Special marker indicating the end of AOF */
            break;
        }

        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1) goto fmterr;

        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            if (buf[0] != '$') goto fmterr;
            len = strtol(buf+1,NULL,10);
             argsds = sdsnewlen_aligned(NULL,len,server.sds_alignment,PM_TRANS_RAM);
             if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds,PM_TRANS_RAM);
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            argv[j] = createObject(REDIS_STRING,argsds,PM_TRANS_RAM);
            if (fread(buf,2,1,fp) == 0) {
                fakeClient->argc = j+1; /* Free up to j. */
                freeFakeClientArgv(fakeClient);
                goto readerr; /* discard CRLF */
            }
        }

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            redisLog(REDIS_WARNING,"Unknown command '%s' reading the append only file", (char*)argv[0]->ptr);
            exit(1);
        }

        /* Run the command in the context of a fake client */
        cmd->proc(fakeClient);

        /* The fake client should not have a reply */
        redisAssert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);
        /* The fake client should never get blocked */
        redisAssert((fakeClient->flags & REDIS_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        freeFakeClientArgv(fakeClient);
        if (server.aof_load_truncated) valid_up_to = ftello(fp);
/*        for (j = 0; j < fakeClient->argc; j++)
            decrRefCount(fakeClient->argv[j],PM_TRANS_RAM);
        zfree(fakeClient->argv);*/
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, log error and quit. */
    if (fakeClient->flags & REDIS_MULTI) goto uxeof;

loaded_ok: /* DB loaded, cleanup and return REDIS_OK to the caller. */
    fclose(fp);
    freeFakeClient(fakeClient);
    server.aof_state = old_aof_state;
    stopLoading();
    aofUpdateCurrentSize();
    server.aof_rewrite_base_size = server.aof_current_size;
    return REDIS_OK;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    if (!feof(fp)) {
        redisLog(REDIS_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);
    }

uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        redisLog(REDIS_WARNING,"!!! Warning: short read while loading the AOF file !!!");
        redisLog(REDIS_WARNING,"!!! Truncating the AOF at offset %llu !!!",
            (unsigned long long) valid_up_to);
        if (valid_up_to == -1 || truncate(filename,valid_up_to) == -1) {
            if (valid_up_to == -1) {
                redisLog(REDIS_WARNING,"Last valid command offset is invalid");
            } else {
                redisLog(REDIS_WARNING,"Error truncating the AOF file: %s",
                    strerror(errno));
            }
        } else {
            /* Make sure the AOF file descriptor points to the end of the
             * file after the truncate call. */
            if (server.aof_fd != -1 && lseek(server.aof_fd,0,SEEK_END) == -1) {
                redisLog(REDIS_WARNING,"Can't seek the end of the AOF file: %s",
                    strerror(errno));
            } else {
                redisLog(REDIS_WARNING,
                    "AOF loaded anyway because aof-load-truncated is enabled");
                goto loaded_ok;
            }
        }
    }
    redisLog(REDIS_WARNING,"Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);

fmterr: /* Format error. */
    redisLog(REDIS_WARNING,"Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}


/* Replay the append log file. On error REDIS_OK is returned. On non fatal
 * error (the append only file is zero-length) REDIS_ERR is returned. On
 * fatal error an error message is logged and the program exists. */
int loadMappedAppendOnlyFile() {
    struct redisClient *fakeClient;
    int old_aof_state = server.aof_state;
    long loops = 0;
    char *ptr = server.aof_mapped_ptr;
    off_t offset = 0;

    /* We're loading the data from an open AOF. */
    if (server.aof_fd == -1) {
        redisLog(REDIS_WARNING,"Incorrect AOF file descriptor (fd == -1)");
        return REDIS_ERR;
    }

    if (!server.aof_mmap_prealloc && (server.aof_current_size == 0)) {
        redisLog(REDIS_WARNING,"Empty AOF file (size == 0)");
        return REDIS_OK;
    }

    if (ptr == NULL) {
        redisLog(REDIS_WARNING,"Incorrect mapped AOF pointer (NULL)");
        return REDIS_ERR;
    }

    /* Temporarily disable AOF, to prevent EXEC from feeding a MULTI
     * to the same file we're about to read. */
    server.aof_state = REDIS_AOF_OFF;

    fakeClient = createFakeClient();

    /* startLoading(); */
    /* Load the DB */
    server.loading = 1;
    server.loading_start_time = time(NULL);
    server.loading_total_bytes = server.aof_current_size;

    /* If aof_mmap is enabled, the AOF file is already open and mmap'ed at initServer(). */

    while(1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        sds argsds;
        struct redisCommand *cmd;

        /* Serve the clients from time to time */
        if (!(loops++ % 1000)) {
            loadingProgress(offset);
            aeProcessEvents(server.el, AE_FILE_EVENTS|AE_DONT_WAIT);
        }

         if (server.aof_mmap_prealloc) {
            if ((ptr[offset] == '#') || (ptr[offset] == '\0')) {
                /* Special marker indicating the end of AOF */
                /* or a newly created file */
                break;
            }
        } else {
             /* Read until the original file size is reached. */
             /* NOTE that the file may be already extended to the multiply of aof_mmap_block_size. */
             if (offset >= server.aof_current_size) {
                 break;
             }
        }

        if (ptr[offset] != '*') {
            redisLog(REDIS_WARNING, "bad fmt at offset: %zu", offset);
            goto fmterr;
        }
        offset++;
        argc = atoi(&ptr[offset]);
        if (argc < 1) goto fmterr;

        /* read until CRLF */
        while (ptr[offset] != '\r') offset++;
        offset++;
        if (ptr[offset] != '\n') goto fmterr;
        offset++;

        argv = zmalloc(sizeof(robj*)*argc);
        for (j = 0; j < argc; j++) {
            if (ptr[offset] != '$') goto fmterr;
            offset++;
            len = strtol(&ptr[offset],NULL,10);
            argsds = sdsnewlen_aligned(NULL,len,server.sds_alignment,PM_TRANS_RAM);

            /* read until CRLF */
            while (ptr[offset] != '\r') offset++;
            offset++;
            if (ptr[offset] != '\n') goto fmterr;
            offset++;

            if (len && memcpy(argsds,&ptr[offset],len) == 0) goto fmterr;
            offset+=len;
            argv[j] = createObject(REDIS_STRING,argsds,PM_TRANS_RAM);
            offset+=2; /* discard CRLF */
        }

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            redisLog(REDIS_WARNING,"Unknown command '%s' reading the append only file", (char*)argv[0]->ptr);
            exit(1);
        }

        /* Run the command in the context of a fake client */
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        cmd->proc(fakeClient);

        /* The fake client should not have a reply */
        redisAssert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);
        /* The fake client should never get blocked */
        redisAssert((fakeClient->flags & REDIS_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        for (j = 0; j < fakeClient->argc; j++)
            decrRefCount(fakeClient->argv[j],PM_TRANS_RAM);
        zfree(fakeClient->argv);
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, log error and quit. */
    if (fakeClient->flags & REDIS_MULTI) goto readerr;

    /* Do not unmap/close the AOF file! */

    freeFakeClient(fakeClient);
    server.aof_state = old_aof_state;
    stopLoading();

    server.aof_current_size = offset;
    server.aof_rewrite_base_size = offset;

    server.loading_total_bytes = offset;
    return REDIS_OK;

readerr:
    redisLog(REDIS_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
    exit(1);
fmterr:
    redisLog(REDIS_WARNING,"Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}



/* ----------------------------------------------------------------------------
 * AOF rewrite
 * ------------------------------------------------------------------------- */

/* Delegate writing an object to writing a bulk string or bulk long long.
 * This is not placed in rio.c since that adds the redis.h dependency. */
int rioWriteBulkObject(rio *r, robj *obj) {
    /* Avoid using getDecodedObject to help copy-on-write (we are often
     * in a child process when this function is called). */
    if (obj->encoding == REDIS_ENCODING_INT) {
        return rioWriteBulkLongLong(r,(long)obj->ptr);
    } else if (obj->encoding == REDIS_ENCODING_RAW) {
        return rioWriteBulkString(r,obj->ptr,sdslen(obj->ptr));
    } else {
        redisPanic("Unknown string encoding");
    }
}

/* Emit the commands needed to rebuild a list object.
 * The function returns 0 on error, 1 on success. */
int rewriteListObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = listTypeLength(o);

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        unsigned char *p = ziplistIndex(zl,0);
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        while(ziplistGet(p,&vstr,&vlen,&vlong)) {
            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"RPUSH",5) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (vstr) {
                if (rioWriteBulkString(r,(char*)vstr,vlen) == 0) return 0;
            } else {
                if (rioWriteBulkLongLong(r,vlong) == 0) return 0;
            }
            p = ziplistNext(zl,p);
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        list *list = o->ptr;
        listNode *ln;
        listIter li;

        listRewind(list,&li);
        while((ln = listNext(&li))) {
            robj *eleobj = listNodeValue(ln);

            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"RPUSH",5) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkObject(r,eleobj) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else {
        redisPanic("Unknown list encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a set object.
 * The function returns 0 on error, 1 on success. */
int rewriteSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = setTypeSize(o);

    if (o->encoding == REDIS_ENCODING_INTSET) {
        int ii = 0;
        int64_t llval;

        while(intsetGet(o->ptr,ii++,&llval)) {
            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"SADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkLongLong(r,llval) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == REDIS_ENCODING_HT) {
        dictIterator *di = dictGetIterator(o->ptr);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            robj *eleobj = dictGetKey(de);
            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"SADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkObject(r,eleobj) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        redisPanic("Unknown set encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a sorted set object.
 * The function returns 0 on error, 1 on success. */
int rewriteSortedSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = zsetLength(o);

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;
        double score;

        eptr = ziplistIndex(zl,0);
        redisAssert(eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        redisAssert(sptr != NULL);

        while (eptr != NULL) {
            redisAssert(ziplistGet(eptr,&vstr,&vlen,&vll));
            score = zzlGetScore(sptr);

            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items*2) == 0) return 0;
                if (rioWriteBulkString(r,"ZADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkDouble(r,score) == 0) return 0;
            if (vstr != NULL) {
                if (rioWriteBulkString(r,(char*)vstr,vlen) == 0) return 0;
            } else {
                if (rioWriteBulkLongLong(r,vll) == 0) return 0;
            }
            zzlNext(zl,&eptr,&sptr);
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        dictIterator *di = dictGetIterator(zs->dict);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            robj *eleobj = dictGetKey(de);
            double *score = dictGetVal(de);

            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items*2) == 0) return 0;
                if (rioWriteBulkString(r,"ZADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkDouble(r,*score) == 0) return 0;
            if (rioWriteBulkObject(r,eleobj) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        redisPanic("Unknown sorted zset encoding");
    }
    return 1;
}

/* Write either the key or the value of the currently selected item of a hash.
 * The 'hi' argument passes a valid Redis hash iterator.
 * The 'what' filed specifies if to write a key or a value and can be
 * either REDIS_HASH_KEY or REDIS_HASH_VALUE.
 *
 * The function returns 0 on error, non-zero on success. */
static int rioWriteHashIteratorCursor(rio *r, hashTypeIterator *hi, int what) {
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            return rioWriteBulkString(r, (char*)vstr, vlen);
        } else {
            return rioWriteBulkLongLong(r, vll);
        }

    } else if (hi->encoding == REDIS_ENCODING_HT) {
        robj *value;

        hashTypeCurrentFromHashTable(hi, what, &value);
        return rioWriteBulkObject(r, value);
    }

    redisPanic("Unknown hash encoding");
    return 0;
}

/* Emit the commands needed to rebuild a hash object.
 * The function returns 0 on error, 1 on success. */
int rewriteHashObject(rio *r, robj *key, robj *o) {
    hashTypeIterator *hi;
    long long count = 0, items = hashTypeLength(o);

    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != REDIS_ERR) {
        if (count == 0) {
            int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

            if (rioWriteBulkCount(r,'*',2+cmd_items*2) == 0) return 0;
            if (rioWriteBulkString(r,"HMSET",5) == 0) return 0;
            if (rioWriteBulkObject(r,key) == 0) return 0;
        }

        if (rioWriteHashIteratorCursor(r, hi, REDIS_HASH_KEY) == 0) return 0;
        if (rioWriteHashIteratorCursor(r, hi, REDIS_HASH_VALUE) == 0) return 0;
        if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
        items--;
    }

    hashTypeReleaseIterator(hi);

    return 1;
}

/* Write a sequence of commands able to fully rebuild the dataset into
 * "filename". Used both by REWRITEAOF and BGREWRITEAOF.
 *
 * In order to minimize the number of commands needed in the rewritten
 * log Redis uses variadic commands when possible, such as RPUSH, SADD
 * and ZADD. However at max REDIS_AOF_REWRITE_ITEMS_PER_CMD items per time
 * are inserted using a single command. */
int rewriteAppendOnlyFile(char *filename) {
    redisLog(REDIS_NOTICE, "%s", __FUNCTION__);

    dictIterator *di = NULL;
    dictEntry *de;
    rio aof;
    FILE *fp = NULL;
    char tmpfile[256];
    char sizefile[256];
    int j;
    long long now = mstime();
    void *ptr = NULL;
    size_t filesize = 0;
    int retval;
    long long start;

    start = ustime();
    /* Note that we have to use a different temp name here compared to the
     * one used by rewriteAppendOnlyFileBackground() function. */
    snprintf(tmpfile,256,"temp-rewriteaof-%d.aof", (int) getpid());
    snprintf(sizefile,256,"%s.size", server.aof_filename);

    if (server.aof_mmap && server.aof_mmap_prealloc) {
        /* write to BAK file */
        rioInitWithMappedFile(&aof, server.aof_fd_bak, server.aof_mapped_ptr_bak, server.aof_mmap_block_size, server.aof_mmap_block_size);
    } else {
        if (server.aof_mmap) {
            fp = fopen(tmpfile,"w+");
        } else {
            fp = fopen(tmpfile,"w");
        }

        if (!fp) {
            redisLog(REDIS_WARNING, "Opening the temp file (%s) for AOF rewrite in rewriteAppendOnlyFile(): %s", tmpfile, strerror(errno));
            return REDIS_ERR;
        }

        if (server.aof_mmap) {
            size_t aof_mapped_size = 0;

            /* create an empty file with default size */
            if (extendAppendOnlyFile(fileno(fp), &ptr, &aof_mapped_size, server.aof_mmap_block_size) != REDIS_OK) {
                redisLog(REDIS_WARNING, "Cannot extend temp file (%s) for AOF rewrite in rewriteAppendOnlyFile(): %s", tmpfile, strerror(errno));
                fclose(fp);
                return REDIS_ERR;
            }

            rioInitWithMappedFile(&aof,fileno(fp),ptr,aof_mapped_size, server.aof_mmap_block_size);
        } else {
            rioInitWithFile(&aof,fp);

            if (server.aof_rewrite_incremental_fsync) {
                rioSetAutoSync(&aof,REDIS_AOF_AUTOSYNC_BYTES);
            }
        }
    }

    for (j = 0; j < server.dbnum; j++) {
        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
        redisDb *db = server.db+j;
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);
        if (!di) {
            redisLog(REDIS_WARNING,"Can't obtain DB dictionary iterator");
            fclose(fp);
            return REDIS_ERR;
        }

        /* SELECT the new DB */
        if (rioWrite(&aof,selectcmd,sizeof(selectcmd)-1) == 0) goto werr;
        if (rioWriteBulkLongLong(&aof,j) == 0) goto werr;

        /* Iterate this DB writing every entry */
        while((de = dictNext(di)) != NULL) {
            sds keystr;
            robj key, *o;
            long long expiretime;

            keystr = dictGetKey(de);
            o = dictGetVal(de);
            initStaticStringObject(key,keystr);

            expiretime = getExpire(db,&key);

            /* If this key is already expired skip it */
            if (expiretime != -1 && expiretime < now) continue;

            /* Save the key and associated value */
            if (o->type == REDIS_STRING) {
                /* Emit a SET command */
                char cmd[]="*3\r\n$3\r\nSET\r\n";
                if (rioWrite(&aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                /* Key and value */
                if (rioWriteBulkObject(&aof,&key) == 0) goto werr;
                if (rioWriteBulkObject(&aof,o) == 0) goto werr;
            } else if (o->type == REDIS_LIST) {
                if (rewriteListObject(&aof,&key,o) == 0) goto werr;
            } else if (o->type == REDIS_SET) {
                if (rewriteSetObject(&aof,&key,o) == 0) goto werr;
            } else if (o->type == REDIS_ZSET) {
                if (rewriteSortedSetObject(&aof,&key,o) == 0) goto werr;
            } else if (o->type == REDIS_HASH) {
                if (rewriteHashObject(&aof,&key,o) == 0) goto werr;
            } else {
                redisPanic("Unknown object type");
            }
            /* Save the expire time */
            if (expiretime != -1) {
                char cmd[]="*3\r\n$9\r\nPEXPIREAT\r\n";
                if (rioWrite(&aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                if (rioWriteBulkObject(&aof,&key) == 0) goto werr;
                if (rioWriteBulkLongLong(&aof,expiretime) == 0) goto werr;
            }
        }
        dictReleaseIterator(di);
        di = NULL;
    }
    if (server.aof_mmap && server.aof_mmap_prealloc) {
        /* Sync the written part of file only */
        redisMsync(server.aof_mapped_ptr_bak, aof.io.mappedfile.pos, MS_SYNC);
        filesize = aof.io.mappedfile.pos;

        /* save the actual data size in a dedicated file */
        int size_fd = open(sizefile, O_RDWR|O_CREAT|O_TRUNC,0644);
        if (size_fd == -1) {
            redisLog(REDIS_WARNING, "Can't open the AOF size file: %s", strerror(errno));
            return REDIS_ERR;
        }
        if (write(size_fd, &filesize, sizeof(size_t)) != sizeof(size_t)) {
            redisLog(REDIS_WARNING, "Can't write the AOF size file: %s", strerror(errno));
            return REDIS_ERR;
        }
    } else {
        if (server.aof_mmap) {
            if (server.aof_mmap_truncate) {
                /* trim the file to the actual used size */
                retval = ftruncate(fileno(fp), aof.io.mappedfile.pos);
                if (retval < 0) {
                    redisLog(REDIS_WARNING, "Could not truncate temporary append-only file. Redis may refuse "
                                            "to load the AOF the next time it starts. "
                                            "ftruncatee: %s", strerror(errno));
                    goto werr;
                }
            }
            /* TODO: msync() is not neccessary here */
            munmap(aof.io.mappedfile.ptr, aof.io.mappedfile.size);
        } else {
            /* Make sure data will not remain on the OS's output buffers */
            /* TODO: do we need both calls ? */
        	if (fflush(fp) == EOF) goto werr;
        	if (fsync(fileno(fp)) == -1) goto werr;
        }

        filesize = rioTell(&aof);
        if (fclose(fp) == EOF) goto werr;

        /* Use RENAME to make sure the DB file is changed atomically only
         * if the generate DB file is ok. */
        if (rename(tmpfile,filename) == -1) {
            redisLog(REDIS_WARNING,"Error moving temp (%s) append only file on the final destination (%s): %s", tmpfile, filename, strerror(errno));
            unlink(tmpfile);
            return REDIS_ERR;
        }
    }

    redisLog(REDIS_NOTICE,"SYNC append only file rewrite performed");

    redisLog(REDIS_NOTICE,"AOF saved on disk: %.3f seconds",
            (float)(ustime()-start)/1000000);
    redisLog(REDIS_NOTICE,"AOF size: %zu bytes",
            filesize);

    return REDIS_OK;

werr:
    redisLog(REDIS_WARNING,"Write error writing append only file on disk: %s", strerror(errno));
    if (server.aof_mmap) {
        munmap(aof.io.mappedfile.ptr, aof.io.mappedfile.size);
    }
    if (fp) {
        fclose(fp);
    }
    unlink(tmpfile);
    if (di) dictReleaseIterator(di);

    return REDIS_ERR;
}


/* This is how rewriting of the append only file in background works:
 *
 * 1) The user calls BGREWRITEAOF
 * 2) Redis calls this function, that forks():
 *    2a) the child rewrite the append only file in a temp file.
 *    2b) the parent accumulates differences in server.aof_rewrite_buf.
 * 3) When the child finished '2a' exists.
 * 4) The parent will trap the exit code, if it's OK, will append the
 *    data accumulated into server.aof_rewrite_buf into the temp file, and
 *    finally will rename(2) the temp file in the actual file name.
 *    The the new file is reopened as the new append only file. Profit!
 */
int rewriteAppendOnlyFileBackground(void) {
    redisLog(REDIS_NOTICE, "%s", __FUNCTION__);

    pid_t childpid;
    long long start;

    if (server.aof_child_pid != -1) {
        redisLog(REDIS_WARNING,"AOF rewriting in progress (PID == %d)", server.aof_child_pid);
        return REDIS_ERR;
    }

    start = ustime();
    if ((childpid = fork()) == 0) {
        char tmpfile[256];

        /* Child */
        closeListeningSockets(0);
        redisSetProcTitle("redis-aof-rewrite");
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) getpid());
        if (rewriteAppendOnlyFile(tmpfile) == REDIS_OK) {
            size_t private_dirty = zmalloc_get_private_dirty();

            if (private_dirty) {
                redisLog(REDIS_NOTICE,
                    "AOF rewrite: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }
            exitFromChild(0);
        } else {
            exitFromChild(1);
        }
    } else {
        /* Parent */
        server.aof_rewrite_size_at_fork = server.aof_current_size;
        server.stat_fork_time = ustime()-start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);
        if (childpid == -1) {
            redisLog(REDIS_WARNING,
                "Can't rewrite append only file in background: fork: %s",
                strerror(errno));
            return REDIS_ERR;
        }
        redisLog(REDIS_NOTICE,
            "Background append only file rewriting started by pid %d",childpid);
        server.aof_rewrite_scheduled = 0;
        server.aof_rewrite_time_start = time(NULL);
        server.aof_child_pid = childpid;
        updateDictResizePolicy();
        /* We set appendseldb to -1 in order to force the next call to the
         * feedAppendOnlyFile() to issue a SELECT command, so the differences
         * accumulated by the parent into server.aof_rewrite_buf will start
         * with a SELECT statement and it will be safe to merge. */
        server.aof_selected_db = -1;
        replicationScriptCacheFlush();
        return REDIS_OK;
    }
    return REDIS_OK; /* unreached */
}

void bgrewriteaofCommand(redisClient *c) {
    redisLog(REDIS_DEBUG, "%s", __FUNCTION__);

    if (server.aof_child_pid != -1) {
        addReplyError(c,"Background append only file rewriting already in progress");
    } else if (server.rdb_child_pid != -1) {
        server.aof_rewrite_scheduled = 1;
        addReplyStatus(c,"Background append only file rewriting scheduled");
    } else if (rewriteAppendOnlyFileBackground() == REDIS_OK) {
        addReplyStatus(c,"Background append only file rewriting started");
    } else {
        addReply(c,shared.err);
    }
}

void aofRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) childpid);
    unlink(tmpfile);
}

/* Update the server.aof_current_size field explicitly using stat(2)
 * to check the size of the file. This is useful after a rewrite or after
 * a restart, normally the size is updated just adding the write length
 * to the current length, that is much faster. */
void aofUpdateCurrentSize(void) {
    struct redis_stat sb;
    mstime_t latency;

    latencyStartMonitor(latency);
    if (redis_fstat(server.aof_fd,&sb) == -1) {
        redisLog(REDIS_WARNING,"Unable to obtain the AOF file length. stat: %s",
            strerror(errno));
    } else {
        server.aof_current_size = sb.st_size;
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("aof-fstat",latency);
}

/* A background append only file rewriting (BGREWRITEAOF) terminated its work.
 * Handle this. */
void backgroundRewriteDoneHandler(int exitcode, int bysignal) {
    redisLog(REDIS_NOTICE, "%s", __FUNCTION__);

    if (!bysignal && exitcode == 0) {
        int newfd;
        int oldfd = -1;
        char tmpfile[256];
        char bakfile[256];
        char sizefile[256];
        long long now = ustime();
        mstime_t latency;
        struct redis_stat sb;
        void *aof_mapped_ptr = NULL;
        size_t aof_mapped_size = 0;
        size_t aof_current_size;
        size_t aof_new_size;
        void *oldptr = NULL;
        size_t oldsize = 0;

        redisLog(REDIS_NOTICE,
            "Background AOF rewrite terminated with success");

        if (server.aof_mmap && server.aof_mmap_prealloc) {
            snprintf(tmpfile,256,"%s.tmp", server.aof_filename);
            snprintf(bakfile,256,"%s.bak", server.aof_filename);
            snprintf(sizefile,256,"%s.size", server.aof_filename);

            size_t aof_delta_size;
            /* Prepare space for rewrite buffer */
            aof_delta_size = server.aof_current_size - server.aof_rewrite_size_at_fork;

            aof_current_size = 0;

            int size_fd = open(sizefile, O_RDWR,0644);
            if (size_fd == -1) {
                redisLog(REDIS_WARNING, "Can't open the AOF size file: %s", strerror(errno));
                return;
            }
            if (read(size_fd, &aof_current_size, sizeof(size_t)) != sizeof(size_t)) {
                redisLog(REDIS_WARNING, "Can't read the AOF size file: %s", strerror(errno));
                return;
            }

            memcpy((char *)server.aof_mapped_ptr_bak + aof_current_size, (char *)server.aof_mapped_ptr + server.aof_rewrite_size_at_fork, aof_delta_size);
            /* Sync the written part of file only */
            redisMsync((char *)server.aof_mapped_ptr_bak + aof_current_size, aof_delta_size, MS_SYNC);
            aof_current_size += aof_delta_size;

            redisLog(REDIS_NOTICE,
                "Parent diff successfully flushed to the rewritten AOF (%lu bytes)", aof_delta_size);

            int tmp_fd = server.aof_fd;
            void *tmp_ptr = server.aof_mapped_ptr;

            /* Now swap AOF files */
            if (link(server.aof_filename,tmpfile) == -1) {
                redisLog(REDIS_WARNING,"Error linking temp AOF file on the final destination: %s", strerror(errno));
                return;
            }
            if (rename(bakfile,server.aof_filename) == -1) {
                redisLog(REDIS_WARNING,"Error moving AOF file on the final destination: %s", strerror(errno));
                return;
            }
            if (rename(tmpfile,bakfile) == -1) {
                redisLog(REDIS_WARNING,"Error moving temp AOF file on the final destination: %s", strerror(errno));
                return;
            }

            /* and mapped pointers... */
            server.aof_fd = server.aof_fd_bak;
            server.aof_mapped_ptr = server.aof_mapped_ptr_bak;

            server.aof_fd_bak = tmp_fd;
            server.aof_mapped_ptr_bak = tmp_ptr;

            server.aof_selected_db = -1; /* Make sure SELECT is re-issued */
            server.aof_current_size = aof_current_size;
            server.aof_rewrite_base_size = server.aof_current_size;

            /* Clear regular AOF buffer since its contents was just written to
             * the new AOF from the background rewrite buffer. */
            sdsfree(server.aof_buf, PM_TRANS_RAM);
            server.aof_buf = sdsempty(PM_TRANS_RAM);
        } else {
        /* Flush the differences accumulated by the parent to the
         * rewritten AOF. */
        latencyStartMonitor(latency);
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof",
            (int)server.aof_child_pid);
        newfd = open(tmpfile,O_WRONLY|O_APPEND);
        if (newfd == -1) {
            redisLog(REDIS_WARNING,
                "Unable to open the temporary AOF produced by the child: %s", strerror(errno));
            goto cleanup;
        }

        if (aofRewriteBufferWrite(newfd) == -1) {
            redisLog(REDIS_WARNING,
                "Error trying to flush the parent diff to the rewritten AOF: %s", strerror(errno));
            close(newfd);
            goto cleanup;
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-rewrite-diff-write",latency);
            if (server.aof_mmap) {
                if (redis_fstat(newfd,&sb) == -1) {
                    redisLog(REDIS_WARNING,"Unable to obtain the AOF file length. stat: %s",
                        strerror(errno));
                    close(newfd);
                    goto cleanup;
                } else {
                    aof_current_size = sb.st_size;
                }

                if (server.aof_mmap_rewrite) {
                    /* Prepare space for rewrite buffer */
                    size_t aof_delta_size = server.aof_current_size - server.aof_rewrite_size_at_fork;
                    aof_new_size = aof_current_size + aof_delta_size;

                    if (extendAppendOnlyFile(newfd, &aof_mapped_ptr, &aof_mapped_size, aof_new_size) == REDIS_ERR) {
                        close(newfd);
                        goto cleanup;
                    }

                    redisLog(REDIS_NOTICE, "Copying the parent diff to the rewritten AOF: dst=%p, off=%012zx, src=%p, off=%012zx, size=%012zx",
                            aof_mapped_ptr, aof_current_size,
                            server.aof_mapped_ptr, server.aof_rewrite_size_at_fork,
                            aof_delta_size);

                    memcpy((char *)aof_mapped_ptr + aof_current_size, (char *)server.aof_mapped_ptr + server.aof_rewrite_size_at_fork, aof_delta_size);
                    aof_current_size += aof_delta_size;

                    redisLog(REDIS_NOTICE,
                        "Parent diff successfully flushed to the rewritten AOF (%lu bytes)", aof_delta_size);
                } else {
                    /* round-up to the multiply of block size */
                    aof_new_size = aof_current_size + server.aof_mmap_block_size;
                    aof_new_size = (aof_new_size / server.aof_mmap_block_size) * server.aof_mmap_block_size;

                    if (extendAppendOnlyFile(newfd, &aof_mapped_ptr, &aof_mapped_size, aof_new_size) == REDIS_ERR) {
                        close(newfd);
                        goto cleanup;
                    }

                    if (aofRewriteBufferWriteMapped(newfd, &aof_mapped_ptr, &aof_mapped_size, &aof_current_size) == -1) {
                        redisLog(REDIS_WARNING,
                                "Error trying to flush the parent diff to the rewritten AOF: %s", strerror(errno));
                        munmap(aof_mapped_ptr, aof_mapped_size);
                        close(newfd);
                        goto cleanup;
                    }

                    redisLog(REDIS_NOTICE,
                        "Parent diff successfully flushed to the rewritten AOF (%lu bytes)", aofRewriteBufferSize());
                }
            } else {

                redisLog(REDIS_NOTICE,
                    "Parent diff successfully flushed to the rewritten AOF (%lu bytes)", aofRewriteBufferSize());
            }

            /* The only remaining thing to do is to rename the temporary file to
             * the configured file and switch the file descriptor used to do AOF
             * writes. We don't want close(2) or rename(2) calls to block the
             * server on old file deletion.
             *
             * There are two possible scenarios:
             *
             * 1) AOF is DISABLED and this was a one time rewrite. The temporary
             * file will be renamed to the configured file. When this file already
             * exists, it will be unlinked, which may block the server.
             *
             * 2) AOF is ENABLED and the rewritten AOF will immediately start
             * receiving writes. After the temporary file is renamed to the
             * configured file, the original AOF file descriptor will be closed.
             * Since this will be the last reference to that file, closing it
             * causes the underlying file to be unlinked, which may block the
             * server.
             *
             * To mitigate the blocking effect of the unlink operation (either
             * caused by rename(2) in scenario 1, or by close(2) in scenario 2), we
             * use a background thread to take care of this. First, we
             * make scenario 1 identical to scenario 2 by opening the target file
             * when it exists. The unlink operation after the rename(2) will then
             * be executed upon calling close(2) for its descriptor. Everything to
             * guarantee atomicity for this switch has already happened by then, so
             * we don't care what the outcome or duration of that close operation
             * is, as long as the file descriptor is released again. */
            if (server.aof_fd == -1) {
                /* AOF disabled */

                /* Don't care if this fails: oldfd will be -1 and we handle that.
                 * One notable case of -1 return is if the old file does
                 * not exist. */
                oldfd = open(server.aof_filename,O_RDONLY|O_NONBLOCK);
            } else {
                /* AOF enabled */
                oldfd = -1; /* We'll set this to the current AOF filedes later. */
            }

		/* Rename the temporary file. This will not unlink the target file if
		 * it exists, because we reference it with "oldfd". */
	    latencyStartMonitor(latency);
	    if (rename(tmpfile,server.aof_filename) == -1) {
	        redisLog(REDIS_WARNING,
	    	"Error trying to rename the temporary AOF file: %s", strerror(errno));
	        if (server.aof_mmap) {
	        	munmap(aof_mapped_ptr, aof_mapped_size);
	        }
	        close(newfd);
	        if (oldfd != -1) close(oldfd);
	        goto cleanup;
	    }
	    latencyEndMonitor(latency);
	    latencyAddSampleIfNeeded("aof-rename",latency);



            if (server.aof_fd == -1) {
                /* AOF disabled, we don't need to set the AOF file descriptor
                 * to this new file, so we can close it. */
                if (server.aof_mmap) {
                    munmap(aof_mapped_ptr, aof_mapped_size);
                }
                close(newfd);
            } else {
                /* AOF enabled, replace the old fd with the new one. */
                oldfd = server.aof_fd;
                server.aof_fd = newfd;

                if (server.aof_mmap) {
                    oldptr = server.aof_mapped_ptr;
                    oldsize = server.aof_mapped_size;
                    server.aof_mapped_ptr = aof_mapped_ptr;
                    server.aof_mapped_size = aof_mapped_size;
                }

                if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
                    if (server.aof_mmap) {
                        redisMsync(server.aof_mapped_ptr, server.aof_mapped_size, MS_SYNC);
                    } else {
                        aof_fsync(newfd);
                    }
                } else if (server.aof_fsync == AOF_FSYNC_EVERYSEC) {
                    aof_background_fsync(newfd);
                }

                server.aof_selected_db = -1; /* Make sure SELECT is re-issued */
                if (server.aof_mmap) {
                    server.aof_current_size = aof_current_size;
                } else {
                    aofUpdateCurrentSize();
                }
                server.aof_rewrite_base_size = server.aof_current_size;

                /* Clear regular AOF buffer since its contents was just written to
                 * the new AOF from the background rewrite buffer. */
                sdsfree(server.aof_buf,PM_TRANS_RAM);
                server.aof_buf = sdsempty(PM_TRANS_RAM);
            }
        }

        server.aof_lastbgrewrite_status = REDIS_OK;

        redisLog(REDIS_NOTICE, "Background AOF rewrite finished successfully");

        /* Change state from WAIT_REWRITE to ON if needed */
        if (server.aof_state == REDIS_AOF_WAIT_REWRITE) {
            redisLog(REDIS_NOTICE, "%s: Changing AOF state: WAIT_REWRITE => ON", __FUNCTION__);
            server.aof_state = REDIS_AOF_ON;
        } else {
            redisLog(REDIS_NOTICE, "%s: AOF state OFF", __FUNCTION__);
        }

        /* Asynchronously close the overwritten AOF. */
        if (server.aof_mmap && !server.aof_mmap_prealloc && oldptr) {
            munmap(oldptr, oldsize);
        }
        if (oldfd != -1) {
            bioCreateBackgroundJob(REDIS_BIO_CLOSE_FILE,(void*)(long)oldfd,NULL,NULL);
        }

        redisLog(REDIS_VERBOSE,
            "Background AOF rewrite signal handler took %lldus", ustime()-now);
    } else if (!bysignal && exitcode != 0) {
        server.aof_lastbgrewrite_status = REDIS_ERR;

        redisLog(REDIS_WARNING,
            "Background AOF rewrite terminated with error");
    } else {
        server.aof_lastbgrewrite_status = REDIS_ERR;

        redisLog(REDIS_WARNING,
            "Background AOF rewrite terminated by signal %d", bysignal);
    }

cleanup:
    aofRewriteBufferReset();
    aofRemoveTempFile(server.aof_child_pid);
    server.aof_child_pid = -1;
    server.aof_rewrite_time_last = time(NULL)-server.aof_rewrite_time_start;
    server.aof_rewrite_time_start = -1;
    /* Schedule a new rewrite if we are waiting for it to switch the AOF ON. */
    if (server.aof_state == REDIS_AOF_WAIT_REWRITE) {
        redisLog(REDIS_WARNING,"Scheduling AOF rewrite...");
        server.aof_rewrite_scheduled = 1;
    }
}
