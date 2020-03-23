/* tracking.c - Client side caching: keys tracking and invalidation
 *
 * Copyright (c) 2019, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "server.h"

/* The tracking table is constituted by a radix tree of keys, each pointing
 * to a radix tree of client IDs, used to track the clients that may have
 * certain keys in their local, client side, cache.
 *
 * When a client enables tracking with "CLIENT TRACKING on", each key served to
 * the client is remembered in the table mapping the keys to the client IDs.
 * Later, when a key is modified, all the clients that may have local copy
 * of such key will receive an invalidation message.
 *
 * Clients will normally take frequently requested objects in memory, removing
 * them when invalidation messages are received. */
rax *TrackingTable = NULL;
rax *PrefixTable = NULL;
uint64_t TrackingTableTotalItems = 0; /* Total number of IDs stored across
                                         the whole tracking table. This givesn
                                         an hint about the total memory we
                                         are using server side for CSC. */
robj *TrackingChannelName;

/* Remove the tracking state from the client 'c'. Note that there is not much
 * to do for us here, if not to decrement the counter of the clients in
 * tracking mode, because we just store the ID of the client in the tracking
 * table, so we'll remove the ID reference in a lazy way. Otherwise when a
 * client with many entries in the table is removed, it would cost a lot of
 * time to do the cleanup. */
void disableTracking(client *c) {
    if (c->flags & CLIENT_TRACKING) {
        server.tracking_clients--;
        c->flags &= ~(CLIENT_TRACKING|CLIENT_TRACKING_BROKEN_REDIR);
    }
}

/* Enable the tracking state for the client 'c', and as a side effect allocates
 * the tracking table if needed. If the 'redirect_to' argument is non zero, the
 * invalidation messages for this client will be sent to the client ID
 * specified by the 'redirect_to' argument. Note that if such client will
 * eventually get freed, we'll send a message to the original client to
 * inform it of the condition. Multiple clients can redirect the invalidation
 * messages to the same client ID. */
void enableTracking(client *c, uint64_t redirect_to, int bcast, robj **prefix, size_t numprefix) {
    c->flags |= CLIENT_TRACKING;
    c->flags &= ~CLIENT_TRACKING_BROKEN_REDIR;
    c->client_tracking_redirection = redirect_to;
    if (!(c->flags & CLIENT_TRACKING)) server.tracking_clients++;
    if (TrackingTable == NULL) {
        TrackingTable = raxNew();
        PrefixTable = raxNew();
        TrackingChannelName = createStringObject("__redis__:invalidate",20);
    }

    if (bcast) {
        c->flags |= CLIENT_TRACKING_BCAST;
        if (numprefix == 0) enableBcastTrackingForPrefix(c,"",0);
        for (int j = 0; j < numprefix; j++) {
            sds sdsprefix = prefix[j]->ptr;
            enableBcastTrackingForPrefix(c,sdsprefix,sdslen(prefix));
        }
    }
}

/* This function is called after the excution of a readonly command in the
 * case the client 'c' has keys tracking enabled. It will populate the
 * tracking ivalidation table according to the keys the user fetched, so that
 * Redis will know what are the clients that should receive an invalidation
 * message with certain groups of keys are modified. */
void trackingRememberKeys(client *c) {
    int numkeys;
    int *keys = getKeysFromCommand(c->cmd,c->argv,c->argc,&numkeys);
    if (keys == NULL) return;

    for(int j = 0; j < numkeys; j++) {
        int idx = keys[j];
        sds sdskey = c->argv[idx]->ptr;
        rax *ids = raxFind(TrackingTable,(unsigned char*)sdskey,sdslen(sdskey));
        if (ids == raxNotFound) {
            ids = raxNew();
            int inserted = raxTryInsert(TrackingTable,(unsigned char*)sdskey,
                                        sdslen(sdskey),ids, NULL);
            serverAssert(inserted == 1);
        }
        if (raxTryInsert(ids,(unsigned char*)&c->id,sizeof(c->id),NULL,NULL))
            TrackingTableTotalItems++;
    }
    getKeysFreeResult(keys);
}

void sendTrackingMessage(client *c, char *keyname, size_t keylen) {
    int using_redirection = 0;
    if (c->client_tracking_redirection) {
        client *redir = lookupClientByID(c->client_tracking_redirection);
        if (!redir) {
            /* We need to signal to the original connection that we
             * are unable to send invalidation messages to the redirected
             * connection, because the client no longer exist. */
            if (c->resp > 2) {
                addReplyPushLen(c,3);
                addReplyBulkCBuffer(c,"tracking-redir-broken",21);
                addReplyLongLong(c,c->client_tracking_redirection);
            }
            return;
        }
        c = redir;
        using_redirection = 1;
    }

    /* Only send such info for clients in RESP version 3 or more. However
     * if redirection is active, and the connection we redirect to is
     * in Pub/Sub mode, we can support the feature with RESP 2 as well,
     * by sending Pub/Sub messages in the __redis__:invalidate channel. */
    if (c->resp > 2) {
        addReplyPushLen(c,2);
        addReplyBulkCBuffer(c,"invalidate",10);
        addReplyArrayLen(c,1);
        addReplyBulkCBuffer(c,keyname,keylen);
    } else if (using_redirection && c->flags & CLIENT_PUBSUB) {
        /* We use a static object to speedup things, however we assume
         * that addReplyPubsubMessage() will not take a reference. */
        robj keyobj;
        initStaticStringObject(keyobj,keyname);
        addReplyPubsubMessage(c,TrackingChannelName,NULL);
        addReplyArrayLen(c,1);
        addReplyBulk(c,&keyobj);
        serverAssert(keyobj.refcount == 1);
    }
}

/* This function is called from signalModifiedKey() or other places in Redis
 * when a key changes value. In the context of keys tracking, our task here is
 * to send a notification to every client that may have keys about such caching
 * slot. */
void trackingInvalidateKey(robj *keyobj) {
    if (TrackingTable == NULL) return;
    sds sdskey = keyobj->ptr;
    rax *ids = raxFind(TrackingTable,(unsigned char*)sdskey,sdslen(sdskey));
    if (ids == raxNotFound) return;;

    raxIterator ri;
    raxStart(&ri,ids);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        uint64_t id;
        memcpy(&id,ri.key,sizeof(id));
        client *c = lookupClientByID(id);
        if (c == NULL || !(c->flags & CLIENT_TRACKING)) continue;
        sendTrackingMessage(c,sdskey,sdslen(sdskey));
    }
    raxStop(&ri);

    /* Free the tracking table: we'll create the radix tree and populate it
     * again if more keys will be modified in this caching slot. */
    TrackingTableTotalItems -= raxSize(ids);
    raxFree(ids);
    raxRemove(TrackingTable,(unsigned char*)sdskey,sdslen(sdskey),NULL);
}

/* This function is called when one or all the Redis databases are flushed
 * (dbid == -1 in case of FLUSHALL). Caching slots are not specific for
 * each DB but are global: currently what we do is sending a special
 * notification to clients with tracking enabled, invalidating the caching
 * slot "-1", which means, "all the keys", in order to avoid flooding clients
 * with many invalidation messages for all the keys they may hold.
 *
 * However trying to flush the tracking table here is very costly:
 * we need scanning 16 million caching slots in the table to check
 * if they are used, this introduces a big delay. So what we do is to really
 * flush the table in the case of FLUSHALL. When a FLUSHDB is called instead
 * we just send the invalidation message to all the clients, but don't
 * flush the table: it will slowly get garbage collected as more keys
 * are modified in the used caching slots. */
void freeTrackingRadixTree(void *rt) {
    raxFree(rt);
}

void trackingInvalidateKeysOnFlush(int dbid) {
    if (server.tracking_clients) {
        listNode *ln;
        listIter li;
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = listNodeValue(ln);
            if (c->flags & CLIENT_TRACKING) {
                sendTrackingMessage(c,"",1);
            }
        }
    }

    /* In case of FLUSHALL, reclaim all the memory used by tracking. */
    if (dbid == -1 && TrackingTable) {
        raxFreeWithCallback(TrackingTable,freeTrackingRadixTree);
        TrackingTableTotalItems = 0;
    }
}

/* Tracking forces Redis to remember information about which client may have
 * certain keys. In workloads where there are a lot of reads, but keys are
 * hardly modified, the amount of information we have to remember server side
 * could be a lot, with the number of keys being totally not bound.
 *
 * So Redis allows the user to configure a maximum number of keys for the
 * invalidation table. This function makes sure that we don't go over the
 * specified fill rate: if we are over, we can just evict informations about
 * a random key, and send invalidation messages to clients like if the key was
 * modified. */
void trackingLimitUsedSlots(void) {
    static unsigned int timeout_counter = 0;
    if (TrackingTable == NULL) return;
    if (server.tracking_table_max_keys == 0) return; /* No limits set. */
    size_t max_keys = server.tracking_table_max_keys;
    if (raxSize(TrackingTable) <= max_keys) {
        timeout_counter = 0;
        return; /* Limit not reached. */
    }

    /* We have to invalidate a few keys to reach the limit again. The effort
     * we do here is proportional to the number of times we entered this
     * function and found that we are still over the limit. */
    int effort = 100 * (timeout_counter+1);

    /* We just remove one key after another by using a random walk. */
    raxIterator ri;
    raxStart(&ri,TrackingTable);
    while(effort > 0) {
        effort--;
        raxSeek(&ri,"^",NULL,0);
        raxRandomWalk(&ri,0);
        rax *ids = ri.data;
        TrackingTableTotalItems -= raxSize(ids);
        raxFree(ids);
        raxRemove(TrackingTable,ri.key,ri.key_len,NULL);
        if (raxSize(TrackingTable) <= max_keys) {
            timeout_counter = 0;
            raxStop(&ri);
            return; /* Return ASAP: we are again under the limit. */
        }
    }

    /* If we reach this point, we were not able to go under the configured
     * limit using the maximum effort we had for this run. */
    raxStop(&ri);
    timeout_counter++;
}

/* This is just used in order to access the amount of used slots in the
 * tracking table. */
uint64_t trackingGetTotalItems(void) {
    return TrackingTableTotalItems;
}

uint64_t trackingGetTotalKeys(void) {
    return raxSize(TrackingTable);
}
