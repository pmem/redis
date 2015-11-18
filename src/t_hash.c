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
#include <math.h>

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/* Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end, PM_TRANS trans) {
    int i;

    if (o->encoding != REDIS_ENCODING_ZIPLIST) return;

    for (i = start; i <= end; i++) {
        if (argv[i]->encoding == REDIS_ENCODING_RAW &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            hashTypeConvert(o, REDIS_ENCODING_HT, trans);
            break;
        }
    }
}

/* Encode given objects in-place when the hash uses a dict. */
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2, PM_TRANS trans) {
    if (subject->encoding == REDIS_ENCODING_HT) {
        if (o1) *o1 = tryObjectEncoding(*o1,trans);
        if (o2) *o2 = tryObjectEncoding(*o2,trans);
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
int hashTypeGetFromZiplist(robj *o, robj *field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll,
                           PM_TRANS trans)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);

    field = getDecodedObject(field,trans);

    zl = o->ptr;
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            vptr = ziplistNext(zl, fptr);
            redisAssert(vptr != NULL);
        }
    }

    decrRefCount(field,trans);

    if (vptr != NULL) {
        ret = ziplistGet(vptr, vstr, vlen, vll);
        redisAssert(ret);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
int hashTypeGetFromHashTable(robj *o, robj *field, robj **value) {
    dictEntry *de;

    redisAssert(o->encoding == REDIS_ENCODING_HT);

    de = dictFind(o->ptr, field);
    if (de == NULL) return -1;
    *value = dictGetVal(de);
    return 0;
}

/* Higher level function of hashTypeGet*() that always returns a Redis
 * object (either new or with refcount incremented), so that the caller
 * can retain a reference or call decrRefCount after the usage.
 *
 * The lower level function can prevent copy on write so it is
 * the preferred way of doing read operations. */
robj *hashTypeGetObject(robj *o, robj *field, PM_TRANS trans) {
    robj *value = NULL;

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll, trans) == 0) {
            if (vstr) {
                value = createStringObject((char*)vstr, vlen, trans);
            } else {
                value = createStringObjectFromLongLong(vll, trans);
            }
        }

    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0) {
            incrRefCount(aux,PM_TRANS_RAM);
            value = aux;
        }
    } else {
        redisPanic("Unknown hash encoding");
    }
    return value;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */
int hashTypeExists(robj *o, robj *field, PM_TRANS trans) {
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll, trans) == 0) return 1;
    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0) return 1;
    } else {
        redisPanic("Unknown hash encoding");
    }
    return 0;
}

/* Add an element, discard the old if the key already exists.
 * Return 0 on insert and 1 on update.
 * This function will take care of incrementing the reference count of the
 * retained fields and value objects. */
int hashTypeSet(robj *o, robj *field, robj *value, PM_TRANS trans) {
    int update = 0;

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        field = getDecodedObject(field,trans);
        value = getDecodedObject(value,trans);

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = ziplistNext(zl, fptr);
                redisAssert(vptr != NULL);
                update = 1;

                /* Delete value */
                zl = ziplistDelete(zl, &vptr,trans);

                /* Insert new value */
                zl = ziplistInsert(zl, vptr, value->ptr, sdslen(value->ptr),trans);
            }
        }

        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            zl = ziplistPush(zl, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL,trans);
            zl = ziplistPush(zl, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL,trans);
        }
        o->ptr = zl;
        decrRefCount(field,trans);
        decrRefCount(value,trans);

        /* Check if the ziplist needs to be converted to a hash table */
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, REDIS_ENCODING_HT, trans);
    } else if (o->encoding == REDIS_ENCODING_HT) {
        if (dictReplace(o->ptr, field, value,trans)) { /* Insert */
            incrRefCount(field,trans);
        } else { /* Update */
            update = 1;
        }
        incrRefCount(value,trans);
    } else {
        redisPanic("Unknown hash encoding");
    }
    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
int hashTypeDelete(robj *o, robj *field, PM_TRANS trans) {
    int deleted = 0;

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;

        field = getDecodedObject(field,trans);

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                zl = ziplistDelete(zl,&fptr,trans);
                zl = ziplistDelete(zl,&fptr,trans);
                o->ptr = zl;
                deleted = 1;
            }
        }

        decrRefCount(field,trans);

    } else if (o->encoding == REDIS_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == REDIS_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        redisPanic("Unknown hash encoding");
    }

    return deleted;
}

/* Return the number of elements in a hash. */
unsigned long hashTypeLength(robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == REDIS_ENCODING_HT) {
        length = dictSize((dict*)o->ptr);
    } else {
        redisPanic("Unknown hash encoding");
    }

    return length;
}

hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        redisPanic("Unknown hash encoding");
    }

    return hi;
}

void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == REDIS_ENCODING_HT) {
        dictReleaseIterator(hi->di);
    }

    zfree(hi);
}

/* Move to the next entry in the hash. Return REDIS_OK when the next entry
 * could be found and REDIS_ERR when the iterator reaches the end. */
int hashTypeNext(hashTypeIterator *hi) {
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            redisAssert(vptr == NULL);
            fptr = ziplistIndex(zl, 0);
        } else {
            /* Advance cursor */
            redisAssert(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }
        if (fptr == NULL) return REDIS_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        vptr = ziplistNext(zl, fptr);
        redisAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return REDIS_ERR;
    } else {
        redisPanic("Unknown hash encoding");
    }
    return REDIS_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    redisAssert(hi->encoding == REDIS_ENCODING_ZIPLIST);

    if (what & REDIS_HASH_KEY) {
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        redisAssert(ret);
    } else {
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        redisAssert(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromHashTable`. */
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst) {
    redisAssert(hi->encoding == REDIS_ENCODING_HT);

    if (what & REDIS_HASH_KEY) {
        *dst = dictGetKey(hi->de);
    } else {
        *dst = dictGetVal(hi->de);
    }
}

/* A non copy-on-write friendly but higher level version of hashTypeCurrent*()
 * that returns an object with incremented refcount (or a new object). It is up
 * to the caller to decrRefCount() the object if no reference is retained. */
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what, PM_TRANS trans) {
    robj *dst;

    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            dst = createStringObject((char*)vstr, vlen, trans);
        } else {
            dst = createStringObjectFromLongLong(vll, trans);
        }

    } else if (hi->encoding == REDIS_ENCODING_HT) {
        hashTypeCurrentFromHashTable(hi, what, &dst);
        incrRefCount(dst,trans);

    } else {
        redisPanic("Unknown hash encoding");
    }

    return dst;
}

robj *hashTypeLookupWriteOrCreate(redisClient *c, robj *key, PM_TRANS trans) {
    robj *o = lookupKeyWrite(c->db,key);
    if (o == NULL) {
        o = createHashObject(trans);
        dbAdd(c->db,key,o, trans);
    } else {
        if (o->type != REDIS_HASH) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
    return o;
}

void hashTypeConvertZiplist(robj *o, int enc, PM_TRANS trans) {
    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);

    if (enc == REDIS_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    } else if (enc == REDIS_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        hi = hashTypeInitIterator(o);
        dict = dictCreate(&hashDictType, NULL);

        while (hashTypeNext(hi) != REDIS_ERR) {
            robj *field, *value;

            field = hashTypeCurrentObject(hi, REDIS_HASH_KEY, trans);
            field = tryObjectEncoding(field, trans);
            value = hashTypeCurrentObject(hi, REDIS_HASH_VALUE, trans);
            value = tryObjectEncoding(value, trans);
            ret = dictAdd(dict, field, value, trans);
            if (ret != DICT_OK) {
                redisLogHexDump(REDIS_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                redisAssert(ret == DICT_OK);
            }
        }

        hashTypeReleaseIterator(hi);
        zfree(o->ptr);

        o->encoding = REDIS_ENCODING_HT;
        o->ptr = dict;

    } else {
        redisPanic("Unknown hash encoding");
    }
}

void hashTypeConvert(robj *o, int enc, PM_TRANS trans) {
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        hashTypeConvertZiplist(o, enc, trans);
    } else if (o->encoding == REDIS_ENCODING_HT) {
        redisPanic("Not implemented");
    } else {
        redisPanic("Unknown hash encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

void hsetCommand(redisClient *c) {
    int update;
    robj *o;

    /*Check that value should be keep in persistent memory*/
    PM_TRANS trans = PM_TRANS_RAM;
    if(c->db->dict->type->persistent)
    {
        trans = pm_pool;
    }

    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1],trans)) == NULL)
    {
        if(PM_TRANS_RAM!=trans)
        {
            /* Finish transaction*/
            pm_trans_end(trans);
        }
        return;
    }

    hashTypeTryConversion(o,c->argv,2,3,trans);
    hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3],trans);
    update = hashTypeSet(o,c->argv[2],c->argv[3],trans);
    addReply(c, update ? shared.czero : shared.cone);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty++;

    if(PM_TRANS_RAM!=trans)
    {
        /* Finish transaction*/
        pm_trans_end(trans);
    }
}

void hsetnxCommand(redisClient *c) {
    robj *o;
    /*Check that value should be keep in persistent memory*/
    PM_TRANS trans = PM_TRANS_RAM;
    if(c->db->dict->type->persistent)
    {
        trans = pm_pool;
    }

    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1],trans)) == NULL)
    {
        if(PM_TRANS_RAM!=trans)
        {
            /* Finish transaction*/
            pm_trans_end(trans);
        }
        return;
    }
    hashTypeTryConversion(o,c->argv,2,3,trans);

    if (hashTypeExists(o, c->argv[2],trans)) {
        addReply(c, shared.czero);
    } else {
        hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3], trans);
        hashTypeSet(o,c->argv[2],c->argv[3],trans);
        addReply(c, shared.cone);
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hset",c->argv[1],c->db->id);
        server.dirty++;
    }

    if(PM_TRANS_RAM!=trans)
    {
        /* Finish transaction*/
        pm_trans_end(trans);
    }
}

void hmsetCommand(redisClient *c) {
    int i;
    robj *o;

    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }

    /*Check that value should be keep in persistent memory*/
    PM_TRANS trans = PM_TRANS_RAM;
    if(c->db->dict->type->persistent)
    {
        trans = pm_pool;
    }

    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1],trans)) == NULL)
    {
        if(PM_TRANS_RAM!=trans)
        {
            /* Finish transaction*/
            pm_trans_end(trans);
        }
        return;
    }
    hashTypeTryConversion(o,c->argv,2,c->argc-1,trans);
    for (i = 2; i < c->argc; i += 2) {
        hashTypeTryObjectEncoding(o,&c->argv[i], &c->argv[i+1], trans);
        hashTypeSet(o,c->argv[i],c->argv[i+1],trans);
    }

    if(PM_TRANS_RAM!=trans)
    {
        /* Finish transaction*/
        pm_trans_end(trans);
    }

    addReply(c, shared.ok);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty++;
}

void hincrbyCommand(redisClient *c) {
    long long value, incr, oldvalue;
    robj *o, *current, *new;

    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != REDIS_OK) return;

    /*Check that value should be keep in persistent memory*/
    PM_TRANS trans = PM_TRANS_RAM;
    if(c->db->dict->type->persistent)
    {
        trans = pm_pool;
    }

    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1], trans)) == NULL)
    {
        if(PM_TRANS_RAM!=trans)
        {
            pm_trans_end(trans);
        }
        return;
    }
    if ((current = hashTypeGetObject(o,c->argv[2],trans)) != NULL) {
        if (getLongLongFromObjectOrReply(c,current,&value,
            "hash value is not an integer") != REDIS_OK) {
            decrRefCount(current,trans);
            if(PM_TRANS_RAM!=trans)
            {
                /* Finish transaction*/
                pm_trans_end(trans);
            }
            return;
        }
        decrRefCount(current,trans);
    } else {
        value = 0;
    }

    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }

    value += incr;
    new = createStringObjectFromLongLong(value,trans);
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL,trans);
    hashTypeSet(o,c->argv[2],new,trans);
    decrRefCount(new,trans);

    if(PM_TRANS_RAM!=trans)
    {
        /* Finish transaction*/
        pm_trans_end(trans);
    }

    addReplyLongLong(c,value);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    server.dirty++;
}

void hincrbyfloatCommand(redisClient *c) {
    double long value, incr;
    robj *o, *current, *new, *aux;

    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != REDIS_OK) return;

    /*Check that value should be keep in persistent memory*/
    PM_TRANS trans = PM_TRANS_RAM;
    if(c->db->dict->type->persistent)
    {
        trans = pm_pool;
    }

    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1],trans)) == NULL){
        if(PM_TRANS_RAM!=trans)
        {
            /* Finish transaction*/
            pm_trans_end(trans);
        }
        return;
    }
    if ((current = hashTypeGetObject(o,c->argv[2],trans)) != NULL) {
        if (getLongDoubleFromObjectOrReply(c,current,&value,
            "hash value is not a valid float") != REDIS_OK) {
            decrRefCount(current,trans);
            if(PM_TRANS_RAM!=trans)
            {
                /* Finish transaction*/
                pm_trans_end(trans);
            }
            return;
        }
        decrRefCount(current,trans);
    } else {
        value = 0;
    }



    value += incr;
    new = createStringObjectFromLongDouble(value,1,trans);
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL,trans);
    hashTypeSet(o,c->argv[2],new,trans);
    addReplyBulk(c,new);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    aux = createStringObject("HSET",4,trans);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux,trans);
    rewriteClientCommandArgument(c,3,new);
    decrRefCount(new,trans);

    if(PM_TRANS_RAM!=trans)
    {
        /* Finish transaction*/
        pm_trans_end(trans);
    }
}

static void addHashFieldToReply(redisClient *c, robj *o, robj *field, PM_TRANS trans) {
    int ret;

    if (o == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll, trans);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            if (vstr) {
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                addReplyBulkLongLong(c, vll);
            }
        }

    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *value;

        ret = hashTypeGetFromHashTable(o, field, &value);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            addReplyBulk(c, value);
        }

    } else {
        redisPanic("Unknown hash encoding");
    }
}

void hgetCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;
    /*Check that value should be keep in persistent memory*/
    PM_TRANS trans = PM_TRANS_RAM;
    if(c->db->dict->type->persistent)
    {
        trans = pm_pool;
    }

    addHashFieldToReply(c, o, c->argv[2],trans);

    if(PM_TRANS_RAM!=trans)
    {
        /* Finish transaction*/
        pm_trans_end(trans);
    }
}

void hmgetCommand(redisClient *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    o = lookupKeyRead(c->db, c->argv[1]);
    if (o != NULL && o->type != REDIS_HASH) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    /*Check that value should be keep in persistent memory*/
    PM_TRANS trans = PM_TRANS_RAM;
    if(c->db->dict->type->persistent)
    {
        trans = pm_pool;
    }

    addReplyMultiBulkLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i],trans);
    }

    if(PM_TRANS_RAM!=trans)
    {
        /* Finish transaction*/
        pm_trans_end(trans);
    }
}

void hdelCommand(redisClient *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    /*Check that value should be keep in persistent memory*/
    PM_TRANS trans = PM_TRANS_RAM;
    if(c->db->dict->type->persistent)
    {
        trans = pm_pool;
    }


    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o,c->argv[j],trans)) {
            deleted++;
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }

    if(PM_TRANS_RAM!=trans)
    {
        /* Finish transaction*/
        pm_trans_end(trans);
    }


    if (deleted) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

void hlenCommand(redisClient *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    addReplyLongLong(c,hashTypeLength(o));
}

static void addHashIteratorCursorToReply(redisClient *c, hashTypeIterator *hi, int what) {
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }

    } else if (hi->encoding == REDIS_ENCODING_HT) {
        robj *value;

        hashTypeCurrentFromHashTable(hi, what, &value);
        addReplyBulk(c, value);

    } else {
        redisPanic("Unknown hash encoding");
    }
}

void genericHgetallCommand(redisClient *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int multiplier = 0;
    int length, count = 0;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
        || checkType(c,o,REDIS_HASH)) return;

    if (flags & REDIS_HASH_KEY) multiplier++;
    if (flags & REDIS_HASH_VALUE) multiplier++;

    length = hashTypeLength(o) * multiplier;
    addReplyMultiBulkLen(c, length);

    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != REDIS_ERR) {
        if (flags & REDIS_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_KEY);
            count++;
        }
        if (flags & REDIS_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_VALUE);
            count++;
        }
    }

    hashTypeReleaseIterator(hi);
    redisAssert(count == length);
}

void hkeysCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_HASH_KEY);
}

void hvalsCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_HASH_VALUE);
}

void hgetallCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_HASH_KEY|REDIS_HASH_VALUE);
}

void hexistsCommand(redisClient *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    /*Check that value should be keep in persistent memory*/
    PM_TRANS trans = PM_TRANS_RAM;
    if(c->db->dict->type->persistent)
    {
        trans = pm_pool;
    }

    addReply(c, hashTypeExists(o,c->argv[2], trans) ? shared.cone : shared.czero);

    if(PM_TRANS_RAM!=trans)
    {
        /* Finish transaction*/
        pm_trans_end(trans);
    }
}

void hscanCommand(redisClient *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == REDIS_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;
    scanGenericCommand(c,o,cursor);
}
