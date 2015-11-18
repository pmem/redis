/* Redis Object implementation.
 *
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
#include <ctype.h>
#include "pm.h"

#ifdef __CYGWIN__
#define strtold(a,b) ((long double)strtod((a),(b)))
#endif


/* Prepare object without persist.
 * If transaction is other that NULL, then external have to be call pm_persist on object.
 * */
static inline
void prepareObject_fast(int type, robj *o, void *ptr) {

    o->type = type;
    o->encoding = REDIS_ENCODING_RAW;
    o->onealloc = 0;
    o->ptr = ptr;
    o->refcount = 1;
    // Set the LRU to the current lruclock (minutes resolution).
    o->lru = server.lruclock;

}


/* Create object without persist.
 * If transaction is other that NULL, then external have to be call pm_persist on object.
 * */
static inline
robj *createObject_fast(int type, void *ptr, PM_TRANS trans) {
    robj *o = NULL;
    if(PM_TRANS_RAM!=trans)
    {
        o = pm_trans_alloc(trans, sizeof(*o), PM_TAG_ALLOC_OBJ,NULL);
    }
    else
    {
        o = zmalloc(sizeof(*o));
    }
    prepareObject_fast(type, o, ptr);
    return o;
}

robj *createObject(int type, void *ptr, PM_TRANS trans) {
    robj *o = createObject_fast(type, ptr,trans);
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,o, sizeof(robj));
    }
    return o;
}

robj *createStringObject(char *ptr, size_t len, PM_TRANS trans) {
    robj *o = NULL;

    if(PM_TRANS_RAM!=trans)
    {
        carg c = {0};
        size_t size = sizeof(robj) + sdsnewlen_alloc_size(len);
        c.ptr = ptr;
        c.len = len;
        /* Set the LRU to the current lruclock (minutes resolution). */
        c.lruclock = server.lruclock;
        o = pm_trans_alloc(trans, size, PM_TAG_ALLOC_OBJ,&c);
        /* Use one allocation to redis object and sds*/
       // void* sds_hdr = ((void*)(((uintptr_t)o) + (uintptr_t)sizeof(robj)));
       // sds str = sdsnewlen_prepare_buff(sds_hdr, ptr, len);
       // prepareObject_fast(REDIS_STRING, o, str);
        //o->onealloc=1;
        //o->refcount = 0;
        pmemobj_persist(trans,o, size);
    }
    else
    {
        o = createObject(REDIS_STRING,sdsnewlen_aligned(ptr,len,server.sds_alignment,trans),trans);
    }
    return o;
}

robj *createStringObjectFromLongLong(long long value, PM_TRANS trans) {
    robj *o;
    /* PM not support shared objects, because shared objects are only stored in RAM*/
    if (value >= 0 && value < REDIS_SHARED_INTEGERS && PM_TRANS_RAM==trans) {
        incrRefCount(shared.integers[value], PM_TRANS_RAM); /*Shared memory is only in ram*/
        o = shared.integers[value];
    } else {
        if (value >= LONG_MIN && value <= LONG_MAX) {
            o = createStringObject(NULL, 0, trans);
            o->encoding = REDIS_ENCODING_INT;
            o->ptr = (void*)((long)value);
            if(PM_TRANS_RAM!=trans)
            {
            	pmemobj_persist(trans,o, sizeof(robj));
            }
        } else {
            if(PM_TRANS_RAM == trans)
            {
                o = createObject(REDIS_STRING,sdsfromlonglong(value,trans), PM_TRANS_RAM);
            }
            else
            {
                sds sds_long = sdsfromlonglong(value,PM_TRANS_RAM);
                o = createStringObject(sds_long, sdslen(sds_long), trans);
                sdsfree(sds_long,PM_TRANS_RAM);
            }
        }
    }
    return o;
}

/* Create a string object from a long double. If humanfriendly is non-zero
 * it does not use exponential format and trims trailing zeroes at the end,
 * however this results in loss of precision. Otherwise exp format is used
 * and the output of snprintf() is not modified.
 *
 * The 'humanfriendly' option is used for INCRBYFLOAT and HINCRBYFLOAT. */
robj *createStringObjectFromLongDouble(long double value, int humanfriendly, PM_TRANS trans) {
    char buf[256];
    int len;

    if (isinf(value)) {
        /* Libc in odd systems (Hi Solaris!) will format infinite in a
         * different way, so better to handle it in an explicit way. */
        if (value > 0) {
            memcpy(buf,"inf",3);
            len = 3;
        } else {
            memcpy(buf,"-inf",4);
            len = 4;
        }
    } else if (humanfriendly) {
        /* We use 17 digits precision since with 128 bit floats that precision
         * after rounding is able to represent most small decimal numbers in a
         * way that is "non surprising" for the user (that is, most small
         * decimal numbers will be represented in a way that when converted
         * back into a string are exactly the same as what the user typed.) */
        len = snprintf(buf,sizeof(buf),"%.17Lf", value);
        /* Now remove trailing zeroes after the '.' */
        if (strchr(buf,'.') != NULL) {
            char *p = buf+len-1;
            while(*p == '0') {
                p--;
                len--;
            }
            if (*p == '.') len--;
        }
    } else {
        len = snprintf(buf,sizeof(buf),"%.17Lg", value);
    }
    return createStringObject(buf,len,trans);
}

size_t getObjectSize(robj *o)
{
    size_t size =  sizeof(robj);
    switch(o->type) {
        case REDIS_STRING:
            switch(o->encoding)
            {
                case REDIS_ENCODING_RAW:
                    size += sdslen(o->ptr);
                    break;
                case REDIS_ENCODING_INT:
                    //rob contain value
                    break;
                default:
                    redisPanic("Unknown string type");
            }
            break;
       /*case REDIS_LIST:

            break;
        case REDIS_SET:

            break;
        case REDIS_ZSET:

            break;
        case REDIS_HASH:

            break;*/
        default:
            redisPanic("Not supported type robj to copy");
            break;
        }
    return size;
}

robj *createValObject(robj *o, PM_TRANS trans)
{
    robj* copy = NULL;
    switch(o->type) {
    case REDIS_STRING:
        switch(o->encoding)
        {
            case REDIS_ENCODING_RAW:
                copy = dupStringObject(o, trans);
                break;
            case REDIS_ENCODING_INT:
            {
                //rob contain value
                long long value = 0;
                if(REDIS_OK == getLongLongFromObject(o, &value))
                {
                     copy = createStringObjectFromLongLong(value, trans);
                }
                else
                {
                    redisPanic("Encode error");
                }
                break;
            }
            default:
                redisPanic("Unknown string type");
        }

        break;
    default:
        redisPanic("Not supported type robj to copy");
        break;
    }


    return copy;
}

robj *dupStringObject(robj *o, PM_TRANS trans) {
    redisAssertWithInfo(NULL,o,o->encoding == REDIS_ENCODING_RAW);
    return createStringObject(o->ptr,sdslen(o->ptr),trans);
}

robj *createListObject(PM_TRANS trans) {
    list *l = listCreate(trans);
    robj *o = createObject_fast(REDIS_LIST,l,trans);
    listSetFreeMethod(l,decrRefCountVoid);
    o->encoding = REDIS_ENCODING_LINKEDLIST;
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,o, sizeof(robj));
    }
    return o;
}

robj *createZiplistObject(PM_TRANS trans) {
    unsigned char *zl = NULL;
    zl = ziplistNew(trans);
    robj *o = createObject_fast(REDIS_LIST,zl, trans);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,o, sizeof(robj));
    }
    return o;
}

robj *createSetObject(PM_TRANS trans) {
    dict *d = dictCreate(&setDictType,trans);
    robj *o = createObject_fast(REDIS_SET,d,trans);
    o->encoding = REDIS_ENCODING_HT;
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,o, sizeof(robj));
    }
    return o;
}

robj *createIntsetObject(PM_TRANS trans) {
    intset *is = intsetNew(trans);
    robj *o = createObject_fast(REDIS_SET,is,trans);
    o->encoding = REDIS_ENCODING_INTSET;
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,o, sizeof(robj));
    }
    return o;
}

robj *createHashObject(PM_TRANS trans) {
    unsigned char *zl = ziplistNew(trans);
    robj *o = createObject_fast(REDIS_HASH, zl,trans);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,o, sizeof(robj));
    }
    return o;
}

robj *createZsetObject(PM_TRANS trans) {
    zset *zs = NULL;
    if(PM_TRANS_RAM!=trans)
    {
        zs = pm_trans_alloc(trans, sizeof(*zs), PM_TAG_ALLOC_UNKN,NULL);
    }
    else
    {
        zs = zmalloc(sizeof(*zs));
    }
    robj *o;

    zs->dict = dictCreate(&zsetDictType,trans);
    zs->zsl = zslCreate(trans);
    o = createObject_fast(REDIS_ZSET,zs,trans);
    o->encoding = REDIS_ENCODING_SKIPLIST;
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,zs, sizeof(*zs));
    	pmemobj_persist(trans,o, sizeof(robj));
    }
    return o;
}

robj *createZsetZiplistObject(PM_TRANS trans) {
    unsigned char *zl = ziplistNew(trans);
    robj *o = createObject_fast(REDIS_ZSET,zl,trans);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,o, sizeof(robj));
    }
    return o;
}

void freeStringObject(robj *o, PM_TRANS trans) {
    if (o->encoding == REDIS_ENCODING_RAW) {
        if(0 == o->onealloc)
        {
            sdsfree(o->ptr, trans);
        }
        else
        {
            /*Check that pointer was not changed*/
            redisAssertWithInfo(NULL,o,(o->ptr == (((void*)(((uintptr_t)o) + (uintptr_t)sizeof(robj))) + sizeof(sds))));
        }
    }
}

void freeListObject(robj *o, PM_TRANS trans) {
    switch (o->encoding) {
    case REDIS_ENCODING_LINKEDLIST:
        listRelease((list*) o->ptr,trans);
        break;
    case REDIS_ENCODING_ZIPLIST:
        if(PM_TRANS_RAM!=trans)
        {
            pm_trans_free(trans, o->ptr);
        }
        else
        {
            zfree(o->ptr);
        }
        break;
    default:
        redisPanic("Unknown list encoding type");
    }
}

void freeSetObject(robj *o, PM_TRANS trans) {
    switch (o->encoding) {
    case REDIS_ENCODING_HT:
        redisAssertWithInfo(NULL,o,PM_TRANS_RAM==trans); /*Persistent memory not supported yet*/
        dictRelease((dict*) o->ptr);
        break;
    case REDIS_ENCODING_INTSET:
        if(PM_TRANS_RAM!=trans)
        {
            pm_trans_free(trans, o->ptr);
        }
        else
        {
            zfree(o->ptr);
        }
        break;
    default:
        redisPanic("Unknown set encoding type");
    }
}

void freeZsetObject_pm(robj *o, PM_TRANS trans) {
    zset *zs;
    switch (o->encoding) {
    case REDIS_ENCODING_SKIPLIST:
        zs = o->ptr;
        dictRelease(zs->dict);/*TODO: Fix dictRelease_pm*/
        zslFree(zs->zsl, trans);
        pm_trans_free(trans, zs);
        break;
    case REDIS_ENCODING_ZIPLIST:
        pm_trans_free(trans, o->ptr);
        break;
    default:
        redisPanic("Unknown sorted set encoding");
    }
}

void freeZsetObject(robj *o, PM_TRANS trans) {
    if(PM_TRANS_RAM!=trans)
    {
        freeZsetObject_pm(o, trans);
        return;
    }
    zset *zs;
    switch (o->encoding) {
    case REDIS_ENCODING_SKIPLIST:
        zs = o->ptr;
        dictRelease(zs->dict);
        zslFree(zs->zsl,trans);
        zfree(zs);
        break;
    case REDIS_ENCODING_ZIPLIST:
        zfree(o->ptr);
        break;
    default:
        redisPanic("Unknown sorted set encoding");
    }
}

void freeHashObject(robj *o, PM_TRANS trans) {
    switch (o->encoding) {
    case REDIS_ENCODING_HT:
        if(PM_TRANS_RAM!=trans)
        {
            redisPanic("Delete dict from PM not supported yet");
        }
        else
        {
            dictRelease((dict*) o->ptr);
        }
        break;
    case REDIS_ENCODING_ZIPLIST:
        if(PM_TRANS_RAM!=trans)
        {
            pm_trans_free(trans, o->ptr);
        }
        else
        {
            zfree(o->ptr);
        }
        break;
    default:
        redisPanic("Unknown hash encoding type");
        break;
    }
}

void incrRefCount(robj *o, PM_TRANS trans) {
    o->refcount++;
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,o, sizeof(robj));
    }
}
/* Decrement "Reference Counter" from robj without removal object */
void decrRefCountWithoutRemoval(robj *o, PM_TRANS trans) {
    o->refcount--;
    if(PM_TRANS_RAM!=trans)
    {
    	pmemobj_persist(trans,o, sizeof(robj));
    }
}

void decrRefCount(robj *o, PM_TRANS trans) {
    //if (o->refcount <= 0) redisPanic("decrRefCount against refcount <= 0");
    //if (o->refcount == 1) {
    if (o->refcount <= 1) {
        switch(o->type) {
        case REDIS_STRING: freeStringObject(o, trans); break;
        case REDIS_LIST: freeListObject(o, trans); break;
        case REDIS_SET: freeSetObject(o,trans); break;
        case REDIS_ZSET: freeZsetObject(o,trans); break;
        case REDIS_HASH: freeHashObject(o,trans); break;
        default: redisPanic("Unknown object type"); break;
        }
        if(PM_TRANS_RAM!=trans)
        {
            pm_trans_free(trans, o);
        }
        else
        {
            zfree(o);
        }
    } else {
        o->refcount--;
        if(PM_TRANS_RAM!=trans)
        {
        	pmemobj_persist(trans,o, sizeof(robj));
        }
    }
}

/* This variant of decrRefCount() gets its argument as void, and is useful
 * as free method in data structures that expect a 'void free_object(void*)'
 * prototype for the free method. */
void decrRefCountVoid(void *o, PM_TRANS trans) {
    decrRefCount(o, trans);
}

/* This function set the ref count to zero without freeing the object.
 * It is useful in order to pass a new object to functions incrementing
 * the ref count of the received object. Example:
 *
 *    functionThatWillIncrementRefCount(resetRefCount(CreateObject(...)));
 *
 * Otherwise you need to resort to the less elegant pattern:
 *
 *    *obj = createObject(...);
 *    functionThatWillIncrementRefCount(obj);
 *    decrRefCount(obj);
 */
robj *resetRefCount(robj *obj) {
    obj->refcount = 0;
    return obj;
}

int checkType(redisClient *c, robj *o, int type) {
    if (o->type != type) {
        addReply(c,shared.wrongtypeerr);
        return 1;
    }
    return 0;
}

int isObjectRepresentableAsLongLong(robj *o, long long *llval) {
    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
    if (o->encoding == REDIS_ENCODING_INT) {
        if (llval) *llval = (long) o->ptr;
        return REDIS_OK;
    } else {
        return string2ll(o->ptr,sdslen(o->ptr),llval) ? REDIS_OK : REDIS_ERR;
    }
}

/* Try to encode a string object in order to save space */
robj *tryObjectEncoding(robj *o, PM_TRANS trans) {
    long value;
    sds s = o->ptr;
    size_t len;
    if (o->encoding != REDIS_ENCODING_RAW)
        return o; /* Already encoded */
    /* It's not safe to encode shared objects: shared objects can be shared
     * everywhere in the "object space" of Redis. Encoded objects can only
     * appear as "values" (and not, for instance, as keys) */
     if (o->refcount > 1) return o;
    /* Currently we try to encode only strings */
    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
    /* Check if we can represent this string as a long integer */
    len = sdslen(s);
    if (len > 21 || !string2l(s,len,&value)) {
        /* We can't encode the object...
         *
         * Do the last try, and at least optimize the SDS string inside
         * the string object to require little space, in case there
         * is more than 10% of free space at the end of the SDS string.
         *
         * We do that for larger strings, using the arbitrary value
         * of 32 bytes. This code was backported from the unstable branch
         * where this is performed when the object is too large to be
         * encoded as EMBSTR. */
        if (len > 32 &&
            o->encoding == REDIS_ENCODING_RAW &&
            sdsavail(s) > len/10)
        {
            o->ptr = sdsRemoveFreeSpace(o->ptr,trans);
            if(PM_TRANS_RAM!=trans)
           {
            	pmemobj_persist(trans,o, sizeof(robj));
           }
        }
        /* Return the original object. */
        return o;
    }

    /* Ok, this object can be encoded...
     *
     * Can I use a shared object? Only if the object is inside a given range
     *
     * Note that we also avoid using shared integers when maxmemory is used
     * because every object needs to have a private LRU field for the LRU
     * algorithm to work well. */
    if ((server.maxmemory == 0 ||
         (server.maxmemory_policy != REDIS_MAXMEMORY_VOLATILE_LRU &&
          server.maxmemory_policy != REDIS_MAXMEMORY_ALLKEYS_LRU)) &&
        value >= 0 && value < REDIS_SHARED_INTEGERS && PM_TRANS_RAM == trans)
    {
        decrRefCount(o, PM_TRANS_RAM);
        incrRefCount(shared.integers[value],PM_TRANS_RAM); /*Shared memory is only in ram*/
        return shared.integers[value];
    } else {
        robj *o_new = createObject_fast(REDIS_STRING, (void*) value, trans);
        o_new->encoding = REDIS_ENCODING_INT;
        if(PM_TRANS_RAM!=trans)
        {
        	pmemobj_persist(trans,o_new, sizeof(robj));
        }
        decrRefCount(o, trans);
        return o_new;
    }
}

/* Get a decoded version of an encoded object (returned as a new object).
 * If the object is already raw-encoded just increment the ref count. */
robj *getDecodedObject(robj *o, PM_TRANS trans) {
    robj *dec;

    if (o->encoding == REDIS_ENCODING_RAW) {
        incrRefCount(o,trans);
        return o;
    }
    if (o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_INT) {
        char buf[32];

        ll2string(buf,32,(long)o->ptr);
        dec = createStringObject(buf,strlen(buf), trans);
        return dec;
    } else {
        redisPanic("Unknown encoding type");
    }
}

/* Compare two string objects via strcmp() or strcoll() depending on flags.
 * Note that the objects may be integer-encoded. In such a case we
 * use ll2string() to get a string representation of the numbers on the stack
 * and compare the strings, it's much faster than calling getDecodedObject().
 *
 * Important note: when REDIS_COMPARE_BINARY is used a binary-safe comparison
 * is used. */

#define REDIS_COMPARE_BINARY (1<<0)
#define REDIS_COMPARE_COLL (1<<1)

int compareStringObjectsWithFlags(robj *a, robj *b, int flags) {
    redisAssertWithInfo(NULL,a,a->type == REDIS_STRING && b->type == REDIS_STRING);
    char bufa[128], bufb[128], *astr, *bstr;
    size_t alen, blen, minlen;

    if (a == b) return 0;
    if (a->encoding != REDIS_ENCODING_RAW) {
        alen = ll2string(bufa,sizeof(bufa),(long) a->ptr);
        astr = bufa;
    } else {
        astr = a->ptr;
        alen = sdslen(astr);
    }
    if (b->encoding != REDIS_ENCODING_RAW) {
        blen = ll2string(bufb,sizeof(bufb),(long) b->ptr);
        bstr = bufb;
    } else {
        bstr = b->ptr;
        blen = sdslen(bstr);
    }
    if (flags & REDIS_COMPARE_COLL) {
        return strcoll(astr,bstr);
    } else {
        int cmp;

        minlen = (alen < blen) ? alen : blen;
        cmp = memcmp(astr,bstr,minlen);
        if (cmp == 0) return alen-blen;
        return cmp;
    }
}

/* Wrapper for compareStringObjectsWithFlags() using binary comparison. */
int compareStringObjects(robj *a, robj *b) {
    return compareStringObjectsWithFlags(a,b,REDIS_COMPARE_BINARY);
}

/* Wrapper for compareStringObjectsWithFlags() using collation. */
int collateStringObjects(robj *a, robj *b) {
    return compareStringObjectsWithFlags(a,b,REDIS_COMPARE_COLL);
}

/* Equal string objects return 1 if the two objects are the same from the
 * point of view of a string comparison, otherwise 0 is returned. Note that
 * this function is faster then checking for (compareStringObject(a,b) == 0)
 * because it can perform some more optimization. */
int equalStringObjects(robj *a, robj *b) {
    if (a->encoding != REDIS_ENCODING_RAW && b->encoding != REDIS_ENCODING_RAW){
        return a->ptr == b->ptr;
    } else {
        return compareStringObjects(a,b) == 0;
    }
}

size_t stringObjectLen(robj *o) {
    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
    if (o->encoding == REDIS_ENCODING_RAW) {
        return sdslen(o->ptr);
    } else {
        char buf[32];

        return ll2string(buf,32,(long)o->ptr);
    }
}

int getDoubleFromObject(robj *o, double *target) {
    double value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
        if (o->encoding == REDIS_ENCODING_RAW) {
            errno = 0;
            value = strtod(o->ptr, &eptr);
            if (isspace(((char*)o->ptr)[0]) ||
                eptr[0] != '\0' ||
                (errno == ERANGE &&
                    (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
                errno == EINVAL ||
                isnan(value))
                return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }
    *target = value;
    return REDIS_OK;
}

int getDoubleFromObjectOrReply(redisClient *c, robj *o, double *target, const char *msg) {
    double value;
    if (getDoubleFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a valid float");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}

int getLongDoubleFromObject(robj *o, long double *target) {
    long double value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
        if (o->encoding == REDIS_ENCODING_RAW) {
            errno = 0;
            value = strtold(o->ptr, &eptr);
            if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' ||
                errno == ERANGE || isnan(value))
                return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }
    *target = value;
    return REDIS_OK;
}

int getLongDoubleFromObjectOrReply(redisClient *c, robj *o, long double *target, const char *msg) {
    long double value;
    if (getLongDoubleFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a valid float");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}

int getLongLongFromObject(robj *o, long long *target) {
    long long value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
        if (o->encoding == REDIS_ENCODING_RAW) {
            errno = 0;
            value = strtoll(o->ptr, &eptr, 10);
            if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' ||
                errno == ERANGE)
                return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }
    if (target) *target = value;
    return REDIS_OK;
}

int getLongLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char *msg) {
    long long value;
    if (getLongLongFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not an integer or out of range");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}

int getLongFromObjectOrReply(redisClient *c, robj *o, long *target, const char *msg) {
    long long value;

    if (getLongLongFromObjectOrReply(c, o, &value, msg) != REDIS_OK) return REDIS_ERR;
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is out of range");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}

char *strEncoding(int encoding) {
    switch(encoding) {
    case REDIS_ENCODING_RAW: return "raw";
    case REDIS_ENCODING_INT: return "int";
    case REDIS_ENCODING_HT: return "hashtable";
    case REDIS_ENCODING_LINKEDLIST: return "linkedlist";
    case REDIS_ENCODING_ZIPLIST: return "ziplist";
    case REDIS_ENCODING_INTSET: return "intset";
    case REDIS_ENCODING_SKIPLIST: return "skiplist";
    default: return "unknown";
    }
}

/* Given an object returns the min number of seconds the object was never
 * requested, using an approximated LRU algorithm. */
unsigned long estimateObjectIdleTime(robj *o) {
    if (server.lruclock >= o->lru) {
        return (server.lruclock - o->lru) * REDIS_LRU_CLOCK_RESOLUTION;
    } else {
        return ((REDIS_LRU_CLOCK_MAX - o->lru) + server.lruclock) *
                    REDIS_LRU_CLOCK_RESOLUTION;
    }
}

/* This is a helper function for the OBJECT command. We need to lookup keys
 * without any modification of LRU or other parameters. */
robj *objectCommandLookup(redisClient *c, robj *key) {
    dictEntry *de;

    if ((de = dictFind(c->db->dict,key->ptr)) == NULL) return NULL;
    return (robj*) dictGetVal(de);
}

robj *objectCommandLookupOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c,key);

    if (!o) addReply(c, reply);
    return o;
}

/* Object command allows to inspect the internals of an Redis Object.
 * Usage: OBJECT <refcount|encoding|idletime> <key> */
void objectCommand(redisClient *c) {
    robj *o;

    if (!strcasecmp(c->argv[1]->ptr,"refcount") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,o->refcount);
    } else if (!strcasecmp(c->argv[1]->ptr,"encoding") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyBulkCString(c,strEncoding(o->encoding));
    } else if (!strcasecmp(c->argv[1]->ptr,"idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,estimateObjectIdleTime(o));
    } else {
        addReplyError(c,"Syntax error. Try OBJECT (refcount|encoding|idletime)");
    }
}

