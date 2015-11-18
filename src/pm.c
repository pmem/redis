/*
 * Copyright (c) 2014, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


#include "pm.h"

#include <pthread.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "zmalloc.h"
#include "dict.h"
#include <assert.h>
#include <unistd.h>
#include <libpmemobj.h>
#include <string.h>
#include "sds.h"
#include "redis.h"




extern int PMTrace; /*Debug flag for persistent memory*/
extern int PMTransDebug; /*Debug flag for transaction, to execute immediately commands in debug, If enable then trans is not safe.*/

/*static PMEMobjpool *pm_pool = NULL;*/
PMEMobjpool *pm_pool = NULL;
static size_t current_alloc_size = 0;
static size_t max_alloc_size = 0;
int pm_memory_counter = NULL;
/* Pool reference stored for "free" operation to not retrieve it each time */
uint64_t uuidTemp = NULL;


/*PMEMobjpool* pm_pool
{
    return pm_pool;
}
*/

/* wrapper for memcpy + pm_persist */
/* just for profiling */
void* pmem_memcpy_persistPM(void *dst, const void *src, size_t size)
{
    pmemobj_memcpy_persist(pm_pool,dst,src,size);
    /*memcpy(dst, src, size);
     *pmem_persist_pool(dst, size, 0);*/
    return dst;
}
/* TODO: check if it is possible with NVML*/
size_t pm_malloc_size(void *ptr)
{
    assert(pm_pool);
    size_t size = 0;

    /*void* alloc_ = PM_OFF(pm_pool, ptr);
     size = pmemalloc_get_usable_size(pm_pool, alloc_);*/
    return size;
}

void pm_trans_clean(PM_TRANS trans)
{
    /* TODO: check if this is still needed
     *pmemalloc_trans_clean(trans);*/
}

void pm_trans_end(PM_TRANS trans)
{
    /* TODO: check if this is still needed
     *pmemalloc_trans_end(trans);*/
}


/* Prepare object without persist.
 * If transaction is other that NULL, then external have to be call pm_persist on object.
 * */
void prepareObject_fast(int type, robj *o, void *ptr,carg *d) {
    o->type = type;
    o->encoding = REDIS_ENCODING_RAW;
    o->onealloc = 1;
    o->ptr = ptr;
    o->refcount = 0; /*it is newly created object, no references yet*/
    /* Set the LRU to the current lruclock (minutes resolution). */
    o->lru = d->lruclock;
}


/* Prepares "Redis Object" - string - using input data. Called only during String (value) allocation */
void stringConstructor(PM_TRANS pool,void *ptr,void *data)
{
    robj *o = ptr;
    carg *d = data;
    /*size_t size = sizeof(robj) + sdsnewlen_alloc_size(d->len);*/
    /* Use one allocation to redis object and sds*/
    void* sds_hdr = ((void*)(((uintptr_t)o) + (uintptr_t)sizeof(robj)));
    sds str = sdsnewlen_prepare_buff(sds_hdr, d->ptr, d->len);
    prepareObject_fast(REDIS_STRING, o, str,d);

}

/* Prepares "Entity" - object of dictionary. Called only during Entity allocation */
void entityConstructor(PM_TRANS pool,void *ptr,void *arg)
{
    dictEntry *entry = ptr;
    carg *data = arg;
    dict *d = data->dict;
    robj *key = data->ptr;
    /* Copy key to once allocation with dictEntry */
    void* sds_hdr = ((void*)(((uintptr_t)entry) + (uintptr_t)sizeof(*entry)));
    sds sds_key = sdsnewlen_prepare_buff(sds_hdr, key, data->len); /* Copy key value to new allocation */
    dictSetKey(d, entry, sds_key); /* Set value Key ptr in dictEntry, but not call pm_persist() */
}


/* Allocates memory for new object, fills it with input data */
void* pm_trans_alloc(PM_TRANS trans, size_t size, size_t pm_tag, void *c)
{

    PMEMoid newPm;
    assert(pm_pool && trans);
   switch(pm_tag)
   {
       case PM_TAG_ALLOC_OBJ:
            if(0!= pmemobj_alloc(pm_pool,&newPm,size,pm_tag,stringConstructor,c))
                return NULL;
            break;
       case PM_TAG_ALLOC_ENTITY:
            if(0!= pmemobj_alloc(pm_pool,&newPm,size,pm_tag,entityConstructor,c))
                return NULL;
            break;
   }
   /*printf("\n new PM = %p",newPm.off);*/
   /*printf("\n new PM direct = %p",pmemobj_direct(newPm));*/
   /*printf("\n previous size = %d, new object size = %d, requested size = %d",get_current_alloc_size(),pmemobj_alloc_usable_size(newPm),size);*/
   if(pm_memory_counter)
	   increase_current_alloc_size(pmemobj_alloc_usable_size(newPm));
   /*printf("\n current size = %d, max size = %d",get_current_alloc_size(),get_max_alloc_size());*/

   /* printf("\n object type = %d size = %d, real size = %d",pm_tag,size,pmemobj_alloc_usable_size(newPm));*/
        return pmemobj_direct(newPm);
}


/* TODO: implement this functionality if possible with NVML*/
void* pm_trans_realloc(PM_TRANS trans, void* ptr, size_t size)
{
//    assert(pm_pool && trans);
//    size_t oldsize = 0;
//    void* result = ptr;
//    if (ptr == NULL)
//    {
//        assert(0); //Unknown tag
//        return pm_trans_alloc(trans, size, PM_TAG_ALLOC_UNKN);
//    }
//    oldsize = pm_malloc_size(ptr);
//    if(oldsize >= size)
//    {
//        return ptr;
//    }
//    result = pm_trans_alloc(trans, size, pm_get_tag(ptr));
//
//    /* memcpy(result, ptr, oldsize);
//     * pm_persist(result, oldsize);*/
//    pmem_memcpy_persist(result, ptr, oldsize);
//
//    //pm_trans_free(trans, ptr);
    return ptr;
}

void pm_trans_free(PM_TRANS trans, void* ptr)
{

    if (NULL != ptr)
    {
    	if(uuidTemp == NULL)
    	{
    		PMEMoid poolTemp = pmemobj_first(trans,PM_TAG_ALLOC_ENTITY);
    		uuidTemp = poolTemp.pool_uuid_lo;
    	}
        PMEMoid pm = {0};
        pm.pool_uuid_lo = uuidTemp;
        pm.off = (void *) ((uintptr_t)ptr - (uintptr_t)trans);

        /*printf("\n previous size = %d, removed object size = %d",get_current_alloc_size(),pmemobj_alloc_usable_size(pm));*/
        if(pm_memory_counter)
        	decrease_current_alloc_size(pmemobj_alloc_usable_size(pm));
        /*printf("\n current size = %d, max size = %d",get_current_alloc_size(),get_max_alloc_size());*/

        pmemobj_free(&pm);

    }
}

void pm_trans_set(PM_TRANS trans, void **parentp, void* value)
{
    assert(trans);
    pmemobj_memcpy_persist(trans,parentp,&value,sizeof(value));
   /* pmemalloc_trans_set(trans, parentp, value);*/
}

void pm_trans_set_mem(PM_TRANS trans, void* dst, void* src, size_t size)
{
    assert(trans);
  /*  pmemalloc_trans_set_mem(trans, dst, src, size);*/
}


int pm_inited()
{
    return pm_pool?1:0;
}

int pm_init(const char* filename, size_t size)
{
    if (access(filename, F_OK) != 0)
    {
        /*Prepare memory file*/
        pm_pool = pmemobj_create(filename, LAYOUT_NAME,size, 0666);
        if (pm_pool == NULL)
        {
            return 1;
        }
    }
    else
    {
        pm_pool = pmemobj_open(filename, LAYOUT_NAME);
        if (pm_pool == NULL)
        {
            return 2;
        }
    }
    return 0;
}



size_t get_current_alloc_size()
{
    return current_alloc_size;
}

size_t get_max_alloc_size()
{
    return max_alloc_size;
}

void reset_current_alloc_size()
{
    current_alloc_size = 0;
}

void reset_max_alloc_size()
{
    max_alloc_size = 0;
}
void increase_current_alloc_size(size_t val)
{
    current_alloc_size += val;
    if(current_alloc_size>max_alloc_size)
        max_alloc_size = current_alloc_size;
}
void decrease_current_alloc_size(size_t val)
{
    current_alloc_size -= val;
}






