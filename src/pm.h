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

#ifndef __PM_H_
#define __PM_H_
#include <stddef.h>
#include <libpmemobj.h>

typedef void* PM_TRANS;
#define	LAYOUT_NAME "store_db"

#define PM_TRANS_RAM NULL

#define PM_TAG_ALLOC_BANNED 0
#define PM_TAG_ALLOC_ENTITY 1
#define PM_TAG_ALLOC_SDS 2
#define PM_TAG_ALLOC_OBJ 3
#define PM_TAG_ALLOC_ZIPLIST 4
#define PM_TAG_ALLOC_UNKN 0xff

#define PM_FILE_SYNCH_MODE_CL 0
#define PM_FILE_SYNCH_MODE_MSYNCH 1
#define PM_FILE_SYNCH_MODE_FIT 2 /*Fault injection */


int pm_init(const char* filename, size_t size);
int pm_inited();
extern PMEMobjpool *pm_pool;
extern int pm_memory_counter;

void pm_trans_clean(PM_TRANS trans);
void pm_trans_end(PM_TRANS trans);
void* pm_trans_alloc(PM_TRANS trans, size_t size, size_t pm_tag,void *c);
void* pm_trans_realloc(PM_TRANS trans, void* ptr, size_t size);
void pm_trans_free(PM_TRANS trans, void* ptr);
void pm_trans_set(PM_TRANS trans, void **parentp, void* value);
void pm_trans_set_mem(PM_TRANS trans, void* dst, void* src, size_t size);

/*Usable to load data from memory pool*/
/*void* pm_get_next_allocation(void* ptr);*/
/*size_t pm_get_tag(void *ptr);*/

size_t pm_malloc_size(void *ptr);

void* pmem_memcpy_persist(void *dst, const void *src, size_t size);

/* Get some statistics about memory pool */
/*size_t pm_get_allocs_num();
size_t pm_get_total_size();
size_t pm_get_usable_size();
size_t pm_get_pool_size();
size_t pm_get_free_size();
size_t pm_get_free_total_size();
size_t pm_get_free_num();*/


PMEMobjpool* pm_pool; /* Return reference to pool*/
size_t get_current_alloc_size();
size_t get_max_alloc_size();
void reset_current_alloc_size();
void reset_max_alloc_size();
void increase_current_alloc_size(size_t val);
void decrease_current_alloc_size(size_t val);
#endif /* __PM_H_ */
