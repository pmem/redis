int mk_je_get_defrag_hint(void* ptr, int *bin_util, int *run_util);
int jemk_mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen);

void *memkind_alloc_wrapper(size_t size);
void *memkind_calloc_wrapper(size_t size);
void *memkind_realloc_wrapper(void *ptr, size_t size);
void memkind_free_wrapper(void *ptr);
size_t memkind_malloc_usable_size_wrapper(void *ptr);
size_t memkind_malloc_used_memory(void);
void *pmem_memcpy_wrapper(void *pmemdest, const void *src, size_t len);
void *pmem_memset_wrapper(void *pmemdest, int c, size_t len);

#define mmalloc memkind_alloc_wrapper
#define mcalloc memkind_calloc_wrapper
#define mrealloc memkind_realloc_wrapper
#define mfree memkind_free_wrapper
#define mmalloc_usable_size memkind_malloc_usable_size_wrapper
#define mget_defrag_hint mk_je_get_defrag_hint
#define mmemcpy pmem_memcpy_wrapper
#define mmemset pmem_memset_wrapper
