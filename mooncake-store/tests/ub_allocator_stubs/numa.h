#pragma once
#include <cstdlib>
inline void* numa_alloc_local(size_t size) { return std::malloc(size); }
inline void* numa_alloc_onnode(size_t size, int) { return std::malloc(size); }
inline void numa_free(void* pointer, size_t) { std::free(pointer); }
