#pragma once

namespace mooncake {

void* ub_allocate_memory(size_t alignment, size_t total_size);

// Same as ub_allocate_memory but binds the allocation to a specific NUMA node
// (numa_node < 0 falls back to node-local). Still allocated via libnuma and
// registered in the store-memory range table, so URMA can register it.
void* ub_allocate_memory_onnode(size_t alignment, size_t total_size,
                                int numa_node);

void ub_free_memory(void* ptr);

// Allocate GPU HBM via CUDA Driver API (cuMemAlloc) on the specified device.
// Sets SYNC_MEMOPS on the allocated pointer. Returns GPU VA, or nullptr on failure.
// Must be freed by ub_free_gpu_hbm. device_id must be >= 0.
void* ub_allocate_gpu_hbm(size_t total_size, int device_id);

// Free GPU HBM memory allocated by ub_allocate_gpu_hbm.
void ub_free_gpu_hbm(void* ptr);

// Query whether the pointer was allocated by ub_allocate_gpu_hbm.
bool ub_is_gpu_hbm(void* ptr);

bool ub_is_store_memory(void* addr, size_t length);

}  // namespace mooncake