#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <limits>
#include <unordered_map>
#include <vector>
#include <glog/logging.h>
#include <numa.h>

#include "cuda_alike.h"
#include "ub_allocator.h"

namespace mooncake {
struct UbStoreMemRange {
    void* base;
    size_t size;
};
std::mutex g_ub_store_mem_mutex;
std::vector<UbStoreMemRange> g_ub_store_mem_ranges;

#if defined(USE_CUDA)
std::mutex g_ub_gpu_hbm_mutex;
std::unordered_map<void*, int> g_ub_gpu_hbm_devices;
#endif

size_t remove_store_memory_range(void* ptr) {
    std::lock_guard<std::mutex> store_lock(g_ub_store_mem_mutex);

    auto it = std::find_if(
        g_ub_store_mem_ranges.begin(), g_ub_store_mem_ranges.end(),
        [ptr](const UbStoreMemRange& range) { return range.base == ptr; });

    if (it == g_ub_store_mem_ranges.end()) {
        LOG(ERROR) << "failed for UB protocol, addr at " << ptr;
        return 0;
    }

    size_t sz = it->size;
    g_ub_store_mem_ranges.erase(it);
    return sz;
}

void* ub_allocate_memory_onnode(size_t alignment, size_t total_size,
                                int numa_node) {
    // numa_node < 0 keeps the original node-local behavior; otherwise bind to
    // the requested node. Both go through libnuma (same family as
    // numa_alloc_local), so URMA can register the result -- unlike a raw
    // mmap+mbind buffer.
    void* ptr = (numa_node < 0) ? numa_alloc_local(total_size)
                                : numa_alloc_onnode(total_size, numa_node);
    if (!ptr) {
        LOG(ERROR) << "failed for UB protocol, size=" << total_size
                   << ", node=" << numa_node << ", alignment : " << alignment;
        return nullptr;
    }
    LOG(INFO) << "UB:  allocated total size : " << total_size << ", node : "
              << numa_node << ", alignment : " << alignment << " addr at "
              << ptr;

    std::lock_guard<std::mutex> store_lock(g_ub_store_mem_mutex);
    g_ub_store_mem_ranges.push_back({ptr, total_size});

    return ptr;
}

void* ub_allocate_memory(size_t alignment, size_t total_size) {
    return ub_allocate_memory_onnode(alignment, total_size, -1);
}

void ub_free_memory(void* ptr) {
    if (!ptr) {
        return;
    }
    auto size = remove_store_memory_range(ptr);
    numa_free(ptr, size);
    LOG(INFO) << "UB: freed  bytes at " << ptr;
}

void* ub_allocate_gpu_hbm(size_t total_size, int device_id) {
    if (total_size == 0 || device_id < 0) {
        LOG(ERROR) << "UB GPU HBM: invalid args size=" << total_size
                   << " device=" << device_id;
        return nullptr;
    }
#if defined(USE_CUDA)
    // The NVIDIA P2P bridge (udma_nv_p2p_bridge.c) rejects non-64KB-aligned
    // lengths at nvidia_p2p_get_pages. Pad the allocation up to 64KB so
    // non-aligned sizes (e.g. --value_size < 64KB) can still be registered.
    // The returned VA is already GPU-page aligned by cuMemAlloc.
    constexpr size_t kGpuHbmAlign = 64 * 1024;
    if (total_size > std::numeric_limits<size_t>::max() - (kGpuHbmAlign - 1)) {
        LOG(ERROR) << "UB GPU HBM: allocation size overflow";
        return nullptr;
    }
    const size_t alloc_size =
        (total_size + kGpuHbmAlign - 1) & ~(kGpuHbmAlign - 1);
    CUresult init_ret = cuInit(0);
    if (init_ret != CUDA_SUCCESS) {
        const char* err_str = nullptr;
        (void)cuGetErrorString(init_ret, &err_str);
        LOG(ERROR) << "UB GPU HBM: cuInit failed, error="
                   << (err_str ? err_str : "unknown");
        return nullptr;
    }

    int old_device = -1;
    if (cudaGetDevice(&old_device) != cudaSuccess) {
        old_device = -1;
    }
    if (cudaSetDevice(device_id) != cudaSuccess) {
        LOG(ERROR) << "UB GPU HBM: failed to set CUDA device " << device_id;
        return nullptr;
    }

    CUdeviceptr dptr = 0;
    CUresult cu_ret = cuMemAlloc(&dptr, alloc_size);
    if (cu_ret != CUDA_SUCCESS) {
        const char* err_str = nullptr;
        (void)cuGetErrorString(cu_ret, &err_str);
        if (old_device >= 0) (void)cudaSetDevice(old_device);
        LOG(ERROR) << "UB GPU HBM: cuMemAlloc failed for size " << alloc_size
                   << ", error=" << (err_str ? err_str : "unknown");
        return nullptr;
    }

    unsigned int enable = 1;
    cu_ret = cuPointerSetAttribute(&enable, CU_POINTER_ATTRIBUTE_SYNC_MEMOPS,
                                   dptr);
    if (cu_ret != CUDA_SUCCESS) {
        const char* err_str = nullptr;
        (void)cuGetErrorString(cu_ret, &err_str);
        (void)cuMemFree(dptr);
        if (old_device >= 0) (void)cudaSetDevice(old_device);
        LOG(ERROR) << "UB GPU HBM: failed to set SYNC_MEMOPS, error="
                   << (err_str ? err_str : "unknown");
        return nullptr;
    }

    if (old_device >= 0) (void)cudaSetDevice(old_device);

    void* ptr = reinterpret_cast<void*>(static_cast<uintptr_t>(dptr));
    {
        std::lock_guard<std::mutex> gdr_lock(g_ub_gpu_hbm_mutex);
        g_ub_gpu_hbm_devices[ptr] = device_id;
    }
    VLOG(1) << "UB GPU HBM: allocated size " << alloc_size
            << " (requested " << total_size << "), device " << device_id
            << ", addr " << ptr;
    return ptr;
#else
    (void)device_id;
    LOG(ERROR) << "UB GPU HBM allocation requires USE_CUDA build";
    return nullptr;
#endif
}

void ub_free_gpu_hbm(void* ptr) {
    if (!ptr) return;
#if defined(USE_CUDA)
    // Serialize frees of the same allocation and retain ownership on failure
    // so callers can retry after correcting the CUDA device/context error.
    std::unique_lock<std::mutex> gdr_lock(g_ub_gpu_hbm_mutex);
    auto it = g_ub_gpu_hbm_devices.find(ptr);
    if (it == g_ub_gpu_hbm_devices.end()) {
        LOG(ERROR) << "UB GPU HBM: pointer " << ptr
                   << " not found in GPU HBM allocation table";
        return;
    }
    const int device_id = it->second;

    int old_device = -1;
    if (cudaGetDevice(&old_device) != cudaSuccess) old_device = -1;
    if (cudaSetDevice(device_id) != cudaSuccess) {
        LOG(ERROR) << "UB GPU HBM: failed to set device " << device_id
                   << " for free";
        return;
    }
    CUresult cu_ret = cuMemFree(reinterpret_cast<CUdeviceptr>(ptr));
    if (old_device >= 0) (void)cudaSetDevice(old_device);
    if (cu_ret != CUDA_SUCCESS) {
        const char* err_str = nullptr;
        (void)cuGetErrorString(cu_ret, &err_str);
        LOG(ERROR) << "UB GPU HBM: cuMemFree failed for " << ptr
                   << ", error=" << (err_str ? err_str : "unknown");
        return;
    }
    g_ub_gpu_hbm_devices.erase(it);
    gdr_lock.unlock();
    VLOG(1) << "UB GPU HBM: freed bytes at " << ptr;
#else
    (void)ptr;
    LOG(ERROR) << "UB GPU HBM free requires USE_CUDA build";
#endif
}

bool ub_is_gpu_hbm(void* ptr) {
    if (!ptr) return false;
#if defined(USE_CUDA)
    std::lock_guard<std::mutex> gdr_lock(g_ub_gpu_hbm_mutex);
    return g_ub_gpu_hbm_devices.find(ptr) != g_ub_gpu_hbm_devices.end();
#else
    (void)ptr;
    return false;
#endif
}

bool ub_is_store_memory(void* addr, size_t length) {
    if (!addr || length == 0) return false;
    auto addr_start = reinterpret_cast<uintptr_t>(addr);
    uintptr_t addr_end = addr_start + length;
    std::lock_guard<std::mutex> lock(g_ub_store_mem_mutex);
    for (const auto& range : g_ub_store_mem_ranges) {
        auto range_start = reinterpret_cast<uintptr_t>(range.base);
        uintptr_t range_end = range_start + range.size;
        if (addr_start >= range_start && addr_end <= range_end) {
            return true;
        }
    }
    return false;
}

}  // namespace mooncake
