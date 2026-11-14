#include "client_buffer.h"

#include <algorithm>
#include <cstdlib>
#include <vector>
#include <sys/mman.h>  // For shm_open, mmap, munmap
#include <sys/stat.h>  // For S_IRUSR, S_IWUSR
#include <fcntl.h>     // For O_CREAT, O_RDWR
#include <unistd.h>    // For ftruncate, close, shm_unlink

#include "common/client_buffer_allocation.h"

#if defined(USE_SUNRISE)
#include "sunrise_allocator.h"
#endif

namespace mooncake {

std::shared_ptr<ClientBufferAllocator> ClientBufferAllocator::create(
    size_t size, const std::string& protocol, bool use_hugepage,
    std::shared_ptr<DmaBufferAllocator> dma_allocator) {
    return std::shared_ptr<ClientBufferAllocator>(new ClientBufferAllocator(
        size, protocol, use_hugepage, std::move(dma_allocator)));
}

std::shared_ptr<ClientBufferAllocator> ClientBufferAllocator::create(
    void* addr, size_t size, const std::string& protocol) {
    return std::shared_ptr<ClientBufferAllocator>(
        new ClientBufferAllocator(addr, size, protocol));
}

ClientBufferAllocator::ClientBufferAllocator(
    size_t size, const std::string& protocol, bool use_hugepage,
    std::shared_ptr<DmaBufferAllocator> dma_allocator)
    : buffer_size_(size),
      use_hugepage_(use_hugepage),
      protocol(protocol),
      dma_allocator_(std::move(dma_allocator)) {
    if (size == 0) {
        buffer_ = nullptr;
        allocator_ = nullptr;
        return;
    }
    // Align to 64 bytes(cache line size) for better cache performance
    // Modified By Yida (v3): USE_NOF_URMA 下 base 至少 4K 对齐（UMMU Table
    // mode 页粒度注册；4K 对齐同时命中 SPDK 注册缓存的不重叠快路径）
#ifdef USE_NOF_URMA
    constexpr size_t alignment = kNofBufferAlignment;
#else
    constexpr size_t alignment = 64;
#endif
    if (use_hugepage_) {
        buffer_ = allocate_buffer_mmap_memory(size, alignment);
    } else {
#if defined(USE_SUNRISE)
        if (protocol == "sunrise_link") {
            buffer_ = sunrise_allocate_memory(size, alignment, false);
        } else
#endif
        {
            buffer_ = allocate_buffer_allocator_memory(
                size, protocol, alignment, dma_allocator_.get());
        }
    }
    if (!buffer_) {
        throw std::bad_alloc();
    }

    allocator_ = mooncake::offset_allocator::OffsetAllocator::create(
        reinterpret_cast<uint64_t>(buffer_), size);
}

ClientBufferAllocator::ClientBufferAllocator(void* addr, size_t size,
                                             const std::string& protocol)
    : buffer_size_(size), protocol(protocol) {
    buffer_ = addr;
    is_external_memory_ = true;
    allocator_ = mooncake::offset_allocator::OffsetAllocator::create(
        reinterpret_cast<uint64_t>(buffer_), size);
}

ClientBufferAllocator::~ClientBufferAllocator() {
    // Free the aligned allocated memory or unmap shared memory
    if (!is_external_memory_ && buffer_) {
        if (use_hugepage_) {
            free_buffer_mmap_memory(buffer_, buffer_size_);
        } else {
            free_memory(protocol, buffer_, dma_allocator_.get());
        }
    }
}

std::optional<BufferHandle> ClientBufferAllocator::allocate(size_t size) {
    if (allocator_ == nullptr) {
        return std::nullopt;
    }
#ifdef USE_NOF_URMA
    // Modified By Yida (v3): 子分配尺寸向上取整到 4K。base 已 4K 对齐、每个
    // 分配消耗 4K 的整数倍空间，分配出的 buffer_address_ 与 size 均满足
    // NoF 提交的块对齐校验（512B/4K sector 均被覆盖）。
    const size_t rounded = (size + kNofBufferAlignment - 1) &
                           ~(kNofBufferAlignment - 1);
    if (rounded != size) {
        VLOG(1) << "NoF aligned allocation: requested=" << size
                << ", rounded=" << rounded;
    }
    size = rounded;
#endif
    auto handle = allocator_->allocate(size);
    if (!handle) {
        return std::nullopt;
    }

    VLOG(1) << "NoF buffer allocated: ptr=" << handle->ptr()
            << ", size=" << handle->size()
            << ", offset=" << reinterpret_cast<uintptr_t>(handle->ptr()) -
                   reinterpret_cast<uintptr_t>(buffer_);

    return std::make_optional<BufferHandle>(shared_from_this(),
                                            std::move(*handle));
}

BufferHandle::BufferHandle(
    std::shared_ptr<ClientBufferAllocator> allocator,
    mooncake::offset_allocator::OffsetAllocationHandle handle)
    : allocator_(std::move(allocator)), handle_(std::move(handle)) {}

BufferHandle::BufferHandle(void* ptr, size_t size,
                           std::function<void()> release_fn)
    : view_ptr_(ptr), view_size_(size), release_fn_(std::move(release_fn)) {}

BufferHandle::~BufferHandle() {
    if (release_fn_) {
        release_fn_();
    }
    // Otherwise OffsetAllocationHandle destructor handles deallocation
}

void* BufferHandle::ptr() const {
    return view_ptr_ ? view_ptr_ : handle_.ptr();
}

size_t BufferHandle::size() const {
    return view_ptr_ ? view_size_ : handle_.size();
}

// Utility functions for buffer and slice management
std::vector<Slice> split_into_slices(BufferHandle& handle) {
    auto base = static_cast<uint8_t*>(handle.ptr());
    auto length = handle.size();
    return split_into_slices(base, length);
}

std::vector<Slice> split_into_slices(void* buffer, size_t length) {
    std::vector<Slice> slices;
    auto base = static_cast<uint8_t*>(buffer);
    size_t offset = 0;

    while (offset < length) {
        size_t chunk_size = std::min(length - offset, kMaxSliceSize);
        slices.push_back({base + offset, chunk_size});
        offset += chunk_size;
    }
    return slices;
}

uint64_t calculate_total_size(const Replica::Descriptor& replica) {
    uint64_t total_length = 0;
    if (replica.is_disk_replica()) {
        auto& disk_descriptor = replica.get_disk_descriptor();
        total_length = disk_descriptor.object_size;
    } else if (replica.is_local_disk_replica()) {
        total_length = replica.get_local_disk_descriptor().object_size;
    } else if (replica.is_dfs_replica()) {
        total_length = replica.get_dfs_descriptor().object_size;
    } else if (replica.is_nof_replica()) {
        total_length = replica.get_nof_descriptor().buffer_descriptor.size_;
    } else if (replica.is_vsegment_replica()) {
        total_length = replica.get_vsegment_descriptor().length;
    } else {
        total_length = replica.get_memory_descriptor().buffer_descriptor.size_;
    }
    return total_length;
}

int allocateSlices(std::vector<Slice>& slices,
                   const Replica::Descriptor& replica, void* buffer_ptr) {
    if (replica.is_disk_replica()) {
        // For disk-based replica, split into slices based on file size
        uint64_t offset = 0;
        uint64_t total_length = replica.get_disk_descriptor().object_size;
        while (offset < total_length) {
            auto chunk_size = std::min(total_length - offset, kMaxSliceSize);
            void* chunk_ptr = static_cast<char*>(buffer_ptr) + offset;
            slices.emplace_back(Slice{chunk_ptr, chunk_size});
            offset += chunk_size;
        }
    } else if (replica.is_local_disk_replica()) {
        slices.emplace_back(
            Slice{buffer_ptr, replica.get_local_disk_descriptor().object_size});
    } else if (replica.is_dfs_replica()) {
        slices.emplace_back(
            Slice{buffer_ptr, replica.get_dfs_descriptor().object_size});
    } else if (replica.is_nof_replica()) {
        auto& handle = replica.get_nof_descriptor().buffer_descriptor;
        void* chunk_ptr = buffer_ptr;
        slices.emplace_back(Slice{chunk_ptr, handle.size_});
    } else if (replica.is_vsegment_replica()) {
        slices.emplace_back(
            Slice{buffer_ptr, replica.get_vsegment_descriptor().length});
    } else {
        // For memory-based replica, split into slices based on buffer
        // descriptors
        auto& handle = replica.get_memory_descriptor().buffer_descriptor;
        void* chunk_ptr = buffer_ptr;
        slices.emplace_back(Slice{chunk_ptr, handle.size_});
    }
    return 0;
}

}  // namespace mooncake
