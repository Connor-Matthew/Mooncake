#include "cuda_alike.h"
#include "ub_allocator.h"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {
std::atomic<bool> fail_device{false};
std::atomic<bool> fail_free{false};
std::atomic<unsigned> successful_frees{0};
thread_local int current_device = 0;
void Require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
}  // namespace

CUresult cuInit(unsigned int) { return CUDA_SUCCESS; }
CUresult cuGetErrorString(CUresult, const char** message) {
    *message = "injected CUDA failure";
    return CUDA_SUCCESS;
}
cudaError_t cudaGetDevice(int* device) {
    *device = current_device;
    return cudaSuccess;
}
cudaError_t cudaSetDevice(int device) {
    if (fail_device) return 1;
    current_device = device;
    return cudaSuccess;
}
CUresult cuMemAlloc(CUdeviceptr* pointer, size_t size) {
    *pointer = reinterpret_cast<CUdeviceptr>(std::malloc(size));
    return *pointer ? CUDA_SUCCESS : 1;
}
CUresult cuMemFree(CUdeviceptr pointer) {
    if (fail_free) return 1;
    std::free(reinterpret_cast<void*>(pointer));
    ++successful_frees;
    return CUDA_SUCCESS;
}
CUresult cuPointerSetAttribute(const void*, int, CUdeviceptr) {
    return CUDA_SUCCESS;
}

int main() {
    try {
        Require(!mooncake::ub_allocate_gpu_hbm(
                    std::numeric_limits<size_t>::max(), 0),
                "GPU allocation rounding must reject overflow");
        void* pointer = mooncake::ub_allocate_gpu_hbm(4096, 1);
        Require(pointer && mooncake::ub_is_gpu_hbm(pointer),
                "allocation must be tracked");
        fail_device = true;
        mooncake::ub_free_gpu_hbm(pointer);
        Require(mooncake::ub_is_gpu_hbm(pointer),
                "device selection failure must retain ownership");
        fail_device = false;
        fail_free = true;
        mooncake::ub_free_gpu_hbm(pointer);
        Require(mooncake::ub_is_gpu_hbm(pointer),
                "cuMemFree failure must remain retryable");
        fail_free = false;
        std::vector<std::thread> threads;
        for (int i = 0; i < 8; ++i) {
            threads.emplace_back([pointer] { mooncake::ub_free_gpu_hbm(pointer); });
        }
        for (auto& thread : threads) thread.join();
        Require(successful_frees == 1 && !mooncake::ub_is_gpu_hbm(pointer),
                "concurrent retries must free an allocation exactly once");
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
    std::cout << "UB allocator lifecycle: PASS\n";
}
