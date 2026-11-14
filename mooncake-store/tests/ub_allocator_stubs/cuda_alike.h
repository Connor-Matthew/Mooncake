#pragma once
#include <cstddef>
#include <cstdint>

using CUdeviceptr = uintptr_t;
using CUresult = int;
using cudaError_t = int;
constexpr int CUDA_SUCCESS = 0;
constexpr int cudaSuccess = 0;
constexpr int CU_POINTER_ATTRIBUTE_SYNC_MEMOPS = 1;
CUresult cuInit(unsigned int);
CUresult cuGetErrorString(CUresult, const char**);
cudaError_t cudaGetDevice(int*);
cudaError_t cudaSetDevice(int);
CUresult cuMemAlloc(CUdeviceptr*, size_t);
CUresult cuMemFree(CUdeviceptr);
CUresult cuPointerSetAttribute(const void*, int, CUdeviceptr);
