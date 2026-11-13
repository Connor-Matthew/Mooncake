#include "device/accelerator_registry.h"
#include "pinned_host_buffer.h"

#include "cuda_alike.h"

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) ||   \
    defined(USE_HYGON) || defined(USE_COREX) || defined(USE_SUPA) || \
    defined(USE_MLU)

namespace mooncake {
namespace device {

void EnsureCudaLikeAcceleratorDeviceLinked() {}

namespace {

void FreeCudaLikePinnedHostBuffer(void* addr) { cudaFreeHost(addr); }

class CudaLikeAcceleratorDevice final : public ProbeCachedAcceleratorDevice {
   public:
    explicit CudaLikeAcceleratorDevice(AcceleratorVendor vendor)
        : vendor_(vendor) {}

    AcceleratorVendor Vendor() const override { return vendor_; }

    bool ProbeAvailable() const override {
        int count = 0;
        return cudaGetDeviceCount(&count) == cudaSuccess && count > 0;
    }

    PointerInfo QueryPointer(const void* ptr) const override {
        cudaPointerAttributes attr{};
        if (cudaPointerGetAttributes(&attr, ptr) == cudaSuccess &&
            attr.type == cudaMemoryTypeDevice) {
            return PointerInfo{.kind = MemoryKind::kDevice,
                               .device_id = attr.device};
        }
        cudaGetLastError();
        return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
    }

    // Modified By Yida (v3): 严格探测。cudaErrorInvalidValue 是 CUDA 11 之前
    // 对非 CUDA 指针的确定回复，视为 kNotDevice；其他错误属探测失败，不可信。
    PointerProbeResult ProbePointerStrict(const void* ptr,
                                          PointerInfo* out_info) const override {
        cudaPointerAttributes attr{};
        cudaError_t rc = cudaPointerGetAttributes(&attr, ptr);
        if (rc == cudaSuccess) {
            const bool is_device = (attr.type == cudaMemoryTypeDevice);
            if (out_info) {
                *out_info = is_device
                                ? PointerInfo{.kind = MemoryKind::kDevice,
                                              .device_id = attr.device}
                                : PointerInfo{.kind = MemoryKind::kHost,
                                              .device_id = -1};
            }
            return is_device ? PointerProbeResult::kDevice
                             : PointerProbeResult::kNotDevice;
        }
        // 清除 sticky 错误，避免污染后续 CUDA 调用
        cudaGetLastError();
        if (rc == cudaErrorInvalidValue) {
            if (out_info) {
                *out_info = PointerInfo{.kind = MemoryKind::kHost,
                                        .device_id = -1};
            }
            return PointerProbeResult::kNotDevice;
        }
        return PointerProbeResult::kProbeError;
    }

    int32_t CurrentDeviceId() const override {
        int device_id = -1;
        return cudaGetDevice(&device_id) == cudaSuccess ? device_id : -1;
    }

    void SetContext(int32_t device_id) const override {
        if (device_id >= 0) cudaSetDevice(device_id);
    }

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection direction) const override {
        cudaMemcpyKind kind = cudaMemcpyDefault;
        switch (direction) {
            case CopyDirection::kHostToDevice:
                kind = cudaMemcpyHostToDevice;
                break;
            case CopyDirection::kDeviceToHost:
                kind = cudaMemcpyDeviceToHost;
                break;
            case CopyDirection::kDeviceToDevice:
                kind = cudaMemcpyDeviceToDevice;
                break;
            case CopyDirection::kHostToHost:
            case CopyDirection::kAuto:
                kind = cudaMemcpyDefault;
                break;
        }
        return cudaMemcpy(dst, src, size, kind) == cudaSuccess;
    }

    PinnedHostBuffer AllocatePinnedHost(size_t size) const override {
        void* addr = nullptr;
        if (cudaMallocHost(&addr, size) != cudaSuccess) {
            cudaGetLastError();
            return PinnedHostBuffer();
        }
        return PinnedHostBuffer(addr, size, FreeCudaLikePinnedHostBuffer);
    }

   private:
    AcceleratorVendor vendor_;
};

#define REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(name, vendor) \
    const CudaLikeAcceleratorDevice name##_device(vendor);  \
    const AcceleratorDeviceRegistrar name##_registrar(name##_device)

#if defined(USE_CUDA)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(nvidia, AcceleratorVendor::kNvidia);
#endif

#if defined(USE_MUSA)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(musa, AcceleratorVendor::kMusa);
#endif

#if defined(USE_MACA)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(maca, AcceleratorVendor::kMaca);
#endif

#if defined(USE_HYGON)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(hygon, AcceleratorVendor::kHygon);
#endif

#if defined(USE_COREX)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(corex, AcceleratorVendor::kCorex);
#endif

#if defined(USE_SUPA)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(supa, AcceleratorVendor::kSupa);
#endif

#if defined(USE_MLU)
REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE(mlu, AcceleratorVendor::kMlu);
#endif

#undef REGISTER_CUDA_LIKE_ACCELERATOR_DEVICE

}  // namespace
}  // namespace device
}  // namespace mooncake

#endif
