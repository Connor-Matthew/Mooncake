#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "pinned_host_buffer.h"

namespace mooncake {
namespace device {

enum class AcceleratorVendor {
    kNvidia,
    kMusa,
    kMaca,
    kHygon,
    kCorex,
    kSupa,
    kHip,
    kAscend,
    kSunrise,
    kIntel,
    kMlu,
};

enum class MemoryKind {
    kHost,
    kDevice,
    kUnknown,
};

// Modified By Yida (v3): 严格指针探测结果。NoF GPU Direct 的内存识别必须
// 区分“确定不是设备指针”与“探测本身失败”——后者不能静默当 host 处理。
enum class PointerProbeResult {
    kNotDevice,   ///< 确定不是该后端管理的设备指针
    kDevice,      ///< 设备指针，out_info 已填充
    kProbeError,  ///< 探测失败（驱动错误等），结果不可信
};

enum class CopyDirection {
    kHostToHost,
    kHostToDevice,
    kDeviceToHost,
    kDeviceToDevice,
    kAuto,
};

struct PointerInfo {
    MemoryKind kind = MemoryKind::kUnknown;
    int32_t device_id = -1;
};

class AcceleratorDevice {
   public:
    virtual ~AcceleratorDevice() = default;

    virtual AcceleratorVendor Vendor() const = 0;
    virtual bool Available(bool ensure = false) const = 0;
    virtual PointerInfo QueryPointer(const void* ptr) const = 0;

    /**
     * Modified By Yida (v3): 严格版 QueryPointer——区分“非设备指针”
     * (kNotDevice) 与“探测失败” (kProbeError)。默认实现直接判定为
     * kNotDevice（后端未实现严格探测时不阻塞 host 路径）。
     */
    virtual PointerProbeResult ProbePointerStrict(const void* ptr,
                                                  PointerInfo* out_info) const {
        (void)ptr;
        if (out_info) {
            *out_info = PointerInfo{};
        }
        return PointerProbeResult::kNotDevice;
    }
    virtual int32_t CurrentDeviceId() const = 0;
    virtual void SetContext(int32_t device_id) const = 0;
    virtual bool Copy(void* dst, const void* src, size_t size,
                      CopyDirection direction) const = 0;
    virtual PinnedHostBuffer AllocatePinnedHost(size_t size) const = 0;
};

class ProbeCachedAcceleratorDevice : public AcceleratorDevice {
   public:
    bool Available(bool ensure = false) const override;

   protected:
    virtual bool ProbeAvailable() const = 0;

   private:
    mutable std::atomic<uint8_t> available_state_{0};
};

}  // namespace device
}  // namespace mooncake
