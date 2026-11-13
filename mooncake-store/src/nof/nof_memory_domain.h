/**
 * nof_memory_domain.h
 * Modified By Yida (v3): NoF GPU Direct —— buffer 类型识别与 SPDK memory
 * domain 管理。
 *
 * 设计与部署约束见 docs/yin/mooncake_nof_deployment.md 的 GPU Direct
 * 验证、故障排查与附录 B。本类的职责是：
 *   1. 调用 Store 已有 accelerator 接口严格识别 pointer；
 *   2. 为每张 GPU 创建并缓存一个 SPDK memory_domain（命名 cuda:mooncake:<id>，
 *      dma_device_id 含 "cuda"，SPDK URMA 侧据此选择 CUDA 内存注册）；
 *   3. 向 SPDK 提供 GPU 内存注册所需的 pin / DMA-BUF export / unpin 回调
 *      （spdk_nvme_urma_memory_provider，进程级注册一次）。
 *
 * 错误策略：已识别为 GPU 的 buffer 若 provider/domain 初始化失败，
 * 返回错误，绝不静默回退 host staging。
 */
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include <spdk/dma.h>
#include <spdk/env.h>
#include <spdk/nvme_urma.h>

#include "nof/nvmeof_initiator.h"  // NofMemoryType, NofBufferInfo (neutral)
#include "nof_gpu_direct_report.h"

// Private to the SPDK backend (src/nof, USE_NOF_URMA builds only): this
// header includes SPDK headers and must never be reachable from include/.
// Callers outside the backend use NVMeoFInitiator::ResolveBuffer /
// SubmitIOWithBuffer and the neutral nof_gpu_direct_report.h functions.

namespace mooncake {

// Neutral classification plus the SPDK memory domain for ext I/O.
struct NofDomainBuffer {
    NofBufferInfo info;
    struct spdk_memory_domain *domain = nullptr;
};

class NofMemoryDomainManager {
   public:
    static NofMemoryDomainManager &GetInstance();
    ~NofMemoryDomainManager();

    NofMemoryDomainManager(const NofMemoryDomainManager &) = delete;
    NofMemoryDomainManager &operator=(const NofMemoryDomainManager &) = delete;

    /**
     * @brief 自动识别 pointer 类型并解析出对应的 memory domain。
     *
     * 返回 kHost 时 domain 可能为 system domain；返回 kCuda 时 domain 为
     * 该 GPU 对应的 vendor-specific domain（cuda:mooncake:<id>）。
     * 返回 kError / kNotSupported 时不应继续提交 NoF I/O。
     */
    NofDomainBuffer ResolveBuffer(void *ptr, size_t length);

    /// Cached CUDA domain for a device classified earlier by ResolveBuffer;
    /// nullptr when it cannot be created (the I/O must then fail).
    struct spdk_memory_domain *GetCudaDomain(int32_t device_id);

    /// GPU Direct 是否可用（CUDA provider + 至少一张可用 GPU）
    bool IsGpuDirectAvailable() const;

    /// GPU Direct 验收指标。计数口径：
    ///   - host_io_total / cuda_io_submitted_total：**提交口径**——
    ///     SubmitRequest 返回成功即计数，证明 SPDK 接受过对应 memory
    ///     domain 的 I/O 提交，不证明请求最终完成成功。
    ///   - cuda_io_success_total：**完成口径**——SPDK completion 无错误
    ///     时计数（nvmf_io_complete），是 GPU I/O 真正成功的证据。
    uint64_t host_io_total() const { return host_io_total_.load(std::memory_order_relaxed); }
    uint64_t cuda_io_submitted_total() const {
        return CudaIoStats(false).submitted + CudaIoStats(true).submitted;
    }
    uint64_t cuda_io_success_total() const {
        return CudaIoStats(false).success + CudaIoStats(true).success;
    }
    uint64_t cuda_io_failed_total() const {
        return CudaIoStats(false).failed + CudaIoStats(true).failed;
    }
    NofGpuIoStats CudaIoStats(bool write) const {
        const size_t index = write ? 1 : 0;
        return {cuda_io_submitted_[index].load(std::memory_order_relaxed),
                cuda_io_success_[index].load(std::memory_order_relaxed),
                cuda_io_failed_[index].load(std::memory_order_relaxed),
                cuda_submitted_bytes_[index].load(std::memory_order_relaxed),
                cuda_success_bytes_[index].load(std::memory_order_relaxed)};
    }
    uint64_t classify_fail_total() const { return classify_fail_total_.load(std::memory_order_relaxed); }
    /// CUDA DMA-BUF 导出成功次数（导出后 SPDK 注册仍可能失败或回退
    /// peer-memory，注册结果看 SPDK URMA 统计的 dmabuf_registrations）。
    uint64_t dmabuf_export_total() const {
        return dmabuf_export_total_.load(std::memory_order_relaxed);
    }
    uint64_t gpu_staging_fallback_total() const {
        return gpu_staging_fallback_total_.load(std::memory_order_relaxed);
    }
    uint64_t payload_staged_bytes() const {
        return payload_staged_bytes_.load(std::memory_order_relaxed);
    }

    void OnIoSubmitted(const NofBufferInfo &info, bool write, uint64_t bytes) {
        if (info.type == NofMemoryType::kCuda) {
            cuda_io_submitted_[write ? 1 : 0].fetch_add(
                1, std::memory_order_relaxed);
            cuda_submitted_bytes_[write ? 1 : 0].fetch_add(
                bytes, std::memory_order_relaxed);
        } else if (info.type == NofMemoryType::kHost) {
            host_io_total_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    /// SPDK completion 处调用；仅统计 CUDA I/O，区分成功与失败。
    /// 注意粒度：按 SPDK 子任务 chunk 计（每次 chunk completion 一次，
    /// 与 OnIoSubmitted 的粒度对称），不是完整 value/object 数——验收时
    /// 需结合 bench 的操作成功/失败统计解读。
    void OnIoCompleted(const NofBufferInfo &info, bool success, bool write,
                       uint64_t bytes) {
        if (info.type == NofMemoryType::kCuda) {
            auto& counter = success ? cuda_io_success_[write ? 1 : 0]
                                    : cuda_io_failed_[write ? 1 : 0];
            counter.fetch_add(1, std::memory_order_relaxed);
            if (success) {
                cuda_success_bytes_[write ? 1 : 0].fetch_add(
                    bytes, std::memory_order_relaxed);
            }
        }
    }
    void OnIoAborted(const NofBufferInfo &info, bool write, uint64_t count) {
        if (info.type == NofMemoryType::kCuda) {
            cuda_io_failed_[write ? 1 : 0].fetch_add(
                count, std::memory_order_relaxed);
        }
    }
    void OnClassifyFail() { classify_fail_total_.fetch_add(1, std::memory_order_relaxed); }
    void OnDmabufExport() {
        dmabuf_export_total_.fetch_add(1, std::memory_order_relaxed);
    }
    void OnGpuStagingFallback() {
        gpu_staging_fallback_total_.fetch_add(1, std::memory_order_relaxed);
    }
    void AddPayloadStagedBytes(uint64_t bytes) {
        payload_staged_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    /// 供验收脚本查询：SPDK URMA 注册统计（dmabuf/peer/失败次数）
    static void GetUrmaMemoryStats(struct spdk_nvme_urma_memory_stats *stats);

   private:
    NofMemoryDomainManager() = default;

    struct spdk_memory_domain *GetOrCreateCudaDomain(int32_t device_id,
                                                     std::string &error);

    std::mutex mutex_;
    std::unordered_map<int32_t, struct spdk_memory_domain *> cuda_domains_;
    bool provider_registered_ = false;

    std::atomic<uint64_t> host_io_total_{0};
    std::array<std::atomic<uint64_t>, 2> cuda_io_submitted_{};
    std::array<std::atomic<uint64_t>, 2> cuda_io_success_{};
    std::array<std::atomic<uint64_t>, 2> cuda_io_failed_{};
    std::array<std::atomic<uint64_t>, 2> cuda_submitted_bytes_{};
    std::array<std::atomic<uint64_t>, 2> cuda_success_bytes_{};
    std::atomic<uint64_t> classify_fail_total_{0};
    std::atomic<uint64_t> dmabuf_export_total_{0};
    std::atomic<uint64_t> gpu_staging_fallback_total_{0};
    std::atomic<uint64_t> payload_staged_bytes_{0};
};

}  // namespace mooncake
