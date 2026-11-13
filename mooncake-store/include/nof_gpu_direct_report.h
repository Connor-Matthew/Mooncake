/**
 * nof_gpu_direct_report.h
 * Modified By Yida (v3): NoF GPU Direct 验收输出接口（外部审阅 #3）。
 *
 * 把 SPDK GPU Direct 的可用性、buffer-domain 识别统计与 SPDK URMA 注册
 * 统计暴露给 bench 等验收方，避免"bench 打印了 gdr-peermem"被当成
 * "SPDK 真的用了 GPU I/O"的证据。
 *
 * SPDK-free 头：本头文件不包含任何 SPDK 头，无 SPDK include 路径的目标
 * 可以安全包含。实现在 nof_memory_domain.cpp，仅于 USE_NOF_URMA 构建中
 * 编译——调用方必须用 #if defined(USE_NOF_URMA) 守卫，否则非 URMA 构建
 * 会产生未定义符号链接错误。
 */
#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <string>

#include "nof/nvmeof_initiator.h"  // NofBufferInfo (SPDK-free)

namespace mooncake {

/// NoF GPU Direct 是否可用（USE_NOF_GPU_DIRECT + CUDA provider 可用）。
/// 仅在 USE_NOF_URMA 构建中有定义。
bool NofGpuDirectAvailable();

/// Whether this build can register a whole buffer pool per endpoint
/// (HAVE_SPDK_URMA_WHOLE_POOL). Registration itself goes through
/// NVMeoFInitiator::RegisterMemoryForEndpoint; the caller must drain I/O and
/// release the returned token before unregistering/freeing the buffer.
bool NofWholePoolRegistrationAvailable();

/// GPU Direct acceptance counters, recorded by the NoF submitter. Only the
/// kCuda / kHost classifications are counted (see NofGpuDirectStats).
void RecordNofIoSubmitted(const NofBufferInfo& buffer, bool write,
                          uint64_t bytes);
void RecordNofIoCompleted(const NofBufferInfo& buffer, bool success,
                          bool write, uint64_t bytes);
void RecordNofIoAborted(const NofBufferInfo& buffer, bool write,
                        uint64_t count);

struct NofGpuIoStats {
    uint64_t submitted = 0;
    uint64_t success = 0;
    uint64_t failed = 0;
    uint64_t submitted_bytes = 0;
    uint64_t success_bytes = 0;
};

// Read after draining I/O for a consistent acceptance result. Registrations
// can be reused across many requests, so their count need not match I/O count.
struct NofGpuDirectStats {
    bool available = false;
    std::string reg_mode;
    NofGpuIoStats reads;
    NofGpuIoStats writes;
    uint64_t host_io = 0;
    uint64_t classify_fail = 0;
    uint64_t dmabuf_exports = 0;
    uint64_t staging_fallbacks = 0;
    uint64_t staged_bytes = 0;
    uint64_t host_registrations = 0;
    uint64_t accelerator_attempts = 0;
    uint64_t dmabuf_registrations = 0;
    uint64_t peer_memory_registrations = 0;
    uint64_t registration_failures = 0;
};

NofGpuDirectStats GetNofGpuDirectStats();
std::string NofGpuDirectAcceptanceReport(const NofGpuDirectStats& stats);

// I/O and failures are scoped to a drained measurement phase. Successful
// registration counts remain cumulative because cached registrations may
// have been created before the phase.
inline NofGpuDirectStats NofGpuDirectDelta(const NofGpuDirectStats& before,
                                           const NofGpuDirectStats& after) {
    auto result = after;
    const auto io = [](const NofGpuIoStats& first, const NofGpuIoStats& last) {
        return NofGpuIoStats{last.submitted - first.submitted,
                             last.success - first.success,
                             last.failed - first.failed,
                             last.submitted_bytes - first.submitted_bytes,
                             last.success_bytes - first.success_bytes};
    };
    result.reads = io(before.reads, after.reads);
    result.writes = io(before.writes, after.writes);
    result.host_io -= before.host_io;
    result.classify_fail -= before.classify_fail;
    result.dmabuf_exports -= before.dmabuf_exports;
    result.staging_fallbacks -= before.staging_fallbacks;
    result.staged_bytes -= before.staged_bytes;
    result.registration_failures -= before.registration_failures;
    return result;
}

// Pure predicate, also usable without SPDK by acceptance tools and tests.
inline bool NofGpuDirectPassed(const NofGpuDirectStats& stats,
                               bool require_read, bool require_write) {
    const auto& reads = stats.reads;
    const auto& writes = stats.writes;
    return stats.available &&
           reads.submitted + writes.submitted > 0 &&
           (!require_read || reads.submitted > 0) &&
           (!require_write || writes.submitted > 0) &&
           reads.success == reads.submitted &&
           writes.success == writes.submitted && reads.failed == 0 &&
           writes.failed == 0 && stats.host_io == 0 && stats.classify_fail == 0 &&
           stats.staging_fallbacks == 0 && stats.staged_bytes == 0 &&
           stats.dmabuf_registrations + stats.peer_memory_registrations > 0 &&
           stats.registration_failures == 0;
}

/**
 * @brief 一次性验收输出（多行，无尾随换行）。
 *
 * 内容（对应审阅意见 #3 及其复审）：
 *   - gpu_direct_available / reg_mode（MC_NOF_GPU_REG_MODE）
 *   - buffer-domain 识别统计：host_io / cuda_io_submitted /
 *     cuda_io_success / cuda_io_failed / classify_fail；CUDA I/O 另按读写拆分。
 *     计数口径：cuda_io_submitted 是**提交口径**（SubmitRequest 返回成功
 *     即计数），>0 仅说明 SPDK 接受过 CUDA memory-domain I/O 提交，不证明
 *     请求最终完成成功；cuda_io_success 是**完成口径**（SPDK completion
 *     无错误计数），是 GPU I/O 真正成功的证据。两者不可混用。注意
 *     cuda_io_success 按 SPDK 子任务 chunk 计，不是完整 value/object 数，
 *     验收时结合 bench 的操作成功/失败统计解读。
 *   - GPU Direct 过程统计：dmabuf_exports（CUDA DMA-BUF 导出成功次数，
 *     导出后 SPDK 注册仍可能失败或回退 peer-memory，注册结果看
 *     spdk_urma_regs.dmabuf）/ gpu_staging_fallbacks / staged_bytes
 *   - SPDK URMA 注册统计：host / accelerator_attempts（**尝试数**——
 *     SPDK 在 provider 检查与注册之前即递增，nvme_urma_common.c:368；
 *     注册成功 = dmabuf + peer_memory，失败看 failures）/
 *     dmabuf / peer_memory / failures
 */
std::string NofGpuDirectAcceptanceReport();

}  // namespace mooncake
