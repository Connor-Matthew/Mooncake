/**
 * nof_memory_domain.cpp
 * Modified By Yida (v3): NoF GPU Direct 实现，见 nof_memory_domain.h。
 */
#include "nof/nof_memory_domain.h"
#include "nof_gpu_direct_report.h"

#include <glog/logging.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <new>
#include <sstream>
#include <strings.h>

#include "device/accelerator_device.h"
#include "device/accelerator_registry.h"

#if defined(USE_NOF_GPU_DIRECT)
#include <cuda.h>
#endif

namespace mooncake {

namespace {

#if defined(USE_NOF_GPU_DIRECT)

/// pin() 句柄：记录指针所属完整 CUDA allocation 及其 context。
/// pin 不改变内存生命周期——I/O 完成前调用者必须持有 tensor，
/// SPDK/URMA 注册仅在单次 I/O 窗口内存在。
struct NofCudaPinHandle {
    CUcontext ctx{nullptr};
    CUdeviceptr alloc_base{0};
    size_t alloc_size{0};
    void *orig_addr{nullptr};
};

/// 解析 (addr, length) 所属的完整 CUDA allocation。PyTorch/vLLM 传入的
/// pointer 可能只是 caching allocator 大块显存的一个子区间，不能假设它
/// 就是 cudaMalloc() 的返回地址。
int NofCudaPin(void *provider_ctx, void *addr, size_t length,
               void **pin_handle) {
    (void)provider_ctx;
    if (addr == nullptr || pin_handle == nullptr) {
        return -EINVAL;
    }
    CUcontext ctx = nullptr;
    CUresult rc = cuPointerGetAttribute(&ctx, CU_POINTER_ATTRIBUTE_CONTEXT,
                                        reinterpret_cast<CUdeviceptr>(addr));
    if (rc != CUDA_SUCCESS || ctx == nullptr) {
        LOG(ERROR) << "NoF CUDA provider pin: cuPointerGetAttribute failed rc="
                   << static_cast<int>(rc);
        return -EIO;
    }
    rc = cuCtxPushCurrent(ctx);
    if (rc != CUDA_SUCCESS) {
        LOG(ERROR) << "NoF CUDA provider pin: cuCtxPushCurrent failed rc="
                   << static_cast<int>(rc);
        return -EIO;
    }
    CUdeviceptr base = 0;
    size_t size = 0;
    rc = cuMemGetAddressRange(&base, &size, reinterpret_cast<CUdeviceptr>(addr));
    CUcontext popped = nullptr;
    CUresult pop_rc = cuCtxPopCurrent(&popped);
    if (rc != CUDA_SUCCESS || base == 0 || size == 0) {
        LOG(ERROR) << "NoF CUDA provider pin: cuMemGetAddressRange failed rc="
                   << static_cast<int>(rc);
        return -EIO;
    }
    if (pop_rc != CUDA_SUCCESS) {
        LOG(ERROR) << "NoF CUDA provider pin: cuCtxPopCurrent failed rc="
                   << static_cast<int>(pop_rc);
        return -EIO;
    }
    const auto address = reinterpret_cast<CUdeviceptr>(addr);
    if (address < base || address - base >= size || length == 0 ||
        length > size - (address - base)) {
        LOG(ERROR) << "NoF CUDA provider pin: request outside owning range";
        return -EIO;
    }
    auto *handle = new (std::nothrow) NofCudaPinHandle{
        ctx, base, size, addr};
    if (handle == nullptr) {
        return -ENOMEM;
    }
    *pin_handle = handle;
    return 0;
}

void NofCudaUnpin(void *provider_ctx, void *pin_handle) {
    (void)provider_ctx;
    delete static_cast<NofCudaPinHandle *>(pin_handle);
}

/// 导出完整 allocation 的 DMA-BUF fd；offset 为原始指针相对 allocation
/// 起点的偏移（SPDK URMA 的 dmabuf 注册把 cfg.va 与该 offset 配对）。
/// 需要 driver 支持 cuMemGetHandleForAddressRange（CUDA 12.2+）。失败时
/// 返回错误，SPDK 会继续走 peer-memory 注册路径——仍是 GPU Direct，
/// 不涉及 host staging。
int NofCudaExportDmabuf(void *provider_ctx, void *pin_handle, int *fd,
                        uint64_t *offset) {
    (void)provider_ctx;
    auto *handle = static_cast<NofCudaPinHandle *>(pin_handle);
    if (handle == nullptr || fd == nullptr || offset == nullptr) {
        return -EINVAL;
    }
    CUresult rc = cuCtxPushCurrent(handle->ctx);
    if (rc != CUDA_SUCCESS) {
        LOG(ERROR) << "NoF CUDA provider export: cuCtxPushCurrent failed rc="
                   << static_cast<int>(rc);
        return -EIO;
    }
    // Modified By Yida (v3): cuMemGetHandleForAddressRange 的 handleType 是
    // CUmemRangeHandleType——注意与 cuMemExportToShareableHandle 用的
    // CUmemAllocationHandleType 是两套语义不同的枚举，不能混用。
    // DMA_BUF_FD 的输出是 Linux dma-buf fd（int）。失败时（如闭源驱动
    // rc=801）走 peer-memory 注册路径——仍是 GPU Direct。
    int dmabuf_fd = -1;
    rc = cuMemGetHandleForAddressRange(
        &dmabuf_fd, handle->alloc_base, handle->alloc_size,
        CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD, 0);
    CUcontext popped = nullptr;
    CUresult pop_rc = cuCtxPopCurrent(&popped);
    if (rc != CUDA_SUCCESS) {
        LOG(ERROR) << "NoF CUDA provider export: "
                      "cuMemGetHandleForAddressRange failed rc="
                   << static_cast<int>(rc)
                   << " (driver不支持 dmabuf export 时会走 peer-memory 注册)";
        return -EIO;
    }
    if (pop_rc != CUDA_SUCCESS) {
        LOG(ERROR) << "NoF CUDA provider export: cuCtxPopCurrent failed rc="
                   << static_cast<int>(pop_rc);
        return -EIO;
    }
    *fd = dmabuf_fd;
    *offset = static_cast<uint64_t>(reinterpret_cast<CUdeviceptr>(handle->orig_addr) -
                                    handle->alloc_base);
    // Modified By Yida (v3): 验收指标——export 成功即一次 DMA-BUF 导出
    // （注意：导出后 SPDK 注册仍可能失败或回退 peer-memory，注册结果看
    // spdk_urma_regs.dmabuf，见外部审阅计数口径复审）
    NofMemoryDomainManager::GetInstance().OnDmabufExport();
    return 0;
}

const char *GetRegModeFromEnv() {
    // MC_NOF_GPU_REG_MODE: dmabuf（默认，尝试 DMA-BUF export，失败由 SPDK
    // 回退 peer-memory 注册）| peer（直接走 peer-memory 注册）
    const char *mode = std::getenv("MC_NOF_GPU_REG_MODE");
    return (mode != nullptr && strcasecmp(mode, "peer") == 0) ? "peer"
                                                              : "dmabuf";
}

#endif  // USE_NOF_GPU_DIRECT

}  // namespace

NofMemoryDomainManager &NofMemoryDomainManager::GetInstance() {
    static NofMemoryDomainManager instance;
    return instance;
}

NofMemoryDomainManager::~NofMemoryDomainManager() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto &[device_id, domain] : cuda_domains_) {
        if (domain != nullptr) {
            spdk_memory_domain_destroy(domain);
        }
    }
    cuda_domains_.clear();
}

bool NofMemoryDomainManager::IsGpuDirectAvailable() const {
#if defined(USE_NOF_GPU_DIRECT)
    const auto &registry = device::GetAcceleratorRegistry();
    for (const auto *dev : registry.RegisteredDevices()) {
        if (dev != nullptr && dev->Vendor() == device::AcceleratorVendor::kNvidia &&
            dev->Available(false)) {
            return true;
        }
    }
#endif
    return false;
}

struct spdk_memory_domain *NofMemoryDomainManager::GetOrCreateCudaDomain(
    int32_t device_id, std::string &error) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cuda_domains_.find(device_id);
    if (it != cuda_domains_.end()) {
        return it->second;
    }

#if defined(USE_NOF_GPU_DIRECT)
    // 进程级注册一次 CUDA provider（SPDK URMA 注册层据此 pin/export GPU 内存）
    if (!provider_registered_) {
        static const bool export_dmabuf_enabled =
            std::strcmp(GetRegModeFromEnv(), "dmabuf") == 0;
        static const struct spdk_nvme_urma_memory_provider provider = {
            .name = "mooncake-cuda",
            .type = SPDK_NVME_URMA_MEM_CUDA,
            .pin = NofCudaPin,
            .unpin = NofCudaUnpin,
            .export_dmabuf =
                export_dmabuf_enabled ? NofCudaExportDmabuf : nullptr,
            .provider_ctx = nullptr,
        };
        int rc = spdk_nvme_urma_register_memory_provider(&provider);
        if (rc != 0 && rc != -EEXIST) {
            error = "spdk_nvme_urma_register_memory_provider failed rc=" +
                    std::to_string(rc);
            LOG(ERROR) << "NoF GPU Direct: " << error;
            return nullptr;
        }
        provider_registered_ = true;
        LOG(INFO) << "NoF GPU Direct: CUDA provider registered, reg_mode="
                  << GetRegModeFromEnv();
    }

    // domain 命名固定为 cuda:mooncake:<id>；dma_device_id 含 "cuda"，
    // SPDK nvme_urma_req_memory_type() 据此选择 CUDA 内存注册
    char id[64];
    std::snprintf(id, sizeof(id), "cuda:mooncake:%d", device_id);
    struct spdk_memory_domain *domain = nullptr;
    struct spdk_memory_domain_ctx dctx = {};
    dctx.size = sizeof(dctx);
    // user_ctx 不需要：URMA 侧通过 provider 回调完成 pin/export
    int rc = spdk_memory_domain_create(
        &domain, SPDK_DMA_DEVICE_VENDOR_SPECIFIC_TYPE_START, &dctx, id);
    if (rc != 0 || domain == nullptr) {
        error = "spdk_memory_domain_create failed rc=" + std::to_string(rc);
        LOG(ERROR) << "NoF GPU Direct: " << error;
        return nullptr;
    }
    cuda_domains_[device_id] = domain;
    LOG(INFO) << "NoF GPU Direct: created memory domain " << id;
    return domain;
#else
    (void)device_id;
    error = "USE_NOF_GPU_DIRECT is not enabled at build time";
    return nullptr;
#endif
}

NofDomainBuffer NofMemoryDomainManager::ResolveBuffer(void *ptr,
                                                      size_t length) {
    if (ptr == nullptr || length == 0) {
        OnClassifyFail();
        return NofDomainBuffer{{NofMemoryType::kError, -1}, nullptr};
    }

    const auto &registry = device::GetAcceleratorRegistry();
    auto runtime = registry.RuntimeAccelerators(false);
    device::PointerInfo info{};
    for (const auto *dev : runtime.Devices()) {
        if (dev == nullptr) {
            continue;
        }
        // 严格探测：区分“不是 GPU 指针”与“探测出错”——后者不能当 host
        auto probe = dev->ProbePointerStrict(ptr, &info);
        if (probe == device::PointerProbeResult::kDevice) {
            if (dev->Vendor() == device::AcceleratorVendor::kNvidia) {
                std::string error;
                auto *domain = GetOrCreateCudaDomain(info.device_id, error);
                if (domain == nullptr) {
                    // 严格错误策略：识别为 GPU 后初始化失败必须报错，
                    // 不允许静默走 host staging
                    OnClassifyFail();
                    return NofDomainBuffer{
                        {NofMemoryType::kError, info.device_id}, nullptr};
                }
                return NofDomainBuffer{{NofMemoryType::kCuda, info.device_id},
                                       domain};
            }
            // ROCm/NPU 使用同一框架后续接入；未实现的类型不能冒充 CUDA/CPU
            LOG(WARNING) << "NoF: device memory of vendor="
                         << static_cast<int>(dev->Vendor())
                         << " is not supported yet (need provider)";
            return NofDomainBuffer{
                {NofMemoryType::kNotSupported, info.device_id}, nullptr};
        }
        if (probe == device::PointerProbeResult::kProbeError) {
            LOG(ERROR) << "NoF: pointer probe failed for ptr=" << ptr
                       << " — refusing to treat as host memory";
            OnClassifyFail();
            return NofDomainBuffer{{NofMemoryType::kError, -1}, nullptr};
        }
    }
    // 所有已注册 accelerator 都判定为非设备指针：普通 CPU 内存
    return NofDomainBuffer{{NofMemoryType::kHost, -1},
                           spdk_memory_domain_get_system_domain()};
}

struct spdk_memory_domain *NofMemoryDomainManager::GetCudaDomain(
    int32_t device_id) {
    std::string error;
    return GetOrCreateCudaDomain(device_id, error);
}

void NofMemoryDomainManager::GetUrmaMemoryStats(
    struct spdk_nvme_urma_memory_stats *stats) {
    spdk_nvme_urma_get_memory_stats(stats);
}

bool NofWholePoolRegistrationAvailable() {
#ifdef HAVE_SPDK_URMA_WHOLE_POOL
    return true;
#else
    return false;
#endif
}

void RecordNofIoSubmitted(const NofBufferInfo &buffer, bool write,
                          uint64_t bytes) {
    NofMemoryDomainManager::GetInstance().OnIoSubmitted(buffer, write, bytes);
}

void RecordNofIoCompleted(const NofBufferInfo &buffer, bool success,
                          bool write, uint64_t bytes) {
    NofMemoryDomainManager::GetInstance().OnIoCompleted(buffer, success,
                                                         write, bytes);
}

void RecordNofIoAborted(const NofBufferInfo &buffer, bool write,
                        uint64_t count) {
    NofMemoryDomainManager::GetInstance().OnIoAborted(buffer, write, count);
}

bool NofGpuDirectAvailable() {
    return NofMemoryDomainManager::GetInstance().IsGpuDirectAvailable();
}

NofGpuDirectStats GetNofGpuDirectStats() {
    const auto &mgr = NofMemoryDomainManager::GetInstance();
    struct spdk_nvme_urma_memory_stats stats {};
    NofMemoryDomainManager::GetUrmaMemoryStats(&stats);
    NofGpuDirectStats result;
    result.available = mgr.IsGpuDirectAvailable();
#if defined(USE_NOF_GPU_DIRECT)
    result.reg_mode = GetRegModeFromEnv();
#endif
    result.reads = mgr.CudaIoStats(false);
    result.writes = mgr.CudaIoStats(true);
    result.host_io = mgr.host_io_total();
    result.classify_fail = mgr.classify_fail_total();
    result.dmabuf_exports = mgr.dmabuf_export_total();
    result.staging_fallbacks = mgr.gpu_staging_fallback_total();
    result.staged_bytes = mgr.payload_staged_bytes();
    result.host_registrations = stats.host_registrations;
    result.accelerator_attempts = stats.accelerator_registrations;
    result.dmabuf_registrations = stats.dmabuf_registrations;
    result.peer_memory_registrations = stats.peer_memory_registrations;
    result.registration_failures = stats.registration_failures;
    return result;
}

std::string NofGpuDirectAcceptanceReport(const NofGpuDirectStats& stats) {
    std::ostringstream oss;
    oss << "gpu_direct_available=" << (stats.available ? "yes" : "no");
    if (!stats.reg_mode.empty()) oss << " reg_mode=" << stats.reg_mode;
    // buffer-domain 识别结果（计数口径，外部审阅 #3 复审）：
    // cuda_io_submitted>0 仅说明 SPDK 接受过 CUDA memory-domain I/O 提交
    // （SubmitRequest 返回成功）；提交≠完成，最终成功以 cuda_io_success
    // （completion 无错误计数）为准。
    oss << "\nbuffer_domain: host_io=" << stats.host_io
        << " cuda_io_submitted="
        << stats.reads.submitted + stats.writes.submitted
        << " cuda_io_success=" << stats.reads.success + stats.writes.success
        << " cuda_io_failed=" << stats.reads.failed + stats.writes.failed
        << " classify_fail=" << stats.classify_fail;
    oss << "\ncuda_read: submitted=" << stats.reads.submitted
        << " success=" << stats.reads.success
        << " failed=" << stats.reads.failed
        << " submitted_bytes=" << stats.reads.submitted_bytes
        << " success_bytes=" << stats.reads.success_bytes;
    oss << "\ncuda_write: submitted=" << stats.writes.submitted
        << " success=" << stats.writes.success
        << " failed=" << stats.writes.failed
        << " submitted_bytes=" << stats.writes.submitted_bytes
        << " success_bytes=" << stats.writes.success_bytes;
    // GPU Direct 过程统计：staging 回退与 staged 字节应恒为 0（严格错误
    // 策略下出现即异常）。dmabuf_exports 是 CUDA DMA-BUF 导出成功次数，
    // 注册是否成功看 spdk_urma_regs.dmabuf。
    oss << "\ngpu_direct_io: dmabuf_exports=" << stats.dmabuf_exports
        << " gpu_staging_fallbacks=" << stats.staging_fallbacks
        << " staged_bytes=" << stats.staged_bytes;
    // SPDK URMA 注册统计。注意 accelerator 是**尝试数**：SPDK 在 provider
    // 检查与 URMA 注册之前就递增（nvme_urma_common.c:368），注册成功 =
    // dmabuf + peer_memory（:408/:431），失败看 failures（:379/:386/:435）。
    oss << "\nspdk_urma_regs: host=" << stats.host_registrations
        << " accelerator_attempts=" << stats.accelerator_attempts
        << " dmabuf=" << stats.dmabuf_registrations
        << " peer_memory=" << stats.peer_memory_registrations
        << " failures=" << stats.registration_failures;
    return oss.str();
}

std::string NofGpuDirectAcceptanceReport() {
    return NofGpuDirectAcceptanceReport(GetNofGpuDirectStats());
}

}  // namespace mooncake
