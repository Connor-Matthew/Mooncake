#define _GNU_SOURCE

#include "nof/spdk_initiator.h"

#include <glog/logging.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <cstdlib>
#include <map>
#include <mutex>
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif
#include <sstream>
#include <thread>
#include <vector>

#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/version.h>

#include "config/spdk_controller_config.h"
#include "environ.h"
#include "environment_variables.h"
#include "nof/page_registry.h"
#include "nof/probe_context_pool.h"

#ifdef USE_NOF_URMA
#include <new>

#include "nof/nof_memory_domain.h"  // private: SPDK memory domains (GDR)
#endif

namespace mooncake {
namespace {

#if defined(__linux__)
// Compact "0-3,8,10-11" rendering of a CPU mask for logging. Mirrors the 930
// SpdkWrapper helper so the affinity diagnostics read the same as the branch
// they were ported from.
std::string FormatCpuAffinity(const cpu_set_t& cpuset) {
    std::ostringstream out;
    bool first = true;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &cpuset)) {
            continue;
        }
        const int begin = cpu;
        while (cpu + 1 < CPU_SETSIZE && CPU_ISSET(cpu + 1, &cpuset)) {
            ++cpu;
        }
        if (!first) {
            out << ',';
        }
        out << begin;
        if (cpu != begin) {
            out << '-' << cpu;
        }
        first = false;
    }
    return out.str();
}
#endif  // defined(__linux__)

// MC_SPDK_RESET_AFFINITY (default true). Read through the shared typed env
// reader like every other MC_* knob; unset/invalid keeps the default (an
// invalid value warns and falls back inside Environ::ReadOr).
bool ResetCallerAffinityAfterEnvInit() {
    return Environ::ReadOr(SpdkEnvEnvironmentVariables::MC_SPDK_RESET_AFFINITY,
                           true);
}

// Completion status strings. SPDK 26.05+ dropped the opcode-less
// spdk_nvme_cpl_get_status_string() in favour of the `_ext` form, which
// takes the submitted opcode so it can consult command-specific status
// tables; v23.01.x (the SPDK that main is developed against) only has the
// opcode-less form. Select at compile time so one tree builds against both.
const char* NofCplStatusString(const struct spdk_nvme_cpl* cpl, uint8_t opc) {
#if defined(SPDK_VERSION_MAJOR) && \
    (SPDK_VERSION_MAJOR > 26 ||    \
     (SPDK_VERSION_MAJOR == 26 && SPDK_VERSION_MINOR >= 5))
    return spdk_nvme_cpl_get_status_string_ext(&cpl->status, opc);
#else
    (void)opc;
    return spdk_nvme_cpl_get_status_string(&cpl->status);
#endif
}

void ApplyCtrlrOptsFromEnv(struct spdk_nvme_ctrlr_opts* opts) {
    const auto config = SpdkControllerConfig::FromEnvironment();
    // Keep SPDK's default KATO unless configured: when a client dies before
    // it can detach, target-side KATO is what reclaims the orphaned
    // controller and its qpairs. Keep-alives are sent by Impl's admin-poll
    // thread, which runs whenever a controller is attached.
    if (config.keep_alive_timeout_ms.has_value()) {
        opts->keep_alive_timeout_ms = *config.keep_alive_timeout_ms;
    }
    if (config.num_io_queues.has_value()) {
        opts->num_io_queues = *config.num_io_queues;
    }
    if (config.io_queue_size.has_value()) {
        opts->io_queue_size = *config.io_queue_size;
    }
    if (config.io_queue_requests.has_value()) {
        opts->io_queue_requests = *config.io_queue_requests;
    }
    if (config.transport_ack_timeout.has_value()) {
        opts->transport_ack_timeout = *config.transport_ack_timeout;
    }
    if (config.admin_queue_size.has_value()) {
        opts->admin_queue_size = *config.admin_queue_size;
    }
    if (config.fabrics_connect_timeout_us.has_value()) {
        opts->fabrics_connect_timeout_us = *config.fabrics_connect_timeout_us;
    }
    if (config.header_digest.has_value()) {
        opts->header_digest = *config.header_digest;
    }
    if (config.data_digest.has_value()) {
        opts->data_digest = *config.data_digest;
    }
    LOG(INFO) << "NVMe ctrlr opts: num_io_queues=" << opts->num_io_queues
              << ", io_queue_size=" << opts->io_queue_size
              << ", io_queue_requests=" << opts->io_queue_requests
              << ", keep_alive_timeout_ms=" << opts->keep_alive_timeout_ms
              << ", transport_ack_timeout="
              << static_cast<int>(opts->transport_ack_timeout)
              << ", admin_queue_size=" << opts->admin_queue_size
              << ", fabrics_connect_timeout_us="
              << opts->fabrics_connect_timeout_us
              << ", header_digest=" << opts->header_digest
              << ", data_digest=" << opts->data_digest;
}

// spdk_nvme_probe/spdk_nvme_detach mutate the NVMe driver's process-global
// controller state and are not thread-safe against each other. The
// pre-refactor SpdkWrapper was a process singleton whose ctrlrs_mutex
// serialized them; with per-instance initiators a process-wide lock is
// needed instead. Held only around the SPDK calls; the lock order is
// ctrlrs_mutex_ < NvmeCtrlrStateMutex (the destructor takes this lock while
// holding ctrlrs_mutex_, and no path takes ctrlrs_mutex_ while holding this
// one), so a slow probe still cannot stall cache lookups. Leaked to avoid
// any static destruction-order hazard.
std::mutex& NvmeCtrlrStateMutex() {
    static std::mutex* m = new std::mutex();
    return *m;
}

}  // namespace

// ---------------------------------------------------------------------------
// Shared, refcounted SPDK environment guard.
//
// DPDK EAL init is process-global. The env is acquired lazily on first use
// and, once initialized, pinned until process exit: SPDK v23.01.1 rejects a
// non-NULL-opts spdk_env_init after spdk_env_fini with -EINVAL, so a
// mid-process fini would break every later Acquire() in this process.
// ---------------------------------------------------------------------------

class SpdkEnvGuard {
   public:
    static std::shared_ptr<SpdkEnvGuard> Acquire() {
        std::lock_guard<std::mutex> lock(RegistryMutex());
        if (auto existing = Registry().lock()) {
            return existing;
        }
        // Raw pointer so an init failure can be plain-deleted under the lock
        // (the Deleter locks RegistryMutex; destroying a shared_ptr while
        // holding it would deadlock).
        auto* raw = new SpdkEnvGuard();
        if (!raw->ok_) {
            delete raw;
            return nullptr;
        }
        std::shared_ptr<SpdkEnvGuard> guard(raw, Deleter());
        Registry() = guard;
        ProcessPin() = guard;  // pinned until process exit (class comment)
        return guard;
    }

    SpdkEnvGuard(const SpdkEnvGuard&) = delete;
    SpdkEnvGuard& operator=(const SpdkEnvGuard&) = delete;

   private:
    SpdkEnvGuard() {
        // rte_eal_init pins its calling thread to the main lcore, and Linux
        // threads inherit the creator's mask — without a restore, threads the
        // host app spawns later (RPC pool, helpers) would inherit that
        // restriction. Save/restore the affinity around spdk_env_init
        // (glibc-only APIs, hence Linux-gated). MC_SPDK_RESET_AFFINITY=0 keeps
        // EAL's placement instead.
        const bool reset_affinity = ResetCallerAffinityAfterEnvInit();
#if defined(__linux__)
        cpu_set_t original_affinity;
        CPU_ZERO(&original_affinity);
        if (reset_affinity &&
            pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t),
                                   &original_affinity) != 0) {
            // Fail instead of proceeding blind: an unknown mask cannot be
            // restored, and silently leaving the process pinned on EAL's main
            // lcore is exactly the regression this code prevents.
            PLOG(ERROR)
                << "Cannot save CPU affinity before SPDK initialization";
            return;  // ok_ stays false; Acquire() reports NoF unavailable
        }
#endif

        struct spdk_env_opts opts;
        spdk_env_opts_init(&opts);
        opts.name = "mooncake";
        // Containers without physical-address access (restricted
        // /proc/self/pagemap) cannot run IOVA-as-PA; MC_NOF_IOVA_MODE lets
        // operators opt into "va" there. When unset, SPDK keeps its
        // PA-preferred default (matching baseline SpdkWrapper).
        const char* iova_mode = std::getenv("MC_NOF_IOVA_MODE");
        if (iova_mode && *iova_mode != '\0') {
            opts.iova_mode = iova_mode;
        }
        ok_ = (spdk_env_init(&opts) == 0);

#if defined(__linux__)
        if (reset_affinity) {
            // Capture EAL's placement before undoing it: it is both the
            // "before" value of the restore and the evidence an operator needs
            // when a later thread still looks pinned.
            cpu_set_t eal_affinity;
            CPU_ZERO(&eal_affinity);
            const bool have_eal_affinity =
                pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t),
                                       &eal_affinity) == 0;
            // Restore even when initialization failed: EAL may already have
            // changed the affinity before returning an error.
            if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t),
                                       &original_affinity) != 0) {
                // Do not retry or fail the init: a successful EAL stays
                // initialized for the whole process (see ProcessPin), so the
                // only useful outcome here is an explicit placement warning.
                PLOG(ERROR)
                    << "Failed to restore CPU affinity after spdk_env_init"
                    << " (init_ok=" << ok_ << "), requested=["
                    << FormatCpuAffinity(original_affinity)
                    << "]; subsequently created threads may inherit EAL's CPU "
                       "restriction";
            } else {
                cpu_set_t restored_affinity;
                CPU_ZERO(&restored_affinity);
                const bool have_restored_affinity =
                    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t),
                                           &restored_affinity) == 0;
                LOG(INFO) << "SPDK initialization CPU affinity: before=["
                          << FormatCpuAffinity(original_affinity)
                          << "] after_init=["
                          << (have_eal_affinity
                                  ? FormatCpuAffinity(eal_affinity)
                                  : "unknown")
                          << "] restored=["
                          << (have_restored_affinity
                                  ? FormatCpuAffinity(restored_affinity)
                                  : "unknown")
                          << "] init_ok=" << ok_;
            }
        } else {
            LOG(INFO) << "SPDK CPU affinity restore disabled by "
                         "MC_SPDK_RESET_AFFINITY=0";
        }
#else
        if (!reset_affinity) {
            LOG(INFO) << "MC_SPDK_RESET_AFFINITY is ignored on this platform: "
                         "no CPU-affinity restore is performed";
        }
#endif

        if (!ok_) {
            LOG(ERROR) << "spdk_env_init failed"
                       << (iova_mode && *iova_mode != '\0'
                               ? ""
                               : " (in a container without "
                                 "physical-address access, set "
                                 "MC_NOF_IOVA_MODE=va)");
        }
    }

    // Runs when the last reference dies. In practice that never happens:
    // ProcessPin is deliberately leaked (see below), so one reference is
    // always alive and spdk_env_fini never runs — the OS reclaims DPDK
    // resources at process exit, and SPDK v23.01.1 could not be
    // re-initialized after fini anyway. spdk_env_fini is serialized with
    // Acquire via RegistryMutex: a concurrent Acquire holds the same mutex
    // while running spdk_env_init, so DPDK teardown and re-init can never
    // race.
    struct Deleter {
        void operator()(SpdkEnvGuard* p) const {
            std::lock_guard<std::mutex> lock(RegistryMutex());
            if (p->ok_) {
                spdk_env_fini();
            }
            delete p;
        }
    };

    static std::mutex& RegistryMutex() {
        // Deliberately leaked (like the ShmHelper singleton): the Deleter
        // can run after this mutex during static destruction when a guard's
        // last reference is held by an earlier-constructed function static.
        static std::mutex* m = new std::mutex();
        return *m;
    }
    static std::weak_ptr<SpdkEnvGuard>& Registry() {
        // Deliberately leaked (like RegistryMutex): a probe-only initiator
        // never registers memory, so the NofRegistryBundle below is first
        // constructed by its ~Impl backstop during static destruction —
        // after function-local statics like this one have already been
        // destroyed. The bundle's constructor calls Acquire(), which must
        // not touch a destroyed static. Leaking also means the pin below
        // never releases, so spdk_env_fini never runs at all; the OS
        // reclaims DPDK resources at exit.
        static std::weak_ptr<SpdkEnvGuard>* w =
            new std::weak_ptr<SpdkEnvGuard>();
        return *w;
    }
    // Process-lifetime pin: keeps one reference from the first successful
    // Acquire until process exit, so the env is never finalized mid-process
    // (SPDK v23.01.1 cannot be re-initialized after fini). Leaked like
    // Registry() for the same static-destruction-order reason.
    static std::shared_ptr<SpdkEnvGuard>& ProcessPin() {
        static std::shared_ptr<SpdkEnvGuard>* pin =
            new std::shared_ptr<SpdkEnvGuard>();
        return *pin;
    }

    bool ok_{false};
};

// ---------------------------------------------------------------------------
// Process-global page registry singleton (#3131).
//
// SPDK's translation table is process-global, so the bookkeeping must be
// too: per-instance refcounts would let one initiator instance unmap pages
// another still uses. Each SpdkInitiator::Impl registers under its own
// `this` as the owner token.
//
// The bundle also pins the SPDK env from first use until process exit, so an
// UnregisterMemory can never run after spdk_env_fini.
// ---------------------------------------------------------------------------

#ifdef HAVE_SPDK_MEM_REGISTER
struct NofRegistryBundle {
    std::shared_ptr<SpdkEnvGuard> env;
    NofPageRegistry registry;

    NofRegistryBundle()
        : env(SpdkEnvGuard::Acquire()),
          registry(&spdk_mem_register, &spdk_mem_unregister) {}
};

NofRegistryBundle& GetNofRegistryBundle() {
    // Deliberately leaked (like RegistryMutex and the ShmHelper singleton):
    // avoids static-destruction-order issues with probe initiators' backstop
    // UnregisterAll. A probe-only process first constructs the bundle inside
    // that backstop during static destruction, so its constructor's Acquire()
    // runs late too — safe because Registry()/ProcessPin() are likewise
    // leaked and never destroyed.
    static NofRegistryBundle* bundle = new NofRegistryBundle();
    return *bundle;
}
#endif  // HAVE_SPDK_MEM_REGISTER

// ---------------------------------------------------------------------------
// Opaque handle definition — visible only inside this file.
// ---------------------------------------------------------------------------

// One handle per I/O lane (queue pair). Lane 0 is the segment handle returned
// by OpenSegment and also owns the segment's other lanes.
class NofSegmentHandle {
   public:
    struct spdk_nvme_qpair* qpair = nullptr;
    struct spdk_nvme_ns* ns = nullptr;
    struct spdk_nvme_ctrlr* ctrlr = nullptr;
    uint32_t lane_index = 0;
    // Set by MarkLaneFailed from any thread; checked before every submit.
    std::atomic<bool> failed{false};
    // Set by ReleaseFailedLane on the lane's polling thread once the qpair
    // is freed; qpair is nullptr afterwards.
    std::atomic<bool> released{false};

    // ---- lane 0 only ----
    bool is_segment = false;
    std::mutex lanes_mutex;
    std::atomic<bool> lanes_ready{false};
    // Lanes 1..N-1, fixed once lanes_ready is published.
    std::vector<std::unique_ptr<NofSegmentHandle>> extra_lanes;
};

namespace {

struct tr_info {
    struct spdk_nvme_transport_id trid;
    std::string ctrlr_key;
    uint32_t ns;
};

struct ctrlr_info {
    struct spdk_nvme_ctrlr* ctrlr;
    std::map<uint32_t, std::unique_ptr<NofSegmentHandle>> ns_seg;
    std::mutex ns_mutex;
    // Last admin-poll error, so a persistent failure is logged once.
    int32_t last_admin_poll_error = 0;
};

void FreeLaneQpair(NofSegmentHandle* lane) {
    if (lane && lane->qpair) {
        spdk_nvme_ctrlr_free_io_qpair(lane->qpair);
        lane->qpair = nullptr;
    }
}

// Process-unique owner tokens for the page registry: unlike an Impl's heap
// address, a monotonically increasing id is never recycled (a new Impl could
// otherwise inherit a dead one's leftover registration records).
std::atomic<uint64_t> g_nof_owner_token{1};

}  // namespace

// ---------------------------------------------------------------------------
// SpdkInitiator::Impl
// ---------------------------------------------------------------------------

class SpdkInitiator::Impl {
   public:
    Impl() = default;

    ~Impl() {
        // Stop keep-alives before any controller is detached below; the
        // thread iterates connected_ctrlrs_.
        StopAdminPoll();
        if (!env_guard_) {
            return;
        }
        // Give timed-out probe commands a bounded chance to complete before
        // the qpairs are freed below.
        DrainQuarantinedProbes();
        {
            std::lock_guard<std::mutex> lock(ctrlrs_mutex_);
            std::lock_guard<std::mutex> nvme_lock(NvmeCtrlrStateMutex());
            for (auto& [_, info] : connected_ctrlrs_) {
                if (info) {
                    for (auto& [_, seg] : info->ns_seg) {
                        if (!seg) {
                            continue;
                        }
                        // Lanes released by ReleaseFailedLane have a null
                        // qpair already.
                        for (auto& lane : seg->extra_lanes) {
                            FreeLaneQpair(lane.get());
                        }
                        FreeLaneQpair(seg.get());
                    }
                    if (info->ctrlr) {
                        spdk_nvme_detach(info->ctrlr);
                    }
                }
            }
            connected_ctrlrs_.clear();
        }
        {
            std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
            for (auto& [_, probe_buffer] : probe_buffers_) {
                if (probe_buffer && probe_buffer->ptr) {
                    spdk_free(probe_buffer->ptr);
                    probe_buffer->ptr = nullptr;
                    probe_buffer->size = 0;
                }
            }
            probe_buffers_.clear();
        }
#ifdef HAVE_SPDK_MEM_REGISTER
        // Backstop, after all I/O teardown above: release every page
        // registration this instance still holds, in case the real-client
        // teardown path missed any.
        if (GetNofRegistryBundle().registry.UnregisterAll(owner_token_) !=
            ErrorCode::OK) {
            LOG(ERROR) << "NoF initiator teardown could not release all page "
                          "registrations; they stay registered until process "
                          "exit";
        }
#endif
        // env_guard_ is released after all qpair/ctrlr teardown above. That
        // only drops this instance's reference: in HAVE_SPDK_MEM_REGISTER
        // builds the leaked NofRegistryBundle keeps the env pinned for the
        // whole process, so spdk_env_fini (in ~SpdkEnvGuard) never runs here.
    }

    NofSegmentHandle* OpenSegment(const std::string& tr_str) {
        if (!EnsureEnv()) {
            return nullptr;
        }
        tr_info tr;
        if (ParseTransPortStr(tr_str, &tr) != 0) {
            return nullptr;
        }

        ctrlr_info* info = nullptr;
        {
            std::lock_guard<std::mutex> lock(ctrlrs_mutex_);
            auto it = connected_ctrlrs_.find(tr.ctrlr_key);
            if (it != connected_ctrlrs_.end()) {
                info = it->second.get();
            }
        }
        if (!info) {
            // Connect OUTSIDE the mutex: spdk_nvme_probe may block for
            // seconds and would stall first-time opens of other endpoints.
            auto new_info = std::make_unique<ctrlr_info>();
            const int rc = ConnectController(&tr.trid, new_info.get());
            if (rc != 0 || new_info->ctrlr == nullptr) {
                LOG(ERROR) << "Failed to attach NoF controller endpoint="
                           << tr_str << " rc=" << rc
                           << " ctrlr=" << new_info->ctrlr;
                return nullptr;
            }
            bool detach_duplicate = false;
            {
                std::lock_guard<std::mutex> lock(ctrlrs_mutex_);
                auto it = connected_ctrlrs_.find(tr.ctrlr_key);
                if (it != connected_ctrlrs_.end()) {
                    // Lost a concurrent first-open race: use the winner;
                    // detach our duplicate outside the mutex (can block).
                    info = it->second.get();
                    detach_duplicate = true;
                } else {
                    info = new_info.get();
                    connected_ctrlrs_[tr.ctrlr_key] = std::move(new_info);
                }
            }
            if (detach_duplicate) {
                std::lock_guard<std::mutex> nvme_lock(NvmeCtrlrStateMutex());
                spdk_nvme_detach(new_info->ctrlr);
            }
        }

        // spdk_nvme_probe() may finish without invoking attach_cb when the
        // remote target rejects controller creation. Never pass that missing
        // controller into spdk_nvme_ctrlr_* helpers; they require a non-null
        // controller. ConnectController() reports this as -ENODEV, but a
        // cached entry is re-checked here so a future detach path cannot
        // silently downgrade the invariant.
        if (info == nullptr || info->ctrlr == nullptr) {
            LOG(ERROR) << "NoF controller is unavailable endpoint=" << tr_str;
            return nullptr;
        }
        // Keep-alives for every attached controller (no-op after the first).
        StartAdminPoll();

        NofSegmentHandle* seg_handle = nullptr;
        {
            auto& ns_seg = info->ns_seg;
            std::lock_guard<std::mutex> lock(info->ns_mutex);
            auto ns_it = ns_seg.find(tr.ns);
            if (ns_it != ns_seg.end()) {
                return ns_it->second.get();
            }

            struct spdk_nvme_ns* ns = nullptr;
            if (spdk_nvme_ctrlr_is_active_ns(info->ctrlr, tr.ns)) {
                ns = spdk_nvme_ctrlr_get_ns(info->ctrlr, tr.ns);
            } else {
                LOG(ERROR) << "spdk_nvme_ctrlr_is_active_ns failed";
                return nullptr;
            }

            struct spdk_nvme_qpair* qpair =
                spdk_nvme_ctrlr_alloc_io_qpair(info->ctrlr, nullptr, 0);
            if (!qpair) {
                LOG(ERROR) << "alloc spdk_nvme_qpair failed";
                return nullptr;
            }

            auto new_seg = std::make_unique<NofSegmentHandle>();
            new_seg->qpair = qpair;
            new_seg->ns = ns;
            new_seg->ctrlr = info->ctrlr;
            new_seg->lane_index = 0;
            new_seg->is_segment = true;
            seg_handle = new_seg.get();
            ns_seg[tr.ns] = std::move(new_seg);
        }
        return seg_handle;
    }

    uint32_t GetBlockSize(const NofSegmentHandle* handle) {
        if (!handle || !handle->ns) {
            return kInvalidBlockSize;
        }
        return spdk_nvme_ns_get_sector_size(handle->ns);
    }

    int SubmitIO(NofSegmentHandle* handle, void* buffer, uint64_t byte_offset,
                 uint64_t byte_length, NofIOOp op, NofIOAdaptor* adaptor) {
        return SubmitCommon(handle, buffer, byte_offset, byte_length, op,
                            adaptor, /*device_buffer=*/nullptr);
    }

    // ---- GPU Direct -------------------------------------------------------

    NofBufferInfo ResolveBuffer(void* ptr, size_t length) {
#ifdef USE_NOF_URMA
        return NofMemoryDomainManager::GetInstance()
            .ResolveBuffer(ptr, length)
            .info;
#else
        (void)ptr;
        (void)length;
        return NofBufferInfo{};
#endif
    }

    int SubmitIOWithBuffer(NofSegmentHandle* handle, void* buffer,
                           uint64_t byte_offset, uint64_t byte_length,
                           NofIOOp op, NofIOAdaptor* adaptor,
                           const NofBufferInfo& buffer_info) {
        if (buffer_info.type == NofMemoryType::kHost) {
            return SubmitIO(handle, buffer, byte_offset, byte_length, op,
                            adaptor);
        }
        if (buffer_info.type != NofMemoryType::kCuda) {
            return -ENOTSUP;  // kError / kNotSupported / unimplemented vendor
        }
#ifdef USE_NOF_URMA
        return SubmitCommon(handle, buffer, byte_offset, byte_length, op,
                            adaptor, &buffer_info);
#else
        return -ENOTSUP;
#endif
    }

    std::shared_ptr<void> RegisterMemoryForEndpoint(
        const std::string& transport_str, void* ptr, size_t size) {
#ifdef HAVE_SPDK_URMA_WHOLE_POOL
        if (!ptr || size == 0) {
            return nullptr;
        }
        if (transport_str.find("trtype:URMA") == std::string::npos) {
            LOG(ERROR) << "NoF whole-pool registration only supports URMA "
                          "endpoints: "
                       << transport_str;
            return nullptr;
        }
        NofSegmentHandle* segment = OpenSegment(transport_str);
        if (!segment || !segment->qpair) {
            return nullptr;
        }
        const auto resolved =
            NofMemoryDomainManager::GetInstance().ResolveBuffer(ptr, size);
        if (resolved.info.type != NofMemoryType::kHost &&
            resolved.info.type != NofMemoryType::kCuda) {
            return nullptr;
        }
        const auto type = resolved.info.type == NofMemoryType::kCuda
                              ? SPDK_NVME_URMA_MEM_CUDA
                              : SPDK_NVME_URMA_MEM_HOST;
        // SPDK keys URMA regions by the shared urma_context, so every lane
        // on the same NIC reuses a region registered through lane 0.
        spdk_nvme_urma_memory_region* region = nullptr;
        const int rc = spdk_nvme_urma_register_memory_for_qpair(
            segment->qpair, ptr, size, type, &region);
        if (rc != 0 || !region) {
            LOG(ERROR) << "NoF whole-pool registration failed endpoint="
                       << transport_str << " buffer=" << ptr
                       << " size=" << size << " rc=" << rc;
            return nullptr;
        }
        LOG(INFO) << "NoF whole-pool registered endpoint=" << transport_str
                  << " buffer=" << ptr << " size=" << size;
        return {region, [](void* value) {
                    spdk_nvme_urma_unregister_memory(
                        static_cast<spdk_nvme_urma_memory_region*>(value));
                }};
#else
        (void)transport_str;
        (void)ptr;
        (void)size;
        return nullptr;
#endif
    }

   private:
    // device_buffer == nullptr: classic host submission. Otherwise a
    // classified device buffer submitted through SPDK ext I/O with its
    // memory domain; the ext options live in adaptor->backend_scratch so a
    // queued request can still read them after this returns.
    int SubmitCommon(NofSegmentHandle* handle, void* buffer,
                     uint64_t byte_offset, uint64_t byte_length, NofIOOp op,
                     NofIOAdaptor* adaptor,
                     const NofBufferInfo* device_buffer) {
        if (handle && (handle->failed.load(std::memory_order_acquire) ||
                       handle->released.load(std::memory_order_acquire))) {
            return -ENXIO;  // isolated lane: see MarkLaneFailed
        }
        if (!handle || !buffer || byte_length == 0 || !handle->qpair ||
            !handle->ns || !adaptor || !adaptor->cb) {
            return -EINVAL;
        }
        const uint32_t block_size = spdk_nvme_ns_get_sector_size(handle->ns);
        // Interface precondition, enforced at submit time.
        if (block_size == 0 || byte_offset % block_size != 0 ||
            byte_length % block_size != 0) {
            LOG(ERROR) << "SubmitIO alignment violation: offset=" << byte_offset
                       << " length=" << byte_length
                       << " block_size=" << block_size;
            return -EINVAL;
        }

        const uint64_t lba = byte_offset / block_size;
        const uint32_t lba_count =
            static_cast<uint32_t>(byte_length / block_size);
#ifdef USE_NOF_URMA
        if (device_buffer != nullptr) {
            static_assert(sizeof(spdk_nvme_ns_cmd_ext_io_opts) <=
                              kNofIOAdaptorScratchBytes,
                          "grow kNofIOAdaptorScratchBytes");
            struct spdk_memory_domain* domain =
                NofMemoryDomainManager::GetInstance().GetCudaDomain(
                    device_buffer->device_id);
            if (domain == nullptr) {
                return -ENODEV;  // never fall back to host staging silently
            }
            auto* opts = new (adaptor->backend_scratch)
                spdk_nvme_ns_cmd_ext_io_opts{};
            opts->size = sizeof(*opts);
            opts->memory_domain = domain;
            if (op == NofIOOp::kRead) {
                return spdk_nvme_ns_cmd_read_ext(handle->ns, handle->qpair,
                                                 buffer, lba, lba_count,
                                                 IOCompleteRead, adaptor, opts);
            }
            if (op == NofIOOp::kWrite) {
                return spdk_nvme_ns_cmd_write_ext(
                    handle->ns, handle->qpair, buffer, lba, lba_count,
                    IOCompleteWrite, adaptor, opts);
            }
            return -EINVAL;
        }
#else
        (void)device_buffer;
#endif
        if (op == NofIOOp::kRead) {
            return spdk_nvme_ns_cmd_read(handle->ns, handle->qpair, buffer, lba,
                                         lba_count, IOCompleteRead, adaptor, 0);
        }
        if (op == NofIOOp::kWrite) {
            return spdk_nvme_ns_cmd_write(handle->ns, handle->qpair, buffer,
                                          lba, lba_count, IOCompleteWrite,
                                          adaptor, 0);
        }
        return -EINVAL;
    }

   public:

    int64_t PollCompletion(NofSegmentHandle* handle, uint32_t max_completions) {
        // A failed-but-not-released lane is still polled so already
        // submitted I/O can complete.
        if (handle && handle->released.load(std::memory_order_acquire)) {
            return -ENXIO;
        }
        if (!handle || !handle->qpair) {
            return -EINVAL;
        }
        // max_completions == 0 means "all pending" (SPDK dialect, kept as an
        // explicit interface contract).
        return spdk_nvme_qpair_process_completions(handle->qpair,
                                                   max_completions);
    }

    ErrorCode RegisterMemory(void* ptr, size_t size) {
        if (!ptr || size == 0) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (!EnsureEnv()) {
            return ErrorCode::INTERNAL_ERROR;
        }
#ifdef HAVE_SPDK_MEM_REGISTER
        // A non-aligned ptr is floored to its 2MB page, which assumes the
        // whole containing page is mapped and hugepage-backed — warn loudly
        // rather than fail silently later inside SPDK.
        if (reinterpret_cast<uintptr_t>(ptr) & ((2ULL << 20) - 1)) {
            LOG(WARNING) << "RegisterMemory: ptr " << ptr
                         << " is not 2MB-aligned; registering the whole "
                            "containing 2MB pages, which must be mapped "
                            "and hugepage-backed";
        }
        // Registrations are charged per initiator instance via owner_token_.
        return GetNofRegistryBundle().registry.Register(owner_token_, ptr,
                                                        size);
#else
        LOG(WARNING) << "SPDK build lacks spdk_mem_register; NoF over RDMA "
                        "will fail for unregistered user buffers (#3131)";
        return ErrorCode::OK;
#endif
    }

    ErrorCode UnregisterMemory(void* ptr) {
#ifdef HAVE_SPDK_MEM_REGISTER
        return GetNofRegistryBundle().registry.Unregister(owner_token_, ptr);
#else
        (void)ptr;
        return ErrorCode::OK;
#endif
    }

    bool ProbeSegment(const std::string& tr_str, uint32_t timeout_ms,
                      std::string* error_reason) {
        if (!EnsureEnv()) {
            if (error_reason) {
                *error_reason = "spdk_env_init_fail";
            }
            return false;
        }

        NofSegmentHandle* seg_handle = OpenSegment(tr_str);
        if (!seg_handle) {
            if (error_reason) {
                *error_reason = "open_fail";
            }
            return false;
        }

        uint32_t block_size = GetBlockSize(seg_handle);
        if (block_size == kInvalidBlockSize || block_size == 0) {
            if (error_reason) {
                *error_reason = "invalid_block_size";
            }
            return false;
        }

        ProbeBuffer* probe_buffer =
            GetOrCreateProbeBuffer(tr_str, block_size, error_reason);
        if (!probe_buffer || !probe_buffer->ptr) {
            return false;
        }

        NofProbeContext* probe_ctx = probe_pool_.Acquire();
        // The probe reuses the normal submit path with an embedded adaptor,
        // exactly like the worker pool's pooled sub-tasks.
        probe_ctx->adaptor = {&ProbeReadComplete, probe_ctx};
        probe_ctx->seg = seg_handle;
        int ret = SubmitIO(seg_handle, probe_buffer->ptr, 0, block_size,
                           NofIOOp::kRead, &probe_ctx->adaptor);
        if (ret != 0) {
            // Nothing was submitted, so no completion can arrive.
            probe_pool_.Recycle(probe_ctx);
            if (error_reason) {
                *error_reason = "submit_fail";
            }
            return false;
        }

        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
        while (!probe_ctx->done.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            PollCompletion(seg_handle, 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        const bool done = probe_ctx->done.load(std::memory_order_acquire);
        const bool ok =
            done && probe_ctx->success.load(std::memory_order_acquire);
        if (!ok && error_reason) {
            if (!done) {
                *error_reason = "completion_timeout";
            } else {
                std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
                *error_reason = probe_ctx->error_reason.empty()
                                    ? "completion_error"
                                    : probe_ctx->error_reason;
            }
        }

        if (done) {
            // Recycle only after the caller has read all results, never
            // inside the callback.
            probe_pool_.Recycle(probe_ctx);
        } else {
            // The timed-out command is still in flight and retains
            // &probe_ctx->adaptor: quarantine until the stale callback runs.
            probe_pool_.Quarantine(probe_ctx);
        }
        return ok;
    }

    // ---- lanes ------------------------------------------------------------

    size_t GetLaneCount(NofSegmentHandle* segment) {
        if (!segment) {
            return 0;
        }
        if (!segment->is_segment) {
            return 1;  // a lane handle stands for itself
        }
        EnsureLanes(segment);
        return 1 + segment->extra_lanes.size();
    }

    NofSegmentHandle* GetLane(NofSegmentHandle* segment, size_t index) {
        if (!segment) {
            return nullptr;
        }
        if (index == 0) {
            return segment;
        }
        if (!segment->is_segment) {
            return nullptr;
        }
        EnsureLanes(segment);
        return index - 1 < segment->extra_lanes.size()
                   ? segment->extra_lanes[index - 1].get()
                   : nullptr;
    }

    static bool IsLaneHealthy(const NofSegmentHandle* lane) {
        return lane && !lane->failed.load(std::memory_order_acquire) &&
               !lane->released.load(std::memory_order_acquire);
    }

    static void MarkLaneFailed(NofSegmentHandle* lane) {
        if (lane && !lane->failed.exchange(true, std::memory_order_acq_rel)) {
            LOG(WARNING) << "NoF lane marked failed lane_index="
                         << lane->lane_index;
        }
    }

    // Contract (nvmeof_initiator.h): called on the lane's polling thread
    // after MarkLaneFailed. spdk_nvme_ctrlr_free_io_qpair aborts outstanding
    // requests (their callbacks run synchronously in here); nothing for this
    // lane can complete afterwards.
    static void ReleaseFailedLane(NofSegmentHandle* lane) {
        if (!lane || lane->released.load(std::memory_order_acquire)) {
            return;
        }
        if (!lane->failed.exchange(true, std::memory_order_acq_rel)) {
            LOG(WARNING) << "ReleaseFailedLane on a lane that was not marked "
                            "failed; marking it now lane_index="
                         << lane->lane_index;
        }
        FreeLaneQpair(lane);
        lane->released.store(true, std::memory_order_release);
        LOG(WARNING) << "NoF lane released lane_index=" << lane->lane_index;
    }

   private:
    // Allocates lanes 1..N-1 once per segment. A partial allocation degrades
    // to fewer lanes with a warning instead of failing the segment: lane 0
    // already works.
    void EnsureLanes(NofSegmentHandle* segment) {
        if (segment->lanes_ready.load(std::memory_order_acquire)) {
            return;
        }
        std::lock_guard<std::mutex> lock(segment->lanes_mutex);
        if (segment->lanes_ready.load(std::memory_order_relaxed)) {
            return;
        }
        for (uint32_t i = 1; i < qpairs_per_segment_; ++i) {
            struct spdk_nvme_qpair* qpair =
                spdk_nvme_ctrlr_alloc_io_qpair(segment->ctrlr, nullptr, 0);
            if (!qpair) {
                LOG(WARNING) << "Allocated only " << i << " of "
                             << qpairs_per_segment_
                             << " NoF lanes for a segment; continuing with "
                                "fewer lanes";
                break;
            }
            auto lane = std::make_unique<NofSegmentHandle>();
            lane->qpair = qpair;
            lane->ns = segment->ns;
            lane->ctrlr = segment->ctrlr;
            lane->lane_index = i;
            segment->extra_lanes.push_back(std::move(lane));
        }
        segment->lanes_ready.store(true, std::memory_order_release);
    }

    // ---- admin poll (keep-alive) ------------------------------------------

    void StartAdminPoll() {
        std::lock_guard<std::mutex> lock(admin_poll_mutex_);
        if (admin_poll_started_ || admin_poll_stop_) {
            return;
        }
        admin_poll_started_ = true;
        admin_poll_thread_ = std::thread([this] { AdminPollLoop(); });
    }

    void StopAdminPoll() {
        {
            std::lock_guard<std::mutex> lock(admin_poll_mutex_);
            admin_poll_stop_ = true;
        }
        admin_poll_cv_.notify_all();
        if (admin_poll_thread_.joinable()) {
            admin_poll_thread_.join();
        }
    }

    // Sends keep-alives and drains admin completions (AER, async events) for
    // every attached controller once per second. Takes only ctrlrs_mutex_,
    // which respects the ctrlrs_mutex_ < NvmeCtrlrStateMutex order.
    void AdminPollLoop() {
        std::unique_lock<std::mutex> wait_lock(admin_poll_mutex_);
        while (!admin_poll_stop_) {
            wait_lock.unlock();
            {
                std::lock_guard<std::mutex> lock(ctrlrs_mutex_);
                for (auto& [endpoint, info] : connected_ctrlrs_) {
                    if (!info || !info->ctrlr) {
                        continue;
                    }
                    const int32_t rc =
                        spdk_nvme_ctrlr_process_admin_completions(info->ctrlr);
                    if (rc < 0 && rc != info->last_admin_poll_error) {
                        LOG(WARNING) << "NoF controller admin poll failed "
                                        "endpoint="
                                     << endpoint << " rc=" << rc;
                    }
                    info->last_admin_poll_error = rc < 0 ? rc : 0;
                }
            }
            wait_lock.lock();
            admin_poll_cv_.wait_for(wait_lock, std::chrono::seconds(1),
                                    [this] { return admin_poll_stop_; });
        }
    }

    struct ProbeBuffer {
        void* ptr{nullptr};
        uint32_t size{0};

        ProbeBuffer() = default;
        ProbeBuffer(const ProbeBuffer&) = delete;
        ProbeBuffer& operator=(const ProbeBuffer&) = delete;
        ProbeBuffer(ProbeBuffer&&) = delete;
        ProbeBuffer& operator=(ProbeBuffer&&) = delete;
    };

    bool EnsureEnv() {
        // Locked check-then-set: an unlocked read of a shared_ptr member is
        // a torn-read UB. Cold path, one mutex acquisition is negligible.
        std::lock_guard<std::mutex> lock(env_mutex_);
        if (!env_guard_) {
            env_guard_ = SpdkEnvGuard::Acquire();
        }
        return env_guard_ != nullptr;
    }

    // Status strings are looked up with the opcode that was submitted: SPDK
    // 26.05+ only exposes the `_ext` form (see NofCplStatusString) and uses
    // the opcode to pick command-specific status tables. NofIOAdaptor carries
    // no opcode by design (no per-sub-IO allocation), so each NVM op gets its
    // own trampoline instead of a per-sub-IO record.
    static void IOComplete(void* cb_arg, const struct spdk_nvme_cpl* cpl,
                           uint8_t opc) {
        auto* adaptor = static_cast<NofIOAdaptor*>(cb_arg);
        NofIOCompletion completion;
        if (spdk_nvme_cpl_is_error(cpl)) {
            completion.success = false;
            completion.sc = cpl->status.sc;
            completion.sct = cpl->status.sct;
            completion.error_string = NofCplStatusString(cpl, opc);
        } else {
            completion.success = true;
        }
        adaptor->cb(adaptor->ctx, completion);
    }

    static void IOCompleteRead(void* cb_arg, const struct spdk_nvme_cpl* cpl) {
        IOComplete(cb_arg, cpl, SPDK_NVME_OPC_READ);
    }

    static void IOCompleteWrite(void* cb_arg, const struct spdk_nvme_cpl* cpl) {
        IOComplete(cb_arg, cpl, SPDK_NVME_OPC_WRITE);
    }

    static void ProbeReadComplete(void* ctx, const NofIOCompletion& c) {
        auto* probe_ctx = reinterpret_cast<NofProbeContext*>(ctx);
        if (!c.success) {
            {
                std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
                probe_ctx->error_reason =
                    std::string("completion_error:") + c.error_string;
            }
            probe_ctx->success.store(false, std::memory_order_release);
        } else {
            probe_ctx->success.store(true, std::memory_order_release);
        }
        probe_ctx->done.store(true, std::memory_order_release);
        // No recycle here — see ProbeSegment.
    }

    int ParseTransPortStr(const std::string& tr_str, tr_info* info) {
        std::memset(&info->trid, 0, sizeof(info->trid));
        info->ns = 1;

        if (spdk_nvme_transport_id_parse(&info->trid, tr_str.c_str()) != 0) {
            LOG(ERROR) << "Error parsing transport address";
            return -1;
        }

        std::string ns_prefix = "ns:";
        size_t ns_pos = tr_str.find(ns_prefix);
        if (ns_pos != std::string::npos) {
            size_t ns_start = ns_pos + ns_prefix.length();
            size_t ns_end = tr_str.find_first_of(" \t", ns_start);

            std::string ns_str;
            if (ns_end == std::string::npos) {
                ns_str = tr_str.substr(ns_start);
            } else {
                ns_str = tr_str.substr(ns_start, ns_end - ns_start);
            }

            try {
                info->ns = std::stoul(ns_str);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Failed to parse ns value: " << ns_str
                           << ", error: " << e.what();
                return -1;
            }
        } else {
            // ns: is optional (both forms are accepted, e.g. from Python);
            // info->ns keeps its default of 1.
            LOG(WARNING) << "No ns field found in transport string, "
                            "defaulting to ns=1";
        }

        info->ctrlr_key = std::string(info->trid.traddr) + "|" +
                          std::string(info->trid.trsvcid) + "|" +
                          std::string(info->trid.subnqn) + "|" +
                          std::to_string(static_cast<int>(info->trid.trtype));

        // Per-segment VLOG: opened once per namespace but reached from every
        // caller that resolves an endpoint, so keep it out of the INFO stream.
        VLOG(1) << "traddr:" << info->trid.traddr
                << "trsvcid:" << info->trid.trsvcid << "ns:" << info->ns
                << "subnqn:" << info->trid.subnqn
                << "trtype:" << info->trid.trtype;

        return 0;
    }

    int ConnectController(const struct spdk_nvme_transport_id* trid,
                          ctrlr_info* info) {
        auto probe_cb = [](void* cb_ctx,
                           const struct spdk_nvme_transport_id* trid,
                           struct spdk_nvme_ctrlr_opts* opts) -> bool {
            ApplyCtrlrOptsFromEnv(opts);
            LOG(INFO) << "Attaching to " << trid->traddr << " " << trid->subnqn;
            return true;
        };

        auto attach_cb = [](void* cb_ctx,
                            const struct spdk_nvme_transport_id* trid,
                            struct spdk_nvme_ctrlr* ctrlr,
                            const struct spdk_nvme_ctrlr_opts* opts) {
            LOG(INFO) << "Attached to " << trid->traddr << " " << trid->subnqn;
            ctrlr_info* info = (ctrlr_info*)cb_ctx;
            info->ctrlr = ctrlr;
        };

        // Serialize probes process-wide (see NvmeCtrlrStateMutex): the
        // callbacks only set info->ctrlr and log, so nothing here can
        // re-enter a lock.
        std::lock_guard<std::mutex> nvme_lock(NvmeCtrlrStateMutex());
        const int rc =
            spdk_nvme_probe(trid, (void*)info, probe_cb, attach_cb, NULL);
        if (rc != 0) {
            return rc;
        }
        // Some transport attach failures complete the probe without invoking
        // the attach callback. Treat a missing controller as a failed
        // connection even when the top-level probe return code is zero.
        return info->ctrlr != nullptr ? 0 : -ENODEV;
    }

    ProbeBuffer* GetOrCreateProbeBuffer(const std::string& tr_str,
                                        uint32_t block_size,
                                        std::string* error_reason) {
        std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
        auto& probe_buffer = probe_buffers_[tr_str];
        if (!probe_buffer) {
            probe_buffer = std::make_unique<ProbeBuffer>();
        }

        if (probe_buffer->ptr != nullptr && probe_buffer->size == block_size) {
            return probe_buffer.get();
        }

        if (probe_buffer->ptr != nullptr) {
            spdk_free(probe_buffer->ptr);
            probe_buffer->ptr = nullptr;
            probe_buffer->size = 0;
        }

        probe_buffer->ptr =
            spdk_zmalloc(block_size, 0x1000, nullptr, -1, SPDK_MALLOC_DMA);
        if (!probe_buffer->ptr) {
            if (error_reason) {
                *error_reason = "alloc_fail";
            }
            return nullptr;
        }
        probe_buffer->size = block_size;
        return probe_buffer.get();
    }

    // Bounded grace period for timed-out probes at teardown: SPDK requires a
    // qpair to have no outstanding I/O when freed, and completions are only
    // processed by polling.
    void DrainQuarantinedProbes() {
        const auto stale = probe_pool_.QuarantinedSnapshot();
        if (stale.empty()) {
            return;
        }
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
        bool pending = true;
        while (pending && std::chrono::steady_clock::now() < deadline) {
            pending = false;
            for (auto* ctx : stale) {
                if (ctx->done.load(std::memory_order_acquire) || !ctx->seg) {
                    continue;
                }
                PollCompletion(ctx->seg, 0);
                pending = pending || !ctx->done.load(std::memory_order_acquire);
            }
            if (pending) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        if (pending) {
            LOG(WARNING) << "Timed-out probe command(s) still outstanding at "
                            "initiator teardown; dropped with the qpair";
        }
    }

    // Member order matters: env_guard_ is declared FIRST so it is destroyed
    // LAST — all qpairs/ctrlrs are detached before spdk_env_fini runs.
    std::shared_ptr<SpdkEnvGuard> env_guard_;
    // Process-unique registry owner token (see g_nof_owner_token).
    const uint64_t owner_token_ =
        g_nof_owner_token.fetch_add(1, std::memory_order_relaxed);
    std::mutex env_mutex_;
    std::map<std::string, std::unique_ptr<ctrlr_info>> connected_ctrlrs_;
    std::mutex ctrlrs_mutex_;
    std::map<std::string, std::unique_ptr<ProbeBuffer>> probe_buffers_;
    std::mutex probe_buffers_mutex_;
    NofProbeContextPool probe_pool_;
    // Read once per initiator; validated in SpdkControllerConfig.
    const uint32_t qpairs_per_segment_ =
        SpdkControllerConfig::FromEnvironment().qpairs_per_segment;
    // Admin-poll thread: started on the first attached controller, stopped
    // and joined first thing in ~Impl.
    std::mutex admin_poll_mutex_;
    std::condition_variable admin_poll_cv_;
    bool admin_poll_started_ = false;  // guarded by admin_poll_mutex_
    bool admin_poll_stop_ = false;     // guarded by admin_poll_mutex_
    std::thread admin_poll_thread_;
};

// ---------------------------------------------------------------------------
// SpdkInitiator forwarding
// ---------------------------------------------------------------------------

SpdkInitiator::SpdkInitiator() : impl_(std::make_unique<Impl>()) {}
SpdkInitiator::~SpdkInitiator() = default;

NofSegmentHandle* SpdkInitiator::OpenSegment(const std::string& tr_str) {
    return impl_->OpenSegment(tr_str);
}

bool SpdkInitiator::ProbeSegment(const std::string& tr_str, uint32_t timeout_ms,
                                 std::string* error_reason) {
    return impl_->ProbeSegment(tr_str, timeout_ms, error_reason);
}

uint32_t SpdkInitiator::GetBlockSize(const NofSegmentHandle* handle) {
    return impl_->GetBlockSize(handle);
}

int SpdkInitiator::SubmitIO(NofSegmentHandle* handle, void* buffer,
                            uint64_t byte_offset, uint64_t byte_length,
                            NofIOOp op, NofIOAdaptor* adaptor) {
    return impl_->SubmitIO(handle, buffer, byte_offset, byte_length, op,
                           adaptor);
}

int64_t SpdkInitiator::PollCompletion(NofSegmentHandle* handle,
                                      uint32_t max_completions) {
    return impl_->PollCompletion(handle, max_completions);
}

size_t SpdkInitiator::GetLaneCount(const NofSegmentHandle* segment) const {
    // Lane allocation is lazy and internally synchronized; the handle is
    // owned by this initiator, so dropping const here is sound.
    return impl_->GetLaneCount(const_cast<NofSegmentHandle*>(segment));
}

NofSegmentHandle* SpdkInitiator::GetLane(NofSegmentHandle* segment,
                                         size_t index) {
    return impl_->GetLane(segment, index);
}

bool SpdkInitiator::IsLaneHealthy(const NofSegmentHandle* lane) const {
    return Impl::IsLaneHealthy(lane);
}

void SpdkInitiator::MarkLaneFailed(NofSegmentHandle* lane) {
    Impl::MarkLaneFailed(lane);
}

void SpdkInitiator::ReleaseFailedLane(NofSegmentHandle* lane) {
    Impl::ReleaseFailedLane(lane);
}

NofBufferInfo SpdkInitiator::ResolveBuffer(void* ptr, size_t length) {
    return impl_->ResolveBuffer(ptr, length);
}

int SpdkInitiator::SubmitIOWithBuffer(NofSegmentHandle* handle, void* buffer,
                                      uint64_t byte_offset,
                                      uint64_t byte_length, NofIOOp op,
                                      NofIOAdaptor* adaptor,
                                      const NofBufferInfo& buffer_info) {
    return impl_->SubmitIOWithBuffer(handle, buffer, byte_offset, byte_length,
                                     op, adaptor, buffer_info);
}

std::shared_ptr<void> SpdkInitiator::RegisterMemoryForEndpoint(
    const std::string& transport_str, void* ptr, size_t size) {
    return impl_->RegisterMemoryForEndpoint(transport_str, ptr, size);
}

ErrorCode SpdkInitiator::RegisterMemory(void* ptr, size_t size) {
    return impl_->RegisterMemory(ptr, size);
}

ErrorCode SpdkInitiator::UnregisterMemory(void* ptr) {
    return impl_->UnregisterMemory(ptr);
}

NofCapabilities SpdkInitiator::GetCapabilities() const {
    NofCapabilities caps;
    caps.supports_sgl = false;  // flips when PR #3251's SGL path lands here
    caps.dma_alignment = 4;
    // Upper bound: registration capability exists in this build;
    // not transport-specific. RegisterMemory on TCP NVMe-oF is harmless,
    // callers must not skip it based on this flag.
#ifdef HAVE_SPDK_MEM_REGISTER
    caps.requires_memory_registration = true;
#else
    caps.requires_memory_registration = false;
#endif
    return caps;
}

// ---------------------------------------------------------------------------
// SpdkDmaAllocator
// ---------------------------------------------------------------------------

SpdkDmaAllocator::SpdkDmaAllocator() = default;
SpdkDmaAllocator::~SpdkDmaAllocator() = default;

void* SpdkDmaAllocator::Alloc(size_t size, size_t align, int socket_id) {
    {
        // Locked check-then-set: an unlocked read of a shared_ptr member is
        // a torn-read UB.
        std::lock_guard<std::mutex> lock(env_mutex_);
        if (!env_guard_) {
            env_guard_ = SpdkEnvGuard::Acquire();
            if (!env_guard_) {
                return nullptr;
            }
        }
    }
    return spdk_zmalloc(size, align, nullptr, socket_id, SPDK_MALLOC_DMA);
}

void SpdkDmaAllocator::Free(void* ptr) {
    if (ptr) {
        spdk_free(ptr);
    }
}

}  // namespace mooncake
