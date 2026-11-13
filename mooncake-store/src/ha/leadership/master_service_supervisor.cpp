#include "ha/leadership/master_service_supervisor.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "config/rpc_protocol_config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "ha/leadership/leader_label_reconciler.h"
#include "ha/kv/ha_kv_backend_factory.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_test_failpoint.h"
#include "ha/standby_controller.h"
#include "k8s_lease_helper.h"
#include "ha/master_metrics_reporter.h"
#include "master_admin_service.h"
#include "rpc_service.h"
#include "rpc_worker_pool.h"
#include "shutdown_request.h"

#include "types.h"
#include "cvm/cvm_controller.h"
#include "cvm/cvm_service_delegate.h"
#include "cvm/etcd_view_store.h"
#include "etcd_helper.h"

namespace mooncake {
namespace ha {

namespace {

constexpr auto kLabelReconcileRetryInterval = std::chrono::seconds(1);
constexpr auto kShutdownPollInterval = std::chrono::milliseconds(100);
constexpr auto kSupervisorRetryInterval = std::chrono::seconds(1);
constexpr auto kRenewCheckInterval = std::chrono::seconds(1);
constexpr auto kAcquireRetryInterval = std::chrono::seconds(1);
constexpr char kLeaderLabelKey[] = "mooncake.io/store-role";
constexpr char kLeaderLabelValue[] = "leader";

bool HasPodIdentity(const MasterServiceSupervisorConfig& config) {
    return !config.pod_name.empty() && !config.pod_namespace.empty() &&
           config.ha_backend_type == "k8s";
}

LeaderLabelReconciler MakeLeaderLabelReconciler(
    const MasterServiceSupervisorConfig& config) {
    return LeaderLabelReconciler(
        HasPodIdentity(config),
        [ns = config.pod_namespace, pod = config.pod_name](bool desired) {
            return desired ? K8sLeaseHelper::SetPodLabel(
                                 ns, pod, kLeaderLabelKey, kLeaderLabelValue)
                           : K8sLeaseHelper::ClearPodLabel(ns, pod,
                                                           kLeaderLabelKey);
        },
        kLabelReconcileRetryInterval);
}

std::string ResolveHABackendConnstring(
    const MasterServiceSupervisorConfig& config) {
    return ResolveConfiguredHABackendConnstring(config.ha_backend_type,
                                                config.ha_backend_connstring,
                                                config.etcd_endpoints);
}

tl::expected<HABackendSpec, ErrorCode> BuildHABackendSpec(
    const MasterServiceSupervisorConfig& config) {
    auto backend_type = ParseHABackendType(config.ha_backend_type);
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto availability = ValidateHABackendAvailability(backend_type.value());
    if (availability != ErrorCode::OK) {
        return tl::make_unexpected(availability);
    }

    auto connstring = ResolveHABackendConnstring(config);
    if (connstring.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return HABackendSpec{
        .type = backend_type.value(),
        .connstring = connstring,
        .cluster_namespace = config.cluster_id,
        .leader_lease_ttl_sec = config.ha_leader_lease_ttl_sec,
        // SP930 alias: an explicit --master_view_lease_ttl_sec overrides the
        // shared leader lease TTL; otherwise master.cpp resolves it to the
        // same value, so the default behavior is unchanged.
        .master_view_lease_ttl_sec = config.master_view_lease_ttl_sec,
    };
}

bool IsFatalHABackendError(ErrorCode err) {
    return err == ErrorCode::INVALID_PARAMS ||
           err == ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

// Polls RenewLeadership on a side thread so a long Oplog catch-up cannot
// outlive the lease. Etcd keepalive starts on the first successful renew and
// keeps running after this guard stops; the guard only stops polling.
class LeadershipRenewalGuard {
   public:
    LeadershipRenewalGuard(LeaderCoordinator& coordinator,
                           const LeadershipSession& session)
        : coordinator_(coordinator) {
        worker_ = std::thread([this, &session] {
            bool logged_start = false;
            while (!stop_.load(std::memory_order_acquire)) {
                auto renewed = coordinator_.RenewLeadership(session);
                if (!renewed) {
                    LOG(WARNING)
                        << "Leadership renewal failed during recovery: "
                        << toString(renewed.error());
                    lost_.store(true, std::memory_order_release);
                    return;
                }
                if (!renewed.value()) {
                    LOG(WARNING) << "Leadership lease lost during recovery";
                    lost_.store(true, std::memory_order_release);
                    return;
                }
                if (!logged_start) {
                    logged_start = true;
                    LOG(INFO) << "Leadership keepalive covers oplog catch-up, "
                              << "lease_ttl_sec=" << session.lease_ttl.count();
                }
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait_for(lock, kRenewCheckInterval, [this] {
                    return stop_.load(std::memory_order_acquire);
                });
            }
        });
    }

    ~LeadershipRenewalGuard() { Stop(); }

    LeadershipRenewalGuard(const LeadershipRenewalGuard&) = delete;
    LeadershipRenewalGuard& operator=(const LeadershipRenewalGuard&) = delete;

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_.store(true, std::memory_order_release);
        }
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    bool Lost() const { return lost_.load(std::memory_order_acquire); }

   private:
    LeaderCoordinator& coordinator_;
    std::thread worker_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> lost_{false};
};

// Every shutdown check goes through here. The first true observation
// latches the request, which starts the watchdog deadline before any
// shutdown work, and keeps later checks true even if an injected predicate
// turns false.
bool ShutdownObserved(const std::function<bool()>& shutdown_requested) {
    if (ShutdownLatched()) {
        return true;
    }
    if (!shutdown_requested()) {
        return false;
    }
    LatchShutdownRequest();
    return true;
}

// What the supervisor holds when a shutdown ends the process. session and
// coordinator are set together, only where the term's lease is acquired.
struct ShutdownLease {
    // Set while this process holds the lease; shutdown releases it once.
    const LeadershipSession* session = nullptr;
    LeaderCoordinator* coordinator = nullptr;
    // The term's renewal polling guard, once it exists. It stops before the
    // release so that a poll cannot race the revoke.
    LeadershipRenewalGuard* renewal = nullptr;
    // Result of an ordinary release that already ran for the session.
    std::optional<ErrorCode> prior_release{};
};

// Last step before the terminal exit, after the admin drain. Releases a
// held lease once and returns the exit status: 0 when no lease is left to
// TTL expiry, kHaShutdownReleaseFailedStatus when a release was not
// confirmed.
int FinishShutdown(const ShutdownLease& lease) {
    // An ordinary release already ran for this term. Never repeat it; a
    // second revoke is not a no-op (it costs a round trip of up to 5 s).
    if (lease.prior_release.has_value()) {
        if (*lease.prior_release == ErrorCode::OK) {
            LOG(INFO)
                << "Shutdown complete after leadership release, result=OK";
            return 0;
        }
        LOG(WARNING) << "Shutdown complete after leadership release, result="
                     << toString(*lease.prior_release);
        return kHaShutdownReleaseFailedStatus;
    }
    if (lease.session != nullptr) {
        if (lease.renewal != nullptr) {
            lease.renewal->Stop();
        }
        LOG(INFO) << "Shutdown releasing leadership, view_version="
                  << lease.session->view.view_version << ", renewal_lost="
                  << (lease.renewal != nullptr && lease.renewal->Lost());
        TestFailPoint::Wait("supervisor_before_shutdown_release");
        const ErrorCode err =
            lease.coordinator->ReleaseLeadership(*lease.session);
        if (err == ErrorCode::OK) {
            LOG(INFO) << "Shutdown complete, leadership released, view_version="
                      << lease.session->view.view_version;
            return 0;
        }
        LOG(WARNING) << "Shutdown complete, leadership release failed, result="
                     << toString(err)
                     << "; release not confirmed, any remaining lease expires "
                        "by TTL, view_version="
                     << lease.session->view.view_version;
        return kHaShutdownReleaseFailedStatus;
    }
    LOG(INFO) << "Shutdown complete, no lease held";
    return 0;
}

// Polls the shutdown request while the listener may run and fences the
// serving session through the gate, like a leadership loss.
class ServingShutdownTrigger {
   public:
    ServingShutdownTrigger(const std::function<bool()>& shutdown_requested,
                           std::function<void()> on_shutdown)
        : shutdown_requested_(shutdown_requested),
          on_shutdown_(std::move(on_shutdown)) {
        worker_ = std::thread([this] { Run(); });
    }

    ~ServingShutdownTrigger() { Stop(); }

    ServingShutdownTrigger(const ServingShutdownTrigger&) = delete;
    ServingShutdownTrigger& operator=(const ServingShutdownTrigger&) = delete;

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_.store(true, std::memory_order_release);
        }
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

   private:
    void Run() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!stop_.load(std::memory_order_acquire)) {
            if (ShutdownObserved(shutdown_requested_)) {
                // on_shutdown_ runs with the mutex unlocked on purpose:
                // Stop() must be able to signal and join while the callback
                // is inside server.stop(). If that callback blocks, the
                // join in Stop() waits, and the watchdog is what bounds the
                // whole process, not this interlock.
                lock.unlock();
                on_shutdown_();
                return;
            }
            cv_.wait_for(lock, kShutdownPollInterval, [this] {
                return stop_.load(std::memory_order_acquire);
            });
        }
    }

    const std::function<bool()>& shutdown_requested_;
    std::function<void()> on_shutdown_;
    std::thread worker_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_{false};
};

void LogLeadershipReleaseWarning(std::string_view context, ErrorCode err) {
    if (err == ErrorCode::OK) {
        return;
    }
    LOG(WARNING) << "Failed to release leadership after " << context << ": "
                 << toString(err);
}

bool HandleSupervisorError(std::string_view action, ErrorCode err,
                           HABackendType backend_type) {
    if (IsFatalHABackendError(err)) {
        LOG(ERROR) << "Failed to " << action << ": " << toString(err)
                   << ", backend_type=" << HABackendTypeToString(backend_type);
        return true;
    }

    LOG(WARNING) << "Failed to " << action << ": " << toString(err)
                 << ", backend_type=" << HABackendTypeToString(backend_type)
                 << ", retrying in " << kSupervisorRetryInterval.count() << "s";
    std::this_thread::sleep_for(kSupervisorRetryInterval);
    return false;
}

bool HandleLeadershipPhaseError(std::string_view release_context,
                                std::string_view action, ErrorCode release_err,
                                ErrorCode err, HABackendType backend_type) {
    LogLeadershipReleaseWarning(release_context, release_err);
    return HandleSupervisorError(action, err, backend_type);
}

tl::expected<bool, ErrorCode> WarmupLeadership(
    LeaderCoordinator& coordinator, const LeadershipSession& session,
    const std::function<bool()>& shutdown_requested) {
    const auto deadline = std::chrono::steady_clock::now() + session.lease_ttl;
    const auto max_sleep_interval =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            kRenewCheckInterval);

    while (true) {
        // The caller checks the request first and exits on a shutdown.
        if (ShutdownObserved(shutdown_requested)) {
            return true;
        }
        auto renewed = coordinator.RenewLeadership(session);
        if (!renewed) {
            return tl::make_unexpected(renewed.error());
        }
        if (!renewed.value()) {
            return false;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return true;
        }

        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                                  now);
        const auto sleep_interval =
            remaining < max_sleep_interval ? remaining : max_sleep_interval;
        std::this_thread::sleep_for(sleep_interval);
    }
}

void SetRuntimeState(MasterAdminServer& admin_server,
                     MasterRuntimeState state) {
    admin_server.SetRuntimeState(state);
    LOG(INFO) << "Master runtime state -> " << MasterRuntimeStateToString(state)
              << ", role=" << MasterRuntimeRoleToString(state);
}

void ActivateServingState(MasterAdminServer& admin_server,
                          const std::shared_ptr<WrappedMasterService>& service,
                          LeaderLabelReconciler& label_reconciler) {
    admin_server.SetServiceDelegate(service);
    admin_server.SetServiceAvailable(true);
    SetRuntimeState(admin_server, MasterRuntimeState::kServing);
    label_reconciler.SetLeader(true);
    // RFC #4700 slice 1: the promotion restore already moved the service
    // Constructed -> Recovering -> Ready; this is the Ready -> Serving
    // edge, taken only after readiness is published.
    service->ActivateServing();
}

void DeactivateServingState(MasterAdminServer& admin_server,
                            LeaderLabelReconciler& label_reconciler) {
    admin_server.SetServiceAvailable(false);
    admin_server.SetServiceDelegate(nullptr);
    label_reconciler.SetLeader(false);
}

void StopLeadershipMonitor(
    std::unique_ptr<LeadershipMonitorHandle>& monitor) {
    if (!monitor) {
        return;
    }
    monitor->Stop();
    monitor.reset();
}

void UpdateObservedLeader(MasterAdminServer& admin_server,
                          StandbyController& standby_controller,
                          const std::optional<MasterView>& leader_view,
                          const MasterSources& sources) {
    admin_server.SetObservedLeader(leader_view);
    standby_controller.UpdateObservedLeader(sources);
}

// Single-leader form: derive the source set from the observed view.
void UpdateObservedLeader(MasterAdminServer& admin_server,
                          StandbyController& standby_controller,
                          const std::optional<MasterView>& leader_view) {
    UpdateObservedLeader(admin_server, standby_controller, leader_view,
                         ha::SingleLeaderSources(leader_view));
}

void ApplyCurrentView(MasterAdminServer& admin_server,
                      StandbyController& standby_controller,
                      const ViewChangeResult& wait_result) {
    if (!wait_result.current_view.has_value() &&
        (wait_result.changed || wait_result.timed_out)) {
        return;
    }
    UpdateObservedLeader(admin_server, standby_controller,
                         wait_result.current_view);
}

// Forward declarations: definitions live below the CVM membership bridge.
std::optional<MasterView> BuildCvmObservedLeader(
    const std::unique_ptr<cvm::CvmController>& cvm_controller);
MasterSources BuildCvmObservedSources(
    const std::unique_ptr<cvm::CvmController>& cvm_controller);

ErrorCode ClaimProducerViewForServing(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config,
    ViewVersionId view_version) {
    if (!config.enable_oplog || config.cluster_id.empty()) {
        return ErrorCode::OK;
    }
    if (!HaBackendSupportsOpLog(spec.type)) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (view_version == 0) {
        return ErrorCode::INVALID_PARAMS;
    }

    // Fence the previous producer through the backend selected by spec, not
    // etcd alone. Redis already implements the transactional compare-and-swap
    // ClaimProducerView needs, so an old Redis primary must be fenced too.
    auto backend = CreateHaKvBackend(spec);
    if (!backend) {
        return backend.error();
    }
    OpLogBatchStorage storage(config.cluster_id, *backend.value());
    return storage.ClaimProducerView(view_version);
}

void EnterStandbyMode(
    MasterAdminServer& admin_server, StandbyController& standby_controller,
    std::atomic<bool>& accept_runtime_updates,
    const std::optional<MasterView>& leader_view,
    const std::unique_ptr<cvm::CvmController>& cvm_controller = {});

void EnterStandbyMode(
    MasterAdminServer& admin_server, StandbyController& standby_controller,
    std::atomic<bool>& accept_runtime_updates,
    const std::optional<MasterView>& leader_view,
    const std::unique_ptr<cvm::CvmController>& cvm_controller) {
    const MasterSources sources = cvm_controller
                                      ? BuildCvmObservedSources(cvm_controller)
                                      : ha::SingleLeaderSources(leader_view);
    UpdateObservedLeader(admin_server, standby_controller, leader_view,
                         sources);
    auto err = standby_controller.StartStandby(sources);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to start standby replication: "
                     << toString(err);
        SetRuntimeState(admin_server, MasterRuntimeState::kStandby);
        return;
    }
    SetRuntimeState(admin_server, standby_controller.GetStandbyRuntimeState());
}

// Bridges CvmController's membership role decisions to the supervisor's
// serving/standby state machine. CvmController::MembershipLoop runs on its own
// thread and calls OnRoleChanged; this class persists the role back to etcd
// (so slot partitioning immediately excludes demoted nodes) and publishes the
// change to the supervisor main loop. It never mutates the supervisor state
// machine directly: on demotion it only invokes a lightweight stop-server
// signal installed by the serve phase, and the main loop performs the full
// downgrade sequence serially after the server unblocks.
class CvmMembershipCoordinator : public cvm::CvmServiceDelegate {
   public:
    CvmMembershipCoordinator(std::string cluster_namespace,
                             std::string master_id)
        : cluster_namespace_(std::move(cluster_namespace)),
          master_id_(std::move(master_id)) {}

    void SetLeaseId(EtcdLeaseId lease_id) {
        lease_id_.store(lease_id, std::memory_order_relaxed);
    }

    // Installed by the serve phase; cleared (empty) when not serving. The
    // signal only stops the coro_rpc server so the serve phase unblocks; the
    // main loop then reads CvmController::GetCurrentRole() and performs the
    // full downgrade sequence.
    void SetServeStopSignal(std::function<void()> signal) {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_serve_signal_ = std::move(signal);
    }

    // Blocks until either a role change or a membership (member set) change is
    // pending, then clears both so each change is consumed exactly once. The
    // caller re-reads CvmController::GetCurrentRole() as the source of truth
    // after waking; a standby uses the wake-up to re-bind its replay sources.
    void WaitForRoleOrMembershipChange() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, kShutdownPollInterval, [this] {
            return has_pending_role_ || has_pending_membership_;
        });
        has_pending_role_ = false;
        has_pending_membership_ = false;
    }

    void OnSlotAcquired(uint16_t /*slot*/) override {}
    void OnSlotReleased(uint16_t /*slot*/) override {}

    void OnMembershipChanged() override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            has_pending_membership_ = true;
        }
        cv_.notify_all();
    }

    void OnRoleChanged(cvm::MasterRole new_role) override {
        const EtcdLeaseId lease_id = lease_id_.load(std::memory_order_relaxed);
        ErrorCode err = cvm::EtcdViewStore::UpdateMasterRole(
            cluster_namespace_, master_id_, new_role, lease_id);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "CvmMembershipCoordinator: failed to persist role="
                         << static_cast<int32_t>(new_role)
                         << " for master_id=" << master_id_ << ": " << err;
        }
        LOG(INFO) << "CvmMembershipCoordinator::OnRoleChanged: master_id="
                  << master_id_ << ", role=" << static_cast<int32_t>(new_role)
                  << ", lease_id=" << lease_id;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            has_pending_role_ = true;
        }
        cv_.notify_all();

        if (new_role == cvm::MasterRole::kStandby) {
            std::function<void()> signal;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                signal = stop_serve_signal_;
            }
            if (signal) {
                signal();
            }
        }
    }

   private:
    std::string cluster_namespace_;
    std::string master_id_;
    std::atomic<EtcdLeaseId> lease_id_{0};
    std::mutex mutex_;
    std::condition_variable cv_;
    bool has_pending_role_{false};
    bool has_pending_membership_{false};
    std::function<void()> stop_serve_signal_;
};

// Builds the representative single-leader view for the admin surface. In the
// multi-submaster model there is no single "leader"; this reports the earliest
// primary for display only. Returns nullopt when no primary is available yet
// (or this node is the first primary itself, so there is no upstream).
std::optional<MasterView> BuildCvmObservedLeader(
    const std::unique_ptr<cvm::CvmController>& cvm_controller) {
    if (!cvm_controller) {
        return std::nullopt;
    }
    const std::string primary = cvm_controller->GetPrimaryAddress();
    if (primary.empty()) {
        return std::nullopt;
    }
    MasterView view;
    view.leader_address = primary;
    view.view_version = 0;  // 无持久化 kv_view；版本不再用于路由。
    return view;
}

// Builds the standby replay source set from the CVM membership ranking. Under
// dynamic binding (2c) a standby only follows the primary(s) that own the slot
// range it is responsible for, instead of following every primary (2b).
MasterSources BuildCvmObservedSources(
    const std::unique_ptr<cvm::CvmController>& cvm_controller) {
    MasterSources sources;
    if (!cvm_controller) {
        return sources;
    }
    for (const auto& member : cvm_controller->GetBindingSources()) {
        sources.push_back(MasterSource{member.master_id, member.address});
    }
    return sources;
}

// ── 环互备双角色回放运行时（互备 1.3.2）──
// serve phase 内本机在 serving 自己 rank 的同时，低频拉取（5s）
// CvmController::GetRingStandbySources（互备 1.3.1）跟随被指派 rank 的
// OpLog 流。与待命池独占模式的 standby_controller 完全独立：
//   - 生命周期不相交：standby 模式用待命池控制器（EnterStandbyMode），
//     serve phase 用本运行时；职责集不相交：phase3 只配未归属成员，
//     JoinAsStandbyRing（互备 1.1）只配已归属成员。
//   - 职责集空 → StopStandby；非空未跟随 → StartStandby；集合变化 →
//     再次 StartStandby（具体实现为动态重绑 UpdateSources：幂等，未运行
//     等价 Start、源集合不变 no-op、变化 Stop+Start）；未变 → 静默
//    （5s 轮询无逐周期日志，防刷屏）。
//   - 起停/重绑仅变更时各一条 INFO；StartStandby 失败 60s 节流 WARNING。
//   - 开关源：CvmController::StandbyMutualPairingEnabled（与互备 1.1
//     JoinAsStandbyRing 门控共用同一进程级缓存值，杜绝配对记录与回放
//     运行时失配）。OFF 时 Start 不启动线程，行为与现状等价。
//   - 线程模型：自持线程串行调用 GetRingStandbySources / StartStandby /
//     StopStandby；主线程仅在 Start/Stop 生命周期边界介入（Stop 先
//     join 线程再停跟随，天然串行）。
//   - 互备 1.4（staging 领养编排）：同 Loop 每 poll 增量执行
//     OrchestrateRingAdoptions——检测 ring-adopt staging rank
//    （FindStagedRingAdoptions）→ 经 wrapped service 物化死 primary 的
//     OpLog 流（AdoptMaterializeFromDeadOwner，P4 过滤幂等）→ CAS
//     kMigrating→kStable 收尾（FinalizeRingAdoption）。稳态 staging 集
//     为空零动作；1.4 全链路（配对→领养→物化→收尾）就绪前开关仍保持
//     OFF。
class RingStandbyRuntime {
   public:
    RingStandbyRuntime(const HABackendSpec& spec,
                       const MasterServiceSupervisorConfig& config,
                       cvm::CvmController* cvm_controller)
        : spec_(spec), config_(config), cvm_controller_(cvm_controller) {}

    ~RingStandbyRuntime() { Stop(); }

    // 单线程生命周期契约：Start/Stop/析构均由 supervisor 主循环同一线程
    // 串行调用（serve 就绪 → Start，降级序列 → Stop，RunSupervisorLoop
    // 退出 → 析构）。mutex_ 仅防御重入/竞态下的状态位一致性，并非为
    // 并发 Start/Stop 设计——thread_ 的赋值与 join 因此无需在锁内。
    // Loop 线程从不在运行期触碰 mutex_（只读 stop_ 原子），故无锁序死锁。
    void Start() {
        if (cvm_controller_ == nullptr ||
            !cvm::CvmController::StandbyMutualPairingEnabled()) {
            return;  // 无 CVM 成员身份 / 开关 OFF：no-op（与现状等价）
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (running_) {
                return;
            }
            running_ = true;
            stop_ = false;
        }
        // 线程创建失败（资源耗尽等）：复位 running_ 防状态失真——否则
        // 后续 Start 静默 no-op、Stop 因 thread_ 非 joinable 跳过 join，
        // running_ 永久卡 true。当前调用方对返回值不敏感（下个 serve
        // 周期重试），故仅吞掉异常并告警。
        try {
            thread_ = std::thread(&RingStandbyRuntime::Loop, this);
        } catch (const std::system_error& e) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                running_ = false;
            }
            LOG(WARNING) << "RingStandbyRuntime: thread creation failed: "
                         << e.what() << " (ring following disabled this cycle)";
        }
    }

    // 幂等；join 线程后统一停跟随（保证控制器调用串行）。
    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_) {
                return;
            }
            running_ = false;
            stop_ = true;
        }
        if (thread_.joinable()) {
            thread_.join();
        }
        if (standby_controller_ && following_.load()) {
            standby_controller_->StopStandby();
            following_.store(false);
            LOG(INFO) << "RingStandbyRuntime: stopped ring following on "
                         "serve exit";
        }
    }

    // 互备 1.4：注入 serve phase 的 wrapped service（物化编排经其安全
    // API AdoptMaterializeFromDeadOwner 进入 MasterService）。单线程生命
    // 周期契约同 Start/Stop：每 serve phase 在 Start 前注入新实例、Stop
    // （已 join Loop 线程）后清空，Loop 线程运行期指针恒有效。
    void SetWrappedMasterService(WrappedMasterService* service) {
        wrapped_service_ = service;
    }

   private:
    // 源集合指纹（master_id@address，排序后逐元素比较）：排序消除 rank
    // 扫描序影响；address 入指纹覆盖同身份换地址的动态重绑。
    static std::vector<std::pair<std::string, std::string>> SignatureOf(
        const MasterSources& sources) {
        std::vector<std::pair<std::string, std::string>> sig;
        sig.reserve(sources.size());
        for (const auto& s : sources) {
            sig.emplace_back(s.master_id, s.address);
        }
        std::sort(sig.begin(), sig.end());
        return sig;
    }

    static std::string JoinSignature(
        const std::vector<std::pair<std::string, std::string>>& sig) {
        std::string joined;
        for (const auto& [id, addr] : sig) {
            if (!joined.empty()) {
                joined += ",";
            }
            joined += id + "@" + addr;
        }
        return joined;
    }

    void Loop() {
        std::vector<std::pair<std::string, std::string>> last_signature;
        // 同步健康观测（1.3.4）：kRecovering 覆盖 FAILED/RECONNECTING/
        // RECOVERING/SYNCING。连续 2 个 poll（10s）仍劣化才告警（排除
        // 重绑后的正常追赶），再 60s 节流；仅 Loop 线程访问。
        int unhealthy_streak = 0;
        int64_t last_health_warn_ms = 0;
        while (!stop_.load(std::memory_order_relaxed)) {
            const auto duties = cvm_controller_->GetRingStandbySources();
            MasterSources sources;
            sources.reserve(duties.size());
            for (const auto& duty : duties) {
                sources.push_back(MasterSource{duty.master_id, duty.address});
            }
            const auto signature = SignatureOf(sources);
            const bool changed = signature != last_signature;
            const bool following = following_.load();
            if (sources.empty()) {
                if (following) {
                    // 职责集空（重指派/晋升完成/降配）：停跟随。controller
                    // 必已创建（following 为真）。
                    standby_controller_->StopStandby();
                    following_.store(false);
                    LOG(INFO) << "RingStandbyRuntime: ring duty set empty, "
                                 "stopped following";
                }
                unhealthy_streak = 0;
            } else {
                // 懒创建：首个非空职责集到达才建控制器（etcd 连接等）。
                if (!standby_controller_) {
                    standby_controller_ =
                        CreateStandbyController(spec_, config_);
                }
                // 跟随期每 poll 调和 StartStandby（1.3.3 核验发现的缺陷
                // 修复）：服务转入 FAILED/STOPPED 时同步回调会把控制器
                // 内部 standby_running_ 静默置 false（本运行时的
                // following_ 无从得知），职责集不变则永远不会再触发
                // StartStandby → 跟随卡死（池模式有成员变动唤醒
                // EnterStandbyMode 兜底，环运行时无此兜底）。每 poll
                // 无条件调用：源未变且运行中 → 内部 UpdateSources 幂等
                // no-op（零开销）；已停 → 走全量 Start 分支自愈。
                const ErrorCode err =
                    standby_controller_->StartStandby(sources);
                if (err == ErrorCode::OK) {
                    const bool started_transition = !following || changed;
                    following_.store(true);
                    if (started_transition) {
                        LOG(INFO) << "RingStandbyRuntime: following "
                                  << sources.size() << " ring source(s) ["
                                  << JoinSignature(signature) << "]";
                    }
                    // 健康检查（1.3.4）：GetStandbyRuntimeState 读控制器
                    // 最后同步状态（锁保护，开销一次锁获取）。
                    const MasterRuntimeState state =
                        standby_controller_->GetStandbyRuntimeState();
                    if (state == MasterRuntimeState::kRecovering) {
                        ++unhealthy_streak;
                        const int64_t now_ms =
                            std::chrono::duration_cast<
                                std::chrono::milliseconds>(
                                std::chrono::steady_clock::now()
                                    .time_since_epoch())
                                .count();
                        if (unhealthy_streak >= 2 &&
                            now_ms - last_health_warn_ms >= 60000) {
                            last_health_warn_ms = now_ms;
                            LOG(WARNING)
                                << "RingStandbyRuntime: sync "
                                   "degraded (state="
                                << static_cast<int>(state)
                                << ", sources=" << JoinSignature(signature)
                                << ")";
                        }
                    } else {
                        unhealthy_streak = 0;
                    }
                } else {
                    // 失败按周期静默重试；60s 节流一条 WARNING。
                    unhealthy_streak = 0;
                    const int64_t now_ms =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
                    if (now_ms - last_warn_ms_ >= 60000) {
                        last_warn_ms_ = now_ms;
                        LOG(WARNING)
                            << "RingStandbyRuntime: StartStandby "
                               "failed, err="
                            << toString(err) << " (retrying each poll)";
                    }
                }
            }
            last_signature = signature;
            // 互备 1.4：领养编排（staging 检测 → 物化 → 收尾），稳态零动作。
            OrchestrateRingAdoptions();
            // 5s 低频拉取；100ms 粒度可中断睡眠保证降级时及时退出。
            for (int i = 0; i < 50 && !stop_.load(std::memory_order_relaxed);
                 ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    // 互备 1.4：环互备领养编排（staging-first ②③）。检测 ring-adopt
    // staging rank（FindStagedRingAdoptions，签名 = primary=本机且
    // kMigrating 且 migrating_to 为空，与 reshard 阶段 1 签名不相交）
    // → 物化（WrappedMasterService::AdoptMaterializeFromDeadOwner：重放
    // 死 primary 的 OpLog 流，P4 owned-slot 过滤只落本机 slot，已定局
    // 幂等短路）→ CAS kMigrating→kStable 收尾（FinalizeRingAdoption），
    // 写屏障解除后领养 slot 随下轮心跳 ImportSlotMetadata→MarkSlotsReady
    // 自然就绪。三态决策（详见 AdoptMaterializeFromDeadOwner 契约）：
    //   OK                → 收尾（INFO 一条）；
    //   INCOMPLETE（预算耗尽，瞬态）→ 保持屏障下轮重试（60s 节流告警）；
    //   其余 FATAL/UNAVAILABLE（越窗 / prefix 缺失 / OpLog 未启用）→
    //     按既有冷接管回退语义（判空、客户端冷重建）收尾 + WARNING。
    // dead_primary_id 为空（领养者在 staging→收尾窗口内重启，进程内源
    // 记录丢失）：保持写屏障 fail-safe（宁可冻结，不可空元数据 serve），
    // 60s 节流 ERROR 供人工处置（admin reshard 重建 / 手工 CAS）。
    void OrchestrateRingAdoptions() {
        if (cvm_controller_ == nullptr) {
            return;
        }
        const auto staged = cvm_controller_->FindStagedRingAdoptions();
        if (staged.empty()) {
            return;  // 稳态：零动作（视图快照为锁内拷贝，无 etcd 读）
        }
        const int64_t now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
        for (const auto& adoption : staged) {
            if (adoption.dead_primary_id.empty()) {
                if (now_ms - last_adoption_warn_ms_ >= 60000) {
                    last_adoption_warn_ms_ = now_ms;
                    LOG(ERROR) << "RingStandbyRuntime: staged ring adoption "
                                  "has no recorded dead primary (adopter "
                                  "restarted in staging window?): rank="
                               << adoption.rank
                               << ", writes fenced until manual resolution";
                }
                continue;
            }
            if (wrapped_service_ == nullptr) {
                continue;  // 非 serve 窗口（防御；Loop 生命周期与 serve
                           // phase 同步，正常不可达）
            }
            bool stream_present = false;
            const ErrorCode err =
                wrapped_service_->AdoptMaterializeFromDeadOwner(
                    adoption.dead_primary_id, stream_present);
            if (err == ErrorCode::INCOMPLETE_OPLOG_CATCH_UP) {
                // 瞬态（30s 重放预算耗尽，etcd 抖动）：保持屏障下轮重试。
                if (now_ms - last_adoption_warn_ms_ >= 60000) {
                    last_adoption_warn_ms_ = now_ms;
                    LOG(WARNING) << "RingStandbyRuntime: adoption replay "
                                    "budget exhausted (retrying): rank="
                                 << adoption.rank
                                 << ", source=" << adoption.dead_primary_id;
                }
                continue;
            }
            if (err != ErrorCode::OK) {
                LOG(WARNING)
                    << "RingStandbyRuntime: adoption replay "
                       "unrecoverable, finalizing with cold-rebuild "
                       "fallback: rank="
                    << adoption.rank << ", source=" << adoption.dead_primary_id
                    << ", err=" << toString(err);
            }
            const ErrorCode fin = cvm_controller_->FinalizeRingAdoption(
                adoption.rank, adoption.epoch, adoption.dead_primary_id);
            if (fin == ErrorCode::OK) {
                LOG(INFO) << "RingStandbyRuntime: ring adoption complete: "
                             "rank="
                          << adoption.rank
                          << ", dead_primary=" << adoption.dead_primary_id
                          << ", stream_present="
                          << (stream_present ? "true" : "false");
            } else if (fin != ErrorCode::STALE_ROUTE) {
                // STALE = 并发推进（他方收尾 / 管理员操作），下轮检测自然
                // 离开 staging 集，静默；其余失败保持屏障下轮重试（物化
                // 已定局，重试仅重走幂等短路 + CAS）。
                LOG(WARNING) << "RingStandbyRuntime: adoption finalize "
                                "failed (retrying): rank="
                             << adoption.rank << ", err=" << toString(fin);
            }
        }
    }

    // supervisor 局部变量的引用/裸指针：声明序保证先于本对象析构。
    const HABackendSpec& spec_;
    const MasterServiceSupervisorConfig& config_;
    cvm::CvmController* cvm_controller_;
    // serve phase 注入（SetWrappedMasterService）：物化编排入口，Loop
    // 线程只读。生命周期见 setter 注释。
    WrappedMasterService* wrapped_service_ = nullptr;
    // 懒创建的独立控制器实例（与待命池 standby_controller 无共享）。
    std::unique_ptr<StandbyController> standby_controller_;
    std::thread thread_;
    std::mutex mutex_;  // 仅保护 Start/Stop 生命周期边界
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{false};
    std::atomic<bool> following_{false};
    int64_t last_warn_ms_ = 0;           // 仅 Loop 线程访问
    int64_t last_adoption_warn_ms_ = 0;  // 仅 Loop 线程访问（1.4 领养告警节流）
};

int RunSupervisorLoop(const HABackendSpec& spec,
                      const MasterServiceSupervisorConfig& config,
                      MasterAdminServer& admin_server,
                      std::unique_ptr<StandbyController> standby_controller,
                      const std::function<bool()>& shutdown_requested) {
                          // ── Metrics reporter (writes master capacity/usage
                          // to HA backend) ──
                          MasterMetricsReporter::Config reporter_config;
                          reporter_config.enabled =
                              config.enable_metrics_report_to_backend;
                          reporter_config.report_interval_sec =
                              config.metrics_report_interval_sec;
                          reporter_config.lease_ttl_sec =
                              config.metrics_report_lease_ttl_sec;
                          reporter_config.master_id =
                              UuidToString(generate_uuid());
                          reporter_config.local_hostname =
                              config.local_hostname;
                          reporter_config.cluster_namespace = config.cluster_id;
                          reporter_config.ha_backend_connstring =
                              ResolveHABackendConnstring(config);
                          MasterMetricsReporter metrics_reporter(
                              reporter_config);

                          auto label_reconciler =
                              MakeLeaderLabelReconciler(config);
                          label_reconciler.SetLeader(false);
                          SetRuntimeState(admin_server,
                                          MasterRuntimeState::kStarting);
                          std::atomic<bool> accept_standby_runtime_updates{
                              false};
                          standby_controller->SetStandbyRuntimeStateCallback(
                              [&](MasterRuntimeState state) {
                                  if (!accept_standby_runtime_updates.load(
                                          std::memory_order_acquire)) {
                                      return;
                                  }
                                  SetRuntimeState(admin_server, state);
                              });

                          // Every supervisor-selected shutdown ends here
                          // without unwinding. Returning would destroy the
                          // per-iteration coordinator, and the etcd
                          // coordinator's destructor revokes a held lease.
                          auto shutdown_exit =
                              [&](std::string_view phase,
                                  const ShutdownLease& lease) -> int {
                              LOG(INFO)
                                  << "Shutdown requested, finishing from phase="
                                  << phase;
                              admin_server.Stop();
                              accept_standby_runtime_updates.store(
                                  false, std::memory_order_release);
                              standby_controller->StopStandby();
                              ExitAfterShutdown(FinishShutdown(lease));
                          };
                          // Result of the ordinary release that ended the
                          // previous term, for a request observed before the
                          // next standby check.
                          std::optional<ErrorCode> released_last_term;

                          if (ShutdownObserved(shutdown_requested)) {
                              return shutdown_exit("starting", ShutdownLease{});
                          }
                          // ── CVM membership (quota coordination) ──
                          // The CvmController owns the etcd lease, master
                          // registration, view snapshot aggregation and the
                          // membership loop that decides this node's role
                          // (primary vs standby) via first-come-first-served
                          // ranking. It lives here (supervisor layer) so
                          // membership keeps running whether or not this node
                          // is currently serving; MasterService only publishes
                          // slot ownership. NOTE: the coordinator is declared
                          // before the controller so it is destroyed after it —
                          // the controller's Stop() joins the membership
                          // thread, guaranteeing no OnRoleChanged callback is
                          // in flight when the coordinator is torn down.
                          CvmMembershipCoordinator cvm_membership_coordinator(
                              config.cluster_id, config.local_hostname);
                          std::unique_ptr<cvm::CvmController> cvm_controller;
                          if (spec.type == HABackendType::ETCD &&
                              !config.local_hostname.empty() &&
                              !config.cluster_id.empty()) {
                              ErrorCode connect_err =
                                  EtcdHelper::ConnectToEtcdStoreClient(
                                      ResolveHABackendConnstring(config));
                              if (connect_err != ErrorCode::OK) {
                                  LOG(WARNING) << "CVM membership disabled: "
                                                  "failed to connect etcd: "
                                               << connect_err;
                              } else {
                                  cvm::CvmController::Config cc_config;
                                  cc_config.cluster_namespace =
                                      config.cluster_id;
                                  cc_config.master_id = config.local_hostname;
                                  cc_config.address = config.local_hostname;
                                  // Start as standby; the membership loop
                                  // promotes to primary when this node ranks
                                  // within the submaster quota.
                                  cc_config.role = cvm::MasterRole::kStandby;
                                  cc_config.http_port = config.cvm_http_port;
                                  cc_config.http_host = config.cvm_http_host;
                                  cc_config.submaster_count =
                                      config.submaster_count;
                                  cvm_controller =
                                      std::make_unique<cvm::CvmController>(
                                          std::move(cc_config));
                                  cvm_controller->SetDelegate(
                                      &cvm_membership_coordinator);
                                  ErrorCode cc_err = cvm_controller->Start();
                                  if (cc_err != ErrorCode::OK) {
                                      LOG(WARNING)
                                          << "Failed to start CvmController: "
                                          << cc_err;
                                      cvm_controller.reset();
                                  } else {
                                      cvm_membership_coordinator.SetLeaseId(
                                          cvm_controller->GetLeaseId());
                                      LOG(INFO)
                                          << "Started supervisor-owned "
                                             "CvmController: master_id="
                                          << config.local_hostname
                                          << ", cluster_namespace="
                                          << config.cluster_id << ", lease_id="
                                          << cvm_controller->GetLeaseId()
                                          << ", submaster_count="
                                          << config.submaster_count;
                                  }
                              }
                          }

                          // 环互备双角色回放运行时（互备 1.3.2）：serve phase
                          // 内跟随被指派 rank 的流。声明序在 cvm_controller
                          // 之后 → 析构先于它（裸指针 安全）；开关 OFF / 无 CVM
                          // 时 Start/Stop 均为 no-op。
                          if (config.submaster_count > 1 && !cvm_controller) {
                              LOG(ERROR) << "Multi-submaster mode requires a running "
                                            "CVM controller; refusing single-leader fallback";
                              return -1;
                          }

                          RingStandbyRuntime ring_standby_runtime(
                              spec, config, cvm_controller.get());

                          // standby 覆盖率 provider 的共享生命周期状态：metric
                          // 线程（拷贝的 provider 闭包）与退出路径（guard
                          // 析构）经互斥互达——provider 要么 先于 guard 完成
                          // live etcd 调用，要么看到 alive=false 直接返回
                          // unavailable，消除 cvm_controller
                          // 悬垂窗口（admin_server 生命周期
                          // 长于本函数作用域，RunSupervisorLoop 不调用其
                          // Stop）。
                          struct StandbyCoverageSharedState {
                              std::mutex mutex;
                              bool alive = false;
                              cvm::CvmController* controller = nullptr;
                          };
                          std::shared_ptr<StandbyCoverageSharedState>
                              coverage_state;
                          if (cvm_controller) {
                              coverage_state = std::make_shared<
                                  StandbyCoverageSharedState>();
                              coverage_state->controller = cvm_controller.get();
                              coverage_state->alive = true;
                              const auto state_ptr = coverage_state;
                              // P2-7-C（§16.18.2 满配缺口可观测化）：admin
                              // metrics 线程按
                              // kMetricReportIntervalSeconds（10s）低频拉取，不走心跳路径。
                              admin_server.SetStandbyCoverageProvider(
                                  [state_ptr]() -> std::string {
                                      std::lock_guard<std::mutex> lock(
                                          state_ptr->mutex);
                                      if (!state_ptr->alive ||
                                          !state_ptr->controller) {
                                          return "unavailable";
                                      }
                                      return state_ptr->controller
                                          ->GetStandbyCoverageSummary();
                                  });
                          }
                          // RAII guard 声明于 cvm_controller 与 coverage_state
                          // 之后 → 析构先于 二者：任意 return 路径都先摘
                          // provider、置 alive=false，再销毁 controller，杜绝
                          // metric 线程在 controller 析构期间/之后解引用。
                          struct StandbyCoverageProviderGuard {
                              MasterAdminServer& admin;
                              std::shared_ptr<StandbyCoverageSharedState> state;
                              ~StandbyCoverageProviderGuard() {
                                  if (state) {
                                      {
                                          std::lock_guard<std::mutex> lock(
                                              state->mutex);
                                          state->alive = false;
                                          state->controller = nullptr;
                                      }
                                      admin.SetStandbyCoverageProvider(nullptr);
                                  }
                              }
                          } standby_coverage_guard{admin_server,
                                                   coverage_state};

                          EnterStandbyMode(admin_server, *standby_controller,
                                           accept_standby_runtime_updates,
                                           std::nullopt, cvm_controller);

                          while (true) {
                              if (ShutdownObserved(shutdown_requested)) {
                                  return shutdown_exit(
                                      "standby",
                                      ShutdownLease{.prior_release =
                                                        released_last_term});
                              }
                              released_last_term.reset();

                              // ── CVM mode (multi-submaster / CVM 名额协调)：
                              // CVM 的角色决定取代下方的主选举路径（角色
                              // 决定优先于 leadership lease，生产部署即此
                              // 模式）。非 CVM（cvm_controller 为空）回落到
                              // main 的单主选举 + 任期 serve。
                              if (cvm_controller) {
                                  const cvm::MasterRole target_role =
                                      cvm_controller->GetCurrentRole();
                                  if (target_role !=
                                      cvm::MasterRole::kPrimary) {
                                      // ── Standby (kStandby): keep
                                      // replicating, wait for promotion ──
                                      EnterStandbyMode(
                                          admin_server,
                                          *standby_controller,
                                          accept_standby_runtime_updates,
                                          std::nullopt, cvm_controller);
                                      cvm_membership_coordinator
                                          .WaitForRoleOrMembershipChange();
                                      continue;
                                  }

                                  // ── Upgrade sequence (kStandby →
                                  // kPrimary) ── first primary（无上游）无需
                                  // final catch-up；有上游 primary 时才从
                                  // standby 导出回放数据。
                                  // PromoteStandbyAndExport 内部完成 final
                                  // catch-up + 导出 PromotionContext，新
                                  // primary 从它恢复。
                                  accept_standby_runtime_updates.store(
                                      false, std::memory_order_release);
                                  const bool has_upstream =
                                      !cvm_controller->GetPrimaryAddress()
                                           .empty();
                                  PromotionContext promotion_ctx{};
                                  if (has_upstream) {
                                      auto ctx = standby_controller
                                          ->PromoteStandbyAndExport();
                                      if (!ctx) {
                                          EnterStandbyMode(
                                              admin_server,
                                              *standby_controller,
                                              accept_standby_runtime_updates,
                                              std::nullopt, cvm_controller);
                                          if (HandleSupervisorError(
                                                  "promote standby for serve",
                                                  ctx.error(), spec.type)) {
                                              return -1;
                                          }
                                          continue;
                                      }
                                      promotion_ctx = std::move(*ctx);
                                  }

                                  LOG(INFO) << "Starting serve phase (CVM "
                                               "role=kPrimary)...";
                                  coro_rpc::coro_rpc_server server(
                                      config.rpc_thread_num, config.rpc_port,
                                      config.rpc_address,
                                      config.rpc_conn_timeout,
                                      config.rpc_enable_tcp_no_delay);
                                  const char* protocol =
                                      std::getenv("MC_RPC_PROTOCOL");
                                  if (protocol &&
                                      std::string_view(protocol) == "rdma") {
                                      server.init_ibv();
                                  }

                                  const ViewVersionId view_version =
                                      0;  // 无持久化 kv_view 版本。
                                  mooncake::WrappedMasterServiceConfig
                                      wrapped_config(config, view_version);
                                  // In HA serving-primary mode, snapshot
                                  // bootstrap belongs to standby. The new
                                  // primary must restore from PromotionContext
                                  // only.
                                  wrapped_config.enable_snapshot_restore =
                                      false;
                                  // The writer callback captures this gate;
                                  // the service and writer are destroyed first.
                                  detail::ServingStateGate serving_state;
                                  auto wrapped_master_service =
                                      std::make_shared<WrappedMasterService>(
                                          wrapped_config,
                                          config.http_metadata_server,
                                          config.http_metadata_remote_url);
                                  wrapped_master_service->SetBatchOpLogTerminalCallback(
                                      [&](const OrderedOpLogWriterTerminalState& state) {
                                          serving_state.RequestShutdown([&]() {
                                              LOG(ERROR) << "CVM OpLog writer terminal: "
                                                         << toString(state.error);
                                              DeactivateServingState(admin_server,
                                                                     label_reconciler);
                                              SetRuntimeState(admin_server,
                                                              MasterRuntimeState::kStandby);
                                              server.stop();
                                          });
                                      });

                                  // Inject the supervisor-owned CvmController
                                  // lease so the slot/segment ownership records
                                  // share this master's registration lifecycle.
                                  if (cvm_controller) {
                                      wrapped_master_service->SetCvmLeaseId(
                                          cvm_controller->GetLeaseId());
                                      // ring_slots 缓存读取（§16.15.5）：slot
                                      // 归属解析走缓存优先
                                      // （miss 回退环推导），supervisor 与
                                      // service 同进程直连。
                                      wrapped_master_service->SetCvmController(
                                          cvm_controller.get());
                                  }

                                  // vsegment auto-plan
                                  // 发布侧重试策略（--vsegment_auto_plan flags
                                  // 经 MasterServiceSupervisorConfig
                                  // 传入）：serve phase 前注入一次，
                                  // 供心跳/mount
                                  // 触发点在配额快照缺失期间节流重试发布（修复
                                  // submaster 先于 client 启动时一次性
                                  // bootstrap 永久失败的死锁）。
                                  if (config.vsegment_auto_plan &&
                                      config.vsegment_auto_plan_member_count !=
                                          0) {
                                      vsegment::VSegmentUserPolicy policy;
                                      policy.member_count =
                                          config
                                              .vsegment_auto_plan_member_count;
                                      policy.stripe_size =
                                          config.vsegment_auto_plan_stripe_size;
                                      policy.member_extent_size =
                                          config
                                              .vsegment_auto_plan_member_extent_size;
                                      policy.reserved_ratio =
                                          config
                                              .vsegment_auto_plan_reserved_ratio;
                                      wrapped_master_service
                                          ->SetVSegmentAutoPlanPolicy(policy);
                                  }

                                  // Restore the complete handoff before making
                                  // the service available. The batch handoff
                                  // owns its metadata in metadata_store rather
                                  // than in the legacy object vector.
                                  if (has_upstream) {
                                      auto restore_result =
                                          promotion_ctx.metadata_store
                                              ? wrapped_master_service
                                                    ->RestoreFromBatchOpLogPromotion(
                                                        *promotion_ctx.metadata_store,
                                                        promotion_ctx.applied_cursor,
                                                        promotion_ctx.segments,
                                                        promotion_ctx.max_replica_id,
                                                        static_cast<size_t>(config.snapshot_chunk_object_count),
                                                        promotion_ctx.vsegment_partitions)
                                              : wrapped_master_service
                                                    ->RestoreFromStandby(
                                                        promotion_ctx.objects,
                                                        promotion_ctx.applied_seq_id,
                                                        promotion_ctx.segments,
                                                        promotion_ctx.weight_metadata,
                                                        promotion_ctx.vsegment_partitions);
                                      if (!restore_result) {
                                          LOG(ERROR) << "CVM promotion restore failed: "
                                                     << toString(restore_result.error());
                                          return -1;
                                      }
                                  } else {
                                      // 缺陷3修复（同身份重启失忆自愈）：无上游
                                      // PromotionContext
                                      // （首启 primary 或重启后 rank
                                      // 仍指向本机的快重启场景）时，
                                      // 从本机 OpLog 流自恢复——否则空内存 serve
                                      // 会导致该 master 全部 owned slots
                                      // 的对象元数据静默丢失（etcd 视图正常、
                                      // 路由无变化，客户端连 STALE_ROUTE
                                      // 重试都不会触发）。 本 else 分支是 serve
                                      // 前的一次性路径（RegisterRpcService
                                      // 之前，每进程至多触发一次；进程内幂等由
                                      // callee 的
                                      // oplog_replay_finalized_sources_
                                      // 定局登记兜底）。 返回 false
                                      // 覆盖两种情形：真·新集群空流（正常空态）与
                                      // 重放失败（瞬态已有界重试、耗尽后 ERROR
                                      // 告警，均在 callee
                                      // 内完成，此处无需重复打点）。
                                      const auto self_recovered =
                                          wrapped_master_service
                                              ->TrySelfRecoverFromOpLog();
                                      if (!self_recovered) {
                                          LOG(ERROR) << "Self OpLog recovery failed; "
                                                        "refusing to serve: "
                                                     << toString(self_recovered.error());
                                          return -1;
                                      }
                                      if (*self_recovered) {
                                          LOG(INFO) << "Supervisor: self OpLog "
                                                       "recovery restored "
                                                       "state before serve";
                                      } else {
                                          // A genuinely empty first start must
                                          // still pass through the restore
                                          // lifecycle before activation.
                                          auto empty_restore =
                                              wrapped_master_service
                                                  ->RestoreFromStandby({}, 0, {});
                                          if (!empty_restore) {
                                              LOG(ERROR) << "Empty startup restore failed: "
                                                         << toString(empty_restore.error());
                                              return -1;
                                          }
                                      }
                                  }

                                  mooncake::RegisterRpcService(
                                      server, *wrapped_master_service);
                                  ServingShutdownTrigger shutdown_trigger(
                                      shutdown_requested, [&]() {
                                          serving_state.RequestShutdown([&]() {
                                              server.stop();
                                          });
                                      });
                                  std::optional<async_simple::Future<coro_rpc::err_code>> ec;
                                  serving_state.RunIfActive([&]() {
                                      if (!ShutdownObserved(shutdown_requested)) {
                                          ec.emplace(server.async_start());
                                      }
                                  });
                                  if (!ec) return -1;
                                  if (ec->hasResult()) {
                                      const coro_rpc::err_code& start_err =
                                          ec->result().value();
                                      LOG(ERROR)
                                          << "Failed to start master service: "
                                             "code="
                                          << start_err.val() << ", reason="
                                          << std::string(
                                                 start_err.message());
                                      DeactivateServingState(admin_server,
                                                             label_reconciler);
                                      EnterStandbyMode(
                                          admin_server,
                                          *standby_controller,
                                          accept_standby_runtime_updates,
                                          std::nullopt, cvm_controller);
                                      return -1;
                                  }

                                  // Lightweight demotion signal: only stops
                                  // the coro_rpc server so the blocking get()
                                  // below unblocks; the main loop then
                                  // performs the full downgrade sequence
                                  // serially.
                                  cvm_membership_coordinator
                                      .SetServeStopSignal([&]() {
                                          serving_state.RequestShutdown([&]() {
                                              LOG(INFO) << "CVM quota demotion: stopping server";
                                              server.stop();
                                          });
                                      });

                                  // A demotion may have been decided while the
                                  // server was starting up (before the stop
                                  // signal was installed). If our role is no
                                  // longer primary, stop the server now so the
                                  // get() below returns promptly instead of
                                  // blocking forever.
                                  const bool demoted_during_startup =
                                      cvm_controller->GetCurrentRole() ==
                                      cvm::MasterRole::kStandby;

                                  const bool activated =
                                      !demoted_during_startup &&
                                      serving_state.RunIfActive([&]() {
                                          if (!ShutdownObserved(shutdown_requested) &&
                                              cvm_controller->GetCurrentRole() ==
                                                  cvm::MasterRole::kPrimary) {
                                              ActivateServingState(
                                                  admin_server,
                                                  wrapped_master_service,
                                                  label_reconciler);
                                          }
                                      }) &&
                                      !ShutdownObserved(shutdown_requested) &&
                                      cvm_controller->GetCurrentRole() ==
                                          cvm::MasterRole::kPrimary;
                                  if (activated) {
                                      metrics_reporter.SetRole("primary");
                                      metrics_reporter.Start();
                                      if (wrapped_master_service
                                              ->StartSlotOwnerHeartbeat() !=
                                          ErrorCode::OK) {
                                          LOG(WARNING)
                                              << "Failed to start "
                                                 "SlotOwnerHeartbeat during "
                                                 "serve phase";
                                      }
                                      // Inter-master RPC channel: etcd-driven
                                      // peer discovery + cached peer
                                      // connections for the CVM
                                      // multi-submaster coordination
                                      // (handshake now, allocation forwarding
                                      // later).
                                      if (wrapped_master_service
                                              ->StartInterMasterRpc() !=
                                          ErrorCode::OK) {
                                          LOG(WARNING)
                                              << "Failed to start "
                                                 "InterMasterRpcClient during "
                                                 "serve phase";
                                      }
                                      // 环互备双角色回放（互备 1.3.2）：
                                      // serve 就绪后开始跟随被指派的 rank 流
                                      // （开关 OFF / 无 CVM 时内部 no-op）。
                                      // 互备 1.4：注入当前 serve 实例供领养
                                      // 编排物化调用（Start 前注入；Stop 后
                                      // 在降级序列清空）。
                                      ring_standby_runtime
                                          .SetWrappedMasterService(
                                              wrapped_master_service.get());
                                      ring_standby_runtime.Start();
                                  } else {
                                      LOG(INFO)
                                          << "Demoted during serve startup; "
                                             "stopping server";
                                      server.stop();
                                  }

                                  auto server_err = std::move(*ec).get();
                                  shutdown_trigger.Stop();
                                  LOG(INFO) << "Master service stopped: "
                                            << server_err;

                                  // ── Downgrade sequence (kPrimary →
                                  // kStandby) ── 先停环跟随（join 线程 +
                                  // StopStandby），保证降级到待命池模式时
                                  // 同一时刻只有一条跟随路径活跃。
                                  ring_standby_runtime.Stop();
                                  // 互备 1.4：环跟随已停（Loop 线程 join
                                  // 完成），清空物化入口，消除对已析构
                                  // serve 实例的悬挂引用。
                                  ring_standby_runtime
                                      .SetWrappedMasterService(nullptr);
                                  metrics_reporter.Stop();
                                  metrics_reporter.SetRole("standby");
                                  wrapped_master_service
                                      ->StopSlotOwnerHeartbeat();
                                  wrapped_master_service->StopInterMasterRpc();
                                  wrapped_master_service->StopBatchOpLogWriter();
                                  cvm_membership_coordinator
                                      .SetServeStopSignal(nullptr);
                                  DeactivateServingState(admin_server,
                                                         label_reconciler);
                                  EnterStandbyMode(
                                      admin_server, *standby_controller,
                                      accept_standby_runtime_updates,
                                      std::nullopt, cvm_controller);
                                  continue;
                              }


                              auto coordinator = CreateLeaderCoordinator(spec);
                              if (!coordinator) {
                                  if (HandleSupervisorError(
                                          "create leader coordinator",
                                          coordinator.error(), spec.type)) {
                                      return -1;
                                  }
                                  continue;
                              }

                              auto& leader_coordinator = *coordinator.value();
                              std::optional<LeadershipSession>
                                  leadership_session;

                              while (!leadership_session.has_value()) {
                                  if (ShutdownObserved(shutdown_requested)) {
                                      return shutdown_exit("candidate",
                                                           ShutdownLease{});
                                  }
                                  SetRuntimeState(
                                      admin_server,
                                      MasterRuntimeState::kCandidate);

                                  auto current_view =
                                      leader_coordinator.ReadCurrentView();
                                  if (ShutdownObserved(shutdown_requested)) {
                                      return shutdown_exit("candidate",
                                                           ShutdownLease{});
                                  }
                                  if (!current_view) {
                                      EnterStandbyMode(
                                          admin_server, *standby_controller,
                                          accept_standby_runtime_updates,
                                          std::nullopt);
                                      if (HandleSupervisorError(
                                              "read current leader view",
                                              current_view.error(),
                                              spec.type)) {
                                          return -1;
                                      }
                                      break;
                                  }

                                  UpdateObservedLeader(admin_server,
                                                       *standby_controller,
                                                       current_view.value());
                                  if (!current_view.value().has_value()) {
                                      if (ShutdownObserved(
                                              shutdown_requested)) {
                                          return shutdown_exit("candidate",
                                                               ShutdownLease{});
                                      }
                                      auto acquire =
                                          leader_coordinator
                                              .TryAcquireLeadership(
                                                  config.local_hostname);
                                      if (!acquire) {
                                          if (ShutdownObserved(
                                                  shutdown_requested)) {
                                              return shutdown_exit(
                                                  "candidate", ShutdownLease{});
                                          }
                                          EnterStandbyMode(
                                              admin_server, *standby_controller,
                                              accept_standby_runtime_updates,
                                              std::nullopt,
                                              cvm_controller);
                                          if (HandleSupervisorError(
                                                  "acquire leadership",
                                                  acquire.error(), spec.type)) {
                                              return -1;
                                          }
                                          break;
                                      }

                                      if (acquire->observed_view.has_value()) {
                                          UpdateObservedLeader(
                                              admin_server,
                                              *standby_controller,
                                              acquire->observed_view);
                                      }

                                      if (acquire->status ==
                                              AcquireLeadershipStatus::
                                                  ACQUIRED &&
                                          acquire->session.has_value()) {
                                          leadership_session =
                                              *acquire->session;
                                          admin_server.SetObservedLeader(
                                              leadership_session->view);
                                          break;
                                      }

                                  if (ShutdownObserved(shutdown_requested)) {
                                      return shutdown_exit("candidate",
                                                           ShutdownLease{});
                                  }
                                  EnterStandbyMode(
                                      admin_server, *standby_controller,
                                      accept_standby_runtime_updates,
                                      acquire->observed_view);
                                  std::optional<ViewVersionId>
                                      observed_version = std::nullopt;
                                  if (acquire->observed_view.has_value()) {
                                      observed_version =
                                          acquire->observed_view->view_version;
                                  }
                                  auto wait =
                                      leader_coordinator.WaitForViewChange(
                                          observed_version,
                                          kAcquireRetryInterval);
                                  if (!wait) {
                                      if (HandleSupervisorError(
                                              "wait for leader view change",
                                              wait.error(), spec.type)) {
                                          return -1;
                                      }
                                      break;
                                  }
                                  ApplyCurrentView(admin_server,
                                                   *standby_controller, *wait);
                                  continue;
                              }

                              EnterStandbyMode(admin_server,
                                               *standby_controller,
                                               accept_standby_runtime_updates,
                                               current_view.value());
                              const auto& known_view =
                                  current_view.value().value();
                              auto wait = leader_coordinator.WaitForViewChange(
                                  known_view.view_version,
                                  kAcquireRetryInterval);
                              if (!wait) {
                                  if (HandleSupervisorError(
                                          "wait for leader view change",
                                          wait.error(), spec.type)) {
                                      return -1;
                                  }
                                  break;
                              }
                              ApplyCurrentView(admin_server,
                                               *standby_controller, *wait);
                          }

                          if (!leadership_session.has_value()) {
                              continue;
                          }

                          ShutdownLease term_lease{
                              .session = &*leadership_session,
                              .coordinator = &leader_coordinator};
                          if (ShutdownObserved(shutdown_requested)) {
                              return shutdown_exit("acquired", term_lease);
                          }
                          // The ordinary release that ends a failed or lost
                          // term. A request observed before it skips the
                          // release; one observed after it keeps the result, so
                          // shutdown neither repeats it nor reports no lease.
                          auto release_term =
                              [&](std::string_view phase) -> ErrorCode {
                              if (ShutdownObserved(shutdown_requested)) {
                                  shutdown_exit(
                                      phase, term_lease);  // Does not return.
                              }
                              const ErrorCode err =
                                  leader_coordinator.ReleaseLeadership(
                                      *leadership_session);
                              term_lease = ShutdownLease{.prior_release = err};
                              released_last_term = err;
                              TestFailPoint::Wait(
                                  "supervisor_after_term_release");
                              if (ShutdownObserved(shutdown_requested)) {
                                  shutdown_exit(
                                      phase, term_lease);  // Does not return.
                              }
                              return err;
                          };

                          // Start the lease keepalive before catch-up. The
                          // grant happens in TryAcquireLeadership, but etcd
                          // KeepAlive previously started only in the warmup
                          // phase, so a large Oplog replay expired the lease
                          // first.
                          LeadershipRenewalGuard renewal(leader_coordinator,
                                                         *leadership_session);
                          term_lease.renewal = &renewal;

                          auto claim_error = ClaimProducerViewForServing(
                              spec, config,
                              leadership_session->view.view_version);
                          if (ShutdownObserved(shutdown_requested)) {
                              return shutdown_exit("claim", term_lease);
                          }
                          if (claim_error != ErrorCode::OK) {
                              EnterStandbyMode(admin_server,
                                               *standby_controller,
                                               accept_standby_runtime_updates,
                                               leadership_session->view);
                              if (HandleLeadershipPhaseError(
                                      "producer view claim failure",
                                      "claim producer view",
                                      release_term("claim"), claim_error,
                                      spec.type)) {
                                  return -1;
                              }
                              continue;
                          }

                          accept_standby_runtime_updates.store(
                                  false, std::memory_order_release);
                              auto promotion_ctx =
                                  standby_controller->PromoteStandbyAndExport();
                              if (ShutdownObserved(shutdown_requested)) {
                                  return shutdown_exit("promotion", term_lease);
                              }
                              if (!promotion_ctx) {
                                  EnterStandbyMode(
                                      admin_server, *standby_controller,
                                      accept_standby_runtime_updates,
                                      leadership_session->view);
                                  if (HandleLeadershipPhaseError(
                                          "standby promotion failure",
                                          "promote standby for serve",
                                          release_term("promotion"),
                                          promotion_ctx.error(), spec.type)) {
                                      return -1;
                                  }
                                  continue;
                              }

                              auto return_to_standby = [&](const std::optional<
                                                           MasterView>&
                                                               leader_view) {
                                  if (ShutdownObserved(shutdown_requested)) {
                                      shutdown_exit(
                                          "standby_reentry",
                                          term_lease);  // Does not return.
                                  }
                                  if (promotion_ctx->metadata_store &&
                                      promotion_ctx->applied_cursor.last_seq !=
                                          0) {
                                      const uint64_t applied_seq =
                                          promotion_ctx->applied_cursor
                                              .last_seq;
                                      auto restore_err =
                                          standby_controller
                                              ->RestorePromotionBaseline(
                                                  std::move(*promotion_ctx));
                                      if (restore_err != ErrorCode::OK) {
                                          LOG(ERROR) << "Failed to preserve "
                                                        "catch-up cursor seq="
                                                     << applied_seq << ": "
                                                     << toString(restore_err);
                                      } else {
                                          LOG(INFO)
                                              << "Preserved catch-up cursor "
                                                 "for retry, "
                                              << "applied_seq=" << applied_seq;
                                      }
                                  }
                                  EnterStandbyMode(
                                      admin_server, *standby_controller,
                                      accept_standby_runtime_updates,
                                      leader_view);
                              };

                              if (renewal.Lost()) {
                                  LOG(WARNING)
                                      << "Leadership expired during oplog "
                                         "catch-up, "
                                      << "applied_seq="
                                      << promotion_ctx->applied_cursor.last_seq;
                                  return_to_standby(std::nullopt);
                                  LogLeadershipReleaseWarning(
                                      "catch-up lease loss",
                                      release_term("catch_up_loss"));
                                  admin_server.SetObservedLeader(std::nullopt);
                                  continue;
                              }

                              LOG(INFO) << "Entering warmup phase...";
                              SetRuntimeState(
                                  admin_server,
                                  MasterRuntimeState::kLeaderWarmup);
                              auto warmup_result = WarmupLeadership(
                                  leader_coordinator, *leadership_session,
                                  shutdown_requested);
                              if (ShutdownObserved(shutdown_requested)) {
                                  return shutdown_exit("warmup", term_lease);
                              }
                              if (!warmup_result) {
                                  return_to_standby(leadership_session->view);
                                  if (HandleLeadershipPhaseError(
                                          "renewal startup failure",
                                          "start leadership renewal",
                                          release_term("warmup"),
                                          warmup_result.error(), spec.type)) {
                                      return -1;
                                  }
                                  continue;
                              }
                              if (!warmup_result.value()) {
                                  return_to_standby(std::nullopt);
                                  LogLeadershipReleaseWarning(
                                      "warmup expiration",
                                      release_term("warmup_expiry"));
                                  LOG(WARNING) << "Leadership expired during "
                                                  "warmup phase";
                                  admin_server.SetObservedLeader(std::nullopt);
                                  continue;
                              }

                              LOG(INFO) << "Starting serve phase...";
                              coro_rpc::coro_rpc_server server(
                                  config.rpc_thread_num, config.rpc_port,
                                  config.rpc_address, config.rpc_conn_timeout,
                                  config.rpc_enable_tcp_no_delay);
                              if (RpcProtocolConfig::FromEnvironment()
                                      .use_rdma) {
#ifdef YLT_ENABLE_IBV
                                  server.init_ibv();
#else
                                  LOG(WARNING) << "RDMA RPC is disabled at "
                                                  "compile time; using TCP RPC";
#endif
                              }

                              mooncake::WrappedMasterServiceConfig
                                  wrapped_config(
                                      config,
                                      leadership_session->view.view_version);
                              // The serving primary handles
                              // heartbeats/unmounts, so forward the metadata
                              // cleanup config here like the non-HA path does.
                              // Keep the gate alive until after the service is
                              // destroyed because the OpLog writer owned by the
                              // service invokes a callback that uses it.
                              detail::ServingStateGate serving_state;
                              auto wrapped_master_service =
                                  std::make_shared<WrappedMasterService>(
                                      wrapped_config,
                                      config.http_metadata_server,
                                      config.http_metadata_remote_url);
                              // Runs the term's handlers once they are
                              // registered. It drains before the service goes,
                              // so release the service through release_service.
                              std::unique_ptr<RpcWorkerPool> worker_pool;
                              // Stops the term's resources that exist, in the
                              // serving-exit order, and keeps the service, the
                              // index and the gate alive through the exit. Each
                              // step is a no-op on a resource not yet created
                              // or already stopped.
                              auto stop_term_resources =
                                  [&](std::unique_ptr<LeadershipMonitorHandle>&
                                          monitor) {
                                      worker_pool.reset();
                                      StopLeadershipMonitor(monitor);
                                      wrapped_master_service
                                          ->StopBatchOpLogWriter();
                                      DeactivateServingState(admin_server,
                                                             label_reconciler);
                                      // Local cleanup requests finish or time
                                      // out before the lease release; a server
                                      // that accepted a request whose reply was
                                      // lost may still apply it afterwards.
                                      wrapped_master_service
                                          ->StopHttpMetadataCleanup();
                                      LOG(INFO) << "Shutdown stopped the "
                                                   "term's workers, monitor "
                                                   "and OpLog writer";
                                  };
                              auto release_service = [&]() {
                                  if (ShutdownObserved(shutdown_requested)) {
                                      std::unique_ptr<LeadershipMonitorHandle>
                                          no_monitor;
                                      stop_term_resources(no_monitor);
                                      shutdown_exit(
                                          "service_release",
                                          term_lease);  // Does not return.
                                  }
                                  worker_pool.reset();
                                  wrapped_master_service.reset();
                              };
                              wrapped_master_service
                                  ->SetBatchOpLogTerminalCallback(
                                      [&](const OrderedOpLogWriterTerminalState&
                                              state) {
                                          serving_state.RequestShutdown([&]() {
                                              LOG(ERROR)
                                                  << "Batch OpLog writer "
                                                     "terminal: "
                                                  << toString(state.error);
                                              DeactivateServingState(
                                                  admin_server,
                                                  label_reconciler);
                                              SetRuntimeState(
                                                  admin_server,
                                                  MasterRuntimeState::kStandby);
                                              server.stop();
                                          });
                                      });

                              // Restore is the serving gate: do not register or
                              // expose a candidate service until the complete
                              // promotion context has been applied. Keep the
                              // handoff until serving is established so restore
                              // failure or a later lease loss can still resume
                              // from the applied cursor.
                              SetRuntimeState(admin_server,
                                              MasterRuntimeState::kRecovering);
                              auto restore_result =
                                  promotion_ctx->metadata_store
                                      ? wrapped_master_service
                                            ->RestoreFromBatchOpLogPromotion(
                                                *promotion_ctx->metadata_store,
                                                promotion_ctx->applied_cursor,
                                                promotion_ctx->segments,
                                                promotion_ctx->max_replica_id,
                                                static_cast<size_t>(
                                                    config
                                                        .snapshot_chunk_object_count),
                                                promotion_ctx->vsegment_partitions)
                                      : wrapped_master_service
                                            ->RestoreFromStandby(
                                                promotion_ctx->objects,
                                                promotion_ctx->applied_seq_id,
                                                promotion_ctx->segments,
                                                promotion_ctx->weight_metadata,
                                                promotion_ctx->vsegment_partitions);
                              if (ShutdownObserved(shutdown_requested)) {
                                  std::unique_ptr<LeadershipMonitorHandle>
                                      no_monitor;
                                  stop_term_resources(no_monitor);
                                  return shutdown_exit("restore", term_lease);
                              }
                              if (!restore_result) {
                                  LOG(ERROR)
                                      << "Standby restore failed: "
                                      << toString(restore_result.error());
                                  release_service();
                                  DeactivateServingState(admin_server,
                                                         label_reconciler);
                                  return_to_standby(leadership_session->view);
                                  SetRuntimeState(
                                      admin_server,
                                      MasterRuntimeState::kRecovering);
                                  if (HandleLeadershipPhaseError(
                                          "standby restore failure",
                                          "restore standby state",
                                          release_term("restore"),
                                          restore_result.error(), spec.type)) {
                                      return -1;
                                  }
                                  continue;
                              }

                              // Restore can outlast catch-up. Recheck the
                              // side-thread guard and renew once more before
                              // registering RPC or publishing a serving
                              // endpoint, otherwise a lease lost mid-restore
                              // keeps recovering.
                              if (renewal.Lost()) {
                                  LOG(WARNING)
                                      << "Leadership expired during restore, "
                                         "applied_seq="
                                      << promotion_ctx->applied_cursor.last_seq;
                                  release_service();
                                  DeactivateServingState(admin_server,
                                                         label_reconciler);
                                  return_to_standby(std::nullopt);
                                  LogLeadershipReleaseWarning(
                                      "restore lease loss",
                                      release_term("restore_loss"));
                                  admin_server.SetObservedLeader(std::nullopt);
                                  continue;
                              }

                              SetRuntimeState(
                                  admin_server,
                                  MasterRuntimeState::kLeaderWarmup);

                              auto serve_preflight =
                                  leader_coordinator.RenewLeadership(
                                      *leadership_session);
                              if (ShutdownObserved(shutdown_requested)) {
                                  std::unique_ptr<LeadershipMonitorHandle>
                                      no_monitor;
                                  stop_term_resources(no_monitor);
                                  return shutdown_exit("preflight", term_lease);
                              }
                              if (!serve_preflight) {
                                  release_service();
                                  DeactivateServingState(admin_server,
                                                         label_reconciler);
                                  return_to_standby(leadership_session->view);
                                  if (HandleLeadershipPhaseError(
                                          "serve preflight failure",
                                          "validate leadership before serving",
                                          release_term("preflight"),
                                          serve_preflight.error(), spec.type)) {
                                      return -1;
                                  }
                                  continue;
                              }
                              if (!serve_preflight.value()) {
                                  release_service();
                                  DeactivateServingState(admin_server,
                                                         label_reconciler);
                                  return_to_standby(std::nullopt);
                                  LogLeadershipReleaseWarning(
                                      "serve preflight expiration",
                                      release_term("preflight_expiry"));
                                  LOG(WARNING) << "Leadership expired before "
                                                  "entering serve phase";
                                  admin_server.SetObservedLeader(std::nullopt);
                                  continue;
                              }

                              worker_pool = std::make_unique<RpcWorkerPool>(
                                  config.handler_worker_thread_num != 0
                                      ? config.handler_worker_thread_num
                                      : config.rpc_thread_num);
                              mooncake::RegisterRpcService(
                                  server, *wrapped_master_service,
                                  worker_pool.get());

                              auto leadership_monitor =
                                  leader_coordinator.StartLeadershipMonitor(
                                      *leadership_session,
                                      [&server, &admin_server, &serving_state,
                                       &label_reconciler](auto reason) {
                                          serving_state.RequestShutdown([&]() {
                                              admin_server.SetServiceAvailable(
                                                  false);
                                              label_reconciler.SetLeader(false);
                                              SetRuntimeState(
                                                  admin_server,
                                                  MasterRuntimeState::kStandby);
                                              LOG(INFO)
                                                  << "Trying to stop server, "
                                                     "reason="
                                                  << LeadershipLossReasonToString(
                                                         reason);
                                              server.stop();
                                          });
                                      });
                              if (ShutdownObserved(shutdown_requested)) {
                                  std::unique_ptr<LeadershipMonitorHandle>
                                      started_monitor;
                                  if (leadership_monitor) {
                                      started_monitor =
                                          std::move(leadership_monitor.value());
                                  }
                                  stop_term_resources(started_monitor);
                                  return shutdown_exit("monitor_start",
                                                       term_lease);
                              }
                              if (!leadership_monitor) {
                                  release_service();
                                  DeactivateServingState(admin_server,
                                                         label_reconciler);
                                  return_to_standby(leadership_session->view);
                                  if (HandleLeadershipPhaseError(
                                          "serve monitor startup failure",
                                          "start leadership monitor",
                                          release_term("monitor_start"),
                                          leadership_monitor.error(),
                                          spec.type)) {
                                      return -1;
                                  }
                                  continue;
                              }
                              auto leadership_monitor_handle =
                                  std::move(leadership_monitor.value());

                              ServingShutdownTrigger shutdown_trigger(
                                  shutdown_requested, [&]() {
                                      serving_state.RequestShutdown([&]() {
                                          admin_server.SetServiceAvailable(
                                              false);
                                          label_reconciler.SetLeader(false);
                                          SetRuntimeState(
                                              admin_server,
                                              MasterRuntimeState::kStandby);
                                          LOG(INFO)
                                              << "Trying to stop server, "
                                                 "reason=shutdown requested";
                                          server.stop();
                                      });
                                  });

                              std::optional<
                                  async_simple::Future<coro_rpc::err_code>>
                                  ec;
                              TestFailPoint::Wait(
                                  "supervisor_before_serve_start");
                              serving_state.RunIfActive([&]() {
                                  if (!ShutdownObserved(shutdown_requested)) {
                                      ec.emplace(server.async_start());
                                  }
                              });
                              if (!ec.has_value()) {
                                  shutdown_trigger.Stop();
                                  if (ShutdownObserved(shutdown_requested)) {
                                      stop_term_resources(
                                          leadership_monitor_handle);
                                      return shutdown_exit("serve_start",
                                                           term_lease);
                                  }
                                  StopLeadershipMonitor(
                                      leadership_monitor_handle);
                                  release_service();
                                  DeactivateServingState(admin_server,
                                                         label_reconciler);
                                  return_to_standby(std::nullopt);
                                  LogLeadershipReleaseWarning(
                                      "serve startup cancellation",
                                      release_term("serve_start_cancel"));
                                  continue;
                              }
                              if (ec->hasResult()) {
                                  shutdown_trigger.Stop();
                                  LOG(ERROR)
                                      << "Failed to start master service: "
                                      << ec->result().value();
                                  if (ShutdownObserved(shutdown_requested)) {
                                      stop_term_resources(
                                          leadership_monitor_handle);
                                      return shutdown_exit(
                                          "serve_start_failure", term_lease);
                                  }
                                  StopLeadershipMonitor(
                                      leadership_monitor_handle);
                                  release_service();
                                  DeactivateServingState(admin_server,
                                                         label_reconciler);
                                  return_to_standby(leadership_session->view);
                                  auto err =
                                      release_term("serve_start_failure");
                                  if (err != ErrorCode::OK) {
                                      LOG(ERROR)
                                          << "Failed to release leadership: "
                                          << toString(err);
                                  }
                                  return -1;
                              }

                              ErrorCode publish_ready_err = ErrorCode::OK;
                              if (serving_state.IsActive() &&
                                  !ShutdownObserved(shutdown_requested)) {
                                  // The backend call can take several seconds.
                                  // Keep it outside the transition gate so
                                  // leadership loss can stop the listener
                                  // promptly.
                                  publish_ready_err =
                                      leader_coordinator.PublishServiceReady(
                                          *leadership_session);
                                  if (publish_ready_err == ErrorCode::OK) {
                                      TestFailPoint::Wait(
                                          "supervisor_before_serve_activation");
                                      serving_state.RunIfActive([&]() {
                                          if (ShutdownObserved(
                                                  shutdown_requested)) {
                                              return;
                                          }
                                          ActivateServingState(
                                              admin_server,
                                              wrapped_master_service,
                                              label_reconciler);
                                          // Serving owns the restored index
                                          // now; drop the handoff so the
                                          // standby copy is not held for the
                                          // whole term.
                                          promotion_ctx->metadata_store.reset();
                                          promotion_ctx->objects.clear();
                                          promotion_ctx->segments.clear();
                                      });
                                  } else {
                                      serving_state.RequestShutdown([&]() {
                                          DeactivateServingState(
                                              admin_server, label_reconciler);
                                          server.stop();
                                      });
                                  }
                              }

                              if (publish_ready_err != ErrorCode::OK) {
                                  LOG(ERROR) << "Failed to publish master "
                                                "service readiness: "
                                             << toString(publish_ready_err);
                                  StopLeadershipMonitor(
                                      leadership_monitor_handle);
                                  auto server_err = std::move(ec.value()).get();
                                  shutdown_trigger.Stop();
                                  LOG(ERROR) << "Master service stopped after "
                                                "readiness failure: "
                                             << server_err;
                                  if (ShutdownObserved(shutdown_requested)) {
                                      stop_term_resources(
                                          leadership_monitor_handle);
                                      return shutdown_exit("readiness_failure",
                                                           term_lease);
                                  }
                                  release_service();
                                  return_to_standby(leadership_session->view);
                                  if (HandleLeadershipPhaseError(
                                          "service readiness publication "
                                          "failure",
                                          "publish master service readiness",
                                          release_term("readiness_failure"),
                                          publish_ready_err, spec.type)) {
                                      return -1;
                                  }
                                  continue;
                              }

                              auto server_err = std::move(ec.value()).get();
                              shutdown_trigger.Stop();
                              // Finish the calls the workers still hold before
                              // the term's OpLog writer and leadership go, as
                              // inline handlers would have.
                              worker_pool.reset();
                              LOG(ERROR)
                                  << "Master service stopped: " << server_err;

                              StopLeadershipMonitor(leadership_monitor_handle);
                              wrapped_master_service->StopBatchOpLogWriter();
                              DeactivateServingState(admin_server,
                                                     label_reconciler);
                              wrapped_master_service->StopHttpMetadataCleanup();
                              auto err = release_term("serving");
                              LOG(INFO)
                                  << "Release leadership: " << toString(err);
                              auto current_view =
                                  leader_coordinator.ReadCurrentView();
                              release_service();
                              if (ShutdownObserved(shutdown_requested)) {
                                  return shutdown_exit("standby_reentry",
                                                       term_lease);
                              }
                              // Serving drops the handoff in
                              // ActivateServingState. If the listener stopped
                              // before that, preserve the still-populated
                              // catch-up cursor.
                              if (promotion_ctx->metadata_store) {
                                  return_to_standby(current_view
                                                        ? current_view.value()
                                                        : std::nullopt);
                              } else if (current_view) {
                                  EnterStandbyMode(
                                      admin_server, *standby_controller,
                                      accept_standby_runtime_updates,
                                      current_view.value());
                              } else {
                                  // ── Standby (kStandby): keep replicating,
                                  // wait for promotion ──
                                  EnterStandbyMode(
                                      admin_server, *standby_controller,
                                      accept_standby_runtime_updates,
                                      std::nullopt, cvm_controller);
                                  if (cvm_controller) {
                                      cvm_membership_coordinator
                                          .WaitForRoleOrMembershipChange();
                                  } else {
                                      // Unreachable: target_role is kPrimary
                                      // when cvm_controller is null. Sleep
                                      // defensively to avoid a tight loop.
                                      std::this_thread::sleep_for(
                                          kSupervisorRetryInterval);
                                  }
                              }
                          }

                          return 0;
                      }

}  // namespace

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config,
    std::function<bool()> shutdown_requested)
    : config_(config), shutdown_requested_(std::move(shutdown_requested)) {}

int MasterServiceSupervisor::Start() {
    auto spec = BuildHABackendSpec(config_);
    if (!spec) {
        LOG(ERROR) << "Failed to parse HA backend config: "
                   << toString(spec.error())
                   << ", backend_type=" << config_.ha_backend_type;
        return -1;
    }

    std::unique_ptr<StandbyController> standby_controller;
    try {
        standby_controller = CreateStandbyController(*spec, config_);
    } catch (const std::exception& error) {
        LOG(ERROR) << "Standby dependency initialization failed: "
                   << error.what();
        return -1;
    }

    const auto metrics = config_.metrics.Get();
    mooncake::MasterAdminServer admin_server(
        static_cast<uint16_t>(metrics.port), metrics.enabled, metrics.host);
    if (!admin_server.Start()) {
        LOG(ERROR) << "Failed to start master admin server, metrics_port="
                   << metrics.port;
        return -1;
    }
    return RunSupervisorLoop(*spec, config_, admin_server,
                             std::move(standby_controller),
                             shutdown_requested_);
}

}  // namespace ha
}  // namespace mooncake
