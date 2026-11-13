#include "cvm/cvm_controller.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <limits>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <utility>

#include <glog/logging.h>

#include "cvm/cvm_http_server.h"
#include "cvm/cvm_keys.h"
#include "cvm/cvm_service_delegate.h"
#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"
#include "etcd_helper.h"

namespace mooncake {
namespace cvm {

namespace {

constexpr int kWatchEventBroken = 2;
constexpr int kWatchStopTimeoutMs = 1000;
constexpr int kKeepAliveReadyTimeoutMs = 1000;

// 晋升超时兜底（§16.15.8 liveness backstop）：primary 死亡持续超过该
// 时长后，本机可越过 standby 优先级直接 CAS 接管。取值须显著大于正常
// 晋升窗口（lease TTL 3s + sync 5s），30s ≈ 6 倍冗余，避免与正常晋升
// 竞争；安全性由 epoch CAS 单赢家承担。
constexpr auto kDeadPrimaryTakeoverTimeout = std::chrono::seconds(30);

// 逗号拼接 id 列表，空时返回 "[]"，用于 membership change 日志字段。
std::string JoinIds(const std::vector<std::string>& ids) {
    std::string out;
    for (size_t i = 0; i < ids.size(); ++i) {
        if (i != 0) {
            out += ",";
        }
        out += ids[i];
    }
    return out.empty() ? "[]" : out;
}

// 确定性兼管者推导（§16.8.1 兼管者定义 / §16.8.3 自愈指定）：rank 的
// 兼管者 = 小于 rank 的最大合法记录的 primary；无下界（rank0 断链）则
// 大于 rank 的最小合法记录的 primary。两个消费点（缺 rank 补建 /
// 兼管自愈）必须从同一快照推导同一兼管者（单一写者前提），收拢为唯一
// 实现防两份推导漂移。合法性判定的差异显式参数化：
//   members == nullptr —— 记录存在即合法（缺 rank 补建，不查存活）；
//   members != nullptr —— primary 非空且存活（自愈要求接管者有 serving
//                        能力，与原散落实现逐条对照保真）。
// max_rank_exclusive：回退段（断链向上找）的 rank 上界。补建传
// group_count（排除跨代残留记录，原实现边界）；自愈传 uint32_t 最大值
// （原实现遍历全量记录，无上界）。
std::string FindCaretakerPrimary(
    const RingSlotsView& view, uint32_t rank, uint32_t max_rank_exclusive,
    const std::vector<MasterRegistration>* members) {
    std::string caretaker;
    // assigns 按 rank 升序（BuildRingSlotsView 保证）：倒序扫描命中即
    // 「小于 rank 的最大合法记录」。
    for (auto it = view.assigns().rbegin(); it != view.assigns().rend();
         ++it) {
        if (it->rank >= rank) {
            continue;
        }
        if (members != nullptr &&
            (it->primary_id.empty() ||
             !IsMemberAlive(*members, it->primary_id))) {
            continue;
        }
        caretaker = it->primary_id;
        break;
    }
    if (!caretaker.empty()) {
        return caretaker;
    }
    // 断链回退：大于 rank 的最小合法记录（升序扫描命中即取）。
    for (const auto& b : view.assigns()) {
        if (b.rank <= rank || b.rank >= max_rank_exclusive) {
            continue;
        }
        if (members != nullptr &&
            (b.primary_id.empty() ||
             !IsMemberAlive(*members, b.primary_id))) {
            continue;
        }
        caretaker = b.primary_id;
        break;
    }
    return caretaker;
}

// 补位阶段方法的三态结果（阶段方法模式：外壳据此决定终止或继续下一阶段）。
//   kStepNoCandidate —— 本阶段无适用 rank，落到下一阶段；
//   kStepDone        —— 本阶段已产生 etcd 写入（外壳返回 true 触发刷新）；
//   kStepAbort       —— 已处理一个候选但未写入（单周期单次动作，下周期重判）。
enum StepOutcome {
    kStepNoCandidate = 0,
    kStepDone = 1,
    kStepAbort = 2,
};

// 补位阶段/归属推进路径索引（LogStepOutcome / last_phase_outcome_ 用）：
// 0-3 为补位四阶段（TryAssignRankForSelf 拆分），4-5 为同一
// ReconcileRole 周期内的另外两条归属推进路径（晋升 / 兼管自愈），
// 6 为已归属成员环互备配对（JoinAsStandbyRing，互备 1.1）。
enum AssignPhase {
    kPhaseRepair = 0,
    kPhaseClaimDead = 1,
    kPhaseClaimCaretaker = 2,
    kPhaseJoinStandby = 3,
    kPhasePromote = 4,
    kPhaseTakeover = 5,
    kPhaseJoinStandbyRing = 6,
};

// 环互备配对总开关实现（MC_STANDBY_MUTUAL_PAIRING，互备分步交付的灰度
// 闸门）：OFF 时已归属成员保持现状（不配对、不回放），与未启用本特性
// 行为等价。必须在互备回放运行时（1.3）部署后才可开启——否则空
// standby_ids 记录会被晋升路径优先采纳而导出空元数据（TryPromote 让位
// 规则）。首次读取后进程内缓存（周期判定热路径零 getenv 开销）。
// 公有门面：CvmController::StandbyMutualPairingEnabled（供 1.1 门控与
// 1.3.2 supervisor 运行时共用同一缓存值）。
bool MutualPairingToggleEnabled() {
    static const bool enabled = [] {
        const char* value = std::getenv("MC_STANDBY_MUTUAL_PAIRING");
        if (value == nullptr) {
            return false;
        }
        const std::string v(value);
        return v == "1" || v == "true" || v == "on";
    }();
    return enabled;
}

// address（"host:port"）提取 host：按末个 ':' 切分（兼容 IPv6 字面量，
// port 段在最后一个 ':' 之后）。
// 提取失败返回空串，调用方按保守语义处理（无法判定跨机即不配对）。
std::string HostOfAddress(const std::string& address) {
    const size_t pos = address.rfind(':');
    if (pos == std::string::npos || pos == 0) {
        return "";
    }
    return address.substr(0, pos);
}

// 跨机判定（环互备配对用）：两个 master_id 的 address host 是否相同。
// 任一地址缺失或 host 提取失败时保守返回 true（按同机处理 → 顺延候选），
// 宁可不配对也不冒双 rank 同故障域的风险。已知局限：同节点以不同别名
//（hostname vs IP）注册会绕过判定，跨机约束按注册 address 字面一致性
// 生效。
bool SamePrimaryHost(const std::string& lhs, const std::string& rhs,
                     const std::map<std::string, std::string>& address_by_master) {
    const auto lit = address_by_master.find(lhs);
    const auto rit = address_by_master.find(rhs);
    if (lit == address_by_master.end() || rit == address_by_master.end()) {
        return true;  // 地址缺失：保守按同机
    }
    const std::string lhs_host = HostOfAddress(lit->second);
    const std::string rhs_host = HostOfAddress(rit->second);
    if (lhs_host.empty() || rhs_host.empty()) {
        return true;  // 提取失败：保守按同机
    }
    return lhs_host == rhs_host;
}

}  // namespace

CvmController::CvmController(Config config) : config_(std::move(config)) {
    current_role_.store(config_.role);
}

CvmController::~CvmController() { Stop(); }

void CvmController::SetSlotsChangedCallback(std::function<void()> callback) {
    std::lock_guard<std::mutex> lock(slots_changed_mutex_);
    slots_changed_callback_ = std::move(callback);
}

void CvmController::NotifySlotsChanged() {
    std::lock_guard<std::mutex> lock(slots_changed_mutex_);
    if (slots_changed_callback_) slots_changed_callback_();
}

void CvmController::SetDelegate(CvmServiceDelegate* delegate) {
    delegate_ = delegate;
}

ErrorCode CvmController::Start() {
    if (running_.load()) {
        return ErrorCode::OK;
    }

    const uint32_t effective_group_count =
        std::max<uint32_t>(1, config_.submaster_count);
    if (effective_group_count > SlotCount()) {
        LOG(ERROR) << "CvmController::Start invalid partition config: "
                   << "submaster_count=" << config_.submaster_count
                   << ", slot_count=" << SlotCount()
                   << ". slot_count must be at least submaster_count.";
        return ErrorCode::INVALID_PARAMS;
    }

    LOG(INFO) << "CvmController::Start begin: cluster_namespace="
              << config_.cluster_namespace << ", master_id="
              << config_.master_id << ", address=" << config_.address
              << ", role=" << static_cast<int32_t>(config_.role)
              << ", registration_lease_ttl_sec="
              << config_.registration_lease_ttl_sec << ", http_host="
              << config_.http_host << ", http_port=" << config_.http_port
              << ", submaster_count=" << config_.submaster_count
              << ", slot_count=" << SlotCount()
              << ", sync_interval_ms=" << config_.sync_interval.count();

    // NOTE: the etcd client is a process-global singleton (EtcdHelper); the
    // embedding master is responsible for connecting it before Start().
    ErrorCode err = EtcdHelper::GrantLease(config_.registration_lease_ttl_sec,
                                           lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "CvmController::Start GrantLease failed: err=" << err
                   << ", cluster_namespace=" << config_.cluster_namespace
                   << ", master_id=" << config_.master_id
                   << ", registration_lease_ttl_sec="
                   << config_.registration_lease_ttl_sec
                   << " (etcd client may not be connected yet)";
        return err;
    }
    LOG(INFO) << "CvmController::Start GrantLease ok: lease_id=" << lease_id_;

    MasterRegistration reg;
    reg.master_id = config_.master_id;
    reg.address = config_.address;
    reg.role = static_cast<int32_t>(config_.role);
    reg.registered_at_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    LOG(INFO) << "CvmController::Start registering master: master_id="
              << reg.master_id << ", address=" << reg.address << ", role="
              << reg.role << ", cluster_namespace=" << config_.cluster_namespace
              << ", lease_id=" << lease_id_;
    err = EtcdViewStore::RegisterMaster(config_.cluster_namespace, reg,
                                        lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "CvmController::Start RegisterMaster failed: err=" << err
                   << ", master_id=" << reg.master_id << ", address="
                   << reg.address << ", role=" << reg.role
                   << ", cluster_namespace=" << config_.cluster_namespace
                   << ", lease_id=" << lease_id_;
        (void)EtcdHelper::RevokeLease(lease_id_);
        return err;
    }
    LOG(INFO) << "CvmController::Start RegisterMaster ok: master_id="
              << reg.master_id;

    // Persist the cluster-wide ring config (§15.3). Idempotent: submaster_count
    // is a static deployment constant, so concurrent writes converge to the
    // same value. Clients read this to locally derive the primary ring.
    // §16.8: slot_group_count (G) 一经 bootstrap 定型恒不变——重启回写必须
    // 读旧值保留，否则会把 G 冲回默认值（新模型下等价于全量段界重划）。
    // fail-safe：Load 失败（etcd 抖动）时跳过本次回写，宁可不更新
    // submaster_count，也不能在未知旧 G 的情况下盲写覆盖。
    RingMeta ring_meta;
    ring_meta.submaster_count = config_.submaster_count;
    ring_meta.slot_count = SlotCount();
    bool ring_meta_valid = true;
    {
        RingMeta existing;
        ViewVersionId meta_version = 0;
        const ErrorCode meta_err = EtcdViewStore::LoadClusterMeta(
            config_.cluster_namespace, existing, meta_version);
        if (meta_err == ErrorCode::OK) {
            if (existing.slot_count != SlotCount()) {
                LOG(ERROR)
                    << "CvmController::Start slot count mismatch: cluster="
                    << existing.slot_count << ", local=" << SlotCount()
                    << ", env=" << kSlotCountEnv
                    << ". Use the same value on every master and client; "
                       "changing it requires a fresh cluster namespace.";
                (void)EtcdHelper::RevokeLease(lease_id_);
                return ErrorCode::INVALID_PARAMS;
            }
            if (std::max<uint32_t>(1, existing.slot_group_count) >
                SlotCount()) {
                LOG(ERROR) << "CvmController::Start invalid cluster metadata: "
                           << "slot_group_count="
                           << existing.slot_group_count
                           << ", slot_count=" << SlotCount();
                (void)EtcdHelper::RevokeLease(lease_id_);
                return ErrorCode::INVALID_PARAMS;
            }
            ring_meta.slot_group_count = existing.slot_group_count;
        } else if (meta_err == ErrorCode::ETCD_KEY_NOT_EXIST) {
            // 新集群首启：meta 尚未创建，正常写入（G 默认 0，随后由
            // TryBootstrapRingSlots 定型）。
        } else {
            // etcd 抖动：旧 G 未知，跳过本次回写——宁可 meta 里的
            // submaster_count 暂旧，不可盲写覆盖 G。
            LOG(WARNING) << "CvmController::Start LoadClusterMeta failed ("
                         << meta_err << "), skip SaveClusterMeta to avoid "
                                        "clobbering G";
            ring_meta_valid = false;
        }
    }
    if (ring_meta_valid) {
        err = EtcdViewStore::SaveClusterMeta(config_.cluster_namespace,
                                             ring_meta);
    } else {
        err = ErrorCode::OK;  // 已存在，无需回写
    }
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmController::Start SaveClusterMeta failed: err="
                     << err << ", cluster_namespace=" << config_.cluster_namespace
                     << ", submaster_count=" << config_.submaster_count
                     << " (clients may fall back to stale ring until retried)";
    } else {
        LOG(INFO) << "CvmController::Start SaveClusterMeta ok: submaster_count="
                  << config_.submaster_count
                  << ", slot_count=" << ring_meta.slot_count;
    }

    masters_watch_state_ = std::make_unique<WatchState>();
    ring_slots_watch_state_ = std::make_unique<WatchState>();
    // 初始缓存先于 watch 建立：watch 只关注未来变更（start_revision=0），
    // 没有初始 Load 会漏掉启动前已存在的归属状态。
    RefreshRingSlotsCache();
    running_.store(true);

    keepalive_thread_ = std::thread([this]() { KeepaliveLoop(); });
    masters_watch_thread_ = std::thread([this]() { MastersWatchLoop(); });
    ring_slots_watch_thread_ =
        std::thread([this]() { RingSlotsWatchLoop(); });
    membership_thread_ = std::thread([this]() { MembershipLoop(); });
    LOG(INFO) << "CvmController::Start started "
                 "keepalive/masters_watch/ring_slots_watch/membership threads";

    if (config_.http_port != 0) {
        CvmHttpServer::Config http_cfg;
        http_cfg.host = config_.http_host;
        http_cfg.port = config_.http_port;
        http_cfg.cluster_namespace = config_.cluster_namespace;
        LOG(INFO) << "CvmController::Start starting CvmHttpServer: host="
                  << http_cfg.host << ", port=" << http_cfg.port;
        http_server_ = std::make_unique<CvmHttpServer>(http_cfg);
        err = http_server_->Start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "CvmController start http server failed: " << err
                       << ", host=" << http_cfg.host << ", port="
                       << http_cfg.port;
            http_server_.reset();
        } else {
            LOG(INFO) << "CvmController CvmHttpServer started";
        }
    }

    LOG(INFO) << "CvmController::Start ok";
    return ErrorCode::OK;
}

void CvmController::Stop() {
    if (!running_.load() && !masters_watch_thread_.joinable() &&
        !keepalive_thread_.joinable() && !membership_thread_.joinable()) {
        return;
    }

    running_.store(false);

    CancelMastersWatchAndWait();
    CancelRingSlotsWatchAndWait();

    if (masters_watch_state_) {
        std::lock_guard<std::mutex> lock(masters_watch_state_->mutex);
        masters_watch_state_->cv.notify_all();
    }
    if (ring_slots_watch_state_) {
        std::lock_guard<std::mutex> lock(ring_slots_watch_state_->mutex);
        ring_slots_watch_state_->cv.notify_all();
    }

    if (keepalive_thread_.joinable()) {
        (void)EtcdHelper::CancelKeepAlive(lease_id_);
        keepalive_thread_.join();
    }
    if (masters_watch_thread_.joinable()) {
        masters_watch_thread_.join();
    }
    if (ring_slots_watch_thread_.joinable()) {
        ring_slots_watch_thread_.join();
    }
    if (membership_thread_.joinable()) {
        membership_cv_.notify_all();
        membership_thread_.join();
    }

    if (http_server_) {
        http_server_->Stop();
        http_server_.reset();
    }

    (void)EtcdHelper::RevokeLease(lease_id_);
    masters_watch_state_.reset();
    ring_slots_watch_state_.reset();
    LOG(INFO) << "CvmController::Stop ok: master_id=" << config_.master_id
              << ", lease_id=" << lease_id_;
}

void CvmController::CancelMastersWatchAndWait() {
    if (masters_watch_armed_.exchange(false)) {
        (void)EtcdViewStore::CancelWatchMasters(config_.cluster_namespace);
        (void)EtcdViewStore::WaitWatchMastersStopped(config_.cluster_namespace,
                                                     kWatchStopTimeoutMs);
    }
}

void CvmController::MastersWatchLoop() {
    while (running_.load()) {
        {
            std::lock_guard<std::mutex> lock(masters_watch_state_->mutex);
            masters_watch_state_->dirty = false;
            masters_watch_state_->broken = false;
        }

        // start_revision=0 → 从当前开始 watch，只关注未来的成员增删（lease
        // 过期删除 / 新节点注册）。
        ErrorCode err = EtcdViewStore::WatchMasters(
            config_.cluster_namespace, /*start_revision=*/0,
            masters_watch_state_.get(), &CvmController::WatchCallback);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "CvmController arm masters watch failed: " << err;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        masters_watch_armed_.store(true);

        // Go 侧 watch 是持久 watch（一次注册，持续派发事件），普通事件后
        // 不会自动注销；仅在真正断开（broken）时才需要重新 arm。因此用内层
        // 循环等待事件：普通事件清 dirty 后继续等，绝不重复注册同前缀——
        // 否则会命中 "already being watched" 并以 1s 间隔刷屏。
        while (running_.load()) {
            std::unique_lock<std::mutex> lock(masters_watch_state_->mutex);
            masters_watch_state_->cv.wait(lock, [this] {
                return masters_watch_state_->dirty || !running_.load();
            });
            if (!running_.load()) {
                break;
            }

            const bool broken = masters_watch_state_->broken;
            masters_watch_state_->dirty = false;
            masters_watch_state_->broken = false;
            lock.unlock();

            // 成员增删（lease 过期）→ 先探测成员集变化并记录日志，再重算角色。
            std::vector<MasterRegistration> members;
            if (LoadRankedMembers(members)) {
                LogMembershipChange(members);
            }
            ReconcileRole();
            // 成员集变化：即使本机角色不变（仍是 standby），也要通知 delegate
            // 重新从最新成员列表本地推导回放源（替代已删除的 kv_view watch）。
            if (delegate_) {
                delegate_->OnMembershipChanged();
            }
            NotifySlotsChanged();

            if (broken) {
                // watch 已断开（goroutine 已自清理并退出），退出内层循环
                // 重新 arm；普通事件则继续等待下一个事件。
                break;
            }
        }
    }
}

void CvmController::CancelRingSlotsWatchAndWait() {
    if (ring_slots_watch_armed_.exchange(false)) {
        (void)EtcdViewStore::CancelWatchRingSlots(config_.cluster_namespace);
        (void)EtcdViewStore::WaitWatchRingSlotsStopped(
            config_.cluster_namespace, kWatchStopTimeoutMs);
    }
}

void CvmController::RingSlotsWatchLoop() {
    while (running_.load()) {
        {
            std::lock_guard<std::mutex> lock(ring_slots_watch_state_->mutex);
            ring_slots_watch_state_->dirty = false;
            ring_slots_watch_state_->broken = false;
        }

        // start_revision=0 → 只关注未来的 owner 切换（晋升 CAS / reshard
        // 状态翻转）。初始状态由 Start 时的 RefreshRingSlotsCache 建立。
        ErrorCode err = EtcdViewStore::WatchRingSlots(
            config_.cluster_namespace, /*start_revision=*/0,
            ring_slots_watch_state_.get(), &CvmController::WatchCallback);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "CvmController arm ring_slots watch failed: "
                         << err;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        ring_slots_watch_armed_.store(true);

        // 与 MastersWatchLoop 同构：持久 watch 普通事件不注销，仅 broken
        //（goroutine 已自清理退出）才退出内层循环重 arm，避免重复注册同
        // 前缀触发 "already being watched" 刷屏。
        while (running_.load()) {
            std::unique_lock<std::mutex> lock(
                ring_slots_watch_state_->mutex);
            ring_slots_watch_state_->cv.wait(lock, [this] {
                return ring_slots_watch_state_->dirty || !running_.load();
            });
            if (!running_.load()) {
                break;
            }

            const bool broken = ring_slots_watch_state_->broken;
            ring_slots_watch_state_->dirty = false;
            ring_slots_watch_state_->broken = false;
            lock.unlock();

            // owner 切换 → 全量重拉缓存（G <= 16384 条，量级安全）。
            // 幽灵写防护（§16.14.6）：本 refresh 让本机「不再是 owner」的
            // 判定即时生效。
            RefreshRingSlotsCache();
            NotifySlotsChanged();

            if (broken) {
                break;
            }
        }
    }
}

void CvmController::RefreshRingSlotsCache() {
    // G 与归属记录分两次读，非原子；G 创建后恒不变（§16.8 前提），读到
    // 旧值 G' 也只是短暂用旧段界，下一轮兜底刷新收敛。
    // 模型已启用（曾 seen）但本轮 refresh 失败：数据面正在用旧视图服务，
    // 是幽灵写风险的直接前置信号——节流 WARNING（30s 一条，防 etcd 持续
    // 抖动时 MembershipLoop 5s 周期刷屏）。
    const auto warn_stale_if_seen = [this] {
        if (!ring_slots_model_seen_.load(std::memory_order_relaxed)) {
            return;  // 旧集群（从未 seen）：Load 失败属预期，不告警
        }
        const int64_t now_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
        const int64_t last =
            last_stale_cache_warn_ms_.load(std::memory_order_relaxed);
        if (now_ms - last < 30000) {
            return;
        }
        int64_t expected = last;  // compare_exchange 需可变引用
        if (last_stale_cache_warn_ms_.compare_exchange_strong(
                expected, now_ms, std::memory_order_relaxed)) {
            LOG(WARNING) << "CvmController ring_slots cache stale "
                            "(etcd unreachable), serving with old view: "
                            "master_id="
                         << config_.master_id
                         << ", cached_ranks=" << [this] {
                                std::lock_guard<std::mutex> lock(
                                    ring_slots_mutex_);
                                return ring_slots_cache_.size();
                            }();
        }
    };
    RingMeta meta;
    ViewVersionId version = 0;
    ErrorCode err = EtcdViewStore::LoadClusterMeta(
        config_.cluster_namespace, meta, version);
    if (err != ErrorCode::OK) {
        // etcd 瞬时不可用：整体保留旧缓存（G 与归属保持一致），宁可旧
        // 不可无。否则 G 会落 0 覆盖掉已缓存的正确值。
        warn_stale_if_seen();
        return;
    }
    if (meta.slot_count != SlotCount()) {
        LOG(ERROR) << "CvmController ring metadata slot count mismatch: cluster="
                   << meta.slot_count << ", local=" << SlotCount()
                   << ", keeping the previous routing view";
        warn_stale_if_seen();
        return;
    }
    uint32_t group_count = meta.slot_group_count;
    if (group_count == 0) {
        group_count = 1;  // 防御：G=0 非法，按退化单主处理
    }
    if (group_count > SlotCount()) {
        LOG(ERROR) << "CvmController invalid ring metadata: groups="
                   << group_count << ", slot_count=" << SlotCount()
                   << ", keeping the previous routing view";
        warn_stale_if_seen();
        return;
    }

    std::vector<RingSlotAssign> assigns;
    if (EtcdViewStore::LoadAllRingSlotAssigns(config_.cluster_namespace,
                                              assigns,
                                              version) != ErrorCode::OK) {
        // 新集群尚无 ring_slots 记录属预期（P2 shadow write 未启用时
        // 恒空），Load 失败也保留旧缓存——宁可旧，不可无。
        warn_stale_if_seen();
        return;
    }
    if (!assigns.empty()) {
        // 模型启用标记（只置位不回落）：数据面幽灵写告警的门控依据。
        ring_slots_model_seen_.store(true, std::memory_order_relaxed);
    }

    std::map<uint32_t, RingSlotAssign> next;
    for (auto& a : assigns) {
        next.emplace(a.rank, std::move(a));
    }

    std::lock_guard<std::mutex> lock(ring_slots_mutex_);
    // 归属变更可见性（仅变化时打印，锁内 diff 防并发交错误报）：
    //   模型启用 / 记录消失 → 单行事件日志；
    //   两者皆非空 → 聚合 rank 级 old→new（晋升 CAS / reshard / 人工改
    //   写的唯一观测点，G 条一行汇总，不逐 slot、不随 5s 周期刷屏）。
    if (!next.empty() && ring_slots_cache_.empty()) {
        LOG(INFO) << "CvmController ring_slots model active: master_id="
                  << config_.master_id << ", groups=" << group_count
                  << ", ranks=" << next.size();
    } else if (next.empty() && !ring_slots_cache_.empty()) {
        LOG(WARNING) << "CvmController ring_slots records vanished "
                        "(rollback or cleanup): master_id="
                     << config_.master_id
                     << ", previous_ranks=" << ring_slots_cache_.size();
    } else {
        std::vector<std::string> changes;
        for (const auto& [rank, a] : next) {
            const auto it = ring_slots_cache_.find(rank);
            if (it == ring_slots_cache_.end()) {
                changes.push_back(std::to_string(rank) + ":+" +
                                  a.primary_id);
            } else if (it->second.primary_id != a.primary_id) {
                changes.push_back(std::to_string(rank) + ":" +
                                  it->second.primary_id + "->" +
                                  a.primary_id);
            }
        }
        for (const auto& [rank, a] : ring_slots_cache_) {
            if (!next.count(rank)) {
                changes.push_back(std::to_string(rank) + ":" +
                                  a.primary_id + "->(removed)");
            }
        }
        if (!changes.empty()) {
            LOG(INFO) << "CvmController ring_slots ownership change: "
                         "master_id="
                      << config_.master_id << ", groups=" << group_count
                      << ", [" << JoinIds(changes) << "]";
        }
    }
    ring_slot_group_count_ = group_count;
    ring_slots_cache_ = std::move(next);
    // 与缓存同一临界区同步 revision（LoadAllRingSlotAssigns 的 range read
    // header ModRevision），保证 route_epoch 门控数据（cvm_ring_revision_）
    // 与 assigns 视图一致；Load 失败路径在上方提前 return，旧值随旧缓存一并
    // 保留（宁可旧，不可无——与 G/ranks 的失败语义对齐）。
    ring_slots_revision_ = version;
}

int64_t CvmController::GetCachedRingSlotsRevision() {
    std::lock_guard<std::mutex> lock(ring_slots_mutex_);
    return ring_slots_revision_;
}

bool CvmController::GetCachedRingSlotAssign(uint32_t rank,
                                            RingSlotAssign& out) {
    std::lock_guard<std::mutex> lock(ring_slots_mutex_);
    const auto it = ring_slots_cache_.find(rank);
    if (it == ring_slots_cache_.end()) {
        return false;
    }
    out = it->second;
    return true;
}

uint32_t CvmController::GetCachedSlotGroupCount() {
    std::lock_guard<std::mutex> lock(ring_slots_mutex_);
    return ring_slot_group_count_;
}

bool CvmController::HasCachedRingSlotAssigns() {
    std::lock_guard<std::mutex> lock(ring_slots_mutex_);
    return !ring_slots_cache_.empty();
}

// ---- RingSlotsView（关系查询实现，语义与原散落谓词逐一对照）----

bool RingSlotsView::is_primary(const std::string& id) const {
    for (const auto& a : assigns_) {
        if (a.primary_id == id) return true;
    }
    return false;
}

bool RingSlotsView::is_migration_target(const std::string& id) const {
    // 仅 kMigrating 状态认 migrating_to（与原散落谓词的状态门控一致，
    // 防 kStable 残留值误判——当前 CAS 切 kStable 时强制清空该字段，
    // 门控为防御性保留）。
    for (const auto& a : assigns_) {
        if (static_cast<SlotState>(a.state) == SlotState::kMigrating &&
            a.migrating_to_id == id) {
            return true;
        }
    }
    return false;
}

bool RingSlotsView::is_standby(const std::string& id,
                               uint32_t* out_rank) const {
    for (const auto& a : assigns_) {
        for (const auto& s : a.standby_ids) {
            if (s == id) {
                if (out_rank) *out_rank = a.rank;
                return true;
            }
        }
    }
    return false;
}

bool RingSlotsView::is_assigned(const std::string& id) const {
    for (const auto& a : assigns_) {
        if (a.primary_id == id) return true;
        for (const auto& s : a.standby_ids) {
            if (s == id) return true;
        }
    }
    return is_migration_target(id);  // 含 kMigrating 状态门控
}

std::string RingSlotsView::paired_primary_of(const std::string& id) const {
    for (const auto& a : assigns_) {
        for (const auto& s : a.standby_ids) {
            if (s == id) return a.primary_id;
        }
    }
    return {};
}

RingSlotsView CvmController::BuildRingSlotsView() {
    std::lock_guard<std::mutex> lock(ring_slots_mutex_);
    std::vector<RingSlotAssign> snapshot;
    snapshot.reserve(ring_slots_cache_.size());
    for (const auto& [rank, assign] : ring_slots_cache_) {
        snapshot.push_back(assign);  // map 按序遍历 = rank 升序
    }
    // assigns 与 G 同锁拷入快照，消费方不会观测到两者错配。
    return RingSlotsView(std::move(snapshot), ring_slot_group_count_,
                         static_cast<uint64_t>(ring_slots_revision_));
}

void CvmController::KeepaliveLoop() {
    (void)EtcdHelper::WaitKeepAliveReady(lease_id_, kKeepAliveReadyTimeoutMs);
    ErrorCode rc = EtcdHelper::KeepAlive(lease_id_);
    if (rc == ErrorCode::ETCD_OPERATION_ERROR) {
        LOG(WARNING) << "CvmController keepalive error: " << rc;
    }
}

bool CvmController::LoadRankedMembers(
    std::vector<MasterRegistration>& out) {
    std::vector<MasterRegistration> masters;
    ViewVersionId version = 0;
    ErrorCode err = EtcdViewStore::LoadAllMasters(config_.cluster_namespace,
                                                  masters, version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmController load masters failed: " << err
                     << ", master_id=" << config_.master_id;
        return false;
    }

    // 收集所有存活 master（含本机）作为候选，按「先到先得」排序：etcd
    // create_revision 升序（谁先注册谁排前），revision 缺失时回退 master_id。
    // create_revision 由 etcd 全局单调分配，不存在本地时钟偏差，节点/客户端
    // 可字节级一致地推导出相同 primary 序列（见 MasterRegistrationRankLess）。
    out.clear();
    out.reserve(masters.size());
    for (const auto& m : masters) {
        if (!m.master_id.empty()) {
            out.push_back(m);
        }
    }

    std::sort(out.begin(), out.end(), MasterRegistrationRankLess);
    return true;
}

void CvmController::LogMembershipChange(
    const std::vector<MasterRegistration>& members) {
    std::vector<std::string> ids;
    ids.reserve(members.size());
    for (const auto& m : members) {
        if (!m.master_id.empty()) {
            ids.push_back(m.master_id);
        }
    }
    // LoadRankedMembers 已按 create_revision 排序，ids 继承该顺序。成员集未变则不打
    // 日志（watch 可能因等值事件重复唤醒）。
    if (ids == last_member_ids_) {
        return;
    }

    std::set<std::string> prev(last_member_ids_.begin(), last_member_ids_.end());
    std::set<std::string> cur(ids.begin(), ids.end());
    std::vector<std::string> joined;
    std::vector<std::string> left;
    for (const auto& id : ids) {
        if (!prev.count(id)) {
            joined.push_back(id);
        }
    }
    for (const auto& id : last_member_ids_) {
        if (!cur.count(id)) {
            left.push_back(id);
        }
    }

    const size_t primary_count =
        std::min<size_t>(ids.size(), config_.submaster_count);
    std::vector<std::string> primaries(ids.begin(),
                                       ids.begin() + primary_count);

    LOG(INFO) << "CvmController membership change: master_id="
              << config_.master_id << ", joined=" << JoinIds(joined)
              << ", left=" << JoinIds(left)
              << ", primaries=" << JoinIds(primaries);

    last_member_ids_ = std::move(ids);
}

MasterRole CvmController::ComputeDesiredRole() {
    // ring_slots 归属优先（§16.15.2）：本机是某 rank 的 primary_id 即
    // kPrimary；是某 rank 的配对 standby 即 kStandby。归属是显式状态，
    // 不随成员集排序位置漂移（环错位根因被切断）。
    {
        const RingSlotsView view = BuildRingSlotsView();
        if (view.active()) {
            if (view.is_primary(config_.master_id)) {
                return MasterRole::kPrimary;
            }
            // 迁移目标（§16.15.3 原生认领 / §16.8.2 扩容热迁移）：本机是
            // 某 rank 的 migrating_to 目标 → 判 kPrimary，使 supervisor
            // 进入 serve 阶段拉起 inter-master RPC / reshard driver，驱动
            // 状态机收敛（intent 持久化，断点续传）。此时 primary_id 尚未
            // 指向本机，slot 心跳不发布该段，不会提前对外服务；owner CAS
            // 由 driver 完成后才真正接管。
            if (view.is_migration_target(config_.master_id)) {
                return MasterRole::kPrimary;
            }
            if (view.is_standby(config_.master_id)) {
                return MasterRole::kStandby;
            }
            // ring_slots 有记录但本机不在任何归属中：两种情形统一判 standby——
            // ① 新成员尚未初始分配（§16.15.3，分配前不应持有 primary 角色）；
            // ② 曾是 primary 的本机已失秩（晋升 CAS 被别的 standby 抢走 /
            // split-brain 恢复），须降级（§16.15.8 幽灵写防护）。
            return MasterRole::kStandby;
        }
        // 缓存空（新集群 shadow write 未启用）：回退现行排序位置推导。
    }

    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members)) {
        return current_role_.load();
    }

    for (size_t i = 0; i < members.size(); ++i) {
        if (members[i].master_id == config_.master_id) {
            return i < config_.submaster_count ? MasterRole::kPrimary
                                               : MasterRole::kStandby;
        }
    }

    // 本机不在成员集（未注册/异常）：保守保持当前角色。
    return current_role_.load();
}

// Admin Metrics 拉取式低频只读视图：计数口径与 JoinAsStandby 一致
//（standby_ids 中任一成员存活即视为已配对）。uncovered 只度量
// 「primary 存活但零存活 standby」的稳态缺口；primary 死亡与迁移中
// 的 rank 属瞬态，单列不计入。
std::string CvmController::GetStandbyCoverageSummary() {
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members) || members.empty()) {
        return "unavailable";
    }
    const RingSlotsView view = BuildRingSlotsView();
    if (!view.active()) {
        return "inactive";
    }
    int total = 0, covered = 0, primary_dead = 0, migrating = 0;
    for (const auto& a : view.assigns()) {
        ++total;
        if (a.primary_id.empty() || !IsMemberAlive(members, a.primary_id)) {
            ++primary_dead;
            continue;
        }
        if (static_cast<SlotState>(a.state) == SlotState::kMigrating) {
            ++migrating;
            continue;
        }
        bool has_live_standby = false;
        for (const auto& sid : a.standby_ids) {
            if (IsMemberAlive(members, sid)) {
                has_live_standby = true;
                break;
            }
        }
        if (has_live_standby) {
            ++covered;
        }
    }
    return "ranks=" + std::to_string(total) + ", covered=" +
           std::to_string(covered) + ", uncovered=" +
           std::to_string(total - covered - primary_dead - migrating) +
           ", primary_dead=" + std::to_string(primary_dead) +
           ", migrating=" + std::to_string(migrating);
}

std::string CvmController::GetPrimaryAddress() {
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members) || members.empty()) {
        return "";
    }
    // 排名第一的成员是集群中最早的 primary。若本机就是它，则无需（也不能）
    // 把自己当作回放源，返回空字符串让调用方保持纯 standby。
    if (members.front().master_id == config_.master_id) {
        return "";
    }
    return members.front().address;
}

std::vector<MasterRegistration> CvmController::GetBindingSources() {
    // ring_slots 配对优先（§16.15.1）：本机是某 rank 的配对 standby 时，
    // 回放源即该 rank 的 primary（1:1 配对，晋升即继承）。全量回放仅
    // 在旧模型下才需要。
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members) || members.empty()) {
        return {};
    }

    const RingSlotsView view = BuildRingSlotsView();
    if (view.active() && !view.is_primary(config_.master_id)) {
        const std::string paired_primary_id =
            view.paired_primary_of(config_.master_id);
        if (!paired_primary_id.empty() &&
            paired_primary_id != config_.master_id) {
            for (const auto& m : members) {
                if (m.master_id == paired_primary_id) {
                    LOG(INFO) << "CvmController standby pairing (ring_slots): "
                                 "master_id="
                              << config_.master_id << ", paired_primary="
                              << paired_primary_id;
                    return {m};
                }
            }
            // 配对 primary 已死（尚未晋升）：暂无回放源，等 ReconcileRole 晋升。
            return {};
        }
    }
    // 缓存空 / 本机不在配对中：回退现行均分区间 + 环推导。

    const size_t primary_count =
        std::min<size_t>(members.size(), config_.submaster_count);

    // 本机在「先到先得」（create_revision 稳定排序）列表中的位置。
    size_t my_index = members.size();
    for (size_t i = 0; i < members.size(); ++i) {
        if (members[i].master_id == config_.master_id) {
            my_index = i;
            break;
        }
    }
    // 未注册 / 本机已是 primary：不作为 standby 回放。
    if (my_index == members.size() || my_index < primary_count) {
        return {};
    }

    const size_t standby_count = members.size() - primary_count;
    if (standby_count == 0) {
        return {};
    }
    const size_t standby_rank = my_index - primary_count;

    // 本 standby 负责的 slot 区间 [start, end)，与其它 standby 均分。
    const uint16_t start = static_cast<uint16_t>(standby_rank * SlotCount() /
                                                 standby_count);
    const uint16_t end = static_cast<uint16_t>((standby_rank + 1) * SlotCount() /
                                               standby_count);

    // 本地一致性哈希环：primary_ids = 排序去重后的前 submaster_count 个成员，
    // 与服务端/客户端完全一致（slot_hash.h）。
    std::vector<std::string> primary_ids;
    primary_ids.reserve(primary_count);
    for (size_t i = 0; i < primary_count; ++i) {
        primary_ids.push_back(members[i].master_id);
    }

    // 求出负责区间内每个 slot 的 primary owner（确定性推导）。
    std::set<std::string> owner_ids;
    for (uint16_t slot = start; slot < end; ++slot) {
        const std::string owner = ResolveSlotOwnerOnRing(primary_ids, slot);
        if (!owner.empty() && owner != config_.master_id) {
            owner_ids.insert(owner);
        }
    }

    // 映射 owner master_id -> MasterRegistration（含 address）。
    std::vector<MasterRegistration> sources;
    sources.reserve(owner_ids.size());
    for (const auto& m : members) {
        if (owner_ids.count(m.master_id)) {
            sources.push_back(m);
        }
    }
    if (!sources.empty()) {
        LOG(INFO) << "CvmController standby binding done: master_id="
                  << config_.master_id << ", slot_range=[" << start << ","
                  << end << "), source_count=" << sources.size();
    }
    return sources;
}

std::vector<MasterRegistration> CvmController::GetRingStandbySources() {
    // 环互备职责源（互备 1.3.1）：遍历 ring 视图，凡 standby_ids 含本机
    // master_id 的 rank，取该 rank 的 primary 作为回放源（OpLog 流命名空间
    // = primary master_id，MasterSource 由 supervisor 层转换）。角色不过滤
    //（双角色语义：本机 serving 自己 rank 的同时跟随他 rank 流）。
    // 防自配对防御性排除（CAS 端 JoinAsStandbyRing 已有防御，此处兜底）。
    // members 即存活全集（lease 驱动）：primary 已死的 rank 自然缺席——
    // 其流不再推进，跟随无意义，晋升窗口由互备 1.4 处理。
    // 供低频拉取（2-5s）：LoadRankedMembers + BuildRingSlotsView 均为
    // live etcd 读，勿在请求热路径调用。
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members) || members.empty()) {
        return {};
    }
    const RingSlotsView view = BuildRingSlotsView();
    if (!view.active()) {
        return {};  // 环模型未启用 / 记录为空：无职责
    }
    std::vector<MasterRegistration> sources;
    // OpLog 流按 primary master_id 命名（非按 rank）：caretaker 多 rank
    // 共享同一 primary 时去重——同一流跟随一次即覆盖其全部 rank，
    // 晋升物化由 P4 owned-slot 过滤按 rank 裁剪。
    std::set<std::string> seen_primaries;
    for (const auto& a : view.assigns()) {
        bool mine = false;
        for (const auto& sid : a.standby_ids) {
            if (sid == config_.master_id) {
                mine = true;
                break;
            }
        }
        if (!mine || a.primary_id.empty() ||
            a.primary_id == config_.master_id) {
            continue;  // 非本机职责 / 空 primary / 防自配对
        }
        if (!seen_primaries.insert(a.primary_id).second) {
            continue;  // 同一 primary 的流已入源：去重
        }
        for (const auto& m : members) {
            if (m.master_id == a.primary_id) {
                sources.push_back(m);  // 仅存活 primary 入源
                break;
            }
        }
    }
    return sources;
}

std::vector<CvmController::StagedRingAdoption>
CvmController::FindStagedRingAdoptions() {
    // 互备 1.4 ②：枚举 ring-adopt staging rank（staging 签名 = primary=
    // 本机 且 kMigrating 且 migrating_to 为空——reshard 阶段 1 的
    // migrating_to=目标、迁出 barrier 的 primary=源，均不落入此签名）。
    // 供 supervisor RingStandbyRuntime 低频拉取（5s，与 GetRingStandby
    // Sources 同轮询），staging 集稳态为空，零开销。视图为锁内快照，
    // dead primary 记录单独加锁拷贝（写入方 ReconcileRole 线程）。
    std::vector<StagedRingAdoption> out;
    const RingSlotsView view = BuildRingSlotsView();
    if (!view.active()) {
        return out;  // 环模型未启用：无 staging 可能
    }
    std::map<uint32_t, std::string> staged;
    {
        std::lock_guard<std::mutex> lock(ring_slots_mutex_);
        staged = staged_adoption_source_;
    }
    for (const auto& a : view.assigns()) {
        if (a.primary_id != config_.master_id ||
            static_cast<SlotState>(a.state) != SlotState::kMigrating ||
            !a.migrating_to_id.empty()) {
            continue;
        }
        StagedRingAdoption item;
        item.rank = a.rank;
        item.epoch = a.epoch;
        const auto it = staged.find(a.rank);
        if (it != staged.end()) {
            item.dead_primary_id = it->second;
        }
        out.push_back(std::move(item));
    }
    return out;
}

ErrorCode CvmController::FinalizeRingAdoption(
    uint32_t rank, uint64_t expected_epoch,
    const std::string& dead_primary_id) {
    // 互备 1.4 ③：物化完成后 kMigrating→kStable 收尾。epoch CAS 单赢家：
    // 检测后 epoch 已被并发推进时返回 STALE_ROUTE（他方收尾 / 管理员
    // reshard / 领养者失秩让位），调用方静默接受——下轮检测按新视图重判。
    // standby 列表保留（nullptr）：P3 不做补位（§16.15.3），原列表在
    // staging CAS 时已去掉本机。dead_primary_id 仅入日志语境（old_owner）。
    RingSlotAssign out;
    const ErrorCode err = EtcdViewStore::AdoptRankViaCAS(
        config_.cluster_namespace, rank, expected_epoch, config_.master_id,
        SlotState::kStable, "", out, nullptr, "ring-adopt-complete",
        dead_primary_id);
    if (err == ErrorCode::OK) {
        std::lock_guard<std::mutex> lock(ring_slots_mutex_);
        staged_adoption_source_.erase(rank);
    }
    return err;
}

bool CvmController::StandbyMutualPairingEnabled() {
    // 公有静态门面：转发匿名命名空间进程级缓存实现。调用方——
    // TryAssignRankForSelf 已归属门控（互备 1.1）与 supervisor
    // RingStandbyRuntime::Start（互备 1.3.2）——两侧始终同值。
    return MutualPairingToggleEnabled();
}

bool CvmController::TryPromoteDeadPrimaryRanks(
    const std::vector<MasterRegistration>& members) {
    // 快照视图。缓存空 = ring_slots 模型未启用。
    const RingSlotsView view = BuildRingSlotsView();
    if (!view.active()) {
        return false;
    }

    const auto now = std::chrono::steady_clock::now();

    // 晋升路径可观测性统计（出口 LogStepOutcome 汇总，VLOG(1) 逐候选）。
    int alive_primary = 0, dead_no_standby = 0, yield_higher = 0,
        timeout_bypass = 0;
    for (const auto& a : view.assigns()) {
        if (a.primary_id.empty() || a.primary_id == config_.master_id) {
            continue;
        }
        // 时间戳维护（ring_slots_mutex_ 下，与缓存同一临界区语义）：
        // primary 存活 → 清除；死亡 → 记录首次观测时刻。
        // dead_for_secs：锁内算好带出，避免锁外访问 map。
        bool dead_long_enough = false;
        long dead_for_secs = 0;
        bool first_observed = false;
        {
            std::lock_guard<std::mutex> lock(ring_slots_mutex_);
            if (IsMemberAlive(members, a.primary_id)) {
                ++alive_primary;  // 锁内仅计数（VLOG/日志出口在锁外统一）
                dead_primary_since_.erase(a.rank);
                continue;  // primary 存活，无晋升诉求
            }
            auto it = dead_primary_since_.find(a.rank);
            if (it == dead_primary_since_.end()) {
                it = dead_primary_since_
                         .emplace(a.rank, now)
                         .first;
                first_observed = true;
            }
            dead_for_secs =
                std::chrono::duration_cast<std::chrono::seconds>(now -
                                                                 it->second)
                    .count();
            dead_long_enough = now - it->second >= kDeadPrimaryTakeoverTimeout;
        }

        // §16.18.1：standby_ids 按晋升优先级升序，取第一个存活者。
        const std::string* candidate = nullptr;
        for (const auto& sid : a.standby_ids) {
            if (IsMemberAlive(members, sid)) {
                candidate = &sid;
                break;
            }
        }
        if (candidate == nullptr) {
            // 无存活 standby：本机非该 rank 配对成员，不越权接管——未配对
            // 节点没有该 rank 的回放数据，CAS 接管 = 空元数据数据丢失
            // （正是 §16 要修的缺陷）。该 rank 由同周期后续的补位/兼管自愈
            // 路径处理（TryAssignRankForSelf ① 待命者优先 / 兼管者兜底，
            // §16.18.3 / §16.8.3）。首次观测告警一次，不随周期刷屏。
            ++dead_no_standby;
            VLOG(1) << "promote skip rank=" << a.rank
                    << ": no alive standby (awaiting designated takeover)";
            if (first_observed) {
                LOG(WARNING) << "CvmController rank has dead primary and "
                                "no alive standby: master_id="
                             << config_.master_id << ", rank=" << a.rank
                             << ", dead_primary=" << a.primary_id
                             << " (awaiting designated takeover)";
            }
            continue;
        }
        if (*candidate != config_.master_id) {
            if (!dead_long_enough) {
                ++yield_higher;
                VLOG(1) << "promote skip rank=" << a.rank
                        << ": higher-priority standby " << *candidate
                        << " alive (not yet timed out)";
                continue;  // 高优先 standby 存活且未超时：让它先抢
            }
            ++timeout_bypass;
            // 超时兜底（liveness backstop）：高优先 standby 活着但晋升
            // 逻辑卡死（keepalive 存活、ReconcileRole 挂起），越过优先级
            // 直接接管。epoch CAS 保证单赢家，被越者失秩自降级。
            LOG(WARNING) << "CvmController takeover past standby priority "
                            "(timeout): master_id="
                         << config_.master_id << ", rank=" << a.rank
                         << ", dead_primary=" << a.primary_id
                         << ", dead_for=" << dead_for_secs << "s";
        }
        // 本机即该 rank 最高优先存活 standby：正常晋升路径。

        // 晋升后的 standby 列表：原序去掉本机（P3 不做补位，§16.15.3）。
        std::vector<std::string> new_standbys;
        new_standbys.reserve(a.standby_ids.size());
        for (const auto& sid : a.standby_ids) {
            if (sid != config_.master_id) {
                new_standbys.push_back(sid);
            }
        }

        // 互备 1.4 staging 门控：环互备开关下「已归属 serving 成员」
        //（本机是某存活 rank 的 primary）领养额外 rank——角色不变
        //（kPrimary→kPrimary），supervisor 升级序列不触发，直接
        // kStable 发布即空元数据 serve。改走 staging-first：CAS
        // kMigrating（migrating_to 留空 = ring-adopt 标记）——写屏障立即
        // 生效且 ImportSlotMetadata 暂缓就绪，物化（OpLog 流重放，复用
        // 冷接管通道）与 kStable 收尾由 supervisor RingStandbyRuntime
        // 编排（FindStagedRingAdoptions / FinalizeRingAdoption）。
        // 纯 standby 领养者（is_primary=false）走原 kStable 直抢——角色
        // 翻转 kStandby→kPrimary 由 supervisor 升级序列在 serve 前物化。
        // 开关 OFF 时保持原语义（1.4 未全量部署前行为不变）。
        const bool serving_adopter =
            StandbyMutualPairingEnabled() &&
            view.is_primary(config_.master_id);

        // CAS 抢 owner（§16.15.4：晋升先于 serving）。非 staging 路径
        // state 强制 kStable：源在 kMigrating 中挂掉的 reshard 即中止
        //（迁移窗口数据由已安装部分保留，缺口走冷启动重建），残留 intent
        // 由 AdoptRankViaCAS 幂等删除（§16.16.7），需要迁移由管理员重新
        // 发起。staging 路径同取 clear_intent=true（接管语义）。
        RingSlotAssign out;
        const ErrorCode err = EtcdViewStore::AdoptRankViaCAS(
            config_.cluster_namespace, a.rank, a.epoch, config_.master_id,
            serving_adopter ? SlotState::kMigrating : SlotState::kStable,
            "", out, &new_standbys,
            serving_adopter ? "ring-adopt" : "promoted", a.primary_id);
        if (err == ErrorCode::OK) {
            if (serving_adopter) {
                // 记录被领养 rank 的死 primary（物化重放的流命名空间），
                // 供 RingStandbyRuntime 检测消费；FinalizeRingAdoption
                // 清除。与 dead_primary_since_ 同锁同线程家族。
                std::lock_guard<std::mutex> lock(ring_slots_mutex_);
                staged_adoption_source_[a.rank] = a.primary_id;
            }
            LogStepOutcome(kPhasePromote, kStepDone,
                           "rank=" + std::to_string(a.rank) +
                               (serving_adopter ? " ring-adopt staged (dead primary "
                                                : " promoted (dead primary ") +
                               a.primary_id +
                               (serving_adopter
                                    ? ")"
                                    : ", timeout_bypass=" +
                                          std::to_string(timeout_bypass) +
                                          ")"));
            return true;
        }
        if (err == ErrorCode::STALE_ROUTE) {
            LogStepOutcome(kPhasePromote, kStepNoCandidate,
                           "rank=" + std::to_string(a.rank) +
                               " lost CAS race (another standby promoted)");
            return true;  // 别的 standby 已抢到：正常竞争，本机保持 standby
        }
        LogStepOutcome(kPhasePromote, kStepAbort,
                       "rank=" + std::to_string(a.rank) +
                           " promotion CAS failed (see ownership log)");
        return false;
    }
    LogStepOutcome(kPhasePromote, kStepNoCandidate,
                   "scanned=" + std::to_string(view.assigns().size()) +
                       ", alive_primary=" + std::to_string(alive_primary) +
                       ", dead_no_standby=" +
                       std::to_string(dead_no_standby) +
                       ", yield_higher=" + std::to_string(yield_higher) +
                       ", timeout_bypass=" +
                       std::to_string(timeout_bypass));
    return false;
}

bool CvmController::TryBootstrapRingSlots() {
    // 新集群冷启动 bootstrap（§16.15.3 关键段）：本机是唯一存活成员 →
    // 创建全 G 个 rank——rank0 原生持有，rank1..G-1 由本机兼管
    // （primary_id 相同，§16.8.1 隐含判定），保证全部 slot 自始有
    // owner。G = submaster_count（部署常量，§16.8 前提）随 cluster_meta
    // 一并定型，此后恒不变（冷操作才能改）。
    const uint32_t group_count =
        config_.submaster_count > 0 ? config_.submaster_count : 1;

    RingMeta meta;
    meta.submaster_count = config_.submaster_count;
    meta.slot_group_count = group_count;
    meta.slot_count = SlotCount();
    const ErrorCode meta_err = EtcdViewStore::SaveClusterMeta(
        config_.cluster_namespace, meta);
    if (meta_err != ErrorCode::OK) {
        LOG(WARNING) << "CvmController bootstrap SaveClusterMeta failed: "
                        "err="
                     << meta_err;
        return false;
    }

    for (uint32_t rank = 0; rank < group_count; ++rank) {
        RingSlotAssign assign;
        assign.rank = rank;
        assign.primary_id = config_.master_id;
        assign.state = static_cast<int32_t>(SlotState::kStable);
        assign.epoch = 1;  // CreateRingSlotAssign 守卫：epoch=0 视为未设置
        const ErrorCode err = EtcdViewStore::CreateRingSlotAssign(
            config_.cluster_namespace, assign);
        if (err == ErrorCode::OK) {
            continue;
        }
        if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
            // 并发 bootstrap（他机同时判定自己唯一存活）：kKeyNotExists
            // 保证单赢家。rank0 被他人创建 → 本机退出 bootstrap，按普通
            // 补位路径走；rank>0 失败 → 他人已持有，跳过即可。
            if (rank == 0) {
                LOG(WARNING) << "CvmController bootstrap lost rank0 race, "
                                "falling back to assignment path";
                return false;
            }
            continue;
        }
        LOG(WARNING) << "CvmController bootstrap create rank=" << rank
                     << " failed: err=" << err;
        return true;  // 部分成功也需刷新缓存；缺口由缺 rank 补建路径修复
    }
    LOG(INFO) << "CvmController bootstrap ring_slots: master_id="
              << config_.master_id << ", groups=" << group_count
              << " (rank0 native, rank1..G-1 caretaker-held)";
    return true;
}

void CvmController::LogStepOutcome(int phase, int outcome,
                                   const std::string& detail) {
    // 验证辅助（§16.23）：阶段结果变更才打 INFO（含跳过统计），稳态
    // 静默防刷屏——补位阶段每 ReconcileRole 周期（5s + watch 事件）都
    // 会被调用，无条件出口打印 = 每节点每 5s ~10 行噪音。
    static const char* kPhaseNames[] = {"repair-missing-ranks",
                                        "claim-dead-primary", "claim-caretaker",
                                        "join-standby", "promote-dead-primary",
                                        "designated-takeover",
                                        "join-standby-ring"};
    static const char* kOutcomeNames[] = {"no-candidate", "done", "abort"};
    const int last =
        last_phase_outcome_[phase].load(std::memory_order_relaxed);
    if (last == outcome) {
        return;
    }
    last_phase_outcome_[phase].store(outcome, std::memory_order_relaxed);
    LOG(INFO) << "CvmController assignment phase [" << kPhaseNames[phase]
             << "] " << (last < 0 ? "first-observed" : "changed")
             << ": outcome=" << kOutcomeNames[outcome]
             << ", master_id=" << config_.master_id << ", " << detail;
}

bool CvmController::TryAssignRankForSelf(
    const std::vector<MasterRegistration>& members) {
    // 快照视图（含 G，单锁一致）。缓存空 = ring_slots 模型未启用。
    const RingSlotsView view = BuildRingSlotsView();
    const uint32_t group_count = view.group_count();

    if (!view.active()) {
        const bool sole_member =
            members.size() == 1 &&
            members.front().master_id == config_.master_id;
        if (sole_member) {
            return TryBootstrapRingSlots();
        }
        // 多成员在场（旧集群灰度门控 or 全新集群同时启动竞态）。两者
        // 无法可靠区分（无记录 = §16.17.6 灰度语义），统一处理：
        // 首次观测随机退避 10-30s 再重判——竞态中的另一节点若启动失败
        // 退出 / lease 过期，幸存者退避期满重判即成唯一成员，正常
        // bootstrap；稳定多成员（真旧集群）退避期满后维持门控不启用，
        // 仅首次 WARNING 提示（新集群部署应先起单节点）。
        if (!bootstrap_defer_armed_.exchange(true,
                                             std::memory_order_relaxed)) {
            std::random_device rd;
            const int defer_sec =
                10 + rd() % 21;  // 均匀随机 [10, 30]s
            const int64_t now_ms = std::chrono::duration_cast<
                std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
            bootstrap_defer_until_ms_.store(now_ms + defer_sec * 1000,
                                            std::memory_order_relaxed);
            LOG(INFO) << "CvmController multi-master with no ring_slots "
                         "records, deferring bootstrap decision by "
                      << defer_sec
                      << "s (startup race backoff): master_id="
                      << config_.master_id
                      << ", alive_members=" << members.size();
            return false;
        }
        const int64_t now_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
        if (now_ms < bootstrap_defer_until_ms_.load(
                        std::memory_order_relaxed)) {
            return false;  // 退避窗口内：暂不判定（给竞态留收敛时间）
        }
        // 退避期满仍多成员：维持灰度门控（旧集群安全），仅首次提示。
        if (!multi_member_no_record_warned_.exchange(
                true, std::memory_order_relaxed)) {
            LOG(WARNING)
                << "CvmController ring_slots model not enabled: "
                   "multiple masters alive and no ownership records. "
                   "If this is a NEW cluster, bootstrap was skipped "
                   "because masters started concurrently -- restart the "
                   "first node alone (or all but one) to let it "
                   "bootstrap, then scale out. If this is an existing "
                   "cluster upgraded in place, this legacy-ring "
                   "fallback is expected and safe: master_id="
                << config_.master_id << ", alive_members="
                << members.size();
        }
        return false;
    }

    // 阶段 0：缺 rank 记录防御性补建（bootstrap 中途失败 / 人工误删）。
    bool changed = false;
    if (group_count > 0 && view.assigns().size() < group_count) {
        changed = RepairMissingRankRecords(view, group_count);
    }

    if (view.is_assigned(config_.master_id)) {
        // 已有归属（含迁移目标）：不参与 ①/② 接管（接管面收缩到未归属
        // 待命池 + standby 晋升，防一成员故障引发全员接管风暴）。环互备
        // 配对（互备 1.1）挂在 MC_STANDBY_MUTUAL_PAIRING 开关后面：
        // 回放运行时（1.3）未部署前必须保持 OFF——空 standby_ids 记录
        // 会被 TryPromote 优先晋升而导出空元数据（正确性闸门）。
        if (!StandbyMutualPairingEnabled()) {
            return changed;
        }
        // 指派候选者由确定性环拓扑推导（全集群对同一 rank 唯一写者），
        // 无 CAS 竞争，故无需「最早的未归属成员」轮次门控。
        const int step = JoinAsStandbyRing(view, members, group_count);
        return step == kStepDone ? true : changed;
    }

    // 「排序最早的未归属存活成员」门控（§16.18.2）：按 rank 升序逐个补，
    // 避免多成员并发 CAS 冲突；轮空者下一周期重判（幂等）。门控开合的
    // 变更触发日志（验证辅助）：grant/lost 各打一次，稳态静默。
    bool my_turn = false;
    for (const auto& m : members) {
        if (!view.is_assigned(m.master_id)) {
            my_turn = (m.master_id == config_.master_id);
            break;
        }
    }
    const bool prev_turn =
        last_my_turn_.exchange(my_turn, std::memory_order_relaxed);
    if (my_turn != prev_turn) {
        LOG(INFO) << "CvmController assignment gate: "
                  << (my_turn
                          ? "turn granted (earliest unassigned alive member)"
                          : "turn lost")
                  << ", master_id=" << config_.master_id;
    }
    if (!my_turn) {
        return changed;
    }

    // 阶段 ①→②→③（§16.18.2 优先级）：每个阶段独立三态；kStepAbort /
    // kStepDone 都终止本周期（单周期单次动作），kStepNoCandidate 落到
    // 下一阶段。全满配时进入待命池（不配对、不回放）。
    const int step1 = ClaimDeadPrimaryRank(view, members);
    if (step1 != kStepNoCandidate) {
        return step1 == kStepDone ? true : changed;
    }
    const int step2 = ClaimCaretakerRank(view, members);
    if (step2 != kStepNoCandidate) {
        return step2 == kStepDone ? true : changed;
    }
    const int step3 = JoinAsStandby(view, members);
    if (step3 != kStepNoCandidate) {
        return step3 == kStepDone ? true : changed;
    }
    return changed;  // 全满配：待命池（不配对、不回放）
}

bool CvmController::RepairMissingRankRecords(const RingSlotsView& view,
                                            uint32_t group_count) {
    // 缺失 rank 由确定性兼管者（前一存在 rank 的 primary；无下界则最小
    // 存在 rank 的 primary）补建，kKeyNotExists 创建天然幂等。仅兼管者
    // 本机执行。返回 true = 有补建（快照不含新 key，调用方需刷新）。
    bool changed = false;
    int missing = 0, repaired = 0;
    std::map<uint32_t, RingSlotAssign> by_rank;
    for (const auto& a : view.assigns()) {
        by_rank.emplace(a.rank, a);
    }
    for (uint32_t r = 0; r < group_count; ++r) {
        if (by_rank.count(r)) {
            continue;
        }
        ++missing;
        // 确定性兼管者（FindCaretakerPrimary 唯一实现，与兼管自愈共用，
        // 防两份推导漂移）。补建路径不查存活（记录存在即可承接，
        // §16.15.3）；上界 group_count 排除跨代残留记录（原实现边界）。
        const std::string caretaker = FindCaretakerPrimary(
            view, r, group_count, /*members=*/nullptr);
        if (caretaker.empty() || caretaker != config_.master_id) {
            VLOG(1) << "assign phase repair skip rank=" << r
                    << ": caretaker=" << caretaker << " (not me)";
            continue;
        }
        RingSlotAssign assign;
        assign.rank = r;
        assign.primary_id = config_.master_id;
        assign.state = static_cast<int32_t>(SlotState::kStable);
        assign.epoch = 1;  // CreateRingSlotAssign 守卫：epoch=0 视为未设置
        const ErrorCode err = EtcdViewStore::CreateRingSlotAssign(
            config_.cluster_namespace, assign);
        if (err == ErrorCode::OK) {
            LOG(WARNING) << "CvmController repaired missing rank record: "
                            "rank="
                         << r << ", caretaker_primary="
                         << config_.master_id;
            changed = true;
            ++repaired;
        } else if (err != ErrorCode::ETCD_TRANSACTION_FAIL) {
            LOG(WARNING) << "CvmController repair create rank=" << r
                         << " failed: err=" << err;
        }
    }
    LogStepOutcome(kPhaseRepair, changed ? kStepDone : kStepNoCandidate,
                   "missing=" + std::to_string(missing) +
                       ", repaired=" + std::to_string(repaired));
    return changed;
}

int CvmController::ClaimDeadPrimaryRank(
    const RingSlotsView& view,
    const std::vector<MasterRegistration>& members) {
    // ① primary 已死/为空的 rank（§16.18.2 步 1 / §16.18.3 待命者优先）：
    // 直接 CAS kStable 接管（冷启动：数据依赖 segment 重建）。有存活 standby
    // 的 rank 一律让位（standby 晋升零搬运优先，§16.15.8——本周期
    // TryPromote 已先行，本路径只兜「无 standby」的空档）。迁移目标存活
    // 的 kMigrating rank 让其 driver 完成接管（含源死亡收尾），不抢。
    int live_primary = 0, target_alive = 0, standby_yield = 0;
    for (const auto& a : view.assigns()) {
        if (!a.primary_id.empty() &&
            IsMemberAlive(members, a.primary_id)) {
            ++live_primary;
            VLOG(1) << "assign phase1 skip rank=" << a.rank
                    << ": live primary " << a.primary_id;
            continue;
        }
        if (static_cast<SlotState>(a.state) == SlotState::kMigrating &&
            IsMemberAlive(members, a.migrating_to_id)) {
            ++target_alive;
            VLOG(1) << "assign phase1 skip rank=" << a.rank
                    << ": migrating target alive " << a.migrating_to_id;
            continue;
        }
        bool has_live_standby = false;
        for (const auto& sid : a.standby_ids) {
            if (IsMemberAlive(members, sid)) {
                has_live_standby = true;
                break;
            }
        }
        if (has_live_standby) {
            ++standby_yield;
            VLOG(1) << "assign phase1 skip rank=" << a.rank
                    << ": live standby exists (promotion path owns it)";
            continue;
        }
        std::vector<std::string> new_standbys;
        for (const auto& sid : a.standby_ids) {
            if (IsMemberAlive(members, sid)) {
                new_standbys.push_back(sid);  // 防御：通常已空
            }
        }
        // 接管以 kStable 覆盖可能的 kMigrating：intent 残留由
        // AdoptRankViaCAS 幂等清理（§16.16.7，同晋升语义）。
        RingSlotAssign out;
        const ErrorCode err = EtcdViewStore::AdoptRankViaCAS(
            config_.cluster_namespace, a.rank, a.epoch, config_.master_id,
            SlotState::kStable, "", out, &new_standbys, "took over (cold)",
            a.primary_id);
        const bool done = err == ErrorCode::OK;
        LogStepOutcome(kPhaseClaimDead, done ? kStepDone : kStepAbort,
                       "rank=" + std::to_string(a.rank) +
                           (done ? " adopted (cold start)"
                                 : " CAS unresolved (see ownership log)"));
        return done ? kStepDone : kStepAbort;
    }
    LogStepOutcome(kPhaseClaimDead, kStepNoCandidate,
                   "scanned=" + std::to_string(view.assigns().size()) +
                       ", live_primary=" + std::to_string(live_primary) +
                       ", migrating_target=" + std::to_string(target_alive) +
                       ", standby_yield=" + std::to_string(standby_yield));
    return kStepNoCandidate;
}

int CvmController::ClaimCaretakerRank(
    const RingSlotsView& view,
    const std::vector<MasterRegistration>& members) {
    // ② 仍被存活兼管者持有的空 rank（primary_id 重复出现在更小 rank，
    // §16.8.1 隐含判定 = 无原生 owner）：写 reshard_intent + CAS kMigrating
    // 发起段级热迁移（§16.8.2）。intent 先行（崩溃安全：CAS 后意图仍在，
    // driver 重启续传）；CAS 成功后 ComputeDesiredRole 认 migrating_to →
    // 本机进入 serve 阶段，reshard driver 接管后续阶段。
    int dead_or_empty = 0, migrating = 0, native_owned = 0;
    for (const auto& a : view.assigns()) {
        if (a.primary_id.empty() ||
            !IsMemberAlive(members, a.primary_id)) {
            ++dead_or_empty;
            VLOG(1) << "assign phase2 skip rank=" << a.rank
                    << ": primary dead or empty";
            continue;
        }
        if (static_cast<SlotState>(a.state) == SlotState::kMigrating) {
            ++migrating;
            VLOG(1) << "assign phase2 skip rank=" << a.rank
                    << ": migration in progress";
            continue;  // 迁移进行中（他方目标）
        }
        bool caretaker_held = false;
        for (const auto& b : view.assigns()) {
            if (b.rank < a.rank && b.primary_id == a.primary_id) {
                caretaker_held = true;
                break;
            }
        }
        if (!caretaker_held) {
            ++native_owned;
            VLOG(1) << "assign phase2 skip rank=" << a.rank
                    << ": natively held by " << a.primary_id;
            continue;  // 原生持有，不可认领
        }
        ReshardIntent intent;
        intent.rank = a.rank;
        intent.source_primary_id = a.primary_id;
        intent.target_primary_id = config_.master_id;
        ErrorCode cerr = EtcdViewStore::CreateReshardIntent(
            config_.cluster_namespace, intent);
        if (cerr == ErrorCode::ETCD_TRANSACTION_FAIL) {
            // 已有意图：与本机认领一致（同源同目标）= 上次断点，续走；
            // 否则他方在处理，让位。
            std::vector<ReshardIntent> all;
            ViewVersionId version = 0;
            bool mine = false;
            if (EtcdViewStore::LoadAllReshardIntents(config_.cluster_namespace,
                                                    all,
                                                    version) ==
                ErrorCode::OK) {
                for (const auto& i : all) {
                    if (i.rank == intent.rank &&
                        i.source_primary_id == intent.source_primary_id &&
                        i.target_primary_id == config_.master_id) {
                        mine = true;
                        break;
                    }
                }
            }
            if (!mine) {
                LogStepOutcome(kPhaseClaimCaretaker, kStepAbort,
                               "rank=" + std::to_string(a.rank) +
                                   ": intent held by another claim");
                return kStepAbort;
            }
        } else if (cerr != ErrorCode::OK) {
            LOG(WARNING) << "assign phase2 intent write failed: rank="
                         << a.rank << ", err=" << cerr;
            LogStepOutcome(kPhaseClaimCaretaker, kStepAbort,
                           "rank=" + std::to_string(a.rank) +
                               ": intent write failed");
            return kStepAbort;
        }
        // 阶段 1（§16.8.2）：CAS kMigrating，primary 暂仍 = 兼管者（读不受
        // 影响，源写由 kMigrating 冻结）。standby 列表保留（nullptr）。
        // kMigrating 语义下不删 intent：intent 是 ReshardDriverLoop 的断点
        // 续传依据。AdoptRankViaCAS 的 clear_intent 默认值是 true（晋升/
        // 接管语义），不显式传 false 会把刚写的 intent 立即删掉——迁移
        // 永远无人驱动，target 0 slot 卡死。
        RingSlotAssign out;
        const ErrorCode err = EtcdViewStore::AdoptRankViaCAS(
            config_.cluster_namespace, a.rank, a.epoch, a.primary_id,
            SlotState::kMigrating, config_.master_id, out, nullptr,
            "claimed", a.primary_id, /*success_as_warning=*/false,
            /*clear_intent=*/false);
        if (err == ErrorCode::OK) {
            LogStepOutcome(kPhaseClaimCaretaker, kStepDone,
                           "rank=" + std::to_string(a.rank) +
                               " claimed (reshard started from caretaker " +
                               a.primary_id + ")");
            return kStepDone;
        }
        // CAS 失配（含并发认领/晋升覆盖）：清掉本机意图，下轮重判。
        (void)EtcdViewStore::DeleteReshardIntent(config_.cluster_namespace,
                                                 a.rank);
        LogStepOutcome(kPhaseClaimCaretaker, kStepAbort,
                       "rank=" + std::to_string(a.rank) +
                           " CAS unresolved (see ownership log), intent "
                           "rolled back");
        return kStepAbort;
    }
    LogStepOutcome(kPhaseClaimCaretaker, kStepNoCandidate,
                   "scanned=" + std::to_string(view.assigns().size()) +
                       ", dead_or_empty=" + std::to_string(dead_or_empty) +
                       ", migrating=" + std::to_string(migrating) +
                       ", native_owned=" + std::to_string(native_owned));
    return kStepNoCandidate;
}

int CvmController::JoinAsStandby(
    const RingSlotsView& view,
    const std::vector<MasterRegistration>& members) {
    // ③ primary 存活但无存活 standby 的 rank（§16.18.2 步 2，rank 升序）：
    // CAS 加入 standby_ids（1:1 整体替换，§16.18.1；顺带清死亡 standby）。
    // kMigrating 的 rank 跳过（迁移瞬态，完成后下周期补——注意：该「下周期
    // 补」仅对 N+ 部署（成员数 > submaster_count，待命池非空）的未归属
    // 候选者可达；submaster_count=N 满配部署下迁移完成的新 owner 属「已
    // 归属」成员，被 TryAssignRankForSelf 阶段 0 门控挡在本 phase3 之外——
    // 该缺口由已归属成员环互备配对（JoinAsStandbyRing，互备 1.1）补齐：
    // 满 N 部署下由环拓扑指派各 rank 的 standby，开关见
    // StandbyMutualPairingEnabled。
    int dead_or_empty = 0, migrating = 0, already_paired = 0, self_primary = 0;
    for (const auto& a : view.assigns()) {
        if (a.primary_id.empty() || !IsMemberAlive(members, a.primary_id)) {
            ++dead_or_empty;
            VLOG(1) << "assign phase3 skip rank=" << a.rank
                    << ": primary dead or empty";
            continue;
        }
        // 防自配对：本机是该 rank 的 primary 时不能同时担任其 standby
        //（当前门控下未归属候选者不会命中；为未来放开已归属成员互备
        // 预置的防御）。
        if (a.primary_id == config_.master_id) {
            ++self_primary;
            VLOG(1) << "assign phase3 skip rank=" << a.rank
                    << ": self is primary";
            continue;
        }
        if (static_cast<SlotState>(a.state) == SlotState::kMigrating) {
            ++migrating;
            VLOG(1) << "assign phase3 skip rank=" << a.rank
                    << ": migration in progress";
            continue;
        }
        bool has_live_standby = false;
        for (const auto& sid : a.standby_ids) {
            if (IsMemberAlive(members, sid)) {
                has_live_standby = true;
                break;
            }
        }
        if (has_live_standby) {
            ++already_paired;
            VLOG(1) << "assign phase3 skip rank=" << a.rank
                    << ": live standby exists";
            continue;
        }
        std::vector<std::string> new_standbys{config_.master_id};
        RingSlotAssign out;
        const ErrorCode err = EtcdViewStore::CASSwitchRingSlotOwner(
            config_.cluster_namespace, a.rank, a.epoch, a.primary_id,
            static_cast<SlotState>(a.state), a.migrating_to_id, out,
            &new_standbys);
        if (err == ErrorCode::OK) {
            LogStepOutcome(kPhaseJoinStandby, kStepDone,
                           "rank=" + std::to_string(a.rank) +
                               " joined as standby (1:1 pairing), primary=" +
                               a.primary_id +
                               ", new_epoch=" + std::to_string(out.epoch));
            return kStepDone;
        }
        if (err != ErrorCode::STALE_ROUTE) {
            LOG(WARNING) << "CvmController standby-join CAS failed: rank="
                         << a.rank << ", err=" << err;
        }
        LogStepOutcome(kPhaseJoinStandby, kStepAbort,
                       "rank=" + std::to_string(a.rank) +
                           " CAS unresolved (contested or raced)");
        return kStepAbort;  // 满配或被抢：待命 / 下周期重判
    }
    LogStepOutcome(kPhaseJoinStandby, kStepNoCandidate,
                   "scanned=" + std::to_string(view.assigns().size()) +
                       ", dead_or_empty=" + std::to_string(dead_or_empty) +
                       ", migrating=" + std::to_string(migrating) +
                       ", already_paired=" + std::to_string(already_paired) +
                       ", self_primary=" + std::to_string(self_primary));
    return kStepNoCandidate;
}

int CvmController::JoinAsStandbyRing(
    const RingSlotsView& view,
    const std::vector<MasterRegistration>& members,
    uint32_t group_count) {
    // 已归属成员环互备（互备 1.1，调用方已验证开关 + 已归属身份）：
    //   - 指派规则（确定性推导，免协商免竞争）：rank r 的唯一指派候选者
    //     = 沿 rank 升序环找下一个「有存活 primary 的 rank」的 primary；
    //     候选者与目标 primary 同机（跨机约束，SamePrimaryHost 保守语义）
    //     或即目标 primary 兼任自身（caretaker 多 rank）时顺延下一个；
    //     全环顺延失败 → 该 rank 本周期不配对。
    //   - 仅「指派候选者 == 本机」才 CAS（1:1 整体替换，同阶段 ③ 语义，
    //     顺带清死亡 standby）。指派唯一 → 全集群对同一 rank 唯一写者，
    //     CAS 冲突仅来自并发写（epoch 竞态），kStepAbort 下周期重判。
    //   - 已有存活 standby 的 rank 不强制重拓扑：防成员变动期配对抖动，
    //     旧配对保持到该 standby 自然死亡后由新指派接续。
    //   - 单周期单动作（与阶段 ①-③ 一致）：多 rank 指派（caretaker
    //     场景本机可被指派多个 rank）跨多个周期逐步配齐。
    //   - kMigrating 跳过（迁移瞬态，完成后下周期重判）；本机是迁移目标
    //     不影响配对资格（standby 职责仅与目标 rank 相关）。
    if (group_count == 0) {
        return kStepNoCandidate;
    }
    // rank 升序快照（确定性单动作顺序 + 环顺延查找）。
    std::map<uint32_t, const RingSlotAssign*> by_rank;
    for (const auto& na : view.assigns()) {
        by_rank.emplace(na.rank, &na);
    }
    // master_id → address（跨机判定用；members 为存活全集，查找必中）。
    std::map<std::string, std::string> address_by_master;
    for (const auto& m : members) {
        address_by_master.emplace(m.master_id, m.address);
    }

    int dead_or_empty = 0, migrating = 0, already_paired = 0, self_primary = 0,
        same_host_skip = 0, not_designated = 0;
    for (const auto& [rank, ap] : by_rank) {
        (void)rank;
        const RingSlotAssign& a = *ap;
        if (a.primary_id.empty() || !IsMemberAlive(members, a.primary_id)) {
            ++dead_or_empty;
            continue;
        }
        if (static_cast<SlotState>(a.state) == SlotState::kMigrating) {
            ++migrating;  // 迁移瞬态：完成后下周期重判
            continue;
        }
        bool has_live_standby = false;
        for (const auto& sid : a.standby_ids) {
            if (IsMemberAlive(members, sid)) {
                has_live_standby = true;
                break;
            }
        }
        if (has_live_standby) {
            ++already_paired;  // 不强制重拓扑：旧配对保持到自然死亡
            continue;
        }
        if (a.primary_id == config_.master_id) {
            ++self_primary;  // 防自配对（caretaker 顺延链可达自身）
            continue;
        }
        // 环指派 + 跨机顺延：推导本 rank 的唯一指派候选者。
        std::string designated;
        for (uint32_t step = 1; step < group_count; ++step) {
            const uint32_t next_rank = (a.rank + step) % group_count;
            const auto next_it = by_rank.find(next_rank);
            if (next_it == by_rank.end()) {
                continue;  // 记录缺失（防御：阶段 0 已补建）：顺延
            }
            const std::string& cand = next_it->second->primary_id;
            if (cand.empty() || !IsMemberAlive(members, cand)) {
                continue;  // 该 rank 无存活 primary：顺延
            }
            if (cand == a.primary_id) {
                continue;  // 目标 primary 兼任（caretaker 多 rank）：顺延
            }
            if (SamePrimaryHost(cand, a.primary_id, address_by_master)) {
                ++same_host_skip;  // 跨机约束：同机顺延
                continue;
            }
            designated = cand;
            break;
        }
        if (designated != config_.master_id) {
            // 空候选（单成员/全同机）或指派他人：不动（指派者自会处理）。
            ++not_designated;
            continue;
        }
        // 指派本机：CAS 加入 standby_ids（1:1 整体替换，同阶段 ③ 原语）。
        std::vector<std::string> new_standbys{config_.master_id};
        RingSlotAssign out;
        const ErrorCode err = EtcdViewStore::CASSwitchRingSlotOwner(
            config_.cluster_namespace, a.rank, a.epoch, a.primary_id,
            static_cast<SlotState>(a.state), a.migrating_to_id, out,
            &new_standbys);
        if (err == ErrorCode::OK) {
            LogStepOutcome(kPhaseJoinStandbyRing, kStepDone,
                           "rank=" + std::to_string(a.rank) +
                               " joined as standby (ring pairing), primary=" +
                               a.primary_id +
                               ", new_epoch=" + std::to_string(out.epoch));
            return kStepDone;
        }
        if (err != ErrorCode::STALE_ROUTE) {
            LOG(WARNING) << "CvmController standby-ring-join CAS failed: rank="
                         << a.rank << ", err=" << err;
        }
        LogStepOutcome(kPhaseJoinStandbyRing, kStepAbort,
                       "rank=" + std::to_string(a.rank) +
                           " CAS unresolved (contested or raced)");
        return kStepAbort;  // 满配或被抢：下周期重判
    }
    LogStepOutcome(kPhaseJoinStandbyRing, kStepNoCandidate,
                   "scanned=" + std::to_string(by_rank.size()) +
                       ", dead_or_empty=" + std::to_string(dead_or_empty) +
                       ", migrating=" + std::to_string(migrating) +
                       ", already_paired=" + std::to_string(already_paired) +
                       ", self_primary=" + std::to_string(self_primary) +
                       ", same_host_skip=" + std::to_string(same_host_skip) +
                       ", not_designated=" + std::to_string(not_designated));
    return kStepNoCandidate;
}

bool CvmController::TryDesignatedTakeover(
    const std::vector<MasterRegistration>& members) {
    // 快照视图。缓存空 = 模型未启用。
    const RingSlotsView view = BuildRingSlotsView();
    if (!view.active()) {
        return false;
    }

    // 待命池非空时由补位路径（TryAssignRankForSelf ①，待命者优先，
    // §16.18.3）处理；兼管自愈仅在待命池空时兜底（§16.8.3）。
    for (const auto& m : members) {
        if (!view.is_assigned(m.master_id)) {
            LogStepOutcome(kPhaseTakeover, kStepNoCandidate,
                           "standby pool non-empty (assignment path owns "
                           "the vacancy)");
            return false;
        }
    }

    // 兼管自愈可观测性统计（出口 LogStepOutcome 汇总，VLOG(1) 逐候选）。
    int alive_or_empty = 0, migrating_target = 0, has_standby = 0,
        not_designated = 0;
    for (const auto& a : view.assigns()) {
        if (a.primary_id.empty() || IsMemberAlive(members, a.primary_id)) {
            ++alive_or_empty;
            VLOG(1) << "designated takeover skip rank=" << a.rank
                    << ": primary alive or empty";
            continue;
        }
        if (static_cast<SlotState>(a.state) == SlotState::kMigrating &&
            IsMemberAlive(members, a.migrating_to_id)) {
            ++migrating_target;
            VLOG(1) << "designated takeover skip rank=" << a.rank
                    << ": migrating target alive " << a.migrating_to_id;
            continue;  // 迁移目标存活：其 driver 会完成接管
        }
        bool has_live_standby = false;
        for (const auto& sid : a.standby_ids) {
            if (IsMemberAlive(members, sid)) {
                has_live_standby = true;
                break;
            }
        }
        if (has_live_standby) {
            ++has_standby;
            VLOG(1) << "designated takeover skip rank=" << a.rank
                    << ": live standby exists (promotion path owns it)";
            continue;  // standby 晋升路径（TryPromote）负责
        }
        // 确定性接管者（§16.8.1/§16.8.3）：r>0 → 前一个有存活 primary 的
        // rank 的 primary；rank0 断链 → 最小有存活 primary 的 rank 的
        // primary。FindCaretakerPrimary 唯一实现（与缺 rank 补建共用，
        // 传 members 查存活 + 无上界，与原散落实现逐条对照）。
        const std::string designated = FindCaretakerPrimary(
            view, a.rank, std::numeric_limits<uint32_t>::max(), &members);
        if (designated.empty() || designated != config_.master_id) {
            ++not_designated;
            VLOG(1) << "designated takeover skip rank=" << a.rank
                    << ": designated=" << designated << " (not me)";
            continue;  // 本机非指定接管者：由其本机执行（单一写者）
        }
        // 兼管者冷启动接管（§16.8.3）：本地无该段回放数据，MarkSlotsReady
        // + segment 重建；standby 全死（门控），整体清空。intent 残留由
        // AdoptRankViaCAS 幂等清理。
        std::vector<std::string> new_standbys;
        RingSlotAssign out;
        const ErrorCode err = EtcdViewStore::AdoptRankViaCAS(
            config_.cluster_namespace, a.rank, a.epoch, config_.master_id,
            SlotState::kStable, "", out, &new_standbys,
            "caretaker takeover", a.primary_id,
            /*success_as_warning=*/true);
        if (err == ErrorCode::OK) {
            LogStepOutcome(kPhaseTakeover, kStepDone,
                           "rank=" + std::to_string(a.rank) +
                               " caretaker takeover (dead primary " +
                               a.primary_id + ")");
            return true;
        }
        if (err == ErrorCode::STALE_ROUTE) {
            LogStepOutcome(kPhaseTakeover, kStepAbort,
                           "rank=" + std::to_string(a.rank) +
                               " lost CAS race (another master took over)");
            return false;  // 他方已处理（快照滞后），下周期收敛
        }
        LogStepOutcome(kPhaseTakeover, kStepAbort,
                       "rank=" + std::to_string(a.rank) +
                           " takeover CAS failed (see ownership log)");
        return false;
    }
    LogStepOutcome(kPhaseTakeover, kStepNoCandidate,
                   "scanned=" + std::to_string(view.assigns().size()) +
                       ", alive_or_empty=" + std::to_string(alive_or_empty) +
                       ", migrating_target=" +
                       std::to_string(migrating_target) +
                       ", has_standby=" + std::to_string(has_standby) +
                       ", not_designated=" +
                       std::to_string(not_designated));
    return false;
}

void CvmController::ReconcileRole() {
    // §16.15.4：晋升 CAS 先于角色切换。本机是「已死 primary」的最高优先
    // 存活 standby 时先抢 rank；抢到后 ComputeDesiredRole 才会判 kPrimary，
    // 保证 serving 以「CAS 已成功」为前提（防幽灵 primary）。
    // 周期内动作优先级（§16.15.8 / §16.18.2）：standby 晋升（含 30s 越权
    // 兜底）> 待命者补位/原生认领（bootstrap / 冷启动接管 / 热迁移认领 /
    // standby 补位）> 兼管自愈（待命池空时兜底）。三者均幂等 CAS，并发
    // 调用（watch + 周期双触发）由 epoch CAS 互斥。
    bool view_changed = false;
    {
        std::vector<MasterRegistration> members;
        if (LoadRankedMembers(members)) {
            view_changed = TryPromoteDeadPrimaryRanks(members);
            view_changed = TryAssignRankForSelf(members) || view_changed;
            view_changed = TryDesignatedTakeover(members) || view_changed;
        }
    }
    if (view_changed) {
        RefreshRingSlotsCache();
    }

    const MasterRole desired = ComputeDesiredRole();
    MasterRole current = current_role_.load();
    if (desired == current) {
        return;
    }
    // CAS：membership_thread_ 与 masters_watch_thread_ 并发调用时，仅一个
    // 线程执行角色迁移与通知，避免重复 OnRoleChanged。
    if (!current_role_.compare_exchange_strong(current, desired)) {
        return;
    }
    LOG(INFO) << "CvmController role decision: master_id="
              << config_.master_id << ", current="
              << static_cast<int32_t>(current)
              << ", desired=" << static_cast<int32_t>(desired)
              << ", submaster_count=" << config_.submaster_count;
    if (delegate_) {
        delegate_->OnRoleChanged(desired);
    }
}

void CvmController::MembershipLoop() {
    while (running_.load()) {
        ReconcileRole();
        // 周期全量兜底（§16.19.3 fail-safe）：watch 丢失事件时下一周期
        // 收敛。G 条记录的 LoadAll，量级安全。
        RefreshRingSlotsCache();

        std::unique_lock<std::mutex> lock(membership_mutex_);
        membership_cv_.wait_for(lock, config_.sync_interval,
                                [this] { return !running_.load(); });
    }
}

void CvmController::WatchCallback(void* ctx, const char* /*key*/,
                                  size_t /*key_size*/, const char* /*value*/,
                                  size_t /*value_size*/, int event_type,
                                  int64_t /*mod_revision*/) {
    auto* state = static_cast<WatchState*>(ctx);
    if (state == nullptr) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->dirty = true;
        if (event_type == kWatchEventBroken) {
            state->broken = true;
        }
    }
    state->cv.notify_all();
}

}  // namespace cvm
}  // namespace mooncake
