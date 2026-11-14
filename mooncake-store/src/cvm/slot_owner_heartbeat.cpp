#include "cvm/slot_owner_heartbeat.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include <glog/logging.h>

#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"

namespace mooncake {
namespace cvm {

namespace {

// Resolves the owned slot list from config. An empty `owned_slots` means the
// submaster owns every logical slot (single-master mode).
std::vector<uint16_t> ResolveOwnedSlots(const SlotOwnerHeartbeat::Config& cfg) {
    if (!cfg.owned_slots.empty()) {
        return cfg.owned_slots;
    }
    std::vector<uint16_t> slots;
    slots.reserve(SlotCount());
    for (uint16_t slot = 0; slot < SlotCount(); ++slot) {
        slots.push_back(slot);
    }
    return slots;
}

}  // namespace

SlotOwnerHeartbeat::SlotOwnerHeartbeat(Config config)
    : config_(std::move(config)),
      owned_slots_(ResolveOwnedSlots(config_)),
      migrator_(SlotMigrator::Config{config_.cluster_namespace,
                                     config_.master_id,
                                     config_.lease_id}) {
    migrator_.SetOnAcquire(config_.on_slot_acquired);
    migrator_.SetOnRelease(config_.on_slot_released);
    migrator_.SetOnReleaseRange(config_.on_slot_range_released);
    migrator_.SetOnAcquireRange(config_.on_slot_range_acquired);
}

SlotOwnerHeartbeat::~SlotOwnerHeartbeat() { Stop(); }

ErrorCode SlotOwnerHeartbeat::Start() {
    if (running_.load()) {
        return ErrorCode::OK;
    }
    running_.store(true);
    thread_ = std::thread([this]() { RunLoop(); });
    return ErrorCode::OK;
}

void SlotOwnerHeartbeat::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    {
        // 空临界区仅作 happens-before 屏障：若 RunLoop 正处于「谓词检查完、
        // 尚未阻塞」的窗口（该窗口内持有 stop_mutex_），Stop 等它阻塞后再
        // notify，保证通知不落空——否则 notify 可能在等待线程睡下前发出，
        // join 最多空等一个 heartbeat_interval（5s）才等到超时唤醒。
        std::lock_guard<std::mutex> lock(stop_mutex_);
    }
    stop_cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

ErrorCode SlotOwnerHeartbeat::PublishOnce() {
    // Under dynamic partition the owned slot set is recomputed every publish so
    // it follows cluster membership changes; otherwise use the static set.
    std::vector<uint16_t> slots = owned_slots_;
    if (config_.dynamic_slot_resolver) {
        slots = config_.dynamic_slot_resolver();
    }

    // Hand the owned-slot diff to the SlotMigrator: it computes gained/
    // released and drives the acquire/release hooks (object-metadata RPC
    // handoff). No etcd publish occurs — ownership is derived, not persisted.
    ErrorCode err = migrator_.Reconcile(slots);
    if (err != ErrorCode::OK) {
        // Reconcile 内部已有逐 slot 失败日志；这里补一条周期级汇总，便于从
        // 心跳线程视角确认本轮 slot 交接未完全成功。
        LOG(WARNING) << "SlotOwnerHeartbeat reconcile incomplete: master_id="
                     << config_.master_id << " owned_slots=" << slots.size()
                     << " err=" << err;
        last_publish_failed_ = true;
    } else if (last_publish_failed_) {
        // 失败→成功转变（vsegment 发布成功 / 门控解除后心跳恢复收口）：
        // 明确观测点，与「All owned slots ready」衔接成「可写入」信号链。
        last_publish_failed_ = false;
        LOG(INFO) << "SlotOwnerHeartbeat reconcile recovered: master_id="
                  << config_.master_id << " owned_slots=" << slots.size()
                  << " (slot handoff healthy again)";
    }
    return err;
}

void SlotOwnerHeartbeat::Kick() {
    {
        std::lock_guard<std::mutex> lock(stop_mutex_);
        kick_pending_ = true;
    }
    stop_cv_.notify_all();
}

void SlotOwnerHeartbeat::RunLoop() {
    const auto minimum_retry = std::min(config_.heartbeat_interval,
                                         std::chrono::milliseconds(100));
    auto retry_delay = minimum_retry;
    while (running_.load()) {
        const auto error = PublishOnce();
        std::unique_lock<std::mutex> lock(stop_mutex_);
        const auto delay = error == ErrorCode::OK
                               ? config_.heartbeat_interval : retry_delay;
        stop_cv_.wait_for(lock, delay,
                          [this] { return !running_.load() || kick_pending_; });
        retry_delay = error == ErrorCode::OK || kick_pending_
                          ? minimum_retry
                          : std::min(retry_delay * 2, config_.heartbeat_interval);
        kick_pending_ = false;
    }
}

}  // namespace cvm
}  // namespace mooncake
