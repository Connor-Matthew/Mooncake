#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cvm/cvm_types.h"
#include "cvm/slot_migrator.h"
#include "types.h"

namespace mooncake {
namespace cvm {

// Drives slot-metadata handoff on a fixed interval (确定性哈希方案 §15.6).
//
// Slot ownership is NO LONGER published to etcd: it is derived
// deterministically from the primary member list (f(members, master_id)).
// On each tick this recomputes the owned slot set via `dynamic_slot_resolver`
// (or the static `owned_slots`) and hands the diff to SlotMigrator, which
// invokes the acquire/release hooks:
//   on_slot_acquired(slot) -> ErrorCode (RPC pull from the previous owner;
//                             non-OK keeps the slot pending for a retry)
//   on_slot_released(slot)  -> stage the export for the new owner to pull.
// No SlotOwner record is written anywhere; clients derive the same ring
// locally instead of reading a persisted kv_view.
class SlotOwnerHeartbeat {
   public:
    struct Config {
        std::string cluster_namespace;
        std::string master_id;
        std::chrono::milliseconds heartbeat_interval{5000};

        // Slots owned by this submaster. When empty, the submaster owns every
        // logical slot (single-master mode).
        std::vector<uint16_t> owned_slots;

        // Optional resolver that recomputes the owned slot set on every
        // publish. When set, it overrides `owned_slots` and is invoked before
        // each PublishOnce so ownership tracks cluster membership changes
        // (dynamic partition). An empty result means the submaster owns no
        // slot. The callback must be safe to invoke from the heartbeat thread.
        std::function<std::vector<uint16_t>()> dynamic_slot_resolver;

        // Retained for diagnostics only: the supervisor-granted member lease
        // (member-level fence). No per-slot record is written with this lease
        // anymore — slot ownership is derived, not published.
        EtcdLeaseId lease_id{0};

        // Optional hooks fired on slot ownership change (确定性哈希方案 §15.7).
        // on_slot_acquired pulls object metadata for a newly-owned slot from the
        // previous owner and returns ErrorCode: OK marks it ready, any other
        // code keeps the slot pending for a retry next cycle. on_slot_released
        // stages the export for the new owner to pull. Leave empty for
        // derive-only behavior.
        std::function<ErrorCode(uint16_t)> on_slot_acquired;
        std::function<void(uint16_t)> on_slot_released;

        // 段级批量 acquire（slot 就绪加速）：gained 聚成连续区间后整段回调，
        // results 为逐 slot 结果（语义同 on_slot_acquired）。留空则全部走逐
        // slot 路径。实现内部对不可批量化的 slot 回退单 slot 路径。
        SlotMigrator::RangeAcquireCallback on_slot_range_acquired;
        SlotMigrator::RangeReleaseCallback on_slot_range_released;
    };

    explicit SlotOwnerHeartbeat(Config config);
    ~SlotOwnerHeartbeat();

    SlotOwnerHeartbeat(const SlotOwnerHeartbeat&) = delete;
    SlotOwnerHeartbeat& operator=(const SlotOwnerHeartbeat&) = delete;

    ErrorCode Start();
    void Stop();

    // Recomputes the owned slot set and drives the SlotMigrator diff/hooks
    // once. Idempotent; safe to call from the heartbeat thread or externally.
    ErrorCode PublishOnce();

    // 立即触发下一轮 PublishOnce（短路 heartbeat_interval 等待）。线程安全：
    // 仅置位标志并 notify；RunLoop 单线程消费，重入语义退化为「当前周期结束
    // 后立刻再跑一轮」。用于事件驱动的重试加速（vsegment 后台首建完成 /
    // auto-plan 发布成功等），替代固定 5s 周期粒度的干等。
    void Kick();

   private:
    void RunLoop();

    Config config_;
    std::vector<uint16_t> owned_slots_;
    // Owns the slot handoff state machine (kMigrating -> kStable / release).
    SlotMigrator migrator_;

    std::atomic<bool> running_{false};
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;
    // Kick 置位标志（与 stop_cv_ 共用 stop_mutex_，notify/kick 双方都覆盖）。
    bool kick_pending_ = false;
    std::thread thread_;
    // 上一轮 reconcile 是否失败（仅 RunLoop 心跳线程访问，无需加锁）：
    // 失败→成功的转变点打一条 INFO（reconcile incomplete 长期刷屏后恢复
    // 的明确观测点），稳态成功静默。
    bool last_publish_failed_ = false;
};

}  // namespace cvm
}  // namespace mooncake
