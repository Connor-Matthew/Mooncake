#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "cvm/cvm_types.h"
#include "types.h"

namespace mooncake {
namespace cvm {

// Drives slot-metadata handoff as slot ownership shifts (确定性哈希方案 §15.6).
//
// Slot ownership is NO LONGER persisted to etcd: it is derived deterministically
// from the primary member list (f(members, master_id)). SlotMigrator therefore
// no longer publishes kv/{slot} ownership records — it only computes the
// gained/released slot diff against the previous cycle and invokes the
// caller-provided hooks:
//
//   释放 slot: on_release(slot)  —— 旧 owner stage（导出元数据到内存，等新
//                                    owner 拉取）
//   获得 slot: on_acquire(slot)   —— 新 owner 拉取旧 owner 元数据并导入；
//                                    非 OK 时不计入 last_owned_slots_，下轮
//                                    重试（直到元数据就绪）
//
// The actual object-metadata transfer/drop is the caller's responsibility via
// these hooks (data bytes always stay in segments).
class SlotMigrator {
   public:
    struct Config {
        std::string cluster_namespace;
        std::string master_id;
        EtcdLeaseId lease_id{0};
    };

    // on_acquire returns ErrorCode: OK means the slot's metadata is now ready
    // locally; any other code keeps the slot out of last_owned_slots_ so it is
    // retried next cycle. on_release is best-effort (no result needed).
    using AcquireCallback = std::function<ErrorCode(uint16_t)>;
    using ReleaseCallback = std::function<void(uint16_t)>;
    using RangeReleaseCallback = std::function<void(uint16_t, uint16_t)>;
    // 段级批量 acquire（slot 就绪加速）：gained slot 聚成连续区间后整段回调，
    // results[i] 为 slot (first + i) 的逐 slot 结果，语义与 on_acquire 完全
    // 一致（OK=就绪，其它=下轮重试）。实现内部对不可批量化的 slot（owner
    // 不连续 / 门控拦截 / RPC 失败）应回退单 slot 路径并填好 results。整段
    // 返回值非 OK 时 migrator 退回逐 slot 兜底，语义不丢失。
    using RangeAcquireCallback = std::function<ErrorCode(
        uint16_t first, uint16_t last, std::vector<ErrorCode>& results)>;

    explicit SlotMigrator(Config config);
    ~SlotMigrator() = default;

    SlotMigrator(const SlotMigrator&) = delete;
    SlotMigrator& operator=(const SlotMigrator&) = delete;

    // Hooks invoked on ownership change. on_acquire materializes object
    // metadata for the slot (RPC pull from the previous owner in Phase 3);
    // on_release stages the export for the new owner to pull.
    void SetOnAcquire(AcquireCallback cb) { on_acquire_ = std::move(cb); }
    void SetOnRelease(ReleaseCallback cb) { on_release_ = std::move(cb); }
    void SetOnReleaseRange(RangeReleaseCallback cb) {
        on_release_range_ = std::move(cb);
    }
    // 段级批量 acquire 钩子：设置后 gained 的连续区间（长度 ≥ 阈值）走整段
    // 回调，短区间与失败兜底仍走逐 slot on_acquire。
    void SetOnAcquireRange(RangeAcquireCallback cb) {
        on_acquire_range_ = std::move(cb);
    }

    // Computes the gained/released slot diff for `owned_slots` and drives the
    // hooks. Idempotent; safe to call from the heartbeat thread. Returns the
    // last non-OK error (if any) but keeps going.
    ErrorCode Reconcile(const std::vector<uint16_t>& owned_slots);

   private:
    Config config_;
    AcquireCallback on_acquire_;
    ReleaseCallback on_release_;
    RangeReleaseCallback on_release_range_;
    RangeAcquireCallback on_acquire_range_;
    std::vector<uint16_t> last_owned_slots_;
};

}  // namespace cvm
}  // namespace mooncake
