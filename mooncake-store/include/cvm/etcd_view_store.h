#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "cvm/cvm_types.h"
#include "partition/vsegment_types.h"
#include "types.h"

namespace mooncake {
namespace cvm {

// Serialization + etcd persistence for CVM metadata, built on EtcdHelper.
//
// Master registration, ring metadata, segment neutral descriptors and
// per-master segment mounts are stored in etcd under the CVM key space (see
// cvm_keys.h). ViewVersionId is the etcd revision returned by reads, so
// consumers can watch from it without gaps.
class EtcdViewStore {
   public:
    // ---- JSON serialization ----
    static ErrorCode SerializeMasterRegistration(const MasterRegistration& reg,
                                                 std::string& out);
    static ErrorCode DeserializeMasterRegistration(const std::string& in,
                                                   MasterRegistration& out);

    static ErrorCode SerializeRingMeta(const RingMeta& meta, std::string& out);
    static ErrorCode DeserializeRingMeta(const std::string& in, RingMeta& out);

    // ---- Segment neutral entity (segments/{segment_id}) ----
    static ErrorCode SerializeSegmentDescriptor(const SegmentDescriptor& desc,
                                                std::string& out);
    static ErrorCode DeserializeSegmentDescriptor(const std::string& in,
                                                  SegmentDescriptor& out);
    static ErrorCode SaveSegmentDescriptor(const std::string& cluster_namespace,
                                           const SegmentDescriptor& desc);
    static ErrorCode DeleteSegmentDescriptor(
        const std::string& cluster_namespace, const std::string& segment_id);
    static ErrorCode LoadAllSegmentDescriptors(
        const std::string& cluster_namespace,
        std::vector<SegmentDescriptor>& out, ViewVersionId& version);

    // ---- Per-master segment mount (snapshot/{id}/segments/{seg}) ----
    static ErrorCode SerializeMountEntry(const MountEntry& entry,
                                         std::string& out);
    static ErrorCode DeserializeMountEntry(const std::string& in,
                                           MountEntry& out);
    static ErrorCode SaveMountEntryWithLease(const std::string& cluster_namespace,
                                             const std::string& master_id,
                                             const MountEntry& entry,
                                             EtcdLeaseId lease_id);
    static ErrorCode DeleteMountEntry(const std::string& cluster_namespace,
                                      const std::string& master_id,
                                      const std::string& segment_id);
    // Aggregates every mount record under snapshot/*/segments/, returning
    // (master_id, MountEntry) pairs so consumers can map each mount back to
    // its owning master.
    static ErrorCode LoadAllMountEntries(
        const std::string& cluster_namespace,
        std::vector<std::pair<std::string, MountEntry>>& out,
        ViewVersionId& version);

    // ---- Master registration ----
    static ErrorCode RegisterMaster(const std::string& cluster_namespace,
                                    const MasterRegistration& reg,
                                    EtcdLeaseId lease_id);
    static ErrorCode UpdateMasterRole(const std::string& cluster_namespace,
                                      const std::string& master_id,
                                      MasterRole role, EtcdLeaseId lease_id);
    static ErrorCode LoadAllMasters(const std::string& cluster_namespace,
                                    std::vector<MasterRegistration>& out,
                                    ViewVersionId& version);

    // ---- Cluster ring metadata (cluster_meta) ----
    // Persists/reads RingMeta { submaster_count, G, slot_count } (§15.3 /
    // §16.14.2). Masters write it once at startup (idempotent); clients read it
    // to derive the same primary ring locally.
    static ErrorCode SaveClusterMeta(const std::string& cluster_namespace,
                                     const RingMeta& meta);
    static ErrorCode LoadClusterMeta(const std::string& cluster_namespace,
                                     RingMeta& out, ViewVersionId& version);

    // ---- RingSlot 槽位组归属（ring_slots/{rank}，§16.14.4-5，P2）----
    // 无 lease 的持久归属状态；liveness 唯一权威在 masters/{id} 的 lease。
    static ErrorCode SerializeRingSlotAssign(const RingSlotAssign& assign,
                                             std::string& out);
    static ErrorCode DeserializeRingSlotAssign(const std::string& in,
                                               RingSlotAssign& out);

    // 初始声明（kKeyNotExists 事务，§16.14.5①）：并发声明同一 rank 仅一个
    // 成功；key 已存在返回 ETCD_TRANSACTION_FAIL，调用方读回校验即可。
    static ErrorCode CreateRingSlotAssign(const std::string& cluster_namespace,
                                          const RingSlotAssign& assign);

    // ---- 归属推进组合原语（CAS + 残留清理 + 结构化日志）----
    // 「CAS 切换 owner/状态 + 可选幂等删 reshard_intent + 统一日志」
    // 的五处共性提取（晋升/补位接管/兼管自愈/reshard driver 阶段1-2/
    // 管理员路径）。参数与 CASSwitchRingSlotOwner 一致，附加：
    //   reason    —— 日志语境（"promoted"/"took over"/"claimed"...），
    //                使各路径日志可 grep 区分；
    //   old_owner —— 日志字段（通常 = current.primary_id）；
    //   success_as_warning —— 成功日志用 WARNING 而非 INFO（数据冷重建
    //                类接管需要运维立即知晓，如兼管自愈）；
    //   clear_intent —— CAS 成功后是否删本 rank 的 reshard_intent。
    //                「接管」语义（晋升/冷启动/自愈覆盖 kMigrating）传
    //                true（残留意图与新视图矛盾，§16.16.7）；「driver
    //                流程内推进」（reshard begin/owner switch/resume）
    //                传 false——intent 是 driver 断点续传的依据，必须
    //                由 driver 全流程完成（含 ack）后才删，阶段2 切
    //                owner 不代表迁移结束（阶段3 安装、阶段4 ack 未做）。
    // 返回值与 CASSwitchRingSlotOwner 相同（OK / STALE_ROUTE / ...），
    // out_assign 仅在 OK 时有效。日志语义：OK → INFO/WARNING（由参数）；
    // STALE → INFO（竞争让位，非异常）；其他失败 → WARNING。
    static ErrorCode AdoptRankViaCAS(const std::string& cluster_namespace,
                                     uint32_t rank, uint64_t expected_epoch,
                                     const std::string& new_primary_id,
                                     SlotState new_state,
                                     const std::string& migrating_to_id,
                                     RingSlotAssign& out_assign,
                                     const std::vector<std::string>* new_standbys,
                                     const char* reason,
                                     const std::string& old_owner,
                                     bool success_as_warning = false,
                                     bool clear_intent = true);

    // 读取单 rank 归属。value 内 rank 与 key 不一致视为 etcd 数据损坏，
    // 返回 INTERNAL_ERROR（防写错槽）。
    static ErrorCode LoadRingSlotAssign(const std::string& cluster_namespace,
                                       uint32_t rank, RingSlotAssign& out,
                                       ViewVersionId& version);

    // 扫描全部 ring_slots，按 key 排序返回（零填充 key 保证 == rank 升序）。
    // 新集群无任何记录时返回 OK + 空 vector（与 LoadAllMasters 语义一致）。
    static ErrorCode LoadAllRingSlotAssigns(
        const std::string& cluster_namespace,
        std::vector<RingSlotAssign>& out, ViewVersionId& version);

    // owner 切换（epoch CAS，仿 CASSwitchPartitionOwner，§16.14.5②）：
    // expected_epoch 匹配当前值才写入 new_primary_id/new_state/
    // migrating_to_id，epoch = expected + 1，完整新记录写回 out。
    // new_standby_ids 为 nullptr 时保留现有 standby 列表；非 null 时整体
    // 替换（用于 §16.14.7 死亡 standby 移除）。
    // 失配返回 STALE_ROUTE（语义 = fencing 被拒，与分区路由 CAS 一致）。
    static ErrorCode CASSwitchRingSlotOwner(
        const std::string& cluster_namespace, uint32_t rank,
        uint64_t expected_epoch, const std::string& new_primary_id,
        SlotState new_state, const std::string& migrating_to_id,
        RingSlotAssign& out,
        const std::vector<std::string>* new_standby_ids = nullptr);

    // ---- Partition 路由（§5.2 vsegment 预留接口，仅存 owner + epoch）----
    // 注意：ETCD 只保存路由（owner + epoch），不保存 free extents / 完整 view。
    // 方法体由 vsegment 实现方落地，此处仅冻结方法签名。
    static ErrorCode SerializePartitionRoute(
        const partition::PartitionRoute& route, std::string& out);
    static ErrorCode DeserializePartitionRoute(const std::string& in,
                                               partition::PartitionRoute& out);

    static ErrorCode SavePartitionRoute(const std::string& cluster_namespace,
                                        const partition::PartitionRoute& route);
    static ErrorCode LoadPartitionRoute(const std::string& cluster_namespace,
                                        const std::string& partition_id,
                                        partition::PartitionRoute& out,
                                        ViewVersionId& version);

    // 原子切换 owner：仅当 etcd 中当前 epoch 与 expected 一致时成功，成功后
    // epoch = expected + 1，并把新路由写入 out。失配返回 STALE_ROUTE /
    // STALE_ALLOCATOR_EPOCH（对应 §5.2.9 的 fencing 语义）。
    static ErrorCode CASSwitchPartitionOwner(
        const std::string& cluster_namespace, const std::string& partition_id,
        uint64_t expected_route_epoch, const std::string& new_owner_submaster_id,
        partition::PartitionState new_state,
        const std::string& target_submaster_id, partition::PartitionRoute& out);

    // ---- Watch ----
    using WatchCallback = void (*)(void*, const char*, size_t, const char*,
                                   size_t, int, int64_t);

    // ---- Master membership watch ----
    // Watches the master registration prefix so member add/remove (e.g. lease
    // expiry) can drive immediate role re-evaluation.
    static ErrorCode WatchMasters(const std::string& cluster_namespace,
                                  ViewVersionId start_revision, void* ctx,
                                  WatchCallback cb);
    static ErrorCode CancelWatchMasters(const std::string& cluster_namespace);
    static ErrorCode WaitWatchMastersStopped(
        const std::string& cluster_namespace, int timeout_ms);

    // ---- RingSlot ownership watch（§16.19.4，P3）----
    // Watches the ring_slots prefix so owner switches (promotion CAS / reshard
    // state flips) drive immediate local bitmap invalidation (ghost-write
    // fencing, §16.14.6) and router hot refresh (§16.17). Callback keys are
    // "ring_slots/<rank>"; use cvm::ParseRankFromRingSlotKey to map back.
    // 与 WatchMasters 同范式：持久 watch，普通事件不注销，仅 broken 时重 arm
    //（由 controller 的两层循环承担）。
    static ErrorCode WatchRingSlots(const std::string& cluster_namespace,
                                   ViewVersionId start_revision, void* ctx,
                                   WatchCallback cb);
    static ErrorCode CancelWatchRingSlots(
        const std::string& cluster_namespace);
    static ErrorCode WaitWatchRingSlotsStopped(
        const std::string& cluster_namespace, int timeout_ms);

    // ---- reshard 意图（reshard_intent/{rank}，§16.19.1，P4）----
    // 显式迁移意图：kKeyNotExists 事务写入（已存在返回
    // ETCD_TRANSACTION_FAIL——同 rank 并发发起仅一个成立，幂等重试读回即可）。
    static ErrorCode CreateReshardIntent(const std::string& cluster_namespace,
                                         const ReshardIntent& intent);

    // 幂等删除（key 不存在视为 OK）：迁移完成 / 晋升覆盖 kMigrating 时清理
    // 残留意图（§16.15.4 / §16.16.7），防止把新 owner 误拉入多余迁移。
    static ErrorCode DeleteReshardIntent(const std::string& cluster_namespace,
                                          uint32_t rank);

    // 扫描全部 reshard 意图，按 key 排序返回（零填充 == rank 升序）。
    static ErrorCode LoadAllReshardIntents(
        const std::string& cluster_namespace,
        std::vector<ReshardIntent>& out, ViewVersionId& version);
};

}  // namespace cvm
}  // namespace mooncake
