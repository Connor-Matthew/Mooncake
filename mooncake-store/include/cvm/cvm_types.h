#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake {
namespace cvm {

// ---------------------------------------------------------------------------
// Enumerations
// ---------------------------------------------------------------------------

// Ownership state of a logical KV slot.
enum class SlotState : int32_t {
    kStable = 0,     // Served by primary_master_id.
    kMigrating = 1,  // Handing off to migrating_to_master_id.
};

// Role of a master within the CVM topology.
enum class MasterRole : int32_t {
    kPrimary = 0,
    kStandby = 1,
};

// ---------------------------------------------------------------------------
// KV slot ownership record
// ---------------------------------------------------------------------------

// NOTE: enum fields are stored as int32_t so the records serialize/deserialize
// identically across languages and compiler settings.
struct SlotOwner {
    uint16_t slot{0};
    std::string primary_master_id;
    int32_t state{0};  // SlotState
    std::string migrating_to_master_id;  // empty when stable
};
YLT_REFL(SlotOwner, slot, primary_master_id, state, migrating_to_master_id);

// ---------------------------------------------------------------------------
// Segment neutral entity (one per segment, no owner) — replaces the old
// single-owner SegmentView model.  Stored under segments/{segment_id}.
// ---------------------------------------------------------------------------

// Lightweight per-master mount record.  key = (master_id, segment_id) naturally
// supports one-segment-multi-master mounting without overwrite conflicts.
struct MountEntry {
    std::string segment_id;
    int64_t mounted_at_ms{0};
    // Future: partition_slot_starts for psegment/vsegment partial mount
    std::vector<uint16_t> partition_slot_starts;
};
YLT_REFL(MountEntry, segment_id, mounted_at_ms, partition_slot_starts);

// Neutral descriptor of a segment — authoritative copy lives under
// segments/{segment_id} and is written once per segment (idempotent).
//
// 资源事实字段（medium / io_alignment / supports_unaligned_io /
// failure_domain / vsegment_exclusive）由 Store 在 mount 注册时上报，供
// PartitionQuotaPlanner 自动发现使用，不应由用户在配额文件中手写。旧
// JSON 缺这些字段时反序列化为默认值，向后兼容。
struct SegmentDescriptor {
    std::string segment_id;
    std::string segment_name;
    size_t capacity{0};
    std::string te_endpoint;  // transport endpoint, e.g. "host:port"
    std::string protocol;
    std::string host_id;
    // Future: partitions for psegment/vsegment split
    struct Partition {
        uint16_t slot_start{0};
        uint16_t slot_end{0};
        uint64_t offset{0};
        uint64_t length{0};
    };
    std::vector<Partition> partitions;

    // ---- 资源事实（Store 注册时上报，供自动发现使用）----
    // 物理介质标识，与 VSegmentProfile.required_medium 匹配，例如
    // "REGISTERED_MEMORY"、"DRAM"、"NVMe"。空表示未上报，自动发现会拒绝。
    std::string medium;
    // I/O 对齐字节数。Planner 取 max(profile.io_alignment, segment.io_alignment)。
    uint64_t io_alignment{1};
    // 是否支持非对齐 I/O。false 时该 psegment 不参与 vsegment 条带化。
    bool supports_unaligned_io{true};
    // 故障域标识，缺省由 host_id 兜底，可由部署标签补充。
    std::string failure_domain;
    // 当前已分配字节数（由 allocator 实时维护）。自动发现按
    // [used_bytes, capacity) 作为 vsegment 可切分范围，避免与其他
    // 分配器占用范围重叠。旧 JSON 缺该字段时反序列化为 0，等同
    // exclusive 空集群，向后兼容。
    uint64_t used_bytes{0};
    // 可选 hint：是否为「vsegment 专用且当前为空」的 psegment。true 时
    // 自动发现要求 used_bytes==0，快路径直接按 capacity 切分；false 时
    // 自动发现按 [used_bytes, capacity) 切分。不再强制声明。
    bool vsegment_exclusive{false};
};
YLT_REFL(SegmentDescriptor, segment_id, segment_name, capacity, te_endpoint,
         protocol, host_id, partitions, medium, io_alignment,
         supports_unaligned_io, failure_domain, used_bytes,
         vsegment_exclusive);

// ---------------------------------------------------------------------------
// Master registration: liveness + role, persisted under an etcd lease
// ---------------------------------------------------------------------------

struct MasterRegistration {
    std::string master_id;
    std::string address;  // RPC endpoint, e.g. "host:port"
    int32_t role{0};      // MasterRole
    int64_t registered_at_ms{0};
    // etcd key create_revision（全局单调）。用于「先到先得」的 primary 推导：
    // 谁先注册 revision 越小。NOT 序列化进 value（YLT_REFL 不含此字段），
    // 由 LoadAllMasters 从 key 元数据填充。
    int64_t create_revision{0};
};
YLT_REFL(MasterRegistration, master_id, address, role, registered_at_ms);

// 确定性 primary 排序键（先到先得）：按 etcd create_revision 升序，谁先注册
// 谁排前；revision 缺失或相等的退化时按 master_id 稳定排序，保证节点/客户端
// 推导结果字节级一致。所有建环路径（服务端角色、slot 归属、客户端路由）必须
// 使用同一比较器，否则环不一致会导致转发/震荡。
inline bool MasterRegistrationRankLess(const MasterRegistration& a,
                                       const MasterRegistration& b) {
    if (a.create_revision != b.create_revision) {
        return a.create_revision < b.create_revision;
    }
    return a.master_id < b.master_id;
}

// 成员存活判定（lease 仍持有 = 存活）：控制面（晋升/补位/自愈）与数据面
//（reshard driver）、HTTP 入口的共用谓词。单一实现防止各处判定漂移
//（如误用 role 字段、误排序）。
inline bool IsMemberAlive(const std::vector<MasterRegistration>& members,
                          const std::string& master_id) {
    for (const auto& m : members) {
        if (m.master_id == master_id) {
            return true;
        }
    }
    return false;
}

// Cluster-wide ring configuration persisted under /cvm/{ns}/cluster_meta
// (确定性哈希方案 §15.3). Clients and masters derive the same primary_ids =
// sort(members by create_revision)[0:submaster_count]（先到先得，见
// MasterRegistrationRankLess）from this count, so slot ownership no longer
// needs to be persisted per slot.
//
// slot_group_count（§16.14.2）：RingSlot 槽位组数 G。旧数据缺省 1（YLT_REFL
// 反序列化落默认值），退化为单 primary、与 §15 环等价，向上兼容可回滚。
// G 在集群创建时设定、运行期恒不变（变更属冷操作，§16.8 前提）。
struct RingMeta {
    uint32_t submaster_count{1};
    uint32_t slot_group_count{1};
    // Defaults to the historical value when reading metadata written by an
    // older version. New deployments persist the value selected by
    // MC_STORE_PARTITION_SLOT_COUNT so clients can reject configuration drift.
    uint32_t slot_count{16384};
};
YLT_REFL(RingMeta, submaster_count, slot_group_count, slot_count);

// ---------------------------------------------------------------------------
// RingSlot 槽位组归属（§16.14.1），持久化于 /cvm/{ns}/ring_slots/{rank}。
// ---------------------------------------------------------------------------

// 槽位组归属记录（rank 是 key 里的稳定值，value 内冗余供校验）。
// 不绑任何 lease：liveness 由 masters/{id} 的 lease 唯一承担（§16.14.4），
// 本结构只保存归属状态；owner 变更统一走 epoch CAS（fencing token）。
struct RingSlotAssign {
    uint32_t rank{0};                     // 槽位组索引 0..G-1（冗余，读取时校验 key 与 value 一致，防写错槽）
    std::string primary_id;               // 当前 serving 该段的 primary master_id（兼管时同一 primary_id 出现在多个 rank）
    std::vector<std::string> standby_ids; // 配对 standby（按晋升优先级升序，1:1 配对时仅 1 个元素，§16.18.1）
    int32_t state{0};                     // SlotState: kStable=0 / kMigrating=1（P2 恒 kStable，为 P4 reshard 预留）
    std::string migrating_to_id;          // reshard 目标 primary（kStable 时为空）
    uint64_t epoch{0};                    // 乐观锁版本号，owner 每次切换 +1（跨代 fencing）
};
YLT_REFL(RingSlotAssign, rank, primary_id, standby_ids, state,
         migrating_to_id, epoch);

// ---------------------------------------------------------------------------
// reshard 意图（§16.19.1），持久化于 /cvm/{ns}/reshard_intent/{rank}。
// ---------------------------------------------------------------------------

// 显式迁移意图：管理员（扩缩容）或原生认领者写入；目标 primary 的
// reshard driver 消费（断点续传：节点重启后意图仍在），完成后删除。
// 与 ring_slots 一样是「持久化状态 = 唯一真相」的延续。
struct ReshardIntent {
    uint32_t rank{0};              // 目标槽位组
    std::string source_primary_id; // 迁出方（快照仍由其持有，ack 前不删）
    std::string target_primary_id; // 迁入方（driver 持有者）
};
YLT_REFL(ReshardIntent, rank, source_primary_id, target_primary_id);

}  // namespace cvm
}  // namespace mooncake
