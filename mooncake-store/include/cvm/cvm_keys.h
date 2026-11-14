#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

namespace mooncake {
namespace cvm {

// Root prefix for all CVM keys in etcd.
inline constexpr std::string_view kCvmRootPrefix = "/cvm/";

// End key for an etcd range scan over [prefix, PrefixEnd(prefix)).
inline std::string PrefixEnd(std::string prefix) {
    for (int i = static_cast<int>(prefix.size()) - 1; i >= 0; --i) {
        unsigned char c = static_cast<unsigned char>(prefix[i]);
        if (c < 0xFF) {
            prefix[i] = static_cast<char>(c + 1);
            prefix.resize(i + 1);
            return prefix;
        }
    }
    return std::string(1, '\0');
}

// "/cvm/<namespace>/"
inline std::string CvmNamespaceRoot(const std::string& cluster_namespace) {
    return std::string(kCvmRootPrefix) + cluster_namespace + "/";
}

// "/cvm/<namespace>/masters/"
inline std::string MasterRegistrationPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "masters/";
}

// "/cvm/<namespace>/masters/<master_id>"
inline std::string MasterRegistrationKey(const std::string& cluster_namespace,
                                         const std::string& master_id) {
    return MasterRegistrationPrefix(cluster_namespace) + master_id;
}

// "/cvm/<namespace>/cluster_meta" — cluster-wide RingMeta (§15.3).
// Cluster-wide ring configuration read by clients to locally derive the same
// primary ring as the masters.
inline std::string ClusterMetaKey(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "cluster_meta";
}

// "/cvm/<namespace>/snapshot/"
inline std::string SnapshotPrefix(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "snapshot/";
}

// "/cvm/<namespace>/snapshot/kv_view"
inline std::string KvViewSnapshotKey(const std::string& cluster_namespace) {
    return SnapshotPrefix(cluster_namespace) + "kv_view";
}

// "/cvm/<namespace>/snapshot/segment_view"
inline std::string SegmentViewSnapshotKey(const std::string& cluster_namespace) {
    return SnapshotPrefix(cluster_namespace) + "segment_view";
}

// "/cvm/<namespace>/snapshot/vsegment_partition_quota"
inline std::string VSegmentPartitionQuotaSnapshotKey(
    const std::string& cluster_namespace) {
    return SnapshotPrefix(cluster_namespace) + "vsegment_partition_quota";
}

// 集群级 mount 活动时间戳（墙钟毫秒，十进制串）：任意 submaster 的
// mount/remount 处理器刷新；PlanAndPublishDiscoveredQuota 发布前读取并
// 等待静默（W6：防滚动部署中段发布覆盖不完整拓扑的一次性快照）。
inline std::string VSegmentMountActivityKey(
    const std::string& cluster_namespace) {
    return SnapshotPrefix(cluster_namespace) + "vsegment_mount_activity";
}

// ---- Segment neutral entity + per-master mount keys (§3 view layout) ----

// "/cvm/<namespace>/segments/"
inline std::string SegmentNeutralEntityPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "segments/";
}

// "/cvm/<namespace>/segments/<segment_id>"
inline std::string SegmentNeutralEntityKey(const std::string& cluster_namespace,
                                           const std::string& segment_id) {
    return SegmentNeutralEntityPrefix(cluster_namespace) + segment_id;
}

// "/cvm/<namespace>/snapshot/<master_id>/segments/"
inline std::string SnapshotSegmentsPrefix(
    const std::string& cluster_namespace, const std::string& master_id) {
    return SnapshotPrefix(cluster_namespace) + master_id + "/segments/";
}

// "/cvm/<namespace>/snapshot/<master_id>/segments/<segment_id>"
inline std::string SnapshotSegmentMountKey(
    const std::string& cluster_namespace, const std::string& master_id,
    const std::string& segment_id) {
    return SnapshotSegmentsPrefix(cluster_namespace, master_id) + segment_id;
}

// ---- Partition 路由（§5.2 vsegment 预留接口，仅存 owner + epoch）----

// "/cvm/<namespace>/partition_route/"
inline std::string PartitionRoutePrefix(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "partition_route/";
}

// "/cvm/<namespace>/partition_route/<partition_id>"
// 保存 Partition owner + route_epoch（迁移期另存 state 与 target）。迁移完成
// 后键仍保留，仅内容随 owner 切换更新。
inline std::string PartitionRouteKey(const std::string& cluster_namespace,
                                     const std::string& partition_id) {
    return PartitionRoutePrefix(cluster_namespace) + partition_id;
}

// ---- RingSlot 槽位组归属（§16.4 / §16.14.3，P2）----

// "/cvm/<namespace>/ring_slots/"
inline std::string RingSlotAssignPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "ring_slots/";
}

// "/cvm/<namespace>/ring_slots/<rank>"
// rank 零填充 5 位：G 上限 SlotCount()（最大 16384），rank 最大 16383，
// %03u 在 rank >= 1000 时字典序 != 数值序（"1000" < "999"），会破坏 range
// 扫描与 PrefixEnd 的排序前提。5 位覆盖全部合法 rank。
inline std::string RingSlotAssignKey(const std::string& cluster_namespace,
                                     uint32_t rank) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%05u", rank);
    return RingSlotAssignPrefix(cluster_namespace) + buf;
}

// RingSlotAssignKey 的逆运算：从 "ring_slots/<rank>" key 尾部解析零填充
// rank；非合法记录 key（后缀非 <= 5 位纯数字）返回 false。
// 消费方：LoadRingSlotAssign/LoadAllRingSlotAssigns 与 value 内冗余 rank
// 交叉校验（防写错槽）；ring_slots watch 回调（§16.19.4）从事件 key 反解
// rank 以定位受影响的槽位组。
inline bool ParseRankFromRingSlotKey(std::string_view key,
                                     std::string_view prefix,
                                     uint32_t& rank) {
    if (key.size() <= prefix.size()) {
        return false;
    }
    const std::string_view digits = key.substr(prefix.size());
    if (digits.size() > 5 ||
        digits.find_first_not_of("0123456789") != std::string_view::npos) {
        return false;
    }
    uint32_t value = 0;
    for (const char c : digits) {
        value = value * 10u + static_cast<uint32_t>(c - '0');
    }
    rank = value;
    return true;
}

// ---- reshard 意图键（§16.19.1，P4）----

// "/cvm/<namespace>/reshard_intent/"
inline std::string ReshardIntentPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "reshard_intent/";
}

// "/cvm/<namespace>/reshard_intent/<rank>"（零填充规则同 RingSlotAssignKey）
inline std::string ReshardIntentKey(const std::string& cluster_namespace,
                                     uint32_t rank) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%05u", rank);
    return ReshardIntentPrefix(cluster_namespace) + buf;
}

}  // namespace cvm
}  // namespace mooncake
