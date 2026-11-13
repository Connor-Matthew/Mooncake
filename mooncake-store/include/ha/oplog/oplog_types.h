#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "types.h"
#include "weight_management.h"

namespace mooncake {

enum class OpType : uint8_t {
    PUT_END = 1,
    PUT_REVOKE = 2,
    REMOVE = 3,
    LEASE_RENEW = 4,
    SEGMENT_MOUNT = 5,
    SEGMENT_UNMOUNT = 6,
    SEGMENT_UPDATE = 7,
    WEIGHT_METADATA_UPSERT = 8,
    WEIGHT_METADATA_DELETE = 9,
    WEIGHT_LEASE_UPSERT = 10,
    WEIGHT_LEASE_DELETE = 11,
    VSEGMENT_STATE = 12,
    // aligned 模式（MC_VSEGMENT_OPLOG_MODE=aligned）下 put 热路径的 reservation
    // 生命周期记录。payload 为序列化的 PartitionOpLogEntry：
    //   VSEGMENT_RESERVE -> event_type = kReserve，payload = {vsegment_id, offset, length}
    //   VSEGMENT_RELEASE -> event_type = kAbort，仅用于 abort / timeout（commit 不写）
    // 未启用 aligned 模式下这两类记录不会被产生。
    VSEGMENT_RESERVE = 13,
    VSEGMENT_RELEASE = 14,
    OP_TYPE_MAX,
};

struct WeightMetadataUpsertOp {
    WeightRevisionMetadata metadata;
    std::optional<WeightResidencyOperation> operation;

    friend bool operator==(const WeightMetadataUpsertOp&,
                           const WeightMetadataUpsertOp&) = default;
};
YLT_REFL(WeightMetadataUpsertOp, metadata, operation);

struct WeightMetadataDeleteOp {
    WeightRevisionIdentity identity;
    uint64_t metadata_generation{0};

    friend bool operator==(const WeightMetadataDeleteOp&,
                           const WeightMetadataDeleteOp&) = default;
};
YLT_REFL(WeightMetadataDeleteOp, identity, metadata_generation);

struct WeightLeaseDeleteOp {
    uint64_t lease_id{0};
    WeightRevisionIdentity identity;
    uint64_t fenced_metadata_generation{0};

    friend bool operator==(const WeightLeaseDeleteOp&,
                           const WeightLeaseDeleteOp&) = default;
};
YLT_REFL(WeightLeaseDeleteOp, lease_id, identity, fenced_metadata_generation);

struct SegmentMountOp {
    std::string segment_name;
    std::string transport_endpoint;
    uint64_t capacity{0};
    bool is_memory_segment{false};
    std::string file_path;

    YLT_REFL(SegmentMountOp, segment_name, transport_endpoint, capacity,
             is_memory_segment, file_path);
};

struct SegmentUnmountOp {
    std::string transport_endpoint;

    YLT_REFL(SegmentUnmountOp, transport_endpoint);
};

struct SegmentUpdateOp {
    std::string segment_name;
    std::string transport_endpoint;
    uint64_t capacity{0};
    bool is_memory_segment{false};
    std::string file_path;

    YLT_REFL(SegmentUpdateOp, segment_name, transport_endpoint, capacity,
             is_memory_segment, file_path);
};

struct OpLogEntry {
    uint64_t sequence_id{0};
    uint64_t timestamp_ms{0};
    OpType op_type{OpType::PUT_END};
    std::string tenant_id{"default"};
    std::string object_key;
    std::string payload;
    uint32_t checksum{0};
    uint32_t prefix_hash{0};
};

// ---------------------------------------------------------------------------
// vsegment 持久化 OpLog 协议（§12.3.8 预留）
//
// 与现有 OpLogEntry（对象级、按 kv/segment 事件）平行：Partition / psegment
// allocation 各维护独立的 revision 与事件流，供恢复对账使用。运行期逻辑与
// payload 字节布局由 vsegment 实现方定义后补充。当前仅冻结字段契约。
// ---------------------------------------------------------------------------

// Partition 级元数据事件类型：对应 PutStart(PutRevoke)/PutEnd 对分区元数据
// 的预留/提交/撤销。
enum class PartitionOpEventType : uint8_t {
    kReserve = 0,
    kCommit = 1,
    kAbort = 2,
};

// psegment 物理分配事件类型：RESERVE/COMMIT/ABORT/RELEASE 四类 extent 仲裁。
enum class PSegmentAllocationEventType : uint8_t {
    kReserve = 0,
    kCommit = 1,
    kAbort = 2,
    kRelease = 3,
};

// Partition 级元数据 OpLog 条目。operation_id 承载两阶段写预留状态关联；
// payload 为序列化后的操作详情（逻辑区间 / ObjectMetadata 等）。
struct PartitionOpLogEntry {
    std::string partition_id;
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::string operation_id;
    int32_t event_type{0};  // PartitionOpEventType
    std::string payload;
};
YLT_REFL(PartitionOpLogEntry, partition_id, route_epoch, metadata_revision,
         operation_id, event_type, payload);

// aligned 模式下 VSEGMENT_RESERVE 记录的 payload：
// 记录被预留的逻辑区间与所属 vsegment，重放时据此恢复 reservations_ 与
// operation_vsegments_（operation_id 在 PartitionOpLogEntry 里）。
struct VSegmentReservationPayload {
    std::string vsegment_id;
    uint64_t offset{0};
    uint64_t length{0};
};
YLT_REFL(VSegmentReservationPayload, vsegment_id, offset, length);

// psegment 物理分配 OpLog 条目。allocation_id 为幂等键，payload 为序列化后的
// PSegmentExtent 等详情。
struct PSegmentAllocationOpLogEntry {
    std::string segment_id;
    uint64_t allocator_epoch{0};
    uint64_t allocation_revision{0};
    std::string allocation_id;
    int32_t event_type{0};  // PSegmentAllocationEventType
    std::string payload;
};
YLT_REFL(PSegmentAllocationOpLogEntry, segment_id, allocator_epoch,
         allocation_revision, allocation_id, event_type, payload);

inline constexpr size_t kMaxOpLogObjectKeySize = 4096;
inline constexpr size_t kMaxOpLogPayloadSize = 10 * 1024 * 1024;

bool NormalizeAndValidateClusterId(std::string& cluster_id);
uint32_t ComputeOpLogChecksum(std::string_view payload);
bool VerifyOpLogChecksum(const OpLogEntry& entry);
bool ValidateOpLogEntrySize(const OpLogEntry& entry,
                            std::string* reason = nullptr);

}  // namespace mooncake
