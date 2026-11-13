#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace mooncake::vsegment {

enum class MappingAlgorithm : uint8_t { ROUND_ROBIN = 0 };
enum class Lifecycle : uint8_t { PREPARING, ACTIVE, DRAINING, RETIRED };

struct PSegmentExtent {
    std::string segment_id;
    uint64_t base_offset{0};
    uint64_t length{0};

    bool operator==(const PSegmentExtent& other) const {
        return segment_id == other.segment_id &&
               base_offset == other.base_offset && length == other.length;
    }
};
YLT_REFL(PSegmentExtent, segment_id, base_offset, length);

struct VSegmentProfile {
    std::string name;
    uint32_t member_count{0};
    uint64_t stripe_size{0};
    uint64_t member_extent_size{0};
    uint64_t io_alignment{1};
    std::string required_medium;
    uint32_t initial_vsegment_count{1};
};
YLT_REFL(VSegmentProfile, name, member_count, stripe_size,
         member_extent_size, io_alignment, required_medium,
         initial_vsegment_count);

using VSegmentProfileSpec = VSegmentProfile;

struct PartitionPSegmentQuota {
    std::string segment_id;
    uint64_t base_offset{0};
    uint64_t length{0};
};
YLT_REFL(PartitionPSegmentQuota, segment_id, base_offset, length);

struct PartitionVSegmentConfig {
    std::string partition_id;
    std::string profile_name;
    uint32_t initial_vsegment_count{0};
    uint64_t config_generation{0};
    std::vector<PartitionPSegmentQuota> quotas;
};
YLT_REFL(PartitionVSegmentConfig, partition_id, profile_name,
         initial_vsegment_count, config_generation, quotas);

struct PSegmentGeometry {
    std::string segment_id;
    // 可用于 vsegment 切分的容量（不含已被其他分配器占用的 used_bytes）。
    uint64_t capacity{0};
    // vsegment 切分起点（= desc.used_bytes）。默认 0，向后兼容旧 JSON。
    // Plan 按 [base_offset, base_offset + capacity) 切分 extent，避免与
    // 其他分配器占用范围 [0, used_bytes) 重叠。
    uint64_t base_offset{0};
    uint64_t io_alignment{1};
    std::string medium;
    bool healthy{true};
    bool supports_unaligned_io{true};
    std::string failure_domain;
};
YLT_REFL(PSegmentGeometry, segment_id, capacity, base_offset, io_alignment,
         medium, healthy, supports_unaligned_io, failure_domain);

struct PartitionPhysicalQuota {
    std::string partition_id;
    std::string profile_name;
    std::string medium;
    std::vector<PSegmentExtent> extents;
};
YLT_REFL(PartitionPhysicalQuota, partition_id, profile_name, medium, extents);

// Immutable cluster-wide result produced by PartitionQuotaPlanner. It is small
// control-plane metadata and is published as one CAS-protected ETCD value.
struct PartitionPhysicalQuotaSnapshot {
    uint64_t config_generation{0};
    std::string policy_digest;
    std::string default_profile;
    std::vector<VSegmentProfileSpec> profile_specs;
    std::vector<PartitionPhysicalQuota> quotas;
};
YLT_REFL(PartitionPhysicalQuotaSnapshot, config_generation, policy_digest,
         default_profile, profile_specs, quotas);

struct VSegmentView {
    std::string vsegment_id;
    std::string partition_id;
    MappingAlgorithm mapping_algorithm{MappingAlgorithm::ROUND_ROBIN};
    uint64_t stripe_size{0};
    uint64_t logical_capacity{0};
    std::vector<PSegmentExtent> members;
    uint32_t checksum{0};
};
YLT_REFL(VSegmentView, vsegment_id, partition_id, mapping_algorithm,
         stripe_size, logical_capacity, members, checksum);

// 写路径加速：结构化相等比较（供 vsegment HA delta 构建使用，替代 JSON
// 双序列化比较；语义与逐字段 JSON 对比一致）。
inline bool operator==(const VSegmentView& left, const VSegmentView& right) {
    return left.vsegment_id == right.vsegment_id &&
           left.partition_id == right.partition_id &&
           left.mapping_algorithm == right.mapping_algorithm &&
           left.stripe_size == right.stripe_size &&
           left.logical_capacity == right.logical_capacity &&
           left.members == right.members && left.checksum == right.checksum;
}

struct VSegmentAllocationResult {
    ErrorCode error{ErrorCode::OK};
    VSegmentView view;
    std::string detail;

    explicit operator bool() const { return error == ErrorCode::OK; }
};

// Checks immutable layout rules. The configured member count is a hard
// requirement: validation and allocation never silently reduce it.
ErrorCode ValidateProfile(const VSegmentProfile& profile,
                          std::string* detail = nullptr);
ErrorCode ValidatePartitionConfig(const PartitionVSegmentConfig& config,
                                  std::string* detail = nullptr);
ErrorCode ValidatePublishedConfig(
    const std::vector<VSegmentProfile>& profiles,
    const std::vector<PartitionVSegmentConfig>& partitions,
    const std::vector<PSegmentGeometry>& segments,
    const std::unordered_map<std::string, uint64_t>& current_generations,
    std::string* detail = nullptr);
ErrorCode ValidateView(const VSegmentView& view,
                       const VSegmentProfile& profile,
                       std::string* detail = nullptr);
ErrorCode ValidateViewStructure(const VSegmentView& view,
                                std::string* detail = nullptr);
ErrorCode ValidateQuotaSnapshot(
    const PartitionPhysicalQuotaSnapshot& snapshot,
    const std::vector<PSegmentGeometry>& segments,
    std::string* detail = nullptr);

// Builds the legacy partition-local allocator input from one profile in a
// published snapshot. This keeps the allocator independent from ETCD while
// allowing the runtime to consume the new multi-profile model.
ErrorCode BuildPartitionConfig(
    const PartitionPhysicalQuotaSnapshot& snapshot,
    const std::string& partition_id, const std::string& profile_name,
    PartitionVSegmentConfig* config, std::string* detail = nullptr);

uint32_t ComputeViewChecksum(const VSegmentView& view);

// Owns only the physical ranges statically assigned to one Partition. Calls
// are serialized so concurrent SubMaster requests cannot allocate the same
// range twice.
class PartitionQuotaAllocator {
   public:
    explicit PartitionQuotaAllocator(PartitionVSegmentConfig config);
    explicit PartitionQuotaAllocator(
        std::vector<PartitionVSegmentConfig> configs);

    // excluded_segments：运行时坏端点排除集（te_endpoint 与 segment_name 双
    // 形式的 string 集合，见 MasterService::invalid_replica_endpoints_）。条带
    // 成员选择时跳过其中的段；可用段不足 member_count 时按配额不足失败，
    // 避免把新 vsegment 的成员分配到已判死的物理段上。
    VSegmentAllocationResult Allocate(
        const std::string& vsegment_id, const VSegmentProfile& profile,
        const std::set<std::string>* excluded_segments = nullptr);
    ErrorCode Restore(const VSegmentView& view,
                      const VSegmentProfile& profile,
                      std::string* detail = nullptr);
    ErrorCode Release(const VSegmentView& view);

    uint64_t FreeBytes(const std::string& segment_id) const;
    uint64_t FreeBytes(const std::string& profile_name,
                       const std::string& segment_id) const;

   private:
    struct Range {
        uint64_t offset;
        uint64_t length;
    };

    static void InsertAndMerge(std::vector<Range>& ranges, Range range);

    struct AllocationRecord {
        std::string profile_name;
        VSegmentView view;
    };
    std::unordered_map<std::string, PartitionVSegmentConfig> configs_;
    mutable std::mutex mutex_;
    std::map<std::string, std::map<std::string, std::vector<Range>>>
        free_ranges_;
    std::unordered_map<std::string, AllocationRecord> allocations_;
};

}  // namespace mooncake::vsegment
