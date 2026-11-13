#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "cvm/cvm_keys.h"
#include "cvm/cvm_types.h"
#include "vsegment/vsegment.h"

namespace mooncake::vsegment {

inline constexpr char kLegacyPartitionQuotaSnapshotKey[] =
    "/mooncake/vsegment/partition-quota/current";

struct PartitionQuotaPlanRequest {
    uint64_t config_generation{0};
    std::string policy_digest;
    std::string default_profile;
    std::vector<std::string> partition_ids;
    std::vector<VSegmentProfileSpec> profile_specs;
    std::vector<PSegmentGeometry> segments;
    // Fraction reserved outside vsegment quotas, in [0, 1).
    double reserved_ratio{0.0};
};
YLT_REFL(PartitionQuotaPlanRequest, config_generation, policy_digest,
         default_profile, partition_ids, profile_specs, segments,
         reserved_ratio);

// 用户策略：只要求 member_count / stripe_size / member_extent_size 三个核心
// 参数。其余字段为可选策略，提供默认值，不应强制用户填写。
struct VSegmentUserPolicy {
    uint32_t member_count{0};       // 必填
    uint64_t stripe_size{0};        // 必填
    uint64_t member_extent_size{0}; // 必填
    // 可选策略（提供默认值）
    std::string default_profile = "default";
    std::string profile_name = "default";
    // 必填介质筛选。空字符串表示 auto：系统自动发现所有介质，为每种介质
    // 自动建独立 profile（继承 member_count/stripe_size/member_extent_size），
    // 每种介质的 segment 分别切分给各 partition。支持混合介质集群，用户
    // 无需为每种介质单独配置。default_profile 选首个发现的介质。
    std::string required_medium{};
    uint64_t io_alignment = 1;
    uint32_t initial_vsegment_count = 1;
    double reserved_ratio = 0.0;    // 默认不预留
    uint64_t config_generation = 1;
};
YLT_REFL(VSegmentUserPolicy, member_count, stripe_size, member_extent_size,
         default_profile, profile_name, required_medium, io_alignment,
         initial_vsegment_count, reserved_ratio, config_generation);

// 从 CVM 自动发现的资源事实（SegmentDescriptor + MountEntry +
// MasterRegistration）构建配额规划请求。资源事实（psegment ID、容量、
// 介质、对齐能力、已用字节、在线状态）由系统自动发现，不应由用户在配额
// 文件中手写。
//
// 空间安全：按 [used_bytes, capacity) 作为 vsegment 可切分范围，避免与
// 其他分配器占用范围 [0, used_bytes) 重叠。vsegment_exclusive=true 时
// 校验 used_bytes==0（快路径，直接按 capacity 切分）；false 时按
// [used_bytes, capacity) 切分。allow_non_exclusive 为兼容旧调用方保留，
// 当前实现已默认接受非独占 segment，该参数不再有实际效果。
ErrorCode BuildDiscoveredQuotaPlan(
    const VSegmentUserPolicy& policy,
    const std::vector<cvm::SegmentDescriptor>& descriptors,
    const std::vector<cvm::MasterRegistration>& masters,
    const std::vector<std::pair<std::string, cvm::MountEntry>>& mounts,
    PartitionQuotaPlanRequest* request, std::string* detail = nullptr,
    bool allow_non_exclusive = false);

struct PartitionQuotaPlanResult {
    ErrorCode error{ErrorCode::OK};
    PartitionPhysicalQuotaSnapshot snapshot;
    std::string detail;
    explicit operator bool() const { return error == ErrorCode::OK; }
};

class PartitionQuotaPlanner {
   public:
    PartitionQuotaPlanResult Plan(const PartitionQuotaPlanRequest& request) const;
};

class PartitionQuotaSnapshotStore {
   public:
    virtual ~PartitionQuotaSnapshotStore() = default;
    virtual ErrorCode Create(const PartitionPhysicalQuotaSnapshot& snapshot,
                             size_t max_serialized_bytes,
                             std::string* detail = nullptr) = 0;
    virtual ErrorCode Load(PartitionPhysicalQuotaSnapshot* snapshot,
                           std::string* detail = nullptr) = 0;
};

// 配额快照 etcd 单值默认上限（32MiB）：16384 partition 快照实测 ~4.6MB
//（planner CLI 的 1500000 默认值对 KV Partition 集群偏小）。master
// --vsegment_auto_plan 与 CvmHttpServer /vsegment_quota 端点共用本默认。
constexpr uint64_t kDefaultQuotaMaxEtcdValueBytes = 33554432;

class EtcdPartitionQuotaSnapshotStore final
    : public PartitionQuotaSnapshotStore {
   public:
    explicit EtcdPartitionQuotaSnapshotStore(
        const std::string& cluster_namespace)
        : key_(cvm::VSegmentPartitionQuotaSnapshotKey(cluster_namespace)) {}

    ErrorCode Create(const PartitionPhysicalQuotaSnapshot& snapshot,
                     size_t max_serialized_bytes,
                     std::string* detail = nullptr) override;
    ErrorCode Load(PartitionPhysicalQuotaSnapshot* snapshot,
                  std::string* detail = nullptr) override;

   private:
    std::string key_;
};

// 在线「发现→规划→发布」一步式编排（CvmHttpServer POST /vsegment_quota 与
// master --vsegment_auto_plan 启动 bootstrap 共用；planner CLI 的
// RunFromDiscovery + Plan + Create 等价复用）。
//
// 前置：EtcdHelper 已连接（调用方负责）。facts 从 etcd 现拉（segments/
// masters/ mounts），发布走 EtcdPartitionQuotaSnapshotStore::Create——etcd
// 原子首写（key 已存在返回 ETCD_TRANSACTION_FAIL），多实例并发规划天然
// first-writer-wins，不会互相覆盖。
//
// out_summary_json（可选）：成功时回填一行人类可读摘要（generation/
// partitions/quotas/profiles），供 HTTP 响应体与启动日志共用。
ErrorCode PlanAndPublishDiscoveredQuota(
    const std::string& cluster_namespace, const VSegmentUserPolicy& policy,
    size_t max_etcd_value_bytes, std::string* detail = nullptr,
    std::string* out_summary_json = nullptr);

// 配额发布成功后的「组成确定」可观测性日志（vsegment/psegment 术语）：
// 从 etcd 回读刚发布的快照 + 段描述符，逐 profile 打印成员池清单
// （psegment id@store(numaN)）。加载失败静默返回（发布日志已给出
// summary，本函数只是增强，绝不反向影响发布路径）。
void LogVSegmentPoolFormed(const std::string& cluster_namespace);

}  // namespace mooncake::vsegment
