#include "vsegment/partition_quota_planner.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>

#include "etcd_helper.h"
#include "crc32c.h"
#include "cvm/cvm_keys.h"
#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"

namespace mooncake::vsegment {
namespace {

PartitionQuotaPlanResult Fail(ErrorCode error, std::string detail) {
    return {error, {}, std::move(detail)};
}

uint64_t AlignDown(uint64_t value, uint64_t alignment) {
    return value - value % alignment;
}

}  // namespace

ErrorCode BuildDiscoveredQuotaPlan(
    const VSegmentUserPolicy& policy,
    const std::vector<cvm::SegmentDescriptor>& descriptors,
    const std::vector<cvm::MasterRegistration>& masters,
    const std::vector<std::pair<std::string, cvm::MountEntry>>& mounts,
    PartitionQuotaPlanRequest* request, std::string* detail,
    bool allow_non_exclusive) {
    (void)allow_non_exclusive;  // 已默认接受非独占 segment，保留参数兼容旧调用
    if (!request) return ErrorCode::INVALID_PARAMS;
    if (policy.member_count == 0 || policy.stripe_size == 0 ||
        policy.member_extent_size == 0) {
        if (detail)
            *detail = "member_count, stripe_size and member_extent_size are "
                      "required user policy parameters";
        return ErrorCode::INVALID_PARAMS;
    }
    if (policy.reserved_ratio < 0.0 || policy.reserved_ratio >= 1.0) {
        if (detail) *detail = "reserved_ratio must be in [0, 1)";
        return ErrorCode::INVALID_PARAMS;
    }
    if (policy.default_profile.empty() || policy.profile_name.empty()) {
        if (detail)
            *detail = "default_profile and profile_name must not be empty";
        return ErrorCode::INVALID_PARAMS;
    }

    PartitionQuotaPlanRequest next;
    next.config_generation = policy.config_generation;
    next.default_profile = policy.default_profile;
    next.reserved_ratio = policy.reserved_ratio;
    // required_medium 模式：
    //   - 非空（用户显式指定）：单一 profile，只接受匹配 medium 的 segment
    //   - 空（auto）：系统自动发现所有介质，为每种介质建独立 profile
    //     （继承 member_count/stripe_size/member_extent_size），每种介质
    //     的 segment 分别切分给各 partition。default_profile 选首个发现的
    //     介质对应的 profile。
    const bool auto_medium_mode = policy.required_medium.empty();
    if (!auto_medium_mode) {
        next.profile_specs = {{policy.profile_name, policy.member_count,
                               policy.stripe_size, policy.member_extent_size,
                               policy.io_alignment, policy.required_medium,
                               policy.initial_vsegment_count}};
        auto error = ValidateProfile(next.profile_specs.front(), detail);
        if (error != ErrorCode::OK) return error;
    }

    // 在线 master 集合（用于过滤 mount 记录：仅 live master 的 mount 才算
    // 该 psegment 在线）。
    std::set<std::string> live_masters;
    for (const auto& master : masters)
        if (!master.master_id.empty()) live_masters.insert(master.master_id);
    std::set<std::string> mounted;
    for (const auto& [master, mount] : mounts)
        if (live_masters.count(master)) mounted.insert(mount.segment_id);

    // 从 SegmentDescriptor 自动发现资源事实（介质/对齐/对齐能力/故障域），
    // 不再硬编码。按 [used_bytes, capacity) 作为 vsegment 可切分范围，避免
    // 与其他分配器占用范围重叠。vsegment_exclusive 降级为可选 hint：true
    // 时校验 used_bytes==0（快路径，直接按 capacity 切分）；false 时按
    // [used_bytes, capacity) 切分，无需用户手动声明。
    std::set<std::string> seen;
    std::vector<std::string> insufficient_space;
    for (const auto& desc : descriptors) {
        if (desc.segment_id.empty() || !seen.insert(desc.segment_id).second) {
            if (detail) *detail = "empty or duplicate CVM segment id";
            return ErrorCode::INVALID_PARAMS;
        }
        if (!mounted.count(desc.segment_id) || desc.capacity == 0 ||
            desc.te_endpoint.empty())
            continue;  // 离线或无效，跳过
        if (desc.medium.empty()) {
            if (detail) {
                *detail = "segment " + desc.segment_id +
                          " has empty medium; allocator must report resource "
                          "facts at mount time";
            }
            return ErrorCode::INVALID_PARAMS;
        }
        // vsegment_exclusive=true 时要求 used_bytes==0，否则声明与实际不符。
        if (desc.vsegment_exclusive && desc.used_bytes > 0) {
            if (detail) {
                *detail = "segment " + desc.segment_id +
                          " declared vsegment_exclusive=true but used_bytes=" +
                          std::to_string(desc.used_bytes) +
                          " > 0; exclusive segment must be empty";
            }
            return ErrorCode::INVALID_PARAMS;
        }
        // 可用空间 = 总容量 - 已用字节。used_bytes > capacity 视为 0 可用。
        uint64_t available = (desc.used_bytes >= desc.capacity)
                                 ? 0
                                 : desc.capacity - desc.used_bytes;
        if (available < policy.member_extent_size) {
            insufficient_space.push_back(desc.segment_id);
            continue;  // 可用空间不足，跳过
        }
        // failure_domain 缺省由 host_id 兜底，仍为空时用 segment_id 兜底，
        // 保证 PSegmentGeometry.failure_domain 非空（便于后续故障域隔离）。
        std::string failure_domain = desc.failure_domain;
        if (failure_domain.empty())
            failure_domain = desc.host_id.empty() ? desc.segment_id
                                                  : desc.host_id;
        // capacity=可用容量，base_offset=used_bytes（切分起点）。
        next.segments.push_back({desc.segment_id, available,
                                 desc.used_bytes,
                                 desc.io_alignment == 0 ? 1 : desc.io_alignment,
                                 desc.medium, /*healthy=*/true,
                                 desc.supports_unaligned_io, failure_domain});
    }
    // auto 模式：按介质分组，为每种 segment 数 >= member_count 的介质自动
    // 建独立 profile；segment 数不足的介质跳过（其 segment 不参与分配）。
    // 非 auto 模式：保留匹配 required_medium 的 segment，总数 < member_count
    // 即失败（原行为）。
    if (auto_medium_mode) {
        std::map<std::string, size_t> per_medium_count;
        for (const auto& seg : next.segments)
            ++per_medium_count[seg.medium];
        std::set<std::string> accepted_media;
        for (const auto& [medium, count] : per_medium_count) {
            if (count >= policy.member_count) accepted_media.insert(medium);
        }
        std::vector<PSegmentGeometry> accepted_segments;
        for (const auto& seg : next.segments)
            if (accepted_media.count(seg.medium))
                accepted_segments.push_back(seg);
        next.segments = std::move(accepted_segments);
        // 为每个接受的介质建独立 profile（继承用户配的 3 个核心参数）。
        for (const auto& medium : accepted_media) {
            next.profile_specs.push_back(
                {policy.profile_name + "-" + medium, policy.member_count,
                 policy.stripe_size, policy.member_extent_size,
                 policy.io_alignment, medium,
                 policy.initial_vsegment_count});
        }
        if (next.profile_specs.empty()) {
            if (detail) {
                std::ostringstream os;
                os << "auto medium mode: no medium has >= member_count="
                   << policy.member_count
                   << " segments; per-medium counts: ";
                bool first = true;
                for (const auto& [medium, count] : per_medium_count) {
                    if (!first) os << ", ";
                    first = false;
                    os << medium << "=" << count;
                }
                *detail = os.str();
            }
            return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
        }
        // default_profile 选首个接受的介质（按字典序，确定性好复现）。
        next.default_profile = next.profile_specs.front().name;
        // 校验每个自动建的 profile。
        for (const auto& profile : next.profile_specs) {
            auto error = ValidateProfile(profile, detail);
            if (error != ErrorCode::OK) return error;
        }
    } else {
        // 非 auto 模式：校验匹配 required_medium 的 segment 数 >= member_count。
        size_t matched = 0;
        for (const auto& seg : next.segments)
            if (seg.medium == policy.required_medium) ++matched;
        if (matched < policy.member_count) {
            if (detail) {
                std::ostringstream os;
                os << "insufficient segments with usable free space for "
                      "member_count="
                   << policy.member_count
                   << ", medium=" << policy.required_medium
                   << ", discovered=" << matched;
                if (!insufficient_space.empty()) {
                    os << " (additionally " << insufficient_space.size()
                       << " segments skipped due to insufficient free space: ";
                    for (size_t i = 0; i < insufficient_space.size(); ++i) {
                        if (i) os << ",";
                        os << insufficient_space[i];
                    }
                    os << ")";
                }
                *detail = os.str();
            }
            return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
        }
    }
    if (next.segments.empty()) {
        if (detail) *detail = "no segments with usable free space discovered";
        return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
    }
    std::sort(next.segments.begin(), next.segments.end(),
              [](const auto& a, const auto& b) {
                  return a.segment_id < b.segment_id;
              });
    // Partition 列表由 KV PT 生成（确定性哈希方案，slot 即 partition）。
    for (uint16_t slot = 0; slot < cvm::SlotCount(); ++slot)
        next.partition_ids.push_back(std::to_string(slot));
    std::string serialized;
    struct_json::to_json(next, serialized);
    Crc32c crc;
    crc.Extend(serialized.data(), serialized.size());
    next.policy_digest = "plan-crc32c-" + std::to_string(crc.Final());
    *request = std::move(next);
    return ErrorCode::OK;
}

PartitionQuotaPlanResult PartitionQuotaPlanner::Plan(
    const PartitionQuotaPlanRequest& request) const {
    if (request.config_generation == 0 || request.policy_digest.empty() ||
        request.default_profile.empty() || request.partition_ids.empty() ||
        request.profile_specs.empty() || request.segments.empty() ||
        request.reserved_ratio < 0.0 || request.reserved_ratio >= 1.0) {
        return Fail(ErrorCode::INVALID_PARAMS,
                    "invalid quota planning request");
    }
    // 保留 request.partition_ids 的原始顺序（数字序），仅用 set 去重校验。
    // 不能用 set 重排：数字序 "0".."16383" 会被字典序打乱（如 "1386" < "13860"），
    // 导致 base_offset 布局与 VerifyDiscoveredSnapshot 的遍历顺序不一致。
    std::set<std::string> partition_ids;
    std::vector<std::string> ordered_partitions;
    for (const auto& id : request.partition_ids) {
        if (id.empty() || !partition_ids.insert(id).second)
            return Fail(ErrorCode::INVALID_PARAMS,
                        "empty or duplicate partition id");
        ordered_partitions.push_back(id);
    }

    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = request.config_generation;
    snapshot.policy_digest = request.policy_digest;
    snapshot.default_profile = request.default_profile;
    snapshot.profile_specs = request.profile_specs;
    std::sort(snapshot.profile_specs.begin(), snapshot.profile_specs.end(),
              [](const auto& a, const auto& b) { return a.name < b.name; });
    if (std::adjacent_find(snapshot.profile_specs.begin(),
                           snapshot.profile_specs.end(),
                           [](const auto& a, const auto& b) {
                               return a.name == b.name;
                           }) != snapshot.profile_specs.end())
        return Fail(ErrorCode::INVALID_PARAMS, "duplicate profile name");

    std::map<std::string, std::vector<PSegmentGeometry>> by_medium;
    for (const auto& segment : request.segments) {
        if (segment.healthy && segment.supports_unaligned_io)
            by_medium[segment.medium].push_back(segment);
    }
    for (auto& [medium, segments] : by_medium)
        std::sort(segments.begin(), segments.end(), [](const auto& a,
                                                       const auto& b) {
            return a.segment_id < b.segment_id;
        });

    // A physical byte can belong to only one profile. Balanced mode therefore
    // requires profile pools to be disjoint; medium is the available hard
    // selector in the current psegment inventory model.
    std::map<std::string, std::vector<const VSegmentProfileSpec*>> profiles;
    for (const auto& profile : snapshot.profile_specs) {
        std::string detail;
        if (ValidateProfile(profile, &detail) != ErrorCode::OK)
            return Fail(ErrorCode::INVALID_PARAMS, std::move(detail));
        profiles[profile.required_medium].push_back(&profile);
    }
    if (std::none_of(snapshot.profile_specs.begin(), snapshot.profile_specs.end(),
                     [&](const auto& profile) {
                         return profile.name == snapshot.default_profile;
                     }))
        return Fail(ErrorCode::INVALID_PARAMS, "unknown default profile");

    for (const auto& [medium, medium_profiles] : profiles) {
        if (medium_profiles.size() != 1) {
            return Fail(ErrorCode::INVALID_PARAMS,
                        "balanced profiles overlap psegment medium " + medium);
        }
        auto pool = by_medium.find(medium);
        if (pool == by_medium.end() || pool->second.empty())
            return Fail(ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT,
                        "no eligible psegments for medium " + medium);
        for (const auto* profile : medium_profiles) {
            if (pool->second.size() < profile->member_count)
                return Fail(ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT,
                            "profile " + profile->name + " requires " +
                                std::to_string(profile->member_count) +
                                " psegments");
        }

        for (const auto* profile_ptr : medium_profiles) {
            const auto& profile = *profile_ptr;
            std::map<std::string, PartitionPhysicalQuota> quotas;
            for (const auto& partition : ordered_partitions) {
                auto& quota = quotas[partition];
                quota.partition_id = partition;
                quota.profile_name = profile.name;
                quota.medium = medium;
            }
            for (const auto& segment : pool->second) {
                const uint64_t usable = AlignDown(
                    static_cast<uint64_t>(std::floor(
                        static_cast<long double>(segment.capacity) *
                        (1.0L - request.reserved_ratio))),
                    segment.io_alignment);
                const uint64_t per_partition = AlignDown(
                    usable / ordered_partitions.size(),
                    std::max<uint64_t>(segment.io_alignment,
                                       profile.io_alignment));
                for (size_t partition_index = 0;
                     partition_index < ordered_partitions.size();
                     ++partition_index) {
                    if (per_partition == 0) continue;
                    quotas[ordered_partitions[partition_index]].extents.push_back(
                        {segment.segment_id,
                         segment.base_offset + per_partition * partition_index,
                         per_partition});
                }
            }
            for (auto& [partition, quota] : quotas)
                snapshot.quotas.push_back(std::move(quota));
        }
    }
    std::string detail;
    auto validation = ValidateQuotaSnapshot(snapshot, request.segments, &detail);
    if (validation != ErrorCode::OK)
        return Fail(ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT,
                    std::move(detail));
    return {ErrorCode::OK, std::move(snapshot), {}};
}

ErrorCode EtcdPartitionQuotaSnapshotStore::Create(
    const PartitionPhysicalQuotaSnapshot& snapshot,
    size_t max_serialized_bytes, std::string* detail) {
    std::string value;
    struct_json::to_json(snapshot, value);
    if (value.size() > max_serialized_bytes) {
        if (detail) {
            std::ostringstream stream;
            stream << "quota snapshot is " << value.size()
                   << " bytes, ETCD limit is " << max_serialized_bytes
                   << ", profiles=" << snapshot.profile_specs.size()
                   << ", quotas=" << snapshot.quotas.size();
            *detail = stream.str();
        }
        return ErrorCode::INVALID_PARAMS;
    }
    auto result = EtcdHelper::Create(key_.data(), key_.size(), value.data(),
                                     value.size());
    if (result != ErrorCode::OK && detail)
        *detail = result == ErrorCode::ETCD_TRANSACTION_FAIL
                      ? "a quota snapshot is already published"
                      : "failed to publish quota snapshot to ETCD";
    return result;
}

ErrorCode EtcdPartitionQuotaSnapshotStore::Load(
    PartitionPhysicalQuotaSnapshot* snapshot, std::string* detail) {
    if (!snapshot) return ErrorCode::INVALID_PARAMS;
    std::string value;
    EtcdRevisionId revision = 0;
    auto result = EtcdHelper::Get(key_.data(), key_.size(), value, revision);
    if (result == ErrorCode::ETCD_KEY_NOT_EXIST) {
        result = EtcdHelper::Get(
            kLegacyPartitionQuotaSnapshotKey,
            sizeof(kLegacyPartitionQuotaSnapshotKey) - 1, value, revision);
        if (result == ErrorCode::OK && detail) {
            *detail = "loaded legacy global quota snapshot; republish it "
                      "under the cluster-scoped key";
        }
    }
    if (result != ErrorCode::OK) {
        if (detail) *detail = "failed to load quota snapshot from ETCD";
        return result;
    }
    try {
        struct_json::from_json(*snapshot, value);
    } catch (const std::exception& error) {
        if (detail) *detail = std::string("invalid quota snapshot: ") + error.what();
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

ErrorCode PlanAndPublishDiscoveredQuota(
    const std::string& cluster_namespace, const VSegmentUserPolicy& policy,
    size_t max_etcd_value_bytes, std::string* detail,
    std::string* out_summary_json) {
    // ---- 1. 从 etcd 现拉资源事实（segments / masters / mounts）----
    std::vector<cvm::SegmentDescriptor> descriptors;
    std::vector<cvm::MasterRegistration> masters;
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts;
    ViewVersionId revision = 0;
    auto error = cvm::EtcdViewStore::LoadAllMasters(cluster_namespace, masters,
                                                    revision);
    if (error == ErrorCode::OK)
        error = cvm::EtcdViewStore::LoadAllSegmentDescriptors(
            cluster_namespace, descriptors, revision);
    if (error == ErrorCode::OK)
        error = cvm::EtcdViewStore::LoadAllMountEntries(cluster_namespace,
                                                        mounts, revision);
    if (error != ErrorCode::OK) {
        if (detail) *detail = "CVM discovery failed: " + toString(error);
        return error;
    }

    // ---- 1b. W6 集群级 mount 静默期门控 ----
    // quota 首写一次定型（first-writer-wins）：滚动部署中段（后续 worker 的
    // 段尚未挂齐）发布会得到覆盖不完整拓扑的快照。mount/remount 处理器把
    // 最近活动墙钟毫秒写入 etcd 共享 key（跨 submaster 生效——静默信号在
    // 集群层面成立，与触发方所在进程无关）；未静默满门控窗口则本轮拒绝
    // 发布，调用方按可重试处理。时钟为各 submaster 墙钟，秒级偏差对窗口
    // 可忽略。
    // 窗口默认 10s（小集群/bench 场景 30s 过长导致写请求在挂载完成后仍
    // STALE_ROUTE 达 30~40s）；大规模滚动部署可经
    // MOONCAKE_VSEGMENT_MOUNT_QUIET_MS 调高；<=0 关闭门控（仅供测试）。
    {
        static const int64_t kMountQuietPeriodMs = [] {
            const char* env = std::getenv("MOONCAKE_VSEGMENT_MOUNT_QUIET_MS");
            return env != nullptr ? std::atoll(env) : 10000;
        }();
        if (kMountQuietPeriodMs > 0) {
            const auto activity_key =
                cvm::VSegmentMountActivityKey(cluster_namespace);
            std::string activity;
            EtcdRevisionId revision = 0;
            if (EtcdHelper::Get(activity_key.data(), activity_key.size(),
                                activity,
                                revision) == ErrorCode::OK &&
                !activity.empty() &&
                activity.find_first_not_of("0123456789") ==
                    std::string::npos) {
                const int64_t last_ms = std::stoll(activity);
                const auto now_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now()
                            .time_since_epoch())
                        .count();
                if (last_ms > 0 && now_ms - last_ms < kMountQuietPeriodMs) {
                    if (detail) *detail = "mount stream not settled yet";
                    return ErrorCode::VSEGMENT_MOUNT_QUIET_PERIOD;
                }
            }
        }
    }

    // ---- 2. 构建规划请求 + 计算（三核心参数校验在 Build 内）----
    PartitionQuotaPlanRequest request;
    error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                     &request, detail);
    if (error != ErrorCode::OK) return error;
    auto result = PartitionQuotaPlanner().Plan(request);
    if (!result) {
        if (detail) *detail = result.detail;
        return result.error;
    }

    // ---- 3. 发布（etcd 原子首写：key 已存在 → ETCD_TRANSACTION_FAIL）----
    EtcdPartitionQuotaSnapshotStore store(cluster_namespace);
    error = store.Create(result.snapshot, max_etcd_value_bytes, detail);
    if (error != ErrorCode::OK) return error;

    LOG(INFO) << "published vsegment quota generation "
              << result.snapshot.config_generation << " with "
              << result.snapshot.quotas.size()
              << " partition/profile quotas (cluster=" << cluster_namespace
              << ", discovered " << request.segments.size() << " segments)";
    if (out_summary_json) {
        std::ostringstream os;
        os << "{\"status\":\"ok\",\"generation\":"
           << result.snapshot.config_generation << ",\"partitions\":"
           << request.partition_ids.size() << ",\"quotas\":"
           << result.snapshot.quotas.size() << ",\"profiles\":"
           << result.snapshot.profile_specs.size()
           << ",\"discovered_segments\":" << request.segments.size() << "}";
        *out_summary_json = os.str();
    }
    return ErrorCode::OK;
}

void LogVSegmentPoolFormed(const std::string& cluster_namespace) {
    PartitionPhysicalQuotaSnapshot snapshot;
    std::string detail;
    EtcdPartitionQuotaSnapshotStore store(cluster_namespace);
    if (store.Load(&snapshot, &detail) != ErrorCode::OK) return;

    // psegment id -> descriptor（endpoint/medium），用于成员清单的
    // id@store(numaN) 呈现；加载失败时退化为仅打印 id。
    std::map<std::string, cvm::SegmentDescriptor> descriptors;
    {
        std::vector<cvm::SegmentDescriptor> loaded;
        ViewVersionId revision = 0;
        if (cvm::EtcdViewStore::LoadAllSegmentDescriptors(cluster_namespace,
                                                          loaded, revision) ==
            ErrorCode::OK) {
            for (const auto& d : loaded) descriptors[d.segment_id] = d;
        }
    }
    auto numa_of = [](const std::string& medium) -> std::string {
        const std::string kPrefix = "cpu:";
        if (medium.rfind(kPrefix, 0) == 0 &&
            medium.size() > kPrefix.size()) {
            try {
                return "numa" + std::to_string(
                                    std::stoi(medium.substr(kPrefix.size())));
            } catch (const std::exception&) {
            }
        }
        return "n/a";
    };

    std::ostringstream os;
    os << "VSegment pool formed: generation=" << snapshot.config_generation
       << ", profiles=" << snapshot.profile_specs.size();
    for (size_t i = 0; i < snapshot.profile_specs.size(); ++i) {
        const auto& profile = snapshot.profile_specs[i];
        os << (i == 0 ? " | " : "; ") << "profile[" << i << "]: name="
           << profile.name << ", medium=" << profile.required_medium
           << ", member_count=" << profile.member_count
           << ", stripe_size=" << profile.stripe_size
           << ", member_extent_size=" << profile.member_extent_size
           << ", members=[";
        // 取 partition "0" 在该 profile 下的配额 extents 作为成员池清单
        // （所有 partition 的成员集合相同：候选 = 池内全部 psegment）。
        bool first_member = true;
        for (const auto& quota : snapshot.quotas) {
            if (quota.partition_id != "0" || quota.profile_name != profile.name)
                continue;
            for (const auto& extent : quota.extents) {
                if (!first_member) os << ", ";
                first_member = false;
                auto found = descriptors.find(extent.segment_id);
                if (found != descriptors.end()) {
                    os << extent.segment_id << "@"
                       << found->second.te_endpoint << "("
                       << numa_of(found->second.medium) << ")";
                } else {
                    os << extent.segment_id;
                }
            }
            break;
        }
        os << "]";
    }
    LOG(INFO) << os.str();
}

}  // namespace mooncake::vsegment
