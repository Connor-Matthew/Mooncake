// vsegment_quota_planner: vsegment 静态配额规划工具。
//
// 正常部署方式（自动发现，用户只需 3 个核心策略参数）：
//   vsegment_quota_planner
//       --etcd_endpoints 127.0.0.1:2379
//       --cluster_namespace my-cluster
//       --member_count 4 --stripe_size 65536 --member_extent_size 1048576
//       [--publish]
//
//   系统从 CVM 自动获取已注册 psegment（含 medium/io_alignment/
//   supports_unaligned_io/failure_domain/used_bytes 等资源事实），
//   从 KV PT 自动获取 Partition 列表，校验容量/介质/对齐能力和可分配范围
//   后，计算各 Partition 的静态配额并（可选）发布到 ETCD。默认 dry-run，
//   加 --publish 才真正发布。
//
//   空间安全：按 [used_bytes, capacity) 作为 vsegment 可切分范围，避免与
//   其他分配器占用范围重叠。vsegment_exclusive=true 时校验 used_bytes==0
//   （快路径）；false 时按 [used_bytes, capacity) 切分。用户无需手动声明
//   exclusive。
//
// 离线规划/单元测试（手写完整 JSON，不作为正式部署方式）：
//   vsegment_quota_planner --input request.json [--dry_run]
//   --input 模式保留给离线规划和单元测试，正常部署不应使用。

#include <fstream>
#include <iostream>
#include <map>
#include <sstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "etcd_helper.h"
#include "cvm/etcd_view_store.h"
#include "vsegment/partition_quota_planner.h"

// 模式选择：默认自动发现；--input 切到离线规划（仅测试）。
DEFINE_string(input, "",
              "Path to a JSON PartitionQuotaPlanRequest. OFFLINE PLANNING / "
              "UNIT TEST ONLY — do not use as a regular deployment path. "
              "Leave empty to use automatic CVM discovery (default).");
DEFINE_string(etcd_endpoints, "", "Semicolon-separated ETCD endpoints");
DEFINE_string(cluster_namespace, "", "CVM cluster namespace");
DEFINE_uint64(max_etcd_value_bytes, 1500000,
              "Maximum serialized ETCD value size");

// 用户策略（正常部署只需这 3 个核心参数）。
DEFINE_uint32(member_count, 0, "Number of distinct members per vsegment");
DEFINE_uint64(stripe_size, 0, "Stripe size in bytes");
DEFINE_uint64(member_extent_size, 0, "Extent size per member in bytes");

// 可选策略（提供默认值，不应强制用户填写）。
DEFINE_double(reserved_ratio, 0.0,
              "Fraction reserved outside vsegment quotas, in [0, 1). "
              "Default 0 means no reservation.");
DEFINE_uint32(initial_vsegment_count, 1,
              "Initial vsegment count per partition. Default 1.");
DEFINE_string(default_profile, "default",
              "Default profile name. Default \"default\".");
DEFINE_string(profile_name, "default",
              "Profile name for the single-profile plan. Default \"default\".");
DEFINE_string(required_medium, "",
              "Required medium for the profile. Empty (default) = auto mode: "
              "system discovers all media and builds an independent profile per "
              "medium (same member_count/stripe_size/member_extent_size). "
              "Non-empty = single profile, only segments matching this medium "
              "are used. Must match the medium reported by SegmentDescriptor "
              "(e.g. \"cpu:0\", \"cuda:0\", \"ssd:0\", or legacy "
              "\"REGISTERED_MEMORY\").");
DEFINE_uint64(io_alignment, 1,
              "I/O alignment in bytes for the profile. Planner takes "
              "max(profile.io_alignment, segment.io_alignment). Default 1.");
DEFINE_uint64(config_generation, 1,
              "Config generation monotonic number. Default 1.");

// 发布与高级开关。
DEFINE_bool(dry_run, true,
            "Print the candidate snapshot without publishing. Default true; "
            "set to false with --publish to publish.");
DEFINE_bool(publish, false,
            "Publish the snapshot to ETCD (requires --dry_run=false). "
            "Default false (safe).");
DEFINE_bool(allow_non_exclusive, false,
            "[DEPRECATED] Kept for backward compatibility. Automatic "
            "discovery now uses [used_bytes, capacity) as the allocatable "
            "range by default, so non-exclusive segments are accepted "
            "without this flag. No longer has any effect.");
DEFINE_bool(skip_verify, false,
            "Skip automatic verification of discovered snapshot before "
            "output/publish. Default false (verify enabled). The verifier "
            "checks base_offset=used_bytes, extent length consistency, "
            "arithmetic progression, medium match, and member_count; "
            "use --skip_verify only for debugging.");

namespace {

// 自动核对 dry-run 结果与请求的一致性，无需用户手动对照 ETCD。
// 核对项（每项输出 [PASS]/[FAIL]/[SKIP] 到 stderr）：
//   1. partition 0 中每个 segment 的 extent.base_offset == seg.base_offset
//      （seg.base_offset = desc.used_bytes，验证切分起点正确）
//   2. 同一 segment 在所有 partition 中的 extent.length 相等
//   3. 同一 segment 在相邻 partition 之间 base_offset 差值 == length（等差递增）
//   4. 介质一致：snapshot.quotas[].medium == seg.medium
//   5. discovered segments >= member_count（每个 profile）
//   6. per_partition==0 的 segment 不出现在任何 partition 的 extents 中
// 返回 true 表示全部通过；false 表示有 FAIL 项。
bool VerifyDiscoveredSnapshot(
    const mooncake::vsegment::PartitionQuotaPlanRequest& request,
    const mooncake::vsegment::PartitionPhysicalQuotaSnapshot& snapshot,
    std::string* detail) {
    std::ostringstream os;
    size_t fail = 0, warn = 0, pass = 0;

    // 收集每个 partition 中每个 segment 的 extent
    std::map<std::string, std::map<std::string, mooncake::vsegment::PSegmentExtent>>
        by_partition;
    for (const auto& quota : snapshot.quotas) {
        for (const auto& ext : quota.extents) {
            by_partition[quota.partition_id][ext.segment_id] = ext;
        }
    }

    // 核对每个发现的 segment
    for (const auto& seg : request.segments) {
        // 找该 segment 在所有 partition 中的 extent（按 partition_ids 顺序）
        std::vector<std::pair<size_t, mooncake::vsegment::PSegmentExtent>>
            seg_extents;
        for (size_t i = 0; i < request.partition_ids.size(); ++i) {
            const auto& pid = request.partition_ids[i];
            auto pit = by_partition.find(pid);
            if (pit == by_partition.end()) continue;
            auto sit = pit->second.find(seg.segment_id);
            if (sit != pit->second.end())
                seg_extents.emplace_back(i, sit->second);
        }

        if (seg_extents.empty()) {
            // 不出现在任何 partition：可能 per_partition==0（usable 太小）
            os << "[SKIP] " << seg.segment_id
               << " base_offset=" << seg.base_offset
               << " capacity=" << seg.capacity
               << " (per_partition=0 or no extents)\n";
            ++warn;
            continue;
        }

        // 1. partition 0 的 base_offset == seg.base_offset
        bool has_p0 = false;
        for (const auto& [idx, ext] : seg_extents) {
            if (idx == 0) {
                has_p0 = true;
                if (ext.base_offset != seg.base_offset) {
                    os << "[FAIL] " << seg.segment_id
                       << " partition 0 base_offset=" << ext.base_offset
                       << " != seg.base_offset=" << seg.base_offset
                       << " (expected base_offset=used_bytes)\n";
                    ++fail;
                } else {
                    ++pass;
                }
            }
        }
        if (!has_p0) {
            os << "[FAIL] " << seg.segment_id << " missing in partition 0\n";
            ++fail;
        }

        // 2. 长度一致
        uint64_t ref_len = seg_extents.front().second.length;
        for (const auto& [idx, ext] : seg_extents) {
            if (ext.length != ref_len) {
                os << "[FAIL] " << seg.segment_id << " partition " << idx
                   << " length=" << ext.length << " != ref=" << ref_len << "\n";
                ++fail;
            }
        }

        // 3. 等差递增：base_offset[i] = base_offset[i-1] + length
        for (size_t k = 1; k < seg_extents.size(); ++k) {
            uint64_t prev = seg_extents[k - 1].second.base_offset;
            uint64_t curr = seg_extents[k].second.base_offset;
            if (curr != prev + ref_len) {
                os << "[FAIL] " << seg.segment_id << " partition "
                   << seg_extents[k].first
                   << " base_offset=" << curr
                   << " != prev+length=" << (prev + ref_len) << "\n";
                ++fail;
            }
        }

        // 4. 介质一致
        for (const auto& quota : snapshot.quotas) {
            bool found = false;
            for (const auto& ext : quota.extents) {
                if (ext.segment_id == seg.segment_id) { found = true; break; }
            }
            if (found && quota.medium != seg.medium) {
                os << "[FAIL] " << seg.segment_id
                   << " partition " << quota.partition_id
                   << " medium=" << quota.medium
                   << " != seg.medium=" << seg.medium << "\n";
                ++fail;
            }
        }
    }

    // 5. discovered >= member_count（每个 profile 按 medium 匹配）
    for (const auto& profile : request.profile_specs) {
        size_t count = 0;
        for (const auto& seg : request.segments) {
            if (seg.medium == profile.required_medium) ++count;
        }
        if (count < profile.member_count) {
            os << "[FAIL] profile " << profile.name
               << " discovered=" << count
               << " < member_count=" << profile.member_count << "\n";
            ++fail;
        } else {
            os << "[PASS] profile " << profile.name
               << " discovered=" << count
               << " >= member_count=" << profile.member_count << "\n";
            ++pass;
        }
    }

    // 汇总
    if (fail == 0) {
        os << "[PASS] verify ok: " << request.segments.size()
           << " segments, " << request.partition_ids.size()
           << " partitions, " << pass << " checks passed, " << warn
           << " skipped\n";
    } else {
        os << "[FAIL] verify failed: " << fail << " failures, " << warn
           << " skipped\n";
    }

    *detail = os.str();
    return fail == 0;
}

int RunFromInput(mooncake::vsegment::PartitionQuotaPlanRequest& request) {
    if (FLAGS_input.empty()) {
        LOG(ERROR) << "--input is required for offline planning mode";
        return 2;
    }
    std::ifstream input(FLAGS_input, std::ios::binary);
    if (!input) {
        LOG(ERROR) << "cannot open " << FLAGS_input;
        return 2;
    }
    std::ostringstream contents;
    contents << input.rdbuf();
    try {
        struct_json::from_json(request, contents.str());
    } catch (const std::exception& error) {
        LOG(ERROR) << "invalid planner input: " << error.what();
        return 2;
    }
    LOG(WARNING) << "Offline planning mode (--input): this is preserved for "
                    "unit tests / offline planning and is NOT the regular "
                    "deployment path. Use automatic CVM discovery instead.";
    return 0;
}

int RunFromDiscovery(mooncake::vsegment::PartitionQuotaPlanRequest& request) {
    if (FLAGS_member_count == 0 || FLAGS_stripe_size == 0 ||
        FLAGS_member_extent_size == 0) {
        LOG(ERROR) << "automatic discovery requires --member_count, "
                      "--stripe_size and --member_extent_size";
        return 2;
    }
    if (FLAGS_etcd_endpoints.empty() || FLAGS_cluster_namespace.empty()) {
        LOG(ERROR) << "automatic discovery requires --etcd_endpoints and "
                      "--cluster_namespace";
        return 2;
    }
    auto error = mooncake::EtcdHelper::ConnectToEtcdStoreClient(
        FLAGS_etcd_endpoints);
    if (error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "cannot connect to etcd: "
                   << mooncake::toString(error);
        return 4;
    }
    std::vector<mooncake::cvm::SegmentDescriptor> descriptors;
    std::vector<mooncake::cvm::MasterRegistration> masters;
    std::vector<std::pair<std::string, mooncake::cvm::MountEntry>> mounts;
    mooncake::ViewVersionId revision = 0;
    error = mooncake::cvm::EtcdViewStore::LoadAllMasters(
        FLAGS_cluster_namespace, masters, revision);
    if (error == mooncake::ErrorCode::OK)
        error = mooncake::cvm::EtcdViewStore::LoadAllSegmentDescriptors(
            FLAGS_cluster_namespace, descriptors, revision);
    if (error == mooncake::ErrorCode::OK)
        error = mooncake::cvm::EtcdViewStore::LoadAllMountEntries(
            FLAGS_cluster_namespace, mounts, revision);
    if (error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "CVM discovery failed: " << mooncake::toString(error);
        return 4;
    }
    mooncake::vsegment::VSegmentUserPolicy policy;
    policy.member_count = FLAGS_member_count;
    policy.stripe_size = FLAGS_stripe_size;
    policy.member_extent_size = FLAGS_member_extent_size;
    policy.default_profile = FLAGS_default_profile;
    policy.profile_name = FLAGS_profile_name;
    policy.required_medium = FLAGS_required_medium;
    policy.io_alignment = FLAGS_io_alignment;
    policy.initial_vsegment_count = FLAGS_initial_vsegment_count;
    policy.reserved_ratio = FLAGS_reserved_ratio;
    policy.config_generation = FLAGS_config_generation;
    std::string detail;
    error = mooncake::vsegment::BuildDiscoveredQuotaPlan(
        policy, descriptors, masters, mounts, &request, &detail,
        /*allow_non_exclusive=*/FLAGS_allow_non_exclusive);
    if (error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << detail;
        return 3;
    }
    LOG(INFO) << "Discovered " << request.segments.size()
              << " psegments with usable free space from CVM; "
              << request.partition_ids.size() << " partitions";
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    const bool input_mode = !FLAGS_input.empty();
    mooncake::vsegment::PartitionQuotaPlanRequest request;
    int rc = input_mode ? RunFromInput(request) : RunFromDiscovery(request);
    if (rc != 0) return rc;

    auto result = mooncake::vsegment::PartitionQuotaPlanner().Plan(request);
    if (!result) {
        LOG(ERROR) << result.detail;
        return 3;
    }
    std::string serialized;
    struct_json::to_json(result.snapshot, serialized);

    // 自动发现模式下自动核对（无需用户手动对照 ETCD）。
    // 核对 base_offset=used_bytes、extent 等差递增、介质一致、member_count。
    // --skip_verify 可跳过（仅调试用）。核对失败则中止，不输出也不发布。
    if (!input_mode && !FLAGS_skip_verify) {
        std::string verify_detail;
        bool ok = VerifyDiscoveredSnapshot(request, result.snapshot,
                                           &verify_detail);
        std::cerr << verify_detail;
        if (!ok) {
            LOG(ERROR) << "Snapshot verification failed; aborting. "
                          "Use --skip_verify to bypass (not recommended).";
            return 6;
        }
        LOG(INFO) << "Snapshot verification passed.";
    }

    const bool want_publish = FLAGS_publish || !FLAGS_dry_run;
    if (!want_publish) {
        std::cout << serialized << std::endl;
        LOG(WARNING) << "Dry-run only. Re-run with --publish (and "
                        "--dry_run=false) to publish to ETCD.";
        return 0;
    }
    if (FLAGS_etcd_endpoints.empty() || FLAGS_cluster_namespace.empty()) {
        LOG(ERROR) << "--publish requires --etcd_endpoints and "
                      "--cluster_namespace";
        return 2;
    }
    if (!input_mode) {
        // 自动发现模式已连接 etcd；离线模式需要在此连接。
    } else {
        auto error = mooncake::EtcdHelper::ConnectToEtcdStoreClient(
            FLAGS_etcd_endpoints);
        if (error != mooncake::ErrorCode::OK) {
            LOG(ERROR) << "cannot connect to etcd: "
                       << mooncake::toString(error);
            return 4;
        }
    }
    mooncake::vsegment::EtcdPartitionQuotaSnapshotStore store(
        FLAGS_cluster_namespace);
    std::string detail;
    auto error = store.Create(result.snapshot, FLAGS_max_etcd_value_bytes,
                              &detail);
    if (error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << detail;
        return 5;
    }
    LOG(INFO) << "published vsegment quota generation "
              << result.snapshot.config_generation << " with "
              << result.snapshot.quotas.size() << " partition/profile quotas";
    return 0;
}
