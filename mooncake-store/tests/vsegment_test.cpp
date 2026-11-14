#include "vsegment/vsegment.h"
#include "vsegment/vsegment_runtime.h"
#include "vsegment/vsegment_transfer.h"
#include "cvm/cvm_keys.h"
#include "cvm/etcd_view_store.h"
#include "vsegment/vsegment_ha.h"
#include "vsegment/vsegment_service.h"
#include "vsegment/vsegment_manager.h"
#include "vsegment/vsegment_metrics.h"
#include "vsegment/vsegment_oplog_mode.h"
#include "vsegment/partition_quota_planner.h"
#include "master_service.h"
#include "rpc_types.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <future>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <thread>

namespace mooncake::vsegment {
namespace {

uint64_t MetricValue(const std::string& name) {
    const auto text = VSegmentMetrics::Instance().Serialize();
    // Match the sample line only: "# TYPE <name> gauge" must not match.
    const std::string prefix = "\n" + name + " ";
    const auto pos = text.find(prefix);
    if (pos == std::string::npos) return std::numeric_limits<uint64_t>::max();
    const auto begin = pos + prefix.size();
    const auto end = text.find('\n', begin);
    return std::stoull(text.substr(begin, end - begin));
}

class ScopedEnv {
   public:
    explicit ScopedEnv(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) old_value_ = value;
    }

    ~ScopedEnv() {
        if (old_value_) {
            setenv(name_, old_value_->c_str(), 1);
        } else {
            unsetenv(name_);
        }
    }

   private:
    const char* name_;
    std::optional<std::string> old_value_;
};

struct LegacySlotMetadataExport {
    uint16_t slot{0};
    std::string source_master_id;
    std::vector<StandbyObjectEntry> objects;
};
YLT_REFL(LegacySlotMetadataExport, slot, source_master_id, objects);

struct LegacyCompletedOperationRecord {
    std::string operation_id;
    std::string allocation_id;
    OperationOutcome outcome{OperationOutcome::ABORTED};
    LogicalRange range;
};
YLT_REFL(LegacyCompletedOperationRecord, operation_id, allocation_id, outcome,
         range);

struct LegacyLogicalAllocationSnapshot {
    uint64_t logical_capacity{0};
    std::vector<LogicalRange> free_ranges;
    std::vector<ReservationRecord> reservations;
    std::vector<LegacyCompletedOperationRecord> completed_operations;
};
YLT_REFL(LegacyLogicalAllocationSnapshot, logical_capacity, free_ranges,
         reservations, completed_operations);

struct LegacyVSegmentOpLogRecord {
    std::string partition_id;
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::string mutation;
    PartitionVSegmentSnapshot state;
};
YLT_REFL(LegacyVSegmentOpLogRecord, partition_id, route_epoch,
         metadata_revision, mutation, state);

TEST(VSegmentKeysTest, PartitionQuotaSnapshotIsClusterScoped) {
    EXPECT_EQ(cvm::VSegmentPartitionQuotaSnapshotKey("cluster-a"),
              "/cvm/cluster-a/snapshot/vsegment_partition_quota");
    EXPECT_NE(cvm::VSegmentPartitionQuotaSnapshotKey("cluster-a"),
              cvm::VSegmentPartitionQuotaSnapshotKey("cluster-b"));
}

TEST(VSegmentMigrationTest, SlotExportAcceptsLegacyPayload) {
    LegacySlotMetadataExport legacy{7, "submaster-a", {}};
    auto bytes = struct_pack::serialize(legacy);
    SlotMetadataExport decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(decoded.slot, 7);
    EXPECT_EQ(decoded.source_master_id, "submaster-a");
    EXPECT_FALSE(decoded.vsegment_partition.has_value());
}

TEST(VSegmentSnapshotCompatibilityTest, ReadsLegacyCompletedOperation) {
    LegacyLogicalAllocationSnapshot legacy{
        64, {}, {}, {{"put-1", "object-1", OperationOutcome::COMMITTED,
                      {0, 64}}}};
    const auto bytes = struct_pack::serialize(legacy);
    LogicalAllocationSnapshot decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    ASSERT_EQ(decoded.completed_operations.size(), 1u);
    EXPECT_EQ(decoded.completed_operations[0].completion_revision.value_or(0),
              0u);
}

struct LegacyGetReplicaListResponse {
    std::vector<Replica::Descriptor> replicas;
    uint64_t lease_ttl_ms{0};
    std::optional<uint64_t> object_checksum;
};
YLT_REFL(LegacyGetReplicaListResponse, replicas, lease_ttl_ms,
         object_checksum);

struct LegacyPutStartResult {
    std::string operation_id;
    std::vector<Replica::Descriptor> replicas;
};
YLT_REFL(LegacyPutStartResult, operation_id, replicas);

VSegmentLayoutEntry MakeTestLayoutEntry() {
    VSegmentLayoutEntry entry;
    entry.vsegment_id = "vs-1";
    entry.partition_id = "partition-1";
    entry.stripe_size = 4096;
    entry.logical_capacity = 8192;
    entry.mapping_algorithm = 0;
    entry.checksum = 0x1234;
    entry.members.push_back(
        {"seg-1", 0, 8192, "127.0.0.1:19000", 0x100000});
    return entry;
}

TEST(VSegmentLayoutWireTest, LegacyGetReplicaListDecodesWithoutLayout) {
    LegacyGetReplicaListResponse legacy{{}, 17, 42};
    const auto bytes = struct_pack::serialize(legacy);
    GetReplicaListResponse decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(decoded.lease_ttl_ms, 17u);
    ASSERT_TRUE(decoded.object_checksum.has_value());
    EXPECT_EQ(*decoded.object_checksum, 42u);
    EXPECT_FALSE(decoded.vsegment_layouts.has_value());
}

TEST(VSegmentLayoutWireTest, NewGetReplicaListIsReadableByLegacyReader) {
    GetReplicaListResponse response;
    response.lease_ttl_ms = 17;
    response.object_checksum = 42;
    response.vsegment_layouts = std::vector<VSegmentLayoutEntry>{
        MakeTestLayoutEntry()};
    const auto bytes = struct_pack::serialize(response);
    LegacyGetReplicaListResponse decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(decoded.lease_ttl_ms, 17u);
    ASSERT_TRUE(decoded.object_checksum.has_value());
    EXPECT_EQ(*decoded.object_checksum, 42u);
}

TEST(VSegmentLayoutWireTest, OldPutStartDecodesWithoutLayout) {
    LegacyPutStartResult legacy{"op-1", {}};
    const auto bytes = struct_pack::serialize(legacy);
    PutStartResult decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(decoded.operation_id, "op-1");
    EXPECT_FALSE(decoded.vsegment_layouts.has_value());
}

struct LegacyCopyStartResponse {
    Replica::Descriptor source;
    std::vector<Replica::Descriptor> targets;
};
YLT_REFL(LegacyCopyStartResponse, source, targets);

struct LegacyMoveStartResponse {
    Replica::Descriptor source;
    std::optional<Replica::Descriptor> target;
};
YLT_REFL(LegacyMoveStartResponse, source, target);

TEST(VSegmentLayoutWireTest, CopyStartRoundTripKeepsLayoutTable) {
    CopyStartResponse response;
    response.vsegment_layouts = std::vector<VSegmentLayoutEntry>{
        MakeTestLayoutEntry()};
    const auto bytes = struct_pack::serialize(response);
    CopyStartResponse decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    ASSERT_TRUE(decoded.vsegment_layouts.has_value());
    ASSERT_EQ(decoded.vsegment_layouts->size(), 1u);
    EXPECT_EQ((*decoded.vsegment_layouts)[0].vsegment_id, "vs-1");
}

TEST(VSegmentLayoutWireTest, LegacyCopyStartDecodesWithoutLayoutTable) {
    LegacyCopyStartResponse legacy;
    const auto bytes = struct_pack::serialize(legacy);
    CopyStartResponse decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    EXPECT_FALSE(decoded.vsegment_layouts.has_value());
}

TEST(VSegmentLayoutWireTest, NewCopyStartIsReadableByLegacyReader) {
    CopyStartResponse response;
    response.vsegment_layouts = std::vector<VSegmentLayoutEntry>{
        MakeTestLayoutEntry()};
    const auto bytes = struct_pack::serialize(response);
    LegacyCopyStartResponse decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    EXPECT_TRUE(decoded.targets.empty());
}

TEST(VSegmentLayoutWireTest, LegacyMoveStartDecodesWithoutLayoutTable) {
    LegacyMoveStartResponse legacy;
    const auto bytes = struct_pack::serialize(legacy);
    MoveStartResponse decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    EXPECT_FALSE(decoded.vsegment_layouts.has_value());
}

TEST(VSegmentLayoutWireTest, BatchVectorRoundTripKeepsDedupTable) {
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> batch;
    GetReplicaListResponse first;
    first.vsegment_layouts = std::vector<VSegmentLayoutEntry>{
        MakeTestLayoutEntry()};
    batch.push_back(std::move(first));
    batch.push_back(tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    const auto bytes = struct_pack::serialize(batch);
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    ASSERT_EQ(decoded.size(), 2u);
    ASSERT_TRUE(decoded[0]->vsegment_layouts.has_value());
    ASSERT_EQ(decoded[0]->vsegment_layouts->size(), 1u);
    EXPECT_EQ(decoded[0]->vsegment_layouts->front().vsegment_id, "vs-1");
    ASSERT_EQ(decoded[0]->vsegment_layouts->front().members.size(), 1u);
    EXPECT_EQ(decoded[0]->vsegment_layouts->front().members.front().endpoint,
              "127.0.0.1:19000");
}

TEST(VSegmentLayoutWireTest, BatchPutStartRoundTripKeepsPerKeyLayouts) {
    std::vector<tl::expected<PutStartResult, ErrorCode>> batch;
    PutStartResult first{"op-1", {}};
    first.vsegment_layouts = std::vector<VSegmentLayoutEntry>{
        MakeTestLayoutEntry()};
    batch.push_back(std::move(first));
    batch.push_back(tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE));

    const auto bytes = struct_pack::serialize(batch);
    std::vector<tl::expected<PutStartResult, ErrorCode>> decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    ASSERT_EQ(decoded.size(), 2u);
    ASSERT_TRUE(decoded[0].has_value());
    EXPECT_EQ(decoded[0]->operation_id, "op-1");
    ASSERT_TRUE(decoded[0]->vsegment_layouts.has_value());
    ASSERT_EQ(decoded[0]->vsegment_layouts->size(), 1u);
    EXPECT_EQ(decoded[0]->vsegment_layouts->front().vsegment_id, "vs-1");
    ASSERT_FALSE(decoded[1].has_value());
    EXPECT_EQ(decoded[1].error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

TEST(VSegmentLayoutWireTest, CachedQueryResultMapRoundTripKeepsLayout) {
    CachedQueryResultResponse cached{GetReplicaListResponse{}};
    cached.value.vsegment_layouts = std::vector<VSegmentLayoutEntry>{
        MakeTestLayoutEntry()};
    std::map<std::string, CachedQueryResultResponse> cache;
    cache.emplace("key-1", std::move(cached));

    const auto bytes = struct_pack::serialize(cache);
    std::map<std::string, CachedQueryResultResponse> decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    ASSERT_EQ(decoded.count("key-1"), 1u);
    ASSERT_TRUE(decoded.at("key-1").value.vsegment_layouts.has_value());
    EXPECT_EQ(decoded.at("key-1").value.vsegment_layouts->front().vsegment_id,
              "vs-1");
}

// BatchGetReplicaList keeps its legacy wire type
// (vector<expected<GetReplicaListResponse, ...>>); the deduplicated table rides
// on the first successful per-key response. This is the same
// GetReplicaListResponse type, so old readers simply ignore the compatible
// field.
TEST(VSegmentLayoutWireTest, BatchVectorCarriesTableOnFirstResponse) {
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> batch;
    batch.push_back(tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    batch.push_back(GetReplicaListResponse{});
    batch.push_back(GetReplicaListResponse{});
    AttachVSegmentLayoutTableToFirstSuccess(
        batch, {MakeTestLayoutEntry()});
    EXPECT_FALSE(batch[0].has_value());
    ASSERT_TRUE(batch[1]->vsegment_layouts.has_value());
    EXPECT_FALSE(batch[2]->vsegment_layouts.has_value());

    // Same wire type round-trip keeps the table on the same element.
    const auto bytes = struct_pack::serialize(batch);
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, bytes),
              struct_pack::errc::ok);
    ASSERT_EQ(decoded.size(), 3u);
    ASSERT_TRUE(decoded[1]->vsegment_layouts.has_value());
    EXPECT_EQ(decoded[1]->vsegment_layouts->front().vsegment_id, "vs-1");

    // And the legacy reader (struct without the compatible field) still works.
    std::vector<tl::expected<LegacyGetReplicaListResponse, ErrorCode>>
        legacy_reader;
    ASSERT_EQ(struct_pack::deserialize_to(legacy_reader, bytes),
              struct_pack::errc::ok);
    ASSERT_EQ(legacy_reader.size(), 3u);
    EXPECT_TRUE(legacy_reader[1].has_value());
}

TEST(VSegmentLayoutWireTest, CollectsTablesFromBatchResults) {
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> batch;
    batch.push_back(GetReplicaListResponse{});
    auto second = MakeTestLayoutEntry();
    second.vsegment_id = "vs-2";
    GetReplicaListResponse with_table;
    with_table.vsegment_layouts = std::vector<VSegmentLayoutEntry>{second};
    batch.push_back(std::move(with_table));

    std::vector<VSegmentLayoutEntry> merged;
    MergeVSegmentLayoutTablesFromResults(batch, merged);
    ASSERT_EQ(merged.size(), 1u);
    EXPECT_EQ(merged.front().vsegment_id, "vs-2");
}

TEST(VSegmentLayoutWireTest, MergeVSegmentLayoutTableDeduplicates) {
    std::vector<VSegmentLayoutEntry> target;
    auto first = MakeTestLayoutEntry();
    MergeVSegmentLayoutTable(target, {first});
    auto duplicate = MakeTestLayoutEntry();
    duplicate.members.front().endpoint = "other:1";
    MergeVSegmentLayoutTable(target, {duplicate});
    ASSERT_EQ(target.size(), 1u);
    EXPECT_EQ(target.front().members.front().endpoint, "127.0.0.1:19000");

    auto second = MakeTestLayoutEntry();
    second.vsegment_id = "vs-2";
    MergeVSegmentLayoutTable(target, {second});
    ASSERT_EQ(target.size(), 2u);
}

TEST(VSegmentRouteStoreTest, RouteSerializationRoundTrip) {
    partition::PartitionRoute route{
        {"partition-7"}, "submaster-a", 9,
        static_cast<int32_t>(partition::PartitionState::kMigrating),
        "submaster-b"};
    std::string encoded;
    ASSERT_EQ(cvm::EtcdViewStore::SerializePartitionRoute(route, encoded),
              ErrorCode::OK);
    partition::PartitionRoute decoded;
    ASSERT_EQ(cvm::EtcdViewStore::DeserializePartitionRoute(encoded, decoded),
              ErrorCode::OK);
    EXPECT_EQ(decoded.partition_id.partition_id,
              route.partition_id.partition_id);
    EXPECT_EQ(decoded.owner_submaster_id, route.owner_submaster_id);
    EXPECT_EQ(decoded.route_epoch, route.route_epoch);
    EXPECT_EQ(decoded.state, route.state);
    EXPECT_EQ(decoded.target_submaster_id, route.target_submaster_id);
}

class TestStateCommitter : public VSegmentStateCommitter {
   public:
    ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                     const std::string& mutation,
                     std::string* detail) override {
        mutations.push_back(mutation);
        last_state = state;
        if (delay_create_begin && mutation == "vsegment_create_begin")
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (fail_next || (!fail_mutation.empty() &&
                          mutation == fail_mutation)) {
            fail_next = false;
            fail_mutation.clear();
            if (detail) *detail = "injected persistence failure";
            return ErrorCode::PERSISTENT_FAIL;
        }
        return ErrorCode::OK;
    }

    bool fail_next{false};
    bool delay_create_begin{false};
    std::string fail_mutation;
    std::vector<std::string> mutations;
    PartitionVSegmentSnapshot last_state;

    // aligned mode (default): the hot path writes two O(1) records instead of
    // persisting the full state. Same mutation names keep assertions valid.
    ErrorCode CommitReservation(const std::string& /*partition_id*/,
                                const std::string& /*operation_id*/,
                                const std::string& /*vsegment_id*/,
                                const LogicalRange& /*range*/,
                                uint64_t /*route_epoch*/) override {
        mutations.push_back("logical_reserve");
        if (fail_next || (!fail_mutation.empty() &&
                          fail_mutation == "logical_reserve")) {
            fail_next = false;
            fail_mutation.clear();
            return ErrorCode::PERSISTENT_FAIL;
        }
        return ErrorCode::OK;
    }

    ErrorCode CommitRelease(const std::string& /*partition_id*/,
                            const std::string& /*operation_id*/,
                            uint64_t /*route_epoch*/) override {
        mutations.push_back("logical_abort");
        if (fail_next || (!fail_mutation.empty() &&
                          fail_mutation == "logical_abort")) {
            fail_next = false;
            fail_mutation.clear();
            return ErrorCode::PERSISTENT_FAIL;
        }
        return ErrorCode::OK;
    }
};

std::shared_ptr<TestStateCommitter> Committer() {
    return std::make_shared<TestStateCommitter>();
}

VSegmentProfile Profile(uint32_t members = 2) {
    return {.name = "default",
            .member_count = members,
            .stripe_size = 64,
            .member_extent_size = 256,
            .io_alignment = 8,
            .required_medium = "DRAM"};
}

PartitionVSegmentConfig Config() {
    return {.partition_id = "partition-1",
            .profile_name = "default",
            .initial_vsegment_count = 1,
            .config_generation = 1,
            .quotas = {{"segment-a", 0, 512}, {"segment-b", 1024, 512}}};
}

VSegmentView View() {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    return allocation.view;
}

TEST(VSegmentConfigTest, RejectsInvalidLayoutAndOverlappingQuota) {
    auto profile = Profile();
    profile.member_extent_size = 100;
    EXPECT_EQ(ValidateProfile(profile), ErrorCode::INVALID_PARAMS);

    auto config = Config();
    config.quotas.push_back({"segment-a", 128, 64});
    EXPECT_EQ(ValidatePartitionConfig(config), ErrorCode::INVALID_PARAMS);
}

TEST(VSegmentConfigTest, ValidatesPublishedConfigAgainstSegmentGeometry) {
    auto profile = Profile();
    auto config = Config();
    std::vector<PSegmentGeometry> segments = {
        {"segment-a", 2048, 0, 8, "DRAM", true, true, ""},
        {"segment-b", 2048, 0, 8, "DRAM", true, true, ""}};
    EXPECT_EQ(ValidatePublishedConfig({profile}, {config}, segments, {}),
              ErrorCode::OK);

    auto overlapping = config;
    overlapping.partition_id = "partition-2";
    overlapping.config_generation = 2;
    EXPECT_EQ(ValidatePublishedConfig({profile}, {config, overlapping},
                                      segments, {}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ValidatePublishedConfig({profile}, {config}, segments,
                                      {{"partition-1", 1}}),
              ErrorCode::INVALID_PARAMS);
}

TEST(PartitionQuotaPlannerTest, SplitsEveryMediumAcrossAllPartitions) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 7;
    request.policy_digest = "policy-v7";
    request.default_profile = "dram";
    request.partition_ids = {"partition-b", "partition-a"};
    request.profile_specs = {
        {.name = "dram",
         .member_count = 2,
         .stripe_size = 64,
         .member_extent_size = 128,
         .io_alignment = 8,
         .required_medium = "DRAM"},
        {.name = "nvme",
         .member_count = 2,
         .stripe_size = 64,
         .member_extent_size = 128,
         .io_alignment = 8,
         .required_medium = "NVMe"}};
    request.segments = {{"dram-a", 1024, 0, 8, "DRAM", true, true, ""},
                        {"dram-b", 1024, 0, 8, "DRAM", true, true, ""},
                        {"nvme-a", 2048, 0, 8, "NVMe", true, true, ""},
                        {"nvme-b", 2048, 0, 8, "NVMe", true, true, ""}};

    auto result = PartitionQuotaPlanner().Plan(request);
    ASSERT_TRUE(result) << result.detail;
    ASSERT_EQ(result.snapshot.quotas.size(), 4);
    EXPECT_EQ(result.snapshot.quotas[0].partition_id, "partition-a");
    EXPECT_EQ(result.snapshot.quotas[0].profile_name, "dram");
    // partition_ids = {"partition-b", "partition-a"}: partition-b 是 index 0
    // (base_offset=0), partition-a 是 index 1 (base_offset=512). snapshot.quotas
    // 按 partition_id 字典序排列，partition-a 在前。
    EXPECT_EQ(result.snapshot.quotas[0].extents[0].base_offset, 512);
    EXPECT_EQ(result.snapshot.quotas[1].extents[0].base_offset, 0);
    EXPECT_EQ(result.snapshot.quotas[2].profile_name, "nvme");
    EXPECT_EQ(ValidateQuotaSnapshot(result.snapshot, request.segments),
              ErrorCode::OK);

    PartitionVSegmentConfig config;
    EXPECT_EQ(BuildPartitionConfig(result.snapshot, "partition-a", "nvme",
                                   &config),
              ErrorCode::OK);
    EXPECT_EQ(config.quotas.size(), 2);
    EXPECT_EQ(config.config_generation, 7);
}

TEST(PartitionQuotaPlannerTest, RejectsInsufficientMemberSegments) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 1;
    request.policy_digest = "policy";
    request.default_profile = "default";
    request.partition_ids = {"partition-a"};
    request.profile_specs = {Profile(2)};
    request.segments = {{"only-one", 4096, 0, 8, "DRAM", true, true, ""}};
    auto result = PartitionQuotaPlanner().Plan(request);
    EXPECT_EQ(result.error, ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    EXPECT_NE(result.detail.find("requires 2 psegments"), std::string::npos);
}

TEST(VSegmentConfigTest, AllowsMultipleProfilesForOnePartition) {
    auto dram = Profile();
    dram.name = "dram";
    auto nvme = Profile();
    nvme.name = "nvme";
    nvme.required_medium = "NVMe";
    auto dram_config = Config();
    dram_config.profile_name = "dram";
    auto nvme_config = Config();
    nvme_config.profile_name = "nvme";
    nvme_config.quotas = {{"nvme-a", 0, 512}, {"nvme-b", 0, 512}};
    std::vector<PSegmentGeometry> segments = {
        {"segment-a", 2048, 0, 8, "DRAM", true, true, ""},
        {"segment-b", 2048, 0, 8, "DRAM", true, true, ""},
        {"nvme-a", 2048, 0, 8, "NVMe", true, true, ""},
        {"nvme-b", 2048, 0, 8, "NVMe", true, true, ""}};
    EXPECT_EQ(ValidatePublishedConfig({dram, nvme},
                                      {dram_config, nvme_config}, segments,
                                      {}),
              ErrorCode::OK);
}

TEST(PartitionQuotaPlannerTest, RejectsOverlappingBalancedProfilePools) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 1;
    request.policy_digest = "policy";
    request.default_profile = "first";
    request.partition_ids = {"partition-1"};
    auto first = Profile();
    first.name = "first";
    auto second = Profile();
    second.name = "second";
    request.profile_specs = {first, second};
    request.segments = {{"segment-a", 1024, 0, 8, "DRAM", true, true, ""},
                        {"segment-b", 1024, 0, 8, "DRAM", true, true, ""}};
    auto result = PartitionQuotaPlanner().Plan(request);
    EXPECT_EQ(result.error, ErrorCode::INVALID_PARAMS);
    EXPECT_NE(result.detail.find("overlap"), std::string::npos);
}

TEST(PartitionQuotaPlannerTest, ExcludesUnhealthySegments) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 1;
    request.policy_digest = "policy";
    request.default_profile = "default";
    request.partition_ids = {"partition-a"};
    request.profile_specs = {Profile(2)};
    request.segments = {{"healthy", 4096, 0, 8, "DRAM", true, true, "host-a"},
                        {"unhealthy", 4096, 0, 8, "DRAM", false, true,
                         "host-b"}};
    EXPECT_EQ(PartitionQuotaPlanner().Plan(request).error,
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
}

TEST(PartitionQuotaPlannerTest, AppliesReservedRatioAndAlignment) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 2;
    request.policy_digest = "policy";
    request.default_profile = "default";
    request.partition_ids = {"partition-b", "partition-a"};
    request.profile_specs = {Profile(2)};
    request.segments = {{"segment-a", 1000, 0, 16, "DRAM", true, true, ""},
                        {"segment-b", 1000, 0, 16, "DRAM", true, true, ""}};
    request.reserved_ratio = 0.1;

    auto result = PartitionQuotaPlanner().Plan(request);
    ASSERT_TRUE(result) << result.detail;
    ASSERT_EQ(result.snapshot.quotas.size(), 2u);
    for (const auto& quota : result.snapshot.quotas) {
        ASSERT_EQ(quota.extents.size(), 2u);
        for (const auto& extent : quota.extents) {
            EXPECT_EQ(extent.length, 448u);
            EXPECT_EQ(extent.base_offset % 16, 0u);
        }
    }
    EXPECT_EQ(result.snapshot.quotas[0].partition_id, "partition-a");
    // partition_ids = {"partition-b", "partition-a"}: partition-b 是 index 0
    // (base_offset=0), partition-a 是 index 1 (base_offset=448).
    EXPECT_EQ(result.snapshot.quotas[0].extents[0].base_offset, 448u);
    EXPECT_EQ(result.snapshot.quotas[1].partition_id, "partition-b");
    EXPECT_EQ(result.snapshot.quotas[1].extents[0].base_offset, 0u);
}

TEST(VSegmentViewTest, ChecksumCoversOrderedMembers) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    EXPECT_EQ(ValidateView(allocation.view, Profile()), ErrorCode::OK);

    std::swap(allocation.view.members[0], allocation.view.members[1]);
    EXPECT_EQ(ValidateView(allocation.view, Profile()),
              ErrorCode::CHECKSUM_MISMATCH);
}

TEST(VSegmentViewTest, LifecycleDoesNotChangeImmutableViewChecksum) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentStateSnapshot state{"default", Lifecycle::ACTIVE,
                                allocation.view, {}};
    const auto checksum = state.view.checksum;
    state.lifecycle = Lifecycle::DRAINING;
    EXPECT_EQ(state.view.checksum, checksum);
    EXPECT_EQ(ValidateView(state.view, Profile()), ErrorCode::OK);
}

TEST(PartitionQuotaAllocatorTest, AllocatesAndReleasesStaticQuota) {
    PartitionQuotaAllocator allocator(Config());
    auto first = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(first);
    ASSERT_EQ(first.view.members.size(), 2);
    EXPECT_EQ(first.view.members[0],
              (PSegmentExtent{"segment-a", 0, 256}));
    EXPECT_EQ(first.view.members[1],
              (PSegmentExtent{"segment-b", 1024, 256}));
    EXPECT_EQ(allocator.FreeBytes("segment-a"), 256);

    EXPECT_EQ(allocator.Release(first.view), ErrorCode::OK);
    EXPECT_EQ(allocator.FreeBytes("segment-a"), 512);
    EXPECT_EQ(allocator.FreeBytes("segment-b"), 512);
}

TEST(PartitionQuotaAllocatorTest, DoesNotDegradeConfiguredMemberCount) {
    auto config = Config();
    config.quotas.pop_back();
    PartitionQuotaAllocator allocator(config);
    auto allocation = allocator.Allocate("vs-1", Profile());
    EXPECT_EQ(allocation.error,
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    EXPECT_NE(allocation.detail.find("requires 2 psegments"),
              std::string::npos);
    EXPECT_EQ(allocator.FreeBytes("segment-a"), 512);
}

TEST(PartitionQuotaAllocatorTest, ConcurrentIdentityIsIdempotentlyRejected) {
    PartitionQuotaAllocator allocator(Config());
    ASSERT_TRUE(allocator.Allocate("vs-1", Profile()));
    auto duplicate = allocator.Allocate("vs-1", Profile());
    EXPECT_EQ(duplicate.error, ErrorCode::SEGMENT_ALREADY_EXISTS);
}

TEST(LogicalRangeAllocatorTest, ReservationIsIdempotentAndAbortReturnsSpace) {
    LogicalRangeAllocator allocator(1024);
    auto first = allocator.Reserve("put-1", 128);
    ASSERT_TRUE(first);
    auto retry = allocator.Reserve("put-1", 128);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry.range.offset, first.range.offset);
    EXPECT_EQ(allocator.FreeBytes(), 896);
    EXPECT_EQ(allocator.ReservationCount(), 1);
    EXPECT_EQ(allocator.Abort("put-1"), ErrorCode::OK);
    EXPECT_EQ(allocator.Abort("put-1"), ErrorCode::OK);
    EXPECT_EQ(allocator.FreeBytes(), 1024);
}

TEST(LogicalRangeAllocatorTest, ReleaseRequiresExactObjectAllocation) {
    LogicalRangeAllocator allocator(1024);
    ASSERT_TRUE(allocator.Reserve("put-1", 128));
    LogicalRange committed;
    ASSERT_EQ(allocator.Commit("put-1", "object-1", &committed),
              ErrorCode::OK);
    EXPECT_EQ(allocator.Release("object-2", committed),
              ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(allocator.Release("object-1",
                                {committed.offset, committed.length / 2}),
              ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(allocator.FreeBytes(), 896);
}

TEST(LogicalRangeAllocatorTest, CommitKeepsRangeAllocatedUntilRelease) {
    LogicalRangeAllocator allocator(1024);
    ASSERT_TRUE(allocator.Reserve("put-1", 128));
    LogicalRange committed;
    EXPECT_EQ(allocator.Commit("put-1", "object-1", &committed),
              ErrorCode::OK);
    LogicalRange retried;
    EXPECT_EQ(allocator.Commit("put-1", "object-1", &retried),
              ErrorCode::OK);
    EXPECT_EQ(retried.offset, committed.offset);
    EXPECT_EQ(allocator.FreeBytes(), 896);
    EXPECT_EQ(allocator.Release("object-1", committed), ErrorCode::OK);
    EXPECT_EQ(allocator.FreeBytes(), 1024);
    EXPECT_EQ(allocator.Release("object-1", committed),
              ErrorCode::OBJECT_NOT_FOUND);
}

TEST(LogicalRangeAllocatorTest, RejectsLengthChangeOnIdempotentRetry) {
    LogicalRangeAllocator allocator(1024);
    ASSERT_TRUE(allocator.Reserve("put-1", 128));
    EXPECT_EQ(allocator.Reserve("put-1", 64).error,
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(allocator.FreeBytes(), 896u);
    EXPECT_EQ(allocator.ReservationCount(), 1u);
}

TEST(LogicalRangeAllocatorTest, InternalRollbackAllowsOperationRetry) {
    LogicalRangeAllocator allocator(128);
    auto first = allocator.Reserve("put-1", 64);
    ASSERT_TRUE(first);
    ASSERT_EQ(allocator.CancelReservation("put-1"), ErrorCode::OK);
    auto retried = allocator.Reserve("put-1", 64);
    ASSERT_TRUE(retried);
    EXPECT_EQ(retried.range.offset, first.range.offset);
}

TEST(VSegmentResolverTest, SplitsAtStripeAndClientSliceBoundaries) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);

    auto resolved = ResolveTransfer(allocation.view, 48, 96,
                                    {{1000, 40}, {2000, 56}});
    ASSERT_TRUE(resolved);
    ASSERT_EQ(resolved.requests.size(), 4);
    EXPECT_EQ(resolved.requests[0].length, 16);
    EXPECT_EQ(resolved.requests[0].segment_id, "segment-a");
    EXPECT_EQ(resolved.requests[0].physical_offset, 48);
    EXPECT_EQ(resolved.requests[1].length, 24);
    EXPECT_EQ(resolved.requests[1].segment_id, "segment-b");
    EXPECT_EQ(resolved.requests[1].client_address, 1016);
    EXPECT_EQ(resolved.requests[2].length, 40);
    EXPECT_EQ(resolved.requests[2].client_address, 2000);
    EXPECT_EQ(resolved.requests[3].length, 16);
    EXPECT_EQ(resolved.requests[3].segment_id, "segment-a");
    EXPECT_EQ(resolved.requests[3].physical_offset, 64);
    EXPECT_EQ(resolved.requests[3].client_buffer_offset, 80);
}

TEST(VSegmentResolverTest, RejectsViewWithInvalidChecksum) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    allocation.view.members[0].base_offset += 8;
    auto resolved = ResolveTransfer(allocation.view, 0, 8, {{1000, 8}});
    EXPECT_EQ(resolved.error, ErrorCode::CHECKSUM_MISMATCH);
}

TEST(VSegmentResolverTest, RejectsBufferLengthMismatchAndLogicalOverflow) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);

    EXPECT_EQ(ResolveTransfer(allocation.view, 0, 16, {{1000, 8}}).error,
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ResolveTransfer(allocation.view,
                              allocation.view.logical_capacity - 8, 16,
                              {{1000, 16}})
                  .error,
              ErrorCode::INVALID_PARAMS);
}

VSegmentLayoutEntry MakeLayoutEntryFromView(const VSegmentView& view,
                                            uint64_t base_address) {
    VSegmentLayoutEntry entry;
    entry.vsegment_id = view.vsegment_id;
    entry.partition_id = view.partition_id;
    entry.stripe_size = view.stripe_size;
    entry.logical_capacity = view.logical_capacity;
    entry.mapping_algorithm = static_cast<uint32_t>(view.mapping_algorithm);
    entry.checksum = view.checksum;
    entry.members.reserve(view.members.size());
    for (const auto& member : view.members) {
        entry.members.push_back({member.segment_id, member.base_offset,
                                 member.length,
                                 "inline://" + member.segment_id,
                                 base_address});
    }
    return entry;
}

TEST(VSegmentTransferPlannerTest, UsesInlineLayoutTable) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentTransferPlanner planner;
    VSegmentDescriptor replica{"partition-1", "vs-1", 48, 96};
    std::vector<VSegmentLayoutEntry> layouts{MakeLayoutEntryFromView(
        allocation.view, /*base_address=*/0x200000)};

    auto plan = planner.Plan(replica, {{1000, 96}}, &layouts);
    ASSERT_TRUE(plan) << plan.detail;
    ASSERT_EQ(plan.requests.size(), 3u);
    EXPECT_EQ(plan.requests[0].endpoint, "inline://segment-a");
    EXPECT_EQ(plan.requests[0].transfer.physical_offset, 0x200000 + 48);
}

TEST(VSegmentTransferPlannerTest, ResolvesEachDistinctMemberEndpoint) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentTransferPlanner planner;
    std::vector<VSegmentLayoutEntry> layouts{MakeLayoutEntryFromView(
        allocation.view, /*base_address=*/0x200000)};

    // 512 bytes over 64-byte stripes = 8 sub-requests across 2 distinct
    // members (member_count = 2).
    VSegmentDescriptor replica{"partition-1", "vs-1", 0, 512};
    auto plan = planner.Plan(replica, {{1000, 512}}, &layouts);
    ASSERT_TRUE(plan) << plan.detail;
    ASSERT_EQ(plan.requests.size(), 8u);
    std::set<std::string> endpoints;
    for (const auto& request : plan.requests) {
        endpoints.insert(request.endpoint);
    }
    EXPECT_EQ(endpoints.size(), 2u);
}

TEST(VSegmentTransferPlannerTest, RequiresLayoutTable) {
    VSegmentTransferPlanner planner;
    VSegmentDescriptor replica{"partition-1", "vs-1", 48, 96};

    auto plan = planner.Plan(replica, {{1000, 96}}, nullptr);
    EXPECT_EQ(plan.error, ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_NE(plan.detail.find("required but absent"), std::string::npos);
    EXPECT_TRUE(plan.requests.empty());
}

TEST(VSegmentTransferPlannerTest, RejectsTableWithoutMatchingVSegment) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentTransferPlanner planner;
    VSegmentDescriptor replica{"partition-1", "vs-1", 48, 96};
    auto entry = MakeLayoutEntryFromView(allocation.view, 0x200000);
    entry.vsegment_id = "vs-other";
    std::vector<VSegmentLayoutEntry> layouts{std::move(entry)};

    auto plan = planner.Plan(replica, {{1000, 96}}, &layouts);
    EXPECT_EQ(plan.error, ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_NE(plan.detail.find("no entry for vs-1"), std::string::npos);
    EXPECT_TRUE(plan.requests.empty());
}

TEST(VSegmentTransferPlannerTest, RejectsLayoutFromAnotherPartition) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentTransferPlanner planner;
    VSegmentDescriptor replica{"partition-1", "vs-1", 48, 96};
    auto entry = MakeLayoutEntryFromView(allocation.view, 0x200000);
    entry.partition_id = "partition-2";
    std::vector<VSegmentLayoutEntry> layouts{std::move(entry)};

    auto plan = planner.Plan(replica, {{1000, 96}}, &layouts);
    EXPECT_EQ(plan.error, ErrorCode::INVALID_VERSION);
    EXPECT_TRUE(plan.requests.empty());
}

TEST(VSegmentTransferPlannerTest, RejectsLayoutChecksumMismatch) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentTransferPlanner planner;
    VSegmentDescriptor replica{"partition-1", "vs-1", 48, 96};
    auto entry = MakeLayoutEntryFromView(allocation.view, 0x200000);
    entry.checksum ^= 1;
    std::vector<VSegmentLayoutEntry> layouts{std::move(entry)};

    auto plan = planner.Plan(replica, {{1000, 96}}, &layouts);
    EXPECT_EQ(plan.error, ErrorCode::CHECKSUM_MISMATCH);
    EXPECT_TRUE(plan.requests.empty());
}

TEST(VSegmentTransferPlannerTest, RejectsLayoutMemberWithoutEndpoint) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentTransferPlanner planner;
    VSegmentDescriptor replica{"partition-1", "vs-1", 48, 96};
    auto entry = MakeLayoutEntryFromView(allocation.view, 0x200000);
    ASSERT_FALSE(entry.members.empty());
    entry.members.front().endpoint.clear();
    std::vector<VSegmentLayoutEntry> layouts{std::move(entry)};

    // An unmounted member has no endpoint and hard C has no RPC fallback.
    auto plan = planner.Plan(replica, {{1000, 96}}, &layouts);
    EXPECT_EQ(plan.error, ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_NE(plan.detail.find("no endpoint"), std::string::npos);
    EXPECT_TRUE(plan.requests.empty());
}

TEST(VSegmentTransferPlannerTest, RejectsAbsoluteAddressOverflow) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentTransferPlanner planner;
    VSegmentDescriptor replica{"partition-1", "vs-1", 1, 8};
    std::vector<VSegmentLayoutEntry> layouts{MakeLayoutEntryFromView(
        allocation.view, std::numeric_limits<uint64_t>::max())};

    EXPECT_EQ(planner.Plan(replica, {{1000, 8}}, &layouts).error,
              ErrorCode::INVALID_PARAMS);
}

TEST(VSegmentViewTest, KeepsProfileIdentityOutsideImmutableView) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-profile", Profile());
    ASSERT_TRUE(allocation);
    EXPECT_EQ(ValidateView(allocation.view, Profile()), ErrorCode::OK);

    // A View describes only mapping. The owning manager snapshot keeps the
    // profile identity needed for quota recovery.
    auto wrong_profile = Profile();
    wrong_profile.name = "other";
    EXPECT_EQ(ValidateView(allocation.view, wrong_profile),
              ErrorCode::OK);
}

TEST(VSegmentReplicaTest, RuntimeMetadataPreservesLogicalDescriptor) {
    VSegmentDescriptor expected{"partition-1", "vs-1", 128, 64};
    Replica replica(expected, ReplicaStatus::COMPLETE);

    EXPECT_EQ(replica.type(), ReplicaType::VSEGMENT);
    EXPECT_TRUE(replica.is_vsegment_replica());
    const auto descriptor = replica.get_descriptor();
    ASSERT_TRUE(descriptor.is_vsegment_replica());
    const auto& actual = descriptor.get_vsegment_descriptor();
    EXPECT_EQ(actual.partition_id, expected.partition_id);
    EXPECT_EQ(actual.vsegment_id, expected.vsegment_id);
    EXPECT_EQ(actual.logical_offset, expected.logical_offset);
    EXPECT_EQ(actual.length, expected.length);
}

TEST(PartitionQuotaAllocatorTest, PreservesConfiguredMemberOrder) {
    auto config = Config();
    std::swap(config.quotas[0], config.quotas[1]);
    PartitionQuotaAllocator allocator(config);
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    EXPECT_EQ(allocation.view.members[0].segment_id, "segment-b");
    EXPECT_EQ(allocation.view.members[1].segment_id, "segment-a");
}

TEST(CreationCoordinatorTest, ConcurrentCallersShareOneCreation) {
    CreationCoordinator coordinator;
    std::atomic<int> calls{0};
    std::promise<void> factory_entered;
    std::promise<void> release_factory;
    auto release = release_factory.get_future().share();
    std::atomic<bool> second_started{false};
    VSegmentAllocationResult first;
    VSegmentAllocationResult second;
    auto factory = [&] {
        ++calls;
        factory_entered.set_value();
        release.wait();
        VSegmentAllocationResult result;
        result.view.vsegment_id = "vs-1";
        return result;
    };
    std::thread one([&] { first = coordinator.GetOrCreate("p/default/0", factory); });
    factory_entered.get_future().wait();
    std::thread two([&] {
        second_started = true;
        second = coordinator.GetOrCreate("p/default/0", factory);
    });
    while (!second_started.load()) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    release_factory.set_value();
    one.join();
    two.join();
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(first.view.vsegment_id, "vs-1");
    EXPECT_EQ(second.view.vsegment_id, "vs-1");
}

TEST(VSegmentManagerTest, SnapshotRestorePreservesReservationsAndFreeSpace) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    auto reservation = manager.ReservePut(allocation.view.vsegment_id,
                                          "put-1", 128);
    ASSERT_TRUE(reservation);

    const auto snapshot = manager.Snapshot();
    ASSERT_EQ(snapshot.vsegments.size(), 1);
    VSegmentManager recovered(Config(), {Profile()}, Committer());
    std::string detail;
    ASSERT_EQ(recovered.Restore(snapshot, &detail), ErrorCode::OK) << detail;

    VSegmentView restored_view;
    EXPECT_TRUE(recovered.FindView(allocation.view.vsegment_id,
                                   &restored_view));
    EXPECT_EQ(restored_view.checksum, allocation.view.checksum);
    LogicalRange committed;
    EXPECT_EQ(recovered.CommitPut(allocation.view.vsegment_id, "put-1",
                                  "object-1", &committed),
              ErrorCode::OK);
    EXPECT_EQ(committed.offset, reservation.range.offset);
    EXPECT_EQ(committed.length, reservation.range.length);
}

TEST(VSegmentManagerTest, RejectsSnapshotFromAnotherConfigGeneration) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    ASSERT_TRUE(manager.Create("default"));
    auto snapshot = manager.Snapshot();
    snapshot.config_generation = 2;

    VSegmentManager recovered(Config(), {Profile()}, Committer());
    EXPECT_EQ(recovered.Restore(snapshot), ErrorCode::INVALID_VERSION);
}

TEST(VSegmentManagerTest, EnforcesLifecycleTransitions) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::DRAINING),
              ErrorCode::OK);
    EXPECT_FALSE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 8));
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OK);
}

TEST(VSegmentManagerTest, RefusesToRetireReferencedVSegment) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::DRAINING),
              ErrorCode::OK);
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OBJECT_REPLICA_BUSY);
}

TEST(VSegmentManagerTest, RetireReturnsExtentsToPartitionQuota) {
    auto config = Config();
    config.quotas[0].length = 256;
    config.quotas[1].length = 256;
    VSegmentManager manager(config, {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    EXPECT_EQ(manager.Create("default").error,
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::DRAINING),
              ErrorCode::OK);
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OK);
    EXPECT_TRUE(manager.Create("default"));
}

TEST(VSegmentManagerTest, ConsumesMultipleProfilesFromPublishedSnapshot) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 3;
    snapshot.policy_digest = "policy-v3";
    snapshot.default_profile = "dram";
    auto dram = Profile();
    dram.name = "dram";
    auto nvme = Profile();
    nvme.name = "nvme";
    nvme.required_medium = "NVMe";
    snapshot.profile_specs = {dram, nvme};
    snapshot.quotas = {
        {"partition-1", "dram", "DRAM",
         {{"dram-a", 0, 256}, {"dram-b", 0, 256}}},
        {"partition-1", "nvme", "NVMe",
         {{"nvme-a", 0, 256}, {"nvme-b", 0, 256}}}};

    VSegmentManager manager(snapshot, "partition-1", Committer());
    auto dram_view = manager.Create("dram");
    auto nvme_view = manager.Create("nvme");
    ASSERT_TRUE(dram_view);
    ASSERT_TRUE(nvme_view);
    EXPECT_EQ(dram_view.view.members[0].segment_id, "dram-a");
    EXPECT_EQ(nvme_view.view.members[0].segment_id, "nvme-a");
    EXPECT_EQ(manager.Snapshot().vsegments.size(), 2);
}

TEST(VSegmentManagerTest, RestoresCommittedIdentityForExactRelease) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    LogicalRange range;
    ASSERT_EQ(manager.CommitPut(allocation.view.vsegment_id, "put-1",
                                "object-1", &range),
              ErrorCode::OK);
    auto snapshot = manager.Snapshot();

    VSegmentManager recovered(Config(), {Profile()}, Committer());
    ASSERT_EQ(recovered.Restore(snapshot), ErrorCode::OK);
    EXPECT_EQ(recovered.ReleaseObject(allocation.view.vsegment_id,
                                      "object-1", range),
              ErrorCode::OK);
    const auto released = recovered.Snapshot();
    ASSERT_EQ(released.vsegments.size(), 1u);
    EXPECT_TRUE(released.operation_vsegments.empty());
    EXPECT_TRUE(
        released.vsegments.front().logical_allocation.completed_operations
            .empty());
}

TEST(VSegmentManagerTest, SnapshotIsJsonSerializable) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));

    std::string json;
    struct_json::to_json(manager.Snapshot(), json);
    PartitionVSegmentSnapshot decoded;
    struct_json::from_json(decoded, json);
    ASSERT_EQ(decoded.vsegments.size(), 1);
    EXPECT_EQ(decoded.partition_id, "partition-1");
    EXPECT_EQ(decoded.vsegments[0].logical_allocation.reservations.size(), 1);
}

TEST(VSegmentManagerTest, PersistsCreationBeforePublishingActive) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_EQ(committer->mutations.size(), 2);
    EXPECT_EQ(committer->mutations[0], "vsegment_create_begin");
    EXPECT_EQ(committer->mutations[1], "vsegment_create_commit");
    ASSERT_EQ(committer->last_state.vsegments.size(), 1);
    EXPECT_EQ(committer->last_state.vsegments[0].lifecycle,
              Lifecycle::ACTIVE);
}

TEST(VSegmentManagerTest, RollsBackReservationWhenPersistenceFails) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    committer->fail_next = true;
    auto failed = manager.ReservePut(allocation.view.vsegment_id, "put-1", 64);
    EXPECT_EQ(failed.error, ErrorCode::PERSISTENT_FAIL);

    auto retry = manager.ReservePut(allocation.view.vsegment_id, "put-1", 64);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry.range.offset, 0);
}

TEST(VSegmentManagerTest, RecoveryRollsBackUncommittedCreation) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    auto snapshot = manager.Snapshot();
    snapshot.vsegments[0].lifecycle = Lifecycle::PREPARING;

    VSegmentManager recovered(Config(), {Profile()}, Committer());
    ASSERT_EQ(recovered.Restore(snapshot), ErrorCode::OK);
    VSegmentView view;
    EXPECT_FALSE(recovered.FindView(allocation.view.vsegment_id, &view));
    EXPECT_TRUE(recovered.Create("default"));
}

TEST(VSegmentManagerTest, ConcurrentProfileCreationSharesOneVSegment) {
    auto committer = Committer();
    committer->delay_create_begin = true;
    VSegmentManager manager(Config(), {Profile()}, committer);
    constexpr size_t kCallers = 8;
    std::barrier start(static_cast<std::ptrdiff_t>(kCallers));
    std::vector<VSegmentAllocationResult> results(kCallers);
    std::vector<std::thread> threads;
    for (size_t index = 0; index < kCallers; ++index) {
        threads.emplace_back([&, index] {
            start.arrive_and_wait();
            results[index] = manager.Create("default");
        });
    }
    for (auto& thread : threads) thread.join();
    for (const auto& result : results) {
        ASSERT_TRUE(result);
        EXPECT_EQ(result.view.vsegment_id, results[0].view.vsegment_id);
    }
    EXPECT_EQ(std::count(committer->mutations.begin(),
                         committer->mutations.end(),
                         "vsegment_create_begin"),
              1);
}

TEST(VSegmentManagerTest, AsyncCreationReturnsRetryableStatus) {
    auto committer = Committer();
    committer->delay_create_begin = true;
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto first = manager.RequestCreate("default", 25);
    EXPECT_EQ(first.error, ErrorCode::VSEGMENT_CREATING);
    EXPECT_NE(first.detail.find("retry_after_ms=25"), std::string::npos);

    VSegmentAllocationResult completed;
    for (int attempt = 0; attempt < 20; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        completed = manager.RequestCreate("default");
        if (completed.error != ErrorCode::VSEGMENT_CREATING) break;
    }
    ASSERT_TRUE(completed) << completed.detail;
    EXPECT_EQ(std::count(committer->mutations.begin(),
                         committer->mutations.end(),
                         "vsegment_create_begin"),
              1);
}

TEST(VSegmentManagerTest, PutStartCreatesThenReturnsIdempotentDescriptor) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto creating = manager.StartPut("put-1", 96);
    EXPECT_EQ(creating.error, ErrorCode::VSEGMENT_CREATING);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        started = manager.StartPut("put-1", 96);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
    }
    ASSERT_TRUE(started) << started.detail;
    EXPECT_EQ(started.replica.partition_id, "partition-1");
    EXPECT_EQ(started.replica.logical_offset, 0);
    EXPECT_EQ(started.replica.length, 96);

    auto retry = manager.StartPut("put-1", 96);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry.replica.vsegment_id, started.replica.vsegment_id);
    EXPECT_EQ(retry.replica.logical_offset, started.replica.logical_offset);
}

TEST(VSegmentManagerTest, FencesStalePartitionOwnerRequests) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    ASSERT_EQ(manager.SetRouteEpoch(7), ErrorCode::OK);
    auto stale = manager.StartPut("put-1", 64, "default", 6);
    EXPECT_EQ(stale.error, ErrorCode::STALE_ROUTE);
    auto current = manager.StartPut("put-1", 64, "default", 7);
    EXPECT_EQ(current.error, ErrorCode::VSEGMENT_CREATING);
    EXPECT_EQ(manager.SetRouteEpoch(6), ErrorCode::STALE_ROUTE);
}

TEST(VSegmentManagerTest, ExposesLifecycleStatsAndGcsOperationTombstone) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    ASSERT_EQ(manager.AbortPut(allocation.view.vsegment_id, "put-1"),
              ErrorCode::OK);
    auto stats = manager.Stats();
    EXPECT_EQ(stats.active, 1);
    EXPECT_EQ(stats.reservations, 0);
    EXPECT_EQ(stats.committed_allocations, 0);
    EXPECT_EQ(manager.ForgetOperation("put-1"), ErrorCode::OK);
    EXPECT_EQ(manager.ForgetOperation("put-1"), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(VSegmentManagerTest, RetriesIncompleteInitialPrecreation) {
    auto committer = Committer();
    committer->fail_mutation = "vsegment_create";
    VSegmentManager manager(Config(), {Profile()}, committer);
    EXPECT_EQ(manager.EnsureInitialVSegments(), ErrorCode::PERSISTENT_FAIL);
    EXPECT_EQ(manager.Stats().preparing, 0u);

    EXPECT_EQ(manager.EnsureInitialVSegments(), ErrorCode::OK);
    EXPECT_EQ(manager.Stats().preparing, 0u);
    EXPECT_EQ(manager.Stats().active, 1u);
}

TEST(VSegmentManagerTest, BoundsAbortedOperationTombstones) {
    VSegmentManager manager(Config(), {Profile()}, Committer(), 2);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    for (const std::string operation : {"z-oldest", "a-middle", "b-newest"}) {
        ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, operation,
                                       8));
        ASSERT_EQ(manager.AbortPut(allocation.view.vsegment_id, operation),
                  ErrorCode::OK);
    }
    const auto state = manager.Snapshot();
    ASSERT_EQ(state.vsegments.size(), 1u);
    EXPECT_LE(state.vsegments[0].logical_allocation.completed_operations.size(),
              2u);
    EXPECT_LE(state.operation_vsegments.size(), 2u);
    EXPECT_FALSE(state.operation_vsegments.contains("z-oldest"));
    EXPECT_TRUE(state.operation_vsegments.contains("a-middle"));
    EXPECT_TRUE(state.operation_vsegments.contains("b-newest"));
}

TEST(VSegmentManagerTest, RecoveryReconcileUsesObjectMetadataAsAuthority) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(
        manager.ReservePut(allocation.view.vsegment_id, "committed-op", 64));
    LogicalRange committed_range;
    ASSERT_EQ(manager.CommitPut(allocation.view.vsegment_id, "committed-op",
                                "orphan-object", &committed_range),
              ErrorCode::OK);
    auto pending =
        manager.ReservePut(allocation.view.vsegment_id, "pending-op", 32);
    ASSERT_TRUE(pending);

    VSegmentDescriptor pending_descriptor{
        "partition-1", allocation.view.vsegment_id, pending.range.offset,
        pending.range.length, "pending-op"};
    ASSERT_EQ(manager.ReconcileObjectReferences(
                  {{pending_descriptor, "pending-object", false}}),
              ErrorCode::OK);
    EXPECT_EQ(manager.Stats().committed_allocations, 0);
    EXPECT_EQ(manager.Stats().reservations, 1);

    ASSERT_EQ(manager.AbortPut(allocation.view.vsegment_id, "pending-op"),
              ErrorCode::OK);
    auto full = manager.ReservePut(allocation.view.vsegment_id, "full-op",
                                   allocation.view.logical_capacity);
    EXPECT_TRUE(full) << full.detail;
}

TEST(VSegmentManagerTest, RecoveryReconcileRestoresCommittedReleaseIdentity) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    VSegmentDescriptor descriptor{"partition-1", allocation.view.vsegment_id,
                                  128, 64, "committed-op"};
    ASSERT_EQ(manager.ReconcileObjectReferences(
                  {{descriptor, "tenant/object", true}}),
              ErrorCode::OK);
    EXPECT_EQ(manager.ReleaseObject(allocation.view.vsegment_id,
                                    "tenant/object", {128, 64}),
              ErrorCode::OK);
}

TEST(VSegmentManagerTest, RetirementPersistenceFailureRollsBackExtent) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                         Lifecycle::DRAINING),
              ErrorCode::OK);

    committer->fail_next = true;
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::PERSISTENT_FAIL);
    VSegmentView view;
    EXPECT_TRUE(manager.FindView(allocation.view.vsegment_id, &view));
    EXPECT_EQ(manager.Stats().draining, 1);

    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OK);
    EXPECT_FALSE(manager.FindView(allocation.view.vsegment_id, &view));
}

TEST(VSegmentHaTest, ReplaysContinuousPartitionRevisions) {
    PartitionVSegmentSnapshot base;
    base.partition_id = "partition-1";
    base.config_generation = 1;
    base.route_epoch = 4;
    base.metadata_revision = 10;
    auto next = base;
    next.metadata_revision = 11;
    VSegmentOpLogRecord record{"partition-1", 4, 11, "logical_reserve",
                               next};
    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);

    PartitionVSegmentSnapshot recovered;
    std::string detail;
    EXPECT_EQ(ReplayVSegmentState(base, {entry}, &recovered, &detail),
              ErrorCode::OK)
        << detail;
    EXPECT_EQ(recovered.metadata_revision, 11);
    EXPECT_EQ(recovered.route_epoch, 4);
}

TEST(VSegmentHaTest, RejectsRevisionGapAndStaleEpoch) {
    PartitionVSegmentSnapshot base;
    base.partition_id = "partition-1";
    base.config_generation = 1;
    base.route_epoch = 4;
    base.metadata_revision = 10;
    auto next = base;
    next.metadata_revision = 12;
    VSegmentOpLogRecord record{"partition-1", 4, 12, "logical_reserve",
                               next};
    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    PartitionVSegmentSnapshot recovered;
    EXPECT_EQ(ReplayVSegmentState(base, {entry}, &recovered),
              ErrorCode::OPLOG_ENTRY_NOT_FOUND);

    record.metadata_revision = 11;
    record.route_epoch = 3;
    record.state.metadata_revision = 11;
    record.state.route_epoch = 3;
    entry.payload.clear();
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    EXPECT_EQ(ReplayVSegmentState(base, {entry}, &recovered),
              ErrorCode::STALE_ROUTE);
}

TEST(VSegmentHaTest, ReplaysIncrementalDeltaAndLegacyFullState) {
    PartitionVSegmentSnapshot base;
    base.partition_id = "partition-1";
    base.config_generation = 1;
    base.route_epoch = 4;
    base.metadata_revision = 10;
    base.operation_vsegments["old-op"] = "vs-old";

    VSegmentStateSnapshot added{"default", Lifecycle::ACTIVE, View(), {}};
    added.view.partition_id = base.partition_id;
    added.view.vsegment_id = "vs-new";
    added.logical_allocation.logical_capacity = added.view.logical_capacity;
    added.logical_allocation.free_ranges = {
        {0, added.view.logical_capacity}};
    VSegmentOpLogRecord delta;
    delta.partition_id = base.partition_id;
    delta.route_epoch = 4;
    delta.metadata_revision = 11;
    delta.mutation = "logical_reserve";
    delta.full_state = false;
    delta.delta.upserted_vsegments.push_back(added);
    delta.delta.removed_operation_ids.push_back("old-op");
    delta.delta.upserted_operations["new-op"] = "vs-new";
    OpLogEntry delta_entry;
    delta_entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(delta, delta_entry.payload);
    delta_entry.checksum = ComputeOpLogChecksum(delta_entry.payload);

    PartitionVSegmentSnapshot recovered;
    std::string detail;
    ASSERT_EQ(ReplayVSegmentState(base, {delta_entry}, &recovered, &detail),
              ErrorCode::OK)
        << detail;
    ASSERT_EQ(recovered.vsegments.size(), 1u);
    EXPECT_EQ(recovered.vsegments.front().view.vsegment_id, "vs-new");
    EXPECT_FALSE(recovered.operation_vsegments.count("old-op"));
    EXPECT_EQ(recovered.operation_vsegments.at("new-op"), "vs-new");

    auto legacy_state = recovered;
    legacy_state.metadata_revision = 12;
    LegacyVSegmentOpLogRecord legacy{base.partition_id, 4, 12,
                                     "logical_commit", legacy_state};
    OpLogEntry legacy_entry;
    legacy_entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(legacy, legacy_entry.payload);
    legacy_entry.checksum = ComputeOpLogChecksum(legacy_entry.payload);
    ASSERT_EQ(ReplayVSegmentState(recovered, {legacy_entry}, &recovered,
                                  &detail),
              ErrorCode::OK)
        << detail;
    EXPECT_EQ(recovered.metadata_revision, 12u);
}

TEST(VSegmentMetricsTest, ExportsCapacityAndFailureSignals) {
    auto& metrics = VSegmentMetrics::Instance();
    metrics.SetCapacity(1024, 256, 2, 3, 1);
    metrics.IncCreateFailure();
    metrics.IncPersistenceFailure();
    metrics.IncStaleRoute();
    const auto text = metrics.Serialize();
    EXPECT_NE(text.find("mooncake_vsegment_logical_capacity_bytes 1024"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_vsegment_logical_free_bytes 256"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_vsegment_create_failure_total"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_vsegment_persistence_failure_total"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_vsegment_stale_route_total"),
              std::string::npos);
}

TEST(VSegmentHaTest, AcceptedCommitWaitsForDurabilityWithoutRollbackSignal) {
    std::atomic<bool> allow_write{false};
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{},
        [&](const OpLogBatchRecord&, const DurablePrefix&) {
            while (!allow_write.load()) std::this_thread::yield();
            return ErrorCode::OK;
        });
    writer.Start();
    OrderedOpLogVSegmentCommitter committer(
        &writer, std::chrono::milliseconds(5));
    PartitionVSegmentSnapshot state;
    state.partition_id = "partition-1";
    state.config_generation = 1;
    state.route_epoch = 1;
    state.metadata_revision = 1;

    auto result = std::async(std::launch::async, [&] {
        return committer.Commit(state, "logical_reserve", nullptr);
    });
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    allow_write = true;
    EXPECT_EQ(result.get(), ErrorCode::OK);
    writer.Stop();
}

TEST(VSegmentHaTest, DurableMetadataCallbackCanPersistReleaseWithoutDeadlock) {
    OrderedOpLogWriter writer(OrderedOpLogWriterConfig{},
                              [](const OpLogBatchRecord&,
                                 const DurablePrefix&) {
                                  return ErrorCode::OK;
                              });
    writer.Start();
    auto committer = std::make_shared<OrderedOpLogVSegmentCommitter>(&writer);
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    LogicalRange range;
    ASSERT_EQ(manager.CommitPut(allocation.view.vsegment_id, "put-1", "object-1", &range),
              ErrorCode::OK);
    std::promise<ErrorCode> completed;
    auto completion = completed.get_future();

    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    OpLogEntry metadata_remove;
    metadata_remove.op_type = OpType::REMOVE;
    metadata_remove.object_key = "object-1";
    metadata_remove.payload = "remove";
    metadata_remove.checksum = ComputeOpLogChecksum(metadata_remove.payload);
    auto pending = writer.Commit(
        std::move(*reservation), std::move(metadata_remove),
        [&](const OpLogEntry&) {
            completed.set_value(
                manager.ReleaseObject(allocation.view.vsegment_id, "object-1", range));
        });
    ASSERT_TRUE(pending.has_value());
    ASSERT_EQ(completion.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(completion.get(), ErrorCode::OK);
    writer.Stop();
}

TEST(VSegmentHaTest, InitialCheckpointAndDeltaUseTheSameReplayPath) {
    VSegmentOpLogRecord record;
    record.partition_id = "partition-1";
    record.route_epoch = 3;
    record.metadata_revision = 42;
    record.state.partition_id = record.partition_id;
    record.state.route_epoch = 3;
    record.state.metadata_revision = 42;
    record.full_state = true;
    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_STATE;
    entry.object_key = record.partition_id;
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    std::unordered_map<std::string, PartitionVSegmentSnapshot> states;
    ASSERT_EQ(ApplyVSegmentStateEntry(entry, states), ErrorCode::OK);
    EXPECT_EQ(states.at("partition-1").metadata_revision, 42u);
    record.full_state = false;
    record.state = {};
    record.route_epoch = 4;
    record.metadata_revision = 43;
    entry.payload.clear();
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    ASSERT_EQ(ApplyVSegmentStateEntry(entry, states), ErrorCode::OK);
    EXPECT_EQ(states.at("partition-1").metadata_revision, 43u);
    EXPECT_EQ(states.at("partition-1").route_epoch, 4u);
}

// These regressions use an in-process storage callback, not an etcd server.
TEST(VSegmentHaTest, DurableWaitDoesNotDependOnCallbackProgress) {
    OrderedOpLogWriter writer(OrderedOpLogWriterConfig{},
        [](const OpLogBatchRecord&, const DurablePrefix&) { return ErrorCode::OK; });
    writer.Start();
    std::promise<void> entered, release;
    auto entered_future = entered.get_future();
    auto release_future = release.get_future().share();
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation);
    OpLogEntry entry;
    entry.op_type = OpType::REMOVE;
    entry.object_key = "callback-blocker";
    auto pending = writer.Commit(std::move(*reservation), std::move(entry),
        [&](const OpLogEntry&) { entered.set_value(); release_future.wait(); });
    ASSERT_TRUE(pending);
    entered_future.wait();
    OrderedOpLogVSegmentCommitter committer(&writer);
    PartitionVSegmentSnapshot state;
    state.partition_id = "partition-1";
    state.route_epoch = 1;
    state.metadata_revision = 1;
    auto result = std::async(std::launch::async, [&] {
        return committer.Commit(state, "checkpoint", nullptr);
    });
    const auto status = result.wait_for(std::chrono::seconds(2));
    release.set_value();
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_EQ(result.get(), ErrorCode::OK);
    writer.Stop();
}

TEST(VSegmentManagerTest, InitialBatchDefersOnlyTheDurabilityBarrier) {
    std::atomic<bool> allow_write{false};
    OrderedOpLogWriter writer(OrderedOpLogWriterConfig{},
        [&](const OpLogBatchRecord&, const DurablePrefix&) {
            while (!allow_write.load()) std::this_thread::yield();
            return ErrorCode::OK;
        });
    writer.Start();
    auto committer = std::make_shared<OrderedOpLogVSegmentCommitter>(&writer);
    VSegmentManager manager(Config(), {Profile()}, committer);
    VSegmentCommitBatch batch;
    auto initialized = std::async(std::launch::async, [&] {
        return manager.EnsureInitialVSegments(nullptr, &batch);
    });
    const auto init_status = initialized.wait_for(std::chrono::seconds(2));
    if (init_status != std::future_status::ready) allow_write = true;
    EXPECT_EQ(init_status, std::future_status::ready);
    EXPECT_EQ(initialized.get(), ErrorCode::OK);
    if (init_status != std::future_status::ready) {
        writer.Stop();
        return;
    }
    EXPECT_EQ(manager.Stats().active, 1u);
    auto result = std::async(std::launch::async, [&] { return batch.Flush(); });
    const auto status = result.wait_for(std::chrono::milliseconds(20));
    allow_write = true;
    EXPECT_EQ(status, std::future_status::timeout);
    EXPECT_EQ(result.get(), ErrorCode::OK);
    writer.Stop();
}

TEST(VSegmentManagerTest, IdempotentStartWaitsForOriginalSubmission) {
    if (VSegmentOplogAlignedMode()) {
        GTEST_SKIP() << "full-mode only: aligned writes no state record, so "
                        "there is no durable fence to wait for";
    }
    std::atomic<bool> allow_write{true};
    OrderedOpLogWriter writer(OrderedOpLogWriterConfig{},
        [&](const OpLogBatchRecord&, const DurablePrefix&) {
            while (!allow_write.load()) std::this_thread::yield();
            return ErrorCode::OK;
        });
    writer.Start();
    auto committer = std::make_shared<OrderedOpLogVSegmentCommitter>(&writer);
    VSegmentManager manager(Config(), {Profile()}, committer);
    ASSERT_EQ(manager.EnsureInitialVSegments(), ErrorCode::OK);
    allow_write = false;
    auto first = std::async(std::launch::async, [&] {
        return manager.StartPut("same-operation", 64);
    });
    // Observe submission without waiting for storage durability.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!manager.Snapshot().operation_vsegments.count("same-operation") &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    auto retry = std::async(std::launch::async, [&] {
        return manager.StartPut("same-operation", 64);
    });
    const auto status = retry.wait_for(std::chrono::milliseconds(20));
    allow_write = true;
    EXPECT_EQ(status, std::future_status::timeout);
    auto original = first.get();
    auto repeated = retry.get();
    ASSERT_TRUE(original);
    ASSERT_TRUE(repeated);
    EXPECT_EQ(original.replica.logical_offset, repeated.replica.logical_offset);
    writer.Stop();
}

TEST(VSegmentManagerTest, DetachedManagerRejectsWritesAtTheSameEpoch) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    ASSERT_EQ(manager.SetRouteEpoch(7), ErrorCode::OK);
    ASSERT_EQ(manager.EnsureInitialVSegments(), ErrorCode::OK);
    ASSERT_EQ(manager.Deactivate(7), ErrorCode::OK);
    EXPECT_EQ(manager.StartPut("late-put", 64, "default", 7).error,
              ErrorCode::STALE_ROUTE);
    EXPECT_EQ(manager.SetRouteEpoch(7), ErrorCode::STALE_ROUTE);
}

TEST(VSegmentServiceTest, PartitionIndexDeduplicatesProfiles) {
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.default_profile = "default";
    auto alternate = Profile();
    alternate.name = "alternate";
    quota.profile_specs = {Profile(), alternate};
    quota.quotas = {
        {"partition-1", "default", "DRAM", {{"segment-a", 0, 512}}},
        {"partition-1", "alternate", "DRAM", {{"segment-b", 0, 512}}}};
    VSegmentService service(quota);
    ASSERT_EQ(service.PartitionIds().size(), 1u);
    EXPECT_TRUE(service.HasPartitionQuota("partition-1"));
    EXPECT_FALSE(service.HasPartitionQuota("missing"));
}

TEST(VSegmentServiceTest, OwnsPartitionPutAndViewLifecycle) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 8, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 8, "put-1", 80);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    EXPECT_EQ(service.CommitPut(started.replica, 8, "put-1", "object-1"),
              ErrorCode::OK);
    VSegmentView view;
    EXPECT_EQ(service.LoadView("partition-1", started.replica.vsegment_id,
                               &view),
              ErrorCode::OK);
    EXPECT_EQ(view.partition_id, "partition-1");
    EXPECT_EQ(service.StartPut("partition-1", 7, "put-2", 16).error,
              ErrorCode::STALE_ROUTE);
    EXPECT_EQ(service.RemovePartition("partition-1", 9), ErrorCode::OK);
    EXPECT_EQ(service.LoadView("partition-1", view.vsegment_id, &view),
              ErrorCode::STALE_ROUTE);
}

TEST(VSegmentServiceTest, ThrottlesMetricsRefreshAndForcesOnOwnershipChange) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {{"partition-1",
                        "default",
                        "DRAM",
                        {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};

    // A long window makes non-forced refreshes deterministic no-ops.
    VSegmentService service(snapshot, std::chrono::hours(1));
    ASSERT_EQ(service.AddPartition("partition-1", 8, Committer()),
              ErrorCode::OK);
    EXPECT_EQ(MetricValue("mooncake_vsegment_logical_capacity_bytes"), 512u);
    EXPECT_EQ(MetricValue("mooncake_vsegment_reservations"), 0u);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 8, "put-1", 80);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    // Non-forced refresh is throttled: the new reservation is not visible.
    EXPECT_EQ(MetricValue("mooncake_vsegment_reservations"), 0u);
    EXPECT_EQ(MetricValue("mooncake_vsegment_logical_capacity_bytes"), 512u);

    // An ownership change forces a refresh: the removed Partition is gone.
    ASSERT_EQ(service.RemovePartition("partition-1", 9), ErrorCode::OK);
    EXPECT_EQ(MetricValue("mooncake_vsegment_logical_capacity_bytes"), 0u);
}

TEST(VSegmentServiceTest, ZeroIntervalRefreshesOnNonForcedCalls) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {{"partition-1",
                        "default",
                        "DRAM",
                        {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};

    VSegmentService service(snapshot, std::chrono::milliseconds(0));
    ASSERT_EQ(service.AddPartition("partition-1", 8, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 8, "put-1", 80);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    // Zero interval disables throttling: the reservation is visible.
    EXPECT_EQ(MetricValue("mooncake_vsegment_reservations"), 1u);
}

TEST(VSegmentServiceTest, ReadsMetricsRefreshIntervalFromEnvironment) {
    constexpr char kIntervalEnv[] =
        "MOONCAKE_VSEGMENT_METRICS_REFRESH_INTERVAL_MS";
    ScopedEnv interval_env(kIntervalEnv);
    ASSERT_EQ(setenv(kIntervalEnv, "0", 1), 0);

    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {{"partition-1",
                        "default",
                        "DRAM",
                        {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};

    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 8, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 8, "put-1", 80);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    EXPECT_EQ(MetricValue("mooncake_vsegment_reservations"), 1u);
}

TEST(VSegmentServiceTest, RejectsDescriptorMismatchBeforeCommit) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 8, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 8, "put-1", 80);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    auto mismatched = started.replica;
    ++mismatched.logical_offset;
    EXPECT_EQ(service.CommitPut(mismatched, 8, "put-1", "object-1"),
              ErrorCode::INVALID_VERSION);
    EXPECT_EQ(service.CommitPut(started.replica, 8, "put-1", "object-1"),
              ErrorCode::OK);
}

TEST(VSegmentServiceTest, RejectsPartitionWithoutPublishedQuota) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    std::string detail;
    EXPECT_EQ(service.AddPartition("partition-2", 1, Committer(), nullptr,
                                   &detail),
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
}

TEST(VSegmentServiceTest, PrecreatesConfiguredInitialVSegments) {
    auto profile = Profile();
    profile.initial_vsegment_count = 2;
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {profile};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);

    PartitionVSegmentSnapshot state;
    ASSERT_EQ(service.SnapshotPartition("partition-1", &state),
              ErrorCode::OK);
    ASSERT_EQ(state.vsegments.size(), 2u);
    EXPECT_TRUE(std::all_of(state.vsegments.begin(), state.vsegments.end(),
                            [](const auto& item) {
                                return item.lifecycle == Lifecycle::ACTIVE;
                            }));
}

TEST(VSegmentServiceTest, ReconcilesManagerFromPartitionRouteEpoch) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    partition::PartitionRoute route;
    route.partition_id.partition_id = "partition-1";
    route.owner_submaster_id = "submaster-a";
    route.route_epoch = 3;
    route.state = static_cast<int32_t>(partition::PartitionState::kActive);

    EXPECT_EQ(service.ReconcilePartitionRoute(route, "submaster-a",
                                              Committer()),
              ErrorCode::OK);
    route.state =
        static_cast<int32_t>(partition::PartitionState::kMigrating);
    route.target_submaster_id = "submaster-b";
    route.route_epoch = 4;
    EXPECT_EQ(service.ReconcilePartitionRoute(route, "submaster-a",
                                              Committer()),
              ErrorCode::OK);
    EXPECT_EQ(service.StartPut("partition-1", 3, "stale", 16).error,
              ErrorCode::STALE_ROUTE);

    route.owner_submaster_id = "submaster-b";
    route.target_submaster_id.clear();
    route.state = static_cast<int32_t>(partition::PartitionState::kActive);
    route.route_epoch = 5;
    EXPECT_EQ(service.ReconcilePartitionRoute(route, "submaster-a", nullptr),
              ErrorCode::OK);
    EXPECT_EQ(service.StartPut("partition-1", 5, "not-owner", 16).error,
              ErrorCode::STALE_ROUTE);
}

TEST(VSegmentServiceTest, ReleasesCommittedObjectRange) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);
    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 1, "put-release", 64);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    ASSERT_EQ(service.CommitPut(started.replica, 1, "put-release", "object-1"),
              ErrorCode::OK);
    EXPECT_EQ(service.ReleaseObject(started.replica, "object-1"),
              ErrorCode::OK);
    EXPECT_EQ(service.ReleaseObject(started.replica, "object-1"),
              ErrorCode::OK);
}

TEST(VSegmentServiceTest, AbortReturnsReservedLogicalRange) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 256}, {"segment-b", 0, 256}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 1, "put-abort", 64);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    ASSERT_EQ(service.AbortPut("partition-1", started.replica.vsegment_id, 1,
                               "put-abort"),
              ErrorCode::OK);

    PartitionVSegmentSnapshot state;
    ASSERT_EQ(service.SnapshotPartition("partition-1", &state),
              ErrorCode::OK);
    ASSERT_EQ(state.vsegments.size(), 1u);
    EXPECT_TRUE(state.vsegments[0].logical_allocation.reservations.empty());
    ASSERT_EQ(state.vsegments[0].logical_allocation.free_ranges.size(), 1u);
    EXPECT_EQ(state.vsegments[0].logical_allocation.free_ranges[0].offset,
              0u);
    EXPECT_EQ(state.vsegments[0].logical_allocation.free_ranges[0].length,
              state.vsegments[0].view.logical_capacity);
}

TEST(VSegmentServiceTest, ReplicaAllocationFailureAbortsEarlierReservations) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    // Exactly one vsegment can be created, while the request needs two
    // distinct vsegments.
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 256}, {"segment-b", 0, 256}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);

    tl::expected<std::vector<VSegmentPutStartResult>, ErrorCode> result;
    for (int attempt = 0; attempt < 40; ++attempt) {
        const auto suffix = std::to_string(attempt);
        result = service.StartPutReplicasOwned(
            "partition-1",
            {"replica-1-" + suffix, "replica-2-" + suffix}, 64);
        if (result || result.error() != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(),
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);

    PartitionVSegmentSnapshot state;
    ASSERT_EQ(service.SnapshotPartition("partition-1", &state),
              ErrorCode::OK);
    ASSERT_EQ(state.vsegments.size(), 1u);
    EXPECT_TRUE(state.vsegments[0].logical_allocation.reservations.empty());
}

TEST(VSegmentServiceTest, SurfacesReplicaRollbackPersistenceFailure) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 256}, {"segment-b", 0, 256}}}};
    auto committer = Committer();
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, committer),
              ErrorCode::OK);
    committer->fail_mutation = "logical_abort";

    auto result = service.StartPutReplicasOwned(
        "partition-1", {"rollback-1", "rollback-2"}, 64);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::PERSISTENT_FAIL);

    PartitionVSegmentSnapshot state;
    ASSERT_EQ(service.SnapshotPartition("partition-1", &state),
              ErrorCode::OK);
    ASSERT_EQ(state.vsegments.size(), 1u);
    EXPECT_EQ(state.vsegments[0].logical_allocation.reservations.size(), 1u);
}

TEST(VSegmentServiceTest, RestoresMigratedPartitionAtNewRouteEpoch) {
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService source(quota);
    ASSERT_EQ(source.AddPartition("partition-1", 3, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = source.StartPut("partition-1", 3, "put-before-move", 64);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    ASSERT_EQ(source.CommitPut(started.replica, 3, "put-before-move",
                               "object-1"),
              ErrorCode::OK);
    PartitionVSegmentSnapshot recovered;
    ASSERT_EQ(source.SnapshotPartition("partition-1", &recovered),
              ErrorCode::OK);

    VSegmentService target(quota);
    ASSERT_EQ(target.AddPartition("partition-1", 4, Committer(), &recovered),
              ErrorCode::OK);
    VSegmentView restored_view;
    EXPECT_EQ(target.LoadView("partition-1", started.replica.vsegment_id,
                              &restored_view),
              ErrorCode::OK);
    EXPECT_EQ(target.StartPut("partition-1", 3, "stale", 16).error,
              ErrorCode::STALE_ROUTE);
    auto next = target.StartPut("partition-1", 4, "put-after-move", 16);
    ASSERT_TRUE(next) << next.detail;
    EXPECT_EQ(next.replica.vsegment_id, started.replica.vsegment_id);
    EXPECT_EQ(next.replica.logical_offset,
              started.replica.logical_offset + started.replica.length);
}

TEST(VSegmentIntegrationTest, OrdinaryPutReturnsAndCommitsVSegmentReplica) {
    const std::string key = "ordinary-vsegment-put";
    const auto& tenant = TenantId::Default();
    const std::string partition_id =
        std::to_string(cvm::KeySlot(tenant, key));
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {
        {partition_id, "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);

    MasterServiceConfig config;
    config.default_kv_lease_ttl = 10000;
    MasterService master(config);
    master.SetVSegmentService(vsegments);
    ReplicateConfig replicas;
    replicas.replica_num = 1;
    UUID client = generate_uuid();
    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64,
                                             replicas);
        if (started && started->has_value()) break;
        if (!started && started.error() != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started.has_value());
    ASSERT_TRUE(started->has_value());
    ASSERT_EQ((**started).replicas.size(), 1u);
    EXPECT_TRUE((**started).replicas.front().is_vsegment_replica());

    ObjectMeta object{key, std::nullopt};
    // The ordinary client API still describes this as its memory write path;
    // the server maps that completion to the returned VSEGMENT replica.
    ASSERT_TRUE(master.PutEnd(client, object, tenant, ReplicaType::MEMORY,
                              (**started).operation_id));
    auto loaded = master.GetReplicaList(key, tenant);
    ASSERT_TRUE(loaded.has_value());
    ASSERT_EQ(loaded->replicas.size(), 1u);
    EXPECT_TRUE(loaded->replicas.front().is_vsegment_replica());
}

TEST(VSegmentLayoutMasterTest, BatchAndSingleResponsesCarryResolvedLayouts) {
    const std::string key = "layout-master-batch";
    const auto& tenant = TenantId::Default();
    const std::string partition_id =
        std::to_string(cvm::KeySlot(tenant, key));
    const UUID segment_a = generate_uuid();
    const UUID segment_b = generate_uuid();

    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {{partition_id,
                     "default",
                     "DRAM",
                     {{UuidToString(segment_a), 0, 512},
                      {UuidToString(segment_b), 0, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);

    MasterServiceConfig config;
    config.default_kv_lease_ttl = 10000;
    MasterService master(config);
    master.SetVSegmentService(vsegments);

    const UUID client = generate_uuid();
    Segment mounted_a;
    mounted_a.id = segment_a;
    mounted_a.name = "layout-seg-a";
    mounted_a.base = 0x400000;
    mounted_a.size = 1 << 20;
    mounted_a.te_endpoint = "10.0.0.1:12345";
    ASSERT_TRUE(master.MountSegment(mounted_a, client).has_value());
    Segment mounted_b;
    mounted_b.id = segment_b;
    mounted_b.name = "layout-seg-b";
    mounted_b.base = 0x500000;
    mounted_b.size = 1 << 20;
    mounted_b.te_endpoint = "10.0.0.2:12345";
    ASSERT_TRUE(master.MountSegment(mounted_b, client).has_value());

    ReplicateConfig replicas;
    replicas.replica_num = 1;
    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64, replicas);
        if (started && started->has_value()) break;
        if (!started && started.error() != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started.has_value());
    ASSERT_TRUE(started->has_value());
    const auto& replica = (**started).replicas.front();
    ASSERT_TRUE(replica.is_vsegment_replica());
    const std::string vsegment_id =
        replica.get_vsegment_descriptor().vsegment_id;
    // Phase 2: the single PutStart response already carries the table.
    ASSERT_TRUE((**started).vsegment_layouts.has_value());
    EXPECT_EQ((**started).vsegment_layouts->front().vsegment_id, vsegment_id);

    ObjectMeta object{key, std::nullopt};
    ASSERT_TRUE(master.PutEnd(client, object, tenant, ReplicaType::MEMORY,
                              (**started).operation_id));

    // Batch: the deduplicated table rides on the first successful response.
    auto batch = master.BatchGetReplicaList({key}, tenant);
    ASSERT_EQ(batch.size(), 1u);
    ASSERT_TRUE(batch[0].has_value());
    ASSERT_TRUE(batch[0]->vsegment_layouts.has_value());
    const auto& entry = batch[0]->vsegment_layouts->front();
    EXPECT_EQ(entry.vsegment_id, vsegment_id);
    EXPECT_EQ(entry.partition_id, partition_id);
    ASSERT_EQ(entry.members.size(), 2u);
    EXPECT_EQ(entry.members[0].endpoint, "10.0.0.1:12345");
    EXPECT_EQ(entry.members[0].base_address, 0x400000u);
    EXPECT_EQ(entry.members[1].endpoint, "10.0.0.2:12345");
    EXPECT_EQ(entry.members[1].base_address, 0x500000u);

    // Cache: a second call returns the same single entry (no duplicate), and
    // only the first successful key carries it.
    auto cached = master.BatchGetReplicaList({key, key}, tenant);
    ASSERT_EQ(cached.size(), 2u);
    ASSERT_TRUE(cached[0]->vsegment_layouts.has_value());
    EXPECT_EQ(cached[0]->vsegment_layouts->size(), 1u);
    EXPECT_FALSE(cached[1]->vsegment_layouts.has_value());

    // Single read also carries the table.
    auto loaded = master.GetReplicaList(key, tenant);
    ASSERT_TRUE(loaded.has_value());
    ASSERT_TRUE(loaded->vsegment_layouts.has_value());
    EXPECT_EQ(loaded->vsegment_layouts->front().vsegment_id, vsegment_id);

    // UpsertStart returns descriptors from the generic upsert state machine;
    // the RPC wrapper enriches that response through this overload.
    auto upsert = master.UpsertStart(client, key, tenant, 64, replicas);
    ASSERT_TRUE(upsert.has_value());
    PutStartResult upsert_response{{}, std::move(upsert.value())};
    master.EnrichVSegmentLayouts(&upsert_response);
    ASSERT_TRUE(upsert_response.vsegment_layouts.has_value());
    EXPECT_EQ(upsert_response.vsegment_layouts->front().vsegment_id,
              vsegment_id);
    ASSERT_TRUE(master.UpsertEnd(client, ObjectMeta{key, std::nullopt}, tenant,
                                 ReplicaType::MEMORY));

    auto batch_upsert =
        master.BatchUpsertStart(client, {key}, tenant, {64}, replicas);
    ASSERT_EQ(batch_upsert.size(), 1u);
    ASSERT_TRUE(batch_upsert.front().has_value());
    PutStartResult batch_upsert_response{
        {}, std::move(batch_upsert.front().value())};
    master.EnrichVSegmentLayouts(&batch_upsert_response);
    ASSERT_TRUE(batch_upsert_response.vsegment_layouts.has_value());
    EXPECT_EQ(batch_upsert_response.vsegment_layouts->front().vsegment_id,
              vsegment_id);
    auto batch_end =
        master.BatchUpsertEnd(client, {ObjectMeta{key, std::nullopt}}, tenant);
    ASSERT_EQ(batch_end.size(), 1u);
    EXPECT_TRUE(batch_end.front().has_value());
}

TEST(VSegmentLayoutMasterTest, UnmountInvalidatesCachedLayouts) {
    const std::string partition_id = "partition-layout-unmount";
    const UUID segment_a = generate_uuid();
    const UUID segment_b = generate_uuid();

    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {{partition_id,
                     "default",
                     "DRAM",
                     {{UuidToString(segment_a), 0, 512},
                      {UuidToString(segment_b), 0, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);
    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = vsegments->StartPut(partition_id, 1, "layout-unmount", 64);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;

    MasterServiceConfig config;
    MasterService master(config);
    master.SetVSegmentService(vsegments);
    const UUID client = generate_uuid();
    Segment mounted_a;
    mounted_a.id = segment_a;
    mounted_a.name = "u-seg-a";
    mounted_a.base = 0x400000;
    mounted_a.size = 1 << 20;
    mounted_a.te_endpoint = "10.0.0.1:12345";
    ASSERT_TRUE(master.MountSegment(mounted_a, client).has_value());
    Segment mounted_b;
    mounted_b.id = segment_b;
    mounted_b.name = "u-seg-b";
    mounted_b.base = 0x500000;
    mounted_b.size = 1 << 20;
    mounted_b.te_endpoint = "10.0.0.2:12345";
    ASSERT_TRUE(master.MountSegment(mounted_b, client).has_value());

    auto table = master.BuildVSegmentLayoutTable(
        {{partition_id, started.replica.vsegment_id}});
    ASSERT_EQ(table.size(), 1u);

    ASSERT_TRUE(master.UnmountSegment(segment_a, client).has_value());
    auto after = master.BuildVSegmentLayoutTable(
        {{partition_id, started.replica.vsegment_id}});
    EXPECT_TRUE(after.empty());
}

TEST(VSegmentIntegrationTest, OrdinaryMemoryRevokeRemovesVSegmentReplica) {
    const std::string key = "ordinary-vsegment-revoke";
    const auto& tenant = TenantId::Default();
    const std::string partition_id =
        std::to_string(cvm::KeySlot(tenant, key));
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {
        {partition_id, "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);

    MasterService master;
    master.SetVSegmentService(vsegments);
    ReplicateConfig config;
    UUID client = generate_uuid();
    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64, config);
        if (started && started->has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started && started->has_value());
    ASSERT_TRUE(master.PutRevoke(client, key, tenant, ReplicaType::MEMORY,
                                 (**started).operation_id));
    EXPECT_EQ(master.GetReplicaList(key, tenant).error(),
              ErrorCode::OBJECT_NOT_FOUND);
}

TEST(VSegmentIntegrationTest, SizeChangingUpsertReleasesOldAndNewRanges) {
    const std::string key = "vsegment-upsert-release";
    const auto& tenant = TenantId::Default();
    const std::string partition_id =
        std::to_string(cvm::KeySlot(tenant, key));
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {
        {partition_id, "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);
    MasterService master;
    master.SetVSegmentService(vsegments);
    ReplicateConfig config;
    UUID client = generate_uuid();

    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64, config);
        if (started && started->has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started && started->has_value());
    const auto original = (**started).replicas.front()
                              .get_vsegment_descriptor()
                              .vsegment_id;
    ASSERT_TRUE(master.PutEnd(client, key, tenant, ReplicaType::MEMORY,
                              (**started).operation_id));

    auto upsert = master.UpsertStart(client, key, tenant, 128, config);
    ASSERT_TRUE(upsert.has_value());
    ASSERT_TRUE(master.PutRevoke(client, key, tenant, ReplicaType::MEMORY));

    auto full = vsegments->StartPutOwned(partition_id, "full-after-upsert",
                                         Profile().member_extent_size * 2);
    ASSERT_TRUE(full) << full.detail;
    EXPECT_EQ(full.replica.vsegment_id, original);
}

TEST(VSegmentIntegrationTest, SameSizeUpsertRevokeReleasesCommittedRange) {
    const std::string key = "vsegment-upsert-inplace-revoke";
    const auto& tenant = TenantId::Default();
    const std::string partition_id = std::to_string(cvm::KeySlot(tenant, key));
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile()};
    quota.quotas = {{partition_id,
                     "default",
                     "DRAM",
                     {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);
    MasterService master;
    master.SetVSegmentService(vsegments);
    ReplicateConfig config;
    UUID client = generate_uuid();

    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64, config);
        if (started && started->has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started && started->has_value());
    const auto original = (**started).replicas.front()
                              .get_vsegment_descriptor()
                              .vsegment_id;
    ASSERT_TRUE(master.PutEnd(client, key, tenant, ReplicaType::MEMORY,
                              (**started).operation_id));

    auto upsert = master.UpsertStart(client, key, tenant, 64, config);
    ASSERT_TRUE(upsert.has_value());
    ASSERT_TRUE(master.UpsertRevoke(client, key, tenant,
                                    ReplicaType::MEMORY));
    EXPECT_EQ(master.GetReplicaList(key, tenant).error(),
              ErrorCode::OBJECT_NOT_FOUND);

    auto full = vsegments->StartPutOwned(partition_id, "full-after-revoke",
                                         Profile().member_extent_size * 2);
    ASSERT_TRUE(full) << full.detail;
    EXPECT_EQ(full.replica.vsegment_id, original);
}

TEST(VSegmentServiceTest, ObjectReplicasUseDistinctVSegments) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 1, Committer()),
              ErrorCode::OK);
    tl::expected<std::vector<VSegmentPutStartResult>, ErrorCode> result;
    for (int attempt = 0; attempt < 40; ++attempt) {
        result = service.StartPutReplicasOwned(
            "partition-1", {"replica-op-1", "replica-op-2"}, 64);
        if (result || result.error() != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    ASSERT_EQ(result->size(), 2u);
    EXPECT_NE((*result)[0].replica.vsegment_id,
              (*result)[1].replica.vsegment_id);
}

TEST(VSegmentServiceTest, PutStartDoesNotWaitForJournalDurability) {
    std::atomic<bool> allow_write{true};
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{},
        [&](const OpLogBatchRecord&, const DurablePrefix&) {
            while (!allow_write.load()) std::this_thread::yield();
            return ErrorCode::OK;
        });
    writer.Start();

    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    auto committer =
        std::make_shared<OrderedOpLogVSegmentCommitter>(&writer);
    ASSERT_EQ(service.AddPartition("partition-1", 1, std::move(committer)),
              ErrorCode::OK);

    allow_write = false;
    auto started = std::async(std::launch::async, [&] {
        return service.StartPutReplicasOwned("partition-1", {"put-1"}, 64);
    });
    const auto status = started.wait_for(std::chrono::seconds(2));
    allow_write = true;
    EXPECT_EQ(status, std::future_status::ready);
    auto result = started.get();
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    ASSERT_EQ(result->size(), 1u);
    writer.Stop();
}

TEST(VSegmentServiceTest, OrdinaryPutSelectsConfiguredProfile) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "profile-selected-object";
    const std::string partition_id =
        std::to_string(cvm::KeySlot(tenant, key));
    auto alternate = Profile();
    alternate.name = "alternate";
    PartitionPhysicalQuotaSnapshot quota;
    quota.config_generation = 1;
    quota.policy_digest = "policy";
    quota.default_profile = "default";
    quota.profile_specs = {Profile(), alternate};
    quota.quotas = {
        {partition_id, "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}},
        {partition_id, "alternate", "DRAM",
         {{"segment-a", 512, 512}, {"segment-b", 512, 512}}}};
    auto vsegments = std::make_shared<VSegmentService>(quota);
    ASSERT_EQ(vsegments->AddPartition(partition_id, 1, Committer()),
              ErrorCode::OK);
    MasterService master;
    master.SetVSegmentService(vsegments);
    ReplicateConfig config;
    config.vsegment_profile_name = "alternate";
    const UUID client = generate_uuid();

    tl::expected<std::optional<PutStartResult>, ErrorCode> started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = master.TryVSegmentPutStart(client, key, tenant, 64, config);
        if (started && started->has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started && started->has_value());
    const auto states = vsegments->SnapshotAllPartitions();
    ASSERT_EQ(states.size(), 1u);
    ASSERT_EQ(states.front().vsegments.size(), 2u);
    EXPECT_EQ(std::count_if(states.front().vsegments.begin(),
                            states.front().vsegments.end(),
                            [](const auto& state) {
                                return state.profile_name == "alternate";
                            }),
              1);
}

// ---- 自动发现：BuildDiscoveredQuotaPlan ----

TEST(BuildDiscoveredQuotaPlanTest, AcceptsNonExclusiveSegmentsByDefault) {
    // 非独占 segment 默认接受：按 [used_bytes, capacity) 切分。
    // used_bytes=0（空集群）时等同独占，base_offset=0。
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg_a;
    seg_a.segment_id = "seg-a";
    seg_a.capacity = 4096;
    seg_a.te_endpoint = "host-a:1234";
    seg_a.host_id = "host-a";
    seg_a.medium = "DRAM";
    seg_a.io_alignment = 8;
    seg_a.vsegment_exclusive = false;  // 非独占，默认接受
    seg_a.used_bytes = 0;              // 空集群
    cvm::SegmentDescriptor seg_b = seg_a;
    seg_b.segment_id = "seg-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {seg_a, seg_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount_a, mount_b;
    mount_a.segment_id = "seg-a";
    mount_b.segment_id = "seg-b";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount_a}, {"master-a", mount_b}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    ASSERT_EQ(request.segments.size(), 2u);
    // used_bytes=0 → base_offset=0，capacity=全部可用。
    EXPECT_EQ(request.segments[0].base_offset, 0u);
    EXPECT_EQ(request.segments[0].capacity, 4096u);
}

TEST(BuildDiscoveredQuotaPlanTest, AcceptsExclusiveSegmentsAndReadsRealFacts) {
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg_a;
    seg_a.segment_id = "seg-a";
    seg_a.capacity = 4096;
    seg_a.te_endpoint = "host-a:1234";
    seg_a.host_id = "host-a";
    seg_a.medium = "DRAM";
    seg_a.io_alignment = 8;
    seg_a.supports_unaligned_io = true;
    seg_a.failure_domain = "rack-1";
    seg_a.vsegment_exclusive = true;
    seg_a.used_bytes = 0;  // 独占必须 used_bytes=0
    cvm::SegmentDescriptor seg_b = seg_a;
    seg_b.segment_id = "seg-b";
    seg_b.host_id = "host-b";
    seg_b.failure_domain.clear();  // 验证 host_id 兜底

    std::vector<cvm::SegmentDescriptor> descriptors = {seg_a, seg_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount_a, mount_b;
    mount_a.segment_id = "seg-a";
    mount_b.segment_id = "seg-b";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount_a}, {"master-a", mount_b}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    ASSERT_EQ(request.segments.size(), 2u);
    // 资源事实从 SegmentDescriptor 读取，不再硬编码。
    EXPECT_EQ(request.segments[0].medium, "DRAM");
    EXPECT_EQ(request.segments[0].io_alignment, 8u);
    EXPECT_EQ(request.segments[0].failure_domain, "rack-1");
    EXPECT_EQ(request.segments[1].failure_domain, "host-b");  // host_id 兜底
    EXPECT_TRUE(request.segments[0].supports_unaligned_io);
    // 独占 + used_bytes=0 → base_offset=0，capacity=全部可用。
    EXPECT_EQ(request.segments[0].base_offset, 0u);
    EXPECT_EQ(request.segments[0].capacity, 4096u);
    // Partition 列表由 KV PT 生成（slot 数）。
    EXPECT_EQ(request.partition_ids.size(), cvm::SlotCount());
    EXPECT_EQ(request.profile_specs.size(), 1u);
    EXPECT_EQ(request.profile_specs[0].member_count, 2u);
}

TEST(BuildDiscoveredQuotaPlanTest, RejectsEmptyMediumFromAllocator) {
    VSegmentUserPolicy policy;
    policy.member_count = 1;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;

    cvm::SegmentDescriptor seg;
    seg.segment_id = "seg-a";
    seg.capacity = 4096;
    seg.te_endpoint = "host-a:1234";
    seg.medium = "";  // allocator 未上报 → 拒绝
    seg.vsegment_exclusive = true;

    std::vector<cvm::SegmentDescriptor> descriptors = {seg};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount;
    mount.segment_id = "seg-a";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    EXPECT_EQ(error, ErrorCode::INVALID_PARAMS);
    EXPECT_NE(detail.find("empty medium"), std::string::npos);
}

TEST(BuildDiscoveredQuotaPlanTest,
     AcceptsNonExclusiveWithUsedBytesAndSlicesCorrectly) {
    // 非独占 + used_bytes>0 → 按 [used_bytes, capacity) 切分，
    // base_offset=used_bytes，capacity=available。
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg_a;
    seg_a.segment_id = "seg-a";
    seg_a.capacity = 8192;
    seg_a.te_endpoint = "host-a:1234";
    seg_a.host_id = "host-a";
    seg_a.medium = "DRAM";
    seg_a.io_alignment = 8;
    seg_a.vsegment_exclusive = false;
    seg_a.used_bytes = 4096;  // 前半段已被其他分配器占用
    cvm::SegmentDescriptor seg_b = seg_a;
    seg_b.segment_id = "seg-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {seg_a, seg_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount_a, mount_b;
    mount_a.segment_id = "seg-a";
    mount_b.segment_id = "seg-b";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount_a}, {"master-a", mount_b}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    ASSERT_EQ(request.segments.size(), 2u);
    // base_offset=used_bytes，capacity=available=capacity-used_bytes。
    EXPECT_EQ(request.segments[0].base_offset, 4096u);
    EXPECT_EQ(request.segments[0].capacity, 4096u);  // 8192 - 4096
}

TEST(BuildDiscoveredQuotaPlanTest, RejectsExclusiveWithNonZeroUsedBytes) {
    // exclusive=true 但 used_bytes>0 → 声明与实际不符，拒绝。
    VSegmentUserPolicy policy;
    policy.member_count = 1;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg;
    seg.segment_id = "seg-a";
    seg.capacity = 8192;
    seg.te_endpoint = "host-a:1234";
    seg.medium = "DRAM";
    seg.vsegment_exclusive = true;
    seg.used_bytes = 100;  // 声明独占但有占用 → 拒绝

    std::vector<cvm::SegmentDescriptor> descriptors = {seg};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount;
    mount.segment_id = "seg-a";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    EXPECT_EQ(error, ErrorCode::INVALID_PARAMS);
    EXPECT_NE(detail.find("vsegment_exclusive=true but used_bytes"), std::string::npos);
}

TEST(BuildDiscoveredQuotaPlanTest, SkipsSegmentsWithInsufficientFreeSpace) {
    // available < member_extent_size → 跳过该 segment。
    VSegmentUserPolicy policy;
    policy.member_count = 1;
    policy.stripe_size = 64;
    policy.member_extent_size = 512;  // 需要至少 512 字节可用
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg;
    seg.segment_id = "seg-a";
    seg.capacity = 4096;
    seg.te_endpoint = "host-a:1234";
    seg.medium = "DRAM";
    seg.vsegment_exclusive = false;
    seg.used_bytes = 3900;  // available=196 < 512 → 跳过

    std::vector<cvm::SegmentDescriptor> descriptors = {seg};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount;
    mount.segment_id = "seg-a";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount}};

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    EXPECT_EQ(error, ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    EXPECT_NE(detail.find("insufficient"), std::string::npos);
    EXPECT_NE(detail.find("seg-a"), std::string::npos);
}

TEST(BuildDiscoveredQuotaPlanTest, AllowNonExclusiveIsDeprecatedNoop) {
    // allow_non_exclusive 现在是 no-op：非独占 segment 默认接受，
    // 传 true/false 行为一致。
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium = "DRAM";

    cvm::SegmentDescriptor seg_a;
    seg_a.segment_id = "seg-a";
    seg_a.capacity = 4096;
    seg_a.te_endpoint = "host-a:1234";
    seg_a.host_id = "host-a";
    seg_a.medium = "DRAM";
    seg_a.io_alignment = 8;
    seg_a.vsegment_exclusive = false;
    cvm::SegmentDescriptor seg_b = seg_a;
    seg_b.segment_id = "seg-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {seg_a, seg_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    cvm::MountEntry mount_a, mount_b;
    mount_a.segment_id = "seg-a";
    mount_b.segment_id = "seg-b";
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", mount_a}, {"master-a", mount_b}};

    // 不传 allow_non_exclusive（默认 false）→ 接受。
    {
        PartitionQuotaPlanRequest request;
        std::string detail;
        auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters,
                                              mounts, &request, &detail);
        ASSERT_EQ(error, ErrorCode::OK) << detail;
        EXPECT_EQ(request.segments.size(), 2u);
    }
    // 传 allow_non_exclusive=true → 行为一致。
    {
        PartitionQuotaPlanRequest request;
        std::string detail;
        auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters,
                                              mounts, &request, &detail,
                                              /*allow_non_exclusive=*/true);
        ASSERT_EQ(error, ErrorCode::OK) << detail;
        EXPECT_EQ(request.segments.size(), 2u);
    }
}

// auto 模式：required_medium 为空时，系统自动发现所有介质，为每种介质
// 建独立 profile。混合介质集群（DRAM + NVMe）无需用户手动配置 required_medium。
TEST(BuildDiscoveredQuotaPlanTest, AutoMediumModeBuildsProfilePerMedium) {
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium.clear();  // auto 模式

    cvm::SegmentDescriptor dram_a;
    dram_a.segment_id = "dram-a";
    dram_a.capacity = 4096;
    dram_a.te_endpoint = "host-a:1234";
    dram_a.host_id = "host-a";
    dram_a.medium = "DRAM";
    dram_a.io_alignment = 8;
    dram_a.used_bytes = 0;
    cvm::SegmentDescriptor dram_b = dram_a;
    dram_b.segment_id = "dram-b";
    cvm::SegmentDescriptor nvme_a = dram_a;
    nvme_a.segment_id = "nvme-a";
    nvme_a.medium = "NVMe";
    cvm::SegmentDescriptor nvme_b = nvme_a;
    nvme_b.segment_id = "nvme-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {dram_a, dram_b,
                                                       nvme_a, nvme_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", {{"dram-a"}, {}, {}}},
        {"master-a", {{"dram-b"}, {}, {}}},
        {"master-a", {{"nvme-a"}, {}, {}}},
        {"master-a", {{"nvme-b"}, {}, {}}},
    };

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    // 两种介质各 2 个 segment，都 >= member_count=2，都建 profile。
    ASSERT_EQ(request.segments.size(), 4u);
    ASSERT_EQ(request.profile_specs.size(), 2u);
    // profile 名格式：profile_name-medium
    EXPECT_EQ(request.profile_specs[0].name, "default-DRAM");
    EXPECT_EQ(request.profile_specs[0].required_medium, "DRAM");
    EXPECT_EQ(request.profile_specs[1].name, "default-NVMe");
    EXPECT_EQ(request.profile_specs[1].required_medium, "NVMe");
    // default_profile 选字典序首个介质对应的 profile。
    EXPECT_EQ(request.default_profile, "default-DRAM");
    // 每个 profile 继承用户配的 3 个核心参数。
    for (const auto& profile : request.profile_specs) {
        EXPECT_EQ(profile.member_count, 2u);
        EXPECT_EQ(profile.stripe_size, 64u);
        EXPECT_EQ(profile.member_extent_size, 256u);
    }

    // Plan 应成功，每种介质各建一套 quota。
    PartitionQuotaPlanner planner;
    auto result = planner.Plan(request);
    ASSERT_EQ(result.error, ErrorCode::OK) << result.detail;
    // 每个 partition 有 2 个 quota（DRAM 一个、NVMe 一个）。
    // quotas.size() == 2 * SlotCount()。
    EXPECT_EQ(result.snapshot.quotas.size(),
              request.partition_ids.size() * 2);
}

// auto 模式：介质 segment 数 < member_count 时跳过该介质。
TEST(BuildDiscoveredQuotaPlanTest, AutoMediumModeSkipsInsufficientMedium) {
    VSegmentUserPolicy policy;
    policy.member_count = 2;
    policy.stripe_size = 64;
    policy.member_extent_size = 256;
    policy.required_medium.clear();  // auto 模式

    cvm::SegmentDescriptor seg;
    seg.capacity = 4096;
    seg.te_endpoint = "host-a:1234";
    seg.host_id = "host-a";
    seg.io_alignment = 8;
    seg.used_bytes = 0;
    // DRAM 只有 1 个（不足 member_count=2），NVMe 有 2 个（足够）。
    cvm::SegmentDescriptor dram_a = seg;
    dram_a.segment_id = "dram-a";
    dram_a.medium = "DRAM";
    cvm::SegmentDescriptor nvme_a = seg;
    nvme_a.segment_id = "nvme-a";
    nvme_a.medium = "NVMe";
    cvm::SegmentDescriptor nvme_b = nvme_a;
    nvme_b.segment_id = "nvme-b";

    std::vector<cvm::SegmentDescriptor> descriptors = {dram_a, nvme_a, nvme_b};
    cvm::MasterRegistration master;
    master.master_id = "master-a";
    std::vector<cvm::MasterRegistration> masters = {master};
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts = {
        {"master-a", {{"dram-a"}, {}, {}}},
        {"master-a", {{"nvme-a"}, {}, {}}},
        {"master-a", {{"nvme-b"}, {}, {}}},
    };

    PartitionQuotaPlanRequest request;
    std::string detail;
    auto error = BuildDiscoveredQuotaPlan(policy, descriptors, masters, mounts,
                                          &request, &detail);
    ASSERT_EQ(error, ErrorCode::OK) << detail;
    // DRAM 跳过，只保留 NVMe 的 2 个 segment。
    ASSERT_EQ(request.segments.size(), 2u);
    ASSERT_EQ(request.profile_specs.size(), 1u);
    EXPECT_EQ(request.profile_specs[0].required_medium, "NVMe");
    EXPECT_EQ(request.default_profile, "default-NVMe");
}

}  // namespace
}  // namespace mooncake::vsegment
