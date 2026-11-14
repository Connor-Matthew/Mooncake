#include "ha/snapshot/batch_oplog/codec.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "vsegment/vsegment_manager.h"

namespace mooncake::test {

namespace {

struct LegacyStandbyObjectMetadata {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<Replica::Descriptor> replicas;
    std::string group_id;
    ObjectDataType data_type{ObjectDataType::UNKNOWN};

    YLT_REFL(LegacyStandbyObjectMetadata, client_id, size, replicas, group_id,
             data_type);
};

struct LegacyStandbyObjectEntry {
    std::string tenant_id{"default"};
    std::string key;
    LegacyStandbyObjectMetadata metadata;

    YLT_REFL(LegacyStandbyObjectEntry, tenant_id, key, metadata);
};

struct LegacyBatchOpLogSnapshotObjectChunk {
    uint64_t chunk_index{0};
    std::vector<LegacyStandbyObjectEntry> objects;

    YLT_REFL(LegacyBatchOpLogSnapshotObjectChunk, chunk_index, objects);
};

}  // namespace

TEST(BatchOpLogSnapshotCodecTest, SegmentsRoundTrip) {
    std::vector<StandbySegmentInfo> segments{{
        .segment_name = "segment-1",
        .transport_endpoint = "127.0.0.1:12345",
        .capacity = 4096,
        .is_memory_segment = true,
        .file_path = "",
    }};

    auto decoded = DecodeBatchOpLogSnapshotSegments(
        EncodeBatchOpLogSnapshotSegments(segments));

    ASSERT_TRUE(decoded) << decoded.error();
    ASSERT_EQ(1u, decoded->size());
    EXPECT_EQ("segment-1", decoded->front().segment_name);
    EXPECT_EQ("127.0.0.1:12345", decoded->front().transport_endpoint);
    EXPECT_EQ(4096u, decoded->front().capacity);
    EXPECT_TRUE(decoded->front().is_memory_segment);
}

TEST(BatchOpLogSnapshotCodecTest, ObjectChunkRoundTripsAndChecksEnvelope) {
    StandbyObjectMetadata metadata;
    metadata.client_id = {1, 2};
    metadata.size = 8192;
    metadata.group_id = "group";
    metadata.hard_pinned = true;
    metadata.session_id = "session-snapshot";
    metadata.object_checksum = 0;
    metadata.residency_affinity_id = "snapshot-affinity";
    std::vector<StandbyObjectEntry> objects{
        {.tenant_id = "tenant", .key = "key", .metadata = metadata}};
    auto encoded = EncodeBatchOpLogSnapshotObjectChunk(7, std::move(objects));

    auto decoded = DecodeBatchOpLogSnapshotObjectChunk(encoded, 7, 1);

    ASSERT_TRUE(decoded) << decoded.error();
    EXPECT_EQ(7u, decoded->chunk_index);
    ASSERT_EQ(1u, decoded->objects.size());
    EXPECT_EQ("tenant", decoded->objects[0].tenant_id);
    EXPECT_EQ("key", decoded->objects[0].key);
    EXPECT_EQ(UUID(1, 2), decoded->objects[0].metadata.client_id);
    EXPECT_EQ(8192u, decoded->objects[0].metadata.size);
    EXPECT_EQ("group", decoded->objects[0].metadata.group_id);
    EXPECT_TRUE(decoded->objects[0].metadata.hard_pinned.value_or(false));
    EXPECT_EQ(decoded->objects[0].metadata.session_id.value_or(""),
              "session-snapshot");
    ASSERT_TRUE(decoded->objects[0].metadata.object_checksum.has_value());
    EXPECT_EQ(decoded->objects[0].metadata.object_checksum.value(), 0U);
    EXPECT_EQ(decoded->objects[0].metadata.residency_affinity_id.value_or(""),
              "snapshot-affinity");

    EXPECT_FALSE(DecodeBatchOpLogSnapshotObjectChunk(encoded, 8, 1));
    EXPECT_FALSE(DecodeBatchOpLogSnapshotObjectChunk(encoded, 7, 300000000));
}

TEST(BatchOpLogSnapshotCodecTest, OldObjectChunkDefaultsHardPinnedToFalse) {
    LegacyBatchOpLogSnapshotObjectChunk old_chunk{
        0, {{"tenant", "key", {{1, 2}, 8192, {}, "group", {}}}}};
    auto old_encoded = struct_pack::serialize(old_chunk);
    std::vector<uint8_t> encoded(old_encoded.begin(), old_encoded.end());

    auto decoded = DecodeBatchOpLogSnapshotObjectChunk(encoded, 0, 1);

    ASSERT_TRUE(decoded) << decoded.error();
    EXPECT_FALSE(decoded->objects[0].metadata.hard_pinned.value_or(false));
    EXPECT_FALSE(decoded->objects[0].metadata.session_id.has_value());
    EXPECT_FALSE(decoded->objects[0].metadata.object_checksum.has_value());
}

TEST(BatchOpLogSnapshotCodecTest, RejectsTruncatedArtifacts) {
    auto segments = EncodeBatchOpLogSnapshotSegments({});
    auto objects = EncodeBatchOpLogSnapshotObjectChunk(
        0, std::vector<StandbyObjectEntry>{
               {.tenant_id = "default", .key = "key", .metadata = {}}});
    segments.pop_back();
    objects.pop_back();

    EXPECT_FALSE(DecodeBatchOpLogSnapshotSegments(segments));
    EXPECT_FALSE(DecodeBatchOpLogSnapshotObjectChunk(objects, 0, 1));
}

TEST(BatchOpLogSnapshotCodecTest, VSegmentPartitionsRoundTrip) {
    // The empty partition set is the "no vsegment state" baseline.
    auto decoded_empty = DecodeBatchOpLogSnapshotVSegmentPartitions(
        EncodeBatchOpLogSnapshotVSegmentPartitions(
            std::vector<vsegment::PartitionVSegmentSnapshot>{}));
    ASSERT_TRUE(decoded_empty) << decoded_empty.error();
    EXPECT_TRUE(decoded_empty->empty());

    vsegment::VSegmentStateSnapshot vsegment_state;
    vsegment_state.profile_name = "profile-a";
    vsegment_state.lifecycle = vsegment::Lifecycle::DRAINING;
    vsegment_state.view.vsegment_id = "vsegment-1";
    vsegment_state.view.partition_id = "partition-1";
    vsegment_state.view.stripe_size = 4096;
    vsegment_state.view.logical_capacity = 1 << 20;
    vsegment_state.view.members = {{.segment_id = "psegment-1",
                                    .base_offset = 0,
                                    .length = 1 << 20}};
    vsegment_state.view.checksum = 7;
    vsegment_state.logical_allocation.logical_capacity = 1 << 20;
    vsegment_state.logical_allocation.free_ranges = {
        {.offset = 1 << 20, .length = 1 << 20}};

    vsegment::PartitionVSegmentSnapshot partition;
    partition.partition_id = "partition-1";
    partition.config_generation = 3;
    partition.route_epoch = 11;
    partition.metadata_revision = 7;
    partition.vsegments = {vsegment_state};
    partition.operation_vsegments = {{"put-1", "vsegment-1"}};

    vsegment::PartitionVSegmentSnapshot second;
    second.partition_id = "partition-2";
    second.route_epoch = 2;
    second.metadata_revision = 1;
    second.operation_vsegments = {{"put-2", "vsegment-2"}};

    auto decoded = DecodeBatchOpLogSnapshotVSegmentPartitions(
        EncodeBatchOpLogSnapshotVSegmentPartitions({partition, second}));

    ASSERT_TRUE(decoded) << decoded.error();
    ASSERT_EQ(2u, decoded->size());

    const auto& first_decoded = (*decoded)[0];
    EXPECT_EQ("partition-1", first_decoded.partition_id);
    EXPECT_EQ(3u, first_decoded.config_generation);
    EXPECT_EQ(11u, first_decoded.route_epoch);
    EXPECT_EQ(7u, first_decoded.metadata_revision);
    ASSERT_EQ(1u, first_decoded.vsegments.size());
    const auto& state_decoded = first_decoded.vsegments[0];
    EXPECT_EQ("profile-a", state_decoded.profile_name);
    EXPECT_EQ(vsegment::Lifecycle::DRAINING, state_decoded.lifecycle);
    EXPECT_EQ("vsegment-1", state_decoded.view.vsegment_id);
    EXPECT_EQ("partition-1", state_decoded.view.partition_id);
    EXPECT_EQ(4096u, state_decoded.view.stripe_size);
    EXPECT_EQ(1u << 20, state_decoded.view.logical_capacity);
    ASSERT_EQ(1u, state_decoded.view.members.size());
    EXPECT_EQ("psegment-1", state_decoded.view.members[0].segment_id);
    EXPECT_EQ(1u << 20, state_decoded.view.members[0].length);
    EXPECT_EQ(7u, state_decoded.view.checksum);
    EXPECT_EQ(1u << 20, state_decoded.logical_allocation.logical_capacity);
    ASSERT_EQ(1u, state_decoded.logical_allocation.free_ranges.size());
    EXPECT_EQ(1u << 20,
              state_decoded.logical_allocation.free_ranges[0].offset);
    EXPECT_EQ(1u, first_decoded.operation_vsegments.size());
    EXPECT_EQ("vsegment-1",
              first_decoded.operation_vsegments.at("put-1"));

    EXPECT_EQ("partition-2", (*decoded)[1].partition_id);
    EXPECT_EQ(2u, (*decoded)[1].route_epoch);
    EXPECT_EQ(1u, (*decoded)[1].metadata_revision);
    EXPECT_TRUE((*decoded)[1].vsegments.empty());
    EXPECT_EQ("vsegment-2", (*decoded)[1].operation_vsegments.at("put-2"));

    auto truncated =
        EncodeBatchOpLogSnapshotVSegmentPartitions({partition, second});
    truncated.pop_back();
    EXPECT_FALSE(DecodeBatchOpLogSnapshotVSegmentPartitions(truncated));
}

}  // namespace mooncake::test
