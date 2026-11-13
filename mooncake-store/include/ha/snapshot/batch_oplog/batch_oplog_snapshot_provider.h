#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/standby_metadata_store.h"
#include "types.h"
#include "vsegment/vsegment_manager.h"

namespace mooncake {

class HaKvBackend;
class OpLogApplier;
class SnapshotObjectStore;

struct BatchOpLogSnapshotRestoreResult {
    uint64_t last_included_seq{0};
    uint64_t last_included_batch_id{0};
    uint64_t last_applied_seq{0};
    uint64_t last_applied_batch_id{0};
    ViewVersionId producer_view_version{0};
    ReplicaID max_replica_id{0};
    // VSegment partition state at last_applied_seq: the snapshot's v3 section
    // (if any) with the replayed suffix applied on top. The provider owns
    // VSegment replay during restore; callers install their own handler on a
    // supplied applier afterwards.
    std::vector<vsegment::PartitionVSegmentSnapshot> vsegment_partitions{};
};

// Restores the new batch-OpLog snapshot format into caller-owned temporary
// state. Legacy catalog snapshots intentionally do not use this provider.
class BatchOpLogSnapshotProvider final {
   public:
    BatchOpLogSnapshotProvider(std::string cluster_id, HaKvBackend& backend,
                               SnapshotObjectStore& object_store,
                               std::string snapshot_root);

    tl::expected<BatchOpLogSnapshotRestoreResult, ErrorCode> RestoreBaseline(
        StandbyMetadataStore& metadata, StandbySegmentRegistry& registry,
        OpLogApplier* applier = nullptr, uint64_t minimum_snapshot_batch = 0,
        std::function<bool()> cancelled = {});

   private:
    std::string cluster_id_;
    HaKvBackend& backend_;
    SnapshotObjectStore& object_store_;
    std::string snapshot_root_;
};

}  // namespace mooncake
