#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "ha/oplog/oplog_batch_storage.h"
#include "ha/standby_metadata_store.h"
#include "metadata_store.h"
#include "types.h"

namespace mooncake {

inline constexpr size_t kDefaultBatchOpLogPromotionChunkObjects = 1'000'000;

struct BatchOpLogPromotionHandoff {
    std::unique_ptr<StandbyMetadataStore> metadata_store;
    std::vector<StandbySegmentInfo> segments;
    DurablePrefix applied_cursor;
    ViewVersionId producer_view_version{0};
    ReplicaID max_replica_id{0};
    // Partition VSegment state at applied_cursor, handed to the new primary's
    // recovery (MasterService::recovered_vsegment_snapshots_).
    std::vector<vsegment::PartitionVSegmentSnapshot> vsegment_partitions{};
};

}  // namespace mooncake
