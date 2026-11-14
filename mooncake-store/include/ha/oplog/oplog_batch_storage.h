#pragma once

#include <string>
#include <vector>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_types.h"
#include "types.h"

namespace mooncake {

class OpLogBatchStorage {
   public:
    // source_id is the stable master id of the submaster owning this OpLog
    // stream. When empty, the legacy cluster-level (single-source) layout is
    // used; when non-empty, keys are namespaced per source.
    OpLogBatchStorage(std::string cluster_id, HaKvBackend& backend,
                      std::string source_id = std::string());

    ErrorCode InitDurablePrefix(DurablePrefix& prefix);
    ErrorCode ClaimProducerView(ViewVersionId producer_view_version);
    ErrorCode ValidateProducerView(ViewVersionId producer_view_version) const;
    ErrorCode ReadProducerView(ViewVersionId& producer_view_version) const;
    ErrorCode ReadCompactionFloor(uint64_t& floor) const;
    ErrorCode ReadDurablePrefix(DurablePrefix& prefix);
    ErrorCode WriteBatchAndAdvancePrefix(const OpLogBatchRecord& batch,
                                         const DurablePrefix& expected_prefix);
    ErrorCode WriteBatchAndAdvancePrefix(const OpLogBatchRecord& batch,
                                         const DurablePrefix& expected_prefix,
                                         ViewVersionId producer_view_version);
    ErrorCode ReadBatch(uint64_t batch_id, OpLogBatchRecord& batch);
    ErrorCode ReadBatchesAfter(uint64_t after_batch_id, size_t limit,
                               std::vector<OpLogBatchRecord>& batches);
    // Idempotently delete records with id <= batch_id in this cluster only.
    // Caller must first publish a reader-visible compaction floor covering
    // this cutoff. Does not choose/validate the cutoff or retry backend errors.
    ErrorCode DeleteBatchesThrough(uint64_t batch_id);

   private:
    bool IsValidClusterId() const;
    ErrorCode WriteBatchAndAdvancePrefixImpl(
        const OpLogBatchRecord& batch, const DurablePrefix& expected_prefix,
        const ViewVersionId* producer_view_version);
    ErrorCode RejectLegacyLayout() const;
    ErrorCode ValidateDurablePrefixAtStartup(const DurablePrefix& prefix);

    std::string cluster_id_;
    std::string source_id_;
    HaKvBackend& backend_;
    bool cluster_id_valid_{false};
};

}  // namespace mooncake
