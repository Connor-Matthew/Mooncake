#pragma once

#include <cstdint>
#include <string>

#include "types.h"

namespace mooncake {
namespace partition {

// Vsegment ownership reuses the KV Partition identity. Layout and allocation
// types live in vsegment/vsegment.h so there is only one runtime data model.
enum class PartitionState : int32_t {
    kActive = 0,
    kMigrating = 1,
};

struct PartitionIdentity {
    std::string partition_id;
};
YLT_REFL(PartitionIdentity, partition_id);

struct PartitionRoute {
    PartitionIdentity partition_id;
    std::string owner_submaster_id;
    uint64_t route_epoch{0};
    int32_t state{0};  // PartitionState
    std::string target_submaster_id;
};
YLT_REFL(PartitionRoute, partition_id, owner_submaster_id, route_epoch, state,
         target_submaster_id);

}  // namespace partition
}  // namespace mooncake
