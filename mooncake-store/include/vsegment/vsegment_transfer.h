#pragma once

#include <string>
#include <vector>

#include "replica.h"
#include "rpc_types.h"
#include "vsegment/vsegment_runtime.h"

namespace mooncake::vsegment {

// Hard C: the runtime-resolved layout table carried by the RPC response is the
// ONLY source of view + endpoint information. There is no view cache, no
// provider, no endpoint resolver and no fallback path: a missing or invalid
// entry makes the transfer fail.

struct EndpointTransferSubRequest {
    TransferSubRequest transfer;
    std::string endpoint;
};

struct VSegmentTransferPlan {
    ErrorCode error{ErrorCode::OK};
    std::vector<EndpointTransferSubRequest> requests;
    std::string detail;
    explicit operator bool() const { return error == ErrorCode::OK; }
};

class VSegmentTransferPlanner {
   public:
    VSegmentTransferPlan Plan(
        const VSegmentDescriptor& replica,
        const std::vector<ClientSlice>& slices,
        const std::vector<VSegmentLayoutEntry>* layouts) const;
};

}  // namespace mooncake::vsegment
