#include "vsegment/vsegment_transfer.h"

#include <algorithm>
#include <limits>
#include <string>
#include <unordered_map>

namespace mooncake::vsegment {

VSegmentTransferPlan VSegmentTransferPlanner::Plan(
    const VSegmentDescriptor& replica,
    const std::vector<ClientSlice>& slices,
    const std::vector<VSegmentLayoutEntry>* layouts) const {
    VSegmentTransferPlan plan;
    if (replica.partition_id.empty() || replica.vsegment_id.empty() ||
        replica.length == 0) {
        plan.error = ErrorCode::INVALID_PARAMS;
        plan.detail = "invalid vsegment transfer planner input";
        return plan;
    }
    if (layouts == nullptr) {
        // Hard C: no table, no transfer. The caller must pass the table that
        // came with the descriptor response.
        plan.error = ErrorCode::SEGMENT_NOT_FOUND;
        plan.detail = "inline layout table is required but absent";
        return plan;
    }

    auto it = std::find_if(
        layouts->begin(), layouts->end(), [&](const VSegmentLayoutEntry& entry) {
            return entry.vsegment_id == replica.vsegment_id;
        });
    if (it == layouts->end()) {
        plan.error = ErrorCode::SEGMENT_NOT_FOUND;
        plan.detail = "inline layout table has no entry for " +
                      replica.vsegment_id;
        return plan;
    }
    if (it->partition_id != replica.partition_id) {
        plan.error = ErrorCode::INVALID_VERSION;
        plan.detail = "inline layout entry belongs to another Partition";
        return plan;
    }

    VSegmentView view;
    view.vsegment_id = it->vsegment_id;
    view.partition_id = it->partition_id;
    view.mapping_algorithm =
        static_cast<MappingAlgorithm>(it->mapping_algorithm);
    view.stripe_size = it->stripe_size;
    view.logical_capacity = it->logical_capacity;
    view.checksum = it->checksum;
    view.members.reserve(it->members.size());
    std::unordered_map<std::string, std::pair<std::string, uint64_t>>
        member_locations;
    for (const auto& member : it->members) {
        view.members.push_back(PSegmentExtent{member.segment_id,
                                              member.base_offset,
                                              member.length});
        if (member.endpoint.empty()) {
            // Unmounted segment (e.g. standby recovery): hard C has no
            // endpoint RPC to fall back to.
            plan.error = ErrorCode::SEGMENT_NOT_FOUND;
            plan.detail = "inline layout member has no endpoint: " +
                          member.segment_id;
            return plan;
        }
        member_locations.emplace(member.segment_id,
                                 std::make_pair(member.endpoint,
                                                member.base_address));
    }
    std::string validation_detail;
    if (ValidateViewStructure(view, &validation_detail) != ErrorCode::OK) {
        plan.error = ErrorCode::CHECKSUM_MISMATCH;
        plan.detail = "inline layout view is invalid: " + validation_detail;
        return plan;
    }

    auto resolved = ResolveTransfer(view, replica.logical_offset,
                                    replica.length, slices);
    if (!resolved) return {resolved.error, {}, std::move(resolved.detail)};

    // Resolve each distinct psegment only once per Plan: the sub-request count
    // grows with (length / stripe_size) while distinct segments are bounded by
    // member_count.
    std::unordered_map<std::string, std::pair<std::string, uint64_t>>
        endpoints;
    for (auto& request : resolved.requests) {
        auto [entry, inserted] = endpoints.try_emplace(request.segment_id);
        if (inserted) {
            auto found = member_locations.find(request.segment_id);
            if (found == member_locations.end()) {
                plan.error = ErrorCode::SEGMENT_NOT_FOUND;
                plan.detail =
                    "inline layout missing endpoint for " + request.segment_id;
                plan.requests.clear();
                return plan;
            }
            entry->second = found->second;
        }
        const auto& [endpoint, base_address] = entry->second;
        if (request.physical_offset >
            std::numeric_limits<uint64_t>::max() - base_address) {
            plan.error = ErrorCode::INVALID_PARAMS;
            plan.detail = "psegment target address overflows";
            plan.requests.clear();
            return plan;
        }
        request.physical_offset += base_address;
        // NOTE: endpoint is shared by every sub-request of this segment, so it
        // must be copied rather than moved.
        plan.requests.push_back({std::move(request), endpoint});
    }
    return plan;
}

}  // namespace mooncake::vsegment
