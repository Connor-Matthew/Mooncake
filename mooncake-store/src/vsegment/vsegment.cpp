#include "vsegment/vsegment.h"

#include <algorithm>
#include <limits>
#include <set>
#include <sstream>

#include "crc32c.h"

namespace mooncake::vsegment {
namespace {

bool AddOverflows(uint64_t left, uint64_t right) {
    return right > std::numeric_limits<uint64_t>::max() - left;
}

ErrorCode Invalid(std::string message, std::string* detail) {
    if (detail) *detail = std::move(message);
    return ErrorCode::INVALID_PARAMS;
}

void AppendUint64(std::string& output, uint64_t value) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
        output.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

void AppendString(std::string& output, const std::string& value) {
    AppendUint64(output, value.size());
    output.append(value);
}

}  // namespace

ErrorCode ValidateProfile(const VSegmentProfile& profile, std::string* detail) {
    if (profile.name.empty()) return Invalid("profile name is empty", detail);
    if (profile.member_count < 2)
        return Invalid("member_count must be at least 2", detail);
    if (profile.stripe_size == 0)
        return Invalid("stripe_size must be positive", detail);
    if (profile.member_extent_size == 0)
        return Invalid("member_extent_size must be positive", detail);
    if (profile.io_alignment == 0)
        return Invalid("io_alignment must be positive", detail);
    if (profile.initial_vsegment_count == 0)
        return Invalid("initial_vsegment_count must be positive", detail);
    if (profile.stripe_size % profile.io_alignment != 0 ||
        profile.member_extent_size % profile.io_alignment != 0) {
        return Invalid("stripe and extent must satisfy io_alignment", detail);
    }
    if (profile.member_extent_size % profile.stripe_size != 0) {
        return Invalid("member_extent_size must be a multiple of stripe_size",
                       detail);
    }
    if (profile.member_extent_size >
        std::numeric_limits<uint64_t>::max() / profile.member_count) {
        return Invalid("profile logical_capacity overflows", detail);
    }
    return ErrorCode::OK;
}

ErrorCode ValidatePartitionConfig(const PartitionVSegmentConfig& config,
                                  std::string* detail) {
    if (config.partition_id.empty())
        return Invalid("partition_id is empty", detail);
    if (config.profile_name.empty())
        return Invalid("profile_name is empty", detail);
    if (config.initial_vsegment_count == 0)
        return Invalid("initial_vsegment_count must be positive", detail);
    if (config.config_generation == 0)
        return Invalid("config_generation must be positive", detail);
    if (config.quotas.empty()) return Invalid("quotas are empty", detail);

    std::map<std::string, std::vector<PartitionPSegmentQuota>> by_segment;
    for (const auto& quota : config.quotas) {
        if (quota.segment_id.empty())
            return Invalid("quota segment_id is empty", detail);
        if (quota.length == 0) return Invalid("quota length is zero", detail);
        if (AddOverflows(quota.base_offset, quota.length))
            return Invalid("quota range overflows", detail);
        by_segment[quota.segment_id].push_back(quota);
    }
    for (auto& [segment, quotas] : by_segment) {
        std::sort(quotas.begin(), quotas.end(), [](const auto& a, const auto& b) {
            return a.base_offset < b.base_offset;
        });
        for (size_t index = 1; index < quotas.size(); ++index) {
            const auto previous_end =
                quotas[index - 1].base_offset + quotas[index - 1].length;
            if (previous_end > quotas[index].base_offset) {
                return Invalid("overlapping quota ranges for " + segment,
                               detail);
            }
        }
    }
    return ErrorCode::OK;
}

ErrorCode ValidatePublishedConfig(
    const std::vector<VSegmentProfile>& profiles,
    const std::vector<PartitionVSegmentConfig>& partitions,
    const std::vector<PSegmentGeometry>& segments,
    const std::unordered_map<std::string, uint64_t>& current_generations,
    std::string* detail) {
    std::unordered_map<std::string, VSegmentProfile> profile_by_name;
    for (const auto& profile : profiles) {
        auto validation = ValidateProfile(profile, detail);
        if (validation != ErrorCode::OK) return validation;
        if (!profile_by_name.emplace(profile.name, profile).second)
            return Invalid("duplicate profile: " + profile.name, detail);
    }
    std::unordered_map<std::string, PSegmentGeometry> segment_by_id;
    for (const auto& segment : segments) {
        if (segment.segment_id.empty() || segment.capacity == 0 ||
            segment.io_alignment == 0)
            return Invalid("invalid psegment geometry", detail);
        if (!segment_by_id.emplace(segment.segment_id, segment).second)
            return Invalid("duplicate psegment geometry", detail);
    }

    struct OwnedQuota {
        std::string partition_id;
        uint64_t offset;
        uint64_t length;
    };
    std::unordered_map<std::string, std::vector<OwnedQuota>> all_quotas;
    std::set<std::pair<std::string, std::string>> partition_profiles;
    for (const auto& partition : partitions) {
        auto validation = ValidatePartitionConfig(partition, detail);
        if (validation != ErrorCode::OK) return validation;
        if (!partition_profiles
                 .emplace(partition.partition_id, partition.profile_name)
                 .second)
            return Invalid("duplicate partition/profile config", detail);
        auto current = current_generations.find(partition.partition_id);
        if (current != current_generations.end() &&
            partition.config_generation <= current->second)
            return Invalid("config_generation is not newer for " +
                               partition.partition_id,
                           detail);
        auto profile = profile_by_name.find(partition.profile_name);
        if (profile == profile_by_name.end())
            return Invalid("unknown profile: " + partition.profile_name,
                           detail);
        const auto& layout = profile->second;
        if (layout.member_extent_size >
            std::numeric_limits<uint64_t>::max() /
                partition.initial_vsegment_count)
            return Invalid("initial allocation size overflows", detail);
        const uint64_t initial_bytes =
            layout.member_extent_size * partition.initial_vsegment_count;
        std::set<std::string> eligible_segments;
        for (const auto& quota : partition.quotas) {
            auto geometry = segment_by_id.find(quota.segment_id);
            if (geometry == segment_by_id.end())
                return Invalid("unknown psegment: " + quota.segment_id,
                               detail);
            const auto& segment = geometry->second;
            if (quota.base_offset + quota.length > segment.capacity)
                return Invalid("quota exceeds psegment capacity", detail);
            const uint64_t alignment =
                std::max(layout.io_alignment, segment.io_alignment);
            if (quota.base_offset % alignment != 0 ||
                quota.length % alignment != 0)
                return Invalid("quota does not satisfy I/O alignment", detail);
            if (!layout.required_medium.empty() &&
                layout.required_medium != segment.medium)
                return Invalid("psegment medium does not match profile",
                               detail);
            if (quota.length >= initial_bytes)
                eligible_segments.insert(quota.segment_id);
            all_quotas[quota.segment_id].push_back(
                {partition.partition_id, quota.base_offset, quota.length});
        }
        if (eligible_segments.size() < layout.member_count)
            return Invalid("Partition cannot satisfy initial vsegment count",
                           detail);
    }
    for (auto& [segment, quotas] : all_quotas) {
        std::sort(quotas.begin(), quotas.end(), [](const auto& a, const auto& b) {
            return a.offset < b.offset;
        });
        for (size_t index = 1; index < quotas.size(); ++index) {
            if (quotas[index - 1].offset + quotas[index - 1].length >
                quotas[index].offset)
                return Invalid("cross-Partition quota overlap on " + segment,
                               detail);
        }
    }
    return ErrorCode::OK;
}

uint32_t ComputeViewChecksum(const VSegmentView& view) {
    std::string bytes;
    AppendString(bytes, view.vsegment_id);
    AppendString(bytes, view.partition_id);
    AppendUint64(bytes, static_cast<uint8_t>(view.mapping_algorithm));
    AppendUint64(bytes, view.stripe_size);
    AppendUint64(bytes, view.logical_capacity);
    AppendUint64(bytes, view.members.size());
    for (const auto& member : view.members) {
        AppendString(bytes, member.segment_id);
        AppendUint64(bytes, member.base_offset);
        AppendUint64(bytes, member.length);
    }
    return Crc32cValue(bytes.data(), bytes.size());
}

ErrorCode ValidateView(const VSegmentView& view,
                       const VSegmentProfile& profile, std::string* detail) {
    auto result = ValidateProfile(profile, detail);
    if (result != ErrorCode::OK) return result;
    result = ValidateViewStructure(view, detail);
    if (result != ErrorCode::OK) return result;
    if (view.vsegment_id.empty() || view.partition_id.empty())
        return Invalid("view identity is empty", detail);
    if (view.stripe_size != profile.stripe_size)
        return Invalid("view stripe_size does not match profile", detail);
    if (view.members.size() != profile.member_count)
        return Invalid("view member count does not match profile", detail);
    if (profile.member_extent_size >
        std::numeric_limits<uint64_t>::max() / profile.member_count) {
        return Invalid("profile logical_capacity overflows", detail);
    }
    const auto expected_capacity =
        profile.member_extent_size * profile.member_count;
    if (view.logical_capacity != expected_capacity) {
        return Invalid("view logical_capacity does not match profile", detail);
    }
    std::set<std::string> member_ids;
    for (const auto& member : view.members) {
        if (member.segment_id.empty() ||
            member.length != profile.member_extent_size ||
            AddOverflows(member.base_offset, member.length)) {
            return Invalid("invalid member extent", detail);
        }
        if (!member_ids.insert(member.segment_id).second)
            return Invalid("duplicate member segment", detail);
    }
    return ErrorCode::OK;
}

ErrorCode ValidateViewStructure(const VSegmentView& view,
                                std::string* detail) {
    if (view.vsegment_id.empty() || view.partition_id.empty())
        return Invalid("view identity is empty", detail);
    if (view.mapping_algorithm != MappingAlgorithm::ROUND_ROBIN)
        return Invalid("unsupported mapping_algorithm", detail);
    if (view.stripe_size == 0 || view.members.empty())
        return Invalid("view layout is empty", detail);
    if (view.logical_capacity % view.members.size() != 0)
        return Invalid("logical capacity is not divisible by members", detail);
    const uint64_t member_length =
        view.logical_capacity / view.members.size();
    if (member_length == 0 || member_length % view.stripe_size != 0)
        return Invalid("member length is incompatible with stripe_size", detail);
    std::set<std::string> ids;
    for (const auto& member : view.members) {
        if (member.segment_id.empty() || member.length != member_length ||
            AddOverflows(member.base_offset, member.length) ||
            !ids.insert(member.segment_id).second) {
            return Invalid("invalid member extent", detail);
        }
    }
    if (view.checksum != ComputeViewChecksum(view))
        return ErrorCode::CHECKSUM_MISMATCH;
    return ErrorCode::OK;
}

ErrorCode ValidateQuotaSnapshot(
    const PartitionPhysicalQuotaSnapshot& snapshot,
    const std::vector<PSegmentGeometry>& segments, std::string* detail) {
    if (snapshot.config_generation == 0)
        return Invalid("quota snapshot generation must be positive", detail);
    if (snapshot.policy_digest.empty())
        return Invalid("quota snapshot policy_digest is empty", detail);
    if (snapshot.default_profile.empty())
        return Invalid("quota snapshot default_profile is empty", detail);

    std::unordered_map<std::string, VSegmentProfileSpec> profiles;
    for (const auto& profile : snapshot.profile_specs) {
        auto result = ValidateProfile(profile, detail);
        if (result != ErrorCode::OK) return result;
        if (!profiles.emplace(profile.name, profile).second)
            return Invalid("duplicate quota snapshot profile", detail);
    }
    if (!profiles.count(snapshot.default_profile))
        return Invalid("default_profile is not present in profile_specs",
                       detail);

    std::unordered_map<std::string, PSegmentGeometry> geometry;
    for (const auto& segment : segments) {
        if (segment.segment_id.empty() || segment.capacity == 0 ||
            segment.io_alignment == 0)
            return Invalid("invalid psegment geometry", detail);
        if (!geometry.emplace(segment.segment_id, segment).second)
            return Invalid("duplicate psegment geometry", detail);
    }

    struct OwnedRange {
        uint64_t offset;
        uint64_t length;
    };
    std::unordered_map<std::string, std::vector<OwnedRange>> ranges;
    std::set<std::pair<std::string, std::string>> quota_keys;
    for (const auto& quota : snapshot.quotas) {
        if (quota.partition_id.empty() || quota.profile_name.empty() ||
            quota.extents.empty())
            return Invalid("invalid empty quota identity or extents", detail);
        if (!quota_keys.emplace(quota.partition_id, quota.profile_name).second)
            return Invalid("duplicate partition/profile quota", detail);
        auto profile = profiles.find(quota.profile_name);
        if (profile == profiles.end())
            return Invalid("quota references unknown profile", detail);
        if (quota.medium != profile->second.required_medium)
            return Invalid("quota medium does not match profile", detail);

        std::set<std::string> members;
        uint32_t eligible = 0;
        if (profile->second.member_extent_size >
            std::numeric_limits<uint64_t>::max() /
                profile->second.initial_vsegment_count)
            return Invalid("initial profile allocation overflows", detail);
        const uint64_t initial_bytes =
            profile->second.member_extent_size *
            profile->second.initial_vsegment_count;
        for (const auto& extent : quota.extents) {
            auto segment = geometry.find(extent.segment_id);
            if (segment == geometry.end())
                return Invalid("quota references unknown psegment", detail);
            if (!segment->second.healthy ||
                !segment->second.supports_unaligned_io)
                return Invalid("quota references ineligible psegment", detail);
            if (segment->second.medium != quota.medium)
                return Invalid("quota psegment medium mismatch", detail);
            if (extent.length == 0 ||
                AddOverflows(extent.base_offset, extent.length) ||
                extent.base_offset + extent.length > segment->second.capacity)
                return Invalid("quota extent exceeds psegment", detail);
            const auto alignment = std::max(profile->second.io_alignment,
                                            segment->second.io_alignment);
            if (extent.base_offset % alignment != 0 ||
                extent.length % alignment != 0)
                return Invalid("quota extent is not aligned", detail);
            if (members.insert(extent.segment_id).second &&
                extent.length >= initial_bytes)
                ++eligible;
            ranges[extent.segment_id].push_back(
                {extent.base_offset, extent.length});
        }
        if (eligible < profile->second.member_count)
            return Invalid("quota cannot create one configured vsegment",
                           detail);
    }
    for (auto& [segment, owned] : ranges) {
        std::sort(owned.begin(), owned.end(), [](const auto& a, const auto& b) {
            return a.offset < b.offset;
        });
        for (size_t i = 1; i < owned.size(); ++i) {
            if (owned[i - 1].offset + owned[i - 1].length > owned[i].offset)
                return Invalid("quota snapshot has overlapping extents on " +
                                   segment,
                               detail);
        }
    }
    return ErrorCode::OK;
}

ErrorCode BuildPartitionConfig(
    const PartitionPhysicalQuotaSnapshot& snapshot,
    const std::string& partition_id, const std::string& profile_name,
    PartitionVSegmentConfig* config, std::string* detail) {
    if (!config) return Invalid("output config is null", detail);
    auto profile = std::find_if(snapshot.profile_specs.begin(),
                                snapshot.profile_specs.end(), [&](const auto& p) {
                                    return p.name == profile_name;
                                });
    if (profile == snapshot.profile_specs.end())
        return Invalid("unknown profile in quota snapshot", detail);
    auto quota = std::find_if(snapshot.quotas.begin(), snapshot.quotas.end(),
                              [&](const auto& q) {
                                  return q.partition_id == partition_id &&
                                         q.profile_name == profile_name;
                              });
    if (quota == snapshot.quotas.end())
        return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
    PartitionVSegmentConfig output;
    output.partition_id = partition_id;
    output.profile_name = profile_name;
    output.initial_vsegment_count = profile->initial_vsegment_count;
    output.config_generation = snapshot.config_generation;
    for (const auto& extent : quota->extents)
        output.quotas.push_back(
            {extent.segment_id, extent.base_offset, extent.length});
    *config = std::move(output);
    return ErrorCode::OK;
}

PartitionQuotaAllocator::PartitionQuotaAllocator(
    PartitionVSegmentConfig config)
    : PartitionQuotaAllocator(
          std::vector<PartitionVSegmentConfig>{std::move(config)}) {}

PartitionQuotaAllocator::PartitionQuotaAllocator(
    std::vector<PartitionVSegmentConfig> configs) {
    for (auto& config : configs) {
        const auto profile_name = config.profile_name;
        for (const auto& quota : config.quotas) {
            InsertAndMerge(free_ranges_[profile_name][quota.segment_id],
                           {quota.base_offset, quota.length});
        }
        configs_.emplace(profile_name, std::move(config));
    }
}

VSegmentAllocationResult PartitionQuotaAllocator::Allocate(
    const std::string& vsegment_id, const VSegmentProfile& profile,
    const std::set<std::string>* excluded_segments) {
    std::lock_guard<std::mutex> lock(mutex_);
    VSegmentAllocationResult result;
    std::string detail;
    auto config = configs_.find(profile.name);
    if (config == configs_.end()) {
        result.error = ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
        result.detail = "Partition has no quota for profile " + profile.name;
        return result;
    }
    if (ValidatePartitionConfig(config->second, &detail) != ErrorCode::OK ||
        ValidateProfile(profile, &detail) != ErrorCode::OK ||
        vsegment_id.empty()) {
        result.error = ErrorCode::INVALID_PARAMS;
        result.detail = vsegment_id.empty() ? "vsegment_id is empty" : detail;
        return result;
    }
    if (allocations_.count(vsegment_id)) {
        result.error = ErrorCode::SEGMENT_ALREADY_EXISTS;
        result.detail = "vsegment already exists";
        return result;
    }

    struct Candidate {
        std::string segment;
        size_t range_index;
        uint64_t offset;
    };
    std::vector<Candidate> candidates;
    std::set<std::string> visited_segments;
    size_t excluded_count = 0;
    auto& profile_ranges = free_ranges_.at(profile.name);
    for (const auto& quota : config->second.quotas) {
        if (!visited_segments.insert(quota.segment_id).second) continue;
        if (excluded_segments != nullptr &&
            excluded_segments->count(quota.segment_id) > 0) {
            ++excluded_count;
            continue;
        }
        const auto& ranges = profile_ranges.at(quota.segment_id);
        auto range = std::find_if(ranges.begin(), ranges.end(), [&](const auto& r) {
            return r.length >= profile.member_extent_size;
        });
        if (range != ranges.end()) {
            candidates.push_back({quota.segment_id,
                                  static_cast<size_t>(range - ranges.begin()),
                                  range->offset});
        }
        if (candidates.size() == profile.member_count) break;
    }
    if (candidates.size() != profile.member_count) {
        result.error = ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
        std::ostringstream message;
        message << "partition " << config->second.partition_id << " requires "
                << profile.member_count << " psegments with "
                << profile.member_extent_size << " free bytes each, but only "
                << candidates.size() << " are available";
        if (excluded_count > 0)
            message << " (" << excluded_count
                    << " excluded by runtime invalid-endpoint reports)";
        result.detail = message.str();
        return result;
    }

    VSegmentView view;
    view.vsegment_id = vsegment_id;
    view.partition_id = config->second.partition_id;
    view.stripe_size = profile.stripe_size;
    view.logical_capacity = profile.member_extent_size * profile.member_count;
    for (const auto& candidate : candidates) {
        auto& ranges = profile_ranges.at(candidate.segment);
        auto& range = ranges[candidate.range_index];
        view.members.push_back(
            {candidate.segment, range.offset, profile.member_extent_size});
        range.offset += profile.member_extent_size;
        range.length -= profile.member_extent_size;
        if (range.length == 0) ranges.erase(ranges.begin() + candidate.range_index);
    }
    view.checksum = ComputeViewChecksum(view);
    allocations_.emplace(vsegment_id,
                         AllocationRecord{profile.name, view});
    result.view = std::move(view);
    return result;
}

ErrorCode PartitionQuotaAllocator::Restore(const VSegmentView& view,
                                           const VSegmentProfile& profile,
                                           std::string* detail) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto config = configs_.find(profile.name);
    if (config == configs_.end())
        return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
    auto validation = ValidateView(view, profile, detail);
    if (validation != ErrorCode::OK) return validation;
    if (view.partition_id != config->second.partition_id)
        return Invalid("view belongs to another partition", detail);
    if (allocations_.count(view.vsegment_id))
        return Invalid("duplicate vsegment in restore state", detail);

    struct Match {
        std::string segment;
        size_t range_index;
        PSegmentExtent extent;
    };
    std::vector<Match> matches;
    for (const auto& member : view.members) {
        auto& profile_ranges = free_ranges_.at(profile.name);
        auto segment = profile_ranges.find(member.segment_id);
        if (segment == profile_ranges.end())
            return Invalid("member is outside Partition quota", detail);
        auto range = std::find_if(
            segment->second.begin(), segment->second.end(), [&](const auto& r) {
                return member.base_offset >= r.offset &&
                       member.base_offset + member.length <= r.offset + r.length;
            });
        if (range == segment->second.end())
            return Invalid("member overlaps another restored view", detail);
        matches.push_back({member.segment_id,
                           static_cast<size_t>(range - segment->second.begin()),
                           member});
    }

    for (const auto& match : matches) {
        auto& ranges = free_ranges_.at(profile.name).at(match.segment);
        const auto original = ranges[match.range_index];
        const auto original_end = original.offset + original.length;
        const auto member_end =
            match.extent.base_offset + match.extent.length;
        ranges.erase(ranges.begin() + match.range_index);
        if (original.offset < match.extent.base_offset) {
            ranges.push_back(
                {original.offset, match.extent.base_offset - original.offset});
        }
        if (member_end < original_end) {
            ranges.push_back({member_end, original_end - member_end});
        }
        std::sort(ranges.begin(), ranges.end(),
                  [](const auto& a, const auto& b) {
                      return a.offset < b.offset;
                  });
    }
    allocations_.emplace(view.vsegment_id,
                         AllocationRecord{profile.name, view});
    return ErrorCode::OK;
}

void PartitionQuotaAllocator::InsertAndMerge(std::vector<Range>& ranges,
                                             Range range) {
    ranges.push_back(range);
    std::sort(ranges.begin(), ranges.end(),
              [](const auto& a, const auto& b) { return a.offset < b.offset; });
    std::vector<Range> merged;
    for (const auto& current : ranges) {
        if (!merged.empty() &&
            merged.back().offset + merged.back().length == current.offset) {
            merged.back().length += current.length;
        } else {
            merged.push_back(current);
        }
    }
    ranges.swap(merged);
}

ErrorCode PartitionQuotaAllocator::Release(const VSegmentView& view) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto allocation = allocations_.find(view.vsegment_id);
    if (allocation == allocations_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
    if (allocation->second.view.checksum != view.checksum)
        return ErrorCode::INVALID_VERSION;
    for (const auto& member : allocation->second.view.members) {
        InsertAndMerge(free_ranges_[allocation->second.profile_name]
                                   [member.segment_id],
                       {member.base_offset, member.length});
    }
    allocations_.erase(allocation);
    return ErrorCode::OK;
}

uint64_t PartitionQuotaAllocator::FreeBytes(
    const std::string& segment_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t total = 0;
    for (const auto& [profile, segments] : free_ranges_) {
        auto ranges = segments.find(segment_id);
        if (ranges == segments.end()) continue;
        for (const auto& range : ranges->second) total += range.length;
    }
    return total;
}

uint64_t PartitionQuotaAllocator::FreeBytes(
    const std::string& profile_name, const std::string& segment_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto profile = free_ranges_.find(profile_name);
    if (profile == free_ranges_.end()) return 0;
    auto ranges = profile->second.find(segment_id);
    if (ranges == profile->second.end()) return 0;
    uint64_t total = 0;
    for (const auto& range : ranges->second) total += range.length;
    return total;
}

}  // namespace mooncake::vsegment
