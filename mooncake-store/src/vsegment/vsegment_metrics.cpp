#include "vsegment/vsegment_metrics.h"

#include <iomanip>
#include <sstream>

namespace mooncake::vsegment {

VSegmentMetrics& VSegmentMetrics::Instance() {
    static VSegmentMetrics metrics;
    return metrics;
}

void VSegmentMetrics::SetGauges(const VSegmentGaugeValues& values) {
    quota_bytes_.store(values.quota_bytes);
    total_capacity_bytes_.store(values.total_capacity_bytes);
    allocated_bytes_.store(values.allocated_bytes);
    reserved_bytes_.store(values.reserved_bytes);
    available_bytes_.store(values.available_bytes);
    formed_capacity_bytes_.store(values.formed_capacity_bytes);
    formed_free_bytes_.store(values.formed_free_bytes);
    reservations_.store(values.reservations);
    committed_allocations_.store(values.committed_allocations);
    pending_creations_.store(values.pending_creations);
}

double VSegmentMetrics::used_ratio() const {
    const auto capacity = total_capacity_bytes_.load();
    if (capacity == 0) return 0.0;
    return static_cast<double>(allocated_bytes_.load()) /
           static_cast<double>(capacity);
}

std::string VSegmentMetrics::Serialize() const {
    std::ostringstream out;
    out << std::fixed << std::setprecision(6);
    out << "# HELP mooncake_vsegment_quota_bytes Configured physical quota "
           "assigned to the VSegments owned by this SubMaster\n"
        << "# TYPE mooncake_vsegment_quota_bytes gauge\n"
        << "mooncake_vsegment_quota_bytes " << quota_bytes_.load() << '\n'
        << "# HELP mooncake_vsegment_total_capacity_bytes Largest logical "
           "capacity that can be formed into VSegments from that quota "
           "(extent and member-count remainders excluded)\n"
        << "# TYPE mooncake_vsegment_total_capacity_bytes gauge\n"
        << "mooncake_vsegment_total_capacity_bytes "
        << total_capacity_bytes_.load() << '\n'
        << "# HELP mooncake_vsegment_allocated_bytes Logical bytes allocated "
           "to objects or reserved by in-flight PutStart\n"
        << "# TYPE mooncake_vsegment_allocated_bytes gauge\n"
        << "mooncake_vsegment_allocated_bytes " << allocated_bytes_.load()
        << '\n'
        << "# HELP mooncake_vsegment_reserved_bytes Logical bytes held by "
           "in-flight PutStart reservations (subset of allocated)\n"
        << "# TYPE mooncake_vsegment_reserved_bytes gauge\n"
        << "mooncake_vsegment_reserved_bytes " << reserved_bytes_.load()
        << '\n'
        << "# HELP mooncake_vsegment_available_bytes Logical bytes still "
           "writable (total_capacity - allocated)\n"
        << "# TYPE mooncake_vsegment_available_bytes gauge\n"
        << "mooncake_vsegment_available_bytes " << available_bytes_.load()
        << '\n'
        << "# HELP mooncake_vsegment_used_ratio Allocated divided by "
           "total_capacity (0..1)\n"
        << "# TYPE mooncake_vsegment_used_ratio gauge\n"
        << "mooncake_vsegment_used_ratio " << used_ratio() << '\n'
        << "# HELP mooncake_vsegment_formed_capacity_bytes Logical capacity of "
           "created (non-retired) VSegments\n"
        << "# TYPE mooncake_vsegment_formed_capacity_bytes gauge\n"
        << "mooncake_vsegment_formed_capacity_bytes "
        << formed_capacity_bytes_.load() << '\n'
        << "# HELP mooncake_vsegment_formed_free_bytes Free logical bytes "
           "inside created (non-retired) VSegments\n"
        << "# TYPE mooncake_vsegment_formed_free_bytes gauge\n"
        << "mooncake_vsegment_formed_free_bytes " << formed_free_bytes_.load()
        << '\n'
        << "# HELP mooncake_vsegment_reservations In-flight PutStart "
           "reservations\n"
        << "# TYPE mooncake_vsegment_reservations gauge\n"
        << "mooncake_vsegment_reservations " << reservations_.load() << '\n'
        << "# HELP mooncake_vsegment_committed_allocations Committed object "
           "allocations not released yet\n"
        << "# TYPE mooncake_vsegment_committed_allocations gauge\n"
        << "mooncake_vsegment_committed_allocations "
        << committed_allocations_.load() << '\n'
        << "# HELP mooncake_vsegment_pending_creations VSegment creations in "
           "flight\n"
        << "# TYPE mooncake_vsegment_pending_creations gauge\n"
        << "mooncake_vsegment_pending_creations "
        << pending_creations_.load() << '\n'
        << "# HELP mooncake_vsegment_create_success_total Successful VSegment "
           "creations\n"
        << "# TYPE mooncake_vsegment_create_success_total counter\n"
        << "mooncake_vsegment_create_success_total " << create_success_.load()
        << '\n'
        << "# HELP mooncake_vsegment_create_failure_total Failed VSegment "
           "creations\n"
        << "# TYPE mooncake_vsegment_create_failure_total counter\n"
        << "mooncake_vsegment_create_failure_total " << create_failure_.load()
        << '\n'
        << "# HELP mooncake_vsegment_release_success_total Successful object "
           "releases\n"
        << "# TYPE mooncake_vsegment_release_success_total counter\n"
        << "mooncake_vsegment_release_success_total "
        << release_success_.load() << '\n'
        << "# HELP mooncake_vsegment_release_failure_total Failed object "
           "releases\n"
        << "# TYPE mooncake_vsegment_release_failure_total counter\n"
        << "mooncake_vsegment_release_failure_total "
        << release_failure_.load() << '\n'
        << "# HELP mooncake_vsegment_persistence_failure_total Failed "
           "VSegment state persistence attempts\n"
        << "# TYPE mooncake_vsegment_persistence_failure_total counter\n"
        << "mooncake_vsegment_persistence_failure_total "
        << persistence_failure_.load() << '\n'
        << "# HELP mooncake_vsegment_recovery_failure_total Failed VSegment "
           "recovery or reconcile attempts\n"
        << "# TYPE mooncake_vsegment_recovery_failure_total counter\n"
        << "mooncake_vsegment_recovery_failure_total "
        << recovery_failure_.load() << '\n'
        << "# HELP mooncake_vsegment_stale_route_total Requests rejected by "
           "route-epoch fencing\n"
        << "# TYPE mooncake_vsegment_stale_route_total counter\n"
        << "mooncake_vsegment_stale_route_total " << stale_route_.load()
        << '\n';
    return out.str();
}

}  // namespace mooncake::vsegment
