#include "vsegment/vsegment_metrics.h"

#include <sstream>

namespace mooncake::vsegment {

VSegmentMetrics& VSegmentMetrics::Instance() {
    static VSegmentMetrics metrics;
    return metrics;
}

void VSegmentMetrics::SetCapacity(uint64_t total, uint64_t free,
                                  uint64_t reservations,
                                  uint64_t committed,
                                  uint64_t pending_creations) {
    total_bytes_.store(total);
    free_bytes_.store(free);
    reservations_.store(reservations);
    committed_.store(committed);
    pending_creations_.store(pending_creations);
}

std::string VSegmentMetrics::Serialize() const {
    std::ostringstream out;
    out << "# TYPE mooncake_vsegment_logical_capacity_bytes gauge\n"
        << "mooncake_vsegment_logical_capacity_bytes "
        << total_bytes_.load() << '\n'
        << "# TYPE mooncake_vsegment_logical_free_bytes gauge\n"
        << "mooncake_vsegment_logical_free_bytes " << free_bytes_.load()
        << '\n'
        << "# TYPE mooncake_vsegment_reservations gauge\n"
        << "mooncake_vsegment_reservations " << reservations_.load() << '\n'
        << "# TYPE mooncake_vsegment_committed_allocations gauge\n"
        << "mooncake_vsegment_committed_allocations " << committed_.load()
        << '\n'
        << "# TYPE mooncake_vsegment_pending_creations gauge\n"
        << "mooncake_vsegment_pending_creations "
        << pending_creations_.load() << '\n'
        << "# TYPE mooncake_vsegment_create_success_total counter\n"
        << "mooncake_vsegment_create_success_total " << create_success_.load()
        << '\n'
        << "# TYPE mooncake_vsegment_create_failure_total counter\n"
        << "mooncake_vsegment_create_failure_total " << create_failure_.load()
        << '\n'
        << "# TYPE mooncake_vsegment_release_success_total counter\n"
        << "mooncake_vsegment_release_success_total "
        << release_success_.load() << '\n'
        << "# TYPE mooncake_vsegment_release_failure_total counter\n"
        << "mooncake_vsegment_release_failure_total "
        << release_failure_.load() << '\n'
        << "# TYPE mooncake_vsegment_persistence_failure_total counter\n"
        << "mooncake_vsegment_persistence_failure_total "
        << persistence_failure_.load() << '\n'
        << "# TYPE mooncake_vsegment_recovery_failure_total counter\n"
        << "mooncake_vsegment_recovery_failure_total "
        << recovery_failure_.load() << '\n'
        << "# TYPE mooncake_vsegment_stale_route_total counter\n"
        << "mooncake_vsegment_stale_route_total " << stale_route_.load()
        << '\n';
    return out.str();
}

}  // namespace mooncake::vsegment
