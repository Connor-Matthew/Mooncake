#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace mooncake::vsegment {

// Process-local gauge snapshot. Capacity layers, outermost first:
//   quota_bytes          = sum of quota extent lengths (planning leftovers
//                          excluded)
//   total_capacity_bytes = writable capacity after dropping the per-extent
//                          and member-count remainders
//   formed_*             = capacity and free bytes of created (non-retired)
//                          VSegments
//   allocated_bytes      = formed_capacity - formed_free (committed plus
//                          in-flight reservations)
//   available_bytes      = total_capacity - allocated
struct VSegmentGaugeValues {
    uint64_t quota_bytes{0};
    uint64_t total_capacity_bytes{0};
    uint64_t allocated_bytes{0};
    uint64_t reserved_bytes{0};
    uint64_t available_bytes{0};
    uint64_t formed_capacity_bytes{0};
    uint64_t formed_free_bytes{0};
    uint64_t reservations{0};
    uint64_t committed_allocations{0};
    uint64_t pending_creations{0};
};

// Process-local vsegment metrics exported through the master's existing
// Prometheus endpoint. Gauges describe the currently owned Partitions;
// counters are monotonic for the lifetime of the SubMaster process.
class VSegmentMetrics {
   public:
    static VSegmentMetrics& Instance();

    void SetGauges(const VSegmentGaugeValues& values);
    void IncCreateSuccess() { create_success_.fetch_add(1); }
    void IncCreateFailure() { create_failure_.fetch_add(1); }
    void IncReleaseSuccess() { release_success_.fetch_add(1); }
    void IncReleaseFailure() { release_failure_.fetch_add(1); }
    void IncPersistenceFailure() { persistence_failure_.fetch_add(1); }
    void IncRecoveryFailure() { recovery_failure_.fetch_add(1); }
    void IncStaleRoute() { stale_route_.fetch_add(1); }

    // Read-side getters for the periodic VSegment Status/Storage log lines.
    uint64_t quota_bytes() const { return quota_bytes_.load(); }
    uint64_t total_capacity_bytes() const {
        return total_capacity_bytes_.load();
    }
    uint64_t allocated_bytes() const { return allocated_bytes_.load(); }
    uint64_t reserved_bytes() const { return reserved_bytes_.load(); }
    uint64_t available_bytes() const { return available_bytes_.load(); }
    uint64_t formed_capacity_bytes() const {
        return formed_capacity_bytes_.load();
    }
    uint64_t formed_free_bytes() const { return formed_free_bytes_.load(); }
    // allocated / total_capacity; 0 when no capacity is owned.
    double used_ratio() const;

    std::string Serialize() const;

   private:
    std::atomic<uint64_t> quota_bytes_{0};
    std::atomic<uint64_t> total_capacity_bytes_{0};
    std::atomic<uint64_t> allocated_bytes_{0};
    std::atomic<uint64_t> reserved_bytes_{0};
    std::atomic<uint64_t> available_bytes_{0};
    std::atomic<uint64_t> formed_capacity_bytes_{0};
    std::atomic<uint64_t> formed_free_bytes_{0};
    std::atomic<uint64_t> reservations_{0};
    std::atomic<uint64_t> committed_allocations_{0};
    std::atomic<uint64_t> pending_creations_{0};
    std::atomic<uint64_t> create_success_{0};
    std::atomic<uint64_t> create_failure_{0};
    std::atomic<uint64_t> release_success_{0};
    std::atomic<uint64_t> release_failure_{0};
    std::atomic<uint64_t> persistence_failure_{0};
    std::atomic<uint64_t> recovery_failure_{0};
    std::atomic<uint64_t> stale_route_{0};
};

}  // namespace mooncake::vsegment
