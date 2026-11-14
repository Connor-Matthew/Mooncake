#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace mooncake::vsegment {

// Process-local vsegment metrics exported through the master's existing
// Prometheus endpoint. Gauges describe the currently owned Partitions;
// counters are monotonic for the lifetime of the SubMaster process.
class VSegmentMetrics {
   public:
    static VSegmentMetrics& Instance();

    void SetCapacity(uint64_t total, uint64_t free, uint64_t reservations,
                     uint64_t committed, uint64_t pending_creations);
    void IncCreateSuccess() { create_success_.fetch_add(1); }
    void IncCreateFailure() { create_failure_.fetch_add(1); }
    void IncReleaseSuccess() { release_success_.fetch_add(1); }
    void IncReleaseFailure() { release_failure_.fetch_add(1); }
    void IncPersistenceFailure() { persistence_failure_.fetch_add(1); }
    void IncRecoveryFailure() { recovery_failure_.fetch_add(1); }
    void IncStaleRoute() { stale_route_.fetch_add(1); }

    // Read-side getters for the periodic VSegment Status log line.
    uint64_t logical_capacity_bytes() const { return total_bytes_.load(); }
    uint64_t logical_free_bytes() const { return free_bytes_.load(); }

    std::string Serialize() const;

   private:
    std::atomic<uint64_t> total_bytes_{0};
    std::atomic<uint64_t> free_bytes_{0};
    std::atomic<uint64_t> reservations_{0};
    std::atomic<uint64_t> committed_{0};
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
