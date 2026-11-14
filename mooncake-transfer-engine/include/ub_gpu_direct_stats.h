#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace mooncake {

struct UbGpuIoStats {
    uint64_t submitted_bytes = 0;
    uint64_t success_bytes = 0;
    uint64_t failed_bytes = 0;
};

struct UbGpuDirectStats {
    UbGpuIoStats reads;
    UbGpuIoStats writes;
    uint64_t staged_bytes = 0;
};

// Updated before publishing completion, including event driven completion.
class UbGpuDirectCounters {
   public:
    void Submit(bool write, uint64_t bytes) {
        submitted_[write ? 1 : 0].fetch_add(bytes, std::memory_order_relaxed);
    }
    void Complete(bool write, bool success, uint64_t bytes) {
        auto& counter = success ? success_[write ? 1 : 0]
                                : failed_[write ? 1 : 0];
        counter.fetch_add(bytes, std::memory_order_relaxed);
    }
    void Stage(uint64_t bytes) {
        staged_.fetch_add(bytes, std::memory_order_relaxed);
    }
    UbGpuDirectStats Snapshot() const {
        const auto io = [this](size_t index) {
            return UbGpuIoStats{submitted_[index].load(std::memory_order_relaxed),
                                success_[index].load(std::memory_order_relaxed),
                                failed_[index].load(std::memory_order_relaxed)};
        };
        return {io(0), io(1), staged_.load(std::memory_order_relaxed)};
    }

   private:
    std::array<std::atomic<uint64_t>, 2> submitted_{};
    std::array<std::atomic<uint64_t>, 2> success_{};
    std::array<std::atomic<uint64_t>, 2> failed_{};
    std::atomic<uint64_t> staged_{0};
};

inline UbGpuDirectCounters& GetUbGpuDirectCounters() {
    static UbGpuDirectCounters counters;
    return counters;
}

inline UbGpuDirectStats UbGpuDirectDelta(const UbGpuDirectStats& before,
                                         const UbGpuDirectStats& after) {
    const auto io = [](const UbGpuIoStats& first, const UbGpuIoStats& last) {
        return UbGpuIoStats{last.submitted_bytes - first.submitted_bytes,
                            last.success_bytes - first.success_bytes,
                            last.failed_bytes - first.failed_bytes};
    };
    return {io(before.reads, after.reads), io(before.writes, after.writes),
            after.staged_bytes - before.staged_bytes};
}

inline bool UbGpuDirectPassed(const UbGpuDirectStats& stats,
                              uint64_t read_bytes, uint64_t write_bytes) {
    return (read_bytes > 0 || write_bytes > 0) && stats.staged_bytes == 0 &&
           stats.reads.submitted_bytes == read_bytes &&
           stats.reads.success_bytes == read_bytes &&
           stats.reads.failed_bytes == 0 &&
           stats.writes.submitted_bytes == write_bytes &&
           stats.writes.success_bytes == write_bytes &&
           stats.writes.failed_bytes == 0;
}

}  // namespace mooncake
