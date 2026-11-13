#pragma once

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace mooncake::bench {

// Merge counts, not window percentiles. Storage is bounded independently of
// benchmark duration. Values below 64 ns are exact; larger values use 64
// sub-buckets per power of two (bucket width at most 1/64 of the value).
class LatencyHistogram {
   public:
    void Record(uint64_t ns) {
        ++buckets_[BucketIndex(ns)];
        ++count_;
        sum_ns_ += ns;
        min_ns_ = std::min(min_ns_, ns);
        max_ns_ = std::max(max_ns_, ns);
    }

    void Merge(const LatencyHistogram& other) {
        for (size_t i = 0; i < buckets_.size(); ++i) {
            buckets_[i] += other.buckets_[i];
        }
        count_ += other.count_;
        sum_ns_ += other.sum_ns_;
        min_ns_ = std::min(min_ns_, other.min_ns_);
        max_ns_ = std::max(max_ns_, other.max_ns_);
    }

    uint64_t Count() const { return count_; }
    uint64_t MinNs() const { return count_ == 0 ? 0 : min_ns_; }
    uint64_t MaxNs() const { return max_ns_; }
    double MeanNs() const {
        return count_ == 0 ? 0.0 : static_cast<double>(sum_ns_ / count_);
    }

    uint64_t PercentileNs(double fraction) const {
        if (count_ == 0) return 0;
        if (fraction <= 0.0) return min_ns_;
        if (fraction >= 1.0) return max_ns_;
        const auto rank = static_cast<uint64_t>(
            std::ceil(static_cast<long double>(fraction) * count_));
        uint64_t seen = 0;
        for (size_t i = 0; i < buckets_.size(); ++i) {
            seen += buckets_[i];
            if (seen >= rank) {
                return std::min(max_ns_, BucketUpperBound(i));
            }
        }
        return max_ns_;
    }

   private:
    static size_t BucketIndex(uint64_t ns) {
        if (ns < 64) return static_cast<size_t>(ns);
        const unsigned exponent = 63U - __builtin_clzll(ns);
        const unsigned shift = exponent - 6U;
        return (shift + 1U) * 64U +
               static_cast<size_t>((ns >> shift) - 64U);
    }

    static uint64_t BucketUpperBound(size_t index) {
        if (index < 64) return index;
        const unsigned shift = static_cast<unsigned>(index / 64 - 1);
        const uint64_t lower = (64ULL + index % 64) << shift;
        return lower + ((1ULL << shift) - 1);
    }

    std::array<uint64_t, 64 + 58 * 64> buckets_{};
    uint64_t count_ = 0;
    long double sum_ns_ = 0;
    uint64_t min_ns_ = std::numeric_limits<uint64_t>::max();
    uint64_t max_ns_ = 0;
};

}  // namespace mooncake::bench
