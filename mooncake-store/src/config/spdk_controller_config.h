#pragma once

#include <cstdint>
#include <optional>

namespace mooncake {

struct SpdkControllerConfig {
    // 1/2/4 lanes measured 31.16/39.83/43.17 GB/s at the same aggregate
    // QD=256 on the SP930 testbed; four also matches the default NoF worker
    // count.
    static constexpr uint32_t kDefaultQpairsPerSegment = 4;
    static constexpr uint32_t kMaxQpairsPerSegment = 64;

    std::optional<uint32_t> num_io_queues;
    std::optional<uint32_t> io_queue_size;
    std::optional<uint32_t> io_queue_requests;
    std::optional<uint8_t> transport_ack_timeout;
    std::optional<uint16_t> admin_queue_size;
    std::optional<uint64_t> fabrics_connect_timeout_us;
    std::optional<bool> header_digest;
    std::optional<bool> data_digest;
    // Unset keeps SPDK's default keep-alive timeout. Any non-zero KATO needs
    // the initiator's admin-poll thread to send keep-alives.
    std::optional<uint32_t> keep_alive_timeout_ms;
    // Always within [1, kMaxQpairsPerSegment]; invalid input falls back to
    // the default with a warning.
    uint32_t qpairs_per_segment = kDefaultQpairsPerSegment;

    static SpdkControllerConfig FromEnvironment();
};

}  // namespace mooncake
