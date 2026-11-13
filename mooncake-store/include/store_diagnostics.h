#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <string>

#include "mooncake_logging.h"

namespace mooncake {

// Store transfer and RPC breakdown diagnostics run on request hot paths.
// Require an explicit opt-in, then apply the process-wide high-frequency
// sampling rate as a second gate.
inline bool ShouldSampleStoreTransferDiagnostics(uint64_t trace_id) {
    static const bool enabled = [] {
        const char* value = std::getenv("MC_STORE_TRANSFER_DIAG");
        if (!value) return false;

        std::string normalized(value);
        std::transform(
            normalized.begin(), normalized.end(), normalized.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return normalized == "1" || normalized == "true" ||
               normalized == "yes" || normalized == "on";
    }();
    return enabled && mooncake::logging::ShouldSampleHiFreqLog(trace_id);
}

}  // namespace mooncake
