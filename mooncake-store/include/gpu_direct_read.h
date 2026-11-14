#pragma once

#include <cstddef>
#include <string_view>

namespace mooncake {

// Only registered subranges on an explicitly selected UB direct path may
// bypass RealClient's host buffer. Callers first classify dst as device memory.
inline bool CanReadUbGpuDirect(bool memory_replica,
                               std::string_view client_protocol,
                               std::string_view gpu_mode, bool registered,
                               size_t region_size, size_t offset, size_t size) {
    return memory_replica && client_protocol == "ub" &&
           gpu_mode == "gdr-peermem" && registered && size > 0 &&
           offset <= region_size && size <= region_size - offset;
}

}  // namespace mooncake
