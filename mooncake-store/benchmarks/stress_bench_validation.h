#pragma once

#include <cstddef>
#include <cstdint>
#include <charconv>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

namespace mooncake::bench {

struct GpuPathOptions {
    std::string mode;
    std::string protocol;
    bool memory_replicas = false;
    bool nof_replicas = false;
    bool require_direct = false;
    bool cuda_build = false;
    bool ub_build = false;
    bool nof_gpu_build = false;
    bool nof_gpu_available = false;
};

inline std::string ValidateGpuPath(const GpuPathOptions& options) {
    const bool gpu = options.mode == "gdr-peermem" || options.mode == "staging";
    if (!gpu) {
        return options.require_direct ? "--require_gpu_direct needs a GPU buffer"
                                      : "";
    }
    if (!options.cuda_build || !options.ub_build) {
        return "GPU buffers require USE_CUDA=ON and USE_UB=ON";
    }
    if (options.memory_replicas && options.mode == "gdr-peermem" &&
        options.protocol != "ub") {
        return "memory gdr-peermem requires --protocol=ub";
    }
    if (options.memory_replicas && options.require_direct &&
        options.mode != "gdr-peermem") {
        return "--require_gpu_direct cannot accept memory staging";
    }
    if (options.nof_replicas &&
        (!options.nof_gpu_build || !options.nof_gpu_available)) {
        return "NoF GPU buffers require USE_NOF_URMA=ON, "
               "USE_NOF_GPU_DIRECT=ON and an available CUDA device";
    }
    return "";
}

struct PlacementRegion {
    uint64_t base = 0;
    uint64_t size = 0;
    std::string endpoint;
    std::string protocol;

    bool Contains(uint64_t address, uint64_t length,
                  const std::string& actual_endpoint) const {
        return actual_endpoint == endpoint && length > 0 && address >= base &&
               address - base <= size && length <= size - (address - base);
    }
};

inline uint64_t NextPatternWord(uint64_t& pattern) {
    pattern = (pattern ^ (pattern >> 30)) * 0xBF58476D1CE4E5B9ULL;
    pattern = (pattern ^ (pattern >> 27)) * 0x94D049BB133111EBULL;
    return pattern ^ (pattern >> 31);
}

// Preserve the existing benchmark's full-word pattern; define trailing bytes
// too, so values smaller than eight bytes cannot bypass integrity checking.
inline void FillPattern(void* data, size_t size, size_t seed) {
    auto* bytes = static_cast<unsigned char*>(data);
    uint64_t pattern = static_cast<uint64_t>(seed) * 0x9E3779B97F4A7C15ULL;
    size_t offset = 0;
    for (; size - offset >= sizeof(uint64_t); offset += sizeof(uint64_t)) {
        const uint64_t word = NextPatternWord(pattern);
        std::memcpy(bytes + offset, &word, sizeof(word));
    }
    if (offset < size) std::memset(bytes + offset, 0, size - offset);
}

inline bool CheckPattern(const void* data, size_t actual_size,
                         size_t expected_size, size_t seed) {
    if (data == nullptr || actual_size != expected_size) return false;
    const auto* bytes = static_cast<const unsigned char*>(data);
    uint64_t pattern = static_cast<uint64_t>(seed) * 0x9E3779B97F4A7C15ULL;
    size_t offset = 0;
    for (; actual_size - offset >= sizeof(uint64_t); offset += sizeof(uint64_t)) {
        uint64_t word;
        std::memcpy(&word, bytes + offset, sizeof(word));
        if (word != NextPatternWord(pattern)) return false;
    }
    for (; offset < actual_size; ++offset) {
        if (bytes[offset] != 0) return false;
    }
    return true;
}

inline int ExitCode(size_t failed_operations, int verification_result = 0) {
    return failed_operations == 0 && verification_result == 0 ? 0 : 1;
}

inline std::optional<size_t> KeySeed(std::string_view key) {
    const auto marker = key.rfind("_key_");
    if (marker == std::string_view::npos) return std::nullopt;
    const auto suffix = key.substr(marker + 5);
    size_t seed = 0;
    const auto result =
        std::from_chars(suffix.data(), suffix.data() + suffix.size(), seed);
    if (result.ec != std::errc{} || result.ptr != suffix.data() + suffix.size()) {
        return std::nullopt;
    }
    return seed;
}

inline bool NofGpuLayoutSupported(uint64_t value_size, uint64_t chunk_size,
                                  uint64_t inflight_size, bool whole_pool) {
    constexpr uint64_t gpu_page = 64 * 1024;
    return value_size > 0 && chunk_size > 0 && inflight_size > 0 &&
           (whole_pool || (value_size % gpu_page == 0 &&
                           chunk_size % gpu_page == 0 &&
                           inflight_size % gpu_page == 0));
}

}  // namespace mooncake::bench
