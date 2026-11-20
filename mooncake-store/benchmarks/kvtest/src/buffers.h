#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "store_adapter.h"

namespace mooncake::kvtest {

struct VerifyOptions {
    std::string level{"size"};  // off | size | sample | full
    uint64_t sample_bytes{4096};
    uint64_t sample_step_bytes{1024 * 1024};
};

// Deterministic value pattern: byte[i] = (seed + i) & 0xff. Kept identical to
// the existing pipeline behaviour so old summaries remain comparable, and used
// by both the basic mode (seed = value_seed) and the pipeline reader
// (seed = sender instance id).
void FillPatternBytes(void *buffer, uint64_t size, uint64_t seed);
bool VerifyPatternBytes(const void *actual, uint64_t actual_size,
                        uint64_t expected_size, uint64_t seed,
                        const VerifyOptions &options, std::string *reason);

// Value source for write operations. Either generates deterministic patterns
// for each configured size or loads one external value file (compatible with
// Mooncake-Performance's value.bin).
class ValueSource {
   public:
    bool Load(uint64_t seed, const std::string &file,
              const std::vector<uint64_t> &sizes, std::string *error);
    bool Loaded() const { return !values_.empty(); }
    const char *Data(uint64_t size) const;
    uint64_t MaxSize() const;
    bool Verify(uint64_t expected_size, const void *actual,
                uint64_t actual_size, const VerifyOptions &options,
                std::string *reason) const;

   private:
    std::map<uint64_t, std::vector<char>> values_;
};

// Page-aligned buffer registered with the store for zero-copy operations.
// One instance per thread; slots avoid concurrent reuse of a single buffer.
class RegisteredBuffer {
   public:
    RegisteredBuffer() = default;
    ~RegisteredBuffer();

    RegisteredBuffer(const RegisteredBuffer &) = delete;
    RegisteredBuffer &operator=(const RegisteredBuffer &) = delete;

    bool Create(StoreAdapter &store, uint64_t bytes_per_slot, size_t slots);
    bool Valid() const { return registered_; }
    void *At(size_t slot) const;
    uint64_t Stride() const { return stride_; }
    uint64_t Capacity() const { return size_; }
    size_t Slots() const { return slots_; }
    // Copy `data` (size bytes) into the first `slot_count` slots.
    void FillSlots(size_t slot_count, uint64_t size, const void *data);

   private:
    void Release();

    StoreAdapter *store_{nullptr};
    void *data_{nullptr};
    uint64_t stride_{0};
    size_t size_{0};
    size_t slots_{0};
    bool registered_{false};
};

}  // namespace mooncake::kvtest
