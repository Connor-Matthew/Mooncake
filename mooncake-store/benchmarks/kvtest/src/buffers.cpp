#include "buffers.h"

#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iterator>
#include <limits>
#include <utility>

namespace mooncake::kvtest {
namespace {

bool CompareExpected(const char *expected, const char *actual, uint64_t begin,
                     uint64_t length) {
    return std::memcmp(expected + begin, actual + begin, length) == 0;
}

bool VerifyAgainst(const char *expected, uint64_t expected_size,
                   const void *actual, uint64_t actual_size,
                   const VerifyOptions &options, std::string *reason) {
    if (options.level == "off") return true;
    if (actual_size != expected_size) {
        if (reason) *reason = "size";
        return false;
    }
    if (expected_size == 0) return true;
    if (actual == nullptr) {
        if (reason) *reason = "null";
        return false;
    }
    if (options.level == "size") return true;

    const auto *bytes = static_cast<const char *>(actual);
    auto check = [&](uint64_t begin, uint64_t length) {
        return CompareExpected(expected, bytes, begin, length);
    };
    if (options.level == "full") {
        if (!check(0, expected_size)) {
            if (reason) *reason = "content";
            return false;
        }
        return true;
    }
    const uint64_t sample = std::min(options.sample_bytes, expected_size);
    if (!check(0, sample)) {
        if (reason) *reason = "head";
        return false;
    }
    if (expected_size > sample &&
        !check(expected_size - sample, sample)) {
        if (reason) *reason = "tail";
        return false;
    }
    const uint64_t step = std::max<uint64_t>(1, options.sample_step_bytes);
    for (uint64_t offset = step; offset + sample < expected_size;
         offset += step) {
        if (!check(offset, sample)) {
            if (reason) *reason = "mid";
            return false;
        }
    }
    return true;
}

}  // namespace

void FillPatternBytes(void *buffer, uint64_t size, uint64_t seed) {
    auto *bytes = static_cast<unsigned char *>(buffer);
    for (uint64_t i = 0; i < size; ++i) {
        bytes[i] = static_cast<unsigned char>((seed + i) & 0xff);
    }
}

bool VerifyPatternBytes(const void *actual, uint64_t actual_size,
                        uint64_t expected_size, uint64_t seed,
                        const VerifyOptions &options, std::string *reason) {
    if (options.level == "off") return true;
    if (actual_size != expected_size) {
        if (reason) *reason = "size";
        return false;
    }
    if (expected_size == 0) return true;
    if (actual == nullptr) {
        if (reason) *reason = "null";
        return false;
    }
    if (options.level == "size") return true;

    const auto *bytes = static_cast<const unsigned char *>(actual);
    auto check = [&](uint64_t begin, uint64_t length) {
        for (uint64_t i = begin; i < begin + length; ++i) {
            if (bytes[i] != static_cast<unsigned char>((seed + i) & 0xff)) {
                return false;
            }
        }
        return true;
    };
    if (options.level == "full") {
        if (!check(0, expected_size)) {
            if (reason) *reason = "content";
            return false;
        }
        return true;
    }
    const uint64_t sample = std::min(options.sample_bytes, expected_size);
    if (!check(0, sample)) {
        if (reason) *reason = "head";
        return false;
    }
    if (expected_size > sample &&
        !check(expected_size - sample, sample)) {
        if (reason) *reason = "tail";
        return false;
    }
    const uint64_t step = std::max<uint64_t>(1, options.sample_step_bytes);
    for (uint64_t offset = step; offset + sample < expected_size;
         offset += step) {
        if (!check(offset, sample)) {
            if (reason) *reason = "mid";
            return false;
        }
    }
    return true;
}

bool ValueSource::Load(uint64_t seed, const std::string &file,
                       const std::vector<uint64_t> &sizes,
                       std::string *error) {
    values_.clear();
    if (sizes.empty()) {
        if (error) *error = "data_sizes is empty";
        return false;
    }
    if (!file.empty()) {
        if (sizes.size() != 1) {
            if (error) *error = "value_file requires exactly one data size";
            return false;
        }
        std::ifstream input(file, std::ios::binary);
        if (!input) {
            if (error) *error = "cannot open value file: " + file;
            return false;
        }
        std::vector<char> data((std::istreambuf_iterator<char>(input)),
                               std::istreambuf_iterator<char>());
        if (data.size() < sizes.front()) {
            if (error) {
                *error = "value file is smaller than the configured data size";
            }
            return false;
        }
        data.resize(sizes.front());
        values_.emplace(sizes.front(), std::move(data));
        return true;
    }
    for (uint64_t size : sizes) {
        std::vector<char> data(static_cast<size_t>(size));
        FillPatternBytes(data.data(), size, seed);
        values_.emplace(size, std::move(data));
    }
    return true;
}

const char *ValueSource::Data(uint64_t size) const {
    const auto it = values_.find(size);
    return it == values_.end() ? nullptr : it->second.data();
}

uint64_t ValueSource::MaxSize() const {
    return values_.empty() ? 0 : values_.rbegin()->first;
}

bool ValueSource::Verify(uint64_t expected_size, const void *actual,
                         uint64_t actual_size, const VerifyOptions &options,
                         std::string *reason) const {
    if (options.level == "off") return true;
    const auto it = values_.find(expected_size);
    if (it == values_.end()) {
        if (reason) *reason = "no-value";
        return false;
    }
    return VerifyAgainst(it->second.data(), expected_size, actual, actual_size,
                         options, reason);
}

RegisteredBuffer::~RegisteredBuffer() { Release(); }

bool RegisteredBuffer::Create(StoreAdapter &store, uint64_t bytes_per_slot,
                              size_t slots) {
    Release();
    if (bytes_per_slot == 0 || slots == 0) return false;
    const uint64_t page = static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
    if (page == 0) return false;
    stride_ = ((bytes_per_slot + page - 1) / page) * page;
    if (stride_ == 0 ||
        slots > std::numeric_limits<size_t>::max() / stride_) {
        return false;
    }
    size_ = static_cast<size_t>(stride_ * slots);
    data_ = mmap(nullptr, size_, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (data_ == MAP_FAILED) {
        data_ = nullptr;
        return false;
    }
    if (store.RegisterBuffer(data_, size_) != 0) {
        munmap(data_, size_);
        data_ = nullptr;
        return false;
    }
    store_ = &store;
    slots_ = slots;
    registered_ = true;
    return true;
}

void *RegisteredBuffer::At(size_t slot) const {
    if (!registered_ || slot >= slots_) return nullptr;
    return static_cast<char *>(data_) + stride_ * slot;
}

void RegisteredBuffer::FillSlots(size_t slot_count, uint64_t size,
                                 const void *data) {
    if (!registered_ || data == nullptr) return;
    const uint64_t copy_size = std::min<uint64_t>(size, stride_);
    const size_t count = std::min(slot_count, slots_);
    for (size_t i = 0; i < count; ++i) {
        std::memcpy(At(i), data, static_cast<size_t>(copy_size));
    }
}

void RegisteredBuffer::Release() {
    if (registered_ && store_ != nullptr && data_ != nullptr) {
        store_->UnregisterBuffer(data_);
    }
    if (data_ != nullptr) {
        munmap(data_, size_);
    }
    store_ = nullptr;
    data_ = nullptr;
    stride_ = 0;
    size_ = 0;
    slots_ = 0;
    registered_ = false;
}

}  // namespace mooncake::kvtest
