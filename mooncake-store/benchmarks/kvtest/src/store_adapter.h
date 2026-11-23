#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace mooncake::kvtest {

// A non-owning view over a store buffer plus the handle that keeps it alive.
// Decoupled from mooncake::BufferHandle so the operation layer and its unit
// tests do not need the full client headers.
struct BufferView {
    const void *ptr{nullptr};
    uint64_t size{0};
    std::shared_ptr<void> owner;

    explicit operator bool() const { return ptr != nullptr; }
};

// Thin interface over mooncake::RealClient. Implementations own any repeat
// configuration (ReplicateConfig) so the operation layer stays SDK-agnostic
// and mockable.
class StoreAdapter {
   public:
    virtual ~StoreAdapter() = default;

    virtual int Put(const std::string &key,
                    std::span<const char> value) = 0;
    virtual int PutBatch(const std::vector<std::string> &keys,
                         const std::vector<std::span<const char>> &values) = 0;
    virtual int PutFrom(const std::string &key, void *buffer,
                        size_t size) = 0;
    virtual std::vector<int> BatchPutFrom(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers,
        const std::vector<size_t> &sizes) = 0;

    virtual int Upsert(const std::string &key,
                       std::span<const char> value) = 0;
    virtual int UpsertFrom(const std::string &key, void *buffer,
                           size_t size) = 0;
    virtual std::vector<int> BatchUpsertFrom(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers,
        const std::vector<size_t> &sizes) = 0;

    virtual BufferView GetBuffer(const std::string &key) = 0;
    virtual std::vector<BufferView> BatchGetBuffer(
        const std::vector<std::string> &keys) = 0;
    virtual int64_t GetInto(const std::string &key, void *buffer,
                            size_t size) = 0;

    virtual int IsExist(const std::string &key) = 0;
    virtual std::vector<int> BatchIsExist(
        const std::vector<std::string> &keys) = 0;

    virtual int Remove(const std::string &key, bool force) = 0;
    virtual std::vector<int> BatchRemove(const std::vector<std::string> &keys,
                                         bool force) = 0;
    virtual long RemoveAll(bool force) = 0;

    virtual int RegisterBuffer(void *buffer, size_t size) = 0;
    virtual int UnregisterBuffer(void *buffer) = 0;
};

}  // namespace mooncake::kvtest
