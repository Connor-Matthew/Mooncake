#pragma once

#include <memory>
#include <string>

#include "store_adapter.h"

namespace mooncake {
class RealClient;
}  // namespace mooncake

namespace mooncake::kvtest {

// StoreAdapter backed by mooncake::RealClient. The replicate configuration is
// owned here so the operation layer stays independent of SDK headers.
class RealStoreAdapter final : public StoreAdapter {
   public:
    explicit RealStoreAdapter(std::shared_ptr<mooncake::RealClient> client,
                              size_t replica_num = 1);
    ~RealStoreAdapter() override;

    int Put(const std::string &key, std::span<const char> value) override;
    int PutBatch(const std::vector<std::string> &keys,
                 const std::vector<std::span<const char>> &values) override;
    int PutFrom(const std::string &key, void *buffer, size_t size) override;
    std::vector<int> BatchPutFrom(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers,
        const std::vector<size_t> &sizes) override;

    int Upsert(const std::string &key, std::span<const char> value) override;
    int UpsertFrom(const std::string &key, void *buffer, size_t size) override;
    std::vector<int> BatchUpsertFrom(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers,
        const std::vector<size_t> &sizes) override;

    BufferView GetBuffer(const std::string &key) override;
    std::vector<BufferView> BatchGetBuffer(
        const std::vector<std::string> &keys) override;
    int64_t GetInto(const std::string &key, void *buffer, size_t size) override;

    int IsExist(const std::string &key) override;
    std::vector<int> BatchIsExist(
        const std::vector<std::string> &keys) override;

    int Remove(const std::string &key, bool force) override;
    std::vector<int> BatchRemove(const std::vector<std::string> &keys,
                                 bool force) override;
    long RemoveAll(bool force) override;

    int RegisterBuffer(void *buffer, size_t size) override;
    int UnregisterBuffer(void *buffer) override;

   private:
    std::shared_ptr<mooncake::RealClient> client_;
    size_t replica_num_{1};
};

}  // namespace mooncake::kvtest
