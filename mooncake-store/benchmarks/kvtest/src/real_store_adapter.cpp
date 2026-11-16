#include "real_store_adapter.h"

#include "client_buffer.h"
#include "real_client.h"

#include <utility>

namespace mooncake::kvtest {
namespace {

ReplicateConfig MakeConfig(size_t replica_num) {
    ReplicateConfig config;
    config.replica_num = replica_num;
    return config;
}

}  // namespace

RealStoreAdapter::RealStoreAdapter(
    std::shared_ptr<mooncake::RealClient> client, size_t replica_num)
    : client_(std::move(client)),
      replica_num_(replica_num == 0 ? 1 : replica_num) {}

RealStoreAdapter::~RealStoreAdapter() = default;

int RealStoreAdapter::Put(const std::string &key,
                          std::span<const char> value) {
    return client_->put(key, value, MakeConfig(replica_num_));
}

int RealStoreAdapter::PutBatch(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values) {
    return client_->put_batch(keys, values, MakeConfig(replica_num_));
}

int RealStoreAdapter::PutFrom(const std::string &key, void *buffer,
                              size_t size) {
    return client_->put_from(key, buffer, size, MakeConfig(replica_num_));
}

std::vector<int> RealStoreAdapter::BatchPutFrom(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes) {
    return client_->batch_put_from(keys, buffers, sizes,
                                   MakeConfig(replica_num_));
}

int RealStoreAdapter::Upsert(const std::string &key,
                             std::span<const char> value) {
    return client_->upsert(key, value, MakeConfig(replica_num_));
}

int RealStoreAdapter::UpsertFrom(const std::string &key, void *buffer,
                                 size_t size) {
    return client_->upsert_from(key, buffer, size, MakeConfig(replica_num_));
}

std::vector<int> RealStoreAdapter::BatchUpsertFrom(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes) {
    return client_->batch_upsert_from(keys, buffers, sizes,
                                      MakeConfig(replica_num_));
}

BufferView RealStoreAdapter::GetBuffer(const std::string &key) {
    auto handle = client_->get_buffer(key);
    if (!handle) return {};
    BufferView view;
    view.ptr = handle->ptr();
    view.size = handle->size();
    view.owner = std::shared_ptr<void>(handle, handle->ptr());
    return view;
}

std::vector<BufferView> RealStoreAdapter::BatchGetBuffer(
    const std::vector<std::string> &keys) {
    std::vector<std::shared_ptr<BufferHandle>> handles =
        client_->batch_get_buffer(keys);
    std::vector<BufferView> views;
    views.reserve(handles.size());
    for (auto &handle : handles) {
        if (!handle) {
            views.push_back(BufferView{});
            continue;
        }
        BufferView view;
        view.ptr = handle->ptr();
        view.size = handle->size();
        view.owner = std::shared_ptr<void>(handle, handle->ptr());
        views.push_back(std::move(view));
    }
    return views;
}

int64_t RealStoreAdapter::GetInto(const std::string &key, void *buffer,
                                  size_t size) {
    return client_->get_into(key, buffer, size);
}

int RealStoreAdapter::IsExist(const std::string &key) {
    return client_->isExist(key);
}

std::vector<int> RealStoreAdapter::BatchIsExist(
    const std::vector<std::string> &keys) {
    return client_->batchIsExist(keys);
}

int RealStoreAdapter::Remove(const std::string &key, bool force) {
    return client_->remove(key, force);
}

std::vector<int> RealStoreAdapter::BatchRemove(
    const std::vector<std::string> &keys, bool force) {
    return client_->batchRemove(keys, force);
}

long RealStoreAdapter::RemoveAll(bool force) {
    return client_->removeAll(force);
}

int RealStoreAdapter::RegisterBuffer(void *buffer, size_t size) {
    return client_->register_buffer(buffer, size);
}

int RealStoreAdapter::UnregisterBuffer(void *buffer) {
    return client_->unregister_buffer(buffer);
}

}  // namespace mooncake::kvtest
