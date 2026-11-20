#pragma once

#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "store_adapter.h"

namespace mooncake::kvtest {

// Minimal in-memory StoreAdapter shared by the unit tests. Leaf methods lock
// the map so BasicRunner can drive several worker threads concurrently.
class FakeStore final : public StoreAdapter {
   public:
    std::map<std::string, std::vector<char>> data;
    int put_failures{0};
    int put_error{-200};
    bool fail_existing_put{false};
    uint64_t put_count{0};
    uint64_t remove_count{0};

    int Put(const std::string &key, std::span<const char> value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (put_failures > 0) {
            --put_failures;
            return put_error;
        }
        if (fail_existing_put && data.find(key) != data.end()) {
            return -2;
        }
        data[key] = std::vector<char>(value.begin(), value.end());
        ++put_count;
        return 0;
    }

    int PutBatch(const std::vector<std::string> &keys,
                 const std::vector<std::span<const char>> &values) override {
        if (keys.size() != values.size()) return -1;
        for (size_t i = 0; i < keys.size(); ++i) {
            const int rc = Put(keys[i], values[i]);
            if (rc != 0) return rc;
        }
        return 0;
    }

    int PutFrom(const std::string &key, void *buffer, size_t size) override {
        return Put(key, std::span<const char>(
                            static_cast<const char *>(buffer), size));
    }

    std::vector<int> BatchPutFrom(const std::vector<std::string> &keys,
                                  const std::vector<void *> &buffers,
                                  const std::vector<size_t> &sizes) override {
        std::vector<int> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.push_back(PutFrom(keys[i], buffers[i], sizes[i]));
        }
        return results;
    }

    int Upsert(const std::string &key, std::span<const char> value) override {
        return Put(key, value);
    }

    int UpsertFrom(const std::string &key, void *buffer,
                   size_t size) override {
        return PutFrom(key, buffer, size);
    }

    std::vector<int> BatchUpsertFrom(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers,
        const std::vector<size_t> &sizes) override {
        return BatchPutFrom(keys, buffers, sizes);
    }

    BufferView GetBuffer(const std::string &key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = data.find(key);
        if (it == data.end()) return {};
        auto holder = std::make_shared<std::vector<char>>(it->second);
        BufferView view;
        view.ptr = holder->data();
        view.size = holder->size();
        view.owner = std::shared_ptr<void>(holder, holder->data());
        return view;
    }

    std::vector<BufferView> BatchGetBuffer(
        const std::vector<std::string> &keys) override {
        std::vector<BufferView> views;
        views.reserve(keys.size());
        for (const auto &key : keys) views.push_back(GetBuffer(key));
        return views;
    }

    int64_t GetInto(const std::string &key, void *buffer,
                    size_t size) override {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = data.find(key);
        if (it == data.end()) return -1;
        if (it->second.size() > size) return -2;
        std::memcpy(buffer, it->second.data(), it->second.size());
        return static_cast<int64_t>(it->second.size());
    }

    int IsExist(const std::string &key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        return data.find(key) == data.end() ? 0 : 1;
    }

    std::vector<int> BatchIsExist(
        const std::vector<std::string> &keys) override {
        std::vector<int> results;
        results.reserve(keys.size());
        for (const auto &key : keys) results.push_back(IsExist(key));
        return results;
    }

    int Remove(const std::string &key, bool /*force*/) override {
        std::lock_guard<std::mutex> lock(mutex_);
        data.erase(key);
        ++remove_count;
        return 0;
    }

    std::vector<int> BatchRemove(const std::vector<std::string> &keys,
                                 bool force) override {
        std::vector<int> results;
        results.reserve(keys.size());
        for (const auto &key : keys) results.push_back(Remove(key, force));
        return results;
    }

    long RemoveAll(bool /*force*/) override {
        std::lock_guard<std::mutex> lock(mutex_);
        const long removed = static_cast<long>(data.size());
        data.clear();
        return removed;
    }

    int RegisterBuffer(void * /*buffer*/, size_t /*size*/) override {
        return 0;
    }
    int UnregisterBuffer(void * /*buffer*/) override { return 0; }

   private:
    mutable std::mutex mutex_;
};

}  // namespace mooncake::kvtest
