#include "cvm/slot_migrator.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace cvm {

namespace {

// 把升序 slot 列表压缩成区间串，如 {0,1,2,5,7,8} -> "0-2,5,7-8"。
std::string FormatSlotRanges(const std::vector<uint16_t>& slots) {
    if (slots.empty()) {
        return "[]";
    }
    std::string out;
    size_t i = 0;
    while (i < slots.size()) {
        size_t j = i;
        while (j + 1 < slots.size() && slots[j + 1] == slots[j] + 1) {
            ++j;
        }
        if (!out.empty()) {
            out += ",";
        }
        out += std::to_string(slots[i]);
        if (j != i) {
            out += "-" + std::to_string(slots[j]);
        }
        i = j + 1;
    }
    return out;
}

}  // namespace

SlotMigrator::SlotMigrator(Config config) : config_(std::move(config)) {}

ErrorCode SlotMigrator::Reconcile(const std::vector<uint16_t>& owned_slots) {
    std::vector<uint16_t> cur = owned_slots;
    std::vector<uint16_t> prev = last_owned_slots_;
    std::sort(cur.begin(), cur.end());
    std::sort(prev.begin(), prev.end());

    std::vector<uint16_t> gained;
    std::vector<uint16_t> released;
    std::set_difference(cur.begin(), cur.end(), prev.begin(), prev.end(),
                        std::back_inserter(gained));
    std::set_difference(prev.begin(), prev.end(), cur.begin(), cur.end(),
                        std::back_inserter(released));

    ErrorCode last_err = ErrorCode::OK;

    // 释放：旧 owner stage 导出元数据（on_release 投递到内存缓存，等新
    // owner RPC 拉取）。slot 归属不再写 etcd，故此处无 Delete 操作。
    for (size_t i = 0; i < released.size();) {
        size_t end = i + 1;
        while (end < released.size() && end - i < 128 &&
               released[end] == released[end - 1] + 1)
            ++end;
        if (on_release_range_) {
            on_release_range_(released[i], released[end - 1]);
        } else if (on_release_) {
            for (size_t j = i; j < end; ++j) on_release_(released[j]);
        }
        i = end;
    }
    if (!released.empty()) {
        LOG(INFO) << "SlotMigrator released master_id=" << config_.master_id
                  << ", slot_count=" << released.size()
                  << ", slots=[" << FormatSlotRanges(released) << "]";
    }

    // 获得：新 owner 拉取旧 owner 元数据（on_acquire 内部 RPC 直传）。返回
    // OK 才视为元数据就绪并计入 last_owned_slots_；失败（旧 owner 未就绪 /
    // RPC 超时 / 反序列化失败）则本轮不记入，下一轮重新进入 gained 重试。
    std::vector<uint16_t> acquired_slots;
    std::vector<uint16_t> pending;
    acquired_slots.reserve(gained.size());
    pending.reserve(gained.size());
    auto record_result = [&](uint16_t slot, ErrorCode error) {
        if (error == ErrorCode::OK) acquired_slots.push_back(slot);
        else {
            pending.push_back(slot);
            last_err = error;
        }
    };
    // 段级批量 acquire（slot 就绪加速）：gained 升序天然聚成连续区间（slot
    // 归属按 rank 段划分），足够长的区间整段回调（实现内部按旧 owner 分组
    // 合并 RPC），短区间 / 未设置批量钩子 / 整段失败时退回逐 slot 路径，
    // 分类与重试语义完全一致。
    constexpr size_t kMinRangeAcquireBatchSize = 8;
    for (size_t i = 0; i < gained.size();) {
        size_t j = i;
        while (j + 1 < gained.size() && gained[j + 1] == gained[j] + 1) {
            ++j;
        }
        const size_t range_len = j - i + 1;
        bool range_done = false;
        if (on_acquire_range_ && range_len >= kMinRangeAcquireBatchSize) {
            std::vector<ErrorCode> results(range_len, ErrorCode::OK);
            const ErrorCode range_err =
                on_acquire_range_(gained[i], gained[j], results);
            if (range_err == ErrorCode::OK && results.size() == range_len) {
                for (size_t k = 0; k < range_len; ++k) {
                    record_result(gained[i + k], results[k]);
                }
                range_done = true;
            }
            // 整段失败（实现未兜底的异常路径）→ 逐 slot 兜底重放。
        }
        if (!range_done) {
            for (size_t k = 0; k < range_len; ++k) {
                const uint16_t slot = gained[i + k];
                record_result(slot,
                              on_acquire_ ? on_acquire_(slot) : ErrorCode::OK);
            }
        }
        i = j + 1;
    }
    if (!acquired_slots.empty()) {
        LOG(INFO) << "SlotMigrator acquired master_id=" << config_.master_id
                  << ", slot_count=" << acquired_slots.size()
                  << ", slots=[" << FormatSlotRanges(acquired_slots) << "]";
    }
    if (!pending.empty()) {
        LOG(INFO) << "SlotMigrator acquire-pending (retry next cycle) "
                  << pending.size() << " slot(s), master_id=" << config_.master_id
                  << ", slots=[" << FormatSlotRanges(pending) << "]";
    }

    // last_owned_slots_ = 本期最终认领成功的 slot：cur 去掉「acquire 未就绪」
    // 的 slot，使其下一轮重新进入 gained → 重试元数据拉取。
    std::vector<uint16_t> final_set;
    if (pending.empty()) {
        final_set = std::move(cur);
    } else {
        std::sort(pending.begin(), pending.end());
        final_set.reserve(cur.size() - pending.size());
        std::set_difference(cur.begin(), cur.end(), pending.begin(),
                            pending.end(), std::back_inserter(final_set));
    }
    last_owned_slots_ = std::move(final_set);
    return pending.empty() ? ErrorCode::OK : last_err;
}

}  // namespace cvm
}  // namespace mooncake
