#include "operations.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <string>
#include <utility>

namespace mooncake::kvtest {
namespace {

using Steady = std::chrono::steady_clock;

constexpr OpSpec kSpecs[] = {
    {OpKind::kPut, "put", OpSide::kWrite, false, false, false, true, false,
     "", "", ""},
    {OpKind::kPutBatch, "put_batch", OpSide::kWrite, true, false, false, true,
     false, "", "", ""},
    {OpKind::kPutFrom, "put_from", OpSide::kWrite, false, true, false, true,
     false, "", "", ""},
    {OpKind::kBatchPutFrom, "batch_put_from", OpSide::kWrite, true, true, false,
     true, false, "", "", ""},
    {OpKind::kUpsert, "upsert", OpSide::kWrite, false, false, false, true,
     false, "setStringView", "", ""},
    {OpKind::kUpsertFrom, "upsert_from", OpSide::kWrite, false, true, false,
     true, false, "upsertFrom", "", ""},
    {OpKind::kBatchUpsertFrom, "batch_upsert_from", OpSide::kWrite, true, true,
     false, true, false, "batchUpsertFrom", "mSet", ""},
    {OpKind::kGet, "get", OpSide::kRead, false, false, false, false, true, "",
     "", ""},
    {OpKind::kGetBatch, "get_batch", OpSide::kRead, true, false, false, false,
     true, "mGet", "", ""},
    {OpKind::kGetInto, "get_into", OpSide::kRead, false, false, true, false,
     true, "", "", ""},
    {OpKind::kGetBuffer, "get_buffer", OpSide::kRead, false, false, false,
     false, true, "getBuffer", "", ""},
    {OpKind::kExist, "exist", OpSide::kRead, false, false, false, false, false,
     "isExist", "", ""},
    {OpKind::kBatchExist, "batch_exist", OpSide::kRead, true, false, false,
     false, false, "batchIsExist", "mExist", ""},
    {OpKind::kRemove, "remove", OpSide::kMaintenance, false, false, false, false,
     false, "del", "", ""},
    {OpKind::kBatchRemove, "batch_remove", OpSide::kMaintenance, true, false,
     false, false, false, "batchRemove", "", ""},
};

double ElapsedMs(const Steady::time_point &start) {
    return std::chrono::duration<double, std::milli>(Steady::now() - start)
        .count();
}

OpOutcome Ok(uint64_t bytes) {
    OpOutcome outcome;
    outcome.ok = true;
    outcome.status = RecordStatus::kOk;
    outcome.bytes = bytes;
    return outcome;
}

OpOutcome Error(int32_t code, std::string detail = {}) {
    OpOutcome outcome;
    outcome.ok = false;
    outcome.status = RecordStatus::kError;
    outcome.code = code;
    outcome.detail = std::move(detail);
    return outcome;
}

OpOutcome Miss() {
    OpOutcome outcome;
    outcome.ok = false;
    outcome.status = RecordStatus::kMiss;
    return outcome;
}

OpOutcome Mismatch(std::string why, bool fail_on_verify) {
    OpOutcome outcome;
    outcome.ok = !fail_on_verify;
    outcome.status = RecordStatus::kMismatch;
    outcome.detail = std::move(why);
    return outcome;
}

bool MatchAlias(const OpSpec &spec, std::string_view name) {
    if (name == spec.canonical) return true;
    if (!spec.alias1.empty() && name == spec.alias1) return true;
    if (!spec.alias2.empty() && name == spec.alias2) return true;
    if (!spec.alias3.empty() && name == spec.alias3) return true;
    return false;
}

bool VerifyRead(const OpContext &context, const void *data, uint64_t size,
                std::string *reason) {
    if (context.verify == nullptr) return size == context.value_size;
    if (context.value != nullptr) {
        return context.value->Verify(context.value_size, data, size,
                                     *context.verify, reason);
    }
    return VerifyPatternBytes(data, size, context.value_size, context.seed,
                              *context.verify, reason);
}

bool PrepareWrite(const OpSpec &spec, OpContext &context,
                  std::string *error) {
    if (context.keys == nullptr || context.scratch == nullptr) {
        *error = "missing context";
        return false;
    }
    if (!spec.needs_value) return true;
    const size_t count = context.keys->size();
    if (context.value == nullptr || context.value_size == 0) {
        *error = "no value source";
        return false;
    }
    const char *data = context.value->Data(context.value_size);
    if (data == nullptr) {
        *error = "value size was not generated";
        return false;
    }
    if (spec.needs_registered_source) {
        if (context.write_buffer == nullptr ||
            !context.write_buffer->Valid() ||
            context.write_buffer->Slots() < count ||
            context.write_buffer->Stride() < context.value_size) {
            *error = "no usable registered write buffer";
            return false;
        }
        context.write_buffer->FillSlots(count, context.value_size, data);
        context.scratch->ptrs.resize(count);
        context.scratch->sizes.assign(count,
                                      static_cast<size_t>(context.value_size));
        for (size_t i = 0; i < count; ++i) {
            context.scratch->ptrs[i] = context.write_buffer->At(i);
        }
    } else {
        context.scratch->spans.assign(
            count, std::span<const char>(data,
                                         static_cast<size_t>(context.value_size)));
    }
    return true;
}

}  // namespace

const OpSpec *FindOp(std::string_view name) {
    for (const auto &spec : kSpecs) {
        if (MatchAlias(spec, name)) return &spec;
    }
    return nullptr;
}

std::string CanonicalName(const std::string &name) {
    const OpSpec *spec = FindOp(name);
    return spec == nullptr ? std::string{} : std::string(spec->canonical);
}

bool ValidateOpList(const std::vector<std::string> &names, OpSide expected,
                    bool registered_buffer_available, std::string *error) {
    for (const auto &name : names) {
        const OpSpec *spec = FindOp(name);
        if (spec == nullptr) {
            if (error) *error = "unsupported operation: " + name;
            return false;
        }
        if (expected == OpSide::kRead && spec->side != OpSide::kRead) {
            if (error) {
                *error = "read-side operation list contains a non-read op: " +
                         name;
            }
            return false;
        }
        if (expected == OpSide::kWrite && spec->side == OpSide::kRead) {
            if (error) {
                *error = "write-side operation list contains a read op: " +
                         name;
            }
            return false;
        }        if ((spec->needs_registered_source ||
             spec->needs_registered_sink) &&
            !registered_buffer_available) {
            if (error) {
                *error = name +
                         " requires a registered buffer, which this worker "
                         "does not own";
            }
            return false;
        }
    }
    return true;
}

OpOutcome ExecuteOp(StoreAdapter &store, const OpSpec &spec,
                    OpContext &context, const ExecuteOptions &options) {
    if (context.keys == nullptr || context.keys->empty()) {
        return Error(-2, "no keys");
    }
    if (context.scratch == nullptr) return Error(-2, "no scratch");

    const std::vector<std::string> &keys = *context.keys;
    const size_t count = keys.size();
    const uint64_t value_size = context.value_size;
    const std::string metric_name = options.metric_name.empty()
                                        ? std::string(spec.canonical)
                                        : options.metric_name;

    if (spec.needs_value) {
        std::string error;
        if (!PrepareWrite(spec, context, &error)) return Error(-2, error);
    }

    bool all_ok = true;
    OpOutcome first_failure;
    uint64_t bytes_sum = 0;
    auto record = [&](double latency_ms, const OpOutcome &outcome) {
        if (options.metrics != nullptr) {
            options.metrics->Record(metric_name, latency_ms, outcome.status,
                                    outcome.bytes, outcome.code,
                                    options.warmup);
        }
        if (outcome.ok) {
            bytes_sum += outcome.bytes;
        } else if (all_ok) {
            first_failure = outcome;
            all_ok = false;
        }
    };

    switch (spec.kind) {
        case OpKind::kPutBatch: {
            const auto start = Steady::now();
            const int rc = store.PutBatch(keys, context.scratch->spans);
            const double latency = ElapsedMs(start);
            record(latency, rc == 0 ? Ok(value_size * count) : Error(rc));
            break;
        }
        case OpKind::kPut: {
            if (spec.batch_call) {
                const auto start = Steady::now();
                const int rc = store.PutBatch(keys, context.scratch->spans);
                const double latency = ElapsedMs(start);
                record(latency, rc == 0 ? Ok(value_size * count) : Error(rc));
            } else {
                for (size_t i = 0; i < count; ++i) {
                    const auto start = Steady::now();
                    const int rc = store.Put(keys[i], context.scratch->spans[i]);
                    const double latency = ElapsedMs(start);
                    record(latency, rc == 0 ? Ok(value_size) : Error(rc));
                }
            }
            break;
        }
        case OpKind::kUpsert: {
            for (size_t i = 0; i < count; ++i) {
                const auto start = Steady::now();
                const int rc = store.Upsert(keys[i], context.scratch->spans[i]);
                const double latency = ElapsedMs(start);
                record(latency, rc == 0 ? Ok(value_size) : Error(rc));
            }
            break;
        }
        case OpKind::kPutFrom:
        case OpKind::kUpsertFrom: {
            for (size_t i = 0; i < count; ++i) {
                const auto start = Steady::now();
                const int rc =
                    spec.kind == OpKind::kPutFrom
                        ? store.PutFrom(keys[i], context.scratch->ptrs[i],
                                        context.scratch->sizes[i])
                        : store.UpsertFrom(keys[i], context.scratch->ptrs[i],
                                           context.scratch->sizes[i]);
                const double latency = ElapsedMs(start);
                record(latency, rc == 0 ? Ok(value_size) : Error(rc));
            }
            break;
        }
        case OpKind::kBatchPutFrom:
        case OpKind::kBatchUpsertFrom: {
            const auto start = Steady::now();
            const std::vector<int> results =
                spec.kind == OpKind::kBatchPutFrom
                    ? store.BatchPutFrom(keys, context.scratch->ptrs,
                                         context.scratch->sizes)
                    : store.BatchUpsertFrom(keys, context.scratch->ptrs,
                                            context.scratch->sizes);
            const double latency = ElapsedMs(start);
            bool ok = results.size() == count;
            int first_code = -3;
            for (int rc : results) {
                if (rc != 0) {
                    ok = false;
                    first_code = rc;
                    break;
                }
            }
            record(latency,
                   ok ? Ok(value_size * count) : Error(first_code));
            break;
        }
        case OpKind::kGet: {
            context.scratch->copy_target.resize(
                static_cast<size_t>(value_size));
            for (size_t i = 0; i < count; ++i) {
                const auto start = Steady::now();
                const BufferView view = store.GetBuffer(keys[i]);
                if (view) {
                    const size_t copy_size = static_cast<size_t>(
                        std::min<uint64_t>(view.size, value_size));
                    std::memcpy(context.scratch->copy_target.data(), view.ptr,
                                copy_size);
                }
                const double latency = ElapsedMs(start);
                OpOutcome outcome;
                if (!view) {
                    outcome = Miss();
                } else if (view.size != value_size) {
                    outcome = Mismatch("size", options.fail_on_verify);
                } else {
                    std::string reason;
                    if (!VerifyRead(context,
                                    context.scratch->copy_target.data(),
                                    value_size, &reason)) {
                        if (options.metrics != nullptr) {
                            options.metrics->RecordVerifyFailure();
                        }
                        outcome = Mismatch(reason, options.fail_on_verify);
                    } else {
                        outcome = Ok(value_size);
                    }
                }
                record(latency, outcome);
            }
            break;
        }
        case OpKind::kGetBuffer: {
            for (size_t i = 0; i < count; ++i) {
                const auto start = Steady::now();
                const BufferView view = store.GetBuffer(keys[i]);
                const double latency = ElapsedMs(start);
                OpOutcome outcome;
                if (!view) {
                    outcome = Miss();
                } else if (view.size != value_size) {
                    outcome = Mismatch("size", options.fail_on_verify);
                } else {
                    std::string reason;
                    if (!VerifyRead(context, view.ptr, view.size, &reason)) {
                        if (options.metrics != nullptr) {
                            options.metrics->RecordVerifyFailure();
                        }
                        outcome = Mismatch(reason, options.fail_on_verify);
                    } else {
                        outcome = Ok(value_size);
                    }
                }
                record(latency, outcome);
            }
            break;
        }
        case OpKind::kGetBatch: {
            const auto start = Steady::now();
            const std::vector<BufferView> views = store.BatchGetBuffer(keys);
            if (views.size() == count) {
                context.scratch->copy_target.assign(
                    static_cast<size_t>(value_size * count), '\0');
                for (size_t i = 0; i < count; ++i) {
                    if (!views[i]) continue;
                    const size_t copy_size = static_cast<size_t>(
                        std::min<uint64_t>(views[i].size, value_size));
                    std::memcpy(context.scratch->copy_target.data() +
                                    static_cast<size_t>(value_size * i),
                                views[i].ptr, copy_size);
                }
            }
            const double latency = ElapsedMs(start);
            if (views.size() != count) {
                record(latency, Error(-3, "batch-size"));
                break;
            }
            RecordStatus worst = RecordStatus::kOk;
            std::string reason;
            for (size_t i = 0; i < count; ++i) {
                if (!views[i]) {
                    worst = RecordStatus::kMiss;
                    break;
                }
                if (views[i].size != value_size) {
                    worst = RecordStatus::kMismatch;
                    reason = "size";
                    break;
                }
            }
            if (worst == RecordStatus::kOk) {
                for (size_t i = 0; i < count; ++i) {
                    if (!VerifyRead(context,
                                    context.scratch->copy_target.data() +
                                        static_cast<size_t>(value_size * i),
                                    value_size, &reason)) {
                        worst = RecordStatus::kMismatch;
                        if (options.metrics != nullptr) {
                            options.metrics->RecordVerifyFailure();
                        }
                        break;
                    }
                }
            }
            OpOutcome outcome;
            if (worst == RecordStatus::kOk) {
                outcome = Ok(value_size * count);
            } else if (worst == RecordStatus::kMiss) {
                outcome = Miss();
            } else {
                outcome = Mismatch(reason, options.fail_on_verify);
            }
            record(latency, outcome);
            break;
        }
        case OpKind::kGetInto: {
            if (context.read_buffer == nullptr ||
                !context.read_buffer->Valid() ||
                context.read_buffer->Slots() < 1 ||
                context.read_buffer->Stride() < value_size) {
                return Error(-2, "no usable registered read buffer");
            }
            for (size_t i = 0; i < count; ++i) {
                // Per-key interface: reuse slot 0 sequentially so the buffer
                // only has to be one value-size slot deep.
                void *target = context.read_buffer->At(0);
                const auto start = Steady::now();
                const int64_t rc = store.GetInto(
                    keys[i], target,
                    static_cast<size_t>(context.read_buffer->Stride()));
                const double latency = ElapsedMs(start);
                OpOutcome outcome;
                if (rc < 0) {
                    outcome = Error(static_cast<int32_t>(rc));
                } else if (static_cast<uint64_t>(rc) != value_size) {
                    outcome = Mismatch("size", options.fail_on_verify);
                } else {
                    std::string reason;
                    if (!VerifyRead(context, target, value_size, &reason)) {
                        if (options.metrics != nullptr) {
                            options.metrics->RecordVerifyFailure();
                        }
                        outcome = Mismatch(reason, options.fail_on_verify);
                    } else {
                        outcome = Ok(value_size);
                    }
                }
                record(latency, outcome);
            }
            break;
        }
        case OpKind::kExist: {
            for (size_t i = 0; i < count; ++i) {
                const auto start = Steady::now();
                const int rc = store.IsExist(keys[i]);
                const double latency = ElapsedMs(start);
                OpOutcome outcome;
                if (rc > 0) {
                    outcome = Ok(0);
                } else if (rc == 0) {
                    outcome = Miss();
                } else {
                    outcome = Error(rc);
                }
                record(latency, outcome);
            }
            break;
        }
        case OpKind::kBatchExist: {
            const auto start = Steady::now();
            const std::vector<int> results = store.BatchIsExist(keys);
            const double latency = ElapsedMs(start);
            if (results.size() != count) {
                record(latency, Error(-3, "batch-size"));
                break;
            }
            bool all = true;
            bool has_error = false;
            int error_code = 0;
            for (int rc : results) {
                if (rc <= 0) {
                    all = false;
                    if (rc < 0) {
                        has_error = true;
                        error_code = rc;
                        break;
                    }
                }
            }
            if (has_error) {
                record(latency, Error(error_code));
            } else {
                record(latency, all ? Ok(0) : Miss());
            }
            break;
        }
        case OpKind::kRemove: {
            for (size_t i = 0; i < count; ++i) {
                const auto start = Steady::now();
                const int rc = store.Remove(keys[i], true);
                const double latency = ElapsedMs(start);
                record(latency, rc == 0 ? Ok(0) : Error(rc));
            }
            break;
        }
        case OpKind::kBatchRemove: {
            const auto start = Steady::now();
            const std::vector<int> results = store.BatchRemove(keys, true);
            const double latency = ElapsedMs(start);
            bool ok = results.size() == count;
            int first_code = -3;
            for (int rc : results) {
                if (rc != 0) {
                    ok = false;
                    first_code = rc;
                    break;
                }
            }
            record(latency, ok ? Ok(0) : Error(first_code));
            break;
        }
    }

    OpOutcome result = all_ok ? Ok(bytes_sum) : first_failure;
    result.bytes = bytes_sum;
    return result;
}

}  // namespace mooncake::kvtest
