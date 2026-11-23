#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "buffers.h"
#include "metrics.h"
#include "store_adapter.h"

namespace mooncake::kvtest {

enum class OpKind : uint8_t {
    kPut,
    kPutBatch,
    kPutFrom,
    kBatchPutFrom,
    kUpsert,
    kUpsertFrom,
    kBatchUpsertFrom,
    kGet,
    kGetBatch,
    kGetInto,
    kGetBuffer,
    kExist,
    kBatchExist,
    kRemove,
    kBatchRemove,
};

enum class OpSide : uint8_t { kWrite, kRead, kMaintenance, kAny };

struct OpSpec {
    OpKind kind;
    std::string_view canonical;
    OpSide side;
    bool batch_call;
    bool needs_registered_source;
    bool needs_registered_sink;
    bool needs_value;
    bool returns_data;
    std::string_view alias1;
    std::string_view alias2;
    std::string_view alias3;
};

// Per-thread reusable scratch so the request path does not allocate.
struct OpScratch {
    std::vector<std::span<const char>> spans;
    std::vector<void *> ptrs;
    std::vector<size_t> sizes;
    std::vector<char> copy_target;
    std::vector<int> results;
};

struct OpContext {
    const std::vector<std::string> *keys{nullptr};
    uint64_t value_size{0};
    const ValueSource *value{nullptr};
    // Used when `value` is null (pipeline reader): deterministic pattern
    // verification with the writer's sender id.
    uint64_t seed{0};
    RegisteredBuffer *write_buffer{nullptr};
    RegisteredBuffer *read_buffer{nullptr};
    OpScratch *scratch{nullptr};
    const VerifyOptions *verify{nullptr};
};

struct ExecuteOptions {
    MetricsCollector *metrics{nullptr};
    std::string metric_name;
    // When false a verification mismatch is still reported as MISMATCH in the
    // metrics but does not abort the pipeline / suppress the notification.
    bool fail_on_verify{false};
    bool warmup{false};
};

struct OpOutcome {
    bool ok{false};
    RecordStatus status{RecordStatus::kOk};
    uint64_t bytes{0};
    int32_t code{0};
    std::string detail;
};

const OpSpec *FindOp(std::string_view name);
std::string CanonicalName(const std::string &name);

// expected == kRead: only read-side operations are accepted.
// expected == kWrite: write-side and maintenance operations are accepted.
bool ValidateOpList(const std::vector<std::string> &names, OpSide expected,
                    bool registered_buffer_available, std::string *error);

OpOutcome ExecuteOp(StoreAdapter &store, const OpSpec &spec, OpContext &context,
                    const ExecuteOptions &options);

}  // namespace mooncake::kvtest
