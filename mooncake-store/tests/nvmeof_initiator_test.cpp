#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "nof/nof_runtime.h"
#include "transfer_task.h"

namespace mooncake {
namespace {

// Mock implementing the interface contract: callbacks fire synchronously
// inside PollCompletion, matching SpdkInitiator's threading contract — which
// is what makes the tests valid.
class MockInitiator : public NVMeoFInitiator {
   public:
    NofSegmentHandle* OpenSegment(const std::string& ep) override {
        auto it = handles_.find(ep);
        if (it != handles_.end()) {
            return it->second;
        }
        // The opaque handle is a forward-declared type (its full definition
        // lives only in spdk_initiator.cpp) and the interface never
        // dereferences it — use the stable address of aligned storage as a
        // unique per-endpoint sentinel pointer (map key). Never new/delete
        // NofSegmentHandle.
        NofSegmentHandle* h = reinterpret_cast<NofSegmentHandle*>(
            &sentinels_[handles_.size() % sentinels_.size()]);
        handles_[ep] = h;
        return h;
    }

    bool ProbeSegment(const std::string&, uint32_t, std::string*) override {
        return probe_ok_;
    }

    uint32_t GetBlockSize(const NofSegmentHandle*) override {
        return block_size_;
    }

    int SubmitIO(NofSegmentHandle*, void*, uint64_t, uint64_t, NofIOOp,
                 NofIOAdaptor* adaptor) override {
        if (fail_submit_) {
            return -1;
        }
        pending_.push_back(adaptor);
        return 0;
    }

    int64_t PollCompletion(NofSegmentHandle*,
                           uint32_t max_completions) override {
        int64_t n = 0;
        while (!pending_.empty() &&
               (max_completions == 0 || n < max_completions)) {
            auto* adaptor = pending_.front();
            pending_.pop_front();
            NofIOCompletion c;
            c.success = !fail_io_;
            c.sc = 1;
            c.sct = 0;
            if (fail_io_) {
                c.error_string = "mock_io_error";
            }
            adaptor->cb(adaptor->ctx, c);
            ++n;
        }
        return n;
    }

    ErrorCode RegisterMemory(void*, size_t) override { return ErrorCode::OK; }
    ErrorCode UnregisterMemory(void*) override { return ErrorCode::OK; }
    NofCapabilities GetCapabilities() const override { return {}; }

    uint32_t block_size_ = 4096;
    bool fail_submit_ = false;
    bool fail_io_ = false;
    bool probe_ok_ = true;
    std::deque<NofIOAdaptor*> pending_;
    std::map<std::string, NofSegmentHandle*> handles_;
    std::array<uint64_t, 16> sentinels_{};
};

TEST(NofInitiatorDefaultContractTest, LaneDefaults) {
    MockInitiator mock;
    auto* segment = mock.OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");

    EXPECT_EQ(mock.GetLaneCount(nullptr), 0u);
    EXPECT_EQ(mock.GetLaneCount(segment), 1u);
    EXPECT_EQ(mock.GetLane(segment, 0), segment);
    EXPECT_EQ(mock.GetLane(segment, 1), nullptr);
}

TEST(NofInitiatorDefaultContractTest, FaultIsolationDefaultsAreNoOps) {
    MockInitiator mock;
    auto* segment = mock.OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");

    EXPECT_TRUE(mock.IsLaneHealthy(segment));
    EXPECT_NO_THROW(mock.MarkLaneFailed(segment));
    EXPECT_NO_THROW(mock.ReleaseFailedLane(segment));
    EXPECT_TRUE(mock.IsLaneHealthy(segment));
}

TEST(NofInitiatorDefaultContractTest, BufferDefaultsAndSubmitDispatch) {
    MockInitiator mock;
    auto* segment = mock.OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");
    int callback_count = 0;
    NofIOAdaptor adaptor{
        [](void* ctx, const NofIOCompletion&) { ++*static_cast<int*>(ctx); },
        &callback_count};
    uint8_t buffer[4096]{};

    const auto resolved = mock.ResolveBuffer(buffer, sizeof(buffer));
    EXPECT_EQ(resolved.type, NofMemoryType::kHost);
    EXPECT_EQ(resolved.device_id, -1);

    EXPECT_EQ(mock.SubmitIOWithBuffer(segment, buffer, 0, sizeof(buffer),
                                      NofIOOp::kRead, &adaptor, resolved),
              0);
    ASSERT_EQ(mock.pending_.size(), 1u);
    EXPECT_EQ(mock.PollCompletion(segment, 0), 1);
    EXPECT_EQ(callback_count, 1);

    for (const auto type : {NofMemoryType::kCuda, NofMemoryType::kError,
                            NofMemoryType::kNotSupported}) {
        const auto pending_before = mock.pending_.size();
        const auto callback_count_before = callback_count;
        EXPECT_EQ(mock.SubmitIOWithBuffer(segment, buffer, 0, sizeof(buffer),
                                          NofIOOp::kRead, &adaptor,
                                          NofBufferInfo{type, 0}),
                  -ENOTSUP);
        EXPECT_EQ(mock.pending_.size(), pending_before);
        EXPECT_EQ(callback_count, callback_count_before);
    }
}

TEST(NofInitiatorDefaultContractTest, EndpointRegistrationIsUnsupported) {
    MockInitiator mock;
    uint8_t buffer[16]{};
    EXPECT_EQ(
        mock.RegisterMemoryForEndpoint("trtype:TCP", buffer, sizeof(buffer)),
        nullptr);
}

static_assert(sizeof(NofIOAdaptor::backend_scratch) ==
              kNofIOAdaptorScratchBytes);
TEST(NofInitiatorDefaultContractTest, AdaptorAggregateZeroInitializesScratch) {
    NofIOAdaptor adaptor{nullptr, nullptr};
    for (const auto byte : adaptor.backend_scratch) {
        EXPECT_EQ(byte, 0);
    }
}

TEST(NofWorkerPoolTest, SubmitAndComplete) {
    auto mock = std::make_shared<MockInitiator>();
    NofWorkerPool pool(mock, 0);
    auto* seg = mock->OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");
    auto state = std::make_shared<NofOperationState>();
    std::vector<uint8_t> buf(8192);
    NofTask task(seg, buf.data(), /*byte_off=*/0, /*byte_len=*/8192,
                 NofIOOp::kWrite, state);
    pool.submitTask(std::move(task));
    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
}

TEST(NofWorkerPoolTest, FailurePropagatesErrorDetail) {
    auto mock = std::make_shared<MockInitiator>();
    mock->fail_io_ = true;
    NofWorkerPool pool(mock, 0);
    auto* seg = mock->OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");
    auto state = std::make_shared<NofOperationState>();
    std::vector<uint8_t> buf(8192);
    NofTask task(seg, buf.data(), 0, 8192, NofIOOp::kWrite, state);
    pool.submitTask(std::move(task));
    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(state->error_detail(), "mock_io_error");
}

TEST(NofWorkerPoolTest, SubmitFailureFailsTask) {
    auto mock = std::make_shared<MockInitiator>();
    mock->fail_submit_ = true;
    NofWorkerPool pool(mock, 0);
    auto* seg = mock->OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");
    auto state = std::make_shared<NofOperationState>();
    std::vector<uint8_t> buf(8192);
    NofTask task(seg, buf.data(), 0, 8192, NofIOOp::kWrite, state);
    pool.submitTask(std::move(task));
    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
}

TEST(NofRuntimeTest, NonNofBuildReturnsNullInitiator) {
#ifndef USE_NOF
    auto rt = CreateNofRuntime();
    EXPECT_EQ(rt.initiator, nullptr);
    EXPECT_NE(rt.dma_allocator, nullptr);
#endif
}

}  // namespace
}  // namespace mooncake
