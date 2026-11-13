// nof_buffer_alignment_test.cpp
// Modified By Yida (v3): USE_NOF_URMA 下 ClientBufferAllocator 的对齐矩阵
// 测试（512B/4KiB/128KiB/1MiB）。base 与子分配的 size/ptr 需满足 NoF 提交的
// 块对齐校验（submitSpdkNofOperation 要求 buffer_address_ % block_size == 0、
// size % block_size == 0）；kNofBufferAlignment=4096 时 512B sector 亦满足。
#include "client_buffer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <memory>
#include <vector>

namespace mooncake {

#if defined(USE_NOF_URMA)

class NofBufferAlignmentTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("NofBufferAlignmentTest");
        FLAGS_logtostderr = 1;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

// 对齐矩阵：512B / 4KiB / 128KiB / 1MiB（含非 4K 倍数的 512B）
TEST_F(NofBufferAlignmentTest, AllocateAlignmentMatrix) {
    constexpr size_t kTotalSize = 64ull * 1024 * 1024;
    const std::vector<size_t> kSizes = {512, 4096, 128 * 1024, 1024 * 1024};

    auto allocator = ClientBufferAllocator::create(kTotalSize, "", false,
                                                   /*use_spdk_dma=*/true);
    ASSERT_NE(allocator, nullptr);

    // base 必须页对齐
    EXPECT_EQ(reinterpret_cast<uintptr_t>(allocator->getBase()) %
                  kNofBufferAlignment,
              0u)
        << "allocator base is not 4K aligned";

    for (size_t size : kSizes) {
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value()) << "allocate(" << size << ") failed";
        const auto addr = reinterpret_cast<uintptr_t>(handle->ptr());
        EXPECT_EQ(addr % 512, 0u) << "512B sector misaligned, size=" << size;
        EXPECT_EQ(handle->size() % 512, 0u)
            << "512B sector misaligned size, requested=" << size;
        EXPECT_EQ(addr % kNofBufferAlignment, 0u)
            << "4K misaligned ptr, requested=" << size;
        EXPECT_EQ(handle->size() % kNofBufferAlignment, 0u)
            << "4K misaligned size, requested=" << size;

        // 写入并读回，确认区间可用
        std::memset(handle->ptr(), 0x5A, handle->size());
        EXPECT_EQ(static_cast<uint8_t*>(handle->ptr())[0], 0x5A);
        EXPECT_EQ(static_cast<uint8_t*>(handle->ptr())[handle->size() - 1],
                  0x5A);
    }
}

// 顺序分配偏移量均为页对齐（freed 区间重用后仍保持）
TEST_F(NofBufferAlignmentTest, RepeatedAllocationStaysPageAligned) {
    constexpr size_t kChunk = 128 * 1024;
    auto allocator = ClientBufferAllocator::create(16 * kChunk, "", false,
                                                   /*use_spdk_dma=*/true);
    ASSERT_NE(allocator, nullptr);

    for (int round = 0; round < 3; ++round) {
        std::vector<std::optional<BufferHandle>> handles;
        for (int i = 0; i < 8; ++i) {
            auto handle = allocator->allocate(kChunk + i * 512);
            ASSERT_TRUE(handle.has_value());
            EXPECT_EQ(
                reinterpret_cast<uintptr_t>(handle->ptr()) %
                    kNofBufferAlignment,
                0u)
                << "round=" << round << " i=" << i;
            handles.push_back(std::move(handle));
        }
        // 析构释放后进入下一轮，验证空闲复用路径同样保持对齐
    }
}

#else  // !USE_NOF_URMA

// 未启用 USE_NOF_URMA 时行为不变：64B 对齐、size 不取整
TEST(NofBufferAlignmentDisabledTest, UnalignedSmallAllocationKept) {
    auto allocator = ClientBufferAllocator::create(1024 * 1024, "", false,
                                                   /*use_spdk_dma=*/false);
    ASSERT_NE(allocator, nullptr);
    auto handle = allocator->allocate(512);
    ASSERT_TRUE(handle.has_value());
    EXPECT_EQ(handle->size(), 512u);
}

#endif

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
