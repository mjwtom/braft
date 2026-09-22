// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/04/11 12:15:37

#include <random>
#include <gtest/gtest.h>
#include <raft/util.h>
#include <base/crc32c.h>
#include <raft/raft.h>

class ChecksumTest : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(ChecksumTest, benchmark) {
    char data[4096];
    for (size_t i = 0; i < ARRAY_SIZE(data); ++i) {
        data[i] = base::fast_rand_in('a', 'z');
    }
    base::Timer timer;
    const size_t N = 10000;
    timer.start();
    for (size_t i = 0; i < N; ++i) {
        raft::murmurhash32(data, sizeof(data));
    }
    timer.stop();
    const long mur_elp = timer.u_elapsed();
    
    timer.start();
    for (size_t i = 0; i < N; ++i) {
        base::crc32c::Value(data, sizeof(data));
    }
    timer.stop();
    const long crc_elp = timer.u_elapsed();

    LOG(INFO) << "murmurhash32_TP=" << sizeof(data) * N / (double)mur_elp << "MB/s"
              << " base_crc32_TP=" << sizeof(data) * N / (double)crc_elp << "MB/s";
    LOG(INFO) << "base_is_fast_crc32_support=" << base::crc32c::IsFastCrc32Supported();

}

static thread_local std::random_device g_random_device;
static thread_local std::mt19937_64 g_random_engine(g_random_device());

static void random_binary(base::IOBuf& data, uint64_t len) {
    LOG(NOTICE) << "start generate " << len / (1024 * 1024L) << "MB random data";
    std::uniform_int_distribution<int16_t> distribution(INT8_MIN, INT8_MAX);
    for (uint64_t i = 0; i < len; i++) {
        CHECK_EQ(0, data.push_back(static_cast<int8_t>(distribution(g_random_engine))));
        if (i % (10 * 1024 * 1024L) == 0) {
            LOG(NOTICE) << "generate " << i / (1024 * 1024L) << "MB random data";
        }
    }
    LOG(NOTICE) << "generate " << len / (1024 * 1024L) << "MB random data";
}

TEST_F(ChecksumTest, isa_crc32) {
    uint64_t len = 1 * 1024 * 1024L;
    base::IOBuf data;
    random_binary(data, len);
    for (uint32_t crc_type = 0; crc_type < (uint32_t)raft::CrcType::TYPE_END; ++crc_type) {
        base::Timer timer;
        timer.start();
        uint32_t crc_code = 0;
        ASSERT_TRUE(raft::get_crc32((raft::CrcType)crc_type, data, &crc_code));
        timer.stop();
        const long elp = timer.u_elapsed();
        LOG(NOTICE) << "crc_type: " << (raft::CrcType)crc_type
                    << " crc_code: " << crc_code
                    << " time_us: " << elp;
    }
    uint32_t crc_code = 0;
    int32_t crc_type_1 = (int32_t)raft::CrcType::TYPE_END + 1;
    LOG(NOTICE) << "crc_type: " << (raft::CrcType)crc_type_1;
    raft::CrcType crc_type = raft::CrcType::TYPE_END;
    LOG(NOTICE) << "crc_type: " << (raft::CrcType)crc_type;
    ASSERT_FALSE(raft::get_crc32(crc_type, data, &crc_code));
}
