/*
 * =====================================================================================
 *
 *       Filename:  test_log.cpp
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2015/09/23 11:14:18
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <pwd.h>
#include <gtest/gtest.h>
#include <base/atomicops.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#include <base/files/dir_reader_posix.h>
#include <base/string_printf.h>
#include <base/logging.h>
#include <base/raw_pack.h>                          // base::RawPacker
#include "raft/util.h"
#include "raft/log.h"
#include "raft/storage.h"
#include "raft/io_uring.h"

namespace raft {
DECLARE_bool(raft_trace_append_entry_latency);
DECLARE_bool(raft_sync_segments);
DECLARE_int32(raft_sync_threads);
extern BvarLatencyRecorder g_sync_segment_latency;
}

class LogStorageTest : public testing::Test {
protected:
    void SetUp() {
        google::SetCommandLineOption("crash_on_fatal_log", "true");
        raft::FLAGS_raft_sync = false;
        raft::FLAGS_raft_sync_segments = false;
        raft::FLAGS_raft_sync_threads = 1;
        ASSERT_EQ(0, raft::g_raft_io_man->init());
    }
    void TearDown() {
        raft::g_raft_io_man->stop();
    }
};

inline std::string to_string(int i) {
    std::stringstream ss;
    ss << i;
    return ss.str();
}

TEST_F(LogStorageTest, open_segment) {
    // open segment operation
    ::system("mkdir data/");
    scoped_refptr<raft::Segment> seg1 = new raft::Segment("./data", 1L, 0);

    // not open
    raft::SynchronizedClosure done;
    raft::LogEntry* entry = nullptr;
    seg1->get(1, &entry, &done);
    done.wait();
    ASSERT_FALSE(done.status().ok());

    // create and open
    ASSERT_EQ(0, seg1->create());
    ASSERT_TRUE(seg1->is_open());

    // append entry
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id.term = 1;
        entry->id.index = i + 1;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        entry->data.append(data_buf);
        done.reset();
        seg1->append(entry, nullptr, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        entry->Release();
    }

    // read entry
    for (int i = 0; i < 10; i++) {
        int64_t term = seg1->get_term(i+1);
        ASSERT_EQ(term, 1);

        done.reset();
        raft::LogEntry* entry = nullptr;
        seg1->get(i+1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    {
        done.reset();
        raft::LogEntry* entry = nullptr;
        seg1->get(0, &entry, &done);
        done.wait();
        ASSERT_FALSE(done.status().ok());
        done.reset();
        seg1->get(11, &entry, &done);
        done.wait();
        ASSERT_FALSE(done.status().ok());
    }

    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    // load open segment
    scoped_refptr<raft::Segment> seg2 = new raft::Segment("./data", 1, 0);
    ASSERT_EQ(0, seg2->load(configuration_manager));

    for (int i = 0; i < 10; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        seg2->get(i+1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    {
        done.reset();
        raft::LogEntry* entry = nullptr;
        seg2->get(0, &entry, &done);
        done.wait();
        ASSERT_FALSE(done.status().ok());
        done.reset();
        seg2->get(11, &entry, &done);
        done.wait();
        ASSERT_FALSE(done.status().ok());
    }
    seg2 = nullptr; // close seg

    // truncate and read
    done.reset();
    seg1->truncate(5, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    done.wait();
    for (int i = 0; i < 5; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id.term = 1;
        entry->id.index = i + 6;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 6);
        entry->data.append(data_buf);

        done.reset();
        seg1->append(entry, nullptr, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        entry->Release();
    }
    for (int i = 0; i < 10; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        seg1->get(i+1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, i+1);

        char data_buf[128];
        if (i < 5) {
            snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        } else {
            snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 1);
        }
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    ASSERT_EQ(0, seg1->close());
    ASSERT_FALSE(seg1->is_open());
    ASSERT_EQ(0, seg1->unlink());

    delete configuration_manager;
}

TEST_F(LogStorageTest, closed_segment) {
    // open segment operation
    scoped_refptr<raft::Segment> seg1 = new raft::Segment("./data", 1L, 0);
    ASSERT_EQ(0, seg1->create());
    ASSERT_TRUE(seg1->is_open());
    raft::SynchronizedClosure done;
    // append entry
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id.term = 1;
        entry->id.index = i + 1;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        entry->data.append(data_buf);

        done.reset();
        seg1->append(entry, nullptr, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        entry->Release();
    }
    seg1->close();

    // read entry
    for (int i = 0; i < 10; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        seg1->get(i+1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    {
        done.reset();
        raft::LogEntry* entry = nullptr;
        seg1->get(0, &entry, &done);
        done.wait();
        ASSERT_FALSE(done.status().ok());
        done.reset();
        seg1->get(11, &entry, &done);
        done.wait();
        ASSERT_FALSE(done.status().ok());
    }

    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    // load open segment
    scoped_refptr<raft::Segment> seg2 = new raft::Segment("./data", 1, 10, 0);
    ASSERT_EQ(0, seg2->load(configuration_manager));

    for (int i = 0; i < 10; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        seg2->get(i+1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    {
        done.reset();
        raft::LogEntry* entry = nullptr;
        seg2->get(0, &entry, &done);
        done.wait();
        ASSERT_FALSE(done.status().ok());
        done.reset();
        seg2->get(11, &entry, &done);
        done.wait();
        ASSERT_FALSE(done.status().ok());
    }
    seg2 = nullptr; // close seg

    // truncate and read
    done.reset();
    seg1->truncate(5, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    for (int i = 0; i < 5; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id.term = 1;
        entry->id.index = i + 6;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 6);
        entry->data.append(data_buf);

        // become open segment again
        done.reset();
        seg1->append(entry, nullptr, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        entry->Release();
    }
    for (int i = 0; i < 10; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        seg1->get(i+1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        char data_buf[128];
        if (i < 5) {
            snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        } else {
            snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 1);
        }
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, i+1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    ASSERT_EQ(0, seg1->unlink());

    delete configuration_manager;
}

TEST_F(LogStorageTest, multi_segment_and_segment_logstorage) {
    ::system("rm -rf data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");

    // init
    ASSERT_EQ(0, storage->init(new raft::ConfigurationManager()));
    ASSERT_EQ(1, storage->first_log_index());
    ASSERT_EQ(0, storage->last_log_index());

    // append entry
    raft::SynchronizedClosure done;
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        done.reset();
        storage->append_entries(entries, NULL, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            entries[j]->Release();
        }
    }

    // read entry
    for (int i = 0; i < 500000; i++) {
        int64_t index = i + 1;
        done.reset();
        raft::LogEntry* entry = nullptr;
        storage->get_entry(index, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry);
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 500000);
    // truncate prefix
    done.reset();
    storage->truncate_prefix(10001, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(storage->first_log_index(), 10001);
    ASSERT_EQ(storage->last_log_index(), 500000);

    // boundary truncate prefix
    {
        raft::SegmentLogStorage::SegmentMap& segments1 = storage->segments();
        size_t old_segment_num = segments1.size();
        scoped_refptr<raft::Segment> first_seg = segments1.begin()->second.get();
        done.reset();
        storage->truncate_prefix(first_seg->last_index(), &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        raft::SegmentLogStorage::SegmentMap& segments2 = storage->segments();
        ASSERT_EQ(old_segment_num, segments2.size());

        done.reset();
        storage->truncate_prefix(first_seg->last_index() + 1, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        raft::SegmentLogStorage::SegmentMap& segments3 = storage->segments();
        ASSERT_EQ(old_segment_num - 1, segments3.size());
    }

    done.reset();
    storage->truncate_prefix(250001, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(storage->first_log_index(), 250001);
    ASSERT_EQ(storage->last_log_index(), 500000);
    for (int i = 250001; i <= 500000; i++) {
        int64_t index = i;
        done.reset();
        raft::LogEntry* entry = nullptr;
        storage->get_entry(index, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry);
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    // append
    for (int i = 100000; i < 200000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        done.reset();
        storage->append_entries(entries, NULL, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // truncate suffix
    ASSERT_EQ(250001, storage->first_log_index());
    ASSERT_EQ(1000000, storage->last_log_index());
    done.reset();
    storage->truncate_suffix(750000, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(250001, storage->first_log_index());
    ASSERT_EQ(750000, storage->last_log_index());

    // boundary truncate suffix
    {
        raft::SegmentLogStorage::SegmentMap& segments1 = storage->segments();
        scoped_refptr<raft::Segment> first_seg = segments1.begin()->second.get();
        if (segments1.size() > 1) {
            done.reset();
            storage->truncate_suffix(first_seg->last_index() + 1, &done);
            done.wait();
            ASSERT_TRUE(done.status().ok());
        }
        raft::SegmentLogStorage::SegmentMap& segments2 = storage->segments();
        ASSERT_EQ(1ul, segments2.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->last_index() + 1);
        done.reset();
        storage->truncate_suffix(first_seg->last_index(), &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        raft::SegmentLogStorage::SegmentMap& segments3 = storage->segments();
        ASSERT_EQ(1ul, segments3.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->last_index());
    }

    // read
    for (int i = 250001; i <= storage->last_log_index(); i++) {
        int64_t index = i;
        done.reset();
        raft::LogEntry* entry = nullptr;
        storage->get_entry(index, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry);
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    delete storage;

    // re load
    ::system("rm -rf data/log_meta");
    raft::SegmentLogStorage* storage2 = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, storage2->init(new raft::ConfigurationManager()));
    ASSERT_EQ(1, storage2->first_log_index());
    ASSERT_EQ(0, storage2->last_log_index());
    delete storage2;
}

TEST_F(LogStorageTest, append_close_load_append) {
    ::system("rm -rf data");
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    raft::SynchronizedClosure done;
    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        done.reset();
        storage->append_entries(entries, NULL, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    delete storage;
    delete configuration_manager;

    // reinit
    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    // append entry
    for (int i = 100000; i < 200000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 2;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        done.reset();
        storage->append_entries(entries, NULL, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // check and read
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 200000*5);

    for (int i = 0; i < 200000*5; i++) {
        int64_t index = i + 1;
        done.reset();
        raft::LogEntry* entry = nullptr;
        storage->get_entry(index, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry);
        if (i < 100000*5) {
            ASSERT_EQ(entry->id.term, 1);
        } else {
            ASSERT_EQ(entry->id.term, 2);
        }
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    delete storage;
    delete configuration_manager;
}

ssize_t file_size(const char* filename) {
    struct stat st;
    stat(filename, &st);
    return st.st_size;
}

int truncate_uninterrupted(const char* filename, off_t length) {
    int rc = 0;
    do {
        rc = truncate(filename, length);
    } while (rc == -1 && errno == EINTR);
    return rc;
}

const static size_t ENTRY_HEADER_SIZE = 24;
TEST_F(LogStorageTest, data_lost) {
    ::system("rm -rf data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        raft::SynchronizedClosure done;
        storage->append_entries(entries, NULL, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    delete storage;
    delete configuration_manager;

    // reinit
    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 100000*5);

    delete storage;
    delete configuration_manager;

    // last segment lost data
    base::DirReaderPosix dir_reader1("./data");
    ASSERT_TRUE(dir_reader1.IsValid());
    while (dir_reader1.Next()) {
        int64_t first_index = 0;
        int match = sscanf(dir_reader1.name(), "log_inprogress_%020ld",
                           &first_index);
        std::string path;
        base::string_appendf(&path, "./data/%s", dir_reader1.name());
        if (match == 1) {
            ASSERT_EQ(truncate_uninterrupted(path.c_str(), file_size(path.c_str()) - 1), 0);
        }
    }

    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 100000*5 - 1);

    scoped_refptr<raft::Segment> segment = storage->open_segment();
    ASSERT_NE(segment, nullptr);
    raft::Segment::LogMeta meta;
    ASSERT_EQ(segment->_get_meta(segment->last_index(), &meta), 0);
    base::IOPortal buf;
    char header_buf[ENTRY_HEADER_SIZE];
    ASSERT_EQ(raft::file_pread(&buf, segment->_fd, meta.offset, ENTRY_HEADER_SIZE), ENTRY_HEADER_SIZE);
    const char *p = (const char *)buf.fetch(header_buf, ENTRY_HEADER_SIZE);
    int64_t term = 0;
    uint32_t meta_field;
    uint32_t data_len = 0;
    uint32_t data_checksum = 0;
    uint32_t header_checksum = 0;
    base::RawUnpacker(p).unpack64((uint64_t&)term)
                  .unpack32(meta_field)
                  .unpack32(data_len)
                  .unpack32(data_checksum)
                  .unpack32(header_checksum);
    header_checksum ++; // make header checksum error
    base::RawPacker packer(header_buf);
    packer.pack64(term)
          .pack32(meta_field)
          .pack32(data_len)
          .pack32(data_checksum)
          .pack32(header_checksum);
    buf.clear();
    buf.append(header_buf, ENTRY_HEADER_SIZE);
    ASSERT_EQ(raft::file_pwrite(buf, segment->_fd, meta.offset), ENTRY_HEADER_SIZE);
    delete storage;
    delete configuration_manager;

    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    ASSERT_EQ(storage->last_log_index(), 100000*5-2);
    delete storage;
    delete configuration_manager;

    // middle segment lost data
    base::DirReaderPosix dir_reader2("./data");
    ASSERT_TRUE(dir_reader2.IsValid());
    while (dir_reader2.Next()) {
        int64_t first_index = 0;
        int64_t last_index = 0;
        int match = sscanf(dir_reader2.name(), "log_%020ld_%020ld",
                           &first_index, &last_index);
        std::string path;
        base::string_appendf(&path, "./data/%s", dir_reader2.name());
        if (match == 2) {
            ASSERT_EQ(truncate_uninterrupted(path.c_str(), file_size(path.c_str()) - 1), 0);
        }
    }

    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_NE(0, storage->init(configuration_manager));

    delete storage;
    delete configuration_manager;
}

TEST_F(LogStorageTest, full_segment_has_garbage) {
    ::system("rm -rf data");
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        raft::SynchronizedClosure done;
        storage->append_entries(entries, NULL, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    delete storage;
    delete configuration_manager;

    // generate garbage entries
    base::DirReaderPosix dir_reader("./data");
    std::string first_segment;
    std::string second_segment;
    while (dir_reader.IsValid()) {
        dir_reader.Next();
        int64_t first_index = 0;
        int64_t last_index = 0;
        int match = sscanf(dir_reader.name(), "log_%020ld_%020ld",
                           &first_index, &last_index);
        if (match != 2) {
            continue;
        }
        if (first_segment.empty()) {
            base::string_appendf(&first_segment, "./data/%s", dir_reader.name());
        } else {
            base::string_appendf(&second_segment, "./data/%s", dir_reader.name());
            break;
        }
    }

    int fd1 = open(first_segment.c_str(), O_RDWR);
    int fd2 = open(second_segment.c_str(), O_RDWR);
    int off1 = 0;
    int off2 = 0;
    ASSERT_TRUE(fd1 >= 0);
    ASSERT_TRUE(fd2 >= 0);
    struct stat st_buf;
    ASSERT_EQ(fstat(fd1, &st_buf), 0);
    off1 = st_buf.st_size;
    for (;;) {
        base::IOPortal buf;
        ssize_t ret = raft::file_pread(&buf, fd2, off2, 8192);
        ASSERT_TRUE(ret >= 0);
        if (ret == 0) {
            break;
        }
        ASSERT_EQ(buf.size(), raft::file_pwrite(buf, fd1, off1));
        off1 += buf.size();
        off2 += buf.size();
    }
    close(fd1);
    close(fd2);

    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_NE(0, storage->init(configuration_manager));
}

std::string get_process_username() {
    std::stringstream ss;
    struct passwd *pwd = getpwuid(geteuid());
    if (NULL == pwd) {
        ss << "unknown";
    } else {
        ss << pwd->pw_name;
    }
    return ss.str();
}

TEST_F(LogStorageTest, append_read_badcase) {
    ::system("rm -rf data");
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    raft::SynchronizedClosure done;
    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        done.reset();
        storage->append_entries(entries, NULL, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // check and read
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 100000*5);

    delete storage;
    delete configuration_manager;

    // make file unwrite
    base::FileEnumerator dir1(base::FilePath("./data"), false,
                              base::FileEnumerator::FILES
                              | base::FileEnumerator::DIRECTORIES);
    for (base::FilePath sub_path = dir1.Next(); !sub_path.empty(); sub_path = dir1.Next()) {
        base::File::Info info;
        base::GetFileInfo(sub_path, &info);
        if (!info.is_directory) {
            chmod(sub_path.value().c_str(), 0444);
        }
    }

    // reinit failed, because load open no permission
    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    // different for different users
    if (get_process_username() != "root") {
        ASSERT_NE(0, storage->init(configuration_manager));
    } else {
        ASSERT_EQ(0, storage->init(configuration_manager));
    }
    delete storage;
    delete configuration_manager;

    base::FileEnumerator dir2(base::FilePath("./data"), false,
                              base::FileEnumerator::FILES
                              | base::FileEnumerator::DIRECTORIES);
    for (base::FilePath sub_path = dir2.Next(); !sub_path.empty(); sub_path = dir2.Next()) {
        base::File::Info info;
        base::GetFileInfo(sub_path, &info);
        if (!info.is_directory) {
            chmod(sub_path.value().c_str(), 0644);
        }
    }

    // reinit success
    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    // make file chaos
    base::FileEnumerator dir3(base::FilePath("./data"), false,
                              base::FileEnumerator::FILES
                              | base::FileEnumerator::DIRECTORIES);
    for (base::FilePath sub_path = dir3.Next(); !sub_path.empty(); sub_path = dir3.Next()) {
        base::File::Info info;
        base::GetFileInfo(sub_path, &info);
        if (!info.is_directory) {
            chmod(sub_path.value().c_str(), 0644);

            int fd = ::open(sub_path.value().c_str(), O_RDWR, 0644);
            int64_t off = rand() % info.size;
            int64_t len = rand() % (info.size - off);
            if (len > 4096) {
                len = 4096;
            }
            char data[4096] = {0};
            ::pwrite(fd, data, len, off);
            ::close(fd);
        }
    }

    // read will fail
    for (int i = 0; i < 100000*5; i++) {
        int64_t index = i + 1;
        done.reset();
        raft::LogEntry* entry = nullptr;
        storage->get_entry(index, &entry, &done);
        done.wait();
        if (entry) {
            entry->Release();
        }
    }

    delete storage;
    delete configuration_manager;
}

TEST_F(LogStorageTest, configuration) {
    ::system("rm -rf data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    {
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_NO_OP;
        entry.id.term = 1;
        entry.id.index = 1;

        raft::SynchronizedClosure done;
        storage->append_entry(&entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
    }

    // add peer
    {
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_CONFIGURATION;
        entry.id.term = 1;
        entry.id.index = 2;
        entry.peers = new std::vector<raft::PeerId>;
        entry.peers->push_back(raft::PeerId("1.1.1.1:1000:0"));
        entry.peers->push_back(raft::PeerId("1.1.1.1:2000:0"));
        entry.peers->push_back(raft::PeerId("1.1.1.1:3000:0"));
        raft::SynchronizedClosure done;
        storage->append_entry(&entry, &done);
        done.wait();
    }

    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 3 + i*5+j;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }
        raft::SynchronizedClosure done;
        storage->append_entries(entries, NULL, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // remove peer
    {
        int64_t index = 2 + 100000*5 + 1;
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_CONFIGURATION;
        entry.id.term = 1;
        entry.id.index = index;
        entry.peers = new std::vector<raft::PeerId>;
        entry.peers->push_back(raft::PeerId("1.1.1.1:1000:0"));
        entry.peers->push_back(raft::PeerId("1.1.1.1:2000:0"));
        raft::SynchronizedClosure done;
        storage->append_entry(&entry, &done);
        done.wait();
    }

    delete storage;

    raft::SegmentLogStorage* storage2 = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, storage2->init(configuration_manager));

    raft::ConfigurationEntry pair;
    configuration_manager->get(2 + 100000*5, &pair);
    ASSERT_EQ(2, pair.id.index);
    LOG(NOTICE) << pair.conf;

    configuration_manager->get(2 + 100000*5 + 1, &pair);
    ASSERT_EQ(2+100000*5+1, pair.id.index);
    LOG(NOTICE) << pair.conf;

    raft::SynchronizedClosure done;
    storage2->truncate_suffix(400000, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    configuration_manager->get(400000, &pair);
    ASSERT_EQ(2, pair.id.index);

    done.reset();
    storage2->truncate_prefix(2, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    configuration_manager->get(400000, &pair);
    ASSERT_EQ(2, pair.id.index);

    delete storage2;
}

base::atomic<int> g_first_read_index(0);
base::atomic<int> g_last_read_index(0);
bool g_stop = false;

void* read_thread_routine(void* arg) {
    raft::SegmentLogStorage* storage = (raft::SegmentLogStorage*)arg;
    raft::SynchronizedClosure done;
    while (!g_stop) {
        int a = g_first_read_index.load(base::memory_order_relaxed);
        int b = g_last_read_index.load(base::memory_order_relaxed);
        EXPECT_LE(a, b);
        int index = base::fast_rand_in(a, b);
        done.reset();
        raft::LogEntry* entry = nullptr;
        storage->get_entry(index, &entry, &done);
        done.wait();
        if (entry != NULL) {
            std::string expect;
            base::string_printf(&expect, "hello_%d", index);
            EXPECT_EQ(expect, entry->data.to_string());
            entry->Release();
        } else {
            EXPECT_LT(index, storage->first_log_index())
                    << "first_read_index=" << g_first_read_index.load()
                    << " last_read_index=" << g_last_read_index.load()
                    << " a=" << a << " b=" << b;
            g_stop = true;
            return NULL;
        }
    }
    return NULL;
}

void* write_thread_routine(void* arg) {
    raft::SegmentLogStorage* storage = (raft::SegmentLogStorage*)arg;
    // Write operation distrubution:
    //  - 10% truncate_prefix
    //  - 10% truncate_suffix,
    //  - 30% increase last_read_index (which stands for commitment in the real
    // world),
    //  - 50% append new entry
    int next_log_index = storage->last_log_index() + 1;
    raft::SynchronizedClosure done;
    while (!g_stop) {
        const int r = base::fast_rand_in(0, 9);
        if (r < 1) {  // truncate_prefix
            int truncate_index = base::fast_rand_in(
                    g_first_read_index.load(base::memory_order_relaxed),
                    g_last_read_index.load(base::memory_order_relaxed));
            done.reset();
            storage->truncate_prefix(truncate_index, &done);
            done.wait();
            EXPECT_TRUE(done.status().ok());
            g_first_read_index.store(truncate_index, base::memory_order_relaxed);
        } else if (r < 2) {  // truncate suffix
            int truncate_index = base::fast_rand_in(
                    g_last_read_index.load(base::memory_order_relaxed),
                    next_log_index - 1);
            done.reset();
            storage->truncate_suffix(truncate_index, &done);
            done.wait();
            EXPECT_TRUE(done.status().ok());
            next_log_index = truncate_index + 1;
        } else if (r < 5) { // increase last_read_index which cannot be truncate
            int next_read_index = base::fast_rand_in(
                    g_last_read_index.load(base::memory_order_relaxed),
                    next_log_index - 1);
            g_last_read_index.store(next_read_index, base::memory_order_relaxed);
        } else  {  // Append entry
            raft::LogEntry* entry = new raft::LogEntry;
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.index = next_log_index;
            std::string data;
            base::string_printf(&data, "hello_%d", next_log_index);
            entry->data.append(data);
            ++next_log_index;
            done.reset();
            storage->append_entry(entry, &done);
            done.wait();
            EXPECT_TRUE(done.status().ok());
            entry->Release();
        }
    }
    return NULL;
}

namespace raft {
DECLARE_int32(raft_max_segment_size);
}

TEST_F(LogStorageTest, multi_read_single_modify_thread_safe) {
    int32_t saved_max_segment_size = raft::FLAGS_raft_max_segment_size;
    raft::FLAGS_raft_max_segment_size = 1024;
    system("rm -rf ./data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    const int N = 10000;
    for (int i = 1; i <= N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id.index = i;
        std::string data;
        base::string_printf(&data, "hello_%d", i);
        entry->data.append(data);
        raft::SynchronizedClosure done;
        storage->append_entry(entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        entry->Release();
    }
    ASSERT_EQ(N, storage->last_log_index());
    g_stop = false;
    g_first_read_index.store(1);
    g_last_read_index.store(N);
    bthread_t read_thread[8];
    for (size_t i = 0; i < ARRAY_SIZE(read_thread); ++i) {
        ASSERT_EQ(0, raft_start_urgent(&read_thread[i], NULL,
                                   read_thread_routine, storage));
    }
    bthread_t write_thread;
    ASSERT_EQ(0, raft_start_urgent(&write_thread, NULL,
                                      write_thread_routine, storage));
    ::usleep(5 * 1000 * 1000);
    g_stop = true;
    for (size_t i = 0; i < ARRAY_SIZE(read_thread); ++i) {
        bthread_join(read_thread[i], NULL);
    }
    bthread_join(write_thread, NULL);

    delete configuration_manager;
    delete storage;
    raft::FLAGS_raft_max_segment_size = saved_max_segment_size;
}

TEST_F(LogStorageTest, max_segment_size_illegal) {
    google::SetCommandLineOption("crash_on_fatal_log", "false");
    int32_t saved_max_segment_size = raft::FLAGS_raft_max_segment_size;
    raft::FLAGS_raft_max_segment_size = -1;
    system("rm -rf ./data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(-1, storage->init(configuration_manager));

    delete configuration_manager;
    delete storage;
    raft::FLAGS_raft_max_segment_size = saved_max_segment_size;
    google::SetCommandLineOption("crash_on_fatal_log", "true");
}

TEST_F(LogStorageTest, large_entry) {
    system("rm -rf ./data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    raft::LogEntry* entry = new raft::LogEntry;
    entry->type = raft::ENTRY_TYPE_DATA;
    entry->id.index = 1;
    entry->id.term = 1;
    std::string data;
    data.resize(512 * 1024 * 1024, 'a');
    entry->data.append(data);
    raft::SynchronizedClosure done;
    storage->append_entry(entry, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    entry->Release();
    done.reset();
    entry = nullptr;
    storage->get_entry(1, &entry, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_TRUE(entry);
    ASSERT_EQ(data, entry->data.to_string());
    entry->Release();

    ASSERT_EQ(1, storage->_first_log_index);
    ASSERT_EQ(1, storage->_last_log_index);
    ASSERT_EQ(0, storage->_segments.size());
    scoped_refptr<raft::Segment> segment = storage->open_segment();
    ASSERT_EQ(1, storage->_segments.size());

    raft::SegmentLogStorage* storage2 = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager2 = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage2->init(configuration_manager2));
    ASSERT_EQ(1, storage2->_first_log_index);
    ASSERT_EQ(1, storage2->_last_log_index);
    ASSERT_EQ(1, storage2->_segments.size());
}

TEST_F(LogStorageTest, reboot_with_checksum_type_changed) {
    system("rm -rf ./data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    storage->_checksum_type = 0;  // murmurhash
    raft::SynchronizedClosure done;
    for (int i = 0; i < 10; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id.term = 1;
        entry->id.index = i + 1;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        entry->data.append(data_buf);

        done.reset();
        storage->append_entry(entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        entry->Release();
    }
    delete storage;
    storage = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, storage->init(configuration_manager));
    storage->_checksum_type = 1;  // crc32
    for (int i = 10; i < 20; i++) {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id.term = 1;
        entry->id.index = i + 1;

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        entry->data.append(data_buf);

        done.reset();
        storage->append_entry(entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        entry->Release();
    }
    delete storage;
    storage = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, storage->init(configuration_manager));
    for (int index = 1; index <= 20; ++index) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        storage->get_entry(index, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    delete storage;
}

TEST_F(LogStorageTest, joint_configuration) {
    system("rm -rf ./data");
    raft::ConfigurationManager cm;
    std::unique_ptr<raft::SegmentLogStorage>
                log_storage(new raft::SegmentLogStorage("./data"));
    ASSERT_EQ(0, log_storage->init(&cm));
    raft::SynchronizedClosure done;
    for (int i = 1; i <= 20; ++i) {
        scoped_refptr<raft::LogEntry> entry = new raft::LogEntry;
        entry->id = raft::LogId(i, 1);
        entry->peers = new std::vector<raft::PeerId>;
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        for (int j = 0; j < 3; ++j) {
            entry->peers->push_back("127.0.0.1:" + to_string(i + j));
        }
        entry->old_peers = new std::vector<raft::PeerId>;
        for (int j = 1; j <= 3; ++j) {
            entry->old_peers->push_back("127.0.0.1:" + to_string(i + j));
        }
        done.reset();
        log_storage->append_entry(entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
    }

    for (int i = 1; i <= 20; ++i) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        log_storage->get_entry(i, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry != NULL);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_CONFIGURATION);
        ASSERT_TRUE(entry->peers != NULL);
        ASSERT_TRUE(entry->old_peers != NULL);
        raft::Configuration conf;
        for (int j = 0; j < 3; ++j) {
            conf.add_peer("127.0.0.1:" + to_string(i + j));
        }
        raft::Configuration old_conf;
        for (int j = 1; j <= 3; ++j) {
            old_conf.add_peer("127.0.0.1:" + to_string(i + j));
        }
        ASSERT_TRUE(conf.equals(*entry->peers))
            << conf << " xxxx " << raft::Configuration(*entry->peers);

        ASSERT_TRUE(old_conf.equals(*entry->old_peers));
        entry->Release();
    }

    // Restart
    log_storage.reset(new raft::SegmentLogStorage("./data"));
    ASSERT_EQ(0, log_storage->init(&cm));
    for (int i = 1; i <= 20; ++i) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        log_storage->get_entry(i, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry != NULL);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_CONFIGURATION);
        ASSERT_TRUE(entry->peers != NULL);
        ASSERT_TRUE(entry->old_peers != NULL);
        raft::Configuration conf;
        for (int j = 0; j < 3; ++j) {
            conf.add_peer("127.0.0.1:" + to_string(i + j));
        }
        raft::Configuration old_conf;
        for (int j = 1; j <= 3; ++j) {
            old_conf.add_peer("127.0.0.1:" + to_string(i + j));
        }
        ASSERT_TRUE(conf.equals(*entry->peers))
            << conf << " xxxx " << raft::Configuration(*entry->peers);

        ASSERT_TRUE(old_conf.equals(*entry->old_peers));
        entry->Release();
    }

    for (int i = 1; i <= 20; ++i) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        log_storage->get_entry(i, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry != NULL);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_CONFIGURATION);
        ASSERT_TRUE(entry->peers != NULL);
        ASSERT_TRUE(entry->old_peers != NULL);
        ASSERT_EQ(1, entry->id.term);
        raft::Configuration conf;
        for (int j = 0; j < 3; ++j) {
            conf.add_peer("127.0.0.1:" + to_string(i + j));
        }
        raft::Configuration old_conf;
        for (int j = 1; j <= 3; ++j) {
            old_conf.add_peer("127.0.0.1:" + to_string(i + j));
        }
        ASSERT_TRUE(conf.equals(*entry->peers));
        ASSERT_TRUE(old_conf.equals(*entry->old_peers));
        entry->Release();
    }

    for (int i = 1; i <= 20; ++i) {
        raft::ConfigurationEntry entry;
        cm.get(i, &entry);
        ASSERT_EQ(raft::LogId(i, 1), entry.id);
        raft::Configuration conf;
        for (int j = 0; j < 3; ++j) {
            conf.add_peer("127.0.0.1:" + to_string(i + j));
        }
        raft::Configuration old_conf;
        for (int j = 1; j <= 3; ++j) {
            old_conf.add_peer("127.0.0.1:" + to_string(i + j));
        }
        ASSERT_TRUE(conf.equals(entry.conf));
        ASSERT_TRUE(old_conf.equals(entry.old_conf));
    }
}

TEST_F(LogStorageTest, append_close_load_append_with_io_metric) {
    ::system("rm -rf data");
    raft::IOMetric metric;
    raft::FLAGS_raft_trace_append_entry_latency = true;
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    raft::SynchronizedClosure done;
    // append entry
    for (int i = 0; i < 100000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        done.reset();
        storage->append_entries(entries, &metric, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    ASSERT_NE(0, metric.open_segment_time_us);
    ASSERT_NE(0, metric.append_entry_time_us);
    ASSERT_NE(0, metric.sync_segment_time_us);

    LOG(NOTICE) << metric;

    delete storage;
    delete configuration_manager;

    // reinit
    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    // append entry
    for (int i = 100000; i < 200000; i++) {
        std::vector<raft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 2;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        done.reset();
        storage->append_entries(entries, &metric, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        for (size_t j = 0; j < entries.size(); j++) {
            delete entries[j];
        }
    }

    // check and read
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 200000*5);

    for (int i = 0; i < 200000*5; i++) {
        int64_t index = i + 1;
        done.reset();
        raft::LogEntry* entry = nullptr;
        storage->get_entry(index, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        if (i < 100000*5) {
            ASSERT_EQ(entry->id.term, 1);
        } else {
            ASSERT_EQ(entry->id.term, 2);
        }
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    delete storage;
    delete configuration_manager;
}

TEST_F(LogStorageTest, action_closure_no_deadlock) {
    scoped_refptr<raft::Segment> segment(new raft::Segment("_test_path", 100, 0));
    raft::Segment::PendingIoQueue io_queue(segment);
    base::IOBuf empty_data;
    raft::SynchronizedClosure done;
    raft::Segment::InFlightIoClosure* inflight =
        new raft::Segment::InFlightIoClosure(0, empty_data, &done);
    io_queue.pend(inflight);
    segment->_pending.emplace_back(std::move(io_queue));

    raft::SynchronizedClosure lock_done;
    raft::Segment::PendingIoQueue io_queue2(segment);
    auto lock_segment = [&](base::Status& st/*status*/) {
        LOG(INFO) << "lock calling";
        std::lock_guard lck(segment->_mutex);
        LOG(INFO) << "locked";
        lock_done.status() = st;
        lock_done.Run();
    };
    io_queue2.set_operation(-1, raft::create_raft_closure(std::move(lock_segment)));
    segment->_pending.emplace_back(std::move(io_queue2));
    segment->append_done(segment, inflight, base::Status(EIO, "mock io error"));
    done.wait();
    lock_done.wait();
    EXPECT_FALSE(done.status().ok());
    EXPECT_FALSE(lock_done.status().ok());
}

TEST_F(LogStorageTest, close_sync_segment) {
    raft::FLAGS_raft_sync_segments = true;
    ::system("rm -rf data");
    raft::IOMetric metric;
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));

    size_t appended = 0;
    // append entry
    raft::SynchronizedClosure done;
    while (true) {
        std::vector<raft::LogEntry*> entries;
        for (int i = 0; i < 5; i++) {
            int64_t index = 5*appended + i + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        done.reset();
        storage->append_entries(entries, &metric, &done);
        done.wait();
        EXPECT_TRUE(done.status().ok());

        for (size_t i = 0; i < entries.size(); i++) {
            delete entries[i];
        }
        appended ++;
        if (!storage->segments().empty()) {
            break;
        }
    }

    delete storage;
    delete configuration_manager;

    // reinit
    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));


    // check and read
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), appended*5);

    for (size_t i = 0; i < appended*5; i++) {
        int64_t index = i + 1;
        raft::LogEntry* entry = nullptr;
        done.reset();
        storage->get_entry(index, &entry, &done);
        done.wait();
        EXPECT_TRUE(done.status().ok());
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    delete storage;
    delete configuration_manager;
    raft::FLAGS_raft_sync_segments = false;
}

TEST_F(LogStorageTest, append_entries_sync_closed_segment) {
    ::system("rm -rf data");
    raft::IOMetric metric;
    raft::FLAGS_raft_trace_append_entry_latency = true;
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    raft::FLAGS_raft_max_segment_size = 1280;
    auto append_closed_segment  = [&](int start) {
        raft::SynchronizedClosure done;
        std::vector<raft::LogEntry*> entries;
        for (int i = 0; i < 128; i++) {
            int64_t index = start + i + 1;
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = index;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }
        storage->append_entries(entries, &metric, &done);
        for (size_t i = 0; i < entries.size(); i++) {
            delete entries[i];
        }
        return done.status();
    };
    {
        LOG(INFO) << "test raft_sync=false sync_segments=false";
        raft::FLAGS_raft_sync = false;
        raft::FLAGS_raft_sync_segments = false;
        auto pre_sync_count = raft::g_sync_segment_latency.count();
        ASSERT_TRUE(append_closed_segment(0).ok());
        auto now_sync_count = raft::g_sync_segment_latency.count();
        ASSERT_EQ(now_sync_count, pre_sync_count);
    }
    {
        LOG(INFO) << "test raft_sync=true sync_segments=false";
        raft::FLAGS_raft_sync = true;
        raft::FLAGS_raft_sync_segments = false;
        auto pre_sync_count = raft::g_sync_segment_latency.count();
        ASSERT_TRUE(append_closed_segment(128).ok());
        int expect_count = 1;
        raft::Segment* lst_ptr = nullptr;
        for (int i = 129; i <= 256; ++i) {
            scoped_refptr<raft::Segment> ptr;
            storage->get_segment(i, &ptr);
            if (ptr.get() != lst_ptr && !ptr->_is_open) {
                expect_count++;
            }
            lst_ptr = ptr.get();
        }
        do {
            if (expect_count + pre_sync_count <=
                    raft::g_sync_segment_latency.count()) {
                break;
            }
            bthread_usleep(1000);
        } while (true);
        ASSERT_EQ(expect_count + pre_sync_count,
                raft::g_sync_segment_latency.count());
    }
    {
        LOG(INFO) << "test raft_sync=false sync_segments=true";
        raft::FLAGS_raft_sync = false;
        raft::FLAGS_raft_sync_segments = true;
        auto pre_sync_count = raft::g_sync_segment_latency.count();
        ASSERT_TRUE(append_closed_segment(256).ok());
        int expect_count = 0;
        raft::Segment* lst_ptr = nullptr;
        for (int i = 257; i <= 384; ++i) {
            scoped_refptr<raft::Segment> ptr;
            storage->get_segment(i, &ptr);
            if (ptr.get() != lst_ptr && !ptr->_is_open) {
                expect_count++;
            }
            lst_ptr = ptr.get();
        }
        do {
            if (expect_count + pre_sync_count <=
                    raft::g_sync_segment_latency.count()) {
                break;
            }
            bthread_usleep(1000);
        } while (true);
        ASSERT_EQ(expect_count + pre_sync_count,
                raft::g_sync_segment_latency.count());
    }
    {
        LOG(INFO) << "test raft_sync=true sync_segments=true";
        raft::FLAGS_raft_sync = true;
        raft::FLAGS_raft_sync_segments = true;
        auto pre_sync_count = raft::g_sync_segment_latency.count();
        ASSERT_TRUE(append_closed_segment(384).ok());
        int expect_count = 1;
        raft::Segment* lst_ptr = nullptr;
        for (int i = 385; i <= 512; ++i) {
            scoped_refptr<raft::Segment> ptr;
            storage->get_segment(i, &ptr);
            if (ptr.get() != lst_ptr && !ptr->_is_open) {
                expect_count++;
            }
            lst_ptr = ptr.get();
        }
        do {
            if (expect_count + pre_sync_count <=
                    raft::g_sync_segment_latency.count()) {
                break;
            }
            bthread_usleep(1000);
        } while (true);
        ASSERT_EQ(expect_count + pre_sync_count,
                raft::g_sync_segment_latency.count());
    }

    delete storage;
    delete configuration_manager;
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
