/*
 * =====================================================================================
 *
 *       Filename:  test_polar_log.cpp
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2024/04/11 11:14:18
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Jingwei Ma majingwei@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include <base/atomicops.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#include <base/files/dir_reader_posix.h>
#include <base/string_printf.h>
#include <base/logging.h>
#include "raft/util.h"
#include "raft/log.h"
#include "raft/storage.h"
#include "raft/io_uring.h"
#include "raft/test/util.h"

#define LOG_RUN(expr) RAFT_SYNC_BTHREAD_RUN_ON_POLAR(expr, 0)

namespace baidu {
namespace hpn {
namespace polar {
DECLARE_string(lcore_mask);
DECLARE_string(pci_bus_list);
DECLARE_string(server_ports_list);
DECLARE_string(host_ip);
DECLARE_int32(client_ports_start);
DECLARE_int32(client_ports_end);
DECLARE_int32(client_ports_num);
DECLARE_string(socket_mem);
}
}
}
namespace raft {
DECLARE_bool(raft_trace_append_entry_latency);
DECLARE_bool(raft_use_polar);
DECLARE_bool(raft_use_iouring);
DECLARE_bool(raft_sync_segments);
}

class LogStorageTest : public testing::TestWithParam<bool> {
protected:
    void SetUp() {
        base::EndPoint addr;
        generate_random_addr(&addr);
        // google::SetCommandLineOption("crash_on_fatal_log", "true");
        google::SetCommandLineOption("verbose", "99");
        google::SetCommandLineOption("min_log_level", "0");
        raft::FLAGS_raft_sync = false;
        raft::FLAGS_raft_sync_segments = GetParam();
        raft::FLAGS_raft_use_polar = true;
        baidu::hpn::polar::FLAGS_lcore_mask = "0x0F0000";
        baidu::hpn::polar::FLAGS_host_ip = base::ip2str(addr.ip).c_str();
        baidu::hpn::polar::FLAGS_server_ports_list = std::to_string(addr.port);
        baidu::hpn::polar::FLAGS_client_ports_start = 9100;
        baidu::hpn::polar::FLAGS_client_ports_end = 9200;
        baidu::hpn::polar::FLAGS_client_ports_num = 50;
        baidu::hpn::polar::FLAGS_socket_mem = "512,512";
        rpc::ServerOptions server_options;
        server_options.use_polar = raft::FLAGS_raft_use_polar;
        ASSERT_EQ(_server.Start(addr, &server_options), 0);
        // mock uring operations to support ut on lower platform
        raft::FLAGS_raft_use_iouring = false;
        raft::SynchronizedClosure done;
        // some platform not support iouring, we need pthread worker
        auto l = [&] {
            scoped_refptr<raft::IOWorker> worker = raft::g_raft_io_man->get_or_create_worker();
            if (!worker) {
                done.status().set_error(EIO, "create worker error");
            }
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        raft::FLAGS_raft_use_iouring = true;
        raft::g_raft_io_man->_stopped = false;
    }
    void TearDown() {}
private:
    rpc::Server _server;
};

inline std::string to_string(int i) {
    std::stringstream ss;
    ss << i;
    return ss.str();
}

TEST_P(LogStorageTest, open_segment) {
    // open segment operation
    scoped_refptr<raft::Segment> seg1;
    raft::SynchronizedClosure done;
    raft::LogEntry* entry = nullptr;
    auto l = [&] {
        ::system("mkdir data/");
        seg1 = new raft::Segment("./data", 1L, 0);

        // not open
        seg1->get(1, &entry, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_FALSE(done.status().ok());

    size_t entry_num = 10;
    done.reset();
    auto l2 = [&] {
        // create and open
        ASSERT_EQ(0, seg1->create());
        ASSERT_TRUE(seg1->is_open());

        raft::ClosureGroup* cg = new raft::ClosureGroup(entry_num, &done);
        // append entry
        for (int i = 0; i < entry_num; i++) {
            raft::LogEntry* entry = new raft::LogEntry();
            entry->AddRef();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = i + 1;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
            entry->data.append(data_buf);

            seg1->append(entry, nullptr, cg->sub_done(i));
            entry->Release();
            LOG(NOTICE) << "append: " << i << " last_index: " << seg1->last_index()
                << " seg: " << seg1;
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok()) << done.status();

    // read entry
    for (int i = 0; i < entry_num; i++) {
        int64_t term = LOG_RUN(seg1->get_term(i+1));
        ASSERT_EQ(term, 1);

        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            seg1->get(i+1, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry != nullptr);
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, i+1);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    done.reset();
    auto l3 = [&] {
        seg1->get(0, &entry, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), 0);
    done.wait();
    ASSERT_FALSE(done.status().ok());

    done.reset();
    auto l4 = [&] {
        seg1->get(11, &entry, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l4)), 0);
    done.wait();
    ASSERT_FALSE(done.status().ok());

    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    // load open segment
    scoped_refptr<raft::Segment> seg2;
    auto new1 = [&] { seg2 = new raft::Segment("./data", 1, 0); };
    LOG_RUN(new1());
    ASSERT_EQ(0, seg2->load(configuration_manager));

    for (int i = 0; i < entry_num; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            seg2->get(i+1, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
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
    done.reset();
    auto l5 = [&] {
        seg2->get(0, &entry, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l5)), 0);
    done.wait();
    ASSERT_FALSE(done.status().ok());

    done.reset();
    auto l6 = [&] {
        seg2->get(11, &entry, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l6)), 0);
    done.wait();
    ASSERT_FALSE(done.status().ok());

    const size_t truncate_left = 5;
    // truncate and read
    done.reset();
    auto l7 = [&] {
        seg1->truncate(truncate_left, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l7)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    const size_t append_num = entry_num - truncate_left;
    done.reset();
    auto l8 = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(append_num, &done);
        for (int i = 0; i < append_num; i++) {
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = i + truncate_left + 1;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 6);
            entry->data.append(data_buf);

            seg1->append(entry, nullptr, cg->sub_done(i));
            entry->Release();
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l8)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    for (int i = 0; i < entry_num; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            seg1->get(i+1, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
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

TEST_P(LogStorageTest, closed_segment) {
    raft::SynchronizedClosure done;
    size_t entry_num = 10;
    scoped_refptr<raft::Segment> seg1;
    auto l = [&] {
        // open segment operation
        seg1 = new raft::Segment("./data", 1L, 0);
        ASSERT_EQ(0, seg1->create());
        ASSERT_TRUE(seg1->is_open());
        raft::ClosureGroup* cg = new raft::ClosureGroup(entry_num, &done);
        // append entry
        for (int i = 0; i < entry_num; i++) {
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = i + 1;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
            entry->data.append(data_buf);

            seg1->append(entry, nullptr, cg->sub_done(i));

            entry->Release();
        }
        seg1->close();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    // read entry
    for (int i = 0; i < entry_num; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            seg1->get(i+1, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
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
        auto ll = [&] {
            seg1->get(0, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        ASSERT_FALSE(done.status().ok());

        done.reset();
        auto ll2 = [&] {
            seg1->get(11, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll2)), 0);
        done.wait();
        ASSERT_FALSE(done.status().ok());
    }

    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    // load open segment
    scoped_refptr<raft::Segment> seg2;
    auto new1 = [&] { seg2 = new raft::Segment("./data", 1, 10, 0); };
    LOG_RUN(new1());
    ASSERT_EQ(0, LOG_RUN(seg2->load(configuration_manager)));

    for (int i = 0; i < entry_num; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            seg2->get(i+1, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
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
        auto ll = [&] {
            seg2->get(0, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        ASSERT_FALSE(done.status().ok());

        done.reset();
        auto ll2 = [&] {
            seg2->get(11, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll2)), 0);
        done.wait();
        ASSERT_FALSE(done.status().ok());
    }

    // truncate and read
    raft::SynchronizedClosure truncate_done;
    raft::SynchronizedClosure append_done;
    const size_t truncate_left = 5;
    auto l2 = [&] {
        seg1->truncate(truncate_left, &truncate_done);
        size_t append_num = entry_num - truncate_left;
        raft::ClosureGroup* cg = new raft::ClosureGroup(append_num, &append_done);
        for (int i = 0; i < append_num; i++) {
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = i + truncate_left + 1;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "HELLO, WORLD: %d", i + 6);
            entry->data.append(data_buf);

            // become open segment again
            seg1->append(entry, nullptr, cg->sub_done(i));

            entry->Release();
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    truncate_done.wait();
    ASSERT_TRUE(truncate_done.status().ok());
    append_done.wait();
    ASSERT_TRUE(append_done.status().ok());

    for (int i = 0; i < entry_num; i++) {
        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            seg1->get(i+1, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
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
    ASSERT_EQ(0, LOG_RUN(seg1->unlink()));

    delete configuration_manager;
}

TEST_P(LogStorageTest, multi_segment_and_segment_logstorage) {
    raft::SynchronizedClosure done;

    ::system("rm -rf data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    // init
    ASSERT_EQ(0, LOG_RUN(storage->init(new raft::ConfigurationManager())));
    ASSERT_EQ(1, storage->first_log_index());
    ASSERT_EQ(0, storage->last_log_index());

    const size_t round = 100000;
    const size_t entry_per_round = 5;
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &done);
        // append entry
        for (int i = 0; i < round; i++) {
            std::vector<raft::LogEntry*> entries;
            for (int j = 0; j < entry_per_round; j++) {
                int64_t index = entry_per_round*i + j + 1;
                raft::LogEntry* entry = new raft::LogEntry();
                entry->type = raft::ENTRY_TYPE_DATA;
                entry->id.term = 1;
                entry->id.index = index;

                char data_buf[128];
                snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
                entry->data.append(data_buf);
                entries.push_back(entry);
            }

            storage->append_entries(entries, NULL, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                entries[j]->Release();
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    size_t entry_num = round * entry_per_round;
    // read entry
    for (int i = 0; i < entry_num; i++) {
        int64_t index = i + 1;
        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            storage->get_entry(index, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), entry_num);

    done.reset();
    // truncate prefix
    auto l2 = [&] { storage->truncate_prefix(10001, &done); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(storage->first_log_index(), 10001);
    ASSERT_EQ(storage->last_log_index(), 500000);

    done.reset();
    // boundary truncate prefix
    raft::SegmentLogStorage::SegmentMap& segments1 = storage->segments();
    size_t old_segment_num = segments1.size();
    raft::Segment* first_seg = segments1.begin()->second.get();
    auto l3 = [&] {
        storage->truncate_prefix(first_seg->last_index(), &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    raft::SegmentLogStorage::SegmentMap& segments2 = storage->segments();
    ASSERT_EQ(old_segment_num, segments2.size());

    done.reset();
    auto l31 = [&] {
        storage->truncate_prefix(first_seg->last_index() + 1, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l31)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    raft::SegmentLogStorage::SegmentMap& segments3 = storage->segments();
    ASSERT_EQ(old_segment_num - 1, segments3.size());

    done.reset();
    auto l4 = [&] { storage->truncate_prefix(250001, &done); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l4)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(storage->first_log_index(), 250001);
    ASSERT_EQ(storage->last_log_index(), 500000);
    for (int i = 250001; i <= 500000; i++) {
        int64_t index = i;
        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            storage->get_entry(index, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    done.reset();
    int start = 100000;
    int end = 200000;
    auto l5 = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(end - start, &done);
        // append
        for (int i = start; i < end; i++) {
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

            storage->append_entries(entries, NULL, cg->sub_done(i - start));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l5)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(250001, storage->first_log_index());
    ASSERT_EQ(1000000, storage->last_log_index());

    done.reset();
    // truncate suffix
    auto l6 = [&] { storage->truncate_suffix(750000, &done); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l6)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(250001, storage->first_log_index());
    ASSERT_EQ(750000, storage->last_log_index());

    done.reset();
    // boundary truncate suffix
    auto l7 = [&] {
        raft::SegmentLogStorage::SegmentMap& segments1 = storage->segments();
        first_seg = segments1.begin()->second.get();
        if (segments1.size() > 1) {
            storage->truncate_suffix(first_seg->last_index() + 1, &done);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l7)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    raft::SegmentLogStorage::SegmentMap& segments21 = storage->segments();
    for (auto s : segments21) {
        LOG(NOTICE) << "left seg: " << s.second.get() << " first index: " << s.second->first_index();
    }
    ASSERT_EQ(1, segments21.size());
    ASSERT_TRUE(storage->_open_segment != nullptr);
    ASSERT_EQ(storage->last_log_index(), first_seg->last_index() + 1);
    done.reset();
    auto l8 = [&] {
        storage->truncate_suffix(first_seg->last_index(), &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l8)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    raft::SegmentLogStorage::SegmentMap& segments31 = storage->segments();
    ASSERT_EQ(1ul, segments31.size());
    ASSERT_EQ(storage->last_log_index(), first_seg->last_index());

    // read
    for (int i = 250001; i <= storage->last_log_index(); i++) {
        int64_t index = i;
        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            storage->get_entry(index, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
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
    ASSERT_EQ(0, LOG_RUN(storage2->init(new raft::ConfigurationManager())));
    ASSERT_EQ(1, storage2->first_log_index());
    ASSERT_EQ(0, storage2->last_log_index());
    delete storage2;
}

TEST_P(LogStorageTest, append_close_load_append) {
    ::system("rm -rf data");
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));
    raft::SynchronizedClosure done;
    const size_t round = 100000;
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &done);
        // append entry
        for (int i = 0; i < round; i++) {
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

            storage->append_entries(entries, NULL, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    delete storage;
    delete configuration_manager;

    // reinit
    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    done.reset();
    auto l2 = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(100000, &done);
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

            storage->append_entries(entries, NULL, cg->sub_done(i-100000));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    // check and read
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 200000*5);

    for (int i = 0; i < 200000*5; i++) {
        int64_t index = i + 1;
        done.reset();
        raft::LogEntry* entry = nullptr;
        auto ll = [&] {
            storage->get_entry(index, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
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

TEST_P(LogStorageTest, data_lost) {
    ::system("rm -rf data");
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    raft::SynchronizedClosure done;
    const size_t round = 100000;
    // append entry
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &done);
        for (int i = 0; i < round; i++) {
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

            storage->append_entries(entries, NULL, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    delete storage;
    delete configuration_manager;

    // reinit
    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

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
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 100000*5 - 1);

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
    ASSERT_NE(0, LOG_RUN(storage->init(configuration_manager)));

    delete storage;
    delete configuration_manager;
}

TEST_P(LogStorageTest, full_segment_has_garbage) {
    ::system("rm -rf data");
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    raft::SynchronizedClosure done;
    const size_t round = 100000;
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &done);
        // append entry
        for (int i = 0; i < round; i++) {
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

            storage->append_entries(entries, NULL, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

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
    ASSERT_NE(0, LOG_RUN(storage->init(configuration_manager)));
}

TEST_P(LogStorageTest, DISABLED_append_read_badcase) {
    ::system("rm -rf data");
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    raft::SynchronizedClosure done;
    const size_t round = 100000;
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &done);
    // append entry
        for (int i = 0; i < round; i++) {
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

            storage->append_entries(entries, NULL, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

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
    ASSERT_NE(0, LOG_RUN(storage->init(configuration_manager)));
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
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

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
        auto ll = [&] {
            storage->get_entry(index, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        if (entry) {
            entry->Release();
        }
    }

    delete storage;
    delete configuration_manager;
}

TEST_P(LogStorageTest, configuration) {
    raft::SynchronizedClosure done;
    ::system("rm -rf data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));
    auto l = [&] {
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_NO_OP;
        entry.id.term = 1;
        entry.id.index = 1;
        storage->append_entry(&entry, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    done.reset();
    // add peer
    auto l2 = [&] {
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_CONFIGURATION;
        entry.id.term = 1;
        entry.id.index = 2;
        entry.peers = new std::vector<raft::PeerId>;
        entry.peers->push_back(raft::PeerId("1.1.1.1:1000:0"));
        entry.peers->push_back(raft::PeerId("1.1.1.1:2000:0"));
        entry.peers->push_back(raft::PeerId("1.1.1.1:3000:0"));
        storage->append_entry(&entry, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    done.reset();
    size_t round = 100000;
    // append entry
    auto l3 = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &done);
        for (int i = 0; i < round; i++) {
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
            storage->append_entries(entries, NULL, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    done.reset();
    // remove peer
    auto l4 = [&] {
        int64_t index = 2 + 100000*5 + 1;
        raft::LogEntry entry;
        entry.type = raft::ENTRY_TYPE_CONFIGURATION;
        entry.id.term = 1;
        entry.id.index = index;
        entry.peers = new std::vector<raft::PeerId>;
        entry.peers->push_back(raft::PeerId("1.1.1.1:1000:0"));
        entry.peers->push_back(raft::PeerId("1.1.1.1:2000:0"));
        storage->append_entry(&entry, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l4)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    delete storage;

    raft::SegmentLogStorage* storage2 = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, LOG_RUN(storage2->init(configuration_manager)));

    raft::ConfigurationEntry pair;
    configuration_manager->get(2 + 100000*5, &pair);
    ASSERT_EQ(2, pair.id.index);
    LOG(NOTICE) << pair.conf;

    configuration_manager->get(2 + 100000*5 + 1, &pair);
    ASSERT_EQ(2+100000*5+1, pair.id.index);
    LOG(NOTICE) << pair.conf;

    done.reset();
    auto l5 = [&] {storage2->truncate_suffix(400000, &done);};
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l5)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    configuration_manager->get(400000, &pair);
    ASSERT_EQ(2, pair.id.index);

    done.reset();
    auto l6 = [&] {storage2->truncate_prefix(2, &done);};
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l6)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    configuration_manager->get(400000, &pair);
    ASSERT_EQ(2, pair.id.index);

    delete storage2;
}

namespace raft {
DECLARE_int32(raft_max_segment_size);
}

TEST_P(LogStorageTest, max_segment_size_illegal) {
    int32_t saved_max_segment_size = raft::FLAGS_raft_max_segment_size;
    raft::FLAGS_raft_max_segment_size = -1;
    system("rm -rf ./data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(-1, LOG_RUN(storage->init(configuration_manager)));

    delete configuration_manager;
    delete storage;
    raft::FLAGS_raft_max_segment_size = saved_max_segment_size;
}

TEST_P(LogStorageTest, large_entry) {
    system("rm -rf ./data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));
    raft::SynchronizedClosure done;
    raft::LogEntry* entry = nullptr;
    std::string data;
    auto l = [&] {
        entry = new raft::LogEntry;
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id.index = 1;
        entry->id.term = 1;
        data.resize(512 * 1024 * 1024, 'a');
        entry->data.append(data);
        storage->append_entry(entry, &done);
        entry->Release();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    done.reset();
    auto ll = [&] {
        storage->get_entry(1, &entry, &done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(data, entry->data.to_string());
    entry->Release();

    ASSERT_EQ(1, storage->_first_log_index);
    ASSERT_EQ(1, storage->_last_log_index);
    ASSERT_EQ(0, storage->_segments.size());
    scoped_refptr<raft::Segment> segment;
    auto new1 = [&] { segment = storage->open_segment(); };
    LOG_RUN(new1());
    ASSERT_EQ(1, storage->_segments.size());

    raft::SegmentLogStorage* storage2 = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager2 = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage2->init(configuration_manager2)));
    ASSERT_EQ(1, storage2->_first_log_index);
    ASSERT_EQ(1, storage2->_last_log_index);
    ASSERT_EQ(1, storage2->_segments.size());
}

TEST_P(LogStorageTest, reboot_with_checksum_type_changed) {
    system("rm -rf ./data");
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));
    raft::SynchronizedClosure done;
    const size_t entry_num = 10;
    storage->_checksum_type = 0;  // murmurhash
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(entry_num, &done);
        for (int i = 0; i < entry_num; i++) {
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = i + 1;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
            entry->data.append(data_buf);

            storage->append_entry(entry, cg->sub_done(i));

            entry->Release();
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    delete storage;
    storage = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));
    storage->_checksum_type = 1;  // crc32
    done.reset();
    auto l2 = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(10, &done);
        for (int i = 10; i < 20; i++) {
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = i + 1;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %d", i + 1);
            entry->data.append(data_buf);

            storage->append_entry(entry, cg->sub_done(i-10));

            entry->Release();
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    delete storage;
    storage = new raft::SegmentLogStorage("./data");
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));
    for (int index = 1; index <= 20; ++index) {
        raft::LogEntry* entry = nullptr;
        done.reset();
        auto ll = [&] {
            storage->get_entry(index, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry);
        if (!entry) {
            // ^^ make bug bye happy
            continue;
        }
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

TEST_P(LogStorageTest, joint_configuration) {
    system("rm -rf ./data");
    raft::ConfigurationManager cm;
    std::unique_ptr<raft::SegmentLogStorage>
                log_storage(new raft::SegmentLogStorage("./data"));
    ASSERT_EQ(0, LOG_RUN(log_storage->init(&cm)));
    raft::SynchronizedClosure done;
    const size_t entry_num = 20;
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(entry_num, &done);
        for (int i = 1; i <= entry_num; ++i) {
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
            log_storage->append_entry(entry, cg->sub_done(i-1));
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    for (int i = 1; i <= 20; ++i) {
        raft::LogEntry* entry = nullptr;
        done.reset();
        auto ll = [&] {
            log_storage->get_entry(i, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
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
    ASSERT_EQ(0, LOG_RUN(log_storage->init(&cm)));
    for (int i = 1; i <= 20; ++i) {
        raft::LogEntry* entry = nullptr;
        done.reset();
        auto ll = [&] {
            log_storage->get_entry(i, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
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
        raft::LogEntry* entry = nullptr;
        done.reset();
        auto ll = [&] {
            log_storage->get_entry(i, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry);
        if (!entry) {
            // ^^ make bug bye happy
            continue;
        }
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

TEST_P(LogStorageTest, append_close_load_append_with_io_metric) {
    ::system("rm -rf data");
    raft::IOMetric metric;
    raft::FLAGS_raft_trace_append_entry_latency = true;
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    // append entry
    raft::SynchronizedClosure done;
    const size_t round = 100000;
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &done);
        for (int i = 0; i < round; i++) {
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

            storage->append_entries(entries, &metric, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    EXPECT_NE(0, metric.open_segment_time_us);
    EXPECT_NE(0, metric.append_entry_time_us);
    EXPECT_NE(0, metric.sync_segment_time_us);

    LOG(NOTICE) << metric;

    delete storage;
    delete configuration_manager;

    // reinit
    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    // append entry
    done.reset();
    auto l2 = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &done);
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

            storage->append_entries(entries, &metric, cg->sub_done(i-100000));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    // check and read
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 200000*5);

    for (int i = 0; i < 200000*5; i++) {
        int64_t index = i + 1;
        raft::LogEntry* entry = nullptr;
        done.reset();
        auto ll = [&] {
            storage->get_entry(index, &entry, &done);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry != NULL);
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

class MockUringWorker : public raft::PthreadWorker {
public:
    MockUringWorker(int32_t id, raft::IOWorker* impl) :
        raft::PthreadWorker(id), _impl(impl), _run(true) {}
    virtual void read(int fd, off_t offset, base::IOPortal* portal,
            size_t size, raft::Closure* done) override {
        if (_run) {
            return _impl->read(fd, offset, portal, size, done);
        }
        raft::PthreadWorker::read(fd, offset, portal, size, done);
    }

    virtual void write(int fd, off_t offset,
            const base::IOBuf& data, raft::Closure* done) override {
        if (_run) {
            return _impl->write(fd, offset, data, done);
        }
        raft::PthreadWorker::write(fd, offset, data, done);
    }

    void resume_io() {
        std::lock_guard<std::mutex> guard(_mutex);
        _run = true;
        for (auto& task : _queue) {
            if (task.type == raft::PthreadWorker::READ) {
                _impl->read(task.fd, task.offset, task.read_buf_ptr, task.size, task.done);
                continue;
            }
            _impl->write(task.fd, task.offset, task.write_buf, task.done);
        }
        _queue.clear();
    }
private:
    scoped_refptr<raft::IOWorker> _impl;
    bool _run;
};

TEST_P(LogStorageTest, read_inflight_entry) {
    ::system("rm -rf data");
    raft::IOMetric metric;
    raft::FLAGS_raft_trace_append_entry_latency = true;
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    raft::SynchronizedClosure done;
    MockUringWorker* worker;
    // replace UringWorker with MockUringWorker
    auto r = [&] {
        int pick_worker_index = -1;
        ASSERT_TRUE(baidu::hpn::polar::running_in_poll_thread(&pick_worker_index));
        scoped_refptr<raft::IOWorker> real_worker = raft::g_raft_io_man->get_or_create_worker();
        ASSERT_NE(real_worker.get(), nullptr);
        worker = new MockUringWorker(pick_worker_index, real_worker);
        worker->_run = false; // make io not finish
        auto modify = [&] (raft::IOManager::WorkerMap& m) {
            m.erase(pick_worker_index);
            CHECK(m.insert(std::make_pair(pick_worker_index, worker)).second);
            return 1lu;
        };
        ASSERT_EQ(raft::g_raft_io_man->_workers.Modify(modify), 1);
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(r)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    // append entry
    done.reset();
    raft::SynchronizedClosure append_done;
    const size_t round = 100000;
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &append_done);
        for (int i = 0; i < round; i++) {
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

            storage->append_entries(entries, &metric, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    for (int i = 0; i < round*5; i++) {
        int64_t index = i + 1;
        raft::LogEntry* entry = nullptr;
        done.reset();
        LOG_RUN(storage->get_entry(index, &entry, &done));
        ASSERT_TRUE(done.status().ok());
        ASSERT_NE(entry, nullptr);
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    int64_t truncate_index = 5 * round / 2 + 1; // truncate half
    done.reset();
    raft::SynchronizedClosure truncate_done;
    auto l3 = [&] {
        storage->truncate_suffix(truncate_index, &truncate_done);
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    for (int i = 0; i < truncate_index + 10; i++) {
        int64_t index = i + 1;
        raft::LogEntry* entry = nullptr;
        done.reset();
        LOG_RUN(storage->get_entry(index, &entry, &done));
        if (index > truncate_index) {
            ASSERT_EQ(entry, nullptr) << index;
            continue;
        }
        ASSERT_NE(entry, nullptr) << index;
        ASSERT_EQ(entry->id.term, 1);
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    done.reset();
    raft::SynchronizedClosure append2_done;
    auto l5 = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &append2_done);
        for (int i = 0; i < round; i++) {
            std::vector<raft::LogEntry*> entries;
            for (int j = 0; j < 5; j++) {
                int64_t index = 5*i + j + truncate_index + 1;
                raft::LogEntry* entry = new raft::LogEntry();
                entry->type = raft::ENTRY_TYPE_DATA;
                entry->id.term = 2;
                entry->id.index = index;

                char data_buf[128];
                snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
                entry->data.append(data_buf);
                entries.push_back(entry);
            }

            storage->append_entries(entries, &metric, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l5)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    int64_t truncate_index2 = 5 * round / 2 + 10; // truncate half
    done.reset();
    raft::SynchronizedClosure truncate2_done;
    auto l6 = [&] {
        storage->truncate_suffix(truncate_index2, &truncate2_done);
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l6)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    for (int i = 0; i < truncate_index2 + 10; i++) {
        int64_t index = i + 1;
        raft::LogEntry* entry = nullptr;
        done.reset();
        LOG_RUN(storage->get_entry(index, &entry, &done));
        if (index > truncate_index2) {
            ASSERT_EQ(entry, nullptr) << index;
            continue;
        }
        ASSERT_TRUE(done.status().ok());
        ASSERT_NE(entry, nullptr) << index;
        if (index <= truncate_index) {
            EXPECT_EQ(entry->id.term, 1);
        } else {
            EXPECT_EQ(entry->id.term, 2);
        }
        ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }

    int64_t truncate_index3 = round;
    done.reset();
    raft::SynchronizedClosure truncate3_done;
    auto l8 = [&] {
        storage->truncate_suffix(truncate_index3, &truncate3_done);
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l8)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    done.reset();
    raft::SynchronizedClosure append3_done;
    auto l9 = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &append3_done);
        for (int i = 0; i < round; i++) {
            std::vector<raft::LogEntry*> entries;
            for (int j = 0; j < 5; j++) {
                int64_t index = 5*i + j + truncate_index3 + 1;
                raft::LogEntry* entry = new raft::LogEntry();
                entry->type = raft::ENTRY_TYPE_DATA;
                entry->id.term = 3;
                entry->id.index = index;

                char data_buf[128];
                snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
                entry->data.append(data_buf);
                entries.push_back(entry);
            }

            storage->append_entries(entries, &metric, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l9)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    auto check_data = [&](std::string output) {
        for (int i = 0; i < truncate_index3 + 5 * round + 10; i++) {
            int64_t index = i + 1;
            raft::LogEntry* entry = nullptr;
            done.reset();
            LOG_RUN(storage->get_entry(index, &entry, &done));
            done.wait();
            if (index > truncate_index3 + round * 5) {
                ASSERT_EQ(entry, nullptr) << index;
                continue;
            }
            ASSERT_NE(entry, nullptr) << index;
            if (index <= truncate_index3) {
                EXPECT_EQ(entry->id.term, 1);
            } else {
                EXPECT_EQ(entry->id.term, 3);
            }
            ASSERT_EQ(entry->type, raft::ENTRY_TYPE_DATA);
            ASSERT_EQ(entry->id.index, index);

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
            ASSERT_EQ(data_buf, entry->data.to_string());
            entry->Release();
        }
        LOG(NOTICE) << "check_data " << output;
    };
    check_data("1");

    // resume io
    done.reset();
    auto l12 = [&] {
        scoped_refptr<MockUringWorker> worker = dynamic_cast<MockUringWorker*>(
                raft::g_raft_io_man->get_or_create_worker().get());
        if (!worker) {
            done.status().set_error(EIO, "create worker error");
        }
        EXPECT_FALSE(worker->_queue.empty());
        worker->resume_io();
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l12)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    LOG(NOTICE) << "resume io";

    append_done.wait();
    EXPECT_TRUE(append_done.status().ok()) << append_done.status();
    LOG(NOTICE) << "append_done";
    truncate_done.wait();
    EXPECT_TRUE(truncate_done.status().ok()) << truncate_done.status();
    LOG(NOTICE) << "truncate_done";
    append2_done.wait();
    EXPECT_TRUE(append2_done.status().ok()) << append2_done.status();
    LOG(NOTICE) << "append2_done";
    truncate2_done.wait();
    EXPECT_TRUE(truncate2_done.status().ok()) << truncate2_done.status();
    LOG(NOTICE) << "truncate2_done";
    truncate3_done.wait();
    EXPECT_TRUE(truncate3_done.status().ok()) << truncate3_done.status();
    LOG(NOTICE) << "truncate3_done";
    append3_done.wait();
    EXPECT_TRUE(append3_done.status().ok()) << append3_done.status();
    LOG(NOTICE) << "append3_done";

    check_data("2"); // check data again

    delete storage;
    delete configuration_manager;
}

TEST_P(LogStorageTest, truncate_on_hole) {
    ::system("rm -rf data");
    raft::IOMetric metric;
    raft::FLAGS_raft_trace_append_entry_latency = true;
    raft::SegmentLogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    raft::SynchronizedClosure done;
    const size_t round = 100000;
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &done);
        for (int i = 0; i < round; i++) {
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

            storage->append_entries(entries, &metric, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    // generate holes
    scoped_refptr<raft::Segment> open_segment = storage->_open_segment;
    ASSERT_GT(open_segment->_offset_and_term.size(), 2);
    size_t index = open_segment->_offset_and_term.size() / 2;
    off_t offset = open_segment->_offset_and_term[index].first;
    off_t next_offset = open_segment->_offset_and_term[index+1].first;
    size_t size = next_offset - offset;
    ASSERT_EQ(fallocate(open_segment->_fd,
        FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
        offset, size), 0);

    delete storage;
    delete configuration_manager;

    storage = new raft::SegmentLogStorage("./data");
    configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    delete storage;
    delete configuration_manager;
}

TEST_P(LogStorageTest, log_id) {
    ::system("rm -rf data");
    raft::IOMetric metric;
    raft::FLAGS_raft_trace_append_entry_latency = true;
    raft::LogStorage* storage = new raft::SegmentLogStorage("./data");
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ASSERT_EQ(0, LOG_RUN(storage->init(configuration_manager)));

    raft::SynchronizedClosure done;
    MockUringWorker* worker;
    // replace UringWorker with MockUringWorker
    auto r = [&] {
        int pick_worker_index = -1;
        ASSERT_TRUE(baidu::hpn::polar::running_in_poll_thread(&pick_worker_index));
        scoped_refptr<raft::IOWorker> real_worker = raft::g_raft_io_man->get_or_create_worker();
        ASSERT_NE(real_worker.get(), nullptr);
        worker = new MockUringWorker(pick_worker_index, real_worker);
        worker->_run = false; // make io not finish
        auto modify = [&] (raft::IOManager::WorkerMap& m) {
            m.erase(pick_worker_index);
            CHECK(m.insert(std::make_pair(pick_worker_index, worker)).second);
            return 1lu;
        };
        ASSERT_EQ(raft::g_raft_io_man->_workers.Modify(modify), 1);
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(r)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());

    // append entry
    done.reset();
    raft::SynchronizedClosure append_done(3); // append entries, last_log_id, append_entries
    const size_t round = 100;
    int64_t index = 1;
    auto l = [&] {
        raft::ClosureGroup* cg = new raft::ClosureGroup(round, &append_done);
        for (int i = 0; i < round; i++) {
            std::vector<raft::LogEntry*> entries;
            for (int j = 0; j < 5; j++) {
                raft::LogEntry* entry = new raft::LogEntry();
                entry->type = raft::ENTRY_TYPE_DATA;
                entry->id.term = 1;
                entry->id.index = index;

                char data_buf[128];
                snprintf(data_buf, sizeof(data_buf), "hello, world: %ld", index);
                entry->data.append(data_buf);
                entries.push_back(entry);
                index ++;
            }

            storage->append_entries(entries, &metric, cg->sub_done(i));

            for (size_t j = 0; j < entries.size(); j++) {
                delete entries[j];
            }
        }
        done.Run();
    };
    auto ll = l;
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    raft::LogId log_id;
    auto l2 = [&] {
        storage->last_log_id(&log_id, &append_done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    done.reset();
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(ll)), 0); // append more entries
    done.wait();
    auto l12 = [&] {
        scoped_refptr<MockUringWorker> worker = dynamic_cast<MockUringWorker*>(
                raft::g_raft_io_man->get_or_create_worker().get());
        if (!worker) {
            done.status().set_error(EIO, "create worker error");
        }
        EXPECT_FALSE(worker->_queue.empty());
        worker->resume_io();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l12)), 0);
    append_done.wait();
    ASSERT_TRUE(append_done.status().ok());
    LOG(NOTICE) << "last_log_id: " << log_id;
    delete storage;
    delete configuration_manager;
}

INSTANTIATE_TEST_CASE_P(SyncSegments, LogStorageTest, testing::Bool());

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
