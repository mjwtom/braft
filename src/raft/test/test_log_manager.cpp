// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/24 16:30:49

#include <gtest/gtest.h>

#include <base/memory/scoped_ptr.h>
#include <base/string_printf.h>
#include <base/macros.h>

#include <bthread/countdown_event.h>
#include "raft/log_manager.h"
#include "raft/configuration.h"
#include "raft/log.h"
#include "raft/fsm_caller.h"
#include "raft/test/util.h"

namespace raft {
DECLARE_bool(raft_use_iouring);
DECLARE_int32(raft_node_cached_log_num);
}

class LogManagerTest : public testing::Test {
protected:
    LogManagerTest() {}
    void SetUp() {
        const ::testing::TestInfo* const test_info = 
            ::testing::UnitTest::GetInstance()->current_test_info();
        LOG(NOTICE) << "We are in " << test_info->test_case_name()
            << "." << test_info->name();
    }
    void TearDown() {}
};

class StuckClosure : public raft::LogManager::StableClosure {
public:
    StuckClosure()
        : _stuck(NULL)
        , _expected_next_log_index(NULL)
        , _event(NULL)
    {}
    ~StuckClosure() {}
    void Run() {
        while (_stuck && _stuck->load(std::memory_order_acquire)) {
            bthread_usleep(100);
        }
        EXPECT_TRUE(status().ok()) << status();
        if (_expected_next_log_index) {
            _expected_next_log_index->fetch_add(1, std::memory_order_release);
        }
        if (_event) {
            _event->signal();
        }
        delete this;
    }
private:
    std::atomic<bool>* _stuck;
    std::atomic<int64_t>* _expected_next_log_index;
    bthread::CountdownEvent* _event;
};

class SyncClosure : public raft::LogManager::StableClosure {
public:
    SyncClosure() : _event(1) {}
    ~SyncClosure() {
    }
    void Run() {
        _event.signal();
    }
    void reset() {
        status().reset();
        _event.reset();
    }
    void join() {
        _event.wait();
    }
private:
    bthread::CountdownEvent _event;
};

class MockFSMCaller : public raft::FSMCaller {
protected:
    MockFSMCaller() {}
    ~MockFSMCaller() {}
    virtual int on_error(const raft::Error& /*e*/) override {
        return 0;
    }
};

TEST_F(LogManagerTest, get_should_be_ok_when_disk_thread_stucks) {
    std::atomic<bool> stuck(true);
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    const size_t N = 10000;
    DEFINE_SMALL_ARRAY(raft::LogEntry*, saved_entries, N, 256);
    std::atomic<int64_t> expected_next_log_index(1);
    bthread::CountdownEvent event(N);
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(i + 1, 1);
        StuckClosure* c = new StuckClosure;
        c->_stuck = &stuck;
        c->_event = &event;
        c->_expected_next_log_index = &expected_next_log_index;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entry->AddRef();
        saved_entries[i] = entry;
        std::vector<raft::LogEntry*> entries;
        entries.push_back(entry);
        lm->append_entries(&entries, c);
    }

    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = nullptr;
        raft::SynchronizedClosure done;
        lm->get_entry(i + 1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok()) << done.status();
        ASSERT_TRUE(entry) << "i=" << i;
        std::string exptected;
        base::string_printf(&exptected, "hello_%lu", i);
        ASSERT_EQ(exptected, entry->data.to_string());
        entry->Release();
    }

    stuck.store(false, std::memory_order_release);
    event.wait();
    ASSERT_EQ(N+1, expected_next_log_index.load(std::memory_order_acquire));

    LOG(INFO) << "Stop and join disk thread";
    ASSERT_EQ(0, lm->stop_disk_thread());
    lm->clear_memory_logs(raft::LogId(N, 1));
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->_ref_count) << i;
        saved_entries[i]->Release();
    }
}

TEST_F(LogManagerTest, configuration_changes) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    const size_t N = 5;
    DEFINE_SMALL_ARRAY(raft::LogEntry*, saved_entries, N, 256);
    raft::ConfigurationEntry conf;
    SyncClosure sc;
    for (size_t i = 0; i < N; ++i) {
        std::vector<raft::PeerId> peers;
        for (size_t j = 0; j <= i; ++j) {
            peers.push_back(raft::PeerId(base::EndPoint(), j));
        }
        std::vector<raft::LogEntry*> entries;
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->peers = new std::vector<raft::PeerId>(peers);
        if (peers.size() > 1u) {
            entry->old_peers = new std::vector<raft::PeerId>(
                    peers.begin() + 1, peers.end());
        }
        entry->AddRef();
        entry->id = raft::LogId(i + 1, 1);
        saved_entries[i] = entry;
        entries.push_back(entry);
        sc.reset();
        lm->append_entries(&entries, &sc);
        ASSERT_TRUE(lm->check_and_set_configuration(&conf));
        ASSERT_EQ(i + 1, conf.conf.size());
        ASSERT_EQ(i, conf.old_conf.size());
        sc.join();
        ASSERT_TRUE(sc.status().ok()) << sc.status();
    }
    raft::ConfigurationEntry new_conf;
    ASSERT_TRUE(lm->check_and_set_configuration(&new_conf));
    ASSERT_EQ(N, new_conf.conf.size());
    ASSERT_EQ(N - 1, new_conf.old_conf.size());

    LOG(INFO) << "Stop and join disk thraad";
    ASSERT_EQ(0, lm->stop_disk_thread());
    lm->clear_memory_logs(raft::LogId(N, 1));
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->_ref_count) << "i=" << i;
        saved_entries[i]->Release();
    }
}

TEST_F(LogManagerTest, truncate_suffix_also_revert_configuration) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    const size_t N = 5;
    DEFINE_SMALL_ARRAY(raft::LogEntry*, saved_entries, N, 256);
    raft::ConfigurationEntry conf;
    SyncClosure sc;
    for (size_t i = 0; i < N; ++i) {
        std::vector<raft::PeerId> peers;
        for (size_t j = 0; j <= i; ++j) {
            peers.push_back(raft::PeerId(base::EndPoint(), j));
        }
        std::vector<raft::LogEntry*> entries;
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->peers = new std::vector<raft::PeerId>(peers);
        entry->AddRef();
        entry->id = raft::LogId(i + 1, 1);
        saved_entries[i] = entry;
        entries.push_back(entry);
        sc.reset();
        lm->append_entries(&entries, &sc);
        ASSERT_TRUE(lm->check_and_set_configuration(&conf));
        ASSERT_EQ(i + 1, conf.conf.size());
        sc.join();
        ASSERT_TRUE(sc.status().ok()) << sc.status();
    }
    raft::ConfigurationEntry new_conf;
    ASSERT_TRUE(lm->check_and_set_configuration(&new_conf));
    ASSERT_EQ(N, new_conf.conf.size());

    lm->unsafe_truncate_suffix(2);
    ASSERT_TRUE(lm->check_and_set_configuration(&new_conf));
    ASSERT_EQ(2u, new_conf.conf.size());
    
    LOG(INFO) << "Stop and join disk thraad";
    ASSERT_EQ(0, lm->stop_disk_thread());
    lm->clear_memory_logs(raft::LogId(N, 1));
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries[i]->_ref_count) << "i=" << i;
        saved_entries[i]->Release();
    }

}

TEST_F(LogManagerTest, append_with_the_same_index) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    const size_t N = 1000;
    std::vector<raft::LogEntry*> entries0;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 1);
        entries0.push_back(entry);
        entry->AddRef();
    }
    std::vector<raft::LogEntry*> saved_entries0(entries0);
    SyncClosure sc;
    lm->append_entries(&entries0, &sc);
    sc.join();
    ASSERT_TRUE(sc.status().ok()) << sc.status();
    ASSERT_EQ(N, lm->last_log_index());

    // Append the same logs, should be ok
    std::vector<raft::LogEntry*> entries1;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 1);
        entries1.push_back(entry);
        entry->AddRef();
    }

    std::vector<raft::LogEntry*> saved_entries1(entries1);
    sc.reset();
    lm->append_entries(&entries1, &sc);
    sc.join();
    ASSERT_TRUE(sc.status().ok()) << sc.status();
    ASSERT_EQ(N, lm->last_log_index());
    for (size_t i = 0; i < N; ++i) {
        if (i < N - raft::FLAGS_raft_node_cached_log_num) {
            ASSERT_EQ(2u, saved_entries0[i]->_ref_count + saved_entries1[i]->_ref_count) << i;
        } else {
            ASSERT_EQ(3u, saved_entries0[i]->_ref_count + saved_entries1[i]->_ref_count) << i;
        }
    }

    // new term should overwrite the old ones
    std::vector<raft::LogEntry*> entries2;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", (i + 1) * 10);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 2);
        entries2.push_back(entry);
        entry->AddRef();
    }
    std::vector<raft::LogEntry*> saved_entries2(entries2);
    sc.reset();
    lm->append_entries(&entries2, &sc);
    sc.join();
    ASSERT_TRUE(sc.status().ok()) << sc.status();
    ASSERT_EQ(N, lm->last_log_index());
    while (true) {
        // wait clear log since Closure runs first
        // sc.join() waked before clear log
        std::unique_lock<raft::Mutex> lck(lm->_mutex);
        if (lm->_logs_in_memory.size() <= raft::FLAGS_raft_node_cached_log_num) {
            break;
        }
        lck.unlock();
        bthread_usleep(100);
    }
    usleep(100 * 1000l); // wait clear log execute since it is done in another thread
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries0[i]->_ref_count) << i;
        ASSERT_EQ(1u, saved_entries1[i]->_ref_count) << i;
        if (i < N - raft::FLAGS_raft_node_cached_log_num) {
            ASSERT_EQ(1u, saved_entries2[i]->_ref_count) << i;
        } else {
            ASSERT_EQ(2u, saved_entries2[i]->_ref_count) << i;
        }
    }

    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = nullptr;
        raft::SynchronizedClosure done;
        lm->get_entry(i + 1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry) << "i=" << i;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", (i + 1) * 10);
        ASSERT_EQ(buf, entry->data.to_string());
        ASSERT_EQ(raft::LogId(i + 1, 2), entry->id);
        entry->Release();
    }
    lm->set_applied_id(raft::LogId(N, 2));
    usleep(100 * 1000l); // wait clear log execute since it is done in another thread

    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(1u, saved_entries0[i]->_ref_count);
        ASSERT_EQ(1u, saved_entries1[i]->_ref_count);
        ASSERT_EQ(1u, saved_entries2[i]->_ref_count);
    }

    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = nullptr;
        raft::SynchronizedClosure done;
        lm->get_entry(i + 1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry) << "i=" << i;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", (i + 1) * 10);
        ASSERT_EQ(buf, entry->data.to_string());
        ASSERT_EQ(raft::LogId(i + 1, 2), entry->id);
        entry->Release();
    }

    for (size_t i = 0; i < N; ++i) {
        saved_entries0[i]->Release();
        saved_entries1[i]->Release();
        saved_entries2[i]->Release();
    }
    lm->stop_disk_thread();
}

TEST_F(LogManagerTest, pipelined_append) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    const size_t N = raft::FLAGS_raft_node_cached_log_num;
    raft::ConfigurationEntry conf;
    std::vector<raft::LogEntry*> entries0;
    for (size_t i = 0; i < N - 1; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", 0lu);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 1);
        entries0.push_back(entry);
        entry->AddRef();
    }
    {
        std::vector<raft::PeerId> peers;
        peers.push_back(raft::PeerId("127.0.0.1:1234"));
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->id = raft::LogId(N, 1);
        entry->peers = new std::vector<raft::PeerId>(peers);
        entries0.push_back(entry);
    }
    SyncClosure sc0;
    lm->append_entries(&entries0, &sc0);
    ASSERT_TRUE(lm->check_and_set_configuration(&conf));
    ASSERT_EQ(raft::LogId(N, 1), conf.id);
    ASSERT_EQ(1u, conf.conf.size());
    ASSERT_EQ(N, lm->last_log_index());

    // entries1 overwrites entries0
    std::vector<raft::LogEntry*> entries1;
    for (size_t i = 0; i < N - 1; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i + 1);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 2);
        entries1.push_back(entry);
        entry->AddRef();
    }
    {
        std::vector<raft::PeerId> peers;
        peers.push_back(raft::PeerId("127.0.0.2:1234"));
        peers.push_back(raft::PeerId("127.0.0.2:2345"));
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->id = raft::LogId(N, 2);
        entry->peers = new std::vector<raft::PeerId>(peers);
        entries1.push_back(entry);
    }
    SyncClosure sc1;
    lm->append_entries(&entries1, &sc1);
    ASSERT_TRUE(lm->check_and_set_configuration(&conf));
    ASSERT_EQ(raft::LogId(N, 2), conf.id);
    ASSERT_EQ(2u, conf.conf.size());
    ASSERT_EQ(N, lm->last_log_index());

    // entries2 is next to entries1
    ASSERT_EQ(2, lm->get_term(N));
    std::vector<raft::LogEntry*> entries2;
    for (size_t i = N; i < 2 * N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i + 1);
        entry->data.append(buf);
        entry->id = raft::LogId(i + 1, 2);
        entries2.push_back(entry);
        entry->AddRef();
    }

    SyncClosure sc2;
    lm->append_entries(&entries2, &sc2);
    ASSERT_FALSE(lm->check_and_set_configuration(&conf));
    ASSERT_EQ(raft::LogId(N, 2), conf.id);
    ASSERT_EQ(2u, conf.conf.size());
    ASSERT_EQ(2 * N, lm->last_log_index());
    LOG(INFO) << conf.conf;

    // It's safe to get entry when disk thread is still running
    for (size_t i = 0; i < 2 * N; ++i) {
        raft::SynchronizedClosure done;
        raft::LogEntry* entry = nullptr;
        lm->get_entry(i + 1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry) << "i=" << i;
        if (entry->type == raft::ENTRY_TYPE_DATA) {
            std::string buf;
            base::string_printf(&buf, "hello_%lu", i + 1);
            ASSERT_EQ(buf, entry->data.to_string()) << i + 1;
        }
        ASSERT_EQ(raft::LogId(i + 1, 2), entry->id) << i + 1;
        entry->Release();
    }

    sc0.join();
    ASSERT_TRUE(sc0.status().ok()) << sc0.status();
    sc1.join();
    ASSERT_TRUE(sc1.status().ok()) << sc1.status();
    sc2.join();
    ASSERT_TRUE(sc2.status().ok()) << sc2.status();

    // Wrong applied id doesn't change _logs_in_memory
    lm->set_applied_id(raft::LogId(N * 2, 1));
    ASSERT_EQ(raft::FLAGS_raft_node_cached_log_num,
            lm->_logs_in_memory.size());

    lm->set_applied_id(raft::LogId(N * 2, 2));
    ASSERT_EQ(0u, lm->_logs_in_memory.size());

    // We can still get the right data from storage
    for (size_t i = 0; i < 2 * N; ++i) {
        raft::SynchronizedClosure done;
        raft::LogEntry* entry = nullptr;
        lm->get_entry(i + 1, &entry, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_TRUE(entry) << "i=" << i;
        if (entry->type == raft::ENTRY_TYPE_DATA) {
            std::string buf;
            base::string_printf(&buf, "hello_%lu", i + 1);
            ASSERT_EQ(buf, entry->data.to_string());
        }
        ASSERT_EQ(raft::LogId(i + 1, 2), entry->id);
        entry->Release();
    }
    lm->stop_disk_thread();
}

TEST_F(LogManagerTest, set_snapshot) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    raft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    lm->set_snapshot(&meta, meta.last_included_index());
    ASSERT_EQ(raft::LogId(1000, 2), lm->last_log_id());
    lm->stop_disk_thread();
}

int on_new_log(void* arg, int /*error_code*/) {
    SyncClosure* sc = (SyncClosure*)arg;
    sc->Run();
    return 0;
}

int append_entry(raft::LogManager* lm, base::StringPiece data, int64_t index, int64_t term = 1) {
    raft::LogEntry* entry = new raft::LogEntry;
    entry->AddRef();
    entry->type = raft::ENTRY_TYPE_DATA;
    entry->data.append(data.data(), data.size());
    entry->id = raft::LogId(index, term);
    SyncClosure sc;
    std::vector<raft::LogEntry*> entries;
    entries.push_back(entry);
    lm->append_entries(&entries, &sc);
    sc.join();
    return sc.status().error_code();
}

TEST_F(LogManagerTest, wait) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    SyncClosure sc;   
    raft::LogManager::WaitId wait_id = 
            lm->wait(lm->last_log_index(), on_new_log, &sc);
    ASSERT_NE(0, wait_id);
    ASSERT_EQ(0, lm->remove_waiter(wait_id));
    ASSERT_EQ(0, append_entry(lm.get(), "hello", 1));
    wait_id = lm->wait(0, on_new_log, &sc);
    ASSERT_EQ(0, wait_id);
    sc.join();
    sc.reset();
    wait_id = lm->wait(lm->last_log_index(), on_new_log, &sc);
    ASSERT_NE(0, wait_id);
    ASSERT_EQ(0, append_entry(lm.get(), "hello", 2));
    sc.join();
    ASSERT_NE(0, lm->remove_waiter(wait_id));
    lm->stop_disk_thread();

}

TEST_F(LogManagerTest, flush_and_get_last_id) {
    system("rm -rf ./data");
    {
        scoped_ptr<raft::ConfigurationManager> cm(
                                    new raft::ConfigurationManager);
        scoped_ptr<raft::SegmentLogStorage> storage(
                                    new raft::SegmentLogStorage("./data"));
        scoped_refptr<raft::LogManager> lm(new raft::LogManager());
        scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
        raft::LogManagerOptions opt;
        opt.log_storage = storage.get();
        opt.configuration_manager = cm.get();
        opt.fsm_caller = fsm.get();
        ASSERT_EQ(0, lm->init(opt));
        raft::SnapshotMeta meta;
        meta.set_last_included_index(100);
        meta.set_last_included_term(100);
        lm->set_snapshot(&meta, meta.last_included_index());
        ASSERT_EQ(raft::LogId(100, 100), lm->last_log_id());
        raft::SynchronizedClosure done;
        raft::LogId log_id;
        lm->last_log_id(&log_id, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(raft::LogId(100, 100), log_id);
        lm->stop_disk_thread();
    }
    // Load from disk again
    {
        scoped_ptr<raft::ConfigurationManager> cm(
                                    new raft::ConfigurationManager);
        scoped_ptr<raft::SegmentLogStorage> storage(
                                    new raft::SegmentLogStorage("./data"));
        scoped_refptr<raft::LogManager> lm(new raft::LogManager());
        scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
        raft::LogManagerOptions opt;
        opt.log_storage = storage.get();
        opt.configuration_manager = cm.get();
        opt.fsm_caller = fsm.get();
        ASSERT_EQ(0, lm->init(opt));
        raft::SnapshotMeta meta;
        meta.set_last_included_index(100);
        meta.set_last_included_term(100);
        lm->set_snapshot(&meta, meta.last_included_index());
        ASSERT_EQ(raft::LogId(100, 100), lm->last_log_id());
        raft::SynchronizedClosure done;
        raft::LogId log_id;
        lm->last_log_id(&log_id, &done);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        ASSERT_EQ(raft::LogId(100, 100), log_id);
        lm->stop_disk_thread();
    }
}

TEST_F(LogManagerTest, check_consistency) {
    system("rm -rf ./data");
    {
        scoped_ptr<raft::ConfigurationManager> cm(
                                    new raft::ConfigurationManager);
        scoped_ptr<raft::SegmentLogStorage> storage(
                                    new raft::SegmentLogStorage("./data"));
        scoped_refptr<raft::LogManager> lm(new raft::LogManager());
        scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
        raft::LogManagerOptions opt;
        opt.log_storage = storage.get();
        opt.configuration_manager = cm.get();
        opt.fsm_caller = fsm.get();
        ASSERT_EQ(0, lm->init(opt));
        base::Status st;
        st = lm->check_consistency();
        ASSERT_TRUE(st.ok()) << st;
        raft::SnapshotMeta meta;
        for (int i = 1; i < 1001; ++i) {
            append_entry(lm.get(), "dummy", i);
        }
        st = lm->check_consistency();
        ASSERT_TRUE(st.ok()) << st;
        meta.set_last_included_index(100);
        meta.set_last_included_term(1);
        lm->set_snapshot(&meta, meta.last_included_index());
        st = lm->check_consistency();
        ASSERT_TRUE(st.ok()) << st;
        lm->clear_bufferred_logs();
        st = lm->check_consistency();
        ASSERT_TRUE(st.ok()) << st;
        lm->stop_disk_thread();
    }
    {
        scoped_ptr<raft::ConfigurationManager> cm(
                                    new raft::ConfigurationManager);
        scoped_ptr<raft::SegmentLogStorage> storage(
                                    new raft::SegmentLogStorage("./data"));
        scoped_refptr<raft::LogManager> lm(new raft::LogManager());
        scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
        raft::LogManagerOptions opt;
        opt.log_storage = storage.get();
        opt.configuration_manager = cm.get();
        opt.fsm_caller = fsm.get();
        ASSERT_EQ(0, lm->init(opt));
        base::Status st;
        st = lm->check_consistency();
        LOG(INFO) << "st : " << st;
        ASSERT_FALSE(st.ok()) << st;
        lm->stop_disk_thread();
    }
}

TEST_F(LogManagerTest, truncate_suffix_to_last_snapshot) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
            new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
            new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    base::Status st;
    ASSERT_TRUE(st.ok()) << st;
    raft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    lm->set_snapshot(&meta, meta.last_included_index());
    raft::SynchronizedClosure done;
    raft::LogId log_id;
    lm->last_log_id(&log_id, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(raft::LogId(1000, 2), log_id);
    ASSERT_EQ(0, append_entry(lm.get(), "dummy2", 1001, 2));
    ASSERT_EQ(0, append_entry(lm.get(), "dummy3", 1001, 3));
    done.reset();
    lm->last_log_id(&log_id, &done);
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(raft::LogId(1001, 3), log_id);
    lm->stop_disk_thread();
}

TEST_F(LogManagerTest, set_snapshot_and_get_log_term) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
            new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
            new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    const int N = 10;
    for (int i = 0; i < N; ++i) {
        append_entry(lm.get(), "test", i + 1, 1);
    }
    raft::SnapshotMeta meta;
    meta.set_last_included_index(N - 1);
    meta.set_last_included_term(1);
    lm->set_snapshot(&meta, meta.last_included_index());
    lm->set_snapshot(&meta, meta.last_included_index());
    ASSERT_EQ(raft::LogId(N, 1), lm->last_log_id());
    ASSERT_EQ(1L, lm->get_term(N - 1));
    LOG(INFO) << "Last_index=" << lm->last_log_index();
    lm->stop_disk_thread();

}

class MockLogStorage : public raft::LogStorage {
public:
    static constexpr int64_t APPEND_M = 10;
    static constexpr int64_t APPEND_N = 4;
    static constexpr int64_t EIO_ID = APPEND_M * APPEND_N / 2 + APPEND_N / 2;

    MockLogStorage() {}
    virtual ~MockLogStorage() {}
    virtual int init(raft::ConfigurationManager* cm) override {
        _term_map[0] = 0;
        return 0;
    }
    virtual int64_t first_log_index() override {
        return _first_log_index;
    }
    virtual int64_t last_log_index() override {
        return _last_log_index;
    }
    virtual void get_entry(const int64_t index, raft::LogEntry** entry,
            raft::Closure* done) override {
        rpc::ClosureGuard done_guard(done);
        LOG(WARNING) << "not implement";
    }
    virtual void last_log_id(raft::LogId* log_id, raft::Closure* done) {
        rpc::ClosureGuard done_guard(done);
        LOG(WARNING) << "not implement";
    }
    virtual int64_t get_term(const int64_t index) override {
        return 0;
    }
    virtual void append_entry(const raft::LogEntry* entry, raft::Closure* done) override {
        rpc::ClosureGuard done_guard(done);
        _append_idx++;
        if (_append_idx == EIO_ID) {
            done->status().set_error(EIO, "mock fail");
        }
    }
    virtual void append_entries(const std::vector<raft::LogEntry*>& entries, raft::IOMetric* metric, raft::Closure* done) override {
        rpc::ClosureGuard done_guard(done);
        for (size_t i = 0; i < entries.size(); i++) {
            _append_idx++;
            if (_append_idx == EIO_ID) {
                done->status().set_error(EIO, "mock fail");
            }
        }
    }
    virtual void truncate_prefix(const int64_t first_index_kept, raft::Closure* done) override {
        LOG(WARNING) << "not implement";
    }
    virtual void truncate_suffix(const int64_t last_index_kept, raft::Closure* done) override {
        LOG(WARNING) << "not implement";
    }
    virtual void reset(const int64_t next_log_index, raft::Closure* done) override {
        LOG(WARNING) << "not implement";
    }
    virtual raft::LogStorage* new_instance(const std::string& uri) const override {
        LOG(WARNING) << "not implement";
        return nullptr;
    }
    virtual base::Status gc_instance(const std::string& uri) const {
        base::Status status;
        return status;
    }

private:
    int64_t _first_log_index = 1;
    int64_t _last_log_index = 0;
    std::map<int64_t, int64_t> _term_map;
    int64_t _append_idx = 0;
};

TEST_F(LogManagerTest, append_batcher_with_fail) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
            new raft::ConfigurationManager);
    scoped_ptr<MockLogStorage> storage(new MockLogStorage());
    scoped_refptr<raft::LogManager> lm(new raft::LogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    int64_t id = 1;
    SyncClosure done;
    for (size_t i = 0; i < MockLogStorage::APPEND_M; ++i) {
        std::vector<raft::LogEntry*> entries;
        for (size_t j = 0; j < MockLogStorage::APPEND_N; ++j) {
            raft::LogEntry* entry = new raft::LogEntry;
            entry->AddRef();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id = raft::LogId(id++, 1);
            std::string buf;
            base::string_printf(&buf, "hello_%lu", id - 1);
            entry->data.append(buf);
            entries.push_back(entry);
        }
        done.reset();
        lm->append_entries(&entries, &done);
        done.join();
        if (i <= MockLogStorage::APPEND_M / 2 - 1) {
            ASSERT_TRUE(done.status().ok()) << i << ":" << done.status();
        } else {
            ASSERT_FALSE(done.status().ok());
        }
        for (auto e : entries) {
            e->Release();
        }
    }
    lm->stop_disk_thread();
}

TEST_F(LogManagerTest, destroy_lm_with_append_batcher) {
    raft::FLAGS_raft_use_iouring = true;
    std::atomic<bool> stuck(true);
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm(new MockLogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    ASSERT_EQ(g_test_log_manager_count.get_value(), 1);
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    const size_t N = 10000;
    DEFINE_SMALL_ARRAY(raft::LogEntry*, saved_entries, N, 256);
    std::atomic<int64_t> expected_next_log_index(1);
    int32_t ref_count = lm->_ref_count;
    bthread::CountdownEvent event(N);
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(i + 1, 1);
        StuckClosure* c = new StuckClosure;
        c->_stuck = &stuck;
        c->_expected_next_log_index = &expected_next_log_index;
        c->_event = &event;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entry->AddRef();
        saved_entries[i] = entry;
        std::vector<raft::LogEntry*> entries;
        entries.push_back(entry);
        lm->append_entries(&entries, c);
    }
    while (ref_count == lm->_ref_count) {
        bthread_usleep(100);
    }
    stuck.store(false, std::memory_order_release);
    event.wait();
    ASSERT_EQ(N+1, expected_next_log_index.load(std::memory_order_acquire));
    // After clear all the memory logs, all the saved entries should have no
    // other reference
    for (size_t i = 0; i < N; ++i) {
        saved_entries[i]->Release();
    }
    lm->stop_disk_thread();
    lm = nullptr;
    while (g_test_log_manager_count.get_value() != 0) {
        bthread_usleep(1000);
    }
    raft::FLAGS_raft_use_iouring = false;
}

TEST_F(LogManagerTest, stop_lm_with_append_batcher) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<MockLogManager> lm(new MockLogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    ASSERT_EQ(g_test_log_manager_count.get_value(), 1);
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    lm->_pending_append_to_storage = true;

    raft::LogEntry* entry = new raft::LogEntry;
    entry->AddRef();
    entry->type = raft::ENTRY_TYPE_DATA;
    entry->id = raft::LogId(1, 1);
    SyncClosure done;
    entry->data.append("stop log manager");
    entry->AddRef();
    std::vector<raft::LogEntry*> entries;
    entries.push_back(entry);
    lm->append_entries(&entries, &done);

    while (!lm->has_pending()) {
        bthread_usleep(100);
    }

    lm->shutdown();
    ASSERT_TRUE(lm->_stopped);
    lm->_pending_append_to_storage = false;
    lm->wake_up();
    done.join();
    ASSERT_FALSE(done.status().ok()) << done.status();
    LOG(NOTICE) << done.status();

    entry->Release();

    lm->stop_disk_thread();
}

TEST_F(LogManagerTest, cache_log_num) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<MockLogManager> lm(new MockLogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    ASSERT_EQ(g_test_log_manager_count.get_value(), 1);
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    size_t N = raft::FLAGS_raft_node_cached_log_num / 2;
    ASSERT_GT(N, 0);
    ASSERT_LT(N, raft::FLAGS_raft_node_cached_log_num);
    SyncClosure done;
    std::vector<raft::LogEntry*> entries;
    int64_t index = 1;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(index, 1);
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entries.push_back(entry);
        index ++;
    }
    lm->append_entries(&entries, &done);
    done.join();
    std::unique_lock<raft::Mutex> lck(lm->_mutex);  // Race with set_applied_id
    ASSERT_EQ(lm->_logs_in_memory.size(), N);
    lck.unlock();
    done.reset();
    entries.clear();
    for (size_t i = 0; i < raft::FLAGS_raft_node_cached_log_num; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(index, 1);
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entries.push_back(entry);
        index ++;
    }
    lm->append_entries(&entries, &done);
    done.join();
    lck.lock();
    ASSERT_EQ(lm->_logs_in_memory.size(), raft::FLAGS_raft_node_cached_log_num);
    lck.unlock();
    lm->stop_disk_thread();
}

TEST_F(LogManagerTest, last_log_id_not_dead_lock) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<MockLogManager> lm(new MockLogManager());
    scoped_ptr<MockFSMCaller> fsm(new MockFSMCaller());
    ASSERT_EQ(g_test_log_manager_count.get_value(), 1);
    raft::LogManagerOptions opt;
    opt.log_storage = storage.get();
    opt.configuration_manager = cm.get();
    opt.fsm_caller = fsm.get();
    ASSERT_EQ(0, lm->init(opt));
    size_t N = raft::FLAGS_raft_node_cached_log_num / 2;
    ASSERT_GT(N, 0);
    ASSERT_LT(N, raft::FLAGS_raft_node_cached_log_num);
    SyncClosure done;
    std::vector<raft::LogEntry*> entries;
    int64_t index = 1;
    for (size_t i = 0; i < N; ++i) {
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(index, 1);
        std::string buf;
        base::string_printf(&buf, "hello_%lu", i);
        entry->data.append(buf);
        entries.push_back(entry);
        index ++;
    }
    lm->append_entries(&entries, &done);
    done.join();
    std::unique_lock<raft::Mutex> lck(lm->_mutex);  // Race with set_applied_id
    ASSERT_EQ(lm->_logs_in_memory.size(), N);
    lck.unlock();

    done.reset();
    auto l = [&](const base::Status& st) {
        ASSERT_EQ(0, lm->stop_disk_thread());
        done.Run();
    };
    raft::LogId log_id;
    lm->last_log_id(&log_id, raft::create_raft_closure(std::move(l)));
    done.join();
}

