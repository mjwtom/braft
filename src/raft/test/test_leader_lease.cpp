// Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Pengfei Zheng (zhengpengfei@baidu.com)

#include <gtest/gtest.h>
#include <base/logging.h>
#include "raft/util.h"
#include "raft/node.h"
#include "raft/lease.h"
#include "raft/test/util.h"

namespace raft {
DECLARE_bool(raft_enable_leader_lease);
DECLARE_int32(raft_election_heartbeat_factor);
}

class BaseLeaseTest : public testing::Test {
protected:
    void SetUp() {
        ::system("rm -rf data");
        logging::FLAGS_verbose = 90;
        raft::FLAGS_raft_sync = false;
        raft::FLAGS_raft_enable_leader_lease = true;
        raft::FLAGS_raft_election_heartbeat_factor = 3;
    }
    void TearDown() {
        ::system("rm -rf data");
    }
};

class ExtendLeaseTest : public testing::TestWithParam<int> {
protected:
    void SetUp() {
        ::system("rm -rf data");
        logging::FLAGS_verbose = 90;
        raft::FLAGS_raft_sync = false;
        raft::FLAGS_raft_enable_leader_lease = true;
        raft::FLAGS_raft_election_heartbeat_factor = 3;
        peer_num = GetParam();
    }
    void TearDown() {
        ::system("rm -rf data");
    }

    int peer_num;
};

void check_if_stale_leader_exist(Cluster* cluster, int line) {
    // Only one peer can be LEASE_NOT_READY or LEASE_VALID
    std::vector<raft::Node*> nodes;
    raft::LeaderLeaseStatus lease_status;
    cluster->all_nodes(&nodes);
    for (int i = 0; i < 2; ++i) {
        int lease_valid_num = 0;
        raft::Node* leader_node = NULL;
        int64_t leader_term = -1;
        for (auto& n : nodes) {
            n->get_leader_lease_status(&lease_status);
            if (lease_status.state == raft::LEASE_VALID) {
                ++lease_valid_num;
                if (lease_valid_num == 1) {
                    leader_node = n;
                    leader_term = lease_status.term;
                } else if (lease_valid_num > 2) {
                    LOG(ERROR) << "found more than two leaders, leader_num: " << lease_valid_num
                               << ", line: " << line;
                    ASSERT_TRUE(false);
                    return;
                } else if (lease_status.term == leader_term || i == 2) {
                    LOG(ERROR) << "found more leaders, leader 1: " << leader_node->node_id()
                               << ", leader 2: " << n->node_id() << " line: " << line;
                    ASSERT_TRUE(false);
                    return;
                }
            }
        }
        if (lease_valid_num == 2) {
            // Little chance that we found two leaders because of leader change,
            // try again to check again
            LOG(WARNING) << "found two leaders, check again";
            continue;
        }
        if (lease_valid_num == 0) {
            LOG(NOTICE) << "no leader";
            return;
        } else {
            LOG(NOTICE) << "leader with lease: " << leader_node->node_id();
            return;
        }
    }
}

#define CHECK_NO_STALE_LEADER(cluster) \
    check_if_stale_leader_exist(cluster, __LINE__);

bool g_check_lease_in_thread_stop = true;

void* check_lease_in_thread(void* arg) {
    Cluster* cluster = static_cast<Cluster*>(arg);
    int round = 0;
    while (!g_check_lease_in_thread_stop) {
        LOG(NOTICE) << "check stale lease, round: " << round++;
        usleep(10 * 1000);
        CHECK_NO_STALE_LEADER(cluster);
    }
    return NULL;
}

TEST_F(BaseLeaseTest, triple_node) {
    ::system("rm -rf data");
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers, 500, 10);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    std::vector<raft::Node*> followers;
    cluster.followers(&followers);
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is elected " << leader->node_id();

    raft::LeaderLeaseStatus lease_status;
    leader->get_leader_lease_status(&lease_status);
    int64_t leader_term = lease_status.term;
    int64_t lease_epoch = lease_status.lease_epoch;
    int64_t start_ms = base::monotonic_time_ms();
    while (lease_status.state == raft::LEASE_NOT_READY ||
           base::monotonic_time_ms() - start_ms < 1000) {
        LOG(NOTICE) << "waiting lease become valid";
        bthread_usleep(100 * 1000);
        leader->get_leader_lease_status(&lease_status);
    }
    ASSERT_EQ(lease_status.state, raft::LEASE_VALID);
    ASSERT_EQ(lease_status.term, leader_term);
    ASSERT_EQ(lease_status.lease_epoch, lease_epoch);
    ASSERT_TRUE(leader->is_leader_lease_valid());

    for (int i = 0; i < 3; ++i) {
        bthread_usleep(100 * 1000);

        leader->get_leader_lease_status(&lease_status);
        ASSERT_EQ(lease_status.state, raft::LEASE_VALID);
        ASSERT_TRUE(leader->is_leader_lease_valid());
        ASSERT_EQ(lease_status.term, leader_term);
        ASSERT_EQ(lease_status.lease_epoch, lease_epoch);

        // lease lazily extend
        bthread_usleep(600 * 1000);

        leader->get_leader_lease_status(&lease_status);
        ASSERT_EQ(lease_status.state, raft::LEASE_VALID);
        ASSERT_TRUE(leader->is_leader_lease_valid());
        ASSERT_EQ(lease_status.term, leader_term);
        ASSERT_EQ(lease_status.lease_epoch, lease_epoch);
    }

    // check followrs
    for (auto& f : followers) {
        ASSERT_FALSE(f->is_leader_lease_valid());
        f->get_leader_lease_status(&lease_status);
        ASSERT_EQ(lease_status.state, raft::LEASE_EXPIRED);
    }

    // disable
    raft::FLAGS_raft_enable_leader_lease = false;
    leader->get_leader_lease_status(&lease_status);
    ASSERT_EQ(lease_status.state, raft::LEASE_DISABLED);
    for (auto& f : followers) {
        ASSERT_FALSE(f->is_leader_lease_valid());
        f->get_leader_lease_status(&lease_status);
        ASSERT_EQ(lease_status.state, raft::LEASE_DISABLED);
    }

    raft::FLAGS_raft_enable_leader_lease = true;

    // stop a follower, lease still valid
    LOG(NOTICE) << "stop a follower";
    cluster.stop(followers[0]->node_id().peer_id.addr);

    leader->get_leader_lease_status(&lease_status);
    ASSERT_EQ(lease_status.state, raft::LEASE_VALID);
    ASSERT_EQ(lease_status.term, leader_term);
    ASSERT_EQ(lease_status.lease_epoch, lease_epoch);
    ASSERT_TRUE(leader->is_leader_lease_valid());

    bthread_usleep(600 * 1000);

    leader->get_leader_lease_status(&lease_status);
    ASSERT_EQ(lease_status.state, raft::LEASE_VALID);
    ASSERT_EQ(lease_status.term, leader_term);
    ASSERT_EQ(lease_status.lease_epoch, lease_epoch);
    ASSERT_TRUE(leader->is_leader_lease_valid());

    // stop all follwers, lease expired
    LOG(NOTICE) << "stop a another";
    cluster.stop(followers[1]->node_id().peer_id.addr);

    leader->get_leader_lease_status(&lease_status);
    ASSERT_EQ(lease_status.state, raft::LEASE_VALID);
    ASSERT_EQ(lease_status.term, leader_term);
    ASSERT_EQ(lease_status.lease_epoch, lease_epoch);
    ASSERT_TRUE(leader->is_leader_lease_valid());

    bthread_usleep(600 * 1000);

    leader->get_leader_lease_status(&lease_status);
    ASSERT_EQ(lease_status.state, raft::LEASE_EXPIRED);
    ASSERT_FALSE(leader->is_leader_lease_valid());

    cluster.stop_all();
}

TEST_F(BaseLeaseTest, change_peers) {
    ::system("rm -rf data");
    std::vector<raft::PeerId> peers;
    raft::PeerId peer0;
    peer0.addr.ip = base::my_ip();
    peer0.addr.port = 5006;
    peer0.idx = 0;

    // start cluster
    peers.push_back(peer0);
    Cluster cluster("unittest", peers, 500, 10);
    ASSERT_EQ(0, cluster.start(peer0.addr));
    LOG(NOTICE) << "start single cluster " << peer0;

    // start a thread to check leader lease
    g_check_lease_in_thread_stop = false;
    pthread_t tid;
    ASSERT_EQ(0, pthread_create(&tid, NULL, check_lease_in_thread, &cluster));

    cluster.wait_leader();

    for (int i = 1; i < 10; ++i) {
        LOG(NOTICE) << "start peer " << i;
        raft::PeerId peer = peer0;
        peer.addr.port += i;
        ASSERT_EQ(0, cluster.start(peer.addr, true));
    }

    for (int i = 1; i < 10; ++i) {
        LOG(NOTICE) << "add peer " << i;
        cluster.wait_leader();
        raft::Node* leader = cluster.leader();
        raft::PeerId peer = peer0;
        peer.addr.port += i;
        raft::SynchronizedClosure done;
        leader->add_peer(peer, &done);
        usleep(50 * 1000);
        done.wait();
        ASSERT_TRUE(done.status().ok()) << done.status();
    }

    for (int i = 1; i < 10; ++i) {
        LOG(NOTICE) << "remove peer " << i;
        cluster.wait_leader();
        raft::Node* leader = cluster.leader();
        raft::PeerId peer = peer0;
        peer.addr.port += i;
        raft::SynchronizedClosure done;
        leader->remove_peer(peer, &done);
        usleep(50 * 1000);
        done.wait();
        ASSERT_TRUE(done.status().ok()) << done.status();
    }

    g_check_lease_in_thread_stop = true;
    pthread_join(tid, NULL);

    cluster.stop_all();
}

TEST_F(BaseLeaseTest, leader_remove_itself) {
    ::system("rm -rf data");
    std::vector<raft::PeerId> peers;
    raft::PeerId peer0;
    peer0.addr.ip = base::my_ip();
    peer0.addr.port = 5006;
    peer0.idx = 0;

    // start cluster
    peers.push_back(peer0);
    Cluster cluster("unittest", peers, 500, 10);
    ASSERT_EQ(0, cluster.start(peer0.addr));
    LOG(NOTICE) << "start single cluster " << peer0;

    // start a thread to check leader lease
    g_check_lease_in_thread_stop = false;
    pthread_t tid;
    ASSERT_EQ(0, pthread_create(&tid, NULL, check_lease_in_thread, &cluster));

    cluster.wait_leader();
    
    const int follower_num = 3;
    for (int i = 1; i <= follower_num; ++i) {
        LOG(NOTICE) << "start peer " << i;
        raft::PeerId peer = peer0;
        peer.addr.port += i;
        ASSERT_EQ(0, cluster.start(peer.addr, true));
    }

    for (int i = 1; i <= follower_num; ++i) {
        LOG(NOTICE) << "add peer " << i;
        cluster.wait_leader();
        raft::Node* leader = cluster.leader();
        raft::PeerId peer = peer0;
        peer.addr.port += i;
        raft::SynchronizedClosure done;
        leader->add_peer(peer, &done);
        usleep(50 * 1000);
        done.wait();
        ASSERT_TRUE(done.status().ok()) << done.status();
    }
    
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_EQ(peer0, leader->node_id().peer_id);
    LOG(NOTICE) << "remove leader " << peer0;
    
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    raft::SynchronizedClosure done;
    for (int i = 0; i < follower_num; ++i) {
        static_cast<MockFSM*>(nodes[i]->_impl->_options.fsm)->set_on_leader_start_closure(&done);
    }
    int64_t begin_ms = base::monotonic_time_ms();
    raft::SynchronizedClosure remove_done;
    // leader remove itself, it's safe to expire leader lease
    leader->remove_peer(peer0, &remove_done);
    remove_done.wait();
    ASSERT_TRUE(remove_done.status().ok()) << remove_done.status();
    done.wait();
    const int64_t vote_time = base::monotonic_time_ms() - begin_ms;
    ASSERT_LT(vote_time, 500);
    
    leader = cluster.leader();
    LOG(NOTICE) << "new leader is " << leader->node_id().peer_id << " election time is " << vote_time;

    g_check_lease_in_thread_stop = true;
    pthread_join(tid, NULL);
    
    cluster.stop_all();
}

TEST_P(ExtendLeaseTest, transfer_leadership_success) {
    ::system("rm -rf data");
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < peer_num; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers, 500, 10);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }
    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    raft::PeerId target = nodes[0]->node_id().peer_id;

    raft::LeaderLeaseStatus old_leader_lease;
    raft::LeaderLeaseStatus new_leader_lease;
    leader->get_leader_lease_status(&old_leader_lease);
    nodes[0]->get_leader_lease_status(&new_leader_lease);
    ASSERT_EQ(old_leader_lease.state, raft::LEASE_VALID);
    ASSERT_EQ(new_leader_lease.state, raft::LEASE_EXPIRED);

    raft::SynchronizedClosure done;
    static_cast<MockFSM*>(nodes[0]->_impl->_options.fsm)->set_on_leader_start_closure(&done);

    ASSERT_EQ(0, leader->transfer_leadership_to(target));
    done.wait();

    leader->get_leader_lease_status(&old_leader_lease);
    nodes[0]->get_leader_lease_status(&new_leader_lease);

    ASSERT_EQ(old_leader_lease.state, raft::LEASE_EXPIRED);
    ASSERT_EQ(new_leader_lease.state, raft::LEASE_VALID);

    leader = cluster.leader();
    ASSERT_EQ(target, leader->node_id().peer_id);

    cluster.stop_all();
}

TEST_P(ExtendLeaseTest, transfer_leadership_timeout) {
    ::system("rm -rf data");
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < peer_num; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers, 500, 10);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }
    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    raft::PeerId target = nodes[0]->node_id().peer_id;

    raft::LeaderLeaseStatus old_leader_lease;
    raft::LeaderLeaseStatus new_leader_lease;
    leader->get_leader_lease_status(&old_leader_lease);
    ASSERT_EQ(old_leader_lease.state, raft::LEASE_VALID);

    cluster.stop(nodes[0]->node_id().peer_id.addr);

    raft::SynchronizedClosure done;
    static_cast<MockFSM*>(leader->_impl->_options.fsm)->set_on_leader_start_closure(&done);
    ASSERT_EQ(0, leader->transfer_leadership_to(target));
    done.wait();

    leader->get_leader_lease_status(&new_leader_lease);

    ASSERT_EQ(old_leader_lease.state, raft::LEASE_VALID);
    ASSERT_EQ(old_leader_lease.term, new_leader_lease.term);
    ASSERT_LT(old_leader_lease.lease_epoch, new_leader_lease.lease_epoch);

    cluster.stop_all();
}

TEST_P(ExtendLeaseTest, vote) {
    ::system("rm -rf data");
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < peer_num; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers, 500, 100);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }
    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    raft::PeerId target = nodes[0]->node_id().peer_id;

    raft::LeaderLeaseStatus old_leader_lease;
    raft::LeaderLeaseStatus new_leader_lease;
    leader->get_leader_lease_status(&old_leader_lease);
    nodes[0]->get_leader_lease_status(&new_leader_lease);
    ASSERT_EQ(old_leader_lease.state, raft::LEASE_VALID);
    ASSERT_EQ(new_leader_lease.state, raft::LEASE_EXPIRED);

    raft::SynchronizedClosure done;
    static_cast<MockFSM*>(nodes[0]->_impl->_options.fsm)->set_on_leader_start_closure(&done);

    // It has very little chance that when we want to vote, the heartbeat from
    // old leader interrupt us.
    for (int i = 0; i < 3; ++ i) {
        int64_t vote_begin_ms = base::monotonic_time_ms();
        nodes[0]->vote(50);
        usleep(100 * 1000);
        leader->get_leader_lease_status(&old_leader_lease);
        if (old_leader_lease.state == raft::LEASE_VALID) {
            continue;
        }
        done.wait();
        ASSERT_LT(base::monotonic_time_ms() - vote_begin_ms, 500);
    }

    leader->get_leader_lease_status(&old_leader_lease);
    nodes[0]->get_leader_lease_status(&new_leader_lease);

    ASSERT_EQ(old_leader_lease.state, raft::LEASE_EXPIRED);
    ASSERT_EQ(new_leader_lease.state, raft::LEASE_VALID);

    leader = cluster.leader();
    ASSERT_EQ(target, leader->node_id().peer_id);

    ASSERT_EQ(500 + 100 - 50, nodes[0]->_impl->_follower_lease._max_clock_drift_ms);
    nodes[0]->reset_election_timeout_ms(500);
    ASSERT_EQ(100, nodes[0]->_impl->_follower_lease._max_clock_drift_ms);

    cluster.stop_all();
}

TEST_P(ExtendLeaseTest, leader_step_down) {
    ::system("rm -rf data");
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < peer_num; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers, 500, 1000);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }
    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);

    raft::SynchronizedClosure done;
    for (int i = 0; i < peer_num - 1; ++i) {
        static_cast<MockFSM*>(nodes[i]->_impl->_options.fsm)->set_on_leader_start_closure(&done);
    }

    int64_t begin_ms = base::monotonic_time_ms();
    cluster.stop(leader->node_id().peer_id.addr);
    done.wait();
    // old leader has already stepped down, it's safe to expire leader lease
    ASSERT_LT(base::monotonic_time_ms() - begin_ms, 500);

    leader = cluster.leader();
    raft::LeaderLeaseStatus lease_status;
    leader->get_leader_lease_status(&lease_status);

    cluster.stop_all();
}

class OnLeaderStartHungClosure : public raft::SynchronizedClosure {
public:
    OnLeaderStartHungClosure(int i)
        : raft::SynchronizedClosure(), idx(i), hung(true), running(false) {}

    void Run() {
        running = true;
        LOG(WARNING) << "start run on leader start hung closure " << idx;
        while (hung) {
            bthread_usleep(10 * 1000);
        }
        LOG(WARNING) << "finish run on leader start hung closure" << idx;
        raft::SynchronizedClosure::Run();
        running = false;
    }

    int idx;
    base::atomic<bool> hung;
    base::atomic<bool> running;
};

TEST_P(ExtendLeaseTest, apply_thread_hung) {
    ::system("rm -rf data");
    raft::LeaderLeaseStatus lease_status;
    std::vector<raft::PeerId> peers;
    std::vector<OnLeaderStartHungClosure*> on_leader_start_closures;
    for (int i = 0; i < peer_num; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
        on_leader_start_closures.push_back(new OnLeaderStartHungClosure(i));
    }

    // start cluster
    Cluster cluster("unittest", peers, 500, 10);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr, false, 30, on_leader_start_closures[i]));
    }

    /*
    std::vector<raft::Node*> nodes;
    cluster.all_nodes(&nodes);
    for (size_t i = 0; i < nodes.size(); ++i) {
        static_cast<MockFSM*>(nodes[i]->_impl->_options.fsm)
            ->set_on_leader_start_closure(on_leader_start_closures[i]);
    }
    */

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    raft::PeerId target = nodes[0]->node_id().peer_id;

    cluster.stop(nodes[0]->node_id().peer_id.addr);

    ASSERT_EQ(0, leader->transfer_leadership_to(target));
    cluster.wait_leader();

    // apply thread hung, lease status always be ready
    leader->get_leader_lease_status(&lease_status);
    ASSERT_EQ(lease_status.state, raft::LEASE_NOT_READY);

    // apply thread resume to work
    for (auto c : on_leader_start_closures) {
        c->hung = false;
        if (c->running) {
            LOG(WARNING) << "waiting leader resume";
            c->wait();
            LOG(WARNING) << "leader resumed";
        }
    }

    usleep(100 * 1000);

    leader->get_leader_lease_status(&lease_status);
    ASSERT_EQ(lease_status.state, raft::LEASE_VALID);

    cluster.stop_all();

    for (auto c : on_leader_start_closures) {
        delete c;
    }
}

TEST_P(ExtendLeaseTest, chaos) {
    ::system("rm -rf data");
    std::vector<raft::PeerId> started_nodes;
    for (int i = 0; i < peer_num; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        started_nodes.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", started_nodes, 500, 10);
    for (size_t i = 0; i < started_nodes.size(); i++) {
        ASSERT_EQ(0, cluster.start(started_nodes[i].addr));
    }

    g_check_lease_in_thread_stop = false;
    pthread_t tid;
    ASSERT_EQ(0, pthread_create(&tid, NULL, check_lease_in_thread, &cluster));

    enum OpType {
        NODE_START,
        NODE_STOP,
        TRANSFER_LEADER,
        VOTE,
        RESET_ELECTION_TIMEOUT,
        OP_END,
    };

    std::vector<raft::PeerId> stopped_nodes;
    for (size_t i = 0; i < 100; ++i) {
        OpType type = static_cast<OpType>(base::fast_rand() % OP_END);
        if (type == NODE_START) {
            if (stopped_nodes.empty()) {
                type = NODE_STOP;
            }
        } else if (type == NODE_STOP || type == VOTE || type == TRANSFER_LEADER) {
            if (stopped_nodes.size() == 2) {
                type = NODE_START;
            }
        }
        raft::Node* target_node = NULL;
        switch (type) {
            case NODE_START: {
                LOG(NOTICE) << "chaos round " << i << ", node start";
                std::vector<raft::PeerId> tmp_nodes;
                size_t j = base::fast_rand() % stopped_nodes.size();
                for (size_t t = 0; t < stopped_nodes.size(); ++t) {
                    if (t == j) {
                        cluster.start(stopped_nodes[t].addr);
                        started_nodes.push_back(stopped_nodes[t]);
                    } else {
                        tmp_nodes.push_back(stopped_nodes[t]);
                    }
                }
                tmp_nodes.swap(stopped_nodes);
                break;
            }
            case NODE_STOP: {
                LOG(NOTICE) << "chaos round " << i << ", node stop";
                std::vector<raft::PeerId> tmp_nodes;
                size_t j = base::fast_rand() % started_nodes.size();
                for (size_t t = 0; t < started_nodes.size(); ++t) {
                    if (t == j) {
                        cluster.stop(started_nodes[t].addr);
                        stopped_nodes.push_back(started_nodes[t]);
                    } else {
                        tmp_nodes.push_back(started_nodes[t]);
                    }
                }
                tmp_nodes.swap(started_nodes);
                break;
            }
            case TRANSFER_LEADER: {
                LOG(NOTICE) << "chaos round " << i << ", transfer leader";
                cluster.wait_leader();
                raft::Node* leader = cluster.leader();
                raft::PeerId target;
                if (stopped_nodes.empty() || base::fast_rand() % 2 == 0) {
                    target = started_nodes[base::fast_rand() % started_nodes.size()];
                } else {
                    target = stopped_nodes[base::fast_rand() % stopped_nodes.size()];
                }
                leader->transfer_leadership_to(target);
                break;
            }
            case VOTE: {
                LOG(NOTICE) << "chaos round " << i << ", vote";
                raft::PeerId target = started_nodes[base::fast_rand() % started_nodes.size()];
                target_node = cluster.find_node(target);
                target_node->vote(50);
                break;
            }
            case RESET_ELECTION_TIMEOUT: {
                LOG(NOTICE) << "chaos round " << i << ", vote";
                raft::PeerId target = started_nodes[base::fast_rand() % started_nodes.size()];
                target_node = cluster.find_node(target);
                target_node->reset_election_timeout_ms(base::fast_rand_in(50, 500));
                break;
            }
            case OP_END:
                CHECK(false);
        }

        usleep(20 * 1000);
        if (type == VOTE) {
            target_node->reset_election_timeout_ms(500);
        }
    }

    g_check_lease_in_thread_stop = true;
    pthread_join(tid, NULL);

    cluster.stop_all();
}

INSTANTIATE_TEST_CASE_P(ExtendLeaseTest, ExtendLeaseTest, ::testing::Values(3, 5));
