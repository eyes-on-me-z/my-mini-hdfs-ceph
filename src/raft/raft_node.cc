#include "raft_node.h"

#include <filesystem>
#include <fstream>
#include <random>

namespace fs = std::filesystem;

namespace mini_storage
{
    RaftNode::RaftNode(const RaftConfig &config)
        :config_(config), my_id_(config.node_id)
    {
        fs::create_directories(config_.data_dir);   // 创建节点的数据目录
        LogEntry dummy;
        dummy.term = 0;
        dummy.index = 0;
        log_.push_back(dummy);  // 添加一条索引为 0、任期为 0 的哨兵日志, 用于简化边界处理

        // Default no-op callbacks, 设置一个默认的空 RPC 回调
        send_rpc_cb_ = [](const std::string&, const RaftMessage&) {};
    }

    RaftNode::~RaftNode()
    {
        Stop();
    }

    bool RaftNode::Start()
    {

    }

    void RaftNode::Stop();

    ProposeResult RaftNode::Propose(const std::string &command, int timeout_ms = 5000);

    // Query
    bool RaftNode::IsLeader() const;
    std::string RaftNode::GetLeaderId() const;
    RaftState RaftNode::GetState() const;
    uint64_t RaftNode::GetCurrentTerm() const;
    uint64_t RaftNode::GetCommitIndex() const;
    uint64_t RaftNode::GetLastLogIndex() const;
    uint64_t RaftNode::GetLastLogTerm() const;
    size_t RaftNode::GetLogSize() const;

    // Timer
    void RaftNode::OnElectionTimeout()
    {
        
    }

    // Snapshot
    bool RaftNode::TakeSnapshot();

    // 用于节点启动时，从 raft_snapshot.dat 恢复状态机快照
    bool RaftNode::RestoreFromSnapshot()
    {
        // 打开快照文件
        std::ifstream file(SnapshotFilePath(), std::ios::binary);
        if (!file.is_open()) return true;
        
        // 读取序列化数据
        uint64_t len = 0;
        file.read(reinterpret_cast<char*>(&len), 8);    // 读取长度
        if (file.gcount() != 8) return true;
        std::string s(len, '\0');
        file.read(&s[0], len);      // 读取内容
        if ((uint64_t)file.gcount() != len) return true;

        // 解析快照
        SnapshotMetadata snap;
        if (!snap.ParseFromString(s))
        {
            std::cerr << "[Raft " << my_id_ << "] Bad snapshot\n";
            return false;
        }
        // 恢复上层状态机
        if (restore_cb_ && !snap.metadata_store_data().empty())
        {
            restore_cb_(snap.metadata_store_data());    // 恢复文件和blocks
        }

        // 恢复快照位置
        snapshot_last_term_ = snap.last_included_term();
        snapshot_last_index_ = snap.last_included_index();
        if (commit_index_ < snapshot_last_index_)
        {
            // 状态机已经恢复到快照索引，所以提交位置和应用位置不能落后于它
            commit_index_ = snapshot_last_index_;
            last_applied_ = snapshot_last_index_;
        }

        std::cout << "[Raft " << my_id_ << "] Restored snapshot index="
                << snapshot_last_index_ << std::endl;
        return true;
    }

    // Handle an incoming request. If non-null response is returned, caller sends it back.
    RequestVoteResponse* RaftNode::HandleRequestVote(const std::string &from,
                                            const RequestVoteRequest &req);
    AppendEntriesResponse* RaftNode::HandleAppendEntries(const std::string &from,
                                                const AppendEntriesRequest &req);
    InstallSnapshotResponse* RaftNode::HandleInstallSnapshot(const std::string &from,
                                                    const InstallSnapshotRequest &req);                                     

    // Handle response messages (arriving from peers we sent requests to)
    void RaftNode::HandleRequestVoteResponse(const std::string &from,
                                    const RequestVoteResponse &resp);
    void RaftNode::HandleAppendEntriesResponse(const std::string &from,
                                    const AppendEntriesResponse &resp);

    // Serialize current MetadataStore state for snapshot
    std::string RaftNode::GetSnapshotData() const;

    // 把当前节点切换为 Follower（跟随者）
    // 参数 term 是触发此次状态转换的任期，通常来自其他节点发送的 RPC
    void RaftNode::BecomeFollower(uint64_t term)
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);

        if (term > current_term_)
        {
            current_term_ = term;   // 更新自己的 current_term_
            voted_for_.clear();     // 清除上一任期的投票记录
            PersistState();         // 将新任期和投票状态持久化到磁盘
        }
        state_ = RaftState::FOLLOWER;
        leader_id_.clear();     // 此时还不能确定新 Leader 是谁，所以先清空
        std::cout << "[Raft " << my_id_ << "] BecomeFollower term=" << current_term_ << std::endl;
    }

    void RaftNode::BecomeCandidate()
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);

        state_ = RaftState::CANDIDATE;
        current_term_++;
        voted_for_ = my_id_;
        leader_id_.clear();
        votes_received_ = 1;
        PersistState();
        std::cout << "[Raft " << my_id_ << "] BecomeCandidate term=" << current_term_ << std::endl;
    }

    // 用于节点赢得选举后切换为 Leader，并初始化日志复制状态
    void RaftNode::BecomeLeader()
    {
        {
            std::lock_guard<std::recursive_mutex> lock(state_mutex_);
            state_ = RaftState::LEADER;
            leader_id_ = my_id_;
            uint64_t last_log_index = std::max(log_.back().index, snapshot_last_index_);
            next_index_.clear();    // 清除上一轮担任 Leader 时留下的复制进度
            match_index_.clear();   // 清除上一轮担任 Leader 时留下的复制进度

            for (const auto &peer : config_.peers)
            {
                if (peer == my_id_) continue;

                // 刚成为 Leader 时，并不知道 Follower 实际拥有多少日志，
                // 因此先假设它已经同步到 Leader 的末尾。如果 AppendEntries 失败，再逐步回退 next_index_
                next_index_[peer] = last_log_index + 1; // 下一条准备发送给该 Follower 的日志索引
                match_index_[peer] = 0; // 已确认复制到该 Follower 的最大日志索引
            }
        }

        SendHeartbeats();
        ResetElectionTimer();
        std::cout << "[Raft " << my_id_ << "] BecomeLeader term=" << current_term_
                << " log_size=" << (log_.size() - 1) << std::endl;
    }

    // 候选节点发起选举
    void RaftNode::StartElection()
    {
        // 准备自己的日志信息，向其他节点发送 RequestVote，然后检查自己是否已经拿到多数票

        uint64_t last_log_term;
        uint64_t last_log_index;
        uint64_t term;
        {
            std::lock_guard<std::recursive_mutex> lock(state_mutex_);
            last_log_term = log_.back().term;
            last_log_index = log_.back().index;
            term = current_term_;
        }

        RequestVoteRequest req;
        req.set_term(term);
        req.set_candidate_id(my_id_);
        req.set_last_log_term(last_log_term);
        req.set_last_log_index(last_log_index);

        RaftMessage msg;
        msg.set_type(RaftMessage::REQUEST_VOTE);
        *msg.mutable_request_vote() = req;

        for (const auto &peer : config_.peers)
        {
            if (peer == my_id_) continue;

            send_rpc_cb_(peer, msg);
        }

        // 这里好像是发起投票请求后，还没收到响应，立马检查票数，不太合理
        // 除非 send_rpc_cb_ 回调函数会处理
        bool should_lead = false;
        {
            std::lock_guard<std::recursive_mutex> lock(state_mutex_);

            int majority = (int)config_.peers.size() / 2 + 1;
            if (state_ == RaftState::CANDIDATE && votes_received_ >= majority)
            {
                should_lead = true;
            }
        }
        if (should_lead)
        {
            BecomeLeader();
        }
    }

    // Leader 向其他节点发送 AppendEntries RPC 的函数。它既可以发送空心跳，
    // 也可以顺便复制缺失日志。
    void RaftNode::SendHeartbeats()
    {
        // 加锁并检查身份, 只有 Leader 能发送心跳
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        if (state_ != RaftState::LEADER) return;

        // 遍历所有其他节点
        for (const auto &peer : config_.peers)
        {
            if (peer == my_id_) continue;   // 跳过自己

            // 确定日志衔接位置
            uint64_t next_idx = next_index_[peer];  // 下一条需要发给该 Follower 的日志索引
            uint64_t prev_log_index = next_idx - 1; // 前一条日志索引
            uint64_t prev_log_term = 0; // 前一条日志所属任期

            // 确定 prev_log_index 对应的任期 prev_log_term
            if (prev_log_index > snapshot_last_index_)
            {
                const LogEntry *prev = GetLogEntry(prev_log_index);
                if (prev) prev_log_term = prev->term;
            }
            else if (prev_log_index == snapshot_last_index_)
            {
                prev_log_term = snapshot_last_term_;
            }

            // 构造 AppendEntries 请求
            AppendEntriesRequest req;
            req.set_term(current_term_);
            req.set_leader_id(my_id_);
            req.set_prev_log_index(prev_log_index);
            req.set_prev_log_term(prev_log_term);
            req.set_leader_commit(commit_index_);

            // 添加 Follower 缺失的日志
            for (const auto &log : log_)
            {
                if (log.index < next_idx) continue;

                auto *entry = req.add_entries();
                entry->set_term(log.term);
                entry->set_index(log.index);
                entry->set_command(log.command);
            }

            // 包装并发送
            RaftMessage msg;
            msg.set_type(RaftMessage::APPEND_ENTRIES);
            *msg.mutable_append_entries() = req;
            send_rpc_cb_(peer, msg);
        }
    }

    void RaftNode::AdvanceCommitIndex();
    void RaftNode::ApplyCommitted(uint64_t up_to_index);

    LogEntry* RaftNode::GetLogEntry(uint64_t index)
    {
        for (auto &e : log_)
        {
            if (e.index == index) return &e;
        }

        return nullptr;
    }

    const LogEntry* RaftNode::GetLogEntry(uint64_t index) const
    {
        for (const auto &e : log_)
        {
            if (e.index == index) return &e;
        }

        return nullptr;
    }

    // 把 Raft 节点的重要状态写入磁盘，节点重启后可由 RestoreState() 恢复
    bool RaftNode::PersistState()
    {
        // std::ios::trunc：如果文件已存在，先清空原内容；如果不存在，则创建文件。
        std::ofstream file(StateFilePath(), std::ios::binary | std::ios::trunc);
        if (!file.is_open())
        {
            std::cerr << "[Raft " << my_id_ << "] Failed to write state\n";
            return false;
        }

        // 写入当前任期
        file.write(reinterpret_cast<const char*>(&current_term_), 8);
        // 写入投票信息
        uint64_t vf_len = voted_for_.size();
        file .write(reinterpret_cast<const char*>(&vf_len), 8);
        if (vf_len > 0) file.write(voted_for_.data(), vf_len);

        // 写入日志数量
        uint64_t log_count = log_.size() - 1;
        file.write(reinterpret_cast<const char*>(&log_count), 8);
        // 逐条写入日志
        for (size_t i = 1; i < log_.size(); ++i)
        {
            file.write(reinterpret_cast<const char*>(&log_[i].term), 8);    // term（8 字节）
            file.write(reinterpret_cast<const char*>(&log_[i].index), 8);   // index（8 字节）
            uint64_t cmd_len = log_[i].command.size();
            file.write(reinterpret_cast<const char*>(&cmd_len), 8); // command 长度（8 字节）
            if (cmd_len > 0) file.write(log_[i].command.data(), cmd_len);   // command 内容（变长）
        }
        file.close();
        return !file.fail();
    }

    // 节点启动时，从磁盘文件中恢复 Raft 的任期、投票记录和日志
    bool RaftNode::RestoreState()
    {
        // 打开状态文件
        std::ifstream file(StateFilePath(), std::ios::binary);
        if (!file.is_open()) return true;

        // 恢复当前任期
        file.read(reinterpret_cast<char*>(&current_term_), 8);
        if (file.gcount() != 8) return true;

        // 恢复投票对象
        uint64_t vf_len = 0;
        file.read(reinterpret_cast<char*>(&vf_len), 8);
        if (file.gcount() == 8 && vf_len > 0)
        {
            voted_for_.resize(vf_len);
            file.read(&voted_for_[0], vf_len);
        }

        // 读取日志数量
        uint64_t log_count = 0;
        file.read(reinterpret_cast<char*>(&log_count), 8);
        if (file.gcount() != 8) return true;

        // 重建日志数组
        log_.clear();
        LogEntry d; d.term = 0; d.index = 0;
        log_.push_back(d);  // 先重新加入索引为 0 的哨兵日志
        for (uint64_t i = 0; i < log_count; ++i)    // 逐条恢复日志
        {
            LogEntry entry;
            file.read(reinterpret_cast<char*>(&entry.term), 8);     // 日志所属任期
            file.read(reinterpret_cast<char*>(&entry.index), 8);    // 日志索引
            uint64_t cmd_len = 0;           
            file.read(reinterpret_cast<char*>(&cmd_len), 8);        // 命令长度
            if (cmd_len > 0)
            {
                entry.command.resize(cmd_len);
                file.read(&entry.command[0], cmd_len);              // 具体命令内容
            }
            log_.push_back(entry);
        }

        commit_index_ = log_.back().index;
        last_applied_ = commit_index_;
        std::cout << "[Raft " << my_id_ << "] Restored term=" << current_term_
                << " log=" << (log_.size() - 1) << " commit=" << commit_index_ << std::endl;

        return true;
    }

    std::string RaftNode::StateFilePath() const
    {
        // 该文件通常用于保存 current_term、voted_for 等需要在节点重启后保留的状态
        return config_.data_dir + "/raft_state.dat";
    }

    std::string RaftNode::SnapshotFilePath() const
    {
        return config_.data_dir + "/raft_snapshot.dat";
    }

    // 重新设置选举超时时间
    void RaftNode::ResetElectionTimer()
    {
        std::lock_guard<std::mutex> lock(timer_mutex_);

        int timeout_ms = RandomElectionTimeout();
        election_deadline_ = std::chrono::steady_clock::now()
                            + std::chrono::milliseconds(timeout_ms);
    }

    void RaftNode::TimerLoop();

    // 生成一个随机的选举超时时间，单位是毫秒
    // 随机化的主要目的是避免多个 Follower 同时超时、同时成为 Candidate，
    // 导致互相分票。各节点超时时间不同，更容易让其中一个节点率先获得多数票：
    int RaftNode::RandomElectionTimeout() const
    {
        // std::random_device{}()：为生成器提供随机种子
        // std::mt19937：梅森旋转随机数生成器
        // static：每个线程只初始化一次，避免每次调用都重新创建
        // thread_local：每个线程拥有独立的生成器，避免多线程同时访问产生数据竞争
        static thread_local std::mt19937 gen(std::random_device{}());
        std::uniform_int_distribution<int> dist(    // 定义均匀整数分布
            config_.election_timeout_ms,
            config_.election_timeout_ms * 2);
        return dist(gen);   // 使用随机数生成器产生一个范围内的整数并返回
    }

} // namespace mini_storage