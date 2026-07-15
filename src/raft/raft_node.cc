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

    // 负责恢复持久化状态、初始化身份、启动后台定时器线程
    bool RaftNode::Start()
    {
        RestoreState();
        RestoreFromSnapshot();
        BecomeFollower(current_term_);
        stop_.store(false);
        ResetElectionTimer();
        timer_thread_ = std::thread(&RaftNode::TimerLoop, this);
        std::cout << "[Raft " << my_id_ << "] Started term=" << current_term_
                << " log_size=" << (log_.size() - 1) << std::endl;
        
        return true;
    }

    void RaftNode::Stop()
    {
        stop_.store(true);
        if (timer_thread_.joinable())
        {
            timer_thread_.join();
        }
    }

    // 客户端向 Raft 集群提交一条命令 的入口
    RaftNode::ProposeResult RaftNode::Propose(const std::string &command, int timeout_ms)
    {
        // 初始化返回结果
        ProposeResult result;
        result.committed = false;
        result.is_leader = false;

        {
            std::lock_guard<std::recursive_mutex> lock(state_mutex_);

            // 判断自己是不是 leader。Raft 里只有 leader 能处理写请求
            if (state_ != RaftState::LEADER)
            {
                result.leader_id = leader_id_;
                return result;
            }

            // leader 把 command 追加到本地日志
            result.is_leader = true;

            LogEntry entry;
            entry.term = current_term_;
            entry.index = log_.back().index + 1;
            entry.command = command;
            log_.push_back(entry);
            PersistState();

            // 更新 leader 自己的复制进度
            next_index_[my_id_] = entry.index + 1;
            match_index_[my_id_] = entry.index;
            result.index = entry.index;
        }

        // Try to advance commit immediately (needed for single-node clusters)
        // 尝试推进 commit，并发送复制请求
        {
            std::lock_guard<std::recursive_mutex> lock(state_mutex_);
            // 单节点集群立即提交新日志
            // 尝试提交之前已经复制到多数派、但还没来得及推进的日志
            AdvanceCommitIndex();   
        }

        SendHeartbeats();

        {
            std::unique_lock<std::mutex> lock(propose_mutex_);
            auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

            while(true)
            {
                {
                    std::lock_guard<std::recursive_mutex> sl(state_mutex_);

                    // 提交成功优先于 leader 身份变化。
                    if (commit_index_ >= result.index)  // 这条日志已经提交
                    {
                        result.committed = true;
                        break;
                    }

                    if (state_ != RaftState::LEADER)    // 等待过程中自己不再是 leader
                    {
                        result.is_leader = false;
                        result.leader_id = leader_id_;
                        break;
                    }
                }
                // 如果一直没有提交，直到超时
                if (propose_cv_.wait_until(lock, deadline) == std::cv_status::timeout) break;
            }
        }

        return result;
    }

    // Query
    bool RaftNode::IsLeader() const
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        return state_ == RaftState::LEADER;
    }

    std::string RaftNode::GetLeaderId() const
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        return leader_id_;
    }

    RaftState RaftNode::GetState() const
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        return state_;
    }

    uint64_t RaftNode::GetCurrentTerm() const
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        return current_term_;
    }

    uint64_t RaftNode::GetCommitIndex() const
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        return commit_index_;
    }

    uint64_t RaftNode::GetLastLogIndex() const
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        return log_.back().index;
    }

    uint64_t RaftNode::GetLastLogTerm() const
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        return log_.back().term;
    }

    size_t RaftNode::GetLogSize() const
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        return log_.size() - 1;
    }

    // Timer
    // 选举定时器到期时触发。leader：发送心跳，重置定时器。follower：成为候选者，开始选举，重置定时器
    void RaftNode::OnElectionTimeout()
    {
        RaftState s;
        {
            std::lock_guard<std::recursive_mutex> lock(state_mutex_);
            s = state_;
        }

        if (s == RaftState::LEADER)
        {
            SendHeartbeats();
            ResetElectionTimer();
        }
        else
        {
            BecomeCandidate();
            StartElection();
            ResetElectionTimer();
        }
    }

    // 把已经应用到状态机的日志压缩掉，减少日志文件长度
    // 从状态机拿一份快照数据，写入快照文件，然后删除已被快照覆盖的日志。
    bool RaftNode::TakeSnapshot()
    {
        // 没有快照回调就直接失败
        if (!snapshot_cb_) return false;

        // 取当前可快照的位置
        std::string data;               // 状态机快照的实际内容
        uint64_t last_included_index;   // 这次快照覆盖到的最后一条日志 index
        uint64_t last_included_term;    // 对应日志的 term
        {
            std::lock_guard<std::recursive_mutex> lock(state_mutex_);
            // 表示当前节点已经应用到状态机的最高日志 index
            // 表示当前已有快照已经覆盖到的最高日志 index
            if (last_applied_ <= snapshot_last_index_) return false;
            data = snapshot_cb_();
            last_included_index = last_applied_;    // 快照最多只能覆盖已经应用到状态机的日志
            const LogEntry *entry = GetLogEntry(last_included_index);
            last_included_term = entry ? entry->term : current_term_;
        }

        // 构造并序列化快照元数据
        SnapshotMetadata snap;
        snap.set_last_included_index(last_included_index);
        snap.set_last_included_term(last_included_term);
        snap.set_metadata_store_data(data);

        std::string serialized;
        snap.SerializeToString(&serialized);

        // 写入快照文件
        std::string tmp = SnapshotFilePath() + ".tmp";
        {
            std::ofstream file(tmp, std::ios::binary | std::ios::trunc);
            if (!file.is_open()) return false;

            uint64_t len = serialized.size();
            file.write(reinterpret_cast<const char*>(&len), 8);
            file.write(serialized.data(), len);
        }
        fs::rename(tmp, SnapshotFilePath());

        // 更新本地 snapshot 元信息并裁剪日志
        {
            std::lock_guard<std::recursive_mutex> lock(state_mutex_);
            snapshot_last_index_ = last_included_index;
            snapshot_last_term_ = last_included_term;
            std::vector<LogEntry> new_log;
            new_log.push_back(log_[0]);
            for (size_t i = 1; i < log_.size(); ++i)
            {
                if (log_[i].index > last_included_index)
                    new_log.push_back(log_[i]);
            }
            log_.swap(new_log);
            PersistState();
        }

        std::cout << "[Raft " << my_id_ << "] Snapshot at index " << last_included_index << std::endl;
        return true;
    }

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

    // 处理别人发来的“请求投票”，这个节点要不要把当前任期的一票投给对方
    RequestVoteResponse* RaftNode::HandleRequestVote(const std::string &from,
                                            const RequestVoteRequest &req)
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);

        vote_resp_.set_term(current_term_);

        // 对方任期比自己旧。Raft 里旧任期的候选人不能拿到新任期节点的票
        if (req.term() < current_term_)
        {
            vote_resp_.set_vote_granted(false); // 直接拒绝
            return &vote_resp_;
        }

        // 对方任期比自己新
        if (req.term() > current_term_)
        {
            current_term_ = req.term();
            state_ = RaftState::FOLLOWER;   // 退回 follower
            voted_for_.clear();     // 进入了新任期，本任期还没投票
            leader_id_.clear();     // 旧 leader 不再属于当前任期
            PersistState();
            vote_resp_.set_term(current_term_);
        }

        // 判断自己能不能投票
        bool can_vote = false;
        // 当前任期还没投过票 或 已经投过这个候选人（处理 RPC 重试）
        if (voted_for_.empty() || voted_for_ == req.candidate_id())
        {
            // Raft 的投票规则要求：不能把票投给日志比自己旧的候选人
            uint64_t my_last_term = log_.back().term;
            uint64_t my_last_index = log_.back().index;
            if (req.last_log_term() > my_last_term ||
                (req.last_log_term() == my_last_term && req.last_log_index() >= my_last_index))
            {
                can_vote = true;
            }
        }

        if (can_vote)
        {
            // 记录这一任期投给了谁，并持久化
            // 这里持久化很重要，因为 Raft 要保证“一个节点在同一任期最多投一票”，
            // 即使节点崩溃重启也不能忘记自己投过谁
            voted_for_ = req.candidate_id();
            PersistState();
            vote_resp_.set_vote_granted(true);
            ResetElectionTimer();   // 因为已经认可了一个候选人，暂时不应该马上自己发起选举
            std::cout << "[Raft " << my_id_ << "] Voted for " << req.candidate_id()
                << " in term " << current_term_ << std::endl;
        }
        else
        {
            vote_resp_.set_vote_granted(false);
        }

        return &vote_resp_;
    }
    
    AppendEntriesResponse* RaftNode::HandleAppendEntries(const std::string &from,
                                                const AppendEntriesRequest &req)
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        ae_resp_.set_term(current_term_);

        // 如果 leader 的 term 比自己旧
        if (req.term() < current_term_)
        {
            ae_resp_.set_success(false);
            return &ae_resp_;
        }

        // 如果请求的 term 比自己新
        if (req.term() > current_term_)
        {
            current_term_ = req.term();
            state_ = RaftState::FOLLOWER;
            voted_for_.clear(); // 清空 voted_for_，因为新 term 还没投票
            PersistState();
            ae_resp_.set_term(current_term_);
        }

        leader_id_ = from;
        state_ = RaftState::FOLLOWER;
        ResetElectionTimer();

        uint64_t prev_log_index = req.prev_log_index();
        uint64_t prev_log_term = req.prev_log_term();

        // 如果 follower 日志太短
        if (prev_log_index > log_.back().index)
        {
            ae_resp_.set_success(false);
            ae_resp_.set_match_index(log_.back().index);
            return &ae_resp_;
        }

        if (prev_log_index >= snapshot_last_index_)
        {
            const LogEntry *prev = GetLogEntry(prev_log_index);

            if (prev && prev->term != prev_log_term)
            {
                // 截断 follower 的冲突日志。从 prev_log_index 开始往后找第一个 term 和 prev_log_term 不同的位置，
                // 然后把这个位置以及之后的日志全部删除
                for (uint64_t idx = prev_log_index; idx <= log_.back().index; ++idx)
                {
                    const LogEntry *e = GetLogEntry(idx);
                    if (e && e->term != prev_log_term)
                    {
                        while(log_.back().index >= idx)
                        {
                            log_.pop_back();
                        }
                        break;
                    }
                }
                ae_resp_.set_success(false);
                ae_resp_.set_match_index(log_.back().index);
                PersistState();
                return &ae_resp_;
            }
        }
        else    // leader 要检查的那条日志，比 follower 的快照点还旧
        {       // 也就是说 follower 本地已经没有这条普通日志了，因为它被快照压缩掉了
            if (prev_log_index != snapshot_last_index_ || prev_log_term != snapshot_last_term_)
            {
                ae_resp_.set_success(false);
                ae_resp_.set_match_index(snapshot_last_index_);
                return &ae_resp_;
            }
        }

        // 日志匹配成功后，开始追加新日志
        for (int i = 0; i < req.entries_size(); ++i)
        {
            const auto &entry = req.entries(i);
            uint64_t entry_index = entry.index();
            if (entry_index > log_.back().index)
            {
                LogEntry le;
                le.index = entry.index();
                le.term = entry.term();
                le.command = entry.command();
                log_.push_back(le);
            }
        }
        PersistState();

        if (req.leader_commit() > commit_index_)
        {
            // follower 的 commit_index_ 不能超过 leader 的提交进度，也不能超过自己本地已有日志的最后位置
            uint64_t new_commit = std::min(req.leader_commit(), log_.back().index);
            if (new_commit > commit_index_)
            {
                commit_index_ = new_commit;
                ApplyCommitted(commit_index_);
            }
        }

        ae_resp_.set_success(true);
        ae_resp_.set_match_index(log_.back().index);
        return &ae_resp_;
    }

    // follower 处理 leader 发来的 InstallSnapshot RPC 的函数
    // 当 follower 落后太多，leader 已经没有足够旧日志可以通过 AppendEntries 补齐时，
    // leader 直接把一个快照发给 follower，让 follower 用快照追上状态
    InstallSnapshotResponse* RaftNode::HandleInstallSnapshot(const std::string &from,
                                                    const InstallSnapshotRequest &req)
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);
        snap_resp_.set_term(current_term_);

        // 拒绝旧 term 的快照
        if (req.term() < current_term_)
        {
            snap_resp_.set_success(false);
            return &snap_resp_;
        }

        // 如果请求 term 更新，就更新自己 term
        if (req.term() > current_term_)
        {
            current_term_ = req.term();
            voted_for_.clear();
            leader_id_.clear();
            state_ = RaftState::FOLLOWER;
            PersistState();
            snap_resp_.set_term(current_term_);
        }

        leader_id_ = from;
        state_ = RaftState::FOLLOWER;
        ResetElectionTimer();

        // 忽略旧快照。
        // leader 发来的快照并不比自己现有快照新，或者快照 index 不超过自己已经提交的 index，就认为不需要安装
        if (req.last_included_index() <= snapshot_last_term_ ||
            req.last_included_index() <= commit_index_)
        {
            snap_resp_.set_success(true);
            return &snap_resp_;
        }

        // 构造快照元数据并序列化
        SnapshotMetadata snap;
        snap.set_last_included_term(req.last_included_term());
        snap.set_last_included_index(req.last_included_index());
        snap.set_metadata_store_data(req.snapshot_data());

        std::string serialized;
        snap.SerializeToString(&serialized);

        // 写入快照文件。先写到临时文件，写完后 rename 成正式快照文件
        std::string tmp_path = SnapshotFilePath() + ".tmp";
        {
            std::ofstream file(tmp_path, std::ios::binary | std::ios::trunc);
            if (!file.is_open())
            {
                snap_resp_.set_success(false);
                return &snap_resp_;
            }

            uint64_t len = serialized.size();
            file.write(reinterpret_cast<const char*>(&len), 8);
            file.write(serialized.data(), len);
            file.close();
        }
        fs::rename(tmp_path, SnapshotFilePath());

        // 把快照恢复到状态机
        if (restore_cb_ && !req.snapshot_data().empty())
            restore_cb_(req.snapshot_data());

        // 更新 snapshot 元信息
        snapshot_last_index_ = req.last_included_index();
        snapshot_last_term_ = req.last_included_term();

        // 删除快照覆盖掉的旧日志。log index <= snapshot_last_index_ 的日志都可以删
        std::vector<LogEntry> new_log;
        new_log.push_back(log_[0]);
        for (size_t i = 1; i < log_.size(); ++i)
        {
            if (log_[i].index > snapshot_last_index_)
                new_log.push_back(log_[i]);
        }
        log_.swap(new_log);

        // 推进 commit / apply 指针
        if (commit_index_ < snapshot_last_index_)
        {
            commit_index_ = snapshot_last_index_;
            last_applied_ = snapshot_last_index_;
        }
        PersistState();
        snap_resp_.set_success(true);

        std::cout << "[Raft " << my_id_ << "] Installed snapshot from " << from
                << " at index " << req.last_included_index() << std::endl;
        return &snap_resp_;
    }                          

    // Handle response messages (arriving from peers we sent requests to)
    void RaftNode::HandleRequestVoteResponse(const std::string &from,
                                    const RequestVoteResponse &resp)
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);

        // 如果对方任期更高，自己立刻认输并退回 follower
        if (resp.term() > current_term_)
        {
            // 任何节点只要发现更高的 term，就必须更新自己的 current_term_，并变成 follower
            current_term_ = resp.term();
            state_ = RaftState::FOLLOWER;
            voted_for_.clear();     // 本任期还没有投票记录
            leader_id_.clear();     // 新 term 里 leader 还未知
            PersistState(); 
            return;
        }

        if (state_ != RaftState::CANDIDATE) return;
        if (resp.term() < current_term_) return;

        if (resp.vote_granted())
        {
            votes_received_++;
            std::cout << "[Raft " << my_id_ << "] Got vote from " << from
                    << " (" << votes_received_ << "/" << config_.peers.size() << ")\n";

            int majority = (int)config_.peers.size() / 2 + 1;
            if (votes_received_ >= majority) BecomeLeader();
        }
    }

    // leader 收到 follower 对 AppendEntries 的响应之后，更新复制进度
    void RaftNode::HandleAppendEntriesResponse(const std::string &from,
                                    const AppendEntriesResponse &resp)
    {
        std::lock_guard<std::recursive_mutex> lock(state_mutex_);

        // 响应里的 term 比自己新
        // Raft 规则：只要发现更高 term，当前节点必须退回 follower。
        // 所以这里更新 current_term_，清空投票记录和 leader 记录，持久化，然后返回
        if (resp.term() > current_term_)
        {
            current_term_ = resp.term();
            state_ = RaftState::FOLLOWER;
            voted_for_.clear();
            leader_id_.clear();
            PersistState();
            return;
        }

        // 自己已经不是 leader
        if (state_ != RaftState::LEADER) return;

        if (resp.success())     // 响应成功
        {
            match_index_[from] = resp.match_index();    // leader 已知 follower from 已经复制成功的最高日志 index
            next_index_[from] = resp.match_index() + 1; // leader 下一次要从哪个日志 index 开始给这个 follower 发送日志
            AdvanceCommitIndex();   // 尝试推进 leader 的 commit_index_
        }
        else        // 响应失败
        {
            // AppendEntries 失败时，leader 并不能可靠确认 follower 新复制成功了哪一条日志，
            // 所以不能更新 match_index_。失败时通常只回退next_index_[from]
            uint64_t match = resp.match_index();
            if (match < next_index_[from])
            {
                // 不要退到 leader 本地 snapshot 之前，因为 leader 已经没有那些日志了
                next_index_[from] = std::max(match + 1, snapshot_last_index_ + 1);
                if (next_index_[from] < 1) next_index_[from] = 1;
            }
        }

        propose_cv_.notify_all();
    }

    // 把当前节点切换为 Follower（跟随者）
    // 参数 term 是触发此次状态转换的任期
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

    // leader 判断某个日志条目是否已经被多数节点复制，如果满足条件，就把它标记为已提交，并应用到状态机
    void RaftNode::AdvanceCommitIndex()
    {
        /*
        如果存在一个 N，大于当前 commitIndex，且多数节点的 matchIndex[i] >= N，
        并且 log[N].term == currentTerm，则令 commitIndex = N。
        */

        uint64_t last_log_index = log_.back().index;
        for (uint64_t n = last_log_index; n > commit_index_; --n)
        {
            const LogEntry *entry  = GetLogEntry(n);
            // leader 只能通过计数多数派来提交当前任期的日志。即使某条旧任期日志已经被多数节点复制，
            // 当前 leader 也不会直接把它作为推进点。只有找到一条 term == current_term_ 的日志并且它被多数节点复制，
            // 才能提交到那里。不过一旦提交到某个当前任期日志 n，那么 n 之前的旧任期日志也会一起被间接提交
            if (!entry || entry->term != current_term_) continue;
            int count = 1;      // leader 自己肯定有这条日志，所以先计数为 1
            for (const auto &peer : config_.peers)
            {
                if (peer == my_id_) continue;
                auto it = match_index_.find(peer);
                if (it != match_index_.end() && it->second >= n) count++;
            }
            int majority = (int)config_.peers.size() / 2 + 1;
            if (count >= majority)      // 如果拥有日志 n 的节点数达到多数派
            {
                commit_index_ = n;
                ApplyCommitted(commit_index_);
                propose_cv_.notify_all();       // 唤醒等待 Propose() 提交结果的线程
                break;
            }
        }
    }

    // 把已经提交的 Raft 日志应用到上层状态机
    void RaftNode::ApplyCommitted(uint64_t up_to_index)
    {
        // up_to_index 表示最多应用到哪一条日志。
        // last_applied_ 表示已经应用到哪一条日志。
        while(last_applied_ < up_to_index)
        {
            last_applied_++;
            const LogEntry *entry = GetLogEntry(last_applied_);
            if (entry && !entry->command.empty() && apply_cb_)
            {
                apply_cb_(last_applied_, entry->command);
            }
        }
    }

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

    // 把 Raft 节点的重要状态（任期，投票，日志）写入磁盘，节点重启后可由 RestoreState() 恢复
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
    // leader: now + heartbeat_interval_ms
    // follower: now + election_timeout_ms ~ 2 * election_timeout_ms
    void RaftNode::ResetElectionTimer()
    {
        std::lock_guard<std::mutex> lock(timer_mutex_);

        int timeout_ms;
        {
            std::lock_guard<std::recursive_mutex> state_lock(state_mutex_);
            if (state_ == RaftState::LEADER) timeout_ms = config_.heartbeat_interval_ms;
            else timeout_ms = RandomElectionTimeout();
        }
        election_deadline_ = std::chrono::steady_clock::now()
                            + std::chrono::milliseconds(timeout_ms);
    }

    // Raft 节点的后台定时器线程，负责不断检查“选举超时时间到了没有”
    void RaftNode::TimerLoop()
    {
        while(!stop_.load())
        {
            bool should_fire = false;
            {
                std::lock_guard<std::mutex> lock(timer_mutex_);
                auto now = std::chrono::steady_clock::now();
                should_fire = (now >= election_deadline_);
            }
            if (should_fire) OnElectionTimeout();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

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