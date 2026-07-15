#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

#include "raft.pb.h"

namespace mini_storage
{
    // Raft 节点的角色
    enum class RaftState
    {
        FOLLOWER,
        CANDIDATE,
        LEADER
    };

    // Raft 节点的配置
    struct RaftConfig
    {
        std::string node_id;    // 当前 Raft 节点自己的 ID
        std::vector<std::string> peers; // 其他 Raft 节点列表
        std::string data_dir;   // Raft 本地持久化目录, 用来保存 Raft 相关数据
        int election_timeout_ms = 3000; // 选举超时时间
        int heartbeat_interval_ms = 500;    // leader 心跳间隔  当前 ResetElectionTimer() 没有使用 heartbeat_interval_ms
        int snapshot_interval = 10000;  // 快照间隔。通常表示日志增长到一定数量后触发快照，避免 Raft log 无限增长
    };

    // Raft 里的一条日志记录
    struct LogEntry
    {
        // leader 不直接让每个节点随便改状态，而是先把操作写成日志，然后复制到多数节点，确认提交后再执行
        uint64_t term = 0;  // 这条日志产生时的任期号
        uint64_t index = 0; // 这条日志在 Raft log 里的位置
        std::string command;    // 真正要执行的命令
    };

    using ApplyCallback = std::function<void(uint64_t, const std::string&)>;    // 日志提交后，应用到状态机
    using SendRPCCallback = std::function<void(const std::string&, const RaftMessage&)>;    // Raft 内部要发网络消息时调用
    using SnapshotCallback = std::function<std::string()>;  // 让状态机导出当前快照
    using RestoreCallback = std::function<bool(const std::string&)>;    // 用快照恢复状态机
    
    class RaftNode
    {
    public:
        RaftNode(const RaftConfig &config);
        ~RaftNode();

        bool Start();
        void Stop();

        /*
        在 Raft 里，“写入 leader 本地日志”还不等于“提交”。必须复制到多数节点后，
        leader 才能推进 commit_index，这条日志才算 committed。
        Raft 里只有 leader 能直接接受客户端写请求。如果请求打到 follower，
        通常不能直接处理，需要告诉客户端 leader 是谁。
        */
        struct ProposeResult    // 一次 propose 操作的返回结果
        {
            // 在 Raft 里，propose 通常表示：客户端向 Raft 集群提交一条命令，
            // 希望 leader 把它写入日志并复制到多数节点，最终提交给状态机。
            uint64_t index; // 表示这条命令被追加到 Raft 日志里的位置，也就是日志索引
            bool committed; // 这条命令最终是否已经被提交
            bool is_leader; // 当前处理这个 propose 请求的节点是不是 leader
            std::string leader_id;  // 当前已知的 leader 节点 ID
        };
        ProposeResult Propose(const std::string &command, int timeout_ms = 5000);

        // Query
        bool IsLeader() const;
        std::string GetLeaderId() const;
        RaftState GetState() const;
        uint64_t GetCurrentTerm() const;
        uint64_t GetCommitIndex() const;
        uint64_t GetLastLogIndex() const;
        uint64_t GetLastLogTerm() const;
        size_t GetLogSize() const;  // 获取日志条数

        // Callbacks
        void SetApplyCallback(ApplyCallback cb) { apply_cb_ = std::move(cb); }
        void SetSendRPCCallback(SendRPCCallback cb) { send_rpc_cb_ = std::move(cb); }
        void SetSnapshotCallback(SnapshotCallback cb) { snapshot_cb_ = std::move(cb); }
        void SetRestoreCallback(RestoreCallback cb) { restore_cb_ = std::move(cb); }

        // Timer
        void OnElectionTimeout();

        // Snapshot
        bool TakeSnapshot();
        bool RestoreFromSnapshot();

        // === RPC processing (called by RaftRPC) ===
        // These handle incoming messages and return the response to be sent

        // Handle an incoming request. If non-null response is returned, caller sends it back.
        RequestVoteResponse* HandleRequestVote(const std::string &from,
                                                const RequestVoteRequest &req);
        AppendEntriesResponse* HandleAppendEntries(const std::string &from,
                                                    const AppendEntriesRequest &req);
        InstallSnapshotResponse* HandleInstallSnapshot(const std::string &from,
                                                        const InstallSnapshotRequest &req);                                     

        // Handle response messages (arriving from peers we sent requests to)
        void HandleRequestVoteResponse(const std::string &from,
                                        const RequestVoteResponse &resp);
        void HandleAppendEntriesResponse(const std::string &from,
                                        const AppendEntriesResponse &resp);

        // Get the number of peers in cluster       
        int PeerCount() const { return (int)config_.peers.size(); }

        // Helper: is a peer one of ours?
        bool IsPeer(const std::string &id) const 
        {
            for (const auto &p : config_.peers)
            {
                if (p == id) return true;
            }
            return false;
        }

        // Serialize current MetadataStore state for snapshot
        std::string GetSnapshotData() const;
        
    private:
        void BecomeFollower(uint64_t term);
        void BecomeCandidate();
        void BecomeLeader();
        void StartElection();
        void SendHeartbeats();
        void AdvanceCommitIndex();
        void ApplyCommitted(uint64_t up_to_index);
        LogEntry* GetLogEntry(uint64_t index);
        const LogEntry* GetLogEntry(uint64_t index) const;
        bool PersistState();
        bool RestoreState();
        std::string StateFilePath() const;
        std::string SnapshotFilePath() const;
        void ResetElectionTimer();
        void TimerLoop();
        int RandomElectionTimeout() const;

        // Raft 节点的核心状态
        RaftConfig config_;             // 保存节点配置
        uint64_t current_term_{0};      // 当前任期号
        std::string voted_for_;         // 当前任期投票给了谁
        // TakeSnapshot() 成功写完快照之后，HandleInstallSnapshot() 安装 leader 快照之后才会删除删除log_
        std::vector<LogEntry> log_;     // Raft 日志。
        uint64_t commit_index_{0};      // 已经被多数节点确认、可以认为“提交成功”的最大日志下标
        uint64_t last_applied_{0};      // 已经真正应用到状态机的最大日志下标
        std::unordered_map<std::string, uint64_t> next_index_;  // leader 认为下次应该发给某个 follower 的日志下标
        std::unordered_map<std::string, uint64_t> match_index_; // leader 确认某个 follower 已经复制成功到哪个日志下标
        uint64_t snapshot_last_index_{0};   // 快照包含到的最后一条日志 index
        uint64_t snapshot_last_term_{0};    // 这条日志对应的 term

        RaftState state_{RaftState::FOLLOWER};  // 当前 Raft 节点的角色状态
        std::string leader_id_;                 // 前节点认为的 leader 是谁
        std::string my_id_;                     // 当前 Raft 节点自己的 ID

        // Vote counting (for candidate state)
        int votes_received_{0}; // 当前节点作为 candidate 发起选举后，已经收到多少张投票

        std::atomic<bool> stop_{false}; // 会被多个线程同时访问。主线程：设置 stop_ = true。后台线程：不断读取 stop_
        std::thread timer_thread_;
        std::chrono::steady_clock::time_point election_deadline_;   // 下一次选举超时的时间点
        mutable std::mutex timer_mutex_;

        /*
        Raft 节点会被多个线程同时访问：
        定时器线程：触发选举、发送心跳
        RPC 线程：处理投票请求、AppendEntries
        客户端线程：调用 Propose 提交写请求
        */
       // 递归互斥锁，同一线程可以重复加锁多次，但也要解锁相同次数。
        mutable std::recursive_mutex state_mutex_;  // 保护 Raft 的核心状态，防止并发读写冲突。允许同一个线程多次锁同一把锁
        std::condition_variable propose_cv_;    // 用于让提交请求的线程等待某个条件发生
        std::mutex propose_mutex_;

        ApplyCallback apply_cb_;
        SendRPCCallback send_rpc_cb_;
        SnapshotCallback snapshot_cb_;
        RestoreCallback restore_cb_;

        // Pool of response objects (reused to avoid allocation)
        RequestVoteResponse vote_resp_;
        AppendEntriesResponse ae_resp_;
        InstallSnapshotResponse snap_resp_;
    };

} // namespace mini_storage