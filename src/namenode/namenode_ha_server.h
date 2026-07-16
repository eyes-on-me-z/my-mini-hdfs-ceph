#pragma once

#include <string>
#include <memory>

#include "namenode_metadata_store.h"
#include "namenode_block_manager.h"
#include "namenode_datanode_manager.h"
#include "namenode_fault_detector.h"
#include "raft_node.h"
#include "raft_rpc.h"
#include "net_thread_pool.h"
#include "namenode.pb.h"

/*
这个 HANameNodeServer 和 NameNodeServer 有什么区别
*/

namespace mini_storage
{
    // HANameNodeServer: High-Availability NameNode with Raft consensus.
    // Replaces NameNodeServer in Phase 3 deployments.
    // Creates a Raft cluster among multiple NameNodes for leader election
    // and state replication.
    class HANameNodeServer
    {
    public:
        // cluster_peers: list of all NameNode addresses ("host:raft_port")
        // my_peer_id: which one of those is me
        // client_host/client_port: the existing NameNode client service port
        // raft_port: port for Raft inter-node communication
        // HA 模式下每个 NameNode 既要对客户端提供服务，也要参与 Raft 集群
        HANameNodeServer(const std::string &data_dir,   // 本地数据目录
                        const std::string &client_host, int client_port,    // 面向客户端的服务地址
                        const std::string &raft_host, int raft_port,        // 当前 NameNode 在 Raft 集群内部通信使用的地址
                        const std::vector<std::string> &cluster_peers,  // 整个 Raft 集群的节点列表
                        int num_workers = 4);
        ~HANameNodeServer();

        bool Start();
        void Stop();

        // Accessors for tests
        MetadataStore* GetMetadataStore() { return metadata_.get(); }
        DataNodeManager* GetDataNodeManager() { return dn_manager_.get(); }
        FaultDetector* GetFaultDetector() { return fault_detector_.get(); }
        RaftNode* GetRaftNode() { return raft_node_.get(); }
        bool IsLeader();

    private:
        // Client request handling (same protocol as NameNodeServer)
        void OnClientMessage(std::shared_ptr<TcpConnection> conn, const std::string &data);
        void ProcessClientRequest(std::shared_ptr<TcpConnection> conn, const std::string &data);

        // Raft callbacks
        void OnApplyCommitted(uint64_t index, const std::string &command);
        void OnSendRaftRPC(const std::string &peer_id, const RaftMessage &msg);

        // Snapshot support
        std::string TakeMetadataSnapshot();
        bool RestoreMetadataSnapshot(const std::string &data);

        // Propose a write operation through Raft
        NameNodeResponse ProposeWrite(const NameNodeRequest &req, int timeout_ms = 5000);

        // Direct handlers (for reads and ephemeral ops)
        NameNodeResponse HandleGetFileBlocks(const GetFileBlocksRequest &req);
        NameNodeResponse HandleListFiles(const ListFilesRequest &req);
        NameNodeResponse HandleRegisterDN(const RegisterDataNodeRequest &req);
        NameNodeResponse HandleHeartbeat(const HeartbeatRequest &req);
        NameNodeResponse HandleBlockReport(const BlockReportRequest &req);

        // Apply handlers (called when Raft commits a log entry)
        void ApplyCreateFile(const CreateFileRequest &req);
        void ApplyDeleteFile(const DeleteFileRequest &req);
        void ApplyAllocateBlock(const AllocateBlockRequest &req);

        // Health check
        void StartHealthCheckTimer();

        // NameNode 的本地数据目录。通常用于保存元数据、Raft 日志、快照或恢复状态等持久化文件
        std::string data_dir_;

        // Phase 2 components (same as NameNodeServer)
        std::unique_ptr<MetadataStore> metadata_;
        std::unique_ptr<BlockManager> block_manager_;
        std::unique_ptr<DataNodeManager> dn_manager_;
        std::unique_ptr<FaultDetector> fault_detector_;

        // Raft components
        std::unique_ptr<RaftNode> raft_node_;
        std::unique_ptr<RaftRPC> raft_rpc_;

        // Client-facing network (Phase 2 protocol), 这是面向客户端的网络服务
        std::unique_ptr<EventLoop> client_loop_;
        std::unique_ptr<TcpServer> client_server_;
        std::unique_ptr<ThreadPool> thread_pool_;
        std::thread client_loop_thread_;

        // Health check
        std::thread health_check_thread_;
        std::atomic<bool> stop_{false};

        // Cluster config
        std::string my_peer_id_;    // "raft_host:raft_port"
        std::vector<std::string> cluster_peers_;

        // Next block ID counter (recovered from state)
        std::atomic<uint64_t> next_block_id_{1};    // ?????????????

        // For propose-to-apply result passing
        // 客户端请求提交到 Raft 后，等待该请求真正 apply 到状态机，然后把结果传回给等待线程
        std::mutex propose_result_mutex_;   // 保护 propose_results_ 这个共享 map
        std::condition_variable propose_result_cv_;
        std::unordered_map<uint64_t, NameNodeResponse> propose_results_;
    };

} // namespace mini_storage