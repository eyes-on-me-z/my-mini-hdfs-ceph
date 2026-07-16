#include "namenode_ha_server.h"

#include <filesystem>

namespace fs = std::filesystem;

namespace mini_storage
{
    HANameNodeServer::HANameNodeServer(const std::string &data_dir,   // 本地数据目录
                        const std::string &client_host, int client_port,    // 面向客户端的服务地址
                        const std::string &raft_host, int raft_port,        // 当前 NameNode 在 Raft 集群内部通信使用的地址
                        const std::vector<std::string> &cluster_peers,  // 整个 Raft 集群的节点列表
                        int num_workers)
        : data_dir_(data_dir), cluster_peers_(cluster_peers)
    {
        my_peer_id_ = raft_host + ":" + std::to_string(raft_port);
        fs::create_directories(data_dir_);

        // Phase 2 components
        metadata_ = std::make_unique<MetadataStore>();
        block_manager_ = std::make_unique<BlockManager>(metadata_.get());
        dn_manager_ = std::make_unique<DataNodeManager>();
        fault_detector_ = std::make_unique<FaultDetector>(metadata_.get(), dn_manager_.get(), 15);

        // Raft
        RaftConfig raft_config;
        raft_config.node_id = my_peer_id_;
        raft_config.peers = cluster_peers_;
        raft_config.data_dir = data_dir_ + "/raft";
        raft_config.election_timeout_ms = 2000;
        raft_config.heartbeat_interval_ms = 500;
        raft_node_ = std::make_unique<RaftNode>(raft_config);
        raft_rpc_ = std::make_unique<RaftRPC>(raft_node_.get(), raft_host, raft_port);
        
        // Wire Raft callbacks
        raft_node_->SetApplyCallback([this](uint64_t index, const std::string &command){
            OnApplyCommitted(index, command);
        });
        raft_node_->SetSendRPCCallback([this](const std::string &peer_id, const RaftMessage &msg){
            OnSendRaftRPC(peer_id, msg);
        });
        raft_node_->SetSnapshotCallback([this](){
            return TakeMetadataSnapshot();
        });
        raft_node_->SetRestoreCallback([this](const std::string &data){
            return RestoreMetadataSnapshot(data);
        });

        // Client-facing server
        client_loop_ = std::make_unique<EventLoop>();
        client_server_ = std::make_unique<TcpServer>(client_loop_.get(), client_host, client_port);
        thread_pool_ = std::make_unique<ThreadPool>(num_workers);

        client_server_->SetMessageCallback([this](auto conn, const auto &data){
            OnClientMessage(conn, data);
        });
    }

    HANameNodeServer::~HANameNodeServer()
    {
        Stop();
    }

    // 先让内部一致性系统准备好，再对外接客
    bool HANameNodeServer::Start()
    {
        /*
        不过你这个实现里有一个小细节值得注意：如果 raft_node_->Start() 成功了，
        但 raft_rpc_->Start() 失败，函数直接 return false，没有调用 raft_node_->Stop() 清理已经启动的部分。
        client_server_->Start() 失败时也类似，前面的 Raft 已经启动了。
        也就是说，启动顺序没问题，但失败回滚不太完整。
        */

        // Start Raft。先准备好 Raft 内部状态，再开放 Raft 网络入口
        if (!raft_node_->Start())
        {
            std::cerr << "[HANameNode] Failed to start Raft\n";
            return false;
        }

        if (!raft_rpc_->Start())
        {
            std::cerr << "[HANameNode] Failed to start RaftRPC\n";
            return false;
        }

        // Start client server
        if (!client_server_->Start())
        {
            std::cerr << "[HANameNode] Failed to start client server\n";
            return false;
        }
        client_loop_thread_ = std::thread([this](){
            client_loop_->Loop();
        });

        // Start health check and fault detector
        StartHealthCheckTimer();
        fault_detector_->Start();   // 里面会开启一个异步线程

        std::cout << "[HANameNode] Started successfully" << std::endl;
        return true;
    }

    void HANameNodeServer::Stop()
    {
        stop_.store(true);
        fault_detector_->Stop();
        if (health_check_thread_.joinable()) { health_check_thread_.join(); }
        raft_node_->Stop();
        raft_rpc_->Stop();
        thread_pool_->Stop();
        client_loop_->Quit();
        if (client_loop_thread_.joinable()) { client_loop_thread_.join(); }
    }

    bool HANameNodeServer::IsLeader()
    {
        return raft_node_->IsLeader();
    }

    // ===== Client Message Handling =====

    // Client request handling (same protocol as NameNodeServer)
    void HANameNodeServer::OnClientMessage(std::shared_ptr<TcpConnection> conn, const std::string &data);
    void HANameNodeServer::ProcessClientRequest(std::shared_ptr<TcpConnection> conn, const std::string &data);

    // Raft callbacks
    void HANameNodeServer::OnApplyCommitted(uint64_t index, const std::string &command);
    void HANameNodeServer::OnSendRaftRPC(const std::string &peer_id, const RaftMessage &msg);

    // Snapshot support
    std::string HANameNodeServer::TakeMetadataSnapshot();
    bool HANameNodeServer::RestoreMetadataSnapshot(const std::string &data);

    // Propose a write operation through Raft
    NameNodeResponse HANameNodeServer::ProposeWrite(const NameNodeRequest &req, int timeout_ms = 5000);

    // Direct handlers (for reads and ephemeral ops)
    NameNodeResponse HANameNodeServer::HandleGetFileBlocks(const GetFileBlocksRequest &req);
    NameNodeResponse HANameNodeServer::HandleListFiles(const ListFilesRequest &req);
    NameNodeResponse HANameNodeServer::HandleRegisterDN(const RegisterDataNodeRequest &req);
    NameNodeResponse HANameNodeServer::HandleHeartbeat(const HeartbeatRequest &req);
    NameNodeResponse HANameNodeServer::HandleBlockReport(const BlockReportRequest &req);

    // Apply handlers (called when Raft commits a log entry)
    void HANameNodeServer::ApplyCreateFile(const CreateFileRequest &req);
    void HANameNodeServer::ApplyDeleteFile(const DeleteFileRequest &req);
    void HANameNodeServer::ApplyAllocateBlock(const AllocateBlockRequest &req);

    // Health check
    void HANameNodeServer::StartHealthCheckTimer();
} // namespace mini_storage