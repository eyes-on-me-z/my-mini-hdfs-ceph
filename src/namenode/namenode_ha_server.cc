#include "namenode_ha_server.h"
#include "net_tcp_connection.h"

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
    void HANameNodeServer::OnClientMessage(std::shared_ptr<TcpConnection> conn, const std::string &data)
    {
        thread_pool_->Submit([this, conn, data](){
            ProcessClientRequest(conn, data);
        });
    }

    // HA NameNode 收到客户端请求后的 统一分发函数
    void HANameNodeServer::ProcessClientRequest(std::shared_ptr<TcpConnection> conn, const std::string &data)
    {
        // 解析请求
        NameNodeRequest req;
        if (!req.ParseFromString(data))
        {
            std::cerr << "[HANameNode] Failed to parse request\n";
            return;
        }

        NameNodeResponse resp;

        // CREATE_FILE、DELETE_FILE、ALLOCATE_BLOCK 走 resp = ProposeWrite(req);
        // 因为它们会修改 NameNode 元数据，HA 模式下不能只在当前节点本地改，必须通过 Raft 复制到多数节点并提交
        switch (req.op())   // 根据请求类型分发
        {
        case NameNodeRequest::CREATE_FILE:
        case NameNodeRequest::DELETE_FILE:
        case NameNodeRequest::ALLOCATE_BLOCK:
        {
            // Writes go through Raft
            resp = ProposeWrite(req);
            break;
        }
        case NameNodeRequest::GET_FILE_BLOCKS:
        {
            resp = HandleGetFileBlocks(req.get_file_blocks());
            break;
        }
        case NameNodeRequest::LIST_FILES:
        {
            resp = HandleListFiles(req.list_files());
            break;
        }
        // 这些主要维护 DataNode 状态、心跳、block 位置信息。它们一般属于可重建的临时状态，
        // 比如 DataNode 重连后还会继续心跳、重新上报 block，所以这里没有走 Raft。
        case NameNodeRequest::REGISTER_DN:
        {
            resp = HandleRegisterDN(req.register_dn());
            break;
        }
        case NameNodeRequest::HEARTBEAT:
        {
            resp = HandleHeartbeat(req.heartbeat());
            break;
        }
        case NameNodeRequest::BLOCK_REPORT:
        {
            resp = HandleBlockReport(req.block_report());
            break;
        }
        default:
        {
            resp.set_success(false);
            resp.set_error("Unknown operation");
            break;
        }
        }

        resp.set_request_id(req.request_id());

        // 序列化响应并发回客户端
        std::string resp_bytes;
        resp.SerializeToString(&resp_bytes);
        client_loop_->RunInLoop([conn, resp_bytes](){   // 把发送动作切回网络事件循环线程中执行
            conn->Send(resp_bytes);
        });
    }

    // Raft callbacks

    // HA NameNode 的 Raft 状态机 apply 回调，负责把已经提交的 Raft 日志转换成具体的 NameNode 元数据修改
    // HA 模式下，写操作不能只在 HandleXXX 里直接改元数据，而要等 OnApplyCommitted 触发后再改
    void HANameNodeServer::OnApplyCommitted(uint64_t index, const std::string &command)
    {
        /*
        客户端写请求
        -> ProcessClientRequest
        -> ProposeWrite
        -> RaftNode::Propose
        -> Raft 日志复制
        -> 多数派确认
        -> commit_index 前进
        -> ApplyCommitted
        -> HANameNodeServer::OnApplyCommitted
        -> ApplyCreateFile / ApplyDeleteFile / ApplyAllocateBlock
        -> metadata_ 被修改
        */

        NameNodeRequest req;
        if (!req.ParseFromString(command))
        {
            std::cerr << "[HANameNode] Failed to parse committed command at index "
                    << index << std::endl;
            return;
        }

        switch (req.op())
        {
        case NameNodeRequest::CREATE_FILE:
            ApplyCreateFile(req.create_file());
            break;
        case NameNodeRequest::DELETE_FILE:
            ApplyDeleteFile(req.delete_file());
            break;
        case NameNodeRequest::ALLOCATE_BLOCK:
            ApplyAllocateBlock(req.allocate_block());
            break;
        default:
            break;
        }
    }

    void HANameNodeServer::OnSendRaftRPC(const std::string &peer_id, const RaftMessage &msg)
    {
        raft_rpc_->SendMessage(peer_id, msg);
    }

    // ===== Snapshot =====

    // 元数据快照生成。把当前 NameNode 的内存元数据打包成一个字符串，交给 Raft 保存成 snapshot。
    // 这样日志太长时，就可以用 snapshot 压缩历史日志，节点恢复时也能直接从快照恢复状态
    std::string HANameNodeServer::TakeMetadataSnapshot()
    {
        MetadataStoreSnapshot snap;

        // 保存所有文件信息
        auto files = metadata_->ListFiles("");  // 列出所有文件
        for (const auto &file : files)
        {
            auto *fe = snap.add_files();
            fe->set_path(file.path);
            fe->set_size(file.size);
            fe->set_create_time(file.create_time);
            fe->set_modify_time(file.modify_time);
            for (BlockId bid : file.blocks)
            {
                fe->add_blocks(bid);
            }
        }

        // 保存所有 block 信息
        for (const auto &file : files)
        {
            for (BlockId bid : file.blocks)
            {
                auto block = metadata_->GetBlock(bid);
                if (block.has_value())
                {
                    auto *be = snap.add_blocks();
                    be->set_block_id(block->block_id);
                    be->set_size(block->size);
                    for (const auto &loc : block->locations)
                    {
                        be->add_locations(loc);
                    }
                }
            }
        }

        std::string data;
        snap.SerializeToString(&data);
        return data;
    }

    // 把 Raft snapshot 中保存的文件元数据和 block 元数据重新写入 MetadataStore，
    // 用于节点启动恢复或 follower 安装 leader 发来的快照
    bool HANameNodeServer::RestoreMetadataSnapshot(const std::string &data)
    {
        MetadataStoreSnapshot snap;
        if (!snap.ParseFromString(data))
        {
            std::cerr << "[HANameNode] Failed to parse snapshot\n";
            return false;
        }

        // 恢复 block。这里先恢复 block，是因为后面恢复 file 时，file 里会引用 block id
        for (const auto &be : snap.blocks())
        {
            BlockInfo bi;
            bi.block_id = be.block_id();
            bi.size = be.size();
            for (const auto &loc : be.locations())
            {   
                bi.locations.push_back(loc);
            }
            metadata_->AddBlock(bi);
        }

        // 恢复 file
        for (const auto &fe : snap.files())
        {
            FileInfo fi;
            fi.path = fe.path();
            fi.size = fe.size();
            fi.create_time = fe.create_time();
            fi.modify_time = fe.modify_time();
            for (BlockId bid : fe.blocks())
            {
                fi.blocks.push_back(bid);
            }
            // 是因为 MetadataStore 好像没有直接 “put file” 的接口，
            // 而 CreateFile 遇到已存在文件会失败。所以它先删掉旧记录，再创建一个空文件记录，最后用完整的 fi 覆盖更新
            metadata_->DeleteFile(fe.path());
            FileInfo unused;
            metadata_->CreateFile(fe.path(), &unused);
            metadata_->UpdateFile(fe.path(), fi);
        }

        std:: cout << "[HANameNode] Restored snapshot: " << snap.files_size()
                << " files, " << snap.blocks_size() << " blocks\n";
        return true;
    }

    // Propose a write operation through Raft
    // 把 NameNode 的写操作交给 Raft 复制和提交，等提交成功后，再从本地 MetadataStore 读取结果，构造响应返回给客户端
    NameNodeResponse HANameNodeServer::ProposeWrite(const NameNodeRequest &req, int timeout_ms)
    {
        NameNodeResponse resp;
        resp.set_request_id(req.request_id());

        // 必须是 Leader。HA 写请求只能由 leader 接收
        if (!raft_node_->IsLeader())
        {
            std::string leader = raft_node_->GetLeaderId();
            resp.set_success(false);
            resp.set_error("Not leader. Leader: " + leader);
            return resp;
        }

        // 序列化请求
        std::string command;
        if (!req.SerializeToString(&command))
        {
            resp.set_success(false);
            resp.set_error("Failed to serialize command");
            return resp;
        }

        // 提交给 Raft。
        // Propose() 会把 command 追加到 leader 的 Raft 日志，
        // 然后通过 AppendEntries 复制给 follower。多数派复制成功后，leader 推进 commit_index_，
        // 调用：ApplyCommitted()，再触发：apply_cb_，而这个 callback 在构造函数里绑定到了：HANameNodeServer::OnApplyCommitted()
        auto result = raft_node_->Propose(command, timeout_ms);

        // 处理 Raft 失败情况
        if (!result.is_leader)  // 写入过程中当前节点失去了 leader 身份
        {
            resp.set_success(false);
            resp.set_error("Not leader anymore. Leader: " + result.leader_id);
            return resp;
        }

        if (!result.committed)  // 请求在超时时间内没有被多数派提交
        {
            resp.set_success(false);
            resp.set_error("Request timed out (not committed)");
            return resp;
        }

        // 如果 result.committed == true，那么对应 Raft log 已经 commit，并且在当前实现里已经同步 apply 到 MetadataStore
        // 根据 apply 后的 MetadataStore 构造响应
        switch (req.op())
        {
        case NameNodeRequest::CREATE_FILE:
        {
            auto file = metadata_->GetFile(req.create_file().path());
            resp.set_success(file.has_value());
            if (!file.has_value())
                resp.set_error("Failed to create file: " + req.create_file().path());
            break;
        }
        case NameNodeRequest::DELETE_FILE:
        {
            auto file = metadata_->GetFile(req.delete_file().path());
            resp.set_success(!file.has_value());
            if (file.has_value())
                resp.set_error("Failed to delete file: " + req.delete_file().path());
            break;
        }
        case NameNodeRequest::ALLOCATE_BLOCK:   // 暂时没办法得知分配block是否成功，默认成功
        {
            auto file = metadata_->GetFile(req.allocate_block().file_path());
            // 如果文件存在且 blocks 不为空，就取最后一个 block 作为刚分配的 block
            if (file.has_value() && !file->blocks.empty())
            {
                BlockId last_bid = file->blocks.back();
                auto block = metadata_->GetBlock(last_bid);
                auto *ar = resp.mutable_allocate_block();
                ar->set_success(true);
                ar->set_block_id(last_bid);
                if (block.has_value())
                {
                    for (const auto &dn : block->locations)
                        ar->add_datanode_addresses(dn);
                }
                resp.set_success(true);
            }
            else
            {
                resp.set_success(false);
                resp.set_error("Block allocation failed");
            }
            break;
        }
        default:
        {
            resp.set_success(false);
            resp.set_error("Unknown write operation");
            break;
        }
        }

        return resp;
    }

    // 读请求处理函数，它不走 Raft，直接从当前 NameNode 本地的 MetadataStore 读取文件和 block 信息
    NameNodeResponse HANameNodeServer::HandleGetFileBlocks(const GetFileBlocksRequest &req)
    {
        NameNodeResponse resp;
        auto file = metadata_->GetFile(req.path());
        // 如果找不到
        if (!file.has_value())
        {
            resp.set_success(false);
            resp.set_error("File not found: " + req.path());
            return resp;
        }

        resp.set_success(true);
        auto *gr = resp.mutable_get_file_blocks();
        gr->set_success(true);
        gr->set_path(file->path);
        gr->set_size(file->size);
        for (const auto &bid : file->blocks)    // 遍历这个文件包含的 block id
        {
            auto block = metadata_->GetBlock(bid);
            auto *bl = gr->add_blocks();
            bl->set_block_id(bid);
            if (block.has_value())
            {
                bl->set_size(block->size);
                for (const auto &loc : block->locations)
                    bl->add_datanodes(loc);
            }
        }

        return resp;
    }

    // 列目录/列文件的读请求处理函数, 不走 Raft，直接读本地 MetadataStore
    NameNodeResponse HANameNodeServer::HandleListFiles(const ListFilesRequest &req)
    {
        NameNodeResponse resp;
        resp.set_success(true);
        auto *lr = resp.mutable_list_files();
        lr->set_success(true);
        auto files = metadata_->ListFiles(req.dir());   // 从 MetadataStore 中找出目录 req.dir() 下的文件
        for (const auto &file : files)
        {
            auto *af = lr->add_files();
            af->set_path(file.path);
            af->set_size(file.size);
            af->set_create_time(file.create_time);
            af->set_block_count((int32_t)file.blocks.size());
        }
        
        return resp;
    }

    // DataNode 注册请求 的处理函数。DataNode 启动后会向 NameNode 报到，
    // NameNode 收到后把这个 DataNode 加入 DataNodeManager 管理
    NameNodeResponse HANameNodeServer::HandleRegisterDN(const RegisterDataNodeRequest &req)
    {
        /*
        这个函数在 ProcessClientRequest 里属于直接处理，不走 Raft
        也就是说 DataNode 注册信息是 每个 HA NameNode 本地维护的临时状态，不是通过 Raft 复制的持久元数据
        文件目录、block 元数据这种核心命名空间状态要走 Raft；DataNode 心跳、在线状态、剩余空间这类易变信息通常可以本地维护，
        靠 DataNode 向多个 NameNode 注册/心跳来更新
        */

        NameNodeResponse resp;
        
        DataNodeInfo dn;
        dn.id = req.datanode_id();
        dn.host = req.host();
        dn.port = req.port();
        dn.free_space = req.free_space();
        dn_manager_->RegisterDataNode(dn);  // 里面会设置时间和状态
        
        resp.set_success(true);
        return resp;
    }

    // DataNode 心跳处理函数, DataNode 注册之后，会定期向 NameNode 发送 heartbeat
    NameNodeResponse HANameNodeServer::HandleHeartbeat(const HeartbeatRequest &req)
    {
        NameNodeResponse resp;

        bool ok = dn_manager_->HandleHeartbeat(req.datanode_id(), req.free_space(), req.block_count());
        
        resp.set_success(ok);
        if (!ok) resp.set_error("DataNode not registered: " + req.datanode_id());

        return resp;
    }

    // DataNode 块汇报处理函数。
    // DataNode 会把自己本地有哪些 block 汇报给 NameNode，NameNode 用它来更新 block 的位置信息和大小信息
    NameNodeResponse HANameNodeServer::HandleBlockReport(const BlockReportRequest &req)
    {
        NameNodeResponse resp;
        for (const auto &item : req.blocks())
        {
            metadata_->UpdateBlockLocation(item.block_id(), req.datanode_id());
            auto block = metadata_->GetBlock(item.block_id());
            if (block.has_value() && block->size == 0 && item.size() > 0)
            {
                BlockInfo bi = *block;
                bi.size = item.size();
                metadata_->UpdateBlock(bi);
            }
        }

        resp.set_success(true);
        return resp;
    }

    // Apply handlers (called when Raft commits a log entry)
    void HANameNodeServer::ApplyCreateFile(const CreateFileRequest &req)
    {
        FileInfo info;
        metadata_->CreateFile(req.path(), &info);
    }

    void HANameNodeServer::ApplyDeleteFile(const DeleteFileRequest &req)
    {
        auto f = metadata_->GetFile(req.path());
        if (f.has_value())
        {
            for (BlockId block : f->blocks)
            {
                metadata_->DeleteBlock(block);
            }
            metadata_->DeleteFile(req.path());
        }
    }

    // Raft 日志提交之后，真正把“分配 block”这个写操作应用到本地元数据状态机里的函数
    // 给已经存在的文件分配block
    void HANameNodeServer::ApplyAllocateBlock(const AllocateBlockRequest &req)
    {
        /*
        客户端请求 ALLOCATE_BLOCK
        -> leader ProposeWrite(req)
        -> Raft 复制日志
        -> 日志 committed
        -> 每个 NameNode apply 这条日志
        -> 调用 ApplyAllocateBlock
        */

        /*
        这里有个设计细节：它依赖当前节点本地的 dn_manager_ 存活 DataNode 状态。
        如果不同 NameNode 上看到的 alive DataNode 不一致，理论上可能导致各节点 apply 出来的 block locations 不一致。
        课程项目里可能接受这个简化；严格 Raft 状态机通常要求 apply 结果完全确定。
        */

        // 先查这个文件是否存在, 如果文件不存在，就不能给它分配 block
        auto f = metadata_->GetFile(req.file_path());
        if (!f.has_value()) return;

        // 获取当前存活的 DataNode 列表
        auto alive_nodes = dn_manager_->GetAliveDataNodes();
        if (alive_nodes.empty()) return;

        try
        {
            BlockInfo block = block_manager_->AllocateBlock(alive_nodes);
            block_manager_->CommitBlock(block);     // 把这个 block 写入 metadata 的 block 表里
            f->blocks.push_back(block.block_id);    // 把新分配的 block id 加到文件的 block 列表末尾
            metadata_->UpdateFile(req.file_path(), *f); // 把修改后的 FileInfo 更新回 metadata
        }
        catch (const std::exception &e)
        {
            std::cerr << "[HANameNode] ApplyAllocateBlock failed: " << e.what() << '\n';
        }
    }

    // Health check
    void HANameNodeServer::StartHealthCheckTimer()
    {
        health_check_thread_ = std::thread([this](){
            while(!stop_.load())
            {
                // 每 5 秒执行一次检查
                std::this_thread::sleep_for(std::chrono::seconds(5));
                if (!stop_.load())
                {
                    // 核心健康检查
                    dn_manager_->CheckDataNodeHealth();
                    std::cout << "[HANameNode] DataNodes: "
                            << dn_manager_->AliveCount() << " alive / "
                            << dn_manager_->TotalCount() << " total"
                            << " | Raft: " << (IsLeader() ? "LEADER" : "FOLLOWER")
                            << std::endl;

                    // 只有 Leader 负责主动创建快照
                    if (IsLeader() && raft_node_->GetLogSize() > 50)
                        raft_node_->TakeSnapshot();
                    
                    /*
                    快照的作用是压缩 Raft 日志。Raft 日志一直追加，如果不清理会越来越大。
                    TakeSnapshot() 会通过之前注册的 snapshot callback 把当前 MetadataStore 序列化保存，然后截断旧日志
                    */
                }
            }
        });
    }

} // namespace mini_storage