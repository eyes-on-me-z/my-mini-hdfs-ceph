#include "namenode_namenode_server.h"
#include "net_tcp_connection.h"

#include <filesystem>

namespace fs = std::filesystem;

namespace mini_storage
{
    NameNodeServer::NameNodeServer(const std::string &data_dir, const std::string &host,
                        int port, int num_workers)
        : data_dir_(data_dir), host_(host), port_(port)
    {
        fs::create_directories(data_dir_);

        metadata_ = std::make_unique<MetadataStore>();
        block_manager_ = std::make_unique<BlockManager>(metadata_.get());
        dn_manager_ = std::make_unique<DataNodeManager>();
        edit_log_ = std::make_unique<EditLog>(data_dir_ + "/edit.log");
        fault_detector_ = std::make_unique<FaultDetector>(metadata_.get(), dn_manager_.get(), 15);

        loop_ = std::make_unique<EventLoop>();
        server_ = std::make_unique<TcpServer>(loop_.get(), host_, port_);
        thread_pool_ = std::make_unique<ThreadPool>(num_workers);

        // 启动重放 edit log 时目前只处理了 CREATE_FILE，其他请求基本都不处理
        edit_log_->Replay([this](const NameNodeRequest &op){
            if (op.op() == NameNodeRequest::CREATE_FILE)
            {
                FileInfo info;
                metadata_->CreateFile(op.create_file().path(), &info);
            }
        });

        server_->SetMessageCallback([this](auto conn, const auto &data){
            OnMessage(conn, data);
        });
    }

    NameNodeServer::~NameNodeServer()
    {
        Stop();
    }

    bool NameNodeServer::Start()
    {
        StartHealthCheckTimer();    // 启动健康检查定时线程
        fault_detector_->Start();   // 启动故障检测器
        return server_->Start();    // 启动 TCP 服务器
    }

    void NameNodeServer::Run()
    {
        loop_->Loop();
    }

    void NameNodeServer::Stop()
    {
        stop_.store(true);
        fault_detector_->Stop();
        if (health_check_thread_.joinable()) health_check_thread_.join();
        thread_pool_->Stop();
        loop_->Quit();
    }

    void NameNodeServer::OnMessage(std::shared_ptr<TcpConnection> conn, const std::string &data)
    {
        // 利用线程池去处理新用户的连接
        thread_pool_->Submit([this, conn, data] {
            ProcessRequest(conn, data);
        });
    }

    void NameNodeServer::ProcessRequest(std::shared_ptr<TcpConnection> conn, const std::string &data)
    {
        NameNodeRequest req;
        if (!req.ParseFromString(data))
        {
            std::cerr << "[NameNode] Failed to parse request\n";
            return;
        }

        NameNodeResponse resp;
        resp.set_request_id(req.request_id());

        switch (req.op())
        {
        case NameNodeRequest::CREATE_FILE:
            resp = HandleCreateFile(req.create_file());
            edit_log_->Append(req);     // 这里会记录create file 的请求
            break;
        case NameNodeRequest::ALLOCATE_BLOCK:
            resp = HandleAllocateBlock(req.allocate_block());
            break;
        case NameNodeRequest::GET_FILE_BLOCKS:
            resp = HandleGetFileBlocks(req.get_file_blocks());
            break;
        case NameNodeRequest::DELETE_FILE:
            resp = HandleDeleteFile(req.delete_file());
            edit_log_->Append(req);     // 这里也会记录delete file的请求
            break;
        case NameNodeRequest::LIST_FILES:
            resp = HandleListFiles(req.list_files());
            break;
        case NameNodeRequest::REGISTER_DN:
            resp = HandleRegisterDN(req.register_dn());
            break;
        case NameNodeRequest::HEARTBEAT:
            resp = HandleHeartbeat(req.heartbeat());
            break;
        case NameNodeRequest::BLOCK_REPORT:
            resp = HandleBlockReport(req.block_report());
            break;
        default:
            resp.set_success(false);
            resp.set_error("Unknown operation");
            break;
        }
        resp.set_request_id(req.request_id());

        std::string resp_bytes;
        resp.SerializeToString(&resp_bytes);
        loop_->RunInLoop([conn, resp_bytes] { conn->Send(resp_bytes); });
    }


    NameNodeResponse NameNodeServer::HandleCreateFile(const CreateFileRequest &req);
    NameNodeResponse NameNodeServer::HandleAllocateBlock(const AllocateBlockRequest &req);
    NameNodeResponse NameNodeServer::HandleGetFileBlocks(const GetFileBlocksRequest &req);
    NameNodeResponse NameNodeServer::HandleDeleteFile(const DeleteFileRequest &req);
    NameNodeResponse NameNodeServer::HandleListFiles(const ListFilesRequest &req);
    NameNodeResponse NameNodeServer::HandleRegisterDN(const RegisterDataNodeRequest &req);
    NameNodeResponse NameNodeServer::HandleHeartbeat(const HeartbeatRequest &req);
    NameNodeResponse NameNodeServer::HandleBlockReport(const BlockReportRequest &req);

    void NameNodeServer::StartHealthCheckTimer();
} // namespace mini_storage