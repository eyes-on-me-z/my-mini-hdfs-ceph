#include "datanode_datanode_server.h"
#include "datanode.pb.h"
#include "net_io_helpers.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

/*
关于客户端的未完成
*/

namespace mini_storage
{
    DataNodeServer::DataNodeServer(const std::string &data_dir,
                                const std::string &host, int port,
                                const std::string &nn_host, int nn_port,
                                int /*worker_threads*/)
        : data_dir_(data_dir), host_(host), port_(port)
    {
        std::string dn_id = host + ":" + std::to_string(port);
        block_store_ = std::make_unique<BlockStore>(data_dir);
        pipeline_handler_ = std::make_unique<PipelineHandler>(block_store_.get());
        nn_client_ = std::make_unique<DataNodeClient>(dn_id, nn_host, nn_port, block_store_.get());
    }

    DataNodeServer::~DataNodeServer()
    {
        Stop();
    }

    bool DataNodeServer::Start()
    {

    }

    void DataNodeServer::Stop()
    {

    }

    void DataNodeServer::ListenLoop()
    {
        server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;    // 1 是启用，0 是关闭
        // 给服务端 socket 设置端口复用选项。允许这个 socket 绑定一个“刚刚用过”的地址/端口
        setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port_);
        inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);

        if (bind(server_fd_, (struct sockaddr*)&addr, sizeof addr) < 0)
        {
            std::cerr << "[DataNode] bind failed port=" << port_
                        << " " << strerror(errno) << "\n";
            return;
        }

        listen(server_fd_, 128);
        std::cout << "[DataNode] listening on " << host_ << ":" << port_ << "\n";

        while(!stop_.load())
        {
            int client_fd = accept(server_fd_, nullptr, nullptr);
            if (client_fd < 0) break;   // 我觉得continue会好一点吧
            std::thread([this, client_fd]{      // 频繁创建和销毁线程，不太好吧
                HandleConnection(client_fd);
                close(client_fd);
            }).detach();
        }
    }

    void DataNodeServer::HandleConnection(int client_fd)
    {
        DataNodeRequest req;
        if (!RecvRequest(client_fd, &req)) return;

        DataNodeResponse resp;
        resp.set_request_id(req.request_id());  // 这次应该是多余的

        switch (req.op())
        {
        case DataNodeRequest::WRITE_BLOCK:
            resp = HandleWriteBlock(req.write_block());
            break;
        case DataNodeRequest::READ_BLOCK:
            resp = HandleReadBlock(req.read_block());
            break;
        case DataNodeRequest::DELETE_BLOCK:
            resp = HandleDeleteBlock(req.delete_block());
            break;
        default:
            resp.set_success(false);
            resp.set_error_message("Unknown op");
        }

        resp.set_request_id(req.request_id());
        SendResponse(client_fd, resp);
    }

    DataNodeResponse DataNodeServer::HandleWriteBlock(const WriteBlockRequest &req)
    {
        DataNodeResponse resp;
        auto write_resp = pipeline_handler_->HandlerWrite(req);
        resp.set_success(write_resp.success());
        resp.set_error_message(write_resp.error_message());
        *resp.mutable_write_block() = write_resp;
        return resp;
    }

    DataNodeResponse DataNodeServer::HandleReadBlock(const ReadBlockRequest &req)
    {
        DataNodeResponse resp;
        std::string data;
        bool ok = block_store_->ReadBlock(req.block_id(), req.offset(), req.length(), &data);
        resp.set_success(ok);
        if (ok)
        {
            auto *rb = resp.mutable_read_block();
            rb->set_success(true);
            rb->set_data(data);
        }
        else
        {
            resp.set_error_message("Block not found or read error");
        }
        return resp;
    }

    DataNodeResponse DataNodeServer::HandleDeleteBlock(const DeleteBlockRequest &req)
    {
        DataNodeResponse resp;
        resp.set_success(block_store_->DeleteBlock(req.block_id()));
        return resp;
    }

    bool DataNodeServer::RecvRequest(int fd, DataNodeRequest *req)
    {
        std::string bytes;
        if (!RecvMsg(fd, &bytes)) return false;
        return req->ParseFromString(bytes);
    }

    bool DataNodeServer::SendResponse(int fd, const DataNodeResponse &resp)
    {
        std::string bytes;
        if (!resp.SerializeToString(&bytes)) return false;
        return SendMsg(fd, bytes);
    }
} // namespace mini_storage