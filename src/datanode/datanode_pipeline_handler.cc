#include "datanode_pipeline_handler.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <net_io_helpers.h>

namespace mini_storage
{
    PipelineHandler::PipelineHandler(BlockStore *block_store)
        : block_store_(block_store)
    {}

    // 1. 先把 block 写到当前 DataNode 本地磁盘
    // 2. 如果 pipeline 里还有下一个 DataNode，就继续转发过去
    WriteBlockResponse PipelineHandler::HandlerWrite(const WriteBlockRequest &req)
    {
        WriteBlockResponse resp;

        // 写入本地磁盘
        if (block_store_->WriteBlock(req.block_id(), req.data()))
        {
            resp.set_success(false);
            resp.set_error_message("Failed to write block to disk");
            return resp;
        }
        std::cout << "[DataNode] Block " << req.block_id() << " written to disk\n";

        // 判断是否还有后续 pipeline 节点
        if (req.pipeline_size() > 0)
        {
            // 构造转发请求
            std::string next_node = req.pipeline(0);
            WriteBlockRequest forward_req;
            forward_req.set_block_id(req.block_id());
            forward_req.set_data(req.data());
            for (int i = 1; i < req.pipeline_size(); ++i)
            {
                forward_req.add_pipeline(req.pipeline(i));
            }

            // 转发给下一个节点
            // 即使当前 DataNode 已经写盘成功，只要后续副本写失败，这次 pipeline 写入也返回失败
            // DN1 成功 = DN1 写成功 + DN2 写成功 + DN3 写成功
            // 第一个 DataNode 的 ForwardToNext 会一直等到后续 pipeline 链路都处理完，才返回
            // 下游整条 pipeline 超过 5 秒才响应，Node1 就会超时失败。
            if (!ForwardToNext(next_node, forward_req))
            {
                resp.set_success(false);
                resp.set_error_message("Failed to forward to " + next_node);
                return resp;
            }
            std::cout << "[DataNode] Forwarded block " << req.block_id()
                        << " to " << next_node << "\n";
        }

        resp.set_success(true);
        return resp;
    }

    // next_node = "ip:port"
    // 把当前 DataNode 收到的写 block 请求，继续转发给 pipeline 里的下一个 DataNode
    bool PipelineHandler::ForwardToNext(const std::string &next_node, const WriteBlockRequest &req)
    {
        // 解析ip 和 port
        size_t colon = next_node.rfind(':');
        if (colon == std::string::npos) return false;
        std::string host = next_node.substr(0, colon);
        int port = std::stoi(next_node.substr(colon + 1));

        int fd = ConnectTo(host, port);
        if (fd < 0) return false;

        DataNodeRequest dn_req;
        dn_req.set_op(DataNodeRequest::WRITE_BLOCK);
        dn_req.set_request_id(req.block_id());
        *dn_req.mutable_write_block() = req;

        bool ok = SendProto(fd, dn_req);
        if (ok)
        {
            DataNodeResponse dn_resp;
            ok = RecvProto(fd, &dn_resp) && dn_resp.success();
        }
        close(fd);
        return ok;
    }
    
    // 成功返回fd，失败返回-1
    int PipelineHandler::ConnectTo(const std::string &host, int port)
    {
        int fd = socket(AF_INET, SOCK_STREAM, 0);   // 默认是阻塞
        if (fd < 0) return -1;

        /*
        struct timeval {
            long tv_sec;   // 秒
            long tv_usec;  // 微秒
        };
        */
        struct timeval tv{5, 0};    // 超时时间 = 5 秒
        // 这两行是在给 socket fd 设置 接收超时 和 发送超时
        // 这两行是为了防止 socket 读写一直卡住，给它设置一个最长等待时间
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        // inet_pton 是 “presentation to network”，
        // 把字符串 IP，比如 "127.0.0.1"，转换成网络字节序的二进制地址
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

        if (connect(fd, (struct sockaddr*)&addr, sizeof addr) < 0)
        {
            close(fd);
            return -1;
        }

        return fd;
    }

    bool PipelineHandler::SendProto(int fd, const DataNodeRequest &req)
    {
        std::string bytes;
        if (!req.SerializeToString(&bytes)) return false;
        // len + data
        return SendMsg(fd, bytes);
    }

    bool PipelineHandler::RecvProto(int fd, DataNodeResponse *resp)
    {
        std::string bytes;
        if (!RecvMsg(fd, &bytes)) return false;
        return resp->ParseFromString(bytes);
    }
} //namespace mini_storage