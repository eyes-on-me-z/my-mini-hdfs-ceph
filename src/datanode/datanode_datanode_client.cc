#include "datanode_datanode_client.h"
#include "net_io_helpers.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace mini_storage
{
    DataNodeClient::DataNodeClient(const std::string &datanode_id,
                        const std::string &nn_host, int nn_port,
                        BlockStore *block_store)
        : datanode_id_(datanode_id),
        nn_host_(nn_host), nn_port_(nn_port),
        block_store_(block_store)
    {}
    
    DataNodeClient::~DataNodeClient()
    {
        Stop();
    }

    bool DataNodeClient::Start()
    {
        if (!RegisterSelf())
        {
            std::cerr << "[DataNodeClient] Registration failed\n";
            return false;
        }
        std::cout << "[DataNodeClient] Registered: " << datanode_id_ << "\n";

        if (!SendBlockReport())
        {
            std::cerr << "[DataNodeClient] Block report failed (non-fatal)\n";
        }

        heartbeat_thread_ = std::thread([this]{ HeartbeatLoop(); });
        return true;
    }

    void DataNodeClient::Stop()
    {
        stop_.store(true);
        if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    }

    bool DataNodeClient::RegisterSelf()
    {
        NameNodeRequest req;
        req.set_op(NameNodeRequest::REGISTER_DN);
        req.set_request_id(1);

        size_t colon = datanode_id_.rfind(':');
        auto *rdn = req.mutable_register_dn();
        rdn->set_datanode_id(datanode_id_);
        if (colon != std::string::npos)
        {
            rdn->set_host(datanode_id_.substr(0, colon));
            rdn->set_port(std::stoi(datanode_id_.substr(colon + 1)));
        }
        rdn->set_free_space(block_store_->GetFreeSpace());

        NameNodeResponse resp;
        return SendToNameNode(req, &resp) && resp.success();
    }

    bool DataNodeClient::SendBlockReport()
    {
        auto blocks = block_store_->ListAllBlocks();

        NameNodeRequest req;
        req.set_op(NameNodeRequest::BLOCK_REPORT);
        req.set_request_id(2);

        auto *br = req.mutable_block_report();
        br->set_datanode_id(datanode_id_);
        for (const auto &block : blocks)
        {
            auto *bm = br->add_blocks();
            bm->set_block_id(block.block_id);
            bm->set_size(block.size);
            bm->set_crc32(block.crc32);
        }
        std::cout << "[DataNodeClient] Sending block report: " << blocks.size() << " blocks\n";

        NameNodeResponse resp;
        return SendToNameNode(req, &resp) && resp.success();
    }

    bool DataNodeClient::SendHeartbeat()
    {
        NameNodeRequest req;
        req.set_op(NameNodeRequest::HEARTBEAT);
        req.set_request_id(3);

        auto *hb = req.mutable_heartbeat();
        hb->set_datanode_id(datanode_id_);
        hb->set_free_space(block_store_->GetFreeSpace());
        hb->set_block_count((int32_t)block_store_->ListAllBlocks().size());

        NameNodeResponse resp;
        return SendToNameNode(req, &resp) && resp.success();
    }

    // 每隔 3s 发送心跳消息
    void DataNodeClient::HeartbeatLoop()
    {
        while(!stop_.load())
        {
            for (int i = 0; i < 30 && !stop_.load(); ++i)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            if (stop_.load()) break;
            if(!SendHeartbeat())
            {
                std::cerr << "[DataNodeClient] Heartbeat failed\n";
            }
        }
    }

    bool DataNodeClient::SendToNameNode(const NameNodeRequest &req, NameNodeResponse *resp)
    {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return false;

        struct timeval tm{5, 0};    // 5s超时时间
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tm, sizeof tm);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tm, sizeof tm);

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(nn_port_);
        inet_pton(AF_INET, nn_host_.c_str(), &addr.sin_addr);

        if (connect(fd, (struct sockaddr*)&addr, sizeof addr) < 0)
        {
            close(fd);
            return false;
        }

        std::string bytes;
        req.SerializeToString(&bytes);
        if (!SendMsg(fd, bytes)) { close(fd); return false; }

        std::string resp_bytes;
        if (!RecvMsg(fd, &resp_bytes)) { close(fd); return false; }
        close(fd);
        
        return resp->ParseFromString(resp_bytes);
    }
    
} // namespace mini_storage