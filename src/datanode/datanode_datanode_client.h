#pragma once

#include <string>
#include <atomic>
#include <thread>

#include "datanode_block_store.h"
#include "namenode.pb.h"

namespace mini_storage
{
    class DataNodeClient
    {
    public:
        DataNodeClient(const std::string &datanode_id,
                        const std::string &namenode_host,
                        int namenode_port,
                        BlockStore *block_store);
        ~DataNodeClient();

        bool Start();
        void Stop();

    private:
        bool RegisterSelf();
        // DataNode 把自己本地保存的所有 block 列表上报给 NameNode
        bool SendBlockReport();
        bool SendHeartbeat();
        void HeartbeatLoop();
        bool SendToNameNode(const NameNodeRequest &req, NameNodeResponse *resp);
        
        std::string datanode_id_;
        std::string nn_host_;
        int nn_port_;
        BlockStore *block_store_;
        std::atomic<bool> stop_{false};
        std::thread  heartbeat_thread_;
    };
} // namespace mini_storage