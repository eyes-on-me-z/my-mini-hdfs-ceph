#pragma once

#include <string>
#include <atomic>
#include <thread>

#include "datanode_block_store.h"

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
        
        std::string datanode_id_;
        std::string nn_host_;
        int nn_port_;
        BlockStore *block_store_;
        std::atomic<bool> stop_{false};
        std::thread  heartbeat_thread_;
    };
} // namespace mini_storage