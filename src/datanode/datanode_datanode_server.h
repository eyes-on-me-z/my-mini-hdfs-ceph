#pragma once

#include <string>
#include <memory>

#include "datanode_block_store.h"
#include "datanode_pipeline_handler.h"
#include "datanode_datanode_client.h"

namespace mini_storage
{
    // 一致性检查作为客户端
    class DataNodeServer
    {
    public:
        DataNodeServer(const std::string &data_dir,
                        const std::string &host, int port,
                        const std::string &namenode_host, int namenode_port,
                        int worker_threads = 4);
        ~DataNodeServer();

        bool Start();
        void Stop();

        // Expose for tests
        BlockStore* GetBlockStore() { return block_store_.get(); }

    private:
        void ListenLoop();
        void HandleConnection(int client_fd);

        DataNodeResponse HandleWriteBlock(const WriteBlockRequest &req);
        DataNodeResponse HandleReadBlock(const ReadBlockRequest &req);
        DataNodeResponse HandleDeleteBlock(const DeleteBlockRequest &req);

        bool RecvRequest(int fd, DataNodeRequest *req);
        bool SendResponse(int fd, const DataNodeResponse &resp);

        std::string data_dir_;
        std::string host_;
        int port_;

        std::unique_ptr<BlockStore> block_store_;
        std::unique_ptr<PipelineHandler> pipeline_handler_;
        std::unique_ptr<DataNodeClient> nn_client_;

        std::atomic<bool> stop_{false};
        int server_fd_{-1};
    };
} // namespace mini_storage