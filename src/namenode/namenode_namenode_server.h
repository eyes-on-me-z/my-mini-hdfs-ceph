#pragma once

#include "namenode_metadata_store.h"
#include "namenode_block_manager.h"
#include "namenode_datanode_manager.h"
#include "namenode_edit_log.h"
#include "namenode_fault_detector.h"
#include "net_event_loop.h"
#include "net_tcp_server.h"
#include "net_thread_pool.h"

#include <string>
#include <memory>

namespace mini_storage
{
    class TcpConnection;

    class NameNodeServer
    {
    public:
        NameNodeServer(const std::string &data_dir, const std::string &host,
                        int port, int num_workers = 4);
        ~NameNodeServer();

        bool Start();
        void Run();
        void Stop();

        DataNodeManager* GetDataNodeManager() { return dn_manager_.get(); }
        MetadataStore* GetMetadataStore() { return metadata_.get(); }
        FaultDetector* GetFaultDetector() { return fault_detector_.get(); }

    private:
        void OnMessage(std::shared_ptr<TcpConnection> conn, const std::string &data);
        void ProcessRequest(std::shared_ptr<TcpConnection> conn, const std::string &data);

        NameNodeResponse HandleCreateFile(const CreateFileRequest &req);
        NameNodeResponse HandleAllocateBlock(const AllocateBlockRequest &req);
        NameNodeResponse HandleGetFileBlocks(const GetFileBlocksRequest &req);
        NameNodeResponse HandleDeleteFile(const DeleteFileRequest &req);
        NameNodeResponse HandleListFiles(const ListFilesRequest &req);
        NameNodeResponse HandleRegisterDN(const RegisterDataNodeRequest &req);
        NameNodeResponse HandleHeartbeat(const HeartbeatRequest &req);
        // 处理 DataNode 上报自己当前持有哪些 block 的请求，也就是 HDFS 里常说的 Block Report
        NameNodeResponse HandleBlockReport(const BlockReportRequest &req);

        // 周期性状态检查和打印日志，打印当前 DataNode 状态
        void StartHealthCheckTimer();

        std::string data_dir_;  // 保存 NameNode 的本地数据目录路径，如 edit log
        std::string host_;
        int port_;

        std::unique_ptr<MetadataStore> metadata_;
        std::unique_ptr<BlockManager> block_manager_;
        std::unique_ptr<DataNodeManager> dn_manager_;
        std::unique_ptr<EditLog> edit_log_;
        std::unique_ptr<FaultDetector> fault_detector_;

        std::unique_ptr<EventLoop> loop_;
        std::unique_ptr<TcpServer> server_;
        std::unique_ptr<ThreadPool> thread_pool_;

        std::thread health_check_thread_;
        std::atomic<bool> stop_{false};
    };
} // namespace mini_storage