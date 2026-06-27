#pragma once

#include "datanode_block_store.h"
#include "datanode.pb.h"

namespace mini_storage
{
    class PipelineHandler
    {
    public:
        explicit PipelineHandler(BlockStore *block_store);
        WriteBlockResponse HandlerWrite(const WriteBlockRequest &req);

    private:
        bool ForwardToNext(const std::string &next_node, const WriteBlockRequest &req);
        int ConnectTo(const std::string &host, int port);
        bool SendProto(int fd, const DataNodeRequest &req);
        bool RecvProto(int fd, DataNodeResponse *resp);
    
        BlockStore *block_store_;
    };
} //namespace mini_storage