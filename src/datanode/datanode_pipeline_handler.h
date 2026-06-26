#pragma once

#include "datanode_block_store.h"
#include "datanode.pb.h"

namespace mini_storage
{
    class PipelineHandler
    {
    private:
        bool ForwardToNext(const std::string &next_node, const WriteBlockRequest &req);
        int ConnectTo(const std::string &host, int port);
        bool SendProto(int fd, const std::string &req);
        bool RecvProto(int fd, )
    
        BlockStore *block_store_;
    };
} //namespace mini_storage