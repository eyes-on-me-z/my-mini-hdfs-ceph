#pragma once

#include "namenode_metadata_store.h"
#include "common_types.h"

#include <atomic>
#include <mutex>
#include <vector>
#include <optional>

namespace mini_storage
{
    class BlockManager
    {
    public:
        explicit BlockManager(MetadataStore *metadata);

        // 为即将写入的新 block 分配一个全局唯一的 block id，并选择这个 block 应该写到哪些 DataNode 上
        BlockInfo AllocateBlock(const std::vector<DataNodeInfo> &available_nodes);
        void CommitBlock(const BlockInfo &block);
        std::optional<BlockInfo> GetBlockInfo(BlockId block_id) const;
        uint64_t GetBlockCount() const;

    private:
        // 从 available 中轮询选择 n 个datanode
        std::vector<DataNodeId> SelectDataNodes(
            const std::vector<DataNodeInfo> &available, int n);

        MetadataStore *metadata_;
        std::atomic<BlockId> next_block_id_{1};
        std::atomic<int> round_robin_index_{0};     // 轮询选择 DataNode 的游标
        mutable std::mutex mutex_;
    };
} // namespace mini_storage