#include "namenode_block_manager.h"

#include <algorithm>
#include <stdexcept>

namespace mini_storage
{
    BlockManager::BlockManager(MetadataStore *metadata)
        : metadata_(metadata)
    {}

    // 返回一个block的元数据
    // 为即将写入的新 block 分配一个全局唯一的 block id，并选择这个 block 应该写到哪些 DataNode 上
    BlockInfo BlockManager::AllocateBlock(const std::vector<DataNodeInfo> &available_nodes)
    {
        // 筛选出所有可用的 datanode
        std::vector<DataNodeInfo> usable;
        for (const auto &dn : available_nodes)
        {
            if (dn.status == DataNodeStatus::ALIVE && dn.free_space >= kBlockSize)
            {
                usable.push_back(dn);
            }
        }
        if (usable.empty())
            throw std::runtime_error("No available DataNode to allocate block");

        // 从可用的datanode 中选择3个（默认）
        int replicas = std::min((int)usable.size(), kReplicationFactor);
        auto selected = SelectDataNodes(usable, replicas);

        // 填写block元数据信息
        BlockInfo block;
        block.block_id = next_block_id_.fetch_add(1);
        block.size = 0;
        block.locations = selected;

        return block;
    }

    void BlockManager::CommitBlock(const BlockInfo &block)
    {
        metadata_->AddBlock(block);
    }

    std::optional<BlockInfo> BlockManager::GetBlockInfo(BlockId block_id) const
    {
        return metadata_->GetBlock(block_id);
    }

    uint64_t BlockManager::GetBlockCount() const
    {
        return metadata_->BlockCount();
    }

    std::vector<DataNodeId> BlockManager::SelectDataNodes(
        const std::vector<DataNodeInfo> &available, int n)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        std::vector<DataNodeId> selected;
        int sz = (int)available.size();
        int start = round_robin_index_.load() % sz;
        for (int i = 0; i < n; ++i)
        {
            int idx = (start + i) % sz;
            selected.push_back(available[idx].id);
        }
        round_robin_index_.store((start + n) % sz);

        return selected;
    }

} // namespace mini_storage