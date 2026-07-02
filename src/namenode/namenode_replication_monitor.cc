#include "namenode_replication_monitor.h"

#include <set>
#include <iostream>

namespace mini_storage
{
    ReplicationMonitor::ReplicationMonitor(MetadataStore *metadata, DataNodeManager *dn_manager)
        : metadata_(metadata), dn_manager_(dn_manager)
    {}

    // 扫描全部 block，返回所有副本不足的 block
    // 根据 NameNode 中保存的文件/block 元数据，以及 DataNodeManager 维护的 DataNode 存活状态，
    // 来判断 block 的有效副本数量是否不足
    std::vector<UnderReplicatedBlock> ReplicationMonitor::ScanUnderReplicated()
    {
        /*
        不是直接读取 DataNode 本地磁盘元数据来判断的。
        它关注的是“副本数量”，不负责验证 block 内容是否真的损坏；内容损坏这部分由 ConsistencyChecker 做
        */

        std::vector<UnderReplicatedBlock> results;

        // 获取所有存活 DN 的 ID 集合，用于快速查找（那为什么不用哈希表呢）
        auto alive_nodes = dn_manager_->GetAliveDataNodes();
        std::set<DataNodeId> alive_set;
        for (const auto &dn : alive_nodes) alive_set.insert(dn.id);

        // 遍历所有 block（通过 ListFiles 拿到所有文件的所有 block）
        std::set<BlockId> visited;
        auto files = metadata_->ListFiles("");
        for (const auto &file : files)
        {
            for (const auto &bid : file.blocks)
            {
                if (visited.count(bid)) continue;
                visited.insert(bid);

                // 在 ListFiles 和 GetBlock 期间，bid可能被其他线程删除了
                auto block = metadata_->GetBlock(bid);
                if (!block.has_value()) continue;

                UnderReplicatedBlock urb;
                urb.block_id = bid;
                urb.target_replicas = kReplicationFactor;
                
                // 分类：存活副本 vs 宕机副本
                for (const auto &dn_id : block->locations)
                {
                    if (alive_set.count(dn_id))
                    {
                        urb.existing_locations.push_back(dn_id);
                    }
                    else
                    {
                        urb.dead_locations.push_back(dn_id);
                    }
                }
                urb.current_replicas = (int)urb.existing_locations.size();

                if (urb.current_replicas < urb.target_replicas)
                {
                    results.push_back(urb);
                    std::cout << "[ReplicationMonitor] Under-replicated block "
                            << bid << ": " << urb.current_replicas
                            << "/" << urb.target_replicas << " replicas\n";
                }
            }
        }

        return results;
    }

    // 查询单个 block 的副本状态
    UnderReplicatedBlock ReplicationMonitor::CheckBlock(BlockId block_id)
    {
        UnderReplicatedBlock result;
        result.block_id = block_id;
        result.target_replicas = kReplicationFactor;

        auto block = metadata_->GetBlock(block_id);
        if (!block.has_value()) return result;

        auto alive_nodes = dn_manager_->GetAliveDataNodes();
        std::set<DataNodeId> alive_set;
        for (const auto &dn : alive_nodes) alive_set.insert(dn.id);

        for (const auto &dn_id : block->locations)
        {
            if (alive_set.count(dn_id)) result.existing_locations.push_back(dn_id);
            else result.dead_locations.push_back(dn_id);
        }
        result.current_replicas = (int)result.existing_locations.size();

        return result;
    }

    ReplicationMonitor::ClusterHealth ReplicationMonitor::GetClusterHealth()
    {
        ClusterHealth health;

        auto alive_nodes = dn_manager_->GetAliveDataNodes();
        std::set<DataNodeId> alive_set;
        for (const auto &dn : alive_nodes) alive_set.insert(dn.id);

        health.alive_datanodes = (int)alive_nodes.size();
        health.dead_datanodes = (int)(dn_manager_->TotalCount() - alive_nodes.size());

        std::set<BlockId> visited;
        auto files = metadata_->ListFiles("");

        for (const auto &file : files)
        {
            for (const auto &bid : file.blocks)
            {
                if (visited.count(bid)) continue;
                visited.insert(bid);
                health.total_blocks++;

                // 在 ListFiles 和 GetBlock 期间，bid可能被其他线程删除了
                auto block = metadata_->GetBlock(bid);
                if (!block.has_value()) continue;

                int alive_count = 0;    // 该block存活的副本数
                for (const auto &dn_id : block->locations)
                {
                    if (alive_set.count(dn_id)) alive_count++;
                }

                if (alive_count == 0) health.lost_blocks++;
                else if (alive_count < kReplicationFactor)  health.under_replicated++;
                else if (alive_count  == kReplicationFactor) health.healthy_blocks++;
                else health.over_replicated++;
            }
        }

        return health;
    }

} // namespace mini_storage