#pragma once
// Week 8: 副本复制器
// 职责：把一个 block 从存活的源 DN 复制到目标 DN，修复副本不足
#include "namenode_metadata_store.h"
#include "namenode_datanode_manager.h"
#include "namenode_replication_monitor.h"

#include <atomic>

namespace mini_storage
{
    struct ReplicationTask
    {
        BlockId block_id;
        DataNodeId source_dn;   // 从哪个 DN 读
        DataNodeId target_dn;   // 复制到哪个 DN
    };

    struct ReplicationResult
    {
        BlockId block_id;
        bool success;
        std::string error;
    };

    class BlockReplicator
    {
    public:
        BlockReplicator(MetadataStore *metadata, DataNodeManager *dn_manager);

        // 执行一个副本修复任务（同步）
        ReplicationResult Replicate(const ReplicationTask &task);

        // 批量修复：对所有副本不足的 block 生成任务并执行
        // 返回成功修复的 block 数量
        int RepairUnderReplicated(const std::vector<UnderReplicatedBlock> &under_replicated);

        // 从源 DN 读取 block 数据
        bool FetchBlock(const DataNodeId &source, BlockId block_id, std::string *data_out);

        // 把 block 数据写入目标 DN（无 pipeline，单副本直写）
        bool PushBlock(const DataNodeId &target, BlockId block_id, const std::string &data);

        uint64_t TotalReplicated() const { return total_replicated_.load(); }
        uint64_t TotalFailed() const { return total_failed_.load(); }

    private:
        // 为一个副本不足的 block 选择目标 DN
        // 排除掉已有副本的 DN
        std::vector<DataNodeId> SelectTargetNodes(
            const UnderReplicatedBlock &urb, int need_count);

        MetadataStore *metadata_;
        DataNodeManager *dn_manager_;

        std::atomic<uint64_t> total_replicated_{0}; // 累计成功完成了多少次 block 副本复制
        std::atomic<uint64_t> total_failed_{0};     // 累计失败了多少次 block 副本复制
    };
} // namespace mini_storage