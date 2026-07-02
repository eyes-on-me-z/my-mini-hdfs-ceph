#pragma once
// Week 8: 数据一致性校验器
// 职责：向 DataNode 发送 block 读取请求，验证 CRC32，发现损坏则触发修复
#include "common_types.h"
#include "namenode_metadata_store.h"
#include "namenode_datanode_manager.h"

namespace mini_storage
{
    struct BlockCheckResult
    {
        BlockId block_id;           // 被检查的 block ID
        DataNodeId datanode_id;     // 被检查的 DataNode ID，也就是这个 block 所在的节点
        bool reachable = false;     // DN 是否可达
        bool crc_ok = false;        // CRC 是否正确
        uint32_t actual_crc = 0;    // 实际检查到的 CRC 值
        int64_t actual_size = 0;    // 实际检查到的 block 大小
        std::string error;          // 错误信息
    };

    // 数据一致性检查器
    // 检查 NameNode 记录的 block 副本，在 DataNode 上是否还能正常读取、是否损坏、大小是否和元数据一致
    class ConsistencyChecker
    {
    public:
        ConsistencyChecker(MetadataStore *metadata, DataNodeManager *dn_manager);

        // 检查单个 block 在某个 DN 上的完整性
        BlockCheckResult CheckBlockOnDN(BlockId block_id, const DataNodeId &dn_id);

        // 检查单个 block 的所有副本
        std::vector<BlockCheckResult> CheckBlockAllReplicas(BlockId block_id);

        // 全量扫描：检查所有 block 的所有副本
        // 返回有问题的检查结果列表
        std::vector<BlockCheckResult> ScanAllBlocks();

        // 打印集群一致性报告
        void PrintReport(const std::vector<BlockCheckResult> &results);

    private:
        // 去某个 DataNode 上把某个 block 读回来，并计算它的 CRC32
        bool FetchAndVerify(const DataNodeId &dn_id, BlockId block_id,
                            std::string *data_out, uint32_t *crc_out);
        
        static uint32_t ComputeCRC32(const std::string &data);

        MetadataStore *metadata_;
        DataNodeManager *dn_manager_;
    };

} // namespace mini_storage