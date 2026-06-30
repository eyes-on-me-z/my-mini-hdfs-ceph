#include "namenode_consistency_checker.h"
#include "datanode.pb.h"
#include "net_io_helpers.h"

#include <zlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>

namespace mini_storage
{
    ConsistencyChecker::ConsistencyChecker(MetadataStore *metadata, DataNodeManager *dn_manager)
        : metadata_(metadata), dn_manager_(dn_manager)
    {}

    uint32_t ConsistencyChecker::ComputeCRC32(const std::string &data)
    {
        return crc32(0, (const Bytef*)&data, data.size());
    }

    // 去某个 DataNode 上把某个 block 读回来，并计算它的 CRC32
    // dn_id: ip:port
    bool ConsistencyChecker::FetchAndVerify(const DataNodeId &dn_id, BlockId block_id,
                        std::string *data_out, uint32_t *crc_out)
    {
        size_t colon = dn_id.rfind(':');
        if (colon == std::string::npos) return false;

        // 解析DataNode的ip和port
        std::string host = dn_id.substr(0, colon);
        int port = std::stoi(dn_id.substr(colon + 1));

        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return false;

        // 设置超时时间为10s
        struct timeval tv{10, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

        // datanode作为服务端会进入listen状态，当前代码为客户端
        if (connect(fd, (struct sockaddr*)&addr, sizeof addr) < 0)
        {
            close(fd);
            return false;
        }

        // 构建读请求
        ReadBlockRequest rb;
        rb.set_block_id(block_id);
        // ReadBlock 里 length <= 0 会被解释成“读到文件末尾”，
        // 所以这是一次完整 block 读取，刚好会触发 CRC 校验。
        rb.set_offset(0);
        rb.set_length(0);

        DataNodeRequest req;
        req.set_op(DataNodeRequest::READ_BLOCK);
        req.set_request_id(block_id);
        *req.mutable_read_block() = rb;

        std::string bytes;
        req.SerializeToString(&bytes);
        if (!SendMsg(fd, bytes)) { close(fd); return false; }

        std::string resp_bytes;
        if (!RecvMsg(fd, &resp_bytes)) { close(fd); return false; }
        close(fd);

        // 构建读响应
        DataNodeResponse resp;
        if (!resp.ParseFromString(resp_bytes) || !resp.success()) return false;

        *data_out = resp.read_block().data();
        *crc_out = ComputeCRC32(*data_out);

        // 这里不做crc校验的原因是 BlockStore::ReadBlock 在读取的时候已经做了CRC校验
        // 如果校验失败返回false，则resp.success()为false

        return true;
    }

    // 检查某个 block_id 在指定 DataNode dn_id 上是否“可读且大小正常”，并返回一个 BlockCheckResult
    BlockCheckResult ConsistencyChecker::CheckBlockOnDN(BlockId block_id, const DataNodeId &dn_id)
    {
        /*
        CheckBlockOnDN -> FetchAndVerify -> 发 READ_BLOCK 给 DataNode -> DataNodeServer::HandleReadBlock
        -> BlockStore::ReadBlock → 如果整块读取时 CRC 不匹配，ReadBlock 返回 false
        → resp.success() 为 false → FetchAndVerify 返回 false
        当前系统的 CRC 校验发生在 DataNode 本地读取阶段，而不是 NameNode 的 CheckBlockOnDN 阶段
        */

        BlockCheckResult result;
        result.block_id = block_id;
        result.datanode_id = dn_id;

        std::string data;
        uint32_t crc = 0;
        if (!FetchAndVerify(dn_id, block_id, &data, &crc))
        {
            result.reachable = false;
            result.crc_ok = false;
            result.error = "Cannot fetch block from " + dn_id;
            return result;
        }

        result.reachable = true;
        result.actual_crc = crc;
        result.actual_size = (int64_t)data.size();

        // 和 NameNode 元数据记录的大小对比
        auto block_meta = metadata_->GetBlock(block_id);    // 获取这个block的元数据信息
        if (block_meta.has_value() && block_meta->size > 0)
        {
            result.crc_ok = (block_meta->size == result.actual_size);
            if (!result.crc_ok)
            {
                result.error = "Size mismatch: expected=" +
                                std::to_string(block_meta->size) +
                                " actual=" + std::to_string(result.actual_size);
            }
        }
        else
        {
            // 没有元数据记录大小时，只要能读到数据就认为 OK
            result.crc_ok = (result.actual_size > 0);
        }

        return result;
    }

    // 检查单个 block 的所有副本
    std::vector<BlockCheckResult> ConsistencyChecker::CheckBlockAllReplicas(BlockId block_id)
    {
        std::vector<BlockCheckResult> results;

        // 检查 block 是否存在
        auto block = metadata_->GetBlock(block_id);
        if (!block.has_value()) return results;

        // 获取所有的 alive 的datanode
        auto alive_nodes = dn_manager_->GetAliveDataNodes();
        std::set<DataNodeId> alive_set;
        for (const auto &dn : alive_nodes) alive_set.insert(dn.id);

        // 遍历 block 所在的datanode
        for (const auto &dn_id : block->locations)
        {
            if (!alive_set.count(dn_id))    // DN 已死，跳过
            {
                BlockCheckResult r;
                r.block_id = block_id;
                r.datanode_id = dn_id;
                r.reachable = false;
                r.crc_ok = false;
                r.error = "DataNode is dead";
                results.push_back(r);
                continue;
            }
            results.push_back(CheckBlockOnDN(block_id, dn_id));
        }

        return results;
    }

    // 全量扫描：检查所有 block 的所有副本
    // 返回有问题的检查结果列表
    std::vector<BlockCheckResult> ConsistencyChecker::ScanAllBlocks()
    {
        std::vector<BlockCheckResult> bad_results;

        // 获取所有文件
        auto files = metadata_->ListFiles("");
        std::set<BlockId> visited;

        for (const auto &file : files)
        {
            for (const BlockId &bid : file.blocks)
            {
                if (visited.count(bid)) continue;
                visited.insert(bid);

                auto results = CheckBlockAllReplicas(bid);
                for (const auto &r : results)
                {
                    if (!r.crc_ok || !r.reachable)
                    {
                        bad_results.push_back(r);
                    }
                }
            }
        }

        return bad_results;
    }

    // 打印集群一致性报告
    void ConsistencyChecker::PrintReport(const std::vector<BlockCheckResult> &results)
    {
        if (results.empty())
        {
            std::cout << "[ConsistencyChecker] ✓ All blocks healthy\n";
            return;
        }

        std::cout << "[ConsistencyChecker] Found " << results.size()
                << " issues:\n";
        for (const auto &r : results)
        {
            std::cout << "  Block " << r.block_id
                    << " on " << r.datanode_id
                    << ": reachable=" << r.reachable
                    << " crc_ok=" << r.crc_ok;
            if (!r.error.empty()) std::cout << " error=" << r.error;
            std::cout << "\n";
        }
    }
} // namespace mini_storage