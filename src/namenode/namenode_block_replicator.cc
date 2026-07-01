#include "namenode_block_replicator.h"
#include "datanode.pb.h"
#include "net_io_helpers.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>

namespace mini_storage
{
    // ===== 低层 DataNode 通信 =====

    // 根据 DataNode 地址创建 TCP 连接，连接成功就返回 socket fd，失败就返回 -1
    static int ConnectToDN(const std::string &addr)
    {
        size_t colon = addr.rfind(':');
        if (colon == std::string::npos) return -1;
        std::string host = addr.substr(0, colon);
        int port = std::stoi(addr.substr(colon + 1));

        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return -1;

        // 设置超时时间为 10s
        struct timeval tv{10, };
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);

        struct sockaddr_in addr_in{};
        addr_in.sin_family = AF_INET;
        addr_in.sin_port = htons(port);
        inet_pton(AF_INET, host.c_str(), &addr_in.sin_addr);

        if (connect(fd, (struct sockaddr*)&addr_in, sizeof addr_in) < 0)
        {
            close(fd);
            return -1;
        }

        return fd;
    }

    // ===== BlockReplicator =====

    BlockReplicator::BlockReplicator(MetadataStore *metadata, DataNodeManager *dn_manager)
        : metadata_(metadata), dn_manager_(dn_manager)
    {}

    // 执行一个副本修复任务（同步）
    ReplicationResult BlockReplicator::Replicate(const ReplicationTask &task)
    {
        ReplicationResult result;
        result.block_id = task.block_id;

        std::cout << "[BlockReplicator] Replicating block " << task.block_id
                << " from " << task.source_dn
                << " to " << task.target_dn << "\n";

        // 1. 从源 DN 读取数据
        std::string data;
        if (!FetchBlock(task.source_dn, task.block_id, &data))
        {
            result.success = false;
            result.error = "FetchBlock failed from " + task.source_dn;
            total_failed_.fetch_add(1); // 线程安全地执行 total_failed_ += 1
            return result;
        }

        // 2. 写入目标 DN
        if (!PushBlock(task.target_dn, task.block_id, data))
        {
            result.success = false;
            result.error = "PushBlock failed to " + task.target_dn;
            total_failed_.fetch_add(1);
            return result;
        }

        // 3. 更新 NameNode 元数据：把 target_dn 加入 block 的 locations
        metadata_->UpdateBlockLocation(task.block_id, task.target_dn);

        std::cout << "[BlockReplicator] Block " << task.block_id
                << " replicated successfully to " << task.target_dn << "\n";

        total_replicated_.fetch_add(1);
        result.success = true;
        return result;
    }

    // 批量修复：对所有副本不足的 block 生成任务并执行
    // 返回成功修复的 block 数量
    int BlockReplicator::RepairUnderReplicated(const std::vector<UnderReplicatedBlock> &under_replicated)
    {
        int repaired = 0;
        for (const auto &urb : under_replicated)
        {
            if (urb.existing_locations.empty())
            {
                std::cerr << "[BlockReplicator] Block " << urb.block_id
                        << " has NO live replicas — data lost!\n";
                continue;
            }

            int need_count = urb.target_replicas - urb.current_replicas;
            if (need_count <= 0) continue;

            
            auto targets = SelectTargetNodes(urb, need_count);
            if (targets.empty())
            {
                std::cerr << "[BlockReplicator] No available target DN for block "
                        << urb.block_id << "\n";
                continue;
            }

            bool block_ok = true;
            for (const auto &dn_id : targets)
            {
                ReplicationTask task;
                task.block_id = urb.block_id;
                task.source_dn = urb.existing_locations[0];
                task.target_dn = dn_id;
                ReplicationResult result = Replicate(task);
                if (!result.success)
                {
                    std::cerr << "[BlockReplicator] Repair failed: "
                            << result.error << "\n";
                    block_ok = false;
                }
            }
            if (block_ok) repaired++;
        }

        return repaired;
    }

    // 从源 DN 读取 block 数据
    bool BlockReplicator::FetchBlock(const DataNodeId &source, BlockId block_id, std::string *data_out)
    {
        int fd = ConnectToDN(source);
        if (fd < 0)
        {
            std::cerr << "[BlockReplicator] Cannot connect to source: " << source << "\n";
            return false;
        }

        ReadBlockRequest rb;
        rb.set_block_id(block_id);
        rb.set_offset(0);
        rb.set_length(0);   // 全量读取

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

        DataNodeResponse resp;
        if (!resp.ParseFromString(resp_bytes) || !resp.success())
        {
            std::cerr << "[BlockReplicator] FetchBlock failed: "
                    << resp.error_message() << "\n";
            return false;
        }

        *data_out = resp.read_block().data();
        return true;
    }

    // 把 block 数据写入目标 DN（无 pipeline，单副本直写）
    bool BlockReplicator::PushBlock(const DataNodeId &target, BlockId block_id, const std::string &data)
    {
        int fd = ConnectToDN(target);
        if (fd < 0)
        {
            std::cerr << "[BlockReplicator] Cannot connect to target: " << target << "\n";
            return false;
        }

        WriteBlockRequest wb;
        wb.set_block_id(block_id);
        wb.set_data(data);
        // 不设置 pipeline —— 直接单副本写入，不再转发

        DataNodeRequest req;
        req.set_op(DataNodeRequest::WRITE_BLOCK);
        req.set_request_id(block_id);
        *req.mutable_write_block() = wb;

        std::string bytes;
        req.SerializeToString(&bytes);
        if (!SendMsg(fd, bytes)) { close(fd); return false; }

        std::string resp_bytes;
        if (!RecvMsg(fd, &resp_bytes)) { close(fd); return false; }
        close(fd);

        DataNodeResponse resp;
        if (!resp.ParseFromString(resp_bytes) || !resp.success())
        {
            std::cerr << "[BlockReplicator] PushBlock failed: "
                    << resp.error_message() << "\n";
            return false;
        }

        return true;
    }

    // 为一个副本不足的 block 选择目标 DN
    // 排除掉已有副本的 DN
    std::vector<DataNodeId> BlockReplicator::SelectTargetNodes(
        const UnderReplicatedBlock &urb, int need_count)
    {
        // 已有副本的 DN 不能再选（包括存活和死亡的）
        std::set<DataNodeId> excluded;
        for (const auto &id : urb.existing_locations) excluded.insert(id);
        for (const auto &id : urb.dead_locations) excluded.insert(id);

        auto alive_nodes = dn_manager_->GetAliveDataNodes();
        std::vector<DataNodeId> candidates;
        for (const auto &dn : alive_nodes)
        {
            if (!excluded.count(dn.id) && dn.free_space >= kBlockSize)
                candidates.push_back(dn.id);
        }

        int actual = std::min((int)candidates.size(), need_count);
        return std::vector<DataNodeId>(candidates.begin(), candidates.begin() + actual);
    }
} // namespace mini_storage