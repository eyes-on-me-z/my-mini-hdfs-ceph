#include "namenode_datanode_manager.h"

#include <iostream>

namespace mini_storage
{
    // 把一个 DataNode 加入 nodes_ 状态表
    bool DataNodeManager::RegisterDataNode(const DataNodeInfo &info)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        DataNodeInfo new_info = info;
        new_info.status = DataNodeStatus::ALIVE;    // 初始化它的状态为 ALIVE
        new_info.last_heartbeat = NowMs();          // 心跳时间为当前时间
        nodes_[info.id] = new_info;
        std::cout << "[DataNodeManager] Registered: " << info.id
                << " free=" << info.free_space / 1024 / 1024 << "MB" << std::endl;
        
        return true;
    }

    // 返回false代表该datanode未注册
    bool DataNodeManager::HandleHeartbeat(const std::string &id, int64_t free_space, int32_t block_count)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = nodes_.find(id);
        if (it == nodes_.end())
        {
            std::cerr << "[DataNodeManager] Unknown heartbeat from: " << id << std::endl;
            return false;
        }

        it->second.last_heartbeat = NowMs();
        it->second.free_space = free_space;
        it->second.block_count = block_count;
        if (it->second.status != DataNodeStatus::ALIVE)
        {
            std::cout << "[DataNodeManager] Node back online: " << id << std::endl;
            it->second.status = DataNodeStatus::ALIVE;
        }

        return true;
    }

    std::vector<DataNodeInfo> DataNodeManager::GetAliveDataNodes() const
    {
        std::lock_guard<std::mutex> lock(mutex_);

        std::vector<DataNodeInfo> result;
        for (const auto &[id, info] : nodes_)
        {
            if (info.status == DataNodeStatus::ALIVE)
            {
                result.push_back(info);
            }
        }

        return result;
    }

    std::optional<DataNodeInfo> DataNodeManager::GetDataNode(const DataNodeId &id) const
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = nodes_.find(id);
        if (it == nodes_.end()) return std::nullopt;

        return it->second;
    }

    // 更新datanode的健康状态
    // 超过 10 秒没心跳 -> ALIVE 变 SUSPECT
    // 超过 30 秒没心跳 -> 变 DEAD
    // 已经 DEAD 的节点跳过
    void DataNodeManager::CheckDataNodeHealth()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        int64_t now = NowMs();
        int64_t timeout_ms = kHeartbeatTimeoutSec * 1000;
        int64_t suspect_ms = (kHeartbeatTimeoutSec / 3) * 1000;

        for (auto &[id, info] : nodes_)
        {
            if (info.status == DataNodeStatus::DEAD) continue;

            int64_t elapsed = now - info.last_heartbeat;
            if (elapsed > timeout_ms)
            {
                std::cout << "[DataNodeManager] Node DEAD: " << id
                        << " (" << elapsed / 1000 << "s no heartbeat)" << std::endl;
                info.status = DataNodeStatus::DEAD;
            }
            else if (elapsed > suspect_ms && info.status == DataNodeStatus::ALIVE)
            {
                std::cout << "[DataNodeManager] Node SUSPECT: " << id
                        << " (" << elapsed / 1000 << "s no heartbeat)" << std::endl;
                info.status = DataNodeStatus::SUSPECT;
            }
        }
    }

    size_t DataNodeManager::TotalCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);

        return nodes_.size();
    }

    size_t DataNodeManager::AliveCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);

        size_t count = 0;
        for (const auto &[id, info] : nodes_)
        {
            if (info.status == DataNodeStatus::ALIVE)
            {
                count++;
            }
        }

        return count;
    }
} // namespace mini_storage