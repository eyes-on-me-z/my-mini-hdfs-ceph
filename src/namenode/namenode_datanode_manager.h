#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>
#include <optional>

#include "common_types.h"

namespace mini_storage
{
    class DataNodeManager
    {
    public:
        DataNodeManager() = default;

        bool RegisterDataNode(const DataNodeInfo &info);
        bool HandleHeartbeat(const std::string &id, int64_t free_space, int32_t block_count);

        std::vector<DataNodeInfo> GetAliveDataNodes() const;
        std::optional<DataNodeInfo> GetDataNode(const DataNodeId &id) const;

        void CheckDataNodeHealth();
        size_t TotalCount() const;
        size_t AliveCount() const;
        
    private:
        // 获取当前时间的毫秒级时间戳
        static int64_t NowMs()
        {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        }

        std::unordered_map<DataNodeId, DataNodeInfo> nodes_;    // 所有的datanode节点
        mutable std::mutex mutex_;
    };
} // namespace mini_storage