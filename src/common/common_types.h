#pragma once

#include <cstdint>
#include <string>

// 未完成

namespace mini_storage
{
    using BlockId = uint64_t;
    using FilePath = std::string;
    using DataNodeId = std::string;

    constexpr BlockId kInvalidBlockId = 0;      // 定义一个编译期常量，表示“无效的 block ID”
    constexpr int kHeartbeatTimeoutSec = 30;    // DataNode 心跳超时时间是 30 秒

    // NameNode 里描述一个 block 的元数据信息
    struct BlockInfo
    {
        BlockId block_id = kInvalidBlockId; // 默认是 kInvalidBlockID，也就是 0，表示还没有有效 block
        int64_t size = 0;                   // 这个 block 的大小
        std::vector<DataNodeId> locations;  // 这个 block 存在哪些 DataNode 上
    };

    // NameNode 里用于描述一个文件的元数据信息
    struct FileInfo
    {
        FilePath path;                  // /user/a.txt
        int64_t size = 0;               // 文件总大小
        int64_t create_time = 0;
        int64_t modify_time = 0;
        std::vector<BlockId> blocks;    // 文件被切分成哪些 block 如：blocks = [101, 102, 103]
    };

    // 用来表示 DataNode 的状态
    enum class DataNodeStatus
    {
        ALIVE,      // 正常存活, 按时向 NameNode 发送 heartbeat
        SUSPECT,    // 可疑状态, 一段时间没收到 heartbeat，但还没完全确认宕机
        DEAD,       // 确认失效, 超过更长时间没有 heartbeat 认为这个 DataNode 已经不可用
    };

    // 记录一个 DataNode 节点状态的元数据结构
    struct DataNodeInfo
    {
        DataNodeId id;
        std::string host;
        int port = 0;
        DataNodeStatus status = DataNodeStatus::ALIVE;
        int64_t last_heartbeat = 0;
        int64_t free_space = 0;
        int32_t block_count = 0;    // 这个 DataNode 当前保存的 block 数量
    };

} // namespace mini_storage