#pragma once

#include <cstdint>
#include <string>

// 未完成

namespace mini_storage
{
    using BlockId = uint64_t;
    using FilePath = std::string;
    using DataNodeId = std::string;

    constexpr BlockId kInvalidBlockId = 0;  // 定义一个编译期常量，表示“无效的 block ID”

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

} // namespace mini_storage