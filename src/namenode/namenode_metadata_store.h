#pragma once

#include "common_types.h"

#include <shared_mutex>
#include <unordered_map>
#include <optional>
#include <vector>

namespace mini_storage
{
    class MetadataStore
    {
    public:
        MetadataStore() = default;

        // File operations
        bool CreateFile(const FilePath &path, FileInfo *out_info);
        std::optional<FileInfo> GetFile(const FilePath &path) const;
        bool UpdateFile(const FilePath &path, const FileInfo &info);
        bool DeleteFile(const FilePath &path);
        std::vector<FileInfo> ListFiles(const std::string &dir_prefix) const;

        // Block operations
        void AddBlock(const BlockInfo &block);
        std::optional<BlockInfo> GetBlock(BlockId block_id) const;
        bool UpdateBlock(const BlockInfo &block);
        bool DeleteBlock(BlockId block_id);

        // Week 7: Update block location from BlockReport
        void UpdateBlockLocation(BlockId block_id, const DataNodeId &dn_id);
        std::vector<DataNodeId> GetBlockLocations(BlockId block_id) const;

        size_t FileCount() const;
        size_t BlockCount() const;

    private:
        std::unordered_map<FilePath, FileInfo> files_;  // 存储所有文件的元数据信息
        std::unordered_map<BlockId, BlockInfo> blocks_; // 存储所有block的元数据信息
        mutable std::shared_mutex mutex_;   // 可在 const 成员函数里加锁的读写锁
    };
} // namespace mini_storage