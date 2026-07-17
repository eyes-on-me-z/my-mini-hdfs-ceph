#include "namenode_metadata_store.h"

#include <mutex>
#include <algorithm>

namespace mini_storage
{
    // 获取当前时间的毫秒级时间戳
    static int64_t NowMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // 在 NameNode 的元数据表里登记一个文件，并不会真的在 DataNode 上写数据,
    // 并没有更新 block 元数据
    bool MetadataStore::CreateFile(const FilePath &path, FileInfo *out_info)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);   // 写锁

        if (files_.count(path)) return false;   // 文件已存在

        FileInfo info;
        info.path = path;
        info.size = 0;
        info.create_time = NowMs();
        info.modify_time = info.create_time;

        files_[path] = info;
        if (out_info) *out_info = info;

        return true;
    }

    std::optional<FileInfo> MetadataStore::GetFile(const FilePath &path) const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);   // 读锁

        auto it = files_.find(path);
        if (it == files_.end()) return std::nullopt;
        return it->second;
    }

    bool MetadataStore::UpdateFile(const FilePath &path, const FileInfo &info)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);   // 写锁

        auto it = files_.find(path);
        if (it == files_.end()) return false;
        it->second = info;      // 不使用files_[path]原因：会再次查找，耗时o(logn);
        it->second.modify_time = NowMs();
        return true;
    }

    bool MetadataStore::DeleteFile(const FilePath &path)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);   // 写锁

         return files_.erase(path) > 0;
    }

    std::vector<FileInfo> MetadataStore::ListFiles(const std::string &dir_prefix) const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);   // 读锁

        std::vector<FileInfo> result;
        for (const auto &[path, info] : files_)
        {
            // 找到了返回起始下标
            if (path.find(dir_prefix) == 0) result.push_back(info);
        }
        return result;
    }

    void MetadataStore::AddBlock(const BlockInfo &block)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);   // 写锁
        blocks_[block.block_id] = block;
    }

    std::optional<BlockInfo> MetadataStore::GetBlock(BlockId block_id) const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);   // 读锁
        auto it = blocks_.find(block_id);
        if (it == blocks_.end()) return std::nullopt;
        return it->second;
    }

    bool MetadataStore::UpdateBlock(const BlockInfo &block)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);   // 写锁

        auto it = blocks_.find(block.block_id);
        if (it == blocks_.end()) return false;
        it->second = block;
        return true;
    }

    bool MetadataStore::DeleteBlock(BlockId block_id)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);   // 写锁

        return blocks_.erase(block_id) > 0;
    }

    // 更新 NameNode 里某个 block 的副本位置，也就是记录“这个 block 存在某个 DataNode 上”
    void MetadataStore::UpdateBlockLocation(BlockId block_id, const DataNodeId &dn_id)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);   // 写锁

        auto &block = blocks_[block_id];
        block.block_id = block_id;
        auto &locs = block.locations;
        if (std::find(locs.begin(), locs.end(), dn_id) == locs.end())
        {
            locs.push_back(dn_id);
        }
    }

    std::vector<DataNodeId> MetadataStore::GetBlockLocations(BlockId block_id) const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);   // 读锁

        auto it = blocks_.find(block_id);
        if (it == blocks_.end()) return {};
        return it->second.locations;
    }

    size_t MetadataStore::FileCount() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);   // 读锁
        return files_.size();
    }

    size_t MetadataStore::BlockCount() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);   // 读锁
        return blocks_.size();
    }
} // namespace mini_storage