#include "datanode_block_store.h"

#include <filesystem>

namespace fs = std::filesystem;

namespace mini_storage
{
    BlockStore::BlockStore(const std::string &data_dir)
        : data_dir_(data_dir), blocks_dir_(data_dir + "/blocks"), tmp_dir_(data_dir + "/tmp")
    {
        fs::create_directories(blocks_dir_);
        fs::create_directories(tmp_dir_);
        CleanTempFiles();
    }

    bool BlockStore::WriteBlock(BlockId block_id, const std::string &data)
    {

    }
    
    bool BlockStore::ReadBlock(BlockId block_id, int64_t offset, int64_t length,
                    std::string *data_out);
    bool BlockStore::DeleteBlock(BlockId block_id);
    bool BlockStore::HasBlock(BlockId block_id) const;

    // std::optional<BlockMeta>: 这个函数可能返回一个 BlockMeta，也可能什么都不返回
    std::optional<BlockMeta> BlockStore::GetBlockMeta(BlockId block_id) const;
    std::vector<BlockMeta> BlockStore::ListAllBlocks() const;
    int64_t BlockStore::GetFreeSpace() const;
    void BlockStore::CleanTempFiles();

    std::string BlockStore::BlockDataPath(BlockId id) const;
    std::string BlockStore::BlockMetaPath(BlockId id) const;
    std::string BlockStore::BlockTmpPath(BlockId id) const;

    bool BlockStore::WriteMeta(BlockId id, const BlockMeta &meta);
    bool BlockStore::ReadMeta(BlockId id, BlockMeta *meta) const;

    static uint32_t BlockStore::ComputeCRC32(const std::string &data);
} // namespace mini_storage