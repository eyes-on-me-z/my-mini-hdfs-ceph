#pragma once

#include "common_types.h"

#include <optional>
#include <vector>

namespace mini_storage
{
    struct BlockMeta
    {
        BlockId block_id = 0;
        int64_t size = 0;
        uint32_t crc32 = 0;
        int64_t create_time = 0;
    };

    class BlockStore
    {
    public:
        explicit BlockStore(const std::string &data_dir);
        
        bool WriteBlock(BlockId block_id, const std::string &data);
        bool ReadBlock(BlockId block_id, int64_t offset, int64_t length,
                        std::string *data_out);
        bool DeleteBlock(BlockId block_id);
        bool HasBlock(BlockId block_id) const;

        // std::optional<BlockMeta>: 这个函数可能返回一个 BlockMeta，也可能什么都不返回
        std::optional<BlockMeta> GetBlockMeta(BlockId block_id) const;
        std::vector<BlockMeta> ListAllBlocks() const;
        int64_t GetFreeSpace() const;
        void CleanTempFiles();

    private:
        std::string BlockDataPath(BlockId id) const;
        std::string BlockMetaPath(BlockId id) const;
        std::string BlockTmpPath(BlockId id) const;

        bool WriteMeta(BlockId id, const BlockMeta &meta);
        bool ReadMeta(BlockId id, BlockMeta *meta) const;

        static uint32_t ComputeCRC32(const std::string &data);
    
        std::string data_dir_;
        std::string blocks_dir_;
        std::string tmp_dir_;
    };
} // namespace mini_storage