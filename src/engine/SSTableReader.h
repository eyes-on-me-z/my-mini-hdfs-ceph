#pragma once

#include <string>
#include <fstream>
#include <memory>
#include <map>

#include "LruCache.h"
#include "BloomFilter.h"

namespace mini_storage
{
    class SSTableReader
    {
    public:
        // cache 可以为 nullptr（不使用缓存）
        // 多个 SSTableReader 共享同一个 LRUCache 实例，由 DB 统一管理
        SSTableReader(const std::string &filename, LRUCache *cache = nullptr);
        ~SSTableReader();

        // 打开文件并加载索引块
        bool Open();

        // 根据 key 查找 value（点查）
        bool Get(const std::string &key, std::string *value);

        // ============================================================
        // 迭代器：Compaction 时需要遍历整个 SSTable
        // ============================================================
        class Iterator
        {
        public:
            // 定位到第一个 key
            void SeekToFirst();
            // 定位到 >= target 的第一个 key
            void Seek(const std::string &target);
            // 是否还有有效数据
            bool Valid() const;
            // 移动到下一个
            void Next();
            // 当前 key / value
            const std::string& Key() const;
            const std::string& Value() const;
            
        private:
            friend class SSTableReader;
            // 迭代器直接持有全量 kv（已从磁盘加载）
            // 对于 Week 4 的数据量这完全够用
            // 工业实现是按需从磁盘读 Block，这里做了简化
            std::vector<std::pair<std::string, std::string>> entries_;
            size_t pos_;
        };

        // 创建迭代器（调用方负责 delete）
        Iterator* NewIterator();

    private:
        // 读取并解析一个 DataBlock，返回其中的 kv 列表
        bool ReadDataBlock(uint64_t offset, uint32_t size,
            std::vector<std::pair<std::string, std::string>> *out);

        std::string filename_;
        std::ifstream file_;
        LRUCache *cache_;   // 不持有所有权，由 DB 主管生命周期

        std::unique_ptr<BloomFilter> filter_;
        
        struct BlockHandle
        {
            uint64_t offset;
            uint32_t size;
        };

        // 索引：max_key -> BlockHandle
        std::map<std::string, BlockHandle> index_;
    };
} // namespace mini_storage