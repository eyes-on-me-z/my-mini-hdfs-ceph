#include "SSTableReader.h"
#include <memory>
#include <iostream>

namespace mini_storage
{
    // cache 可以为 nullptr（不使用缓存）
        // 多个 SSTableReader 共享同一个 LRUCache 实例，由 DB 统一管理
        SSTableReader::SSTableReader(const std::string &filename, LRUCache *cache)
            : filename_(filename), cache_(cache)
        {}

        SSTableReader::~SSTableReader()
        {
            if (file_.is_open())
            {
                file_.close();
            }
        }

        // 打开文件并加载索引块
        bool SSTableReader::Open()
        {
            file_.open(filename_, std::ios::binary);
            if (!file_) return false;

            //1、读取最后的12字节footer
            file_.seekg(0, std::ios::end);  // 移动到文件末尾
            size_t file_size = file_.tellg();   // 用来获取当前读指针的位置(也即文件大小)
            if (file_size < 24) return false;   // 必须大于等于Footer长度

            char footer_buf[24];
            file_.seekg(file_size - 24);
            file_.read(footer_buf, 24);

            // 2、解析索引块的位置(对应之前写入的fixed64和fixed32)
		    // 解析顺序必须和Builder写入顺序一致
            uint64_t index_offset = DecodeFixed64(footer_buf);
            uint32_t total_index_size = DecodeFixed32(footer_buf + 8);
            uint64_t filter_offset = DecodeFixed64(footer_buf + 12);
            uint32_t filter_size = DecodeFixed32(footer_buf + 20);

            //3、加载布隆过滤器
            if (filter_size > 0)
            {
                std::vector<char> filter_data(filter_size);
                file_.seekg(filter_offset);
                file_.read(filter_data.data(), filter_size);

                // 假设 BloomFilter 构造函数支持从 raw data 恢复
                filter_ = std::make_unique<BloomFilter>(std::string(filter_data.begin(), filter_data.end()));
            }

            // 4、读取并解析整个索引块
            std::string full_index_data;
            full_index_data.resize(total_index_size);
            file_.seekg(index_offset);
            file_.read(&full_index_data[0], total_index_size);

            size_t index_data_size = total_index_size - 4;
            uint32_t saved_crc = DecodeFixed32(&full_index_data[index_data_size]);
            uint32_t actual_crc = ValueCRC32(0xffffffff, full_index_data.data(), index_data_size);

            if (saved_crc != actual_crc)
            {
                std::cerr << "[错误] 索引块 CRC 校验失败！文件可能损坏。" << std::endl;
                return false;
            }

            // 5. 解析索引条目
            const char *p = full_index_data.data();
            const char *limit = p + index_data_size;
            while(p < limit)
            {
                std::string max_string;
                if (!DecodeString(&p, limit, &max_string))
                    break;  // 解析key

                uint32_t offset, size;
                int n1 = DecodeVarint32(p, &offset);
                if (n1  <= 0) break;
                p += n1;    // 解析块偏移量

                int n2 = DecodeVarint32(p, &size);
                if (n2 <= 0) break;
                p += n2;    // 解析块大小

                index_[max_string] = {offset, size};
            }

            return true;
        }

        // ReadDataBlock：先查 LRU Cache，未命中再读磁盘
        bool SSTableReader::ReadDataBlock(uint64_t offset, uint32_t size,
            std::vector<std::pair<std::string, std::string>> *out)
        {
            // 生成缓存 key = "文件名:偏移量"
            std::string cache_key = MakeCacheKey(filename_, offset);

            // 1. 查缓存（命中直接返回，节省磁盘 IO）
            if (cache_ && cache_->Get(cache_key, out))
                return true;    // Cache Hit ✅

            // 2. 缓存未命中，从磁盘读取
            std::string full_block_data;
            full_block_data.resize(size);
            file_.seekg(offset);
            file_.read(&full_block_data[0], size);

            // 3.CRC32校验逻辑
            size_t block_data_size = size - 4;
            uint32_t saved_crc = DecodeFixed32(&full_block_data[block_data_size]);
            uint32_t actual_crc = ValueCRC32(0xffffffff, full_block_data.data(), block_data_size);

            if (saved_crc != actual_crc)
            {
                std::cerr << "[SSTableReader] DataBlock CRC 校验失败，偏移: " << offset << std::endl;
                return false;
            }

            // 4. 解析 kv
            const char *p = full_block_data.data();
            const char *limit = p + block_data_size;
            while(p < limit)
            {
                std::string k, v;
                if (!DecodeString(&p, limit, &k)) break;
                if (!DecodeString(&p, limit, &v)) break;
                out->push_back({k, v});
            }

            // 5. 写入缓存（供下次命中）
            if (cache_)
            {
                cache_->Insert(cache_key, *out);
            }

            return true;
        }

        // Get：点查（先过 BloomFilter，再定位 Block，再线性扫描）
        bool SSTableReader::Get(const std::string &key, std::string *value)
        {
            //关键：先问过滤器，如果过滤器说没有，直接判定不存在，剩下一次磁盘IO
            if (filter_ && !filter_->MayContain(key))
            {
                return false;
            }

            // 1、在内存索引中寻找可能包含该key的数据块（寻找第一个>=key的块）
            auto it = index_.lower_bound(key);  // 找到第一个大于等于 key 的索引项
            if (it == index_.end())
                return false;

            //读取并解析DataBlock
            std::vector<std::pair<std::string, std::string>> entries;
            if (!ReadDataBlock(it->second.offset, it->second.size, &entries))
                return false;

            //线性扫描块内数据
            for (const auto &[k, v] : entries)
            {
                if (key == k)
                {
                    *value = v;
                    return true;
                }
            }

            return false;
        }

        // NewIterator：把所有 DataBlock 的 kv 全部读出来，
	    //              构建一个内存迭代器
        SSTableReader::Iterator* SSTableReader::NewIterator()
        {
            auto *iter = new Iterator();
            iter->pos_ = 0;

            // 按索引顺序遍历所有 DataBlock（索引 map 有序，天然有序）
            for (const auto &[max_key, handle] : index_)
            {
                std::vector<std::pair<std::string, std::string>> block_entries;
                if (ReadDataBlock(handle.offset, handle.size, &block_entries))
                {
                    for (auto &kv : block_entries)
                    {
                        iter->entries_.push_back(std::move(kv));
                    }
                }
            }

            return iter;
        }

        // Iterator 实现
        void SSTableReader::Iterator::SeekToFirst()
        {
            pos_ = 0;
        }
        
        // 定位到 >= target 的第一个 key
        void SSTableReader::Iterator::Seek(const std::string &target)
        {
            //二分查找第一个>=target的位置
            size_t lo = 0, hi = entries_.size();
            while(lo < hi)
            {
                size_t mid = (lo + hi) / 2;
                if (entries_[mid].first < target)
                {
                    lo = mid + 1;
                }
                else
                {
                    hi = mid;
                }
            }
            pos_ = lo;
        }
        
        // 是否还有有效数据
        bool SSTableReader::Iterator::Valid() const
        {
            return pos_ < entries_.size();
        }

        // 移动到下一个
        void SSTableReader::Iterator::Next()
        {
            if (Valid())
                pos_++;
        }

        // 当前 key / value
        const std::string& SSTableReader::Iterator::Key() const
        {
            return entries_[pos_].first;
        }

        const std::string& SSTableReader::Iterator::Value() const
        {
            return entries_[pos_].second;
        }
} // namespace mini_storage