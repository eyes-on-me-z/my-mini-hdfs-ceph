#pragma once

#include <vector>
#include <string>

#include "coding.h"

namespace mini_storage
{
    // 布隆过滤器本质就是一个 bit 数组，刚开始所有 bit 都是 0
    class BloomFilter
    {
    public:
        // entries: 预计会放多少个 key
        // bits_per_key: 每个key分配多少位（通常10位能达到1%报错率）
        BloomFilter(int entries, int bits_per_key = 10)
        {
            size_t bits = entries * bits_per_key;
            // 即使 key 很少，也至少分配 64 bit，避免过滤器太小导致误判率过高
            if (bits < 64) bits = 64;
            bits_ = ((bits + 7) / 8) * 8;   // 把 bit 数向上对齐到 8 的倍数，也就是按字节对齐
            data_.assign(bits_ / 8, 0);
            num_hashes_ = static_cast<int>(0.69 * bits_per_key);    //k=ln2*m/n
            if (num_hashes_ < 1)    // 保证至少有一个哈希函数。否则布隆过滤器就没法工作了
                num_hashes_ = 1;
        }

        // 从磁盘数据恢复过滤器
        BloomFilter(const std::string &raw_data)
        {
            if (raw_data.empty())
            {
                bits_ = 0;
                num_hashes_ = 1;
                return;
            }

            num_hashes_ = static_cast<int>(static_cast<unsigned char>(raw_data[0]));
            data_.assign(raw_data.begin() + 1, raw_data.end());
            bits_ = data_.size() * 8;
        }

        void Add(const std::string &key)
        {
            uint32_t h = ValueCRC32(0, key.data(), key.size());
            for (int i = 0; i < num_hashes_; ++i)
            {
                size_t bit_pos = h % bits_;
                data_[bit_pos / 8] |= (1 << (bit_pos % 8));
                h = ValueCRC32(h, key.data(), key.size());  // 简单迭代哈希
            }
        }

        bool MayContain(const std::string &key)
        {
            if (data_.empty()) return true;

            uint32_t h = ValueCRC32(0, key.data(), key.size());
            for (int i = 0; i < num_hashes_; ++i)
            {
                uint32_t bit_pos = h % bits_;
                if(!(data_[bit_pos / 8] & (1 << (bit_pos % 8))))
                    return false;   // 绝对不存在
                
                h = ValueCRC32(h, key.data(), key.size());
            }

            return true;    // 可能存在
        }

        const std::string& Data() const
        {
            static std::string res;
            res.assign(data_.begin(), data_.end());

            return res;
        }

        const std::vector<unsigned char>& RawData() const { return data_; }

        int NumHashes() const { return num_hashes_; }

    private:
        size_t bits_;
        int num_hashes_;
        std::vector<unsigned char> data_;
    };
} // namespace mini_storage