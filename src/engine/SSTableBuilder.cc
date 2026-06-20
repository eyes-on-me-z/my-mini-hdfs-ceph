#include "SSTableBuilder.h"
#include "coding.h"
#include "BloomFilter.h"

namespace mini_storage
{
    SSTableBuilder::SSTableBuilder(const std::string &filename)
        : filename_(filename)
        , file_(filename, std::ios::binary)
        , num_entries_(0)
        , offset_(0)
    {}

    SSTableBuilder::~SSTableBuilder()
    {
        if (file_.is_open())
        {
            Finish();   // 确保析构时尝试关闭
        }
    }

    // 添加键值对（外部循环调用迭代器，然后传进来）
    void  SSTableBuilder::Add(const std::string &key, const std::string &value)
    {
        // 1、将kv编码并存入buffer_(data block缓冲区)
        PutLengthPrefixedString(&buffer_, key);     // [len][data]
        PutLengthPrefixedString(&buffer_, value);   // [len][data]

        last_key_ = key;
        num_entries_++;

        // 2、如果buffer超过4kb，强制落盘作为一个data block
        if (buffer_.size() >= 4096)
        {
            FlushDataBlock();
        }

        keys_.push_back(key);   // 记录key用于生成过滤器
    }

    void  SSTableBuilder::FlushDataBlock()
    {
        /*
        把当前缓冲区 buffer_ 里的一批 key-value 写成一个 DataBlock 到 SSTable 文件里，并为这个块生成索引
        每落盘一次，index_block_中就追加 [last_key_长度][last_key_内容][buffer_在文件中的起始字节][buffer_长度+4]
        每刷一个 DataBlock，就记录一条索引
        */

        if (buffer_.empty()) return;

        // A、位置关键点：在buffer_还是原始状态时计算
        uint32_t crc = ValueCRC32(0xffffffff, buffer_.data(), buffer_.size());  // [cite:1]

        // B、记录索引：[这个块最大的key][偏移量][块大小],更新索引，告诉reader，这个块的大小包含了那4个字节的CRC
        PutLengthPrefixedString(&index_block_, last_key_);
        PutVarint32(&index_block_, static_cast<uint32_t>(offset_));
        PutVarint32(&index_block_, static_cast<uint32_t>(buffer_.size() + 4));  // [cite:1]

        //C、执行物理写入：数据和校验码必须紧挨着
        file_.write(buffer_.data(), buffer_.size());    // [cite:1]
        char crc_buf[4];
        EncodeFixed32(crc_buf, crc);    // [cite:1]
        file_.write(crc_buf, 4);    // [cite:1]

        // D、更新状态，偏移量更新：必须加上这4字节，否则下一个块的位置就偏了
        offset_ += (buffer_.size() + 4);
        buffer_.clear();
    }

    bool  SSTableBuilder::Finish()
    {
        /*
        [DataBlock...][BloomFilter][IndexBlock][Footer]
        为什么 Footer 要放最后？因为 Reader 打开 SSTable 时，
        可以直接读文件最后 24 字节，就知道 IndexBlock 和 BloomFilter 在哪里。

        Finish() 把最后剩余数据刷成 DataBlock，然后写 BloomFilter、IndexBlock 和 Footer，
        让这个 SSTable 成为一个可以被 Reader 正确打开和查询的完整文件。
        */

        // 1、刷入最后剩余的data block
        if (!buffer_.empty()) FlushDataBlock();

        // 2、生成布隆过滤器并写入
        // [BloomFilter] = [num_hashes_byte][bf_data]
        BloomFilter bf(keys_.size());   // keys_ 保存了这个 SSTable 的所有 key
        for (const auto &k : keys_) bf.Add(k);

        uint64_t filter_offset = offset_;
        const auto &bf_data = bf.RawData();
        // 先写1字节的 num_hashes，让 Reader 恢复时读到正确的哈希次数
        char num_hashes_byte = static_cast<char>(bf.NumHashes());
        file_.write(&num_hashes_byte, 1);
        file_.write(reinterpret_cast<const char*>(bf_data.data()), bf_data.size());
        uint32_t filter_size = 1 + static_cast<uint32_t>(bf_data.size());
        offset_ += filter_size;

        // 3、写入index block（逻辑不变）
        // [IndexBlock] = [index_block_][crc]
        uint64_t index_offset = offset_;

        // A.计算索引块数据的CRC
        uint32_t index_crc = ValueCRC32(0xffffffff, index_block_.data(), index_block_.size());
        
        //B.将索引块数据写入文件
        file_.write(index_block_.data(), index_block_.size());

        // C.将4字节CRC紧随其后写入文件
        char crc_buf[4];
        EncodeFixed32(crc_buf, index_crc);
        file_.write(crc_buf, 4);

        // D.计算总的索引块大小（原始数据+4字节总大小）
        uint32_t index_block_size = static_cast<uint32_t>(index_block_.size() + 4);
        offset_ += index_block_size;

        // 4、导入footer(12字节：8字节offset+4字节size)
        char footer[24];
        EncodeFixed64(footer, index_offset);
        EncodeFixed32(footer + 8, index_block_size);
        EncodeFixed64(footer + 12, filter_offset);
        EncodeFixed32(footer + 20, filter_size);
        file_.write(footer, 24);

        offset_ += 24;
        file_.close();
        keys_.clear();
        // 惯用技巧：利用一个空的临时 vector 进行交换，强制系统回收 keys_ 的预留空间
        std::vector<std::string>().swap(keys_);
        return true;
    }
} // namespace mini_storage