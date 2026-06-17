#include "db_write_batch.h"
#include "coding.h"
#include "log_format.h"

namespace mini_storage
{
    // 强制固定：Batch Header = 8字节(Seq) + 4字节(Count) = 12字节
    static const size_t kBatchHeaderSize = 12;

    WriteBatch::WriteBatch()
    {
        Clear();
    }

    //写入操作
    void WriteBatch::Put(const std::string &key, const std::string &value)
    {
        // 更新 Count
        uint32_t count = DecodeFixed32(rep_.data() + 8);
        EncodeFixed32(&rep_[8], count + 1);

        // 添加操作类型
        rep_.push_back(static_cast<char>(kTypeValue));
        // 添加数据
        EncodeString(&rep_, key);
        EncodeString(&rep_, value);
    }

    //删除操作
    void WriteBatch::Delete(const std::string &key)
    {
        uint32_t count = DecodeFixed32(&rep_[8]);
        EncodeFixed32(&rep_[8], count + 1);

        rep_.push_back(static_cast<char>(kTypeDeletion));
        EncodeString(&rep_, key);
    }

    //清空
    void WriteBatch::Clear()
    {
        rep_.clear();
        rep_.resize(kBatchHeaderSize, 0);
    }

    //操作数量
    int WriteBatch::Count() const
    {
        if (rep_.size() < kBatchHeaderSize) return 0;

        return DecodeFixed32(rep_.data() + 8);
    }

    //设置起始序列号（由DB类在写入时分配）
    void WriteBatch::SetSequence(uint64_t seq)
    {
        if (rep_.size() >= 8)
        {
            EncodeFixed64(&rep_[0], seq);
        }
    }

    uint64_t WriteBatch::Sequence() const
    {
        if (rep_.size() < 8) return 0;

        return DecodeFixed64(rep_.data());
    }

    //将batch里的操作逐个分发给handler
    bool WriteBatch::Iterate(Handler *handler) const
    {
        // 【核心修复点】
        // 这里的 rep_ 是由 DB::Recover 读出的 record 赋值给它的。
        // record 是 AddRecord(batch.Data()) 存进去的，所以它包含完整的 12 字节 Header。
        if (rep_.size() < kBatchHeaderSize) return false;

        // 必须跳过 12 字节！绝对不能用 kHeaderSize (7)
        const char *input = rep_.data() + kBatchHeaderSize;
        const char *limit = rep_.data() + rep_.size();

        while(input < limit)
        {
            ValueType type = static_cast<ValueType>(*input++);
            std::string key, value;

            if (type == kTypeValue)
            {
                if (DecodeString(&input, limit, &key) && DecodeString(&input, limit, &value))
                {
                    handler->Put(key, value);
                }
                else
                {
                    return false;
                }
            }
            else if (type == kTypeDeletion)
            {
                if (DecodeString(&input, limit, &key))
                {
                    handler->Delete(key);
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        return true;
    }
}// namespace mini_storage