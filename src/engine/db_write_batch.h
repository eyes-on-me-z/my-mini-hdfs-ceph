#pragma once

#include <string>
#include <cstdint>

namespace mini_storage
{
    // 一次数据库写入操作的“打包器”和“可回放日志格式”
    class WriteBatch
    {
    public:
        WriteBatch();
        ~WriteBatch() = default;

        //写入操作
        void Put(const std::string &key, const std::string &value);

        //删除操作
        // 这里并没有真正删除数据，它只是把“我要删除这个 key”这条操作记录进 rep_ 里
        void Delete(const std::string &key);

        //清空
        void Clear();

        //获取序列化后的内容（供WAL写入）
        const std::string& Data() const { return rep_; }

        //操作数量
        int Count() const;

        //设置起始序列号（由DB类在写入时分配）
        void SetSequence(uint64_t seq);
        uint64_t Sequence() const;

        void SetContents(const std::string &contents) { rep_ = contents; }

        class Handler
        {
        public:
            virtual ~Handler() {}
            virtual void Put(const std::string &key, const std::string &value) = 0;
            virtual void Delete(const std::string &key) = 0;
        };

        //将batch里的操作逐个分发给handler
        bool Iterate(Handler *handler) const;

    private:
        std::string rep_;
        //rep_格式
		// [Fixed64: Sequence Number] 
		// [Fixed32: Count] 
		// [Records...]
		// Record 格式: [Type(1 byte)] [KeyLen(Varint)] [Key] [ValueLen(Varint)] [Value]
    };
}// namespace mini_storage