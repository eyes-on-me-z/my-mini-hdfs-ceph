#include "db.h"
#include "log_reader.h"
#include "db_write_batch.h"
#include "SSTableReader.h"

#include <filesystem>
#include <algorithm>
#include <iostream>

namespace mini_storage
{
    // MemTableUnserter:把WAL里的WriteBatch回放到MemTable里，供Recover使用
    class MemTableInserter : public WriteBatch::Handler
    {
    public:
        MemTable *mem_ = nullptr;
        void Put(const std::string &key, const std::string &value) override
        {
            mem_->Put(key, value);
        }

        void Delete(const std::string &key) override
        {
            mem_->Put(key, "");     // 空值代表墓碑（已删除)
        }
    };

    // 构造
    DB::DB(const Options &options)
        : options_(options), next_log_id_(0), mem_table_(nullptr), log_write_(nullptr)
    {
        std::filesystem::create_directories(options_.db_path);
        mem_table_ = new MemTable();

        // ✅ 创建 LRU 缓存
        block_cache_ = new LRUCache(options_.block_cache_size);

        // 1.扫描已有的SSTable文件，恢复sst_files_和next_sst_id_
        LoadSSTableList();

        // 2.从 WAL 恢复数据到 MemTable
        Recover();

        // 关键修复：Recover 完成后，旧 WAL 的使命结束
        // 必须用 truncate 模式创建新 LogWriter，清空旧文件内容
        // 否则新写入会追加到旧日志后面，刷盘后文件仍然很大
        log_write_ = new LogWriter(GenerateLogFilename(), true);
    }

    // 析构
    DB::~DB()
    {
        delete log_write_;
        delete mem_table_;
        delete block_cache_;    // 释放缓存
    }

    // LoadSSTableList:扫描目录，按编号排序加载所有.sst文件
    void DB::LoadSSTableList()
    {
        sst_files_.clear();
        next_sst_id_ = 1;

        if (!std::filesystem::exists(options_.db_path)) return;

        // 收集所有.sst文件
        std::vector<std::string> found; // 存放 "./data/000001.sst"
        for (const auto &entry : std::filesystem::directory_iterator(options_.db_path))
        {
            if (entry.path().extension() == ".sst")
            {
                found.push_back(entry.path().string());
            }
        }

        // 按文件名排序（文件名是000001.sst这样的格式，字典序=数字序）
        std::sort(found.begin(), found.end());
        sst_files_ = found;

        // 更新next_sst_id:找出最大编号+1；
        for (const auto &f : sst_files_)
        {
            // 文件名形如/path/to/000003.sst,stem()="000003"
            std::string stem = std::filesystem::path(f).stem().string();
            try
            {
                uint64_t id = std::stoll(stem);
                if (id >= next_sst_id_)
                {
                    next_sst_id_ = id + 1;
                }
            }
            catch(...)  // catch (...) 就是 C++ 里的“捕获所有异常”。
            {
                // 非数字命名的 sst 文件（如老版本的 data.sst）忽略
            }
        }

        if (!sst_files_.empty())
        {
            std::cout << "[DB] 加载了 " << sst_files_.size()
                    << " 个 SSTable 文件，下一个编号: "
                    << next_sst_id_ << std::endl;
        }
    }

    // GenerateSSTableFilename：生成带编号的 SSTable 文件名
    // 格式：000001.sst, 000002.sst ...
    std::string DB::GenerateSSTableFilename()
    {
        char buf[32];
        snprintf(buf, sizeof buf, "%06llu.sst", (unsigned long long)next_sst_id_);
        next_sst_id_++;
        return options_.db_path + "/" + buf;
    }

    // Recover：从 WAL 恢复数据到 MemTable
    void DB::Recover()
    {
        /*
        磁盘上的 current.log 实际格式是:
        [CRC 4字节][Length 2字节][Type 1字节][Sequence 8字节][Count 4字节][Records...]
        Record 格式: [Type(1 byte)] [KeyLen(Varint)] [Key] [ValueLen(Varint)] [Value]
        */

        std::string log_filename = GenerateLogFilename();
        if (!std::filesystem::exists(log_filename)) return;

        LogReader reader(log_filename);
        std::string record;
        MemTableInserter inserter;
        inserter.mem_ = mem_table_;

        int count = 0;
        while(reader.ReadRecord(&record))   // [Sequence 8字节][Count 4字节][Records...]
        {
            WriteBatch batch;
            batch.SetContents(record);
            if (batch.Iterate(&inserter))   // 会去解析records中的所有操作，调用memtable的put方法
            {
                count++;
            }
            else
            {
                std::cerr << "[DB] 记录解析失败，数据可能错位！" << std::endl;
            }
        }

        if (count > 0)
        {
            std::cout << "[DB] 恢复成功，回放了 " << count << " 条 Batch 记录。" << std::endl;
        }
    }

    std::string DB::GenerateLogFilename()
    {
        return options_.db_path + "/current.log";
    }

    // 核心接口
    bool DB::Put(const std::string &key, const std::string &value)
    {
        /*
        调用一次写入如下：
        [CRC32 4]
        [Length 2]
        [WAL Type 1 = kFullType]
        [
            [Sequence 8]
            [Count 4 = 1]
            [ValueType 1 = kTypeValue]
            [KeyLen varint]
            [Key bytes]
            [ValueLen varint]
            [Value bytes]
        ]
        */
        std::lock_guard<std::mutex> lock(mutex_);

        // 1.先写WAL
        WriteBatch batch;
        batch.Put(key, value);

        if (!log_write_->AddRecord(batch.Data())) return false;
        log_write_->Sync();

        // 2.再写MemTable
        mem_table_->Put(key, value);

        // 3.判断是否需要刷盘
        if (mem_table_->Size() >= options_.write_buffer_size)
        {
            FlushMemTable();
        }
        
        return true;
    }

    // Delete：写墓碑（value 为空）
    bool DB::Delete(const std::string &key)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 1.先写WAL
        WriteBatch batch;
        batch.Delete(key);
        if (!log_write_->AddRecord(batch.Data())) return false;
        log_write_->Sync();

        // 2.再写MemTable
        mem_table_->Put(key, "");   // 空值=墓碑
        
        return true;
    }

    // Get：先查 MemTable，再从新到旧查所有 SSTable
    bool DB::Get(const std::string &key, std::string *value)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 1. 先查 MemTable（最新数据在这里）
        if (mem_table_->Get(key, value))
        {
            // 关键修复：如果值为空字符串，说明 key 被删除了
            if (value->empty()) return false;

            return true;
        }

        // 2. MemTable 里没有，从 SSTable 里找（从最新到最旧）
        return GetFromSSTables(key, value);
    }

    // GetFromSSTables：从最新到最旧遍历，传入 block_cache_
    // 新文件排在 sst_files_ 末尾，所以从后往前遍历
    bool DB::GetFromSSTables(const std::string &key, std::string *value)
    {
        // 从最新（末尾）到最旧（开头）查找
        for (int i = (int)sst_files_.size() - 1; i >= 0; --i)
        {
            // 把 block_cache_ 传给 SSTableReader，实现跨文件共享缓存
            SSTableReader reader(sst_files_[i], block_cache_);
            if (!reader.Open())
            {
                std::cerr << "[DB] 打开 SSTable 失败: " << sst_files_[i] << std::endl;
                continue;
            }

            if (reader.Get(key, value))
            {
                //在这个SSTable找到了
                //如果值为空，说明这是一个墓碑记录，key已被删除
                if (value->empty()) return false;
                return true;
            }
        }

        return false;   // 所有SSTable都没找到
    }

    void DB::FlushMemTable()
    {

    }
    
    void DB::Compaction();      // 手动触发compaction(供测试使用)

    void DB::MaybeScheduleFlush();
    
    

    // Compaction 实现
    void DB::DoCompaction();
    void DB::MaybeScheduleCompaction();

} // namespace mini_storage