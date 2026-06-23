#include "db.h"
#include "log_reader.h"
#include "db_write_batch.h"
#include "SSTableReader.h"
#include "SSTableBuilder.h"

#include <filesystem>
#include <algorithm>
#include <iostream>
#include <map>

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

    // FlushMemTable：把 MemTable 刷到带编号的 SSTable 文件
    void DB::FlushMemTable()
    {
        if (mem_table_->Empty()) return;

        // 1. 生成新的 SSTable 文件
        std::string sst_name = GenerateSSTableFilename();
        SSTableBuilder builder(sst_name);

        auto *iter = mem_table_->NewIterator();
        for (; iter->Valid(); iter->Next())
        {
            // 注意：墓碑（空值）也要写入 SSTable
            // 这样 Compaction 时才能正确去掉旧版本的数据
            builder.Add(iter->Key(), iter->Value());
        }
        builder.Finish();
        delete iter;

        // 2. 把新文件加入文件列表
        sst_files_.push_back(sst_name);
        std::cout << "[DB] MemTable 刷盘完成 -> " << sst_name
                    << "（当前共 " << sst_files_.size() << " 个 SSTable) "
                    << std::endl;

        //3.清空MemTable
        mem_table_->Clear();

        // 4. 切换 WAL：删旧的，用 truncate 模式创建新的
        delete log_write_;
        log_write_ = nullptr;
        std::filesystem::remove(GenerateLogFilename()); // 删除 WAL 文件

        // 重新打开时文件已不存在，app 模式会创建新文件，大小为 0
        // truncate=true：刷盘后重建日志，必须清空旧文件内容
        log_write_ = new LogWriter(GenerateLogFilename(), true);
        
        // 5. 判断是否需要 Compaction
        MaybeScheduleCompaction();
    }
    
    // MaybeScheduleCompaction：SSTable 文件太多时触发
    void DB::MaybeScheduleCompaction()
    {
        if ((int)sst_files_.size() >= options_.max_sstable_count)
        {
            std::cout << "[DB] SSTable 数量达到 " << sst_files_.size()
                << "，触发 Compaction..." << std::endl;
            DoCompaction();
        }
    }

    // Compaction:对外接口（加锁）
    void DB::Compaction()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        DoCompaction();
    }
    
    // DoCompaction：多路归并所有 SSTable，去重去墓碑
    //
    // 策略：
    //   1. 遍历所有 SSTable，把所有 kv 读入内存 map
    //      map 的遍历顺序是从旧到新，所以新版本会覆盖旧版本
    //   2. 遍历 map，跳过墓碑（空值），写入新的 SSTable
    //   3. 删除旧的所有 SSTable，用新的替换
    //
    // 注意：对于大数据量，应该做外部归并排序。
    //       Week 4 的数据量有限，全量读入内存是合理的简化。
    void DB::DoCompaction()
    {
        if (sst_files_.size() <= 1)
        {
            std::cout << "[DB] SSTable 数量 <= 1, 无需 Compaction。" << std::endl;
            return;
        }

        std::cout << "[DB] 开始 Compaction, 合并 " << sst_files_.size()
                << " 个 SSTable..." << std::endl;

        // Step 1：多路归并，用 map 保存每个 key 的最新版本
        // map 保证 key 有序（SSTable 要求有序写入）
        // 从旧到新遍历，后读的（更新的）覆盖先读的（更旧的）
        std::map<std::string, std::string> merged;

        for (const auto &sst_file : sst_files_)
        {
            SSTableReader reader(sst_file);
            if (!reader.Open())
            {
                std::cerr << "[DB] Compaction: 打开 SSTable 失败: " << sst_file << std::endl;
                continue;
            }

            // 用迭代器遍历整个SSTable
            auto *iter = reader.NewIterator();  // 把所有 DataBlock 的 kv 全部读出来
            if (iter == nullptr)
            {
                std::cerr << "[DB] Compaction: 无法创建迭代器: " << sst_file << std::endl;
                continue;
            }

            for (iter->SeekToFirst(); iter->Valid(); iter->Next())
            {
                // 后处理的文件（更新）会覆盖先处理的（更旧），operator[] 实现覆盖
                merged[iter->Key()] = iter->Value();
            }
            delete iter;
        }

        // Step 2：把 merged 里的有效 kv 写入新的 SSTable
        // 跳过墓碑（value 为空的 key 已被删除，合并时可以彻底丢弃）
        std::string new_sst_name = GenerateSSTableFilename();
        SSTableBuilder builder(new_sst_name);

        int written = 0, skipped = 0;
        for (const auto &[key, value] : merged)
        {
            if (value.empty())  // 墓碑：这个key已被删除，合并时彻底丢弃
            {
                skipped++;
            }
            else
            {
                builder.Add(key, value);
                written++;
            }
        }
        builder.Finish();

        // Compaction 后旧缓存全部失效，清空
        if (block_cache_) block_cache_->Clear();

        // Step 3：删除所有旧 SSTable
        for (const auto &old_file : sst_files_)
        {
            std::filesystem::remove(old_file);
        }

        // Step 4：更新文件列表，只保留新合并的文件
        sst_files_.clear();
        sst_files_.push_back(new_sst_name);

        std::cout << "[DB] Compaction 完成！"
            << " 写入: " << written << " 条"
            << "，丢弃墓碑: " << skipped << " 条"
            << " -> " << new_sst_name << std::endl;
    }
} // namespace mini_storage