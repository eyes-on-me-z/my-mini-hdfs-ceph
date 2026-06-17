#pragma once

#include "log_format.h"

#include <string>
#include <fstream>

namespace mini_storage
{
    class LogWriter
    {
    public:
        // truncate=false（默认）：追加模式，文件不存在时创建
		// truncate=true        ：截断模式，用于刷盘后重建 WAL（清空旧文件）
        explicit LogWriter(const std::string &filename, bool truncate = false);
        ~LogWriter();
        
        //追加一条记录
		//内部会自动添加CRC32校验
        bool AddRecord(const std::string &data);

        //强制刷新到磁盘(fsync)
		//只在需要强持久化时调用（有性能开销）
        bool Sync();

        bool Ok() const { return ok_; }
        
    private:
        //写入一个物理block
        bool EmitPhysicalRecord(RecordType type, const char *data, size_t length);

        std::ofstream file_;
        std::string filename_;

        bool ok_;
        int block_offset_;  // 当前块内的偏移
    };
}// namespace mini_storage