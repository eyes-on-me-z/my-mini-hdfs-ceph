#pragma once

#include <fstream>

#include "log_format.h"

namespace mini_storage
{
    class LogReader
    {
    public:
        explicit LogReader(const std::string &filename);
        ~LogReader();

        // 读取下一条记录
		// 返回true表示读取成功，record填充记录内容
		// 返回false表示文件结束或出错
        bool ReadRecord(std::string *record);

        bool Ok() const { return ok_; }

        // 跳过到指定偏移量（用于checkpoint恢复）
        bool SkipToOffset(uint64_t offset);

    private:
        std::ifstream file_;
        std::string filename_;
        bool ok_;
        bool eof_;

        // 读取物理记录
        bool ReadPhysicalRecord(RecordType *type, std::string *record);
    };
}// namespace mini_storage