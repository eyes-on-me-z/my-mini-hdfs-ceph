#include "log_writer.h"
#include "coding.h"

#include <iostream>

namespace mini_storage
{
    LogWriter::LogWriter(const std::string &filename, bool truncate)
        :filename_(filename), ok_(true), block_offset_(0)
    {
        //修改点：使用out|app确保文件不存在时创建，存在时追加
        // truncate=true 用于刷盘后重建日志（清空旧文件）
        // truncate=false（默认）用于正常追加写，文件不存在时自动创建
        auto mode = std::ios::binary | std::ios::out | 
            (truncate ? std::ios::trunc : std::ios::app);
        
        file_.open(filename, mode);
        if (!file_.is_open())
        {
            std::cerr << "[LogWriter] 打开文件失败: " << filename << std::endl;
            ok_ = false;
        }
    }

    LogWriter::~LogWriter()
    {
        if (file_.is_open())
        {
            file_.flush();
            file_.close();
        }
    }

    bool LogWriter::AddRecord(const std::string &data)
    {
        if (!ok_) return false;

        return EmitPhysicalRecord(kFullType, data.data(), data.size());
    }

    bool LogWriter::EmitPhysicalRecord(RecordType type, const char *data, size_t length)
    {
        // 构建 Header
        // 格式：[CRC32(4字节)][Length(2字节)][Type(1字节)]
        char header[kHeaderSize];

        // 先填 Length 和 Type（CRC 要覆盖这两个字段）
        // 要求 length <= 65535
        header[4] = static_cast<char>(length & 0xff);
        header[5] = static_cast<char>(length >> 8);
        header[6] = static_cast<char>(type);

        // 计算 CRC32：覆盖 type + data
        uint32_t crc = ValueCRC32(0xFFFFFFFF, header + 6, 1);   // type
        crc = ValueCRC32(crc, data, length);    // data
        EncodeFixed32(header, crc);

        // 写入 Header
        file_.write(header, kHeaderSize);
        // 写入数据
        file_.write(data, length);

        ok_ = file_.good();
        return ok_;
    }

    bool LogWriter::Sync()
    {
        file_.flush();
        return file_.good();    // 流当前没有任何错误状态?
    }
}