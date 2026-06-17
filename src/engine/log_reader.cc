#include "log_reader.h"
#include "coding.h"

namespace mini_storage
{
    LogReader::LogReader(const std::string &filename)
        :filename_(filename), ok_(true), eof_(false)
    {
        //以二进制模式打开文件进行读取
        file_.open(filename, std::ios::binary | std::ios::in);
        if (!file_.is_open())
        {
            ok_ = false;
        }
    }

    LogReader::~LogReader()
    {
        if (file_.is_open())
        {
            file_.close();
        }
    }

    // 让 LogReader 跳到 WAL 文件的某个字节位置，从那里开始读
    bool LogReader::SkipToOffset(uint64_t offset)
    {
        if (!file_.is_open()) return false;

        // 如果之前读文件时已经读到末尾，fstream 可能会有 eofbit 或 failbit 状态。
        // 在这种状态下，后面的 seekg() 可能不会正常工作。

        // //清除可能存在的EOF等标志位，确保seekg成功
        file_.clear();

        // seekg 是移动“读指针”的函数。g 可以理解为 get，也就是读取位置
        // 需要的参数类型通常是 std::streamoff 或 std::streampos
        // 这是 C++ 标准库专门用来表示文件流偏移量的类型

        // 把文件读取位置移动到 offset 字节处。
        file_.seekg(static_cast<std::streamoff>(offset));

        // 跳转后检查文件流状态是否正常
        return file_.good();
    }

    bool LogReader::ReadRecord(std::string *record)
    {
        if (!ok_ || eof_)
        {
            return false;
        }

        record->clear();
        while(true)
        {
            RecordType type;
            std::string fragment;

            if (!ReadPhysicalRecord(&type, &fragment))
            {   
                //如果读取物理记录失败，通常意味着到达文件末尾或遇到错误
                eof_ = true;
                return false;
            }

            switch (type)
            {
            case kFullType:
                *record = std::move(fragment);
                return true;    //最常见的情况：完整记录直接返回

            case kZeroType:
                //遇到零填充（通常在block末尾），继续尝试读取下一个记录
                break;

                // 目前版本暂不实现分片记录 (kFirstType, kMiddleType, kLastType)
				// 如果在 WAL 中发现了这些类型而未处理，视为不匹配的格式错误
            default:
                ok_ = false;
                return false;
            }
        }
    }

    // 读取物理记录
    bool LogReader::ReadPhysicalRecord(RecordType *type, std::string *result)
    {
        // 1.读取7字节header
        char header[kHeaderSize];
        file_.read(header, kHeaderSize);

        // 检查读取到的字节数是否足够header长度
        // gcount() 用来获取上一次非格式化输入操作实际读取的字符数
        if (file_.gcount() < kHeaderSize)
        {
            // 如果读取了0字节且文件到了eof，是正常结束
			// 如果读取了部分字节，说明文件损坏或截断
            return false;
        }

        // 解析header：[CRC32(4)][Length(2)][Type(1)]
		// 使用coding.h提供的DecodeFixed32处理CRC
        uint32_t storage_crc = DecodeFixed32(header);

        // 解析长度（小端序）
        uint32_t length = (static_cast<uint8_t>(header[5]) << 8) | 
            static_cast<uint8_t>(header[4]);
        *type = static_cast<RecordType>(header[6]);

        // 3.读取数据Payload
        result->resize(length);
        file_.read(&(*result)[0], length);

        // 检查实际读取的数据长度是否符合 header 描述
        if (static_cast<uint32_t>(file_.gcount()) < length)
        {
            ok_ = false;    // 文件被意外截断
            return false;
        }

        // 4. 验证 CRC32 (需与 LogWriter 写入时的计算逻辑完全一致)
	    // 验证范围包括：Type (1字节) + Data (length字节)
        uint32_t expected_crc = ValueCRC32(0xFFFFFFFF, header + 6, 1);  // 校验 Type
        expected_crc = ValueCRC32(expected_crc, result->data(), length);    // 校验 Data

        if (expected_crc != storage_crc)
        {
            // 数据校验失败，说明日志损坏
            ok_ = false;
            return false;
        }

        return true;
    }


} // namespace mini_storage
