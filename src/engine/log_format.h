#pragma once

#include <cstdint>

namespace mini_storage
{
    /* 
    * Write-Ahead Log 预写日志
    * 定义 log_writer.cpp 和 log_reader.cpp 都要遵守的“格式规则”。
    * 每条 WAL 物理记录前面有 7 字节 header
    * [CRC32][Length][Type][真正的数据 payload]
    */

    //每条WAL记录的header大小
	//CRC(4字节)+Length(2字节)+Type(1字节)=7字节
    static const int kHeaderSize = 7;

    //WAL块大小(参考levelDB，32kb)
	//把文件分成固定大小的块，方便读取
    // 把 WAL 日志文件按固定大小切分时，每一块的大小
    static const int kBlockSize = 32768;    // 32 * 1024

    //操作类型  对应crc + length + type 中的type
    enum RecordType : uint8_t   // 用来标记 一条 WAL 物理记录的类型
    {
        kZeroType = 0,      // 预留
        kFullType = 1,      // 完整记录（最常见）
        kFirstType = 2,     // 大记录的第一块（暂时不实现）
        kMiddleType = 3,    // 大记录的中间快（暂时不实现）
        kLastType = 4,      // 大记录的最后一块（暂时不实现）
    };

    // WriteBatch内部的操作类型
    enum ValueType : uint8_t
    {
        kTypeDeletion = 0,  // 删除操作
        kTypeValue = 1,     // 写入操作
    };

    /*
    * RecordType：WAL 物理记录层面的类型
    * ValueType ：真正数据库操作层面的类型。是写入批次里每条 kv 操作的标记，用来区分“写入”还是“删除”。
    */
}// namespace mini_storage