#pragma once

#include <cstdint>
#include <string>

namespace mini_storage
{
    // ===== 固定长度编码（速度快，适合数值） =====

    // 编码32位整数（小端序，4字节）
    void EncodeFixed32(char *buf, uint32_t value);

    // 解码32位整数。把 4 个字节 还原成一个 uint32_t 整数
    uint32_t DecodeFixed32(const char *buf);

    // 编码64位整数（小端序，8字节）
    void EncodeFixed64(char *buf, uint64_t value);

    // 解码64位整数
    uint64_t DecodeFixed64(const char *buf);

    // ===== 变长编码（节省空间，适合小数字） =====

    // 编码32位整数（变长，1-5字节）
	// 返回：使用的字节数
    int EncodeVarint32(char *buf, uint32_t value);

    // 解码32位整数
	// 返回：消耗的字节数
    int DecodeVarint32(const char *buf, uint32_t *value);

    // ===== 字符串编码 =====

    // 编码字符串：[长度(varint)] [数据]
    void EncodeString(std::string *dst, const std::string &value);

    // 解码字符串
	// 返回：是否成功
    // 从一段二进制缓冲区里解析出一个“长度前缀字符串”
    // limit: 缓冲区结束位置，用来防止读越界
    // value: 输出参数，解析出来的字符串会放到这里
    bool DecodeString(const char **buf, const char *limit, std::string *value);

    // ===== 辅助函数 =====

    // 追加Fixed32到字符串
    void PutFixed32(std::string *dst, uint32_t value);

    // 追加Fixed64到字符串
    void PutFixed64(std::string *dst, uint64_t value);

    // 追加Varint32到字符串
    void PutVarint32(std::string *dst, uint32_t value);

    // 追加字符串
    // 把一个字符串 value 按照“长度 + 内容”的格式追加到 dst 里。
    void PutLengthPrefixedString(std::string *dst, const std::string &value);

    // 计算数据的 CRC32 校验和
	// icrc: 初始值，通常传入 0xFFFFFFFF
	// data: 待校验数据指针
	// n: 数据字节长度
    // 用来在读取 WAL / SSTable 时检查数据有没有坏掉
    uint32_t ValueCRC32(uint32_t icrc, const char *data, size_t n);
}// namespace mini_storage