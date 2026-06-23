#pragma once

#include <unistd.h>
#include <string>
#include <cstdint>
#include <arpa/inet.h>

// Reliable write: handles partial writes (important for large data)
// 把 buf 里的 n 个字节全部写入 fd
static inline bool WriteAll(int fd, const void *buf, size_t n)
{
    const char *p = static_cast<const char*>(buf);
    while(n > 0)
    {
        ssize_t w = write(fd, p, n);
        if (w <= 0) return false;
        p += w;
        n -= (size_t)w;
    }

    return true;
}

// Reliable read: handles partial reads
// 从 fd 里读取 n 个字节，放到 buf 指向的内存里
static inline bool ReadAll(int fd, void *buf, size_t n)
{
    char *p = static_cast<char*>(buf);
    while(n > 0)
    {
        ssize_t r = read(fd, p, n);
        if (r <= 0) return false;
        p += r;
        n -= (size_t)r;
    }
    return true;
}

// Send a length-prefixed message reliably
static inline bool SendMsg(int fd, const std::string &data)
{
    uint32_t len = htonl((uint32_t)data.size());
    if (!WriteAll(fd, &len, 4)) return false;
    return WriteAll(fd, data.data(), data.size());
}

// Receive a length-prefixed message reliably
static inline bool RecvMsg(int fd, std::string *data)
{
    uint32_t len;
    if (!ReadAll(fd, &len, 4)) return false;
    len = ntohl(len);
    data->resize(len);
    return ReadAll(fd, &(*data)[0], len);
}