#include "net_tcp_connection.h"
#include "net_channel.h"

#include <unistd.h>
#include <memory>
#include <cstring>
#include <arpa/inet.h>

namespace mini_storage
{
    TcpConnection::TcpConnection(EventLoop *loop, int fd)
        : loop_(loop), fd_(fd)
    {
        channel_ = std::make_unique<Channel>(loop_, fd_);
    }

    TcpConnection::~TcpConnection()
    {
        if (fd_ >= 0)
        {
            channel_->DisableAll(); // channel析构函数会调用DisableAll()，这里再调用一次有点多余吧
            close(fd_);             // 连接关闭 fd 前，先主动把 Channel 从 EventLoop/epoll 里摘掉
            fd_ = -1;
        }
    }

    void TcpConnection::Start()
    {
        // 这里有风险，传this指针。如果HandleEvent期间TcpConnection对象被析构，就会出问题
        // 可以借鉴一下 muduo 网络库。让 Channel 绑定一个 weak_ptr，
        // 事件处理前提升为 shared_ptr，保证整个 HandleEvent 期间 owner 活着
        channel_->SetCloseCallback([this] { HandleClose(); });
        channel_->SetErrorCallback([this] { HandleClose(); });
        channel_->SetReadCallback([this] { HandleRead(); });
        channel_->SetWriteCallback([this] { HandleWrite(); });
        channel_->EnableReading();
    }
    
    void TcpConnection::HandleRead()
    {
        char buf[4096];
        while(true)
        {
            ssize_t n = read(fd_, buf, sizeof buf);
            if (n > 0)
            {
                read_buf_.append(buf, n);
                ProcessMessages();
            }
            else if (n == 0)    // 对端已经正常关闭连接了(TCP 对端关闭了写端)
            {
                HandleClose();
                return;
            }
            else    // 没有成功读到数据。此时要看 errno
            {
                // 内核缓冲区被读空了, 对于非阻塞 socket，继续 read() 不会阻塞等待，而是立刻返回 -1
                // 并设置：errno = EAGAIN, 意思是：“现在没数据了，下次有数据再来。”
                if (errno == EAGAIN || errno == EWOULDBLOCK) break; // 现在暂时没有更多数据可读了，不是真的错误
                HandleClose();  // 真的出错了
                return;
            }
        }
    }
    
    void TcpConnection::ProcessMessages()
    {
        while(read_buf_.size() >= 4)
        {
            uint32_t msg_len;
            memcpy(&msg_len, read_buf_.data(), 4);
            msg_len = ntohl(msg_len);

            if (read_buf_.size() < 4 + (size_t)msg_len) break;

            // message不需要考虑字节序，只有多字节整数类型才需要考虑字节序，比如：
            // uint16_t uint32_t uint64_t
            std::string message(read_buf_.data() + 4, msg_len);
            read_buf_.erase(0, msg_len + 4);

            if (msg_cb_) msg_cb_(shared_from_this(), message);
        }
    }

    void TcpConnection::Send(const std::string &data)
    {
        uint32_t len = htonl((uint32_t)data.size());
        std::string framed;
        framed.append(reinterpret_cast<char*>(&len), 4);
        framed.append(data);

        if (!write_buf_.empty())
        {
            write_buf_.append(framed);
            return;
        }

        // n > 0   成功写入了 n 个字节
        // n == 0  通常表示没有写入任何字节，比较少见
        // n < 0   写入失败，具体原因看 errno
        ssize_t n = write(fd_, framed.data(), framed.size());
        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK) n = 0; // socket 发送缓冲区满了
            else { HandleClose(); return; } // 真正错误
        }
        if ((size_t)n < framed.size())
        {
            write_buf_.append(framed.data() + n, framed.size() - n);
            channel_->EnableWriting();  // 只有 write_buf_ 里面有待发送的数据才关心该连接的写事件
        }
    }

    void TcpConnection::HandleWrite()
    {
        if (write_buf_.empty()) { channel_->DisableWriting(); return; }

        ssize_t n = write(fd_, write_buf_.data(), write_buf_.size());
        if (n > 0)
        {
            write_buf_.erase(0, n);
            if (write_buf_.empty()) channel_->DisableWriting();
        }
        else if (n < 0 && errno != EAGAIN)
        {
            HandleClose();
        }
    }

    /*
    HandleClose() 不直接 close，是因为它负责“发起关闭流程”；
    真正的 close(fd_) 放在析构函数里，由 shared_ptr 生命周期统一处理。
    */

    // 把连接从事件循环里摘掉，并通知上层“这个连接该关闭了”
    // 真正的 fd 关闭交给 TcpConnection 析构函数统一处理
    // 这样做的好处是：关闭 fd 的时机和对象生命周期绑定在一起，
    // 避免 fd 关了但 TcpConnection 对象还活着、后续代码又误用这个 fd。
    // 这要求 close_cb_() 真的会释放连接
    void TcpConnection::HandleClose()
    {
        channel_->DisableAll();
        if (close_cb_) close_cb_();
    }

    void TcpConnection::Close()
    {
        HandleClose();
    }
} // namespace mini_storage