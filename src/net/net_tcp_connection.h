#pragma once

#include <string>
#include <memory>
#include <functional>

namespace mini_storage
{
    class EventLoop;
    class Channel;
    // 继承enable_shared_from_this<TcpConnection>的原因：
    // 需要在成员函数里拿到“指向自己的 shared_ptr”
    class TcpConnection : public std::enable_shared_from_this<TcpConnection>
    {
    public:
        using MessageCallback = std::function<void(std::shared_ptr<TcpConnection>,
                                                    const std::string&)>;
        using CloseCallback = std::function<void()>;

        TcpConnection(EventLoop *loop, int fd);
        ~TcpConnection();

        void Start();
        void Send(const std::string &data);
        void Close();

        void SetMessageCallback(MessageCallback cb) { msg_cb_ = std::move(cb); }    //在 TcpServer::HandleNewConnection 中设置
        void SetCloseCallback(CloseCallback cb) { close_cb_ = std::move(cb); }  //在 TcpServer::HandleNewConnection 中设置
        
        int fd() const { return fd_; }

    private:
        void HandleRead();
        void HandleWrite();
        void HandleClose();
        void ProcessMessages();

        EventLoop *loop_;
        int fd_;
        std::unique_ptr<Channel> channel_;

        std::string read_buf_;
        std::string write_buf_;

        MessageCallback msg_cb_;
        CloseCallback close_cb_;
    };
} // namespace mini_storage