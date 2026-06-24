#pragma once

#include <string>
#include <memory>
#include <map>
#include <functional>

namespace mini_storage
{
    class EventLoop;
    class Channel;
    class TcpConnection;

    class TcpServer
    {
    public:
        using ConnectionCallback = std::function<void(std::shared_ptr<TcpConnection>)>;
        using MessageCallback = std::function<void(std::shared_ptr<TcpConnection>,
                                                    const std::string&)>;

        TcpServer(EventLoop *loop, const std::string &ip, int port);
        ~TcpServer();

        void SetConnectionCallback(ConnectionCallback cb) { conn_cb_ = std::move(cb); }
        void SetMessageCallback(MessageCallback cb) { msg_cb_ = std::move(cb); }

        bool Start();

    private:
        void HandleNewConnection();

        EventLoop *loop_;
        std::string host_;
        int port_;
        int server_fd_{-1}; // listen fd

        std::unique_ptr<Channel> accept_channel_;
        std::map<int, std::shared_ptr<TcpConnection>> connections_;

        ConnectionCallback conn_cb_;
        MessageCallback msg_cb_;
    };
} // namespace mini_storage