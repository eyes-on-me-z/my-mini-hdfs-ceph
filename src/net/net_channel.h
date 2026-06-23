#pragma once

#include <cstdint>
#include <functional>
#include <sys/epoll.h>

namespace mini_storage
{
    class EventLoop;

    class Channel
    {
    public:
        using Callback = std::function<void()>;

        Channel(EventLoop *loop, int fd);
        ~Channel();

        int fd() const { return fd_; }

        void SetReadCallback(Callback cb) { read_cb_ = std::move(cb); }
        void SetWriteCallback(Callback cb) { write_cb_ = std::move(cb); }
        void SetCloseCallback(Callback cb) { close_cb_ = std::move(cb); }
        void SetErrorCallback(Callback cb) { error_cb_ = std::move(cb); }

        void EnableReading();
        void DisableReading();
        void EnableWriting();
        void DisableWriting();
        void DisableAll();

        bool IsReading() const { return events_ & EPOLLIN; }
        bool IsWriting() const { return events_ & EPOLLOUT; }

        void HandleEvent(uint32_t revents);
        uint32_t Events() const { return events_; }

    private:
        void Update();
        
        EventLoop *loop_;
        int fd_;
        uint32_t events_{0};

        Callback read_cb_;
        Callback write_cb_;
        Callback close_cb_;
        Callback error_cb_;
    };
} // namespace mini_storage