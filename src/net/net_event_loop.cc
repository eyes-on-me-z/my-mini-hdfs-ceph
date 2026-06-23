#include "net_event_loop.h"
#include "net_channel.h"

#include <stdexcept>
#include <unistd.h>
#include <sys/eventfd.h>

namespace mini_storage
{
    EventLoop::EventLoop()
    {
        // 创建一个 epoll 实例，设置 close-on-exec 标志
        // 如果当前进程以后调用 exec() 启动另一个程序，这个 fd 会自动关闭，不会泄漏到新程序里
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0) throw std::runtime_error("epoll_create1 failed");

        wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wakeup_fd_ < 0) throw std::runtime_error("eventfd failed");

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = wakeup_fd_;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &ev);

        events_.resize(16);
    }

    EventLoop::~EventLoop()
    {
        close(epoll_fd_);
        close(wakeup_fd_);
    }

    void EventLoop::Loop()
    {
        while(!quit_)
        {
            int n = epoll_wait(epoll_fd_, events_.data(), (int)events_.size(), 10);
            for (int i = 0; i < n; ++i)
            {
                int fd = events_[i].data.fd;
                uint32_t revents = events_[i].events;

                if (fd == wakeup_fd_) { HandleWakeUp(); continue; }

                // 我觉得在往epoll实例中注册 fd 的时候，可以设置epoll_event.data.ptr = channel,
                // 这样就不用去查找了，直接就拿到了channel
                auto it = channels_.find(fd);
                if (it != channels_.end()) it->second->HandleEvent(revents);
            }
            if (n == (int)events_.size()) events_.resize(events_.size() * 2);
            HandlePendingTasks();
        }
    }

    void EventLoop::RunInLoop(Task task)
    {
        {
            std::unique_lock<std::mutex> lock(pending_mutex_);
            pending_tasks_.push_back(std::move(task));
        }
        WakeUp();
    }

    void EventLoop::UpdateChannel(Channel *channel)
    {
        int fd = channel->fd();
        uint32_t events = channel->Events();

        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;

        if (channels_.count(fd) == 0)
        {
            channels_[fd] = channel;
            epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
        }
        else if (events == 0)
        {
            channels_.erase(fd);
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, &ev);
        }
        else
        {
            epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
        }
    }
    
    void EventLoop::RemoveChannel(Channel *channel)
    {
        int fd = channel->fd();
        channels_.erase(fd);
        epoll_event ev{};

        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, &ev);
    }

    void EventLoop::WakeUp()
    {
        uint64_t one = 1;
        write(wakeup_fd_, &one, sizeof one);
    }

    void EventLoop::HandleWakeUp()
    {
        uint64_t val;
        read(wakeup_fd_, &val, sizeof val);
    }

    void EventLoop::HandlePendingTasks()
    {
        std::vector<Task> tasks;
        {
            std::unique_lock<std::mutex> lock(pending_mutex_);
            tasks.swap(pending_tasks_);
        }
        for (auto &t : tasks) t();
    }

    void EventLoop::Quit()
    {
        quit_ = true;
        WakeUp();
    }
} // namespace mini_storage