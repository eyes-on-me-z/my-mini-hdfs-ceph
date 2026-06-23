#pragma once

#include <vector>
#include <map>
#include <functional>
#include <sys/epoll.h>
#include <mutex>

namespace mini_storage
{
    class Channel;

    class EventLoop
    {
    public:
        using Task = std::function<void()>;

        EventLoop();
        ~EventLoop();

        void Loop();
        void Quit();
        void RunInLoop(Task task);

        void RemoveChannel(Channel *channel);
        void UpdateChannel(Channel *channel);

    private:
        void HandlePendingTasks();
        void WakeUp();
        void HandleWakeUp();

        int epoll_fd_;
        int wakeup_fd_;
        bool quit_{false};

        std::vector<epoll_event> events_;
        std::map<int, Channel*> channels_;
        std::vector<Task> pending_tasks_;
        std::mutex pending_mutex_;
    };
} // namespace mini_storage