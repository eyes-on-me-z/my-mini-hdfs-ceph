#pragma once

#include <vector>
#include <thread>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace mini_storage
{
    class ThreadPool
    {
    public:
        using Task = std::function<void()>;

        explicit ThreadPool(int num_threads);
        ~ThreadPool();

        void Submit(Task task);
        size_t QueueSize() const;
        void Stop();
        
    private:
        void WorkerLoop();

        std::vector<std::thread> workers_;
        std::queue<Task> task_queue_;

        mutable std::mutex queue_mutex_;
        std::condition_variable cv_;
        
        std::atomic<bool> stop_{false};
    };
} // namespace mini_storage