#include "net_thread_pool.h"

namespace mini_storage
{
    ThreadPool::ThreadPool(int num_threads)
    {
        for (int i = 0; i < num_threads; ++i)
        {
            workers_.emplace_back([this] { WorkerLoop(); });
        }
    }

    ThreadPool::~ThreadPool()
    {
        Stop();
    }

    void ThreadPool::Submit(Task task)
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            task_queue_.push(std::move(task));
        }
        cv_.notify_one();
    }

    size_t ThreadPool::QueueSize() const
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        return task_queue_.size();
    }

    void ThreadPool::Stop()
    {
        stop_.store(true);
        cv_.notify_all();
        for (auto &t : workers_)
        {
            if (t.joinable())   // 防止对已经 join()、detach() 或无效的线程对象再次调用 join()
            {
                t.join();
            }
        }
    }
    
    void ThreadPool::WorkerLoop()
    {
        while(true)
        {
            Task task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                // 1. task_queue_ 里有任务可执行；2. stop_ 被设为 true，表示线程池准备停止。
                cv_.wait(lock, [this] {
                    return !task_queue_.empty() || stop_.load();
                });

                // 如果已经要求停止，并且队列里也没有剩余任务，那么当前工作线程直接退出。
                if (stop_.load() && task_queue_.empty()) return;

                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
            task();
        }
    }
} // namespace mini_storage