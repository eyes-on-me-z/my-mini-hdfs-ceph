#include "LruCache.h"

namespace mini_storage
{
    LRUCache::LRUCache(size_t capacity)
        : capacity_(capacity), hits_(0), misses_(0)
    {
        if (capacity_ == 0) capacity_ = 1;

        //创建哨兵节点，简化链表边界处理
        head_ = new Node("__head__", {});
        tail_ = new Node("__tail__", {});
        head_->next = tail_;
        tail_->prev = head_;
    }

    LRUCache::~LRUCache()
    {
        //释放所有业务节点
        Node *cur = head_->next;
        while(cur != tail_)
        {
            Node *next = cur->next;
            delete cur;
            cur = next;
        }
        //释放哨兵节点
        delete head_;
        delete tail_;
    }

    // 查找缓存
    bool LRUCache::Get(const std::string &key, BolckData *out)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = cache_map_.find(key);
        if (it == cache_map_.end())
        {
            misses_++;
            return false;   //未命中
        }

        MoveToFront(it->second);
        *out = it->second->data;
        hits_++;
        return true;
    }

    //插入缓存（从磁盘读完后调用）
    void LRUCache::Insert(const std::string &key, const BolckData &data)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        //如果key已存在，更新数据并移到头部
        auto it = cache_map_.find(key);
        if (it != cache_map_.end())
        {
            it->second->data = data;
            MoveToFront(it->second);
            return;
        }

        // 容量满了，先淘汰最久未用的（尾部）
        if (cache_map_.size() >= capacity_)
        {
            Exict();
        }

        //新节点插入头部
        Node *node = new Node(key, data);
        InsertToFront(node);
        cache_map_[key] = node;
    }

    //统计信息
    size_t LRUCache::Size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return cache_map_.size();
    }

    size_t LRUCache::Capacity() const
    {
        return capacity_;   // 这里不需要加锁，LRU构造好后就不会更改了
    }

    uint64_t LRUCache::Hits() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return hits_;
    }

    uint64_t LRUCache::Misses() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return misses_;
    }

    double LRUCache::HitRate() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t total = hits_ + misses_;
        if (total == 0) return 0.0; // 防止除 0

        return static_cast<double>(hits_) / total * 100.0;
    }

    //清空缓存
    void LRUCache::Clear()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        //清空链表
        Node *cur = head_->next;
        while(cur != tail_)
        {
            Node *next = cur->next;
            delete cur;
            cur = next;
        }

        head_->next = tail_;
        tail_->prev = head_;
        cache_map_.clear();
        hits_ = 0;
        misses_ = 0;
    }

    //把节点移到链表头部（最近使用）
    void LRUCache::MoveToFront(Node *node)
    {
        RemoveFromList(node);
        InsertToFront(node);
    }

    //把新节点插入链表头部
    void LRUCache::InsertToFront(Node *node)
    {
        node->next = head_->next;
        head_->next->prev = node;
        head_->next = node;
        node->prev = head_;
    }

    //删除链表中的节点（不释放内存）
    void LRUCache::RemoveFromList(Node *node)
    {
        node->prev->next = node->next;
        node->next->prev = node->prev;
        node->next = nullptr;
        node->prev = nullptr;
    }

    //淘汰链表尾部节点（最久未用）
    void LRUCache::Exict()
    {
        //淘汰 tail_->prev（最久未用的节点）
        Node *lru = tail_->prev;
        if (lru == head_) return;

        RemoveFromList(lru);
        cache_map_.erase(lru->key);
        delete lru;
    }
}