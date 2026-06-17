#pragma once

/*
 * LRUCache：缓存 SSTable 的 DataBlock，避免重复磁盘 IO
 *
 * 设计：
 *   - Key   = "文件名:块偏移量"（字符串，唯一标识一个 DataBlock）
 *   - Value = 该 DataBlock 解析出的所有 kv 对
 *   - 容量  = 最多缓存 N 个 Block（默认 128 个）
 *
 * 实现：经典的 HashMap + 双向链表
 *   - HashMap  快速定位节点（O(1) 查找）
 *   - 双向链表 维护访问顺序（头部 = 最近使用，尾部 = 最久未用）
 *   - 每次 Get 命中后，把节点移到链表头部
 *   - 容量满时，淘汰链表尾部节点
 */

 #include <vector>
 #include <string>
 #include <mutex>
 #include <unordered_map>

namespace mini_storage
{
    //DataBlock的内容：一组有序的kv对
    // SSTable 里的一个 DataBlock 本来就是一批排好序的 key-value 记录
    using BolckData = std::vector<std::pair<std::string, std::string>>;

    class LRUCache
    {
    public:
        //capacity:最多缓存多少个Block
        explicit LRUCache(size_t capacity = 128);
        ~LRUCache();

        //禁止拷贝
        LRUCache(const LRUCache&) = delete;
        LRUCache& operator=(const LRUCache&) = delete;

        // 查找缓存
		// 返回 true 表示命中，out 填充 Block 数据
		// 返回 false 表示未命中（需要从磁盘读）
        bool Get(const std::string &key, BolckData *out);

        //插入缓存（从磁盘读完后调用）
        void Insert(const std::string &key, const BolckData &data);

        //统计信息
        size_t Size() const;        // 当前缓存的 Block 数量
        size_t Capacity() const;    // 最大容量
        uint64_t Hits() const;        // 命中次数
        uint64_t Misses() const;      // 未命中次数
        double HitRate() const;     // 命中率（百分比）

        //清空缓存
        void Clear();

    private:
        // 双向链表节点
        struct Node
        {
            std::string key;    // 用来从 map 删除，对于同一个缓存节点，
            BolckData data;     // Node 里的 key 和 unordered_map 里的 key 是一样的
            Node *prev;
            Node *next;
            Node(const std::string &k, const BolckData &d)
                : key(k), data(d), prev(nullptr), next(nullptr) {}
        };

        //把节点移到链表头部（最近使用）
        void MoveToFront(Node *node);

        //把新节点插入链表头部
        void InsertToFront(Node *node);

        //删除链表中的节点（不释放内存）
        void RemoveFromList(Node *node);

        //淘汰链表尾部节点（最久未用）
        void Exict();

        size_t capacity_;
        mutable std::mutex mutex_;

        //HashMap：cache_key-> Node*
        std::unordered_map<std::string, Node*> cache_map_;  // 快速找到缓存数据

        //双向链表：dummy head和dummy tail(哨兵节点简化边界处理)
        Node *head_;    //哨兵头，head_->next最近使用的节点
        Node *tail_;     //哨兵尾，tail_->prev最久未使用的节点

        // 统计
        uint64_t hits_;
        uint64_t misses_;
    };

    // 生成 cache key 的辅助函数
	// 格式："文件路径:偏移量"，例如 "./data/000001.sst:4096"
    inline std::string MakeCacheKey(const std::string &filename, uint64_t offset)
    {
        return filename + ":" + std::to_string(offset);
    }
}//namespace mini_storage