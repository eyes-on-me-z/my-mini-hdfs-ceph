#include "MemTable.h"

namespace mini_storage
{
    // 构造函数：初始化头节点(key/value为空，层数为最大)
    MemTable::MemTable()
        : head_(new SkipListNode("", "", MAX_LEVEL))
        , level_count_(1)
        , count_(0)
        , size_(0)
        , rng_(std::time(0))    // 用当前时间作为种子，初始化 mt19937 随机数引擎
    {}

    // 析构函数：释放链表所有节点
    MemTable::~MemTable()
    {
        Clear();
        delete head_;
    }

    // 随机层数产生器：P=0.25的概率增加一层，最大不超过MAX_LEVEL
    int MemTable::RandomLevel()
    {
        /*新节点默认 1 层，有 25% 概率升到第 2 层
        如果升到了第 2 层，又有 25% 概率升到第 3 层
        继续这样随机，最多不超过 MAX_LEVEL，也就是 16 层
        1 层：75%   2 层：18.75%    3 层：4.6875%   4 层：1.171875% ...
        */

        int level = 1;
        // 使用均匀分布随机数。生成 [0, 1) 附近的随机小数
        std::uniform_real_distribution<double> dist(0, 1);
        while(dist(rng_) < 0.25 && level < MAX_LEVEL)
        {
            level++;
        }

        return level;
    }

    //插入/更新数据(Put)
    void MemTable::Put(const std::string &key, const std::string &value)
    {
        std::lock_guard<std::mutex> lock(mutex_);   // 物理锁保证并发安全

        //用于记录每一层中，目标位置的前驱节点
        std::vector<SkipListNode*> update(MAX_LEVEL, nullptr);
        SkipListNode *cur = head_;

        // 1.从最高层向下寻找插入位置
        for (int i = level_count_ - 1; i >= 0; --i)
        {
            while(cur->next[i] != nullptr && cur->next[i]->key < key)
            {
                cur = cur->next[i];
            }
            update[i] = cur;
        }

        // 检查最底层的下一个节点是否就是我们要找的key
        cur = cur->next[0];

        if (cur != nullptr && cur->key == key)
        {
            // 情况A：key已存在，执行更新操作
            // 先减去旧value的大小
            size_ -= (cur->value.size());
            cur->value = value;
            //加上新value的大小
            size_ += (value.size());
        }
        else
        {
            // 情况B：key不存在，插入新节点
            int new_level = RandomLevel();

            //如果新节点层数超过当前跳表高度，更新高层的前驱为头节点
            if (new_level > level_count_)
            {
                for (int i = new_level - 1; i >= level_count_; --i)
                {
                    update[i] = head_;
                }
                level_count_ = new_level;
            }

            // 创建并插入节点
            SkipListNode *newNode = new SkipListNode(key, value, new_level);
            for (int i = 0; i < new_level; ++i)
            {
                newNode->next[i] = update[i]->next[i];
                update[i]->next[i] = newNode;
            }

            // 更新统计信息
            count_++;
            size_ += (key.size() + value.size());
        }
    }

    // 读取数据(Get)
    bool MemTable::Get(const std::string &key, std::string *value)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        SkipListNode *cur = head_;

        // 从顶层向下查找，利用跳表的索引实现快速定位
        for (int i = level_count_ - 1; i >= 0; --i)
        {
            while(cur->next[i] != nullptr && cur->next[i]->key < key)
            {
                cur = cur->next[i];
            }
        }

        // 移向最底层可能的匹配节点
        cur = cur->next[0];
        if (cur != nullptr && cur->key == key)
        {
            if (value) *value = cur->value;
            return true;
        }

        return false;
    }

        //返回占用内存大小
        size_t MemTable::Size() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return size_;
        }

        // 是否为空
        bool MemTable::Empty() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return count_ == 0;
        }

        // 清空数据
        void MemTable::Clear()
        {
            std::lock_guard<std::mutex> lock(mutex_);

            // 从最底层的链表开始，逐个释放节点内存
            SkipListNode *cur = head_->next[0];
            while(cur != nullptr)
            {
                SkipListNode *next = cur->next[0];
                delete cur;
                cur = next;
            }

            // 重置头节点的指针数组
            for (int i = 0; i < MAX_LEVEL; ++i)
            {
                head_->next[i] = nullptr;
            }

            size_ = 0;
            count_ = 0;
            level_count_ = 1;
        }

        // 计算内存占用（辅助方法）
        size_t MemTable::CalculateSize(const std::string &key, const std::string &value)
        {
            return key.size() + value.size();
        }

        //迭代器接口实现

        //创建新迭代器
        MemTable::Iterator* MemTable::NewIterator()
        {
            /*
            直接把当前跳表的 head_ 交给迭代器
            问题是：迭代器遍历期间，MemTable 可能被其他线程修改。
            Iterator 正在指向某个节点
            另一个线程 Clear() 把节点 delete 了
            Iterator 再访问 current_->key直接出问题

            第一种：加锁。迭代期间持有锁，不允许别人修改 MemTable
            第二种：使用快照。创建迭代器时得到一个稳定视图：Iterator 看到的是某一时刻的 MemTable 内容
            后续 Put/Delete 不影响这个 Iterator

            在 LSM-tree 里更常见的是：刷盘时把当前 mem_table_ 切换成 immutable memtable，
            新的写入进入新的 MemTable，旧 MemTable 不再修改，只负责被迭代刷盘。
            */

            // 注意：在实际工业级实现中，这里通常会加锁或使用快照
            // 教学版这样可以理解主流程，
            // 但工业级会把旧 MemTable 冻结成 immutable memtable，再切一个新的 MemTable 接着写。
            return new Iterator(head_);
        }


        // 迭代器构造函数：跳表迭代器只需要从最底层的第一个节点开始即可
        MemTable::Iterator::Iterator(SkipListNode *head)
        {
            // 跳表的level0就是一个有序单链表
            current_ = head->next[0];
        }

        bool MemTable::Iterator::Valid() const
        {
            return current_ != nullptr;
        }

        void MemTable::Iterator::Next()
        {
            if (Valid())
                current_ = current_->next[0];
        }

        std::string MemTable::Iterator::Key() const
        {
            return current_->key;
        }

        std::string MemTable::Iterator::Value() const
        {
            return current_->value;
        }
} // namespace mini_storage