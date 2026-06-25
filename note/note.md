SSTable 是 Sorted String Table 的缩写，意思是“有序字符串表”。
在 LSM-Tree / LevelDB / RocksDB 这类存储系统里，SSTable 通常指一种磁盘上的不可变有序文件：
里面存的是很多 key-value 数据
数据按 key 排好序
文件写好之后一般不再原地修改
查询时可以通过索引、布隆过滤器、块偏移等方式快速定位
后台会通过 compaction 把多个 SSTable 合并、清理旧版本数据

在 LSM-tree(Log Structured Merge Tree) 数据库里，通常会有两种 MemTable：
1. active memtable      当前正在接收写入的 MemTable
2. immutable memtable   已经冻结、不再接收写入、等待刷盘的 MemTable
为什么需要它？因为 MemTable 满了以后要刷成 SSTable。如果直接拿当前 MemTable 去刷盘，同时还继续写入，就会有并发问题：
线程 A 正在遍历 MemTable 写 SSTable，线程 B 又 Put/Delete 修改 MemTable，这样刷出来的数据可能不一致。
所以常见做法是：
1. 当前 memtable 写满
2. 把它标记为 immutable memtable，不再允许修改
3. 新建一个 active memtable，后续写入进入新的 memtable
4. 后台线程慢慢把 immutable memtable 刷成 SSTable
5. 刷盘完成后释放 immutable memtable
immutable 的好处是：刷盘时数据稳定。不用长时间阻塞新写入。迭代器遍历更安全


布隆过滤器
有不一定有，但是没有一定没有

epoll_event 是 Linux epoll 机制里用来描述“某个 fd 关心什么事件 / 发生了什么事件”的结构体
struct epoll_event {
    uint32_t events;  // 事件类型，比如 EPOLLIN、EPOLLOUT
    epoll_data_t data; // 用户自定义数据，常用来存 fd 或指针
};
events 字段表示事件类型：
EPOLLIN   // 可读
EPOLLOUT  // 可写
EPOLLERR  // 错误
EPOLLHUP  // 对端关闭或挂起
EPOLLET   // 边缘触发模式

为什么要设置 SO_REUSEADDR？
服务器程序经常绑定一个固定端口，比如：0.0.0.0:9000。如果服务器刚退出又马上重启，端口可能还处于 TIME_WAIT 等状态。没有 SO_REUSEADDR 时，bind() 可能失败，报：Address already in use。设置后，通常可以更快重新启动服务并绑定同一个端口。

.join()：等待线程结束
当前线程会阻塞，直到目标线程运行完毕。
.detach()：让线程独立运行
当前线程不再等待，也无法再通过该 std::thread 对象控制或等待它。
一个可运行的 std::thread 在析构前必须调用 join() 或 detach() 其中之一；否则程序会调用 std::terminate()。线程池通常应使用 join()，而不是 detach()。

边缘触发 ET 模式必须非阻塞
如果用 EPOLLET，事件只在状态变化时通知一次。常见写法是：
while (true) {
    ssize_t n = read(fd, buf, sizeof(buf));
    if (n > 0) {
        // 处理数据
    } else if (n == -1 && errno == EAGAIN) {
        break; // 已经读干净了
    } else {
        // 关闭或错误
    }
}
这里必须靠非阻塞的 EAGAIN 判断“读到尽头”。如果是阻塞 fd，循环最后一次 read 会直接挂住。


为什么进程上下文切换更“重”？
因为进程有独立的虚拟地址空间。切换进程时，CPU 不只是换一组寄存器，还可能要换页表。
页表一换，之前缓存的虚拟地址到物理地址的映射，也就是 TLB，可能失效。后续访问内存时，需要重新建立映射，性能会受影响。
线程如果在同一个进程内切换，则共享地址空间，页表不用换，所以一般更轻。



select、poll、epoll的区别是什么