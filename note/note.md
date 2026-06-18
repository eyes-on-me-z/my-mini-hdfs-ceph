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