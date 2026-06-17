SSTable 是 Sorted String Table 的缩写，意思是“有序字符串表”。
在 LSM-Tree / LevelDB / RocksDB 这类存储系统里，SSTable 通常指一种磁盘上的不可变有序文件：
里面存的是很多 key-value 数据
数据按 key 排好序
文件写好之后一般不再原地修改
查询时可以通过索引、布隆过滤器、块偏移等方式快速定位
后台会通过 compaction 把多个 SSTable 合并、清理旧版本数据