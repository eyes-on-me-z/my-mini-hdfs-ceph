#include "datanode_block_store.h"

#include <fstream>
#include <filesystem>
#include <zlib.h>
#include <sys/statvfs.h>

namespace fs = std::filesystem;

namespace mini_storage
{
    // 返回当前时间距离 Unix 时间戳起点的毫秒数
    // 当前时间 - 1970-01-01 00:00:00 UTC
    static int64_t NowMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();    
    }

    BlockStore::BlockStore(const std::string &data_dir)
        : data_dir_(data_dir), blocks_dir_(data_dir + "/blocks"), tmp_dir_(data_dir + "/tmp")
    {
        fs::create_directories(blocks_dir_);
        fs::create_directories(tmp_dir_);
        CleanTempFiles();
    }

    // 把一个 block 的数据写入本地磁盘，并生成对应的元数据文件
    // 如果元数据写失败，就删除临时数据文件。更严谨的话，也可以删除可能残留的 .meta 文件
    bool BlockStore::WriteBlock(BlockId block_id, const std::string &data)
    {
        // 写临时 data 文件 + 计算元数据 + 写 meta 文件 + 把临时 data 文件改名为正式 data 文件
        std::string tmp_path = BlockTmpPath(block_id);
        {
            std::ofstream f(tmp_path, std::ios::binary);
            if(!f.is_open()) return false;
            f.write(data.data(), (std::streamsize)data.size());
            f.flush();
        }

        BlockMeta meta;
        meta.block_id = block_id;
        meta.size = (int64_t)data.size();
        meta.crc32 = ComputeCRC32(data);
        meta.create_time = NowMs();
        if (!WriteMeta(block_id, meta))
        {
            // 未来改进的点：如果 WriteMeta 失败，删除 tmp，也删除 meta
            fs::remove(tmp_path);
            return false;
        }

        std::string dat_path = BlockDataPath(block_id);
        if (std::rename(tmp_path.c_str(), dat_path.c_str()) != 0)
        {
            fs::remove(tmp_path);
            return false;
        }

        return true;
    }

    bool BlockStore::ReadBlock(BlockId block_id, int64_t offset, int64_t length,
                    std::string *data_out)
    {
        BlockMeta meta;
        if (!ReadMeta(block_id, &meta)) return false;

        std::ifstream f(BlockDataPath(block_id), std::ios::binary);
        if (!f.is_open()) return false;

        // 把读取请求整理成一个合法范围
        int64_t file_size = meta.size;
        if (offset < 0 || offset > file_size) return false;     // 检查 offset 是否合法
        if (length <= 0 || offset + length > file_size) // length <= 0 表示读到文件尾, 请求长度超过文件末尾，也改成读到末尾
            length = file_size - offset;
        if (length == 0) { data_out->clear(); return true; }    // 如果最终要读取的长度是 0，就把输出清空，并返回成功

        // 读取
        f.seekg(offset);
        data_out->resize((size_t)length);
        f.read(&(*data_out)[0], length);
        if (f.gcount() != length) return false;

        // CRC check on full read
        if (offset == 0 && length == file_size)
        {
            uint32_t actual = ComputeCRC32(*data_out);
            if (actual != meta.crc32) return false;
        }

        return true;
    }

    bool BlockStore::DeleteBlock(BlockId block_id)
    {
        bool ok = true;
        auto dat = BlockDataPath(block_id);
        auto meta = BlockMetaPath(block_id);
        if (fs::exists(dat)) ok &= (bool)fs::remove(dat);
        if (fs::exists(meta)) ok &= (bool)fs::remove(meta);
        return ok;
    }

    bool BlockStore::HasBlock(BlockId block_id) const
    {
        return fs::exists(BlockDataPath(block_id));
    }

    // std::optional<BlockMeta>: 这个函数可能返回一个 BlockMeta，也可能什么都不返回
    std::optional<BlockMeta> BlockStore::GetBlockMeta(BlockId block_id) const
    {
        BlockMeta m;
        if (!ReadMeta(block_id, &m)) return std::nullopt;
        return m;
    }

    // 扫描 blocks_dir_ 目录，找出所有 .meta 文件，读取每个 block 的元数据并返回
    std::vector<BlockMeta> BlockStore::ListAllBlocks() const
    {
        // s.substr(起始下标, 长度)
        std::vector<BlockMeta> result;
        if (!fs::exists(blocks_dir_)) return result;

        for (const auto &entry : fs::directory_iterator(blocks_dir_))
        {
            std::string name = entry.path().filename().string();
            if (name.size() > 5 && name.substr(name.size() - 5) == ".meta")
            {
                // block_000001.meta → id = "000001"
                if (name.size() < 12) continue; // // "block_" + 6 + ".meta" = 17
                std::string id_str = name.substr(6, name.size() - 11);
                try
                {
                    BlockId id = std::stoull(id_str);
                    BlockMeta m;
                    if (ReadMeta(id, &m)) result.push_back(m);
                }
                catch(...) {}
            }
        }

        return result;
    }

    // 查询 data_dir_ 所在磁盘/文件系统还剩多少可用空间，单位是字节
    // 用于向 NameNode 汇报我当前还有多少可用磁盘空间
    int64_t BlockStore::GetFreeSpace() const
    {
        /*
        statvfs 是 POSIX 系统里用来保存文件系统统计信息的结构体
        */
        struct statvfs stat;
        if (statvfs(data_dir_.c_str(), &stat) != 0) return 0;
        // stat.f_bavail: 非特权用户可用的空闲块数
        // stat.f_frsize: 文件系统块大小，单位字节
        // f_bfree   文件系统总空闲块数，包括 root 保留块; f_bavail  普通用户可用空闲块数，不包括保留块
        return (int64_t)stat.f_bavail * (int64_t)stat.f_frsize;
    }

    // 清空 tmp_dir_ 目录下面的临时文件
    // 用于启动时清理未完成写入的临时 block 文件
    void BlockStore::CleanTempFiles()
    {
        if (!fs::exists(tmp_dir_)) return;
        for (const auto &entry : fs::directory_iterator(tmp_dir_))
        {
            fs::remove(entry.path());
        }
    }

    // 根据 block id 生成该 block 的数据文件路径
    // 如：/data/blocks/block_000012.dat
    std::string BlockStore::BlockDataPath(BlockId id) const
    {
        char buf[64];
        snprintf(buf, sizeof buf, "block_%06llu.dat", (unsigned long long)id);
        return blocks_dir_ + "/" + buf;
    }

    // 根据 block id 生成该 block 的元数据文件路径
    // 如：/data/blocks/block_000012.meta
    std::string BlockStore::BlockMetaPath(BlockId id) const
    {
        char buf[64];
        snprintf(buf, sizeof buf, "block_%06llu.meta", (unsigned long long)id);
        return blocks_dir_ + "/" + buf;
    }

    // 根据 block id 生成该 block 的临时文件路径
    // 如：/data/tmp/block_000012.data.tmp
    std::string BlockStore::BlockTmpPath(BlockId id) const
    {
        char buf[64];
        snprintf(buf, sizeof buf, "block_%06llu.dat.tmp", (unsigned long long)id);
        return tmp_dir_ + "/" + buf;
    }

    // 把某个 block 的元数据写入对应的 .meta 文件
    // 路径：blocks/block_000012.meta
    // 内容：12 4096 1234567890 1782450000123
    bool BlockStore::WriteMeta(BlockId id, const BlockMeta &meta)
    {
        /*
        如果程序写到一半崩溃，.meta 文件可能变成半截。
        更可靠的做法是先写 .tmp，成功后再 rename 成正式 .meta。
        */
        std::ofstream f(BlockMetaPath(id));
        if (!f.is_open()) return false;
        f << meta.block_id << " " << meta.size << " "
            << meta.crc32 << " " << meta.create_time << "\n";

        return true;
    }

    // 从某个 block 的 .meta 文件里读取元数据，填到 BlockMeta 结构体中
    bool BlockStore::ReadMeta(BlockId id, BlockMeta *meta) const
    {
        std::ifstream f(BlockMetaPath(id));
        if (!f.is_open()) return false;
        // 从文件中按空白字符分隔读取 4 个字段
        f >> meta->block_id >> meta->size >> meta->crc32 >> meta->create_time;
        return !f.fail();
    }

    // 计算一段数据的 CRC32 校验值
    uint32_t BlockStore::ComputeCRC32(const std::string &data)
    {
        return crc32(0, (const Bytef*)data.data(), data.size());
    }
} // namespace mini_storage