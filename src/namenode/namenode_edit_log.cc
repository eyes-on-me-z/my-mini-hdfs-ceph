#include "namenode_edit_log.h"

#include <stdexcept>
#include <arpa/inet.h>

namespace mini_storage
{
    EditLog::EditLog(const std::string &log_path)
        : log_path_(log_path)
    {
        write_file_.open(log_path_, std::ios::binary | std::ios::app);
        if (!write_file_.is_open())
            throw std::runtime_error("Cannot open edit log: " + log_path);
    }

    EditLog::~EditLog()
    {
        Close();
    }

    bool EditLog::Append(const NameNodeRequest &op)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        std::string data;
        if (!op.SerializeToString(&data)) return false;

        // 为什么要 htonl？
        // 因为不同机器的整数内存存储顺序可能不同
        uint32_t len = htonl((uint32_t)data.size());
        write_file_.write(reinterpret_cast<const char*>(&len), 4);
        write_file_.write(data.data(), data.size());
        write_file_.flush();

        return !write_file_.fail();
    }

    // 读取日志文件里的每一条 NameNodeRequest，然后把每条请求交给 callback 处理。
    bool EditLog::Replay(ReplayCallback callback)
    {
        // Replay 负责读日志。callback 负责决定读出来的日志怎么恢复到内存状态
        std::ifstream file(log_path_, std::ios::binary);
        if (!file.is_open()) return true;  // no history

        int count = 0;
        while(file.good())
        {
            uint32_t len;
            file.read(reinterpret_cast<char*>(&len), 4);
            if (file.gcount() == 0) break;
            if (file.gcount() != 4) { std::cerr << "[EditLog] Truncated\n"; break; }

            len = ntohl(len);
            std::string bytes(len, '\0');
            file.read(&bytes[0], len);
            if ((uint32_t)file.gcount() != len) { std::cerr << "[EditLog] Incomplete\n"; break; }

            NameNodeRequest op;
            if (op.ParseFromString(bytes)) {callback(op); count++; }
        }

        std::cout << "[EditLog] Replayed " << count << " entries\n";
        return true;
    }

    void EditLog::Close()
    {
        if (write_file_.is_open())
        {
            write_file_.close();
        }
    }
} // namespace mini_storage