#pragma once

#include "namenode.pb.h"

#include <functional>
#include <string>
#include <fstream>
#include <mutex>

namespace mini_storage
{
    class EditLog
    {
    public:
        explicit EditLog(const std::string &log_path);
        ~EditLog();

        bool Append(const NameNodeRequest &op);

        using ReplayCallback = std::function<void(const NameNodeRequest&)>;
        // 读取日志文件里的每一条 NameNodeRequest，然后把每条请求交给 callback 处理。
        bool Replay(ReplayCallback callback);

        void Close();

    private:
        // ofstream：output file stream，用来写文件。
        // ifstream：input file stream，用来读文件。
        std::string log_path_;
        std::ofstream write_file_;
        std::mutex mutex_;
    };
} // namespace mini_storage