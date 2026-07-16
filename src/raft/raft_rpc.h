#pragma once

#include "raft_node.h"
#include "net_tcp_server.h"
#include "net_event_loop.h"
#include "raft.pb.h"

#include <memory>

namespace mini_storage
{
    // RaftRPC handles networking for Raft consensus.
    // Each HANameNode has one RaftRPC instance that:
    //   - Listens on a port for incoming Raft messages
    //   - Sends Raft messages to peers (connect/send/close per message)

    // RaftRPC 只负责网络收发；真正的 Raft 状态变化，比如统计投票、更新 follower 复制进度，是交给 RaftNode 做的。
    class RaftRPC
    {
    public:
        RaftRPC(RaftNode *raft_node, const std::string &host, int port);
        ~RaftRPC();

        bool Start();
        void Stop();

        // Send a RaftMessage to a specific peer (peer_id format: "host:port")
        // SendMessage 负责发出请求，并同步接收该请求的响应
        bool SendMessage(const std::string &peer_id, const RaftMessage &msg);

    private:
        // OnMessage 主要处理别人发来的请求
        void OnMessage(std::shared_ptr<TcpConnection> conn, const std::string &data);
        void HandleIncoming(const RaftMessage &msg);

        RaftNode *raft_node_;
        std::string host_;
        int port_;

        std::unique_ptr<EventLoop> loop_;
        std::unique_ptr<TcpServer> server_;
        std::thread loop_thread_;
        std::atomic<bool> stop_{false};
    };

} // namespace mini_storage