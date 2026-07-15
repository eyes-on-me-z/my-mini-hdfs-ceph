#include "raft_rpc.h"
#include "net_io_helpers.h"
#include "net_tcp_connection.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>

namespace mini_storage
{
    RaftRPC::RaftRPC(RaftNode *raft_node, const std::string &host, int port)
        : raft_node_(raft_node), host_(host), port_(port)
    {
        loop_ = std::make_unique<EventLoop>();
        server_ = std::make_unique<TcpServer>(loop_.get(), host_, port_);
        server_->SetMessageCallback([this] (auto conn, const auto &data) { OnMessage(conn, data); });
    }

    RaftRPC::~RaftRPC()
    {
        Stop();
    }

    bool RaftRPC::Start()
    {
        if (!server_->Start())
        {
            std::cerr << "[RaftRPC] Failed to start on " << host_ << ":" << port_ << "\n";
            return false;
        }
        loop_thread_ = std::thread([this] { loop_->Loop(); });
        std::cout << "[RaftRPC] Listening on " << host_ << ":" << port_ << "\n";
        return true;
    }

    void RaftRPC::Stop()
    {
        stop_.store(true);
        loop_->Quit();
        if (loop_thread_.joinable())
        {
            loop_thread_.join();
        }
    }

    // Send a RaftMessage to a specific peer (peer_id format: "host:port")
    // Raft 节点主动给某个 peer 发送一条 Raft 消息
    bool RaftRPC::SendMessage(const std::string &peer_id, const RaftMessage &msg)
    {
        // 解析 peer_id
        size_t colon = peer_id.rfind(':');
        if (colon == std::string::npos) return false;

        std::string host = peer_id.substr(0, colon);
        int port = std::stoi(peer_id.substr(colon + 1));

        // 创建 TCP socket
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return false;

        // 设置 3 秒收发超时
        struct timeval tv{3, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

        // 连接 peer
        if (connect(fd, (struct sockaddr*)(&addr), sizeof addr) < 0)
        {
            close(fd);
            return false;
        }

        // 序列化 RaftMessage
        std::string bytes;
        if (!msg.SerializeToString(&bytes)) { close(fd); return false; };

        // 发送消息
        if (!SendMsg(fd, bytes)) { close(fd); return false; };

        // 如果是请求类消息，就等待响应
        auto msg_type = msg.type();
        if (msg_type == RaftMessage::REQUEST_VOTE ||
            msg_type == RaftMessage::APPEND_ENTRIES ||
            msg_type == RaftMessage::INSTALL_SNAPSHOT)
        {
            std::string resp_bytes;
            if (RecvMsg(fd, &resp_bytes))
            {
                // 解析响应并交给 RaftNode
                RaftMessage resp_msg;
                if (resp_msg.ParseFromString(resp_bytes))
                {
                    switch (resp_msg.type())
                    {
                    case RaftMessage::REQUEST_VOTE_RESPONSE:
                        raft_node_->HandleRequestVoteResponse(peer_id, resp_msg.request_vote_response());
                        break;
                    case RaftMessage::APPEND_ENTRIES_RESPONSE:
                        raft_node_->HandleAppendEntriesResponse(peer_id, resp_msg.append_entries_response());
                        break;
                    case RaftMessage::INSTALL_SNAPSHOT_RESPONSE:
                        // Snap response - currently no special handling needed
                        break;
                    default:
                        break;
                    }
                }
            }
        }
        close(fd);
        return true;
    }

    void RaftRPC::OnMessage(std::shared_ptr<TcpConnection> conn, const std::string &data)
    {
        // 解析消息
        RaftMessage msg;
        if (!msg.ParseFromString(data))
        {
            std::cerr << "[RaftRPC] Failed to parse RaftMessage\n";
            return;
        }

        RaftMessage reply;

        // 根据消息类型分发
        switch (msg.type())
        {
        case RaftMessage::REQUEST_VOTE:
        {
            reply.set_type(RaftMessage::REQUEST_VOTE_RESPONSE);
            auto *resp = raft_node_->HandleRequestVote(msg.request_vote().candidate_id(), msg.request_vote());
            *reply.mutable_request_vote_response() = *resp;
            break;
        }
        case RaftMessage::APPEND_ENTRIES:
        {
            reply.set_type(RaftMessage::APPEND_ENTRIES_RESPONSE);
            auto *resp = raft_node_->HandleAppendEntries(msg.append_entries().leader_id(), msg.append_entries());
            *reply.mutable_append_entries_response() = *resp;
            break;
        }
        case RaftMessage::INSTALL_SNAPSHOT:
        {
            reply.set_type(RaftMessage::INSTALL_SNAPSHOT_RESPONSE);
            auto *resp = raft_node_->HandleInstallSnapshot(msg.install_snapshot().leader_id(), msg.install_snapshot());
            *reply.mutable_install_snapshot_response() = *resp;
            break;
        }
        case RaftMessage::REQUEST_VOTE_RESPONSE:    // 如果收到的是响应类消息，不再回包
        {
            raft_node_->HandleRequestVoteResponse("", msg.request_vote_response());
            return;
        }
        case RaftMessage::APPEND_ENTRIES_RESPONSE:
        {
            raft_node_->HandleAppendEntriesResponse("", msg.append_entries_response());
            return;
        }
        case RaftMessage::INSTALL_SNAPSHOT_RESPONSE:
        {
            return;
        }
        }

        std::string reply_bytes;
        if (reply.SerializeToString(&reply_bytes))
            conn->Send(reply_bytes);
    }

    void RaftRPC::HandleIncoming(const RaftMessage &msg);

} // namespace mini_storage