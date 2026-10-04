#include <optional>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include "../rpc/protocol/payloads.hpp"
#include "../core/node.hpp"
#ifdef DEBUG
#include <iostream>
#endif

template <ApplyFunc A, OnCommitCallback C, OnReadStateCallback R>
struct RaftClient {
    RaftClient() = default;
    ~RaftClient();
    RaftClient<A,C,R>& operator=(const RaftClient<A,C,R>&) = delete;
    RaftClient(const RaftClient<A,C,R>&) = delete;
    RaftClient<A,C,R>& operator=(RaftClient<A,C,R>&&) = delete;
    RaftClient(RaftClient<A,C,R>&&) = delete;

    static std::optional<std::string> CreateRaftClient(RaftClient<A,C,R>* client, A&& apply_func, C&& on_commit_callback, R&& on_read_state_callback);
    void AppendCommands(const std::vector<const std::byte*>& commands);
    void AppendCommands(const std::byte (&commands)[MAX_ENTRIES][CMD_SIZE], size_t num_entries);
    void ReadState(FILE*);
    std::optional<std::string> Start();
    void Stop();

    Node<A,C,R> node_;
    ELNodeInbox eli_{};
    ClientNodeInbox ci_{};
    std::jthread node_thread_;

};

template <ApplyFunc A, OnCommitCallback C, OnReadStateCallback R>
inline RaftClient<A,C,R>::~RaftClient() {
    if (node_.running_.load(std::memory_order_acquire)) {
        Stop();
    }
    if (node_thread_.joinable()) {
        node_thread_.join();
    }
}

template <ApplyFunc A, OnCommitCallback C, OnReadStateCallback R>
inline void RaftClient<A,C,R>::AppendCommands(const std::vector<const std::byte*>& commands) {
    #ifdef DEBUG
    std::cout << "client request to append commands\n";
    #endif
    size_t sent{0};
    while (sent < commands.size()) {
        size_t to_send = std::min(MAX_ENTRIES, commands.size() - sent);
        AppendClientReq req{.num_commands = to_send};
        for (int i = 0; i < to_send; ++i) {
            std::memcpy(req.commands[i], commands[sent + i], CMD_SIZE);
        }

        bool done = false;
        while (!done) {
            done = ci_.PushOne(std::move(req));
        }
        sent += to_send;
    }
    node_.Wake();
}

template <ApplyFunc A, OnCommitCallback C, OnReadStateCallback R>
inline void RaftClient<A,C,R>::AppendCommands(const std::byte (&commands)[MAX_ENTRIES][CMD_SIZE], size_t num_commands) {
    assert(num_commands <= MAX_ENTRIES);
    assert(num_commands > 0);

    AppendClientReq req{.num_commands = std::min(MAX_ENTRIES, num_commands)};
    std::memcpy(req.commands, commands, req.num_commands * CMD_SIZE);

    bool done = false;
    while (!done) {
        done = ci_.PushOne(std::move(req));
    }
    node_.Wake();
}

template <ApplyFunc A, OnCommitCallback C, OnReadStateCallback R>
inline void RaftClient<A,C,R>::ReadState(FILE* out) {
    #ifdef DEBUG
    std::cout << "Client request to read state\n";
    #endif
    bool done = false;
    while (!done) {
        done = ci_.PushOne(ReadStateClientReq{out});
    }
    node_.Wake();
}

template <ApplyFunc A, OnCommitCallback C, OnReadStateCallback R>
inline std::optional<std::string> RaftClient<A,C,R>::Start() {
    std::optional<std::string> err;
    node_thread_ = std::jthread([&node_r = node_, &err]() {
        err = node_r.MainLoop();
        if (err) {
            #ifdef DEBUG
            std::cout << "Raft node crashed: " << err.value() << "\n";
            #endif
        }
        return err;
    });
    return err;
}

template <ApplyFunc A, OnCommitCallback C, OnReadStateCallback R>
inline void RaftClient<A,C,R>::Stop() {
    bool done = false;
    while (!done) {
        done = ci_.PushOne(StopNodeMsg{});
    }
    node_.Wake();
}

template <ApplyFunc A, OnCommitCallback C, OnReadStateCallback R>
inline std::optional<std::string>
RaftClient<A,C,R>::CreateRaftClient(RaftClient<A,C,R>* client, A&& apply_func, C&& on_commit_callback, R&& on_read_state_callback) {
    std::optional<std::string> node_err = Node<A,C,R>::CreateNode(&client->node_, &client->eli_, &client->ci_,
                                                        std::forward<decltype(apply_func)&&>(apply_func),
                                                        std::forward<decltype(on_commit_callback)>(on_commit_callback),
                                                        std::forward<decltype(on_read_state_callback)>(on_read_state_callback));

    if (node_err) {
        return node_err.value();
    }

    return {};
}
