#include "../client/client.hpp"
#include <thread>

#include <iostream>
int main() {
    std::cout << "Testing RPC correctness\n";
    auto apply_func = [](FILE* state_machine_fp, const LogEntry& entry) {
        ::fseek(state_machine_fp, 0, SEEK_END);
        int num{69};
        ::fwrite(&num, sizeof(num), 1, state_machine_fp);
    };

    auto on_commit = [](std::span<LogEntry>, int){};
    auto on_read = [](FILE*, int){};

    using Client = RaftClient<decltype(apply_func), decltype(on_commit), decltype(on_read)>;
    Client c{};
    std::optional<std::string> client_err = Client::CreateRaftClient(&c, std::move(apply_func), std::move(on_commit), std::move(on_read));
    if (client_err) {
        #ifdef DEBUG
        std::cout << client_err.value() << "\n";
        #endif
        return 1;
    }

    std::byte one[CMD_SIZE] = {std::byte{0x00}, std::byte{0x12}, std::byte{0x34}, std::byte{0x56}};
    std::byte two[CMD_SIZE] = {std::byte{0x01}, std::byte{0x69}, std::byte{0x67}, std::byte{0x91}};
    std::byte three[CMD_SIZE] = {std::byte{0x02}, std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};

    auto data = std::vector<const std::byte*>{
        one,
        two,
        three
    };

    c.Start();
    c.AppendCommands(data);
    std::this_thread::sleep_for(std::chrono::seconds(15));
    c.Stop();

    std::cout << "Test Passed\n";
}
