#include <iostream>
// #include "../config.hpp"
#include <cstring>
#include <cassert>
#include <vector>
#include <arpa/inet.h>

int main() {
    #ifdef DEBUG
    std::cout << "client request to append commands\n";
    #endif
    size_t sent{0};
    const size_t MAX_ENTRIES = 3;
    const size_t CMD_SIZE = 4;

    const std::byte one[CMD_SIZE] = {std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    const std::byte two[CMD_SIZE] = {std::byte{5}, std::byte{6}, std::byte{7}, std::byte{8}};
    const std::byte three[CMD_SIZE] = {std::byte{13}, std::byte{14}, std::byte{15}, std::byte{16}};
    const std::byte four[CMD_SIZE] = {std::byte{20}, std::byte{21}, std::byte{22}, std::byte{23}};
    const std::byte five[CMD_SIZE] = {std::byte{3}, std::byte{5}, std::byte{7}, std::byte{9}};
    const std::byte six[CMD_SIZE] = {std::byte{1}, std::byte{5}, std::byte{2}, std::byte{6}};
    const std::byte seven[CMD_SIZE] = {std::byte{9}, std::byte{8}, std::byte{7}, std::byte{6}};

    std::vector<const std::byte*> commands = {one, two, three, four, five, six, seven};

    int i{0};
    while (sent < commands.size()) {
        size_t to_send = std::min(MAX_ENTRIES, commands.size() - sent);
        std::byte buf[MAX_ENTRIES][CMD_SIZE];
        for (int i = 0; i < to_send; ++i) {
            std::memcpy(buf[i], commands[sent + i], CMD_SIZE);
        }

        for (int x = 0; x < to_send; ++x) {
            for (int y = 0; y < sizeof(buf[0]); ++y) {
                std::cout << static_cast<int>(buf[x][y]) << ", ";
                assert(static_cast<int>(buf[x][y]) == static_cast<int>(commands[sent + x][y]));
            }
            std::cout << "\n";
        }

        ++i;
        sent += to_send;
    }
    assert(i == (commands.size() / MAX_ENTRIES) + (commands.size() % MAX_ENTRIES));
}
