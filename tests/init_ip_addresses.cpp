#include "../config.hpp"
#include <iostream>
#include <cassert>
#include <arpa/inet.h>

int main() {
    struct in_addr addrs1[BASE_CLUSTER_SIZE];
    for (int i = 0; i < BASE_CLUSTER_SIZE; ++i) {
        if (inet_pton(AF_INET, INIT_CLUSTER[i].c_str(), &addrs1[i]) != 1) return -1;
    }

    constexpr std::array<IPAddr, BASE_CLUSTER_SIZE> addrs = get_addrs();
    for (int i = 0; i < BASE_CLUSTER_SIZE; ++i) {
        assert(addrs[i] == addrs1[i].s_addr);
    }
    std::cout << "Test passed\n";

}
