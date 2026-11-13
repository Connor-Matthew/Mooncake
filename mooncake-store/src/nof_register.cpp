#include "ssd_register_client.h"
#include <iostream>
#include <cstdlib>

int main(int argc, char* argv[]) {
    if (argc < 7) {
        std::cerr << "Usage: " << argv[0] << " <nqn> <nsid> <traddr> <trsvcid> <trtype> <master_addr> [size_bytes]\n";
        return 1;
    }
    std::string nqn = argv[1];
    size_t nsid = std::stoul(argv[2]);
    std::string traddr = argv[3];
    size_t trsvcid = std::stoul(argv[4]);
    std::string trtype = argv[5];
    std::string master_addr = argv[6];
    uintptr_t base = 0;
    size_t size = (argc >= 8) ? std::stoull(argv[7]) : 0;
    std::cout << "Registering: nqn=" << nqn << " nsid=" << nsid
              << " traddr=" << traddr << " trsvcid=" << trsvcid
              << " trtype=" << trtype << " base=" << base << " size=" << size << "\n";
    mooncake::NoFRegisterClient client;
    int ret = client.set_register(nqn, nsid, traddr, trsvcid, trtype, base, size, master_addr);
    if (ret == 0) { std::cout << "NoF registration succeeded\n"; return 0; }
    else { std::cerr << "NoF registration failed\n"; return 1; }
}
