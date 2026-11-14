#include "master_client.h"
#include "types.h"
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0]
                  << " <master_addr> [traddr] [--list]\n";
        return 1;
    }
    std::string master_addr = argv[1];
    bool list_only = false;
    std::string match;
    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--list") list_only = true;
        else match = a;
    }
    mooncake::MasterClient client(mooncake::generate_uuid());
    auto conn = client.Connect(master_addr);
    if (conn != mooncake::ErrorCode::OK) {
        std::cerr << "Connect failed, rc=" << static_cast<int>(conn) << "\n";
        return 1;
    }
    auto segs = client.GetAllNoFSegments();
    if (!segs) {
        std::cerr << "GetAllNoFSegments failed\n";
        return 1;
    }
    int removed = 0;
    for (auto& seg : *segs) {
        std::cout << "seg id=" << mooncake::UuidToString(seg.id)
                  << " name=" << seg.name
                  << " te=" << seg.te_endpoint
                  << " size=" << seg.size << "\n";
        if (list_only) continue;
        if (!match.empty() &&
            (seg.name.find(match) != std::string::npos ||
             seg.te_endpoint.find(match) != std::string::npos)) {
            auto r = client.UnmountNoFSegment(seg.id);
            std::cout << "  -> unmount " << seg.name << ": "
                      << (r ? "OK" : "FAILED") << "\n";
            if (r) ++removed;
        }
    }
    std::cout << "removed=" << removed << "\n";
    return 0;
}
