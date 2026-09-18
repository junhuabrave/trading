// libFuzzer target: the snapshot loader over arbitrary bytes must reject or load, never crash.
#include "snapshot.hpp"
#include <filesystem>
#include <fstream>
#include <atomic>
#include <vector>
#include <algorithm>
#include <cstring>
using namespace trading; using namespace trading::refdata;
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    static std::atomic<unsigned> n{0};
    std::string path = "/tmp/fuzz_snapshot_" + std::to_string(::getpid()) + "_" + std::to_string(n++ % 64) + ".bin";
    {
        std::ofstream f(path, std::ios::binary);
        // pad to a page so the size check passes and the header fields are what gets fuzzed
        std::vector<char> buf(PAGE * 3, 0);
        std::memcpy(buf.data(), "RFDS", 4);
        std::memcpy(buf.data() + 4, data, std::min(size, buf.size() - 4));
        f.write(buf.data(), std::streamsize(buf.size()));
    }
    try {
        Snapshot s(path);
        (void)s.instruments().size(); (void)s.lookupTicker("AAPL"); (void)s.inList(ListId::EasyToBorrow, 5);
    } catch (const std::exception&) {}
    std::filesystem::remove(path);
    return 0;
}
