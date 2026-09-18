// libFuzzer target: a journal segment with arbitrary bytes after a valid header must either
// open (truncating the tail) or throw; it must never crash, hang or read out of bounds.
#include "journal.hpp"
#include <filesystem>
#include <fstream>
#include <atomic>
using namespace trading; using namespace trading::seq;
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    static std::atomic<unsigned> n{0};
    std::string dir = "/tmp/fuzz_journal_" + std::to_string(::getpid()) + "_" + std::to_string(n++ % 64);
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        std::ofstream f(dir + "/seg-000000.jnl", std::ios::binary);
        JournalHeader h{}; std::memcpy(h.magic, "SEQJ", 4); h.formatVersion = 2; h.streamId = 7; h.schemaVersion = SCHEMA_VERSION; h.firstSeq = 1;
        if (size > 0 && (data[0] & 1)) { h.magic[0] = 'X'; }           // sometimes corrupt the header too
        f.write(reinterpret_cast<const char*>(&h), sizeof h);
        f.write(reinterpret_cast<const char*>(data), std::streamsize(size));
    }
    try {
        Journal j(dir, 7);
        uint64_t seen = 0;
        j.replay(1, 0, [&](const FrameHeader* f) { seen += f->frameLength; });
        Frame<Heartbeat> hb; hb.init(); hb.header.seq = j.lastSeq() + 1; j.append(&hb.header); j.commit(false);
    } catch (const std::exception&) {}
    std::filesystem::remove_all(dir);
    return 0;
}
