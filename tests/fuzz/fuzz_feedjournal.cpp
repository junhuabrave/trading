// libFuzzer target: a feed-journal segment with arbitrary bytes after a valid header must either
// open (truncating the tail) or throw; it must never crash, hang or read out of bounds. This runs
// on whatever was on the disk when the process died, so it is the one code path in the journal that
// is guaranteed to be handed hostile input in production.
#include "feed_journal.hpp"
#include <atomic>
#include <filesystem>
#include <fstream>
using namespace trading; using namespace trading::md;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    static std::atomic<unsigned> n{0};
    const std::string dir = "/tmp/fuzz_feedjournal_" + std::to_string(::getpid()) + "_" + std::to_string(n++ % 64);
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    // Half the corpus exercises the packet journal and half the frame journal, because the two
    // validate their payloads differently and only one of them would otherwise be reached.
    const bool frames = size > 0 && (data[0] & 2);
    {
        std::ofstream f(dir + "/seg-000000.mdj", std::ios::binary);
        FeedJournalHeader h{};
        std::memcpy(h.magic, "MDJN", 4);
        h.formatVersion = 1; h.feedId = 1;
        h.kind = uint16_t(frames ? JournalKind::Frames : JournalKind::Packets);
        h.schemaVersion = SCHEMA_VERSION; h.firstVenueSeq = 1; h.segmentIndex = 0; h.businessDate = 20260915;
        if (size > 0 && (data[0] & 1)) h.magic[0] = 'X';              // sometimes corrupt the header too
        f.write(reinterpret_cast<const char*>(&h), sizeof h);
        f.write(reinterpret_cast<const char*>(data), std::streamsize(size));
    }
    try {
        FeedJournal j(dir, 1, frames ? JournalKind::Frames : JournalKind::Packets, 20260915);
        uint64_t bytes = 0;
        j.replay(0, 0, [&](const FeedJournalRecord& r, const std::byte*) { bytes += r.length; });
        j.replay(j.lastVenueSeq(), 0, [&](const FeedJournalRecord& r, const std::byte*) { bytes += r.length; });
        if (frames) {
            Frame<Heartbeat> hb; hb.init();
            j.append(j.lastVenueSeq() + 1, 1, 0, 0, &hb.header, hb.header.frameLength);
        } else {
            alignas(16) std::byte buf[MAX_PACKET];
            PacketBuilder pb(buf);
            pb.begin(1, 0, j.lastVenueSeq() + 1, 0);
            j.append(j.lastVenueSeq() + 1, 0, 0, 0, buf, uint32_t(pb.size()));
        }
        j.commit(false);
    } catch (const std::exception&) {}
    std::filesystem::remove_all(dir);
    return 0;
}
