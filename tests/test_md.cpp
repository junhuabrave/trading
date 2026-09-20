// tests/test_md.cpp <snapshot> <workdir>
//
// The market-data transport layer: the line receiver and capture (MD-2), and the feed publisher's
// lines, impairments and recovery services (MD-10). Nothing here decodes a venue protocol or
// arbitrates between lines; those are MD-4 and MD-3. What is proved here is what they will stand on:
//   1. a day of packets is captured byte-exact and replays identically
//   2. malformed and misaddressed packets are counted, not trusted, and never reach the capture
//   3. drops on one line are covered by the other
//   4. drops on both lines leave a gap that the retransmit service fills
//   5. duplicates and reordering happen, and the union of the lines is still complete
//   6. two publishers given the same canonical stream emit byte-identical packets, which is the
//      precondition for deduping redundant sources by identity in MD-7
#include "line_receiver.hpp"
#include "identity.hpp"
#include "feed_publisher.hpp"
#include "feed_sim.hpp"
#include "refdata.hpp"
#include <cstdio>
#include <filesystem>
#include <map>
#include <set>
#include <vector>

using namespace trading;
using namespace trading::md;
using namespace trading::sim;
namespace fs = std::filesystem;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static constexpr int64_t T0 = 1'700'000'000'000'000'000LL;
static constexpr uint32_t FEED = 1;

// What a consumer of the lines can reconstruct: the set of venue sequences it saw, however it saw
// them. The arbitrator (MD-3) will turn this into an ordered gap-free stream; here we only need to
// know the data was sufficient for it to do so.
struct Seen {
    std::set<uint64_t> seqs;
    std::map<uint64_t, std::vector<std::byte>> firstPayload;   // venueSeq -> frame bytes, first arrival wins
    uint64_t packets = 0, duplicatePackets = 0, frames = 0;
    // Keyed by line: the same packet arriving on A and on B is the redundancy working, not a
    // duplicate. Only a repeat on the same line is one, and counting it any other way would make
    // the duplicate drill pass without duplicating anything.
    std::set<std::pair<uint16_t, uint64_t>> perLineSeen;
    void take(const PacketHeader& h, size_t, uint16_t lineId) {
        ++packets;
        if (!perLineSeen.insert({lineId, h.firstSeq}).second && h.msgCount) ++duplicatePackets;
        uint64_t s = h.firstSeq;
        forEachFrame(h, [&](const FrameHeader* f) {
            ++frames;
            uint64_t v = identity(f).venueSeq ? identity(f).venueSeq : s;
            seqs.insert(v);
            const auto* p = reinterpret_cast<const std::byte*>(f);
            firstPayload.emplace(v, std::vector<std::byte>(p, p + f->frameLength));
            ++s;
        });
    }
    bool covers(uint64_t lo, uint64_t hi) const {
        for (uint64_t s = lo; s <= hi; ++s) if (!seqs.count(s)) return false;
        return true;
    }
    uint64_t firstGap(uint64_t lo, uint64_t hi) const {
        for (uint64_t s = lo; s <= hi; ++s) if (!seqs.count(s)) return s;
        return 0;
    }
};

int main(int argc, char** argv) {
    if (argc != 3) { std::fprintf(stderr, "usage: %s snapshot workdir\n", argv[0]); return 2; }
    fs::create_directories(argv[2]);
    refdata::Snapshot snap(argv[1]); refdata::RefData rd; rd.load(snap);
    CHECK(rd.knownFeed(FEED));
    const uint32_t NSYM = 10;

    // ---- the canonical stream: what the venue actually published, generated once and shared, so
    // two publishers of it can be compared rather than assumed identical
    std::vector<std::vector<std::byte>> canonical;
    {
        FeedSim src(rd, NSYM, 2, 8, 4242);
        int64_t t = T0;
        while (canonical.size() < 2000) {
            src.burst(4, t, [&](FrameHeader* f) {
                const auto* p = reinterpret_cast<const std::byte*>(f);
                canonical.emplace_back(p, p + f->frameLength);
            });
            t += 5000;
        }
    }
    auto frameAt = [&](size_t i) { return reinterpret_cast<const FrameHeader*>(canonical[i].data()); };
    const uint64_t firstSeq = identity(frameAt(0)).venueSeq;
    const uint64_t lastSeq = identity(frameAt(canonical.size() - 1)).venueSeq;
    CHECK(firstSeq > 1'000'000 && lastSeq == firstSeq + canonical.size() - 1);
    std::printf("canonical: %zu messages, venue seq %llu..%llu\n", canonical.size(),
                (unsigned long long)firstSeq, (unsigned long long)lastSeq);

    // ---- 1 and 2: a clean day, captured and replayed
    std::string capPath = std::string(argv[2]) + "/feed1-clean.cap";
    Seen live;
    {
        CaptureWriter cap(capPath, FEED, snap.businessDate(), T0);
        auto sink = [&](const PacketHeader& h, size_t len, uint16_t line, int64_t) { live.take(h, len, line); };
        LineReceiver a(FEED, 0, &cap, sink), b(FEED, 1, &cap, sink);
        FeedPublisher pub(FeedPublisher::Params{FEED, 2, 100, 4, 4096}, 7);
        auto wire = [&](const void* bytes, size_t len, uint16_t line, int64_t ts) {
            (line == 0 ? a : b).onPacket(bytes, len, ts);
        };
        int64_t t = T0;
        for (auto& c : canonical) { pub.publish(reinterpret_cast<const FrameHeader*>(c.data()), t, wire); t += 2000; }
        pub.flush(t, wire);
        // a packet that is not ours, and one that is truncated: counted, and never captured
        PacketHeader bogus{}; std::memcpy(bogus.magic, "MDPK", 4); bogus.feedId = 999; bogus.payloadLen = 0;
        a.onPacket(&bogus, sizeof bogus, t);
        char trunc[8] = {}; a.onPacket(trunc, sizeof trunc, t);
        CHECK(a.malformed() == 1 && a.wrongFeed() == 1);
        CHECK(live.covers(firstSeq, lastSeq));
        CHECK(live.duplicatePackets == 0);        // a clean day has none, which is what makes the count mean something
        cap.close();
        std::printf("clean day: %llu packets, %llu frames, capture %llu records / %llu bytes\n",
                    (unsigned long long)live.packets, (unsigned long long)live.frames,
                    (unsigned long long)cap.records(), (unsigned long long)cap.bytes());
        CHECK(cap.records() == a.captured() + b.captured());
    }
    // replay the capture: same packets, same lines, same times, same frames
    {
        Seen replayed; uint64_t recs = 0; std::vector<std::pair<uint16_t, int64_t>> order;
        recs = replayCapture(capPath, [&](const PacketHeader& h, size_t len, uint16_t line, int64_t ts) {
            replayed.take(h, len, line); order.emplace_back(line, ts);
        });
        std::printf("replay: %llu records, %llu frames, covers the day: %s\n", (unsigned long long)recs,
                    (unsigned long long)replayed.frames, replayed.covers(firstSeq, lastSeq) ? "yes" : "NO");
        CHECK(recs > 0 && replayed.frames == live.frames && replayed.packets == live.packets);
        CHECK(replayed.seqs == live.seqs);
        CHECK(replayed.firstPayload == live.firstPayload);     // byte-exact, not merely equivalent
    }

    // ---- 3: line A loses 20 percent, line B covers it
    {
        Seen seen;
        auto sink = [&](const PacketHeader& h, size_t len, uint16_t line, int64_t) { seen.take(h, len, line); };
        LineReceiver a(FEED, 0, nullptr, sink), b(FEED, 1, nullptr, sink);
        FeedPublisher pub(FeedPublisher::Params{FEED, 2, 100, 4, 4096}, 11);
        pub.impair(0, {0.20, 0.0, 0.0, 0});
        auto wire = [&](const void* p, size_t len, uint16_t line, int64_t ts) { (line == 0 ? a : b).onPacket(p, len, ts); };
        int64_t t = T0;
        for (auto& c : canonical) { pub.publish(reinterpret_cast<const FrameHeader*>(c.data()), t, wire); t += 2000; }
        pub.flush(t, wire);
        std::printf("A drops 20%%: line A delivered %llu, dropped %llu; line B delivered %llu; union complete: %s\n",
                    (unsigned long long)pub.delivered(0), (unsigned long long)pub.dropped(0),
                    (unsigned long long)pub.delivered(1), seen.covers(firstSeq, lastSeq) ? "yes" : "NO");
        CHECK(pub.dropped(0) > 0 && pub.dropped(1) == 0);
        CHECK(seen.covers(firstSeq, lastSeq));                 // this is what the second line is for
    }

    // ---- 4: both lines lose packets; the gap is real, and retransmit fills it
    {
        Seen seen;
        auto sink = [&](const PacketHeader& h, size_t len, uint16_t line, int64_t) { seen.take(h, len, line); };
        LineReceiver a(FEED, 0, nullptr, sink), b(FEED, 1, nullptr, sink), rx(FEED, 100, nullptr, sink);
        FeedPublisher pub(FeedPublisher::Params{FEED, 2, 100, 4, 4096}, 13);
        pub.impair(0, {0.35, 0.0, 0.0, 0});
        pub.impair(1, {0.35, 0.0, 0.0, 0});
        auto wire = [&](const void* p, size_t len, uint16_t line, int64_t ts) {
            (line == 0 ? a : (line == 1 ? b : rx)).onPacket(p, len, ts);
        };
        int64_t t = T0;
        for (auto& c : canonical) { pub.publish(reinterpret_cast<const FrameHeader*>(c.data()), t, wire); t += 2000; }
        pub.flush(t, wire);
        uint64_t gap = seen.firstGap(firstSeq, lastSeq);
        std::printf("both lines drop 35%%: first gap at venue seq %llu\n", (unsigned long long)gap);
        CHECK(gap != 0);                                        // with both lines lossy, a gap must exist
        // Ask only for the gap, the way an arbitrator will: a recovery service that can only
        // re-send the whole day is not one you can use during the open.
        uint32_t narrow = pub.retransmit(gap, gap, t, wire);
        std::printf("retransmit: %u packet(s) for the one gap at %llu, that sequence recovered: %s\n",
                    narrow, (unsigned long long)gap, seen.seqs.count(gap) ? "yes" : "NO");
        CHECK(narrow >= 1 && narrow <= 2);                      // the packet containing it, and at most a boundary neighbour
        CHECK(seen.seqs.count(gap));
        uint32_t sent = pub.retransmit(firstSeq, lastSeq, t, wire);
        std::printf("retransmit: %u further packets for the rest, complete after recovery: %s\n",
                    sent, seen.covers(firstSeq, lastSeq) ? "yes" : "NO");
        CHECK(sent > 0 && rx.captured() == sent + narrow);
        CHECK(seen.covers(firstSeq, lastSeq));                  // recovery is what makes the feed whole
    }

    // ---- 5: duplicates and reordering. Both are normal on a real line and neither may lose data.
    {
        Seen seen;
        auto sink = [&](const PacketHeader& h, size_t len, uint16_t line, int64_t) { seen.take(h, len, line); };
        LineReceiver a(FEED, 0, nullptr, sink), b(FEED, 1, nullptr, sink);
        FeedPublisher pub(FeedPublisher::Params{FEED, 2, 100, 4, 4096}, 17);
        pub.impair(0, {0.0, 0.15, 0.20, 0});
        pub.impair(1, {0.05, 0.0, 0.10, 0});
        auto wire = [&](const void* p, size_t len, uint16_t line, int64_t ts) { (line == 0 ? a : b).onPacket(p, len, ts); };
        int64_t t = T0;
        for (auto& c : canonical) { pub.publish(reinterpret_cast<const FrameHeader*>(c.data()), t, wire); t += 2000; }
        pub.flush(t, wire);
        std::printf("duplicates and reordering: %llu packets seen, %llu were repeats, union complete: %s\n",
                    (unsigned long long)seen.packets, (unsigned long long)seen.duplicatePackets,
                    seen.covers(firstSeq, lastSeq) ? "yes" : "NO");
        CHECK(seen.duplicatePackets > 0);                       // the drill has to actually duplicate
        CHECK(seen.covers(firstSeq, lastSeq));
    }

    // ---- 6: two publishers, same canonical stream, byte-identical packets. Without this there is
    // no such thing as deduping redundant sources by identity, so MD-7 rests on it.
    {
        std::vector<std::vector<std::byte>> fromP1, fromP2;
        FeedPublisher p1(FeedPublisher::Params{FEED, 1, 100, 4, 4096}, 23);
        FeedPublisher p2(FeedPublisher::Params{FEED, 1, 100, 4, 4096}, 99);   // different seed, same output
        auto keep = [](std::vector<std::vector<std::byte>>& into) {
            return [&into](const void* p, size_t len, uint16_t, int64_t) {
                const auto* b = static_cast<const std::byte*>(p);
                into.emplace_back(b, b + len);
            };
        };
        auto w1 = keep(fromP1), w2 = keep(fromP2);
        int64_t t = T0;
        for (auto& c : canonical) {
            const auto* f = reinterpret_cast<const FrameHeader*>(c.data());
            p1.publish(f, t, w1); p2.publish(f, t, w2); t += 2000;
        }
        p1.flush(t, w1); p2.flush(t, w2);
        std::printf("two publishers: %zu and %zu packets\n", fromP1.size(), fromP2.size());
        CHECK(fromP1.size() == fromP2.size() && !fromP1.empty());
        CHECK(fromP1 == fromP2);                                // identical bytes, not merely identical content
    }

    // ---- the recovery services a late joiner needs
    {
        std::vector<std::vector<std::byte>> got;
        FeedPublisher pub(FeedPublisher::Params{FEED, 2, 100, 4, 4096}, 29);
        auto wire = [&](const void* p, size_t len, uint16_t, int64_t) {
            const auto* b = static_cast<const std::byte*>(p); got.emplace_back(b, b + len);
        };
        std::vector<const FrameHeader*> book{frameAt(0), frameAt(1), frameAt(2)};
        pub.snapshot(book, lastSeq, T0, wire);
        pub.heartbeat(T0, wire);
        CHECK(got.size() == 3);                                  // one snapshot, two heartbeats (one per line)
        const auto* snapPkt = reinterpret_cast<const PacketHeader*>(got[0].data());
        CHECK((snapPkt->flags & PacketFlags::snapshot) && snapPkt->msgCount == 3 && snapPkt->firstSeq == lastSeq);
        const auto* hb = reinterpret_cast<const PacketHeader*>(got[1].data());
        CHECK((hb->flags & PacketFlags::heartbeat) && hb->msgCount == 0);
        std::printf("recovery services: snapshot of %u messages as of %llu, heartbeat on every line\n",
                    snapPkt->msgCount, (unsigned long long)snapPkt->firstSeq);
    }

    std::printf("market-data transport tests ok\n");
    return 0;
}
