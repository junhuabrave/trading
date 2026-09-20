// tests/test_selector.cpp <snapshot> <workdir>
//
// Per-feed journals (MD-6) and the source selector (MD-7).
//
// Journals:
//   1. a day of arbitrated packets and decoded frames written and replayed back exactly
//   2. replay to an arbitrary watermark is exact, and seeks: it reads a fraction of the journal,
//      and the number is checked rather than asserted in a comment
//   3. segments rotate and replay crosses them
//   4. a snapshot resynchronisation makes the key jump, which is recorded rather than rejected
//   5. a torn tail is truncated on open and the journal carries on; damage further back is an error
//
// Selector:
//   6. two publishers of one feed, a million messages: every message forwarded exactly once
//   7. one publisher killed mid-packet: still exactly once, no gap and no duplicate
//   8. several messages sharing one venue sequence are told apart, which is the case a sequence
//      number alone cannot handle and the reason identity carries an index
//   9. the direct feed stalls, the SIP takes over, and the switch says so on the core stream
//  10. the direct feed comes back and the selector does not follow it until it has held up
//  11. the same inputs twice produce the same switches
#include "feed_journal.hpp"
#include "selector.hpp"
#include "itch.hpp"
#include "itch_sim.hpp"
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <vector>

using namespace trading;
using namespace trading::md;
using namespace trading::sim;
namespace fs = std::filesystem;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static constexpr uint32_t FEED_DIRECT = 1, FEED_SIP = 5, FEED_VENDOR = 7;
static constexpr int64_t MIDNIGHT = 1'757'894'400'000'000'000LL;
static constexpr int64_t T0 = MIDNIGHT + 9 * 3600 * 1'000'000'000LL;

int main(int argc, char** argv) {
    if (argc != 3) { std::fprintf(stderr, "usage: %s snapshot workdir\n", argv[0]); return 2; }
    const std::string work = argv[2];
    fs::remove_all(work);
    fs::create_directories(work);
    refdata::Snapshot snap(argv[1]);
    refdata::RefData rd; rd.load(snap);
    CHECK(rd.knownFeed(FEED_DIRECT) && rd.knownFeed(FEED_SIP) && rd.knownFeed(FEED_VENDOR));

    // ---- a day on the wire, packetised the way the arbitrator hands it over
    struct Pkt { std::vector<std::byte> bytes; uint64_t venueSeq; uint16_t count; };
    std::vector<Pkt> packets;
    std::vector<std::vector<std::byte>> frames;
    std::vector<uint64_t> frameSeq;
    {
        std::vector<std::vector<uint8_t>> wire;
        ItchSim venue(snap, 10, 20260915, MIDNIGHT);
        auto keep = [&](const uint8_t* m, size_t len) { wire.emplace_back(m, m + len); };
        venue.open(keep);
        for (int i = 0; i < 40'000; ++i) venue.step(keep);

        alignas(16) std::byte buf[MAX_PACKET];
        uint64_t seq = 1'000'000;
        size_t i = 0;
        while (i < wire.size()) {
            PacketBuilder pb(buf);
            pb.begin(FEED_DIRECT, 0, seq, MIDNIGHT);
            itch::BlockWriter bw(buf + sizeof(PacketHeader), MAX_PACKET - sizeof(PacketHeader));
            size_t n = 0;
            while (i < wire.size() && n < 8 && bw.add(wire[i].data(), wire[i].size())) { ++i; ++n; }
            if (n == 0) break;
            pb.header()->msgCount = uint16_t(n);
            pb.header()->payloadLen = uint16_t(bw.size());
            packets.push_back({std::vector<std::byte>(buf, buf + pb.size()), seq, uint16_t(n)});
            seq += n;
        }
        // and the frames those packets decode into, which is the second journal
        itch::Decoder::Params ip{};
        ip.feedId = FEED_DIRECT; ip.venueId = 2; ip.sourceId = 8; ip.sessionMidnightNs = MIDNIGHT;
        itch::Decoder dec(snap, ip);
        for (const Pkt& p : packets) {
            dec.decode(*reinterpret_cast<const PacketHeader*>(p.bytes.data()), 0, [&](const FrameHeader* f) {
                const auto* b = reinterpret_cast<const std::byte*>(f);
                frames.emplace_back(b, b + f->frameLength);
                frameSeq.push_back(identityVenueSeq(f));
            });
        }
        std::printf("day: %zu wire messages, %zu packets, %zu decoded frames, venue seq %llu..%llu\n",
                    wire.size(), packets.size(), frames.size(),
                    (unsigned long long)packets.front().venueSeq,
                    (unsigned long long)(packets.back().venueSeq + packets.back().count - 1));
        CHECK(!packets.empty() && !frames.empty());
    }
    const uint64_t firstSeq = packets.front().venueSeq;
    const uint64_t lastCovered = packets.back().venueSeq + packets.back().count - 1;

    // ---- 1, 2 and 3: write both journals, replay them back, and seek into them
    const std::string pdir = work + "/feed1-packets", fdir = work + "/feed1-frames";
    {
        FeedJournal pj(pdir, FEED_DIRECT, JournalKind::Packets, 20260915, 1u << 18);   // small segments on purpose, so rotation is exercised
        FeedJournal fj(fdir, FEED_DIRECT, JournalKind::Frames, 20260915, 1u << 18);
        int64_t t = T0;
        for (const Pkt& p : packets) { pj.append(p.venueSeq, p.count, t, 0, p.bytes.data(), uint32_t(p.bytes.size())); t += 2000; }
        for (size_t i = 0; i < frames.size(); ++i)
            fj.append(frameSeq[i], 1, T0, 0, frames[i].data(), uint32_t(frames[i].size()));
        pj.commit(true); fj.commit(true);
        std::printf("journals: packets %llu records in %zu segments / %llu bytes; frames %llu records in %zu segments\n",
                    (unsigned long long)pj.records(), pj.segments(), (unsigned long long)pj.sizeBytes(),
                    (unsigned long long)fj.records(), fj.segments());
        CHECK(pj.segments() > 3 && fj.segments() > 3);          // segment rotation really happened
        CHECK(pj.records() == packets.size() && fj.records() == frames.size());
        CHECK(pj.coveredTo() == lastCovered && pj.jumps() == 0);

        // exact from the start, byte for byte and in order
        size_t k = 0; bool same = true;
        const uint64_t n = pj.replay(0, 0, [&](const FeedJournalRecord& r, const std::byte* p) {
            if (k >= packets.size()) { same = false; return; }
            if (r.venueSeq != packets[k].venueSeq || r.count != packets[k].count) same = false;
            if (r.length - sizeof(FeedJournalRecord) != packets[k].bytes.size()) same = false;
            else if (std::memcmp(p, packets[k].bytes.data(), packets[k].bytes.size()) != 0) same = false;
            ++k;
        });
        std::printf("replay from the open: %llu records, byte-identical: %s, read %llu of %llu bytes\n",
                    (unsigned long long)n, same && k == packets.size() ? "yes" : "NO",
                    (unsigned long long)pj.bytesScanned(), (unsigned long long)pj.sizeBytes());
        CHECK(n == packets.size() && k == packets.size() && same);

        // a watermark three quarters of the way in: exact, and a seek rather than a scan
        const uint64_t mark = firstSeq + (lastCovered - firstSeq) * 3 / 4;
        size_t expect = 0;
        for (const Pkt& p : packets) if (p.venueSeq + p.count > mark) ++expect;
        uint64_t got = 0; bool ordered = true; uint64_t prev = 0;
        pj.replay(mark, 0, [&](const FeedJournalRecord& r, const std::byte* p) {
            ++got;
            if (r.venueSeq < prev) ordered = false;
            prev = r.venueSeq;
            const auto* h = reinterpret_cast<const PacketHeader*>(p);
            if (h->firstSeq != r.venueSeq || h->feedId != FEED_DIRECT) ordered = false;
        });
        const double read = double(pj.bytesScanned()) / double(pj.sizeBytes());
        std::printf("replay to a watermark at %llu: %llu records (wanted %zu), in order: %s, read %.1f%% of the journal\n",
                    (unsigned long long)mark, (unsigned long long)got, expect, ordered ? "yes" : "NO", read * 100.0);
        CHECK(got == expect && ordered);
        CHECK(read < 0.40);                                     // a seek, not a scan: a quarter plus one stride

        // and the frame journal, where many records share one venue sequence
        uint64_t fgot = 0;
        fj.replay(frameSeq[frames.size() / 2], 0, [&](const FeedJournalRecord& r, const std::byte* p) {
            ++fgot;
            const auto* f = reinterpret_cast<const FrameHeader*>(p);
            if (identityVenueSeq(f) != r.venueSeq) fgot = 0;     // the key has to be the frame's own identity
        });
        std::printf("frame journal: %llu frames at or after the midpoint of %zu, read %.1f%%\n",
                    (unsigned long long)fgot, frames.size(),
                    100.0 * double(fj.bytesScanned()) / double(fj.sizeBytes()));
        CHECK(fgot > 0 && fgot <= frames.size());
    }

    // ---- 4: a resynchronisation. The stream jumps, and that is a fact about the day.
    {
        const std::string jdir = work + "/jump";
        FeedJournal j(jdir, FEED_DIRECT, JournalKind::Packets, 20260915);
        alignas(16) std::byte buf[MAX_PACKET];
        auto put = [&](uint64_t s, uint16_t count, uint16_t flags) {
            PacketBuilder pb(buf);
            pb.begin(FEED_DIRECT, 0, s, MIDNIGHT);
            pb.header()->msgCount = count;
            j.append(s, count, T0, flags, buf, uint32_t(pb.size()));
        };
        put(100, 4, 0);
        put(104, 4, 0);
        put(200, 4, JournalFlags::fromRecovery);                // a snapshot put us here; 108..199 are gone
        put(204, 4, 0);
        uint64_t jumped = 0;
        j.replay(0, 0, [&](const FeedJournalRecord& r, const std::byte*) { if (r.flags & JournalFlags::afterJump) ++jumped; });
        std::printf("resynchronisation: %llu records, %llu jumps recorded, covered to %llu\n",
                    (unsigned long long)j.records(), (unsigned long long)j.jumps(), (unsigned long long)j.coveredTo());
        CHECK(j.records() == 4 && j.jumps() == 1 && jumped == 1);
        CHECK(j.coveredTo() == 207);
        bool threw = false;
        try { put(150, 4, 0); } catch (const std::logic_error&) { threw = true; }
        CHECK(threw);                                            // but backwards is not a fact, it is a bug
    }

    // ---- 5: a torn tail, and damage that is not a torn tail
    {
        const std::string tdir = work + "/torn";
        uint64_t before = 0;
        {
            FeedJournal j(tdir, FEED_DIRECT, JournalKind::Packets, 20260915);
            for (size_t i = 0; i < 64 && i < packets.size(); ++i)
                j.append(packets[i].venueSeq, packets[i].count, T0, 0, packets[i].bytes.data(), uint32_t(packets[i].bytes.size()));
            j.commit(true);
            before = j.records();
        }
        const std::string seg = tdir + "/seg-000000.mdj";
        const auto full = fs::file_size(seg);
        fs::resize_file(seg, full - 40);                         // the process died mid-record
        {
            FeedJournal j(tdir, FEED_DIRECT, JournalKind::Packets, 20260915);
            std::printf("torn tail: %llu records before, %llu after, %llu bytes truncated\n",
                        (unsigned long long)before, (unsigned long long)j.records(),
                        (unsigned long long)j.truncatedOnOpen());
            CHECK(j.truncatedOnOpen() > 0);
            CHECK(j.records() == before - 1);                    // exactly the torn one is gone
            // and it carries on from where it stopped
            const uint64_t next = j.coveredTo() + 1;
            alignas(16) std::byte buf[MAX_PACKET];
            PacketBuilder pb(buf);
            pb.begin(FEED_DIRECT, 0, next, MIDNIGHT);
            pb.header()->msgCount = 1;
            j.append(next, 1, T0, 0, buf, uint32_t(pb.size()));
            CHECK(j.records() == before);
        }
        // damage in the middle of the journal is a hole in the day, not a tail
        {
            const std::string ddir = work + "/damaged";
            {
                FeedJournal j(ddir, FEED_DIRECT, JournalKind::Packets, 20260915, 4096);
                for (size_t i = 0; i < 64 && i < packets.size(); ++i)
                    j.append(packets[i].venueSeq, packets[i].count, T0, 0, packets[i].bytes.data(), uint32_t(packets[i].bytes.size()));
                j.commit(true);
                CHECK(j.segments() > 2);
            }
            std::fstream f(ddir + "/seg-000001.mdj", std::ios::in | std::ios::out | std::ios::binary);
            f.seekp(std::streamoff(sizeof(FeedJournalHeader) + 4));
            const char junk[8] = {'\xff', '\xff', '\xff', '\xff', 0, 0, 0, 0};
            f.write(junk, sizeof junk);
            f.close();
            bool threw = false;
            try { FeedJournal j(ddir, FEED_DIRECT, JournalKind::Packets, 20260915, 4096); }
            catch (const std::exception& e) { threw = true; std::printf("damage inside the journal rejected: %s\n", e.what()); }
            CHECK(threw);
        }
    }

    // ---- 6, 7 and 8: deduplicating redundant publishers
    {
        SourceSelector::Params sp{}; sp.sourceId = 20;
        std::vector<MdSourceSwitch> switches;
        SourceSelector sel(rd, sp, [&](const FrameHeader* f) { switches.push_back(*as<MdSourceSwitch>(f)); });
        sel.addSource(FEED_DIRECT, 0);
        uint64_t out = 0, lastSeq = 0, lastIdx = 0, backwards = 0;
        std::vector<uint64_t> emitted;
        emitted.reserve(2'100'000);
        auto sink = [&](const FrameHeader* f) {
            ++out;
            const uint64_t vs = identityVenueSeq(f);
            emitted.push_back(vs);
            if (vs < lastSeq) ++backwards;
            else if (vs == lastSeq) { if (lastIdx == 0) ++lastIdx; }
            else { lastSeq = vs; lastIdx = 0; }
        };
        // The same stream from two publishers on two hosts. Publisher A is a message ahead half the
        // time, which is the point: whichever is first wins and the other is dropped.
        Frame<BookDelta> a{}, b{};
        uint64_t logical = 0, onTheWire = 0;
        int64_t t = T0;
        const size_t N = 1'000'000;
        for (size_t i = 0; i < N; ++i) {
            const uint64_t vs = 1'000'000 + i / 2;               // two messages share every venue sequence
            a.init(); a.header.streamId = FEED_DIRECT; a.header.sourceId = 8;
            a.body.symbolIdx = uint32_t(1 + i % 10); a.body.venueId = 2; a.body.venueSeq = vs;
            a.body.side = (i & 1) ? BookSide::Bid : BookSide::Ask; a.body.qty = int64_t(100 + i % 900);
            b = a; b.header.sourceId = 9;                        // the second publisher, same message
            ++logical; onTheWire += 2;
            if (i % 2 == 0) { sel.onMessage(&a.header, t, sink); sel.onMessage(&b.header, t, sink); }
            else            { sel.onMessage(&b.header, t, sink); sel.onMessage(&a.header, t, sink); }
            t += 100;
        }
        // now kill publisher A: B alone carries on, and nothing downstream can tell
        const uint64_t afterBoth = out;
        for (size_t i = 0; i < 1000; ++i) {
            const uint64_t vs = 1'000'000 + (N + i) / 2;
            b.init(); b.header.streamId = FEED_DIRECT; b.header.sourceId = 9;
            b.body.symbolIdx = 1; b.body.venueId = 2; b.body.venueSeq = vs; b.body.qty = 100;
            sel.onMessage(&b.header, t, sink);
            ++logical; ++onTheWire;
            t += 100;
        }
        // every venue sequence appears exactly twice in the output and never out of order
        uint64_t gaps = 0, wrongCount = 0;
        for (size_t i = 1; i < emitted.size(); ++i) {
            if (emitted[i] < emitted[i - 1]) ++backwards;
            if (emitted[i] > emitted[i - 1] + 1) ++gaps;
        }
        for (size_t i = 0; i + 1 < emitted.size(); i += 2)
            if (emitted[i] != emitted[i + 1]) ++wrongCount;
        std::printf("two publishers: %llu distinct messages arriving %llu times, %llu forwarded, "
                    "%llu dropped as the second copy; after A was killed: %llu more, still exactly once; "
                    "out of order %llu, gaps %llu\n",
                    (unsigned long long)logical, (unsigned long long)onTheWire, (unsigned long long)out,
                    (unsigned long long)sel.duplicates(), (unsigned long long)(out - afterBoth),
                    (unsigned long long)backwards, (unsigned long long)gaps);
        CHECK(logical > 1'000'000 && onTheWire == logical + 1'000'000);
        CHECK(out == logical);                                    // every message exactly once
        CHECK(sel.duplicates() == onTheWire - logical);           // and every second copy dropped
        CHECK(out - afterBoth == 1000);                           // A dying cost nothing
        CHECK(backwards == 0 && gaps == 0);
        CHECK(wrongCount == 0);                                   // both messages of a sequence survived
        CHECK(switches.size() == 1 && switches[0].toFeedId == FEED_DIRECT);
        CHECK(switches[0].reason == switchReason(MdSwitchReason::Initial));
        CHECK(sel.quality() == MdQuality::Depth);
    }

    // ---- 9, 10 and 11: choosing between sources that are not the same
    struct Marks { uint32_t first, afterStall, halfRecovered, fullyRecovered, afterDown; MdQuality stallQuality; };
    auto sourceDrill = [&](std::vector<MdSourceSwitch>& out) {
        SourceSelector::Params sp{};
        sp.sourceId = 20;
        sp.staleAfterNs = 200'000'000;
        sp.recoveringGraceNs = 50'000'000;
        sp.failBackAfterNs = 1'000'000'000;
        SourceSelector sel(rd, sp, [&](const FrameHeader* f) { out.push_back(*as<MdSourceSwitch>(f)); });
        sel.addSource(FEED_DIRECT, 0);      // depth
        sel.addSource(FEED_SIP, 1);         // the official top of book
        sel.addSource(FEED_VENDOR, 2);      // and the long tail
        auto data = [](const FrameHeader*) {};
        Frame<BookDelta> d{};
        uint64_t seq = 1'000'000;
        int64_t t = T0;
        auto feed = [&](uint32_t feedId, uint16_t src) {
            d.init(); d.header.streamId = feedId; d.header.sourceId = src;
            d.body.symbolIdx = 1; d.body.venueId = 2; d.body.venueSeq = ++seq; d.body.qty = 100;
            sel.onMessage(&d.header, t, data);
        };
        Marks m{};
        for (int i = 0; i < 20; ++i) { feed(FEED_DIRECT, 8); feed(FEED_SIP, 9); feed(FEED_VENDOR, 10); t += 1'000'000; }
        m.first = sel.selected();
        // the direct feed stalls: only the tape and the vendor keep talking
        for (int i = 0; i < 400; ++i) { feed(FEED_SIP, 9); feed(FEED_VENDOR, 10); sel.tick(t); t += 1'000'000; }
        m.afterStall = sel.selected();
        m.stallQuality = sel.quality();
        // it comes back, and must hold up for a second before we follow it
        for (int i = 0; i < 500; ++i) { feed(FEED_DIRECT, 8); feed(FEED_SIP, 9); sel.tick(t); t += 1'000'000; }
        m.halfRecovered = sel.selected();
        for (int i = 0; i < 1200; ++i) { feed(FEED_DIRECT, 8); feed(FEED_SIP, 9); sel.tick(t); t += 1'000'000; }
        m.fullyRecovered = sel.selected();
        // and the arbitrator declaring it down moves us at once, without waiting for silence
        FeedStatus fs{}; fs.feedId = FEED_DIRECT; fs.state = FeedState::Down; fs.quality = MdQuality::NoData;
        sel.onFeedStatus(fs, t);
        m.afterDown = sel.selected();
        return m;
    };
    {
        std::vector<MdSourceSwitch> got;
        const Marks m = sourceDrill(got);
        std::printf("source policy: first %u, after the direct stalls %u (quality %u), half a second later %u, "
                    "after it has held up %u, after it is declared down %u; %zu switches\n",
                    m.first, m.afterStall, unsigned(m.stallQuality), m.halfRecovered, m.fullyRecovered,
                    m.afterDown, got.size());
        CHECK(m.first == FEED_DIRECT);                            // depth wins when it is there
        CHECK(m.afterStall == FEED_SIP);                          // and the tape takes over when it is not
        CHECK(m.stallQuality == MdQuality::TopOfBook);            // and the consumer is told what it now has
        CHECK(m.halfRecovered == FEED_SIP);                       // no flapping back the moment it returns
        CHECK(m.fullyRecovered == FEED_DIRECT);                   // but it does return once it has held up
        CHECK(m.afterDown == FEED_SIP);                           // and Down moves us without waiting for silence
        CHECK(got.size() == 4);
        CHECK(got[0].reason == switchReason(MdSwitchReason::Initial));
        CHECK(got[1].reason == switchReason(MdSwitchReason::SourceStale) && got[1].fromFeedId == FEED_DIRECT);
        CHECK(got[2].reason == switchReason(MdSwitchReason::Recovered) && got[2].toFeedId == FEED_DIRECT);
        CHECK(got[3].reason == switchReason(MdSwitchReason::SourceDown));
        CHECK(got[1].quality == MdQuality::TopOfBook && got[2].quality == MdQuality::Depth);

        // 11: the same day twice, the same switches. A consumer replaying the log has to make the
        // decisions the live system made, and a source switch is one of them.
        std::vector<MdSourceSwitch> again;
        const Marks m2 = sourceDrill(again);
        CHECK(std::memcmp(&m, &m2, sizeof(Marks)) == 0);
        CHECK(again.size() == got.size());
        for (size_t i = 0; i < got.size(); ++i) CHECK(std::memcmp(&got[i], &again[i], sizeof(MdSourceSwitch)) == 0);
        std::printf("determinism: %zu switches, identical across runs\n", again.size());
    }

    std::printf("feed journal and source selector tests ok\n");
    return 0;
}
