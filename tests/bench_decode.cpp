// Decoder benchmarks. The decoder is the busiest component in the system: one call per message on
// every feed, all day, and on a busy open Nasdaq alone runs into the millions per second. Document
// 3 sets the bar at three million messages per second per feed process and this fails below it,
// because a decoder that cannot keep up does not fall behind gracefully - it falls behind until the
// arbitrator's reorder buffer bursts and the feed is declared down.
//
// It also fails if the decode path allocates. The order reference table and the level table are
// sized at construction; a rehash during the day would be a configuration error, and finding one
// here rather than at the open is the point of the guard.
#define TRADING_COUNT_ALLOCS
#include "alloc_guard.hpp"
#include "bench.hpp"
#include "itch.hpp"
#include "sip.hpp"
#include "selector.hpp"
#include "feed_journal.hpp"
#include "book.hpp"
#include "itch_sim.hpp"
#include <filesystem>
#include <vector>

using namespace trading; using namespace trading::md; using namespace trading::sim;
using namespace trading::bench; using trading::util::AllocScope;

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "usage: %s <snapshot>\n", argv[0]); return 2; }
    refdata::Snapshot snap(argv[1]);
    const int64_t MIDNIGHT = 1'757'894'400'000'000'000LL;
    const size_t B = 64;

    // A day on the wire, generated once. Every row below reads these same bytes.
    std::vector<uint8_t> flat;
    std::vector<std::pair<uint32_t, uint16_t>> at;      // offset, length
    {
        ItchSim venue(snap, 10, 20260915, MIDNIGHT);
        auto keep = [&](const uint8_t* m, size_t len) {
            at.emplace_back(uint32_t(flat.size()), uint16_t(len));
            flat.insert(flat.end(), m, m + len);
        };
        venue.open(keep);
        for (int i = 0; i < 400'000; ++i) venue.step(keep);
        std::fprintf(stderr, "wire: %zu messages, %zu bytes\n", at.size(), flat.size());
    }

    uint64_t frames = 0;
    auto sink = [&](const FrameHeader*) { ++frames; };

    // 1. ITCH, message by message: the number that has to clear three million a second.
    {
        itch::Decoder::Params p{};
        p.feedId = 1; p.venueId = 2; p.sourceId = 8; p.sessionMidnightNs = MIDNIGHT;
        itch::Decoder d(snap, p);
        uint64_t seq = 1'000'000;
        // A quarter of the day as warm-up. The order reference table is sized for production and is
        // tens of megabytes; its first touch is a page fault per page, paid once in the opening
        // minutes of a real session, and letting that land in the measured region would report a
        // startup cost as a per-message one.
        const size_t warm = at.size() / 4;
        for (size_t i = 0; i < warm; ++i) d.decodeMessage(flat.data() + at[i].first, at[i].second, ++seq, false, sink);
        Recorder r(B, at.size() / B); AllocScope a;
        for (size_t i = warm; i + B <= at.size(); i += B) {
            r.begin();
            for (size_t k = 0; k < B; ++k) d.decodeMessage(flat.data() + at[i + k].first, at[i + k].second, ++seq, false, sink);
            r.end();
        }
        const uint64_t al = a.delta();
        const Percentiles pc = r.finish();
        report("md.itch_decode", pc, al, "one ITCH message: parse, order table, level aggregate, emit");
        // Reported, not gated. It is 1/mean of the row above, and mean on an unpinned host swings
        // by more than any sensible gate; the bar itself is the hard check below, which does not
        // depend on a baseline at all.
        const double perSec = 1e9 / pc.mean;
        reportValue("md.itch_messages_per_sec", "msg/s", perSec, "document 3 sets the bar at three million per feed process; the benchmark fails below it");
        if (al) { std::fprintf(stderr, "FAIL: itch decode allocated %llu times after warm-up\n", (unsigned long long)al); return 1; }
        if (perSec < 3.0e6) {
            std::fprintf(stderr, "FAIL: itch decode at %.2f M messages/s, below the 3 M bar\n", perSec / 1e6);
            return 1;
        }
        std::fprintf(stderr, "itch: %llu frames out, %zu orders and %zu levels live at the end\n",
                     (unsigned long long)frames, d.liveOrders(), d.liveLevels());
    }

    // 2. the same day in packets, which is what the arbitrator actually hands over
    {
        itch::Decoder::Params p{};
        p.feedId = 1; p.venueId = 2; p.sourceId = 8; p.sessionMidnightNs = MIDNIGHT;
        itch::Decoder d(snap, p);
        std::vector<std::vector<std::byte>> packets;
        {
            size_t i = 0; uint64_t seq = 1'000'000;
            alignas(16) std::byte buf[MAX_PACKET];
            while (i < at.size()) {
                PacketBuilder pb(buf);
                pb.begin(1, 0, seq + 1, MIDNIGHT);
                itch::BlockWriter bw(buf + sizeof(PacketHeader), MAX_PACKET - sizeof(PacketHeader));
                size_t n = 0;
                while (i < at.size() && n < 8 && bw.add(flat.data() + at[i].first, at[i].second)) { ++i; ++n; }
                if (n == 0) break;
                pb.header()->msgCount = uint16_t(n);
                pb.header()->payloadLen = uint16_t(bw.size());
                packets.emplace_back(buf, buf + pb.size());
                seq += n;
            }
        }
        const size_t warm = packets.size() / 4;
        for (size_t i = 0; i < warm; ++i) d.decode(*reinterpret_cast<const PacketHeader*>(packets[i].data()), 0, sink);
        Recorder r(8, packets.size()); AllocScope a;       // one packet a block, eight messages to a packet
        for (size_t i = warm; i < packets.size(); ++i) {
            r.begin();
            d.decode(*reinterpret_cast<const PacketHeader*>(packets[i].data()), 0, sink);
            r.end();
        }
        const uint64_t al = a.delta();
        report("md.itch_decode_packet", r.finish(), al, "per message, decoded eight to a packet as the arbitrator delivers them");
        if (al) { std::fprintf(stderr, "FAIL: packet decode allocated %llu times\n", (unsigned long long)al); return 1; }
        std::fprintf(stderr, "packets: %zu\n", packets.size());
    }

    // 3. the consolidated tape, which is slower per message and far smaller in volume
    {
        sip::Decoder::Params p{};
        p.feedId = 5; p.sourceId = 9; p.sessionMidnightNs = MIDNIGHT;
        sip::Decoder d(snap, p);
        d.mapParticipant('Q', 2);
        std::string ticker;
        for (const SymbolRecord& r : snap.instruments()) if (r.symbolIdx == 1) ticker.assign(r.ticker, ::strnlen(r.ticker, 16));
        uint8_t msg[sip::MAX_MESSAGE];
        sip::Writer w(msg);
        w.header(sip::category::Quote, sip::type::ShortQuote, 'Q', 1, 9ull * 3600 * 1'000'000'000, ticker);
        const size_t len = w.quote(22'00000000LL, 500, 22'01000000LL, 300,
                                   22'00000000LL, 500, 22'01000000LL, 300, 'Q', 'Q', sip::QuoteCondition::Normal, true);
        const size_t N = 200'000;
        uint64_t seq = 5'000'000;
        for (size_t i = 0; i < N / 10; ++i) d.decodeMessage(msg, len, ++seq, false, sink);
        Recorder r(B, N / B); AllocScope a;
        for (size_t i = 0; i + B <= N; i += B) {
            r.begin();
            for (size_t k = 0; k < B; ++k) d.decodeMessage(msg, len, ++seq, false, sink);
            r.end();
        }
        const uint64_t al = a.delta();
        report("md.sip_decode", r.finish(), al, "one SIP quote: header, symbol lookup, the national best");
        if (al) { std::fprintf(stderr, "FAIL: sip decode allocated %llu times\n", (unsigned long long)al); return 1; }
    }

    // 4. the divergence monitor, which sees every update from both sides. This is its worst case
    // rather than its typical one: the direct feed always moves first, so every pair opens an
    // episode on the direct update and closes it on the tape's, and the open and close paths are
    // both in the number.
    {
        sip::NbboMonitor::Params p{};
        p.feedId = 5; p.sourceId = 9; p.minEpisodeNs = 1'000'000; p.alertAfterNs = 50'000'000; p.staleAfterNs = 2'000'000'000;
        sip::NbboMonitor mon(16, p);
        uint64_t records = 0;
        auto out = [&](const FrameHeader*) { ++records; };
        const size_t N = 400'000;
        int64_t t = MIDNIGHT;
        uint64_t seq = 0;
        auto pair = [&](size_t i) {
            const uint32_t sym = uint32_t(1 + i % 10);
            const int64_t bid = 22'00000000LL + int64_t(i % 7) * 1'000'000;
            mon.onDirect(sym, bid, bid + 1'000'000, t, ++seq, out);
            mon.onSip(sym, bid, bid + 1'000'000, t, ++seq, out);
            t += 1000;
        };
        for (size_t i = 0; i < N / 10; ++i) pair(i);
        Recorder r(B * 2, N / B); AllocScope a;             // two updates a round
        for (size_t i = 0; i + B <= N; i += B) { r.begin(); for (size_t k = 0; k < B; ++k) pair(i + k); r.end(); }
        const uint64_t al = a.delta();
        report("md.nbbo_divergence", r.finish(), al, "per update; worst case, an episode opened and closed on every pair");
        if (al) { std::fprintf(stderr, "FAIL: divergence monitor allocated %llu times\n", (unsigned long long)al); return 1; }
        std::fprintf(stderr, "divergence: %llu episodes opened and closed, %llu long enough to record\n",
                     (unsigned long long)mon.episodes(), (unsigned long long)records);
    }

    // 5. the source selector, in its steady state: two publishers of one feed, so half of what it
    // sees is the second copy of something it has already forwarded. That is the shape of a normal
    // day with redundancy on, and it is one call per message on top of the decoder.
    {
        refdata::RefData rd; rd.load(snap);
        SourceSelector::Params sp{}; sp.sourceId = 20;
        SourceSelector sel(rd, sp, [](const FrameHeader*) {});
        sel.addSource(1, 0);
        uint64_t out = 0;
        auto pass = [&](const FrameHeader*) { ++out; };
        Frame<BookDelta> a{}, b{};
        a.init(); a.header.streamId = 1; a.header.sourceId = 8;
        a.body.symbolIdx = 1; a.body.venueId = 2; a.body.qty = 100;
        b = a; b.header.sourceId = 9;
        const size_t N = 1'000'000;
        uint64_t vs = 1'000'000;
        auto both = [&](int64_t at) {
            a.body.venueSeq = b.body.venueSeq = ++vs;
            sel.onMessage(&a.header, at, pass);
            sel.onMessage(&b.header, at, pass);
        };
        for (size_t i = 0; i < N / 10; ++i) both(int64_t(i) * 100);
        Recorder r(B * 2, N / B); AllocScope al;              // two arrivals per distinct message
        int64_t t = int64_t(N) * 100;
        for (size_t i = 0; i + B <= N; i += B) { r.begin(); for (size_t k = 0; k < B; ++k) both(t + int64_t(i + k) * 100); r.end(); }
        const uint64_t alloc = al.delta();
        report("md.selector_dedupe", r.finish(), alloc, "per arrival with two publishers: identity compared, one forwarded and one dropped");
        if (alloc) { std::fprintf(stderr, "FAIL: selector allocated %llu times\n", (unsigned long long)alloc); return 1; }
        std::fprintf(stderr, "selector: %llu forwarded, %llu dropped as the second copy\n",
                     (unsigned long long)out, (unsigned long long)sel.duplicates());
    }

    // 6. writing the arbitrated packet stream to its journal, which happens once per packet on
    // every feed and is the only thing in the market-data path that touches a disk.
    {
        const std::string dir = "build/mdjbench";
        std::filesystem::remove_all(dir);
        alignas(16) std::byte pkt[MAX_PACKET];
        PacketBuilder pb(pkt);
        pb.begin(1, 0, 1'000'000, MIDNIGHT);
        for (int i = 0; i < 4; ++i) {
            Frame<BookDelta> f; f.init(); f.header.streamId = 1;
            f.body.symbolIdx = uint32_t(1 + i); f.body.venueId = 2; f.body.qty = 500;
            f.body.venueSeq = 1'000'000 + uint64_t(i);
            pb.add(&f.header);
        }
        const uint32_t len = uint32_t(pb.size());
        FeedJournal j(dir, 1, JournalKind::Packets, 20260915);
        uint64_t seq = 1'000'000;
        const size_t N = 200'000;
        for (size_t i = 0; i < N / 10; ++i) { pb.header()->firstSeq = seq; j.append(seq, 4, MIDNIGHT, 0, pkt, len); seq += 4; }
        Recorder r(B, N / B); AllocScope al;
        for (size_t i = 0; i + B <= N; i += B) {
            r.begin();
            for (size_t k = 0; k < B; ++k) { pb.header()->firstSeq = seq; j.append(seq, 4, MIDNIGHT, 0, pkt, len); seq += 4; }
            r.end();
        }
        const uint64_t alloc = al.delta();
        report("md.feed_journal_append", r.finish(), alloc, "one arbitrated packet appended to the per-feed journal, buffered");
        if (alloc) { std::fprintf(stderr, "FAIL: journal append allocated %llu times\n", (unsigned long long)alloc); return 1; }
        j.commit(true);

        // and the seek the done-when is about: replaying from near the end must read the tail, not
        // the journal. Reported as the fraction actually read.
        const uint64_t mark = j.coveredTo() - (j.coveredTo() - j.firstVenueSeq()) / 10;
        uint64_t got = 0;
        const int64_t t0 = nowNs();
        j.replay(mark, 0, [&](const FeedJournalRecord&, const std::byte*) { ++got; });
        const int64_t t1 = nowNs();
        Percentiles p;
        p.p50 = p.p99 = p.p999 = p.max = p.mean = double(t1 - t0) / double(got ? got : 1);
        p.samples = 1;
        report("md.feed_journal_replay", p, 0, "mean per record replaying the last tenth of the journal");
        reportValue("md.feed_journal_seek_fraction", "percent", 100.0 * double(j.bytesScanned()) / double(j.sizeBytes()),
                    "of the journal read to replay its last tenth; a scan would be 100");
        std::fprintf(stderr, "journal: %llu records, %llu bytes, replayed %llu reading %.1f%%\n",
                     (unsigned long long)j.records(), (unsigned long long)j.sizeBytes(), (unsigned long long)got,
                     100.0 * double(j.bytesScanned()) / double(j.sizeBytes()));
        std::filesystem::remove_all(dir);
    }

    // 7. packet to published top of book, which is the figure document 3 puts a number on: a
    // packet off the wire, decoded, applied to the ladders, and the consolidated top published
    // into shared memory where the router will read it. Under two microseconds at the median.
    {
        refdata::RefData rd; rd.load(snap);
        std::vector<std::vector<std::byte>> packets;
        {
            size_t i = 0; uint64_t seq = 1'000'000;
            alignas(16) std::byte buf[MAX_PACKET];
            while (i < at.size()) {
                PacketBuilder pb(buf);
                pb.begin(1, 0, seq + 1, MIDNIGHT);
                itch::BlockWriter bw(buf + sizeof(PacketHeader), MAX_PACKET - sizeof(PacketHeader));
                size_t n = 0;
                while (i < at.size() && n < 8 && bw.add(flat.data() + at[i].first, at[i].second)) { ++i; ++n; }
                if (n == 0) break;
                pb.header()->msgCount = uint16_t(n);
                pb.header()->payloadLen = uint16_t(bw.size());
                packets.emplace_back(buf, buf + pb.size());
                seq += n;
            }
        }
        const std::string segPath = "build/bookbench.seg";
        std::filesystem::remove(segPath);
        BookSegment seg(segPath, 10, 4, 20260915, true);
        BookBuilder::Params bp{};
        bp.sourceId = 12; bp.feedId = 1; bp.maxSymbolIdx = 10; bp.maxVenueId = 4;
        BookBuilder book(rd, bp, seg.base());
        itch::Decoder::Params ip{};
        ip.feedId = 1; ip.venueId = 2; ip.sourceId = 8; ip.sessionMidnightNs = MIDNIGHT;
        itch::Decoder dec(snap, ip);
        uint64_t published = 0;
        int64_t now = MIDNIGHT;
        auto out = [&](const FrameHeader*) { ++published; };
        auto one = [&](const std::vector<std::byte>& p) {
            dec.decode(*reinterpret_cast<const PacketHeader*>(p.data()), 0,
                       [&](const FrameHeader* f) { book.apply(f, now, out); });
            now += 2000;
        };
        const size_t warm = packets.size() / 4;
        for (size_t i = 0; i < warm; ++i) one(packets[i]);
        Recorder r(1, packets.size()); AllocScope a;          // one packet a block
        for (size_t i = warm; i < packets.size(); ++i) { r.begin(); one(packets[i]); r.end(); }
        const uint64_t alloc = a.delta();
        const Percentiles pc = r.finish();
        report("md.packet_to_published", pc, alloc, "a packet off the wire decoded, applied to the ladders and published to shared memory");
        if (alloc) { std::fprintf(stderr, "FAIL: the packet path allocated %llu times\n", (unsigned long long)alloc); return 1; }
        if (pc.p50 > 2000.0) {
            std::fprintf(stderr, "FAIL: packet to published %.0f ns at p50, over the 2 us bar\n", pc.p50);
            return 1;
        }
        report("md.book_apply", Percentiles{pc.p50 / 8, pc.p99 / 8, pc.p999 / 8, pc.max / 8, pc.mean / 8, pc.samples},
               0, "the same divided by the eight messages a packet carries");
        std::fprintf(stderr, "book: %zu packets, %llu top-of-book messages published, %llu deltas, %llu rebases\n",
                     packets.size(), (unsigned long long)published, (unsigned long long)book.deltas(),
                     (unsigned long long)book.rebases());

        // 8. and what it costs the router to read one, which it does on every order
        {
            BookReader rdr = book.reader();
            TopBody t{};
            uint64_t good = 0;
            const size_t N = 1'000'000;
            for (size_t i = 0; i < N / 10; ++i) good += rdr.top(uint32_t(1 + i % 10), t);
            Recorder rr(B, N / B); AllocScope aa;
            for (size_t i = 0; i + B <= N; i += B) {
                rr.begin();
                for (size_t k = 0; k < B; ++k) good += rdr.top(uint32_t(1 + (i + k) % 10), t);
                rr.end();
            }
            const uint64_t al2 = aa.delta();
            report("md.book_read_top", rr.finish(), al2, "a reader copying the top of book out of shared memory and checking it");
            if (al2) { std::fprintf(stderr, "FAIL: the read path allocated %llu times\n", (unsigned long long)al2); return 1; }
            std::fprintf(stderr, "reads: %llu consistent\n", (unsigned long long)good);
        }
        std::filesystem::remove(segPath);
    }

    std::fprintf(stderr, "sink saw %llu frames\n", (unsigned long long)frames);
    return 0;
}
