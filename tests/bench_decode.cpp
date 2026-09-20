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
#include "itch_sim.hpp"
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

    std::fprintf(stderr, "sink saw %llu frames\n", (unsigned long long)frames);
    return 0;
}
