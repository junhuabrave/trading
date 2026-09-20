// tests/test_decode.cpp <snapshot> <workdir> [--golden FILE] [--write-golden FILE]
//
// The decoders: Nasdaq TotalView-ITCH 5.0 (MD-4) and the consolidated tape (MD-5).
//
// A decoder is the one component that can be wrong in a way nothing downstream can detect, because
// everything downstream believes it. So the drills here do not ask whether it produced output; they
// ask whether the book it produced is the book the venue described.
//
//   1. every ITCH message type on the wire, at the published length, decoded
//   2. the book invariant: the levels the decoder built one delta at a time equal the levels you
//      get by aggregating the orders the venue actually left live. Two computations, one answer.
//   3. the order reference table matches the venue's own live set exactly
//   4. packets: skip is honoured, and the last message of each packet is marked endOfPacket
//   5. a decoder handed the same bytes twice produces byte-identical output, and that output's
//      hash is held against a stored golden
//   6. malformed input: a truncated block, an unknown type, a wrong length, an execution for an
//      order never added, a locate we do not carry. All counted, none fatal, none silent.
//   7. the SIP: quotes with the national best, short-form round lots, prints and trading actions
//   8. divergence: a tape that is merely late is not news; one that is wrong, crossed or stopped is
//
// What is NOT proved here is MD-4's done-when in full. That asks for a golden run against a
// published Nasdaq sample day, and this generates its own. The wire format is the real one - the
// offsets and lengths below are the published ones and the decoder reads nothing else - but a real
// day carries orderings no generator thinks to produce. Running one through --golden is the
// remaining step, and it needs no code change.
#include "itch.hpp"
#include "sip.hpp"
#include "itch_sim.hpp"
#include "sip_sim.hpp"
#include "blake3.h"
#include <cstdio>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <regex>
#include <string>
#include <vector>

using namespace trading;
using namespace trading::md;
using namespace trading::sim;
namespace fs = std::filesystem;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static constexpr uint32_t FEED_ITCH = 1, FEED_SIP = 5;
static constexpr uint16_t VENUE_XNAS = 2;
static constexpr int64_t MIDNIGHT = 1'757'894'400'000'000'000LL;   // the session date, 00:00

static std::string hex(const std::array<uint8_t, 32>& h) {
    static const char* d = "0123456789abcdef";
    std::string s;
    for (int i = 0; i < 8; ++i) { s += d[h[i] >> 4]; s += d[h[i] & 15]; }
    return s;
}

// Everything a decoder emitted, hashed and counted.
//
// The hash skips the frame header's schemaVersion and covers everything else. That field moves on
// every schema bump, including bumps with nothing to do with market data, and a golden that moves
// then cannot tell a decoder change from a version stamp - which is the only question it exists to
// answer. The version is recorded in the golden as its own field instead, so the bump is still
// visible and still checked, and a changed hash means the decode changed.
struct Sink {
    blake3_hasher h;
    uint64_t frames = 0;
    std::map<uint16_t, uint64_t> byTemplate;
    uint64_t endOfPacket = 0;
    std::vector<std::byte> raw;
    bool keepRaw = false;

    Sink() { blake3_hasher_init(&h); }
    void operator()(const FrameHeader* f) {
        ++frames;
        ++byTemplate[f->templateId];
        if (f->flags & FrameFlags::endOfPacket) ++endOfPacket;
        const auto* p = reinterpret_cast<const std::byte*>(f);
        static_assert(offsetof(FrameHeader, schemaVersion) == 6);
        blake3_hasher_update(&h, p, 6);
        blake3_hasher_update(&h, p + 8, f->frameLength - 8);
        if (keepRaw) raw.insert(raw.end(), p, p + f->frameLength);
    }
    std::array<uint8_t, 32> digest() {
        std::array<uint8_t, 32> out{};
        blake3_hasher_finalize(&h, out.data(), 32);
        return out;
    }
};

// The best bid and offer, kept the plain way, because the decoder's answer has to be checked
// against something that is not the decoder. This is the book builder's job properly (MD-8).
struct Best {
    std::map<uint32_t, std::map<int64_t, int64_t, std::greater<>>> bids;   // symbol -> price desc -> qty
    std::map<uint32_t, std::map<int64_t, int64_t>> asks;
    void apply(const BookDelta& d) {
        if (d.side == BookSide::Bid) {
            if (d.action == BookAction::Delete || d.qty == 0) bids[d.symbolIdx].erase(d.price);
            else bids[d.symbolIdx][d.price] = d.qty;
        } else {
            if (d.action == BookAction::Delete || d.qty == 0) asks[d.symbolIdx].erase(d.price);
            else asks[d.symbolIdx][d.price] = d.qty;
        }
    }
    std::pair<int64_t, int64_t> bid(uint32_t s) const {
        auto it = bids.find(s);
        if (it == bids.end() || it->second.empty()) return {0, 0};
        return {it->second.begin()->first, it->second.begin()->second};
    }
    std::pair<int64_t, int64_t> ask(uint32_t s) const {
        auto it = asks.find(s);
        if (it == asks.end() || it->second.empty()) return {0, 0};
        return {it->second.begin()->first, it->second.begin()->second};
    }
};

static std::string jsonField(const std::string& j, const char* key) {
    std::regex re("\"" + std::string(key) + "\"\\s*:\\s*\"?([^\",}\\n]+)\"?");
    std::smatch m;
    return std::regex_search(j, m, re) ? m[1].str() : "";
}

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "usage: %s snapshot workdir [--golden F] [--write-golden F]\n", argv[0]); return 2; }
    fs::create_directories(argv[2]);
    std::string golden, writeGolden;
    for (int i = 3; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--golden" && i + 1 < argc) golden = argv[++i];
        else if (a == "--write-golden" && i + 1 < argc) writeGolden = argv[++i];
    }
    refdata::Snapshot snap(argv[1]);
    refdata::RefData rd; rd.load(snap);

    // ---- generate a day on the wire, once, and keep the bytes. Every drill below reads these
    // same bytes, so a difference in outcome is a difference in the decoder.
    std::vector<std::vector<uint8_t>> wire;
    size_t liveAtEnd = 0;
    std::vector<ItchSim::Live> venueOrders;
    {
        ItchSim venue(snap, 10, 20260915, MIDNIGHT);
        auto keep = [&](const uint8_t* m, size_t len) { wire.emplace_back(m, m + len); };
        venue.open(keep);
        for (int i = 0; i < 200'000; ++i) venue.step(keep);
        venue.close(keep);
        venueOrders = venue.live();
        liveAtEnd = venueOrders.size();
        std::printf("itch day: %llu messages on the wire, %zu symbols, %zu orders still live at the close\n",
                    (unsigned long long)venue.sent(), venue.symbols(), liveAtEnd);
        CHECK(venue.sent() == wire.size());
    }

    // ---- 1 and 2: decode it, and check the book against the venue's own live orders
    Sink sink;
    Best best;
    itch::Decoder::Params ip{};
    ip.feedId = FEED_ITCH; ip.venueId = VENUE_XNAS; ip.sourceId = 8; ip.sessionMidnightNs = MIDNIGHT;
    itch::Decoder dec(snap, ip);
    {
        uint64_t seq = 1'000'000;
        auto emit = [&](const FrameHeader* f) {
            sink(f);
            if (const auto* d = as<BookDelta>(f)) best.apply(*d);
        };
        for (const auto& m : wire) dec.decodeMessage(m.data(), m.size(), ++seq, false, emit);
        dec.finish(emit);
        std::printf("decoded: %llu itch messages -> %llu frames (%llu deltas, %llu trades, %llu statuses, "
                    "%llu imbalances, %llu ignored)\n",
                    (unsigned long long)dec.messages(), (unsigned long long)dec.emitted(),
                    (unsigned long long)dec.deltas(), (unsigned long long)dec.trades(),
                    (unsigned long long)dec.statuses(), (unsigned long long)dec.imbalances(),
                    (unsigned long long)dec.ignored());
        CHECK(dec.messages() == wire.size());
        CHECK(dec.malformed() == 0 && dec.unknownType() == 0);
        CHECK(dec.negativeLevel() == 0);            // a level driven below zero is a decoder defect
        CHECK(dec.missingOrder() == 0);             // the venue only ever referred to orders it added
        CHECK(dec.unmappedLocate() == 0);           // and only to symbols the snapshot carries
        CHECK(dec.deltas() > 0 && dec.trades() > 0 && dec.statuses() > 0 && dec.imbalances() > 0 && dec.ignored() > 0);
        CHECK(dec.directoryEntries() == 10 && dec.regSho() > 0);
    }
    // the invariant: aggregate the venue's live orders into levels and demand the same answer
    {
        std::map<std::tuple<uint32_t, uint8_t, int64_t>, std::pair<int64_t, uint32_t>> want;
        for (const ItchSim::Live& o : venueOrders) {
            auto& e = want[{o.symbolIdx, o.side, o.price}];
            e.first += o.shares;
            e.second += 1;
        }
        size_t got = 0, mismatched = 0;
        dec.forEachLevel([&](uint32_t sym, BookSide side, int64_t px, int64_t qty, uint32_t count) {
            ++got;
            auto it = want.find({sym, uint8_t(side), px});
            if (it == want.end() || it->second.first != qty || it->second.second != count) ++mismatched;
        });
        std::printf("book invariant: %zu levels from %zu live orders, decoder holds %zu levels and %zu orders, "
                    "mismatched: %zu\n", want.size(), liveAtEnd, got, dec.liveOrders(), mismatched);
        CHECK(!want.empty());
        CHECK(dec.liveOrders() == liveAtEnd);       // 3: the order table is the venue's live set
        CHECK(got == want.size());
        CHECK(mismatched == 0);
    }

    // ---- 4: the same day through packets, with a skip the arbitrator would have applied
    {
        Sink packed;
        itch::Decoder d2(snap, ip);
        alignas(16) std::byte pkt[MAX_PACKET];
        auto emit = [&](const FrameHeader* f) { packed(f); };
        uint64_t seq = 1'000'000;
        size_t i = 0, packets = 0, skipped = 0;
        while (i < wire.size()) {
            PacketBuilder pb(pkt);
            pb.begin(FEED_ITCH, 0, seq + 1, MIDNIGHT);
            itch::BlockWriter bw(pkt + sizeof(PacketHeader), MAX_PACKET - sizeof(PacketHeader));
            size_t n = 0;
            while (i < wire.size() && n < 8 && bw.add(wire[i].data(), wire[i].size())) { ++i; ++n; }
            if (n == 0) break;
            pb.header()->msgCount = uint16_t(n);
            pb.header()->payloadLen = uint16_t(bw.size());
            // Every fifth packet is one the arbitrator says we have already seen the front of.
            const uint16_t skip = (packets % 5 == 0 && n > 2) ? 2 : 0;
            skipped += skip;
            d2.decode(*pb.header(), skip, emit);
            seq += n;
            ++packets;
        }
        std::printf("packets: %zu packets, %zu leading messages skipped, %llu frames, %llu marked end of packet\n",
                    packets, skipped, (unsigned long long)packed.frames, (unsigned long long)packed.endOfPacket);
        CHECK(packets > 0 && skipped > 0);
        CHECK(d2.messages() == wire.size() - skipped);
        // One end-of-packet marker per packet that produced anything at all; never more.
        CHECK(packed.endOfPacket > 0 && packed.endOfPacket <= packets);
        CHECK(packed.frames < sink.frames);          // because messages were skipped, not because it failed
    }

    // ---- 5: determinism and the stored golden
    {
        Sink again;
        itch::Decoder d3(snap, ip);
        uint64_t seq = 1'000'000;
        auto emit = [&](const FrameHeader* f) { again(f); };
        for (const auto& m : wire) d3.decodeMessage(m.data(), m.size(), ++seq, false, emit);
        d3.finish(emit);
        CHECK(again.frames == sink.frames);
        CHECK(again.digest() == sink.digest());
    }

    // ---- 6: input a decoder must survive
    {
        itch::Decoder bad(snap, ip);
        Sink quiet;
        auto emit = [&](const FrameHeader* f) { quiet(f); };
        uint8_t m[64];
        std::memset(m, 0, sizeof m);
        m[0] = 'Z';                                          // a type ITCH does not define
        bad.decodeMessage(m, 12, 1, false, emit);
        m[0] = itch::msg::AddOrder;
        bad.decodeMessage(m, 20, 2, false, emit);            // right type, wrong length
        m[0] = itch::msg::OrderDelete;
        itch::putBe64(m + 11, 999'999);
        bad.decodeMessage(m, 19, 3, false, emit);            // an order never added
        m[0] = itch::msg::AddOrder;
        itch::putBe16(m + 1, 40'000);                        // a locate no Stock Directory covered
        itch::putBe64(m + 11, 1); m[19] = 'B';
        itch::putBe32(m + 20, 100); itch::putBe32(m + 32, 1'000'000);
        bad.decodeMessage(m, 36, 4, false, emit);
        std::printf("bad input: unknown %llu, malformed %llu, missing order %llu, unmapped locate %llu, emitted %llu\n",
                    (unsigned long long)bad.unknownType(), (unsigned long long)bad.malformed(),
                    (unsigned long long)bad.missingOrder(), (unsigned long long)bad.unmappedLocate(),
                    (unsigned long long)bad.emitted());
        CHECK(bad.unknownType() == 1 && bad.malformed() == 1 && bad.missingOrder() == 1 && bad.unmappedLocate() == 1);
        CHECK(bad.emitted() == 0);                           // nothing invented from nonsense
        // a payload whose length prefix runs past the end stops the walk instead of reading on
        alignas(16) std::byte pkt[MAX_PACKET];
        PacketBuilder pb(pkt);
        pb.begin(FEED_ITCH, 0, 1, MIDNIGHT);
        auto* p = reinterpret_cast<uint8_t*>(pkt) + sizeof(PacketHeader);
        itch::putBe16(p, 900);                               // claims 900 bytes in a 16-byte payload
        std::memset(p + 2, 0, 14);
        pb.header()->msgCount = 1;
        pb.header()->payloadLen = 16;
        CHECK(bad.decode(*pb.header(), 0, emit) == 0);
    }

    // ---- 7 and 8: the consolidated tape, and what it says against what we saw
    {
        std::vector<std::string> tickers;
        for (const SymbolRecord& r : snap.instruments())
            if (r.symbolIdx >= 1 && r.symbolIdx <= 10) tickers.emplace_back(r.ticker, ::strnlen(r.ticker, 16));
        CHECK(tickers.size() == 10);

        sip::Decoder::Params sp{};
        sp.feedId = FEED_SIP; sp.sourceId = 9; sp.sessionMidnightNs = MIDNIGHT; sp.dialect = sip::Dialect::Utp;
        sip::Decoder tape(snap, sp);
        tape.mapParticipant('Q', VENUE_XNAS);
        Sink tapeSink;
        uint64_t sipSeq = 5'000'000;

        // The direct view a router would act on: it stands still between moves, which is exactly
        // what lets a lagging tape catch up. Without that, direct and tape never agree at the same
        // instant, every episode stays open forever, and "nothing was reported" would be true for
        // the wrong reason.
        struct DirectBest { int64_t bid = 0, ask = 0, bidQty = 0, askQty = 0; bool moved = false; };
        std::vector<DirectBest> direct(11);
        uint32_t seeded = 0;
        for (uint32_t sym = 1; sym <= 10; ++sym) {
            auto [b, bq] = best.bid(sym);
            auto [a, aq] = best.ask(sym);
            if (!b || !a || b >= a) continue;                       // only symbols with a real two-sided market
            direct[sym] = {b, a, bq, aq, true};
            ++seeded;
        }
        CHECK(seeded >= 5);
        std::mt19937_64 moveRng(77);
        int64_t now = MIDNIGHT + 9 * 3600 * 1'000'000'000LL;

        auto phase = [&](SipSim& s, sip::NbboMonitor& mon, std::vector<NbboDivergence>& out, uint32_t rounds) {
            auto divEmit = [&](const FrameHeader* f) { out.push_back(*as<NbboDivergence>(f)); };
            for (uint32_t r = 0; r < rounds; ++r) {
                now += 500'000;                                      // half a millisecond a round
                for (uint32_t sym = 1; sym <= 10; ++sym) {
                    DirectBest& d = direct[sym];
                    if (!d.bid) continue;
                    if (moveRng() % 20 == 0) {                       // about one move in twenty rounds
                        const int64_t step = (moveRng() & 1) ? SipSim::TICK : -SipSim::TICK;
                        d.bid += step; d.ask += step; d.moved = true;
                    }
                    mon.onDirect(sym, d.bid, d.ask, now, ++sipSeq, divEmit);
                    if (d.moved) { s.quote(sym, tickers[sym - 1], d.bid, d.bidQty, d.ask, d.askQty, now); d.moved = false; }
                }
                s.advance(now, [&](const uint8_t* m, size_t len) {
                    tape.decodeMessage(m, len, ++sipSeq, true, [&](const FrameHeader* f) {
                        tapeSink(f);
                        if (const auto* q = as<Nbbo>(f)) mon.onSip(q->symbolIdx, q->sipBid, q->sipAsk, now, ++sipSeq, divEmit);
                    });
                });
            }
            mon.tick(now, divEmit);
        };

        sip::NbboMonitor::Params mp{};
        mp.feedId = FEED_SIP; mp.sourceId = 9;
        mp.minEpisodeNs = 2'000'000;              // eight times the tape's lag
        mp.alertAfterNs = 50'000'000;
        mp.staleAfterNs = 150'000'000;

        // A tape that is only late. Episodes open on every move and close when it catches up, and
        // none of them is worth a line on the log.
        std::vector<NbboDivergence> lateFound;
        sip::NbboMonitor lateMon(rd.maxSymbolIdx(), mp);
        {
            SipSim::Params gp{}; gp.lagNs = 250'000; gp.seed = 3;
            SipSim good(gp);
            phase(good, lateMon, lateFound, 400);
            good.print(tickers[0], 22'00000000LL, 500, now, [&](const uint8_t* m, size_t len) {
                tape.decodeMessage(m, len, ++sipSeq, true, [&](const FrameHeader* f) { tapeSink(f); });
            });
            good.action(tickers[1], 'H', now, [&](const uint8_t* m, size_t len) {
                tape.decodeMessage(m, len, ++sipSeq, true, [&](const FrameHeader* f) { tapeSink(f); });
            });
            std::printf("tape: %llu quotes, %llu prints, %llu actions decoded; only late: %llu episodes opened, "
                        "%llu closed, %llu reported, longest %lld us\n",
                        (unsigned long long)tape.quotes(), (unsigned long long)tape.trades(),
                        (unsigned long long)tape.actions(), (unsigned long long)lateMon.episodes(),
                        (unsigned long long)lateMon.closed(), (unsigned long long)lateMon.reported(),
                        (long long)(lateMon.longestNs() / 1000));
            CHECK(tape.quotes() > 0 && tape.trades() == 1 && tape.actions() == 1);
            CHECK(tape.malformed() == 0 && tape.unknownSymbol() == 0);
            CHECK(lateMon.episodes() > 0);                 // a lagging tape does disagree
            CHECK(lateMon.closed() == lateMon.episodes()); // and every disagreement ends
            CHECK(lateMon.reported() == 0);                // and none of them is news
            CHECK(lateFound.empty());
            CHECK(lateMon.longestNs() > 0 && lateMon.longestNs() < mp.minEpisodeNs);
        }

        // A tape that is wrong, crossed, and stops. Each is a different kind and each is said once.
        std::vector<NbboDivergence> found;
        sip::NbboMonitor mon(rd.maxSymbolIdx(), mp);
        {
            SipSim::Params bp{};
            bp.lagNs = 250'000; bp.disagreePct = 0.25; bp.crossPct = 0.10;
            bp.stallPct = 0.004; bp.stallNs = 300'000'000; bp.seed = 5;
            SipSim bad(bp);
            phase(bad, mon, found, 3000);
            uint64_t kinds[6] = {};
            for (const NbboDivergence& d : found) if (uint8_t(d.kind) < 6) ++kinds[uint8_t(d.kind)];
            uint64_t ended = 0, stillOpen = 0;
            for (const NbboDivergence& d : found) (d.durationNs ? ended : stillOpen)++;
            std::printf("tape misbehaving: %llu wrong quotes, %llu crossed, %llu stalls swallowing %llu updates; "
                        "%llu episodes, %llu reported, %llu still-open alerts; records: %llu ended, %llu open "
                        "(price %llu, crossed %llu, sip stale %llu), longest %lld ms\n",
                        (unsigned long long)bad.wrong(), (unsigned long long)bad.crossed(),
                        (unsigned long long)bad.stalls(), (unsigned long long)bad.stalled(),
                        (unsigned long long)mon.episodes(), (unsigned long long)mon.reported(),
                        (unsigned long long)mon.alerts(), (unsigned long long)ended, (unsigned long long)stillOpen,
                        (unsigned long long)kinds[uint8_t(DivergenceKind::Price)],
                        (unsigned long long)kinds[uint8_t(DivergenceKind::Crossed)],
                        (unsigned long long)kinds[uint8_t(DivergenceKind::SipStale)],
                        (long long)(mon.longestNs() / 1'000'000));
            CHECK(bad.wrong() > 0 && bad.crossed() > 0 && bad.stalls() > 0);
            CHECK(mon.reported() > 0);                     // now there is something to report
            CHECK(!found.empty());
            CHECK(mon.crossed() > 0);
            CHECK(ended > 0 && stillOpen > 0);             // both kinds of record get written
            // All three shapes have to appear, or a shape is going unnoticed rather than not
            // happening: the tape was wrong, it read crossed, and it stopped.
            CHECK(kinds[uint8_t(DivergenceKind::Price)] > 0);
            CHECK(kinds[uint8_t(DivergenceKind::Crossed)] > 0);
            CHECK(kinds[uint8_t(DivergenceKind::SipStale)] > 0);
            CHECK(mon.longestNs() > mp.staleAfterNs);      // the stall is the longest episode of the day
            // Every line on the log names both sides and the shape, so it answers on its own.
            for (const NbboDivergence& d : found) {
                CHECK(d.symbolIdx >= 1 && d.symbolIdx <= 10);
                CHECK(d.kind != DivergenceKind(0));
                CHECK(d.directBid != 0 && d.directAsk != 0);
                if (d.durationNs) CHECK(d.durationNs >= mp.minEpisodeNs);
            }
        }
    }

    // ---- the golden: counts and the hash of everything the ITCH decoder produced
    {
        char buf[1024];
        std::snprintf(buf, sizeof buf,
            "{\n  \"schemaVersion\": %u,\n  \"wireMessages\": %zu,\n  \"frames\": %llu,\n  \"deltas\": %llu,\n"
            "  \"trades\": %llu,\n  \"statuses\": %llu,\n  \"imbalances\": %llu,\n  \"liveOrders\": %zu,\n"
            "  \"liveLevels\": %zu,\n  \"decodedHash\": \"%s\"\n}\n",
            unsigned(SCHEMA_VERSION), wire.size(), (unsigned long long)sink.frames, (unsigned long long)dec.deltas(),
            (unsigned long long)dec.trades(), (unsigned long long)dec.statuses(),
            (unsigned long long)dec.imbalances(), dec.liveOrders(), dec.liveLevels(),
            hex(sink.digest()).c_str());
        const std::string json = buf;
        if (!writeGolden.empty()) { std::ofstream(writeGolden) << json; std::printf("wrote golden %s\n", writeGolden.c_str()); }
        if (!golden.empty()) {
            std::ifstream in(golden);
            if (!in) { std::printf("FAIL: no golden at %s (run with --write-golden)\n", golden.c_str()); return 1; }
            const std::string stored((std::istreambuf_iterator<char>(in)), {});
            int rc = 0;
            for (const char* k : {"schemaVersion", "wireMessages", "frames", "deltas", "trades", "statuses",
                                  "imbalances", "liveOrders", "liveLevels", "decodedHash"}) {
                const std::string a = jsonField(stored, k), b = jsonField(json, k);
                if (a != b) { std::printf("FAIL: golden %s: %s stored=%s now=%s\n", golden.c_str(), k, a.c_str(), b.c_str()); rc = 1; }
            }
            if (rc) return 1;
            std::printf("golden %s: identical (%s)\n", golden.c_str(), hex(sink.digest()).c_str());
        }
    }

    std::printf("decoder tests ok\n");
    return 0;
}
