// tests/test_book.cpp <snapshot> <workdir>
//
// The book builder and the shared-memory view it publishes through (MD-8).
//
//   1. a recorded day of ITCH through the decoder and into the book, with the invariants checked
//      continuously rather than at the end: no venue book crossed, no negative quantity, the
//      consolidated best equal to the best across venues, quantities matching the ladder
//   2. the builder's ladders against the venue's own live orders - the same two-independent-
//      computations check the decoder gets, one layer further up
//   3. the top of book a reader copies out of shared memory equals what the builder wrote
//   4. readers never block the writer: three reader threads hammering the segment while the writer
//      publishes a million updates, and the writer's cost with them is the writer's cost without
//   5. no reader ever sees a torn record, which the checksum would catch even if the sequence
//      number did not
//   6. a second process's view: the segment reopened read-only shows the same book
//   7. BookSnapshot carries the touch first and is what a late joiner would rebuild from
//   8. the ladder re-bases when the market walks away from its window, and says what it dropped
#include "book.hpp"
#include "itch.hpp"
#include "itch_sim.hpp"
#include <atomic>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <thread>
#include <chrono>
#include <vector>

using namespace trading;
using namespace trading::md;
using namespace trading::sim;
namespace fs = std::filesystem;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static constexpr uint32_t FEED_ITCH = 1;
static constexpr uint16_t VENUE_XNAS = 2;
static constexpr int64_t MIDNIGHT = 1'757'894'400'000'000'000LL;

int main(int argc, char** argv) {
    if (argc != 3) { std::fprintf(stderr, "usage: %s snapshot workdir\n", argv[0]); return 2; }
    const std::string work = argv[2];
    fs::remove_all(work);
    fs::create_directories(work);
    refdata::Snapshot snap(argv[1]);
    refdata::RefData rd; rd.load(snap);

    const uint32_t MAXSYM = 10;
    const uint32_t MAXVENUE = 4;
    const std::string segPath = work + "/book.seg";

    // ---- a recorded day, decoded into the book
    std::vector<ItchSim::Live> venueOrders;
    BookSegment seg(segPath, MAXSYM, MAXVENUE, 20260915, true);
    BookBuilder::Params bp{};
    bp.sourceId = 12; bp.feedId = FEED_ITCH; bp.maxSymbolIdx = MAXSYM; bp.maxVenueId = MAXVENUE;
    bp.snapshotIntervalNs = 1'000'000'000;
    BookBuilder book(rd, bp, seg.base());
    BookReader reader = book.reader();

    uint64_t nbbos = 0, snapshots = 0, checked = 0, crossedNbbo = 0;
    {
        std::vector<std::vector<uint8_t>> wire;
        ItchSim venue(snap, MAXSYM, 20260915, MIDNIGHT);
        auto keep = [&](const uint8_t* m, size_t len) { wire.emplace_back(m, m + len); };
        venue.open(keep);
        for (int i = 0; i < 150'000; ++i) venue.step(keep);
        venueOrders = venue.live();

        itch::Decoder::Params ip{};
        ip.feedId = FEED_ITCH; ip.venueId = VENUE_XNAS; ip.sourceId = 8; ip.sessionMidnightNs = MIDNIGHT;
        itch::Decoder dec(snap, ip);

        int64_t now = MIDNIGHT + 9 * 3600 * 1'000'000'000LL;
        uint64_t seq = 1'000'000;
        auto out = [&](const FrameHeader* f) {
            if (as<Nbbo>(f)) ++nbbos;
            else if (as<BookSnapshot>(f)) ++snapshots;
        };
        // The invariants are checked as the day runs, not at the close: a book that is right at
        // five o'clock and wrong at eleven is a book that was wrong.
        auto invariants = [&]() -> bool {
            for (uint32_t s = 1; s <= MAXSYM; ++s) {
                TopBody t{};
                if (!reader.top(s, t)) return false;
                if (!t.bid && !t.ask) continue;
                if (t.bid && t.ask && t.bid >= t.ask) { ++crossedNbbo; return false; }
                if (t.bidQty < 0 || t.askQty < 0) return false;
                LadderSnapshot l{};
                if (t.bid) {
                    if (!reader.ladder(s, t.bidVenue, l)) return false;
                    if (!l.bidMask) return false;
                    const int i = 63 - std::countl_zero(l.bidMask);
                    if (l.priceAt(BookSide::Bid, i) != t.bid) return false;   // the top is the ladder's top
                    if (l.bidQty[i] != t.bidQty) return false;
                }
                if (t.ask) {
                    if (!reader.ladder(s, t.askVenue, l)) return false;
                    if (!l.askMask) return false;
                    const int i = std::countr_zero(l.askMask);
                    if (l.priceAt(BookSide::Ask, i) != t.ask) return false;
                    if (l.askQty[i] != t.askQty) return false;
                }
                ++checked;
            }
            return true;
        };
        size_t i = 0;
        bool ok = true;
        for (const auto& m : wire) {
            dec.decodeMessage(m.data(), m.size(), ++seq, false, [&](const FrameHeader* f) {
                book.apply(f, now, out);
            });
            now += 2000;
            if (++i % 2000 == 0) { if (!invariants()) { ok = false; break; } book.publishSnapshots(now, out); }
        }
        dec.finish([&](const FrameHeader* f) { book.apply(f, now, out); });
        std::printf("day: %zu itch messages, %llu deltas into the book, %llu top changes, %llu deep-only, "
                    "%llu NBBOs out, %llu snapshots, %llu rebases dropping %llu levels, %llu past the window\n",
                    wire.size(), (unsigned long long)book.deltas(), (unsigned long long)book.topChanges(),
                    (unsigned long long)book.deepOnly(), (unsigned long long)nbbos,
                    (unsigned long long)snapshots, (unsigned long long)book.rebases(),
                    (unsigned long long)book.droppedOnRebase(), (unsigned long long)book.outsideWindow());
        CHECK(ok);
        CHECK(invariants());
        CHECK(checked > 500);                       // the invariant really was evaluated
        CHECK(book.deltas() > 100'000);
        CHECK(book.crossedVenue() == 0);             // a venue never crosses its own book
        CHECK(crossedNbbo == 0);
        CHECK(nbbos > 0 && snapshots > 0);
        CHECK(book.offLadder() == 0);                // every price landed on the tick grid
        CHECK(book.deepOnly() > book.topChanges());  // and most deltas never reached the touch
    }

    // ---- 2: the ladders against the venue's own live orders, aggregated independently
    {
        std::map<std::tuple<uint32_t, uint8_t, int64_t>, int64_t> want;
        for (const ItchSim::Live& o : venueOrders) want[{o.symbolIdx, o.side, o.price}] += o.shares;
        size_t compared = 0, mismatched = 0, outsideWindow = 0;
        for (const auto& [key, qty] : want) {
            const auto [sym, side, px] = key;
            LadderSnapshot l{};
            if (!reader.ladder(sym, VENUE_XNAS, l)) { ++mismatched; continue; }
            const int64_t base = side == uint8_t(BookSide::Bid) ? l.bidBase : l.askBase;
            const int64_t tick = side == uint8_t(BookSide::Bid) ? l.bidTick : l.askTick;
            if (tick <= 0) { ++outsideWindow; continue; }
            const int64_t off = px - base;
            if (off < 0 || off % tick != 0 || off / tick >= int64_t(BOOK_LEVELS)) { ++outsideWindow; continue; }
            const size_t i = size_t(off / tick);
            const int64_t got = side == uint8_t(BookSide::Bid) ? l.bidQty[i] : l.askQty[i];
            ++compared;
            if (got != qty) ++mismatched;
        }
        std::printf("ladders against the venue's live orders: %zu levels compared, %zu outside the "
                    "sixty-four-tick window, %zu mismatched\n", compared, outsideWindow, mismatched);
        CHECK(compared > 50);
        CHECK(mismatched == 0);
    }

    // ---- 3, 4 and 5: readers that never block the writer, and never see half a record
    {
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> reads{0}, retries{0}, torn{0}, crossed{0};
        auto readerLoop = [&]() {
            BookReader r(seg.base(), MAXSYM, MAXVENUE);
            TopBody t{};
            while (!stop.load(std::memory_order_relaxed)) {
                for (uint32_t s = 1; s <= MAXSYM; ++s) {
                    if (!r.top(s, t)) { ++retries; continue; }
                    ++reads;
                    if (t.checksum != topChecksum(t)) ++torn;     // the sequence number missed one
                    if (t.bid && t.ask && t.bid >= t.ask) ++crossed;
                }
            }
        };
        // What the writer costs with nobody reading.
        auto writeBurst = [&](size_t n) {
            const int64_t t0 = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            Frame<BookDelta> d;
            int64_t now = MIDNIGHT;
            for (size_t i = 0; i < n; ++i) {
                d.init(); d.header.streamId = FEED_ITCH;
                d.body.symbolIdx = uint32_t(1 + i % MAXSYM); d.body.venueId = VENUE_XNAS;
                d.body.side = (i & 1) ? BookSide::Bid : BookSide::Ask;
                const int64_t ref = rd.refPrice(d.body.symbolIdx);
                d.body.price = d.body.side == BookSide::Bid ? ref - 1'000'000 * int64_t(1 + i % 8)
                                                           : ref + 1'000'000 * int64_t(1 + i % 8);
                d.body.qty = int64_t(100 + (i % 20) * 100);
                d.body.orderCount = 1 + uint32_t(i % 5);
                d.body.venueSeq = 9'000'000 + i;
                d.body.venueTs = now;
                book.apply(&d.header, now, [](const FrameHeader*) {});
                now += 100;
            }
            const int64_t t1 = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            return double(t1 - t0) / double(n);
        };
        const size_t N = 1'000'000;
        writeBurst(N / 10);                                        // warm
        const double alone = writeBurst(N);
        std::vector<std::thread> readers;
        for (int i = 0; i < 3; ++i) readers.emplace_back(readerLoop);
        const double withReaders = writeBurst(N);
        stop.store(true);
        for (auto& th : readers) th.join();
        std::printf("seqlock: writer %.1f ns/update alone, %.1f ns with three readers (%.0f%%); "
                    "%llu reads, %llu retries, %llu torn, %llu crossed\n",
                    alone, withReaders, 100.0 * withReaders / alone,
                    (unsigned long long)reads.load(), (unsigned long long)retries.load(),
                    (unsigned long long)torn.load(), (unsigned long long)crossed.load());
        CHECK(reads.load() > 100'000);                             // the readers really ran
        CHECK(torn.load() == 0);                                   // and never saw half a record
        CHECK(crossed.load() == 0);
        // Two things make "never blocks" a statement rather than a hope. Structurally there is no
        // wait in the writer at all: it stores an odd number, writes, stores an even one. What the
        // measurement adds is that the cost does not run away - the slowdown here is three cores
        // pulling the writer's cache lines out from under it on every store, which is memory
        // traffic and a bounded constant. A writer waiting on a lock a reader held would not be a
        // constant, and its tail would be in microseconds rather than nanoseconds.
        CHECK(withReaders < alone * 4.0);
        // And the readers are not starved either: a retry happens only when one catches the writer
        // mid-record, and it is a rounding error against the reads that succeeded first time.
        CHECK(retries.load() * 100 < reads.load());
    }

    // ---- 6: another process's view of the same segment
    {
        BookSegment ro(segPath, MAXSYM, MAXVENUE, 20260915, false);
        BookReader other(ro.base(), MAXSYM, MAXVENUE);
        size_t agreed = 0;
        for (uint32_t s = 1; s <= MAXSYM; ++s) {
            TopBody mine{}, theirs{};
            CHECK(reader.top(s, mine));
            CHECK(other.top(s, theirs));
            CHECK(std::memcmp(&mine, &theirs, sizeof(TopBody)) == 0);
            ++agreed;
        }
        std::printf("a second mapping of the segment agrees on all %zu symbols\n", agreed);
        CHECK(agreed == MAXSYM);
        bool threw = false;
        try { BookSegment wrong(segPath, MAXSYM + 5, MAXVENUE, 20260915, false); }
        catch (const std::exception&) { threw = true; }
        CHECK(threw);                                              // a different shape is not the same book
    }

    // ---- 7: what a late joiner is handed
    {
        std::vector<BookSnapshot> got;
        book.publishSnapshots(MIDNIGHT + 20ll * 3600 * 1'000'000'000, [&](const FrameHeader* f) {
            if (const auto* s = as<BookSnapshot>(f)) got.push_back(*s);
        });
        CHECK(!got.empty());
        size_t withDepth = 0;
        for (const BookSnapshot& s : got) {
            CHECK(s.symbolIdx >= 1 && s.symbolIdx <= MAXSYM);
            CHECK(s.bidCount <= 16 && s.askCount <= 16);
            for (int i = 1; i < s.bidCount; ++i) CHECK(s.bidPx[i] < s.bidPx[i - 1]);   // best first
            for (int i = 1; i < s.askCount; ++i) CHECK(s.askPx[i] > s.askPx[i - 1]);
            if (s.bidCount && s.askCount) { CHECK(s.bidPx[0] < s.askPx[0]); ++withDepth; }
            // and the touch it carries is the touch the reader sees
            if (s.bidCount) {
                TopBody t{};
                CHECK(reader.top(s.symbolIdx, t));
                if (t.bidVenue == s.venueId) CHECK(s.bidPx[0] == t.bid);
            }
        }
        std::printf("snapshots: %zu published, %zu two-sided, best first on both sides\n", got.size(), withDepth);
        CHECK(withDepth > 0);
    }

    // ---- 8: a market that walks out of the window
    {
        BookSegment s2(work + "/walk.seg", 2, 2, 20260915, true);
        BookBuilder::Params wp{};
        wp.sourceId = 12; wp.feedId = FEED_ITCH; wp.maxSymbolIdx = 2; wp.maxVenueId = 2;
        BookBuilder walk(rd, wp, s2.base());
        BookReader wr = walk.reader();
        uint64_t seqCounter = 8'000'000;
        const int64_t start = rd.refPrice(1);
        Frame<BookDelta> d;
        auto put = [&](int64_t px, int64_t qty) {
            d.init(); d.header.streamId = FEED_ITCH;
            d.body.symbolIdx = 1; d.body.venueId = 1; d.body.side = BookSide::Bid;
            d.body.price = px; d.body.qty = qty; d.body.orderCount = 1; d.body.venueSeq = ++seqCounter;
            walk.apply(&d.header, MIDNIGHT, [](const FrameHeader*) {});
        };
        for (int i = 0; i < 40; ++i) put(start - int64_t(i) * 1'000'000, 100);   // fill the window
        LadderSnapshot l{};
        CHECK(wr.ladder(1, 1, l));
        const int before = std::popcount(l.bidMask);
        // now walk a dollar up, a cent at a time: the window follows and the far end falls off
        for (int i = 1; i <= 100; ++i) put(start + int64_t(i) * 1'000'000, 100);
        CHECK(wr.ladder(1, 1, l));
        TopBody t{};
        CHECK(wr.top(1, t));
        std::printf("walking market: %d levels before, %d after, %llu rebases dropping %llu levels, "
                    "top now %lld\n", before, std::popcount(l.bidMask),
                    (unsigned long long)walk.rebases(), (unsigned long long)walk.droppedOnRebase(),
                    (long long)t.bid);
        CHECK(walk.rebases() > 0 && walk.droppedOnRebase() > 0);
        CHECK(std::popcount(l.bidMask) <= int(BOOK_LEVELS));
        CHECK(t.bid == start + 100'000'000);                       // the touch is the highest bid seen
        CHECK(walk.crossedVenue() == 0);
    }

    std::printf("book builder tests ok\n");
    return 0;
}
