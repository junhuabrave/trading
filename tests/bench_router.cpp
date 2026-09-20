// Router benchmarks. A plan is computed once per accepted parent and once more per re-route, so it
// sits on the order path between the risk decision and the child going out. It reads the book
// through the same shared-memory view the production router reads, so what is measured here is
// what a real one pays: a seqlock read per venue, the cost model per quote, a sort, and the
// allocation guard insisting none of it allocates.
#define TRADING_COUNT_ALLOCS
#include "alloc_guard.hpp"
#include "bench.hpp"
#include "router.hpp"
#include "book.hpp"
#include <filesystem>

using namespace trading; using namespace trading::router; using namespace trading::bench;
using trading::util::AllocScope;

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "usage: %s <snapshot>\n", argv[0]); return 2; }
    refdata::Snapshot snap(argv[1]);
    refdata::RefData rd; rd.load(snap);
    const uint32_t MAXSYM = 10;
    const uint16_t MAXVENUE = 4;
    const std::string segPath = "build/routerbench.seg";
    std::filesystem::remove(segPath);
    md::BookSegment seg(segPath, MAXSYM, MAXVENUE, 20260915, true);
    md::BookBuilder::Params bp{};
    bp.sourceId = 12; bp.feedId = 1; bp.maxSymbolIdx = MAXSYM; bp.maxVenueId = MAXVENUE;
    md::BookBuilder book(rd, bp, seg.base());
    md::BookReader reader = book.reader();

    Router::Params rp{};
    rp.sourceId = 11; rp.maxVenueId = MAXVENUE; rp.sessionsPerVenue = 3;
    Router router(snap, rp);
    router.setBook(&reader);
    for (uint16_t v = 1; v <= MAXVENUE; ++v)
        for (uint16_t i = 0; i < 3; ++i) {
            Frame<VenueSessionStatus> s; s.init();
            s.body.venueId = v; s.body.sessionIdx = i; s.body.state = SessionState::Up;
            router.apply(&s.header);
        }

    uint64_t seq = 1'000'000;
    auto quote = [&](uint16_t venueId, BookSide side, int64_t price, int64_t qty) {
        Frame<BookDelta> d; d.init(); d.header.streamId = 1;
        d.body.symbolIdx = 1; d.body.venueId = venueId; d.body.side = side;
        d.body.price = price; d.body.qty = qty; d.body.orderCount = 1;
        d.body.action = qty > 0 ? BookAction::Set : BookAction::Delete;
        d.body.venueSeq = ++seq;
        book.apply(&d.header, 0, [](const FrameHeader*) {});
    };

    NewOrder o{};
    o.accountIdx = 42; o.symbolIdx = 1; o.side = Side::Buy; o.qty = 500;
    o.price = 22'05000000LL; o.ordType = OrdType::Limit; o.tif = Tif::Day;

    const size_t N = 500'000, B = 32;
    uint64_t children = 0;

    // 1. one venue with enough size: the common case, and the floor
    {
        quote(1, BookSide::Ask, 22'00000000LL, 100'000);
        Plan p;
        for (size_t i = 0; i < N / 10; ++i) { router.plan(o, 500, 0, p); children += p.count; }
        Recorder r(B, N / B); AllocScope a;
        for (size_t i = 0; i + B <= N; i += B) {
            r.begin();
            for (size_t k = 0; k < B; ++k) { router.plan(o, 500, 0, p); children += p.count; }
            r.end();
        }
        const uint64_t al = a.delta();
        report("router.plan_one_venue", r.finish(), al, "one venue showing enough: the book read, the cost model, and a plan of one child");
        if (al) { std::fprintf(stderr, "FAIL: the plan path allocated %llu times\n", (unsigned long long)al); return 1; }
    }
    // 2. a sweep across four venues and four prices, which is the expensive shape
    {
        quote(1, BookSide::Ask, 22'00000000LL, 100);
        quote(2, BookSide::Ask, 22'01000000LL, 100);
        quote(3, BookSide::Ask, 22'02000000LL, 100);
        quote(4, BookSide::Ask, 22'03000000LL, 100);
        Plan p;
        for (size_t i = 0; i < N / 10; ++i) { router.plan(o, 500, 0, p); children += p.count; }
        Recorder r(B, N / B); AllocScope a;
        for (size_t i = 0; i + B <= N; i += B) {
            r.begin();
            for (size_t k = 0; k < B; ++k) { router.plan(o, 500, 0, p); children += p.count; }
            r.end();
        }
        const uint64_t al = a.delta();
        report("router.plan_sweep", r.finish(), al, "four venues at four prices: sorted, swept, ISO marked, remainder rested");
        if (al) { std::fprintf(stderr, "FAIL: the sweep path allocated %llu times\n", (unsigned long long)al); return 1; }
        Plan check;
        router.plan(o, 500, 0, check);
        std::fprintf(stderr, "sweep plan: %u children, reason %u, iso %s\n", check.count, check.reason,
                     (check.children[0].orderFlags & OrderFlags::iso) ? "set" : "clear");
    }
    std::fprintf(stderr, "planned %llu children\n", (unsigned long long)children);
    std::filesystem::remove(segPath);
    return 0;
}
