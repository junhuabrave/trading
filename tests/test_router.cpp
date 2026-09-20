// tests/test_router.cpp <snapshot> <workdir>
//
// The smart order router: the skeleton that replaces the stub (RT-1), the cost model (RT-6) and
// the Sweep strategy (RT-2).
//
// A router is judged on the decisions it can defend, so the drills are named market shapes with a
// right answer that can be worked out by hand, and the arithmetic is checked against those numbers
// rather than against whatever the code happens to produce.
//
//   1. the cost model on worked examples: a taker fee raises a buyer's cost and lowers a seller's
//      proceeds, a rebate does the reverse, a fill probability below one charges the shortfall, and
//      reversion is a cost on either side
//   2. one venue with enough size: one child, no ISO, and the rejected alternative names the venue
//      that was not used
//   3. rebate versus price: a venue showing a worse price with a better rebate does not win, and
//      price priority is why
//   4. the same price on two venues: now the rebate does decide, and the cheaper net venue is first
//   5. a sweep across prices: children in price order, quantity never over the parent's leaves, and
//      ISO set because a protected quote was crossed
//   6. a better price we cannot route to: the sweep stops there rather than stepping over it
//   7. a limit order that cannot be filled: what is left rests, and it rests where posting pays
//   8. post-only never takes
//   9. release offsets line the children up so the slowest venue is not last to see the order
//  10. determinism: the same parent against the same book twice, byte for byte
#include "router.hpp"
#include "book.hpp"
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <array>
#include <random>
#include <vector>

using namespace trading;
using namespace trading::router;
namespace fs = std::filesystem;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static constexpr uint32_t SYM = 1;
static constexpr uint32_t MAXSYM = 10;
static constexpr uint16_t MAXVENUE = 5;      // 1..4 are exchanges, 5 is the dark pool
static constexpr uint16_t LIT = 4;           // the venues that display a quote
static constexpr uint16_t DARK = 5;

// A book we can state in a sentence: put a price and a size on a venue and nothing else is there.
struct Market {
    md::BookBuilder& book;
    uint64_t seq = 1'000'000;
    void quote(uint16_t venueId, BookSide side, int64_t price, int64_t qty) {
        Frame<BookDelta> d;
        d.init();
        d.header.streamId = 1;
        d.body.symbolIdx = SYM; d.body.venueId = venueId; d.body.side = side;
        d.body.price = price; d.body.qty = qty; d.body.orderCount = 1;
        d.body.action = qty > 0 ? BookAction::Set : BookAction::Delete;
        d.body.venueSeq = ++seq;
        book.apply(&d.header, 0, [](const FrameHeader*) {});
    }
    void clear(uint16_t venueId) {
        Frame<BookDelta> d;
        d.init();
        d.header.streamId = 1;
        d.body.symbolIdx = SYM; d.body.venueId = venueId; d.body.action = BookAction::ClearBook;
        d.body.venueSeq = ++seq;
        book.apply(&d.header, 0, [](const FrameHeader*) {});
    }
};

// Field by field rather than a memcmp: Plan has padding between count and the children array, and
// padding is not part of the value. What has to be identical is what the router decided.
static bool same(const Plan& a, const Plan& b) {
    if (a.count != b.count || a.reason != b.reason || a.strategyTag != b.strategyTag) return false;
    if (a.expectedCost != b.expectedCost || a.rejectedAltCost != b.rejectedAltCost
        || a.rejectedAltVenue != b.rejectedAltVenue) return false;
    for (uint8_t i = 0; i < a.count; ++i) {
        const Child& x = a.children[i];
        const Child& y = b.children[i];
        if (x.venueId != y.venueId || x.venueSessionIdx != y.venueSessionIdx || x.qty != y.qty
            || x.price != y.price || x.displayQty != y.displayQty || x.ordType != y.ordType
            || x.tif != y.tif || x.orderFlags != y.orderFlags || x.releaseOffsetNs != y.releaseOffsetNs
            || x.expectedCostPerShare != y.expectedCostPerShare) return false;
    }
    return true;
}

static NewOrder parent(Side side, int64_t qty, int64_t price, uint8_t flags = 0, OrdType type = OrdType::Limit) {
    NewOrder o{};
    o.accountIdx = 42; o.symbolIdx = SYM; o.side = side; o.qty = qty; o.price = price;
    o.ordType = type; o.tif = Tif::Day; o.orderFlags = flags;
    return o;
}

static void sessionsUp(Router& r, uint16_t venueId, uint16_t count, uint32_t open = 0, uint32_t throttleBps = 0) {
    for (uint16_t i = 0; i < count; ++i) {
        Frame<VenueSessionStatus> s;
        s.init();
        s.body.venueId = venueId; s.body.sessionIdx = i; s.body.state = SessionState::Up;
        s.body.openOrders = open; s.body.throttleUsedBps = throttleBps;
        r.apply(&s.header);
    }
}
static void sessionsDown(Router& r, uint16_t venueId, uint16_t count) {
    for (uint16_t i = 0; i < count; ++i) {
        Frame<VenueSessionStatus> s;
        s.init();
        s.body.venueId = venueId; s.body.sessionIdx = i; s.body.state = SessionState::Down;
        r.apply(&s.header);
    }
}

int main(int argc, char** argv) {
    if (argc != 3) { std::fprintf(stderr, "usage: %s snapshot workdir\n", argv[0]); return 2; }
    fs::remove_all(argv[2]);
    fs::create_directories(argv[2]);
    refdata::Snapshot snap(argv[1]);
    refdata::RefData rd; rd.load(snap);

    md::BookSegment seg(std::string(argv[2]) + "/book.seg", MAXSYM, MAXVENUE, 20260915, true);
    md::BookBuilder::Params bp{};
    bp.sourceId = 12; bp.feedId = 1; bp.maxSymbolIdx = MAXSYM; bp.maxVenueId = MAXVENUE;
    md::BookBuilder book(rd, bp, seg.base());
    md::BookReader reader = book.reader();
    Market market{book};

    Router::Params rp{};
    rp.sourceId = 11; rp.maxVenueId = MAXVENUE; rp.sessionsPerVenue = 3;
    Router router(snap, rp);
    router.setBook(&reader);
    for (uint16_t v = 1; v <= MAXVENUE; ++v) sessionsUp(router, v, 3);

    // The fixture's schedule: every venue charges 0.003 to take and pays 0.002 to post.
    const int64_t take = router.feePerShare(1, Liquidity::Removed);
    const int64_t post = router.feePerShare(1, Liquidity::Added);
    std::printf("fees: taking costs %lld, posting pays %lld (1e-8 units per share)\n",
                (long long)take, (long long)post);
    CHECK(take == 300'000 && post == -200'000);

    // ---- 1: the cost model, on numbers that can be checked by hand
    {
        const int64_t px = 22'00000000LL;                      // $22.00
        // A buyer taking pays the price plus the fee.
        CHECK(router.netPerShare(1, Side::Buy, px, Liquidity::Removed) == px + take);
        // A seller taking receives the price less the fee, and the figure is negated so that lower
        // is better for the client on both sides.
        CHECK(router.netPerShare(1, Side::Sell, px, Liquidity::Removed) == take - px);
        // Posting pays a rebate, so it lowers a buyer's cost and raises a seller's proceeds.
        CHECK(router.netPerShare(1, Side::Buy, px, Liquidity::Added) == px + post);
        CHECK(router.netPerShare(1, Side::Sell, px, Liquidity::Added) == post - px);
        // With a certain fill and no reversion, the expected cost is just that figure.
        const int64_t certain = router.expectedPerShare(1, Side::Buy, px, Liquidity::Removed, 0);
        CHECK(certain == px + take);
        // Introduce a venue that fills nine times in ten and reverts 10 bps against us.
        Frame<VenueScorecard> sc;
        sc.init();
        sc.body.venueId = 3; sc.body.symbolIdx = 0; sc.body.fillRateBps = 9000; sc.body.reversionBps = 10;
        router.apply(&sc.header);
        const int64_t fallback = px + take + router.missPenaltyPerShare(px);
        const int64_t got = router.expectedPerShare(3, Side::Buy, px, Liquidity::Removed, fallback);
        const int64_t want = ((px + take) * 9000 + fallback * 1000) / 10'000 + px * 10 / 10'000;
        std::printf("cost model: certain %lld, nine-in-ten with 10 bps reversion %lld (worked: %lld)\n",
                    (long long)certain, (long long)got, (long long)want);
        CHECK(got == want);
        CHECK(got > certain);                                   // an unreliable venue is not free
    }

    // ---- 2: one venue with enough size
    {
        for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
        market.quote(1, BookSide::Ask, 22'00000000LL, 1000);
        market.quote(2, BookSide::Ask, 22'01000000LL, 1000);    // a cent worse
        Plan p;
        CHECK(router.plan(parent(Side::Buy, 500, 22'05000000LL), 500, 0, p));
        std::printf("one venue: %u child(ren), venue %u qty %lld at %lld, reason %u, "
                    "expected %lld, alternative venue %u at %lld\n",
                    p.count, p.children[0].venueId, (long long)p.children[0].qty,
                    (long long)p.children[0].price, p.reason,
                    (long long)p.expectedCost, p.rejectedAltVenue, (long long)p.rejectedAltCost);
        CHECK(p.count == 1);
        CHECK(p.children[0].venueId == 1 && p.children[0].qty == 500);
        CHECK(p.children[0].price == 22'00000000LL);
        CHECK((p.children[0].orderFlags & OrderFlags::iso) == 0);   // nothing was crossed
        CHECK(p.reason == PlanReason::Swept);
        CHECK(p.qty() == 500);
        CHECK(p.rejectedAltVenue == 2);                         // the venue we did not use
        CHECK(p.rejectedAltCost > p.expectedCost);              // and it would have cost more
    }

    // ---- 3: rebate versus price. A rebate cannot buy its way past a better quote.
    {
        for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
        market.quote(1, BookSide::Ask, 22'01000000LL, 1000);    // worse price
        market.quote(2, BookSide::Ask, 22'00000000LL, 1000);    // better price
        // Make venue 1 cheap to take from, cheaper than the whole penny it is behind.
        FeeRecord cheap{};
        cheap.venueId = 1; cheap.liquidity = uint8_t(Liquidity::Removed); cheap.feePerShare = -2'000'000;
        Frame<FeeScheduleUpdate> fs;
        fs.init();
        fs.body.record = cheap;
        router.apply(&fs.header);
        CHECK(router.feePerShare(1, Liquidity::Removed) == -2'000'000);
        CHECK(router.netPerShare(1, Side::Buy, 22'01000000LL, Liquidity::Removed)
            < router.netPerShare(2, Side::Buy, 22'00000000LL, Liquidity::Removed));   // cheaper net
        Plan p;
        CHECK(router.plan(parent(Side::Buy, 500, 22'05000000LL), 500, 0, p));
        std::printf("rebate versus price: net at the worse price is cheaper, and the router still "
                    "took venue %u at %lld\n", p.children[0].venueId, (long long)p.children[0].price);
        CHECK(p.children[0].venueId == 2);                      // price priority, not preference
        CHECK(p.children[0].price == 22'00000000LL);
        // put the schedule back
        FeeRecord normal{};
        normal.venueId = 1; normal.liquidity = uint8_t(Liquidity::Removed); normal.feePerShare = 300'000;
        fs.init(); fs.body.record = normal; router.apply(&fs.header);
        CHECK(router.feePerShare(1, Liquidity::Removed) == 300'000);
    }

    // ---- 4: the same price on two venues. Now net cost is what decides.
    {
        for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
        market.quote(1, BookSide::Ask, 22'00000000LL, 300);
        market.quote(2, BookSide::Ask, 22'00000000LL, 300);
        FeeRecord cheaper{};
        cheaper.venueId = 2; cheaper.liquidity = uint8_t(Liquidity::Removed); cheaper.feePerShare = 100'000;
        Frame<FeeScheduleUpdate> fs; fs.init(); fs.body.record = cheaper; router.apply(&fs.header);
        Plan p;
        CHECK(router.plan(parent(Side::Buy, 600, 22'05000000LL), 600, 0, p));
        std::printf("same price, different fees: first child venue %u, second venue %u\n",
                    p.children[0].venueId, p.count > 1 ? p.children[1].venueId : 0);
        CHECK(p.count == 2);
        CHECK(p.children[0].venueId == 2);                      // the cheaper one goes first
        CHECK(p.children[1].venueId == 1);
        CHECK(p.children[0].price == p.children[1].price);      // and no trade-through happened
        CHECK(p.qty() == 600);
        CHECK((p.children[0].orderFlags & OrderFlags::iso) == 0);
        FeeRecord back{};
        back.venueId = 2; back.liquidity = uint8_t(Liquidity::Removed); back.feePerShare = 300'000;
        fs.init(); fs.body.record = back; router.apply(&fs.header);
    }

    // ---- 5: a sweep across prices, which is what ISO exists for
    {
        for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
        market.quote(1, BookSide::Ask, 22'00000000LL, 200);
        market.quote(2, BookSide::Ask, 22'01000000LL, 200);
        market.quote(3, BookSide::Ask, 22'02000000LL, 200);
        Plan p;
        CHECK(router.plan(parent(Side::Buy, 500, 22'05000000LL), 500, 0, p));
        std::printf("sweep: %u children at", p.count);
        for (uint8_t i = 0; i < p.count; ++i)
            std::printf(" v%u:%lldx%lld", p.children[i].venueId, (long long)p.children[i].qty, (long long)p.children[i].price);
        std::printf(", iso %s, reason %u\n", (p.children[0].orderFlags & OrderFlags::iso) ? "set" : "clear", p.reason);
        // Two hundred at each of three prices is six hundred available against five hundred wanted,
        // so the third venue is only partly taken and nothing is left to rest.
        CHECK(p.count == 3);
        for (uint8_t i = 1; i < p.count; ++i) CHECK(p.children[i].price > p.children[i - 1].price);   // price order
        CHECK(p.children[0].venueId == 1 && p.children[1].venueId == 2 && p.children[2].venueId == 3);
        CHECK(p.children[0].qty == 200 && p.children[1].qty == 200 && p.children[2].qty == 100);
        CHECK(p.qty() == 500);                                   // never more than the parent's leaves
        for (uint8_t i = 0; i < p.count; ++i) CHECK(p.children[i].orderFlags & OrderFlags::iso);
        CHECK(p.reason == PlanReason::SweptWithIso);
    }

    // ---- 6: a better price we cannot reach
    {
        for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
        market.quote(1, BookSide::Ask, 22'00000000LL, 200);     // the best price
        market.quote(2, BookSide::Ask, 22'01000000LL, 500);
        sessionsDown(router, 1, 3);                             // and no way to reach it
        Plan p;
        const bool any = router.plan(parent(Side::Buy, 500, 22'05000000LL), 500, 0, p);
        std::printf("blocked by a better price: routed %s, %u child(ren), reason %u\n",
                    any ? "something" : "nothing", p.count, p.reason);
        CHECK(p.reason == PlanReason::BlockedByBetter);
        for (uint8_t i = 0; i < p.count; ++i) {
            // Whatever it did, it did not take at the worse price while the better one stood.
            CHECK(!(p.children[i].tif == Tif::Ioc && p.children[i].price > 22'00000000LL));
        }
        sessionsUp(router, 1, 3);
    }

    // ---- 7: a limit order that cannot be filled rests, where posting pays best
    {
        for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
        market.quote(2, BookSide::Ask, 22'00000000LL, 100);
        FeeRecord generous{};
        generous.venueId = 4; generous.liquidity = uint8_t(Liquidity::Added); generous.feePerShare = -500'000;
        Frame<FeeScheduleUpdate> fs; fs.init(); fs.body.record = generous; router.apply(&fs.header);
        Plan p;
        CHECK(router.plan(parent(Side::Buy, 1000, 22'00000000LL), 1000, 0, p));
        const Child& rest = p.children[p.count - 1];
        std::printf("remainder: took %lld, rested %lld on venue %u at %lld (best rebate), reason %u\n",
                    (long long)(p.qty() - rest.qty), (long long)rest.qty, rest.venueId,
                    (long long)rest.price, p.reason);
        CHECK(p.count == 2);
        CHECK(p.children[0].qty == 100 && p.children[0].tif == Tif::Ioc);
        CHECK(rest.qty == 900 && rest.tif == Tif::Day);
        CHECK(rest.venueId == 4);                               // the best rebate takes the rest
        CHECK(rest.price == 22'00000000LL);
        CHECK(p.qty() == 1000);
        CHECK(p.reason == PlanReason::PostedRemainder);
    }

    // ---- 8: post-only never takes
    {
        for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
        market.quote(2, BookSide::Ask, 22'00000000LL, 1000);
        Plan p;
        CHECK(router.plan(parent(Side::Buy, 500, 22'05000000LL, OrderFlags::postOnly), 500, 0, p));
        std::printf("post only: %u child(ren), tif %u, qty %lld - took nothing though 1000 was there\n",
                    p.count, unsigned(p.children[0].tif), (long long)p.children[0].qty);
        CHECK(p.count == 1);
        CHECK(p.children[0].tif != Tif::Ioc);
        CHECK(p.children[0].qty == 500);
        CHECK(p.reason == PlanReason::PostedRemainder);
    }

    // ---- 9: children land together
    {
        for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
        market.quote(1, BookSide::Ask, 22'00000000LL, 200);     // XNYS, the slowest in the fixture
        market.quote(2, BookSide::Ask, 22'00000000LL, 200);     // XNAS, the fastest
        Plan p;
        CHECK(router.plan(parent(Side::Buy, 400, 22'05000000LL), 400, 0, p));
        CHECK(p.count == 2);
        int64_t slowest = 0;
        for (uint8_t i = 0; i < p.count; ++i)
            slowest = std::max(slowest, router.venueLatencyNs(p.children[i].venueId));
        std::printf("release: slowest venue %lld ns;", (long long)slowest);
        for (uint8_t i = 0; i < p.count; ++i) {
            std::printf(" v%u waits %lld", p.children[i].venueId, (long long)p.children[i].releaseOffsetNs);
            // Everything arrives at the same moment: the wait plus the flight equals the slowest flight.
            CHECK(p.children[i].releaseOffsetNs + router.venueLatencyNs(p.children[i].venueId) == slowest);
        }
        std::printf("\n");
        CHECK(p.children[0].releaseOffsetNs != p.children[1].releaseOffsetNs);   // the drill has teeth
    }

    // ---- 10: the same parent against the same book, twice
    {
        Plan a, b;
        const NewOrder o = parent(Side::Buy, 400, 22'05000000LL);
        CHECK(router.plan(o, 400, 0, a));
        CHECK(router.plan(o, 400, 0, b));
        CHECK(same(a, b));
        // and a sell is priced by the same code, with the sign the other way round
        for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
        market.quote(1, BookSide::Bid, 21'99000000LL, 300);
        market.quote(2, BookSide::Bid, 22'00000000LL, 300);
        Plan s;
        CHECK(router.plan(parent(Side::Sell, 400, 21'95000000LL), 400, 0, s));
        std::printf("determinism: identical plans; a sell takes venue %u at %lld first (the higher bid)\n",
                    s.children[0].venueId, (long long)s.children[0].price);
        CHECK(s.children[0].venueId == 2 && s.children[0].price == 22'00000000LL);
        CHECK(s.children[1].price == 21'99000000LL);
        CHECK(s.qty() == 400);
    }

    // ---- 11: Regulation NMS, as a property over random markets rather than a chosen one
    //
    // Named shapes prove the cases someone thought of. A trade-through is what happens in the case
    // nobody thought of, so this generates markets instead: every venue independently quoting or
    // not, at prices that lock and cross each other, with sessions up and down, and a parent that
    // may be a buy or a sell, a limit or a market order. The rules are checked on every plan.
    {
        struct Quoted { int64_t bid = 0, ask = 0, bidQty = 0, askQty = 0; bool up = false; };
        std::mt19937_64 rng(20260920);
        const int64_t base = 22'00000000LL;
        uint64_t planned = 0, tookSomething = 0, isoPlans = 0 , blocked = 0, posted = 0;

        for (int iter = 0; iter < 20000; ++iter) {
            // Lit venues only. An alternative trading system displays no quote, so it is not what a
            // trade-through is measured against and it has no touch to sweep.
            std::array<Quoted, MAXVENUE + 1> mkt{};
            for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
            sessionsDown(router, DARK, 3);
            for (uint16_t v = 1; v <= LIT; ++v) {
                Quoted& q = mkt[v];
                q.up = (rng() % 10) != 0;                       // one venue in ten has no session
                if (q.up) sessionsUp(router, v, 3); else sessionsDown(router, v, 3);
                // Prices straddle the base by up to five cents on each side, which lets venues
                // lock each other (equal bid and ask) and cross each other (bid above an ask).
                if (rng() % 4 != 0) {
                    q.bid = base + (int64_t(rng() % 11) - 5) * 1'000'000;
                    q.bidQty = int64_t(100 * (1 + rng() % 5));
                    market.quote(v, BookSide::Bid, q.bid, q.bidQty);
                }
                if (rng() % 4 != 0) {
                    q.ask = base + (int64_t(rng() % 11) - 5) * 1'000'000;
                    q.askQty = int64_t(100 * (1 + rng() % 5));
                    market.quote(v, BookSide::Ask, q.ask, q.askQty);
                }
            }
            const bool buy = (rng() & 1) != 0;
            const bool marketOrder = rng() % 8 == 0;
            const int64_t qty = int64_t(100 * (1 + rng() % 12));
            const int64_t limit = marketOrder ? 0 : base + (int64_t(rng() % 13) - 6) * 1'000'000;
            NewOrder o = parent(buy ? Side::Buy : Side::Sell, qty, limit,
                                0, marketOrder ? OrdType::Market : OrdType::Limit);
            Plan p;
            const bool any = router.plan(o, qty, 0, p);
            ++planned;
            if (!any) { CHECK(p.count == 0); continue; }

            // the best protected quote anywhere, whoever is showing it
            int64_t bestProtected = 0;
            for (uint16_t v = 1; v <= LIT; ++v) {
                const int64_t px = buy ? mkt[v].ask : mkt[v].bid;
                if (!px) continue;
                if (!bestProtected || (buy ? px < bestProtected : px > bestProtected)) bestProtected = px;
            }

            int64_t total = 0;
            int takenPrices = 0;
            int64_t seenPrices[MAX_CHILDREN]{};
            bool anyIso = false, anyTake = false;
            std::array<bool, MAXVENUE + 1> takenAt{};
            for (uint8_t i = 0; i < p.count; ++i) {
                const Child& c = p.children[i];
                total += c.qty;
                CHECK(c.qty > 0);
                CHECK(c.venueId >= 1 && c.venueId <= LIT);
                CHECK(mkt[c.venueId].up);                       // never routed where there is no session
                if (c.orderFlags & OrderFlags::iso) anyIso = true;
                if (c.tif == Tif::Ioc) {                        // a taking child
                    anyTake = true;
                    takenAt[c.venueId] = true;
                    const int64_t shown = buy ? mkt[c.venueId].ask : mkt[c.venueId].bid;
                    const int64_t size = buy ? mkt[c.venueId].askQty : mkt[c.venueId].bidQty;
                    CHECK(c.price == shown);                    // took a price that was actually displayed
                    CHECK(c.qty <= size);                       // and no more than was displayed
                    if (limit) CHECK(buy ? c.price <= limit : c.price >= limit);
                    bool fresh = true;
                    for (int k = 0; k < takenPrices; ++k) if (seenPrices[k] == c.price) fresh = false;
                    if (fresh) seenPrices[takenPrices++] = c.price;
                } else {
                    ++posted;
                    if (limit) CHECK(c.price == limit);
                }
            }
            CHECK(total <= qty);                                 // never more than the parent asked for
            if (anyTake) ++tookSomething;

            // No trade-through: for every child that took, any venue showing a strictly better
            // price must have been taken in the same plan. If it could not be - no session - the
            // plan must not have taken at the worse price at all, which is what stops an
            // intermarket sweep that does not actually sweep.
            for (uint8_t i = 0; i < p.count; ++i) {
                const Child& c = p.children[i];
                if (c.tif != Tif::Ioc) continue;
                for (uint16_t v = 1; v <= LIT; ++v) {
                    const int64_t px = buy ? mkt[v].ask : mkt[v].bid;
                    if (!px) continue;
                    const bool better = buy ? px < c.price : px > c.price;
                    if (better) CHECK(takenAt[v]);
                }
            }
            // ISO exactly when it is needed: set if and only if the plan took at more than one
            // price. Setting it otherwise claims a sweep that did not happen; not setting it when
            // several prices were taken is the trade-through the flag exists to make lawful.
            if (takenPrices > 1) { CHECK(anyIso); ++isoPlans; }
            else for (uint8_t i = 0; i < p.count; ++i) CHECK(!(p.children[i].orderFlags & OrderFlags::iso));
            if (p.reason == PlanReason::BlockedByBetter) ++blocked;
            // The best price taken is the best price anywhere, or nothing was taken.
            if (anyTake && bestProtected) {
                int64_t bestTaken = 0;
                for (uint8_t i = 0; i < p.count; ++i) {
                    const Child& c = p.children[i];
                    if (c.tif != Tif::Ioc) continue;
                    if (!bestTaken || (buy ? c.price < bestTaken : c.price > bestTaken)) bestTaken = c.price;
                }
                CHECK(bestTaken == bestProtected);
            }
        }
        std::printf("Regulation NMS over %llu random markets: %llu plans took liquidity, %llu across "
                    "more than one price (all ISO), %llu stopped at a price they could not reach, "
                    "%llu children rested\n",
                    (unsigned long long)planned, (unsigned long long)tookSomething,
                    (unsigned long long)isoPlans, (unsigned long long)blocked, (unsigned long long)posted);
        CHECK(planned == 20000);
        CHECK(tookSomething > 2000);        // the generator really did produce marketable orders
        CHECK(isoPlans > 100);              // and markets worth sweeping across
        CHECK(blocked > 50);                // and venues that could not be reached
        CHECK(posted > 500);
        for (uint16_t v = 1; v <= MAXVENUE; ++v) sessionsUp(router, v, 3);
    }

    // ---- 12 to 17: the strategy library, and the client instructions that override it
    {
        auto setProfile = [&](uint16_t profileId, RouteStrategy st, int64_t darkWait, int64_t passiveWait) {
            Frame<RoutingProfileUpdate> u;
            u.init();
            u.body.profileId = profileId; u.body.strategy = st;
            u.body.params[0] = darkWait; u.body.params[1] = passiveWait;
            router.apply(&u.header);
        };
        // account 42 in the fixture carries routing profile 3
        const uint16_t PROFILE = 3;
        auto twoSided = [&]() {
            for (uint16_t v = 1; v <= MAXVENUE; ++v) market.clear(v);
            market.quote(2, BookSide::Bid, 22'00000000LL, 500);
            market.quote(2, BookSide::Ask, 22'02000000LL, 500);
        };

        // 12: DarkFirst rests at the midpoint where nobody can see it, and waits
        setProfile(PROFILE, RouteStrategy::DarkFirst, 40'000'000, 0);
        twoSided();
        Plan d;
        CHECK(router.plan(parent(Side::Buy, 400, 22'05000000LL, OrderFlags::allowDark), 400, 0, d));
        std::printf("dark first: venue %u (%s) at %lld, displayed %lld, waits %lld ms, "
                    "the lit alternative was venue %u at %lld\n",
                    d.children[0].venueId, router.venueKind(d.children[0].venueId) == uint8_t(VenueType::Ats) ? "an ATS" : "lit",
                    (long long)d.children[0].price, (long long)d.children[0].displayQty,
                    (long long)(d.waitNs / 1'000'000), d.rejectedAltVenue, (long long)d.rejectedAltCost);
        CHECK(d.count == 1);
        CHECK(d.children[0].venueId == DARK);
        CHECK(router.venueKind(DARK) == uint8_t(VenueType::Ats));
        CHECK(d.children[0].price == 22'01000000LL);          // the midpoint of 22.00 and 22.02
        CHECK(d.children[0].displayQty == 0);                 // dark: nothing is shown
        CHECK(d.children[0].qty == 400);
        CHECK(d.waitNs == 40'000'000);
        CHECK(d.reason == PlanReason::DarkFirst);
        CHECK(d.strategyTag == uint16_t(RouteStrategy::DarkFirst));
        // the runner-up is what crossing now would have cost, which is how a review judges the wait
        CHECK(d.rejectedAltVenue == 2 && d.rejectedAltCost != 0);
        CHECK(d.expectedCost < d.rejectedAltCost);            // the midpoint beats paying the offer

        // 13: the same order without allowDark never sees a dark venue
        Plan lit;
        CHECK(router.plan(parent(Side::Buy, 400, 22'05000000LL), 400, 0, lit));
        std::printf("dark declined: %u child(ren) on venue %u, reason %u\n",
                    lit.count, lit.children[0].venueId, lit.reason);
        for (uint8_t i = 0; i < lit.count; ++i) CHECK(lit.children[i].venueId != DARK);
        CHECK(lit.strategyTag == uint16_t(RouteStrategy::Sweep));
        CHECK(lit.reason == PlanReason::DarkDeclined);      // and the log says the client refused dark
        CHECK(lit.children[0].price == 22'02000000LL);        // it crossed the spread instead

        // 14: PassivePost joins the near touch rather than paying the spread, and posts only
        setProfile(PROFILE, RouteStrategy::PassivePost, 0, 150'000'000);
        twoSided();
        Plan pp;
        CHECK(router.plan(parent(Side::Buy, 400, 22'05000000LL), 400, 0, pp));
        std::printf("passive post: venue %u at %lld (the bid, not the offer), post only %s, waits %lld ms\n",
                    pp.children[0].venueId, (long long)pp.children[0].price,
                    (pp.children[0].orderFlags & OrderFlags::postOnly) ? "yes" : "NO",
                    (long long)(pp.waitNs / 1'000'000));
        CHECK(pp.count == 1);
        CHECK(pp.children[0].price == 22'00000000LL);         // joined the bid
        CHECK(pp.children[0].tif == Tif::Day);
        CHECK(pp.children[0].orderFlags & OrderFlags::postOnly);
        CHECK(pp.children[0].venueId != DARK);                // a displayed quote is not dark
        CHECK(pp.waitNs == 150'000'000);
        CHECK(pp.reason == PlanReason::Passive);

        // 15: the timer goes off. What was resting crosses, and the log says that is why.
        const uint64_t pid = parent(Side::Buy, 400, 22'05000000LL).orderId;
        CHECK(!router.escalated(pid));
        router.onTimer(pid);
        CHECK(router.escalated(pid));
        Plan esc;
        NewOrder po = parent(Side::Buy, 400, 22'05000000LL);
        po.orderId = pid;
        CHECK(router.plan(po, 400, 0, esc));
        std::printf("timer fired: %u child(ren), venue %u at %lld, tif %u, reason %u\n",
                    esc.count, esc.children[0].venueId, (long long)esc.children[0].price,
                    unsigned(esc.children[0].tif), esc.reason);
        CHECK(esc.children[0].tif == Tif::Ioc);               // it crossed
        CHECK(esc.children[0].price == 22'02000000LL);        // at the offer
        CHECK(esc.reason == PlanReason::Escalated);
        CHECK(esc.waitNs == 0);                               // and it is not waiting for anything now
        router.forget(pid);
        CHECK(!router.escalated(pid));

        // 16: MidPeg rests at the midpoint on a lit venue
        setProfile(PROFILE, RouteStrategy::MidPeg, 0, 0);
        twoSided();
        Plan mp;
        CHECK(router.plan(parent(Side::Buy, 400, 22'05000000LL), 400, 0, mp));
        std::printf("mid peg: venue %u at %lld, type %u\n",
                    mp.children[0].venueId, (long long)mp.children[0].price, unsigned(mp.children[0].ordType));
        CHECK(mp.count == 1);
        CHECK(mp.children[0].price == 22'01000000LL);
        CHECK(mp.children[0].ordType == OrdType::Pegged);
        CHECK(mp.children[0].venueId != DARK);
        CHECK(mp.reason == PlanReason::MidPeg);

        // 17: a client that says no internalisation is obeyed, and a strategy with nowhere dark to
        // go falls through to the lit market rather than doing nothing
        setProfile(PROFILE, RouteStrategy::DarkFirst, 40'000'000, 0);
        twoSided();
        sessionsDown(router, DARK, 3);
        Plan fell;
        CHECK(router.plan(parent(Side::Buy, 400, 22'05000000LL, OrderFlags::allowDark), 400, 0, fell));
        std::printf("no dark session: fell through to venue %u at %lld\n",
                    fell.children[0].venueId, (long long)fell.children[0].price);
        CHECK(fell.children[0].venueId == 2);
        sessionsUp(router, DARK, 3);
        {
            // the fixture has no wholesaler, so this checks the rule rather than a venue: an
            // internalising venue is refused when the client said not to internalise
            NewOrder ni = parent(Side::Buy, 400, 22'05000000LL, OrderFlags::noInternalize | OrderFlags::allowDark);
            Plan p2;
            CHECK(router.plan(ni, 400, 0, p2));
            for (uint8_t i = 0; i < p2.count; ++i) {
                const uint8_t k = router.venueKind(p2.children[i].venueId);
                CHECK(k != uint8_t(VenueType::Internal) && k != uint8_t(VenueType::Wholesaler));
            }
            std::printf("no internalize: %u child(ren), none on an internalising venue\n", p2.count);
        }
        setProfile(PROFILE, RouteStrategy::Sweep, 0, 0);
    }

    std::printf("router tests ok\n");
    return 0;
}
