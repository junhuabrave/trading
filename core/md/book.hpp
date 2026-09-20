// core/md/book.hpp : the book, and the window everything else reads it through.
//
// Below this the stack is about not losing messages. Here it is about answering one question fast
// and consistently: what is the market, right now, for this symbol. The router asks it on every
// order, the market maker asks it continuously, and a risk check asks it to price a collar. So the
// answer lives in shared memory rather than behind a call, and the readers are other processes.
//
// Three decisions worth stating.
//
// Flat ladders. Each (symbol, venue, side) is 64 quantities indexed by tick from a base price, with
// a 64-bit occupancy mask beside them. Finding the best price is then one instruction on the mask
// rather than a search, and a level update is an array store. A price outside the window re-bases
// the ladder around the touch and drops what falls off the end, which is the honest cost of a
// fixed window: sixty-four ticks of depth per side is what the router and the market maker use, and
// anything deeper belongs to a consumer that should be reading the venue's own feed.
//
// One writer, no locks, no blocking. Every record carries a sequence number that the writer makes
// odd while it is changing the record and even when it is done. A reader takes a copy between two
// even reads of the same number, and retries if the number moved. The writer never waits for a
// reader and never even looks at one; a slow reader gets a retry, not a stall, and the thing it is
// racing with is bounded by how long a memcpy takes. Every record also carries a checksum over its
// own payload, so a torn read that somehow slipped past the sequence check is caught rather than
// acted on.
//
// The state is the shared segment. The builder does not keep a private copy and then publish it;
// it writes into the mapping, under the sequence numbers. Two copies of a book are two books, and
// the one that is wrong is always the one somebody is reading.
#pragma once
#include "identity.hpp"
#include "refdata.hpp"
#include <atomic>
#include <bit>
#include <cstring>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace trading::md {

inline constexpr size_t BOOK_LEVELS = 64;

// ---- what a reader copies out -----------------------------------------------------------------
struct TopBody {
    uint32_t symbolIdx;
    uint16_t bidVenue, askVenue;
    uint16_t bidLevels, askLevels;
    uint16_t status;            // TradingStatus, so a consumer knows a halt without another lookup
    uint16_t pad;
    int64_t  bid, ask, bidQty, askQty;
    int64_t  sipBid, sipAsk;    // the official NBBO, carried beside ours rather than mixed into it
    uint64_t venueSeq;
    int64_t  venueTs;
    uint32_t checksum;
    uint32_t pad2;
    uint8_t  reserved[32];
};
static_assert(sizeof(TopBody) == 120);

struct TopOfBook {
    std::atomic<uint32_t> seq;   // odd while the body is being changed
    uint32_t pad;
    TopBody body;
};
static_assert(sizeof(TopOfBook) == 128);

struct SharedLadder {
    std::atomic<uint32_t> seq;
    uint32_t pad;
    int64_t  bidBase, askBase, bidTick, askTick;
    uint64_t bidMask, askMask;
    int64_t  bidQty[BOOK_LEVELS], askQty[BOOK_LEVELS];
    uint32_t bidCount[BOOK_LEVELS], askCount[BOOK_LEVELS];
};

struct BookViewHeader {
    char     magic[8];          // "MDBOOK\0\0"
    uint32_t formatVersion;
    uint32_t maxSymbolIdx;
    uint32_t maxVenueId;
    uint32_t businessDate;
    uint64_t reserved[5];
};
static_assert(sizeof(BookViewHeader) == 64);

// A checksum whose only job is to make a torn read visible. Cheap, order-dependent, not a hash.
inline uint32_t topChecksum(const TopBody& b) noexcept {
    uint64_t h = 0xcbf29ce484222325ull;
    auto mix = [&](uint64_t v) { h = (h ^ v) * 0x100000001b3ull; };
    mix(b.symbolIdx); mix(uint64_t(b.bidVenue) << 16 | b.askVenue);
    mix(uint64_t(b.bid)); mix(uint64_t(b.ask));
    mix(uint64_t(b.bidQty)); mix(uint64_t(b.askQty));
    mix(uint64_t(b.sipBid)); mix(uint64_t(b.sipAsk));
    mix(b.venueSeq); mix(uint64_t(b.venueTs));
    return uint32_t(h ^ (h >> 32));
}

// ---- reading ----------------------------------------------------------------------------------
// Returns false only if the record was being written for longer than the caller is willing to wait,
// which on one writer means the caller's spin budget was smaller than a memcpy.
inline bool readTop(const TopOfBook& t, TopBody& out, int spins = 64) noexcept {
    for (int i = 0; i < spins; ++i) {
        const uint32_t s1 = t.seq.load(std::memory_order_acquire);
        if (s1 & 1u) continue;
        std::memcpy(&out, &t.body, sizeof(TopBody));
        std::atomic_thread_fence(std::memory_order_acquire);
        if (t.seq.load(std::memory_order_relaxed) == s1) return true;
    }
    return false;
}

struct LadderSnapshot {
    int64_t  bidBase, askBase, bidTick, askTick;
    uint64_t bidMask, askMask;
    int64_t  bidQty[BOOK_LEVELS], askQty[BOOK_LEVELS];
    uint32_t bidCount[BOOK_LEVELS], askCount[BOOK_LEVELS];
    int64_t  priceAt(BookSide side, int slot) const noexcept {
        return side == BookSide::Bid ? bidBase + int64_t(slot) * bidTick : askBase + int64_t(slot) * askTick;
    }
};

inline bool readLadder(const SharedLadder& l, LadderSnapshot& out, int spins = 64) noexcept {
    for (int i = 0; i < spins; ++i) {
        const uint32_t s1 = l.seq.load(std::memory_order_acquire);
        if (s1 & 1u) continue;
        std::memcpy(&out, &l.bidBase, sizeof(LadderSnapshot));
        std::atomic_thread_fence(std::memory_order_acquire);
        if (l.seq.load(std::memory_order_relaxed) == s1) return true;
    }
    return false;
}

// ---- the mapping ------------------------------------------------------------------------------
// A file-backed mapping, which is what both a real shared segment and a test want. The builder
// creates it; a reader in another process opens it read-only and sees the same bytes.
class BookSegment {
public:
    static size_t bytesFor(uint32_t maxSymbolIdx, uint32_t maxVenueId) noexcept {
        return sizeof(BookViewHeader)
             + size_t(maxSymbolIdx + 1) * sizeof(TopOfBook)
             + size_t(maxSymbolIdx + 1) * size_t(maxVenueId + 1) * sizeof(SharedLadder);
    }
    BookSegment(const std::string& path, uint32_t maxSymbolIdx, uint32_t maxVenueId,
                uint32_t businessDate, bool create)
        : bytes_(bytesFor(maxSymbolIdx, maxVenueId)) {
        const int flags = create ? (O_RDWR | O_CREAT | O_TRUNC) : O_RDWR;
        fd_ = ::open(path.c_str(), flags, 0644);
        if (fd_ < 0) throw std::runtime_error("book segment: cannot open " + path);
        auto fail = [&](const std::string& why) { ::close(fd_); fd_ = -1; throw std::runtime_error(why); };
        if (create && ::ftruncate(fd_, off_t(bytes_)) != 0) fail("book segment: cannot size " + path);
        void* m = ::mmap(nullptr, bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (m == MAP_FAILED) fail("book segment: cannot map " + path);
        base_ = static_cast<std::byte*>(m);
        auto* h = reinterpret_cast<BookViewHeader*>(base_);
        if (create) {
            std::memset(base_, 0, bytes_);
            std::memcpy(h->magic, "MDBOOK", 7);
            h->formatVersion = 1; h->maxSymbolIdx = maxSymbolIdx; h->maxVenueId = maxVenueId;
            h->businessDate = businessDate;
        } else if (std::memcmp(h->magic, "MDBOOK", 6) != 0 || h->formatVersion != 1
                   || h->maxSymbolIdx != maxSymbolIdx || h->maxVenueId != maxVenueId) {
            ::munmap(base_, bytes_);
            fail("book segment: not a book view, or a different shape: " + path);
        }
    }
    ~BookSegment() { if (base_) ::munmap(base_, bytes_); if (fd_ >= 0) ::close(fd_); }
    BookSegment(const BookSegment&) = delete;
    BookSegment& operator=(const BookSegment&) = delete;

    std::byte* base() const noexcept { return base_; }
    size_t bytes() const noexcept { return bytes_; }

private:
    size_t bytes_;
    int fd_ = -1;
    std::byte* base_ = nullptr;
};

// A read-only view over a segment, for the router, the market maker, or a tool. It never writes and
// never blocks the writer.
class BookReader {
public:
    BookReader(std::byte* base, uint32_t maxSymbolIdx, uint32_t maxVenueId)
        : base_(base), maxSym_(maxSymbolIdx), maxVenue_(maxVenueId) {}

    bool top(uint32_t symbolIdx, TopBody& out) const noexcept {
        if (symbolIdx > maxSym_) return false;
        if (!readTop(tops()[symbolIdx], out)) return false;
        return out.checksum == topChecksum(out);
    }
    bool ladder(uint32_t symbolIdx, uint16_t venueId, LadderSnapshot& out) const noexcept {
        if (symbolIdx > maxSym_ || venueId > maxVenue_) return false;
        return readLadder(ladders()[size_t(symbolIdx) * (maxVenue_ + 1) + venueId], out);
    }
    const TopOfBook* tops() const noexcept {
        return reinterpret_cast<const TopOfBook*>(base_ + sizeof(BookViewHeader));
    }
    const SharedLadder* ladders() const noexcept {
        return reinterpret_cast<const SharedLadder*>(base_ + sizeof(BookViewHeader)
                                                     + size_t(maxSym_ + 1) * sizeof(TopOfBook));
    }

private:
    std::byte* base_;
    uint32_t maxSym_, maxVenue_;
};

// ---- the builder ------------------------------------------------------------------------------
class BookBuilder {
public:
    struct Params {
        uint16_t sourceId = 0;
        uint32_t feedId = 0;                        // stamped on what the builder publishes
        uint32_t maxSymbolIdx = 0;
        uint32_t maxVenueId = 0;
        int64_t  snapshotIntervalNs = 5'000'000'000;   // BookSnapshot per symbol, for late joiners
    };

    BookBuilder(const refdata::RefData& rd, Params p, std::byte* base)
        : rd_(rd), p_(p), base_(base), sym_(size_t(p.maxSymbolIdx) + 1) {}

    // One normalised message. Emits an Nbbo when the top of book moves and a BookSnapshot when a
    // symbol is due one. Everything else it does is a write into the shared segment.
    template <class E>
    void apply(const FrameHeader* f, int64_t now, E&& emit) {
        ++applied_;
        if (const auto* d = as<BookDelta>(f))    { onDelta(*d, now, emit); return; }
        if (const auto* n = as<Nbbo>(f))         { onSip(*n, now, emit); return; }
        if (const auto* s = as<SymbolStatus>(f)) { onStatus(*s, now, emit); return; }
        if (as<Trade>(f) || as<Imbalance>(f) || as<VenueStatus>(f)) { ++passed_; return; }
        ++ignored_;
    }

    // Late joiners need a starting point that is not the open. Called from the owner's timer.
    template <class E>
    void publishSnapshots(int64_t now, E&& emit) {
        for (uint32_t s = 1; s <= p_.maxSymbolIdx; ++s) {
            Symbol& st = sym_[s];
            if (!st.touched) continue;
            if (st.lastSnapshotTs && now - st.lastSnapshotTs < p_.snapshotIntervalNs) continue;
            st.lastSnapshotTs = now;
            for (uint16_t v = 1; v <= p_.maxVenueId; ++v) {
                if (!(st.venueMask & (1ull << v))) continue;
                emitSnapshot(s, v, now, emit);
            }
        }
    }

    uint64_t applied() const noexcept { return applied_; }
    uint64_t deltas() const noexcept { return deltas_; }
    uint64_t topChanges() const noexcept { return topChanges_; }
    uint64_t nbbos() const noexcept { return nbbos_; }
    uint64_t snapshots() const noexcept { return snapshots_; }
    uint64_t rebases() const noexcept { return rebases_; }
    uint64_t droppedOnRebase() const noexcept { return dropped_; }      // levels that fell off the window
    uint64_t offLadder() const noexcept { return offLadder_; }          // prices that did not land on a tick
    uint64_t outsideWindow() const noexcept { return outsideWindow_; }  // depth past what the window keeps
    uint64_t passed() const noexcept { return passed_; }
    uint64_t ignored() const noexcept { return ignored_; }
    uint64_t crossedVenue() const noexcept { return crossedVenue_; }    // a venue book crossing itself: a defect
    uint64_t deepOnly() const noexcept { return deepOnly_; }            // deltas that moved nothing at the touch

    BookReader reader() const noexcept { return BookReader(base_, p_.maxSymbolIdx, p_.maxVenueId); }

private:
    struct Symbol {
        uint64_t venueMask = 0;          // which venues have said anything
        int64_t  sipBid = 0, sipAsk = 0;
        int64_t  lastSnapshotTs = 0;
        uint16_t status = uint16_t(TradingStatus::Trading);
        bool     touched = false;
    };

    TopOfBook* tops() noexcept { return reinterpret_cast<TopOfBook*>(base_ + sizeof(BookViewHeader)); }
    SharedLadder* ladders() noexcept {
        return reinterpret_cast<SharedLadder*>(base_ + sizeof(BookViewHeader)
                                               + size_t(p_.maxSymbolIdx + 1) * sizeof(TopOfBook));
    }
    SharedLadder& ladderFor(uint32_t s, uint16_t v) noexcept {
        return ladders()[size_t(s) * (p_.maxVenueId + 1) + v];
    }

    // A ladder is written under its own sequence number so a depth consumer never sees half of one.
    struct Held {
        SharedLadder& l;
        explicit Held(SharedLadder& r) : l(r) {
            l.seq.store(l.seq.load(std::memory_order_relaxed) + 1, std::memory_order_release);
            std::atomic_thread_fence(std::memory_order_release);
        }
        ~Held() {
            std::atomic_thread_fence(std::memory_order_release);
            l.seq.store(l.seq.load(std::memory_order_relaxed) + 1, std::memory_order_release);
        }
    };

    void onDelta(const BookDelta& d, int64_t now, auto& emit) {
        ++deltas_;
        if (d.symbolIdx == 0 || d.symbolIdx > p_.maxSymbolIdx) { ++ignored_; return; }
        if (d.venueId == 0 || d.venueId > p_.maxVenueId) { ++ignored_; return; }
        Symbol& st = sym_[d.symbolIdx];
        st.touched = true;
        st.venueMask |= (1ull << d.venueId);
        SharedLadder& L = ladderFor(d.symbolIdx, d.venueId);
        // Most deltas are deep in the book and change nothing a consumer of the top would act on.
        // Comparing this venue's own best before and after costs two instructions and skips the
        // consolidation across venues entirely.
        int64_t px0 = 0, q0 = 0, px1 = 0, q1 = 0;
        uint16_t lev0 = 0, lev1 = 0;
        const bool had = bestOf(L, d.side, px0, q0, lev0);
        {
            Held h(L);
            if (d.action == BookAction::ClearBook) { clearSide(L, BookSide::Bid); clearSide(L, BookSide::Ask); }
            else if (d.action == BookAction::ClearSide) clearSide(L, d.side);
            else setLevel(L, d.symbolIdx, d.side, d.price, d.action == BookAction::Delete ? 0 : d.qty, d.orderCount);
        }
        const bool has = bestOf(L, d.side, px1, q1, lev1);
        if (had == has && px0 == px1 && q0 == q1 && lev0 == lev1
            && d.action != BookAction::ClearBook && d.action != BookAction::ClearSide) {
            TopBody& t = tops()[d.symbolIdx].body;
            t.venueSeq = d.venueSeq;
            t.venueTs = d.venueTs ? d.venueTs : now;
            t.checksum = topChecksum(t);
            ++deepOnly_;
            return;
        }
        republish(d.symbolIdx, d.venueSeq, d.venueTs ? d.venueTs : now, now, emit);
    }

    void onSip(const Nbbo& n, int64_t now, auto& emit) {
        if (n.symbolIdx == 0 || n.symbolIdx > p_.maxSymbolIdx) { ++ignored_; return; }
        Symbol& st = sym_[n.symbolIdx];
        // Only the official figures are taken from the tape. Our own best comes from the direct
        // feeds' ladders and is never overwritten by a slower view of the same market.
        st.sipBid = n.sipBid ? n.sipBid : n.bid;
        st.sipAsk = n.sipAsk ? n.sipAsk : n.ask;
        st.touched = true;
        republish(n.symbolIdx, n.venueSeq, now, now, emit);
    }

    void onStatus(const SymbolStatus& s, int64_t now, auto& emit) {
        if (s.symbolIdx == 0 || s.symbolIdx > p_.maxSymbolIdx) { ++ignored_; return; }
        sym_[s.symbolIdx].status = uint16_t(s.status);
        sym_[s.symbolIdx].touched = true;
        republish(s.symbolIdx, s.venueSeq, s.venueTs ? s.venueTs : now, now, emit);
    }

    static void clearSide(SharedLadder& L, BookSide side) noexcept {
        if (side == BookSide::Bid) { L.bidMask = 0; std::memset(L.bidQty, 0, sizeof L.bidQty); std::memset(L.bidCount, 0, sizeof L.bidCount); }
        else                       { L.askMask = 0; std::memset(L.askQty, 0, sizeof L.askQty); std::memset(L.askCount, 0, sizeof L.askCount); }
    }

    void setLevel(SharedLadder& L, uint32_t symbolIdx, BookSide side, int64_t price, int64_t qty, uint32_t count) {
        int64_t& base = side == BookSide::Bid ? L.bidBase : L.askBase;
        int64_t& tick = side == BookSide::Bid ? L.bidTick : L.askTick;
        uint64_t& mask = side == BookSide::Bid ? L.bidMask : L.askMask;
        int64_t* q = side == BookSide::Bid ? L.bidQty : L.askQty;
        uint32_t* c = side == BookSide::Bid ? L.bidCount : L.askCount;
        const int64_t span = int64_t(BOOK_LEVELS) * (tick ? tick : 1);

        if (tick == 0) {
            if (qty == 0) return;                          // deleting from a ladder that holds nothing
            recentre(L, symbolIdx, side, price);
        }
        int64_t off = price - base;
        if (off % tick != 0) {
            // The tick band changed under the window, or the venue quoted inside the grid. Either
            // way the window has to be rebuilt around this price to hold it exactly.
            if (qty == 0) return;
            recentre(L, symbolIdx, side, price);
            off = price - base;
            if (off % tick != 0) { ++offLadder_; return; }
        }
        if (off < 0 || off >= int64_t(BOOK_LEVELS) * tick) {
            if (qty == 0) return;                          // never held it, nothing to remove
            // The window holds the touch and sixty-three ticks of depth behind it. A price past the
            // far end is deeper than that and is not kept; a price past the near end is a new touch
            // and the window follows it. Getting this the wrong way round costs the one level that
            // must never be missing.
            const bool improvesTouch = side == BookSide::Bid ? off >= int64_t(BOOK_LEVELS) * tick : off < 0;
            if (!improvesTouch) { ++outsideWindow_; return; }
            recentre(L, symbolIdx, side, price);
            off = price - base;
            if (off < 0 || off >= int64_t(BOOK_LEVELS) * tick || off % tick != 0) { ++offLadder_; return; }
        }
        const size_t i = size_t(off / tick);
        q[i] = qty;
        c[i] = count;
        if (qty > 0) mask |= (1ull << i); else mask &= ~(1ull << i);
        (void)span;
    }

    // Move the window so that `price` is the touch: for bids it sits at the top of the window with
    // the depth below it, for asks at the bottom with the depth above. Whatever survives the move
    // is carried across and the rest is counted as dropped, which is the cost of a fixed window.
    void recentre(SharedLadder& L, uint32_t symbolIdx, BookSide side, int64_t price) {
        ++rebases_;
        int64_t& base = side == BookSide::Bid ? L.bidBase : L.askBase;
        int64_t& tick = side == BookSide::Bid ? L.bidTick : L.askTick;
        uint64_t& mask = side == BookSide::Bid ? L.bidMask : L.askMask;
        int64_t* q = side == BookSide::Bid ? L.bidQty : L.askQty;
        uint32_t* c = side == BookSide::Bid ? L.bidCount : L.askCount;

        int64_t newTick = rd_.tick(symbolIdx, price);
        if (newTick <= 0) newTick = 1'000'000;             // a cent, if the tick table says nothing
        // base is derived from price by whole ticks, so price always lands on a slot exactly.
        const int64_t newBase = side == BookSide::Bid ? price - int64_t(BOOK_LEVELS - 1) * newTick : price;

        int64_t nq[BOOK_LEVELS]{};
        uint32_t nc[BOOK_LEVELS]{};
        uint64_t nmask = 0;
        if (tick > 0) {
            uint64_t m = mask;
            while (m) {
                const int i = std::countr_zero(m);
                m &= m - 1;
                const int64_t px = base + int64_t(i) * tick;
                const int64_t off = px - newBase;
                if (off < 0 || off % newTick != 0 || off / newTick >= int64_t(BOOK_LEVELS)) { ++dropped_; continue; }
                const size_t j = size_t(off / newTick);
                nq[j] = q[i]; nc[j] = c[i];
                nmask |= (1ull << j);
            }
        }
        std::memcpy(q, nq, sizeof nq);
        std::memcpy(c, nc, sizeof nc);
        mask = nmask;
        base = newBase;
        tick = newTick;
    }

    // Best price on a side: one instruction on the occupancy mask, no search.
    static bool bestOf(const SharedLadder& L, BookSide side, int64_t& px, int64_t& qty, uint16_t& levels) noexcept {
        const uint64_t mask = side == BookSide::Bid ? L.bidMask : L.askMask;
        if (!mask) { px = 0; qty = 0; levels = 0; return false; }
        const int i = side == BookSide::Bid ? 63 - std::countl_zero(mask) : std::countr_zero(mask);
        px = (side == BookSide::Bid ? L.bidBase : L.askBase) + int64_t(i) * (side == BookSide::Bid ? L.bidTick : L.askTick);
        qty = (side == BookSide::Bid ? L.bidQty : L.askQty)[i];
        levels = uint16_t(std::popcount(mask));
        return true;
    }

    // The consolidated top, recomputed across the venues that have spoken. Sixteen compares at most,
    // and only when something moved.
    template <class E>
    void republish(uint32_t symbolIdx, uint64_t venueSeq, int64_t venueTs, int64_t now, E& emit) {
        Symbol& st = sym_[symbolIdx];
        TopBody b{};
        b.symbolIdx = symbolIdx;
        b.status = st.status;
        b.sipBid = st.sipBid;
        b.sipAsk = st.sipAsk;
        b.venueSeq = venueSeq;
        b.venueTs = venueTs;
        uint64_t m = st.venueMask;
        while (m) {
            const int v = std::countr_zero(m);
            m &= m - 1;
            const SharedLadder& L = ladderFor(symbolIdx, uint16_t(v));
            int64_t bidPx = 0, bidQty = 0, askPx = 0, askQty = 0;
            uint16_t bidLev = 0, askLev = 0;
            const bool hb = bestOf(L, BookSide::Bid, bidPx, bidQty, bidLev);
            const bool ha = bestOf(L, BookSide::Ask, askPx, askQty, askLev);
            // A venue whose own book is crossed is a decoder or a feed defect, not a market.
            if (hb && ha && bidPx >= askPx) ++crossedVenue_;
            if (hb) {
                if (bidPx > b.bid) { b.bid = bidPx; b.bidQty = bidQty; b.bidVenue = uint16_t(v); b.bidLevels = bidLev; }
                else if (bidPx == b.bid) b.bidQty += bidQty;
            }
            if (ha) {
                if (!b.ask || askPx < b.ask) { b.ask = askPx; b.askQty = askQty; b.askVenue = uint16_t(v); b.askLevels = askLev; }
                else if (askPx == b.ask) b.askQty += askQty;
            }
        }
        b.checksum = topChecksum(b);

        TopOfBook& t = tops()[symbolIdx];
        // venueSeq and venueTs move on every message and say nothing about the market, so the
        // comparison is over what a consumer would act on differently.
        if (sameMarket(t.body, b)) {
            t.body.venueSeq = b.venueSeq;                 // nothing moved; the position still advances
            t.body.venueTs = b.venueTs;
            t.body.checksum = topChecksum(t.body);
            return;
        }
        ++topChanges_;
        writeTop(t, b);
        emitNbbo(b, now, emit);
    }

    static bool sameMarket(const TopBody& a, const TopBody& b) noexcept {
        return a.bid == b.bid && a.ask == b.ask && a.bidQty == b.bidQty && a.askQty == b.askQty
            && a.sipBid == b.sipBid && a.sipAsk == b.sipAsk && a.status == b.status
            && a.bidVenue == b.bidVenue && a.askVenue == b.askVenue
            && a.bidLevels == b.bidLevels && a.askLevels == b.askLevels;
    }

    static void writeTop(TopOfBook& t, const TopBody& b) noexcept {
        t.seq.store(t.seq.load(std::memory_order_relaxed) + 1, std::memory_order_release);
        std::atomic_thread_fence(std::memory_order_release);
        t.body = b;
        std::atomic_thread_fence(std::memory_order_release);
        t.seq.store(t.seq.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }

    template <class E>
    void emitNbbo(const TopBody& b, int64_t now, E& emit) {
        Frame<Nbbo> n;
        n.init();
        n.header.sourceId = p_.sourceId;
        n.header.streamId = p_.feedId;
        n.header.originTs = now;
        n.body.symbolIdx = b.symbolIdx;
        n.body.bid = b.bid; n.body.ask = b.ask;
        n.body.bidQty = b.bidQty; n.body.askQty = b.askQty;
        n.body.bidVenue = b.bidVenue; n.body.askVenue = b.askVenue;
        n.body.sipBid = b.sipBid; n.body.sipAsk = b.sipAsk;
        n.body.venueSeq = b.venueSeq;
        ++nbbos_;
        emit(&n.header);
    }

    template <class E>
    void emitSnapshot(uint32_t symbolIdx, uint16_t venueId, int64_t now, E& emit) {
        const SharedLadder& L = ladderFor(symbolIdx, venueId);
        Frame<BookSnapshot> s;
        s.init();
        s.header.sourceId = p_.sourceId;
        s.header.streamId = p_.feedId;
        s.header.originTs = now;
        s.body.symbolIdx = symbolIdx;
        s.body.venueId = venueId;
        s.body.venueSeq = tops()[symbolIdx].body.venueSeq;
        // Best first on both sides: a late joiner cares about the touch and may not care about
        // level sixteen, and a truncated snapshot should lose the far end rather than the near one.
        uint8_t n = 0;
        uint64_t m = L.bidMask;
        while (m && n < 16) {
            const int i = 63 - std::countl_zero(m);
            m &= ~(1ull << i);
            s.body.bidPx[n] = L.bidBase + int64_t(i) * L.bidTick;
            s.body.bidQty[n] = L.bidQty[i];
            ++n;
        }
        s.body.bidCount = n;
        n = 0;
        m = L.askMask;
        while (m && n < 16) {
            const int i = std::countr_zero(m);
            m &= m - 1;
            s.body.askPx[n] = L.askBase + int64_t(i) * L.askTick;
            s.body.askQty[n] = L.askQty[i];
            ++n;
        }
        s.body.askCount = n;
        ++snapshots_;
        emit(&s.header);
    }

    const refdata::RefData& rd_;
    Params p_;
    std::byte* base_;
    std::vector<Symbol> sym_;
    uint64_t applied_ = 0, deltas_ = 0, topChanges_ = 0, nbbos_ = 0, snapshots_ = 0;
    uint64_t rebases_ = 0, dropped_ = 0, offLadder_ = 0, passed_ = 0, ignored_ = 0, crossedVenue_ = 0;
    uint64_t deepOnly_ = 0, outsideWindow_ = 0;
};

} // namespace trading::md
