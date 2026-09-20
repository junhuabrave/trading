// core/md/itch.hpp : Nasdaq TotalView-ITCH 5.0, decoded.
//
// A decoder turns one venue's bytes into our normalised messages and nothing else. It is the only
// component that knows ITCH from PITCH; everything below it (receiver, capture, arbitrator) reads
// the envelope only, and everything above it (selector, book builder, engines) reads our messages
// only. That is what makes a new venue a new decoder rather than a new stack.
//
// Deterministic by construction. It has no clock, no randomness and no I/O: the same packets in the
// same order produce byte-identical output, in production, in replay and in the simulator. Its only
// state is what ITCH forces it to keep.
//
// Two things ITCH forces. First, an order reference table: ITCH names an order once and then refers
// to it by a 64-bit reference, so an execution or a cancel is meaningless without the add that came
// before it. Second, price-level aggregates: our BookDelta carries the absolute quantity at a level
// after the change, because a consumer that has to accumulate deltas cannot join late and cannot
// recover from a gap. Turning order events into level events is therefore the decoder's job, and it
// is why the decoder holds the only per-venue book in the system. The ladders, the consolidated
// view and the NBBO across venues are the book builder's (MD-8).
//
// Wire format, from the published specification: big-endian throughout, a six-byte timestamp of
// nanoseconds since midnight Eastern, prices as uint32 with four implied decimals ($0.0001), stock
// symbols as eight space-padded characters, and a two-byte stock locate that indexes the Stock
// Directory messages sent before the open. Sizes are fixed per message type and are asserted here.
#pragma once
#include "packet.hpp"
#include "flatmap.hpp"
#include "refdata.hpp"
#include <array>
#include <cstring>
#include <functional>

namespace trading::md::itch {

// ---- wire primitives -------------------------------------------------------------------------
inline uint16_t be16(const uint8_t* p) noexcept { return uint16_t(uint16_t(p[0]) << 8 | p[1]); }
inline uint32_t be32(const uint8_t* p) noexcept {
    return uint32_t(p[0]) << 24 | uint32_t(p[1]) << 16 | uint32_t(p[2]) << 8 | uint32_t(p[3]);
}
inline uint64_t be48(const uint8_t* p) noexcept {   // the timestamp: ns since midnight
    return uint64_t(p[0]) << 40 | uint64_t(p[1]) << 32 | uint64_t(p[2]) << 24 |
           uint64_t(p[3]) << 16 | uint64_t(p[4]) << 8  | uint64_t(p[5]);
}
inline uint64_t be64(const uint8_t* p) noexcept { return uint64_t(be32(p)) << 32 | be32(p + 4); }

inline void putBe16(uint8_t* p, uint16_t v) noexcept { p[0] = uint8_t(v >> 8); p[1] = uint8_t(v); }
inline void putBe32(uint8_t* p, uint32_t v) noexcept { for (int i = 0; i < 4; ++i) p[i] = uint8_t(v >> (24 - 8 * i)); }
inline void putBe48(uint8_t* p, uint64_t v) noexcept { for (int i = 0; i < 6; ++i) p[i] = uint8_t(v >> (40 - 8 * i)); }
inline void putBe64(uint8_t* p, uint64_t v) noexcept { for (int i = 0; i < 8; ++i) p[i] = uint8_t(v >> (56 - 8 * i)); }

// ITCH prices are uint32 with four implied decimals; ours are int64 at 1e-8.
inline constexpr int64_t PRICE_SCALE = 10'000;
inline constexpr int64_t toPrice(uint32_t itch) noexcept { return int64_t(itch) * PRICE_SCALE; }
inline constexpr uint32_t fromPrice(int64_t px) noexcept { return uint32_t(px / PRICE_SCALE); }

// ---- message types ---------------------------------------------------------------------------
namespace msg {
inline constexpr char SystemEvent        = 'S';
inline constexpr char StockDirectory     = 'R';
inline constexpr char TradingAction      = 'H';
inline constexpr char RegSho             = 'Y';
inline constexpr char ParticipantPos     = 'L';
inline constexpr char MwcbLevel          = 'V';
inline constexpr char MwcbStatus         = 'W';
inline constexpr char IpoQuotingPeriod   = 'K';
inline constexpr char AddOrder           = 'A';
inline constexpr char AddOrderMpid       = 'F';
inline constexpr char OrderExecuted      = 'E';
inline constexpr char OrderExecutedPrice = 'C';
inline constexpr char OrderCancel        = 'X';
inline constexpr char OrderDelete        = 'D';
inline constexpr char OrderReplace       = 'U';
inline constexpr char TradeNonCross      = 'P';
inline constexpr char CrossTrade         = 'Q';
inline constexpr char BrokenTrade        = 'B';
inline constexpr char Noii               = 'I';
inline constexpr char LuldAuctionCollar  = 'J';
inline constexpr char OperationalHalt    = 'h';
}

// What we can say about a print from ITCH alone. A last-sale consumer filters on these: a hidden
// match moves no book, a cross is an auction rather than continuous trading, a non-printable
// execution is not a last sale, and a broken trade un-does one that was already reported.
namespace TradeCondition {
inline constexpr uint8_t nonDisplayed = 1u << 0;   // 'P': matched against hidden liquidity
inline constexpr uint8_t cross        = 1u << 1;   // 'Q'
inline constexpr uint8_t opening      = 1u << 2;
inline constexpr uint8_t closing      = 1u << 3;
inline constexpr uint8_t halt         = 1u << 4;   // the halt or IPO cross
inline constexpr uint8_t broken       = 1u << 5;   // 'B': a trade already reported is void
inline constexpr uint8_t nonPrintable = 1u << 6;   // 'C' with printable = N
}

// Fixed length of each message, including its one-byte type. Anything else is a malformed block.
inline constexpr int messageLength(char t) noexcept {
    switch (t) {
        case msg::SystemEvent:        return 12;
        case msg::StockDirectory:     return 39;
        case msg::TradingAction:      return 25;
        case msg::RegSho:             return 20;
        case msg::ParticipantPos:     return 26;
        case msg::MwcbLevel:          return 35;
        case msg::MwcbStatus:         return 12;
        case msg::IpoQuotingPeriod:   return 28;
        case msg::AddOrder:           return 36;
        case msg::AddOrderMpid:       return 40;
        case msg::OrderExecuted:      return 31;
        case msg::OrderExecutedPrice: return 36;
        case msg::OrderCancel:        return 23;
        case msg::OrderDelete:        return 19;
        case msg::OrderReplace:       return 35;
        case msg::TradeNonCross:      return 44;
        case msg::CrossTrade:         return 40;
        case msg::BrokenTrade:        return 19;
        case msg::Noii:               return 50;
        case msg::LuldAuctionCollar:  return 35;
        case msg::OperationalHalt:    return 21;
        default:                      return -1;
    }
}
inline constexpr size_t MAX_MESSAGE = 50;

// MoldUDP64 frames each message with a two-byte big-endian length. The packet envelope already
// said how many there are and where the first one sits in the venue's sequence, so walking the
// payload is the decoder's only framing job.
template <class F>
inline size_t forEachBlock(const void* payload, size_t len, F&& fn) {
    const auto* p = static_cast<const uint8_t*>(payload);
    size_t off = 0, n = 0;
    while (off + 2 <= len) {
        const uint16_t blockLen = be16(p + off);
        if (blockLen == 0 || off + 2 + blockLen > len) break;
        fn(p + off + 2, size_t(blockLen));
        ++n;
        off += 2 + blockLen;
    }
    return n;
}

// Writes MoldUDP64 message blocks into a packet payload. Used by the simulator and by any tool
// that has to produce a sample day; the decoder itself never writes.
class BlockWriter {
public:
    explicit BlockWriter(std::byte* payload, size_t cap) : p_(reinterpret_cast<uint8_t*>(payload)), cap_(cap) {}
    bool add(const void* msg, size_t len) noexcept {
        if (off_ + 2 + len > cap_) return false;
        putBe16(p_ + off_, uint16_t(len));
        std::memcpy(p_ + off_ + 2, msg, len);
        off_ += 2 + len;
        ++count_;
        return true;
    }
    size_t size() const noexcept { return off_; }
    uint16_t count() const noexcept { return count_; }
private:
    uint8_t* p_; size_t cap_, off_ = 0; uint16_t count_ = 0;
};

// ---- the decoder -----------------------------------------------------------------------------
class Decoder {
public:
    struct Params {
        uint32_t feedId = 0;
        uint16_t venueId = 0;
        uint16_t sourceId = 0;
        // ITCH stamps nanoseconds since the venue day's midnight; this is that midnight, so every
        // venueTs downstream is an ordinary epoch timestamp and nothing has to know about Eastern.
        int64_t  sessionMidnightNs = 0;
        // Nasdaq carries a few million live orders at the peak of a busy day. The production sizing
        // in document 3 is 2^26; the default here is what a test or a single-symbol replay needs.
        size_t   maxOrders = 1u << 22;
        size_t   maxLevels = 1u << 20;
    };
    // The callback is a template parameter, not a std::function: a decoder is called once per
    // message on every feed all day, and an indirect call with a constructed wrapper per message is
    // not a price worth paying for a uniformity nobody needs.
    Decoder(const refdata::Snapshot& snap, Params p)
        : p_(p), snap_(snap), orders_(p.maxOrders), levels_(p.maxLevels) { locate_.fill(0); }

    // One arbitrated packet. skip is what the arbitrator says the consumer already has, so a
    // recovery block that straddles the cursor decodes only its new tail.
    template <class E>
    size_t decode(const PacketHeader& h, uint16_t skip, E&& emit) {
        const auto* payload = reinterpret_cast<const std::byte*>(&h) + sizeof(PacketHeader);
        uint64_t seq = h.firstSeq;
        size_t i = 0, decoded = 0;
        const bool recovered = (h.flags & (PacketFlags::retransmit | PacketFlags::snapshot)) != 0;
        forEachBlock(payload, h.payloadLen, [&](const uint8_t* m, size_t len) {
            if (i++ >= skip) { decoded += one(m, len, seq, recovered, emit); }
            ++seq;
        });
        flush(emit);                    // the last message of a packet is marked as such
        ++packets_;
        return decoded;
    }

    // One message on its own, for a golden file that is a bare ITCH stream rather than packets.
    template <class E>
    size_t decodeMessage(const uint8_t* m, size_t len, uint64_t venueSeq, bool endOfPacket, E&& emit) {
        size_t n = one(m, len, venueSeq, false, emit);
        if (endOfPacket) flush(emit);
        return n;
    }
    // Ends whatever packet is open; call it when a stream ends so the last message is marked.
    template <class E> void finish(E&& emit) { flush(emit); }

    // ---- what the decoder knows, for tests, tools and the book builder that follows
    uint32_t symbolOf(uint16_t stockLocate) const noexcept { return locate_[stockLocate]; }
    int64_t levelQty(uint32_t symbolIdx, BookSide side, int64_t price) const noexcept {
        const Level* l = levels_.find(levelKey(symbolIdx, side, price));
        return l ? l->qty : 0;
    }
    size_t liveOrders() const noexcept { return orders_.size(); }
    size_t liveLevels() const noexcept { return levels_.size(); }
    // Every level the decoder currently believes in, for a test that computes the same thing a
    // different way, or for the book builder loading from a running decoder.
    template <class F> void forEachLevel(F&& f) const {
        levels_.forEach([&](const util::Key128& k, const Level& l) {
            f(uint32_t(k.hi >> 8), BookSide(uint8_t(k.hi & 0xFF)), int64_t(k.lo), l.qty, l.count);
        });
    }
    bool shortSaleRestricted(uint32_t symbolIdx) const noexcept {
        return symbolIdx < ssr_.size() && ssr_[symbolIdx];
    }

    uint64_t packets() const noexcept { return packets_; }
    uint64_t messages() const noexcept { return messages_; }     // ITCH messages consumed
    uint64_t emitted() const noexcept { return emitted_; }       // normalised frames produced
    uint64_t malformed() const noexcept { return malformed_; }   // wrong length for its type
    uint64_t unknownType() const noexcept { return unknownType_; }
    uint64_t unmappedLocate() const noexcept { return unmappedLocate_; }   // no Stock Directory, or not ours
    uint64_t missingOrder() const noexcept { return missingOrder_; }       // a reference we never saw added
    uint64_t negativeLevel() const noexcept { return negativeLevel_; }     // a level driven below zero: a real defect
    uint64_t trades() const noexcept { return trades_; }
    uint64_t deltas() const noexcept { return deltas_; }
    uint64_t statuses() const noexcept { return statuses_; }
    uint64_t imbalances() const noexcept { return imbalances_; }
    uint64_t ignored() const noexcept { return ignored_; }       // understood, nothing of ours to say
    uint64_t regSho() const noexcept { return regSho_; }
    uint64_t directoryEntries() const noexcept { return directory_; }

private:
    struct Order { int64_t price; int64_t shares; uint32_t symbolIdx; uint8_t side; };
    struct Level { int64_t qty; uint32_t count; };

    static util::Key128 levelKey(uint32_t symbolIdx, BookSide side, int64_t price) noexcept {
        return {uint64_t(symbolIdx) << 8 | uint64_t(uint8_t(side)), uint64_t(price)};
    }

    // Every emitted frame is held back by one so the last one of a packet can be marked. A consumer
    // that acts per packet rather than per message needs that boundary, and only the decoder knows
    // where it falls.
    template <class M, class E>
    void push(Frame<M>& f, bool recovered, E& emit) {
        f.header.sourceId = p_.sourceId;
        f.header.streamId = p_.feedId;
        if (recovered) f.header.flags = uint16_t(f.header.flags | FrameFlags::replayed);
        if (pending_) emit(reinterpret_cast<const FrameHeader*>(hold_));
        std::memcpy(hold_, &f, f.header.frameLength);
        pending_ = true;
        ++emitted_;
    }
    template <class E> void flush(E& emit) {
        if (!pending_) return;
        auto* h = reinterpret_cast<FrameHeader*>(hold_);
        h->flags = uint16_t(h->flags | FrameFlags::endOfPacket);
        if (auto* d = const_cast<BookDelta*>(as<BookDelta>(h))) d->flags = uint8_t(d->flags | BookFlags::endOfPacket);
        emit(h);
        pending_ = false;
    }

    // A level changed by delta shares. Emits the absolute quantity that remains, or a Delete.
    template <class E>
    void moveLevel(uint32_t symbolIdx, BookSide side, int64_t price, int64_t delta, int32_t countDelta,
                   uint64_t venueSeq, int64_t ts, bool recovered, E& emit) {
        const util::Key128 k = levelKey(symbolIdx, side, price);
        Level& l = levels_[k];
        l.qty += delta;
        l.count = uint32_t(int32_t(l.count) + countDelta < 0 ? 0 : int32_t(l.count) + countDelta);
        if (l.qty < 0) { ++negativeLevel_; l.qty = 0; }      // counted loudly: this is never normal
        Frame<BookDelta> d; d.init();
        d.body.symbolIdx = symbolIdx; d.body.venueId = p_.venueId; d.body.side = side;
        d.body.price = price; d.body.qty = l.qty; d.body.orderCount = l.count;
        d.body.venueSeq = venueSeq; d.body.venueTs = ts;
        if (l.qty == 0) { d.body.action = BookAction::Delete; levels_.erase(k); }
        else d.body.action = BookAction::Set;
        ++deltas_;
        push(d, recovered, emit);
    }

    template <class E>
    void emitTrade(uint32_t symbolIdx, BookSide aggressor, int64_t price, int64_t qty, uint64_t matchNo,
                   uint8_t conditions, uint64_t venueSeq, int64_t ts, bool recovered, E& emit) {
        Frame<Trade> t; t.init();
        t.body.symbolIdx = symbolIdx; t.body.venueId = p_.venueId; t.body.aggressorSide = aggressor;
        t.body.conditions = conditions; t.body.price = price; t.body.qty = qty;
        t.body.venueSeq = venueSeq; t.body.venueTs = ts; t.body.tradeId = matchNo;
        ++trades_;
        push(t, recovered, emit);
    }

    static BookSide sideOf(uint8_t c) noexcept { return c == 'B' ? BookSide::Bid : BookSide::Ask; }
    int64_t stamp(const uint8_t* m) const noexcept { return p_.sessionMidnightNs + int64_t(be48(m + 5)); }

    template <class E> size_t one(const uint8_t* m, size_t len, uint64_t venueSeq, bool recovered, E& emit);

    Params p_;
    const refdata::Snapshot& snap_;
    util::FlatMap<uint64_t, Order> orders_;
    util::FlatMap<util::Key128, Level> levels_;
    std::array<uint32_t, 65536> locate_{};
    std::array<uint8_t, 65536> ssr_{};
    alignas(16) std::byte hold_[sizeof(FrameHeader) + 128]{};
    bool pending_ = false;
    uint64_t packets_ = 0, messages_ = 0, emitted_ = 0, malformed_ = 0, unknownType_ = 0;
    uint64_t unmappedLocate_ = 0, missingOrder_ = 0, negativeLevel_ = 0;
    uint64_t trades_ = 0, deltas_ = 0, statuses_ = 0, imbalances_ = 0, ignored_ = 0;
    uint64_t regSho_ = 0, directory_ = 0;
};

// ---- one message ------------------------------------------------------------------------------
// Offsets are from the published specification: every message opens with type(1), stockLocate(2),
// trackingNumber(2) and timestamp(6), so its own fields start at byte 11.
template <class E>
size_t Decoder::one(const uint8_t* m, size_t len, uint64_t venueSeq, bool recovered, E& emit) {
    ++messages_;
    const char type = char(m[0]);
    const int want = messageLength(type);
    if (want < 0) { ++unknownType_; return 0; }
    if (len != size_t(want)) { ++malformed_; return 0; }

    const uint16_t locate = be16(m + 1);
    const int64_t ts = stamp(m);
    const size_t before = emitted_;

    switch (type) {
    case msg::StockDirectory: {
        // The only message that tells us what a stock locate means. Nasdaq carries every listed
        // name; we carry the ones in our snapshot, so most entries resolve to nothing and the
        // messages that follow for them are counted rather than decoded.
        const char* raw = reinterpret_cast<const char*>(m + 11);
        size_t n = 8; while (n > 0 && raw[n - 1] == ' ') --n;
        locate_[locate] = snap_.lookupTicker(std::string_view(raw, n));
        ++directory_;
        break;
    }
    case msg::AddOrder:
    case msg::AddOrderMpid: {
        const uint32_t sym = locate_[locate];
        if (!sym) { ++unmappedLocate_; break; }
        const uint64_t ref = be64(m + 11);
        const BookSide side = sideOf(m[19]);
        const int64_t shares = int64_t(be32(m + 20));
        const int64_t price = toPrice(be32(m + 32));
        orders_.insert(ref, Order{price, shares, sym, uint8_t(side)});
        moveLevel(sym, side, price, shares, +1, venueSeq, ts, recovered, emit);
        break;
    }
    case msg::OrderExecuted:
    case msg::OrderExecutedPrice: {
        const uint64_t ref = be64(m + 11);
        Order* o = orders_.find(ref);
        if (!o) { ++missingOrder_; break; }
        const int64_t asked = int64_t(be32(m + 19));
        const int64_t exec = asked < o->shares ? asked : o->shares;
        const uint64_t matchNo = be64(m + 23);
        const bool priced = type == msg::OrderExecutedPrice;
        const int64_t px = priced ? toPrice(be32(m + 32)) : o->price;
        uint8_t cond = 0;
        if (priced && m[31] != 'Y') cond = uint8_t(cond | TradeCondition::nonPrintable);
        // The order on the book is the resting side, so the aggressor is the other one.
        const BookSide resting = BookSide(o->side);
        const BookSide aggressor = resting == BookSide::Bid ? BookSide::Ask : BookSide::Bid;
        const uint32_t sym = o->symbolIdx;
        const int64_t restPx = o->price;
        o->shares -= exec;
        const bool gone = o->shares <= 0;
        emitTrade(sym, aggressor, px, exec, matchNo, cond, venueSeq, ts, recovered, emit);
        // The book loses the shares at the order's own price, whatever price the print carried.
        moveLevel(sym, resting, restPx, -exec, gone ? -1 : 0, venueSeq, ts, recovered, emit);
        if (gone) orders_.erase(ref);
        break;
    }
    case msg::OrderCancel: {
        const uint64_t ref = be64(m + 11);
        Order* o = orders_.find(ref);
        if (!o) { ++missingOrder_; break; }
        const int64_t asked = int64_t(be32(m + 19));
        const int64_t gone = asked < o->shares ? asked : o->shares;
        o->shares -= gone;
        const bool empty = o->shares <= 0;
        const uint32_t sym = o->symbolIdx; const BookSide side = BookSide(o->side); const int64_t px = o->price;
        moveLevel(sym, side, px, -gone, empty ? -1 : 0, venueSeq, ts, recovered, emit);
        if (empty) orders_.erase(ref);
        break;
    }
    case msg::OrderDelete: {
        const uint64_t ref = be64(m + 11);
        Order* o = orders_.find(ref);
        if (!o) { ++missingOrder_; break; }
        const Order dead = *o;
        orders_.erase(ref);
        moveLevel(dead.symbolIdx, BookSide(dead.side), dead.price, -dead.shares, -1, venueSeq, ts, recovered, emit);
        break;
    }
    case msg::OrderReplace: {
        // A replace keeps the original's stock and side and takes a new reference, quantity and
        // price. It is two level changes, and the old one has to be undone before the new one is
        // applied or a replace within a level nets to nothing visible.
        const uint64_t oldRef = be64(m + 11), newRef = be64(m + 19);
        Order* o = orders_.find(oldRef);
        if (!o) { ++missingOrder_; break; }
        const Order dead = *o;
        orders_.erase(oldRef);
        const int64_t shares = int64_t(be32(m + 27));
        const int64_t price = toPrice(be32(m + 31));
        orders_.insert(newRef, Order{price, shares, dead.symbolIdx, dead.side});
        moveLevel(dead.symbolIdx, BookSide(dead.side), dead.price, -dead.shares, -1, venueSeq, ts, recovered, emit);
        moveLevel(dead.symbolIdx, BookSide(dead.side), price, shares, +1, venueSeq, ts, recovered, emit);
        break;
    }
    case msg::TradeNonCross: {
        // A match against non-displayed liquidity: a print, and no book change, because the order
        // it matched was never on the book we built. The specification notes that Nasdaq pins the
        // side indicator to B, so the aggressor here is not to be relied on downstream.
        const uint32_t sym = locate_[locate];
        if (!sym) { ++unmappedLocate_; break; }
        const BookSide hidden = sideOf(m[19]);
        const BookSide aggressor = hidden == BookSide::Bid ? BookSide::Ask : BookSide::Bid;
        emitTrade(sym, aggressor, toPrice(be32(m + 32)), int64_t(be32(m + 20)), be64(m + 36),
                  TradeCondition::nonDisplayed, venueSeq, ts, recovered, emit);
        break;
    }
    case msg::CrossTrade: {
        const uint32_t sym = locate_[locate];
        if (!sym) { ++unmappedLocate_; break; }
        uint8_t cond = TradeCondition::cross;
        switch (m[39]) {
            case 'O': cond = uint8_t(cond | TradeCondition::opening); break;
            case 'C': cond = uint8_t(cond | TradeCondition::closing); break;
            case 'H': cond = uint8_t(cond | TradeCondition::halt); break;
            default: break;                                  // 'I': the intraday or extended cross
        }
        // An auction has no aggressor, so aggressorSide is left unset rather than guessed.
        Frame<Trade> t; t.init();
        t.body.symbolIdx = sym; t.body.venueId = p_.venueId; t.body.conditions = cond;
        t.body.price = toPrice(be32(m + 27)); t.body.qty = int64_t(be64(m + 11));
        t.body.venueSeq = venueSeq; t.body.venueTs = ts; t.body.tradeId = be64(m + 31);
        ++trades_;
        push(t, recovered, emit);
        break;
    }
    case msg::BrokenTrade: {
        const uint32_t sym = locate_[locate];
        if (!sym) { ++unmappedLocate_; break; }
        // Price and quantity are not repeated: the match number is what identifies the trade being
        // un-done, and a consumer keeping a print tape looks it up by that.
        emitTrade(sym, BookSide(0), 0, 0, be64(m + 11), TradeCondition::broken, venueSeq, ts, recovered, emit);
        break;
    }
    case msg::TradingAction: {
        const uint32_t sym = locate_[locate];
        if (!sym) { ++unmappedLocate_; break; }
        Frame<SymbolStatus> st; st.init();
        st.body.symbolIdx = sym; st.body.venueId = p_.venueId; st.body.venueTs = ts; st.body.venueSeq = venueSeq;
        switch (m[19]) {
            case 'T': st.body.status = TradingStatus::Trading; break;
            case 'H': st.body.status = TradingStatus::Halted; break;
            case 'P': st.body.status = TradingStatus::Paused; break;
            case 'Q': st.body.status = TradingStatus::PreOpen; break;   // quotation only, no trading
            default:  st.body.status = TradingStatus::Halted; break;
        }
        st.body.reason = uint8_t(m[21]);                     // first character of the four-byte reason
        ++statuses_;
        push(st, recovered, emit);
        break;
    }
    case msg::RegSho: {
        // The short-sale restriction belongs to reference data, not to the book, so it is kept here
        // and read through shortSaleRestricted(); sequencing it onto the reference stream is the
        // secmaster's job, not the decoder's.
        const uint32_t sym = locate_[locate];
        if (!sym) { ++unmappedLocate_; break; }
        if (sym < ssr_.size()) ssr_[sym] = uint8_t(m[19] != '0');
        ++regSho_;
        break;
    }
    case msg::Noii: {
        const uint32_t sym = locate_[locate];
        if (!sym) { ++unmappedLocate_; break; }
        Frame<Imbalance> im; im.init();
        im.body.symbolIdx = sym; im.body.venueId = p_.venueId;
        im.body.pairedQty = int64_t(be64(m + 11));
        im.body.imbalanceQty = int64_t(be64(m + 19));
        switch (m[27]) {
            case 'B': im.body.imbalanceSide = BookSide::Bid; break;
            case 'S': im.body.imbalanceSide = BookSide::Ask; break;
            default:  break;                                 // 'N' none, 'O' insufficient orders
        }
        im.body.auctionType = uint8_t(m[48]);
        im.body.refPrice = toPrice(be32(m + 44));
        im.body.venueTs = ts; im.body.venueSeq = venueSeq;
        ++imbalances_;
        push(im, recovered, emit);
        break;
    }
    case msg::SystemEvent: {
        Frame<VenueStatus> v; v.init();
        v.body.venueId = p_.venueId; v.body.venueTs = ts;
        switch (m[11]) {
            case 'O': v.body.status = TradingStatus::PreOpen; break;    // start of messages
            case 'S': v.body.status = TradingStatus::PreOpen; break;    // start of system hours
            case 'Q': v.body.status = TradingStatus::Trading; break;    // start of market hours
            case 'M': v.body.status = TradingStatus::Closed; break;     // end of market hours
            case 'E': v.body.status = TradingStatus::Closed; break;     // end of system hours
            case 'C': v.body.status = TradingStatus::Closed; break;     // end of messages
            default:  v.body.status = TradingStatus::Closed; break;
        }
        ++statuses_;
        push(v, recovered, emit);
        break;
    }
    default:
        // Understood, and nothing of ours to say: participant positions, the market-wide circuit
        // breaker levels and status, IPO quoting periods, LULD auction collars, operational halts.
        ++ignored_;
        break;
    }
    return emitted_ - before;
}

} // namespace trading::md::itch
