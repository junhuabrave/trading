// core/md/sip.hpp : the consolidated tape, decoded, and what it says against the direct feeds.
//
// The SIPs (UTP for Nasdaq-listed names, CTA for the rest) publish the official National Best Bid
// and Offer. It is slower than the direct feeds and it is not what a fast system trades on, but it
// is what a best-execution review is measured against, so the system has to carry it, compare it
// with what it actually saw, and keep the disagreements on the log. That comparison is the reason
// this exists; the NbboMonitor below is the half that matters.
//
// On the wire format. ITCH's specification is published and core/md/itch.hpp uses its real byte
// offsets. The UQDF and CQS binary output specifications are not: they come with a subscriber
// agreement. What is public is their shape, and that is what is implemented here - a per-message
// header of category, type, participant, sequence, timestamp and symbol, then a quote body
// carrying both the participant's own quote and the national best with the participants that set
// it, a trade body with an explicit price denominator, and an administrative body for trading
// actions. The offsets below are this file's own, and the encoder that writes them lives beside
// the decoder that reads them precisely so that substituting the real ones is a change to one
// file. Until a vendor sample day has been run through it, that substitution is outstanding work
// and is named as such in the delivery plan.
//
// The two dialects differ in the participant codes, in which conditions suppress a quote from the
// national best, and in the round-lot convention for sizes. Those differences are parameters here,
// not separate parsers, because the consolidation logic is identical and duplicating it would mean
// two places to fix a best-execution bug.
#pragma once
#include "itch.hpp"
#include "refdata.hpp"
#include <array>
#include <functional>
#include <vector>

namespace trading::md::sip {

using itch::be16;
using itch::be32;
using itch::be64;
using itch::putBe16;
using itch::putBe32;
using itch::putBe64;

enum class Dialect : uint8_t { Utp = 1, Cta = 2 };

namespace category {
inline constexpr char Quote = 'Q';
inline constexpr char Trade = 'T';
inline constexpr char Admin = 'A';
}
namespace type {
inline constexpr char ShortQuote = 'Q';   // sizes in round lots
inline constexpr char LongQuote  = 'B';   // sizes in shares
inline constexpr char ShortTrade = 'S';
inline constexpr char LongTrade  = 'L';
inline constexpr char Action     = 'H';   // trading action for one symbol
}

// Header, 24 bytes, common to every message.
namespace off {
inline constexpr size_t category = 0, type = 1, participant = 2, reserved = 3;
inline constexpr size_t sequence = 4, timestamp = 8, symbol = 16, symbolLen = 8;
inline constexpr size_t body = 24;
// Quote body, 40 bytes.
inline constexpr size_t bidPx = body + 0, bidSz = body + 4, askPx = body + 8, askSz = body + 12;
inline constexpr size_t nbboIndicator = body + 16, bidParticipant = body + 17,
                        askParticipant = body + 18, denominator = body + 19;
inline constexpr size_t nbBidPx = body + 20, nbBidSz = body + 24, nbAskPx = body + 28, nbAskSz = body + 32;
inline constexpr size_t quoteCondition = body + 36;
// Trade body, 16 bytes.
inline constexpr size_t tradePx = body + 0, tradeVol = body + 4, tradeDenom = body + 8,
                        tradeCondition = body + 9;
// Admin body, 8 bytes.
inline constexpr size_t actionType = body + 0, actionReason = body + 1;
}
inline constexpr size_t QUOTE_LEN = 64, TRADE_LEN = 40, ADMIN_LEN = 32;
inline constexpr size_t MAX_MESSAGE = QUOTE_LEN;

inline int messageLength(char cat, char) noexcept {
    switch (cat) {
        case category::Quote: return int(QUOTE_LEN);
        case category::Trade: return int(TRADE_LEN);
        case category::Admin: return int(ADMIN_LEN);
        default: return -1;
    }
}

// The SIPs carry a denominator code rather than assuming a scale, because the tape has outlived
// several of them. 'B' is the four-decimal one everything uses today.
inline int64_t toPrice(uint32_t raw, uint8_t denom) noexcept {
    switch (denom) {
        case 'A': return int64_t(raw) * 1'000'000;      // two decimals
        case 'B': return int64_t(raw) * 10'000;         // four decimals
        case 'C': return int64_t(raw) * 100;            // six decimals
        case 'D': return int64_t(raw);                  // eight decimals
        default:  return int64_t(raw) * 10'000;
    }
}
inline uint32_t fromPrice(int64_t px, uint8_t denom = 'B') noexcept {
    switch (denom) {
        case 'A': return uint32_t(px / 1'000'000);
        case 'C': return uint32_t(px / 100);
        case 'D': return uint32_t(px);
        default:  return uint32_t(px / 10'000);
    }
}

// The national best is suppressed for quotes in certain conditions; a consumer that ignores this
// will read a locked or crossed market that the tape never meant to publish.
namespace QuoteCondition {
inline constexpr char Normal        = 'A';
inline constexpr char Closed        = 'C';
inline constexpr char NewsDissemination = 'D';
inline constexpr char Halted        = 'H';
inline constexpr char NonFirm       = 'N';   // not eligible to set the national best
inline constexpr char OpeningQuote  = 'O';
}
inline bool setsNationalBest(char condition) noexcept {
    switch (condition) {
        case QuoteCondition::NonFirm:
        case QuoteCondition::Halted:
        case QuoteCondition::Closed:
        case QuoteCondition::NewsDissemination:
            return false;
        default: return true;
    }
}

// ---- encoder ---------------------------------------------------------------------------------
// Beside the decoder on purpose: the layout above has exactly one definition, so the day a real
// specification arrives, both sides move together.
class Writer {
public:
    explicit Writer(uint8_t* buf) : b_(buf) {}
    void header(char cat, char t, uint8_t participant, uint32_t seq, uint64_t tsNs, std::string_view sym) noexcept {
        std::memset(b_, 0, MAX_MESSAGE);
        b_[off::category] = uint8_t(cat); b_[off::type] = uint8_t(t); b_[off::participant] = participant;
        putBe32(b_ + off::sequence, seq);
        putBe64(b_ + off::timestamp, tsNs);
        std::memset(b_ + off::symbol, ' ', off::symbolLen);
        std::memcpy(b_ + off::symbol, sym.data(), sym.size() < off::symbolLen ? sym.size() : off::symbolLen);
    }
    size_t quote(int64_t bid, int64_t bidQty, int64_t ask, int64_t askQty,
                 int64_t nbBid, int64_t nbBidQty, int64_t nbAsk, int64_t nbAskQty,
                 uint8_t bidPart, uint8_t askPart, char condition, bool shortForm) noexcept {
        const int64_t unit = shortForm ? 100 : 1;
        putBe32(b_ + off::bidPx, fromPrice(bid));   putBe32(b_ + off::bidSz, uint32_t(bidQty / unit));
        putBe32(b_ + off::askPx, fromPrice(ask));   putBe32(b_ + off::askSz, uint32_t(askQty / unit));
        b_[off::nbboIndicator] = uint8_t(nbBid || nbAsk ? '4' : '0');   // 4: the message carries the national best
        b_[off::bidParticipant] = bidPart; b_[off::askParticipant] = askPart; b_[off::denominator] = 'B';
        putBe32(b_ + off::nbBidPx, fromPrice(nbBid)); putBe32(b_ + off::nbBidSz, uint32_t(nbBidQty / unit));
        putBe32(b_ + off::nbAskPx, fromPrice(nbAsk)); putBe32(b_ + off::nbAskSz, uint32_t(nbAskQty / unit));
        b_[off::quoteCondition] = uint8_t(condition);
        return QUOTE_LEN;
    }
    size_t trade(int64_t price, int64_t volume, char condition) noexcept {
        putBe32(b_ + off::tradePx, fromPrice(price));
        putBe32(b_ + off::tradeVol, uint32_t(volume));
        b_[off::tradeDenom] = 'B';
        b_[off::tradeCondition] = uint8_t(condition);
        return TRADE_LEN;
    }
    size_t action(char what, char reason) noexcept {
        b_[off::actionType] = uint8_t(what);
        b_[off::actionReason] = uint8_t(reason);
        return ADMIN_LEN;
    }
private:
    uint8_t* b_;
};

// ---- decoder ---------------------------------------------------------------------------------
class Decoder {
public:
    struct Params {
        Dialect  dialect = Dialect::Utp;
        uint32_t feedId = 0;
        uint16_t sourceId = 0;
        int64_t  sessionMidnightNs = 0;
        bool     shortFormRoundLots = true;   // both SIPs express short-form sizes in round lots
    };
    // Templated on the callback for the same reason as the ITCH decoder: this is a per-message
    // path, and a constructed wrapper per message is a cost with nothing to show for it.
    Decoder(const refdata::Snapshot& snap, Params p) : p_(p), snap_(snap) { participants_.fill(0); }

    // Which of our venues a participant character stands for. The SIPs use single-character codes
    // that differ between the two tapes, so this is configuration rather than a constant.
    void mapParticipant(uint8_t code, uint16_t venueId) noexcept { participants_[code] = venueId; }

    template <class E>
    size_t decode(const PacketHeader& h, uint16_t skip, E&& emit) {
        const auto* p = reinterpret_cast<const uint8_t*>(&h) + sizeof(PacketHeader);
        uint64_t seq = h.firstSeq;
        size_t off = 0, i = 0, decoded = 0;
        while (off + off::body <= h.payloadLen) {
            const int len = messageLength(char(p[off + off::category]), char(p[off + off::type]));
            if (len < 0 || off + size_t(len) > h.payloadLen) { ++malformed_; break; }
            if (i++ >= skip) decoded += one(p + off, size_t(len), seq, emit);
            ++seq;
            off += size_t(len);
        }
        flush(emit);
        ++packets_;
        return decoded;
    }
    template <class E>
    size_t decodeMessage(const uint8_t* m, size_t len, uint64_t venueSeq, bool endOfPacket, E&& emit) {
        size_t n = one(m, len, venueSeq, emit);
        if (endOfPacket) flush(emit);
        return n;
    }
    template <class E> void finish(E&& emit) { flush(emit); }

    uint64_t packets() const noexcept { return packets_; }
    uint64_t messages() const noexcept { return messages_; }
    uint64_t quotes() const noexcept { return quotes_; }
    uint64_t trades() const noexcept { return trades_; }
    uint64_t actions() const noexcept { return actions_; }
    uint64_t suppressed() const noexcept { return suppressed_; }   // quotes not eligible for the national best
    uint64_t unknownSymbol() const noexcept { return unknownSymbol_; }
    uint64_t malformed() const noexcept { return malformed_; }
    uint64_t emitted() const noexcept { return emitted_; }

private:
    template <class M, class E>
    void push(Frame<M>& f, E& emit) {
        f.header.sourceId = p_.sourceId;
        f.header.streamId = p_.feedId;
        if (pending_) emit(reinterpret_cast<const FrameHeader*>(hold_));
        std::memcpy(hold_, &f, f.header.frameLength);
        pending_ = true;
        ++emitted_;
    }
    template <class E> void flush(E& emit) {
        if (!pending_) return;
        auto* h = reinterpret_cast<FrameHeader*>(hold_);
        h->flags = uint16_t(h->flags | FrameFlags::endOfPacket);
        emit(h);
        pending_ = false;
    }

    template <class E>
    size_t one(const uint8_t* m, size_t len, uint64_t venueSeq, E& emit) {
        ++messages_;
        const char cat = char(m[off::category]), t = char(m[off::type]);
        if (len != size_t(messageLength(cat, t))) { ++malformed_; return 0; }
        const char* raw = reinterpret_cast<const char*>(m + off::symbol);
        size_t n = off::symbolLen; while (n > 0 && raw[n - 1] == ' ') --n;
        const uint32_t sym = snap_.lookupTicker(std::string_view(raw, n));
        if (!sym) { ++unknownSymbol_; return 0; }
        const int64_t ts = p_.sessionMidnightNs + int64_t(be64(m + off::timestamp));
        const size_t before = emitted_;

        switch (cat) {
        case category::Quote: {
            ++quotes_;
            const char condition = char(m[off::quoteCondition]);
            const uint8_t denom = m[off::denominator];
            const int64_t unit = (t == type::ShortQuote && p_.shortFormRoundLots) ? 100 : 1;
            Frame<Nbbo> q; q.init();
            q.body.symbolIdx = sym;
            q.body.bid = toPrice(be32(m + off::bidPx), denom);
            q.body.ask = toPrice(be32(m + off::askPx), denom);
            q.body.bidQty = int64_t(be32(m + off::bidSz)) * unit;
            q.body.askQty = int64_t(be32(m + off::askSz)) * unit;
            q.body.bidVenue = participants_[m[off::participant]];
            q.body.askVenue = participants_[m[off::participant]];
            // The official national best, which is the whole point of carrying this feed.
            if (m[off::nbboIndicator] != '0') {
                q.body.sipBid = toPrice(be32(m + off::nbBidPx), denom);
                q.body.sipAsk = toPrice(be32(m + off::nbAskPx), denom);
                q.body.bidVenue = participants_[m[off::bidParticipant]];
                q.body.askVenue = participants_[m[off::askParticipant]];
            }
            if (!setsNationalBest(condition)) ++suppressed_;
            q.body.venueSeq = venueSeq;
            q.header.originTs = ts;
            push(q, emit);
            break;
        }
        case category::Trade: {
            ++trades_;
            Frame<Trade> tr; tr.init();
            tr.body.symbolIdx = sym;
            tr.body.venueId = participants_[m[off::participant]];
            tr.body.price = toPrice(be32(m + off::tradePx), m[off::tradeDenom]);
            tr.body.qty = int64_t(be32(m + off::tradeVol));
            tr.body.conditions = m[off::tradeCondition];
            tr.body.venueSeq = venueSeq;
            tr.body.venueTs = ts;
            push(tr, emit);
            break;
        }
        case category::Admin: {
            ++actions_;
            Frame<SymbolStatus> st; st.init();
            st.body.symbolIdx = sym;
            st.body.venueId = participants_[m[off::participant]];
            st.body.venueTs = ts;
            st.body.venueSeq = venueSeq;
            switch (m[off::actionType]) {
                case 'T': st.body.status = TradingStatus::Trading; break;
                case 'H': st.body.status = TradingStatus::Halted; break;
                case 'P': st.body.status = TradingStatus::Paused; break;
                case 'Q': st.body.status = TradingStatus::PreOpen; break;
                default:  st.body.status = TradingStatus::Halted; break;
            }
            st.body.reason = m[off::actionReason];
            push(st, emit);
            break;
        }
        default: break;
        }
        return emitted_ - before;
    }

    Params p_;
    const refdata::Snapshot& snap_;
    std::array<uint16_t, 256> participants_{};
    alignas(16) std::byte hold_[sizeof(FrameHeader) + 128]{};
    bool pending_ = false;
    uint64_t packets_ = 0, messages_ = 0, quotes_ = 0, trades_ = 0, actions_ = 0;
    uint64_t suppressed_ = 0, unknownSymbol_ = 0, malformed_ = 0, emitted_ = 0;
};

// ---- direct against official -------------------------------------------------------------------
// The SIP is always a little behind the direct feeds, so a system that reported every disagreement
// would report nothing but noise. What matters is a disagreement that lasted: long enough that an
// order routed on the direct view would have been measured against a different official best. One
// record per episode, on the log, queryable by symbol - which is the form a best-execution review
// or a customer complaint arrives in.
//
// Every record names the shape the disagreement took, because that is the question being asked; a
// record that only said "they differed and then stopped" would answer nothing. durationNs is how
// long it lasted, or zero when the episode was still open when the record was written, which is
// what a live consumer gets for one that has already run long enough to be worth saying out loud.
class NbboMonitor {
public:
    struct Params {
        uint32_t feedId = 0;
        uint16_t sourceId = 0;
        int64_t  minEpisodeNs = 1'000'000;    // shorter than this is the SIP being the SIP
        int64_t  alertAfterNs = 50'000'000;   // an episode still open this long is said out loud once
        int64_t  staleAfterNs = 2'000'000'000;
    };
    NbboMonitor(uint32_t maxSymbolIdx, Params p) : p_(p), st_(size_t(maxSymbolIdx) + 1) {}

    // The best bid and offer our own direct feeds say, and the one the tape says. Either may arrive
    // first and either may go quiet; the monitor only speaks when both have spoken at least once.
    template <class E>
    void onDirect(uint32_t sym, int64_t bid, int64_t ask, int64_t ts, uint64_t venueSeq, E&& emit) {
        if (sym >= st_.size()) return;
        State& s = st_[sym];
        s.directBid = bid; s.directAsk = ask; s.directTs = ts; s.venueSeq = venueSeq; s.haveDirect = true;
        evaluate(sym, s, ts, emit);
    }
    template <class E>
    void onSip(uint32_t sym, int64_t bid, int64_t ask, int64_t ts, uint64_t venueSeq, E&& emit) {
        if (sym >= st_.size()) return;
        State& s = st_[sym];
        s.sipBid = bid; s.sipAsk = ask; s.sipTs = ts; s.venueSeq = venueSeq; s.haveSip = true;
        evaluate(sym, s, ts, emit);
    }
    // Staleness only becomes visible when nothing arrives, so the owner drives it from its timer.
    template <class E> void tick(int64_t now, E&& emit) {
        for (uint32_t sym = 0; sym < st_.size(); ++sym) {
            State& s = st_[sym];
            if (s.haveDirect && s.haveSip) evaluate(sym, s, now, emit);
        }
    }

    uint64_t episodes() const noexcept { return episodes_; }
    uint64_t closed() const noexcept { return closed_; }         // episodes that ended, reported or not
    uint64_t reported() const noexcept { return reported_; }     // episodes that outlived minEpisodeNs
    uint64_t alerts() const noexcept { return alerts_; }         // still-open episodes said out loud
    uint64_t crossed() const noexcept { return crossed_; }
    int64_t  longestNs() const noexcept { return longest_; }

private:
    struct State {
        int64_t directBid = 0, directAsk = 0, sipBid = 0, sipAsk = 0;
        int64_t directTs = 0, sipTs = 0, since = 0;
        uint64_t venueSeq = 0;
        DivergenceKind kind = DivergenceKind(0);
        bool haveDirect = false, haveSip = false, open = false, alerted = false, counted = false;
    };

    // An episode takes the worst shape it was ever seen in, and a feed that has stopped is the
    // worst: it is unambiguous, it is actionable, and it is usually the cause of whatever else is
    // being observed. A stalled tape's frozen prices will read crossed against a market that has
    // moved on, and reporting that as a crossed market would name the symptom and lose the fault.
    static int severity(DivergenceKind k) noexcept {
        switch (k) {
            case DivergenceKind::SipStale:
            case DivergenceKind::DirectStale: return 3;
            case DivergenceKind::Crossed: return 2;
            case DivergenceKind::Price: return 1;
            default: return 0;
        }
    }

    DivergenceKind classify(const State& s, int64_t now) const noexcept {
        // Silence first, for the reason severity() gives: a side that has not spoken in this long
        // is not disagreeing, it is absent, and everything else observed about it follows from that.
        if (now - s.sipTs >= p_.staleAfterNs) return DivergenceKind::SipStale;
        if (now - s.directTs >= p_.staleAfterNs) return DivergenceKind::DirectStale;
        // Then a market where one view's bid is at or through the other's offer, which is what
        // makes an order look marketable when it is not.
        if (s.directBid && s.sipAsk && s.directBid >= s.sipAsk) return DivergenceKind::Crossed;
        if (s.sipBid && s.directAsk && s.sipBid >= s.directAsk) return DivergenceKind::Crossed;
        if (s.directBid != s.sipBid || s.directAsk != s.sipAsk) return DivergenceKind::Price;
        return DivergenceKind(0);
    }

    template <class E> void evaluate(uint32_t sym, State& s, int64_t now, E& emit) {
        if (!s.haveDirect || !s.haveSip) return;
        const DivergenceKind k = classify(s, now);
        if (k != DivergenceKind(0)) {
            if (!s.open) { s.open = true; s.since = now; s.kind = k; s.alerted = false; ++episodes_; }
            else if (severity(k) > severity(s.kind)) s.kind = k;
            if (s.kind == DivergenceKind::Crossed && !s.counted) { ++crossed_; s.counted = true; }
            if (!s.alerted && now - s.since >= p_.alertAfterNs) {   // still open, and long enough to say so
                s.alerted = true; ++alerts_;
                publish(sym, s, s.kind, 0, now, emit);
            }
            return;
        }
        if (!s.open) return;
        const int64_t held = now - s.since;
        s.open = false; s.counted = false;
        ++closed_;
        if (held > longest_) longest_ = held;
        if (held < p_.minEpisodeNs && !s.alerted) return;           // the SIP catching up is not news
        ++reported_;
        publish(sym, s, s.kind, held, now, emit);
    }

    template <class E>
    void publish(uint32_t sym, const State& s, DivergenceKind k, int64_t held, int64_t now, E& emit) {
        Frame<NbboDivergence> d; d.init();
        d.header.sourceId = p_.sourceId;
        d.header.streamId = p_.feedId;
        d.body.symbolIdx = sym;
        d.body.kind = k;
        d.body.directBid = s.directBid; d.body.directAsk = s.directAsk;
        d.body.sipBid = s.sipBid;       d.body.sipAsk = s.sipAsk;
        d.body.durationNs = held;
        d.body.venueSeq = s.venueSeq;
        d.body.venueTs = now;
        emit(&d.header);
    }

    Params p_;
    std::vector<State> st_;
    uint64_t episodes_ = 0, closed_ = 0, reported_ = 0, alerts_ = 0, crossed_ = 0;
    int64_t longest_ = 0;
};

} // namespace trading::md::sip
