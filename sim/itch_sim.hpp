// sim/itch_sim.hpp : a Nasdaq day on the wire.
//
// Emits real TotalView-ITCH 5.0 bytes, big-endian and at the published lengths, so the decoder is
// exercised against the format rather than against a convenient shape. It keeps its own list of
// live orders and only executes, cancels, deletes or replaces ones it actually added, which is what
// makes the day self-consistent and what lets a test aggregate those orders into levels and compare
// them with the levels the decoder built incrementally. Two independent computations of the same
// book is a stronger statement than any single one.
//
// What it does not do is replace a published sample day. A real one carries the venue's own
// sequencing of odd cases - a replace across a halt, a cross with no paired shares, a broken trade
// for a print from an earlier packet - and MD-4's done-when asks for exactly that. This generator is
// what lets the decoder be built and gated before such a file is in hand; run_itch_golden below
// takes a path, so a real file drops straight in.
#pragma once
#include "itch.hpp"
#include "refdata.hpp"
#include <random>
#include <string_view>
#include <vector>

namespace trading::sim {

using namespace trading::md;

class ItchSim {
public:
    struct Live { uint64_t ref; int64_t price; int64_t shares; uint32_t symbolIdx; uint8_t side; };

    // The stock locate is the venue's own index for a symbol. Using our symbolIdx for it would make
    // the decoder's locate table look right for the wrong reason, so the venue numbers them its own
    // way and the decoder has to learn the mapping from the Stock Directory, as it does in life.
    ItchSim(const refdata::Snapshot& snap, uint32_t symbols, uint64_t seed, int64_t midnightNs)
        : snap_(snap), n_(symbols), rng_(seed), midnight_(midnightNs) {
        live_.reserve(1 << 16);
        for (const SymbolRecord& r : snap_.instruments()) {
            if (r.symbolIdx == 0 || r.symbolIdx > n_) continue;
            const uint16_t locate = uint16_t(1000 + r.symbolIdx * 7);       // deliberately not our index
            locates_.push_back({locate, r.symbolIdx, std::string(r.ticker, ::strnlen(r.ticker, 16)), r.refPrice});
        }
    }

    // Start of day: the system events and the Stock Directory a decoder needs before anything else
    // it sees means anything.
    template <class F> void open(F&& emit) {
        systemEvent('O', emit);
        systemEvent('S', emit);
        for (const Sym& s : locates_) stockDirectory(s, emit);
        systemEvent('Q', emit);
    }
    template <class F> void close(F&& emit) { systemEvent('M', emit); systemEvent('C', emit); }

    // One message of the trading day. The mix is roughly a real one: adds dominate, executions and
    // cancels follow them, and the rest is a long tail.
    template <class F> void step(F&& emit) {
        clock_ += 1 + rng_() % 5000;
        const uint32_t roll = uint32_t(rng_() % 1000);
        if (roll < 400 || live_.empty())    addOrder(roll % 7 == 0, emit);
        else if (roll < 560)                execute(roll % 5 == 0, emit);
        else if (roll < 700)                cancel(emit);
        else if (roll < 850)                del(emit);
        else if (roll < 910)                replace(emit);
        else if (roll < 940)                nonCrossTrade(emit);
        else if (roll < 955)                crossTrade(emit);
        else if (roll < 970)                noii(emit);
        else if (roll < 980)                tradingAction(emit);
        else if (roll < 990)                regSho(emit);
        else                                ignored(emit);
    }

    const std::vector<Live>& live() const noexcept { return live_; }
    size_t symbols() const noexcept { return locates_.size(); }
    uint64_t sent() const noexcept { return sent_; }
    int64_t midnight() const noexcept { return midnight_; }

private:
    struct Sym { uint16_t locate; uint32_t symbolIdx; std::string ticker; int64_t refPrice; };

    // Every message opens the same way: type, stock locate, tracking number, and the six-byte
    // nanoseconds-since-midnight timestamp.
    uint8_t* head(char type, uint16_t locate) noexcept {
        buf_[0] = uint8_t(type);
        itch::putBe16(buf_ + 1, locate);
        itch::putBe16(buf_ + 3, uint16_t(track_++));
        itch::putBe48(buf_ + 5, clock_);
        return buf_;
    }
    template <class F> void send(char type, F&& emit) {
        const int len = itch::messageLength(type);
        emit(buf_, size_t(len));
        ++sent_;
    }
    const Sym& pickSym() noexcept { return locates_[rng_() % locates_.size()]; }
    size_t pickLive() noexcept { return size_t(rng_() % live_.size()); }
    void dropLive(size_t i) noexcept { live_[i] = live_.back(); live_.pop_back(); }

    template <class F> void systemEvent(char code, F&& emit) {
        head(itch::msg::SystemEvent, 0);
        buf_[11] = uint8_t(code);
        send(itch::msg::SystemEvent, emit);
    }
    template <class F> void stockDirectory(const Sym& s, F&& emit) {
        head(itch::msg::StockDirectory, s.locate);
        std::memset(buf_ + 11, ' ', 8);
        std::memcpy(buf_ + 11, s.ticker.data(), s.ticker.size() < 8 ? s.ticker.size() : 8);
        buf_[19] = 'Q';                                  // market category: Nasdaq Global Select
        buf_[20] = 'N';                                  // financial status: normal
        itch::putBe32(buf_ + 21, 100);                   // round lot size
        buf_[25] = 'N'; buf_[26] = 'C';
        buf_[27] = ' '; buf_[28] = ' ';
        buf_[29] = 'P'; buf_[30] = 'N'; buf_[31] = 'N'; buf_[32] = '1'; buf_[33] = 'N';
        itch::putBe32(buf_ + 34, 0);
        buf_[38] = 'N';
        send(itch::msg::StockDirectory, emit);
    }
    template <class F> void addOrder(bool withMpid, F&& emit) {
        const Sym& s = pickSym();
        const char type = withMpid ? itch::msg::AddOrderMpid : itch::msg::AddOrder;
        const uint64_t ref = ++nextRef_;
        const bool bid = (rng_() & 1) != 0;
        const int64_t off = int64_t(1 + rng_() % 20) * 1'000'000;            // within 20 cents of the reference
        const int64_t price = bid ? s.refPrice - off : s.refPrice + off;
        const int64_t shares = int64_t(100 * (1 + rng_() % 20));
        head(type, s.locate);
        itch::putBe64(buf_ + 11, ref);
        buf_[19] = uint8_t(bid ? 'B' : 'S');
        itch::putBe32(buf_ + 20, uint32_t(shares));
        std::memset(buf_ + 24, ' ', 8);
        std::memcpy(buf_ + 24, s.ticker.data(), s.ticker.size() < 8 ? s.ticker.size() : 8);
        itch::putBe32(buf_ + 32, itch::fromPrice(price));
        if (withMpid) std::memcpy(buf_ + 36, "NSDQ", 4);
        live_.push_back(Live{ref, price, shares, s.symbolIdx, uint8_t(bid ? BookSide::Bid : BookSide::Ask)});
        send(type, emit);
    }
    template <class F> void execute(bool withPrice, F&& emit) {
        const size_t i = pickLive();
        Live& o = live_[i];
        const int64_t qty = o.shares > 100 ? int64_t(100 * (1 + rng_() % uint64_t(o.shares / 100))) : o.shares;
        const char type = withPrice ? itch::msg::OrderExecutedPrice : itch::msg::OrderExecuted;
        head(type, locateOf(o.symbolIdx));
        itch::putBe64(buf_ + 11, o.ref);
        itch::putBe32(buf_ + 19, uint32_t(qty));
        itch::putBe64(buf_ + 23, ++match_);
        if (withPrice) {
            buf_[31] = uint8_t(rng_() % 4 == 0 ? 'N' : 'Y');                 // some executions do not print
            itch::putBe32(buf_ + 32, itch::fromPrice(o.price));
        }
        o.shares -= qty;
        const bool gone = o.shares <= 0;
        send(type, emit);
        if (gone) dropLive(i);
    }
    template <class F> void cancel(F&& emit) {
        const size_t i = pickLive();
        Live& o = live_[i];
        const int64_t qty = o.shares > 100 ? int64_t(100 * (1 + rng_() % uint64_t(o.shares / 100))) : o.shares;
        head(itch::msg::OrderCancel, locateOf(o.symbolIdx));
        itch::putBe64(buf_ + 11, o.ref);
        itch::putBe32(buf_ + 19, uint32_t(qty));
        o.shares -= qty;
        const bool gone = o.shares <= 0;
        send(itch::msg::OrderCancel, emit);
        if (gone) dropLive(i);
    }
    template <class F> void del(F&& emit) {
        const size_t i = pickLive();
        head(itch::msg::OrderDelete, locateOf(live_[i].symbolIdx));
        itch::putBe64(buf_ + 11, live_[i].ref);
        send(itch::msg::OrderDelete, emit);
        dropLive(i);
    }
    template <class F> void replace(F&& emit) {
        const size_t i = pickLive();
        Live& o = live_[i];
        const uint64_t newRef = ++nextRef_;
        const int64_t shares = int64_t(100 * (1 + rng_() % 20));
        // A replace may move the price, but not through the other side: a venue's book is never
        // crossed, and a generator that lets it drift across would be testing the decoder against
        // a market that cannot happen.
        const int64_t ref = refOf(o.symbolIdx);
        int64_t price = o.price + (int64_t(rng_() % 5) - 2) * 1'000'000;
        if (o.side == uint8_t(BookSide::Bid)) { if (price >= ref) price = ref - 1'000'000; }
        else                                  { if (price <= ref) price = ref + 1'000'000; }
        head(itch::msg::OrderReplace, locateOf(o.symbolIdx));
        itch::putBe64(buf_ + 11, o.ref);
        itch::putBe64(buf_ + 19, newRef);
        itch::putBe32(buf_ + 27, uint32_t(shares));
        itch::putBe32(buf_ + 31, itch::fromPrice(price));
        o.ref = newRef; o.shares = shares; o.price = price;
        send(itch::msg::OrderReplace, emit);
    }
    template <class F> void nonCrossTrade(F&& emit) {
        const Sym& s = pickSym();
        head(itch::msg::TradeNonCross, s.locate);
        itch::putBe64(buf_ + 11, 0);                                         // hidden orders carry no reference
        buf_[19] = 'B';                                                      // the venue pins this to B
        itch::putBe32(buf_ + 20, uint32_t(100 * (1 + rng_() % 5)));
        std::memset(buf_ + 24, ' ', 8);
        std::memcpy(buf_ + 24, s.ticker.data(), s.ticker.size() < 8 ? s.ticker.size() : 8);
        itch::putBe32(buf_ + 32, itch::fromPrice(s.refPrice));
        itch::putBe64(buf_ + 36, ++match_);
        send(itch::msg::TradeNonCross, emit);
    }
    template <class F> void crossTrade(F&& emit) {
        const Sym& s = pickSym();
        head(itch::msg::CrossTrade, s.locate);
        itch::putBe64(buf_ + 11, uint64_t(100 * (1 + rng_() % 500)));
        std::memset(buf_ + 19, ' ', 8);
        std::memcpy(buf_ + 19, s.ticker.data(), s.ticker.size() < 8 ? s.ticker.size() : 8);
        itch::putBe32(buf_ + 27, itch::fromPrice(s.refPrice));
        itch::putBe64(buf_ + 31, ++match_);
        static const char kinds[] = {'O', 'C', 'H', 'I'};
        buf_[39] = uint8_t(kinds[rng_() % 4]);
        send(itch::msg::CrossTrade, emit);
    }
    template <class F> void noii(F&& emit) {
        const Sym& s = pickSym();
        head(itch::msg::Noii, s.locate);
        itch::putBe64(buf_ + 11, uint64_t(100 * (1 + rng_() % 2000)));
        itch::putBe64(buf_ + 19, uint64_t(100 * (rng_() % 500)));
        static const char dirs[] = {'B', 'S', 'N', 'O'};
        buf_[27] = uint8_t(dirs[rng_() % 4]);
        std::memset(buf_ + 28, ' ', 8);
        std::memcpy(buf_ + 28, s.ticker.data(), s.ticker.size() < 8 ? s.ticker.size() : 8);
        itch::putBe32(buf_ + 36, itch::fromPrice(s.refPrice));
        itch::putBe32(buf_ + 40, itch::fromPrice(s.refPrice));
        itch::putBe32(buf_ + 44, itch::fromPrice(s.refPrice));
        buf_[48] = 'O'; buf_[49] = ' ';
        send(itch::msg::Noii, emit);
    }
    template <class F> void tradingAction(F&& emit) {
        const Sym& s = pickSym();
        head(itch::msg::TradingAction, s.locate);
        std::memset(buf_ + 11, ' ', 8);
        std::memcpy(buf_ + 11, s.ticker.data(), s.ticker.size() < 8 ? s.ticker.size() : 8);
        static const char states[] = {'T', 'H', 'P', 'Q'};
        buf_[19] = uint8_t(states[rng_() % 4]);
        buf_[20] = ' ';
        std::memcpy(buf_ + 21, "T1  ", 4);
        send(itch::msg::TradingAction, emit);
    }
    template <class F> void regSho(F&& emit) {
        const Sym& s = pickSym();
        head(itch::msg::RegSho, s.locate);
        std::memset(buf_ + 11, ' ', 8);
        std::memcpy(buf_ + 11, s.ticker.data(), s.ticker.size() < 8 ? s.ticker.size() : 8);
        static const char acts[] = {'0', '1', '2'};
        buf_[19] = uint8_t(acts[rng_() % 3]);
        send(itch::msg::RegSho, emit);
    }
    // Messages a decoder must skip cleanly rather than choke on.
    template <class F> void ignored(F&& emit) {
        const Sym& s = pickSym();
        head(itch::msg::ParticipantPos, s.locate);
        std::memcpy(buf_ + 11, "NSDQ", 4);
        std::memset(buf_ + 15, ' ', 8);
        std::memcpy(buf_ + 15, s.ticker.data(), s.ticker.size() < 8 ? s.ticker.size() : 8);
        buf_[23] = 'Y'; buf_[24] = 'N'; buf_[25] = 'A';
        send(itch::msg::ParticipantPos, emit);
    }
    uint16_t locateOf(uint32_t symbolIdx) const noexcept {
        for (const Sym& s : locates_) if (s.symbolIdx == symbolIdx) return s.locate;
        return 0;
    }
    int64_t refOf(uint32_t symbolIdx) const noexcept {
        for (const Sym& s : locates_) if (s.symbolIdx == symbolIdx) return s.refPrice;
        return 0;
    }

    const refdata::Snapshot& snap_;
    uint32_t n_;
    std::mt19937_64 rng_;
    int64_t midnight_;
    std::vector<Sym> locates_;
    std::vector<Live> live_;
    uint8_t buf_[64]{};
    uint64_t clock_ = 9ull * 3600 * 1'000'000'000;       // the day starts at 09:00 venue time
    uint64_t nextRef_ = 0, match_ = 0, sent_ = 0;
    uint32_t track_ = 1;
};

} // namespace trading::sim
