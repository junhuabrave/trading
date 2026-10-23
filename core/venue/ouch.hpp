// core/venue/ouch.hpp : Nasdaq OUCH 5.0, both directions, on the wire.
//
// The codec and nothing else: every message the specification defines as a struct, the bytes it is
// on the wire, and back. Mapping a ChildOrder onto an Enter Order, choosing UserRefNums and turning
// an execution into an ExecReport belong to the session worker (VG-3), which owns the state that
// needs. The same codec serves the worker, which writes inbound messages and reads outbound ones,
// and the simulator twin, which does the reverse.
//
// Wire format, from the specification (October 2025): a one-byte type, fixed fields at fixed
// offsets, then on most messages a two-byte appendage length and an appendage of TagValue elements
// (length, tag, value) carrying the optional fields. Numbers are unsigned big-endian unless stated;
// prices are eight bytes with four implied decimals; alpha fields are left-justified and padded
// with spaces. Type letters are reused across directions (U, C, D, E, M, X and Q mean different
// messages inbound and outbound), so there is a decoder per direction.
//
// Strict on decode: a message is exactly its fixed part plus the appendage its length announces; a
// known tag carries exactly its size and appears once. An unknown tag is skipped and counted, since
// Nasdaq adds options over time. Where the specification makes the appendage optional on a message
// it may be absent, and the encoder leaves it out when there is nothing to put in it, as Nasdaq does.
// Field values are not interpreted here: a side of 'Z' decodes, and the worker decides what it means.
#pragma once
#include "bigendian.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>
#include <variant>

namespace trading::venue::ouch {

using util::be16; using util::putBe16;

template <size_t N> using Alpha = std::array<char, N>;
// A string as an alpha field: left-justified, padded with spaces, cut at the field's width.
template <size_t N> Alpha<N> alpha(std::string_view s) noexcept {
    Alpha<N> a; a.fill(' ');
    std::memcpy(a.data(), s.data(), s.size() < N ? s.size() : N);
    return a;
}
// The field without its padding.
template <size_t N> std::string_view trimmed(const Alpha<N>& a) noexcept {
    size_t n = N;
    while (n > 0 && a[n - 1] == ' ') --n;
    return {a.data(), n};
}

// Prices: four implied decimals on the wire, 1e-8 in our messages.
inline constexpr int64_t PRICE_SCALE = 10'000;
inline constexpr uint64_t MAX_PRICE = 0x7735939C;      // $199,999.9900, the highest limit price
inline constexpr uint64_t MARKET_PRICE = 0x7FFFFFFF;   // $214,748.3647, a market order for a cross
inline constexpr int64_t toPrice(uint64_t wire) noexcept { return int64_t(wire) * PRICE_SCALE; }
inline constexpr uint64_t fromPrice(int64_t px) noexcept { return uint64_t(px / PRICE_SCALE); }

// ---- the optional appendage ------------------------------------------------------------------
enum class Tag : uint8_t {
    SecondaryOrdRefNum = 1, Firm = 2, MinQty = 3, CustomerType = 4, MaxFloor = 5, PriceType = 6, PegOffset = 7,
    DiscretionPrice = 9, DiscretionPriceType = 10, DiscretionPegOffset = 11, PostOnly = 12, RandomReserves = 13,
    Route = 14, ExpireTime = 15, TradeNow = 16, HandleInst = 17, BboWeightIndicator = 18, DisplayQuantity = 22,
    DisplayPrice = 23, GroupId = 24, SharesLocated = 25, LocateBroker = 26, Side = 27, UserRefIdx = 28,
    AiqStrategy = 29, AiqGroupId = 30,
};
inline constexpr uint8_t MAX_TAG = 30;
// Each tag's value size in bytes, from Appendix A; zero for a number the specification does not use.
inline constexpr std::array<uint8_t, MAX_TAG + 1> TAG_SIZE = {0, 8, 4, 4, 1, 4, 1, 4, 0, 8, 1, 4, 1, 4, 4, 4,
                                                              1, 1, 1, 0, 0, 0, 4, 8, 2, 1, 4, 1, 1, 1, 2};

class Options {
public:
    bool has(Tag t) const noexcept { return present_ & bit(t); }
    bool empty() const noexcept { return present_ == 0; }
    // A numeric option's value. PegOffset and DiscretionPegOffset are signed: read them as int32_t.
    uint64_t num(Tag t) const noexcept {
        uint64_t v = 0;
        for (uint8_t i = 0; i < size(t); ++i) v = v << 8 | raw_[uint8_t(t)][i];
        return v;
    }
    std::string_view alpha(Tag t) const noexcept { return {reinterpret_cast<const char*>(raw_[uint8_t(t)].data()), size(t)}; }
    Options& set(Tag t, uint64_t v) noexcept {
        auto& r = raw_[uint8_t(t)]; r.fill(0);
        for (uint8_t i = 0; i < size(t); ++i) r[i] = uint8_t(v >> (8 * (size(t) - 1 - i)));
        present_ |= bit(t);
        return *this;
    }
    Options& setAlpha(Tag t, std::string_view s) noexcept {
        auto& r = raw_[uint8_t(t)]; r.fill(0);
        for (uint8_t i = 0; i < size(t); ++i) r[i] = uint8_t(i < s.size() ? s[i] : ' ');
        present_ |= bit(t);
        return *this;
    }
    void clear(Tag t) noexcept { raw_[uint8_t(t)].fill(0); present_ &= ~bit(t); }
    // Unknown tags skipped on decode; not part of the value, so not compared and not re-encoded.
    uint32_t unknown() const noexcept { return unknown_; }

    size_t wireSize() const noexcept {
        size_t n = 0;
        for (uint8_t t = 1; t <= MAX_TAG; ++t) if (present_ & (1u << t)) n += 2 + TAG_SIZE[t];
        return n;
    }
    // In ascending tag order; the specification sets no order, so this is ours.
    void put(uint8_t* p) const noexcept {
        for (uint8_t t = 1; t <= MAX_TAG; ++t) {
            if (!(present_ & (1u << t))) continue;
            p[0] = uint8_t(1 + TAG_SIZE[t]); p[1] = t;
            std::memcpy(p + 2, raw_[t].data(), TAG_SIZE[t]);
            p += 2 + TAG_SIZE[t];
        }
    }
    bool get(const uint8_t* p, size_t n) noexcept {
        *this = Options{};
        while (n > 0) {
            const size_t len = p[0];                       // what follows the length byte: tag and value
            if (len == 0 || len + 1 > n) return false;
            const uint8_t t = p[1];
            if (t <= MAX_TAG && TAG_SIZE[t] != 0) {
                if (len != 1 + size_t(TAG_SIZE[t]) || (present_ & (1u << t))) return false;
                std::memcpy(raw_[t].data(), p + 2, TAG_SIZE[t]);
                present_ |= 1u << t;
            } else if (unknown_ < UINT32_MAX) ++unknown_;
            p += len + 1; n -= len + 1;
        }
        return true;
    }
    bool operator==(const Options& o) const noexcept { return present_ == o.present_ && raw_ == o.raw_; }

private:
    static constexpr uint32_t bit(Tag t) noexcept { return 1u << uint8_t(t); }
    static constexpr uint8_t size(Tag t) noexcept { return TAG_SIZE[uint8_t(t)]; }
    uint32_t present_ = 0;
    std::array<std::array<uint8_t, 8>, MAX_TAG + 1> raw_{};
    uint32_t unknown_ = 0;
};

// ---- field access, shared by encode and decode -----------------------------------------------
// Each message lists its fields once, as (offset, member) pairs to a visitor that either writes
// them to the wire or reads them from it. A static template over the message rather than C++23's
// explicit object parameter, which g++ 13 (the CI compiler) does not have.
struct Writer {
    uint8_t* p;
    template <class T> void operator()(size_t off, const T& v) const noexcept {
        if constexpr (std::is_same_v<T, char>) p[off] = uint8_t(v);
        else if constexpr (std::is_integral_v<T>) { for (size_t i = 0; i < sizeof(T); ++i) p[off + i] = uint8_t(uint64_t(v) >> (8 * (sizeof(T) - 1 - i))); }
        else std::memcpy(p + off, v.data(), v.size());
    }
};
struct Reader {
    const uint8_t* p;
    template <class T> void operator()(size_t off, T& v) const noexcept {
        if constexpr (std::is_same_v<T, char>) v = char(p[off]);
        else if constexpr (std::is_integral_v<T>) { uint64_t x = 0; for (size_t i = 0; i < sizeof(T); ++i) x = x << 8 | p[off + i]; v = T(x); }
        else std::memcpy(v.data(), p + off, v.size());
    }
};

enum class Appendage : uint8_t { None, Optional, Required };

// ---- inbound: participant to Nasdaq ----------------------------------------------------------
struct EnterOrder {
    static constexpr char TYPE = 'O'; static constexpr size_t FIXED = 45; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint32_t userRefNum = 0; char side = 0; uint32_t quantity = 0; Alpha<8> symbol{}; uint64_t price = 0;
    char timeInForce = 0, display = 0, capacity = 0, interMarketSweep = 0, crossType = 0; Alpha<14> clOrdId{};
    Options opts;
    static void fields(auto& m, auto& f) {
        f(1, m.userRefNum); f(5, m.side); f(6, m.quantity); f(10, m.symbol); f(18, m.price); f(26, m.timeInForce);
        f(27, m.display); f(28, m.capacity); f(29, m.interMarketSweep); f(30, m.crossType); f(31, m.clOrdId);
    }
    bool operator==(const EnterOrder&) const = default;
};
struct ReplaceOrder {
    static constexpr char TYPE = 'U'; static constexpr size_t FIXED = 38; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint32_t origUserRefNum = 0, userRefNum = 0, quantity = 0; uint64_t price = 0;
    char timeInForce = 0, display = 0, interMarketSweep = 0; Alpha<14> clOrdId{};
    Options opts;
    static void fields(auto& m, auto& f) {
        f(1, m.origUserRefNum); f(5, m.userRefNum); f(9, m.quantity); f(13, m.price); f(21, m.timeInForce);
        f(22, m.display); f(23, m.interMarketSweep); f(24, m.clOrdId);
    }
    bool operator==(const ReplaceOrder&) const = default;
};
struct CancelOrder {
    static constexpr char TYPE = 'X'; static constexpr size_t FIXED = 9; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint32_t userRefNum = 0, quantity = 0;              // the new intended size; zero cancels what is left
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.userRefNum); f(5, m.quantity); }
    bool operator==(const CancelOrder&) const = default;
};
struct ModifyOrder {
    static constexpr char TYPE = 'M'; static constexpr size_t FIXED = 10; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint32_t userRefNum = 0; char side = 0; uint32_t quantity = 0;
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.userRefNum); f(5, m.side); f(6, m.quantity); }
    bool operator==(const ModifyOrder&) const = default;
};
struct MassCancelRequest {
    static constexpr char TYPE = 'C'; static constexpr size_t FIXED = 17; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint32_t userRefNum = 0; Alpha<4> firm{}; Alpha<8> symbol{};   // symbol all spaces: every symbol
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.userRefNum); f(5, m.firm); f(9, m.symbol); }
    bool operator==(const MassCancelRequest&) const = default;
};
struct DisableOrderEntryRequest {
    static constexpr char TYPE = 'D'; static constexpr size_t FIXED = 9; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint32_t userRefNum = 0; Alpha<4> firm{};
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.userRefNum); f(5, m.firm); }
    bool operator==(const DisableOrderEntryRequest&) const = default;
};
struct EnableOrderEntryRequest {
    static constexpr char TYPE = 'E'; static constexpr size_t FIXED = 9; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint32_t userRefNum = 0; Alpha<4> firm{};
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.userRefNum); f(5, m.firm); }
    bool operator==(const EnableOrderEntryRequest&) const = default;
};
struct AccountQueryRequest {
    static constexpr char TYPE = 'Q'; static constexpr size_t FIXED = 1; static constexpr Appendage APPENDAGE = Appendage::Optional;
    Options opts;
    static void fields(auto&, auto&) {}
    bool operator==(const AccountQueryRequest&) const = default;
};

// ---- outbound: Nasdaq to participant ---------------------------------------------------------
// Every outbound message carries a timestamp: nanoseconds since midnight.
struct SystemEvent {
    static constexpr char TYPE = 'S'; static constexpr size_t FIXED = 10; static constexpr Appendage APPENDAGE = Appendage::None;
    uint64_t timestamp = 0; char eventCode = 0;         // S start of day, E end of day
    Options opts;                                        // always empty: the message has no appendage
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.eventCode); }
    bool operator==(const SystemEvent&) const = default;
};
struct OrderAccepted {
    static constexpr char TYPE = 'A'; static constexpr size_t FIXED = 62; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint64_t timestamp = 0; uint32_t userRefNum = 0; char side = 0; uint32_t quantity = 0; Alpha<8> symbol{}; uint64_t price = 0;
    char timeInForce = 0, display = 0; uint64_t orderReferenceNumber = 0;
    char capacity = 0, interMarketSweep = 0, crossType = 0, orderState = 0; Alpha<14> clOrdId{};
    Options opts;
    static void fields(auto& m, auto& f) {
        f(1, m.timestamp); f(9, m.userRefNum); f(13, m.side); f(14, m.quantity); f(18, m.symbol); f(26, m.price);
        f(34, m.timeInForce); f(35, m.display); f(36, m.orderReferenceNumber); f(44, m.capacity);
        f(45, m.interMarketSweep); f(46, m.crossType); f(47, m.orderState); f(48, m.clOrdId);
    }
    bool operator==(const OrderAccepted&) const = default;
};
struct OrderReplaced {
    static constexpr char TYPE = 'U'; static constexpr size_t FIXED = 66; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint64_t timestamp = 0; uint32_t origUserRefNum = 0, userRefNum = 0; char side = 0; uint32_t quantity = 0; Alpha<8> symbol{};
    uint64_t price = 0; char timeInForce = 0, display = 0; uint64_t orderReferenceNumber = 0;
    char capacity = 0, interMarketSweep = 0, crossType = 0, orderState = 0; Alpha<14> clOrdId{};
    Options opts;
    static void fields(auto& m, auto& f) {
        f(1, m.timestamp); f(9, m.origUserRefNum); f(13, m.userRefNum); f(17, m.side); f(18, m.quantity); f(22, m.symbol);
        f(30, m.price); f(38, m.timeInForce); f(39, m.display); f(40, m.orderReferenceNumber); f(48, m.capacity);
        f(49, m.interMarketSweep); f(50, m.crossType); f(51, m.orderState); f(52, m.clOrdId);
    }
    bool operator==(const OrderReplaced&) const = default;
};
struct OrderCanceled {
    static constexpr char TYPE = 'C'; static constexpr size_t FIXED = 18; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint64_t timestamp = 0; uint32_t userRefNum = 0, quantity = 0; char reason = 0;   // quantity: decremented, not remaining
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); f(13, m.quantity); f(17, m.reason); }
    bool operator==(const OrderCanceled&) const = default;
};
struct AiqCanceled {
    static constexpr char TYPE = 'D'; static constexpr size_t FIXED = 32; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint64_t timestamp = 0; uint32_t userRefNum = 0, decrementShares = 0; char reason = 0; uint32_t quantityPrevented = 0;
    uint64_t executionPrice = 0; char liquidityFlag = 0, aiqStrategy = 0;
    Options opts;
    static void fields(auto& m, auto& f) {
        f(1, m.timestamp); f(9, m.userRefNum); f(13, m.decrementShares); f(17, m.reason); f(18, m.quantityPrevented);
        f(22, m.executionPrice); f(30, m.liquidityFlag); f(31, m.aiqStrategy);
    }
    bool operator==(const AiqCanceled&) const = default;
};
struct OrderExecuted {
    static constexpr char TYPE = 'E'; static constexpr size_t FIXED = 34; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint64_t timestamp = 0; uint32_t userRefNum = 0, quantity = 0; uint64_t price = 0; char liquidityFlag = 0; uint64_t matchNumber = 0;
    Options opts;
    static void fields(auto& m, auto& f) {
        f(1, m.timestamp); f(9, m.userRefNum); f(13, m.quantity); f(17, m.price); f(25, m.liquidityFlag); f(26, m.matchNumber);
    }
    bool operator==(const OrderExecuted&) const = default;
};
struct BrokenTrade {
    static constexpr char TYPE = 'B'; static constexpr size_t FIXED = 36; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint64_t timestamp = 0; uint32_t userRefNum = 0; uint64_t matchNumber = 0; char reason = 0; Alpha<14> clOrdId{};
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); f(13, m.matchNumber); f(21, m.reason); f(22, m.clOrdId); }
    bool operator==(const BrokenTrade&) const = default;
};
struct OrderRejected {
    static constexpr char TYPE = 'J'; static constexpr size_t FIXED = 29; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint64_t timestamp = 0; uint32_t userRefNum = 0; uint16_t reason = 0; Alpha<14> clOrdId{};
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); f(13, m.reason); f(15, m.clOrdId); }
    bool operator==(const OrderRejected&) const = default;
};
struct CancelPending {
    static constexpr char TYPE = 'P'; static constexpr size_t FIXED = 13; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint64_t timestamp = 0; uint32_t userRefNum = 0;
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); }
    bool operator==(const CancelPending&) const = default;
};
struct CancelReject {
    static constexpr char TYPE = 'I'; static constexpr size_t FIXED = 13; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint64_t timestamp = 0; uint32_t userRefNum = 0;
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); }
    bool operator==(const CancelReject&) const = default;
};
struct PriorityUpdate {
    static constexpr char TYPE = 'T'; static constexpr size_t FIXED = 30; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint64_t timestamp = 0; uint32_t userRefNum = 0; uint64_t price = 0; char display = 0; uint64_t orderReferenceNumber = 0;
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); f(13, m.price); f(21, m.display); f(22, m.orderReferenceNumber); }
    bool operator==(const PriorityUpdate&) const = default;
};
struct OrderModified {
    static constexpr char TYPE = 'M'; static constexpr size_t FIXED = 18; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint64_t timestamp = 0; uint32_t userRefNum = 0; char side = 0; uint32_t quantity = 0;
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); f(13, m.side); f(14, m.quantity); }
    bool operator==(const OrderModified&) const = default;
};
struct OrderRestated {
    static constexpr char TYPE = 'R'; static constexpr size_t FIXED = 14; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint64_t timestamp = 0; uint32_t userRefNum = 0; char reason = 0;   // R refresh of display, P display price update
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); f(13, m.reason); }
    bool operator==(const OrderRestated&) const = default;
};
struct MassCancelResponse {
    static constexpr char TYPE = 'X'; static constexpr size_t FIXED = 25; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint64_t timestamp = 0; uint32_t userRefNum = 0; Alpha<4> firm{}; Alpha<8> symbol{};
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); f(13, m.firm); f(17, m.symbol); }
    bool operator==(const MassCancelResponse&) const = default;
};
struct DisableOrderEntryResponse {
    static constexpr char TYPE = 'G'; static constexpr size_t FIXED = 17; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint64_t timestamp = 0; uint32_t userRefNum = 0; Alpha<4> firm{};
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); f(13, m.firm); }
    bool operator==(const DisableOrderEntryResponse&) const = default;
};
struct EnableOrderEntryResponse {
    static constexpr char TYPE = 'K'; static constexpr size_t FIXED = 17; static constexpr Appendage APPENDAGE = Appendage::Required;
    uint64_t timestamp = 0; uint32_t userRefNum = 0; Alpha<4> firm{};
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.userRefNum); f(13, m.firm); }
    bool operator==(const EnableOrderEntryResponse&) const = default;
};
struct AccountQueryResponse {
    static constexpr char TYPE = 'Q'; static constexpr size_t FIXED = 13; static constexpr Appendage APPENDAGE = Appendage::Optional;
    uint64_t timestamp = 0; uint32_t nextUserRefNum = 0;
    Options opts;
    static void fields(auto& m, auto& f) { f(1, m.timestamp); f(9, m.nextUserRefNum); }
    bool operator==(const AccountQueryResponse&) const = default;
};

// ---- one message ------------------------------------------------------------------------------
template <class M> size_t wireSize(const M& m) noexcept {
    if (M::APPENDAGE == Appendage::None || (M::APPENDAGE == Appendage::Optional && m.opts.empty())) return M::FIXED;
    return M::FIXED + 2 + m.opts.wireSize();
}
// The message's bytes into out; its length, or 0 if cap is too small.
template <class M> size_t encode(const M& m, uint8_t* out, size_t cap) noexcept {
    const size_t n = wireSize(m);
    if (n > cap) return 0;
    out[0] = uint8_t(M::TYPE);
    Writer w{out};
    M::fields(m, w);
    if (n > M::FIXED) { putBe16(out + M::FIXED, uint16_t(n - M::FIXED - 2)); m.opts.put(out + M::FIXED + 2); }
    return n;
}
// Exactly n bytes as one message of this type, or false.
template <class M> bool decode(const uint8_t* p, size_t n, M& m) noexcept {
    if (n < M::FIXED || p[0] != uint8_t(M::TYPE)) return false;
    m = M{};
    Reader r{p};
    M::fields(m, r);
    if (n == M::FIXED) return M::APPENDAGE != Appendage::Required;
    if (M::APPENDAGE == Appendage::None || n < M::FIXED + 2) return false;
    const size_t len = be16(p + M::FIXED);
    return n == M::FIXED + 2 + len && m.opts.get(p + M::FIXED + 2, len);
}

// ---- either direction -------------------------------------------------------------------------
using Inbound = std::variant<EnterOrder, ReplaceOrder, CancelOrder, ModifyOrder, MassCancelRequest,
                             DisableOrderEntryRequest, EnableOrderEntryRequest, AccountQueryRequest>;
using Outbound = std::variant<SystemEvent, OrderAccepted, OrderReplaced, OrderCanceled, AiqCanceled, OrderExecuted,
                              BrokenTrade, OrderRejected, CancelPending, CancelReject, PriorityUpdate, OrderModified,
                              OrderRestated, MassCancelResponse, DisableOrderEntryResponse, EnableOrderEntryResponse,
                              AccountQueryResponse>;

namespace detail {
template <class V, size_t I = 0> bool decodeAs(const uint8_t* p, size_t n, V& out) noexcept {
    if constexpr (I == std::variant_size_v<V>) return false;
    else {
        using M = std::variant_alternative_t<I, V>;
        if (p[0] == uint8_t(M::TYPE)) return decode(p, n, out.template emplace<M>());
        return decodeAs<V, I + 1>(p, n, out);
    }
}
} // namespace detail

// What a participant sends, as the host (or the simulator twin) reads it.
inline bool decodeInbound(const uint8_t* p, size_t n, Inbound& out) noexcept { return n > 0 && detail::decodeAs(p, n, out); }
// What Nasdaq sends, as the session worker reads it.
inline bool decodeOutbound(const uint8_t* p, size_t n, Outbound& out) noexcept { return n > 0 && detail::decodeAs(p, n, out); }
inline size_t encode(const Inbound& m, uint8_t* out, size_t cap) noexcept { return std::visit([&](const auto& x) { return encode(x, out, cap); }, m); }
inline size_t encode(const Outbound& m, uint8_t* out, size_t cap) noexcept { return std::visit([&](const auto& x) { return encode(x, out, cap); }, m); }

} // namespace trading::venue::ouch
