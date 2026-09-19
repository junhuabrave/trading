// core/risk/risk.hpp : the pre-trade risk engine.
//
// A deterministic state machine over the core stream. Everything it knows arrives as
// sequenced messages (limits, buying power, locates, kill switches, order lifecycle,
// NBBO, symbol status, reference data, position snapshots); everything it decides leaves
// as a RiskDecision. No wall clock: rate limiting uses seqTs. No allocation on the order
// path: tables are open-addressing and reserved at construction. Same binary replays
// identically from the journal.
//
// Limit precedence: (account, symbol) > account > firm. Within the winning set, a zero
// field means "no limit". Kill switches stack: firm, account and strategy each block.
#pragma once
#include "refdata.hpp"
#include "flatmap.hpp"
#include "money.hpp"
#include <vector>
#include <array>

namespace trading::risk {

using namespace trading::refdata;
using trading::util::FlatMap;
using trading::util::Key128;

struct Limits {
    int64_t  maxOrderQty = 0, maxOrderNotional = 0;          // per order: 1e-8 units are ample
    money::i128 maxGrossExposure = 0, maxNetExposure = 0;    // firm-wide sums: held wide, read with the message's unit
    uint32_t priceCollarBps = 0, maxMsgRate = 0, maxOpenOrders = 0, dupWindowMs = 0;
    bool     set = false;
    void from(const LimitUpdate& m) noexcept {
        maxOrderQty = m.maxOrderQty; maxOrderNotional = m.maxOrderNotional;
        maxGrossExposure = money::toNano(m.maxGrossExposure, m.moneyScale);
        maxNetExposure = money::toNano(m.maxNetExposure, m.moneyScale);
        priceCollarBps = m.priceCollarBps; maxMsgRate = m.maxMsgRate;
        maxOpenOrders = m.maxOpenOrders; dupWindowMs = m.dupWindowMs; set = true;
    }
};

// Bits of RiskDecision.checkMask: which checks ran (not which failed; the reason says that).
namespace Check {
    inline constexpr uint32_t Symbol = 1u << 0, Account = 1u << 1, Kill = 1u << 2, Qty = 1u << 3, Price = 1u << 4,
        Tick = 1u << 5, Collar = 1u << 6, Short = 1u << 7, Locate = 1u << 8, Ssr = 1u << 9, BuyingPower = 1u << 10,
        Exposure = 1u << 11, OpenOrders = 1u << 12, MsgRate = 1u << 13, Session = 1u << 14, Duplicate = 1u << 15,
        Replace = 1u << 16;
}

using money::i128;   // exposure sums are computed wide so they can never overflow silently

class RiskEngine {
public:
    static constexpr size_t MAX_ACCOUNTS = 65536;
    static constexpr int64_t BPS = 10'000;
    static constexpr int64_t RATE_BUCKET_NS = 100'000'000;      // sliding one-second window in ten buckets
    static constexpr size_t RATE_BUCKETS = 10;
    static constexpr size_t DUP_RING = 8;

    // expectedOrders: orders per day (the clOrdId set must hold them all); expectedOpen: concurrently open.
    explicit RiskEngine(RefData& rd, size_t expectedOrders = 1 << 20, size_t expectedOpen = 1 << 16)
        : rd_(rd), accounts_(MAX_ACCOUNTS), symbolLimits_(4096), openOrders_(expectedOpen), positions_(1 << 16),
          locates_(1 << 16), clOrdIds_(expectedOrders),
          bid_(MAX_SYMBOLS, 0), ask_(MAX_SYMBOLS, 0), lastTrade_(MAX_SYMBOLS, 0), halted_(MAX_SYMBOLS, 0), strategyKilled_(65536, 0) {
        // margin requirement per marginClass, in bps of notional: 0 unused, 1 Reg T 50%, 2 100%, 3 100% (new issue / low priced), 4 30% (portfolio)
        marginBps_ = {0, 5000, 10000, 10000, 3000, 10000, 10000, 10000};
    }

    // Start of day: accounts from the snapshot are known before any BuyingPowerUpdate arrives.
    void load(const Snapshot& snap) {
        for (const AccountRecord& a : snap.accounts())
            if (a.accountType != 0 && a.accountIdx != 0 && a.accountIdx < MAX_ACCOUNTS) accounts_[a.accountIdx].known = true;
    }

    // ---------------------------------------------------------------- state ingestion
    // Feed every core-stream frame here (before or after evaluate, in sequence order).
    // Returns true if consumed.
    bool apply(const FrameHeader* h) noexcept {
        if (rd_.apply(h)) return true;
        if (auto* m = as<LimitUpdate>(h)) {
            if (m->accountIdx == 0 && m->symbolIdx == 0) firm_.from(*m);
            else if (m->symbolIdx == 0 && m->accountIdx < MAX_ACCOUNTS) accounts_[m->accountIdx].limits.from(*m);
            else symbolLimits_[key(m->accountIdx, m->symbolIdx)].from(*m);
            return true;
        }
        if (auto* m = as<BuyingPowerUpdate>(h)) {
            if (m->accountIdx < MAX_ACCOUNTS) { auto& a = accounts_[m->accountIdx]; a.bpBase = m->buyingPower; a.bpCommitted = 0; a.known = true; }
            return true;
        }
        if (auto* m = as<KillSwitch>(h)) {
            bool on = m->release == 0;                 // v1 frames decode release as 0: a kill
            if (m->accountIdx == 0 && m->strategyId == 0) firmKilled_ = on;
            else if (m->accountIdx != 0 && m->accountIdx < MAX_ACCOUNTS) accounts_[m->accountIdx].killed = on;
            if (m->strategyId != 0 && m->strategyId < strategyKilled_.size()) strategyKilled_[m->strategyId] = on ? 1 : 0;
            return true;
        }
        if (auto* m = as<LocateGranted>(h)) {
            if (m->result != LocateResult::Denied)
                locates_.insert(m->locateId, Locate{m->accountIdx, m->symbolIdx, m->qty, m->expiryTs});
            return true;
        }
        if (auto* m = as<LocateExpired>(h)) { locates_.erase(m->locateId); return true; }
        if (auto* m = as<Nbbo>(h)) { if (m->symbolIdx < MAX_SYMBOLS) { bid_[m->symbolIdx] = m->bid; ask_[m->symbolIdx] = m->ask; } return true; }
        if (auto* m = as<Trade>(h)) { if (m->symbolIdx < MAX_SYMBOLS) lastTrade_[m->symbolIdx] = m->price; return true; }
        if (auto* m = as<SymbolStatus>(h)) {
            if (m->symbolIdx < MAX_SYMBOLS && (m->venueId == 0 || m->venueId == primaryVenue(m->symbolIdx)))
                halted_[m->symbolIdx] = (m->status == TradingStatus::Halted || m->status == TradingStatus::Paused) ? 1 : 0;
            return true;
        }
        if (auto* m = as<PositionSnapshot>(h)) { onPositionSnapshot(*m); return true; }
        if (auto* m = as<ExposureSnapshot>(h)) {
            // The position keeper's view of position exposure re-bases ours; the open-order
            // fields are its reconciliation of our state, and ours stay authoritative.
            if (m->accountIdx < MAX_ACCOUNTS) { auto& a = accounts_[m->accountIdx];
                a.grossPosition = money::toNano(m->grossExposure, m->moneyScale); a.netPosition = money::toNano(m->netExposure, m->moneyScale); }
            return true;
        }
        if (auto* m = as<ExecReport>(h)) { if (m->childOrderId == 0) onExec(*m); return true; }   // parent-level only; venue-level reports are the OMS's input
        if (auto* m = as<OrderState>(h)) { onOrderState(*m); return true; }
        if (auto* m = as<RiskDecision>(h)) { (void)m; return true; }   // our own output on replay; state was updated at evaluate()
        return false;
    }

    // ---------------------------------------------------------------- the decisions
    // Fills `out` and updates internal state exactly as if `out` had been sequenced.
    // Returns the verdict. Deterministic given the sequence of apply() calls before it.
    RiskVerdict evaluate(const FrameHeader* h, const NewOrder& o, RiskDecision& out) noexcept {
        return decide(h, o, out, nullptr);
    }

    // A replace re-runs the credit, shape and price checks on the order as it would be after
    // the change. Quantity is the new total (FIX semantics); leaves = new total - filled.
    RiskVerdict evaluateReplace(const FrameHeader* h, const ReplaceOrder& r, RiskDecision& out) noexcept {
        out.orderId = r.orderId; out.accountIdx = r.accountIdx; out.symbolIdx = 0;
        out.checkMask = Check::Replace; out.reason = uint16_t(Reason::NoReason); out.notional = 0; out.buyingPowerAfter = 0;
        OpenOrder* oo = openOrders_.find(r.orderId);
        if (!oo || oo->accountIdx != r.accountIdx) { out.verdict = RiskVerdict::Reject; out.reason = uint16_t(Reason::UnknownOrder); ++rejects_; return RiskVerdict::Reject; }
        out.symbolIdx = oo->symbolIdx;
        NewOrder n{}; n.clOrdId = r.clOrdId; n.orderId = r.orderId; n.accountIdx = oo->accountIdx; n.symbolIdx = oo->symbolIdx;
        n.side = oo->side; n.ordType = oo->ordType; n.strategyId = oo->strategyId; n.sessionId = r.sessionId;
        n.locateId = oo->locateId; n.displayQty = r.displayQty; n.stopPrice = r.stopPrice;
        int64_t filled = oo->qty - oo->leaves;
        n.qty = (r.qty ? r.qty : oo->qty) - filled;
        n.price = r.price ? r.price : oo->px;
        return decide(h, n, out, oo);
    }

    // ---------------------------------------------------------------- introspection
    uint64_t accepts() const noexcept { return accepts_; }
    uint64_t rejects() const noexcept { return rejects_; }
    int64_t buyingPower(uint32_t acct) const noexcept { const auto& a = accounts_[acct]; return a.bpBase - a.bpCommitted; }
    uint32_t openOrders(uint32_t acct) const noexcept { return accounts_[acct].openOrders; }
    money::i128 grossExposure(uint32_t acct) const noexcept { const auto& a = accounts_[acct]; return a.grossPosition + a.openBuyNotional + a.openSellNotional; }
    money::i128 netExposure(uint32_t acct) const noexcept { const auto& a = accounts_[acct]; return a.netPosition + a.openBuyNotional - a.openSellNotional; }
    int64_t position(uint32_t acct, uint32_t sym) const noexcept { const int64_t* p = positions_.find(key(acct, sym)); return p ? *p : 0; }
    int64_t locateRemaining(uint64_t id) const noexcept { const Locate* l = locates_.find(id); return l ? l->remaining : -1; }
    int64_t openLeaves(uint64_t orderId) const noexcept { const OpenOrder* o = openOrders_.find(orderId); return o ? o->leaves : -1; }
    bool accountKnown(uint32_t acct) const noexcept { return acct < MAX_ACCOUNTS && accounts_[acct].known; }
    const RefData& refData() const noexcept { return rd_; }
    // The sliding rate window for an account: (bucket index, count in the last second).
    std::pair<int64_t, uint32_t> rateState(uint32_t acct) const noexcept { const auto& a = accounts_[acct]; return {a.rateBucketIdx, a.rateSum}; }

    std::array<uint8_t, 32> stateHash() const noexcept {
        blake3_hasher hs; blake3_hasher_init(&hs);
        for (uint32_t i = 0; i < MAX_ACCOUNTS; ++i) {
            const Account& a = accounts_[i];
            if (!a.known) continue;
            blake3_hasher_update(&hs, &i, 4);
            blake3_hasher_update(&hs, &a.bpBase, 8); blake3_hasher_update(&hs, &a.bpCommitted, 8);
            blake3_hasher_update(&hs, &a.openOrders, 4); blake3_hasher_update(&hs, &a.openBuyNotional, 16);
            blake3_hasher_update(&hs, &a.openSellNotional, 16); blake3_hasher_update(&hs, &a.grossPosition, 16);
            blake3_hasher_update(&hs, &a.netPosition, 16); blake3_hasher_update(&hs, &a.killed, 1);
        }
        blake3_hasher_update(&hs, &accepts_, 8); blake3_hasher_update(&hs, &rejects_, 8);
        size_t n1 = openOrders_.size(), n2 = locates_.size(), n3 = positions_.size();
        blake3_hasher_update(&hs, &n1, 8); blake3_hasher_update(&hs, &n2, 8); blake3_hasher_update(&hs, &n3, 8);
        auto rh = rd_.stateHash(); blake3_hasher_update(&hs, rh.data(), 32);
        std::array<uint8_t, 32> out{}; blake3_hasher_finalize(&hs, out.data(), 32); return out;
    }

private:
    struct DupEntry { uint64_t fp; int64_t ts; };
    struct Account {
        Limits limits; int64_t bpBase = 0, bpCommitted = 0;              // per account: 1e-8 units are ample
        money::i128 openBuyNotional = 0, openSellNotional = 0, grossPosition = 0, netPosition = 0;
        uint32_t openOrders = 0; bool known = false, killed = false;
        int64_t rateBucketIdx = -1; uint32_t rateSum = 0; std::array<uint16_t, RATE_BUCKETS> rateBuckets{};
        std::array<DupEntry, DUP_RING> dups{}; uint8_t dupHead = 0;
    };
    struct OpenOrder {
        uint32_t accountIdx, symbolIdx; int64_t qty, leaves, px, req; Side side; OrdType ordType; uint16_t strategyId; uint64_t locateId;
        bool buy() const noexcept { return side == Side::Buy; }
    };
    struct Locate { uint32_t accountIdx, symbolIdx; int64_t remaining, expiryTs; };

    static_assert(sizeof(OpenOrder) <= 64, "an open order must fit one cache line");
    static_assert(sizeof(Locate) <= 32);
    static uint64_t key(uint32_t a, uint32_t s) noexcept { return (uint64_t(a) << 32) | s; }
    static uint64_t clKey(const NewOrder& o) noexcept { return util::FlatSet64::fingerprint(Key128{(uint64_t(o.accountIdx) << 32) | o.sessionId, o.clOrdId}); }
    uint16_t primaryVenue(uint32_t) const noexcept { return 0; }   // SymbolStatus with venueId 0 is the consolidated status
    int64_t referencePrice(uint32_t s) const noexcept { return lastTrade_[s] ? lastTrade_[s] : rd_.refPrice(s); }
    int64_t requirement(uint32_t s, int64_t notional) const noexcept {
        uint8_t mc = rd_.marginClass(s); if (mc >= marginBps_.size()) mc = 2;
        return int64_t(i128(notional) * marginBps_[mc] / BPS);   // wide: notional x bps overflows int64 near the ceiling
    }
    const Limits& limitsFor(uint32_t acct, uint32_t sym) const noexcept {
        const Limits* l = symbolLimits_.find(key(acct, sym));
        if (l && l->set) return *l;
        if (accounts_[acct].limits.set) return accounts_[acct].limits;
        return firm_;
    }

    // Sliding window: ten 100 ms buckets on sequencer time. Counts every evaluation, accepted or not.
    bool rateExceeded(Account& a, int64_t seqTs, uint32_t limit) noexcept {
        int64_t idx = seqTs / RATE_BUCKET_NS;
        if (a.rateBucketIdx < 0) { a.rateBucketIdx = idx; a.rateSum = 0; a.rateBuckets.fill(0); }
        else if (idx > a.rateBucketIdx) {
            int64_t adv = idx - a.rateBucketIdx; if (adv > int64_t(RATE_BUCKETS)) adv = int64_t(RATE_BUCKETS);
            for (int64_t k = 1; k <= adv; ++k) { size_t s = size_t((a.rateBucketIdx + k) % int64_t(RATE_BUCKETS)); a.rateSum -= a.rateBuckets[s]; a.rateBuckets[s] = 0; }
            a.rateBucketIdx = idx;
        }
        ++a.rateBuckets[size_t(idx % int64_t(RATE_BUCKETS))]; ++a.rateSum;
        return a.rateSum > limit;
    }
    // Same symbol, side, quantity and price from the account within the window.
    bool duplicateOrder(Account& a, const NewOrder& o, int64_t seqTs, uint32_t windowMs) noexcept {
        uint64_t fp = util::mix64((uint64_t(o.symbolIdx) << 8) | uint64_t(o.side)) ^ util::mix64(uint64_t(o.qty) * 0x9E37ull) ^ util::mix64(uint64_t(o.price));
        bool dup = false; int64_t w = int64_t(windowMs) * 1'000'000;
        for (const auto& d : a.dups) if (d.fp == fp && d.ts && seqTs - d.ts <= w) { dup = true; break; }
        a.dups[a.dupHead] = DupEntry{fp, seqTs}; a.dupHead = uint8_t((a.dupHead + 1) % DUP_RING);
        return dup;
    }
    void uncommit(Account& a, OpenOrder& oo) noexcept {
        int64_t remaining = oo.leaves * oo.px;
        if (oo.buy()) a.openBuyNotional -= remaining; else a.openSellNotional -= remaining;
        a.bpCommitted -= requirement(oo.symbolIdx, remaining);
        if (a.openOrders) --a.openOrders;
        if (oo.locateId) { Locate* l = locates_.find(oo.locateId); if (l) l->remaining += oo.leaves; }
    }
    void recommit(Account& a, OpenOrder& oo) noexcept {
        int64_t remaining = oo.leaves * oo.px;
        if (oo.buy()) a.openBuyNotional += remaining; else a.openSellNotional += remaining;
        a.bpCommitted += requirement(oo.symbolIdx, remaining);
        ++a.openOrders;
        if (oo.locateId) { Locate* l = locates_.find(oo.locateId); if (l) l->remaining -= oo.leaves; }
    }

    // The check list. `replacing` is the open order a replace would modify; its commitments
    // are lifted while the new shape is checked and restored on a reject.
    RiskVerdict decide(const FrameHeader* h, const NewOrder& o, RiskDecision& out, OpenOrder* replacing) noexcept {
        out.orderId = o.orderId; out.accountIdx = o.accountIdx; out.symbolIdx = o.symbolIdx;
        out.checkMask |= 0; out.reason = uint16_t(Reason::NoReason); out.notional = 0; out.buyingPowerAfter = 0;
        if (!replacing) out.checkMask = 0;
        uint32_t& ck = out.checkMask;
        Account* acct = (o.accountIdx != 0 && o.accountIdx < MAX_ACCOUNTS) ? &accounts_[o.accountIdx] : nullptr;
        if (replacing && acct) uncommit(*acct, *replacing);
        auto reject = [&](Reason r) {
            if (replacing && acct) recommit(*acct, *replacing);
            out.verdict = RiskVerdict::Reject; out.reason = uint16_t(r); ++rejects_; return RiskVerdict::Reject;
        };

        // session: duplicate clOrdId
        ck |= Check::Session;
        uint64_t sk = clKey(o);
        if (clOrdIds_.contains(sk)) return reject(Reason::DuplicateClOrdId);

        // account
        ck |= Check::Account;
        if (!acct || !acct->known) return reject(Reason::UnknownAccount);
        Account& a = *acct;
        ck |= Check::Kill;
        if (firmKilled_) return reject(Reason::FirmKilled);
        if (a.killed) return reject(Reason::AccountKilled);
        if (o.strategyId != 0 && o.strategyId < strategyKilled_.size() && strategyKilled_[o.strategyId]) return reject(Reason::StrategyKilled);

        // message rate (sliding one-second window on sequencer time)
        const Limits& lim = limitsFor(o.accountIdx, o.symbolIdx);
        ck |= Check::MsgRate;
        if (lim.maxMsgRate && rateExceeded(a, h->seqTs, lim.maxMsgRate)) return reject(Reason::MsgRateExceeded);

        // symbol
        ck |= Check::Symbol;
        uint32_t s = o.symbolIdx;
        if (s == 0 || s >= MAX_SYMBOLS || rd_.status(s) == 0) return reject(Reason::UnknownSymbol);
        if (!rd_.tradable(s)) return reject(Reason::SymbolNotTradable);
        if (halted_[s]) return reject(Reason::SymbolHalted);
        if (rd_.restricted(s)) return reject(Reason::SymbolRestricted);

        // order shape
        ck |= Check::Qty | Check::Price;
        if (o.side == Side::Unset) return reject(Reason::BadSide);
        if (o.qty <= 0) return reject(Reason::QtyZero);
        if (o.minQty > o.qty) return reject(Reason::MinQtyExceedsQty);
        if (lim.maxOrderQty && o.qty > lim.maxOrderQty) return reject(Reason::MaxOrderQty);
        bool priced = o.ordType == OrdType::Limit || o.ordType == OrdType::StopLimit;
        if (priced && o.price <= 0) return reject(Reason::MissingPrice);
        if ((o.ordType == OrdType::Stop || o.ordType == OrdType::StopLimit) && o.stopPrice <= 0) return reject(Reason::MissingPrice);
        int64_t ref = referencePrice(s);
        int64_t px = priced ? o.price : ref;                       // market orders are sized at reference for limits
        i128 wide = i128(px) * o.qty;
        if (wide > INT64_MAX / 4) return reject(Reason::MaxOrderNotional);   // cannot be represented safely
        int64_t notional = int64_t(wide); out.notional = notional;
        if (lim.maxOrderNotional && notional > lim.maxOrderNotional) return reject(Reason::MaxOrderNotional);
        if (priced) {
            ck |= Check::Tick;
            int64_t tick = rd_.tick(s, o.price);
            if (tick > 0 && o.price % tick != 0) return reject(Reason::PriceNotOnTick);
            ck |= Check::Collar;
            if (lim.priceCollarBps && ref > 0) {
                int64_t dev = o.price > ref ? o.price - ref : ref - o.price;
                if (dev * BPS > int64_t(lim.priceCollarBps) * ref) return reject(Reason::PriceOutsideCollar);
            }
        }
        if (!replacing && lim.dupWindowMs) {
            ck |= Check::Duplicate;
            if (duplicateOrder(a, o, h->seqTs, lim.dupWindowMs)) return reject(Reason::DuplicateOrder);
        }

        // short sale
        Locate* loc = nullptr;
        if (o.side == Side::SellShort || o.side == Side::SellShortExempt) {
            ck |= Check::Short;
            if (!rd_.shortable(s)) return reject(Reason::ShortNotShortable);
            if (!rd_.easyToBorrow(s)) {
                ck |= Check::Locate;
                if (o.locateId == 0) return reject(Reason::LocateRequired);
                loc = locates_.find(o.locateId);
                if (!loc) return reject(Reason::LocateUnknown);
                if (loc->accountIdx != o.accountIdx || loc->symbolIdx != s) return reject(Reason::LocateWrongSymbol);
                if (loc->expiryTs && h->seqTs > loc->expiryTs) return reject(Reason::LocateExpired);
                if (loc->remaining < o.qty) return reject(Reason::LocateInsufficient);
            }
            if (o.side == Side::SellShort && rd_.ssrActive(s)) {
                ck |= Check::Ssr;
                // Rule 201: a short sale may not execute at or below the current best bid
                if (!priced || (bid_[s] > 0 && o.price <= bid_[s])) return reject(Reason::SsrPriceTest);
            }
        }

        // credit: open orders, exposure, buying power
        ck |= Check::OpenOrders;
        if (lim.maxOpenOrders && a.openOrders >= lim.maxOpenOrders) return reject(Reason::MaxOpenOrders);
        ck |= Check::Exposure;
        bool buy = o.side == Side::Buy;
        i128 gross = i128(a.grossPosition) + a.openBuyNotional + a.openSellNotional + notional;
        i128 net = i128(a.netPosition) + a.openBuyNotional - a.openSellNotional + (buy ? notional : -notional);
        if (lim.maxGrossExposure && gross > lim.maxGrossExposure) return reject(Reason::GrossExposure);
        if (lim.maxNetExposure && (net > lim.maxNetExposure || -net > lim.maxNetExposure)) return reject(Reason::NetExposure);
        // The only hard ceiling left is what an ExposureSnapshot can carry: 1e-4 units in int64,
        // about $922 trillion. Beyond that we could hold the number but could not report it.
        if (!money::reportable(gross) || !money::reportable(net)) return reject(Reason::GrossExposure);
        ck |= Check::BuyingPower;
        int64_t req = requirement(s, notional);
        int64_t bpAfter = a.bpBase - a.bpCommitted - req;
        out.buyingPowerAfter = bpAfter;
        if (bpAfter < 0) return reject(Reason::InsufficientBuyingPower);

        // accept: commit state
        clOrdIds_.insert(sk);
        a.bpCommitted += req; ++a.openOrders;
        if (buy) a.openBuyNotional += notional; else a.openSellNotional += notional;
        if (loc) loc->remaining -= o.qty;
        if (replacing) {
            int64_t filled = replacing->qty - replacing->leaves;
            replacing->qty = o.qty + filled; replacing->leaves = o.qty; replacing->px = px; replacing->req = req;
        } else {
            openOrders_.insert(o.orderId, OpenOrder{o.accountIdx, s, o.qty, o.qty, px, req, o.side, o.ordType, o.strategyId, loc ? o.locateId : 0});
        }
        out.verdict = RiskVerdict::Accept; ++accepts_;
        return RiskVerdict::Accept;
    }

    void onPositionSnapshot(const PositionSnapshot& p) noexcept {
        if (p.accountIdx >= MAX_ACCOUNTS) return;
        Account& a = accounts_[p.accountIdx];
        int64_t mark = p.markPx ? p.markPx : p.avgCost;
        int64_t& pos = positions_[key(p.accountIdx, p.symbolIdx)];
        int64_t before = pos < 0 ? -pos : pos; int64_t old = pos;
        pos = p.qty * mark; int64_t after = pos < 0 ? -pos : pos;
        a.grossPosition += after - before; a.netPosition += pos - old;
    }
    // Fills reduce the open commitment and move notional into position; buying power is released
    // only for the unfilled remainder (the filled part stays committed until the margin engine re-bases).
    void onExec(const ExecReport& e) noexcept {
        OpenOrder* oo = openOrders_.find(e.orderId);
        if (!oo) return;
        Account& a = accounts_[oo->accountIdx];
        if (e.execType == ExecType::PartialFill || e.execType == ExecType::Fill) {
            int64_t filled = e.lastQty * oo->px;
            if (oo->buy()) a.openBuyNotional -= filled; else a.openSellNotional -= filled;
            int64_t signedNotional = e.lastQty * e.lastPx * (oo->buy() ? 1 : -1);
            int64_t& pos = positions_[key(oo->accountIdx, oo->symbolIdx)];
            int64_t before = pos < 0 ? -pos : pos; pos += signedNotional; int64_t after = pos < 0 ? -pos : pos;
            a.grossPosition += after - before; a.netPosition += signedNotional;
            oo->leaves -= e.lastQty;
        }
        // execType Rejected with ordStatus still live is a cancel or replace reject: the order stands
        bool rejected = e.execType == ExecType::Rejected && e.ordStatus == OrdStatus::Rejected;
        if (e.execType == ExecType::Fill || e.execType == ExecType::Cancelled || rejected || e.execType == ExecType::Expired)
            closeOrder(e.orderId, *oo);
    }
    void onOrderState(const OrderState& s) noexcept {
        if (s.status == OrdStatus::Cancelled || s.status == OrdStatus::Rejected || s.status == OrdStatus::Expired || s.status == OrdStatus::Filled) {
            OpenOrder* oo = openOrders_.find(s.orderId);
            if (oo) closeOrder(s.orderId, *oo);
        }
    }
    void closeOrder(uint64_t orderId, OpenOrder& oo) noexcept {
        Account& a = accounts_[oo.accountIdx];
        int64_t remaining = oo.leaves * oo.px;
        if (oo.buy()) a.openBuyNotional -= remaining; else a.openSellNotional -= remaining;
        a.bpCommitted -= requirement(oo.symbolIdx, remaining);          // release the unfilled part
        if (a.openOrders) --a.openOrders;
        if (oo.locateId) { Locate* l = locates_.find(oo.locateId); if (l) l->remaining += oo.leaves; }
        openOrders_.erase(orderId);
    }

    RefData& rd_;
    Limits firm_;
    std::vector<Account> accounts_;
    FlatMap<uint64_t, Limits> symbolLimits_;
    FlatMap<uint64_t, OpenOrder> openOrders_;
    FlatMap<uint64_t, int64_t> positions_;       // (acct,sym) -> signed notional
    FlatMap<uint64_t, Locate> locates_;
    util::FlatSet64 clOrdIds_;
    std::array<int64_t, 8> marginBps_{};
    std::vector<int64_t> bid_, ask_, lastTrade_;
    std::vector<uint8_t> halted_;
    std::vector<uint8_t> strategyKilled_;
    bool firmKilled_ = false;
    uint64_t accepts_ = 0, rejects_ = 0;
};

} // namespace trading::risk
