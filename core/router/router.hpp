// core/router/router.hpp : where best execution is either provable or not.
//
// The order manager hands this an accepted parent and asks for a plan. What comes back is up to
// eight children with a venue, a session, a quantity, a price and a release offset - and, just as
// importantly, the cost of the alternative that was not taken. That second number is the whole
// point. A router that only records what it did can say it routed somewhere; a router that records
// what else it could have done and what that would have cost can be asked, months later and by
// someone unsympathetic, why it chose what it chose.
//
// Three things constrain the arithmetic, in this order.
//
// Price priority is not an optimisation. A rebate can make a worse price look cheaper net, and
// taking it while a better protected quote stands is a trade-through. So venues are ordered by
// price first and by net cost only within a price, and the sweep exhausts every better price
// before it touches a worse one. Where it does cross several prices at once it marks the children
// ISO, which is what makes that lawful, and it is lawful only because it is simultaneously taking
// the better quotes. If a better-priced venue cannot be routed to at all - no session up - the
// sweep stops at that price rather than stepping over it, because an intermarket sweep order that
// does not actually sweep is a trade-through with a flag on it.
//
// Then cost. Expected net cost per share is the price the client pays or receives, adjusted by the
// venue's fee or rebate, weighted by how likely that venue is to fill, with the shortfall charged
// at what the fallback would cost, and with the scorecard's post-fill reversion added as the
// adverse-selection cost it is. Everything is per share in 1e-8 units and signed so that lower is
// better for the client on either side, which is the only way a buy and a sell can be compared by
// the same code.
//
// Then session placement: among the venue's sessions that are up and have throttle headroom, the
// least loaded, counting what this plan has already put on them. Symbol affinity and treating
// saturation as a cost rather than a wall wait for the venue gateways (RT-7).
//
// Deterministic: no clock, no map iteration order, venues walked by id. The same parent against the
// same book and the same scorecards produces the same plan, live and in replay, which is what lets
// the harness recompute every decision from the journal.
#pragma once
#include "book.hpp"
#include "flatmap.hpp"
#include "refdata.hpp"
#include "blake3.h"
#include <algorithm>
#include <array>

namespace trading::router {

inline constexpr size_t MAX_CHILDREN = 8;
inline constexpr size_t MAX_VENUES = 16;
inline constexpr size_t MAX_SESSIONS = 16;

// Why a plan came out the way it did. Recorded in RouteDecision.reason, so a best-execution review
// reads a cause rather than inferring one.
namespace PlanReason {
inline constexpr uint16_t Swept            = 1;   // displayed liquidity taken across venues
inline constexpr uint16_t SweptWithIso     = 2;   // and it crossed a protected quote, so ISO
inline constexpr uint16_t NoLiquidity      = 3;   // nothing displayed at or better than the limit
inline constexpr uint16_t PostedRemainder  = 4;   // what the sweep could not take was rested
inline constexpr uint16_t BlockedByBetter  = 5;   // a better price existed that we could not route to
inline constexpr uint16_t NoSession        = 6;   // no venue had a session that could carry it
inline constexpr uint16_t DarkFirst        = 7;   // resting at the midpoint where nobody can see it
inline constexpr uint16_t DarkDeclined     = 8;   // the client would not allow dark, so it went lit
inline constexpr uint16_t Passive          = 9;   // resting at the near touch for the rebate
inline constexpr uint16_t MidPeg           = 10;  // resting at the midpoint on a lit venue
inline constexpr uint16_t Escalated        = 11;  // the timer went off and what was resting crossed
}

struct Child {
    uint16_t venueId = 0, venueSessionIdx = 0, strategyTag = 0;
    int64_t  qty = 0, price = 0, displayQty = 0;
    OrdType  ordType = OrdType::Limit;
    Tif      tif = Tif::Day;
    uint8_t  orderFlags = 0;
    int64_t  releaseOffsetNs = 0;
    int64_t  expectedCostPerShare = 0;      // what the model thought this child would cost
};

struct Plan {
    uint8_t  count = 0;
    Child    children[MAX_CHILDREN]{};
    int64_t  expectedCost = 0;              // expected net, in 1e-8 units, lower is better
    int64_t  rejectedAltCost = 0;           // the same figure for the best alternative not taken
    uint16_t rejectedAltVenue = 0;
    uint16_t strategyTag = 0;
    uint16_t reason = 0;
    // How long the plan is willing to wait before it gives up on resting and crosses. Zero means
    // it is not waiting for anything. The engine arms a sequenced Timer for it; nothing in here
    // watches a clock.
    int64_t  waitNs = 0;

    int64_t qty() const noexcept {
        int64_t t = 0;
        for (uint8_t i = 0; i < count; ++i) t += children[i].qty;
        return t;
    }
};

class Router {
public:
    struct Params {
        uint16_t sourceId = 0;
        uint16_t maxVenueId = 4;
        uint16_t sessionsPerVenue = 3;
        // What the model assumes when the TCA service has not spoken yet. A fill rate of one and no
        // reversion is the neutral assumption: it makes the model reduce to price plus fee, which
        // is what a router without a scorecard should do.
        uint32_t defaultFillRateBps = 10'000;
        int32_t  defaultReversionBps = 0;
        // What not being filled is assumed to cost, over the price we would have paid. Charged
        // against the unfilled fraction so a venue that is cheap but unreliable is not free.
        int64_t  missPenaltyBps = 25;
        int64_t  maxChildren = MAX_CHILDREN;
        int64_t  darkWaitNs = 50'000'000;       // how long DarkFirst rests before it spills to lit
        int64_t  passiveWaitNs = 200'000'000;   // and how long PassivePost rests before it crosses
    };

    Router(const refdata::Snapshot& snap, Params p)
        : p_(p), fees_(256), accountProfile_(1024), escalated_(1024) {
        for (const FeeRecord& f : snap.fees()) putFee(f);
        for (auto& v : venue_) v = VenueState{};
        latency_.fill(0);
        kind_.fill(0);
        for (const VenueRecord& v : snap.venues())
            if (v.venueId < MAX_VENUES) { latency_[v.venueId] = v.oneWayLatencyNs; kind_[v.venueId] = v.venueType; }
        for (const AccountRecord& a : snap.accounts()) accountProfile_.insert(a.accountIdx, a.routingProfile);
        for (auto& pr : profile_) pr = Profile{};
    }

    void setBook(const md::BookReader* b) noexcept { book_ = b; }

    // Everything the router learns from the sequenced core stream.
    void apply(const FrameHeader* f) {
        if (const auto* s = as<VenueSessionStatus>(f)) {
            if (s->venueId >= MAX_VENUES || s->sessionIdx >= MAX_SESSIONS) return;
            Session& ss = venue_[s->venueId].sessions[s->sessionIdx];
            ss.up = s->state == SessionState::Up;
            ss.openOrders = s->openOrders;
            ss.queueDepth = s->queueDepth;
            ss.throttleUsedBps = s->throttleUsedBps;
            ss.known = true;
        } else if (const auto* c = as<VenueScorecard>(f)) {
            if (c->venueId >= MAX_VENUES) return;
            // symbolIdx 0 is the venue-wide card; a per-symbol card overrides it for that symbol.
            Scorecard& sc = c->symbolIdx == 0 ? venue_[c->venueId].card : venue_[c->venueId].card;
            sc.fillRateBps = c->fillRateBps;
            sc.reversionBps = c->reversionBps;
            sc.latencyNs = c->latencyP50Ns;
            sc.known = true;
            ++scorecards_;
        } else if (const auto* rp = as<RoutingProfileUpdate>(f)) {
            if (rp->profileId < profile_.size()) {
                Profile& pr = profile_[rp->profileId];
                pr.strategy = rp->strategy;
                pr.darkWaitNs = rp->params[0] ? rp->params[0] : p_.darkWaitNs;
                pr.passiveWaitNs = rp->params[1] ? rp->params[1] : p_.passiveWaitNs;
                pr.known = true;
                ++profiles_;
            }
        } else if (const auto* fs = as<FeeScheduleUpdate>(f)) {
            putFee(fs->record);
        } else if (const auto* ch = as<ChildOrder>(f)) {
            if (ch->venueId < MAX_VENUES) ++venue_[ch->venueId].children;
            ++children_;
        }
    }

    // The plan for `leaves` of this parent. Returns false when there is nothing routable at all,
    // which the caller turns into a reject or a wait rather than an empty child.
    // The plan for `leaves` of this parent. Returns false when there is nothing routable at all,
    // which the caller turns into a reject or a wait rather than an empty child.
    //
    // Which strategy runs comes from the account's routing profile, except when a timer has already
    // gone off for this parent: a strategy that was resting and ran out of patience crosses, and
    // that decision is recorded as its own reason rather than looking like a plain sweep.
    bool plan(const NewOrder& o, int64_t leaves, int64_t now, Plan& out) const {
        out = Plan{};
        if (leaves <= 0) { out.reason = PlanReason::NoLiquidity; return false; }
        const Profile pr = profileFor(o.accountIdx);
        const bool escalate = escalated_.contains(o.orderId);
        RouteStrategy st = escalate ? RouteStrategy::Sweep : pr.strategy;
        bool declinedDark = false;
        if (st == RouteStrategy::DarkFirst && !(o.orderFlags & OrderFlags::allowDark)) {
            st = RouteStrategy::Sweep;
            declinedDark = true;
            ++darkDeclined_;
        }
        out.strategyTag = uint16_t(st);
        bool ok;
        switch (st) {
            case RouteStrategy::DarkFirst:   ok = darkFirst(o, leaves, now, pr, out); break;
            case RouteStrategy::PassivePost: ok = passivePost(o, leaves, now, pr, out); break;
            case RouteStrategy::MidPeg:      ok = midPeg(o, leaves, now, out); break;
            default:                         ok = sweep(o, leaves, now, out); break;
        }
        // The reason the strategy chose is overwritten by what actually happened to the order,
        // because a review asks why this order went where it went. A client instruction that
        // changed the strategy outranks both: it is the only one of the three the client can see.
        if (ok && escalate) out.reason = PlanReason::Escalated;
        if (ok && declinedDark) out.reason = PlanReason::DarkDeclined;
        return ok;
    }

    // A timer armed for this parent has gone off: whatever it was waiting for is not coming, and
    // the next plan crosses. Cleared when the parent is done with.
    void onTimer(uint64_t parentOrderId) { escalated_.insert(parentOrderId, uint8_t(1)); ++escalations_; }
    void forget(uint64_t parentOrderId) { escalated_.erase(parentOrderId); }
    bool escalated(uint64_t parentOrderId) const { return escalated_.contains(parentOrderId); }

    // Tell the router a plan was acted on, so the next plan in the same poll sees the load it added.
    void placed(const Plan& p) noexcept {
        for (uint8_t i = 0; i < p.count; ++i) {
            const Child& c = p.children[i];
            if (c.venueId < MAX_VENUES && c.venueSessionIdx < MAX_SESSIONS)
                ++venue_[c.venueId].sessions[c.venueSessionIdx].placed;
        }
        ++plans_;
    }

    void hashInto(blake3_hasher& h) const {
        for (uint16_t v = 0; v < MAX_VENUES; ++v) {
            blake3_hasher_update(&h, &venue_[v].children, sizeof(uint64_t));
            for (const Session& s : venue_[v].sessions) blake3_hasher_update(&h, &s.placed, sizeof(uint64_t));
        }
        blake3_hasher_update(&h, &children_, sizeof children_);
    }

    uint64_t plans() const noexcept { return plans_; }
    uint64_t children() const noexcept { return children_; }
    uint64_t scorecards() const noexcept { return scorecards_; }
    uint64_t isoPlans() const noexcept { return isoPlans_; }
    uint64_t blockedByBetter() const noexcept { return blockedByBetter_; }
    uint64_t posted() const noexcept { return posted_; }
    uint64_t profiles() const noexcept { return profiles_; }
    uint64_t escalations() const noexcept { return escalations_; }
    uint64_t darkChildren() const noexcept { return darkChildren_; }
    uint64_t darkDeclined() const noexcept { return darkDeclined_; }
    uint64_t internalizeBlocked() const noexcept { return internalizeBlocked_; }
    uint8_t  venueKind(uint16_t venueId) const noexcept { return venueId < MAX_VENUES ? kind_[venueId] : 0; }
    uint64_t noLiquidity() const noexcept { return noLiquidity_; }
    int64_t  venueLatencyNs(uint16_t venueId) const noexcept { return venueId < MAX_VENUES ? latency_[venueId] : 0; }
    uint64_t placedOn(uint16_t venueId, uint16_t session) const noexcept {
        return venueId < MAX_VENUES && session < MAX_SESSIONS ? venue_[venueId].sessions[session].placed : 0;
    }
    // What every session of a venue has been given, for a harness that reports session balance.
    std::array<uint64_t, MAX_SESSIONS> placements(uint16_t venueId) const noexcept {
        std::array<uint64_t, MAX_SESSIONS> out{};
        if (venueId < MAX_VENUES) for (size_t i = 0; i < MAX_SESSIONS; ++i) out[i] = venue_[venueId].sessions[i].placed;
        return out;
    }

    // ---- the cost model, exposed so it can be unit-tested against worked examples ------------
    struct Quote { uint16_t venueId; int64_t price; int64_t qty; };

    int64_t feePerShare(uint16_t venueId, Liquidity liq) const noexcept {
        const int64_t* f = fees_.find(feeKey(venueId, liq));
        return f ? *f : 0;
    }
    // The price the client actually pays or receives per share, fee included, signed so that lower
    // is better on either side. A buyer's cost goes up with the price and with a taker fee; a
    // seller's goes down as the price goes up, so the seller's figure is the negated proceeds.
    int64_t netPerShare(uint16_t venueId, Side side, int64_t price, Liquidity liq) const noexcept {
        const int64_t fee = feePerShare(venueId, liq);
        return side == Side::Buy ? price + fee : fee - price;
    }
    // And what the model expects that to cost once the venue's own record is taken into account:
    // the fill it probably gets, the shortfall at what the fallback would cost, and the post-fill
    // reversion the scorecard has measured, which is a cost whichever way the order went.
    int64_t expectedPerShare(uint16_t venueId, Side side, int64_t price, Liquidity liq,
                             int64_t fallbackPerShare) const noexcept {
        const Scorecard& sc = card(venueId);
        const int64_t filled = netPerShare(venueId, side, price, liq);
        const int64_t pFillBps = sc.fillRateBps;
        const int64_t missBps = 10'000 - pFillBps;
        const int64_t reversion = price * sc.reversionBps / 10'000;
        int64_t v = (filled * pFillBps + fallbackPerShare * missBps) / 10'000;
        return v + reversion;
    }
    int64_t missPenaltyPerShare(int64_t price) const noexcept { return price * p_.missPenaltyBps / 10'000; }

private:
    struct Profile {
        RouteStrategy strategy = RouteStrategy::Sweep;
        int64_t darkWaitNs = 0, passiveWaitNs = 0;
        bool known = false;
    };
    struct Session {
        uint64_t placed = 0;
        uint32_t openOrders = 0, queueDepth = 0, throttleUsedBps = 0;
        bool up = false, known = false;
    };
    struct Scorecard { uint32_t fillRateBps = 0; int32_t reversionBps = 0; int64_t latencyNs = 0; bool known = false; };
    struct VenueState {
        std::array<Session, MAX_SESSIONS> sessions{};
        Scorecard card{};
        uint64_t children = 0;
    };

    static uint64_t feeKey(uint16_t venue, Liquidity liq) noexcept { return (uint64_t(venue) << 8) | uint8_t(liq); }
    void putFee(const FeeRecord& f) { fees_.insert(feeKey(f.venueId, Liquidity(f.liquidity)), f.feePerShare); }

    Scorecard card(uint16_t venueId) const noexcept {
        if (venueId < MAX_VENUES && venue_[venueId].card.known) return venue_[venueId].card;
        Scorecard d{};
        d.fillRateBps = p_.defaultFillRateBps;
        d.reversionBps = p_.defaultReversionBps;
        return d;
    }

    // The least loaded session that is up and has throttle headroom, counting what this plan has
    // already put on it. Returns false when the venue cannot carry the order at all.
    bool pickSession(uint16_t venueId, uint16_t& out) const noexcept {
        if (venueId >= MAX_VENUES) return false;
        const VenueState& v = venue_[venueId];
        bool found = false;
        uint64_t best = 0;
        for (uint16_t i = 0; i < p_.sessionsPerVenue && i < MAX_SESSIONS; ++i) {
            const Session& s = v.sessions[i];
            // A session we have never heard from is not a session. Routing to a venue on the
            // assumption that a gateway exists is the same fault as an intermarket sweep that does
            // not actually sweep: the order goes nowhere and the parent waits for an ack that is
            // never coming.
            if (!s.known || !s.up) continue;
            if (s.throttleUsedBps >= 10'000) continue;              // saturated: a wall until RT-7
            // What is already on the session, what is queued behind it, and what this plan has
            // just put there - the last one so two children of one parent do not pile onto the
            // same port because neither could see the other.
            const uint64_t load = uint64_t(s.openOrders) + s.queueDepth + s.placed;
            if (!found || load < best) { best = load; out = i; found = true; }
        }
        return found;
    }

    // Every displayed quote at or better than the limit, one per venue, from the shared book.
    // Venues are walked by id so the order never depends on anything but the data.
    size_t quotesAtOrBetter(const NewOrder& o, int64_t limit, Quote* into, size_t cap,
                            int64_t& bestProtected) const {
        size_t n = 0;
        bestProtected = 0;
        if (!book_) return 0;
        const bool buy = o.side == Side::Buy;
        for (uint16_t v = 1; v <= p_.maxVenueId && v < MAX_VENUES; ++v) {
            md::LadderSnapshot l{};
            if (!book_->ladder(o.symbolIdx, v, l)) continue;
            const uint64_t mask = buy ? l.askMask : l.bidMask;
            if (!mask) continue;
            const int slot = buy ? std::countr_zero(mask) : 63 - std::countl_zero(mask);
            const int64_t px = l.priceAt(buy ? BookSide::Ask : BookSide::Bid, slot);
            const int64_t qty = (buy ? l.askQty : l.bidQty)[slot];
            if (qty <= 0) continue;
            // The protected quote is the best displayed price anywhere, limit or no limit: it is
            // what a trade-through is measured against.
            if (!bestProtected || (buy ? px < bestProtected : px > bestProtected)) bestProtected = px;
            if (limit && (buy ? px > limit : px < limit)) continue;
            if (n < cap) into[n++] = Quote{v, px, qty};
        }
        return n;
    }

    Profile profileFor(uint32_t accountIdx) const {
        const uint16_t* id = accountProfile_.find(accountIdx);
        if (id && *id < profile_.size() && profile_[*id].known) return profile_[*id];
        Profile d{};
        d.strategy = RouteStrategy::Sweep;
        d.darkWaitNs = p_.darkWaitNs;
        d.passiveWaitNs = p_.passiveWaitNs;
        return d;
    }
    bool isDark(uint16_t venueId) const noexcept { return venueKind(venueId) == uint8_t(VenueType::Ats); }
    // A client that says no internalisation means it: neither our own book nor a wholesaler.
    bool internalising(uint16_t venueId) const noexcept {
        const uint8_t k = venueKind(venueId);
        return k == uint8_t(VenueType::Internal) || k == uint8_t(VenueType::Wholesaler);
    }
    bool allowed(const NewOrder& o, uint16_t venueId) const noexcept {
        if (isDark(venueId) && !(o.orderFlags & OrderFlags::allowDark)) return false;
        if (internalising(venueId) && (o.orderFlags & OrderFlags::noInternalize)) return false;
        return true;
    }
    // The midpoint of the consolidated market, which is the only price a dark venue will print at.
    bool midpoint(uint32_t symbolIdx, int64_t& out) const noexcept {
        if (!book_) return false;
        md::TopBody t{};
        if (!book_->top(symbolIdx, t)) return false;
        if (!t.bid || !t.ask || t.bid >= t.ask) return false;   // no two-sided market, no midpoint
        out = (t.bid + t.ask) / 2;
        return true;
    }

    bool sweep(const NewOrder& o, int64_t leaves, int64_t now, Plan& out) const;
    bool darkFirst(const NewOrder& o, int64_t leaves, int64_t now, const Profile& pr, Plan& out) const;
    bool passivePost(const NewOrder& o, int64_t leaves, int64_t now, const Profile& pr, Plan& out) const;
    bool midPeg(const NewOrder& o, int64_t leaves, int64_t now, Plan& out) const;

    Params p_;
    util::FlatMap<uint64_t, int64_t> fees_;
    const md::BookReader* book_ = nullptr;
    mutable std::array<VenueState, MAX_VENUES> venue_{};
    std::array<int64_t, MAX_VENUES> latency_{};
    std::array<uint8_t, MAX_VENUES> kind_{};          // VenueType, so dark is a fact and not a guess
    std::array<Profile, 256> profile_{};
    util::FlatMap<uint64_t, uint16_t> accountProfile_;
    mutable util::FlatMap<uint64_t, uint8_t> escalated_;
    mutable uint64_t plans_ = 0, children_ = 0, scorecards_ = 0, isoPlans_ = 0, blockedByBetter_ = 0;
    mutable uint64_t posted_ = 0, noLiquidity_ = 0, profiles_ = 0, escalations_ = 0;
    mutable uint64_t darkChildren_ = 0, darkDeclined_ = 0, internalizeBlocked_ = 0;
};

// ---- the sweep ---------------------------------------------------------------------------------
inline bool Router::sweep(const NewOrder& o, int64_t leaves, int64_t now, Plan& out) const {
    (void)now;
    const bool buy = o.side == Side::Buy;
    const int64_t limit = o.ordType == OrdType::Market ? 0 : o.price;

    Quote q[MAX_VENUES];
    int64_t bestProtected = 0;
    const size_t n = quotesAtOrBetter(o, limit, q, MAX_VENUES, bestProtected);

    struct Ranked { Quote quote; int64_t expected; uint16_t session; bool routable; };
    Ranked r[MAX_VENUES];
    size_t rn = 0;
    for (size_t i = 0; i < n; ++i) {
        Ranked x{};
        x.quote = q[i];
        uint16_t s = 0;
        x.routable = pickSession(q[i].venueId, s);
        x.session = s;
        // What it costs to miss here: the same price plus the penalty, which is the model's stand-in
        // for having to come back and pay up somewhere else.
        const int64_t fallback = netPerShare(q[i].venueId, o.side, q[i].price, Liquidity::Removed)
                               + missPenaltyPerShare(q[i].price);
        x.expected = expectedPerShare(q[i].venueId, o.side, q[i].price, Liquidity::Removed, fallback);
        r[rn++] = x;
    }
    // Price first, because price priority is regulation and not preference. Net cost decides only
    // between venues showing the same price, and the venue id makes the order total so that two
    // runs over the same book sort identically.
    std::sort(r, r + rn, [&](const Ranked& a, const Ranked& b) {
        if (a.quote.price != b.quote.price) return buy ? a.quote.price < b.quote.price : a.quote.price > b.quote.price;
        if (a.expected != b.expected) return a.expected < b.expected;
        return a.quote.venueId < b.quote.venueId;
    });

    int64_t remaining = leaves, worstTaken = 0;
    bool blocked = false;
    // A post-only order has said it will not take liquidity. Sweeping one would have the venue
    // reject every child, so the taking phase is skipped and the whole quantity is rested.
    const bool postOnly = (o.orderFlags & OrderFlags::postOnly) != 0;
    for (size_t i = 0; !postOnly && i < rn && remaining > 0 && out.count < p_.maxChildren; ++i) {
        // A better price we cannot reach stops the sweep here. Stepping over it would be a
        // trade-through, and an ISO does not excuse one: the flag is lawful only because the
        // better quote is being taken at the same time, which it cannot be if there is no session.
        if (!r[i].routable) { blocked = true; break; }
        const int64_t take = std::min(remaining, r[i].quote.qty);
        if (take <= 0) continue;
        Child& c = out.children[out.count++];
        c.venueId = r[i].quote.venueId;
        c.venueSessionIdx = r[i].session;
        c.qty = take;
        c.price = r[i].quote.price;
        c.displayQty = take;
        c.ordType = OrdType::Limit;
        c.tif = Tif::Ioc;                                   // a sweep takes or it does not
        c.orderFlags = o.orderFlags;
        c.strategyTag = uint16_t(RouteStrategy::Sweep);
        c.expectedCostPerShare = r[i].expected;
        remaining -= take;
        worstTaken = c.price;
    }
    if (blocked) ++blockedByBetter_;

    // Crossing several prices at once is lawful as an intermarket sweep, and only as one, because
    // the better quotes are in the same plan and go out together.
    const bool crossed = out.count > 0 && bestProtected
                      && (buy ? worstTaken > bestProtected : worstTaken < bestProtected);
    if (crossed) {
        for (uint8_t i = 0; i < out.count; ++i) out.children[i].orderFlags = uint8_t(out.children[i].orderFlags | OrderFlags::iso);
        ++isoPlans_;
    }

    // What the sweep could not take rests at the limit, where posting pays best. A market order
    // rests nothing: it asked to trade, not to quote.
    bool postedHere = false;
    if (remaining > 0 && out.count < p_.maxChildren && limit != 0) {
        uint16_t bestVenue = 0, bestSession = 0;
        int64_t bestCost = 0;
        bool have = false;
        for (uint16_t v = 1; v <= p_.maxVenueId && v < MAX_VENUES; ++v) {
            uint16_t s = 0;
            if (!pickSession(v, s)) continue;
            const int64_t fallback = netPerShare(v, o.side, limit, Liquidity::Added) + missPenaltyPerShare(limit);
            const int64_t cost = expectedPerShare(v, o.side, limit, Liquidity::Added, fallback);
            if (!have || cost < bestCost) { bestCost = cost; bestVenue = v; bestSession = s; have = true; }
        }
        if (have) {
            Child& c = out.children[out.count++];
            c.venueId = bestVenue;
            c.venueSessionIdx = bestSession;
            c.qty = remaining;
            c.price = limit;
            c.displayQty = o.displayQty ? std::min(o.displayQty, remaining) : remaining;
            c.ordType = OrdType::Limit;
            c.tif = o.tif;
            c.orderFlags = o.orderFlags;
            c.strategyTag = uint16_t(RouteStrategy::Sweep);
            c.expectedCostPerShare = bestCost;
            remaining = 0;
            postedHere = true;
            ++posted_;
        }
    }

    if (out.count == 0) {
        out.reason = blocked ? PlanReason::BlockedByBetter : (rn ? PlanReason::NoSession : PlanReason::NoLiquidity);
        ++noLiquidity_;
        return false;
    }
    // The reason names what actually happened to this parent, worst first: a better price we could
    // not reach is the thing a reviewer wants to find, then an intermarket sweep, then a remainder
    // left resting, and only then a plain sweep.
    if (blocked)             out.reason = PlanReason::BlockedByBetter;
    else if (crossed)        out.reason = PlanReason::SweptWithIso;
    else if (postedHere)     out.reason = PlanReason::PostedRemainder;
    else                     out.reason = PlanReason::Swept;

    // Children land together: the slowest venue in the plan sets the clock and everything faster
    // waits for it, so one venue does not print before another has seen the order.
    int64_t slowest = 0;
    for (uint8_t i = 0; i < out.count; ++i) slowest = std::max(slowest, venueLatencyNs(out.children[i].venueId));
    for (uint8_t i = 0; i < out.count; ++i)
        out.children[i].releaseOffsetNs = slowest - venueLatencyNs(out.children[i].venueId);

    for (uint8_t i = 0; i < out.count; ++i) out.expectedCost += out.children[i].expectedCostPerShare * out.children[i].qty;

    // The runner-up, which is the number a best-execution review actually asks for: what the whole
    // order on one venue would have cost. When the plan is already one venue, the alternative is
    // the next venue; when it is a sweep, it is the best single venue.
    bool haveAlt = false;
    for (size_t i = 0; i < rn; ++i) {
        if (out.count == 1 && r[i].quote.venueId == out.children[0].venueId) continue;
        const int64_t cost = r[i].expected * leaves;
        if (!haveAlt || cost < out.rejectedAltCost) { out.rejectedAltCost = cost; out.rejectedAltVenue = r[i].quote.venueId; haveAlt = true; }
    }
    return true;
}

// ---- DarkFirst -----------------------------------------------------------------------------
// Rest at the midpoint where nobody can see it, for as long as the profile allows, then spill to
// lit. The point is price improvement: a midpoint fill is half the spread better than crossing,
// and nothing about the order is displayed while it waits. What it costs is time, which is why the
// plan carries a deadline and the engine arms a timer for it.
inline bool Router::darkFirst(const NewOrder& o, int64_t leaves, int64_t now, const Profile& pr, Plan& out) const {
    int64_t mid = 0;
    if (!midpoint(o.symbolIdx, mid)) return sweep(o, leaves, now, out);   // no two-sided market to peg to
    // A midpoint worse than the client's limit is not a price we may pay.
    if (o.ordType != OrdType::Market && o.price) {
        if (o.side == Side::Buy ? mid > o.price : mid < o.price) return sweep(o, leaves, now, out);
    }
    for (uint16_t v = 1; v <= p_.maxVenueId && v < MAX_VENUES; ++v) {
        if (!isDark(v) || !allowed(o, v)) continue;
        uint16_t sess = 0;
        if (!pickSession(v, sess)) continue;
        Child& c = out.children[out.count++];
        c.venueId = v;
        c.venueSessionIdx = sess;
        c.qty = leaves;
        c.price = mid;                                   // a midpoint peg, which may sit inside the tick
        c.displayQty = 0;                                // dark: nothing is shown
        c.ordType = OrdType::Limit;
        c.tif = Tif::Day;
        c.orderFlags = o.orderFlags;
        c.strategyTag = uint16_t(RouteStrategy::DarkFirst);
        c.expectedCostPerShare = netPerShare(v, o.side, mid, Liquidity::Added);
        out.expectedCost += c.expectedCostPerShare * c.qty;
        ++darkChildren_;
        break;                                           // one dark venue at a time; the timer spills the rest
    }
    if (out.count == 0) { ++darkDeclined_; return sweep(o, leaves, now, out); }
    out.reason = PlanReason::DarkFirst;
    out.waitNs = pr.darkWaitNs;
    // The runner-up is the lit market: what crossing now would have cost, which is the number that
    // says whether the wait was worth it.
    Plan lit{};
    if (sweep(o, leaves, now, lit) && lit.count) {
        out.rejectedAltCost = lit.expectedCost;
        out.rejectedAltVenue = lit.children[0].venueId;
    }
    return true;
}

// ---- PassivePost ---------------------------------------------------------------------------
// Rest at the near touch where the rebate and the fill rate are best, and cross when the timer says
// the wait has cost more than the rebate is worth. A buyer joins the bid rather than paying the
// offer, which earns the spread instead of paying it - when it fills.
inline bool Router::passivePost(const NewOrder& o, int64_t leaves, int64_t now, const Profile& pr, Plan& out) const {
    if (!book_) return sweep(o, leaves, now, out);
    const bool buy = o.side == Side::Buy;
    // The near touch: where a passive order joins the queue rather than crossing it.
    int64_t rest = 0;
    md::TopBody t{};
    if (book_->top(o.symbolIdx, t)) rest = buy ? t.bid : t.ask;
    if (!rest) rest = o.price;
    if (!rest) return sweep(o, leaves, now, out);
    if (o.ordType != OrdType::Market && o.price) {
        if (buy ? rest > o.price : rest < o.price) rest = o.price;   // never through the client's limit
    }
    uint16_t bestVenue = 0, bestSession = 0;
    int64_t bestCost = 0;
    bool have = false;
    for (uint16_t v = 1; v <= p_.maxVenueId && v < MAX_VENUES; ++v) {
        if (isDark(v) || !allowed(o, v)) continue;       // a passive post is a displayed quote
        uint16_t sess = 0;
        if (!pickSession(v, sess)) continue;
        const int64_t fallback = netPerShare(v, o.side, rest, Liquidity::Added) + missPenaltyPerShare(rest);
        const int64_t cost = expectedPerShare(v, o.side, rest, Liquidity::Added, fallback);
        if (!have || cost < bestCost) { bestCost = cost; bestVenue = v; bestSession = sess; have = true; }
    }
    if (!have) return sweep(o, leaves, now, out);
    Child& c = out.children[out.count++];
    c.venueId = bestVenue;
    c.venueSessionIdx = bestSession;
    c.qty = leaves;
    c.price = rest;
    c.displayQty = o.displayQty ? std::min(o.displayQty, leaves) : leaves;
    c.ordType = OrdType::Limit;
    c.tif = Tif::Day;
    c.orderFlags = uint8_t(o.orderFlags | OrderFlags::postOnly);   // it is a post; it must not take
    c.strategyTag = uint16_t(RouteStrategy::PassivePost);
    c.expectedCostPerShare = bestCost;
    out.expectedCost = bestCost * leaves;
    out.reason = PlanReason::Passive;
    out.waitNs = pr.passiveWaitNs;
    ++posted_;
    Plan lit{};
    if (sweep(o, leaves, now, lit) && lit.count) {
        out.rejectedAltCost = lit.expectedCost;
        out.rejectedAltVenue = lit.children[0].venueId;
    }
    return true;
}

// ---- MidPeg --------------------------------------------------------------------------------
// Rest at the midpoint on a lit venue and follow it. Unlike DarkFirst this is a displayed venue's
// pegged order type, so it is visible as a peg even though the price is not a tick.
inline bool Router::midPeg(const NewOrder& o, int64_t leaves, int64_t now, Plan& out) const {
    int64_t mid = 0;
    if (!midpoint(o.symbolIdx, mid)) return sweep(o, leaves, now, out);
    if (o.ordType != OrdType::Market && o.price) {
        if (o.side == Side::Buy ? mid > o.price : mid < o.price) mid = o.price;
    }
    uint16_t bestVenue = 0, bestSession = 0;
    int64_t bestCost = 0;
    bool have = false;
    for (uint16_t v = 1; v <= p_.maxVenueId && v < MAX_VENUES; ++v) {
        if (isDark(v) || !allowed(o, v)) continue;
        uint16_t sess = 0;
        if (!pickSession(v, sess)) continue;
        const int64_t cost = netPerShare(v, o.side, mid, Liquidity::Added);
        if (!have || cost < bestCost) { bestCost = cost; bestVenue = v; bestSession = sess; have = true; }
    }
    if (!have) return sweep(o, leaves, now, out);
    Child& c = out.children[out.count++];
    c.venueId = bestVenue;
    c.venueSessionIdx = bestSession;
    c.qty = leaves;
    c.price = mid;
    c.displayQty = leaves;
    c.ordType = OrdType::Pegged;
    c.tif = Tif::Day;
    c.orderFlags = o.orderFlags;
    c.strategyTag = uint16_t(RouteStrategy::MidPeg);
    c.expectedCostPerShare = bestCost;
    out.expectedCost = bestCost * leaves;
    out.reason = PlanReason::MidPeg;
    return true;
}

} // namespace trading::router
