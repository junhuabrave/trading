// sim/stub_router.hpp : stand-in for core/router (v0.3). One venue, one child per parent,
// placed on the least loaded session from sequenced VenueSessionStatus, priced at the
// parent's limit, expected cost from the book the engine has seen. It exists so the harness
// exercises the whole path (watermark batch, RouteDecision, ChildOrder, session placement)
// and so the cold replay has a routing decision to recompute.
#pragma once
#include "book_lite.hpp"
#include "blake3.h"
#include <array>

namespace trading::sim {

class StubRouter {
public:
    struct Plan { uint16_t venueId, sessionIdx; int64_t expectedCost; };
    explicit StubRouter(uint16_t venueId = 2, uint32_t sessions = 3) : venueId_(venueId), sessions_(sessions) { for (auto& s : sessionUp_) s = 1; }

    void apply(const FrameHeader* f) {
        if (auto* s = as<VenueSessionStatus>(f)) {
            if (s->venueId != venueId_ || s->sessionIdx >= MAX) return;
            sessionUp_[s->sessionIdx] = s->state == SessionState::Up; sessionLoad_[s->sessionIdx] = s->openOrders;
        } else if (auto* c = as<ChildOrder>(f)) {
            if (c->venueId == venueId_) ++children_;
        }
    }
    Plan plan(const NewOrder& o, const BookLite& book) const {
        uint16_t best = 0; bool found = false; uint64_t load = 0;
        for (uint16_t i = 0; i < sessions_ && i < MAX; ++i) {
            if (!sessionUp_[i]) continue;
            uint64_t l = uint64_t(sessionLoad_[i]) + placed_[i];
            if (!found || l < load) { best = i; load = l; found = true; }
        }
        int64_t touch = o.side == Side::Buy ? book.bestAsk(o.symbolIdx) : book.bestBid(o.symbolIdx);
        return Plan{venueId_, found ? best : uint16_t(0), (touch ? touch : o.price) * o.qty};
    }
    // Called when a plan is acted on, so the next plan in the same poll sees the load it just added.
    void placed(const Plan& p) { if (p.sessionIdx < MAX) ++placed_[p.sessionIdx]; }
    void fillChild(const NewOrder& o, const Plan& p, ChildOrder& c, uint64_t childId) const {
        c.childOrderId = childId; c.parentOrderId = o.orderId; c.accountIdx = o.accountIdx; c.symbolIdx = o.symbolIdx;
        c.venueId = p.venueId; c.venueSessionIdx = p.sessionIdx; c.side = o.side; c.ordType = o.ordType; c.tif = o.tif;
        c.orderFlags = o.orderFlags; c.strategyTag = 1; c.qty = o.qty; c.price = o.price; c.displayQty = o.displayQty;
    }
    void hashInto(blake3_hasher& h) const { blake3_hasher_update(&h, placed_.data(), placed_.size() * 8); blake3_hasher_update(&h, &children_, 8); }
    uint64_t children() const { return children_; }
    const std::array<uint64_t, 16>& placed() const { return placed_; }
private:
    static constexpr size_t MAX = 16;
    uint16_t venueId_; uint32_t sessions_;
    std::array<uint8_t, MAX> sessionUp_{}; std::array<uint32_t, MAX> sessionLoad_{}; std::array<uint64_t, MAX> placed_{};
    uint64_t children_ = 0;
};

} // namespace trading::sim
