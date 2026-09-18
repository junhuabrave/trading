// sim/client_sim.hpp : synthetic client flow. Random orders around the mid, cancels of live
// orders, and an adversarial mode (duplicate ids, rate bursts, off-tick prices, cancels of
// unknown orders, short sales without locates). Deterministic for a seed.
#pragma once
#include "trading.hpp"
#include <random>
#include <vector>

namespace trading::sim {

class ClientSim {
public:
    struct Params { uint32_t accountIdx = 42; uint16_t sourceId = 2; uint32_t sessionId = 9; uint32_t symbols = 10; bool adversarial = false; int shortPct = 15; int cancelPct = 30; };
    ClientSim(Params p, uint64_t seed) : p_(p), rng_(seed) {}

    // Produce one client action for this step. midOf(sym) gives the current mid. emit(frame) sequences it.
    template <class Mid, class F> void step(int64_t now, Mid&& midOf, F&& emit) {
        uint32_t roll = uint32_t(rng_() % 100);
        if (roll < uint32_t(p_.cancelPct) && !live_.empty()) {
            size_t k = rng_() % live_.size();
            Frame<CancelOrder> c; c.init(); c.header.sourceId = p_.sourceId; c.header.originTs = now;
            c.body.orderId = live_[k]; c.body.clOrdId = ++clOrd_; c.body.accountIdx = p_.accountIdx; c.body.sessionId = p_.sessionId;
            if (p_.adversarial && rng_() % 20 == 0) c.body.orderId = 0xBAD0000 + (rng_() % 100);      // cancel of an unknown order
            else { live_[k] = live_.back(); live_.pop_back(); }
            emit(&c.header); ++cancels_;
            return;
        }
        uint32_t s = uint32_t(1 + rng_() % p_.symbols);
        int64_t mid = midOf(s); if (mid <= 0) return;
        Frame<NewOrder> o; o.init(); o.header.sourceId = p_.sourceId; o.header.originTs = now;
        NewOrder& b = o.body;
        b.orderId = (uint64_t(p_.sourceId) << 48) | ++orders_;
        b.clOrdId = (p_.adversarial && rng_() % 25 == 0 && clOrd_ > 0) ? clOrd_ : ++clOrd_;             // sometimes a duplicate id
        b.accountIdx = p_.accountIdx; b.symbolIdx = s; b.sessionId = p_.sessionId;
        int side = int(rng_() % 100);
        b.side = side < p_.shortPct ? Side::SellShort : (side & 1) ? Side::Buy : Side::Sell;
        b.ordType = OrdType::Limit; b.tif = (rng_() % 8 == 0) ? Tif::Ioc : Tif::Day;
        b.qty = int64_t(100 * (1 + rng_() % 10));
        int64_t off = (int64_t(rng_() % 7) - 3) * 1'000'000;                                             // within 3 ticks of mid
        b.price = mid + off;
        if (p_.adversarial && rng_() % 15 == 0) b.price += 3;                                             // off tick
        if (p_.adversarial && rng_() % 30 == 0) b.qty = 0;
        if (p_.adversarial && rng_() % 40 == 0) b.price = mid * 2;                                        // collar
        b.clientTag = orders_;
        live_.push_back(b.orderId);
        if (live_.size() > 400) { live_[rng_() % live_.size()] = live_.back(); live_.pop_back(); }
        emit(&o.header);
    }
    // Rate burst: n orders in one call, for the message-rate check.
    template <class Mid, class F> void burst(uint32_t n, int64_t now, Mid&& midOf, F&& emit) { for (uint32_t i = 0; i < n; ++i) step(now, midOf, emit); }
    uint64_t orders() const { return orders_; } uint64_t cancels() const { return cancels_; }
private:
    Params p_; std::mt19937_64 rng_; std::vector<uint64_t> live_; uint64_t orders_ = 0, clOrd_ = 0, cancels_ = 0;
};

} // namespace trading::sim
