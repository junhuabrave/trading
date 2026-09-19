// sim/feed_sim.hpp : synthetic market data. A random-walk level book per symbol around the
// reference price, emitting BookDelta (absolute quantities), Trade and Nbbo for one venue.
// Every message carries a venueSeq, monotonic within the feed: that, with the feedId the frame's
// streamId carries, is what identifies it and what a watermark records. Deterministic for a seed.
// Line-level behaviour (A/B, gaps, retransmit) belongs to the arbitrator drills once the feed
// layer exists (MD-3).
#pragma once
#include "trading.hpp"
#include "refdata.hpp"
#include <random>
#include <vector>

namespace trading::sim {

class FeedSim {
public:
    // venueSeqBase: a real feed's sequence does not restart because our handler attached. Starting
    // well away from 1 keeps the venue's sequence visibly distinct from any we assign, so a consumer
    // that confuses the two fails loudly instead of passing by coincidence.
    static constexpr uint64_t VENUE_SEQ_BASE = 1'000'000;
    FeedSim(const refdata::RefData& rd, uint32_t symbols, uint16_t venueId, uint16_t sourceId, uint64_t seed)
        : rd_(rd), n_(symbols), venue_(venueId), src_(sourceId), rng_(seed), mid_(symbols + 1), venueSeq_(VENUE_SEQ_BASE) {
        for (uint32_t s = 1; s <= n_; ++s) mid_[s] = rd_.refPrice(s);
    }
    static constexpr int64_t TICK = 1'000'000;   // $0.01

    // Emit a burst of up to `k` messages via `emit(frame)`; frames are stack temporaries, emit must copy or submit.
    template <class F> void burst(uint32_t k, int64_t now, F&& emit) {
        for (uint32_t i = 0; i < k; ++i) {
            uint32_t s = uint32_t(1 + rng_() % n_);
            if (mid_[s] <= 0) continue;
            uint32_t roll = uint32_t(rng_() % 10);
            if (roll < 7) {
                Frame<BookDelta> d; d.init(); d.header.sourceId = src_; d.header.originTs = now;
                d.body.symbolIdx = s; d.body.venueId = venue_;
                d.body.side = (rng_() & 1) ? BookSide::Bid : BookSide::Ask;
                int64_t level = int64_t(rng_() % 5);
                d.body.price = d.body.side == BookSide::Bid ? mid_[s] - TICK * (1 + level) : mid_[s] + TICK * (1 + level);
                d.body.action = (rng_() % 6 == 0) ? BookAction::Delete : BookAction::Set;
                d.body.qty = d.body.action == BookAction::Set ? int64_t(100 * (1 + rng_() % 40)) : 0;
                d.body.venueSeq = ++venueSeq_; d.body.venueTs = now; d.body.flags = BookFlags::endOfPacket;
                emit(&d.header); ++deltas_;
            } else if (roll < 9) {
                Frame<Trade> t; t.init(); t.header.sourceId = src_; t.header.originTs = now;
                t.body.symbolIdx = s; t.body.venueId = venue_;
                t.body.aggressorSide = (rng_() & 1) ? BookSide::Bid : BookSide::Ask;
                t.body.price = t.body.aggressorSide == BookSide::Bid ? mid_[s] + TICK : mid_[s] - TICK;
                t.body.qty = int64_t(100 * (1 + rng_() % 10)); t.body.venueSeq = ++venueSeq_; t.body.venueTs = now; t.body.tradeId = venueSeq_;
                if (rng_() % 4 == 0) mid_[s] += (rng_() & 1 ? TICK : -TICK);        // drift
                if (mid_[s] < 10 * TICK) mid_[s] = 10 * TICK;
                emit(&t.header); ++trades_;
            } else {
                Frame<Nbbo> n; n.init(); n.header.sourceId = src_; n.header.originTs = now;
                n.body.symbolIdx = s; n.body.bid = mid_[s] - TICK; n.body.ask = mid_[s] + TICK; n.body.bidQty = 500; n.body.askQty = 500;
                n.body.bidVenue = venue_; n.body.askVenue = venue_; n.body.sipBid = n.body.bid; n.body.sipAsk = n.body.ask;
                n.body.venueSeq = ++venueSeq_;
                emit(&n.header); ++nbbos_;
            }
        }
    }
    int64_t mid(uint32_t s) const { return mid_[s]; }
    uint64_t deltas() const { return deltas_; } uint64_t trades() const { return trades_; } uint64_t nbbos() const { return nbbos_; }
private:
    const refdata::RefData& rd_; uint32_t n_; uint16_t venue_, src_;
    std::mt19937_64 rng_; std::vector<int64_t> mid_; uint64_t venueSeq_, deltas_ = 0, trades_ = 0, nbbos_ = 0;
};

} // namespace trading::sim
