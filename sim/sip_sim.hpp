// sim/sip_sim.hpp : the consolidated tape, behaving the way it does.
//
// The SIP is fed by the same venues as the direct feeds and publishes the same prices, later. What
// makes it worth simulating is not the lag itself - a decoder that handles lag handles nothing -
// but the four things that follow from it: a quote that is merely behind and catches up, a quote
// that is wrong and stays wrong, a national best that reads crossed against what we can see, and a
// tape that stops. Those are the cases the divergence monitor exists to tell apart, so they are
// what this produces.
#pragma once
#include "sip.hpp"
#include <deque>
#include <random>
#include <string>

namespace trading::sim {

using namespace trading::md;

class SipSim {
public:
    struct Params {
        sip::Dialect dialect = sip::Dialect::Utp;
        uint8_t  participant = 'Q';       // the venue whose quote the tape is carrying
        int64_t  lagNs = 250'000;         // the tape is behind; this alone is not a divergence
        double   disagreePct = 0.0;       // and sometimes it is simply wrong
        double   crossPct = 0.0;          // wrong in the direction that reads as a crossed market
        double   stallPct = 0.0;          // and sometimes it stops
        int64_t  stallNs = 5'000'000'000;
        bool     shortForm = true;
        uint64_t seed = 1;
    };

    explicit SipSim(Params p) : p_(p), rng_(p.seed) {}

    // The direct feeds moved. The tape will say so, later, and not always correctly.
    void quote(uint32_t symbolIdx, std::string_view ticker, int64_t bid, int64_t bidQty,
               int64_t ask, int64_t askQty, int64_t ts) {
        if (stallUntil_ > ts) { ++stalled_; return; }
        if (roll() < p_.stallPct) { stallUntil_ = ts + p_.stallNs; ++stalls_; ++stalled_; return; }
        Pending q{ts + p_.lagNs, symbolIdx, std::string(ticker), bid, bidQty, ask, askQty, false};
        if (roll() < p_.crossPct) {
            // The tape's best bid sits at or through what we can see on the offer: the shape a
            // wrong feed takes, and the one that makes an order look marketable when it is not.
            q.bid = ask + TICK; q.ask = q.bid + TICK; q.wrong = true; ++crossed_;
        } else if (roll() < p_.disagreePct) {
            q.bid = bid - TICK; q.ask = ask + TICK; q.wrong = true; ++wrong_;
        }
        queue_.push_back(std::move(q));
    }

    // Release everything the tape is ready to publish. emit(bytes, len) takes one SIP message.
    template <class F> void advance(int64_t now, F&& emit) {
        while (!queue_.empty() && queue_.front().releaseAt <= now) {
            const Pending q = std::move(queue_.front());
            queue_.pop_front();
            sip::Writer w(buf_);
            w.header(sip::category::Quote, p_.shortForm ? sip::type::ShortQuote : sip::type::LongQuote,
                     p_.participant, ++seq_, uint64_t(q.releaseAt), q.ticker);
            const size_t len = w.quote(q.bid, q.bidQty, q.ask, q.askQty, q.bid, q.bidQty, q.ask, q.askQty,
                                       p_.participant, p_.participant, sip::QuoteCondition::Normal, p_.shortForm);
            emit(buf_, len);
            ++published_;
        }
    }
    template <class F> void print(std::string_view ticker, int64_t price, int64_t volume, int64_t ts, F&& emit) {
        sip::Writer w(buf_);
        w.header(sip::category::Trade, sip::type::ShortTrade, p_.participant, ++seq_, uint64_t(ts), ticker);
        emit(buf_, w.trade(price, volume, '@'));
        ++published_;
    }
    template <class F> void action(std::string_view ticker, char what, int64_t ts, F&& emit) {
        sip::Writer w(buf_);
        w.header(sip::category::Admin, sip::type::Action, p_.participant, ++seq_, uint64_t(ts), ticker);
        emit(buf_, w.action(what, 'T'));
        ++published_;
    }

    static constexpr int64_t TICK = 1'000'000;      // a cent at 1e-8
    uint64_t published() const noexcept { return published_; }
    uint64_t wrong() const noexcept { return wrong_; }
    uint64_t crossed() const noexcept { return crossed_; }
    uint64_t stalls() const noexcept { return stalls_; }
    uint64_t stalled() const noexcept { return stalled_; }   // updates the tape never published
    size_t   inFlight() const noexcept { return queue_.size(); }

private:
    struct Pending {
        int64_t releaseAt; uint32_t symbolIdx; std::string ticker;
        int64_t bid, bidQty, ask, askQty; bool wrong;
    };
    double roll() { return double(rng_() % 1'000'000) / 1'000'000.0; }

    Params p_;
    std::mt19937_64 rng_;
    std::deque<Pending> queue_;
    uint8_t buf_[sip::MAX_MESSAGE]{};
    int64_t stallUntil_ = 0;
    uint32_t seq_ = 0;
    uint64_t published_ = 0, wrong_ = 0, crossed_ = 0, stalls_ = 0, stalled_ = 0;
};

} // namespace trading::sim
