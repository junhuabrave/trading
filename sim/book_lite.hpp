// sim/book_lite.hpp : per-symbol, single-venue price-level book for the venue simulator.
//
// The engine and the router no longer use this: they read core/md/book.hpp's flat ladders through
// the shared view, which is what production does. What is left for it is the venue simulator's own
// side of the market - the book a venue keeps of the orders resting on it - where a std::map of
// price to quantity is the clearest thing to read and nothing is on a hot path.
#pragma once
#include "trading.hpp"
#include <map>
#include <vector>

namespace trading::sim {

class BookLite {
public:
    explicit BookLite(size_t symbols = 4096) : bids_(symbols), asks_(symbols) {}
    void apply(const BookDelta& d) {
        if (d.symbolIdx >= bids_.size()) return;
        auto& side = d.side == BookSide::Bid ? bids_[d.symbolIdx] : asks_[d.symbolIdx];
        if (d.action == BookAction::Set) { if (d.qty > 0) side[d.price] = d.qty; else side.erase(d.price); }
        else if (d.action == BookAction::Delete) side.erase(d.price);
        else if (d.action == BookAction::ClearSide) side.clear();
        else if (d.action == BookAction::ClearBook) { bids_[d.symbolIdx].clear(); asks_[d.symbolIdx].clear(); }
    }
    int64_t bestBid(uint32_t s) const { return s < bids_.size() && !bids_[s].empty() ? bids_[s].rbegin()->first : 0; }
    int64_t bestAsk(uint32_t s) const { return s < asks_.size() && !asks_[s].empty() ? asks_[s].begin()->first : 0; }
    int64_t qtyAt(uint32_t s, BookSide side, int64_t px) const {
        const auto& m = side == BookSide::Bid ? bids_[s] : asks_[s];
        auto it = m.find(px); return it == m.end() ? 0 : it->second;
    }
    std::map<int64_t, int64_t>& bids(uint32_t s) { return bids_[s]; }
    std::map<int64_t, int64_t>& asks(uint32_t s) { return asks_[s]; }
private:
    std::vector<std::map<int64_t, int64_t>> bids_, asks_;
};

} // namespace trading::sim
