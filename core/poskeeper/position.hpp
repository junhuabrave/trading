// core/poskeeper/position.hpp : what a position is, and the one function that changes it.
//
// A position is a signed quantity and the average cost of holding it. Buying adds to a long at the
// weighted average; selling into a long realises profit or loss against that average and leaves
// the average where it was; selling through zero closes the long and opens a short at the trade
// price. Its exposure is quantity times average cost, so a flat position carries none.
//
// The position keeper and the risk engine both fold fills with this function and nothing else.
// That is what makes a PositionSnapshot - a quantity and an average cost - mean the same thing to
// every consumer: an engine that loads one holds exactly what the engine that folded the fills
// holds, to the unit, because the average is kept in the same integer units and rounded the same
// way by the same code.
#pragma once
#include "money.hpp"
#include <cstdint>

namespace trading::pos {

struct Position {
    int64_t qty = 0;          // signed shares: long positive, short negative
    int64_t avgCost = 0;      // 1e-8 per share; zero when flat
    int64_t realized = 0;     // 1e-8, profit or loss locked in by reducing the position
};

inline money::i128 exposure(const Position& p) noexcept { return money::i128(p.qty) * p.avgCost; }

// One fill: signedQty is positive for a buy, negative for a sell. The average is rounded toward
// zero, in 1e-8 units per share, which is a hundred-millionth of a dollar.
inline void fill(Position& p, int64_t signedQty, int64_t px) noexcept {
    if (signedQty == 0) return;
    const int64_t had = p.qty;
    const bool adding = had == 0 || (had > 0) == (signedQty > 0);
    if (adding) {
        const int64_t a = had < 0 ? -had : had, b = signedQty < 0 ? -signedQty : signedQty;
        const money::i128 cost = money::i128(a) * p.avgCost + money::i128(b) * px;
        p.qty = had + signedQty;
        p.avgCost = int64_t(cost / (a + b));
        return;
    }
    const int64_t held = had < 0 ? -had : had, traded = signedQty < 0 ? -signedQty : signedQty;
    const int64_t closed = traded < held ? traded : held;
    // A long that sells above its cost made money; a short that buys back below its cost did.
    p.realized += int64_t(money::i128(closed) * (px - p.avgCost) * (had > 0 ? 1 : -1));
    p.qty = had + signedQty;
    if (p.qty == 0) p.avgCost = 0;
    else if ((p.qty > 0) != (had > 0)) p.avgCost = px;   // through zero: the rest opens at this price
}

// A client-level execution report as a signed quantity: buys add, every kind of sell subtracts. A
// bust undoes a fill by trading the same quantity back at the same price.
inline int64_t signedQty(Side side, int64_t qty) noexcept { return side == Side::Buy ? qty : -qty; }

} // namespace trading::pos
