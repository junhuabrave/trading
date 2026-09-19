// core/util/money.hpp : how money crosses the wire, and what it cannot express.
//
// Money on the wire is int64. At 1e-8 units that tops out at $92.2 billion, which is ample for one
// order and far too small for a firm's gross exposure, so the two messages that carry a firm-wide
// aggregate say which unit they used. Unset means 1e-8, which is what every message written before
// schema v5 meant, so old logs keep their meaning exactly.
//
// Engines work internally in 1e-8 units in 128 bits, which is wide enough that no realistic sum
// overflows; the only ceiling that bites is what can be written back out, and that is stated here
// rather than discovered.
#pragma once
#include "trading.hpp"
#include <cstdint>

namespace trading::money {

__extension__ typedef __int128 i128;

inline constexpr i128 PER_1E4 = 10'000;                              // 1e-4 units per 1e-8 unit
inline constexpr i128 MAX_NANO = i128(INT64_MAX);                    // $92.2 billion at 1e-8
inline constexpr i128 MAX_REPORTABLE = i128(INT64_MAX) * PER_1E4;    // $922 trillion, written at 1e-4

// Wire value to internal 1e-8 units.
inline constexpr i128 toNano(int64_t wire, MoneyScale s) noexcept {
    return s == MoneyScale::Unit1e4 ? i128(wire) * PER_1E4 : i128(wire);
}
// Internal 1e-8 units to the wire, choosing the unit that fits and saying which it was.
// Rounds toward zero when it has to drop to 1e-4; a hundredth of a cent on a firm aggregate.
inline constexpr int64_t toWire(i128 v, MoneyScale& scaleOut) noexcept {
    if (v <= MAX_NANO && v >= -MAX_NANO) { scaleOut = MoneyScale::Unit1e8; return int64_t(v); }
    scaleOut = MoneyScale::Unit1e4;
    i128 c = v / PER_1E4;
    if (c > MAX_NANO) c = MAX_NANO;
    if (c < -MAX_NANO) c = -MAX_NANO;
    return int64_t(c);
}
inline constexpr bool reportable(i128 v) noexcept { return v <= MAX_REPORTABLE && v >= -MAX_REPORTABLE; }

} // namespace trading::money
