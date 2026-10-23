// core/util/bigendian.hpp : big-endian integers on the wire, which is how every venue protocol we
// speak (ITCH, SIP, OUCH, SoupBinTCP) lays them out.
#pragma once
#include <cstdint>

namespace trading::util {

inline uint16_t be16(const uint8_t* p) noexcept { return uint16_t(uint16_t(p[0]) << 8 | p[1]); }
inline uint32_t be32(const uint8_t* p) noexcept {
    return uint32_t(p[0]) << 24 | uint32_t(p[1]) << 16 | uint32_t(p[2]) << 8 | uint32_t(p[3]);
}
inline uint64_t be64(const uint8_t* p) noexcept { return uint64_t(be32(p)) << 32 | be32(p + 4); }

inline void putBe16(uint8_t* p, uint16_t v) noexcept { p[0] = uint8_t(v >> 8); p[1] = uint8_t(v); }
inline void putBe32(uint8_t* p, uint32_t v) noexcept { for (int i = 0; i < 4; ++i) p[i] = uint8_t(v >> (24 - 8 * i)); }
inline void putBe64(uint8_t* p, uint64_t v) noexcept { for (int i = 0; i < 8; ++i) p[i] = uint8_t(v >> (56 - 8 * i)); }

} // namespace trading::util
