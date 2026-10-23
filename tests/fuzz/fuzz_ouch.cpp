// libFuzzer target: the bytes a venue (or, for the simulator twin, a participant) sends, through the
// SoupBinTCP framer in chunks the input chooses, and each packet and the raw input through both OUCH
// decoders. Nothing may crash or read out of bounds, and anything that decodes must encode to bytes
// that decode to the same message and encode to themselves: what the session worker reads is what
// it would write.
#include "ouch.hpp"
#include "soupbin.hpp"
#include <algorithm>
using namespace trading::venue;

static uint8_t buf[70'000];

template <class V, class D> static void roundTrip(const uint8_t* p, size_t n, D decode) {
    V m;
    if (!decode(p, n, m)) return;
    const size_t k = ouch::encode(m, buf, sizeof buf);
    if (k == 0) __builtin_trap();
    V again;
    if (!decode(buf, k, again) || !(again == m)) __builtin_trap();
    static uint8_t buf2[70'000];
    if (ouch::encode(again, buf2, sizeof buf2) != k || std::memcmp(buf, buf2, k) != 0) __builtin_trap();
}

static void packet(const soup::Packet& k) {
    if (k.type == soup::type::SequencedData) roundTrip<ouch::Outbound>(k.payload, k.size, ouch::decodeOutbound);
    if (k.type == soup::type::UnsequencedData) roundTrip<ouch::Inbound>(k.payload, k.size, ouch::decodeInbound);
    soup::LoginRequest lr; soup::LoginAccepted la; char why;
    if (soup::parse(k, lr) && lr.user().size() > 6) __builtin_trap();
    if (soup::parse(k, la) && la.sessionId().size() > 10) __builtin_trap();
    (void)soup::parseReject(k, why);
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    roundTrip<ouch::Outbound>(data, size, ouch::decodeOutbound);
    roundTrip<ouch::Inbound>(data, size, ouch::decodeInbound);
    if (size == 0) return 0;
    soup::Framer f;
    const size_t step = 1 + data[0] % 16;                 // the first byte chooses how the stream is cut
    soup::Packet k;
    for (size_t at = 1; at < size;) {
        const size_t n = std::min(step, size - at);
        auto [dst, cap] = f.space();
        if (cap < n) __builtin_trap();                    // there is always room for one more packet
        std::memcpy(dst, data + at, n); f.commit(n); at += n;
        while (f.next(k)) packet(k);
    }
    return 0;
}
