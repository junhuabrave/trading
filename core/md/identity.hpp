// core/md/identity.hpp : what identifies a market-data message.
//
// Not the sequence number we assign. A normalised market-data message is identified by the feed it
// came from and the sequence that feed's venue put on it, so two handlers of the same feed produce
// byte-identical streams and a consumer can switch between them with no gap and no duplicate. The
// frame header's streamId carries the feedId: a market-data stream is one feed.
//
// A venueSeq of zero means "no identity": either a v3 frame written before the field existed, or a
// message the feed layer does not sequence. Such a frame is still delivered, but it cannot be
// watermarked and so cannot be replayed to.
#pragma once
#include "trading.hpp"

namespace trading::md {

struct Identity { uint32_t feedId; uint64_t venueSeq; };

inline Identity identity(const FrameHeader* h) noexcept {
    if (auto* m = as<BookDelta>(h))    return {h->streamId, m->venueSeq};
    if (auto* m = as<Trade>(h))        return {h->streamId, m->venueSeq};
    if (auto* m = as<Nbbo>(h))         return {h->streamId, m->venueSeq};
    if (auto* m = as<Imbalance>(h))    return {h->streamId, m->venueSeq};
    if (auto* m = as<SymbolStatus>(h)) return {h->streamId, m->venueSeq};
    if (auto* m = as<BookSnapshot>(h)) return {h->streamId, m->venueSeq};
    return {h->streamId, 0};
}

inline uint64_t identityVenueSeq(const FrameHeader* h) noexcept { return identity(h).venueSeq; }

// Template ids 200 to 299 are the market-data family.
inline bool isMarketData(const FrameHeader* h) noexcept { return h->templateId >= 200 && h->templateId < 300; }

} // namespace trading::md
