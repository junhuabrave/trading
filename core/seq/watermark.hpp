// core/seq/watermark.hpp : records the market-data position an engine has consumed
// and turns it into MdWatermark frames to sequence alongside a decision.
//
// The position is a (feedId, venueSeq) pair: the feed a message came from and the sequence that
// feed's venue assigned, never a sequence of ours. That is what lets a replayer reconstruct the
// exact book an engine saw no matter which publisher of that feed served it, and what lets a live
// consumer fail over between publishers without a gap or a duplicate.
//
// One MdWatermark carries eight pairs. An engine that consumes more feeds than that sequences
// several watermarks in the same batch, all before the decision; banks() says how many,
// fill(m, bank) fills each. load() merges, so a replayer can feed every watermark of a batch
// into one tracker.
#pragma once
#include "trading.hpp"
#include <array>

namespace trading::seq {

class WatermarkTracker {
public:
    static constexpr size_t MAX_STREAMS = 16;     // feeds tracked; eight per MdWatermark message
    static constexpr size_t PER_MESSAGE = 8;
    // A venueSeq of 0 means the frame carries no identity (a pre-v4 frame, or a message the feed
    // layer does not sequence); it is delivered but cannot be watermarked, so it is not recorded.
    void observe(uint32_t feedId, uint64_t venueSeq) noexcept {
        if (venueSeq == 0) return;
        for (size_t i = 0; i < n_; ++i) if (ids_[i] == feedId) { if (venueSeq > seqs_[i]) seqs_[i] = venueSeq; return; }
        if (n_ < MAX_STREAMS) { ids_[n_] = feedId; seqs_[n_] = venueSeq; ++n_; }
    }
    uint64_t seqFor(uint32_t feedId) const noexcept {
        for (size_t i = 0; i < n_; ++i) if (ids_[i] == feedId) return seqs_[i];
        return 0;
    }
    size_t banks() const noexcept { return n_ == 0 ? 1 : (n_ + PER_MESSAGE - 1) / PER_MESSAGE; }
    void fill(MdWatermark& m, size_t bank = 0) const noexcept {
        size_t base = bank * PER_MESSAGE, cnt = n_ > base ? n_ - base : 0;
        if (cnt > PER_MESSAGE) cnt = PER_MESSAGE;
        m.count = uint8_t(cnt);
        for (size_t i = 0; i < PER_MESSAGE; ++i) {
            m.streamIds[i] = i < cnt ? ids_[base + i] : 0; m.seqs[i] = i < cnt ? seqs_[base + i] : 0;
        }
    }
    void load(const MdWatermark& m) noexcept {
        size_t c = m.count > PER_MESSAGE ? PER_MESSAGE : m.count;
        for (size_t i = 0; i < c; ++i) observe(m.streamIds[i], m.seqs[i]);
    }
    void clear() noexcept { n_ = 0; }
    size_t size() const noexcept { return n_; }
private:
    std::array<uint32_t, MAX_STREAMS> ids_{}; std::array<uint64_t, MAX_STREAMS> seqs_{}; size_t n_ = 0;
};

} // namespace trading::seq
