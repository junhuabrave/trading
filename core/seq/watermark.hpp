// core/seq/watermark.hpp : records the market-data position an engine has consumed
// and turns it into MdWatermark frames to sequence alongside a decision.
//
// One MdWatermark carries eight (stream, seq) pairs. An engine that consumes more feeds
// than that sequences several watermarks in the same batch, all before the decision;
// banks() says how many, fill(m, bank) fills each. load() merges, so a replayer can
// feed every watermark of a batch into one tracker.
#pragma once
#include "trading.hpp"
#include <array>

namespace trading::seq {

class WatermarkTracker {
public:
    static constexpr size_t MAX_STREAMS = 16;
    static constexpr size_t PER_MESSAGE = 8;
    void observe(uint32_t streamId, uint64_t seq) noexcept {
        for (size_t i = 0; i < n_; ++i) if (ids_[i] == streamId) { seqs_[i] = seq; return; }
        if (n_ < MAX_STREAMS) { ids_[n_] = streamId; seqs_[n_] = seq; ++n_; }
    }
    uint64_t seqFor(uint32_t streamId) const noexcept {
        for (size_t i = 0; i < n_; ++i) if (ids_[i] == streamId) return seqs_[i];
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
