// core/seq/watermark.hpp : records the market-data position an engine has consumed
// and turns it into an MdWatermark frame to sequence alongside a decision.
#pragma once
#include "trading.hpp"
#include <array>

namespace trading::seq {

class WatermarkTracker {
public:
    static constexpr size_t MAX_STREAMS = 8;
    void observe(uint32_t streamId, uint64_t seq) noexcept {
        for (size_t i = 0; i < n_; ++i) if (ids_[i] == streamId) { seqs_[i] = seq; return; }
        if (n_ < MAX_STREAMS) { ids_[n_] = streamId; seqs_[n_] = seq; ++n_; }
    }
    uint64_t seqFor(uint32_t streamId) const noexcept {
        for (size_t i = 0; i < n_; ++i) if (ids_[i] == streamId) return seqs_[i];
        return 0;
    }
    void fill(MdWatermark& m) const noexcept {
        m.count = uint8_t(n_);
        for (size_t i = 0; i < MAX_STREAMS; ++i) { m.streamIds[i] = i < n_ ? ids_[i] : 0; m.seqs[i] = i < n_ ? seqs_[i] : 0; }
    }
    void load(const MdWatermark& m) noexcept {
        n_ = m.count > MAX_STREAMS ? MAX_STREAMS : m.count;
        for (size_t i = 0; i < n_; ++i) { ids_[i] = m.streamIds[i]; seqs_[i] = m.seqs[i]; }
    }
    size_t size() const noexcept { return n_; }
private:
    std::array<uint32_t, MAX_STREAMS> ids_{}; std::array<uint64_t, MAX_STREAMS> seqs_{}; size_t n_ = 0;
};

} // namespace trading::seq
