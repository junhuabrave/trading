// core/seq/reader.hpp : a consumer's view of one stream with gap detection.
//
// Live frames come from the ring; if the reader ever sees seq != expected (it fell a
// full ring behind and was overtaken, or it is starting late) it refills the gap from
// the journal before continuing. Consumers therefore always see every frame, in order,
// exactly once, which is what makes their state machines deterministic.
#pragma once
#include "journal.hpp"
#include "ring.hpp"
#include <functional>

namespace trading::seq {

class StreamReader {
public:
    using Handler = std::function<void(const FrameHeader*)>;

    // startSeq: first sequence this reader wants (e.g. checkpoint+1 on restart, journal.firstSeq() from cold).
    StreamReader(BroadcastRing& ring, const Journal& journal, uint64_t startSeq)
        : ring_(ring), journal_(journal), expect_(startSeq) { id_ = ring_.subscribe(); }
    ~StreamReader() { ring_.unsubscribe(id_); }

    // Drain everything available; returns frames delivered. Refills gaps from the journal.
    uint64_t poll(const Handler& h) {
        uint64_t n = 0;
        while (const FrameHeader* f = ring_.poll(id_)) {
            if (f->seq < expect_) { ring_.advance(id_); continue; }        // already delivered via journal
            if (f->seq > expect_) {                                          // gap: refill
                n += refill(f->seq - 1, h);
            }
            h(f); expect_ = f->seq + 1; ++n; ++live_;
            ring_.advance(id_);
        }
        // nothing on the ring but journal is ahead (late start)
        if (expect_ <= journal_.lastSeq()) n += refill(journal_.lastSeq(), h);
        return n;
    }
    uint64_t expected() const noexcept { return expect_; }
    uint64_t refilled() const noexcept { return refilled_; }
    uint64_t live() const noexcept { return live_; }
    uint64_t gaps() const noexcept { return gaps_; }

private:
    uint64_t refill(uint64_t upTo, const Handler& h) {
        ++gaps_;
        uint64_t n = journal_.replay(expect_, upTo, [&](const FrameHeader* f) { h(f); expect_ = f->seq + 1; });
        refilled_ += n;
        return n;
    }
    BroadcastRing& ring_; const Journal& journal_;
    int id_; uint64_t expect_; uint64_t refilled_ = 0, live_ = 0, gaps_ = 0;
};

} // namespace trading::seq
