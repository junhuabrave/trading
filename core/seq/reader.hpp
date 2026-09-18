// core/seq/reader.hpp : a consumer's view of one stream with gap detection.
//
// Live frames come from the ring; if the reader ever sees seq != expected (it fell a
// full ring behind and was overtaken, or it is starting late) it refills the gap from
// the journal before continuing. Consumers therefore always see every frame, in order,
// exactly once, which is what makes their state machines deterministic. Frames that
// came through the journal rather than the ring carry FrameFlags::replayed.
// poll() is a template over the handler so there is no indirect call per live frame.
#pragma once
#include "journal.hpp"
#include "ring.hpp"
#include <cstring>

namespace trading::seq {

class StreamReader {
public:
    // startSeq: first sequence this reader wants (e.g. checkpoint+1 on restart, journal.firstSeq() from cold).
    StreamReader(BroadcastRing& ring, const Journal& journal, uint64_t startSeq)
        : ring_(ring), journal_(journal), expect_(startSeq) { id_ = ring_.subscribe(); }
    ~StreamReader() { ring_.unsubscribe(id_); }
    StreamReader(const StreamReader&) = delete; StreamReader& operator=(const StreamReader&) = delete;

    // Drain everything available; returns frames delivered. Refills gaps from the journal.
    template <class H>
    uint64_t poll(H&& h) {
        uint64_t n = 0;
        while (const FrameHeader* f = ring_.poll(id_)) {
            if (f->seq < expect_) { ring_.advance(id_); continue; }        // already delivered via journal
            if (f->seq > expect_) n += refill(f->seq - 1, h);              // gap: refill
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
    template <class H>
    uint64_t refill(uint64_t upTo, H& h) {
        ++gaps_;
        alignas(16) std::byte buf[Journal::MAX_FRAME];
        uint64_t n = journal_.replay(expect_, upTo, [&](const FrameHeader* f) {
            std::memcpy(buf, f, f->frameLength);
            auto* c = reinterpret_cast<FrameHeader*>(buf);
            c->flags = uint16_t(c->flags | FrameFlags::replayed);
            h(static_cast<const FrameHeader*>(c)); expect_ = f->seq + 1;
        });
        refilled_ += n;
        return n;
    }
    BroadcastRing& ring_; const Journal& journal_;
    int id_; uint64_t expect_; uint64_t refilled_ = 0, live_ = 0, gaps_ = 0;
};

} // namespace trading::seq
