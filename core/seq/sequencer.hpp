// core/seq/sequencer.hpp : the single-writer ordering core for one stream.
//
// submit() stamps seq/seqTs/streamId, appends to the journal, publishes to the ring.
// submitBatch() does the same for N frames as one atomic unit (all or nothing on the
// ring; flags.lastInBatch set on the final frame) so an engine can pair an MdWatermark
// with the decision it explains. The sequencer itself emits SessionStart on open,
// Checkpoint every checkpointEvery frames, and Heartbeat on demand.
#pragma once
#include "journal.hpp"
#include "ring.hpp"
#include "clock.hpp"
#include <span>
#include <array>

namespace trading::seq {

class Sequencer {
public:
    Sequencer(uint32_t streamId, uint16_t sourceId, Journal& journal, BroadcastRing& ring, Clock& clock,
              uint64_t checkpointEvery = 100'000, bool durableCommit = false)
        : streamId_(streamId), sourceId_(sourceId), journal_(journal), ring_(ring), clock_(clock),
          checkpointEvery_(checkpointEvery), durable_(durableCommit), next_(journal.lastSeq() + 1) {}

    uint64_t nextSeq() const noexcept { return next_; }
    uint64_t lastSeq() const noexcept { return next_ - 1; }

    // Open a session: writes SessionStart carrying the snapshot identity. Consumers refuse to run
    // past this message unless the hash matches what they loaded.
    uint64_t openSession(uint32_t businessDate, uint32_t snapshotVersion, std::span<const uint8_t, 32> snapshotHash) {
        Frame<SessionStart> f; f.init();
        f.body.businessDate = businessDate; f.body.snapshotVersion = snapshotVersion; f.body.schemaVersion = SCHEMA_VERSION;
        std::memcpy(f.body.snapshotHash.data(), snapshotHash.data(), 32);
        f.body.sessionTs = clock_.nowNs();
        f.header.sourceId = sourceId_;
        while (!submit(&f.header)) {}
        return f.header.seq;
    }

    // Sequence one frame. The header is mutated in place. Returns false on back-pressure (retry).
    bool submit(FrameHeader* f) {
        if (!ring_.canPublish(1)) return false;
        stamp(f, true);
        journal_.append(f); journal_.commit(durable_);
        ring_.tryPublish(f);
        maybeCheckpoint();
        return true;
    }

    // Sequence N frames adjacently. Either all are sequenced or none.
    bool submitBatch(std::span<FrameHeader*> frames) {
        if (frames.empty()) return true;
        if (!ring_.canPublish(frames.size())) return false;
        for (size_t i = 0; i < frames.size(); ++i) {
            stamp(frames[i], i + 1 == frames.size());
            journal_.append(frames[i]);
        }
        journal_.commit(durable_);
        for (auto* f : frames) ring_.tryPublish(f);
        maybeCheckpoint();
        return true;
    }

    // Force a checkpoint marker now (e.g. before a planned failover).
    uint64_t checkpoint() {
        journal_.commit(true);                       // a checkpoint is always durable
        Frame<Checkpoint> f; f.init();
        f.body.checkpointId = ++checkpointId_; f.body.sourceSeq = lastSeq();
        f.header.sourceId = sourceId_;
        while (!submit(&f.header)) {}
        return f.header.seq;
    }

    uint64_t heartbeat(ComponentState state) {
        Frame<Heartbeat> f; f.init();
        f.body.componentSeq = lastSeq(); f.body.state = state; f.header.sourceId = sourceId_;
        while (!submit(&f.header)) {}
        return f.header.seq;
    }

private:
    void stamp(FrameHeader* f, bool last) noexcept {
        f->seq = next_++;
        f->seqTs = clock_.nowNs();
        f->streamId = streamId_;
        if (f->originTs == 0) f->originTs = f->seqTs;
        f->flags = uint16_t((f->flags & ~FrameFlags::lastInBatch) | (last ? FrameFlags::lastInBatch : 0));
        ++sinceCheckpoint_;
    }
    void maybeCheckpoint() {
        if (checkpointEvery_ && sinceCheckpoint_ >= checkpointEvery_) { sinceCheckpoint_ = 0; checkpoint(); }
    }

    uint32_t streamId_; uint16_t sourceId_;
    Journal& journal_; BroadcastRing& ring_; Clock& clock_;
    uint64_t checkpointEvery_; bool durable_;
    uint64_t next_, sinceCheckpoint_ = 0, checkpointId_ = 0;
};

} // namespace trading::seq
