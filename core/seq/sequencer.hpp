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
#include <algorithm>
#include <span>
#include <vector>
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

    // Arm a timer. Nothing fires it by itself: fireTimers() emits the due ones as ordinary
    // sequenced frames, so a strategy waiting on a clock leaves the same mark on the log as
    // anything else and a replay reads it back instead of having to re-derive when it would have
    // gone off. That is the whole reason the timer is a message.
    void armTimer(uint16_t strategyId, uint32_t timerId, int64_t fireTs) {
        timers_.push_back({fireTs, timerId, strategyId});
    }
    // Emit every timer due at `now`, earliest first, ties broken by id so the order is total.
    uint32_t fireTimers(int64_t now) {
        if (timers_.empty()) return 0;
        std::sort(timers_.begin(), timers_.end());
        uint32_t fired = 0;
        size_t i = 0;
        for (; i < timers_.size() && timers_[i].fireTs <= now; ++i) {
            Frame<Timer> f; f.init();
            f.header.sourceId = sourceId_;
            f.body.strategyId = timers_[i].strategyId;
            f.body.timerId = timers_[i].timerId;
            f.body.fireTs = timers_[i].fireTs;
            while (!submit(&f.header)) {}
            ++fired;
        }
        timers_.erase(timers_.begin(), timers_.begin() + long(i));
        return fired;
    }
    size_t armedTimers() const noexcept { return timers_.size(); }

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

    struct Armed {
        int64_t fireTs; uint32_t timerId; uint16_t strategyId;
        bool operator<(const Armed& o) const noexcept {
            if (fireTs != o.fireTs) return fireTs < o.fireTs;
            return timerId < o.timerId;                      // total, so two runs fire in one order
        }
    };
    std::vector<Armed> timers_;

    uint32_t streamId_; uint16_t sourceId_;
    Journal& journal_; BroadcastRing& ring_; Clock& clock_;
    uint64_t checkpointEvery_; bool durable_;
    uint64_t next_, sinceCheckpoint_ = 0, checkpointId_ = 0;
};

} // namespace trading::seq
