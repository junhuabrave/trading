// core/md/arbitrator.hpp : one per feed. Turns several lossy, unordered, duplicating lines into one
// gap-free stream in venue-sequence order.
//
// A venue sends the same messages down two independent multicast lines and expects you to take
// whichever arrives first, to notice what neither line delivered, and to ask its recovery service
// for the rest. That is the whole job: merge by venue sequence, first arrival wins, detect the
// holes, request a retransmit, splice the answer in order, and when the answer never comes, fall
// back to a snapshot and say out loud that the stream jumped.
//
// It is protocol-agnostic. It reads the packet envelope and never the payload, so the same code
// arbitrates ITCH, PITCH, Pillar and a vendor line; the decoder (MD-4) is the only piece that has
// to know one from another.
//
// Two properties are worth stating because the tests exist to hold them:
//
//   Gap-free and duplicate-free. Every venue sequence the arbitrator emits is emitted exactly once,
//   in order, with no hole, except where a snapshot deliberately jumps the stream forward. A packet
//   may overlap what the consumer already has (a retransmit answers in whole packets), so each
//   emitted packet carries skip: how many of its leading messages are already downstream.
//
//   Deterministic. There is no clock in here. Every timeout is measured against timestamps the
//   caller supplies, so the same arrival order with the same timestamps produces the same output,
//   the same recovery requests and the same FeedStatus sequence, run after run and in replay.
//
// The request callback must not call back into the arbitrator. A real recovery request goes out on
// a socket and the answer arrives later as an ordinary packet; the tests queue it the same way.
#pragma once
#include "packet.hpp"
#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

namespace trading::md {

// Walk the messages of an emitted packet that the consumer has not already seen.
template <class F>
inline size_t forEachNewFrame(const PacketHeader& h, uint16_t skip, F&& fn) {
    size_t i = 0, n = 0;
    forEachFrame(h, [&](const FrameHeader* f) { if (i++ >= skip) { fn(f); ++n; } });
    return n;
}

class LineArbitrator {
public:
    enum class Recovery : uint8_t { Retransmit = 1, Snapshot = 2 };

    struct Params {
        uint32_t  feedId = 0;
        uint64_t  startSeq = 0;              // 0: adopt the sequence of the first data packet we see
        uint32_t  maxPending = 256;          // reorder buffer, in packets
        int64_t   gapTimeoutNs   =   500'000;    // how long a hole may stand before we ask for it
        int64_t   retryTimeoutNs = 5'000'000;    // before asking again for the same unchanged hole
        uint32_t  maxAttempts = 3;               // retransmit attempts before falling back to a snapshot
        int64_t   staleAfterNs = 1'000'000'000;  // silence, including heartbeats, before Stale
        int64_t   downAfterNs  = 5'000'000'000;  // and before Down
        MdQuality quality = MdQuality::Depth;    // what this feed provides when it is healthy
    };

    // An accepted packet, in order. skip is how many of its leading messages are already downstream.
    using Emit    = std::function<void(const PacketHeader& h, size_t len, uint16_t skip)>;
    // Ask the venue's recovery service for [fromSeq, toSeq]. For a Retransmit that range is what to
    // re-send. For a Snapshot it only describes what is missing: a snapshot service answers with the
    // current image at whatever sequence it has reached. Must not re-enter the arbitrator.
    using Request = std::function<void(Recovery kind, uint64_t fromSeq, uint64_t toSeq)>;
    using Status  = std::function<void(const FeedStatus&)>;

    LineArbitrator(Params p, Emit emit, Request request = {}, Status status = {})
        : p_(p), emit_(std::move(emit)), request_(std::move(request)), status_(std::move(status)),
          pool_(new Slot[p.maxPending]) {
        order_.reserve(p_.maxPending);
        free_.reserve(p_.maxPending);
        for (uint32_t i = p_.maxPending; i-- > 0;) free_.push_back(i);
        expected_ = venueHigh_ = p_.startSeq;
        haveStart_ = p_.startSeq != 0;
    }

    // One packet, from any line of this feed. recvTs is the hardware receive time the line receiver
    // stamped; it is the only clock the arbitrator has.
    void onPacket(const PacketHeader& h, size_t len, uint16_t lineId, int64_t recvTs) {
        if (h.feedId != p_.feedId) { ++wrongFeed_; return; }
        ++packets_;
        lastActivity_ = recvTs;

        if (h.msgCount == 0) {                       // a heartbeat carries no data, only a position
            ++heartbeats_;
            if (h.firstSeq > venueHigh_) venueHigh_ = h.firstSeq;
            after(recvTs);
            return;
        }
        noteLatency(h.firstSeq, lineId, recvTs);
        if (nextSeqAfter(h) > venueHigh_) venueHigh_ = nextSeqAfter(h);

        if (h.flags & PacketFlags::snapshot) { onSnapshot(h, len, recvTs); return; }
        if (h.flags & PacketFlags::retransmit) ++recoveryPackets_;

        // A late joiner starts wherever it starts: the first data packet defines the stream's origin.
        if (!haveStart_) { expected_ = h.firstSeq; haveStart_ = true; }

        if (nextSeqAfter(h) <= expected_) { ++duplicates_; after(recvTs); return; }   // wholly behind us
        if (h.firstSeq <= expected_) { deliver(h, len); drain(); }                     // deliverable now
        else buffer(h, len, lineId, recvTs);                                          // ahead: hold it
        after(recvTs);
    }

    // Drives timeouts when nothing is arriving, which is exactly when a feed dies. Idempotent.
    void tick(int64_t now) { maybeRequest(now); refreshState(now); }

    // Publish the current view whether or not it changed; the owner calls this on its status timer.
    void publishStatus() { if (status_) { ++statusPublished_; status_(status()); } }

    FeedStatus status() const noexcept {
        FeedStatus f{};
        f.feedId = p_.feedId;
        f.state = state_;
        f.quality = state_ == FeedState::Down ? MdQuality::NoData : p_.quality;
        f.lastVenueSeq = lastVenueSeq_;
        f.gapCount = gapCount_;
        f.lineLatencyDeltaNs = lineLatencyDelta_;
        return f;
    }

    uint64_t expected() const noexcept { return expected_; }
    FeedState state() const noexcept { return state_; }
    uint64_t packets() const noexcept { return packets_; }            // seen on every line
    uint64_t emitted() const noexcept { return emitted_; }            // accepted into the stream
    uint64_t messages() const noexcept { return messages_; }          // messages handed downstream
    uint64_t duplicates() const noexcept { return duplicates_; }      // the sibling line doing its job
    uint64_t buffered() const noexcept { return buffered_; }          // held for a hole ahead of them
    uint64_t overlaps() const noexcept { return overlaps_; }          // packets that straddled the cursor
    uint64_t gaps() const noexcept { return gapCount_; }              // distinct holes, not sequences
    uint64_t lost() const noexcept { return lost_; }                  // sequences the stream jumped
    uint64_t retransmitRequests() const noexcept { return retransmitRequests_; }
    uint64_t snapshotRequests() const noexcept { return snapshotRequests_; }
    uint64_t recoveryPackets() const noexcept { return recoveryPackets_; }
    uint64_t snapshotsApplied() const noexcept { return snapshotsApplied_; }
    uint64_t snapshotsIgnored() const noexcept { return snapshotsIgnored_; }
    uint64_t heartbeats() const noexcept { return heartbeats_; }
    uint64_t wrongFeed() const noexcept { return wrongFeed_; }
    uint64_t overflows() const noexcept { return overflows_; }
    uint64_t statusPublished() const noexcept { return statusPublished_; }
    size_t   pending() const noexcept { return order_.size(); }
    int64_t  lineLatencyDeltaNs() const noexcept { return lineLatencyDelta_; }

private:
    struct Slot { uint32_t len; alignas(16) std::byte bytes[MAX_PACKET]; };
    struct Entry { uint64_t firstSeq; uint32_t slot; };

    // One past the highest sequence we know exists, stopping at the first packet we are holding.
    uint64_t gapHigh() const noexcept { return order_.empty() ? venueHigh_ : order_.front().firstSeq; }

    void deliver(const PacketHeader& h, size_t len) {
        uint16_t skip = uint16_t(expected_ > h.firstSeq ? expected_ - h.firstSeq : 0);
        if (skip > h.msgCount) skip = h.msgCount;
        if (skip) ++overlaps_;
        expected_ = nextSeqAfter(h);
        lastVenueSeq_ = expected_ - 1;
        ++emitted_;
        messages_ += h.msgCount - skip;
        if (emit_) emit_(h, len, skip);
    }

    void drain() {
        while (!order_.empty()) {
            const Entry e = order_.front();
            if (e.firstSeq > expected_) break;                       // the hole is still a hole
            const Slot& s = pool_[e.slot];
            const auto& h = *reinterpret_cast<const PacketHeader*>(s.bytes);
            if (nextSeqAfter(h) > expected_) deliver(h, s.len);
            else ++duplicates_;                                      // recovery overtook it
            order_.erase(order_.begin());
            free_.push_back(e.slot);
        }
    }

    void buffer(const PacketHeader& h, size_t len, uint16_t, int64_t recvTs) {
        auto at = std::lower_bound(order_.begin(), order_.end(), h.firstSeq,
                                   [](const Entry& e, uint64_t s) { return e.firstSeq < s; });
        if (at != order_.end() && at->firstSeq == h.firstSeq) { ++duplicates_; return; }
        if (order_.size() == p_.maxPending) {
            overflow(recvTs);                                        // frees at least one slot
            at = std::lower_bound(order_.begin(), order_.end(), h.firstSeq,
                                  [](const Entry& e, uint64_t s) { return e.firstSeq < s; });
            if (at != order_.end() && at->firstSeq == h.firstSeq) { ++duplicates_; return; }
            if (order_.size() == p_.maxPending) return;              // cannot happen; drop rather than corrupt
        }
        const uint32_t slot = free_.back(); free_.pop_back();
        Slot& s = pool_[slot];
        s.len = uint32_t(len);
        std::memcpy(s.bytes, &h, len);
        order_.insert(at, Entry{h.firstSeq, slot});
        ++buffered_;
    }

    // The reorder buffer is full, which means a hole we cannot fill is holding back live data.
    // Forward progress wins: jump the cursor to what we are holding, count what was lost, ask for a
    // snapshot because only that makes the consumer's state whole again, and say so in FeedStatus.
    // Everything the stream skips here is counted in lost(), so a consumer is never short without
    // the arbitrator knowing by exactly how much.
    void overflow(int64_t now) {
        ++overflows_;
        const uint64_t from = expected_, to = order_.front().firstSeq;
        lost_ += to - from;
        expected_ = to;
        clearGap();
        drain();
        if (request_) {
            wantSnapshot_ = true;
            ++snapshotRequests_;
            lastRequestTs_ = now;
            request_(Recovery::Snapshot, from, to - 1);
        }
        refreshState(now);
    }

    // A snapshot is the state as of, and including, its firstSeq. Accepting one is a deliberate
    // discontinuity, so we take it only when we asked and only when it is at least as current as
    // what we already have.
    void onSnapshot(const PacketHeader& h, size_t len, int64_t now) {
        const uint64_t resumeAt = h.firstSeq + 1;
        if (!wantSnapshot_ || resumeAt < expected_) { ++snapshotsIgnored_; after(now); return; }
        ++emitted_;
        messages_ += h.msgCount;
        if (emit_) emit_(h, len, 0);
        if (resumeAt > expected_) lost_ += resumeAt - expected_;
        expected_ = resumeAt;
        if (expected_ > venueHigh_) venueHigh_ = expected_;
        lastVenueSeq_ = h.firstSeq;
        haveStart_ = true;
        ++snapshotsApplied_;
        clearGap();
        wantSnapshot_ = false;
        drain();
        after(now);
    }

    void clearGap() noexcept { gapSince_ = 0; attempts_ = 0; requestedFrom_ = 0; requestedTo_ = 0; lastRequestTs_ = 0; }

    void after(int64_t now) { maybeRequest(now); refreshState(now); }

    void maybeRequest(int64_t now) {
        const uint64_t hi = gapHigh();
        if (hi <= expected_) { if (gapSince_) clearGap(); return; }  // nothing missing
        if (!gapSince_) { gapSince_ = now; ++gapCount_; }
        if (now - gapSince_ < p_.gapTimeoutNs) return;               // give the sibling line its chance
        if (!request_) return;

        const uint64_t from = expected_, to = hi - 1;
        const bool moved = from != requestedFrom_ || to != requestedTo_;
        if (moved) attempts_ = 0;                                    // a partial answer earns a fresh budget
        else if (lastRequestTs_ && now - lastRequestTs_ < p_.retryTimeoutNs) return;

        lastRequestTs_ = now;
        requestedFrom_ = from; requestedTo_ = to;
        if (attempts_ >= p_.maxAttempts) {                           // retransmit is not coming
            wantSnapshot_ = true;
            ++snapshotRequests_;
            request_(Recovery::Snapshot, from, to);
            return;
        }
        ++attempts_;
        ++retransmitRequests_;
        request_(Recovery::Retransmit, from, to);
    }

    void refreshState(int64_t now) {
        FeedState s;
        if (lastActivity_ && now - lastActivity_ >= p_.downAfterNs) s = FeedState::Down;
        else if (lastActivity_ && now - lastActivity_ >= p_.staleAfterNs) s = FeedState::Stale;
        else if (gapSince_ && now - gapSince_ >= p_.gapTimeoutNs) s = FeedState::Recovering;
        else s = FeedState::Healthy;
        if (s == state_) return;
        state_ = s;
        publishStatus();
    }

    // The first line-quality metric: how far behind its sibling each line runs. A packet arrives
    // twice, once per line; the second arrival is what tells us the difference. Direct-mapped by
    // sequence so it costs one probe, and a collision simply forgets an older packet.
    void noteLatency(uint64_t seq, uint16_t lineId, int64_t ts) noexcept {
        if (lineId > 1) return;                                      // the recovery line has no sibling
        Arrival& a = arrivals_[seq & (ARRIVALS - 1)];
        if (a.seq == seq && a.line != lineId) {
            lineLatencyDelta_ = lineId == 1 ? ts - a.ts : a.ts - ts;  // line 1 minus line 0
            a.seq = 0;                                               // one measurement per packet
            return;
        }
        a.seq = seq; a.ts = ts; a.line = lineId;
    }

    static constexpr size_t ARRIVALS = 1024;
    struct Arrival { uint64_t seq = 0; int64_t ts = 0; uint16_t line = 0; };

    Params p_;
    Emit emit_;
    Request request_;
    Status status_;
    std::unique_ptr<Slot[]> pool_;
    std::vector<Entry> order_;          // buffered packets, ascending by firstSeq
    std::vector<uint32_t> free_;        // free slots in pool_
    std::array<Arrival, ARRIVALS> arrivals_{};

    uint64_t expected_ = 0, venueHigh_ = 0, lastVenueSeq_ = 0;
    bool haveStart_ = false, wantSnapshot_ = false;
    int64_t lastActivity_ = 0, gapSince_ = 0, lastRequestTs_ = 0, lineLatencyDelta_ = 0;
    uint64_t requestedFrom_ = 0, requestedTo_ = 0;
    uint32_t attempts_ = 0, gapCount_ = 0;
    FeedState state_ = FeedState::Healthy;

    uint64_t packets_ = 0, emitted_ = 0, messages_ = 0, duplicates_ = 0, buffered_ = 0, overlaps_ = 0;
    uint64_t lost_ = 0, retransmitRequests_ = 0, snapshotRequests_ = 0, recoveryPackets_ = 0;
    uint64_t snapshotsApplied_ = 0, snapshotsIgnored_ = 0, heartbeats_ = 0, wrongFeed_ = 0;
    uint64_t overflows_ = 0, statusPublished_ = 0;
};

} // namespace trading::md
