// core/md/selector.hpp : which market data a consumer actually acts on.
//
// Two jobs that look alike and are not.
//
// The first is deduplication. The same feed is published by two processes on two hosts, so that
// losing one loses nothing. Because identity is the venue's own (feedId, venueSeq) and the decoder
// is a pure function of the packets, those two processes emit the same messages, and the selector
// keeps whichever arrives first and drops the other. That is what makes a publisher dying
// mid-packet invisible: the survivor was already sending every message, so there is no gap to fill
// and no duplicate to suppress downstream.
//
// One venue message can decode into several of ours - an execution is a trade and a book change -
// so they share a venue sequence and the sequence alone cannot tell them apart. The identity is
// therefore (venueSeq, index within that sequence), and the index is recovered rather than carried:
// each publisher's messages arrive in order, so counting them per sequence per publisher gives the
// same index on both, which is precisely the property that makes the comparison work.
//
// The second job is choosing between sources that are not the same: a direct feed with full depth,
// a SIP with the official top of book, a vendor feed for the long tail. When the preferred one goes
// quiet or spends too long recovering, the selector moves to the next and says so, because a
// consumer that silently starts acting on top-of-book where it had depth is a consumer that will
// make a decision it cannot explain. The move is an MdSourceSwitch, sequenced on the core stream,
// so a replay makes the same decisions the live system did.
//
// Policy is reconsidered when a source that is not the selected one speaks, when the arbitrator
// reports a feed's state, and on tick(). The hot path - a message from the source we are already
// acting on - does none of that work. tick() is what notices silence, which is the one failure no
// arriving message can report, so a selector that is never ticked will sit on a feed that has
// stopped; the owner drives it from the same timer it drives everything else from.
//
// No clock of its own: every timeout is against timestamps the caller supplies, so the same
// messages in the same order produce the same forwarding and the same switches, live and in replay.
#pragma once
#include "identity.hpp"
#include "refdata.hpp"
#include <functional>
#include <vector>

namespace trading::md {

// MdSourceSwitch.reason is frozen as uint8; schema enum MdSwitchReason names what it carries.
inline constexpr uint8_t switchReason(MdSwitchReason r) noexcept { return uint8_t(r); }

class SourceSelector {
public:
    struct Params {
        uint16_t sourceId = 0;
        // A source that has said nothing for this long is not one to act on, whatever its last
        // FeedStatus claimed: the status message itself may be what stopped arriving.
        int64_t  staleAfterNs = 500'000'000;
        // A feed recovering a hole is still mostly usable, and switching away for every retransmit
        // would be worse than the hole. This is how long it may stay that way.
        int64_t  recoveringGraceNs = 100'000'000;
        // And a source that comes back must hold up this long before we move back to it, so a feed
        // flapping once a second does not take the consumer with it.
        int64_t  failBackAfterNs = 5'000'000'000;
        uint32_t maxSourceId = 1024;
    };

    // Market data and source switches go to different places: the data to whichever consumer asked
    // for it, the switch to the sequencer, because a switch changes what every later decision was
    // made on and replay has to see it in the same position. They are therefore different
    // callbacks, and the control one is taken once at construction because a switch is a handful of
    // events in a day while a message is tens of millions.
    using Control = std::function<void(const FrameHeader*)>;

    SourceSelector(const refdata::RefData& rd, Params p, Control control)
        : rd_(rd), p_(p), control_(std::move(control)), cursors_(p.maxSourceId) {
        sources_.reserve(16);
    }

    // A source the selector may choose, in preference order, lowest first. Its kind and what it
    // provides come from the feed registry in the snapshot (MD-1) rather than from the caller,
    // because two components disagreeing about what a feed carries is a bug nobody notices.
    void addSource(uint32_t feedId, uint8_t preference) {
        if (!rd_.knownFeed(feedId)) throw std::logic_error("selector: feed not in the registry");
        for (const Src& s : sources_) if (s.feedId == feedId) return;
        Src s{};
        s.feedId = feedId;
        s.preference = preference;
        s.quality = qualityOf(feedId);
        s.kind = rd_.feed(feedId).kind;
        sources_.push_back(s);
    }

    // One normalised message from one publisher. Returns true if it went downstream. Everything
    // else is either the second copy of something already forwarded, or a message from a source we
    // are not currently acting on - and that second case still advances the source's high-water
    // mark, so a later switch to it resumes without replaying what we already had.
    template <class E>
    bool onMessage(const FrameHeader* f, int64_t now, E&& emit) {
        ++seen_;
        Src* s = find(f->streamId);
        if (!s) { ++unknownFeed_; return false; }
        s->lastMsgTs = now;
        // A source we are not acting on has just proved it is alive, which is exactly the evidence
        // a fail-back decision needs. The message we are here for, from the source we are already
        // on, costs nothing.
        if (s->feedId != selected_) chooseAndAnnounce(now);
        const uint64_t vs = identityVenueSeq(f);
        if (vs == 0) { ++unidentified_; return forward(*s, f, emit); }   // nothing to compare; pass it on
        if (f->sourceId >= cursors_.size()) { ++unknownPublisher_; return forward(*s, f, emit); }
        Cursor& c = cursors_[f->sourceId];
        if (c.venueSeq == vs) ++c.index; else { c.venueSeq = vs; c.index = 0; }
        if (!(vs > s->hwSeq || (vs == s->hwSeq && c.index > s->hwIndex))) {
            ++s->duplicates; ++duplicates_;
            return false;
        }
        s->hwSeq = vs; s->hwIndex = c.index;
        ++s->accepted;
        return forward(*s, f, emit);
    }

    // The arbitrator's view of a feed. This is the fast path for noticing trouble; the slow one is
    // silence, which only tick() can see.
    void onFeedStatus(const FeedStatus& fs, int64_t now) {
        Src* s = find(fs.feedId);
        if (!s) { ++unknownFeed_; return; }
        if (s->state != fs.state) {
            s->state = fs.state;
            if (fs.state == FeedState::Recovering) s->recoveringSince = now;
        }
        chooseAndAnnounce(now);
    }

    void tick(int64_t now) { chooseAndAnnounce(now); }

    // An operator forcing a source, which is a thing that happens at three in the afternoon and has
    // to be on the log with everything else.
    void force(uint32_t feedId, int64_t now) {
        if (!find(feedId)) return;
        forced_ = feedId;
        switchTo(feedId, MdSwitchReason::Operator, now);
    }
    void unforce(int64_t now) { forced_ = 0; chooseAndAnnounce(now); }

    uint32_t selected() const noexcept { return selected_; }
    MdQuality quality() const noexcept { return quality_; }
    uint64_t seen() const noexcept { return seen_; }
    uint64_t forwarded() const noexcept { return forwarded_; }
    uint64_t duplicates() const noexcept { return duplicates_; }      // the redundant publisher doing its job
    uint64_t suppressed() const noexcept { return suppressed_; }      // from a source we are not acting on
    uint64_t switches() const noexcept { return switches_; }
    uint64_t unidentified() const noexcept { return unidentified_; }
    uint64_t unknownFeed() const noexcept { return unknownFeed_; }
    uint64_t unknownPublisher() const noexcept { return unknownPublisher_; }
    uint64_t acceptedFrom(uint32_t feedId) const noexcept { const Src* s = find(feedId); return s ? s->accepted : 0; }
    uint64_t duplicatesFrom(uint32_t feedId) const noexcept { const Src* s = find(feedId); return s ? s->duplicates : 0; }
    uint64_t highWater(uint32_t feedId) const noexcept { const Src* s = find(feedId); return s ? s->hwSeq : 0; }

private:
    struct Src {
        uint32_t feedId = 0;
        uint8_t preference = 0, kind = 0;
        MdQuality quality = MdQuality::NoData;
        FeedState state = FeedState::Healthy;
        int64_t lastMsgTs = 0, usableSince = 0, recoveringSince = 0;
        uint64_t hwSeq = 0, accepted = 0, duplicates = 0, suppressed = 0;
        uint32_t hwIndex = 0;
    };
    struct Cursor { uint64_t venueSeq = 0; uint32_t index = 0; };

    MdQuality qualityOf(uint32_t feedId) const noexcept {
        if (rd_.feedProvides(feedId, FeedProvides::depth)) return MdQuality::Depth;
        if (rd_.feedProvides(feedId, FeedProvides::topOfBook)) return MdQuality::TopOfBook;
        if (rd_.feedProvides(feedId, FeedProvides::trades)) return MdQuality::TradesOnly;
        return MdQuality::NoData;
    }
    Src* find(uint32_t feedId) noexcept {
        for (Src& s : sources_) if (s.feedId == feedId) return &s;
        return nullptr;
    }
    const Src* find(uint32_t feedId) const noexcept {
        for (const Src& s : sources_) if (s.feedId == feedId) return &s;
        return nullptr;
    }

    template <class E>
    bool forward(Src& s, const FrameHeader* f, E& emit) {
        if (s.feedId != selected_) { ++s.suppressed; ++suppressed_; return false; }
        ++forwarded_;
        emit(f);
        return true;
    }

    bool usable(const Src& s, int64_t now) const noexcept {
        if (s.state == FeedState::Down || s.state == FeedState::Stale) return false;
        if (s.state == FeedState::Recovering && s.recoveringSince &&
            now - s.recoveringSince >= p_.recoveringGraceNs) return false;
        if (s.lastMsgTs && now - s.lastMsgTs >= p_.staleAfterNs) return false;
        if (!s.lastMsgTs) return false;                              // never said anything: not a source yet
        return true;
    }
    MdSwitchReason whyNot(const Src& s, int64_t now) const noexcept {
        if (s.state == FeedState::Down) return MdSwitchReason::SourceDown;
        if (s.state == FeedState::Recovering) return MdSwitchReason::SourceGapped;
        if (s.lastMsgTs && now - s.lastMsgTs >= p_.staleAfterNs) return MdSwitchReason::SourceStale;
        return MdSwitchReason::SourceStale;
    }

    void chooseAndAnnounce(int64_t now) {
        if (forced_) return;
        // How long each source has been continuously usable. Any interruption restarts the clock,
        // which is what stops a feed that flaps once a second from taking the consumer with it.
        const Src* best = nullptr;
        for (Src& s : sources_) {
            if (usable(s, now)) { if (!s.usableSince) s.usableSince = now; }
            else { s.usableSince = 0; continue; }
            if (!best || s.preference < best->preference) best = &s;
        }
        const Src* cur = find(selected_);
        if (!best) {
            if (selected_) switchTo(0, MdSwitchReason::NoSource, now);
            return;
        }
        if (best->feedId == selected_) return;
        if (cur && usable(*cur, now)) {
            // The one we are on still works. Only move for something strictly better, and only once
            // it has proved it will stay: a feed that flaps must not take the consumer with it.
            if (best->preference >= cur->preference) return;
            if (!best->usableSince || now - best->usableSince < p_.failBackAfterNs) return;
            switchTo(best->feedId, MdSwitchReason::Recovered, now);
            return;
        }
        const MdSwitchReason why = cur ? whyNot(*cur, now) : MdSwitchReason::Initial;
        switchTo(best->feedId, why, now);
    }

    void switchTo(uint32_t feedId, MdSwitchReason why, int64_t now) {
        if (feedId == selected_) return;
        const Src* to = find(feedId);
        Frame<MdSourceSwitch> m;
        m.init();
        m.header.sourceId = p_.sourceId;
        m.header.originTs = now;
        m.body.fromFeedId = selected_;
        m.body.toFeedId = feedId;
        m.body.quality = to ? to->quality : MdQuality::NoData;
        m.body.reason = switchReason(why);
        selected_ = feedId;
        quality_ = m.body.quality;
        ++switches_;
        if (control_) control_(&m.header);
    }

    const refdata::RefData& rd_;
    Params p_;
    Control control_;
    std::vector<Src> sources_;
    std::vector<Cursor> cursors_;
    uint32_t selected_ = 0, forced_ = 0;
    MdQuality quality_ = MdQuality::NoData;
    uint64_t seen_ = 0, forwarded_ = 0, duplicates_ = 0, suppressed_ = 0, switches_ = 0;
    uint64_t unidentified_ = 0, unknownFeed_ = 0, unknownPublisher_ = 0;
};

} // namespace trading::md
