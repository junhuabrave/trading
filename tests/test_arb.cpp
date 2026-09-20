// tests/test_arb.cpp <snapshot> <workdir>
//
// The line arbitrator (MD-3): several lossy, unordered, duplicating lines in, one gap-free stream
// in venue-sequence order out.
//
// A merger that simply forwards every packet it is handed looks correct until someone checks it
// against a hole it should have noticed, so the first drill here runs exactly that naive merger
// through the same checker and requires it to FAIL. Every later drill is only worth its output
// because drill 0 proves the checker has teeth.
//
//   0. a forwarder that does not arbitrate is caught: out of order, repeated, and full of holes
//   1. a clean day: every message once, in order, byte-identical to what the venue published, no
//      recovery asked for, and the A-versus-B line latency measured
//   2. one line dies mid-day: the stream does not notice, and still asks for nothing
//   3. both lines lossy: holes are found, a retransmit is asked for exactly the missing range,
//      the answer is spliced in order, and the day comes out whole
//   4. the retransmit service itself loses 40 percent of its answer: asked again, still whole
//   5. duplicates and reordering on both lines: every sequence exactly once, in order
//   6. the retransmit service never answers: escalated to a snapshot, the jump is counted and
//      reported, and the stream resumes from the snapshot
//   7. the reorder buffer overflows: the stream jumps rather than stalls, and every sequence it
//      skips is counted and asked about
//   8. the same impaired day run twice is byte-identical, including every FeedStatus
//   9. silence: healthy, then stale, then down, on the clock the caller supplies
#include "arbitrator.hpp"
#include "line_receiver.hpp"
#include "identity.hpp"
#include "feed_publisher.hpp"
#include "feed_sim.hpp"
#include "refdata.hpp"
#include <cstdio>
#include <filesystem>
#include <map>
#include <random>
#include <set>
#include <vector>

using namespace trading;
using namespace trading::md;
using namespace trading::sim;
namespace fs = std::filesystem;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static constexpr int64_t T0 = 1'700'000'000'000'000'000LL;
static constexpr uint32_t FEED = 1;
static constexpr uint16_t RX_LINE = 100;

// ---------------------------------------------------------------------------------------------
// What a consumer downstream of the arbitrator actually receives, and every way that can be wrong.
// The arbitrator's contract is that backwards, repeats and holes are all zero and that the bytes
// are the venue's own; anything else is a defect, whoever produced it.
struct Stream {
    std::vector<std::byte> raw;                              // every emitted packet, for run-to-run comparison
    std::map<uint64_t, std::vector<std::byte>> payload;      // venueSeq -> message bytes
    std::set<uint64_t> seen;
    uint64_t cursor = 0, messages = 0, backwards = 0, repeats = 0, holes = 0, resyncs = 0, jumped = 0;

    void take(const PacketHeader& h, size_t len, uint16_t skip) {
        const auto* p = reinterpret_cast<const std::byte*>(&h);
        raw.insert(raw.end(), p, p + len);
        raw.push_back(std::byte(skip));                      // skip is part of what was emitted
        if (h.flags & PacketFlags::snapshot) {               // a declared discontinuity, not a hole
            ++resyncs;
            if (cursor && h.firstSeq + 1 > cursor) jumped += h.firstSeq + 1 - cursor;
            cursor = h.firstSeq + 1;
            return;
        }
        uint64_t s = h.firstSeq + skip;
        forEachNewFrame(h, skip, [&](const FrameHeader* f) {
            ++messages;
            if (cursor == 0) cursor = s;
            if (s < cursor) ++backwards;
            else { holes += s - cursor; cursor = s + 1; }
            if (!seen.insert(s).second) ++repeats;
            const auto* b = reinterpret_cast<const std::byte*>(f);
            payload.emplace(s, std::vector<std::byte>(b, b + f->frameLength));
            ++s;
        });
    }
    bool covers(uint64_t lo, uint64_t hi) const {
        for (uint64_t s = lo; s <= hi; ++s) if (!seen.count(s)) return false;
        return true;
    }
    bool clean() const { return backwards == 0 && repeats == 0 && holes == 0; }
};

struct Req { LineArbitrator::Recovery kind; uint64_t from, to; };

struct Day {
    Stream out;
    std::vector<FeedStatus> statuses;
    std::vector<Req> requests;
    uint64_t retransmitPackets = 0, snapshotsSent = 0, recoveryDropped = 0;
    // counters lifted off the arbitrator once the day is over
    uint64_t packets = 0, emitted = 0, duplicates = 0, buffered = 0, overlaps = 0, gaps = 0, lost = 0;
    uint64_t overflows = 0, snapshotsApplied = 0, heartbeats = 0;
    int64_t lineLatencyDeltaNs = 0;
};

struct Options {
    FeedPublisher::Impairment lineA{}, lineB{};
    size_t   killLineAAt = 0;          // canonical index after which line A goes dark (0: never)
    double   retransmitDrop = 0.0;     // the recovery service losing its own answer
    bool     answerRetransmit = true;
    uint16_t rebatch = 0;              // the recovery service frames its answer N to a packet, not ours
    bool     answerSnapshot = false;
    uint64_t seed = 1;
    LineArbitrator::Params arb{};
};

// One simulated day: publish the canonical stream onto two impaired lines, arbitrate it, and
// answer recovery requests the way a venue would - out of band, after the packet that provoked
// them, never re-entering the arbitrator from inside its own callback.
static void runDay(const std::vector<std::vector<std::byte>>& canonical, const Options& o, Day& day) {
    auto frameAt = [&](size_t i) { return reinterpret_cast<const FrameHeader*>(canonical[i].data()); };

    FeedPublisher pub(FeedPublisher::Params{FEED, 2, RX_LINE, 4, 8192}, o.seed);
    pub.impair(0, o.lineA);
    pub.impair(1, o.lineB);

    LineArbitrator::Params ap = o.arb;
    ap.feedId = FEED;
    std::vector<Req> queue;
    LineArbitrator arb(
        ap,
        [&](const PacketHeader& h, size_t len, uint16_t skip) { day.out.take(h, len, skip); },
        [&](LineArbitrator::Recovery k, uint64_t from, uint64_t to) {
            day.requests.push_back({k, from, to});
            queue.push_back({k, from, to});
        },
        [&](const FeedStatus& f) { day.statuses.push_back(f); });

    auto sink = [&](const PacketHeader& h, size_t len, uint16_t line, int64_t ts) { arb.onPacket(h, len, line, ts); };
    LineReceiver a(FEED, 0, nullptr, sink), b(FEED, 1, nullptr, sink), rx(FEED, RX_LINE, nullptr, sink);

    std::mt19937_64 rng(o.seed * 7919 + 13);
    auto wire = [&](const void* p, size_t len, uint16_t line, int64_t ts) {
        if (line == RX_LINE && o.retransmitDrop > 0.0 &&
            double(rng() % 1000000) / 1000000.0 < o.retransmitDrop) { ++day.recoveryDropped; return; }
        (line == 0 ? a : (line == 1 ? b : rx)).onPacket(p, len, ts);
    };

    // A real recovery service frames its answer to suit itself. Nothing obliges it to reproduce the
    // packet boundaries of the live stream, so the answer can start before the hole and run past it,
    // and the arbitrator has to hand the overlap downstream marked rather than repeat it.
    const uint64_t base = identityVenueSeq(frameAt(0));
    auto rebatched = [&](uint64_t from, uint64_t to, int64_t now) {
        alignas(16) std::byte buf[MAX_PACKET];
        const uint64_t n = o.rebatch;
        for (uint64_t s = base + ((from - base) / n) * n; s <= to;) {
            PacketBuilder pb(buf);
            pb.begin(FEED, RX_LINE, s, now, PacketFlags::retransmit);
            uint64_t e = s;
            for (; e < s + n && e - base < canonical.size(); ++e) if (!pb.add(frameAt(size_t(e - base)))) break;
            if (pb.count() == 0) break;
            wire(buf, pb.size(), RX_LINE, now);
            ++day.retransmitPackets;
            s = e;
        }
    };

    std::vector<const FrameHeader*> book{frameAt(0), frameAt(1), frameAt(2)};
    auto answer = [&](int64_t now) {
        while (!queue.empty()) {
            const Req r = queue.front();
            queue.erase(queue.begin());
            if (r.kind == LineArbitrator::Recovery::Retransmit) {
                if (!o.answerRetransmit) continue;
                if (o.rebatch) rebatched(r.from, r.to, now);
                else day.retransmitPackets += pub.retransmit(r.from, r.to, now, wire);
            } else if (o.answerSnapshot) {
                pub.snapshot(book, r.to, now, wire);            // current as of the top of the hole
                ++day.snapshotsSent;
            }
        }
    };

    int64_t t = T0;
    for (size_t i = 0; i < canonical.size(); ++i) {
        if (o.killLineAAt && i == o.killLineAAt) pub.impair(0, {1.0, 0.0, 0.0, 0});
        pub.publish(frameAt(i), t, wire);
        t += 2000;
        answer(t);
    }
    pub.flush(t, wire);
    answer(t);
    // The close of the day: heartbeats keep the line alive while the last holes are chased down.
    auto round = [&] { t += 50'000; pub.heartbeat(t, wire); arb.tick(t); answer(t); };
    for (int k = 0; k < 200; ++k) round();
    // and then as long as it takes to settle, because a hole discovered in the last second of the
    // day is still a hole. A feed with nothing outstanding stops here, so a quiet day stays quiet.
    for (int k = 0; k < 800 && !(arb.state() == FeedState::Healthy && arb.pending() == 0 && queue.empty()); ++k) round();

    day.packets = arb.packets(); day.emitted = arb.emitted(); day.duplicates = arb.duplicates();
    day.buffered = arb.buffered(); day.overlaps = arb.overlaps(); day.gaps = arb.gaps();
    day.lost = arb.lost(); day.overflows = arb.overflows(); day.snapshotsApplied = arb.snapshotsApplied();
    day.heartbeats = arb.heartbeats(); day.lineLatencyDeltaNs = arb.lineLatencyDeltaNs();
}

static bool sameStatuses(const std::vector<FeedStatus>& x, const std::vector<FeedStatus>& y) {
    if (x.size() != y.size()) return false;
    for (size_t i = 0; i < x.size(); ++i) if (std::memcmp(&x[i], &y[i], sizeof(FeedStatus)) != 0) return false;
    return true;
}
static const char* stateName(FeedState s) {
    switch (s) {
        case FeedState::Unset: return "unset";
        case FeedState::Healthy: return "healthy";
        case FeedState::Recovering: return "recovering";
        case FeedState::Stale: return "stale";
        case FeedState::Down: return "down";
    }
    return "?";
}

int main(int argc, char** argv) {
    if (argc != 3) { std::fprintf(stderr, "usage: %s snapshot workdir\n", argv[0]); return 2; }
    fs::create_directories(argv[2]);
    refdata::Snapshot snap(argv[1]); refdata::RefData rd; rd.load(snap);
    CHECK(rd.knownFeed(FEED));

    // the canonical stream: what the venue published, generated once and shared by every drill
    std::vector<std::vector<std::byte>> canonical;
    {
        FeedSim src(rd, 10, 2, 8, 4242);
        int64_t t = T0;
        while (canonical.size() < 2000) {
            src.burst(4, t, [&](FrameHeader* f) {
                const auto* p = reinterpret_cast<const std::byte*>(f);
                canonical.emplace_back(p, p + f->frameLength);
            });
            t += 5000;
        }
    }
    auto frameAt = [&](size_t i) { return reinterpret_cast<const FrameHeader*>(canonical[i].data()); };
    const uint64_t firstSeq = identity(frameAt(0)).venueSeq;
    const uint64_t lastSeq = identity(frameAt(canonical.size() - 1)).venueSeq;
    CHECK(lastSeq == firstSeq + canonical.size() - 1);
    // every emitted message must be the venue's own bytes, not merely a message with the right number
    auto matchesVenue = [&](const Stream& s) {
        for (const auto& [seq, bytes] : s.payload)
            if (seq >= firstSeq && seq <= lastSeq && bytes != canonical[seq - firstSeq]) return false;
        return true;
    };
    std::printf("canonical: %zu messages, venue seq %llu..%llu\n", canonical.size(),
                (unsigned long long)firstSeq, (unsigned long long)lastSeq);

    Options drills{};                       // the impairment budget every drill below shares
    drills.arb.gapTimeoutNs = 20'000;       // 20 us of A/B skew tolerated before we ask
    drills.arb.retryTimeoutNs = 200'000;
    drills.arb.maxAttempts = 3;

    // ---- 0: the checker has teeth. A merger that forwards every packet in arrival order, which is
    // what "merge the lines" looks like when it is not actually written, must fail this checker.
    {
        Stream naive;
        FeedPublisher pub(FeedPublisher::Params{FEED, 2, RX_LINE, 4, 8192}, 31);
        pub.impair(0, {0.15, 0.10, 0.20, 0});
        pub.impair(1, {0.15, 0.0, 0.10, 3000});
        auto sink = [&](const PacketHeader& h, size_t len, uint16_t, int64_t) { naive.take(h, len, 0); };
        LineReceiver a(FEED, 0, nullptr, sink), b(FEED, 1, nullptr, sink);
        auto wire = [&](const void* p, size_t len, uint16_t line, int64_t ts) { (line == 0 ? a : b).onPacket(p, len, ts); };
        int64_t t = T0;
        for (auto& c : canonical) { pub.publish(reinterpret_cast<const FrameHeader*>(c.data()), t, wire); t += 2000; }
        pub.flush(t, wire);
        std::printf("naive forwarder: %llu messages, %llu backwards, %llu repeated, %llu holes -> %s\n",
                    (unsigned long long)naive.messages, (unsigned long long)naive.backwards,
                    (unsigned long long)naive.repeats, (unsigned long long)naive.holes,
                    naive.clean() ? "CLEAN (checker is broken)" : "rejected, as it must be");
        CHECK(!naive.clean());
        CHECK(naive.repeats > 0);            // the sibling line's copy, forwarded as if it were new
        CHECK(naive.backwards > 0);          // reordering, forwarded as if it were in order
        CHECK(naive.holes > 0);              // both lines lost packets and nothing asked for them
    }

    // ---- 1: a clean day
    {
        Options o = drills; o.seed = 7;
        o.lineB.delayNs = 4000;              // B runs 4 us behind A, which is what the metric measures
        Day day; runDay(canonical, o, day);
        std::printf("clean day: %llu messages out, %llu packets in, %llu duplicates from the sibling line, "
                    "B minus A = %lld ns\n",
                    (unsigned long long)day.out.messages, (unsigned long long)day.packets,
                    (unsigned long long)day.duplicates, (long long)day.lineLatencyDeltaNs);
        CHECK(day.out.clean());
        CHECK(day.out.messages == canonical.size());
        CHECK(day.out.covers(firstSeq, lastSeq));
        CHECK(matchesVenue(day.out));
        CHECK(day.requests.empty() && day.gaps == 0 && day.lost == 0);
        CHECK(day.duplicates >= day.emitted - 1);      // every packet arrived twice; one copy is spare
        CHECK(day.lineLatencyDeltaNs == 4000);
        CHECK(day.heartbeats == 400);                   // 200 rounds on two lines
        CHECK(day.statuses.empty());                    // nothing changed, so nothing was published
    }

    // ---- 2: line A dies halfway through and never comes back
    {
        Options o = drills; o.seed = 11; o.killLineAAt = canonical.size() / 2;
        Day day; runDay(canonical, o, day);
        std::printf("line A dies at message %zu: %llu messages out, still complete: %s, recovery asked for: %zu\n",
                    canonical.size() / 2, (unsigned long long)day.out.messages,
                    day.out.covers(firstSeq, lastSeq) ? "yes" : "NO", day.requests.size());
        CHECK(day.out.clean() && day.out.messages == canonical.size());
        CHECK(day.out.covers(firstSeq, lastSeq) && matchesVenue(day.out));
        CHECK(day.requests.empty() && day.gaps == 0);   // the second line is the whole point
    }

    // ---- 3: both lines lossy. Now there are real holes, and only the venue can fill them.
    {
        Options o = drills; o.seed = 13;
        o.lineA = {0.30, 0.0, 0.0, 0};
        o.lineB = {0.30, 0.0, 0.0, 2000};
        Day day; runDay(canonical, o, day);
        uint64_t sequencesAsked = 0;
        for (const Req& r : day.requests) { CHECK(r.kind == LineArbitrator::Recovery::Retransmit); sequencesAsked += r.to - r.from + 1; }
        std::printf("both lines drop 30%%: %llu holes, %zu retransmit requests covering %llu sequences, "
                    "%llu packets re-sent, %llu messages out, complete: %s\n",
                    (unsigned long long)day.gaps, day.requests.size(), (unsigned long long)sequencesAsked,
                    (unsigned long long)day.retransmitPackets, (unsigned long long)day.out.messages,
                    day.out.covers(firstSeq, lastSeq) ? "yes" : "NO");
        CHECK(day.gaps > 0);                             // the drill has to actually make holes
        CHECK(!day.requests.empty());
        CHECK(sequencesAsked < canonical.size());        // ask for the hole, not for the day
        CHECK(day.buffered > 0);                         // packets really were held back for a hole
        CHECK(day.out.clean() && day.lost == 0 && day.overflows == 0);
        CHECK(day.out.messages == canonical.size());
        CHECK(day.out.covers(firstSeq, lastSeq) && matchesVenue(day.out));
    }

    // ---- 3b: the same, but the recovery service frames its answer six to a packet against our
    // four. Its blocks straddle what we already have, so the arbitrator must pass the overlap on
    // marked with how much of it is old rather than repeat those messages downstream.
    {
        Options o = drills; o.seed = 37;
        o.lineA = {0.30, 0.0, 0.0, 0};
        o.lineB = {0.30, 0.0, 0.0, 0};
        o.rebatch = 6;
        Day day; runDay(canonical, o, day);
        std::printf("recovery re-frames its answer: %llu packets straddled what we had, "
                    "%llu messages out, every sequence once: %s\n",
                    (unsigned long long)day.overlaps, (unsigned long long)day.out.messages,
                    day.out.clean() ? "yes" : "NO");
        CHECK(day.overlaps > 0);                         // the straddle really happened
        CHECK(day.out.clean() && day.lost == 0);
        CHECK(day.out.messages == canonical.size());
        CHECK(day.out.covers(firstSeq, lastSeq) && matchesVenue(day.out));
    }

    // ---- 4: the retransmit service loses 40 percent of its own answer
    {
        Options o = drills; o.seed = 17;
        o.lineA = {0.25, 0.0, 0.0, 0};
        o.lineB = {0.25, 0.0, 0.0, 0};
        o.retransmitDrop = 0.40;
        o.arb.maxAttempts = 12;              // a lossy recovery line earns more than three tries
        Day day; runDay(canonical, o, day);
        std::printf("retransmit itself gaps: %zu requests, %llu re-sent packets, %llu of them lost, "
                    "complete: %s\n", day.requests.size(), (unsigned long long)day.retransmitPackets,
                    (unsigned long long)day.recoveryDropped, day.out.covers(firstSeq, lastSeq) ? "yes" : "NO");
        CHECK(day.recoveryDropped > 0);                  // the recovery path really did gap
        CHECK(day.requests.size() > day.gaps);           // so it had to be asked more than once per hole
        CHECK(day.out.clean() && day.lost == 0);
        CHECK(day.out.messages == canonical.size());
        CHECK(day.out.covers(firstSeq, lastSeq) && matchesVenue(day.out));
    }

    // ---- 5: duplicates and reordering, which are normal on a real line
    {
        Options o = drills; o.seed = 19;
        o.lineA = {0.0, 0.15, 0.20, 0};
        o.lineB = {0.05, 0.05, 0.10, 1000};
        Day day; runDay(canonical, o, day);
        std::printf("duplicates and reordering: %llu packets in, %llu emitted, %llu discarded as duplicates, "
                    "%llu held for reordering, every sequence once: %s\n",
                    (unsigned long long)day.packets, (unsigned long long)day.emitted,
                    (unsigned long long)day.duplicates, (unsigned long long)day.buffered,
                    day.out.clean() ? "yes" : "NO");
        CHECK(day.buffered > 0);                         // reordering actually happened
        CHECK(day.out.clean() && day.out.messages == canonical.size());
        CHECK(day.out.covers(firstSeq, lastSeq) && matchesVenue(day.out));
    }

    // ---- 6: the retransmit service never answers. After its budget the arbitrator stops asking
    // for what it cannot have, takes a snapshot, and reports the jump rather than hiding it.
    {
        Options o = drills; o.seed = 23;
        o.lineA = {0.30, 0.0, 0.0, 0};
        o.lineB = {0.30, 0.0, 0.0, 0};
        o.answerRetransmit = false;
        o.answerSnapshot = true;
        Day day; runDay(canonical, o, day);
        size_t snapAsked = 0, rxAsked = 0;
        for (const Req& r : day.requests) (r.kind == LineArbitrator::Recovery::Snapshot ? snapAsked : rxAsked)++;
        bool sawRecovering = false, endedHealthy = !day.statuses.empty();
        for (const FeedStatus& f : day.statuses) if (f.state == FeedState::Recovering) sawRecovering = true;
        if (!day.statuses.empty()) endedHealthy = day.statuses.back().state == FeedState::Healthy;
        std::printf("retransmit never answers: %zu retransmit asks, %zu snapshot asks, %llu snapshots applied, "
                    "%llu sequences given up on (%llu at a resync, %llu at an overflow), "
                    "state went through recovering: %s, ended %s\n",
                    rxAsked, snapAsked, (unsigned long long)day.snapshotsApplied,
                    (unsigned long long)day.lost, (unsigned long long)day.out.jumped,
                    (unsigned long long)day.out.holes, sawRecovering ? "yes" : "NO",
                    day.statuses.empty() ? "unchanged" : stateName(day.statuses.back().state));
        CHECK(rxAsked > 0 && snapAsked > 0);
        CHECK(day.snapshotsApplied > 0);
        CHECK(day.lost > 0);                             // a snapshot is a jump, and it is counted
        CHECK(day.out.resyncs == day.snapshotsApplied);
        // Everything the consumer did not get, counted: a snapshot resync is a jump, an overflow is
        // a hole, and between them they must account for every sequence the arbitrator gave up on.
        CHECK(day.out.jumped + day.out.holes == day.lost);
        CHECK(day.out.backwards == 0 && day.out.repeats == 0);
        CHECK(sawRecovering && endedHealthy);
        CHECK(day.statuses.back().gapCount == day.gaps);
        CHECK(matchesVenue(day.out));                    // what did arrive is still the venue's bytes
    }

    // ---- 7: a hole nobody will fill, and a reorder buffer far too small to wait it out. The
    // stream must move rather than stall, and must know exactly how much it skipped: a consumer
    // that is short without being told is worse than one that is told it is short.
    {
        Options o = drills; o.seed = 41;
        o.lineA = {0.50, 0.0, 0.0, 0};
        o.lineB = {0.50, 0.0, 0.0, 0};
        o.answerRetransmit = false;                      // nothing is coming
        o.arb.maxPending = 8;                            // and there is nowhere to wait
        Day day; runDay(canonical, o, day);
        size_t snapAsked = 0;
        for (const Req& r : day.requests) if (r.kind == LineArbitrator::Recovery::Snapshot) ++snapAsked;
        std::printf("reorder buffer overflows: %llu overflows, %llu sequences skipped, "
                    "%llu holes downstream, %zu snapshots asked for, repeated or out of order: %s\n",
                    (unsigned long long)day.overflows, (unsigned long long)day.lost,
                    (unsigned long long)day.out.holes, snapAsked,
                    (day.out.backwards || day.out.repeats) ? "YES" : "no");
        CHECK(day.overflows > 0);                        // the drill really did overflow
        CHECK(day.out.messages > 0 && day.emitted > day.overflows);   // it kept moving
        CHECK(day.out.backwards == 0 && day.out.repeats == 0);
        CHECK(day.out.jumped + day.out.holes == day.lost);   // every skipped sequence is a counted one
        CHECK(snapAsked >= day.overflows);               // and every overflow asked for a snapshot
    }

    // ---- 8: the same impaired day, twice, byte for byte
    {
        Options o = drills; o.seed = 29;
        o.lineA = {0.20, 0.10, 0.15, 0};
        o.lineB = {0.20, 0.05, 0.10, 3000};
        o.retransmitDrop = 0.20;
        Day one, two;
        runDay(canonical, o, one);
        runDay(canonical, o, two);
        std::printf("determinism: %zu bytes emitted, %zu status messages, identical across runs: %s\n",
                    one.out.raw.size(), one.statuses.size(),
                    (one.out.raw == two.out.raw && sameStatuses(one.statuses, two.statuses)) ? "yes" : "NO");
        CHECK(!one.out.raw.empty());
        CHECK(one.out.raw == two.out.raw);
        CHECK(sameStatuses(one.statuses, two.statuses));
        CHECK(one.requests.size() == two.requests.size());
        for (size_t i = 0; i < one.requests.size(); ++i)
            CHECK(one.requests[i].kind == two.requests[i].kind && one.requests[i].from == two.requests[i].from &&
                  one.requests[i].to == two.requests[i].to);
        CHECK(one.gaps == two.gaps && one.lost == two.lost && one.emitted == two.emitted);
        CHECK(one.out.clean() && one.out.messages == canonical.size());
    }

    // ---- 9: silence. A feed that stops is not a healthy feed, and only the caller's clock can
    // say so, because nothing is arriving to say it.
    {
        std::vector<FeedStatus> pub;
        LineArbitrator::Params ap{}; ap.feedId = FEED;
        ap.staleAfterNs = 1'000'000; ap.downAfterNs = 4'000'000;
        LineArbitrator arb(ap, {}, {}, [&](const FeedStatus& f) { pub.push_back(f); });
        alignas(16) std::byte buf[MAX_PACKET];
        PacketBuilder pb(buf);
        pb.begin(FEED, 0, firstSeq, T0);
        pb.add(frameAt(0));
        arb.onPacket(*pb.header(), pb.size(), 0, T0);
        CHECK(arb.state() == FeedState::Healthy && arb.expected() == firstSeq + 1);
        arb.tick(T0 + 500'000);   CHECK(arb.state() == FeedState::Healthy);
        arb.tick(T0 + 2'000'000); CHECK(arb.state() == FeedState::Stale);
        arb.tick(T0 + 9'000'000); CHECK(arb.state() == FeedState::Down);
        CHECK(arb.status().quality == MdQuality::NoData);
        arb.onPacket(*pb.header(), pb.size(), 1, T0 + 9'100'000);   // the sibling line's copy revives it
        CHECK(arb.state() == FeedState::Healthy && arb.status().quality == MdQuality::Depth);
        std::printf("silence: %zu status messages,", pub.size());
        for (const FeedStatus& f : pub) std::printf(" %s", stateName(f.state));
        std::printf("\n");
        CHECK(pub.size() == 3);
        CHECK(pub[0].state == FeedState::Stale && pub[1].state == FeedState::Down && pub[2].state == FeedState::Healthy);
        CHECK(pub[2].lastVenueSeq == firstSeq);
    }

    std::printf("line arbitrator tests ok\n");
    return 0;
}
