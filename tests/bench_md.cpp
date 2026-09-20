// Market-data transport benchmarks: what it costs to take a packet off a line, capture it, hand it
// up and arbitrate it. The receiver is the first thing that touches a packet and the arbitrator is
// the second, so both are paid once per packet on every line of every feed, all day.
//
// The arbitrator reads only the envelope, so these numbers do not move with the size or shape of
// the payload; what they do move with is whether a packet is new, a sibling line's copy, or early.
#define TRADING_COUNT_ALLOCS
#include "alloc_guard.hpp"
#include "bench.hpp"
#include "line_receiver.hpp"
#include "arbitrator.hpp"
#include <filesystem>
#include <vector>
#include <random>

using namespace trading; using namespace trading::md; using namespace trading::bench; using trading::util::AllocScope;

int main(int argc, char** argv) {
    std::string dir = argc > 1 ? argv[1] : "build/mdbench";
    std::filesystem::create_directories(dir);
    const uint32_t FEED = 1;
    const uint64_t BASE = 1'000'000;
    const size_t N = 200'000, B = 16;

    // a realistic packet: four book deltas, which is what a busy line carries
    alignas(16) std::byte pkt[MAX_PACKET];
    PacketBuilder pb(pkt);
    pb.begin(FEED, 0, BASE, 0);
    for (int i = 0; i < 4; ++i) {
        Frame<BookDelta> f; f.init(); f.header.streamId = FEED;
        f.body.symbolIdx = uint32_t(1 + i); f.body.venueId = 2; f.body.price = 1'000'000'000; f.body.qty = 500;
        f.body.venueSeq = BASE + uint64_t(i); pb.add(&f.header);
    }
    const size_t len = pb.size();

    uint64_t seen = 0;
    auto sink = [&](const PacketHeader&, size_t, uint16_t, int64_t) { ++seen; };

    // 1. receive and hand up, no capture: the floor
    {
        LineReceiver r(FEED, 0, nullptr, sink);
        for (size_t i = 0; i < N / 10; ++i) r.onPacket(pkt, len, 1);
        Recorder rec(B, N / B); AllocScope a;
        for (size_t i = 0; i + B <= N; i += B) { rec.begin(); for (size_t k = 0; k < B; ++k) r.onPacket(pkt, len, int64_t(i + k)); rec.end(); }
        uint64_t al = a.delta();
        report("md.receive_no_capture", rec.finish(), al, "validate, check the feed, hand up");
        if (al) { std::fprintf(stderr, "FAIL: receive path allocated %llu times\n", (unsigned long long)al); return 1; }
    }
    // 2. receive and capture: what production actually pays
    std::string cap = dir + "/bench.cap";
    {
        CaptureWriter w(cap, FEED, 20260915, 0);
        LineReceiver r(FEED, 0, &w, sink);
        for (size_t i = 0; i < N / 10; ++i) r.onPacket(pkt, len, 1);
        Recorder rec(B, N / B); AllocScope a;
        for (size_t i = 0; i + B <= N; i += B) { rec.begin(); for (size_t k = 0; k < B; ++k) r.onPacket(pkt, len, int64_t(i + k)); rec.end(); }
        uint64_t al = a.delta();
        report("md.receive_and_capture", rec.finish(), al, "the same, plus the buffered capture write");
        reportValue("md.capture_bytes_per_packet", "bytes", double(w.bytes()) / double(w.records()));
        if (al) { std::fprintf(stderr, "FAIL: capture path allocated %llu times\n", (unsigned long long)al); return 1; }
        w.close();
    }
    // 3. replaying a capture, which is how a recorded day reaches the simulator
    {
        uint64_t n = 0; AllocScope a; int64_t t0 = nowNs();
        replayCapture(cap, [&](const PacketHeader&, size_t, uint16_t, int64_t) { ++n; });
        int64_t t1 = nowNs();
        Percentiles p; p.p50 = p.p99 = p.p999 = p.max = p.mean = double(t1 - t0) / double(n); p.samples = 1;
        report("md.capture_replay", p, a.delta(), "mean per packet over the whole capture; the one allocation is the reader's buffer, once per file");
    }
    // 4. arbitration, the steady state: every packet arrives twice, once per line, and one copy of
    // each is discarded. This is what a healthy A/B feed costs all day.
    {
        uint64_t outPackets = 0, outMessages = 0;
        LineArbitrator::Params ap{}; ap.feedId = FEED; ap.startSeq = BASE;
        LineArbitrator arb(ap, [&](const PacketHeader& h, size_t, uint16_t skip) {
            ++outPackets; outMessages += h.msgCount - skip;
        });
        auto* hdr = reinterpret_cast<PacketHeader*>(pkt);
        uint64_t seq = BASE;
        auto pair = [&](int64_t ts) {                       // line 0 first, line 1 three microseconds later
            hdr->firstSeq = seq;
            arb.onPacket(*hdr, len, 0, ts);
            arb.onPacket(*hdr, len, 1, ts + 3000);
            seq += hdr->msgCount;
        };
        for (size_t i = 0; i < N / 10; ++i) pair(int64_t(i) * 8000);
        Recorder rec(B * 2, N / B); AllocScope a;             // two calls per logical packet
        int64_t t = int64_t(N) * 8000;
        for (size_t i = 0; i + B <= N; i += B) { rec.begin(); for (size_t k = 0; k < B; ++k) pair(t + int64_t(i + k) * 8000); rec.end(); }
        uint64_t al = a.delta();
        Percentiles p = rec.finish();
        report("md.arbitrate_two_lines", p, al, "per packet off one line, averaged over the A/B pair: one accepted, one discarded as the sibling's copy");
        // Derived from the row above rather than measured separately, so it is reported and not
        // gated. It is the envelope ceiling only: what a feed process can actually sustain is set
        // by the decoder (MD-4), which is the piece that reads the payload.
        reportValue("md.arbitrate_messages_per_sec", "msg/s", double(hdr->msgCount) * 1e9 / (2.0 * p.mean),
                    "envelope only; the decoder sets the real feed-process ceiling");
        if (al) { std::fprintf(stderr, "FAIL: arbitration allocated %llu times after construction\n", (unsigned long long)al); return 1; }
        if (outMessages != (seq - BASE)) { std::fprintf(stderr, "FAIL: arbitrator emitted %llu of %llu messages\n", (unsigned long long)outMessages, (unsigned long long)(seq - BASE)); return 1; }
        std::fprintf(stderr, "arbitrator emitted %llu packets, %llu messages\n", (unsigned long long)outPackets, (unsigned long long)outMessages);
    }
    // 5. arbitration with the reorder buffer in play: each pair of packets arrives back to front, so
    // every packet is held and then drained. This is the cost of a line that is not behaving.
    {
        uint64_t outPackets = 0;
        LineArbitrator::Params ap{}; ap.feedId = FEED; ap.startSeq = BASE;
        ap.gapTimeoutNs = 1'000'000'000;                      // never ask; we are measuring the buffer, not recovery
        LineArbitrator arb(ap, [&](const PacketHeader&, size_t, uint16_t) { ++outPackets; });
        auto* hdr = reinterpret_cast<PacketHeader*>(pkt);
        uint64_t seq = BASE;
        auto swapped = [&](int64_t ts) {
            hdr->firstSeq = seq + hdr->msgCount; arb.onPacket(*hdr, len, 0, ts);          // the later one first
            hdr->firstSeq = seq;                  arb.onPacket(*hdr, len, 0, ts + 1000);  // then the one that fills the hole
            seq += uint64_t(hdr->msgCount) * 2;
        };
        for (size_t i = 0; i < N / 20; ++i) swapped(int64_t(i) * 8000);
        Recorder rec(B * 2, N / B); AllocScope a;
        int64_t t = int64_t(N) * 8000;
        for (size_t i = 0; i + B <= N / 2; i += B) { rec.begin(); for (size_t k = 0; k < B; ++k) swapped(t + int64_t(i + k) * 8000); rec.end(); }
        uint64_t al = a.delta();
        report("md.arbitrate_reordered", rec.finish(), al, "per packet, every pair arriving back to front: buffered, then drained in order");
        if (al) { std::fprintf(stderr, "FAIL: reorder path allocated %llu times after construction\n", (unsigned long long)al); return 1; }
        std::fprintf(stderr, "arbitrator emitted %llu packets through the reorder buffer\n", (unsigned long long)outPackets);
    }
    std::fprintf(stderr, "sink saw %llu packets\n", (unsigned long long)seen);
    std::filesystem::remove(cap);
    return 0;
}
