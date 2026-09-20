// Market-data transport benchmarks: what it costs to take a packet off a line, capture it, and
// hand it up, and what replaying a capture costs. The receiver is the first thing that touches a
// packet, so its cost is paid once per packet on every feed, all day.
#define TRADING_COUNT_ALLOCS
#include "alloc_guard.hpp"
#include "bench.hpp"
#include "line_receiver.hpp"
#include <filesystem>
#include <vector>
#include <random>

using namespace trading; using namespace trading::md; using namespace trading::bench; using trading::util::AllocScope;

int main(int argc, char** argv) {
    std::string dir = argc > 1 ? argv[1] : "build/mdbench";
    std::filesystem::create_directories(dir);
    const uint32_t FEED = 1;
    const size_t N = 200'000, B = 16;

    // a realistic packet: four book deltas, which is what a busy line carries
    alignas(16) std::byte pkt[MAX_PACKET];
    PacketBuilder pb(pkt);
    pb.begin(FEED, 0, 1'000'000, 0);
    for (int i = 0; i < 4; ++i) {
        Frame<BookDelta> f; f.init(); f.header.streamId = FEED;
        f.body.symbolIdx = uint32_t(1 + i); f.body.venueId = 2; f.body.price = 1'000'000'000; f.body.qty = 500;
        f.body.venueSeq = 1'000'000 + uint64_t(i); pb.add(&f.header);
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
    std::fprintf(stderr, "sink saw %llu packets\n", (unsigned long long)seen);
    std::filesystem::remove(cap);
    return 0;
}
