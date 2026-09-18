// Sequencer benchmarks: stamp + journal + publish per frame, journal replay, and cross-thread
// ring hand-off latency. Emits JSON lines for tools/bench.py.
#define TRADING_COUNT_ALLOCS
#include "alloc_guard.hpp"
#include "bench.hpp"
#include "sequencer.hpp"
#include "reader.hpp"
#include <thread>
#include <filesystem>

using namespace trading; using namespace trading::seq; using namespace trading::bench; using trading::util::AllocScope;

int main(int argc, char** argv) {
    std::string dir = argc > 1 ? argv[1] : "build/seqbench"; std::filesystem::create_directories(dir);
    std::filesystem::remove_all(dir + "/core.jnl");
    const size_t B = 32;
    {
        const size_t N = 4'000'000;
        BroadcastRing ring(1 << 16); Journal j(dir + "/core.jnl", 0); FakeClock c; Sequencer s(0, 1, j, ring, c, 0);
        StreamReader r(ring, j, 1); uint64_t seen = 0;
        Frame<NewOrder> f; f.init(); f.body.qty = 100; f.body.price = 1'000'000'000;
        auto one = [&]() { f.header.seq = 0; f.header.originTs = 0; while (!s.submit(&f.header)) seen += r.poll([](const FrameHeader*) {}); };
        for (size_t i = 0; i < N / 10; ++i) { one(); if ((i & 1023) == 0) seen += r.poll([](const FrameHeader*) {}); }   // warm-up
        Recorder rec(B, N / B); AllocScope a; int64_t t0 = nowNs();
        for (size_t i = 0; i + B <= N; i += B) {
            rec.begin(); for (size_t k = 0; k < B; ++k) one(); rec.end();
            if ((i & 1023) == 0) seen += r.poll([](const FrameHeader*) {});
        }
        int64_t t1 = nowNs(); seen += r.poll([](const FrameHeader*) {});
        uint64_t al = a.delta();
        report("seq.stamp_journal_publish", rec.finish(), al, "one frame: stamp, buffered journal write, ring publish; reader drained in the same thread");
        reportThroughput("seq.frames_per_second", double(N) * 1e9 / double(t1 - t0), al);
        int64_t t2 = nowNs(); j.commit(true); int64_t t3 = nowNs();
        reportValue("seq.fsync_whole_journal", "ms", double(t3 - t2) / 1e6, std::to_string(j.sizeBytes() / 1000000) + " MB");
        uint64_t n = 0; AllocScope ar; int64_t t4 = nowNs(); j.replay(1, 0, [&](const FrameHeader*) { ++n; }); int64_t t5 = nowNs();
        uint64_t alr = ar.delta();
        Percentiles p; p.p50 = p.p99 = p.p999 = p.max = p.mean = double(t5 - t4) / double(n); p.samples = 1;
        report("seq.journal_replay", p, alr, "mean per frame over the whole journal, one sample (replay opens a file handle per segment)");
        std::fprintf(stderr, "reader saw %llu\n", (unsigned long long)seen);
    }
    {
        BroadcastRing ring(1 << 12); const size_t N = 1'000'000;
        int rid = ring.subscribe();
        std::vector<double> lat; lat.reserve(N);
        std::thread reader([&] {
            size_t got = 0;
            while (got < N) { if (const FrameHeader* f = ring.poll(rid)) { lat.push_back(double(nowNs() - f->originTs)); ring.advance(rid); ++got; } }
        });
        Frame<Heartbeat> f; f.init();
        for (size_t i = 0; i < N; ++i) {
            f.header.seq = i + 1; f.header.originTs = nowNs();
            while (!ring.tryPublish(&f.header)) {}
            for (int k = 0; k < 200; ++k) { asm volatile("" ::: "memory"); }   // pace the writer: measure hand-off, not queueing
        }
        reader.join();
        report("ring.handoff_cross_thread", percentiles(lat), 0, "writer thread to reader thread, unpinned");
    }
    return 0;
}
