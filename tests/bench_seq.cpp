// Sequencer throughput (single thread, journal + ring) and ring hand-off latency across two threads.
#include "sequencer.hpp"
#include "reader.hpp"
#include <thread>
#include <chrono>
#include <algorithm>
#include <cstdio>
#include <filesystem>

using namespace trading; using namespace trading::seq;
using clk = std::chrono::steady_clock;
static int64_t now() { return std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now().time_since_epoch()).count(); }

int main(int argc, char** argv) {
    std::string dir = argc > 1 ? argv[1] : "build/seqbench"; std::filesystem::create_directories(dir);
    std::filesystem::remove_all(dir + "/core.jnl");
    // 1. throughput: 5M NewOrder frames sequenced, journaled (buffered), published, drained in the same thread
    {
        const size_t N = 5'000'000;
        BroadcastRing ring(1 << 16); Journal j(dir + "/core.jnl", 0); FakeClock c; Sequencer s(0, 1, j, ring, c, 0);
        StreamReader r(ring, j, 1); uint64_t seen = 0;
        Frame<NewOrder> f; f.init(); f.body.qty = 100; f.body.price = 1'000'000'000;
        auto t0 = now();
        for (size_t i = 0; i < N; ++i) {
            f.header.seq = 0; f.header.originTs = 0;
            while (!s.submit(&f.header)) seen += r.poll([](const FrameHeader*) {});
            if ((i & 1023) == 0) seen += r.poll([](const FrameHeader*) {});
        }
        seen += r.poll([](const FrameHeader*) {});
        auto t1 = now();
        double ns = double(t1 - t0) / double(N);
        std::printf("sequencer: %zu frames in %.2f s = %.1f ns/frame (%.2f M/s), journal %.1f MB, reader saw %llu\n",
            N, double(t1 - t0) / 1e9, ns, 1e3 / ns, double(j.sizeBytes()) / 1e6, (unsigned long long)seen);
        auto t2 = now(); j.commit(true); auto t3 = now();
        std::printf("fdatasync of the whole journal: %.1f ms\n", double(t3 - t2) / 1e6);
        // replay speed
        uint64_t n = 0; auto t4 = now(); j.replay(1, 0, [&](const FrameHeader*) { ++n; }); auto t5 = now();
        std::printf("journal replay: %llu frames, %.1f ns/frame\n", (unsigned long long)n, double(t5 - t4) / double(n));
    }
    // 2. ring hand-off latency, writer thread -> reader thread
    {
        BroadcastRing ring(1 << 12); const size_t N = 1'000'000;
        int rid = ring.subscribe();
        std::vector<int64_t> lat; lat.reserve(N);
        std::thread reader([&] {
            size_t got = 0;
            while (got < N) {
                if (const FrameHeader* f = ring.poll(rid)) { lat.push_back(now() - f->originTs); ring.advance(rid); ++got; }
            }
        });
        Frame<Heartbeat> f; f.init();
        for (size_t i = 0; i < N; ++i) {
            f.header.seq = i + 1; f.header.originTs = now();
            while (!ring.tryPublish(&f.header)) {}
            for (int k = 0; k < 200; ++k) { asm volatile("" ::: "memory"); }   // pace the writer: measure hand-off, not queueing
        }
        reader.join();
        std::sort(lat.begin(), lat.end());
        std::printf("ring hand-off (cross-thread, unpinned): p50 %lld ns, p99 %lld ns, p99.9 %lld ns, max %lld ns\n",
            (long long)lat[N / 2], (long long)lat[N * 99 / 100], (long long)lat[N * 999 / 1000], (long long)lat.back());
    }
    return 0;
}
