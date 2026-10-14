// sim/run_sim.cpp : run a scenario through the harness, verify it by cold replay, run the
// drills, and compare against (or write) a stored baseline.
//
//   run_sim <snapshot> <workdir> [--scenario NAME] [--seed N] [--steps N] [--adversarial]
//           [--drill kill|session-drop|rate-burst|torn-tail] [--events FILE]
//           [--baseline FILE] [--write-baseline FILE]
//           [--profile dark-first|passive-post|mid-peg] [--dark-venue CONTRA_PCT] [--allow-dark PCT] [--venues 1|2]
#include "harness.hpp"
#include "bench.hpp"
#include <cstring>
#include <fstream>
#include <regex>

using namespace trading; using namespace trading::sim;

static std::string baselineJson(const std::string& scenario, const Config& c, const Stats& s, const Harness::VerifyResult& v) {
    char buf[2048];
    std::snprintf(buf, sizeof buf,
        "{\n  \"scenario\": \"%s\",\n  \"seed\": %llu,\n  \"steps\": %u,\n  \"orders\": %llu,\n  \"accepted\": %llu,\n  \"rejected\": %llu,\n"
        "  \"fills\": %llu,\n  \"cancelled\": %llu,\n  \"children\": %llu,\n  \"reroutes\": %llu,\n  \"checkpoints\": %llu,\n  \"coreSeq\": %llu,\n  \"mdSeq\": %llu,\n"
        "  \"verified\": %llu,\n  \"finalHash\": \"%s\"\n}\n",
        scenario.c_str(), (unsigned long long)c.seed, c.steps, (unsigned long long)s.orders, (unsigned long long)s.accepted, (unsigned long long)s.rejected,
        (unsigned long long)s.fills, (unsigned long long)s.cancelled, (unsigned long long)s.children, (unsigned long long)s.reroutes, (unsigned long long)s.checkpoints,
        (unsigned long long)s.coreSeq, (unsigned long long)s.mdSeq, (unsigned long long)v.verified, hex(s.finalHash).c_str());
    return buf;
}
static std::string jsonField(const std::string& j, const char* key) {
    std::regex re("\"" + std::string(key) + "\"\\s*:\\s*\"?([^\",}\\n]+)\"?");
    std::smatch m; return std::regex_search(j, m, re) ? m[1].str() : "";
}

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "usage: run_sim <snapshot> <workdir> [options]\n"); return 2; }
    Config c; c.snapshot = argv[1]; c.workdir = argv[2];
    std::string scenario = "random-day", baseline, writeBaseline;
    for (int i = 3; i < argc; ++i) {
        std::string a = argv[i]; auto next = [&]() { return std::string(argv[++i]); };
        if (a == "--scenario") scenario = next(); else if (a == "--seed") c.seed = std::stoull(next()); else if (a == "--steps") c.steps = uint32_t(std::stoul(next()));
        else if (a == "--adversarial") c.adversarial = true; else if (a == "--drill") c.drill = next(); else if (a == "--events") c.eventsFile = next();
        else if (a == "--baseline") baseline = next(); else if (a == "--write-baseline") writeBaseline = next();
        else if (a == "--sessions") c.sessions = uint32_t(std::stoul(next())); else if (a == "--throttle") c.throttlePerSec = uint32_t(std::stoul(next()));
        else if (a == "--bench") c.benchMode = true;
        else if (a == "--profile") c.profile = next(); else if (a == "--dark-venue") c.darkContraPct = std::stoi(next());
        else if (a == "--allow-dark") c.allowDarkPct = std::stoi(next());
        else if (a == "--venues") c.litVenues = uint32_t(std::stoul(next()));
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); return 2; }
    }
    Harness h(c);
    if (c.benchMode) {
        // front to back: real clock, no drills, no replay; stage latencies from the journal, JSON for tools/bench.py
        Stats s = h.runLive();
        for (auto& st : h.stageLatencies()) trading::bench::report(st.name, trading::bench::percentiles(st.ns), 0, "harness single thread, includes the harness loop between stages");
        trading::bench::reportValue("f2b.max_engine_ring_lag", "frames", double(s.maxRingLag), "engine reader behind the sequencer at its worst; ring is 16384");
        trading::bench::reportValue("f2b.orders", "count", double(s.orders));
        return s.maxRingLag > 16384 / 4 ? 1 : 0;
    }
    Stats s = h.runLive();
    std::printf("live [%s%s]: core seq=%llu md seq=%llu orders=%llu accepted=%llu rejected=%llu fills=%llu cancelled=%llu venueRejects=%llu children=%llu checkpoints=%llu delayed=%llu\n",
        scenario.c_str(), c.drill.empty() ? "" : (" drill=" + c.drill).c_str(), (unsigned long long)s.coreSeq, (unsigned long long)s.mdSeq,
        (unsigned long long)s.orders, (unsigned long long)s.accepted, (unsigned long long)s.rejected, (unsigned long long)s.fills, (unsigned long long)s.cancelled,
        (unsigned long long)s.venueRejects, (unsigned long long)s.children, (unsigned long long)s.checkpoints, (unsigned long long)s.delayed);
    std::printf("  reroutes %llu, parents left with leaves and no plan: %llu\n",
                (unsigned long long)s.reroutes, (unsigned long long)s.stranded);
    std::printf("  sessions used: "); for (size_t i = 0; i < c.sessions; ++i) std::printf("%zu:%llu ", i, (unsigned long long)s.placed[i]); std::printf("\n");
    int rc = 0;
    // drop copy must agree with the OMS-side fill count: what the venue says it did equals what we booked
    if (s.dropCopyFills != s.fills) { std::printf("FAIL: drop copy has %llu fills, OMS booked %llu\n", (unsigned long long)s.dropCopyFills, (unsigned long long)s.fills); rc = 1; }
    if (s.orders == 0 || s.accepted == 0 || s.fills == 0) { std::printf("FAIL: scenario produced no flow\n"); rc = 1; }
    // every market-data frame must carry a (feedId, venueSeq) identity, or nothing can replay to it
    if (s.unidentifiedMd) { std::printf("FAIL: %llu market-data frames had no venueSeq\n", (unsigned long long)s.unidentifiedMd); rc = 1; }
    if (c.drill == "session-drop" && (s.placed[0] == 0 || s.cancelled == 0)) { std::printf("FAIL: session-drop drill did not cancel or re-place\n"); rc = 1; }
    // Every path that ends a child either plans again, finishes a cancel or replace, or gives up
    // and tells the client. A parent that has quantity left and nobody working it means one of
    // those paths is missing, and an order that quietly stopped working is the worst kind of bug.
    if (s.stranded) { std::printf("FAIL: %llu parents left with leaves and no plan\n", (unsigned long long)s.stranded); rc = 1; }
    if (c.drill == "kill" && s.rejected == 0) { std::printf("FAIL: kill drill rejected nothing\n"); rc = 1; }
    if (c.drill == "rate-burst" && s.rejected == 0) { std::printf("FAIL: rate burst rejected nothing\n"); rc = 1; }

    // RT-8: the children of one plan land together. The bound is the wire's jitter, which no offset
    // can remove; without the offsets they would land the difference in latency apart.
    if (c.litVenues >= 2) {
        std::printf("  plans across both lit venues %llu: arrival spread p50 %lld ns, max %lld ns (bound %lld); released all at once it would be p50 %lld, max %lld\n",
                    (unsigned long long)s.multiVenuePlans, (long long)s.spreadP50, (long long)s.spreadMax, (long long)LATENCY_JITTER_NS,
                    (long long)s.unheldSpreadP50, (long long)s.unheldSpreadMax);
        if (s.multiVenuePlans == 0) { std::printf("FAIL: no plan used both lit venues\n"); rc = 1; }
        if (s.spreadMax > LATENCY_JITTER_NS) { std::printf("FAIL: children of one plan landed further apart than the jitter\n"); rc = 1; }
    }
    // The resting strategies. Each one has to have actually rested, its timer has to have been a
    // sequenced message that went off, and what it got has to be what the strategy is for.
    if (!c.profile.empty()) {
        std::printf("  timers armed %llu, fired %llu, plans that crossed because the wait ran out %llu\n",
                    (unsigned long long)s.timersArmed, (unsigned long long)s.timersFired, (unsigned long long)s.escalations);
        if (s.timersFired == 0 || s.escalations == 0) { std::printf("FAIL: no resting plan ran out of time and crossed\n"); rc = 1; }
        // A wait that ran out has to end the wait: whatever is still resting at the close is at most
        // what the timers not yet due are holding. More than that is a child the timer left behind.
        std::printf("  resting at the close %llu, timers not yet due %llu\n", (unsigned long long)s.stillResting, (unsigned long long)(s.timersArmed - s.timersFired));
        if (s.stillResting > s.timersArmed - s.timersFired) { std::printf("FAIL: children still resting after their timer went off\n"); rc = 1; }
    }
    if (c.profile == "dark-first") {
        std::printf("  dark children %llu, declined for want of allowDark %llu, dark fills %llu (all added liquidity: %s)\n",
                    (unsigned long long)s.darkChildren, (unsigned long long)s.darkDeclined, (unsigned long long)s.darkFills.fills,
                    s.darkFills.added == s.darkFills.fills ? "yes" : "NO");
        if (s.darkChildren == 0) { std::printf("FAIL: nothing went dark\n"); rc = 1; }
        if (c.allowDarkPct < 100 && s.darkDeclined == 0) { std::printf("FAIL: no order without allowDark was kept out of the dark\n"); rc = 1; }
        if (c.darkContraPct > 0 && s.darkFills.fills == 0) { std::printf("FAIL: a pool with contra flow filled nothing\n"); rc = 1; }
        if (c.darkContraPct == 0 && s.darkFills.fills != 0) { std::printf("FAIL: a pool with no contra flow filled something\n"); rc = 1; }
        if (s.darkFills.added != s.darkFills.fills) { std::printf("FAIL: a dark fill removed liquidity\n"); rc = 1; }
    }
    if (c.profile == "passive-post") {
        // A post joins the queue at its own price and is filled by flow arriving at it: every fill
        // adds liquidity, earning the rebate the strategy chose the venue for, at the child's price.
        std::printf("  passive fills %llu, added liquidity %llu, at the posted price %llu; posts cancelled back for crossing on arrival %llu\n",
                    (unsigned long long)s.passiveFills.fills, (unsigned long long)s.passiveFills.added, (unsigned long long)s.passiveFills.atLimit,
                    (unsigned long long)s.postOnlyCancelled);
        if (s.passiveFills.fills == 0) { std::printf("FAIL: no passive post filled\n"); rc = 1; }
        if (s.passiveFills.added != s.passiveFills.fills || s.passiveFills.atLimit != s.passiveFills.fills) { std::printf("FAIL: a passive post took liquidity or printed away from its price\n"); rc = 1; }
    }

    Harness::VerifyResult v = h.verify();
    std::printf("cold replay: %llu frames, %llu decisions/routes/acks verified, %llu mismatches, %llu checkpoints, hash %s %s\n",
        (unsigned long long)v.frames, (unsigned long long)v.verified, (unsigned long long)v.mismatches, (unsigned long long)v.checkpoints,
        hex(v.finalHash, 8).c_str(), v.finalHash == s.finalHash ? "identical" : "DIFFERENT");
    if (v.mismatches || v.finalHash != s.finalHash) { std::printf("FAIL: %s\n", v.first.c_str()); rc = 1; }

    // PK-1. The position keeper and the risk engine fold the same fills with the same function, so
    // at every checkpoint the risk engine has kept up with, the keeper's figures must be its own.
    std::printf("positions: %llu held, %llu snapshots published; risk took %llu of them (%llu older than its last fill, skipped) and disagreed with %llu\n",
                (unsigned long long)v.pkPositions, (unsigned long long)v.pkSnapshots, (unsigned long long)v.riskRebases,
                (unsigned long long)v.riskStale, (unsigned long long)v.riskCorrections);
    if (v.pkSnapshots == 0 || v.riskRebases == 0) { std::printf("FAIL: the keeper published nothing the risk engine could check\n"); rc = 1; }
    if (v.riskCorrections) { std::printf("FAIL: the risk engine and the position keeper disagree about a position\n"); rc = 1; }
    const auto& rs = v.restart;
    std::printf("restart: risk engine started from the keeper's snapshots at checkpoint seq %llu (%llu positions), folded %llu fills after it; "
                "%llu positions and account totals compared with the engine that ran from the open, %llu differ\n",
                (unsigned long long)rs.atSeq, (unsigned long long)rs.loaded, (unsigned long long)rs.foldedAfter,
                (unsigned long long)rs.compared, (unsigned long long)rs.differ);
    if (rs.atSeq == 0 || rs.loaded == 0 || rs.foldedAfter == 0) { std::printf("FAIL: the restart drill had nothing to restart from\n"); rc = 1; }
    if (rs.differ || rs.corrections) { std::printf("FAIL: a risk engine restarted mid-day does not reach the same exposure\n"); rc = 1; }

    if (c.drill == "torn-tail") {
        // append a partial frame to the active core segment, reopen, resume the sequencer, replay
        Journal j(c.workdir + "/core.jnl", 0, 1, c.segmentBytes);
        uint64_t before = j.lastSeq();
        { std::FILE* fp = std::fopen(j.activeSegmentPath().c_str(), "ab"); Frame<Heartbeat> hb; hb.init(); hb.header.seq = before + 1; std::fwrite(&hb, 1, 30, fp); std::fclose(fp); }
        Journal re(c.workdir + "/core.jnl", 0, 1, c.segmentBytes);
        bool ok = re.lastSeq() == before && re.truncatedOnOpen() == 30;
        BroadcastRing ring(64); FakeClock clk; Sequencer resumed(0, 1, re, ring, clk, 0);
        ok = ok && resumed.nextSeq() == before + 1; resumed.heartbeat(ComponentState::Live); ok = ok && re.lastSeq() == before + 1;
        std::printf("torn-tail: truncated %llu bytes, resumed at %llu: %s\n", (unsigned long long)re.truncatedOnOpen(), (unsigned long long)before + 1, ok ? "ok" : "FAIL");
        if (!ok) rc = 1;
    }

    std::string json = baselineJson(scenario, c, s, v);
    if (!writeBaseline.empty()) { std::ofstream(writeBaseline) << json; std::printf("wrote baseline %s\n", writeBaseline.c_str()); }
    if (!baseline.empty()) {
        std::ifstream in(baseline); if (!in) { std::printf("FAIL: no baseline at %s (run with --write-baseline)\n", baseline.c_str()); return 1; }
        std::string stored((std::istreambuf_iterator<char>(in)), {});
        for (const char* k : {"orders", "accepted", "rejected", "fills", "cancelled", "children", "reroutes", "checkpoints", "coreSeq", "verified", "finalHash"}) {
            std::string a = jsonField(stored, k), b = jsonField(json, k);
            if (a != b) { std::printf("FAIL: baseline %s: %s stored=%s now=%s\n", baseline.c_str(), k, a.c_str(), b.c_str()); rc = 1; }
        }
        if (rc == 0) std::printf("baseline %s: identical\n", baseline.c_str());
    }
    std::printf(rc ? "sim FAILED\n" : "sim ok\n");
    return rc;
}
