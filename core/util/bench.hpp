// core/util/bench.hpp : what every benchmark uses to measure and report.
//
// Ops are timed in blocks (the timer costs more than the op) and the per-op figure of each
// block is recorded; percentiles are over blocks, which understates the true p99.9 of single
// ops slightly and is stated as such in the report. Reports are one JSON line each, which
// tools/bench.py collects and compares against bench/baselines/<host-class>.json.
#pragma once
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/utsname.h>
#if defined(__APPLE__)
#include <sys/sysctl.h>
#endif

namespace trading::bench {

inline int64_t nowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

struct Percentiles { double p50 = 0, p99 = 0, p999 = 0, max = 0, mean = 0; uint64_t samples = 0; };

inline Percentiles percentiles(std::vector<double>& v) {
    Percentiles p; if (v.empty()) return p;
    std::sort(v.begin(), v.end());
    p.samples = v.size();
    auto at = [&](double q) { size_t i = size_t(q * double(v.size() - 1)); return v[i]; };
    p.p50 = at(0.5); p.p99 = at(0.99); p.p999 = at(0.999); p.max = v.back();
    double s = 0; for (double x : v) s += x; p.mean = s / double(v.size());
    return p;
}

// Records per-op nanoseconds in blocks of `block` ops.
class Recorder {
public:
    explicit Recorder(size_t block = 32, size_t expectedBlocks = 1 << 16) : block_(block) { perOp_.reserve(expectedBlocks); }
    void begin() noexcept { t0_ = nowNs(); }
    void end() noexcept { int64_t t1 = nowNs(); perOp_.push_back(double(t1 - t0_) / double(block_)); }
    size_t block() const noexcept { return block_; }
    Percentiles finish() { return percentiles(perOp_); }
private:
    size_t block_; int64_t t0_ = 0; std::vector<double> perOp_;
};

inline std::string hostClass() {
    struct utsname u{}; ::uname(&u);
    std::string cpu = "unknown";
#if defined(__APPLE__)
    char buf[256]; size_t len = sizeof buf;
    if (::sysctlbyname("machdep.cpu.brand_string", buf, &len, nullptr, 0) == 0) cpu = buf;
#else
    if (FILE* f = std::fopen("/proc/cpuinfo", "r")) {
        char line[512];
        while (std::fgets(line, sizeof line, f)) if (std::string(line).rfind("model name", 0) == 0) { cpu = line; cpu = cpu.substr(cpu.find(':') + 2); break; }
        std::fclose(f);
    }
#endif
    while (!cpu.empty() && (cpu.back() == '\n' || cpu.back() == ' ')) cpu.pop_back();
    return std::string(u.machine) + "/" + cpu;
}

inline std::string jsonEscape(const std::string& s) { std::string o; for (char c : s) { if (c == '"' || c == '\\') o += '\\'; o += c; } return o; }

// One JSON line per benchmark. unit is "ns/op" for latency-style rows and "ops/s" for throughput rows.
inline void report(const std::string& name, const Percentiles& p, uint64_t allocs, const std::string& note = "") {
    std::printf("{\"bench\":\"%s\",\"unit\":\"ns/op\",\"p50\":%.1f,\"p99\":%.1f,\"p999\":%.1f,\"max\":%.1f,\"mean\":%.1f,\"samples\":%llu,\"allocs\":%llu,\"note\":\"%s\"}\n",
        jsonEscape(name).c_str(), p.p50, p.p99, p.p999, p.max, p.mean, (unsigned long long)p.samples, (unsigned long long)allocs, jsonEscape(note).c_str());
}
inline void reportThroughput(const std::string& name, double opsPerSec, uint64_t allocs, const std::string& note = "") {
    std::printf("{\"bench\":\"%s\",\"unit\":\"ops/s\",\"value\":%.0f,\"allocs\":%llu,\"note\":\"%s\"}\n", jsonEscape(name).c_str(), opsPerSec, (unsigned long long)allocs, jsonEscape(note).c_str());
}
inline void reportValue(const std::string& name, const std::string& unit, double value, const std::string& note = "") {
    std::printf("{\"bench\":\"%s\",\"unit\":\"%s\",\"value\":%.1f,\"allocs\":0,\"note\":\"%s\"}\n", jsonEscape(name).c_str(), unit.c_str(), value, jsonEscape(note).c_str());
}

} // namespace trading::bench
