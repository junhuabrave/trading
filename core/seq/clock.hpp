// core/seq/clock.hpp : injected time source. Production uses the TSC-calibrated wall clock;
// tests and the simulator use a deterministic clock so replays are exact.
#pragma once
#include <cstdint>
#include <chrono>

namespace trading::seq {

struct Clock {
    virtual ~Clock() = default;
    virtual int64_t nowNs() noexcept = 0;
};

struct WallClock final : Clock {
    int64_t nowNs() noexcept override {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
};

// Monotonic clock for benchmarks: deltas are real elapsed time, the epoch is arbitrary.
struct SteadyClock final : Clock {
    int64_t nowNs() noexcept override {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    }
};

// Advances a fixed step per call; fully reproducible.
struct FakeClock final : Clock {
    int64_t t; int64_t step;
    explicit FakeClock(int64_t start = 1'700'000'000'000'000'000LL, int64_t stepNs = 250) : t(start), step(stepNs) {}
    int64_t nowNs() noexcept override { t += step; return t; }
};

} // namespace trading::seq
