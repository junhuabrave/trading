// core/seq/ring.hpp : single-writer, multi-reader broadcast ring of frames.
//
// This is the in-process stand-in for Aeron IPC: same shape (one publisher, N independent
// subscribers each with their own cursor, back-pressure when the slowest lags a full ring).
// Fixed-size slots, no allocation, no locks; writer and each reader touch separate cache lines.
#pragma once
#include "trading.hpp"
#include <atomic>
#include <cstring>
#include <new>
#include <vector>
#include <stdexcept>

namespace trading::seq {

class BroadcastRing {
public:
    static constexpr size_t SLOT = ((48 + MAX_BLOCK_LENGTH + 63) / 64) * 64;   // largest frame, cache-line rounded
    static constexpr size_t MAX_READERS = 16;
    static_assert(SLOT >= 48 + MAX_BLOCK_LENGTH, "largest message must fit");

    explicit BroadcastRing(size_t capacityPow2)
        : cap_(capacityPow2), mask_(capacityPow2 - 1), slots_(capacityPow2) {
        if ((cap_ & (cap_ - 1)) != 0 || cap_ < 2) throw std::invalid_argument("ring capacity must be a power of two");
        for (auto& r : readers_) r.cursor.store(UINT64_MAX, std::memory_order_relaxed);
    }

    // Returns a reader id. Readers start at the current head (they see only new frames).
    int subscribe() {
        for (size_t i = 0; i < MAX_READERS; ++i)
            if (readers_[i].cursor.load(std::memory_order_relaxed) == UINT64_MAX) {
                readers_[i].cursor.store(head_.load(std::memory_order_acquire), std::memory_order_release);
                return int(i);
            }
        throw std::runtime_error("ring: too many readers");
    }
    void unsubscribe(int id) { readers_[size_t(id)].cursor.store(UINT64_MAX, std::memory_order_release); }

    // Writer side. Returns false (nothing written) if any reader is a full ring behind.
    bool tryPublish(const FrameHeader* f) noexcept {
        if (f->frameLength > SLOT) return false;
        uint64_t head = head_.load(std::memory_order_relaxed);
        if (head - slowest() >= cap_) return false;
        std::memcpy(slots_[head & mask_].data, f, f->frameLength);
        head_.store(head + 1, std::memory_order_release);
        return true;
    }
    // Space check for an atomic batch of n frames.
    bool canPublish(size_t n) const noexcept {
        return head_.load(std::memory_order_relaxed) - slowest() + n <= cap_;
    }

    // Reader side. Returns the next frame or nullptr. Caller must call advance() when done with it.
    const FrameHeader* poll(int id) const noexcept {
        const auto& r = readers_[size_t(id)];
        uint64_t c = r.cursor.load(std::memory_order_relaxed);
        if (c >= head_.load(std::memory_order_acquire)) return nullptr;
        return reinterpret_cast<const FrameHeader*>(slots_[c & mask_].data);
    }
    void advance(int id) noexcept {
        auto& r = readers_[size_t(id)];
        r.cursor.store(r.cursor.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }
    uint64_t lag(int id) const noexcept {
        return head_.load(std::memory_order_acquire) - readers_[size_t(id)].cursor.load(std::memory_order_relaxed);
    }
    size_t capacity() const noexcept { return cap_; }

private:
    uint64_t slowest() const noexcept {
        uint64_t s = UINT64_MAX;
        for (const auto& r : readers_) { uint64_t c = r.cursor.load(std::memory_order_acquire); if (c < s) s = c; }
        return s == UINT64_MAX ? head_.load(std::memory_order_relaxed) : s;
    }
    struct alignas(64) Slot { std::byte data[SLOT]; };
    struct alignas(64) Reader { std::atomic<uint64_t> cursor; char pad[56]; };

    size_t cap_, mask_;
    std::vector<Slot> slots_;
    alignas(64) std::atomic<uint64_t> head_{0};
    alignas(64) Reader readers_[MAX_READERS];
};

} // namespace trading::seq
