// core/util/alloc_guard.hpp : counts heap allocations so a benchmark or test can assert that a
// hot path allocates nothing after warm-up. Define TRADING_COUNT_ALLOCS in exactly one
// translation unit (each benchmark is one) to install the counting operators.
#pragma once
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>

namespace trading::util {
struct AllocCounter {
    static inline std::atomic<uint64_t> news{0}, deletes{0};
    static uint64_t allocations() noexcept { return news.load(std::memory_order_relaxed); }
};
// RAII: allocations that happened between construction and delta().
struct AllocScope {
    uint64_t start = AllocCounter::allocations();
    uint64_t delta() const noexcept { return AllocCounter::allocations() - start; }
};
}

#ifdef TRADING_COUNT_ALLOCS
// noinline: GCC otherwise inlines the replacement, sees malloc inside, and reports every
// sized delete as a mismatched allocation function
#define TRADING_NOINLINE __attribute__((noinline))
TRADING_NOINLINE void* operator new(std::size_t n) { trading::util::AllocCounter::news.fetch_add(1, std::memory_order_relaxed); if (void* p = std::malloc(n ? n : 1)) return p; throw std::bad_alloc(); }
TRADING_NOINLINE void* operator new[](std::size_t n) { trading::util::AllocCounter::news.fetch_add(1, std::memory_order_relaxed); if (void* p = std::malloc(n ? n : 1)) return p; throw std::bad_alloc(); }
TRADING_NOINLINE void operator delete(void* p) noexcept { trading::util::AllocCounter::deletes.fetch_add(1, std::memory_order_relaxed); std::free(p); }
TRADING_NOINLINE void operator delete[](void* p) noexcept { trading::util::AllocCounter::deletes.fetch_add(1, std::memory_order_relaxed); std::free(p); }
TRADING_NOINLINE void operator delete(void* p, std::size_t) noexcept { trading::util::AllocCounter::deletes.fetch_add(1, std::memory_order_relaxed); std::free(p); }
TRADING_NOINLINE void operator delete[](void* p, std::size_t) noexcept { trading::util::AllocCounter::deletes.fetch_add(1, std::memory_order_relaxed); std::free(p); }
#endif
