// core/util/flatmap.hpp : open-addressing hash table keyed by our 64-bit or 128-bit ids.
//
// Replaces std::unordered_map on the order path: one contiguous array, linear probing,
// no per-node allocation, erase is a backward shift (no tombstones, so a cancel-heavy day
// never degrades probe lengths). Sized at construction; it grows only if the reserve was
// wrong, which the engines treat as a configuration error to fix.
#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>
#include <utility>

namespace trading::util {

struct Key128 {
    uint64_t hi, lo;
    bool operator==(const Key128& o) const noexcept { return hi == o.hi && lo == o.lo; }
};

inline uint64_t mix64(uint64_t x) noexcept {           // splitmix64 finaliser
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}
inline uint64_t hashKey(uint64_t k) noexcept { return mix64(k); }
inline uint64_t hashKey(const Key128& k) noexcept { return mix64(k.hi ^ mix64(k.lo)); }

template <class K, class V>
class FlatMap {
public:
    explicit FlatMap(size_t expected = 1024) { init(expected); }

    V* find(const K& k) noexcept {
        size_t i = probe(k);
        return i == npos ? nullptr : &slots_[i].val;
    }
    const V* find(const K& k) const noexcept {
        size_t i = probe(k);
        return i == npos ? nullptr : &slots_[i].val;
    }
    bool contains(const K& k) const noexcept { return probe(k) != npos; }

    // Insert or overwrite. Returns the stored value.
    V& insert(const K& k, const V& v) {
        if ((used_ + 1) * 4 > cap_ * 3) rehash(cap_ * 2);
        size_t i = hashKey(k) & mask_;
        for (;;) {
            Slot& s = slots_[i];
            if (s.state == USED) { if (s.key == k) { s.val = v; return s.val; } }
            else break;
            i = (i + 1) & mask_;
        }
        Slot& s = slots_[i]; s.key = k; s.val = v; s.state = USED; ++used_;
        return s.val;
    }
    V& operator[](const K& k) {
        if (V* p = find(k)) return *p;
        return insert(k, V{});
    }
    // Backward-shift deletion: pull later entries of the probe chain back so no hole is left.
    bool erase(const K& k) noexcept {
        size_t i = probe(k);
        if (i == npos) return false;
        size_t j = i;
        for (;;) {
            j = (j + 1) & mask_;
            Slot& sj = slots_[j];
            if (sj.state != USED) break;
            size_t home = hashKey(sj.key) & mask_;
            // sj may move to i if its home is not in the cyclic range (i, j]
            bool inRange = i <= j ? (home > i && home <= j) : (home > i || home <= j);
            if (!inRange) { slots_[i] = sj; i = j; }
        }
        slots_[i].state = EMPTY; --used_;
        return true;
    }
    size_t size() const noexcept { return used_; }
    size_t capacity() const noexcept { return cap_; }
    void clear() noexcept { for (auto& s : slots_) s.state = EMPTY; used_ = 0; }
    template <class F> void forEach(F&& f) const { for (const auto& s : slots_) if (s.state == USED) f(s.key, s.val); }
    template <class F> void forEach(F&& f) { for (auto& s : slots_) if (s.state == USED) f(s.key, s.val); }

private:
    enum : uint8_t { EMPTY = 0, USED = 1 };
    struct Slot { K key; V val; uint8_t state; };
    static constexpr size_t npos = size_t(-1);

    void init(size_t expected) {
        cap_ = 16; while (cap_ < expected * 2) cap_ <<= 1;
        mask_ = cap_ - 1; slots_.assign(cap_, Slot{}); used_ = 0;
    }
    size_t probe(const K& k) const noexcept {
        size_t i = hashKey(k) & mask_;
        for (;;) {
            const Slot& s = slots_[i];
            if (s.state == EMPTY) return npos;
            if (s.state == USED && s.key == k) return i;
            i = (i + 1) & mask_;
        }
    }
    void rehash(size_t newCap) {
        std::vector<Slot> old; old.swap(slots_);
        cap_ = newCap; mask_ = cap_ - 1; slots_.assign(cap_, Slot{}); used_ = 0;
        for (auto& s : old) if (s.state == USED) insert(s.key, s.val);
    }
    size_t cap_ = 0, mask_ = 0, used_ = 0;
    std::vector<Slot> slots_;
};

// Set of 64-bit fingerprints, 8 bytes per slot, 0 reserved as empty. For day-long membership
// (every clOrdId seen) where the table must be large and each probe is one cache line.
class FlatSet64 {
public:
    explicit FlatSet64(size_t expected = 1024) { cap_ = 16; while (cap_ < expected * 2) cap_ <<= 1; mask_ = cap_ - 1; slots_.assign(cap_, 0); }
    static uint64_t fingerprint(const Key128& k) noexcept { uint64_t f = hashKey(k); return f ? f : 1; }
    bool contains(uint64_t fp) const noexcept {
        for (size_t i = fp & mask_;; i = (i + 1) & mask_) { uint64_t v = slots_[i]; if (v == fp) return true; if (v == 0) return false; }
    }
    // Returns false if it was already present.
    bool insert(uint64_t fp) {
        if ((used_ + 1) * 4 > cap_ * 3) rehash(cap_ * 2);
        for (size_t i = fp & mask_;; i = (i + 1) & mask_) {
            uint64_t v = slots_[i]; if (v == fp) return false;
            if (v == 0) { slots_[i] = fp; ++used_; return true; }
        }
    }
    size_t size() const noexcept { return used_; }
private:
    void rehash(size_t newCap) {
        std::vector<uint64_t> old; old.swap(slots_);
        cap_ = newCap; mask_ = cap_ - 1; slots_.assign(cap_, 0); used_ = 0;
        for (uint64_t v : old) if (v) insert(v);
    }
    size_t cap_ = 0, mask_ = 0, used_ = 0;
    std::vector<uint64_t> slots_;
};

} // namespace trading::util
