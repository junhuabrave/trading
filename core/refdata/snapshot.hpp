// core/refdata/snapshot.hpp
// Memory-mapped, hash-verified reference-data snapshot. Read-only; engines copy
// the fields they need into their own arrays (see refdata.hpp) and never write here.
#pragma once
#include "trading.hpp"
#include "blake3.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <span>
#include <stdexcept>

namespace trading::refdata {

enum class Section : uint16_t {
    Instruments = 1, Listings = 2, Venues = 3, TickTables = 4, Calendars = 5,
    CorporateActions = 6, Lists = 7, Fees = 8, Accounts = 9, Components = 10, TickerIndex = 11, Feeds = 12,
};

__extension__ typedef unsigned __int128 u128;   // bounds arithmetic that cannot wrap
inline constexpr size_t PAGE = 4096;
inline constexpr size_t MAX_SYMBOLS = 65536;
inline constexpr size_t LIST_BYTES = MAX_SYMBOLS / 8;

class Snapshot {
public:
    // A constructor that throws does not run the destructor, so the mapping and descriptor are
    // released explicitly on every rejection path (a rejected snapshot must not leak).
    explicit Snapshot(const std::string& path) {
        try {
            fd_ = ::open(path.c_str(), O_RDONLY);
            if (fd_ < 0) throw std::runtime_error("snapshot: cannot open " + path);
            struct stat st{};
            if (::fstat(fd_, &st) != 0) throw std::runtime_error("snapshot: fstat failed");
            size_ = size_t(st.st_size);
            if (size_ < PAGE) throw std::runtime_error("snapshot: file smaller than header");
            base_ = static_cast<const std::byte*>(::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0));
            if (base_ == MAP_FAILED) { base_ = nullptr; throw std::runtime_error("snapshot: mmap failed"); }
            hdr_ = reinterpret_cast<const SnapshotHeader*>(base_);
            verify();
        } catch (...) { release(); throw; }
    }
    ~Snapshot() { release(); }
    Snapshot(const Snapshot&) = delete;
    Snapshot& operator=(const Snapshot&) = delete;

    const SnapshotHeader& header() const noexcept { return *hdr_; }
    uint32_t businessDate() const noexcept { return hdr_->businessDate; }
    uint32_t version() const noexcept { return hdr_->snapshotVersion; }
    std::span<const uint8_t, 32> contentHash() const noexcept {
        return std::span<const uint8_t, 32>(hdr_->contentHash.data(), 32);
    }

    const SectionEntry* section(Section s) const noexcept {
        const auto* entries = reinterpret_cast<const SectionEntry*>(hdr_->sections.data());
        for (uint16_t i = 0; i < hdr_->sectionCount; ++i)
            if (entries[i].sectionType == uint16_t(s)) return &entries[i];
        return nullptr;
    }
    template <class R> std::span<const R> records(Section s) const {
        const SectionEntry* e = section(s);
        if (!e) return {};
        if (e->recordSize != sizeof(R)) throw std::runtime_error("snapshot: record size mismatch");
        if (e->recordCount > (size_ - e->offset) / sizeof(R)) throw std::runtime_error("snapshot: section beyond EOF");
        return std::span<const R>(reinterpret_cast<const R*>(base_ + e->offset), e->recordCount);
    }
    std::span<const SymbolRecord> instruments() const { return records<SymbolRecord>(Section::Instruments); }
    std::span<const ListingRecord> listings() const { return records<ListingRecord>(Section::Listings); }
    std::span<const VenueRecord> venues() const { return records<VenueRecord>(Section::Venues); }
    std::span<const TickTableRecord> tickTables() const { return records<TickTableRecord>(Section::TickTables); }
    std::span<const CorporateActionRecord> corporateActions() const { return records<CorporateActionRecord>(Section::CorporateActions); }
    std::span<const FeeRecord> fees() const { return records<FeeRecord>(Section::Fees); }
    std::span<const AccountRecord> accounts() const { return records<AccountRecord>(Section::Accounts); }
    std::span<const TickerIndexEntry> tickerIndex() const { return records<TickerIndexEntry>(Section::TickerIndex); }
    std::span<const FeedRecord> feeds() const { return records<FeedRecord>(Section::Feeds); }

    // Bitset for a list (1-based ListId). Returns LIST_BYTES bytes.
    std::span<const uint8_t> list(ListId id) const {
        const SectionEntry* e = section(Section::Lists);
        if (!e || uint32_t(id) == 0 || uint32_t(id) > e->recordCount) return {};
        return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(base_ + e->offset) + (uint32_t(id) - 1) * LIST_BYTES, LIST_BYTES);
    }
    bool inList(ListId id, uint32_t symbolIdx) const {
        auto bs = list(id);
        return symbolIdx < MAX_SYMBOLS && !bs.empty() && (bs[symbolIdx >> 3] & (1u << (symbolIdx & 7)));
    }

    // Ticker lookup: binary search over the sorted ticker index. Edge use only.
    uint32_t lookupTicker(std::string_view ticker) const {
        auto idx = tickerIndex();
        size_t lo = 0, hi = idx.size();
        while (lo < hi) {
            size_t mid = (lo + hi) / 2;
            std::string_view t(idx[mid].ticker, ::strnlen(idx[mid].ticker, 16));
            if (t < ticker) lo = mid + 1; else hi = mid;
        }
        if (lo < idx.size() && std::string_view(idx[lo].ticker, ::strnlen(idx[lo].ticker, 16)) == ticker) return idx[lo].symbolIdx;
        return 0;
    }

private:
    void release() noexcept {
        if (base_) ::munmap(const_cast<std::byte*>(base_), size_);
        if (fd_ >= 0) ::close(fd_);
        base_ = nullptr; fd_ = -1;
    }
    void verify() const {
        if (std::memcmp(hdr_->magic, "RFDS", 4) != 0) throw std::runtime_error("snapshot: bad magic");
        if (hdr_->formatVersion != 1) throw std::runtime_error("snapshot: unsupported format version");
        if (hdr_->schemaVersion > SCHEMA_VERSION) throw std::runtime_error("snapshot: schema newer than codec");
        if (hdr_->fileSize != size_) throw std::runtime_error("snapshot: size mismatch");
        if (hdr_->sectionCount > 32) throw std::runtime_error("snapshot: too many sections");
        const auto* entries = reinterpret_cast<const SectionEntry*>(hdr_->sections.data());
        blake3_hasher content;
        blake3_hasher_init(&content);
        for (uint16_t i = 0; i < hdr_->sectionCount; ++i) {
            const SectionEntry& e = entries[i];
            if (e.offset % PAGE != 0) throw std::runtime_error("snapshot: section not page aligned");
            if (e.offset > size_) throw std::runtime_error("snapshot: section beyond EOF");
            // bounds in wide arithmetic: a corrupt header's recordSize x recordCount can wrap size_t
            u128 wide = u128(e.recordSize) * e.recordCount;
            if (wide > size_ - e.offset) throw std::runtime_error("snapshot: section beyond EOF");
            size_t len = size_t(wide);
            uint8_t h[32];
            blake3_hasher sh; blake3_hasher_init(&sh);
            blake3_hasher_update(&sh, base_ + e.offset, len);
            blake3_hasher_finalize(&sh, h, 32);
            if (std::memcmp(h, e.sectionHash.data(), 32) != 0) throw std::runtime_error("snapshot: section hash mismatch");
            blake3_hasher_update(&content, base_ + e.offset, len);
        }
        uint8_t h[32];
        blake3_hasher_finalize(&content, h, 32);
        if (std::memcmp(h, hdr_->contentHash.data(), 32) != 0) throw std::runtime_error("snapshot: content hash mismatch");
    }

    int fd_ = -1;
    size_t size_ = 0;
    const std::byte* base_ = nullptr;
    const SnapshotHeader* hdr_ = nullptr;
};

} // namespace trading::refdata
