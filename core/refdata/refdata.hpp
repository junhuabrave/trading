// core/refdata/refdata.hpp
// The engine-side view of reference data. Preallocated struct-of-arrays sized
// for MAX_SYMBOLS, filled from a Snapshot at start of day, then mutated only
// by sequenced messages applied in log order. No allocation after construction.
#pragma once
#include "snapshot.hpp"
#include "blake3.h"
#include <algorithm>
#include <vector>
#include <array>

namespace trading::refdata {

struct TickTable {
    uint8_t bandCount = 0;
    std::array<int64_t, 32> bands{};   // (floor, tick) pairs
    int64_t tickFor(int64_t price) const noexcept {
        int64_t tick = 0;
        for (uint8_t i = 0; i < bandCount; ++i)
            if (price >= bands[2 * i]) tick = bands[2 * i + 1];
        return tick;
    }
};

class RefData {
public:
    RefData()
        : status_(MAX_SYMBOLS, 0), instrType_(MAX_SYMBOLS, 0), tickTable_(MAX_SYMBOLS, 0),
          lotSize_(MAX_SYMBOLS, 0), flags_(MAX_SYMBOLS, 0), marginClass_(MAX_SYMBOLS, 0),
          refPrice_(MAX_SYMBOLS, 0), adv30_(MAX_SYMBOLS, 0), sharesOut_(MAX_SYMBOLS, 0),
          listingVenue_(MAX_SYMBOLS, 0), ssrActive_(MAX_SYMBOLS, 0), ssrUntil_(MAX_SYMBOLS, 0),
          lists_(5 * LIST_BYTES, 0), tickTables_(256) {}

    // Start of day: copy the hot fields out of the mapped snapshot.
    void load(const Snapshot& snap) {
        std::fill(status_.begin(), status_.end(), 0);
        for (const SymbolRecord& r : snap.instruments()) {
            uint32_t i = r.symbolIdx;
            if (r.status == 0 || i >= MAX_SYMBOLS) continue;
            status_[i] = r.status; instrType_[i] = r.instrumentType; tickTable_[i] = r.tickTableId;
            lotSize_[i] = r.lotSize; flags_[i] = r.flags; marginClass_[i] = r.marginClass;
            refPrice_[i] = r.refPrice; adv30_[i] = r.adv30; sharesOut_[i] = r.sharesOutstanding;
            listingVenue_[i] = r.listingVenue;
            ssrActive_[i] = (r.flags & SymbolFlags::ssrActive) ? 1 : 0;
            maxIdx_ = std::max(maxIdx_, i);
        }
        for (uint8_t lid = 1; lid <= 5; ++lid) {
            auto bs = snap.list(ListId(lid));
            if (!bs.empty()) std::memcpy(lists_.data() + (lid - 1) * LIST_BYTES, bs.data(), LIST_BYTES);
        }
        for (const TickTableRecord& tt : snap.tickTables()) {
            if (tt.tickTableId >= tickTables_.size()) continue;
            tickTables_[tt.tickTableId].bandCount = tt.bandCount;
            tickTables_[tt.tickTableId].bands = tt.bands;
        }
        std::memcpy(snapshotHash_.data(), snap.contentHash().data(), 32);
        businessDate_ = snap.businessDate();
    }

    // Sequenced updates. Returns false if the message is not a reference message.
    bool apply(const FrameHeader* h) noexcept {
        if (auto* m = as<SymbolAdd>(h)) {
            const SymbolRecord& r = m->record;
            if (r.symbolIdx >= MAX_SYMBOLS) return true;
            uint32_t i = r.symbolIdx;
            status_[i] = r.status; instrType_[i] = r.instrumentType; tickTable_[i] = r.tickTableId;
            lotSize_[i] = r.lotSize; flags_[i] = r.flags; marginClass_[i] = r.marginClass;
            refPrice_[i] = r.refPrice; adv30_[i] = r.adv30; sharesOut_[i] = r.sharesOutstanding;
            listingVenue_[i] = r.listingVenue; ssrActive_[i] = (r.flags & SymbolFlags::ssrActive) ? 1 : 0;
            maxIdx_ = std::max(maxIdx_, i);
            return true;
        }
        if (auto* m = as<SymbolUpdate>(h)) {
            uint32_t i = m->symbolIdx;
            if (i >= MAX_SYMBOLS) return true;
            using M = SymbolUpdateMask::type;
            M k = m->mask;
            if (k & SymbolUpdateMask::status) status_[i] = uint8_t(m->status);
            if (k & SymbolUpdateMask::tickTableId) tickTable_[i] = m->tickTableId;
            if (k & SymbolUpdateMask::lotSize) lotSize_[i] = m->lotSize;
            if (k & SymbolUpdateMask::flags) { flags_[i] = m->flags; ssrActive_[i] = (m->flags & SymbolFlags::ssrActive) ? 1 : 0; }
            if (k & SymbolUpdateMask::refPrice) refPrice_[i] = m->refPrice;
            if (k & SymbolUpdateMask::adv30) adv30_[i] = m->adv30;
            if (k & SymbolUpdateMask::marginClass) marginClass_[i] = m->marginClass;
            if (k & SymbolUpdateMask::sharesOutstanding) sharesOut_[i] = m->sharesOutstanding;
            return true;
        }
        if (auto* m = as<ListUpdate>(h)) {
            uint32_t lid = uint32_t(m->listId), i = m->symbolIdx;
            if (lid == 0 || lid > 5 || i >= MAX_SYMBOLS) return true;
            uint8_t& byte = lists_[(lid - 1) * LIST_BYTES + (i >> 3)];
            uint8_t bit = uint8_t(1u << (i & 7));
            if (m->op == ListOp::Add) byte |= bit; else byte &= uint8_t(~bit);
            // keep the cached flag bits coherent with the list bitsets
            uint32_t fb = lid == 1 ? SymbolFlags::etb : lid == 2 ? SymbolFlags::hardToBorrow
                        : lid == 3 ? SymbolFlags::restricted : lid == 4 ? SymbolFlags::threshold : 0;
            if (fb) { if (m->op == ListOp::Add) flags_[i] |= fb; else flags_[i] &= ~fb; }
            return true;
        }
        if (auto* m = as<ShortSaleRestriction>(h)) {
            uint32_t i = m->symbolIdx;
            if (i >= MAX_SYMBOLS) return true;
            ssrActive_[i] = m->active; ssrUntil_[i] = m->effectiveUntil;
            if (m->active) flags_[i] |= SymbolFlags::ssrActive; else flags_[i] &= ~SymbolFlags::ssrActive;
            return true;
        }
        if (auto* m = as<TickTableUpdate>(h)) {
            if (m->record.tickTableId < tickTables_.size()) {
                tickTables_[m->record.tickTableId].bandCount = m->record.bandCount;
                tickTables_[m->record.tickTableId].bands = m->record.bands;
            }
            return true;
        }
        if (h->templateId == uint16_t(TemplateId::CorporateAction) || h->templateId == uint16_t(TemplateId::CalendarUpdate)
            || h->templateId == uint16_t(TemplateId::FeeScheduleUpdate)) {
            return true;   // consumed by the position keeper / router; nothing in the hot symbol arrays changes
        }
        return false;
    }

    // Hot accessors (all O(1), array index).
    bool tradable(uint32_t i) const noexcept { return i < MAX_SYMBOLS && status_[i] == uint8_t(SymbolStatusCode::Active); }
    bool shortable(uint32_t i) const noexcept { return (flags_[i] & SymbolFlags::shortable) != 0; }
    bool easyToBorrow(uint32_t i) const noexcept { return inList(ListId::EasyToBorrow, i); }
    bool restricted(uint32_t i) const noexcept { return inList(ListId::Restricted, i); }
    bool ssrActive(uint32_t i) const noexcept { return ssrActive_[i] != 0; }
    int64_t refPrice(uint32_t i) const noexcept { return refPrice_[i]; }
    uint32_t lotSize(uint32_t i) const noexcept { return lotSize_[i]; }
    uint32_t flags(uint32_t i) const noexcept { return flags_[i]; }
    uint8_t status(uint32_t i) const noexcept { return status_[i]; }
    uint8_t marginClass(uint32_t i) const noexcept { return marginClass_[i]; }
    uint16_t tickTableId(uint32_t i) const noexcept { return tickTable_[i]; }
    int64_t tick(uint32_t i, int64_t price) const noexcept { return tickTables_[tickTable_[i]].tickFor(price); }
    bool inList(ListId id, uint32_t i) const noexcept {
        uint32_t lid = uint32_t(id);
        return lid >= 1 && lid <= 5 && i < MAX_SYMBOLS && (lists_[(lid - 1) * LIST_BYTES + (i >> 3)] & (1u << (i & 7)));
    }
    uint32_t maxSymbolIdx() const noexcept { return maxIdx_; }
    uint32_t businessDate() const noexcept { return businessDate_; }
    const std::array<uint8_t, 32>& snapshotHash() const noexcept { return snapshotHash_; }

    // Deterministic hash of everything the hot path reads. Two engines that applied the same
    // log must agree; this is what Checkpoint.stateHash carries for the reference component.
    std::array<uint8_t, 32> stateHash() const noexcept {
        blake3_hasher hs; blake3_hasher_init(&hs);
        size_t n = size_t(maxIdx_) + 1;
        auto upd = [&](const auto& v, size_t count) { blake3_hasher_update(&hs, v.data(), count * sizeof(v[0])); };
        upd(status_, n); upd(instrType_, n); upd(tickTable_, n); upd(lotSize_, n); upd(flags_, n);
        upd(marginClass_, n); upd(refPrice_, n); upd(adv30_, n); upd(sharesOut_, n); upd(listingVenue_, n);
        upd(ssrActive_, n); upd(lists_, lists_.size());
        for (const auto& tt : tickTables_) { blake3_hasher_update(&hs, &tt.bandCount, 1); blake3_hasher_update(&hs, tt.bands.data(), sizeof(tt.bands)); }
        std::array<uint8_t, 32> out{}; blake3_hasher_finalize(&hs, out.data(), 32); return out;
    }

private:
    std::vector<uint8_t>  status_, instrType_;
    std::vector<uint16_t> tickTable_;
    std::vector<uint32_t> lotSize_, flags_;
    std::vector<uint8_t>  marginClass_;
    std::vector<int64_t>  refPrice_, adv30_, sharesOut_;
    std::vector<uint16_t> listingVenue_;
    std::vector<uint8_t>  ssrActive_;
    std::vector<int64_t>  ssrUntil_;
    std::vector<uint8_t>  lists_;
    std::vector<TickTable> tickTables_;
    std::array<uint8_t, 32> snapshotHash_{};
    uint32_t maxIdx_ = 0;
    uint32_t businessDate_ = 0;
};

} // namespace trading::refdata
