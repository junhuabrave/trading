// core/poskeeper/corpaction.hpp : corporate actions, and the sequence number at which they act.
//
// A CorporateAction on the core stream takes effect at max(its own sequence number, effectiveSeq):
// immediately when effectiveSeq is zero or already past, otherwise at the first core frame at or
// beyond it, before that frame is handled. Only a record in the Applied state acts - one that is
// not yet applied is an announcement - and an action acts once, however often it is re-sent.
//
// The position keeper and the risk engine each hold an ActionSchedule and adjust through adjust()
// below. They see the same frames, so they adjust at the same sequence number with the same
// arithmetic, and the keeper's next snapshot of each adjusted position equals the risk engine's.
//
// What an action does to a position that is not flat:
//   split, reverse split   quantity times ratioNum/ratioDen (new shares per old), truncated toward
//                          zero; the cost per share scales the other way. A fraction of a share
//                          leaves at cost: the cash paid in lieu of it is the ledger's to book.
//   cash dividend          realised gains qty x cashAmount per share: a long receives, a short owes
//   symbol change          nothing while the index stays the same, which is what the security
//                          master does; to a different newSymbolIdx the position moves, merging
//                          with one already held there
//   delist                 nothing: the shares are still held, and status is reference data
// Merger, stock dividend, spin-off, and a ratio that is not positive are counted and not applied.
#pragma once
#include "position.hpp"
#include "flatmap.hpp"
#include <algorithm>
#include <vector>

namespace trading::pos {

enum class Effect : uint8_t { None, Ratio, Cash, Move, Unapplied };

inline Effect effectOf(const CorporateActionRecord& r) noexcept {
    switch (CorpActionType(r.actionType)) {
        case CorpActionType::Split: case CorpActionType::ReverseSplit: return r.ratioNum > 0 && r.ratioDen > 0 ? Effect::Ratio : Effect::Unapplied;
        case CorpActionType::CashDividend: return Effect::Cash;
        case CorpActionType::SymbolChange: return r.newSymbolIdx != 0 && r.newSymbolIdx != r.symbolIdx ? Effect::Move : Effect::None;
        case CorpActionType::Delist: return Effect::None;
        default: return Effect::Unapplied;
    }
}

// A Ratio or Cash action on one position of its symbol. Returns the cash it moved (1e-8).
inline int64_t adjust(Position& p, const CorporateActionRecord& r) noexcept {
    if (effectOf(r) == Effect::Cash) {
        const int64_t cash = int64_t(money::i128(p.qty) * r.cashAmount);
        p.realized += cash;
        return cash;
    }
    p.qty = int64_t(money::i128(p.qty) * r.ratioNum / r.ratioDen);
    p.avgCost = p.qty == 0 ? 0 : int64_t(money::i128(p.avgCost) * r.ratioDen / r.ratioNum);
    return 0;
}

// A position moved onto another, which may already hold one: added at its own average cost.
inline void merge(Position& into, const Position& from) noexcept {
    fill(into, from.qty, from.avgCost);
    into.realized += from.realized;
}

class ActionSchedule {
public:
    // Every frame the consumer reads, before it handles it. Calls act(record) for each action that
    // takes effect at this frame. Only core-stream frames move it: market data counts its own seqs.
    template <class F>
    void onFrame(const FrameHeader* f, F&& act) {
        if (f->streamId != 0) return;
        if (const auto* m = as<CorporateAction>(f)) admit(m->record, std::max(f->seq, m->record.effectiveSeq));
        while (!pending_.empty() && pending_.front().at <= f->seq) {
            const CorporateActionRecord r = pending_.front().r;
            pending_.erase(pending_.begin());
            const Effect e = effectOf(r);
            if (e == Effect::Unapplied) { ++unapplied_; continue; }
            ++taken_;
            if (e != Effect::None) act(r);
        }
    }

    // For a consumer that starts from the keeper's snapshots as of asOfSeq: each CorporateAction
    // sequenced at or before it. One already in effect is in those snapshots; a later one waits.
    void restore(const FrameHeader* f, uint64_t asOfSeq) {
        const auto* m = as<CorporateAction>(f);
        if (!m || f->streamId != 0 || f->seq > asOfSeq) return;
        const uint64_t at = std::max(f->seq, m->record.effectiveSeq);
        if (at <= asOfSeq) { if (m->record.applied) known_.insert(m->record.actionId); }
        else admit(m->record, at);
    }

    uint64_t taken() const noexcept { return taken_; }           // in effect, delists and same-index renames included
    uint64_t unapplied() const noexcept { return unapplied_; }   // a kind this does not apply, or a bad ratio
    size_t pending() const noexcept { return pending_.size(); }

private:
    struct Pending { uint64_t at; CorporateActionRecord r; };
    void admit(const CorporateActionRecord& r, uint64_t at) {
        if (!r.applied || !known_.insert(r.actionId)) return;   // id 0 is unset and reads as already known
        // in order of effect, and in order of arrival among those due at the same frame
        auto it = std::upper_bound(pending_.begin(), pending_.end(), at, [](uint64_t a, const Pending& p) { return a < p.at; });
        pending_.insert(it, Pending{at, r});
    }
    std::vector<Pending> pending_;
    util::FlatSet64 known_{256};
    uint64_t taken_ = 0, unapplied_ = 0;
};

} // namespace trading::pos
