// core/poskeeper/poskeeper.hpp : the position keeper's hot view.
//
// Positions by (account, symbol), folded from every client-level fill on the core stream and adjusted
// by every corporate action at the sequence number it takes effect (corpaction.hpp), and each
// account's gross and net exposure across them. At every Checkpoint it publishes a
// CorporateActionApplied for each position an action adjusted since the last one, a
// PositionSnapshot for each position that changed and an ExposureSnapshot for each account that
// did, stamped as of the checkpoint's sequence: what an engine starting mid-day loads instead of
// replaying the day. The same figures are kept in a shared segment under a seqlock per record, for
// the market maker and the margin engine, which read them without asking.
//
// Deterministic: it reacts only to sequenced frames, and it publishes in the order positions first
// changed, never in a hash table's order, so a replay emits the same frames byte for byte.
#pragma once
#include "corpaction.hpp"
#include "flatmap.hpp"
#include "trading.hpp"
#include "blake3.h"
#include <atomic>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace trading::pos {

// ---- the shared view -----------------------------------------------------------------------------
// Two open-addressing tables in one block of memory, one record per (account, symbol) and one per
// account. Records are claimed and never released during a day, so a reader's probe that reaches
// an empty slot has proved the key is absent. Every record has its own sequence number, odd while
// the writer is changing it.
struct PositionRecord {
    std::atomic<uint32_t> seq;
    uint32_t used;
    uint64_t key;                 // (account << 32) | symbol
    Position p;
};
struct AccountRecord {
    std::atomic<uint32_t> seq;
    uint32_t used;
    uint64_t account;
    money::i128 gross, net;       // 1e-8, across the account's positions at average cost
};
struct ViewHeader {
    char     magic[8];            // "POSVIEW\0"
    uint32_t formatVersion, positionSlots, accountSlots, pad;
    uint64_t reserved[5];
};
static_assert(sizeof(ViewHeader) == 64);

inline uint64_t positionKey(uint32_t account, uint32_t symbol) noexcept { return (uint64_t(account) << 32) | symbol; }
inline size_t slotFor(uint64_t k, uint32_t slots) noexcept { return size_t((k * 0x9E3779B97F4A7C15ull) >> 32) & (slots - 1); }

class PositionView {
public:
    static size_t bytesFor(uint32_t positionSlots, uint32_t accountSlots) noexcept {
        return sizeof(ViewHeader) + size_t(positionSlots) * sizeof(PositionRecord) + size_t(accountSlots) * sizeof(AccountRecord);
    }
    PositionView(std::byte* base, uint32_t positionSlots, uint32_t accountSlots, bool create) : base_(base) {
        auto* h = reinterpret_cast<ViewHeader*>(base_);
        if (create) {
            if ((positionSlots & (positionSlots - 1)) || (accountSlots & (accountSlots - 1))) throw std::runtime_error("position view: slot counts must be powers of two");
            std::memset(base_, 0, bytesFor(positionSlots, accountSlots));
            std::memcpy(h->magic, "POSVIEW", 8);
            h->formatVersion = 1; h->positionSlots = positionSlots; h->accountSlots = accountSlots;
        } else if (std::memcmp(h->magic, "POSVIEW", 7) != 0 || h->formatVersion != 1 || h->positionSlots != positionSlots || h->accountSlots != accountSlots) {
            throw std::runtime_error("position view: not a position view, or a different shape");
        }
        pslots_ = positionSlots; aslots_ = accountSlots;
    }

    // ---- reading: never blocks the writer, retries if the record moved under it
    bool position(uint32_t account, uint32_t symbol, Position& out) const noexcept {
        const uint64_t k = positionKey(account, symbol);
        for (size_t i = slotFor(k, pslots_), n = 0; n < pslots_; i = (i + 1) & (pslots_ - 1), ++n) {
            const PositionRecord& r = positions()[i];
            if (!r.used) return false;
            if (r.key != k) continue;
            return read(r.seq, &r.p, &out, sizeof out);
        }
        return false;
    }
    bool account(uint32_t account, money::i128& gross, money::i128& net) const noexcept {
        for (size_t i = slotFor(account, aslots_), n = 0; n < aslots_; i = (i + 1) & (aslots_ - 1), ++n) {
            const AccountRecord& r = accounts()[i];
            if (!r.used) return false;
            if (r.account != account) continue;
            money::i128 v[2];
            if (!read(r.seq, &r.gross, v, sizeof v)) return false;
            gross = v[0]; net = v[1];
            return true;
        }
        return false;
    }

    // ---- writing: one writer
    bool writePosition(uint64_t key, const Position& p) noexcept {
        PositionRecord* r = claim(positions(), pslots_, key, [](const PositionRecord& x) { return x.key; }, [&](PositionRecord& x) { x.key = key; });
        if (!r) return false;
        begin(r->seq); r->p = p; end(r->seq);
        return true;
    }
    bool writeAccount(uint64_t account, money::i128 gross, money::i128 net) noexcept {
        AccountRecord* r = claim(accounts(), aslots_, account, [](const AccountRecord& x) { return x.account; }, [&](AccountRecord& x) { x.account = account; });
        if (!r) return false;
        begin(r->seq); r->gross = gross; r->net = net; end(r->seq);
        return true;
    }

private:
    PositionRecord* positions() const noexcept { return reinterpret_cast<PositionRecord*>(base_ + sizeof(ViewHeader)); }
    AccountRecord* accounts() const noexcept { return reinterpret_cast<AccountRecord*>(base_ + sizeof(ViewHeader) + size_t(pslots_) * sizeof(PositionRecord)); }

    template <class R, class K, class S>
    static R* claim(R* table, uint32_t slots, uint64_t key, K keyOf, S setKey) noexcept {
        for (size_t i = slotFor(key, slots), n = 0; n < slots; i = (i + 1) & (slots - 1), ++n) {
            R& r = table[i];
            if (r.used && keyOf(r) == key) return &r;
            if (!r.used) {
                // The key goes in under the record's sequence and the slot is marked used last, so a
                // reader probing past it sees either an empty slot or a complete one.
                begin(r.seq); setKey(r); end(r.seq);
                std::atomic_thread_fence(std::memory_order_release);
                r.used = 1;
                return &r;
            }
        }
        return nullptr;                                   // full: the view stops tracking new keys
    }
    static void begin(std::atomic<uint32_t>& s) noexcept { s.store(s.load(std::memory_order_relaxed) + 1, std::memory_order_release); std::atomic_thread_fence(std::memory_order_release); }
    static void end(std::atomic<uint32_t>& s) noexcept { std::atomic_thread_fence(std::memory_order_release); s.store(s.load(std::memory_order_relaxed) + 1, std::memory_order_release); }
    static bool read(const std::atomic<uint32_t>& s, const void* from, void* to, size_t n, int spins = 64) noexcept {
        for (int i = 0; i < spins; ++i) {
            const uint32_t s1 = s.load(std::memory_order_acquire);
            if (s1 & 1u) continue;
            std::memcpy(to, from, n);
            std::atomic_thread_fence(std::memory_order_acquire);
            if (s.load(std::memory_order_relaxed) == s1) return true;
        }
        return false;
    }

    std::byte* base_;
    uint32_t pslots_ = 0, aslots_ = 0;
};

// ---- the keeper ----------------------------------------------------------------------------------
class PositionKeeper {
public:
    struct Params { uint16_t sourceId = 6; uint32_t positionSlots = 1u << 16; uint32_t accountSlots = 1u << 12; };

    // viewBase is a block of PositionView::bytesFor(positionSlots, accountSlots) bytes, shared or not.
    PositionKeeper(Params p, std::byte* viewBase)
        : p_(p), view_(viewBase, p.positionSlots, p.accountSlots, true), index_(p.positionSlots), accountIndex_(p.accountSlots) {
        entries_.reserve(p.positionSlots); accounts_.reserve(p.accountSlots);
        dirtyPos_.reserve(p.positionSlots); dirtyAcct_.reserve(p.accountSlots);
    }

    // Every core-stream frame, in sequence order. Emits only at a Checkpoint.
    template <class E>
    void apply(const FrameHeader* f, E&& emit) {
        // What the frame is, read before an action can run: GCC cannot otherwise see that adjusting
        // positions leaves the frame alone, and warns about reading a Checkpoint as an ExecReport.
        const auto* e = as<ExecReport>(f);
        const bool checkpoint = as<Checkpoint>(f) != nullptr;
        actions_.onFrame(f, [this](const CorporateActionRecord& r) { onAction(r); });
        if (e) { if (e->childOrderId == 0) onExec(*e); return; }
        if (checkpoint) publish(f->seq, emit);
    }

    const Position* find(uint32_t account, uint32_t symbol) const noexcept {
        const uint32_t* i = index_.find(positionKey(account, symbol));
        return i ? &entries_[*i].p : nullptr;
    }
    bool exposure(uint32_t account, money::i128& gross, money::i128& net) const noexcept {
        const uint32_t* i = accountIndex_.find(account);
        if (!i) return false;
        gross = accounts_[*i].gross; net = accounts_[*i].net;
        return true;
    }
    const PositionView& view() const noexcept { return view_; }

    // Positions in the order they first appeared, which both a live run and its replay share.
    void hashInto(blake3_hasher& h) const {
        for (const Entry& e : entries_) { blake3_hasher_update(&h, &e.key, 8); blake3_hasher_update(&h, &e.p, sizeof e.p); }
        for (const Acct& a : accounts_) { blake3_hasher_update(&h, &a.account, 4); blake3_hasher_update(&h, &a.gross, 16); blake3_hasher_update(&h, &a.net, 16); }
    }

    uint64_t fills() const noexcept { return fills_; }
    uint64_t busts() const noexcept { return busts_; }
    uint64_t positionSnapshots() const noexcept { return positionSnapshots_; }
    uint64_t exposureSnapshots() const noexcept { return exposureSnapshots_; }
    uint64_t viewFull() const noexcept { return viewFull_; }
    const ActionSchedule& actions() const noexcept { return actions_; }
    size_t positions() const noexcept { return entries_.size(); }

private:
    struct Entry { uint64_t key; Position p; bool dirty; };
    struct Acct { uint32_t account; money::i128 gross, net; bool dirty; };

    void onExec(const ExecReport& e) {
        int64_t q;
        if (e.execType == ExecType::PartialFill || e.execType == ExecType::Fill) { q = signedQty(e.side, e.lastQty); ++fills_; }
        else if (e.execType == ExecType::TradeBust) { q = -signedQty(e.side, e.lastQty); ++busts_; }
        else return;
        if (q == 0) return;                                // a Fill that only closes the order, carrying no shares
        const uint32_t i = entry(positionKey(e.accountIdx, e.symbolIdx));
        Position p = entries_[i].p;
        fill(p, q, e.lastPx);
        set(i, p);
    }

    // Every position held in the action's symbol, in the order positions first appeared.
    void onAction(const CorporateActionRecord& r) {
        const Effect fx = effectOf(r);
        for (uint32_t i = 0, n = uint32_t(entries_.size()); i < n; ++i) {
            const Position before = entries_[i].p;
            if (uint32_t(entries_[i].key) != r.symbolIdx || before.qty == 0) continue;
            const uint32_t acct = uint32_t(entries_[i].key >> 32);
            CorporateActionApplied rec{}; rec.actionId = r.actionId; rec.accountIdx = acct; rec.symbolIdx = r.symbolIdx; rec.qtyBefore = before.qty;
            if (fx == Effect::Move) {                      // the quantity carried to newSymbolIdx
                set(i, Position{});
                const uint32_t j = entry(positionKey(acct, r.newSymbolIdx));
                Position to = entries_[j].p;
                merge(to, before);
                set(j, to);
                rec.qtyAfter = before.qty;
            } else {
                Position p = before;
                rec.cashAmount = adjust(p, r);
                set(i, p);
                rec.qtyAfter = p.qty;
            }
            applied_.push_back(rec);
        }
    }

    uint32_t entry(uint64_t k) {
        if (const uint32_t* ip = index_.find(k)) return *ip;
        const uint32_t i = uint32_t(entries_.size());
        entries_.push_back({k, Position{}, false}); index_.insert(k, i);
        return i;
    }
    // A position's new figures, into the view and its account's totals, and marked for the checkpoint.
    void set(uint32_t i, const Position& next) {
        Entry& en = entries_[i];
        const money::i128 before = pos::exposure(en.p), after = pos::exposure(next);
        en.p = next;
        if (!en.dirty) { en.dirty = true; dirtyPos_.push_back(i); }
        if (!view_.writePosition(en.key, en.p)) ++viewFull_;

        const uint32_t account = uint32_t(en.key >> 32);
        const uint32_t* ia = accountIndex_.find(account);
        uint32_t ai;
        if (ia) ai = *ia;
        else { ai = uint32_t(accounts_.size()); accounts_.push_back({account, 0, 0, false}); accountIndex_.insert(account, ai); }
        Acct& a = accounts_[ai];
        a.gross += (after < 0 ? -after : after) - (before < 0 ? -before : before);
        a.net += after - before;
        if (!a.dirty) { a.dirty = true; dirtyAcct_.push_back(ai); }
        if (!view_.writeAccount(a.account, a.gross, a.net)) ++viewFull_;
    }

    template <class E>
    void publish(uint64_t asOfSeq, E& emit) {
        for (const CorporateActionApplied& rec : applied_) {
            Frame<CorporateActionApplied> c; c.init(); c.header.sourceId = p_.sourceId;
            c.body = rec;
            emit(&c.header);
        }
        applied_.clear();
        for (uint32_t i : dirtyPos_) {
            Entry& en = entries_[i];
            Frame<PositionSnapshot> s; s.init(); s.header.sourceId = p_.sourceId;
            s.body.accountIdx = uint32_t(en.key >> 32); s.body.symbolIdx = uint32_t(en.key);
            s.body.qty = en.p.qty; s.body.avgCost = en.p.avgCost; s.body.realizedPnl = en.p.realized;
            s.body.asOfSeq = asOfSeq;                      // marks and unrealised P&L are the ledger's (PK-3)
            en.dirty = false; ++positionSnapshots_;
            emit(&s.header);
        }
        for (uint32_t i : dirtyAcct_) {
            Acct& a = accounts_[i];
            Frame<ExposureSnapshot> x; x.init(); x.header.sourceId = p_.sourceId;
            x.body.accountIdx = a.account;
            // One unit for both figures: the finer one if both fit it, otherwise the coarser.
            MoneyScale sg = MoneyScale::Unit1e8, sn = MoneyScale::Unit1e8;
            money::toWire(a.gross, sg); money::toWire(a.net, sn);
            const bool coarse = sg == MoneyScale::Unit1e4 || sn == MoneyScale::Unit1e4;
            x.body.moneyScale = coarse ? MoneyScale::Unit1e4 : MoneyScale::Unit1e8;
            x.body.grossExposure = int64_t(coarse ? a.gross / money::PER_1E4 : a.gross);
            x.body.netExposure = int64_t(coarse ? a.net / money::PER_1E4 : a.net);
            a.dirty = false; ++exposureSnapshots_;
            emit(&x.header);
        }
        dirtyPos_.clear(); dirtyAcct_.clear();
    }

    Params p_;
    PositionView view_;
    std::vector<Entry> entries_;
    std::vector<Acct> accounts_;
    util::FlatMap<uint64_t, uint32_t> index_;
    util::FlatMap<uint64_t, uint32_t> accountIndex_;
    std::vector<uint32_t> dirtyPos_, dirtyAcct_;
    ActionSchedule actions_;
    std::vector<CorporateActionApplied> applied_;
    uint64_t fills_ = 0, busts_ = 0, positionSnapshots_ = 0, exposureSnapshots_ = 0, viewFull_ = 0;
};

} // namespace trading::pos
