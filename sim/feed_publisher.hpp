// sim/feed_publisher.hpp : publishes one feed's canonical message stream onto its lines, the way a
// venue does, and misbehaves on purpose.
//
// A venue sends the same messages down two independent multicast lines, A and B, and expects you to
// take whichever arrives first and to notice when both are missing something. It also runs a
// retransmit service for the gaps and a snapshot service for joining late. Everything the line
// arbitrator (MD-3) and the source selector (MD-7) have to survive is generated here: drops on one
// line, drops on both, duplicates, reordering, and the same feed published twice by different
// processes.
//
// The publisher is fed a canonical stream rather than owning one, because that is what makes the
// redundancy real: two publishers handed the same messages must produce byte-identical packets, and
// a test can check that rather than assume it.
#pragma once
#include "packet.hpp"
#include <functional>
#include <random>
#include <vector>
#include <deque>
#include <optional>
#include <cstring>

namespace trading::sim {

using namespace trading::md;

class FeedPublisher {
public:
    // Per line, as probabilities in [0,1]. A real A/B pair loses different packets on each line,
    // which is the entire reason there are two.
    struct Impairment { double dropPct = 0.0, dupPct = 0.0, reorderPct = 0.0; int64_t delayNs = 0; };
    struct Params {
        uint32_t feedId = 1;
        uint16_t lineCount = 2;              // A and B
        uint16_t retransmitLine = 100;       // the recovery service answers on its own line
        uint16_t maxMsgsPerPacket = 4;
        size_t   retransmitWindow = 4096;    // how far back the venue will re-send
    };
    // Where a packet goes: in the simulator, straight into a LineReceiver.
    using Wire = std::function<void(const void* bytes, size_t len, uint16_t lineId, int64_t recvTs)>;

    FeedPublisher(Params p, uint64_t seed) : p_(p), rng_(seed), impair_(p.lineCount), held_(p.lineCount) {}

    void impair(uint16_t lineId, Impairment i) { if (lineId < impair_.size()) impair_[lineId] = i; }

    // Accumulate one canonical message. Packets close at maxMsgsPerPacket or when flush() is called,
    // exactly as a venue batches whatever is ready when the wire is free.
    void publish(const FrameHeader* f, int64_t now, const Wire& wire) {
        if (building_ == 0) { builder_.begin(p_.feedId, 0, md::identityVenueSeq(f), now); }
        if (!builder_.add(f)) { send(now, wire); builder_.begin(p_.feedId, 0, md::identityVenueSeq(f), now); builder_.add(f); }
        ++building_;
        if (building_ >= p_.maxMsgsPerPacket) send(now, wire);
    }
    void flush(int64_t now, const Wire& wire) { if (building_) send(now, wire); }

    // The venue's recovery service: re-send the packets covering [fromSeq, toSeq] on its own line,
    // flagged so a consumer knows this is not live traffic.
    uint32_t retransmit(uint64_t fromSeq, uint64_t toSeq, int64_t now, const Wire& wire) {
        uint32_t sent = 0;
        for (auto& st : store_) {
            const auto* h = reinterpret_cast<const PacketHeader*>(st.data());
            if (nextSeqAfter(*h) <= fromSeq || h->firstSeq > toSeq) continue;
            std::vector<std::byte> copy = st;
            auto* c = reinterpret_cast<PacketHeader*>(copy.data());
            c->flags = uint16_t(c->flags | PacketFlags::retransmit); c->lineId = p_.retransmitLine;
            wire(copy.data(), copy.size(), p_.retransmitLine, now); ++sent; ++retransmitted_;
        }
        return sent;
    }
    // The snapshot service, for a consumer that joined too late for retransmit to help.
    void snapshot(const std::vector<const FrameHeader*>& frames, uint64_t asOfSeq, int64_t now, const Wire& wire) {
        alignas(16) std::byte buf[MAX_PACKET];
        PacketBuilder b(buf);
        b.begin(p_.feedId, p_.retransmitLine, asOfSeq, now, PacketFlags::snapshot);
        for (const FrameHeader* f : frames) if (!b.add(f)) break;
        wire(buf, b.size(), p_.retransmitLine, now); ++snapshots_;
    }
    void heartbeat(int64_t now, const Wire& wire) {
        alignas(16) std::byte buf[MAX_PACKET];
        PacketBuilder b(buf); b.begin(p_.feedId, 0, nextSeq_, now, PacketFlags::heartbeat);
        for (uint16_t line = 0; line < p_.lineCount; ++line) deliver(line, buf, b.size(), now, wire);
    }

    uint64_t packets() const noexcept { return packets_; }
    uint64_t delivered(uint16_t line) const noexcept { return line < impair_.size() ? deliveredPerLine_[line] : 0; }
    uint64_t dropped(uint16_t line) const noexcept { return line < impair_.size() ? droppedPerLine_[line] : 0; }
    uint64_t retransmitted() const noexcept { return retransmitted_; }
    uint64_t snapshots() const noexcept { return snapshots_; }

private:
    void send(int64_t now, const Wire& wire) {
        size_t len = builder_.size();
        nextSeq_ = nextSeqAfter(*builder_.header());
        store_.emplace_back(buf_, buf_ + len);
        if (store_.size() > p_.retransmitWindow) store_.pop_front();
        ++packets_;
        for (uint16_t line = 0; line < p_.lineCount; ++line) {
            const Impairment& im = impair_[line];
            if (roll() < im.dropPct) { ++droppedPerLine_[line]; continue; }
            // hold this one back so it arrives after the next: reordering within a line
            if (roll() < im.reorderPct && !held_[line]) { held_[line] = std::vector<std::byte>(buf_, buf_ + len); continue; }
            deliver(line, buf_, len, now + im.delayNs, wire);
            if (held_[line]) { deliver(line, held_[line]->data(), held_[line]->size(), now + im.delayNs, wire); held_[line].reset(); }
            if (roll() < im.dupPct) deliver(line, buf_, len, now + im.delayNs + 1, wire);
        }
        building_ = 0;
    }
    void deliver(uint16_t line, const std::byte* pkt, size_t len, int64_t recvTs, const Wire& wire) {
        alignas(16) std::byte copy[MAX_PACKET];
        std::memcpy(copy, pkt, len);
        reinterpret_cast<PacketHeader*>(copy)->lineId = line;   // the line stamps itself, as a real one does
        wire(copy, len, line, recvTs);
        if (line < deliveredPerLine_.size()) ++deliveredPerLine_[line];
    }
    double roll() { return double(rng_() % 1000000) / 1000000.0; }

    Params p_;
    std::mt19937_64 rng_;
    std::vector<Impairment> impair_;
    std::vector<std::optional<std::vector<std::byte>>> held_;
    std::deque<std::vector<std::byte>> store_;
    alignas(16) std::byte buf_[MAX_PACKET]{};
    PacketBuilder builder_{buf_};
    uint16_t building_ = 0;
    uint64_t nextSeq_ = 0, packets_ = 0, retransmitted_ = 0, snapshots_ = 0;
    std::array<uint64_t, 256> deliveredPerLine_{}, droppedPerLine_{};
};

} // namespace trading::sim
