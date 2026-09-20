// core/md/packet.hpp : the transport envelope a market-data line carries.
//
// Shaped like MoldUDP64: a small header naming the feed, the line, the venue sequence of the first
// message inside, and how many messages follow. The envelope is what the line arbitrator reasons
// about (gaps, A/B arbitration, retransmit requests); the payload is opaque bytes it never reads.
// That split is deliberate. A receiver and an arbitrator that cannot parse the venue's protocol are
// a receiver and an arbitrator that work for every venue, and the decoder (MD-4) is the only piece
// that needs to know ITCH from PITCH.
//
// In the simulator the payload happens to be our own frames. In production it is the venue's bytes.
// Neither the receiver nor the capture cares.
#pragma once
#include "trading.hpp"
#include <cstring>
#include <cstdint>

namespace trading::md {

namespace PacketFlags {
    inline constexpr uint16_t retransmit = 1u << 0;   // answering a gap request, not live
    inline constexpr uint16_t snapshot   = 1u << 1;   // a recovery snapshot, not an increment
    inline constexpr uint16_t heartbeat  = 1u << 2;   // no messages; proves the line is alive
}

struct PacketHeader {
    char     magic[4];        // "MDPK"
    uint32_t feedId;
    uint64_t firstSeq;        // venue sequence of the first message in the payload
    uint16_t msgCount;        // 0 for a heartbeat
    uint16_t lineId;          // which line of the feed delivered it
    uint16_t flags;
    uint16_t payloadLen;      // bytes after this header
    int64_t  venueTs;         // when the venue says it sent
};
static_assert(sizeof(PacketHeader) == 32);
static_assert(sizeof(PacketHeader) % 16 == 0, "payload frames stay 16-byte aligned");

inline constexpr size_t MAX_PACKET = 2048;

inline bool validPacket(const void* p, size_t len) noexcept {
    if (len < sizeof(PacketHeader) || len > MAX_PACKET) return false;
    const auto* h = static_cast<const PacketHeader*>(p);
    if (std::memcmp(h->magic, "MDPK", 4) != 0) return false;
    return sizeof(PacketHeader) + h->payloadLen == len;
}
// venue sequence one past the last message in the packet
inline uint64_t nextSeqAfter(const PacketHeader& h) noexcept { return h.firstSeq + h.msgCount; }

// Walk the frames inside a packet. Payload frames are our wire format in the simulator; a caller
// that knows the payload is something else simply does not use this.
template <class F>
inline size_t forEachFrame(const PacketHeader& h, F&& fn) {
    const auto* p = reinterpret_cast<const std::byte*>(&h) + sizeof(PacketHeader);
    size_t off = 0, n = 0;
    while (off + sizeof(FrameHeader) <= h.payloadLen) {
        const auto* f = reinterpret_cast<const FrameHeader*>(p + off);
        if (f->frameLength < sizeof(FrameHeader) || off + f->frameLength > h.payloadLen) break;
        fn(f); ++n; off += f->frameLength;
    }
    return n;
}

// Builds one packet in a caller-owned buffer.
class PacketBuilder {
public:
    explicit PacketBuilder(std::byte* buf) : buf_(buf) {}
    void begin(uint32_t feedId, uint16_t lineId, uint64_t firstSeq, int64_t venueTs, uint16_t flags = 0) noexcept {
        auto* h = header();
        std::memcpy(h->magic, "MDPK", 4);
        h->feedId = feedId; h->lineId = lineId; h->firstSeq = firstSeq; h->venueTs = venueTs;
        h->flags = flags; h->msgCount = 0; h->payloadLen = 0;
    }
    bool add(const FrameHeader* f) noexcept {
        auto* h = header();
        if (sizeof(PacketHeader) + h->payloadLen + f->frameLength > MAX_PACKET) return false;
        std::memcpy(buf_ + sizeof(PacketHeader) + h->payloadLen, f, f->frameLength);
        h->payloadLen = uint16_t(h->payloadLen + f->frameLength); ++h->msgCount;
        return true;
    }
    size_t size() const noexcept { return sizeof(PacketHeader) + header()->payloadLen; }
    uint16_t count() const noexcept { return header()->msgCount; }
    PacketHeader* header() noexcept { return reinterpret_cast<PacketHeader*>(buf_); }
    const PacketHeader* header() const noexcept { return reinterpret_cast<const PacketHeader*>(buf_); }
private:
    std::byte* buf_;
};

} // namespace trading::md
