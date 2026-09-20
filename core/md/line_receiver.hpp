// core/md/line_receiver.hpp : one per line of a feed. Takes bytes off the wire, stamps the hardware
// receive time, writes them to the feed's capture, and hands the packet up. It does not parse the
// venue's protocol and does not arbitrate between lines; those are the decoder and the arbitrator
// (MD-4, MD-3).
//
// Keeping it this thin is the point. The receiver is the only component that touches the network,
// so it must never be the reason a packet is lost: it captures before anything can reject the
// packet, and a malformed one is counted and dropped rather than thrown.
//
// The capture belongs to the feed, not the line, because the arbitrator's input is every line at
// once. The lines of one feed therefore share a CaptureWriter, which they own jointly and none of
// them owns: a feed is captured by a single writer on a single thread.
#pragma once
#include "capture.hpp"
#include <functional>

namespace trading::md {

class LineReceiver {
public:
    using Sink = std::function<void(const PacketHeader&, size_t len, uint16_t lineId, int64_t recvTs)>;

    // capture may be null, which is only for tests measuring something else.
    LineReceiver(uint32_t feedId, uint16_t lineId, CaptureWriter* capture, Sink sink)
        : feedId_(feedId), lineId_(lineId), capture_(capture), sink_(std::move(sink)) {}

    // One datagram off this line. recvTs is the hardware timestamp; it is what ends up in originTs,
    // so every latency figure downstream is measured from the wire rather than from our first look.
    void onPacket(const void* bytes, size_t len, int64_t recvTs) {
        ++received_;
        if (!validPacket(bytes, len)) { ++malformed_; return; }
        const auto& h = *static_cast<const PacketHeader*>(bytes);
        if (h.feedId != feedId_) { ++wrongFeed_; return; }
        if (capture_) capture_->write(bytes, len, lineId_, recvTs);   // capture before anything can reject it
        bytes_ += len; ++captured_;
        if (sink_) sink_(h, len, lineId_, recvTs);
    }

    uint16_t lineId() const noexcept { return lineId_; }
    uint64_t received() const noexcept { return received_; }
    uint64_t captured() const noexcept { return captured_; }
    uint64_t malformed() const noexcept { return malformed_; }
    uint64_t wrongFeed() const noexcept { return wrongFeed_; }
    uint64_t bytes() const noexcept { return bytes_; }

private:
    uint32_t feedId_; uint16_t lineId_;
    CaptureWriter* capture_;
    Sink sink_;
    uint64_t received_ = 0, captured_ = 0, malformed_ = 0, wrongFeed_ = 0, bytes_ = 0;
};

// Replay a capture back exactly as it arrived: same packets, same lines, same receive times. This
// is what makes the capture the simulator's input and the evidence in a dispute.
template <class F>
inline uint64_t replayCapture(const std::string& path, F&& sink) {
    CaptureReader r(path);
    return r.forEach([&](const CaptureRecord& rec, const std::byte* pkt) {
        sink(*reinterpret_cast<const PacketHeader*>(pkt), size_t(rec.length), rec.lineId, rec.recvTs);
    });
}

} // namespace trading::md
