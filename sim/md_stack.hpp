// sim/md_stack.hpp : the real market-data stack, wired end to end, as the simulator's source of
// market data.
//
// Until now the harness made its market up: sim/feed_sim.hpp walked a price around a reference and
// emitted our own normalised messages directly. That was the right stand-in while there was nothing
// to stand in for, and it is the wrong one now, because every component between the wire and the
// book existed only in its own tests. What runs here instead is the production path:
//
//   ItchSim writes real ITCH 5.0 bytes
//     -> packetised into MoldUDP64-shaped packets, delivered on two lines
//       -> LineReceiver stamps and hands up, the same packet arriving twice
//         -> LineArbitrator merges by venue sequence, first arrival wins, second copy dropped
//           -> itch::Decoder turns venue bytes into our normalised messages
//             -> BookBuilder maintains the ladders and publishes the top of book to shared memory
//               -> the normalised messages go on the sequenced market-data stream
//
// So a simulated day now exercises the decoder, the arbitrator and the book on every message, and
// the price the client simulator trades around is the one the book actually holds rather than one
// the generator remembered. sim/feed_sim.hpp stays for what it is still good for - a cheap
// deterministic message source for tests that are not about market data - but it is no longer what
// the harness believes.
#pragma once
#include "arbitrator.hpp"
#include "book.hpp"
#include "itch.hpp"
#include "line_receiver.hpp"
#include "itch_sim.hpp"
#include <deque>

namespace trading::sim {

using namespace trading::md;

class MdStack {
public:
    struct Params {
        uint32_t feedId = 1;
        uint16_t venueId = 2;
        uint16_t sourceId = 8;
        uint32_t symbols = 10;
        uint32_t maxVenueId = 4;
        uint16_t msgsPerPacket = 4;
        int64_t  midnight = 0;
        uint64_t seed = 1;
    };

    MdStack(const refdata::Snapshot& snap, const refdata::RefData& rd, Params p, std::byte* bookBase)
        : rd_(rd), p_(p),
          venue_(snap, p.symbols, p.seed, p.midnight),
          arb_(arbParams(p), [this](const PacketHeader& h, size_t, uint16_t skip) { onArbitrated(h, skip); }),
          dec_(snap, decParams(p)),
          book_(rd, bookParams(p), bookBase),
          a_(p.feedId, 0, nullptr, [this](const PacketHeader& h, size_t len, uint16_t line, int64_t ts) { arb_.onPacket(h, len, line, ts); }),
          b_(p.feedId, 1, nullptr, [this](const PacketHeader& h, size_t len, uint16_t line, int64_t ts) { arb_.onPacket(h, len, line, ts); }) {
        venue_.open([this](const uint8_t* m, size_t len) { pending_.emplace_back(m, m + len); });
    }
    MdStack(const MdStack&) = delete;
    MdStack& operator=(const MdStack&) = delete;

    // Produce and consume `messages` venue messages. Everything the stack normalises, and
    // everything the book concludes from it, is handed to emit - which is what the harness
    // sequences onto the market-data stream.
    template <class F>
    void burst(uint32_t messages, int64_t now, F&& emit) {
        for (uint32_t i = 0; i < messages; ++i)
            venue_.step([this](const uint8_t* m, size_t len) { pending_.emplace_back(m, m + len); });
        run(now, emit);
    }

    // What a symbol is worth, taken from the book rather than remembered: the midpoint if there is
    // a two-sided market, otherwise whichever side exists, otherwise the reference price.
    //
    // Rounded to the tick. A midpoint between two prices an odd number of ticks apart does not sit
    // on the grid, and anything that prices an order from it would be quoting a price the venue
    // cannot accept.
    int64_t mid(uint32_t symbolIdx) const {
        TopBody t{};
        int64_t px = 0;
        if (book_.reader().top(symbolIdx, t)) {
            if (t.bid && t.ask) px = (t.bid + t.ask) / 2;
            else if (t.bid) px = t.bid;
            else if (t.ask) px = t.ask;
        }
        if (!px) return rd_.refPrice(symbolIdx);
        const int64_t tick = rd_.tick(symbolIdx, px);
        return tick > 0 ? px - px % tick : px;
    }

    template <class F> void snapshots(int64_t now, F&& emit) { book_.publishSnapshots(now, emit); }

    const BookBuilder& book() const noexcept { return book_; }
    const itch::Decoder& decoder() const noexcept { return dec_; }
    const LineArbitrator& arbitrator() const noexcept { return arb_; }
    uint64_t venueMessages() const noexcept { return venue_.sent(); }
    uint64_t packets() const noexcept { return packets_; }

private:
    static LineArbitrator::Params arbParams(const Params& p) {
        LineArbitrator::Params a{};
        a.feedId = p.feedId;
        a.gapTimeoutNs = 1'000'000'000;      // the harness loses nothing; the drills are in test_arb
        return a;
    }
    static itch::Decoder::Params decParams(const Params& p) {
        itch::Decoder::Params d{};
        d.feedId = p.feedId; d.venueId = p.venueId; d.sourceId = p.sourceId;
        d.sessionMidnightNs = p.midnight;
        d.maxOrders = 1u << 18; d.maxLevels = 1u << 14;
        return d;
    }
    static BookBuilder::Params bookParams(const Params& p) {
        BookBuilder::Params b{};
        b.sourceId = p.sourceId; b.feedId = p.feedId;
        b.maxSymbolIdx = p.symbols; b.maxVenueId = p.maxVenueId;
        return b;
    }

    // Where this burst's output goes, without a std::function and so without an allocation per
    // step: the caller's lambda lives for the duration of the call and the stack borrows it.
    void out(const FrameHeader* f) { out_(outCtx_, f); }

    template <class F>
    void run(int64_t now, F& emit) {
        outCtx_ = &emit;
        out_ = [](void* c, const FrameHeader* f) { (*static_cast<std::decay_t<F>*>(c))(f); };
        now_ = now;
        while (!pending_.empty()) {
            alignas(16) std::byte buf[MAX_PACKET];
            PacketBuilder pb(buf);
            pb.begin(p_.feedId, 0, nextSeq_, now);
            itch::BlockWriter bw(buf + sizeof(PacketHeader), MAX_PACKET - sizeof(PacketHeader));
            uint16_t n = 0;
            while (!pending_.empty() && n < p_.msgsPerPacket
                   && bw.add(pending_.front().data(), pending_.front().size())) {
                pending_.pop_front();
                ++n;
            }
            if (n == 0) { pending_.pop_front(); continue; }     // a message too large to frame
            pb.header()->msgCount = n;
            pb.header()->payloadLen = uint16_t(bw.size());
            nextSeq_ += n;
            ++packets_;
            // The same packet down both lines, as a venue sends it. The arbitrator takes the first
            // and discards the second, so the redundancy is exercised on every packet of every
            // simulated day rather than only in its own drills.
            a_.onPacket(buf, pb.size(), now);
            b_.onPacket(buf, pb.size(), now + 2000);
        }
        out_ = nullptr;
        outCtx_ = nullptr;
    }

    void onArbitrated(const PacketHeader& h, uint16_t skip) {
        dec_.decode(h, skip, [this](const FrameHeader* f) {
            out(f);                                             // the normalised message
            book_.apply(f, now_, [this](const FrameHeader* g) { out(g); });   // and what the book concludes
        });
    }

    const refdata::RefData& rd_;
    Params p_;
    ItchSim venue_;
    LineArbitrator arb_;
    itch::Decoder dec_;
    BookBuilder book_;
    LineReceiver a_, b_;
    std::deque<std::vector<uint8_t>> pending_;
    void (*out_)(void*, const FrameHeader*) = nullptr;
    void* outCtx_ = nullptr;
    int64_t now_ = 0;
    uint64_t nextSeq_ = 1'000'000, packets_ = 0;
};

} // namespace trading::sim
