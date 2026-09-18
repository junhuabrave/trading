// Round-trip test for gen/cpp/trading.hpp.
//   test_codec write <path>  : writes a small log of frames
//   test_codec read  <path>  : reads a log (possibly written by Python) and validates it
#include "trading.hpp"
#include <cstdio>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>

using namespace trading;

#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static int write_log(const char* path) {
    std::vector<std::byte> buf;
    auto push = [&](const auto& frame) {
        const auto* p = reinterpret_cast<const std::byte*>(&frame);
        buf.insert(buf.end(), p, p + sizeof(frame));
    };

    Frame<SessionStart> ss; ss.init();
    ss.header.seq = 1; ss.header.seqTs = 1'700'000'000'000'000'000LL; ss.header.sourceId = 1;
    ss.body.businessDate = 20260915; ss.body.snapshotVersion = 3; ss.body.schemaVersion = SCHEMA_VERSION;
    for (int i = 0; i < 32; ++i) ss.body.snapshotHash[i] = uint8_t(i * 7);
    push(ss);

    Frame<NewOrder> no; no.init();
    no.header.seq = 2; no.header.seqTs = ss.header.seqTs + 1500; no.header.originTs = ss.header.seqTs + 900;
    no.header.sourceId = 2; no.header.streamId = 0;
    no.body.clOrdId = 0xC10; no.body.orderId = (uint64_t(2) << 48) | 1;
    no.body.accountIdx = 42; no.body.symbolIdx = 7;
    no.body.side = Side::SellShort; no.body.ordType = OrdType::Limit; no.body.tif = Tif::Day;
    no.body.orderFlags = OrderFlags::locateAttached | OrderFlags::allowDark;
    no.body.qty = 500; no.body.price = 12'345'000'000LL;  // $123.45
    no.body.locateId = (uint64_t(10) << 48) | 77;
    no.body.routingProfile = 3; no.body.sessionId = 9; no.body.clientTag = 0xBEEF;
    push(no);

    Frame<RiskDecision> rd; rd.init();
    rd.header.seq = 3; rd.header.seqTs = no.header.seqTs + 1000; rd.header.causeSeq = 2; rd.header.sourceId = 3;
    rd.body.orderId = no.body.orderId; rd.body.accountIdx = 42; rd.body.symbolIdx = 7;
    rd.body.verdict = RiskVerdict::Accept; rd.body.checkMask = 0x7F;
    rd.body.notional = no.body.qty * no.body.price; rd.body.buyingPowerAfter = 1'000'000LL * 100'000'000LL;
    push(rd);

    Frame<BookDelta> bd; bd.init();
    bd.header.seq = 1001; bd.header.streamId = 1; bd.header.sourceId = 8;
    bd.body.symbolIdx = 7; bd.body.venueId = 2; bd.body.action = BookAction::Set; bd.body.side = BookSide::Bid;
    bd.body.price = 12'344'000'000LL; bd.body.qty = 1200; bd.body.orderCount = 4;
    bd.body.flags = BookFlags::endOfPacket; bd.body.venueSeq = 555; bd.body.venueTs = 1;
    push(bd);

    Frame<ExecReport> er; er.init();
    er.header.seq = 4; er.header.causeSeq = 3; er.header.sourceId = 7;
    er.body.orderId = no.body.orderId; er.body.clOrdId = 0xC10; er.body.execId = (uint64_t(7) << 48) | 5;
    er.body.accountIdx = 42; er.body.symbolIdx = 7; er.body.execType = ExecType::Fill; er.body.ordStatus = OrdStatus::Filled;
    er.body.side = Side::SellShort; er.body.liquidityFlag = Liquidity::Removed; er.body.venueId = 2;
    er.body.lastQty = 500; er.body.lastPx = 12'345'000'000LL; er.body.cumQty = 500; er.body.avgPx = er.body.lastPx;
    er.body.fee = -30'000'000LL; er.body.nbboBid = 12'344'000'000LL; er.body.nbboAsk = 12'346'000'000LL;
    std::memcpy(er.body.venueExecId, "XNAS0000000000000042", 20);
    er.body.clientTag = 0xBEEF;
    push(er);

    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(buf.data()), std::streamsize(buf.size()));
    std::printf("wrote %zu bytes, 5 frames\n", buf.size());
    return 0;
}

static int read_log(const char* path) {
    std::ifstream in(path, std::ios::binary);
    std::vector<char> raw((std::istreambuf_iterator<char>(in)), {});
    std::vector<std::byte> buf(raw.size()); std::memcpy(buf.data(), raw.data(), raw.size());
    size_t off = 0; int n = 0;
    uint64_t lastSeq[4] = {0, 0, 0, 0};
    while (off + sizeof(FrameHeader) <= buf.size()) {
        const auto* h = reinterpret_cast<const FrameHeader*>(buf.data() + off);
        CHECK(h->frameLength >= sizeof(FrameHeader));
        CHECK(off + h->frameLength <= buf.size());
        CHECK(h->schemaVersion <= SCHEMA_VERSION);
        const MessageInfo* mi = lookup(h->templateId);
        CHECK(mi != nullptr);
        CHECK(h->frameLength == sizeof(FrameHeader) + mi->blockLength);
        CHECK(h->streamId < 4);
        CHECK(h->seq > lastSeq[h->streamId]);           // monotonic per stream
        lastSeq[h->streamId] = h->seq;
        if (auto* m = as<NewOrder>(h)) {
            CHECK(m->side == Side::SellShort);
            CHECK(m->qty == 500 && m->price == 12'345'000'000LL);
            CHECK((m->orderFlags & OrderFlags::locateAttached) != 0);
            CHECK(m->locateId != 0);
            std::printf("  seq %llu NewOrder acct=%u sym=%u %s qty=%lld px=%lld\n",
                (unsigned long long)h->seq, m->accountIdx, m->symbolIdx, to_string(m->side), (long long)m->qty, (long long)m->price);
        } else if (auto* m = as<RiskDecision>(h)) {
            CHECK(h->causeSeq == 2);
            CHECK(m->verdict == RiskVerdict::Accept);
            std::printf("  seq %llu RiskDecision cause=%llu %s\n", (unsigned long long)h->seq, (unsigned long long)h->causeSeq, to_string(m->verdict));
        } else if (auto* m = as<ExecReport>(h)) {
            CHECK(m->execType == ExecType::Fill);
            CHECK(std::memcmp(m->venueExecId, "XNAS", 4) == 0);
            std::printf("  seq %llu ExecReport %s lastQty=%lld fee=%lld nbbo=%lld/%lld\n", (unsigned long long)h->seq,
                to_string(m->execType), (long long)m->lastQty, (long long)m->fee, (long long)m->nbboBid, (long long)m->nbboAsk);
        } else if (auto* m = as<BookDelta>(h)) {
            CHECK(m->action == BookAction::Set);
            std::printf("  stream %u seq %llu BookDelta sym=%u venue=%u qty=%lld\n", h->streamId, (unsigned long long)h->seq, m->symbolIdx, m->venueId, (long long)m->qty);
        } else if (auto* m = as<SessionStart>(h)) {
            CHECK(m->businessDate == 20260915);
            std::printf("  seq %llu SessionStart date=%u snap=v%u\n", (unsigned long long)h->seq, m->businessDate, m->snapshotVersion);
        } else {
            std::printf("  seq %llu %s\n", (unsigned long long)h->seq, mi->name);
        }
        off += h->frameLength; ++n;
    }
    CHECK(off == buf.size());
    CHECK(n == 5);
    std::printf("read %d frames ok\n", n);
    return 0;
}

int main(int argc, char** argv) {
    static_assert(sizeof(FrameHeader) == 48);
    static_assert(sizeof(NewOrder) == 128);
    static_assert(sizeof(ExecReport) == 176);
    static_assert(sizeof(SymbolRecord) == 256);
    static_assert(sizeof(SnapshotHeader) == 4096);
    static_assert(offsetof(NewOrder, price) == 40);
    static_assert(offsetof(FrameHeader, causeSeq) == 32);
    if (argc != 3) { std::fprintf(stderr, "usage: %s write|read path\n", argv[0]); return 2; }
    return std::strcmp(argv[1], "write") == 0 ? write_log(argv[2]) : read_log(argv[2]);
}
