// test_refdata day1.bin day2.bin diff.log
//   1. load day1, verify hashes, check a few facts
//   2. apply the diff log in order
//   3. state hash must equal a fresh load of day2
//   4. a tampered copy of day1 must be rejected
#include "refdata.hpp"
#include <fstream>
#include <vector>
#include <cstdio>
#include <cstring>

using namespace trading;
using namespace trading::refdata;

#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static std::vector<std::byte> slurp(const char* p) {
    std::ifstream in(p, std::ios::binary);
    std::vector<char> raw((std::istreambuf_iterator<char>(in)), {});
    std::vector<std::byte> b(raw.size()); std::memcpy(b.data(), raw.data(), raw.size()); return b;
}
static std::string hex(const std::array<uint8_t, 32>& h) {
    static const char* d = "0123456789abcdef"; std::string s;
    for (int i = 0; i < 8; ++i) { s += d[h[i] >> 4]; s += d[h[i] & 15]; } return s;
}

int main(int argc, char** argv) {
    if (argc != 4) { std::fprintf(stderr, "usage: %s day1.bin day2.bin diff.log\n", argv[0]); return 2; }

    // 1. load day 1
    Snapshot s1(argv[1]);
    RefData rd; rd.load(s1);
    std::printf("day1: date=%u v%u symbols<=%u\n", s1.businessDate(), s1.version(), rd.maxSymbolIdx());
    uint32_t aapl = s1.lookupTicker("AAPL"), gme = s1.lookupTicker("GME"), xyzq = s1.lookupTicker("XYZQ"), nvda = s1.lookupTicker("NVDA");
    CHECK(aapl == 1 && gme == 7 && xyzq == 9 && nvda == 10);
    CHECK(s1.lookupTicker("NEWCO") == 0);
    CHECK(rd.tradable(xyzq));
    CHECK(rd.easyToBorrow(aapl) && !rd.easyToBorrow(gme) && rd.inList(ListId::HardToBorrow, gme));
    CHECK(!rd.restricted(5));
    CHECK(rd.tickTableId(gme) == 1 && rd.tick(gme, 22'00000000LL) == 1'000000LL);   // $0.01 above $1
    CHECK(rd.tick(gme, 50000000LL) == 10000LL);                                        // $0.0001 below $1
    int64_t nvdaRef = rd.refPrice(nvda);
    auto h1 = rd.stateHash();

    // 2. apply the diff log
    auto log = slurp(argv[3]);
    size_t off = 0; int applied = 0;
    while (off + sizeof(FrameHeader) <= log.size()) {
        const auto* h = reinterpret_cast<const FrameHeader*>(log.data() + off);
        CHECK(h->frameLength >= sizeof(FrameHeader) && off + h->frameLength <= log.size());
        CHECK(rd.apply(h));
        off += h->frameLength; ++applied;
    }
    std::printf("applied %d reference updates\n", applied);

    // 3. compare with a fresh day-2 load
    Snapshot s2(argv[2]);
    RefData rd2; rd2.load(s2);
    auto hApplied = rd.stateHash(), hFresh = rd2.stateHash();
    std::printf("state hash day1        = %s\n", hex(h1).c_str());
    std::printf("state hash day1+diff   = %s\n", hex(hApplied).c_str());
    std::printf("state hash day2 fresh  = %s\n", hex(hFresh).c_str());
    CHECK(hApplied == hFresh);
    CHECK(h1 != hFresh);
    CHECK(!rd.tradable(xyzq));                       // delisted via SymbolUpdate
    CHECK(rd.tradable(11) && rd.easyToBorrow(11));   // NEWCO added via SymbolAdd + ListUpdate
    CHECK(rd.restricted(5));                          // MS restricted via ListUpdate
    CHECK(rd.easyToBorrow(gme) == false && !rd.inList(ListId::HardToBorrow, 9));
    CHECK(rd.tickTableId(gme) == 2 && rd.tick(gme, 22'00000000LL) == 500000LL);      // half-penny pilot
    CHECK(rd.refPrice(nvda) == nvdaRef / 4);         // 4:1 split reflected in refPrice
    CHECK((rd.flags(nvda) & SymbolFlags::corpActionToday) != 0);

    // 4. tamper detection: flip one byte in the instruments section of a copy of day1
    {
        auto bytes = slurp(argv[1]);
        bytes[4096 + 256 + 64] ^= std::byte{0x01};   // second record, ticker byte
        std::string tmp = std::string(argv[1]) + ".tampered";
        std::ofstream out(tmp, std::ios::binary); out.write(reinterpret_cast<const char*>(bytes.data()), std::streamsize(bytes.size())); out.close();
        bool rejected = false;
        try { Snapshot bad(tmp); } catch (const std::exception& e) { rejected = true; std::printf("tampered copy rejected: %s\n", e.what()); }
        CHECK(rejected);
    }
    std::printf("refdata determinism test ok\n");
    return 0;
}
