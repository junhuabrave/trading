// tests/test_ouch.cpp
//
// OUCH 5.0 and SoupBinTCP 3.00 on the wire (VG-2).
//
// The specifications give field tables (name, offset, length, value) but no hex dumps of whole
// messages; the only byte values they state are the two price limits. So the vectors below were
// written from the tables - every message type in both directions with its optional fields, and
// every SoupBinTCP packet type - with each field's bytes separated by a space, by a script that
// shares nothing with the codec. Each must decode to the fields stated here and encode back to the
// same bytes.
//
//   1. every OUCH message: the vector decodes to the expected message, which encodes to the vector
//   2. the specification's own numbers: the price limits, a signed offset, an alpha field's padding
//   3. what is refused: every shorter prefix (bar a message whose appendage is optional), a byte too
//      many, a wrong appendage length, a tag of the wrong size or given twice, an element that runs
//      off the end, the other direction's type letters, a buffer too small to encode into; and an
//      unknown tag is skipped, counted, and the message still decodes
//   4. every SoupBinTCP packet, built by the encoders and parsed back, including the login fields'
//      text numbers at and beyond 64 bits
//   5. the framer: the same packets out whatever the chunking, a maximum-length packet, and a zero
//      length that breaks the stream for good
#include "ouch.hpp"
#include "soupbin.hpp"
#include <cstdio>
#include <random>
#include <string>
#include <vector>

using namespace trading::venue;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static std::vector<uint8_t> hex(std::string_view s) {
    std::vector<uint8_t> out; int hi = -1;
    for (char c : s) {
        if (c == ' ') continue;
        const int v = c <= '9' ? c - '0' : c - 'a' + 10;
        if (hi < 0) hi = v; else { out.push_back(uint8_t(hi << 4 | v)); hi = -1; }
    }
    return out;
}

static constexpr uint64_t TS = 34'200'000'000'123;     // 09:30:00.000000123, nanoseconds since midnight
using ouch::Tag;

struct InVector { const char* name; const char* bytes; ouch::Inbound expect; };
struct OutVector { const char* name; const char* bytes; ouch::Outbound expect; };

int main() {
    const auto CL1 = ouch::alpha<14>("CHILD00000001"), CL2 = ouch::alpha<14>("CHILD00000002"), CL3 = ouch::alpha<14>("CHILD00000003");
    const auto AAPL = ouch::alpha<8>("AAPL"), ALL = ouch::alpha<8>("");
    const auto ABCD = ouch::alpha<4>("ABCD");
    const ouch::Options none;
    const auto idx7 = ouch::Options().set(Tag::UserRefIdx, 7);

    const std::vector<InVector> in = {
        {"EnterOrder", "4f 00000001 42 000001f4 4141504c20202020 000000000016ed24 30 59 41 4e 4e 4348494c44303030303030303120 000c 050241424344 050300000064",
         ouch::EnterOrder{.userRefNum = 1, .side = 'B', .quantity = 500, .symbol = AAPL, .price = 1'502'500, .timeInForce = '0', .display = 'Y',
                          .capacity = 'A', .interMarketSweep = 'N', .crossType = 'N', .clOrdId = CL1,
                          .opts = ouch::Options().setAlpha(Tag::Firm, "ABCD").set(Tag::MinQty, 100)}},
        {"ReplaceOrder", "55 00000001 00000002 00000258 000000000016ef18 30 59 4e 4348494c44303030303030303220 0009 0507ffffff9c 021c07",
         ouch::ReplaceOrder{.origUserRefNum = 1, .userRefNum = 2, .quantity = 600, .price = 1'503'000, .timeInForce = '0', .display = 'Y',
                            .interMarketSweep = 'N', .clOrdId = CL2, .opts = ouch::Options().set(Tag::PegOffset, uint32_t(-100)).set(Tag::UserRefIdx, 7)}},
        {"CancelOrder", "58 00000001 00000000", ouch::CancelOrder{.userRefNum = 1, .quantity = 0, .opts = none}},
        {"CancelOrder with an appendage", "58 00000001 00000064 0003 021c07", ouch::CancelOrder{.userRefNum = 1, .quantity = 100, .opts = idx7}},
        {"ModifyOrder", "4d 00000001 54 00000190 0009 021959 051a4753434f",
         ouch::ModifyOrder{.userRefNum = 1, .side = 'T', .quantity = 400, .opts = ouch::Options().setAlpha(Tag::SharesLocated, "Y").setAlpha(Tag::LocateBroker, "GSCO")}},
        {"MassCancelRequest", "43 00000003 41424344 2020202020202020 0003 021b42",
         ouch::MassCancelRequest{.userRefNum = 3, .firm = ABCD, .symbol = ALL, .opts = ouch::Options().setAlpha(Tag::Side, "B")}},
        {"DisableOrderEntryRequest", "44 00000004 41424344 0000", ouch::DisableOrderEntryRequest{.userRefNum = 4, .firm = ABCD, .opts = none}},
        {"EnableOrderEntryRequest", "45 00000005 41424344 0000", ouch::EnableOrderEntryRequest{.userRefNum = 5, .firm = ABCD, .opts = none}},
        {"AccountQueryRequest", "51", ouch::AccountQueryRequest{}},
    };
    const std::vector<OutVector> out = {
        {"SystemEvent", "53 00001f1aced9f07b 53", ouch::SystemEvent{.timestamp = TS, .eventCode = 'S', .opts = none}},
        {"OrderAccepted", "41 00001f1aced9f07b 00000001 42 000001f4 4141504c20202020 000000000016ed24 30 59 0000000012345678 41 4e 4e 4c 4348494c44303030303030303120 000c 050241424344 050300000064",
         ouch::OrderAccepted{.timestamp = TS, .userRefNum = 1, .side = 'B', .quantity = 500, .symbol = AAPL, .price = 1'502'500, .timeInForce = '0',
                             .display = 'Y', .orderReferenceNumber = 0x12345678, .capacity = 'A', .interMarketSweep = 'N', .crossType = 'N',
                             .orderState = 'L', .clOrdId = CL1, .opts = ouch::Options().setAlpha(Tag::Firm, "ABCD").set(Tag::MinQty, 100)}},
        {"OrderReplaced", "55 00001f1aced9f07b 00000001 00000002 42 00000190 4141504c20202020 000000000016ef18 30 59 0000000012345679 41 4e 4e 4c 4348494c44303030303030303220 0000",
         ouch::OrderReplaced{.timestamp = TS, .origUserRefNum = 1, .userRefNum = 2, .side = 'B', .quantity = 400, .symbol = AAPL, .price = 1'503'000,
                             .timeInForce = '0', .display = 'Y', .orderReferenceNumber = 0x12345679, .capacity = 'A', .interMarketSweep = 'N',
                             .crossType = 'N', .orderState = 'L', .clOrdId = CL2, .opts = none}},
        {"OrderCanceled", "43 00001f1aced9f07b 00000002 00000190 55", ouch::OrderCanceled{.timestamp = TS, .userRefNum = 2, .quantity = 400, .reason = 'U', .opts = none}},
        {"AiqCanceled", "44 00001f1aced9f07b 00000002 00000064 51 00000064 000000000016ef18 52 4f",
         ouch::AiqCanceled{.timestamp = TS, .userRefNum = 2, .decrementShares = 100, .reason = 'Q', .quantityPrevented = 100, .executionPrice = 1'503'000,
                           .liquidityFlag = 'R', .aiqStrategy = 'O', .opts = none}},
        {"OrderExecuted", "45 00001f1aced9f07b 00000001 00000064 000000000016ed24 52 0000000a00000001 0000",
         ouch::OrderExecuted{.timestamp = TS, .userRefNum = 1, .quantity = 100, .price = 1'502'500, .liquidityFlag = 'R', .matchNumber = 0x0000000A00000001, .opts = none}},
        {"BrokenTrade", "42 00001f1aced9f07b 00000001 0000000a00000001 45 4348494c44303030303030303120",
         ouch::BrokenTrade{.timestamp = TS, .userRefNum = 1, .matchNumber = 0x0000000A00000001, .reason = 'E', .clOrdId = CL1, .opts = none}},
        {"OrderRejected", "4a 00001f1aced9f07b 00000003 0006 4348494c44303030303030303320 0003 021c07",
         ouch::OrderRejected{.timestamp = TS, .userRefNum = 3, .reason = 0x0006, .clOrdId = CL3, .opts = idx7}},
        {"CancelPending", "50 00001f1aced9f07b 00000002", ouch::CancelPending{.timestamp = TS, .userRefNum = 2, .opts = none}},
        {"CancelReject", "49 00001f1aced9f07b 00000002", ouch::CancelReject{.timestamp = TS, .userRefNum = 2, .opts = none}},
        {"PriorityUpdate", "54 00001f1aced9f07b 00000002 000000000016ef18 59 000000001234567a",
         ouch::PriorityUpdate{.timestamp = TS, .userRefNum = 2, .price = 1'503'000, .display = 'Y', .orderReferenceNumber = 0x1234567A, .opts = none}},
        {"OrderModified", "4d 00001f1aced9f07b 00000001 45 00000190", ouch::OrderModified{.timestamp = TS, .userRefNum = 1, .side = 'E', .quantity = 400, .opts = none}},
        {"OrderRestated", "52 00001f1aced9f07b 00000002 52 001a 0901000000001234567b 051600000064 0917000000000016ef18",
         ouch::OrderRestated{.timestamp = TS, .userRefNum = 2, .reason = 'R',
                             .opts = ouch::Options().set(Tag::SecondaryOrdRefNum, 0x1234567B).set(Tag::DisplayQuantity, 100).set(Tag::DisplayPrice, 1'503'000)}},
        {"MassCancelResponse", "58 00001f1aced9f07b 00000003 41424344 2020202020202020 0003 021b42",
         ouch::MassCancelResponse{.timestamp = TS, .userRefNum = 3, .firm = ABCD, .symbol = ALL, .opts = ouch::Options().setAlpha(Tag::Side, "B")}},
        {"DisableOrderEntryResponse", "47 00001f1aced9f07b 00000004 41424344 0000", ouch::DisableOrderEntryResponse{.timestamp = TS, .userRefNum = 4, .firm = ABCD, .opts = none}},
        {"EnableOrderEntryResponse", "4b 00001f1aced9f07b 00000005 41424344 0000", ouch::EnableOrderEntryResponse{.timestamp = TS, .userRefNum = 5, .firm = ABCD, .opts = none}},
        {"AccountQueryResponse", "51 00001f1aced9f07b 00000006", ouch::AccountQueryResponse{.timestamp = TS, .nextUserRefNum = 6, .opts = none}},
    };

    // ---- 1: every message, both ways
    uint8_t buf[512];
    for (const auto& v : in) {
        const auto b = hex(v.bytes);
        ouch::Inbound got;
        if (!ouch::decodeInbound(b.data(), b.size(), got) || !(got == v.expect)) { std::fprintf(stderr, "decode %s\n", v.name); return 1; }
        const size_t n = ouch::encode(v.expect, buf, sizeof buf);
        if (n != b.size() || std::memcmp(buf, b.data(), n) != 0) { std::fprintf(stderr, "encode %s\n", v.name); return 1; }
    }
    for (const auto& v : out) {
        const auto b = hex(v.bytes);
        ouch::Outbound got;
        if (!ouch::decodeOutbound(b.data(), b.size(), got) || !(got == v.expect)) { std::fprintf(stderr, "decode %s\n", v.name); return 1; }
        const size_t n = ouch::encode(v.expect, buf, sizeof buf);
        if (n != b.size() || std::memcmp(buf, b.data(), n) != 0) { std::fprintf(stderr, "encode %s\n", v.name); return 1; }
    }
    std::printf("vectors: %zu inbound and %zu outbound messages decode to their fields and encode to their bytes\n", in.size(), out.size());

    // ---- 2: the specification's numbers
    CHECK(ouch::MAX_PRICE == 1'999'999'900 && ouch::toPrice(ouch::MAX_PRICE) == 199'999'99LL * 1'000'000);   // $199,999.99
    CHECK(ouch::MARKET_PRICE == 2'147'483'647 && ouch::toPrice(ouch::MARKET_PRICE) == 214'748'3647LL * 10'000);   // $214,748.3647
    CHECK(ouch::fromPrice(150'25'000'000) == 1'502'500);                                    // $150.25
    {
        ouch::EnterOrder m = std::get<ouch::EnterOrder>(in[0].expect);
        m.price = ouch::MARKET_PRICE;
        const size_t n = ouch::encode(m, buf, sizeof buf);
        CHECK(n == 59 && hex("000000007fffffff") == std::vector<uint8_t>(buf + 18, buf + 26));
        const auto& r = std::get<ouch::ReplaceOrder>(in[1].expect);
        CHECK(int32_t(uint32_t(r.opts.num(Tag::PegOffset))) == -100);                       // a signed price, two's complement
        CHECK(ouch::trimmed(m.symbol) == "AAPL" && m.symbol[4] == ' ' && m.opts.alpha(Tag::Firm) == "ABCD");
        CHECK(ouch::alpha<4>("ABCDEFG") == ABCD);                                           // cut at the field's width
    }
    std::printf("spec numbers: the price limits 7735939C and 7FFFFFFF, a negative peg offset, space padding\n");

    // ---- 3: what is refused
    size_t prefixes = 0;
    for (const auto& v : in) {
        const auto b = hex(v.bytes);
        const auto [fixed, optional] = std::visit([](const auto& m) { using M = std::decay_t<decltype(m)>; return std::pair{M::FIXED, M::APPENDAGE == ouch::Appendage::Optional}; }, v.expect);
        ouch::Inbound got;
        for (size_t k = 0; k < b.size(); ++k, ++prefixes)
            if (ouch::decodeInbound(b.data(), k, got) != (k == fixed && optional)) { std::fprintf(stderr, "prefix %zu of %s\n", k, v.name); return 1; }
        auto longer = b; longer.push_back(0);
        CHECK(!ouch::decodeInbound(longer.data(), longer.size(), got));
        ouch::Outbound wrong;
        if (b[0] == 'O') CHECK(!ouch::decodeOutbound(b.data(), b.size(), wrong));        // no outbound message is an 'O'
    }
    for (const auto& v : out) {
        const auto b = hex(v.bytes);
        const auto [fixed, optional] = std::visit([](const auto& m) { using M = std::decay_t<decltype(m)>; return std::pair{M::FIXED, M::APPENDAGE == ouch::Appendage::Optional}; }, v.expect);
        ouch::Outbound got;
        for (size_t k = 0; k < b.size(); ++k, ++prefixes)
            if (ouch::decodeOutbound(b.data(), k, got) != (k == fixed && optional)) { std::fprintf(stderr, "prefix %zu of %s\n", k, v.name); return 1; }
        auto longer = b; longer.push_back(0);
        CHECK(!ouch::decodeOutbound(longer.data(), longer.size(), got));
        ouch::Inbound wrong;
        if (b[0] == 'A' || b[0] == 'S' || b[0] == 'J') CHECK(!ouch::decodeInbound(b.data(), b.size(), wrong));
    }
    {
        ouch::EnterOrder m;
        const auto base = hex(in[0].bytes);                                                  // appendage length at 45, elements from 47
        auto bad = base; bad[46] = 13;                                                       // announces a byte more than there is
        CHECK(!ouch::decode(bad.data(), bad.size(), m));
        bad = base; bad[46] = 11;                                                            // a byte less
        CHECK(!ouch::decode(bad.data(), bad.size(), m));
        bad = base; bad[47] = 4;                                                             // Firm is four bytes, so its element is five
        CHECK(!ouch::decode(bad.data(), bad.size(), m));
        auto longFirm = hex("4f 00000001 42 000001f4 4141504c20202020 000000000016ed24 30 59 41 4e 4e 4348494c44303030303030303120 000d 06024142434420 050300000064");
        CHECK(!ouch::decode(longFirm.data(), longFirm.size(), m));                           // and not six either, though the lengths add up
        bad = base; bad[54] = 2;                                                             // the MinQty element relabelled as a second Firm
        CHECK(!ouch::decode(bad.data(), bad.size(), m));
        bad = base; bad[53] = 0;                                                             // an element with no tag
        CHECK(!ouch::decode(bad.data(), bad.size(), m));
        bad = base; bad[53] = 6;                                                             // runs one byte past the appendage
        CHECK(!ouch::decode(bad.data(), bad.size(), m));
        // an unknown tag (19 is unassigned) and one beyond the table, in the middle: skipped and counted
        auto unk = hex("4f 00000001 42 000001f4 4141504c20202020 000000000016ed24 30 59 41 4e 4e 4348494c44303030303030303120 0013 050241424344 03130102 026307 050300000064");
        CHECK(ouch::decode(unk.data(), unk.size(), m) && m.opts.unknown() == 2 && m == std::get<ouch::EnterOrder>(in[0].expect));
        CHECK(ouch::encode(m, buf, 58) == 0 && ouch::encode(m, buf, 59) == 59);           // too small, then exactly enough
        ouch::SystemEvent s;
        auto sys = hex("53 00001f1aced9f07b 53 0000");                                       // no appendage on this one, not even an empty one
        CHECK(!ouch::decode(sys.data(), sys.size(), s));
        ouch::Inbound none0;
        CHECK(!ouch::decodeInbound(buf, 0, none0));
    }
    std::printf("refusals: %zu short prefixes, trailing bytes, appendage and element lengths, a duplicate tag, the other direction; unknown tags skipped\n", prefixes);

    // ---- 4: SoupBinTCP packets
    {
        struct P { const char* name; const char* bytes; };
        uint8_t pk[128];
        const auto se = hex(out[0].bytes), co = hex(in[2].bytes);
        const P packets[] = {
            {"Debug", "0006 2b 68656c6c6f"},
            {"LoginAccepted", "001f 41 202020202020202053312020202020202020202020202020202020202031"},
            {"LoginRejected", "0002 4a 41"},
            {"SequencedData", "000b 53 5300001f1aced9f07b53"},
            {"ServerHeartbeat", "0001 48"},
            {"EndOfSession", "0001 5a"},
            {"LoginRequest", "002f 4c 4649524d303173656372657420202020202020202020202020202020202020202020202020202020202020202031"},
            {"UnsequencedData", "000a 55 580000000100000000"},
            {"ClientHeartbeat", "0001 52"},
            {"LogoutRequest", "0001 4f"},
        };
        auto build = [&](int i) -> size_t {
            switch (i) {
                case 0: return soup::debug("hello", pk, sizeof pk);
                case 1: return soup::loginAccepted("S1", 1, pk, sizeof pk);
                case 2: return soup::loginRejected(soup::REJECT_NOT_AUTHORIZED, pk, sizeof pk);
                case 3: return soup::sequenced(se.data(), se.size(), pk, sizeof pk);
                case 4: return soup::empty(soup::type::ServerHeartbeat, pk, sizeof pk);
                case 5: return soup::empty(soup::type::EndOfSession, pk, sizeof pk);
                case 6: return soup::loginRequest("FIRM01", "secret", "", 1, pk, sizeof pk);
                case 7: return soup::unsequenced(co.data(), co.size(), pk, sizeof pk);
                case 8: return soup::empty(soup::type::ClientHeartbeat, pk, sizeof pk);
                default: return soup::empty(soup::type::LogoutRequest, pk, sizeof pk);
            }
        };
        std::vector<uint8_t> stream;
        for (int i = 0; i < 10; ++i) {
            const size_t n = build(i);
            const auto b = hex(packets[i].bytes);
            if (n != b.size() || std::memcmp(pk, b.data(), n) != 0) { std::fprintf(stderr, "soup %s\n", packets[i].name); return 1; }
            stream.insert(stream.end(), b.begin(), b.end());
        }
        CHECK(soup::loginRequest("FIRM01", "secret", "", 1, pk, 48) == 0);                // one byte short of the packet

        // parsed back, with the text numbers and padding
        soup::Framer fr; auto [p, room] = fr.space(); std::memcpy(p, stream.data(), stream.size()); fr.commit(stream.size());
        std::vector<soup::Packet> got; soup::Packet k;
        while (fr.next(k)) got.push_back(k);
        CHECK(got.size() == 10 && fr.buffered() == 0);
        soup::LoginAccepted la; soup::LoginRequest lr; char why = 0;
        CHECK(soup::parse(got[1], la) && la.sessionId() == "S1" && la.nextSeq == 1);
        CHECK(soup::parseReject(got[2], why) && why == 'A');
        ouch::Outbound ev;
        CHECK(got[3].type == 'S' && ouch::decodeOutbound(got[3].payload, got[3].size, ev) && ev == out[0].expect);
        CHECK(soup::parse(got[6], lr) && lr.user() == "FIRM01" && lr.sessionId().empty() && lr.nextSeq == 1);
        CHECK(!soup::parse(got[1], lr) && !soup::parse(got[6], la));                        // the wrong packet for the parser
        auto accepted = [&](const char* seq20, uint64_t& v) {
            uint8_t q[64]; std::string payload = std::string("        S1") + seq20;
            const size_t n = soup::packet(soup::type::LoginAccepted, payload.data(), payload.size(), q, sizeof q);
            soup::LoginAccepted a; const bool ok = soup::parse(soup::Packet{'A', q + 3, n - 3}, a); v = a.nextSeq; return ok;
        };
        uint64_t v = 0;
        CHECK(accepted("18446744073709551615", v) && v == UINT64_MAX);                        // the largest that fits
        CHECK(!accepted("18446744073709551616", v));                                          // one more does not
        CHECK(accepted("00000000000000000042", v) && v == 42);                                // zero padding is still digits
        CHECK(!accepted("                    ", v) && !accepted("               12a 5", v) && !accepted("               -1234", v));
        std::printf("soup: ten packet types built and parsed back; sequence numbers to 2^64 - 1 and no further\n");

        // ---- 5: the framer, whatever the chunking
        std::mt19937 rng(7);
        for (int pass = 0; pass < 200; ++pass) {
            soup::Framer f; std::vector<std::pair<char, std::vector<uint8_t>>> seen; size_t at = 0;
            while (at < stream.size()) {
                const size_t chunk = pass == 0 ? 1 : 1 + rng() % 9;
                const size_t n = std::min(chunk, stream.size() - at);
                auto [dst, cap] = f.space(); CHECK(cap >= n);
                std::memcpy(dst, stream.data() + at, n); f.commit(n); at += n;
                while (f.next(k)) seen.emplace_back(k.type, std::vector<uint8_t>(k.payload, k.payload + k.size));
            }
            CHECK(seen.size() == 10 && f.buffered() == 0);
            for (size_t i = 0; i < 10; ++i) CHECK(seen[i].first == got[i].type && seen[i].second == std::vector<uint8_t>(got[i].payload, got[i].payload + got[i].size));
        }
        {   // the longest packet the length field allows, in two halves
            std::vector<uint8_t> big(soup::MAX_PACKET), msg(65534, 0xAB);
            CHECK(soup::sequenced(msg.data(), msg.size(), big.data(), big.size()) == soup::MAX_PACKET);
            CHECK(soup::sequenced(msg.data(), msg.size() + 1, big.data(), big.size() + 1) == 0);   // no room in sixteen bits
            soup::Framer f;
            auto [d1, c1] = f.space(); std::memcpy(d1, big.data(), 40000); f.commit(40000);
            CHECK(!f.next(k));
            auto [d2, c2] = f.space(); CHECK(c2 >= big.size() - 40000);
            std::memcpy(d2, big.data() + 40000, big.size() - 40000); f.commit(big.size() - 40000);
            CHECK(f.next(k) && k.type == 'S' && k.size == 65534 && k.payload[65533] == 0xAB);
        }
        {   // a zero length: no type byte, so nothing after it can be trusted
            soup::Framer f; const auto z = hex("0001 48 0000 0001 48");
            auto [d, c] = f.space(); std::memcpy(d, z.data(), z.size()); f.commit(z.size());
            CHECK(f.next(k) && k.type == 'H' && !f.next(k) && f.broken() && !f.next(k));
        }
        std::printf("framer: the same ten packets from 200 chunkings, a 65537-byte packet in two halves, a zero length breaks the stream\n");
    }

    std::printf("ouch codec tests ok\n");
    return 0;
}
