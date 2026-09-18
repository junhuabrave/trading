// Cross-language round trip for gen/go: read the five-frame log the C++ test wrote and
// validate it exactly as tests/test_codec.py does; then write the same log and require
// it to be byte-identical (the Makefile compares build/go.log to build/cpp.log).
package tradingtests

import (
	"bytes"
	"os"
	"testing"

	"trading"
)

func logPath(t *testing.T, name string) string {
	p := os.Getenv("TRADING_BUILD")
	if p == "" {
		p = "../../build"
	}
	return p + "/" + name
}

func TestReadCppLog(t *testing.T) {
	buf, err := os.ReadFile(logPath(t, "cpp.log"))
	if err != nil {
		t.Skip("no build/cpp.log; run make test first")
	}
	type fr struct {
		h    trading.FrameHeader
		body trading.Message
	}
	var frames []fr
	err = trading.IterFrames(buf, func(h trading.FrameHeader, body trading.Message, raw []byte) error {
		frames = append(frames, fr{h, body})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 5 {
		t.Fatalf("frames = %d", len(frames))
	}
	ss, ok := frames[0].body.(*trading.SessionStart)
	if !ok || ss.BusinessDate != 20260915 || ss.SnapshotVersion != 3 {
		t.Fatalf("frame 0: %#v", frames[0].body)
	}
	for i := 0; i < 32; i++ {
		if ss.SnapshotHash[i] != uint8(i*7&0xFF) {
			t.Fatalf("snapshotHash[%d]", i)
		}
	}
	no, ok := frames[1].body.(*trading.NewOrder)
	if !ok || no.Side != trading.SideSellShort || no.Qty != 500 || no.Price != 12_345_000_000 {
		t.Fatalf("frame 1: %#v", frames[1].body)
	}
	if no.OrderFlags&trading.OrderFlagsLocateAttached == 0 {
		t.Fatal("locateAttached not set")
	}
	if frames[1].h.Seq != 2 || frames[1].h.OriginTs != frames[1].h.SeqTs-600 {
		t.Fatal("frame 1 header")
	}
	rd, ok := frames[2].body.(*trading.RiskDecision)
	if !ok || frames[2].h.CauseSeq != 2 || rd.Verdict != trading.RiskVerdictAccept || rd.Notional != 500*12_345_000_000 {
		t.Fatalf("frame 2: %#v", frames[2].body)
	}
	bd, ok := frames[3].body.(*trading.BookDelta)
	if !ok || frames[3].h.StreamID != 1 || bd.Qty != 1200 {
		t.Fatalf("frame 3: %#v", frames[3].body)
	}
	er, ok := frames[4].body.(*trading.ExecReport)
	if !ok || string(er.VenueExecID[:]) != "XNAS0000000000000042" || er.Fee != -30_000_000 {
		t.Fatalf("frame 4: %#v", frames[4].body)
	}
	// causality chain: ExecReport -> RiskDecision -> NewOrder
	bySeq := map[uint64]fr{}
	for _, f := range frames {
		if f.h.StreamID == 0 {
			bySeq[f.h.Seq] = f
		}
	}
	var chain []string
	for seq := frames[4].h.Seq; seq != 0; {
		f := bySeq[seq]
		chain = append(chain, trading.MessageName(f.h.TemplateID))
		seq = f.h.CauseSeq
	}
	if len(chain) != 3 || chain[0] != "ExecReport" || chain[1] != "RiskDecision" || chain[2] != "NewOrder" {
		t.Fatalf("chain = %v", chain)
	}
	t.Logf("go read C++ log ok; cause chain: %v", chain)
}

func TestWriteLog(t *testing.T) {
	var out []byte
	const t0 = int64(1_700_000_000_000_000_000)
	ss := &trading.SessionStart{BusinessDate: 20260915, SnapshotVersion: 3, SchemaVersion: trading.SchemaVersion}
	for i := 0; i < 32; i++ {
		ss.SnapshotHash[i] = uint8(i * 7 & 0xFF)
	}
	out = append(out, trading.Frame(trading.FrameHeader{Seq: 1, SeqTs: t0, SourceID: 1}, ss)...)
	no := &trading.NewOrder{ClOrdID: 0xC10, OrderID: (2 << 48) | 1, AccountIdx: 42, SymbolIdx: 7,
		Side: trading.SideSellShort, OrdType: trading.OrdTypeLimit, Tif: trading.TifDay,
		OrderFlags: trading.OrderFlagsLocateAttached | trading.OrderFlagsAllowDark,
		Qty: 500, Price: 12_345_000_000, LocateID: (10 << 48) | 77, RoutingProfile: 3, SessionID: 9, ClientTag: 0xBEEF}
	out = append(out, trading.Frame(trading.FrameHeader{Seq: 2, SeqTs: t0 + 1500, OriginTs: t0 + 900, SourceID: 2}, no)...)
	rd := &trading.RiskDecision{OrderID: no.OrderID, AccountIdx: 42, SymbolIdx: 7, Verdict: trading.RiskVerdictAccept,
		CheckMask: 0x7F, Notional: 500 * 12_345_000_000, BuyingPowerAfter: 1e14}
	out = append(out, trading.Frame(trading.FrameHeader{Seq: 3, SeqTs: t0 + 2500, CauseSeq: 2, SourceID: 3}, rd)...)
	bd := &trading.BookDelta{SymbolIdx: 7, VenueID: 2, Action: trading.BookActionSet, Side: trading.BookSideBid,
		Price: 12_344_000_000, Qty: 1200, OrderCount: 4, Flags: trading.BookFlagsEndOfPacket, VenueSeq: 555, VenueTs: 1}
	out = append(out, trading.Frame(trading.FrameHeader{Seq: 1001, StreamID: 1, SourceID: 8}, bd)...)
	er := &trading.ExecReport{OrderID: no.OrderID, ClOrdID: 0xC10, ExecID: (7 << 48) | 5, AccountIdx: 42, SymbolIdx: 7,
		ExecType: trading.ExecTypeFill, OrdStatus: trading.OrdStatusFilled, Side: trading.SideSellShort,
		LiquidityFlag: trading.LiquidityRemoved, VenueID: 2, LastQty: 500, LastPx: 12_345_000_000, CumQty: 500,
		AvgPx: 12_345_000_000, Fee: -30_000_000, NbboBid: 12_344_000_000, NbboAsk: 12_346_000_000, ClientTag: 0xBEEF}
	copy(er.VenueExecID[:], "XNAS0000000000000042")
	out = append(out, trading.Frame(trading.FrameHeader{Seq: 4, CauseSeq: 3, SourceID: 7}, er)...)
	if err := os.WriteFile(logPath(t, "go.log"), out, 0o644); err != nil {
		t.Fatal(err)
	}
	if cpp, err := os.ReadFile(logPath(t, "cpp.log")); err == nil && !bytes.Equal(cpp, out) {
		t.Fatalf("go.log differs from cpp.log (%d vs %d bytes)", len(out), len(cpp))
	}
	t.Logf("go wrote %d bytes, 5 frames", len(out))
}
