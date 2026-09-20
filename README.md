# Trading stack

**Status: v0.3 in progress, pre-alpha infrastructure.** Schema (v7), codecs in C++, Python and Go,
snapshot tooling, a segmented sequencer core, the pre-trade risk engine, the order manager, a
benchmark harness with regression gates, and a simulator harness that runs a synthetic day from
client order through risk, OMS, a stub router and a simulated venue and back, and proves it replays
bit for bit including every client report. The market-data stack is complete and is what the
simulator now runs on: line receivers with capture, the line arbitrator, the ITCH 5.0 and SIP
decoders, the direct-against-official NBBO comparison, per-feed journals keyed by venue sequence,
the source selector, and the book builder publishing to a shared-memory view. A simulated day is
ITCH bytes on two lines, arbitrated, decoded and built into a book, and the price the client
simulator trades around is the one the book holds. The smart order router is in: the simulator runs
on it, not on a stub, and every plan it makes is recomputed from the journal in cold replay. Not a
trading system yet: no venue or client gateways, and the router's dark, passive and peg strategies,
its Regulation NMS property tests and the TCA feedback loop are still to come. Two things
named in the delivery plan are still outstanding inside what is built: the ITCH golden runs against
a generated day rather than a published Nasdaq sample, and the SIP wire layout is a stand-in for
the licensed one. The design documents are in `docs/`: 1 is the system design, 2 the schema and
security master, 3 the gap analysis and component designs, 4 the delivery plan to go-live (every
remaining task as an assignable work item).

Everything here is exercised by one command:

    make test      # generate codecs, check schema, build C++, run all round-trip, determinism and simulator tests
    make bench     # every benchmark, JSON rows, compared to bench/baselines/<host-class>.json (informational)
    make benchgate # the same, failing on regression (p50 +5 %, throughput -5 %, allocations, syscalls on Linux)
    make bench-baseline  # record this host's baseline after a reviewed change (--pinned on a quiet host gates p99.9 too)
    make fuzz      # libFuzzer over journal recovery and the snapshot loader (clang)
    make gotest    # Go codec round trip (needs a Go toolchain)
    make baselines # regenerate simulator baselines after an intentional behaviour change
    make goldens   # regenerate the decoder golden after an intentional decoder or generator change

Requires g++ 13+ or clang 17+ (C++23), python3 with `blake3` (`pip install blake3`).
On macOS: `make test CXX=clang++` (Homebrew LLVM works; Apple clang needs a working SDK).

## What is here

| Path | Purpose |
|---|---|
| `schema/trading.xml` | The one schema: primitive aliases, 31 enums, 7 bit sets, 15 snapshot record composites, 73 messages. SBE-compatible XML. |
| `schema/CHANGELOG.md` | Every version bump and why. |
| `tools/sbegen.py` | Generator. Parses the schema, computes and verifies layouts (natural alignment, declared sizes, 16-byte message padding), emits `gen/cpp/trading.hpp`, `gen/py/trading.py`, `gen/go/trading.go`, `gen/layout.md`. |
| `tools/loginspect.py` | Look inside journals and frame logs: `dump` by sequence range or template, `chain` follows `causeSeq` backwards (the latency waterfall), `diff` two logs, `stats` per template and source. |
| `tools/schema_check.py OLD NEW` | Compatibility checker: append-only fields, frozen ids and offsets, frozen FrameHeader, version bump required. Exit 1 on any violation. |
| `tools/snapshot.py` | Reference-data snapshot: `build` from a JSON fixture with validation, `verify` (BLAKE3 section and content hashes plus semantic checks), `dump`, and `diff OLD NEW out.log` which emits the sequenced update messages that turn OLD into NEW. |
| `core/refdata/snapshot.hpp` | mmap loader: verifies magic, format, schema version, file size, every section hash and the content hash before any engine reads a byte. Typed section access, ticker lookup for the edge. |
| `core/md/packet.hpp` | The transport envelope a line carries, shaped like MoldUDP64: feed, line, the venue sequence of the first message inside, and how many follow. The payload is opaque bytes, so the receiver and the arbitrator work for every venue and only the decoder knows one protocol from another. |
| `core/md/capture.hpp` | Every packet a line delivered, on disk, unaltered, with its hardware receive time. The capture is the evidence and the simulator's input, so it stores what arrived rather than what we made of it. |
| `core/md/line_receiver.hpp` | One per line: stamps the receive time, captures before anything can reject the packet, hands it up. Does not parse and does not arbitrate. The lines of a feed share one capture, because the arbitrator's input is every line at once. |
| `core/md/arbitrator.hpp` | One per feed: merges its lines by venue sequence with first arrival winning, finds the holes neither line delivered, asks the venue's recovery service for exactly those, splices the answer in order, and falls back to a snapshot when the answer never comes. A recovery block that straddles what the consumer already has is passed on marked with how much of it is old, never repeated. Publishes `FeedStatus` (healthy, recovering, stale, down) with each line's latency against its sibling. Reads the envelope and never the payload, so it arbitrates every protocol. No clock of its own and no allocation after construction: the same arrivals produce the same stream, the same requests and the same status messages, run after run. |
| `core/md/itch.hpp` | Nasdaq TotalView-ITCH 5.0, at the published byte offsets and lengths. Order reference table and price-level aggregates, because our `BookDelta` carries the absolute quantity at a level after the change: a consumer that has to accumulate deltas cannot join late and cannot recover from a gap. Emits `BookDelta`, `Trade`, `SymbolStatus`, `Imbalance` and `VenueStatus`, with the last message of every packet marked. No clock, no allocation after construction, and the callback is a template rather than a `std::function`, because this is one call per message on every feed all day. |
| `core/md/sip.hpp` | The consolidated tape (UTP and CTA) for the official NBBO, and `NbboMonitor`, which is why it is carried: it compares what our direct feeds saw with what the tape says and writes one `NbboDivergence` per episode, naming the shape (a price difference, a crossed market, a side that has stopped) and how long it lasted. A tape that is merely late is not news and is not written. The wire layout here is a stand-in with the right shape, not the licensed one; encoder and decoder sit in the same file so substituting the real offsets is one change. |
| `core/md/feed_journal.hpp` | One feed's day on disk, keyed by the venue's own sequence: the arbitrated packet stream, which is the authority, and the decoded frames, which are a cache kept only because replaying them beats decoding again. Segmented, with a sparse index, so replay to a watermark is a seek — `bytesScanned()` reports what a replay actually read, so that is a number rather than a claim. Keys are not contiguous, not unique and may jump at a snapshot resynchronisation; what they may not do is go backwards. A torn tail is truncated on open, damage further back is an error, and `tests/fuzz/fuzz_feedjournal.cpp` runs the recovery scan over arbitrary bytes. |
| `core/md/selector.hpp` | What a consumer actually acts on. Deduplicates redundant publishers of one feed by identity — `(venueSeq, index within that sequence)`, the index recovered by counting rather than carried — so a publisher dying mid-packet costs nothing. Then chooses between sources that are not the same, by preference, moving off one that has gone quiet or spent too long recovering and refusing to move back until the returning one has held up. Every move is an `MdSourceSwitch` on the core stream with an `MdSwitchReason`, because a consumer that silently starts acting on top-of-book where it had depth will make a decision it cannot explain. |
| `core/md/book.hpp` | The book, and the window everything else reads it through. Each (symbol, venue, side) is 64 quantities indexed by tick from a base price with an occupancy mask beside them, so the best price is one instruction rather than a search; the window holds the touch and sixty-three ticks behind it, and follows the touch rather than the last price it saw. The consolidated top across venues, the NBBO from the direct feeds with the SIP's carried beside it, and `BookSnapshot` for late joiners. The state lives in a shared memory mapping, not in a private copy that is then published: one writer, no locks, a sequence number per record that readers read around, and a checksum so a torn read is caught rather than acted on. |
| `core/router/router.hpp` | The smart order router. Price priority first, because a rebate that buys its way past a better quote is a trade-through, so venues are ordered by price and net cost decides only within a price; where the sweep crosses several prices at once it marks the children ISO, which is lawful only because the better quotes are in the same plan. A better price it cannot reach stops the sweep rather than being stepped over. Then the cost model: price plus the venue's fee or rebate, weighted by the scorecard's fill rate with the shortfall charged at the fallback, plus measured reversion — signed so a buy and a sell are compared by the same code. Every plan records the runner-up and what it would have cost, which is the number a best-execution review asks for. Children carry a release offset so the slowest venue is not the last to see the order. |
| `sim/md_stack.hpp` | The whole market-data path wired together as the simulator's source of market data: ITCH bytes onto two lines, receivers, arbitrator, decoder, book. What the client simulator prices against is the top of book the builder holds, not a number a generator remembered. |
| `sim/itch_sim.hpp`, `sim/sip_sim.hpp` | A Nasdaq day on the wire, and a tape that is late, wrong, crossed or stopped. The ITCH generator only executes, cancels and replaces orders it added and never crosses its own book, so the orders it leaves live can be aggregated into levels and compared with the levels the decoder built one delta at a time. |
| `sim/feed_publisher.hpp` | A venue that misbehaves on purpose: the same messages down lines A and B, with per-line drops, duplicates, reordering and delay, plus the retransmit and snapshot services, and the ability to publish one canonical stream from two processes. |
| `core/md/identity.hpp` | What identifies a market-data message: the feed it came from and the sequence that feed's venue assigned, never one of ours. Two handlers of the same feed therefore produce identical streams, and a consumer can fail over between publishers with no gap and no duplicate. |
| `core/util/money.hpp` | Money on the wire is int64, which at 1e-8 units tops out at $92.2bn. The two messages carrying a firm-wide aggregate say which unit they used; engines work in 1e-8 in 128 bits, and the only hard ceiling left is what an `ExposureSnapshot` can carry, about $922 trillion. |
| `core/refdata/refdata.hpp` | Engine-side struct-of-arrays view: `load(snapshot)` at start of day, `apply(frame)` for `SymbolAdd`, `SymbolUpdate`, `ListUpdate`, `ShortSaleRestriction`, `TickTableUpdate`; O(1) hot accessors; `stateHash()` for checkpoints. No allocation after construction. |
| `core/util/flatmap.hpp` | Open-addressing tables keyed by our 64-bit ids (backward-shift delete, no tombstones) and a 64-bit fingerprint set; the order-path replacement for `std::unordered_map`. |
| `core/seq/journal.hpp` | Append-only, segmented frame journal per stream: a directory of 1 GiB segments (configurable), each validated on open, torn tail truncated on the last one, sparse index for replay-from-seq across segments. |
| `core/seq/ring.hpp` | Single-writer, multi-reader broadcast ring (the in-process stand-in for Aeron IPC): independent cursors, back-pressure when the slowest reader is a full ring behind. |
| `core/seq/sequencer.hpp` | The ordering core: stamps `seq`/`seqTs`/`streamId`, journals, publishes. Atomic `submitBatch` (all-or-nothing, `lastInBatch` flag) so an `MdWatermark` and the decision it explains are adjacent. Emits `SessionStart`, periodic durable `Checkpoint`, `Heartbeat`. Injected clock. |
| `core/seq/reader.hpp` | Consumer view with gap detection: any missing sequence is refilled from the journal (and flagged `replayed`), so consumers see every frame in order exactly once. The handler is a template, no indirect call per frame. |
| `core/seq/watermark.hpp` | Tracks the market-data position an engine has consumed (up to 16 streams) and fills one `MdWatermark` per eight, all sequenced in the decision's batch. |
| `tests/test_seq.cpp` | A live engine consumes a core stream and a market-data stream in random interleaving, making 20k risk verdicts and 17.7k routing decisions that depend on the book it saw. A cold consumer replays the core journal only, pulling market data to each watermark, and recomputes every decision exactly. Checkpoint hashes agree across live, late-start (journal refill) and cold. Also: batch adjacency, torn-tail recovery, back-pressure. |
| `schema/reasons.csv` | Reject and decision reason codes, generated into both codecs as `Reason`. |
| `schema/versions/` | Frozen copies of each released schema version; `make check` verifies the current schema is a compatible evolution of the previous one. |
| `core/oms/oms.hpp` | The order manager. Arena of 256-byte order slots (parents and children), open-addressing tables by id and by (account, session, clOrdId), the parent state machine as a data table (illegal transitions counted, never guessed), fee table from the snapshot plus `FeeScheduleUpdate`, NBBO on every client report. Accept, child ack and reject, partial fills and fills aggregated to the parent, cancel (by id or original clOrdId) and cancel reject, replace accepted (children cancelled and re-planned) and rejected, re-route via `OrderState` reason `RerouteRequired` with a budget, IOC, kill switch, bust. A cancel or replace on a parent whose router answer has not arrived waits for it: a child already on its way is never orphaned. No allocation after construction. |
| `tests/test_oms.cpp` | Table invariants, every lifecycle on purpose, arena exhaustion, illegal events, then 20k random events and a replay that regenerates every client report byte for byte with the same state hash. |
| `core/risk/risk.hpp` | Pre-trade risk engine. Deterministic state machine over the core stream: limit precedence (account+symbol > account > firm), kill switches (firm/account/strategy, with release), per-account message rate as a sliding one-second window on sequencer time, duplicate-order window (same symbol/side/qty/price), symbol status/halt/restricted, qty/notional/collar/tick checks, short-sale rules (shortable, ETB-or-locate with locate consumption and return on cancel, Rule 201 price test), open-order cap, gross/net exposure, buying power with margin classes; `evaluateReplace` re-checks the order as it would be after a replace; accounts from the snapshot and `PositionSnapshot`/`ExposureSnapshot` at start of session; fills move open notional into position. Wide arithmetic on notional, requirement and exposure sums with hard ceilings. |
| `tests/test_risk.cpp` | All 29 reject reasons exercised on purpose with the exact expected reason; locate, replace and buying-power accounting checked to the unit; then 200k random orders and 40k replaces, and a cold engine that replays the journal and must reproduce every verdict, reason and buying-power figure, plus the final state hash. |
| `sim/` | The simulator harness. `venue_sim.hpp` (level book, several sessions with throttles and cancel-on-disconnect, price-time matching, queue-position fills, drop copy), `feed_sim.hpp` (no longer the harness's market data; kept as a cheap message source for tests that are not about market data), `client_sim.hpp` (random and adversarial flow with cancels and replaces), `harness.hpp` (wires the streams, drives the day with the real risk engine and OMS, then replays the core journal cold and recomputes every decision, plan, placement, checkpoint hash and every OMS frame byte for byte), `run_sim.cpp` (scenarios, drills, baselines, `--bench` for front-to-back stage latencies), `scenarios/make_scenario.py` (a scenario is a log of external events). Baselines in `sim/baselines/`. |
| `core/util/bench.hpp`, `alloc_guard.hpp` | Block-timed recorder with percentiles and JSON rows; a counting allocator so benchmarks assert zero allocations after warm-up. `tools/bench.py` runs the suite, compares to the host-class baseline, and on Linux checks the order path makes no syscalls under strace. |
| `tests/test_md.cpp` | The transport layer: a day captured byte-exact and replayed identically, malformed and misaddressed packets counted but never captured, drops on one line covered by the other, drops on both filled by a targeted retransmit, duplicates and reordering survived, and two publishers of one canonical stream emitting byte-identical packets. |
| `tests/test_arb.cpp` | The line arbitrator. The first drill runs a merger that simply forwards every packet through the same checker and requires it to fail, so the eight that follow mean something: a clean day emitted once and in order, one line dying unnoticed, both lines lossy and recovered by targeted retransmit, a recovery service that frames its answer differently from the live stream, a recovery service that loses 40 percent of its own answer, duplicates and reordering, a retransmit that never comes and is escalated to a snapshot with the jump counted, the same impaired day byte-identical across two runs, and a silent feed going stale and then down on the caller's clock. |
| `tests/test_decode.cpp` | The decoders. Every ITCH message type on the wire at the published length; the book invariant, where the levels built one delta at a time must equal the aggregate of the orders the venue actually left live; the order reference table against the venue's own live set; packets with a skip honoured and the end of each marked; the same bytes twice producing an identical hash, held against `tests/golden/itch-day.json`; and input a decoder has to survive rather than trust. Then the tape: quotes with the national best, prints, trading actions, and a divergence monitor that must stay silent for a tape that is only late and must name all three shapes for one that is wrong, crossed and stopped. |
| `tests/test_selector.cpp` | The per-feed journals and the selector. A day written and replayed back byte for byte; replay to a watermark exact and reading under 40 % of the journal; a resynchronisation recorded as a jump while a backwards key is refused; a torn tail losing exactly the torn record and the journal carrying on; damage further back rejected. Then a million messages from two publishers of one feed, every one forwarded exactly once with one killed midway, several messages sharing a venue sequence told apart, the direct feed stalling and the tape taking over with the switch on the core stream, no flapping back until it has held up, and the same switches on a second run. |
| `tests/test_router.cpp` | Named market shapes with an answer that can be worked out by hand: the cost model checked against arithmetic written in the test rather than against its own output; a venue with a rebate big enough to beat a penny, which still loses to the better price; the same price on two venues, where the rebate does decide; a sweep across three prices with ISO set and the parent's leaves never exceeded; a better price with no session, which stops the sweep; a remainder rested where posting pays best; post-only taking nothing; and release offsets that land the children together. |
| `tests/test_book.cpp` | The book builder. A recorded day with the invariants checked as it runs rather than at the close — no venue book crossed, the consolidated top equal to the best across venues, the top equal to the ladder's top; the ladders against the venue's own live orders; three reader threads hammering the segment through a million writer updates with zero torn reads; a second mapping of the segment agreeing; snapshots carrying the touch first; and a market that walks out of its window, re-basing and saying what it dropped. |
| `tests/fuzz/` | libFuzzer targets for journal recovery, the snapshot loader, and the per-feed journal's recovery scan. |
| `tests/go/` | Go codec round trip against the C++ log. |
| `tests/test_codec.cpp`, `tests/test_codec.py` | Cross-language round trip. C++ writes a five-frame log, Python decodes it and walks the `causeSeq` chain; Python writes the same log, C++ decodes it; the two files are byte-identical. |
| `tests/test_refdata.cpp` | Determinism: load day 1, apply the 22-message diff, state hash equals a fresh load of day 2. A tampered snapshot is rejected. |
| `fixtures/make_fixtures.py` | Two consecutive business days of reference data with an IPO, a delisting, a 4:1 split, a tick-table change, list moves, a fee change. |
| `third_party/blake3/` | BLAKE3 reference C implementation (Apache 2.0 / CC0), portable build. |

## Design rules the tools enforce

* Every field is naturally aligned; padding is explicit in the schema, never inserted silently. The generator fails the build otherwise.
* Declared `blockLength` and composite `length` must equal the computed layout. (This caught three arithmetic errors in the first hand-written draft.)
* Every message is a multiple of 16 bytes. `FrameHeader` is 48 bytes and can never change.
* Generated C++ is plain trivially-copyable structs with a `static_assert(offsetof(...))` for every field; decode is a pointer cast (`as<NewOrder>(header)`).
* Python codecs are generated from the same file and produce byte-identical output.
* A snapshot is loaded only after its hash is verified; intraday changes arrive only as sequenced messages; `snapshot.py diff` is how a correction is delivered, never a reload.

## On SBE

The schema is written in SBE's XML dialect so the official `sbe-tool` can generate codecs for Java, Go, Rust and C# when needed. This repository ships its own generator for the subset we use (fixed-length messages, no groups or var-data) because it lets us enforce the explicit-offset rules above at generation time, and because the official jar is distributed via Maven Central, which the build environment used to produce this increment could not reach. The two are interchangeable for the wire format; ours is stricter.

## Benchmarks and guardrails

Three levels, as document 3 specifies. Component benchmarks (`bench`, `bench_seq`, `bench_risk`,
`bench_oms`) time blocks of operations and report p50, p99, p99.9 and max as JSON rows, and fail
if the measured path allocates. Front to back, `run_sim --bench` runs the day on a real clock and
derives every stage's latency from the journal alone: `originTs` to `seqTs` for ingress, then each
`causeSeq` hop (order to decision, decision to child, child to venue ack, venue fill to client
report). End to end wire-to-wire waits for the gateways. `tools/bench.py` compares every row with
`bench/baselines/<host-class>.json`: p50 may grow 5 %, throughput may fall 5 %, allocations may not
grow, and p99.9 is gated only when the baseline was recorded with `--pinned` on a quiet host,
because on a laptop it is scheduler noise. Baselines move only by a reviewed commit.

## The simulator

`run_sim` runs a scenario (random flow with a seed, adversarial flow, or a scripted event log made by
`sim/scenarios/make_scenario.py`) through one core stream and one market-data stream: client orders,
cancels and replaces go through the risk engine, the OMS owns the parent, a stub router places one
child per accepted order (and per re-route request) on the least loaded venue session, the venue
simulator acks, fills or rests it, the OMS aggregates venue-level reports into client-level ones
with fees and the NBBO, and the risk engine books the fills. The sequencer
checkpoints periodically and the engine answers each `Checkpoint` with a `CheckpointAck` carrying its
state hash. Then the journal is replayed cold: market data is pulled to each `MdWatermark`, every
risk verdict, routing plan and session placement is recomputed and compared to what was sequenced,
and every `CheckpointAck` hash must match. Drills: firm kill switch mid-day, a venue session dropped
and restored (cancel-on-disconnect, re-placement on the survivors), a 2 500-order burst against the
rate limit, and a torn journal tail with sequencer resumption. Each scenario has a stored baseline
(counts and final hash); `make test` fails on any drift and `make baselines` regenerates them after a
reviewed change.

## Numbers (single-core sandbox, no pinning, portable BLAKE3, buffered journal)

    decode + dispatch + five reference-data checks:  ~24 ns per order
    apply ListUpdate:                                 ~2 ns
    stateHash over hot arrays (checkpoint):           ~230 us
    sequencer (stamp + journal write + ring publish): ~190 ns per frame, 5.2 M frames/s
    journal replay:                                   ~54 ns per frame
    risk evaluate, full check list, accept path:      ~42 ns per order; ~136 ns with the cancel/close path
                                                      (the day-long duplicate-clOrdId set costs one cache miss per order)
    oms transition (order, decision, child, ack, fill): ~18 ns per sequenced frame, zero allocations
    market-data receive, validate and hand up:        ~5 ns per packet
    the same, plus writing the capture:               ~65 ns per packet (~500 ns before the capture
                                                      buffer was sized; the benchmark found that)
    line arbitration, healthy A/B feed:               ~7 ns per packet off one line, zero allocations
                                                      (one copy accepted, the sibling's discarded)
    the same, every pair arriving back to front:      ~13 ns per packet through the reorder buffer
    ITCH 5.0 decode:                                  ~58 ns per message, zero allocations; document 3
                                                      asks for 3 M messages/s per feed process and
                                                      bench_decode fails below it
    SIP quote decode:                                 ~40 ns per message
    direct against official NBBO:                     ~4 ns per update, worst case (an episode opened
                                                      and closed on every one)
    source selector, two publishers:                  ~3 ns per arrival, zero allocations
    feed journal append:                              ~50 ns per packet (~480 ns before the write
                                                      buffer was sized; the benchmark found that too)
    feed journal replay of the last tenth:            reads 10.2 % of the journal, so it seeks
    packet to published top of book:                  ~660 ns at p50 for a packet of eight messages,
                                                      decoded, applied to the ladders and published
                                                      to shared memory (document 3 asks for 2 us)
    a reader copying the top of book:                 ~4.6 ns, and never blocking the writer
    router plan, one venue:                           ~180 ns per accepted parent, zero allocations
    router plan, four-venue sweep:                    ~260 ns, including the sort and the ISO decision
    ring hand-off across threads:                     not measurable on one core; needs a pinned two-core host

Throughput figures on an unpinned laptop swing by about half between runs of the same binary, which
is why `tools/bench.py` gates p99.9 only against a baseline recorded with `--pinned`, and why the
delivery plan has an item for a pinned CI runner. The front-to-back rows swing further still,
because they are queueing latencies through a whole simulated day rather than a tight loop.

## Schema evolution in practice

v2 added `KillSwitch.release` (there was no way to lift a kill). The change went through
`schema_check.py` against the frozen v1 copy, the v1 log written before the change still
decodes under the v2 codec (`test_codec read build/cpp.log`), and the engine treats a v1
frame's zero-default as a kill. That is the whole workflow for every future field.

## Known limits to fix before production

* SSR handling rejects rather than re-pricing the short sale to bid + one tick, which is
  what most brokers do; re-pricing needs the OMS.

## On Aeron

The design puts transport and archive on Aeron. `BroadcastRing` and `Journal` have the same shape (one publisher, independent subscriber cursors, replay by position) and sit behind the `Sequencer`/`StreamReader` interfaces so Aeron IPC/UDP and Aeron Archive drop in where they are today. The ordering logic, batching, watermarks and checkpoints, which are the parts that must be ours, do not change.

## Next (v0.3, see docs/03)

1. `core/md`: line receiver, line arbitrator, decoders (ITCH, SIP), source selector, book builder; per-feed journals keyed by venue sequence.
2. `core/router`: the strategy library over the book view and `VenueScorecard`, replacing `sim/stub_router.hpp`.
3. OMS: Day expiry through sequenced timers, GTC carry, venue-side replace once a venue protocol supports it.
