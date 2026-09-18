# Trading stack

**Status: v0.2.0, pre-alpha infrastructure.** Schema (v3), codecs in C++, Python and Go, snapshot
tooling, a segmented sequencer core, the pre-trade risk engine, and a simulator harness that runs a
synthetic day from client order to simulated venue and back and proves it replays bit for bit. Not a
trading system yet: no OMS, router, venue gateways or market-data handlers; the harness uses explicit
stand-ins for them. The design documents are in `docs/`; document 3 is the gap analysis and the plan.

Everything here is exercised by one command:

    make test      # generate codecs, check schema, build C++, run all round-trip, determinism and simulator tests
    make bench     # rough single-core numbers for decode/dispatch, sequencer, risk
    make fuzz      # libFuzzer over journal recovery and the snapshot loader (clang)
    make gotest    # Go codec round trip (needs a Go toolchain)
    make baselines # regenerate simulator baselines after an intentional behaviour change

Requires g++ 13+ or clang 17+ (C++23), python3 with `blake3` (`pip install blake3`).
On macOS: `make test CXX=clang++` (Homebrew LLVM works; Apple clang needs a working SDK).

## What is here

| Path | Purpose |
|---|---|
| `schema/trading.xml` | The one schema: primitive aliases, 23 enums, 6 bit sets, 14 snapshot record composites, 64 messages. SBE-compatible XML. |
| `schema/CHANGELOG.md` | Every version bump and why. |
| `tools/sbegen.py` | Generator. Parses the schema, computes and verifies layouts (natural alignment, declared sizes, 16-byte message padding), emits `gen/cpp/trading.hpp`, `gen/py/trading.py`, `gen/go/trading.go`, `gen/layout.md`. |
| `tools/loginspect.py` | Look inside journals and frame logs: `dump` by sequence range or template, `chain` follows `causeSeq` backwards (the latency waterfall), `diff` two logs, `stats` per template and source. |
| `tools/schema_check.py OLD NEW` | Compatibility checker: append-only fields, frozen ids and offsets, frozen FrameHeader, version bump required. Exit 1 on any violation. |
| `tools/snapshot.py` | Reference-data snapshot: `build` from a JSON fixture with validation, `verify` (BLAKE3 section and content hashes plus semantic checks), `dump`, and `diff OLD NEW out.log` which emits the sequenced update messages that turn OLD into NEW. |
| `core/refdata/snapshot.hpp` | mmap loader: verifies magic, format, schema version, file size, every section hash and the content hash before any engine reads a byte. Typed section access, ticker lookup for the edge. |
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
| `core/risk/risk.hpp` | Pre-trade risk engine. Deterministic state machine over the core stream: limit precedence (account+symbol > account > firm), kill switches (firm/account/strategy, with release), per-account message rate as a sliding one-second window on sequencer time, duplicate-order window (same symbol/side/qty/price), symbol status/halt/restricted, qty/notional/collar/tick checks, short-sale rules (shortable, ETB-or-locate with locate consumption and return on cancel, Rule 201 price test), open-order cap, gross/net exposure, buying power with margin classes; `evaluateReplace` re-checks the order as it would be after a replace; accounts from the snapshot and `PositionSnapshot`/`ExposureSnapshot` at start of session; fills move open notional into position. Wide arithmetic on notional, requirement and exposure sums with hard ceilings. |
| `tests/test_risk.cpp` | All 29 reject reasons exercised on purpose with the exact expected reason; locate, replace and buying-power accounting checked to the unit; then 200k random orders and 40k replaces, and a cold engine that replays the journal and must reproduce every verdict, reason and buying-power figure, plus the final state hash. |
| `sim/` | The simulator harness. `venue_sim.hpp` (level book, several sessions with throttles and cancel-on-disconnect, price-time matching, queue-position fills, drop copy), `feed_sim.hpp`, `client_sim.hpp` (random and adversarial flow), `oms_lite.hpp` and `stub_router.hpp` (explicit stand-ins for `core/oms` and `core/router`), `harness.hpp` (wires the streams, drives the day, then replays the core journal cold and recomputes every decision, plan and checkpoint hash), `run_sim.cpp` (scenarios, drills, baselines), `scenarios/make_scenario.py` (a scenario is a log of external events). Baselines in `sim/baselines/`. |
| `tests/fuzz/` | libFuzzer targets for journal recovery and the snapshot loader. |
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

## The simulator

`run_sim` runs a scenario (random flow with a seed, adversarial flow, or a scripted event log made by
`sim/scenarios/make_scenario.py`) through one core stream and one market-data stream: client orders
and cancels go through the risk engine, a stub router places one child per accepted order on the
least loaded venue session, the venue simulator acks, fills or rests it, the OMS stand-in aggregates
venue-level reports into client-level ones, and the risk engine books the fills. The sequencer
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
    ring hand-off across threads:                     not measurable on one core; needs a pinned two-core host

## Schema evolution in practice

v2 added `KillSwitch.release` (there was no way to lift a kill). The change went through
`schema_check.py` against the frozen v1 copy, the v1 log written before the change still
decodes under the v2 codec (`test_codec read build/cpp.log`), and the engine treats a v1
frame's zero-default as a kill. That is the whole workflow for every future field.

## Known limits to fix before production

* Money and notional are int64 in 1e-8 units, so any single aggregate caps at $92 billion.
  Per-account that is fine; firm-wide gross exposure at a large broker is not. The
  engine computes in 128-bit and rejects at a hard ceiling rather than overflowing, but
  the aggregate fields should move to 1e-4 units (or 128-bit) in the next schema version.
* SSR handling rejects rather than re-pricing the short sale to bid + one tick, which is
  what most brokers do; re-pricing needs the OMS.

## On Aeron

The design puts transport and archive on Aeron. `BroadcastRing` and `Journal` have the same shape (one publisher, independent subscriber cursors, replay by position) and sit behind the `Sequencer`/`StreamReader` interfaces so Aeron IPC/UDP and Aeron Archive drop in where they are today. The ordering logic, batching, watermarks and checkpoints, which are the parts that must be ours, do not change.

## Next (v0.3, see docs/03)

1. `core/oms`: the real parent/child state machine, replacing `sim/oms_lite.hpp`: cancel/replace, fee model over `FeeRecord`, `OrderState` emission, NBBO on every client report.
2. `core/md`: line receiver, line arbitrator, decoders (ITCH, SIP), source selector, book builder; per-feed journals keyed by venue sequence.
3. `core/router`: the strategy library over the book view and `VenueScorecard`, replacing `sim/stub_router.hpp`.
4. Benchmark harness with stored baselines and regression gates.
