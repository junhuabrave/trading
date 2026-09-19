# Schema changelog

Every version bump is recorded here with the reason. `tools/schema_check.py OLD NEW`
must pass before a bump merges.

## v4 (2026-09-19)
Frozen v3 copy under `schema/versions/trading-v3.xml`. Market-data identity moves from the sequence
we assign to the one the venue assigned, so that redundant feed handlers produce identical streams
and a consumer can fail over between publishers with no gap and no duplicate (MD-11).
* `venueSeq` (uint64, sinceVersion 4) appended to `Nbbo`, `Imbalance` and `SymbolStatus` out of each
  message's trailing `Pad8`. `BookDelta`, `Trade` and `BookSnapshot` already carried one. No block
  length changed; a v3 reader sees zero, which the engines treat as "no identity".
* `MdWatermark` is re-documented rather than renamed: `streamIds` carries the feedId and `seqs` the
  venue's sequence. The field names are frozen by the compatibility checker and a rename would buy
  nothing.

## v3 (2026-09-18)
Frozen v2 copy under `schema/versions/trading-v2.xml`. All additions append-only; no block length changed.
* `CheckpointAck` (13): an engine's state hash after a `Checkpoint`, so divergence is visible in production.
* `ClockSync` (14), `MdSourceSwitch` (15): cross-host time, and an engine's change of market-data source.
* `VenueSessionStatus` (110) and `ChildOrder.venueSessionIdx` (out of trailing pad): venue session pools.
* `BookSnapshot` (205), `FeedStatus` (207): late-join recovery and line-arbitrator health for the market-data layering.
* `VenueScorecard` (308), `RoutingProfileUpdate` (309): router inputs from TCA and the control plane.
* `LimitUpdate.dupWindowMs` (out of trailing pad): duplicate-order window; 0 keeps the old behaviour.
* `FeedRecord` composite, snapshot section 12 (feeds). `ComponentRole.DropCopy`. `OrderFlags.ssrRepriced`.
* Enums `MdQuality`, `FeedState`, `FeedKind`, `SessionState`, `RouteStrategy`; set `FeedProvides`.
* Reason 118 `DuplicateOrder`.
* Generator now emits Go (`gen/go`) and `MAX_BLOCK_LENGTH`.

## v2 (2026-09-16)
`KillSwitch.release` (uint8, sinceVersion 2) added out of trailing padding: a kill switch
can now be lifted by the same message with release=1. Old readers ignore the field and
see a kill; new readers decoding v1 frames see release=0. No other change.

## v1 (2026-09-15)
Initial schema. 64 messages across control (1-99), orders (100-199), market data
(200-299), reference (300-399), risk (400-499), market making (500-599), positions
(600-699), stock loan (700-799), strategy (800-899), telemetry (900-999). 14 snapshot
record composites. FrameHeader frozen at 48 bytes.
