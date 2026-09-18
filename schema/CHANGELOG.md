# Schema changelog

Every version bump is recorded here with the reason. `tools/schema_check.py OLD NEW`
must pass before a bump merges.

## v2 (2026-09-16)
`KillSwitch.release` (uint8, sinceVersion 2) added out of trailing padding: a kill switch
can now be lifted by the same message with release=1. Old readers ignore the field and
see a kill; new readers decoding v1 frames see release=0. No other change.

## v1 (2026-09-15)
Initial schema. 64 messages across control (1-99), orders (100-199), market data
(200-299), reference (300-399), risk (400-499), market making (500-599), positions
(600-699), stock loan (700-799), strategy (800-899), telemetry (900-999). 14 snapshot
record composites. FrameHeader frozen at 48 bytes.
