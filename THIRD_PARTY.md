# Third-party code

| Component | Path | Licence | Notes |
|---|---|---|---|
| BLAKE3 reference C implementation | `third_party/blake3/` | Apache-2.0 (also CC0) | Portable build only; SIMD backends disabled via `BLAKE3_NO_*`. Licence text in `third_party/blake3/LICENSE_A2`. |
| Simple Binary Encoding (schema dialect) | `schema/trading.xml` | n/a | The XML dialect is the FIX Trading Community SBE standard; no SBE code is vendored. |
| blake3 (Python package) | build-time dependency | Apache-2.0 / CC0 | Used by `tools/snapshot.py`. |
