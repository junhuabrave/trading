# Trading stack: schema, codecs, snapshot tools, reference data. One command: make test
CXX      ?= g++
CC       ?= gcc
CXXFLAGS ?= -std=c++23 -O2 -Wall -Wextra -Werror -pedantic
B3FLAGS   = -DBLAKE3_NO_SSE2 -DBLAKE3_NO_SSE41 -DBLAKE3_NO_AVX2 -DBLAKE3_NO_AVX512 -DBLAKE3_USE_NEON=0
INC       = -Igen/cpp -Icore/util -Icore/refdata -Icore/seq -Icore/risk -Icore/oms -Icore/md -Isim -Ithird_party/blake3
B3OBJ     = build/blake3.o build/blake3_dispatch.o build/blake3_portable.o

.PHONY: all gen check test clean bench benchgate bench-baseline fuzz gotest baselines

all: gen build/test_codec build/test_refdata build/test_seq build/test_risk build/test_oms build/run_sim build/bench build/bench_seq build/bench_risk build/bench_oms

gen: gen/cpp/trading.hpp
gen/cpp/trading.hpp gen/py/trading.py gen/go/trading.go gen/layout.md: schema/trading.xml schema/reasons.csv tools/sbegen.py
	python3 tools/sbegen.py schema/trading.xml gen

check:
	python3 tools/schema_check.py schema/versions/trading-v1.xml schema/versions/trading-v2.xml
	python3 tools/schema_check.py schema/versions/trading-v2.xml schema/versions/trading-v3.xml
	python3 tools/schema_check.py schema/versions/trading-v3.xml schema/trading.xml

build:
	mkdir -p build

build/%.o: third_party/blake3/%.c | build
	$(CC) -O2 $(B3FLAGS) -Ithird_party/blake3 -c $< -o $@

build/test_codec: tests/test_codec.cpp gen/cpp/trading.hpp | build
	$(CXX) $(CXXFLAGS) $(INC) $< -o $@

build/test_refdata: tests/test_refdata.cpp core/refdata/*.hpp gen/cpp/trading.hpp $(B3OBJ) | build
	$(CXX) $(CXXFLAGS) $(INC) $< $(B3OBJ) -o $@

build/test_seq: tests/test_seq.cpp core/seq/*.hpp core/refdata/*.hpp gen/cpp/trading.hpp $(B3OBJ) | build
	$(CXX) $(CXXFLAGS) $(INC) $< $(B3OBJ) -o $@

build/bench_seq: tests/bench_seq.cpp core/seq/*.hpp gen/cpp/trading.hpp | build
	$(CXX) $(CXXFLAGS) $(INC) $< -o $@ -lpthread

build/test_risk: tests/test_risk.cpp core/risk/*.hpp core/seq/*.hpp core/refdata/*.hpp gen/cpp/trading.hpp $(B3OBJ) | build
	$(CXX) $(CXXFLAGS) $(INC) $< $(B3OBJ) -o $@

build/run_sim: sim/run_sim.cpp sim/*.hpp core/md/*.hpp core/oms/*.hpp core/risk/*.hpp core/seq/*.hpp core/refdata/*.hpp core/util/*.hpp gen/cpp/trading.hpp $(B3OBJ) | build
	$(CXX) $(CXXFLAGS) $(INC) $< $(B3OBJ) -o $@

build/test_oms: tests/test_oms.cpp core/oms/*.hpp core/refdata/*.hpp core/util/*.hpp gen/cpp/trading.hpp $(B3OBJ) | build
	$(CXX) $(CXXFLAGS) $(INC) $< $(B3OBJ) -o $@

build/bench_oms: tests/bench_oms.cpp core/oms/*.hpp core/refdata/*.hpp core/util/*.hpp gen/cpp/trading.hpp $(B3OBJ) | build
	$(CXX) $(CXXFLAGS) $(INC) $< $(B3OBJ) -o $@

build/bench_risk: tests/bench_risk.cpp core/risk/*.hpp core/refdata/*.hpp gen/cpp/trading.hpp $(B3OBJ) | build
	$(CXX) $(CXXFLAGS) $(INC) $< $(B3OBJ) -o $@

build/bench: tests/bench.cpp core/refdata/*.hpp gen/cpp/trading.hpp $(B3OBJ) | build
	$(CXX) $(CXXFLAGS) $(INC) $< $(B3OBJ) -o $@

fixtures/day1.json fixtures/day2.json: fixtures/make_fixtures.py
	python3 fixtures/make_fixtures.py

build/refdata-20260915-v1.bin: fixtures/day1.json gen/py/trading.py tools/snapshot.py | build
	python3 tools/snapshot.py build fixtures/day1.json $@ --date 20260915 --version 1
build/refdata-20260916-v1.bin: fixtures/day2.json gen/py/trading.py tools/snapshot.py | build
	python3 tools/snapshot.py build fixtures/day2.json $@ --date 20260916 --version 1
build/refdata-diff.log: build/refdata-20260915-v1.bin build/refdata-20260916-v1.bin
	python3 tools/snapshot.py diff build/refdata-20260915-v1.bin build/refdata-20260916-v1.bin $@

test: check all build/refdata-diff.log
	@echo "== codec: C++ <-> Python round trip"
	./build/test_codec write build/cpp.log
	python3 tests/test_codec.py sizes gen/cpp/trading.hpp
	python3 tests/test_codec.py read build/cpp.log
	python3 tests/test_codec.py write build/py.log
	./build/test_codec read build/py.log
	cmp build/cpp.log build/py.log && echo "logs byte-identical"
	@echo "== snapshot: verify + refdata determinism"
	python3 tools/snapshot.py verify build/refdata-20260915-v1.bin
	python3 tools/snapshot.py verify build/refdata-20260916-v1.bin
	./build/test_refdata build/refdata-20260915-v1.bin build/refdata-20260916-v1.bin build/refdata-diff.log
	@echo "== sequencer: replay determinism, gap refill, crash recovery, back-pressure"
	./build/test_seq build/refdata-20260915-v1.bin build/seqtest
	@echo "== risk: every reject reason, credit maths, replay determinism"
	./build/test_risk build/refdata-20260915-v1.bin build/risktest
	@echo "== oms: state table, every lifecycle, replay regenerates every client report"
	./build/test_oms build/refdata-20260915-v1.bin build/omstest
	python3 tools/reason_coverage.py build/risktest/risk.jnl build/omstest/oms.log
	@echo "== simulator: golden runs against stored baselines, drills, scripted scenario"
	./build/run_sim build/refdata-20260915-v1.bin build/sim/random-day --scenario random-day --steps 6000 --baseline sim/baselines/random-day.json
	./build/run_sim build/refdata-20260915-v1.bin build/sim/adversarial --scenario adversarial --steps 4000 --adversarial --baseline sim/baselines/adversarial.json
	./build/run_sim build/refdata-20260915-v1.bin build/sim/kill --scenario kill --steps 3000 --drill kill --baseline sim/baselines/kill.json
	./build/run_sim build/refdata-20260915-v1.bin build/sim/session-drop --scenario session-drop --steps 3000 --drill session-drop --baseline sim/baselines/session-drop.json
	./build/run_sim build/refdata-20260915-v1.bin build/sim/rate-burst --scenario rate-burst --steps 2000 --drill rate-burst
	./build/run_sim build/refdata-20260915-v1.bin build/sim/torn-tail --scenario torn-tail --steps 1000 --drill torn-tail
	python3 sim/scenarios/make_scenario.py build/refdata-20260915-v1.bin build/sim/scripted.log --orders 1500
	./build/run_sim build/refdata-20260915-v1.bin build/sim/scripted --scenario scripted --events build/sim/scripted.log --baseline sim/baselines/scripted.json
	@echo "== all tests passed"

# Benchmarks: every binary prints JSON rows; tools/bench.py collects, compares to
# bench/baselines/<host-class>.json. `bench` is informational, `benchgate` fails on regression
# (use on a pinned host), `bench-baseline` records a new baseline after a reviewed change.
BENCH_BINS = build/bench build/bench_seq build/bench_risk build/bench_oms build/run_sim
bench: $(BENCH_BINS) build/refdata-20260915-v1.bin
	python3 tools/bench.py run
benchgate: $(BENCH_BINS) build/refdata-20260915-v1.bin
	python3 tools/bench.py run --gate --strace
bench-baseline: $(BENCH_BINS) build/refdata-20260915-v1.bin
	python3 tools/bench.py run --write

# libFuzzer targets (clang only): journal recovery and the snapshot loader over corrupt input
fuzz: build/fuzz_journal build/fuzz_snapshot
	./build/fuzz_journal -max_total_time=$(FUZZ_SECONDS) -max_len=8192
	./build/fuzz_snapshot -max_total_time=$(FUZZ_SECONDS) -max_len=8192
FUZZ_SECONDS ?= 30
build/fuzz_%: tests/fuzz/fuzz_%.cpp core/seq/*.hpp core/refdata/*.hpp gen/cpp/trading.hpp $(B3OBJ) | build
	$(CXX) -std=c++23 -O1 -g -fsanitize=fuzzer,address,undefined $(INC) $< $(B3OBJ) -o $@

# Go codec round trip (needs a Go toolchain; CI runs it)
gotest: gen build/cpp.log
	cd tests/go && go vet ./... && TRADING_BUILD=$(CURDIR)/build go test -v ./...
	cmp build/cpp.log build/go.log && echo "go log byte-identical"
build/cpp.log: build/test_codec
	./build/test_codec write build/cpp.log

clean:
	rm -rf build gen/cpp gen/py gen/go gen/layout.md

# Regenerate the simulator baselines after an intentional behaviour change (review the diff).
baselines: build/run_sim build/refdata-20260915-v1.bin
	./build/run_sim build/refdata-20260915-v1.bin build/sim/random-day --scenario random-day --steps 6000 --write-baseline sim/baselines/random-day.json
	./build/run_sim build/refdata-20260915-v1.bin build/sim/adversarial --scenario adversarial --steps 4000 --adversarial --write-baseline sim/baselines/adversarial.json
	./build/run_sim build/refdata-20260915-v1.bin build/sim/kill --scenario kill --steps 3000 --drill kill --write-baseline sim/baselines/kill.json
	./build/run_sim build/refdata-20260915-v1.bin build/sim/session-drop --scenario session-drop --steps 3000 --drill session-drop --write-baseline sim/baselines/session-drop.json
	python3 sim/scenarios/make_scenario.py build/refdata-20260915-v1.bin build/sim/scripted.log --orders 1500
	./build/run_sim build/refdata-20260915-v1.bin build/sim/scripted --scenario scripted --events build/sim/scripted.log --write-baseline sim/baselines/scripted.json
