# Trading stack: schema, codecs, snapshot tools, reference data. One command: make test
CXX      ?= g++
CC       ?= gcc
CXXFLAGS ?= -std=c++23 -O2 -Wall -Wextra -Werror -pedantic
B3FLAGS   = -DBLAKE3_NO_SSE2 -DBLAKE3_NO_SSE41 -DBLAKE3_NO_AVX2 -DBLAKE3_NO_AVX512
INC       = -Igen/cpp -Icore/refdata -Icore/seq -Icore/risk -Ithird_party/blake3
B3OBJ     = build/blake3.o build/blake3_dispatch.o build/blake3_portable.o

.PHONY: all gen check test clean bench

all: gen build/test_codec build/test_refdata build/test_seq build/test_risk build/bench build/bench_seq build/bench_risk

gen: gen/cpp/trading.hpp
gen/cpp/trading.hpp gen/py/trading.py gen/layout.md: schema/trading.xml schema/reasons.csv tools/sbegen.py
	python3 tools/sbegen.py schema/trading.xml gen

check:
	python3 tools/schema_check.py schema/versions/trading-v1.xml schema/trading.xml

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
	@echo "== all tests passed"

bench: build/bench build/bench_seq build/refdata-20260915-v1.bin
	./build/bench build/refdata-20260915-v1.bin
	./build/bench_seq build/seqbench
	./build/bench_risk build/refdata-20260915-v1.bin

clean:
	rm -rf build gen/cpp gen/py gen/layout.md
