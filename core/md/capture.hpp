// core/md/capture.hpp : every packet a line ever delivered, on disk, unaltered.
//
// The capture is the evidence. It is what the simulator replays, what a dispute about what the
// market showed is settled from, and what a decoder is re-run against when it turns out to have had
// a bug. So it stores the packet exactly as it arrived, with the receive timestamp beside it, and
// never anything derived: a capture that has been through our parsing is not evidence of what the
// venue sent, only of what we thought it sent.
//
// File = 64-byte header + records, each a 32-byte record header then the packet bytes.
#pragma once
#include "packet.hpp"
#include <cstdio>
#include <string>
#include <stdexcept>
#include <vector>
#include <unistd.h>
#include <fcntl.h>

namespace trading::md {

struct CaptureHeader {
    char     magic[4];        // "MDCP"
    uint32_t formatVersion;   // 1
    uint32_t feedId;
    uint32_t businessDate;    // yyyymmdd
    int64_t  openedTs;
    uint8_t  reserved[40];
};
static_assert(sizeof(CaptureHeader) == 64);

struct CaptureRecord {
    int64_t  recvTs;          // hardware receive time, the same value that reaches originTs
    uint32_t length;          // packet bytes following this record header
    uint16_t lineId;
    uint16_t flags;           // copied from the packet, so a scan can skip recovery traffic
    uint64_t recordSeq;       // position in this capture, 1-based
    uint8_t  reserved[8];
};
static_assert(sizeof(CaptureRecord) == 32);

class CaptureWriter {
public:
    // A busy feed delivers millions of packets a second and the receive thread pays this cost on
    // every one, so the buffer is sized to make the write a memcpy and the syscall rare. Measured
    // on this machine: about 500 ns per packet with stdio's default buffer, 65 ns with this one.
    // Receiving without capturing is about 5 ns, so capture still dominates the receive path; a
    // feed hot enough for that to matter wants the capture on its own thread behind a ring, which
    // is a change to where this runs rather than to what it does.
    static constexpr size_t BUFFER_BYTES = 4u << 20;

    CaptureWriter(const std::string& path, uint32_t feedId, uint32_t businessDate, int64_t openedTs)
        : path_(path), iobuf_(BUFFER_BYTES) {
        fp_ = std::fopen(path.c_str(), "wb");
        if (!fp_) throw std::runtime_error("capture: cannot create " + path);
        std::setvbuf(fp_, reinterpret_cast<char*>(iobuf_.data()), _IOFBF, iobuf_.size());
        CaptureHeader h{}; std::memcpy(h.magic, "MDCP", 4); h.formatVersion = 1;
        h.feedId = feedId; h.businessDate = businessDate; h.openedTs = openedTs;
        if (std::fwrite(&h, sizeof h, 1, fp_) != 1) throw std::runtime_error("capture: header write failed");
    }
    ~CaptureWriter() { close(); }
    CaptureWriter(const CaptureWriter&) = delete; CaptureWriter& operator=(const CaptureWriter&) = delete;

    void write(const void* packet, size_t len, uint16_t lineId, int64_t recvTs) {
        if (!fp_) throw std::logic_error("capture: closed");
        CaptureRecord r{}; r.recvTs = recvTs; r.length = uint32_t(len); r.lineId = lineId;
        r.flags = len >= sizeof(PacketHeader) ? static_cast<const PacketHeader*>(packet)->flags : 0;
        r.recordSeq = ++records_;
        // stdio copies into the big buffer above; the syscall happens only when it fills
        if (std::fwrite(&r, sizeof r, 1, fp_) != 1 || std::fwrite(packet, len, 1, fp_) != 1)
            throw std::runtime_error("capture: write failed");
        bytes_ += sizeof r + len;
    }
    void close() noexcept {
        if (!fp_) return;
        std::fflush(fp_);
#if defined(__APPLE__)
        if (::fcntl(::fileno(fp_), F_FULLFSYNC) != 0) ::fsync(::fileno(fp_));
#else
        ::fdatasync(::fileno(fp_));
#endif
        std::fclose(fp_); fp_ = nullptr;
    }
    uint64_t records() const noexcept { return records_; }
    uint64_t bytes() const noexcept { return bytes_; }
private:
    std::string path_; std::vector<char> iobuf_; std::FILE* fp_ = nullptr; uint64_t records_ = 0, bytes_ = 0;
};

class CaptureReader {
public:
    explicit CaptureReader(const std::string& path) {
        fp_ = std::fopen(path.c_str(), "rb");
        if (!fp_) throw std::runtime_error("capture: cannot open " + path);
        if (std::fread(&hdr_, sizeof hdr_, 1, fp_) != 1 || std::memcmp(hdr_.magic, "MDCP", 4) != 0 || hdr_.formatVersion != 1)
            { std::fclose(fp_); fp_ = nullptr; throw std::runtime_error("capture: bad header in " + path); }
    }
    ~CaptureReader() { if (fp_) std::fclose(fp_); }
    CaptureReader(const CaptureReader&) = delete; CaptureReader& operator=(const CaptureReader&) = delete;

    const CaptureHeader& header() const noexcept { return hdr_; }

    // Walk every record in order. cb(record, packet bytes). Returns how many were delivered.
    template <class F>
    uint64_t forEach(F&& cb) {
        std::fseek(fp_, long(sizeof(CaptureHeader)), SEEK_SET);
        std::vector<std::byte> buf(MAX_PACKET);
        uint64_t n = 0; CaptureRecord r{};
        while (std::fread(&r, sizeof r, 1, fp_) == 1) {
            if (r.length > MAX_PACKET) throw std::runtime_error("capture: oversized record");
            if (std::fread(buf.data(), r.length, 1, fp_) != 1) break;      // torn tail: stop, keep what is good
            cb(r, buf.data()); ++n;
        }
        return n;
    }
private:
    std::FILE* fp_ = nullptr; CaptureHeader hdr_{};
};

} // namespace trading::md
