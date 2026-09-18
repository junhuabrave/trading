// core/seq/journal.hpp : append-only, segmented frame journal for one stream.
//
// A journal is a directory of segments. Each segment = 64-byte JournalHeader (with the
// segment's first seq) + frames (FrameHeader + body), exactly the wire format. A new
// segment starts when the active one would exceed segmentBytes, so a day of market data
// is many files, recovery of a crash scans only the last one for a torn tail, and replay
// from a sequence number seeks to the right segment. Open validates every frame of every
// segment (length, known template, contiguous seq); a torn tail on the last segment is
// truncated; damage anywhere else is an error, because the sequence would have a hole.
// A sparse index (every INDEX_STRIDE frames) makes replay-from-seq O(1)+.
#pragma once
#include "trading.hpp"
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <span>
#include <stdexcept>
#include <functional>
#include <filesystem>
#include <algorithm>
#include <unistd.h>
#include <fcntl.h>

namespace trading::seq {

struct JournalHeader {
    char     magic[4];        // "SEQJ"
    uint32_t formatVersion;   // 2 = segmented
    uint32_t streamId;
    uint16_t schemaVersion;
    uint16_t reserved0;
    uint64_t firstSeq;        // seq of first frame in this segment
    uint32_t segmentIndex;
    uint8_t  reserved[36];
};
static_assert(sizeof(JournalHeader) == 64);

class Journal {
public:
    static constexpr uint64_t INDEX_STRIDE = 1024;
    static constexpr uint32_t MAX_FRAME = 4096;
    static constexpr uint64_t DEFAULT_SEGMENT_BYTES = 1ull << 30;

    // Opens or creates the journal directory. After return, lastSeq() is the last durable sequence.
    Journal(const std::string& dir, uint32_t streamId, uint64_t firstSeqIfNew = 1,
            uint64_t segmentBytes = DEFAULT_SEGMENT_BYTES)
        : dir_(dir), streamId_(streamId), segBytes_(std::max<uint64_t>(segmentBytes, sizeof(JournalHeader) + MAX_FRAME)) {
        namespace fs = std::filesystem;
        if (fs::exists(dir_) && !fs::is_directory(dir_)) throw std::runtime_error("journal: not a directory: " + dir_);
        fs::create_directories(dir_);
        index_.reserve(1 << 16); segs_.reserve(256);          // no allocation on the append path for 64M frames / 256 segments
        std::vector<fs::path> files;
        for (const auto& e : fs::directory_iterator(dir_))
            if (e.is_regular_file() && e.path().extension() == ".jnl") files.push_back(e.path());
        std::sort(files.begin(), files.end());
        if (files.empty()) { lastSeq_ = firstSeqIfNew - 1; openSegment(0, firstSeqIfNew); return; }
        try { recover(files); } catch (...) { if (fp_) { std::fclose(fp_); fp_ = nullptr; } throw; }
    }
    ~Journal() { if (fp_) std::fclose(fp_); }
    Journal(const Journal&) = delete; Journal& operator=(const Journal&) = delete;

    uint64_t lastSeq() const noexcept { return lastSeq_; }
    uint64_t firstSeq() const noexcept { return segs_.front().firstSeq; }
    uint32_t streamId() const noexcept { return streamId_; }
    uint64_t sizeBytes() const noexcept { uint64_t t = 0; for (const auto& s : segs_) t += s.bytes; return t; }
    uint64_t frames() const noexcept { return frames_; }
    size_t segments() const noexcept { return segs_.size(); }
    uint64_t truncatedOnOpen() const noexcept { return truncatedBytes_; }
    const std::string& activeSegmentPath() const noexcept { return segs_.back().path; }
    const std::string& directory() const noexcept { return dir_; }

    // Append one already-sequenced frame. Caller guarantees seq == lastSeq()+1.
    void append(const FrameHeader* f) {
        if (f->seq != lastSeq_ + 1) throw std::logic_error("journal: non-contiguous seq");
        if (f->frameLength < sizeof(FrameHeader) || f->frameLength > MAX_FRAME) throw std::logic_error("journal: bad frameLength");
        Segment& seg = segs_.back();
        if (seg.frames > 0 && seg.bytes + f->frameLength > segBytes_) { rotate(f->seq); }
        Segment& s = segs_.back();
        if (frames_ % INDEX_STRIDE == 0) index_.push_back({f->seq, uint32_t(segs_.size() - 1), s.bytes});
        if (std::fwrite(f, f->frameLength, 1, fp_) != 1) throw std::runtime_error("journal: write failed");
        s.bytes += f->frameLength; s.lastSeq = f->seq; ++s.frames;
        lastSeq_ = f->seq; ++frames_;
    }
    // Persist everything appended so far. Visibility to replay readers does not need this:
    // replay() flushes the writer's buffer itself. Durable commit is a policy choice
    // (per batch on NVMe in production, at checkpoints in tests).
    void commit(bool durable) {
        if (durable) { std::fflush(fp_); syncData(::fileno(fp_)); }
    }

    // Replay frames with fromSeq <= seq <= toSeq (toSeq 0 = to end) into cb. Returns count.
    // Reads through separate FILE handles so it never disturbs the writer's position.
    uint64_t replay(uint64_t fromSeq, uint64_t toSeq, const std::function<void(const FrameHeader*)>& cb) const {
        if (fromSeq < firstSeq()) fromSeq = firstSeq();
        if (fromSeq > lastSeq_) return 0;
        std::fflush(fp_);                                   // make the buffered tail visible to the read handles
        // locate the segment and the nearest index entry at or before fromSeq
        size_t segIdx = 0;
        for (size_t i = 0; i < segs_.size(); ++i) if (segs_[i].firstSeq <= fromSeq) segIdx = i; else break;
        uint64_t off = sizeof(JournalHeader);
        for (const auto& e : index_) if (e.seq <= fromSeq && e.segment == segIdx) off = e.offset; else if (e.seq > fromSeq) break;
        alignas(16) std::byte buf[MAX_FRAME];
        uint64_t n = 0;
        for (; segIdx < segs_.size(); ++segIdx, off = sizeof(JournalHeader)) {
            const Segment& seg = segs_[segIdx];
            if (seg.firstSeq > fromSeq && toSeq && seg.firstSeq > toSeq) break;
            std::FILE* rp = std::fopen(seg.path.c_str(), "rb");
            if (!rp) throw std::runtime_error("journal: cannot open for replay " + seg.path);
            std::fseek(rp, long(off), SEEK_SET);
            bool stop = false;
            while (off < seg.bytes) {
                if (std::fread(buf, sizeof(FrameHeader), 1, rp) != 1) break;
                const auto* h = reinterpret_cast<const FrameHeader*>(buf);
                uint32_t len = h->frameLength;
                if (len > sizeof(FrameHeader) && std::fread(buf + sizeof(FrameHeader), len - sizeof(FrameHeader), 1, rp) != 1) break;
                if (h->seq >= fromSeq) {
                    if (toSeq && h->seq > toSeq) { stop = true; break; }
                    cb(h); ++n;
                }
                off += len;
            }
            std::fclose(rp);
            if (stop) break;
        }
        return n;
    }

private:
    struct Segment { std::string path; uint64_t firstSeq, lastSeq, bytes, frames; };
    struct IndexEntry { uint64_t seq; uint32_t segment; uint64_t offset; };

    static std::string segName(const std::string& dir, uint32_t idx) {
        char b[32]; std::snprintf(b, sizeof b, "/seg-%06u.jnl", idx); return dir + b;
    }
    void openSegment(uint32_t idx, uint64_t firstSeq) {
        std::string path = segName(dir_, idx);
        fp_ = std::fopen(path.c_str(), "w+b");
        if (!fp_) throw std::runtime_error("journal: cannot create " + path);
        JournalHeader h{}; std::memcpy(h.magic, "SEQJ", 4); h.formatVersion = 2; h.streamId = streamId_;
        h.schemaVersion = SCHEMA_VERSION; h.firstSeq = firstSeq; h.segmentIndex = idx;
        std::fwrite(&h, sizeof h, 1, fp_); std::fflush(fp_);
        segs_.push_back({path, firstSeq, firstSeq - 1, sizeof(JournalHeader), 0});
    }
    void rotate(uint64_t nextSeq) {
        std::fflush(fp_); syncData(::fileno(fp_)); std::fclose(fp_); fp_ = nullptr;
        openSegment(uint32_t(segs_.size()), nextSeq);
    }
    void recover(const std::vector<std::filesystem::path>& files) {
        uint64_t expect = 0; bool first = true;
        alignas(16) std::byte buf[MAX_FRAME];
        for (size_t fi = 0; fi < files.size(); ++fi) {
            const bool last = fi + 1 == files.size();
            std::string path = files[fi].string();
            std::FILE* fp = std::fopen(path.c_str(), last ? "r+b" : "rb");
            if (!fp) throw std::runtime_error("journal: cannot open " + path);
            auto fail = [&](const std::string& why) { std::fclose(fp); throw std::runtime_error(why); };
            std::fseek(fp, 0, SEEK_END); long fileLen = std::ftell(fp); std::fseek(fp, 0, SEEK_SET);
            JournalHeader hdr{};
            if (fileLen < long(sizeof(JournalHeader)) || std::fread(&hdr, sizeof hdr, 1, fp) != 1
                || std::memcmp(hdr.magic, "SEQJ", 4) != 0 || hdr.formatVersion != 2)
                fail("journal: bad header in " + path);
            if (hdr.streamId != streamId_) fail("journal: stream id mismatch in " + path);
            if (hdr.schemaVersion > SCHEMA_VERSION) fail("journal: schema newer than codec");
            if (hdr.segmentIndex != fi) fail("journal: missing segment before " + path);
            if (first) { expect = hdr.firstSeq; first = false; }
            else if (hdr.firstSeq != expect) fail("journal: sequence gap at " + path);
            Segment seg{path, hdr.firstSeq, hdr.firstSeq - 1, sizeof(JournalHeader), 0};
            uint64_t off = sizeof(JournalHeader);
            while (off + sizeof(FrameHeader) <= uint64_t(fileLen)) {
                std::fseek(fp, long(off), SEEK_SET);
                if (std::fread(buf, sizeof(FrameHeader), 1, fp) != 1) break;
                const auto* h = reinterpret_cast<const FrameHeader*>(buf);
                uint32_t len = h->frameLength;
                if (len < sizeof(FrameHeader) || len > MAX_FRAME || off + len > uint64_t(fileLen)) break;   // torn tail
                const MessageInfo* mi = lookup(h->templateId);
                if (!mi || len != sizeof(FrameHeader) + mi->blockLength) break;                           // garbage
                if (h->seq != expect) break;                                                              // discontinuity
                if (frames_ % INDEX_STRIDE == 0) index_.push_back({h->seq, uint32_t(fi), off});
                off += len; seg.lastSeq = h->seq; ++expect; ++seg.frames; ++frames_;
            }
            seg.bytes = off;
            if (off < uint64_t(fileLen)) {
                if (!last) fail("journal: damaged frame inside " + path);
                if (::ftruncate(::fileno(fp), off_t(off)) != 0) fail("journal: truncate failed");
                truncatedBytes_ = uint64_t(fileLen) - off;
            }
            segs_.push_back(seg);
            if (last) { std::fseek(fp, long(off), SEEK_SET); fp_ = fp; } else std::fclose(fp);
        }
        lastSeq_ = expect - 1;
    }
    // Durable data sync. fdatasync is Linux; macOS has no fdatasync and its fsync does not
    // flush the drive cache, so F_FULLFSYNC is the equivalent there.
    static void syncData(int fd) noexcept {
#if defined(__APPLE__)
        if (::fcntl(fd, F_FULLFSYNC) != 0) ::fsync(fd);
#else
        ::fdatasync(fd);
#endif
    }

    std::string dir_;
    uint32_t streamId_;
    uint64_t segBytes_;
    mutable std::FILE* fp_ = nullptr;
    std::vector<Segment> segs_;
    std::vector<IndexEntry> index_;
    uint64_t lastSeq_ = 0, frames_ = 0, truncatedBytes_ = 0;
};

} // namespace trading::seq
