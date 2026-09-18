// core/seq/journal.hpp : append-only frame journal for one stream.
//
// File = 64-byte JournalHeader + frames (FrameHeader + body), exactly the wire format.
// Open scans the file, validates every frame (length, known template, contiguous seq)
// and truncates at the first damaged frame, so a crash mid-write loses at most the
// partial frame. A sparse index (every INDEX_STRIDE frames) makes replay-from-seq O(1)+.
#pragma once
#include "trading.hpp"
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <span>
#include <stdexcept>
#include <functional>
#include <unistd.h>

namespace trading::seq {

struct JournalHeader {
    char     magic[4];        // "SEQJ"
    uint32_t formatVersion;   // 1
    uint32_t streamId;
    uint16_t schemaVersion;
    uint16_t reserved0;
    uint64_t firstSeq;        // seq of first frame in this file
    uint8_t  reserved[40];
};
static_assert(sizeof(JournalHeader) == 64);

class Journal {
public:
    static constexpr uint64_t INDEX_STRIDE = 1024;
    static constexpr uint32_t MAX_FRAME = 4096;

    // Opens or creates. After return, lastSeq() is the last durable sequence (0 if empty).
    Journal(const std::string& path, uint32_t streamId, uint64_t firstSeqIfNew = 1)
        : path_(path), streamId_(streamId) {
        fp_ = std::fopen(path.c_str(), "r+b");
        if (!fp_) {
            fp_ = std::fopen(path.c_str(), "w+b");
            if (!fp_) throw std::runtime_error("journal: cannot create " + path);
            JournalHeader h{}; std::memcpy(h.magic, "SEQJ", 4); h.formatVersion = 1; h.streamId = streamId;
            h.schemaVersion = SCHEMA_VERSION; h.firstSeq = firstSeqIfNew;
            std::fwrite(&h, sizeof h, 1, fp_); std::fflush(fp_);
            hdr_ = h; lastSeq_ = firstSeqIfNew - 1; end_ = sizeof(JournalHeader);
            return;
        }
        recover();
    }
    ~Journal() { if (fp_) std::fclose(fp_); }
    Journal(const Journal&) = delete; Journal& operator=(const Journal&) = delete;

    uint64_t lastSeq() const noexcept { return lastSeq_; }
    uint64_t firstSeq() const noexcept { return hdr_.firstSeq; }
    uint32_t streamId() const noexcept { return streamId_; }
    uint64_t sizeBytes() const noexcept { return end_; }
    uint64_t frames() const noexcept { return frames_; }

    // Append one already-sequenced frame. Caller guarantees seq == lastSeq()+1.
    void append(const FrameHeader* f) {
        if (f->seq != lastSeq_ + 1) throw std::logic_error("journal: non-contiguous seq");
        if (f->frameLength < sizeof(FrameHeader) || f->frameLength > MAX_FRAME) throw std::logic_error("journal: bad frameLength");
        if (frames_ % INDEX_STRIDE == 0) index_.push_back({f->seq, end_});
        if (std::fwrite(f, f->frameLength, 1, fp_) != 1) throw std::runtime_error("journal: write failed");
        end_ += f->frameLength; lastSeq_ = f->seq; ++frames_;
    }
    // Persist everything appended so far. Visibility to replay readers does not need this:
    // replay() flushes the writer's buffer itself. Durable commit is a policy choice
    // (per batch on NVMe in production, at checkpoints in tests).
    void commit(bool durable) {
        if (durable) { std::fflush(fp_); ::fdatasync(::fileno(fp_)); }
    }

    // Replay frames with fromSeq <= seq <= toSeq (toSeq 0 = to end) into cb. Returns count.
    // Reads through a separate FILE so it never disturbs the writer's position.
    uint64_t replay(uint64_t fromSeq, uint64_t toSeq, const std::function<void(const FrameHeader*)>& cb) const {
        if (fromSeq < hdr_.firstSeq) fromSeq = hdr_.firstSeq;
        if (fromSeq > lastSeq_) return 0;
        std::fflush(fp_);                                   // make the buffered tail visible to the read handle
        std::FILE* rp = std::fopen(path_.c_str(), "rb");
        if (!rp) throw std::runtime_error("journal: cannot open for replay");
        // find index entry <= fromSeq
        uint64_t off = sizeof(JournalHeader), seq = hdr_.firstSeq;
        for (const auto& e : index_) if (e.seq <= fromSeq) { off = e.offset; seq = e.seq; } else break;
        std::fseek(rp, long(off), SEEK_SET);
        alignas(16) std::byte buf[MAX_FRAME];
        uint64_t n = 0;
        while (off < end_) {
            if (std::fread(buf, sizeof(FrameHeader), 1, rp) != 1) break;
            const auto* h = reinterpret_cast<const FrameHeader*>(buf);
            uint32_t len = h->frameLength;
            if (len > sizeof(FrameHeader) && std::fread(buf + sizeof(FrameHeader), len - sizeof(FrameHeader), 1, rp) != 1) break;
            if (h->seq >= fromSeq) {
                if (toSeq && h->seq > toSeq) break;
                cb(h); ++n;
            }
            off += len; seq = h->seq + 1;
        }
        (void)seq;
        std::fclose(rp);
        return n;
    }

private:
    struct IndexEntry { uint64_t seq; uint64_t offset; };

    void recover() {
        std::fseek(fp_, 0, SEEK_END); long fileLen = std::ftell(fp_);
        std::fseek(fp_, 0, SEEK_SET);
        if (fileLen < long(sizeof(JournalHeader)) || std::fread(&hdr_, sizeof hdr_, 1, fp_) != 1
            || std::memcmp(hdr_.magic, "SEQJ", 4) != 0 || hdr_.formatVersion != 1)
            throw std::runtime_error("journal: bad header in " + path_);
        if (hdr_.streamId != streamId_) throw std::runtime_error("journal: stream id mismatch");
        if (hdr_.schemaVersion > SCHEMA_VERSION) throw std::runtime_error("journal: schema newer than codec");
        uint64_t off = sizeof(JournalHeader), expect = hdr_.firstSeq;
        alignas(16) std::byte buf[MAX_FRAME];
        lastSeq_ = hdr_.firstSeq - 1;
        while (off + sizeof(FrameHeader) <= uint64_t(fileLen)) {
            std::fseek(fp_, long(off), SEEK_SET);
            if (std::fread(buf, sizeof(FrameHeader), 1, fp_) != 1) break;
            const auto* h = reinterpret_cast<const FrameHeader*>(buf);
            uint32_t len = h->frameLength;
            if (len < sizeof(FrameHeader) || len > MAX_FRAME || off + len > uint64_t(fileLen)) break;     // torn tail
            const MessageInfo* mi = lookup(h->templateId);
            if (!mi || len != sizeof(FrameHeader) + mi->blockLength) break;                             // garbage
            if (h->seq != expect) break;                                                                // discontinuity
            if (frames_ % INDEX_STRIDE == 0) index_.push_back({h->seq, off});
            off += len; lastSeq_ = h->seq; ++expect; ++frames_;
        }
        end_ = off;
        if (off < uint64_t(fileLen)) {                    // truncate damaged tail
            if (::ftruncate(::fileno(fp_), off_t(off)) != 0) throw std::runtime_error("journal: truncate failed");
            truncatedBytes_ = uint64_t(fileLen) - off;
        }
        std::fseek(fp_, long(off), SEEK_SET);
    }
public:
    uint64_t truncatedOnOpen() const noexcept { return truncatedBytes_; }
private:
    std::string path_;
    uint32_t streamId_;
    mutable std::FILE* fp_ = nullptr;
    JournalHeader hdr_{};
    uint64_t lastSeq_ = 0, end_ = 0, frames_ = 0, truncatedBytes_ = 0;
    std::vector<IndexEntry> index_;
};

} // namespace trading::seq
