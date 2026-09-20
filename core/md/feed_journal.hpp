// core/md/feed_journal.hpp : one feed's day on disk, keyed by the venue's own sequence.
//
// Two streams are kept per feed. The arbitrated packet stream is the venue's bytes, gap-free and in
// order, and it is the authority: everything else about the feed can be recomputed from it. The
// decoded frame stream is our normalised messages, and it is a cache - kept only because replaying
// normalised messages is an order of magnitude cheaper than decoding the packets again. Both use
// this class, because what makes them a journal is the same thing: a key you can seek to.
//
// The key is (feedId, venueSeq), not a sequence of ours, for the reason the whole market-data stack
// rests on: two handlers of the same feed must be able to produce the same journal, and no two
// sequencers can agree on numbers they assign themselves. That has three consequences the seq
// journal does not have to deal with.
//
//   Keys are not contiguous. One packet covers as many sequences as it carries messages.
//   Keys are not unique. One venue message can decode into several of ours - an execution is a
//   trade and a book change - and they share its sequence.
//   Keys may jump. When the arbitrator gives up on a hole and resynchronises from a snapshot, the
//   stream skips, and that is a fact about the day rather than corruption. It is recorded as such.
//
// So the invariant on open is that keys never go backwards, and replay is by overlap rather than by
// equality. A sparse index makes replay to a watermark a seek: bytesScanned() reports what a replay
// actually read, so "seeks rather than scans" is a number and not a claim.
#pragma once
#include "packet.hpp"
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

namespace trading::md {

enum class JournalKind : uint16_t { Packets = 1, Frames = 2 };

struct FeedJournalHeader {
    char     magic[4];          // "MDJN"
    uint32_t formatVersion;
    uint32_t feedId;
    uint16_t kind;
    uint16_t schemaVersion;
    uint64_t firstVenueSeq;
    uint32_t segmentIndex;
    uint32_t businessDate;
    uint8_t  reserved[32];
};
static_assert(sizeof(FeedJournalHeader) == 64);

namespace JournalFlags {
inline constexpr uint16_t fromRecovery = 1u << 0;   // arrived as a retransmit or a snapshot
inline constexpr uint16_t afterJump    = 1u << 1;   // the stream skipped before this record
}

// 32 bytes, so a record's payload starts 16-byte aligned when the record does, and a frame read
// straight out of the mapping is aligned like any other.
struct FeedJournalRecord {
    uint32_t length;       // this header plus the payload, before padding
    uint16_t count;        // venue sequences this record covers; 0 for one that covers none
    uint16_t flags;
    uint64_t venueSeq;     // the first venue sequence in the payload
    int64_t  recvTs;       // when the line receiver stamped it
    uint64_t reserved;
};
static_assert(sizeof(FeedJournalRecord) == 32);
static_assert(sizeof(FeedJournalRecord) % 16 == 0);

class FeedJournal {
public:
    static constexpr uint32_t MAX_PAYLOAD = 4096;
    static constexpr uint64_t INDEX_STRIDE = 512;
    static constexpr uint64_t DEFAULT_SEGMENT_BYTES = 1ull << 30;
    // Stdio's default buffer is a few kilobytes, which on a stream of half-kilobyte packets is a
    // write syscall every eight records and a tail an order of magnitude worse than the median.
    // The capture writer learned this the same way, from the benchmark.
    static constexpr size_t BUFFER_BYTES = 1u << 20;

    FeedJournal(const std::string& dir, uint32_t feedId, JournalKind kind, uint32_t businessDate,
                uint64_t segmentBytes = DEFAULT_SEGMENT_BYTES)
        : dir_(dir), feedId_(feedId), kind_(kind), date_(businessDate),
          segBytes_(std::max<uint64_t>(segmentBytes, sizeof(FeedJournalHeader) + sizeof(FeedJournalRecord) + MAX_PAYLOAD)),
          iobuf_(BUFFER_BYTES) {
        namespace fs = std::filesystem;
        if (fs::exists(dir_) && !fs::is_directory(dir_)) throw std::runtime_error("feed journal: not a directory: " + dir_);
        fs::create_directories(dir_);
        index_.reserve(1 << 16);
        segs_.reserve(256);
        std::vector<fs::path> files;
        for (const auto& e : fs::directory_iterator(dir_))
            if (e.is_regular_file() && e.path().extension() == ".mdj") files.push_back(e.path());
        std::sort(files.begin(), files.end());
        if (files.empty()) { openSegment(0, 0); return; }
        try { recover(files); } catch (...) { if (fp_) { std::fclose(fp_); fp_ = nullptr; } throw; }
    }
    ~FeedJournal() { if (fp_) std::fclose(fp_); }
    FeedJournal(const FeedJournal&) = delete;
    FeedJournal& operator=(const FeedJournal&) = delete;

    // One record. venueSeq must not go backwards; a forward jump is allowed and marked, because
    // that is what a snapshot resynchronisation looks like from here.
    void append(uint64_t venueSeq, uint16_t count, int64_t recvTs, uint16_t flags,
                const void* payload, uint32_t payloadLen) {
        if (payloadLen > MAX_PAYLOAD) throw std::logic_error("feed journal: payload too large");
        if (records_ && venueSeq < lastKey_) throw std::logic_error("feed journal: venue sequence went backwards");
        if (records_ && venueSeq > expectNext_) flags = uint16_t(flags | JournalFlags::afterJump);
        const uint32_t len = uint32_t(sizeof(FeedJournalRecord)) + payloadLen;
        const uint32_t padded = pad(len);
        Segment& active = segs_.back();
        if (active.records > 0 && active.bytes + padded > segBytes_) rotate(venueSeq);
        Segment& s = segs_.back();
        if (s.records == 0 && s.firstVenueSeq == 0) s.firstVenueSeq = venueSeq;
        if (records_ % INDEX_STRIDE == 0) index_.push_back({venueSeq, uint32_t(segs_.size() - 1), s.bytes});
        FeedJournalRecord r{};
        r.length = len; r.count = count; r.flags = flags; r.venueSeq = venueSeq; r.recvTs = recvTs;
        if (std::fwrite(&r, sizeof r, 1, fp_) != 1) throw std::runtime_error("feed journal: write failed");
        if (payloadLen && std::fwrite(payload, payloadLen, 1, fp_) != 1) throw std::runtime_error("feed journal: write failed");
        if (padded > len) {
            static const std::byte zero[16]{};
            if (std::fwrite(zero, padded - len, 1, fp_) != 1) throw std::runtime_error("feed journal: write failed");
        }
        s.bytes += padded; s.lastVenueSeq = venueSeq; ++s.records;
        lastKey_ = venueSeq;
        expectNext_ = venueSeq + (count ? count : 1);
        if (expectNext_ - 1 > coveredTo_) coveredTo_ = expectNext_ - 1;
        ++records_;
        if (flags & JournalFlags::afterJump) ++jumps_;
    }
    void commit(bool durable) { if (durable) { std::fflush(fp_); syncData(::fileno(fp_)); } }

    // Replay every record whose venue sequences overlap [fromSeq, toSeq]. toSeq 0 means to the end.
    // cb(const FeedJournalRecord&, const std::byte* payload). Returns the number of records handed over.
    template <class F>
    uint64_t replay(uint64_t fromSeq, uint64_t toSeq, F&& cb) const {
        uint64_t n = 0;
        replayWhile(fromSeq, [&](const FeedJournalRecord& r, const std::byte* p) {
            if (toSeq && r.venueSeq > toSeq) return false;
            cb(r, p);
            ++n;
            return true;
        });
        return n;
    }

    // Replay from fromSeq while cb returns true. Seeks: the segment is chosen by its first key and
    // the offset by the sparse index, so the cost is the tail actually wanted plus at most one
    // index stride, not the journal. Reads through its own handle, never disturbing the writer.
    template <class F>
    uint64_t replayWhile(uint64_t fromSeq, F&& cb) const {
        scanned_ = 0;
        if (!records_) return 0;
        std::fflush(fp_);                                   // the buffered tail has to be visible
        size_t segIdx = 0;
        for (size_t i = 0; i < segs_.size(); ++i) {
            if (segs_[i].firstVenueSeq <= fromSeq) segIdx = i; else break;
        }
        uint64_t off = sizeof(FeedJournalHeader);
        for (const auto& e : index_) {
            if (e.segment < segIdx) continue;
            if (e.segment > segIdx) break;
            if (e.venueSeq <= fromSeq) off = e.offset; else break;
        }
        alignas(16) std::byte buf[sizeof(FeedJournalRecord) + MAX_PAYLOAD];
        uint64_t handed = 0;
        for (; segIdx < segs_.size(); ++segIdx, off = sizeof(FeedJournalHeader)) {
            const Segment& seg = segs_[segIdx];
            std::FILE* rp = std::fopen(seg.path.c_str(), "rb");
            if (!rp) throw std::runtime_error("feed journal: cannot open for replay " + seg.path);
            std::fseek(rp, long(off), SEEK_SET);
            bool stop = false;
            while (off + sizeof(FeedJournalRecord) <= seg.bytes) {
                if (std::fread(buf, sizeof(FeedJournalRecord), 1, rp) != 1) break;
                const auto* r = reinterpret_cast<const FeedJournalRecord*>(buf);
                const uint32_t len = r->length;
                if (len < sizeof(FeedJournalRecord) || len > sizeof(FeedJournalRecord) + MAX_PAYLOAD) break;
                const uint32_t body = len - uint32_t(sizeof(FeedJournalRecord));
                if (body && std::fread(buf + sizeof(FeedJournalRecord), body, 1, rp) != 1) break;
                scanned_ += pad(len);
                const uint64_t covers = r->venueSeq + (r->count ? r->count : 1);
                if (covers > fromSeq) {
                    if (!cb(*r, buf + sizeof(FeedJournalRecord))) { stop = true; break; }
                    ++handed;
                }
                off += pad(len);
                std::fseek(rp, long(off), SEEK_SET);
            }
            std::fclose(rp);
            if (stop) break;
        }
        return handed;
    }

    uint32_t feedId() const noexcept { return feedId_; }
    JournalKind kind() const noexcept { return kind_; }
    uint64_t records() const noexcept { return records_; }
    uint64_t firstVenueSeq() const noexcept { return segs_.empty() ? 0 : segs_.front().firstVenueSeq; }
    uint64_t lastVenueSeq() const noexcept { return lastKey_; }
    uint64_t coveredTo() const noexcept { return coveredTo_; }      // highest sequence any record covers
    uint64_t jumps() const noexcept { return jumps_; }              // resynchronisations recorded
    uint64_t sizeBytes() const noexcept { uint64_t t = 0; for (const auto& s : segs_) t += s.bytes; return t; }
    size_t   segments() const noexcept { return segs_.size(); }
    uint64_t truncatedOnOpen() const noexcept { return truncatedBytes_; }
    uint64_t bytesScanned() const noexcept { return scanned_; }     // what the last replay actually read
    const std::string& directory() const noexcept { return dir_; }

private:
    struct Segment { std::string path; uint64_t firstVenueSeq, lastVenueSeq, bytes, records; };
    struct IndexEntry { uint64_t venueSeq; uint32_t segment; uint64_t offset; };

    static constexpr uint32_t pad(uint32_t n) noexcept { return (n + 15u) & ~15u; }
    static std::string segName(const std::string& dir, uint32_t idx) {
        char b[32]; std::snprintf(b, sizeof b, "/seg-%06u.mdj", idx); return dir + b;
    }
    void openSegment(uint32_t idx, uint64_t firstVenueSeq) {
        const std::string path = segName(dir_, idx);
        fp_ = std::fopen(path.c_str(), "w+b");
        if (!fp_) throw std::runtime_error("feed journal: cannot create " + path);
        std::setvbuf(fp_, iobuf_.data(), _IOFBF, iobuf_.size());
        FeedJournalHeader h{};
        std::memcpy(h.magic, "MDJN", 4);
        h.formatVersion = 1; h.feedId = feedId_; h.kind = uint16_t(kind_);
        h.schemaVersion = SCHEMA_VERSION; h.firstVenueSeq = firstVenueSeq;
        h.segmentIndex = idx; h.businessDate = date_;
        std::fwrite(&h, sizeof h, 1, fp_);
        std::fflush(fp_);
        segs_.push_back({path, firstVenueSeq, firstVenueSeq, sizeof(FeedJournalHeader), 0});
    }
    void rotate(uint64_t nextSeq) {
        std::fflush(fp_); syncData(::fileno(fp_)); std::fclose(fp_); fp_ = nullptr;
        openSegment(uint32_t(segs_.size()), nextSeq);
    }

    // A record is trusted only if it is self-consistent and its payload is what the segment says it
    // holds. The fuzz target exists because this runs on whatever was on the disk after a crash.
    bool validPayload(const FeedJournalRecord& r, const std::byte* p) const noexcept {
        const uint32_t body = r.length - uint32_t(sizeof(FeedJournalRecord));
        if (kind_ == JournalKind::Packets) {
            if (!validPacket(p, body)) return false;
            const auto* h = reinterpret_cast<const PacketHeader*>(p);
            return h->feedId == feedId_ && h->firstSeq == r.venueSeq && h->msgCount == r.count;
        }
        if (body < sizeof(FrameHeader)) return false;
        const auto* f = reinterpret_cast<const FrameHeader*>(p);
        if (f->frameLength != body) return false;
        const MessageInfo* mi = lookup(f->templateId);
        return mi && body == sizeof(FrameHeader) + mi->blockLength;
    }

    void recover(const std::vector<std::filesystem::path>& files) {
        namespace fs = std::filesystem;
        alignas(16) std::byte buf[sizeof(FeedJournalRecord) + MAX_PAYLOAD];
        bool first = true;
        for (size_t fi = 0; fi < files.size(); ++fi) {
            const bool last = fi + 1 == files.size();
            const std::string path = files[fi].string();
            std::FILE* fp = std::fopen(path.c_str(), last ? "r+b" : "rb");
            if (!fp) throw std::runtime_error("feed journal: cannot open " + path);
            auto fail = [&](const std::string& why) { std::fclose(fp); throw std::runtime_error(why); };
            std::fseek(fp, 0, SEEK_END);
            const long fileLen = std::ftell(fp);
            std::fseek(fp, 0, SEEK_SET);
            FeedJournalHeader hdr{};
            if (fileLen < long(sizeof(FeedJournalHeader)) || std::fread(&hdr, sizeof hdr, 1, fp) != 1
                || std::memcmp(hdr.magic, "MDJN", 4) != 0 || hdr.formatVersion != 1)
                fail("feed journal: bad header in " + path);
            if (hdr.feedId != feedId_) fail("feed journal: feed id mismatch in " + path);
            if (hdr.kind != uint16_t(kind_)) fail("feed journal: kind mismatch in " + path);
            if (hdr.schemaVersion > SCHEMA_VERSION) fail("feed journal: schema newer than codec");
            if (hdr.segmentIndex != fi) fail("feed journal: missing segment before " + path);
            if (!first && hdr.firstVenueSeq < lastKey_) fail("feed journal: segment starts behind the one before it");
            first = false;

            Segment seg{path, hdr.firstVenueSeq, hdr.firstVenueSeq, sizeof(FeedJournalHeader), 0};
            uint64_t off = sizeof(FeedJournalHeader);
            while (off + sizeof(FeedJournalRecord) <= uint64_t(fileLen)) {
                std::fseek(fp, long(off), SEEK_SET);
                if (std::fread(buf, sizeof(FeedJournalRecord), 1, fp) != 1) break;
                const auto* r = reinterpret_cast<const FeedJournalRecord*>(buf);
                const uint32_t len = r->length;
                if (len < sizeof(FeedJournalRecord) || len > sizeof(FeedJournalRecord) + MAX_PAYLOAD) break;   // torn or garbage
                if (off + pad(len) > uint64_t(fileLen)) break;                                   // torn tail
                const uint32_t body = len - uint32_t(sizeof(FeedJournalRecord));
                if (body && std::fread(buf + sizeof(FeedJournalRecord), body, 1, fp) != 1) break;
                if (seg.records && r->venueSeq < seg.lastVenueSeq) break;                        // keys never go back
                if (!validPayload(*r, buf + sizeof(FeedJournalRecord))) break;
                if (records_ % INDEX_STRIDE == 0) index_.push_back({r->venueSeq, uint32_t(fi), off});
                if (r->flags & JournalFlags::afterJump) ++jumps_;
                off += pad(len);
                seg.lastVenueSeq = r->venueSeq;
                lastKey_ = r->venueSeq;
                expectNext_ = r->venueSeq + (r->count ? r->count : 1);
                if (expectNext_ - 1 > coveredTo_) coveredTo_ = expectNext_ - 1;
                ++seg.records; ++records_;
            }
            seg.bytes = off;
            if (off < uint64_t(fileLen)) {
                // Anything unreadable in the middle of the journal is a hole in the day, and a hole
                // cannot be healed by ignoring it. Only the segment being written when the process
                // died may end early.
                if (!last) fail("feed journal: damaged record inside " + path);
                if (::ftruncate(::fileno(fp), off_t(off)) != 0) fail("feed journal: truncate failed");
                truncatedBytes_ = uint64_t(fileLen) - off;
            }
            segs_.push_back(seg);
            // The segment being written when we died becomes the one we carry on writing. It is
            // reopened rather than kept, because a buffer can only be attached to a stream before
            // any I/O on it and recovery has just read the whole file through this one.
            std::fclose(fp);
            if (last) {
                fp_ = std::fopen(path.c_str(), "r+b");
                if (!fp_) throw std::runtime_error("feed journal: cannot reopen " + path);
                std::setvbuf(fp_, iobuf_.data(), _IOFBF, iobuf_.size());
                std::fseek(fp_, long(off), SEEK_SET);
            }
        }
        if (segs_.empty()) openSegment(0, 0);
    }

    static void syncData(int fd) noexcept {
#if defined(__APPLE__)
        if (::fcntl(fd, F_FULLFSYNC) != 0) ::fsync(fd);
#else
        ::fdatasync(fd);
#endif
    }

    std::string dir_;
    uint32_t feedId_;
    JournalKind kind_;
    uint32_t date_;
    uint64_t segBytes_;
    std::vector<char> iobuf_;
    mutable std::FILE* fp_ = nullptr;
    std::vector<Segment> segs_;
    std::vector<IndexEntry> index_;
    uint64_t records_ = 0, lastKey_ = 0, expectNext_ = 0, coveredTo_ = 0, jumps_ = 0, truncatedBytes_ = 0;
    mutable uint64_t scanned_ = 0;
};

} // namespace trading::md
