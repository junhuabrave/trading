// core/venue/soupbin.hpp : SoupBinTCP 3.00, the session layer OUCH rides on.
//
// A logical packet is a two-byte big-endian length (of what follows it: the type byte and the
// payload), a type byte and a payload. TCP may split or merge packets in any way, so the Framer
// reassembles them from whatever chunks the socket hands over. The server sends sequenced data
// with no sequence number on the wire: both ends count, starting from the number in the Login
// Accepted packet. Counting, heartbeats and reconnecting are the session worker's (VG-3); this is
// the packet layer.
//
// The login packets carry text: username and password padded on the right with spaces, the session
// padded on the left, and sequence numbers as ASCII digits padded on the left with spaces. A
// sequence number that is not digits, or does not fit 64 bits, does not parse.
#pragma once
#include "bigendian.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>
#include <vector>

namespace trading::venue::soup {

using util::be16; using util::putBe16;

namespace type {
inline constexpr char Debug = '+';
// server to client
inline constexpr char LoginAccepted = 'A', LoginRejected = 'J', SequencedData = 'S', ServerHeartbeat = 'H', EndOfSession = 'Z';
// client to server
inline constexpr char LoginRequest = 'L', UnsequencedData = 'U', ClientHeartbeat = 'R', LogoutRequest = 'O';
}
inline constexpr char REJECT_NOT_AUTHORIZED = 'A', REJECT_SESSION_UNAVAILABLE = 'S';
inline constexpr size_t MAX_PACKET = 2 + 65535;        // the length field is sixteen bits

// ---- packets out --------------------------------------------------------------------------------
// Each writes one whole packet into out and returns its length, or 0 if cap is too small.
inline size_t packet(char t, const void* payload, size_t n, uint8_t* out, size_t cap) noexcept {
    if (n + 1 > 65535 || n + 3 > cap) return 0;
    putBe16(out, uint16_t(n + 1)); out[2] = uint8_t(t);
    if (n) std::memcpy(out + 3, payload, n);
    return n + 3;
}
inline size_t sequenced(const uint8_t* msg, size_t n, uint8_t* out, size_t cap) noexcept { return packet(type::SequencedData, msg, n, out, cap); }
inline size_t unsequenced(const uint8_t* msg, size_t n, uint8_t* out, size_t cap) noexcept { return packet(type::UnsequencedData, msg, n, out, cap); }
inline size_t empty(char t, uint8_t* out, size_t cap) noexcept { return packet(t, nullptr, 0, out, cap); }   // heartbeats, end of session, logout

namespace detail {
template <size_t N> void padRight(uint8_t* p, std::string_view s) noexcept { for (size_t i = 0; i < N; ++i) p[i] = uint8_t(i < s.size() ? s[i] : ' '); }
template <size_t N> void padLeft(uint8_t* p, std::string_view s) noexcept {
    const size_t n = s.size() < N ? s.size() : N;
    std::memset(p, ' ', N - n); std::memcpy(p + N - n, s.data(), n);
}
inline void number20(uint8_t* p, uint64_t v) noexcept {
    char d[20]; size_t n = 0;
    do { d[19 - n++] = char('0' + v % 10); v /= 10; } while (v);
    padLeft<20>(p, {d + 20 - n, n});
}
// Digits, possibly after spaces; nothing else, and nothing that overflows.
inline bool parse20(const uint8_t* p, uint64_t& v) noexcept {
    size_t i = 0;
    while (i < 20 && p[i] == ' ') ++i;
    if (i == 20) return false;
    v = 0;
    for (; i < 20; ++i) {
        if (p[i] < '0' || p[i] > '9') return false;
        const uint64_t d = p[i] - '0';
        if (v > (UINT64_MAX - d) / 10) return false;
        v = v * 10 + d;
    }
    return true;
}
template <size_t N> std::string_view trim(const std::array<char, N>& a) noexcept {
    size_t b = 0, e = N;
    while (b < e && a[b] == ' ') ++b;
    while (e > b && a[e - 1] == ' ') --e;
    return {a.data() + b, e - b};
}
} // namespace detail

inline size_t loginRequest(std::string_view user, std::string_view password, std::string_view session, uint64_t nextSeq, uint8_t* out, size_t cap) noexcept {
    uint8_t p[46];
    detail::padRight<6>(p, user); detail::padRight<10>(p + 6, password); detail::padLeft<10>(p + 16, session); detail::number20(p + 26, nextSeq);
    return packet(type::LoginRequest, p, sizeof p, out, cap);
}
inline size_t loginAccepted(std::string_view session, uint64_t nextSeq, uint8_t* out, size_t cap) noexcept {
    uint8_t p[30];
    detail::padLeft<10>(p, session); detail::number20(p + 10, nextSeq);
    return packet(type::LoginAccepted, p, sizeof p, out, cap);
}
inline size_t loginRejected(char reason, uint8_t* out, size_t cap) noexcept { return packet(type::LoginRejected, &reason, 1, out, cap); }
inline size_t debug(std::string_view text, uint8_t* out, size_t cap) noexcept { return packet(type::Debug, text.data(), text.size(), out, cap); }

// ---- packets in ---------------------------------------------------------------------------------
struct Packet { char type; const uint8_t* payload; size_t size; };

struct LoginRequest {
    std::array<char, 6> username; std::array<char, 10> password, session; uint64_t nextSeq;
    std::string_view user() const noexcept { return detail::trim(username); }
    std::string_view sessionId() const noexcept { return detail::trim(session); }   // empty: the current session
};
struct LoginAccepted {
    std::array<char, 10> session; uint64_t nextSeq;
    std::string_view sessionId() const noexcept { return detail::trim(session); }
};
inline bool parse(const Packet& k, LoginRequest& out) noexcept {
    if (k.type != type::LoginRequest || k.size != 46) return false;
    std::memcpy(out.username.data(), k.payload, 6); std::memcpy(out.password.data(), k.payload + 6, 10);
    std::memcpy(out.session.data(), k.payload + 16, 10);
    return detail::parse20(k.payload + 26, out.nextSeq);
}
inline bool parse(const Packet& k, LoginAccepted& out) noexcept {
    if (k.type != type::LoginAccepted || k.size != 30) return false;
    std::memcpy(out.session.data(), k.payload, 10);
    return detail::parse20(k.payload + 10, out.nextSeq);
}
inline bool parseReject(const Packet& k, char& reason) noexcept {
    if (k.type != type::LoginRejected || k.size != 1) return false;
    reason = char(k.payload[0]);
    return true;
}

// Whole packets out of a byte stream. Bytes go in through space() and commit(); next() hands out
// each complete packet, pointing into the buffer, valid until the next call to space(). A length
// of zero has no room for a type byte: the stream is broken and stays broken.
class Framer {
public:
    Framer() : buf_(2 * MAX_PACKET) {}
    // Room for at least one whole packet beyond what is buffered. Moves unread bytes to the front.
    std::pair<uint8_t*, size_t> space() noexcept {
        if (head_ > 0) { std::memmove(buf_.data(), buf_.data() + head_, tail_ - head_); tail_ -= head_; head_ = 0; }
        return {buf_.data() + tail_, buf_.size() - tail_};
    }
    void commit(size_t n) noexcept { tail_ += n; }
    bool next(Packet& out) noexcept {
        if (broken_ || tail_ - head_ < 2) return false;
        const size_t len = be16(buf_.data() + head_);
        if (len == 0) { broken_ = true; return false; }
        if (tail_ - head_ < 2 + len) return false;
        out = Packet{char(buf_[head_ + 2]), buf_.data() + head_ + 3, len - 1};
        head_ += 2 + len;
        return true;
    }
    bool broken() const noexcept { return broken_; }
    size_t buffered() const noexcept { return tail_ - head_; }

private:
    std::vector<uint8_t> buf_;
    size_t head_ = 0, tail_ = 0;
    bool broken_ = false;
};

} // namespace trading::venue::soup
