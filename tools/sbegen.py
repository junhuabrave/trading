#!/usr/bin/env python3
"""
sbegen.py : code generator for schema/trading.xml

Reads the SBE-compatible subset we use (fixed-length messages, no repeating
groups, no var data) and emits:

  gen/cpp/trading.hpp      C++23 POD structs with static_asserts on every offset
  gen/py/trading.py        Python codecs (struct-based), enums, registry
  gen/go/trading.go        Go codecs (encoding/binary), enums, registry
  gen/layout.md            offset report for every message and composite

Layout rules enforced here (the schema must satisfy them, we never silently fix):
  * every field is naturally aligned (uint16 on 2, uint32 on 4, uint64 on 8)
  * declared blockLength / composite length equals the computed size
  * every message size is a multiple of 16
  * padding is explicit in the schema (fields named pad*/reserved*)
"""
import sys, os, re, hashlib
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional

NS = {"sbe": "http://fixprotocol.io/2016/sbe"}

PRIM = {
    "char":   (1, "char",     "c"),
    "int8":   (1, "int8_t",   "b"),
    "uint8":  (1, "uint8_t",  "B"),
    "int16":  (2, "int16_t",  "h"),
    "uint16": (2, "uint16_t", "H"),
    "int32":  (4, "int32_t",  "i"),
    "uint32": (4, "uint32_t", "I"),
    "int64":  (8, "int64_t",  "q"),
    "uint64": (8, "uint64_t", "Q"),
}

@dataclass
class Prim:
    name: str
    prim: str
    length: int = 1
    desc: str = ""
    @property
    def size(self): return PRIM[self.prim][0] * self.length
    @property
    def align(self): return PRIM[self.prim][0]

@dataclass
class Enum:
    name: str
    enc: str
    values: List[tuple]   # (name, int)
    @property
    def size(self): return PRIM[self.enc][0]
    @property
    def align(self): return self.size

@dataclass
class BitSet:
    name: str
    enc: str
    choices: List[tuple]  # (name, bit)
    @property
    def size(self): return PRIM[self.enc][0]
    @property
    def align(self): return self.size

@dataclass
class Field:
    name: str
    id: int
    typ: object           # Prim | Enum | BitSet | Composite
    offset: int = 0
    since: int = 0
    desc: str = ""
    @property
    def size(self): return self.typ.size

@dataclass
class Composite:
    name: str
    fields: List[Field]
    declared: Optional[int]
    size: int = 0
    align: int = 1

@dataclass
class Message:
    name: str
    id: int
    fields: List[Field]
    declared: Optional[int]
    size: int = 0
    desc: str = ""

class Schema:
    def __init__(self, path):
        self.path = path
        tree = ET.parse(path)
        root = tree.getroot()
        self.package = root.get("package")
        self.id = int(root.get("id"))
        self.version = int(root.get("version"))
        self.header_type = root.get("headerType")
        self.types: Dict[str, object] = {}
        self.composites: List[Composite] = []
        self.enums: List[Enum] = []
        self.sets: List[BitSet] = []
        self.messages: List[Message] = []
        self.errors: List[str] = []
        for p in PRIM:
            self.types[p] = Prim(p, p)
        types = root.find("types")
        for el in types:
            tag = el.tag.split("}")[-1]
            if tag == "type":
                t = Prim(el.get("name"), el.get("primitiveType"),
                         int(el.get("length", "1")), el.get("description", ""))
                self.types[t.name] = t
            elif tag == "enum":
                e = Enum(el.get("name"), el.get("encodingType"),
                         [(v.get("name"), int(v.text)) for v in el])
                self.types[e.name] = e; self.enums.append(e)
            elif tag == "set":
                s = BitSet(el.get("name"), el.get("encodingType"),
                           [(c.get("name"), int(c.text)) for c in el])
                self.types[s.name] = s; self.sets.append(s)
            elif tag == "composite":
                fields = []
                for i, sub in enumerate(el):
                    p = Prim(sub.get("name"), sub.get("primitiveType"),
                             int(sub.get("length", "1")), sub.get("description", ""))
                    fields.append(Field(p.name, i + 1, p, desc=p.desc))
                c = Composite(el.get("name"), fields,
                              int(el.get("length")) if el.get("length") else None)
                self._layout(c, c.fields, f"composite {c.name}")
                c.align = max(f.typ.align for f in fields)
                self.types[c.name] = c; self.composites.append(c)
        for el in root.findall("sbe:message", NS):
            fields = []
            for f in el.findall("field"):
                tname = f.get("type")
                length = int(f.get("length", "1"))
                if tname in PRIM and length > 1:
                    typ = Prim(f"{tname}[{length}]", tname, length)
                else:
                    typ = self.types.get(tname)
                    if typ is None:
                        self.errors.append(f"message {el.get('name')}: unknown type {tname}")
                        continue
                    if length > 1:
                        self.errors.append(f"message {el.get('name')}.{f.get('name')}: length only allowed on primitives")
                fields.append(Field(f.get("name"), int(f.get("id")), typ,
                                    since=int(f.get("sinceVersion", "0")),
                                    desc=f.get("description", "")))
            m = Message(el.get("name"), int(el.get("id")), fields,
                        int(el.get("blockLength")) if el.get("blockLength") else None,
                        desc=el.get("description", ""))
            self._layout(m, m.fields, f"message {m.name}")
            if m.size % 16 != 0:
                self.errors.append(f"message {m.name}: size {m.size} is not a multiple of 16")
            self.messages.append(m)
        ids = {}
        for m in self.messages:
            if m.id in ids:
                self.errors.append(f"duplicate template id {m.id}: {ids[m.id]} and {m.name}")
            ids[m.id] = m.name
        self.reasons = []
        rpath = os.path.join(os.path.dirname(path), "reasons.csv")
        if os.path.exists(rpath):
            import csv
            with open(rpath) as fh:
                for row in csv.DictReader(fh):
                    self.reasons.append((int(row["code"]), row["name"], row["family"], row["description"]))
            codes = [c for c, *_ in self.reasons]
            if len(codes) != len(set(codes)): self.errors.append("reasons.csv: duplicate code")
        hdr = self.types.get(self.header_type)
        if not isinstance(hdr, Composite) or hdr.size != 48:
            self.errors.append("FrameHeader must be a 48 byte composite")

    def _layout(self, owner, fields, label):
        off = 0
        for f in fields:
            a = f.typ.align
            if off % a != 0:
                self.errors.append(
                    f"{label}: field '{f.name}' at offset {off} needs alignment {a}; "
                    f"add explicit padding of {a - off % a} bytes before it")
                off += (a - off % a)
            f.offset = off
            off += f.size
        owner.size = off
        if owner.declared is not None and owner.declared != off:
            self.errors.append(
                f"{label}: declared length {owner.declared} but computed {off}")

    def content_hash(self):
        with open(self.path, "rb") as fh:
            return hashlib.sha256(fh.read()).hexdigest()

# --------------------------------------------------------------------- C++
def cpp_type(t):
    if isinstance(t, Prim):
        base = PRIM[t.prim][1]
        return base, t.length
    if isinstance(t, Enum):
        return t.name, 1
    if isinstance(t, BitSet):
        return f"{PRIM[t.enc][1]} /*{t.name}*/", 1
    if isinstance(t, Composite):
        return t.name, 1
    raise TypeError(t)

def emit_field_cpp(f):
    base, n = cpp_type(f.typ)
    if n == 1:
        return f"    {base} {f.name};"
    if isinstance(f.typ, Prim) and f.typ.prim == "char":
        return f"    char {f.name}[{n}];"
    return f"    std::array<{base}, {n}> {f.name};"

def gen_cpp(s: Schema) -> str:
    L = []
    L.append("// GENERATED by tools/sbegen.py from schema/trading.xml. Do not edit.")
    L.append(f"// schema id {s.id} version {s.version} sha256 {s.content_hash()[:16]}")
    L.append("#pragma once")
    L.append("#include <cstdint>\n#include <cstddef>\n#include <array>\n#include <type_traits>\n#include <cstring>\n")
    L.append(f"namespace {s.package} {{\n")
    L.append(f"inline constexpr uint16_t SCHEMA_ID = {s.id};")
    L.append(f"inline constexpr uint16_t SCHEMA_VERSION = {s.version};")
    L.append(f"inline constexpr uint16_t MAX_BLOCK_LENGTH = {max(m.size for m in s.messages)};\n")
    for e in s.enums:
        L.append(f"enum class {e.name} : {PRIM[e.enc][1]} {{")
        L.append("    Unset = 0,")
        for n, v in e.values:
            L.append(f"    {n} = {v},")
        L.append("};")
        L.append(f"inline const char* to_string({e.name} v) noexcept {{")
        L.append("    switch (v) {")
        for n, v in e.values:
            L.append(f"        case {e.name}::{n}: return \"{n}\";")
        L.append(f"        default: return \"Unset\";")
        L.append("    }\n}\n")
    for b in s.sets:
        L.append(f"namespace {b.name} {{")
        L.append(f"    using type = {PRIM[b.enc][1]};")
        for n, bit in b.choices:
            L.append(f"    inline constexpr type {n} = type(1) << {bit};")
        L.append("}\n")
    if s.reasons:
        L.append("enum class Reason : uint16_t {")
        for code, name, fam, desc in s.reasons:
            L.append(f"    {name} = {code},   // {fam}: {desc}")
        L.append("};")
        L.append("inline const char* to_string(Reason r) noexcept {")
        L.append("    switch (r) {")
        for code, name, fam, desc in s.reasons:
            L.append(f"        case Reason::{name}: return \"{name}\";")
        L.append("        default: return \"?\";")
        L.append("    }\n}\n")
    for c in s.composites:
        L.append(f"struct {c.name} {{")
        L.append(f"    static constexpr size_t SIZE = {c.size};")
        for f in c.fields:
            L.append(emit_field_cpp(f))
        L.append("};")
        L.append(f"static_assert(sizeof({c.name}) == {c.size}, \"{c.name} size\");")
        L.append(f"static_assert(std::is_trivially_copyable_v<{c.name}>);")
        for f in c.fields:
            L.append(f"static_assert(offsetof({c.name}, {f.name}) == {f.offset});")
        L.append("")
    L.append("enum class TemplateId : uint16_t {")
    for m in s.messages:
        L.append(f"    {m.name} = {m.id},")
    L.append("};\n")
    for m in s.messages:
        L.append(f"struct {m.name} {{")
        L.append(f"    static constexpr TemplateId TEMPLATE_ID = TemplateId::{m.name};")
        L.append(f"    static constexpr uint16_t BLOCK_LENGTH = {m.size};")
        for f in m.fields:
            L.append(emit_field_cpp(f))
        L.append("};")
        L.append(f"static_assert(sizeof({m.name}) == {m.size}, \"{m.name} size\");")
        L.append(f"static_assert(sizeof({m.name}) % 16 == 0);")
        L.append(f"static_assert(std::is_trivially_copyable_v<{m.name}>);")
        for f in m.fields:
            L.append(f"static_assert(offsetof({m.name}, {f.name}) == {f.offset});")
        L.append("")
    L.append("struct MessageInfo { TemplateId id; uint16_t blockLength; const char* name; };")
    L.append("inline constexpr MessageInfo MESSAGES[] = {")
    for m in s.messages:
        L.append(f"    {{TemplateId::{m.name}, {m.size}, \"{m.name}\"}},")
    L.append("};")
    L.append("inline constexpr size_t MESSAGE_COUNT = sizeof(MESSAGES) / sizeof(MESSAGES[0]);")
    L.append("inline const MessageInfo* lookup(uint16_t templateId) noexcept {")
    L.append("    for (const auto& m : MESSAGES) if (uint16_t(m.id) == templateId) return &m;")
    L.append("    return nullptr;\n}\n")
    L.append("// Frame = FrameHeader followed by the message block; frameLength covers both.")
    L.append("template <class M> struct Frame {")
    L.append("    FrameHeader header;")
    L.append("    M body;")
    L.append("    static constexpr uint32_t FRAME_LENGTH = uint32_t(sizeof(FrameHeader) + sizeof(M));")
    L.append("    static_assert(sizeof(FrameHeader) == 48);")
    L.append("    void init() noexcept {")
    L.append("        std::memset(this, 0, sizeof(*this));")
    L.append("        header.frameLength = FRAME_LENGTH;")
    L.append("        header.templateId = uint16_t(M::TEMPLATE_ID);")
    L.append("        header.schemaVersion = SCHEMA_VERSION;")
    L.append("    }")
    L.append("};")
    L.append("template <class M> inline const M* as(const FrameHeader* h) noexcept {")
    L.append("    return h->templateId == uint16_t(M::TEMPLATE_ID)")
    L.append("        ? reinterpret_cast<const M*>(reinterpret_cast<const std::byte*>(h) + sizeof(FrameHeader))")
    L.append("        : nullptr;\n}\n")
    L.append(f"}} // namespace {s.package}")
    return "\n".join(L) + "\n"

# --------------------------------------------------------------------- Python
def py_fmt(t):
    if isinstance(t, Prim):
        code = PRIM[t.prim][2]
        if t.prim == "char":
            return f"{t.length}s", 1
        return (f"{t.length}{code}" if t.length > 1 else code), t.length
    if isinstance(t, Enum):
        return PRIM[t.enc][2], 1
    if isinstance(t, BitSet):
        return PRIM[t.enc][2], 1
    if isinstance(t, Composite):
        return f"{t.size}s", 1
    raise TypeError(t)

def gen_py(s: Schema) -> str:
    L = []
    L.append('"""GENERATED by tools/sbegen.py from schema/trading.xml. Do not edit."""')
    L.append("import struct, enum\nfrom dataclasses import dataclass, field\nfrom typing import Tuple\n")
    L.append(f"SCHEMA_ID = {s.id}\nSCHEMA_VERSION = {s.version}\nSCHEMA_SHA256 = '{s.content_hash()}'\n")
    for e in s.enums:
        L.append(f"class {e.name}(enum.IntEnum):")
        L.append("    Unset = 0")
        for n, v in e.values:
            L.append(f"    {n} = {v}")
        L.append("")
    for b in s.sets:
        L.append(f"class {b.name}(enum.IntFlag):")
        for n, bit in b.choices:
            L.append(f"    {n} = 1 << {bit}")
        L.append("")
    if s.reasons:
        L.append("class Reason(enum.IntEnum):")
        for code, name, fam, desc in s.reasons:
            L.append(f"    {name} = {code}")
        L.append("")
    L.append("class _Codec:")
    L.append("    __slots__ = ()")
    L.append("    _FMT = ''; _FIELDS = (); _ARRAYS = {}; _COMPOSITES = {}")
    L.append("    def pack(self) -> bytes:")
    L.append("        vals = []")
    L.append("        for f in self._FIELDS:")
    L.append("            v = getattr(self, f)")
    L.append("            if f in self._COMPOSITES: vals.append(v.pack())")
    L.append("            elif f in self._ARRAYS: vals.extend(v)")
    L.append("            elif isinstance(v, str): vals.append(v.encode('ascii'))")
    L.append("            else: vals.append(int(v))")
    L.append("        return struct.pack(self._FMT, *vals)")
    L.append("    @classmethod")
    L.append("    def unpack(cls, buf: bytes, offset: int = 0):")
    L.append("        raw = struct.unpack_from(cls._FMT, buf, offset)")
    L.append("        obj = cls(); i = 0")
    L.append("        for f in cls._FIELDS:")
    L.append("            if f in cls._COMPOSITES:")
    L.append("                setattr(obj, f, cls._COMPOSITES[f].unpack(raw[i])); i += 1")
    L.append("            elif f in cls._ARRAYS:")
    L.append("                n = cls._ARRAYS[f]; setattr(obj, f, list(raw[i:i+n])); i += n")
    L.append("            else:")
    L.append("                v = raw[i]; i += 1")
    L.append("                if isinstance(v, bytes): v = v.rstrip(b'\\x00').decode('ascii', 'replace')")
    L.append("                setattr(obj, f, v)")
    L.append("        return obj")
    L.append("    def __repr__(self):")
    L.append("        return type(self).__name__ + '(' + ', '.join(f'{f}={getattr(self,f)!r}' for f in self._FIELDS if not f.startswith(('pad','reserved'))) + ')'")
    L.append("")
    def emit_struct(name, fields, extra):
        fmt = "<"
        arrays = {}
        comps = {}
        L.append("@dataclass")
        L.append(f"class {name}(_Codec):")
        for f in fields:
            code, n = py_fmt(f.typ)
            fmt += code
            if isinstance(f.typ, Composite):
                comps[f.name] = f.typ.name
                L.append(f"    {f.name}: '{f.typ.name}' = field(default_factory=lambda: {f.typ.name}())")
            elif isinstance(f.typ, Prim) and f.typ.prim == "char":
                L.append(f"    {f.name}: str = ''")
            elif n > 1:
                arrays[f.name] = n
                L.append(f"    {f.name}: list = field(default_factory=lambda: [0]*{n})")
            else:
                L.append(f"    {f.name}: int = 0")
        L.append(f"    _FMT = '{fmt}'")
        L.append(f"    _FIELDS = ({', '.join(repr(f.name) for f in fields)},)")
        L.append(f"    _ARRAYS = {arrays!r}")
        L.append("    _COMPOSITES = {" + ", ".join(f"'{k}': {v}" for k, v in comps.items()) + "}")
        for k, v in extra.items():
            L.append(f"    {k} = {v}")
        L.append(f"    SIZE = {sum(f.size for f in fields)}")
        L.append("")
    for c in s.composites:
        emit_struct(c.name, c.fields, {})
    for m in s.messages:
        emit_struct(m.name, m.fields, {"TEMPLATE_ID": m.id, "BLOCK_LENGTH": m.size})
    L.append("MESSAGES = {")
    for m in s.messages:
        L.append(f"    {m.id}: {m.name},")
    L.append("}")
    L.append("HEADER_SIZE = 48\n")
    L.append("def frame(msg, seq=0, seq_ts=0, origin_ts=0, cause_seq=0, source_id=0, flags=0, stream_id=0) -> bytes:")
    L.append("    h = FrameHeader(frameLength=HEADER_SIZE + msg.BLOCK_LENGTH, templateId=msg.TEMPLATE_ID, schemaVersion=SCHEMA_VERSION,")
    L.append("                    seq=seq, seqTs=seq_ts, originTs=origin_ts, causeSeq=cause_seq, sourceId=source_id, flags=flags, streamId=stream_id)")
    L.append("    return h.pack() + msg.pack()\n")
    L.append("def iter_frames(buf: bytes):")
    L.append("    off = 0")
    L.append("    while off + HEADER_SIZE <= len(buf):")
    L.append("        h = FrameHeader.unpack(buf, off)")
    L.append("        if h.frameLength < HEADER_SIZE or off + h.frameLength > len(buf): raise ValueError(f'bad frame at {off}')")
    L.append("        cls = MESSAGES.get(h.templateId)")
    L.append("        body = cls.unpack(buf, off + HEADER_SIZE) if cls else buf[off+HEADER_SIZE:off+h.frameLength]")
    L.append("        yield h, body")
    L.append("        off += h.frameLength\n")
    return "\n".join(L) + "\n"

# --------------------------------------------------------------------- Go
GO_PRIM = {"char": "byte", "int8": "int8", "uint8": "uint8", "int16": "int16", "uint16": "uint16",
           "int32": "int32", "uint32": "uint32", "int64": "int64", "uint64": "uint64"}

def go_name(n):
    n = n[0].upper() + n[1:]
    return n.replace("Id", "ID") if n.endswith("Id") else n

def go_type(t):
    if isinstance(t, Prim):
        base = GO_PRIM[t.prim]
        return (f"[{t.length}]{base}" if t.length > 1 else base), t.length
    if isinstance(t, Enum) or isinstance(t, BitSet):
        return t.name, 1
    if isinstance(t, Composite):
        return t.name, 1
    raise TypeError(t)

def go_put(prim, off, expr):
    if prim in ("uint8", "char"): return f"b[{off}] = byte({expr})"
    if prim == "int8": return f"b[{off}] = byte({expr})"
    w = PRIM[prim][0] * 8
    return f"binary.LittleEndian.PutUint{w}(b[{off}:], uint{w}({expr}))"   # always cast: enums and sets are named types

def go_get(prim, off, gotype):
    if prim in ("uint8", "char"): return f"{gotype}(b[{off}])"
    if prim == "int8": return f"int8(b[{off}])"
    w = PRIM[prim][0] * 8
    return f"{gotype}(binary.LittleEndian.Uint{w}(b[{off}:]))"

def gen_go(s: Schema) -> str:
    L = []
    L.append("// GENERATED by tools/sbegen.py from schema/trading.xml. Do not edit.")
    L.append(f"// schema id {s.id} version {s.version} sha256 {s.content_hash()[:16]}")
    L.append(f"package {s.package}\n")
    L.append('import (\n\t"encoding/binary"\n\t"errors"\n)\n')
    L.append(f"const SchemaID uint16 = {s.id}")
    L.append(f"const SchemaVersion uint16 = {s.version}")
    L.append("const HeaderSize = 48")
    L.append(f"const MaxBlockLength = {max(m.size for m in s.messages)}\n")
    L.append('var ErrShort = errors.New("trading: buffer too short")')
    L.append('var ErrBadFrame = errors.New("trading: bad frame")\n')
    for e in s.enums:
        L.append(f"type {e.name} {GO_PRIM[e.enc]}\n")
        L.append("const (")
        L.append(f"\t{e.name}Unset {e.name} = 0")
        for n, v in e.values:
            L.append(f"\t{e.name}{n} {e.name} = {v}")
        L.append(")\n")
        L.append(f"func (v {e.name}) String() string {{")
        L.append("\tswitch v {")
        for n, v in e.values:
            L.append(f"\tcase {e.name}{n}:\n\t\treturn \"{n}\"")
        L.append("\t}\n\treturn \"Unset\"\n}\n")
    for b in s.sets:
        L.append(f"type {b.name} {GO_PRIM[b.enc]}\n")
        L.append("const (")
        for n, bit in b.choices:
            L.append(f"\t{b.name}{go_name(n)} {b.name} = 1 << {bit}")
        L.append(")\n")
    if s.reasons:
        L.append("type Reason uint16\n")
        L.append("const (")
        for code, name, fam, desc in s.reasons:
            L.append(f"\tReason{name} Reason = {code}")
        L.append(")\n")
        L.append("func (r Reason) String() string {\n\tswitch r {")
        for code, name, fam, desc in s.reasons:
            L.append(f"\tcase Reason{name}:\n\t\treturn \"{name}\"")
        L.append("\t}\n\treturn \"?\"\n}\n")

    def emit_struct(name, fields, size, is_msg, tid=None):
        L.append(f"type {name} struct {{")
        for f in fields:
            gt, n = go_type(f.typ)
            L.append(f"\t{go_name(f.name)} {gt}")
        L.append("}\n")
        if is_msg:
            L.append(f"const {name}TemplateID uint16 = {tid}\n")
            L.append(f"func (*{name}) TemplateID() uint16 {{ return {tid} }}")
        L.append(f"func (*{name}) Size() int {{ return {size} }}\n")
        # Pack
        L.append(f"func (m *{name}) Pack(b []byte) {{")
        L.append(f"\t_ = b[{size - 1}]")
        for f in fields:
            fn = go_name(f.name); t = f.typ
            if isinstance(t, Composite):
                L.append(f"\tm.{fn}.Pack(b[{f.offset}:{f.offset + t.size}])")
            elif isinstance(t, Enum) or isinstance(t, BitSet):
                L.append("\t" + go_put(t.enc, f.offset, f"m.{fn}"))
            elif t.length > 1:
                w = PRIM[t.prim][0]
                L.append(f"\tfor i := 0; i < {t.length}; i++ {{")
                L.append("\t\t" + go_put(t.prim, f"{f.offset}+i*{w}", f"m.{fn}[i]"))
                L.append("\t}")
            else:
                L.append("\t" + go_put(t.prim, f.offset, f"m.{fn}"))
        L.append("}\n")
        # Unpack
        L.append(f"func (m *{name}) Unpack(b []byte) error {{")
        L.append(f"\tif len(b) < {size} {{\n\t\treturn ErrShort\n\t}}")
        for f in fields:
            fn = go_name(f.name); t = f.typ
            if isinstance(t, Composite):
                L.append(f"\tif err := m.{fn}.Unpack(b[{f.offset}:{f.offset + t.size}]); err != nil {{\n\t\treturn err\n\t}}")
            elif isinstance(t, Enum) or isinstance(t, BitSet):
                L.append(f"\tm.{fn} = " + go_get(t.enc, f.offset, t.name))
            elif t.length > 1:
                w = PRIM[t.prim][0]
                L.append(f"\tfor i := 0; i < {t.length}; i++ {{")
                L.append(f"\t\tm.{fn}[i] = " + go_get(t.prim, f"{f.offset}+i*{w}", GO_PRIM[t.prim]))
                L.append("\t}")
            else:
                L.append(f"\tm.{fn} = " + go_get(t.prim, f.offset, GO_PRIM[t.prim]))
        L.append("\treturn nil\n}\n")

    for c in s.composites:
        emit_struct(c.name, c.fields, c.size, False)
    for m in s.messages:
        emit_struct(m.name, m.fields, m.size, True, m.id)
    L.append("// Message is any generated message body.")
    L.append("type Message interface {\n\tTemplateID() uint16\n\tSize() int\n\tPack(b []byte)\n\tUnpack(b []byte) error\n}\n")
    L.append("// New returns an empty message for a template id, or nil if unknown.")
    L.append("func New(templateID uint16) Message {\n\tswitch templateID {")
    for m in s.messages:
        L.append(f"\tcase {m.id}:\n\t\treturn &{m.name}{{}}")
    L.append("\t}\n\treturn nil\n}\n")
    L.append("// MessageName returns the schema name of a template id.")
    L.append("func MessageName(templateID uint16) string {\n\tswitch templateID {")
    for m in s.messages:
        L.append(f"\tcase {m.id}:\n\t\treturn \"{m.name}\"")
    L.append("\t}\n\treturn \"?\"\n}\n")
    L.append("// Frame encodes header + body; the header's FrameLength, TemplateID and SchemaVersion are filled in.")
    L.append("func Frame(h FrameHeader, m Message) []byte {")
    L.append("\th.FrameLength = uint32(HeaderSize + m.Size())")
    L.append("\th.TemplateID = m.TemplateID()")
    L.append("\th.SchemaVersion = SchemaVersion")
    L.append("\tb := make([]byte, h.FrameLength)")
    L.append("\th.Pack(b[:HeaderSize])")
    L.append("\tm.Pack(b[HeaderSize:])")
    L.append("\treturn b\n}\n")
    L.append("// IterFrames walks a buffer of frames. body is nil for an unknown template id.")
    L.append("func IterFrames(buf []byte, fn func(h FrameHeader, body Message, raw []byte) error) error {")
    L.append("\toff := 0")
    L.append("\tfor off+HeaderSize <= len(buf) {")
    L.append("\t\tvar h FrameHeader")
    L.append("\t\tif err := h.Unpack(buf[off:]); err != nil {\n\t\t\treturn err\n\t\t}")
    L.append("\t\tif h.FrameLength < HeaderSize || off+int(h.FrameLength) > len(buf) {\n\t\t\treturn ErrBadFrame\n\t\t}")
    L.append("\t\traw := buf[off+HeaderSize : off+int(h.FrameLength)]")
    L.append("\t\tbody := New(h.TemplateID)")
    L.append("\t\tif body != nil {\n\t\t\tif len(raw) < body.Size() || body.Unpack(raw) != nil {\n\t\t\t\tbody = nil\n\t\t\t}\n\t\t}")
    L.append("\t\tif err := fn(h, body, raw); err != nil {\n\t\t\treturn err\n\t\t}")
    L.append("\t\toff += int(h.FrameLength)")
    L.append("\t}\n\treturn nil\n}")
    return "\n".join(L) + "\n"

# --------------------------------------------------------------------- layout report
def gen_layout(s: Schema) -> str:
    L = [f"# Layout report: schema {s.package} v{s.version}", ""]
    L.append("## Composites\n")
    for c in s.composites:
        L.append(f"### {c.name} ({c.size} bytes)\n")
        L.append("| offset | field | type | size |\n|---:|---|---|---:|")
        for f in c.fields:
            L.append(f"| {f.offset} | {f.name} | {f.typ.prim}{'[%d]'%f.typ.length if f.typ.length>1 else ''} | {f.size} |")
        L.append("")
    L.append("## Messages\n")
    for m in s.messages:
        L.append(f"### {m.name} (template {m.id}, {m.size} bytes)\n")
        L.append("| offset | field | type | size |\n|---:|---|---|---:|")
        for f in m.fields:
            t = f.typ
            tn = t.name if not isinstance(t, Prim) else (t.prim + (f"[{t.length}]" if t.length > 1 else ""))
            L.append(f"| {f.offset} | {f.name} | {tn} | {f.size} |")
        L.append("")
    return "\n".join(L) + "\n"

def main():
    if len(sys.argv) < 3:
        print("usage: sbegen.py schema.xml outdir"); sys.exit(2)
    s = Schema(sys.argv[1])
    if s.errors:
        for e in s.errors: print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
    out = sys.argv[2]
    os.makedirs(os.path.join(out, "cpp"), exist_ok=True)
    os.makedirs(os.path.join(out, "py"), exist_ok=True)
    os.makedirs(os.path.join(out, "go"), exist_ok=True)
    with open(os.path.join(out, "cpp", "trading.hpp"), "w") as fh: fh.write(gen_cpp(s))
    with open(os.path.join(out, "py", "trading.py"), "w") as fh: fh.write(gen_py(s))
    with open(os.path.join(out, "go", "trading.go"), "w") as fh: fh.write(gen_go(s))
    with open(os.path.join(out, "go", "go.mod"), "w") as fh: fh.write(f"module {s.package}\n\ngo 1.22\n")
    with open(os.path.join(out, "layout.md"), "w") as fh: fh.write(gen_layout(s))
    print(f"ok: {len(s.messages)} messages, {len(s.composites)} composites, "
          f"{len(s.enums)} enums, {len(s.sets)} sets -> {out}")

if __name__ == "__main__":
    main()
