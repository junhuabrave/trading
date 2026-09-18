#!/usr/bin/env python3
"""
schema_check.py OLD.xml NEW.xml

Fails (exit 1) if NEW is not a backward-compatible evolution of OLD:

  * schema id unchanged; version must increase if anything changed
  * no template id removed or renumbered; a removed message must stay in the
    schema (marked deprecated="<version>") so historical logs still decode
  * within a message: existing fields keep name, id, type, length and offset;
    new fields are appended after all existing ones and carry sinceVersion
    equal to the new schema version
  * composites: same rule (existing layout frozen, new fields only at the end,
    which for a fixed-length composite means they must come out of reserved
    space with the declared length unchanged)
  * enums and sets: existing values keep their numbers; new values only added
  * FrameHeader is never changed
  * primitive aliases (Price, Qty, ...) never change their primitiveType

Also runs the layout rules from sbegen on NEW.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from sbegen import Schema, Prim, Enum, BitSet, Composite

def fail(msgs):
    for m in msgs: print("INCOMPATIBLE:", m, file=sys.stderr)
    sys.exit(1)

def field_sig(f):
    t = f.typ
    if isinstance(t, Prim):
        tn = f"{t.prim}[{t.length}]"
    else:
        tn = t.name
    return (f.name, f.id, tn, f.offset, f.size)

def check(old: Schema, new: Schema):
    errs = []
    if new.errors:
        errs += [f"new schema layout error: {e}" for e in new.errors]
    if old.id != new.id:
        errs.append(f"schema id changed {old.id} -> {new.id}")
    changed = False

    # primitive aliases
    for name, t in old.types.items():
        if isinstance(t, Prim) and name not in ("char", *[k for k in old.types if k == t.prim]):
            nt = new.types.get(name)
            if nt is None:
                errs.append(f"type alias {name} removed")
            elif not isinstance(nt, Prim) or nt.prim != t.prim or nt.length != t.length:
                errs.append(f"type alias {name} changed")

    # enums
    for e in old.enums:
        ne = new.types.get(e.name)
        if not isinstance(ne, Enum):
            errs.append(f"enum {e.name} removed"); continue
        if ne.enc != e.enc:
            errs.append(f"enum {e.name} encoding changed")
        ov = dict(e.values); nv = dict(ne.values)
        for n, v in ov.items():
            if n not in nv: errs.append(f"enum {e.name}.{n} removed")
            elif nv[n] != v: errs.append(f"enum {e.name}.{n} renumbered {v}->{nv[n]}")
        used = {}
        for n, v in nv.items():
            if v in used: errs.append(f"enum {e.name}: value {v} used by {used[v]} and {n}")
            used[v] = n
        if nv != ov: changed = True

    # sets
    for b in old.sets:
        nb = new.types.get(b.name)
        if not isinstance(nb, BitSet):
            errs.append(f"set {b.name} removed"); continue
        oc = dict(b.choices); nc = dict(nb.choices)
        for n, bit in oc.items():
            if n not in nc: errs.append(f"set {b.name}.{n} removed")
            elif nc[n] != bit: errs.append(f"set {b.name}.{n} moved bit {bit}->{nc[n]}")
        if nc != oc: changed = True

    # composites
    for c in old.composites:
        nc = new.types.get(c.name)
        if not isinstance(nc, Composite):
            errs.append(f"composite {c.name} removed"); continue
        if c.name == "FrameHeader" and [field_sig(f) for f in c.fields] != [field_sig(f) for f in nc.fields]:
            errs.append("FrameHeader changed; it is version 0 forever")
        if nc.size != c.size:
            errs.append(f"composite {c.name} size changed {c.size}->{nc.size}")
        osig = [field_sig(f) for f in c.fields if not f.name.startswith("reserved")]
        nsig = [field_sig(f) for f in nc.fields if not f.name.startswith("reserved")]
        if nsig[:len(osig)] != osig:
            errs.append(f"composite {c.name}: existing (non-reserved) fields changed or reordered")
        if nsig != osig: changed = True

    # messages
    oldm = {m.id: m for m in old.messages}
    newm = {m.id: m for m in new.messages}
    for tid, m in oldm.items():
        nm = newm.get(tid)
        if nm is None:
            errs.append(f"message {m.name} (template {tid}) removed; mark deprecated instead"); continue
        if nm.name != m.name:
            errs.append(f"template {tid} renamed {m.name}->{nm.name}")
        osig = [field_sig(f) for f in m.fields]
        nsig = [field_sig(f) for f in nm.fields]
        if nsig[:len(osig)] != osig:
            # allow trailing pad to be replaced by real fields at the same offsets
            core_old = [x for x in osig if not x[0].startswith("pad")]
            core_new = [x for x in nsig if not x[0].startswith("pad")]
            if core_new[:len(core_old)] != core_old:
                errs.append(f"message {m.name}: existing fields changed, reordered or removed")
            else:
                changed = True
                old_names = {x.name for x in m.fields}
                for f in nm.fields:
                    if f.name not in old_names and not f.name.startswith("pad") and f.since != new.version:
                        errs.append(f"message {m.name}: new field {f.name} must have sinceVersion={new.version}")
        if nm.size != m.size:
            errs.append(f"message {m.name} blockLength changed {m.size}->{nm.size}")
    names_old = {m.name: m.id for m in old.messages}
    for m in new.messages:
        if m.id not in oldm:
            changed = True
            if m.name in names_old:
                errs.append(f"message name {m.name} reused with new template id {m.id}")
    if changed and new.version <= old.version:
        errs.append(f"schema changed but version not bumped ({old.version} -> {new.version})")
    if errs: fail(errs)
    print(f"compatible: v{old.version} -> v{new.version}" + (" (no changes)" if not changed else ""))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__); sys.exit(2)
    check(Schema(sys.argv[1]), Schema(sys.argv[2]))
