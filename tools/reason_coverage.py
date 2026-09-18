#!/usr/bin/env python3
"""
reason_coverage.py JOURNAL...   every reason in schema/reasons.csv must appear in at least one
RiskDecision.reason or ExecReport.rejectReason or OrderState.reason across the journals given.
A code nobody can trigger is either dead or untested.
"""
import sys, os, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gen", "py"))
sys.path.insert(0, os.path.dirname(__file__))
import trading as t
from loginspect import frames

def main():
    reasons = {int(r["code"]): r["name"] for r in csv.DictReader(open(os.path.join(os.path.dirname(__file__), "..", "schema", "reasons.csv")))}
    seen = set()
    for path in sys.argv[1:]:
        for h, body in frames(path):
            if isinstance(body, t.RiskDecision): seen.add(body.reason)
            elif isinstance(body, t.ExecReport): seen.add(body.rejectReason)
            elif isinstance(body, t.OrderState): seen.add(body.reason)
            elif isinstance(body, t.VenueReject): seen.add(body.reason)
    missing = sorted(c for c in reasons if c not in seen)
    print(f"reason coverage: {len(reasons) - len(missing)}/{len(reasons)} codes produced by the tests")
    if missing:
        for c in missing: print(f"  MISSING {c} {reasons[c]}")
        sys.exit(1)

if __name__ == "__main__":
    main()
