#!/usr/bin/env python3
"""Generate fixtures/day1.json and fixtures/day2.json.
Day 2 differs from day 1 by: one new listing, one delisting, a 4:1 split, a tick-table change,
ETB/HTB list moves, a fee change, a refPrice roll and a restricted-list add."""
import json, os, random
random.seed(20260915)
HERE = os.path.dirname(__file__)

VENUES = [
    {"venueId": 1, "mic": "XNYS", "name": "NYSE", "venueType": "Exchange", "protocol": 2, "sessionIds": [101, 102], "oneWayLatencyNs": 38000},
    {"venueId": 2, "mic": "XNAS", "name": "Nasdaq", "venueType": "Exchange", "protocol": 1, "sessionIds": [201, 202, 203], "oneWayLatencyNs": 12000},
    {"venueId": 3, "mic": "BATS", "name": "Cboe BZX", "venueType": "Exchange", "protocol": 3, "sessionIds": [301], "oneWayLatencyNs": 21000},
    {"venueId": 4, "mic": "IEXG", "name": "IEX", "venueType": "Exchange", "protocol": 9, "sessionIds": [401], "oneWayLatencyNs": 25000},
    {"venueId": 9, "mic": "INTL", "name": "Internal", "venueType": "Internal", "protocol": 0, "sessionIds": [], "oneWayLatencyNs": 0},
]
TICKS = [
    {"tickTableId": 1, "bands": [[0, 10000], [100000000, 1000000]]},          # sub-$1: $0.0001, else $0.01
    {"tickTableId": 2, "bands": [[0, 10000], [100000000, 500000]]},           # half-penny pilot
]
BASE = [
    ("AAPL", "Apple Inc", 2, 22745000000, "037833100", "US0378331005", "BBG000B9XRY4"),
    ("MSFT", "Microsoft Corp", 2, 41520000000, "594918104", "US5949181045", "BBG000BPH459"),
    ("JPM",  "JPMorgan Chase", 1, 21015000000, "46625H100", "US46625H1005", "BBG000DMBXR2"),
    ("GS",   "Goldman Sachs", 1, 48150000000, "38141G104", "US38141G1040", "BBG000C6CFJ5"),
    ("MS",   "Morgan Stanley", 1, 10430000000, "617446448", "US6174464486", "BBG000BLZRJ2"),
    ("SPY",  "SPDR S&P 500 ETF", 1, 55830000000, "78462F103", "US78462F1030", "BBG000BDTBL9"),
    ("GME",  "GameStop Corp", 1, 2315000000, "36467W109", "US36467W1099", "BBG000BB5BF6"),
    ("TSLA", "Tesla Inc", 2, 26340000000, "88160R101", "US88160R1014", "BBG000N9MNX3"),
    ("XYZQ", "Xyzq Holdings (to be delisted)", 2, 45000000, "999999999", "US9999999991", "BBG00000ZZZ1"),
    ("NVDA", "NVIDIA Corp", 2, 12010000000, "67066G104", "US67066G1040", "BBG000BBJQV0"),
]

def instruments(day):
    out = []
    for i, (tk, nm, ven, px, cusip, isin, figi) in enumerate(BASE, start=1):
        rec = {"symbolIdx": i, "ticker": tk, "name": nm, "listingVenue": ven, "tickTableId": 1, "lotSize": 100,
               "refPrice": px, "prevClose": px, "adv30": random.randint(2_000_000, 80_000_000) * 1,
               "sharesOutstanding": random.randint(200, 20000) * 1_000_000, "issuerId": 1000 + i, "sector": 10 + (i % 4),
               "cusip": cusip, "isin": isin, "figi": figi, "marginClass": 1, "validFrom": 1_600_000_000_000_000_000,
               "instrumentType": "Etf" if tk == "SPY" else "CommonStock", "flags": ["marginable", "shortable"]}
        if day == 2:
            rec["refPrice"] = int(px * (1 + random.uniform(-0.03, 0.03)))
            rec["prevClose"] = rec["refPrice"]
            if tk == "XYZQ": rec["status"] = "Delisted"; rec["flags"] = []
            if tk == "NVDA":   # 4:1 split effective day 2
                rec["refPrice"] = px // 4; rec["prevClose"] = px; rec["sharesOutstanding"] *= 4
                rec["flags"].append("corpActionToday"); rec["lastCorpActionId"] = 5001
            if tk == "GME": rec["tickTableId"] = 2
        out.append(rec)
    if day == 2:
        out.append({"symbolIdx": 11, "ticker": "NEWCO", "name": "Newco Robotics (IPO)", "listingVenue": 2, "tickTableId": 1,
                    "lotSize": 100, "refPrice": 2400000000, "prevClose": 2400000000, "adv30": 0, "sharesOutstanding": 50_000_000,
                    "issuerId": 1011, "sector": 12, "cusip": "123456789", "isin": "US1234567890", "figi": "BBG00NEWCO001",
                    "marginClass": 3, "validFrom": 1_757_894_400_000_000_000, "flags": ["marginable"]})
    return out

def fixture(day):
    inst = instruments(day)
    listings = []
    for i in inst:
        for v in (1, 2, 3, 4):
            listings.append({"symbolIdx": i["symbolIdx"], "venueId": v, "venueSymbol": i["ticker"], "tradable": i.get("status") != "Delisted"})
    lists = {"etb": [1, 2, 3, 4, 5, 6, 8, 10], "htb": [7, 9], "restricted": [], "threshold": [7], "watch": []}
    if day == 2:
        lists = {"etb": [1, 2, 3, 4, 5, 6, 8, 10, 11], "htb": [7], "restricted": [5], "threshold": [7], "watch": [11]}
    cals = [{"venueId": v["venueId"], "date": 20260915 + (day - 1), "openTs": 1_757_950_200_000_000_000, "closeTs": 1_757_973_600_000_000_000,
             "auctionTs": 1_757_973_600_000_000_000} for v in VENUES if v["venueId"] != 9]
    ca = []
    if day == 2:
        ca.append({"actionId": 5001, "symbolIdx": 10, "actionType": "Split", "exDate": 20260916, "recordDate": 20260912,
                   "payDate": 20260915, "ratioNum": 4, "ratioDen": 1, "applied": True})
        ca.append({"actionId": 5002, "symbolIdx": 9, "actionType": "Delist", "exDate": 20260916, "applied": True})
    fees = [{"venueId": v, "feeCode": c, "liquidity": liq, "feePerShare": fee, "effectiveDate": 20260901}
            for v in (1, 2, 3, 4) for c, liq, fee in ((1, "Removed", 300000), (2, "Added", -200000))]
    if day == 2:
        fees[1]["feePerShare"] = -250000; fees[1]["effectiveDate"] = 20260916   # NYSE rebate change
    return {
        "instruments": inst, "listings": listings, "venues": VENUES, "tickTables": TICKS, "calendars": cals,
        "corporateActions": ca, "lists": lists, "fees": fees,
        "accounts": [{"accountIdx": 1, "accountType": "Firm", "routingProfile": 1, "externalRef": "FIRM-MM"},
                     {"accountIdx": 42, "accountType": "Margin", "routingProfile": 3, "limitSetId": 7, "externalRef": "CL-000042"}],
        "components": [{"sourceId": 1, "role": "Sequencer", "name": "seq-a"}, {"sourceId": 2, "role": "Gateway", "name": "gw-fix-1"},
                       {"sourceId": 3, "role": "Risk", "name": "risk-1"}, {"sourceId": 7, "role": "VenueGateway", "name": "vgw-xnas"},
                       {"sourceId": 8, "role": "FeedHandler", "name": "fh-itch-1"}, {"sourceId": 15, "role": "SecMaster", "name": "secmaster"}],
        "sources": [{"sourceId": 1, "sourceTs": 1_757_900_000_000_000_000, "fileHash": "aa" * 32},
                    {"sourceId": 2, "sourceTs": 1_757_900_100_000_000_000, "fileHash": "bb" * 32}],
    }

for d in (1, 2):
    json.dump(fixture(d), open(os.path.join(HERE, f"day{d}.json"), "w"), indent=1)
print("wrote day1.json day2.json")
