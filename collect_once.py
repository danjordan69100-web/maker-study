#!/usr/bin/env python3
"""
Version GitHub Actions du collecteur : UN cycle de snapshots puis exit.
Ecrit un CSV horodate dans snaps/ (committe par le workflow).
Aucune cle, lecture seule d'APIs publiques Polymarket.
"""
import csv, json, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path

HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
REQ_SLEEP = 0.3
BUCKETS = {"Weather": 120, "Esports": 40, "Sports": 25, "Politics": 25, "Crypto": 15}
LONGTAIL_SAMPLE = 25


def get(url, timeout=20):
    req = urllib.request.Request(url, headers=HDR)
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def fetch_universe():
    markets, cursor = [], ""
    while True:
        d = get("https://clob.polymarket.com/sampling-markets"
                + (f"?next_cursor={cursor}" if cursor else ""))
        markets.extend(d.get("data", []))
        cursor = d.get("next_cursor", "LTE=")
        if cursor == "LTE=":
            break
        time.sleep(0.1)

    def daily(m):
        return sum(float(x.get("rewards_daily_rate", 0))
                   for x in (m.get("rewards") or {}).get("rates") or [])

    def tags(m):
        return [str(t) for t in (m.get("tags") or [])]

    chosen, seen = [], set()

    def add(m):
        tok = (m.get("tokens") or [{}])[0].get("token_id")
        if not tok or tok in seen:
            return
        seen.add(tok)
        r = m.get("rewards") or {}
        chosen.append({
            "token_id": tok, "cid": m.get("condition_id", ""),
            "question": (m.get("question") or "")[:80].replace("\n", " "),
            "tag": (tags(m) or ["?"])[0],
            "budget": daily(m),
            "min_size": float(r.get("min_size") or 0),
            "max_spread": float(r.get("max_spread") or 3.0),
        })

    for bucket, n in BUCKETS.items():
        pool = sorted((m for m in markets if bucket in tags(m)), key=daily, reverse=True)
        for m in pool[:n]:
            add(m)
    lt = [m for m in markets if 3 <= daily(m) <= 20]
    for m in lt[::max(1, len(lt) // LONGTAIL_SAMPLE)][:LONGTAIL_SAMPLE]:
        add(m)
    return chosen


def main():
    ts = datetime.now(timezone.utc)
    universe = fetch_universe()
    rows = []
    for u in universe:
        try:
            book = get(f"https://clob.polymarket.com/book?token_id={u['token_id']}", timeout=15)
            bids = [(float(x["price"]), float(x["size"])) for x in book.get("bids", [])]
            asks = [(float(x["price"]), float(x["size"])) for x in book.get("asks", [])]
            if not bids or not asks:
                continue
            bb, ba = max(p for p, _ in bids), min(p for p, _ in asks)
            mid = (bb + ba) / 2
            lo, hi = mid - u["max_spread"] / 100, mid + u["max_spread"] / 100
            rows.append([ts.isoformat(timespec="seconds"), u["token_id"], u["cid"],
                         u["question"], u["tag"], u["budget"], u["min_size"], u["max_spread"],
                         bb, ba, round(mid, 4),
                         round(sum(p * s for p, s in bids if p >= lo), 2),
                         round(sum(p * s for p, s in asks if p <= hi), 2),
                         sum(1 for p, _ in bids if p >= lo),
                         sum(1 for p, _ in asks if p <= hi)])
        except Exception:
            pass
        time.sleep(REQ_SLEEP)

    out = Path("snaps")
    out.mkdir(exist_ok=True)
    fname = out / f"snap_{ts.strftime('%Y%m%dT%H%M')}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ts", "token_id", "cid", "question", "tag", "budget_daily",
                    "min_size", "max_spread", "best_bid", "best_ask", "mid",
                    "depth_bid_range", "depth_ask_range", "n_bid_levels", "n_ask_levels"])
        w.writerows(rows)
    print(f"{fname}: {len(rows)} snaps")


if __name__ == "__main__":
    main()
