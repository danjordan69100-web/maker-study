#!/usr/bin/env python3
"""
Phase A2 - Collecteur d'etude de faisabilite market making Polymarket.
AUCUN ordre, AUCUNE cle : lecture seule d'APIs publiques, stockage SQLite local.

Toutes les 10 min : pour l'univers cible, snapshot du carnet (best bid/ask, mid,
depth postee dans le range eligible aux rewards) -> maker_study.db.
L'univers (et les budgets rewards) est rafraichi toutes les heures depuis
clob.polymarket.com/sampling-markets.

But : mesurer la concurrence DANS LE TEMPS (pas un instantane) et detecter les
poches persistantes budget/depth eleve -> input du GATE de faisabilite.
"""
import json, sqlite3, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).parent
DB = BASE / "maker_study.db"
LOG = BASE / "collect.log"
HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
CYCLE_SEC = 600          # 10 min entre snapshots
UNIVERSE_REFRESH = 3600  # 1 h entre refreshs d'univers
REQ_SLEEP = 0.35         # politesse API

# Tailles d'univers par bucket (top par budget quotidien)
BUCKETS = {"Weather": 120, "Esports": 40, "Sports": 25, "Politics": 25, "Crypto": 15}
LONGTAIL_SAMPLE = 25     # petite longue traine 3-20$/j en reference


def log(msg):
    line = f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def get(url, timeout=20):
    req = urllib.request.Request(url, headers=HDR)
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def init_db():
    con = sqlite3.connect(DB)
    con.execute("""CREATE TABLE IF NOT EXISTS book_snap(
        ts TEXT, token_id TEXT, cid TEXT, question TEXT, tag TEXT,
        budget_daily REAL, min_size REAL, max_spread REAL,
        best_bid REAL, best_ask REAL, mid REAL,
        depth_bid_range REAL, depth_ask_range REAL,
        n_bid_levels INTEGER, n_ask_levels INTEGER)""")
    con.execute("""CREATE TABLE IF NOT EXISTS universe_snap(
        ts TEXT, n_markets INTEGER, budget_total REAL)""")
    con.execute("CREATE INDEX IF NOT EXISTS ix_bs ON book_snap(token_id, ts)")
    con.commit()
    return con


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
            "question": (m.get("question") or "")[:80],
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

    budget_total = sum(daily(m) for m in markets)
    return chosen, len(markets), budget_total


def snapshot_cycle(con, universe):
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows, errs = [], 0
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
            dbid = sum(p * s for p, s in bids if p >= lo)
            dask = sum(p * s for p, s in asks if p <= hi)
            rows.append((ts, u["token_id"], u["cid"], u["question"], u["tag"],
                         u["budget"], u["min_size"], u["max_spread"],
                         bb, ba, mid, round(dbid, 2), round(dask, 2),
                         sum(1 for p, _ in bids if p >= lo),
                         sum(1 for p, _ in asks if p <= hi)))
        except Exception:
            errs += 1
        time.sleep(REQ_SLEEP)
    con.executemany("INSERT INTO book_snap VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    return len(rows), errs


def main():
    con = init_db()
    universe, last_refresh = [], 0.0
    log("collector start")
    while True:
        t0 = time.time()
        try:
            if time.time() - last_refresh > UNIVERSE_REFRESH or not universe:
                universe, n_mkts, budget = fetch_universe()
                last_refresh = time.time()
                con.execute("INSERT INTO universe_snap VALUES(?,?,?)",
                            (datetime.now(timezone.utc).isoformat(timespec="seconds"),
                             n_mkts, round(budget, 2)))
                con.commit()
                log(f"universe refreshed: {len(universe)} cibles / {n_mkts} marches, "
                    f"budget total {budget:,.0f}$/j")
            n, errs = snapshot_cycle(con, universe)
            log(f"cycle ok: {n} snaps, {errs} err, {time.time()-t0:.0f}s")
        except Exception as e:
            log(f"cycle ERROR: {str(e)[:200]}")
        time.sleep(max(10, CYCLE_SEC - (time.time() - t0)))


if __name__ == "__main__":
    main()
