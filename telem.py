# Heimdall-GPS — unified telemetry receiver for F-34 (trades) + Heimdall/PSY (market)
# ASGI app (FastAPI). Endpoints:
#   GET  /health
#   POST /ingest   (body is the raw pipe string; auth via ?token=... or Authorization: Bearer ...)
#   GET  /export/trades?date=YYYY-MM-DD
#   GET  /export/market?date=YYYY-MM-DD
#
# Auth:
#   - URL token:  ?token=<TELEM_URL_TOKEN>            (Render Alert Webhook query param)
#   - Header:     Authorization: Bearer <TELEM_SECRET> (TV “Custom Headers”)
#
# Storage: /data/telem.db (SQLite) + daily CSV append in /data

import os, io, csv, hashlib, sqlite3, datetime as dt
from typing import Optional, Dict, Any, Tuple
from fastapi import FastAPI, Request, HTTPException, Header, Query
from fastapi.responses import JSONResponse, StreamingResponse

# ------------------ CONFIG (defaults include your hard-coded tokens) ------------------
APP_NAME     = os.getenv("APP_NAME", "Heimdall-GPS")
DATA_DIR     = os.getenv("EXPORT_DIR", "/data")

# Accept multiple env names; fall back to your provided defaults
URL_TOKEN = (
    os.getenv("TELEM_URL_TOKEN")
    or os.getenv("TELEM_URL")
    or "0bb658f5cbaa696a882487a4fcb89bab"
)
HEADER_SECRET = (
    os.getenv("TELEM_SECRET")
    or os.getenv("TELEM_TOKEN")
    or "c9a9f6748a941a4177c827c67155c975"
)

# An extra optional shared token if you want a second header secret
ALT_HEADER_SECRET = os.getenv("TELEM_ALT_TOKEN", "sugarsecret123")

os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "telem.db")

# ------------------ DB utils ------------------
def utc_ts() -> int:
    return int(dt.datetime.utcnow().timestamp())

def db():
    conn = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    conn = db()
    conn.execute(
        """CREATE TABLE IF NOT EXISTS trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc INTEGER NOT NULL,
            sym TEXT NOT NULL, tf TEXT NOT NULL, verb TEXT NOT NULL,
            persona TEXT, ag INTEGER, al REAL, hr REAL, ds REAL, sl REAL,
            mx TEXT, rs TEXT, wr20 REAL,
            src TEXT, raw TEXT
        );"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_sym_tf_ts ON trades(sym, tf, ts_utc);")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS market(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc INTEGER NOT NULL,
            sym TEXT NOT NULL, tf TEXT NOT NULL,
            p REAL, v REAL, atr REAL, bbw REAL, al REAL, hr REAL,
            rg TEXT, mx TEXT, adx REAL, bb REAL, liq REAL, spr REAL,
            sess TEXT, fp TEXT,
            src TEXT, raw TEXT
        );"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_sym_tf_ts ON market(sym, tf, ts_utc);")
    conn.close()

init_db()

# ------------------ small in-memory dedupe (TV retries) ------------------
_recent = set()
def seen(msg: str) -> bool:
    h = hashlib.sha1(msg.encode("utf-8")).hexdigest()
    if h in _recent:
        return True
    _recent.add(h)
    if len(_recent) > 2000:
        for _ in range(500):
            try: _recent.pop()
            except KeyError: break
    return False

# ------------------ parse helpers ------------------
def kv_parse(msg: str) -> Tuple[str, Dict[str, str]]:
    parts = msg.strip().split("|")
    if not parts:
        return "", {}
    prefix = parts[0].strip().upper()  # e.g., TELEM / EL / PSY / MKT
    kv = {}
    for p in parts[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip()] = v.strip()
    return prefix, kv

def norm_tf(s: Optional[str]) -> str:
    if not s:
        return "na"
    return s.strip()  # TV usually sends "1","5","15","60","240", etc.

def write_csv(kind: str, row: Dict[str, Any]):
    day = dt.datetime.utcfromtimestamp(row["ts_utc"]).strftime("%Y%m%d")
    path = os.path.join(DATA_DIR, f"{kind}_{day}.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)

# ------------------ storage ------------------
def store_trade(raw: str, kv: Dict[str, str], src: str):
    ts = utc_ts()
    verb = kv.get("sig") or kv.get("verb") or raw.split("|", 1)[0]
    row = {
        "ts_utc": ts,
        "sym": kv.get("sym", "na"),
        "tf": norm_tf(kv.get("tf")),
        "verb": verb,
        "persona": kv.get("sq") or kv.get("p"),
        "ag": int(kv["ag"]) if kv.get("ag") not in (None, "",) else None,
        "al": float(kv["al"]) if kv.get("al") not in (None, "",) else None,
        "hr": float(kv["hr"]) if kv.get("hr") not in (None, "",) else None,
        "ds": float(kv["ds"]) if kv.get("ds") not in (None, "",) else None,
        "sl": float(kv["sl"]) if kv.get("sl") not in (None, "",) else None,
        "mx": kv.get("mx"),
        "rs": kv.get("rs") or kv.get("reg"),
        "wr20": float(kv["wr20"]) if kv.get("wr20") not in (None, "",) else None,
        "src": src,
        "raw": raw,
    }
    conn = db()
    conn.execute(
        """INSERT INTO trades(ts_utc,sym,tf,verb,persona,ag,al,hr,ds,sl,mx,rs,wr20,src,raw)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            row["ts_utc"], row["sym"], row["tf"], row["verb"], row["persona"],
            row["ag"], row["al"], row["hr"], row["ds"], row["sl"],
            row["mx"], row["rs"], row["wr20"], row["src"], row["raw"],
        ),
    )
    conn.close()
    write_csv("trades", row)

def store_market(raw: str, kv: Dict[str, str], src: str):
    ts = utc_ts()
    row = {
        "ts_utc": ts,
        "sym": kv.get("sym", "na"),
        "tf": norm_tf(kv.get("tf")),
        "p":  float(kv["p"])   if kv.get("p")   not in (None, "",) else None,
        "v":  float(kv["v"])   if kv.get("v")   not in (None, "",) else None,
        "atr":float(kv["atr"]) if kv.get("atr") not in (None, "",) else None,
        # accept either bbw= or bb= (PSY sends ratio in bb sometimes)
        "bbw":float(kv["bbw"]) if kv.get("bbw") not in (None, "",)
              else (float(kv["bb"]) if kv.get("bb") not in (None, "",) else None),
        "al": float(kv["al"])  if kv.get("al")  not in (None, "",) else None,
        "hr": float(kv["hr"])  if kv.get("hr")  not in (None, "",) else None,
        "rg": kv.get("rg") or kv.get("regime") or kv.get("reg"),
        "mx": kv.get("mx"),
        "adx":float(kv["adx"]) if kv.get("adx") not in (None, "",) else None,
        "bb": float(kv["bb"])  if kv.get("bb")  not in (None, "",) else None,
        "liq":float(kv["liq"]) if kv.get("liq") not in (None, "",) else None,
        "spr":float(kv["spr"]) if kv.get("spr") not in (None, "",) else None,
        "sess": kv.get("sess"),
        "fp":   kv.get("fp"),
        "src":  src,
        "raw":  raw,
    }
    conn = db()
    conn.execute(
        """INSERT INTO market(ts_utc,sym,tf,p,v,atr,bbw,al,hr,rg,mx,adx,bb,liq,spr,sess,fp,src,raw)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            row["ts_utc"], row["sym"], row["tf"], row["p"], row["v"], row["atr"], row["bbw"],
            row["al"], row["hr"], row["rg"], row["mx"], row["adx"], row["bb"], row["liq"],
            row["spr"], row["sess"], row["fp"], row["src"], row["raw"],
        ),
    )
    conn.close()
    write_csv("market", row)

def classify_and_store(raw: str) -> str:
    prefix, kv = kv_parse(raw)
    pf = prefix.upper()
    if pf in ("EL","ES","XL","XS","XA"):
        kv.setdefault("verb", pf)
        store_trade(raw, kv, src="F34")
        return "trade"
    if pf in ("TELEM","TRD","TRADE"):
        store_trade(raw, kv, src="F34")
        return "trade"
    if pf in ("PSY","MKT","MKT2"):
        store_market(raw, kv, src="HEIMDALL")
        return "market"
    # fallback: infer by keys
    if any(k in kv for k in ("sig","verb")):
        store_trade(raw, kv, src="F34")
        return "trade"
    if any(k in kv for k in ("adx","bbw","liq","fp","spr","sess")):
        store_market(raw, kv, src="HEIMDALL")
        return "market"
    raise ValueError("Unrecognized payload")

# ------------------ FastAPI ------------------
app = FastAPI(title=APP_NAME)

def check_auth(token_q: Optional[str], auth_h: Optional[str]):
    ok = False
    if URL_TOKEN and token_q == URL_TOKEN:
        ok = True
    if not ok and auth_h:
        parts = auth_h.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            bearer = parts[1]
            if bearer == HEADER_SECRET or bearer == ALT_HEADER_SECRET:
                ok = True
    if not ok:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/health")
def health():
    return {"app": APP_NAME, "status": "ok"}

@app.post("/ingest")
async def ingest(request: Request, token: Optional[str] = Query(None), authorization: Optional[str] = Header(None)):
    check_auth(token, authorization)

    body = await request.body()
    raw = body.decode("utf-8", errors="ignore").strip()

    if not raw:
        # TradingView can also send JSON with { "message": "..." }
        try:
            payload = await request.json()
            raw = (payload.get("message") or "").strip()
        except Exception:
            pass

    if not raw:
        raise HTTPException(status_code=400, detail="Empty payload")

    if seen(raw):
        return JSONResponse({"status": "dup", "len": len(raw)})

    try:
        kind = classify_and_store(raw)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Parse error: {e}")

    return JSONResponse({"status": "ok", "kind": kind, "len": len(raw)})

# ------------------ CSV exports for Excel ------------------
def export_csv(kind: str, date_str: Optional[str]):
    conn = db()
    if not date_str:
        date_str = dt.datetime.utcnow().strftime("%Y-%m-%d")
    d0 = dt.datetime.strptime(date_str, "%Y-%m-%d")
    t0 = int(d0.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    t1 = t0 + 86400
    if kind == "trades":
        cur = conn.execute("SELECT * FROM trades WHERE ts_utc>=? AND ts_utc<? ORDER BY ts_utc ASC", (t0, t1))
    else:
        cur = conn.execute("SELECT * FROM market WHERE ts_utc>=? AND ts_utc<? ORDER BY ts_utc ASC", (t0, t1))
    cols = [c[0] for c in cur.description]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    for row in cur:
        w.writerow(row)
    conn.close()
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename={kind}_{d0.strftime("%Y%m%d")}.csv'}
    )

@app.get("/export/trades")
def export_trades(date: Optional[str] = None):
    return export_csv("trades", date)

@app.get("/export/market")
def export_market(date: Optional[str] = None):
    return export_csv("market", date)

