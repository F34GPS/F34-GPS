# Heimdall-GPS — unified telemetry receiver for F-34 (trades) + Heimdall/PSY (market)
# ASGI app (FastAPI). One endpoint: POST /ingest
# Auth: ?token=<TELEM_URL_TOKEN> or Authorization: Bearer <TELEM_SECRET>
# Storage: /data/telem.db (SQLite) + daily CSV append in /data

import os, io, csv, json, hashlib, sqlite3, datetime as dt
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request, HTTPException, Header, Query
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

APP_NAME = os.getenv("APP_NAME", "Heimdall-GPS")
DATA_DIR = os.getenv("EXPORT_DIR", "/data")
URL_TOKEN = os.getenv("TELEM_URL_TOKEN", "")
HEADER_SECRET = os.getenv("TELEM_SECRET", "")

os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "telem.db")

def utc_ts() -> int:
    return int(dt.datetime.utcnow().timestamp())

def db():
    conn = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    conn = db()
    conn.execute("""CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_utc INTEGER NOT NULL,
        sym TEXT NOT NULL, tf TEXT NOT NULL, verb TEXT NOT NULL,
        persona TEXT, ag INTEGER, al REAL, hr REAL, ds REAL, sl REAL,
        mx TEXT, rs TEXT, wr20 REAL,
        src TEXT, raw TEXT
    );""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_sym_tf_ts ON trades(sym, tf, ts_utc);")
    conn.execute("""CREATE TABLE IF NOT EXISTS market(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_utc INTEGER NOT NULL,
        sym TEXT NOT NULL, tf TEXT NOT NULL,
        p REAL, v REAL, atr REAL, bbw REAL, al REAL, hr REAL,
        rg TEXT, mx TEXT, adx REAL, bb REAL, liq REAL, spr REAL,
        sess TEXT, fp TEXT,
        src TEXT, raw TEXT
    );""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_sym_tf_ts ON market(sym, tf, ts_utc);")
    conn.close()

init_db()

# --- small in-memory dedupe (protect from duplicate TV retries) ---
_recent = set()
def seen(msg: str) -> bool:
    h = hashlib.sha1(msg.encode("utf-8")).hexdigest()
    if h in _recent: return True
    _recent.add(h)
    # trim
    if len(_recent) > 2000:
        for _ in range(500): _recent.pop()
    return False

def kv_parse(msg: str) -> (str, Dict[str, str]):
    """Parse pipe '|' separated k=v string. Return prefix & dict."""
    parts = msg.strip().split("|")
    if not parts: return "", {}
    prefix = parts[0]  # e.g., TELEM / EL / PSY / MKT
    kv = {}
    for p in parts[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip()] = v.strip()
    return prefix, kv

def norm_tf(s: Optional[str]) -> str:
    if not s: return "na"
    s = s.strip()
    # TV uses "1", "5", "15", "60", "240" etc.
    return s

def write_csv(kind: str, row: Dict[str, Any]):
    day = dt.datetime.utcfromtimestamp(row["ts_utc"]).strftime("%Y%m%d")
    path = os.path.join(DATA_DIR, f"{kind}_{day}.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new: w.writeheader()
        w.writerow(row)

def store_trade(raw: str, kv: Dict[str,str], src: str):
    ts = utc_ts()
    verb = kv.get("sig") or kv.get("verb") or raw.split("|",1)[0]
    row = {
        "ts_utc": ts,
        "sym": kv.get("sym", "na"),
        "tf": norm_tf(kv.get("tf")),
        "verb": verb,
        "persona": kv.get("sq") or kv.get("p"),
        "ag": int(kv.get("ag","0")) if kv.get("ag") else None,
        "al": float(kv.get("al","0")) if kv.get("al") else None,
        "hr": float(kv.get("hr","0")) if kv.get("hr") else None,
        "ds": float(kv.get("ds","0")) if kv.get("ds") else None,
        "sl": float(kv.get("sl","0")) if kv.get("sl") else None,
        "mx": kv.get("mx"),
        "rs": kv.get("rs") or kv.get("reg"),
        "wr20": float(kv.get("wr20","0")) if kv.get("wr20") else None,
        "src": src,
        "raw": raw
    }
    conn = db()
    conn.execute("""INSERT INTO trades(ts_utc,sym,tf,verb,persona,ag,al,hr,ds,sl,mx,rs,wr20,src,raw)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                 (row["ts_utc"], row["sym"], row["tf"], row["verb"], row["persona"],
                  row["ag"], row["al"], row["hr"], row["ds"], row["sl"],
                  row["mx"], row["rs"], row["wr20"], row["src"], row["raw"]))
    conn.close()
    write_csv("trades", row)

def store_market(raw: str, kv: Dict[str,str], src: str):
    ts = utc_ts()
    row = {
        "ts_utc": ts,
        "sym": kv.get("sym","na"),
        "tf": norm_tf(kv.get("tf")),
        "p":  float(kv.get("p","0")) if kv.get("p") else None,
        "v":  float(kv.get("v","0")) if kv.get("v") else None,
        "atr":float(kv.get("atr","0")) if kv.get("atr") else None,
        "bbw":float(kv.get("bbw","0")) if kv.get("bbw") else (float(kv.get("bb","0")) if kv.get("bb") else None),
        "al": float(kv.get("al","0")) if kv.get("al") else None,
        "hr": float(kv.get("hr","0")) if kv.get("hr") else None,
        "rg": kv.get("rg") or kv.get("regime") or kv.get("reg"),
        "mx": kv.get("mx"),
        "adx":float(kv.get("adx","0")) if kv.get("adx") else None,
        "bb": float(kv.get("bb","0")) if kv.get("bb") else None,
        "liq":float(kv.get("liq","0")) if kv.get("liq") else None,
        "spr":float(kv.get("spr","0")) if kv.get("spr") else None,
        "sess": kv.get("sess"),
        "fp":   kv.get("fp"),
        "src":  src,
        "raw":  raw
    }
    conn = db()
    conn.execute("""INSERT INTO market(ts_utc,sym,tf,p,v,atr,bbw,al,hr,rg,mx,adx,bb,liq,spr,sess,fp,src,raw)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                 (row["ts_utc"], row["sym"], row["tf"], row["p"], row["v"], row["atr"], row["bbw"],
                  row["al"], row["hr"], row["rg"], row["mx"], row["adx"], row["bb"], row["liq"],
                  row["spr"], row["sess"], row["fp"], row["src"], row["raw"]))
    conn.close()
    write_csv("market", row)

def classify_and_store(raw: str):
    prefix, kv = kv_parse(raw)
    pf = prefix.upper()
    if pf in ("EL","ES","XL","XS","XA"):
        # broker-style pack
        kv.setdefault("verb", pf)
        store_trade(raw, kv, src="F34")
        return "trade"
    if pf in ("TELEM","TRD","TRADE"):
        # F-34 'TELEM|...|sig=EL'
        store_trade(raw, kv, src="F34")
        return "trade"
    if pf in ("PSY","MKT","MKT2"):
        store_market(raw, kv, src="HEIMDALL")
        return "market"
    # fallback: try to guess by keys
    if "sig" in kv or "verb" in kv:
        store_trade(raw, kv, src="F34")
        return "trade"
    if "adx" in kv or "bbw" in kv or "liq" in kv or "fp" in kv:
        store_market(raw, kv, src="HEIMDALL")
        return "market"
    # unknown
    raise ValueError("Unrecognized payload")

app = FastAPI(title=APP_NAME)

def check_auth(token_q: Optional[str], auth_h: Optional[str]):
    ok = False
    if URL_TOKEN and token_q == URL_TOKEN:
        ok = True
    if not ok and HEADER_SECRET and auth_h:
        parts = auth_h.split()
        if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1] == HEADER_SECRET:
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
    raw = body.decode("utf-8").strip()
    if not raw:
        try:
            payload = await request.json()
            raw = payload.get("message", "")
        except Exception:
            pass
    if not raw:
        raise HTTPException(status_code=400, detail="Empty payload")

    if seen(raw):
        return JSONResponse({"status":"dup", "len": len(raw)})

    try:
        kind = classify_and_store(raw)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Parse error: {e}")

    return JSONResponse({"status":"ok", "kind": kind, "len": len(raw)})

def export_csv(kind: str, date_str: Optional[str]):
    conn = db()
    if not date_str:
        date_str = dt.datetime.utcnow().strftime("%Y-%m-%d")
    d0 = dt.datetime.strptime(date_str, "%Y-%m-%d")
    t0 = int(d0.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    t1 = t0 + 86400
    if kind == "trades":
        cur = conn.execute("SELECT * FROM trades WHERE ts_utc>=? AND ts_utc<? ORDER BY ts_utc ASC", (t0,t1))
        cols = [c[0] for c in cur.description]
    else:
        cur = conn.execute("SELECT * FROM market WHERE ts_utc>=? AND ts_utc<? ORDER BY ts_utc ASC", (t0,t1))
        cols = [c[0] for c in cur.description]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    for row in cur:
        w.writerow(row)
    conn.close()
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]),
                             media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={kind}_{d0.strftime('%Y%m%d')}.csv"})

@app.get("/export/trades")
def export_trades(date: Optional[str] = None):
    return export_csv("trades", date)

@app.get("/export/market")
def export_market(date: Optional[str] = None):
    return export_csv("market", date)


