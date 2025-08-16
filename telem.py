
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
import sqlite3, time, os

# ---------- Config via environment variables ----------
DB_PATH = os.environ.get("TELEM_DB", "telem.db")
URL_TOKEN = os.environ.get("TELEM_URL_TOKEN", "set-a-long-random-token")
SHARED_SECRET = os.environ.get("TELEM_SECRET", "CHANGE_ME")

app = FastAPI(title="F34 GPS — Telemetry Receiver", version="2.0")

# ---------- DB helpers ----------
def _db():
    conn = sqlite3.connect(DB_PATH)
    # keep rows returned as tuples, AUTOINCREMENT is fine for small write volume
    conn.execute("""CREATE TABLE IF NOT EXISTS telemetry(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER, sym TEXT, tf TEXT, sq TEXT,
        reg TEXT, m INTEGER, sc REAL, d REAL, al REAL, atrx REAL,
        sig TEXT, raw TEXT, inserted_at INTEGER
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts     ON telemetry(ts_ms)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sym_tf ON telemetry(sym, tf)")
    return conn

def parse_telem(raw: str) -> dict:
    """
    Parse compact pipe-delimited body.
    Example:
    TELEM|v=2|sym=KUCOIN:SOLUSDT|tf=1|t=1723824000000|sq=C|reg=X|m=1|sc=12.3|d=2.1|al=7.0|atrx=1.12|sig=HB|sec=SECRET
    """
    if not raw.startswith("TELEM|"):
        raise ValueError("bad prefix")
    out = {}
    for chunk in raw.strip().split("|")[1:]:
        if "=" in chunk:
            k, v = chunk.split("=", 1)
            out[k] = v
    return out

# ---------- Routes ----------
@app.get("/health")
def health():
    return {"ok": True, "service": "f34-gps", "time_ms": int(time.time()*1000)}

@app.post("/telem")
async def telem_ingest(request: Request, token: str):
    # URL token check
    if token != URL_TOKEN:
        raise HTTPException(401, "bad token")
    # Raw body (TradingView sends text/plain)
    raw = (await request.body()).decode("utf-8", errors="ignore").strip()
    if not raw:
        raise HTTPException(400, "empty body")
    try:
        d = parse_telem(raw)
    except Exception as e:
        raise HTTPException(400, f"parse error: {e}")
    # Shared-secret check (from Pine input)
    if d.get("sec") != SHARED_SECRET:
        raise HTTPException(401, "bad secret")

    # Minimal validation
    required = ["v","sym","tf","t","sq","reg","m","sc","d","al","atrx","sig"]
    missing = [k for k in required if k not in d]
    if missing:
        raise HTTPException(400, f"missing fields: {','.join(missing)}")

    conn = _db()
    try:
        conn.execute(
            """INSERT INTO telemetry(ts_ms,sym,tf,sq,reg,m,sc,d,al,atrx,sig,raw,inserted_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (int(d["t"]), d["sym"], d["tf"], d["sq"], d["reg"], int(d["m"]),
             float(d["sc"]), float(d["d"]), float(d["al"]), float(d["atrx"]),
             d["sig"], raw, int(time.time()*1000))
        )
        conn.commit()
    finally:
        conn.close()
    return JSONResponse({"ok": True})

@app.get("/metrics/heartbeat")
def last_heartbeat(sym: str, tf: str):
    conn = _db()
    try:
        row = conn.execute(
            """SELECT ts_ms, sq, reg, m, sc, d, al, atrx, sig
               FROM telemetry WHERE sym=? AND tf=? ORDER BY ts_ms DESC LIMIT 1""",
            (sym, tf)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(404, "no data")
    keys = ["ts_ms","sq","reg","m","sc","d","al","atrx","sig"]
    return dict(zip(keys, row))

@app.get("/metrics/rollup")
def rollup(sym: str, minutes: int = 60):
    since = int(time.time()*1000) - minutes*60*1000
    conn = _db()
    try:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM telemetry WHERE sym=? AND ts_ms>=?",
            (sym, since)
        ).fetchone()[0]
        sigs = conn.execute(
            "SELECT sig, COUNT(*) FROM telemetry WHERE sym=? AND ts_ms>=? GROUP BY sig",
            (sym, since)
        ).fetchall()
    finally:
        conn.close()
    return {"since_ms": since, "count": cnt, "by_sig": dict(sigs)}
