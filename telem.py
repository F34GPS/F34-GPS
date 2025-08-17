from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import sqlite3, time, os

DB_PATH   = os.environ.get("TELEM_DB", "telem.db")
URL_TOKEN = os.environ.get("TELEM_URL_TOKEN", "set-a-long-random-token")
BODY_SEC  = os.environ.get("TELEM_SECRET",       "CHANGE_ME")

app = FastAPI()

def db():
    conn = sqlite3.connect(DB_PATH)
    # Trades (from strategy order comments: TELEM_TRADE)
    conn.execute("""CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER NOT NULL,
        sym TEXT NOT NULL, tf TEXT NOT NULL, verb TEXT NOT NULL,  -- EL/ES/XL/XS/XA
        persona TEXT, ag INTEGER, al REAL, hr REAL, ds REAL, sl REAL,
        mx TEXT, rs TEXT, wr20 REAL, raw TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_sym_tf_ts ON trades(sym, tf, ts_ms)")
    # Market snapshots (from Heimdall / PSY)
    conn.execute("""CREATE TABLE IF NOT EXISTS market(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER NOT NULL,
        sym TEXT NOT NULL, tf TEXT NOT NULL,
        al REAL, hr REAL, adx REAL, bb REAL, liq REAL, spr REAL,
        mx TEXT, rg TEXT, sess TEXT, fp TEXT, raw TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_sym_tf_ts ON market(sym, tf, ts_ms)")
    return conn

def parse_pipe(raw: str, expect_prefix: str):
    raw = raw.strip()
    if not raw.startswith(expect_prefix + "|"):
        raise ValueError(f"bad prefix: expected {expect_prefix}")
    out = {}
    # First chunk is the prefix, the rest k=v or tokens
    for chunk in raw.split("|")[1:]:
        if "=" in chunk:
            k, v = chunk.split("=", 1)
            out[k] = v
    return out

@app.get("/health")
def health():
    return {"ok": True, "db": DB_PATH}

# -------- TELEM (orders) --------
@app.post("/telem")
async def telem_ingest(request: Request, token: str):
    if token != URL_TOKEN:
        raise HTTPException(401, "bad token")
    raw = (await request.body()).decode("utf-8", errors="ignore").strip()
    d = parse_pipe(raw, "TELEM")  # must start with TELEM|
    # optional shared-secret field
    if BODY_SEC and d.get("sec") != BODY_SEC:
        raise HTTPException(401, "bad secret")
    # Required fields (see your TELEM pack)
    # Minimal fallback: accept even if some optional fields missing
    ts_ms = int(d.get("t", int(time.time()*1000)))
    sym   = d.get("sym","?")
    tf    = d.get("tf","?")
    sig   = d.get("sig","?")       # EL/ES/XL/XS/XA
    persona = d.get("sq") or d.get("p")
    ag    = int(float(d.get("ag", "0")))
    al    = float(d.get("al","0"))
    hr    = float(d.get("atrx","0"))
    ds    = float(d.get("d","0"))
    sl    = float(d.get("sl","0"))
    mx    = "UP" if d.get("m","0") in ("1","UP") else "DN"
    rs    = d.get("reg","")
    wr20  = float(d.get("wr20","-1")) if d.get("wr20") else None

    conn = db()
    conn.execute("""INSERT INTO trades(ts_ms,sym,tf,verb,persona,ag,al,hr,ds,sl,mx,rs,wr20,raw)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ts_ms,sym,tf,sig,persona,ag,al,hr,ds,sl,mx,rs,wr20,raw))
    conn.commit(); conn.close()
    return JSONResponse({"ok": True})

# -------- PSY / Heimdall (market) --------
@app.post("/psy")
async def psy_ingest(request: Request, token: str):
    if token != URL_TOKEN:
        raise HTTPException(401, "bad token")
    raw = (await request.body()).decode("utf-8", errors="ignore").strip()
    d = parse_pipe(raw, "PSY")  # must start with PSY|
    # optional shared-secret field if you add it in Pine
    if "sec" in d and BODY_SEC and d["sec"] != BODY_SEC:
        raise HTTPException(401, "bad secret")

    ts_ms = int(time.time()*1000)  # Heimdall doesn’t send time; we stamp server time
    sym   = d.get("sym","?")
    tf    = d.get("tf","?")
    al    = float(d.get("al","0"))
    hr    = float(d.get("hr","0"))
    adx   = float(d.get("adx","0"))
    bb    = float(d.get("bb","0"))
    liq   = float(d.get("liq","0"))
    spr   = float(d.get("spr","0"))
    mx    = d.get("mx","na")
    rg    = d.get("rg","na")
    sess  = d.get("sess","ALL")
    fp    = d.get("fp","")

    conn = db()
    conn.execute("""INSERT INTO market(ts_ms,sym,tf,al,hr,adx,bb,liq,spr,mx,rg,sess,fp,raw)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ts_ms,sym,tf,al,hr,adx,bb,liq,spr,mx,rg,sess,fp,raw))
    conn.commit(); conn.close()
    return JSONResponse({"ok": True})

# --------- tiny metrics for quick checks ----------
@app.get("/metrics/psy_last")
def psy_last(sym: str, tf: str):
    conn = db()
    row = conn.execute("""SELECT ts_ms, al, hr, adx, bb, liq, spr, mx, rg, sess, fp
                          FROM market
                          WHERE sym=? AND tf=?
                          ORDER BY ts_ms DESC LIMIT 1""", (sym, tf)).fetchone()
    conn.close()
    if not row: raise HTTPException(404, "no data")
    keys = ["ts_ms","al","hr","adx","bb","liq","spr","mx","rg","sess","fp"]
    return dict(zip(keys,row))

@app.get("/metrics/rollup")
def rollup(sym: str = "", minutes: int = 60):
    since = int(time.time()*1000) - minutes*60*1000
    conn = db()
    if sym:
        cnt = conn.execute("""SELECT COUNT(*) FROM market WHERE sym=? AND ts_ms>=?""",(sym,since)).fetchone()[0]
    else:
        cnt = conn.execute("""SELECT COUNT(*) FROM market WHERE ts_ms>=?""",(since,)).fetchone()[0]
    conn.close()
    return {"since_ms": since, "count": cnt, "sym": sym or "*"}

