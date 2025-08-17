# telem.py — dual-channel receiver (F-34 trades + Heimdall market)
# FastAPI + CSV append; optional XLSX export if pandas/openpyxl present.

import os, io, csv, time
from datetime import datetime, timezone
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import PlainTextResponse, StreamingResponse

# --- config via Render environment variables ---
URL_TOKEN   = os.environ.get("TELEM_URL_TOKEN",   "CHANGE_ME_URL_TOKEN")
SHARED_SEC  = os.environ.get("TELEM_SHARED_SECRET","CHANGE_ME_SHARED_SECRET")
DATA_DIR    = os.environ.get("DATA_DIR", "/data")  # mount a Persistent Disk here on Render
os.makedirs(DATA_DIR, exist_ok=True)

# --- optional pandas for XLSX export ---
try:
    import pandas as pd
    HAVE_PD = True
except Exception:
    HAVE_PD = False

app = FastAPI(title="F34GPS", version="1.1")

def utc_ms() -> int:
    return int(time.time() * 1000)

def today_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")

def csv_path(table: str) -> str:
    return os.path.join(DATA_DIR, f"{table}_{today_tag()}.csv")

def append_row(path: str, headers: list[str], row: list):
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(headers)
        w.writerow(row)

def parse_pipe(body: str) -> tuple[str, dict]:
    """
    Returns (kind, fields)
    kind: 'trade' for EL|ES|XL|XS|XA|..., 'market' for PSY|...
    """
    body = body.strip()
    if not body:
        raise ValueError("empty body")

    if body.startswith(("EL|","ES|","XL|","XS|","XA|","TELEM|")):
        kind = "trade"
        # first token is the verb (EL/ES/XL/XS/XA)
        parts = body.split("|")
        fields = {"verb": parts[0]}
        for p in parts[1:]:
            if "=" in p:
                k, v = p.split("=", 1)
                fields[k] = v
        return kind, fields

    if body.startswith(("PSY|","MKT|")):   # Heimdall / market scout
        kind = "market"
        parts = body.split("|")[1:]  # drop "PSY" token
        fields = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                fields[k] = v
        return kind, fields

    raise ValueError("unknown format")

def sec_ok(fields: dict) -> bool:
    # If a 'sec' is present in the payload, enforce it; otherwise pass (URL token still required).
    if "sec" in fields:
        return fields["sec"] == SHARED_SEC
    return True

@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": utc_ms(), "data_dir": DATA_DIR}

@app.post("/ingest")
async def ingest(request: Request, token: str):
    # 1) URL token guard
    if token != URL_TOKEN:
        raise HTTPException(status_code=401, detail="bad token")

    # 2) Read and parse body
    raw = (await request.body()).decode("utf-8", errors="ignore")
    kind, fields = parse_pipe(raw)

    # 3) Optional shared secret (if present)
    if not sec_ok(fields):
        raise HTTPException(status_code=401, detail="bad secret")

    # 4) Normalize minimal fields
    ts = utc_ms()
    sym = fields.get("sym", fields.get("symbol", "NA"))
    tf  = fields.get("tf", "NA")

    if kind == "trade":
        # trades table columns
        hdr = ["ts_utc","sym","tf","verb","persona","ag","al","hr","ds","sl","mx","rs","wr20","raw"]
        row = [
            ts, sym, tf,
            fields.get("verb","NA"),
            fields.get("p",""),
            fields.get("ag",""),
            fields.get("al",""),
            fields.get("hr",""),
            fields.get("ds",""),
            fields.get("sl",""),
            fields.get("mx",""),
            fields.get("rs",""),
            fields.get("wr20",""),
            raw
        ]
        append_row(csv_path("trades"), hdr, row)
        return {"ok": True, "kind": "trade", "sym": sym, "tf": tf}

    else:
        # market table columns (Heimdall / PSY)
        hdr = ["ts_utc","sym","tf","al","hr","adx","bb","liq","spr","mx","rg","sess","fp","raw"]
        row = [
            ts, sym, tf,
            fields.get("al",""),
            fields.get("hr",""),
            fields.get("adx",""),
            fields.get("bb",""),
            fields.get("liq",""),
            fields.get("spr",""),
            fields.get("mx",""),
            fields.get("rg",""),
            fields.get("sess",""),
            fields.get("fp",""),
            raw
        ]
        append_row(csv_path("market"), hdr, row)
        return {"ok": True, "kind": "market", "sym": sym, "tf": tf}

# --- quick CSV download endpoints ---
@app.get("/export/{table}/today.csv")
def export_today(table: str):
    if table not in ("trades","market"):
        raise HTTPException(404, "bad table")
    path = csv_path(table)
    if not os.path.exists(path):
        raise HTTPException(404, "no data for today")
    with open(path, "rb") as f:
        data = f.read()
    return Response(content=data, media_type="text/csv")

# --- Excel workbook with both sheets (if pandas available) ---
@app.get("/export/xlsx")
def export_xlsx():
    if not HAVE_PD:
        raise HTTPException(501, "pandas/openpyxl not installed")
    out = io.BytesIO()
    writer = pd.ExcelWriter(out, engine="openpyxl")
    for table in ("trades","market"):
        path = csv_path(table)
        if os.path.exists(path):
            df = pd.read_csv(path)
            df.to_excel(writer, sheet_name=table, index=False)
    writer.close()
    out.seek(0)
    return StreamingResponse(out, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition":"attachment; filename=heimdall_today.xlsx"})

