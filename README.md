
# F34-GPS (Telemetry Receiver)

This is the tiny FastAPI webhook that receives F‑34 telemetry from TradingView and stores it in a local SQLite DB.

## Files
- `telem.py` — FastAPI app (endpoints: `/telem`, `/health`, `/metrics/heartbeat`, `/metrics/rollup`)
- `requirements.txt` — Python deps
- `.gitignore` — ignores Python caches + SQLite db

## Run locally
```bash
python -m venv .venv && . .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Set secrets
set TELEM_URL_TOKEN=your-long-url-token         # PowerShell: $env:TELEM_URL_TOKEN="..."
set TELEM_SECRET=your-shared-secret
# optional
set TELEM_DB=telem.db

uvicorn telem:app --host 0.0.0.0 --port 8787
```

Test:
```bash
curl -X POST "http://localhost:8787/telem?token=your-long-url-token" \
  -d "TELEM|v=2|sym=KUCOIN:SOLUSDT|tf=1|t=1723824000000|sq=C|reg=X|m=1|sc=12.3|d=2.1|al=7|atrx=1.12|sig=HB|sec=your-shared-secret"
curl "http://localhost:8787/health"
```

## Deploy on Render — Web Service
1) Create a new **Web Service** in Render and connect this GitHub repo.
2) Environment: **Python 3.11** (default is fine).
3) **Build Command**: `pip install -r requirements.txt`
4) **Start Command**: `uvicorn telem:app --host 0.0.0.0 --port $PORT`
5) Add these **Environment Variables** in Render → *Environment*:
   - `TELEM_URL_TOKEN` = a long random token (used in the webhook URL query `?token=...`)
   - `TELEM_SECRET`    = shared secret (must match the Pine input `telemSecret`)
   - (optional) `TELEM_DB` = `telem.db`

When Render shows the service **Live**, your webhook URL looks like:
```
https://<your-service>.onrender.com/telem?token=<TELEM_URL_TOKEN>
```

## TradingView alerts (two separate alert rules)
**1) Orders (WT / “Squad”)**
- Condition: **Your F34 strategy**
- Event: **Order fills**
- Message: `{{strategy.order.comment}}`
- Name: `F34_SQD_<SYM>_<TF>`
- Webhook: **your WunderTrading hook** (unchanged)

**2) Telemetry (“GPS”)**
- Condition: **Your F34 strategy**
- Event: **Any alert() function call**
- Webhook URL: `https://<your-service>.onrender.com/telem?token=<TELEM_URL_TOKEN>`
- Message: *(leave empty; Pine builds it)*
- Name: `F34_GPS_<SYM>_<TF>`

Remember to set the Pine input `telemSecret` to the same value as `TELEM_SECRET`.
