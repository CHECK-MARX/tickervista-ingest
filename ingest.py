import os, time, sys, requests
import psycopg2, psycopg2.extras
from datetime import timezone
from dateutil import parser as dtparse
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

# --- env ---
DB_URL = os.environ.get("DATABASE_URL", "")
API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY", "")
SYMBOLS = [s.strip() for s in os.environ.get("SYMBOLS","AAPL,MSFT,GOOGL,7203.T,6758.T").split(",") if s.strip()]

if not DB_URL:
    print("ERROR: DATABASE_URL が未設定です。Secrets または .env を確認してください。", file=sys.stderr); sys.exit(1)
if not API_KEY:
    print("ERROR: ALPHAVANTAGE_API_KEY が未設定です。Secrets または .env を確認してください。", file=sys.stderr); sys.exit(1)

# --- Pooler用の保険：options=project%3D<ref> が無ければ自動付与（あなたの project ref を既定値に） ---
DEFAULT_PROJECT_REF = "dynrvdexbngvrawdtjsc"  # あなたの環境に合わせて既定化（必要なら SUPABASE_PROJECT_REF で上書き可）
PROJECT_REF = os.environ.get("SUPABASE_PROJECT_REF", DEFAULT_PROJECT_REF)

def ensure_supabase_pooler_options(dsn: str) -> str:
    """Poolerの接続URLに options=project%3D<ref> / sslmode=require を確実に入れる"""
    parts = urlsplit(dsn)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    # sslmode
    if q.get("sslmode") != "require":
        q["sslmode"] = "require"
    # options
    need_project = ("pooler.supabase.com" in parts.netloc)
    has_project = "options" in q and "project=" in (q.get("options") or "")
    if need_project and not has_project:
        # 既存optionsがあれば追記、無ければ設定
        if q.get("options"):
            if "project=" not in q["options"]:
                q["options"] = q["options"].strip() + f" project={PROJECT_REF}"
        else:
            q["options"] = f"project={PROJECT_REF}"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q, doseq=True), parts.fragment))

DB_URL = ensure_supabase_pooler_options(DB_URL)

DDL = """
CREATE SCHEMA IF NOT EXISTS tickervista;

CREATE TABLE IF NOT EXISTS tickervista.symbols (
  id bigserial PRIMARY KEY,
  symbol text UNIQUE,
  name text NULL,
  exchange text NULL,
  sector text NULL,
  currency text NULL,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tickervista.ohlcv_1d (
  symbol text NOT NULL,
  ts timestamptz NOT NULL,
  open numeric NULL,
  high numeric NULL,
  low numeric NULL,
  close numeric NULL,
  adj_close numeric NULL,
  volume bigint NULL,
  source text NULL,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_1d_ts ON tickervista.ohlcv_1d(ts);
"""

UPSERT = """
INSERT INTO tickervista.ohlcv_1d
(symbol, ts, open, high, low, close, adj_close, volume, source)
VALUES (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(adj_close)s, %(volume)s, %(source)s)
ON CONFLICT (symbol, ts) DO UPDATE SET
  open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
  adj_close=EXCLUDED.adj_close, volume=EXCLUDED.volume, source=EXCLUDED.source, ingested_at=now();
"""

def run_ddl(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

def fetch_daily_adjusted(symbol: str):
    url = "https://www.alphavantage.co/query"
    params = {"function":"TIME_SERIES_DAILY_ADJUSTED","symbol":symbol,"outputsize":"compact","apikey":API_KEY}
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "Error Message" in data:
                raise RuntimeError(data["Error Message"])
            if "Note" in data:
                wait = 15 + attempt*10
                print(f"[{symbol}] Rate limit. wait {wait}s")
                time.sleep(wait); continue
            ts = data.get("Time Series (Daily)")
            if not ts: raise RuntimeError("No 'Time Series (Daily)' in response")
            rows = []
            for ds, v in ts.items():
                rows.append({
                    "symbol": symbol,
                    "ts": dtparse.isoparse(ds).replace(tzinfo=timezone.utc),
                    "open": float(v["1. open"]),
                    "high": float(v["2. high"]),
                    "low": float(v["3. low"]),
                    "close": float(v["4. close"]),
                    "adj_close": float(v.get("5. adjusted close", v["4. close"])),
                    "volume": int(v["6. volume"]),
                    "source": "alphavantage"
                })
            rows.sort(key=lambda r: r["ts"])
            return rows
        except Exception as e:
            if attempt == 2: raise
            time.sleep(5*(2**attempt))
    return []

def ensure_symbols(conn, symbols):
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur,
            "INSERT INTO tickervista.symbols(symbol) VALUES (%s) ON CONFLICT(symbol) DO NOTHING;",
            [(s,) for s in symbols], page_size=200)
    conn.commit()

def main():
    print("SYMBOLS:", ",".join(SYMBOLS))
    # 接続テスト（可視化）
    print("DB HOST:", urlsplit(DB_URL).netloc)
    print("HAS options=project%3D ?:", "options=project%3D" in DB_URL)
    with psycopg2.connect(DB_URL) as conn:
        conn.autocommit = True
        run_ddl(conn)
        ensure_symbols(conn, SYMBOLS)
        total = 0
        for i, sym in enumerate(SYMBOLS, 1):
            print(f"[{i}/{len(SYMBOLS)}] ingest {sym}")
            try:
                rows = fetch_daily_adjusted(sym)
                if not rows:
                    print(f"no data: {sym}"); continue
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=500)
                total += len(rows)
                print(f"done {sym}: upsert {len(rows)} rows")
                if i < len(SYMBOLS): time.sleep(13)  # rate limit
            except Exception as e:
                print(f"skip {sym}: {e}")
        print("TOTAL UPSERT:", total)

if __name__ == "__main__":
    main()
