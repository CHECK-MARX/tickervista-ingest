import os, time, requests
import psycopg2
from dateutil import parser as dtp

DB_URL = os.environ["DATABASE_URL"]
API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
SYMBOLS = [s.strip() for s in os.environ.get("SYMBOLS","AAPL").split(",") if s.strip()]

def fetch_daily(symbol: str):
    url = "https://www.alphavantage.co/query"
    params = {
        "function":"TIME_SERIES_DAILY_ADJUSTED",
        "symbol":symbol,
        "outputsize":"compact",
        "apikey":API_KEY
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "Time Series (Daily)" not in data:
        raise RuntimeError(f"API error for {symbol}: {data.get('Note') or data.get('Error Message') or data}")
    ts = data["Time Series (Daily)"]
    rows = []
    for ds, v in ts.items():
        rows.append((
            symbol,
            dtp.parse(ds).isoformat(),
            float(v["1. open"]), float(v["2. high"]), float(v["3. low"]), float(v["4. close"]),
            float(v["5. adjusted close"]), int(v["6. volume"]), "alphavantage"
        ))
    return rows

def main():
    with psycopg2.connect(DB_URL) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for sym in SYMBOLS:
                print(f"ingest {sym} ...")
                try:
                    rows = fetch_daily(sym)
                except Exception as e:
                    print(f"skip {sym}: {e}")
                    time.sleep(15)
                    continue

                cur.execute("""
                    INSERT INTO tickervista.symbols(symbol, is_active, created_at, updated_at)
                    VALUES (%s, true, NOW(), NOW())
                    ON CONFLICT (symbol) DO UPDATE SET updated_at=EXCLUDED.updated_at, is_active=true;
                """, (sym,))

                cur.executemany("""
                    INSERT INTO tickervista.ohlcv_1d
                      (symbol, ts, open, high, low, close, adj_close, volume, source_text, ingested_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (symbol, ts) DO NOTHING;
                """, rows)

                print(f"done {sym}, rows={len(rows)}")
                time.sleep(15)

if __name__ == "__main__":
    main()
