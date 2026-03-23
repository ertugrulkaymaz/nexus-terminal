"""
NEXUS Terminal Backend — Alpaca Market Data
Real-time US equity data for all S&P 500 stocks
"""
import asyncio, json, logging, time, os, re
import httpx, xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nexus")

# ── CONFIG ─────────────────────────────────
ALPACA_KEY    = os.environ.get("ALPACA_KEY",    "PKA7RRNT63NBN62X4FDDW4J6MD")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET", "2q8ATV4vAS3ifA8gBYqMCCCMEd4Xy6FmcRZuMftL92bte")
ALPACA_DATA   = "https://data.alpaca.markets/v2"
ALPACA_HEADS  = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}
PORT          = int(os.environ.get("PORT", 8000))

EDGAR_FEED  = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&dateb=&owner=include&count=20&search_text=&output=atom"
COINGECKO   = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,binancecoin&vs_currencies=usd&include_24hr_change=true&include_market_cap=true"

SYMBOLS = [
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","JPM","V","MA",
    "UNH","XOM","JNJ","PG","HD","COST","ABBV","MRK","CVX","BAC",
    "KO","PEP","WMT","DIS","NFLX","ADBE","CRM","AMD","GS","BLK",
    "SBUX","GE","HON","CAT","BA","LMT","NEE","GD","UNP","FDX",
    "AMGN","GILD","REGN","VRTX","MDT","SYK","ISRG","BSX","HCA","CVS",
    "COP","SLB","OXY","DVN","NEM","FCX","SHW","NUE","WFC","PNC",
    "USB","TFC","COF","AXP","SPGI","MCO","ICE","CME","BK","STT",
    "PRU","MET","AFL","ALL","PGR","TRV","CB","MMC","AON","V",
    "PYPL","FIS","FISV","GPN","INTC","QCOM","TXN","AMAT","LRCX","KLAC",
    "NOW","PANW","CRWD","FTNT","ADSK","ROP","SNPS","CDNS","MRVL","ANSS",
]

# ── STATE ──────────────────────────────────
quotes:      Dict[str, dict] = {}
sec_filings: List[dict]      = []
crypto_data: dict            = {}
clients:     Set[WebSocket]  = set()
seen_sec:    Set[str]        = set()

app = FastAPI(title="NEXUS Terminal API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def broadcast(msg: dict):
    dead = set()
    for ws in clients.copy():
        try:
            await ws.send_json(msg)
        except:
            dead.add(ws)
    clients.difference_update(dead)

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    log.info(f"Client connected. Total: {len(clients)}")
    await ws.send_json({"type":"snapshot","quotes":quotes,"sec":sec_filings[:20],"crypto":crypto_data})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)

@app.get("/")
async def root():
    return {"status":"ok","quotes":len(quotes),"sec":len(sec_filings),"clients":len(clients)}

@app.get("/health")
async def health():
    return {"status":"ok","quotes":len(quotes),"sec":len(sec_filings),"ts":int(time.time())}

@app.get("/quotes")
async def get_quotes():
    return {"quotes":quotes,"ts":int(time.time())}

@app.get("/sec")
async def get_sec():
    return {"filings":sec_filings[:30]}

@app.get("/crypto")
async def get_crypto():
    return crypto_data

# ── ALPACA QUOTE FETCHER ───────────────────
async def fetch_alpaca_quotes():
    """Fetch quotes from Alpaca — supports bulk requests, no rate limit issues"""
    async with httpx.AsyncClient() as client:
        while True:
            try:
                # Alpaca supports bulk snapshot — all symbols in one request
                syms_str = ",".join(SYMBOLS)
                r = await client.get(
                    f"{ALPACA_DATA}/stocks/snapshots",
                    headers=ALPACA_HEADS,
                    params={"symbols": syms_str, "feed": "iex"},
                    timeout=15
                )
                if r.status_code == 200:
                    data = r.json()
                    updated = 0
                    for sym, snap in data.items():
                        try:
                            dp = snap.get("dailyBar", {}) or snap.get("latestTrade", {}) or {}
                            lp = snap.get("latestTrade", {}) or {}
                            prev = snap.get("prevDailyBar", {}) or {}
                            
                            price = lp.get("p") or dp.get("c") or 0
                            prev_close = prev.get("c") or price
                            
                            if price and price > 0:
                                old = quotes.get(sym, {}).get("c", 0)
                                quotes[sym] = {
                                    "c":  price,
                                    "pc": prev_close,
                                    "o":  dp.get("o", price),
                                    "h":  dp.get("h", price),
                                    "l":  dp.get("l", price),
                                    "v":  dp.get("v", 0),
                                    "ts": int(time.time()),
                                }
                                updated += 1
                                if old and abs(price - old) / old > 0.0001:
                                    await broadcast({"type":"price","symbol":sym,"price":price,"data":quotes[sym]})
                        except Exception as e:
                            pass
                    log.info(f"Alpaca: {updated} symbols updated")
                else:
                    log.warning(f"Alpaca snapshot error: {r.status_code} {r.text[:200]}")
            except Exception as e:
                log.warning(f"Alpaca fetch error: {e}")
            
            await asyncio.sleep(15)  # refresh every 15 seconds

# ── SEC EDGAR SCRAPER ──────────────────────
async def edgar_loop():
    forms = ["8-K","SC+13G","10-Q","4"]
    async with httpx.AsyncClient(headers={"User-Agent":"NEXUS research@nexus.io"}) as client:
        while True:
            for form in forms:
                try:
                    r = await client.get(EDGAR_FEED.format(form=form), timeout=10)
                    if r.status_code != 200:
                        continue
                    root = ET.fromstring(r.text)
                    ns = {"a":"http://www.w3.org/2005/Atom"}
                    for entry in root.findall("a:entry", ns)[:5]:
                        eid   = entry.findtext("a:id", namespaces=ns) or ""
                        if eid in seen_sec:
                            continue
                        seen_sec.add(eid)
                        title   = entry.findtext("a:title", namespaces=ns) or ""
                        link_el = entry.find("a:link", ns)
                        link    = link_el.get("href","") if link_el is not None else ""
                        m       = re.search(r'\(([A-Z]{1,5})\)', title)
                        ticker  = m.group(1) if m else ""
                        company = re.sub(r'\s*\([^)]*\)', '', title).strip()[:60]
                        pos_w   = ["acqui","merger","buyback","dividend","beat","raised","upgrade","profit"]
                        neg_w   = ["loss","downgrade","investigation","fine","penalty","restate","risk","warning"]
                        score   = 0.1
                        tl      = title.lower()
                        for w in pos_w:
                            if w in tl: score += 0.2
                        for w in neg_w:
                            if w in tl: score -= 0.25
                        score = round(max(-1, min(1, score)), 2)
                        filing = {
                            "ticker":ticker, "company":company,
                            "form":form.replace("+", " "),
                            "title":title[:100], "link":link,
                            "score":score,
                            "sent":"pos" if score>0.1 else "neg" if score<-0.1 else "neu",
                            "ts":int(time.time()),
                            "time":datetime.now().strftime("%H:%M:%S"),
                        }
                        sec_filings.insert(0, filing)
                        if len(sec_filings) > 100:
                            sec_filings.pop()
                        log.info(f"SEC {form}: {ticker} {company[:30]}")
                        await broadcast({"type":"sec","filing":filing})
                except Exception as e:
                    log.warning(f"EDGAR error: {e}")
                await asyncio.sleep(5)
            await asyncio.sleep(120)

# ── CRYPTO ────────────────────────────────
async def crypto_loop():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                r = await client.get(COINGECKO, timeout=10)
                data = r.json()
                for cid, key in [("bitcoin","BTC"),("ethereum","ETH"),("solana","SOL"),("binancecoin","BNB")]:
                    if cid in data:
                        d = data[cid]
                        crypto_data[key] = {
                            "price": d.get("usd",0),
                            "chg24": round(d.get("usd_24h_change",0),2),
                            "ts":    int(time.time())
                        }
                log.info(f"Crypto: BTC=${crypto_data.get('BTC',{}).get('price','?')}")
                await broadcast({"type":"crypto","data":crypto_data})
            except Exception as e:
                log.warning(f"Crypto error: {e}")
            await asyncio.sleep(60)

# ── STARTUP ───────────────────────────────
@app.on_event("startup")
async def startup():
    asyncio.create_task(fetch_alpaca_quotes())
    asyncio.create_task(edgar_loop())
    asyncio.create_task(crypto_loop())
    log.info("NEXUS Backend ready — Alpaca + SEC + Crypto")

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
