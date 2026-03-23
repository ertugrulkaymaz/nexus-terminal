"""
NEXUS Terminal Backend — Yahoo Finance
Free, no API key, full US market coverage
"""
import asyncio, logging, time, os, re, json
import httpx, xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nexus")

PORT       = int(os.environ.get("PORT", 8000))
YF_BASE    = "https://query1.finance.yahoo.com"
YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}
EDGAR_FEED = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&dateb=&owner=include&count=20&search_text=&output=atom"
COINGECKO  = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,binancecoin&vs_currencies=usd&include_24hr_change=true&include_market_cap=true"

SYMBOLS = [
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","JPM","V","MA",
    "UNH","XOM","JNJ","PG","HD","COST","ABBV","MRK","CVX","BAC",
    "KO","PEP","WMT","DIS","NFLX","ADBE","CRM","AMD","GS","BLK",
    "SBUX","GE","HON","CAT","BA","LMT","NEE","GD","UNP","FDX",
    "AMGN","GILD","REGN","VRTX","MDT","SYK","ISRG","BSX","HCA","CVS",
    "COP","SLB","OXY","DVN","NEM","FCX","SHW","NUE","WFC","PNC",
    "USB","TFC","COF","AXP","SPGI","MCO","ICE","CME","BK","STT",
    "PRU","MET","AFL","ALL","PGR","TRV","CB","MMC","AON","PYPL",
    "FIS","FISV","GPN","INTC","QCOM","TXN","AMAT","LRCX","KLAC","NOW",
    "PANW","CRWD","FTNT","ADSK","ROP","SNPS","CDNS","MRVL","ANSS","INTU",
]

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
    await ws.send_json({"type":"snapshot","quotes":quotes,"sec":sec_filings[:20],"crypto":crypto_data})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)

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

# ── YAHOO FINANCE QUOTE FETCHER ────────────
async def fetch_yahoo_batch(symbols: list, client: httpx.AsyncClient) -> dict:
    """Fetch multiple quotes - try multiple Yahoo endpoints"""
    # Try v8 crumb-free endpoint first
    endpoints = [
        f"{YF_BASE}/v8/finance/quote?symbols={{syms}}&corsDomain=finance.yahoo.com&crumb=",
        f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={{syms}}&fields=regularMarketPrice,regularMarketChange,regularMarketChangePercent,regularMarketOpen,regularMarketDayHigh,regularMarketDayLow,regularMarketVolume,marketCap",
        f"{YF_BASE}/v7/finance/quote?symbols={{syms}}",
    ]
    syms = ",".join(symbols)
    headers_list = [
        {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36", "Accept": "application/json", "Accept-Language": "en-US,en;q=0.9", "Origin": "https://finance.yahoo.com", "Referer": "https://finance.yahoo.com/"},
        {"User-Agent": "python-httpx/0.27.0", "Accept": "*/*"},
    ]
    for url_tmpl in endpoints:
        for hdrs in headers_list:
            try:
                url = url_tmpl.replace("{syms}", syms)
                r   = await client.get(url, headers=hdrs, timeout=10, follow_redirects=True)
                if r.status_code == 200:
                    data = r.json()
                    results = data.get("quoteResponse", {}).get("result", [])
                    if results:
                        return {q["symbol"]: q for q in results if q.get("regularMarketPrice")}
            except Exception as e:
                pass
    return {}

async def yahoo_loop():
    async with httpx.AsyncClient() as client:
        while True:
            updated = 0
            # Fetch in small batches of 5, 10 seconds apart
            for i in range(0, len(SYMBOLS), 5):
                batch = SYMBOLS[i:i+5]
                results = await fetch_yahoo_batch(batch, client)
                for sym, q in results.items():
                    price = q.get("regularMarketPrice", 0)
                    if price and price > 0:
                        prev = quotes.get(sym, {}).get("c", 0)
                        quotes[sym] = {
                            "c":  price,
                            "pc": q.get("regularMarketPreviousClose", price),
                            "o":  q.get("regularMarketOpen", price),
                            "h":  q.get("regularMarketDayHigh", price),
                            "l":  q.get("regularMarketDayLow", price),
                            "v":  q.get("regularMarketVolume", 0),
                            "mc": q.get("marketCap", 0),
                            "pe": q.get("trailingPE", 0),
                            "ts": int(time.time()),
                        }
                        updated += 1
                        if prev and abs(price - prev) / prev > 0.0005:
                            await broadcast({"type":"price","symbol":sym,"price":price,"data":quotes[sym]})
                await asyncio.sleep(10)  # 10 seconds between batches

            log.info(f"Yahoo: {updated}/{len(SYMBOLS)} symbols updated")
            await asyncio.sleep(60)  # wait 60s before next full cycle

# ── SEC EDGAR ──────────────────────────────
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
                    ns   = {"a":"http://www.w3.org/2005/Atom"}
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
                        score   = 0.1
                        tl      = title.lower()
                        for w in ["acqui","merger","buyback","dividend","beat","raised","upgrade","profit"]:
                            if w in tl: score += 0.2
                        for w in ["loss","downgrade","investigation","fine","penalty","restate","risk","warning"]:
                            if w in tl: score -= 0.25
                        score = round(max(-1, min(1, score)), 2)
                        filing = {
                            "ticker":ticker, "company":company,
                            "form":form.replace("+", " "), "title":title[:100],
                            "link":link, "score":score,
                            "sent":"pos" if score>0.1 else "neg" if score<-0.1 else "neu",
                            "ts":int(time.time()), "time":datetime.now().strftime("%H:%M:%S"),
                        }
                        sec_filings.insert(0, filing)
                        if len(sec_filings) > 100: sec_filings.pop()
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
                        crypto_data[key] = {"price":d.get("usd",0),"chg24":round(d.get("usd_24h_change",0),2),"ts":int(time.time())}
                log.info(f"Crypto: BTC=${crypto_data.get('BTC',{}).get('price','?')}")
                await broadcast({"type":"crypto","data":crypto_data})
            except Exception as e:
                log.warning(f"Crypto error: {e}")
            await asyncio.sleep(60)

@app.on_event("startup")
async def startup():
    asyncio.create_task(yahoo_loop())
    asyncio.create_task(edgar_loop())
    asyncio.create_task(crypto_loop())
    log.info("NEXUS Backend ready — Yahoo Finance + SEC + Crypto")

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
