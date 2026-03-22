"""
NEXUS Terminal Backend — Railway Production
Optimized for free tier: 20 symbols, slow polling
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

API_KEY      = os.environ.get("FINNHUB_KEY", "d700iu1r01qjh1odpe6gd700iu1r01qjh1odpe70")
PORT         = int(os.environ.get("PORT", 8000))
FINNHUB_REST = "https://finnhub.io/api/v1"
EDGAR_FEED   = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&dateb=&owner=include&count=20&search_text=&output=atom"
COINGECKO    = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,binancecoin&vs_currencies=usd&include_24hr_change=true&include_market_cap=true"

# Top 20 only — stays within free tier rate limit
SYMBOLS = [
    "AAPL","MSFT","NVDA","GOOGL","AMZN",
    "META","TSLA","JPM","V","MA",
    "JNJ","PG","KO","XOM","BAC",
    "WMT","DIS","NFLX","GS","AMD",
]

quotes:       Dict[str, dict] = {}
sec_filings:  List[dict]      = []
crypto_data:  dict            = {}
clients:      Set[WebSocket]  = set()
seen_sec:     Set[str]        = set()

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

async def quote_loop():
    async with httpx.AsyncClient() as client:
        while True:
            for sym in SYMBOLS:
                try:
                    r = await client.get(f"{FINNHUB_REST}/quote?symbol={sym}&token={API_KEY}", timeout=5)
                    d = r.json()
                    if d and d.get("c") and d["c"] > 0:
                        quotes[sym] = {"c":d["c"],"pc":d["pc"],"o":d["o"],"h":d["h"],"l":d["l"],"v":d.get("v",0),"ts":int(time.time())}
                        liveP = d["c"]
                        await broadcast({"type":"price","symbol":sym,"price":liveP,"data":quotes[sym]})
                        log.info(f"{sym} = ${d['c']}")
                except Exception as e:
                    log.warning(f"{sym} error: {e}")
                await asyncio.sleep(3)  # 20 req/min — well within limit
            log.info(f"Cycle done. {len(quotes)} symbols.")
            await asyncio.sleep(60)  # wait 60s before next full cycle

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
    asyncio.create_task(quote_loop())
    asyncio.create_task(edgar_loop())
    asyncio.create_task(crypto_loop())
    log.info("NEXUS Backend ready.")

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
