"""
NEXUS Terminal — Backend Server
FastAPI + WebSocket + SEC EDGAR Scraper + Finnhub
Run: python server.py
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

# ── CONFIG ────────────────────────────────────
API_KEY      = os.environ.get("FINNHUB_KEY", "d700iu1r01qjh1odpe6gd700iu1r01qjh1odpe70")
PORT         = int(os.environ.get("PORT", 8000))
FINNHUB_REST = "https://finnhub.io/api/v1"
EDGAR_FEED   = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&dateb=&owner=include&count=20&search_text=&output=atom"
COINGECKO    = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,binancecoin&vs_currencies=usd&include_24hr_change=true&include_market_cap=true"

SYMBOLS = [
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","JPM","V","MA",
    "UNH","XOM","JNJ","PG","HD","COST","ABBV","MRK","CVX","BAC",
    "KO","PEP","WMT","DIS","NFLX","ADBE","CRM","AMD","GS","BLK",
    "SBUX","GE","HON","CAT","BA","LMT","NEE","GD","UNP","FDX",
    "AMGN","GILD","REGN","VRTX","MDT","SYK","ISRG","BSX","HCA","CVS",
    "COP","XOM","SLB","OXY","DVN","NEM","FCX","SHW","NUE","ALB",
]

# ── STATE ─────────────────────────────────────
quotes:    Dict[str, dict] = {}
sec_filings: List[dict]   = []
crypto_data: dict         = {}
whale_signals: List[dict] = []
clients: Set[WebSocket]   = set()
price_history: Dict[str, List[float]] = defaultdict(list)
seen_sec: Set[str]        = set()

app = FastAPI(title="NEXUS Terminal API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── WEBSOCKET ─────────────────────────────────
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
    await ws.send_json({
        "type": "snapshot",
        "quotes": quotes,
        "sec": sec_filings[:20],
        "crypto": crypto_data,
        "whales": whale_signals[:10],
    })
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)

# ── REST ──────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "NEXUS Terminal API", "quotes": len(quotes), "clients": len(clients)}

@app.get("/health")
async def health():
    return {"status": "ok", "quotes": len(quotes), "sec": len(sec_filings), "clients": len(clients), "ts": int(time.time())}

@app.get("/quotes")
async def get_quotes():
    return {"quotes": quotes, "ts": int(time.time())}

@app.get("/sec")
async def get_sec(limit: int = 50):
    return {"filings": sec_filings[:limit]}

@app.get("/crypto")
async def get_crypto():
    return crypto_data

@app.get("/whales")
async def get_whales():
    return {"signals": whale_signals[:20]}

@app.get("/feargreed")
async def fear_greed():
    if not quotes:
        return {"score": 50, "label": "NEUTRAL"}
    up   = sum(1 for q in quotes.values() if q.get("c", 0) > q.get("pc", 0))
    tot  = len(quotes)
    bread = up / tot * 100 if tot else 50
    mom   = sum((q.get("c",0)-q.get("pc",0))/q.get("pc",1)*100 for q in quotes.values() if q.get("pc")) / max(len(quotes),1)
    score = round(min(99, max(1, bread * 0.6 + 50 + mom * 2)))
    label = "EXTREME GREED" if score>=80 else "GREED" if score>=60 else "NEUTRAL" if score>=45 else "FEAR" if score>=25 else "EXTREME FEAR"
    return {"score": score, "label": label, "breadth": round(bread,1), "momentum": round(mom,2)}

@app.get("/alpha/{symbol}")
async def get_alpha(symbol: str):
    sym = symbol.upper()
    q   = quotes.get(sym)
    if not q:
        return {"error": "no data"}
    p, pc, v = q.get("c",0), q.get("pc",0), q.get("v",0)
    pct      = (p - pc) / pc * 100 if pc else 0
    vol_s    = 30 if v>80e6 else 20 if v>30e6 else 10 if v>10e6 else 0
    mom_s    = 30 if pct>3 else 20 if pct>1 else 10 if pct>0 else -10 if pct<-1 else 0
    sec_s    = sum(f.get("score",0)*20 for f in sec_filings if f.get("ticker")==sym and time.time()-f.get("ts",0)<86400)
    alpha    = round(min(99, max(-99, vol_s + mom_s + sec_s)))
    signal   = "STRONG BUY" if alpha>50 else "BUY" if alpha>20 else "NEUTRAL" if alpha>-20 else "SELL" if alpha>-50 else "STRONG SELL"
    return {"symbol":sym,"alpha":alpha,"signal":signal,"price":p,"pct_change":round(pct,2)}

# ── QUOTE FETCHER ─────────────────────────────
async def fetch_quotes_loop():
    async with httpx.AsyncClient() as client:
        while True:
            log.info(f"Fetching {len(SYMBOLS)} quotes...")
            for sym in SYMBOLS:
                try:
                    r = await client.get(f"{FINNHUB_REST}/quote?symbol={sym}&token={API_KEY}", timeout=5)
                    d = r.json()
                    if d and d.get("c") and d["c"] > 0:
                        old = quotes.get(sym, {}).get("c", 0)
                        quotes[sym] = {"c":d["c"],"pc":d["pc"],"o":d["o"],"h":d["h"],"l":d["l"],"v":d.get("v",0),"ts":int(time.time())}
                        price_history[sym].append(d["c"])
                        if len(price_history[sym]) > 500:
                            price_history[sym] = price_history[sym][-500:]
                        if old and abs(d["c"] - old) / old > 0.0001:
                            await broadcast({"type":"price","symbol":sym,"price":d["c"],"prev":old,"data":quotes[sym]})
                            await detect_whale(sym)
                except:
                    pass
                await asyncio.sleep(1.1)  # 60 req/min limit
            log.info(f"Cycle done. {len(quotes)} symbols loaded.")
            await asyncio.sleep(20)

# ── SEC EDGAR SCRAPER ─────────────────────────
async def edgar_loop():
    forms = ["8-K", "SC+13G", "10-Q", "4"]
    async with httpx.AsyncClient(headers={"User-Agent": "NEXUS research@nexus.io"}) as client:
        while True:
            for form in forms:
                try:
                    url = EDGAR_FEED.format(form=form)
                    r   = await client.get(url, timeout=10)
                    if r.status_code != 200:
                        continue
                    root = ET.fromstring(r.text)
                    ns   = {"a": "http://www.w3.org/2005/Atom"}
                    for entry in root.findall("a:entry", ns)[:5]:
                        eid   = entry.findtext("a:id", namespaces=ns) or ""
                        if eid in seen_sec:
                            continue
                        seen_sec.add(eid)
                        title = entry.findtext("a:title", namespaces=ns) or ""
                        upd   = entry.findtext("a:updated", namespaces=ns) or ""
                        link_el = entry.find("a:link", ns)
                        link  = link_el.get("href","") if link_el is not None else ""
                        m     = re.search(r'\(([A-Z]{1,5})\)', title)
                        ticker  = m.group(1) if m else ""
                        company = re.sub(r'\s*\([^)]*\)', '', title).strip()[:60]
                        pos_w = ["acqui","merger","buyback","dividend","beat","raised","upgrade","profit"]
                        neg_w = ["loss","downgrade","investigation","fine","penalty","restate","risk","warning"]
                        tl    = title.lower()
                        score = 0.1
                        for w in pos_w:
                            if w in tl: score += 0.2
                        for w in neg_w:
                            if w in tl: score -= 0.25
                        score = round(max(-1, min(1, score)), 2)
                        filing = {
                            "id": eid, "ticker": ticker, "company": company,
                            "form": form.replace("+", " "), "title": title[:100],
                            "link": link, "score": score,
                            "sent": "pos" if score>0.1 else "neg" if score<-0.1 else "neu",
                            "ts": int(time.time()), "time": datetime.now().strftime("%H:%M:%S"),
                        }
                        sec_filings.insert(0, filing)
                        if len(sec_filings) > 200:
                            sec_filings.pop()
                        log.info(f"SEC: {form} {ticker} {company[:30]} score={score}")
                        await broadcast({"type": "sec", "filing": filing})
                except Exception as e:
                    log.warning(f"EDGAR error ({form}): {e}")
                await asyncio.sleep(3)
            await asyncio.sleep(60)

# ── CRYPTO ────────────────────────────────────
async def crypto_loop():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                r    = await client.get(COINGECKO, timeout=10)
                data = r.json()
                for cid, key in [("bitcoin","BTC"),("ethereum","ETH"),("solana","SOL"),("binancecoin","BNB")]:
                    if cid in data:
                        d = data[cid]
                        crypto_data[key] = {"price":d.get("usd",0),"chg24":round(d.get("usd_24h_change",0),2),"mcap":d.get("usd_market_cap",0),"ts":int(time.time())}
                await broadcast({"type": "crypto", "data": crypto_data})
                log.info(f"Crypto: BTC=${crypto_data.get('BTC',{}).get('price','?')}")
            except Exception as e:
                log.warning(f"Crypto error: {e}")
            await asyncio.sleep(30)

# ── WHALE DETECTOR ────────────────────────────
async def detect_whale(sym: str):
    q = quotes.get(sym)
    if not q:
        return
    p, pc, v = q.get("c",0), q.get("pc",p if (p:=q.get("c",0)) else 0), q.get("v",0)
    pc = q.get("pc", p)
    if not p or not pc or not v:
        return
    pct      = abs((p - pc) / pc * 100)
    notional = v * p
    if notional > 50_000_000 and pct > 1.5:
        size = "MEGA" if notional > 500_000_000 else "LARGE" if notional > 100_000_000 else "MID"
        recent = [w for w in whale_signals if w["sym"]==sym and time.time()-w["ts"]<300]
        if not recent:
            sig = {"sym":sym,"price":p,"vol":v,"notional":round(notional/1e6,1),"pct":round(pct,2),"dir":"ACCUMULATION" if p>pc else "DISTRIBUTION","size":size,"bullish":p>pc,"ts":int(time.time()),"time":datetime.now().strftime("%H:%M:%S")}
            whale_signals.insert(0, sig)
            if len(whale_signals) > 50:
                whale_signals.pop()
            log.info(f"WHALE: {sym} {sig['dir']} ${sig['notional']}M")
            await broadcast({"type": "whale", "signal": sig})

# ── STARTUP ───────────────────────────────────
@app.on_event("startup")
async def startup():
    log.info("NEXUS Backend starting...")
    asyncio.create_task(fetch_quotes_loop())
    asyncio.create_task(edgar_loop())
    asyncio.create_task(crypto_loop())
    log.info("All tasks launched. Ready.")

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
