"""
NEXUS Terminal Backend v2.0
- Alpaca IEX: Gercek zamanli ABD borsasi
- SEC EDGAR: Canli dosyalama takibi
- RSS News: Reuters, CNBC, MarketWatch, Bloomberg, FT
- CoinGecko: Kripto fiyatlari
- Whale Flow: Hacim anomali tespiti
- Fear & Greed: Canli hesaplama
"""
import asyncio, logging, time, os, re, json
import httpx, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nexus")

# ── CONFIG ─────────────────────────────────────────────
ALPACA_KEY    = os.environ.get("ALPACA_KEY",    "PKZOJPXXKK3KYBDO5AKUIGH2IN")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET", "AcTmCfGqwQFkfFW15yZVeTpTTB8kvxXZoEJCcVmakXZq")
ALPACA_DATA   = "https://data.alpaca.markets/v2"
ALPACA_HEADS  = {
    "APCA-API-KEY-ID":     ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
    "Accept":              "application/json",
}
PORT = int(os.environ.get("PORT", 8000))

EDGAR_FEED  = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&dateb=&owner=include&count=20&search_text=&output=atom"
COINGECKO   = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,binancecoin&vs_currencies=usd&include_24hr_change=true&include_market_cap=true&include_24hr_vol=true"

# RSS haber kaynaklari
NEWS_FEEDS = [
    {"name": "Reuters Business", "url": "https://feeds.reuters.com/reuters/businessNews"},
    {"name": "CNBC",             "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html"},
    {"name": "MarketWatch",      "url": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines"},
    {"name": "Seeking Alpha",    "url": "https://seekingalpha.com/feed.xml"},
    {"name": "Yahoo Finance",    "url": "https://finance.yahoo.com/news/rssindex"},
    {"name": "Investing.com",    "url": "https://www.investing.com/rss/news.rss"},
]

# S&P 500 top 100
SYMBOLS = [
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","JPM","V","MA",
    "UNH","XOM","JNJ","PG","HD","COST","ABBV","MRK","CVX","BAC",
    "KO","PEP","WMT","DIS","NFLX","ADBE","CRM","AMD","GS","BLK",
    "SBUX","GE","HON","CAT","BA","LMT","NEE","GD","UNP","FDX",
    "AMGN","GILD","REGN","VRTX","MDT","SYK","ISRG","BSX","HCA","CVS",
    "COP","SLB","OXY","DVN","NEM","FCX","SHW","NUE","WFC","PNC",
    "USB","TFC","COF","AXP","SPGI","MCO","ICE","CME","BK","STT",
    "PYPL","INTC","QCOM","TXN","AMAT","LRCX","KLAC","NOW","PANW","CRWD",
    "FTNT","ADSK","INTU","SNPS","CDNS","MRVL","ANSS","ROP","NDAQ","CBOE",
    "IBM","CSCO","ORCL","ACN","CRM","NOW","WDAY","ZM","SNOW","PLTR",
]

# ── STATE ──────────────────────────────────────────────
quotes:       Dict[str, dict] = {}
sec_filings:  List[dict]      = []
news_items:   List[dict]      = []
crypto_data:  dict            = {}
whale_sigs:   List[dict]      = []
clients:      Set[WebSocket]  = set()
seen_sec:     Set[str]        = set()
seen_news:    Set[str]        = set()
fg_score:     dict            = {"score": 50, "label": "NEUTRAL"}

app = FastAPI(title="NEXUS Terminal API v2")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── WEBSOCKET ──────────────────────────────────────────
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
        "type":   "snapshot",
        "quotes": quotes,
        "sec":    sec_filings[:20],
        "news":   news_items[:20],
        "crypto": crypto_data,
        "whales": whale_sigs[:10],
        "fg":     fg_score,
    })
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
        log.info(f"Client disconnected. Total: {len(clients)}")

# ── REST ENDPOINTS ─────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "NEXUS Terminal v2", "quotes": len(quotes), "sec": len(sec_filings), "news": len(news_items), "clients": len(clients)}

@app.get("/health")
async def health():
    return {"status": "ok", "quotes": len(quotes), "sec": len(sec_filings), "news": len(news_items), "crypto": len(crypto_data), "clients": len(clients), "ts": int(time.time())}

@app.get("/quotes")
async def get_quotes():
    return {"quotes": quotes, "ts": int(time.time())}

@app.get("/quotes/{symbol}")
async def get_quote(symbol: str):
    return quotes.get(symbol.upper(), {"error": "not found"})

@app.get("/sec")
async def get_sec(limit: int = 30):
    return {"filings": sec_filings[:limit], "count": len(sec_filings)}

@app.get("/news")
async def get_news(limit: int = 30):
    return {"items": news_items[:limit], "count": len(news_items)}

@app.get("/crypto")
async def get_crypto():
    return crypto_data

@app.get("/whales")
async def get_whales():
    return {"signals": whale_sigs[:20]}

@app.get("/feargreed")
async def get_feargreed():
    return fg_score

@app.get("/alpha/{symbol}")
async def get_alpha(symbol: str):
    sym = symbol.upper()
    q   = quotes.get(sym)
    if not q:
        return {"error": "no data"}
    p, pc, v = q.get("c", 0), q.get("pc", 0), q.get("v", 0)
    pct  = (p - pc) / pc * 100 if pc else 0
    vs   = 30 if v > 80e6 else 20 if v > 30e6 else 10 if v > 10e6 else 0
    ms   = 30 if pct > 3 else 20 if pct > 1 else 10 if pct > 0 else -10 if pct < -1 else 0
    ss   = sum(f.get("score", 0) * 20 for f in sec_filings if f.get("ticker") == sym and time.time() - f.get("ts", 0) < 86400)
    alpha = round(min(99, max(-99, vs + ms + ss)))
    signal = "STRONG BUY" if alpha > 50 else "BUY" if alpha > 20 else "NEUTRAL" if alpha > -20 else "SELL" if alpha > -50 else "STRONG SELL"
    return {"symbol": sym, "alpha": alpha, "signal": signal, "price": p, "pct_change": round(pct, 2), "vol_score": vs}

# ── ALPACA IEX QUOTE FETCHER ───────────────────────────
async def alpaca_loop():
    """Fetch real-time quotes from Alpaca IEX feed — bulk snapshot"""
    async with httpx.AsyncClient() as client:
        while True:
            try:
                syms_str = ",".join(SYMBOLS)
                r = await client.get(
                    f"{ALPACA_DATA}/stocks/snapshots",
                    headers=ALPACA_HEADS,
                    params={"symbols": syms_str, "feed": "iex"},
                    timeout=20,
                )
                if r.status_code == 200:
                    data    = r.json()
                    updated = 0
                    for sym, snap in data.items():
                        try:
                            lt   = snap.get("latestTrade") or {}
                            lq   = snap.get("latestQuote") or {}
                            db   = snap.get("dailyBar") or {}
                            pb   = snap.get("prevDailyBar") or {}
                            price = lt.get("p") or db.get("c") or 0
                            if not price or price <= 0:
                                continue
                            prev = pb.get("c") or price
                            old  = quotes.get(sym, {}).get("c", 0)
                            quotes[sym] = {
                                "c":  round(price, 2),
                                "pc": round(prev, 2),
                                "o":  round(db.get("o") or price, 2),
                                "h":  round(db.get("h") or price, 2),
                                "l":  round(db.get("l") or price, 2),
                                "v":  int(db.get("v") or 0),
                                "vw": round(db.get("vw") or price, 2),
                                "ask": round(lq.get("ap") or price, 2),
                                "bid": round(lq.get("bp") or price, 2),
                                "ts": int(time.time()),
                            }
                            updated += 1
                            if old and abs(price - old) / old > 0.0001:
                                await broadcast({"type": "price", "symbol": sym, "price": price, "data": quotes[sym]})
                                await detect_whale(sym)
                        except:
                            pass
                    log.info(f"Alpaca IEX: {updated}/{len(SYMBOLS)} quotes updated")
                    # Recalc fear & greed after each update
                    calc_fear_greed()
                elif r.status_code == 401:
                    log.error("Alpaca 401 — check API keys")
                    await asyncio.sleep(60)
                elif r.status_code == 429:
                    log.warning("Alpaca rate limit — waiting 30s")
                    await asyncio.sleep(30)
                else:
                    log.warning(f"Alpaca error {r.status_code}: {r.text[:100]}")
            except Exception as e:
                log.warning(f"Alpaca fetch error: {e}")
            await asyncio.sleep(15)  # refresh every 15s

# ── FEAR & GREED ───────────────────────────────────────
def calc_fear_greed():
    if not quotes:
        return
    up    = sum(1 for q in quotes.values() if q.get("c", 0) > q.get("pc", 0))
    total = len(quotes)
    bread = up / total * 100 if total else 50
    mom   = sum((q.get("c", 0) - q.get("pc", 0)) / q.get("pc", 1) * 100 for q in quotes.values() if q.get("pc")) / max(total, 1)
    score = round(min(99, max(1, bread * 0.6 + 50 + mom * 2)))
    label = "EXTREME GREED" if score >= 80 else "GREED" if score >= 60 else "NEUTRAL" if score >= 45 else "FEAR" if score >= 25 else "EXTREME FEAR"
    fg_score.update({"score": score, "label": label, "breadth": round(bread, 1), "momentum": round(mom, 2), "ts": int(time.time())})

# ── WHALE FLOW DETECTOR ────────────────────────────────
async def detect_whale(sym: str):
    q = quotes.get(sym)
    if not q:
        return
    p, pc, v = q.get("c", 0), q.get("pc", 0), q.get("v", 0)
    if not p or not pc or not v:
        return
    pct      = abs((p - pc) / pc * 100)
    notional = v * p
    if notional > 50_000_000 and pct > 1.5:
        size   = "MEGA" if notional > 500_000_000 else "LARGE" if notional > 100_000_000 else "MID"
        recent = [w for w in whale_sigs if w["sym"] == sym and time.time() - w["ts"] < 300]
        if not recent:
            sig = {
                "sym":      sym,
                "price":    p,
                "notional": round(notional / 1e6, 1),
                "pct":      round(pct, 2),
                "dir":      "ACCUMULATION" if p > pc else "DISTRIBUTION",
                "size":     size,
                "bullish":  p > pc,
                "ts":       int(time.time()),
                "time":     datetime.now().strftime("%H:%M:%S"),
            }
            whale_sigs.insert(0, sig)
            if len(whale_sigs) > 50:
                whale_sigs.pop()
            log.info(f"WHALE: {sym} {sig['dir']} ${sig['notional']}M {sig['pct']}%")
            await broadcast({"type": "whale", "signal": sig})

# ── SEC EDGAR SCRAPER ──────────────────────────────────
async def edgar_loop():
    forms = ["8-K", "SC+13G", "13F-HR", "10-Q", "4", "S-1"]
    async with httpx.AsyncClient(headers={"User-Agent": "NEXUS research@nexus.io"}) as client:
        while True:
            for form in forms:
                try:
                    r = await client.get(EDGAR_FEED.format(form=form), timeout=10)
                    if r.status_code != 200:
                        continue
                    root = ET.fromstring(r.text)
                    ns   = {"a": "http://www.w3.org/2005/Atom"}
                    for entry in root.findall("a:entry", ns)[:5]:
                        eid     = entry.findtext("a:id", namespaces=ns) or ""
                        if eid in seen_sec:
                            continue
                        seen_sec.add(eid)
                        title   = entry.findtext("a:title", namespaces=ns) or ""
                        link_el = entry.find("a:link", ns)
                        link    = link_el.get("href", "") if link_el is not None else ""
                        upd     = entry.findtext("a:updated", namespaces=ns) or ""
                        m       = re.search(r'\(([A-Z]{1,5})\)', title)
                        ticker  = m.group(1) if m else ""
                        company = re.sub(r'\s*\([^)]*\)', '', title).strip()[:60]
                        score   = 0.1
                        tl      = title.lower()
                        for w in ["acqui", "merger", "buyback", "dividend", "beat", "raised", "upgrade", "profit", "record", "growth"]:
                            if w in tl: score += 0.15
                        for w in ["loss", "downgrade", "investigation", "fine", "penalty", "restate", "risk", "warning", "decline", "miss"]:
                            if w in tl: score -= 0.2
                        score = round(max(-1, min(1, score)), 2)
                        filing = {
                            "ticker":  ticker,
                            "company": company,
                            "form":    form.replace("+", " "),
                            "title":   title[:100],
                            "link":    link,
                            "score":   score,
                            "sent":    "pos" if score > 0.1 else "neg" if score < -0.1 else "neu",
                            "ts":      int(time.time()),
                            "time":    datetime.now().strftime("%H:%M:%S"),
                        }
                        sec_filings.insert(0, filing)
                        if len(sec_filings) > 200:
                            sec_filings.pop()
                        log.info(f"SEC {form}: {ticker} {company[:30]} score={score}")
                        await broadcast({"type": "sec", "filing": filing})
                except Exception as e:
                    log.warning(f"EDGAR error ({form}): {e}")
                await asyncio.sleep(4)
            await asyncio.sleep(90)

# ── RSS NEWS FETCHER ───────────────────────────────────
POS_WORDS = ["surge","jump","beat","record","profit","growth","gain","rise","rally","bullish","upgrade","soar","high","strong","buy"]
NEG_WORDS = ["fall","drop","miss","loss","risk","warn","downgrade","cut","bearish","decline","plunge","slump","weak","sell","crash","fear"]

def score_headline(title: str) -> tuple:
    tl = title.lower()
    score = sum(0.2 for w in POS_WORDS if w in tl) - sum(0.2 for w in NEG_WORDS if w in tl)
    score = round(max(-1, min(1, score)), 2)
    sent  = "pos" if score > 0.1 else "neg" if score < -0.1 else "neu"
    return score, sent

async def news_loop():
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0 NEXUS/2.0"}, follow_redirects=True) as client:
        while True:
            for feed in NEWS_FEEDS:
                try:
                    r = await client.get(feed["url"], timeout=10)
                    if r.status_code != 200:
                        continue
                    root = ET.fromstring(r.content)
                    # Handle both RSS and Atom
                    items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
                    for item in items[:5]:
                        # RSS
                        title = item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title") or ""
                        link  = item.findtext("link") or ""
                        if not link:
                            le = item.find("{http://www.w3.org/2005/Atom}link")
                            link = le.get("href", "") if le is not None else ""
                        pub   = item.findtext("pubDate") or item.findtext("{http://www.w3.org/2005/Atom}updated") or ""
                        uid   = link or title[:80]
                        if uid in seen_news or not title:
                            continue
                        seen_news.add(uid)
                        score, sent = score_headline(title)
                        # Extract ticker mentions
                        tickers = re.findall(r'\b([A-Z]{2,5})\b', title)
                        tickers = [t for t in tickers if t in SYMBOLS][:3]
                        news_item = {
                            "source":   feed["name"],
                            "title":    title[:120],
                            "link":     link,
                            "score":    score,
                            "sent":     sent,
                            "tickers":  tickers,
                            "ts":       int(time.time()),
                            "time":     datetime.now().strftime("%H:%M:%S"),
                            "pub":      pub[:30],
                        }
                        news_items.insert(0, news_item)
                        if len(news_items) > 200:
                            news_items.pop()
                        log.info(f"NEWS [{feed['name']}]: {title[:50]} ({sent})")
                        await broadcast({"type": "news", "item": news_item})
                except Exception as e:
                    log.warning(f"RSS error [{feed['name']}]: {e}")
                await asyncio.sleep(3)
            await asyncio.sleep(120)  # re-fetch all feeds every 2 min

# ── CRYPTO ─────────────────────────────────────────────
# Top 100 crypto IDs
CRYPTO_IDS = [
    "bitcoin","ethereum","tether","binancecoin","solana","ripple","usd-coin","cardano",
    "avalanche-2","dogecoin","polkadot","chainlink","tron","polygon","shiba-inu",
    "litecoin","dai","bitcoin-cash","stellar","monero","ethereum-classic","okb",
    "cosmos","uniswap","hedera-hashgraph","filecoin","internet-computer","cronos",
    "lido-dao","near","vechain","algorand","quant-network","aptos","arbitrum",
    "the-graph","fantom","theta-token","aave","elrond-erd-2","flow","tezos",
    "axie-infinity","decentraland","eos","bitcoin-sv","kucoin-shares","maker",
    "neo","waves","iota","dash","zcash","compound-governance-token","yearn-finance",
    "sushi","pancakeswap-token","1inch","loopring","curve-dao-token","balancer",
    "synthetix-network-token","uma","band-protocol","ren","kyber-network-crystal",
    "ocean-protocol","basic-attention-token","augur","district0x","status",
    "storj","golem","civic","power-ledger","numeraire","wrapped-bitcoin",
    "huobi-token","gate","crypto-com-chain","ftx-token","leo-token","oec-token",
    "thor","harmony","celo","ontology","icon","zilliqa","qtum","nano",
    "digibyte","horizen","ravencoin","siacoin","verge","decred","stratis",
    "wax","hive","steem","ark","pivx",
]

async def crypto_loop():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                # Fetch top 100 in batches of 50 (CoinGecko limit)
                all_data = {}
                for i in range(0, len(CRYPTO_IDS), 50):
                    batch = CRYPTO_IDS[i:i+50]
                    ids_str = ",".join(batch)
                    r = await client.get(
                        f"https://api.coingecko.com/api/v3/simple/price?ids={ids_str}&vs_currencies=usd&include_24hr_change=true&include_market_cap=true&include_24hr_vol=true",
                        timeout=15
                    )
                    if r.status_code == 200:
                        all_data.update(r.json())
                    await asyncio.sleep(2)

                for cid in CRYPTO_IDS:
                    if cid not in all_data:
                        continue
                    d = all_data[cid]
                    # Use uppercase symbol as key
                    sym = cid.upper()[:8]
                    # Map common ones to proper symbols
                    sym_map = {
                        "BITCOIN":"BTC","ETHEREUM":"ETH","BINANCECOI":"BNB","SOLANA":"SOL",
                        "RIPPLE":"XRP","USD-COIN":"USDC","CARDANO":"ADA","DOGECOIN":"DOGE",
                        "AVALANCHE":"AVAX","POLKADOT":"DOT","CHAINLINK":"LINK","TRON":"TRX",
                        "POLYGON":"MATIC","SHIBA-INU":"SHIB","LITECOIN":"LTC","STELLAR":"XLM",
                        "MONERO":"XMR","COSMOS":"ATOM","UNISWAP":"UNI","NEAR":"NEAR",
                        "APTOS":"APT","ARBITRUM":"ARB","AAVE":"AAVE","MAKER":"MKR",
                        "WRAPPED-BI":"WBTC","TETHER":"USDT","DAI":"DAI","HEDERA-HAS":"HBAR",
                        "FILECOIN":"FIL","INTERNET-C":"ICP","VECHAIN":"VET","ALGORAND":"ALGO",
                        "FANTOM":"FTM","TEZOS":"XTZ","EOS":"EOS","DASH":"DASH","ZCASH":"ZEC",
                        "LIDO-DAO":"LDO","THE-GRAPH":"GRT","FLOW":"FLOW","ELROND-ERD":"EGLD",
                    }
                    symbol = sym_map.get(sym, sym)
                    crypto_data[symbol] = {
                        "id":    cid,
                        "price": d.get("usd", 0),
                        "chg24": round(d.get("usd_24h_change", 0), 2),
                        "vol24": d.get("usd_24h_vol", 0),
                        "mcap":  d.get("usd_market_cap", 0),
                        "ts":    int(time.time()),
                    }

                log.info(f"Crypto: {len(crypto_data)} coins updated. BTC=${crypto_data.get('BTC',{}).get('price','?')}")
                await broadcast({"type": "crypto", "data": crypto_data})
            except Exception as e:
                log.warning(f"Crypto error: {e}")
            await asyncio.sleep(120)  # CoinGecko free tier: update every 2 min

# ── STARTUP ────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    log.info("NEXUS Backend v2 starting...")
    asyncio.create_task(alpaca_loop())
    asyncio.create_task(edgar_loop())
    asyncio.create_task(news_loop())
    asyncio.create_task(crypto_loop())
    log.info("All tasks launched. Ready.")

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
