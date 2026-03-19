"""
Bot 2 — FO Stocks Bot
Scans: Top 150 F&O eligible stocks
Equity signal every day (score 5/6)
Stock option signal same time as equity signal always
"""
import os, asyncio, logging, json, urllib.request
import pandas as pd
import numpy as np
from datetime import datetime, time, date, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():  return datetime.now(IST)
def time_ist(): return now_ist().time()
def date_ist(): return now_ist().date()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ANGEL_API_KEY    = os.getenv("ANGEL_API_KEY", "")
ANGEL_CLIENT_ID  = os.getenv("ANGEL_CLIENT_ID", "")
ANGEL_PASSWORD   = os.getenv("ANGEL_PASSWORD", "")
ANGEL_TOTP       = os.getenv("ANGEL_TOTP_SECRET", "")
PAPER_MODE       = True

smart_api   = None
angel_ready = False
angel_error = "Not attempted"
INSTRUMENTS = {}

def connect_angel():
    global smart_api, angel_ready, angel_error
    try:
        if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP]):
            angel_error = f"Missing creds API={bool(ANGEL_API_KEY)} CID={bool(ANGEL_CLIENT_ID)}"
            return False
        import pyotp
        from SmartApi import SmartConnect
        totp_code = pyotp.TOTP(ANGEL_TOTP).now()
        obj  = SmartConnect(api_key=ANGEL_API_KEY)
        data = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp_code)
        if data and data.get("status"):
            smart_api   = obj
            angel_ready = True
            angel_error = "OK"
            logger.info("Angel One connected!")
            return True
        angel_error = f"Login failed: {data}"
    except Exception as e:
        angel_error = f"Error: {e}"
    logger.error(f"Angel connect failed: {angel_error}")
    return False

def load_instruments():
    global INSTRUMENTS
    try:
        logger.info("Loading F&O instruments...")
        url  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        data = urllib.request.urlopen(url, timeout=20).read()
        items = json.loads(data)
        result = {}
        for item in items:
            exch   = item.get("exch_seg","")
            itype  = item.get("instrumenttype","")
            symbol = item.get("symbol","")
            token  = item.get("token","")
            lotsize= item.get("lotsize",1)
            if not symbol or not token: continue
            if exch in ("NFO","BFO") and itype in ("OPTSTK","OPTIDX","FUTSTK","FUTIDX"):
                result[symbol] = {"token":token,"symbol":symbol,"exchange":exch,
                                  "itype":itype,"lotsize":int(lotsize or 1)}
        INSTRUMENTS = result
        logger.info(f"Loaded {len(INSTRUMENTS)} F&O instruments")
    except Exception as e:
        logger.error(f"Instrument load error: {e}")

# ── 150 F&O Eligible Stocks with tokens ──────────────────────────────────────
FO_STOCKS = {
    "RELIANCE": {"token":"2885",  "exchange":"NSE"},
    "TCS":      {"token":"11536", "exchange":"NSE"},
    "HDFCBANK": {"token":"1333",  "exchange":"NSE"},
    "INFY":     {"token":"1594",  "exchange":"NSE"},
    "ICICIBANK":{"token":"4963",  "exchange":"NSE"},
    "SBIN":     {"token":"3045",  "exchange":"NSE"},
    "KOTAKBANK":{"token":"1922",  "exchange":"NSE"},
    "LT":       {"token":"11483", "exchange":"NSE"},
    "AXISBANK": {"token":"5900",  "exchange":"NSE"},
    "BAJFINANCE":{"token":"317",  "exchange":"NSE"},
    "WIPRO":    {"token":"3787",  "exchange":"NSE"},
    "HCLTECH":  {"token":"1363",  "exchange":"NSE"},
    "TECHM":    {"token":"13538", "exchange":"NSE"},
    "MARUTI":   {"token":"10999", "exchange":"NSE"},
    "SUNPHARMA":{"token":"3351",  "exchange":"NSE"},
    "TITAN":    {"token":"3506",  "exchange":"NSE"},
    "ASIANPAINT":{"token":"236",  "exchange":"NSE"},
    "BHARTIARTL":{"token":"10604","exchange":"NSE"},
    "ULTRACEMCO":{"token":"11532","exchange":"NSE"},
    "ITC":      {"token":"1660",  "exchange":"NSE"},
    "HINDUNILVR":{"token":"1394", "exchange":"NSE"},
    "POWERGRID":{"token":"14977", "exchange":"NSE"},
    "NTPC":     {"token":"11630", "exchange":"NSE"},
    "ONGC":     {"token":"2475",  "exchange":"NSE"},
    "TATASTEEL":{"token":"3499",  "exchange":"NSE"},
    "ADANIENT": {"token":"25",    "exchange":"NSE"},
    "ADANIPORTS":{"token":"15083","exchange":"NSE"},
    "COALINDIA":{"token":"20374", "exchange":"NSE"},
    "EICHERMOT":{"token":"910",   "exchange":"NSE"},
    "DIVISLAB": {"token":"10940", "exchange":"NSE"},
    "DRREDDY":  {"token":"881",   "exchange":"NSE"},
    "CIPLA":    {"token":"694",   "exchange":"NSE"},
    "APOLLOHOSP":{"token":"157",  "exchange":"NSE"},
    "TATACONSUM":{"token":"3432", "exchange":"NSE"},
    "BRITANNIA":{"token":"547",   "exchange":"NSE"},
    "GRASIM":   {"token":"1232",  "exchange":"NSE"},
    "INDUSINDBK":{"token":"5258", "exchange":"NSE"},
    "SBILIFE":  {"token":"21808", "exchange":"NSE"},
    "HDFCLIFE": {"token":"467",   "exchange":"NSE"},
    "TATAPOWER":{"token":"3426",  "exchange":"NSE"},
    "TATAMOTORS":{"token":"3456", "exchange":"NSE"},
    "M&M":      {"token":"2031",  "exchange":"NSE"},
    "PERSISTENT":{"token":"18365","exchange":"NSE"},
    "COFORGE":  {"token":"23650", "exchange":"NSE"},
    "VOLTAS":   {"token":"3597",  "exchange":"NSE"},
    "HAVELLS":  {"token":"430",   "exchange":"NSE"},
    "POLYCAB":  {"token":"23650", "exchange":"NSE"},
    "BERGEPAINT":{"token":"404",  "exchange":"NSE"},
    "INDIGO":   {"token":"11195", "exchange":"NSE"},
    "IRCTC":    {"token":"13611", "exchange":"NSE"},
    "NAUKRI":   {"token":"13751", "exchange":"NSE"},
    "ZOMATO":   {"token":"5097",  "exchange":"NSE"},
    "TATAELXSI":{"token":"3453",  "exchange":"NSE"},
    "KPITTECH": {"token":"4453",  "exchange":"NSE"},
    "BPCL":     {"token":"526",   "exchange":"NSE"},
    "HINDALCO": {"token":"1363",  "exchange":"NSE"},
    "IOC":      {"token":"1624",  "exchange":"NSE"},
    "RECLTD":   {"token":"21614", "exchange":"NSE"},
    "PFC":      {"token":"14299", "exchange":"NSE"},
    "DMART":    {"token":"19913", "exchange":"NSE"},
    "TRENT":    {"token":"3513",  "exchange":"NSE"},
    "IDFCFIRSTB":{"token":"16675","exchange":"NSE"},
    "FEDERALBNK":{"token":"1023", "exchange":"NSE"},
    "PNB":      {"token":"2730",  "exchange":"NSE"},
    "BANKBARODA":{"token":"4668", "exchange":"NSE"},
    "CANBK":    {"token":"2763",  "exchange":"NSE"},
    "MUTHOOTFIN":{"token":"3012", "exchange":"NSE"},
    "CHOLAFIN": {"token":"685",   "exchange":"NSE"},
    "SBICARD":  {"token":"317",   "exchange":"NSE"},
    "LTTS":     {"token":"18365", "exchange":"NSE"},
    "MPHASIS":  {"token":"4503",  "exchange":"NSE"},
    "LTIM":     {"token":"17818", "exchange":"NSE"},
    "SAIL":     {"token":"3273",  "exchange":"NSE"},
    "NMDC":     {"token":"15332", "exchange":"NSE"},
    "IRFC":     {"token":"20143", "exchange":"NSE"},
    "NHPC":     {"token":"13751", "exchange":"NSE"},
    "ADANIGREEN":{"token":"236",  "exchange":"NSE"},
    "PAGEIND":  {"token":"14413", "exchange":"NSE"},
    "JUBLFOOD":  {"token":"18096","exchange":"NSE"},
    "BAJAJFINSV":{"token":"16675","exchange":"NSE"},
    "HEROMOTOCO":{"token":"1348", "exchange":"NSE"},
    "ASHOKLEY": {"token":"212",   "exchange":"NSE"},
    "TVSMOTOR": {"token":"3539",  "exchange":"NSE"},
    "MRF":      {"token":"2277",  "exchange":"NSE"},
    "APOLLOTYRE":{"token":"163",  "exchange":"NSE"},
    "BALKRISIND":{"token":"335",  "exchange":"NSE"},
    "CONCOR":   {"token":"4717",  "exchange":"NSE"},
    "AUROPHARMA":{"token":"275",  "exchange":"NSE"},
    "TORNTPHARM":{"token":"3518", "exchange":"NSE"},
    "CROMPTON":  {"token":"13538","exchange":"NSE"},
    "JSWSTEEL":  {"token":"11723","exchange":"NSE"},
    "SHREECEM":  {"token":"3103", "exchange":"NSE"},
    "AMBUJACEM": {"token":"1270", "exchange":"NSE"},
    "ICICIPRULI":{"token":"18652","exchange":"NSE"},
    "NESTLEIND": {"token":"17963","exchange":"NSE"},
    "OFSS":      {"token":"10738","exchange":"NSE"},
    "BANDHANBNK":{"token":"2263", "exchange":"NSE"},
    "LICHSGFIN": {"token":"1997", "exchange":"NSE"},
    "MANAPPURAM":{"token":"15083","exchange":"NSE"},
    "TORNTPOWER":{"token":"3518", "exchange":"NSE"},
}

# ── Settings ──────────────────────────────────────────────────────────────────
TRADE_START    = time(10, 0)
TRADE_END      = time(15, 15)
SQUAREOFF_TIME = time(15, 15)
TARGET_MULT    = 2.0
VIX_HIGH       = 20.0
VIX_TOKEN      = "99919000"
VIX_SYMBOL     = "India VIX"

pending_signals  = {}
paper_positions  = {}
active_positions = {}
paper_trades     = []
daily_trades     = []
squaredoff_today = False

def is_trading_time():
    n = now_ist()
    return n.weekday() < 5 and TRADE_START <= n.time() <= TRADE_END

def last_expiry_month(exp_day=3):
    today = date_ist()
    m = today.month % 12 + 1
    y = today.year + (1 if today.month == 12 else 0)
    last = date(y, m, 1) - timedelta(days=1)
    while last.weekday() != exp_day:
        last -= timedelta(days=1)
    return last

def get_stock_qty(ltp):
    if ltp<=100: return 50
    elif ltp<=250: return 25
    elif ltp<=500: return 20
    elif ltp<=1000: return 10
    elif ltp<=2000: return 5
    elif ltp<=5000: return 2
    return 1

# ── Data ──────────────────────────────────────────────────────────────────────
def get_ltp(token, exchange, symbol):
    if not angel_ready or smart_api is None: return None
    try:
        r = smart_api.ltpData(exchange, symbol, token)
        if r and r.get("status") and r.get("data"):
            return float(r["data"]["ltp"])
    except Exception as e:
        logger.error(f"LTP [{symbol}]: {e}")
    return None

def fetch_candles(token, exchange, symbol):
    if not angel_ready or smart_api is None: return None
    try:
        today     = date_ist()
        from_date = (today - timedelta(days=5)).strftime("%Y-%m-%d")
        resp = smart_api.getCandleData({
            "exchange": exchange, "symboltoken": str(token),
            "interval": "FIVE_MINUTE",
            "fromdate": f"{from_date} 09:15",
            "todate":   now_ist().strftime("%Y-%m-%d %H:%M"),
        })
        if not resp or not resp.get("status") or not resp.get("data"): return None
        df = pd.DataFrame(resp["data"], columns=["timestamp","open","high","low","close","volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp").astype(float)
        return df if len(df) >= 20 else None
    except Exception as e:
        logger.error(f"Candle [{symbol}]: {e}")
        return None

def resample_tf(df, tf):
    return df.resample(tf).agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()

def get_stock_option_token(symbol, strike, opt_type, expiry):
    if not angel_ready or smart_api is None: return None, None, 1
    exp_str = expiry.strftime("%d%b%y").upper()
    sym     = f"{symbol}{exp_str}{int(strike)}{opt_type}"
    try:
        r = smart_api.searchScrip("NFO", sym)
        if r and r.get("status") and r.get("data"):
            lotsize = int(r["data"][0].get("lotsize",1))
            return r["data"][0]["symboltoken"], r["data"][0].get("tradingsymbol",sym), lotsize
    except Exception as e:
        logger.error(f"StockOpt [{sym}]: {e}")
    return None, None, 1

def place_order(token, exchange, symbol, txn_type, quantity):
    if PAPER_MODE:
        return {"status":True,"data":{"orderid":f"PAPER-{int(now_ist().timestamp())}"}}
    if not angel_ready or smart_api is None:
        return {"status":False,"message":"Not connected"}
    try:
        return smart_api.placeOrder({
            "variety":"NORMAL","tradingsymbol":symbol,"symboltoken":token,
            "transactiontype":txn_type,"exchange":exchange,"ordertype":"MARKET",
            "producttype":"INTRADAY","duration":"DAY","quantity":str(quantity),"price":"0",
        })
    except Exception as e:
        return {"status":False,"message":str(e)}

# ── Indicators ────────────────────────────────────────────────────────────────
def calc_supertrend(df, period=10, mult=3.0):
    h,l,c = df["high"],df["low"],df["close"]
    hl2   = (h+l)/2
    tr    = pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    atr   = tr.ewm(span=period,adjust=False).mean()
    upper = hl2+mult*atr; lower = hl2-mult*atr
    d = pd.Series(0,index=df.index,dtype=int)
    for i in range(1,len(df)):
        if c.iloc[i]>upper.iloc[i-1]:   d.iloc[i]=1
        elif c.iloc[i]<lower.iloc[i-1]: d.iloc[i]=-1
        else:                            d.iloc[i]=d.iloc[i-1]
    return d

def calc_vwap(df):
    tp = (df["high"]+df["low"]+df["close"])/3
    return (tp*df["volume"]).cumsum()/df["volume"].cumsum()

def calc_adx(df, period=14):
    h,l,c = df["high"],df["low"],df["close"]
    up,dn = h.diff(),-l.diff()
    pdm = pd.Series(np.where((up>dn)&(up>0),up,0.0),index=df.index)
    ndm = pd.Series(np.where((dn>up)&(dn>0),dn,0.0),index=df.index)
    tr  = pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    atr = tr.ewm(span=period,adjust=False).mean()
    pdi = 100*pdm.ewm(span=period,adjust=False).mean()/atr
    ndi = 100*ndm.ewm(span=period,adjust=False).mean()/atr
    dx  = 100*(pdi-ndi).abs()/(pdi+ndi).replace(0,np.nan)
    return dx.ewm(span=period,adjust=False).mean(),pdi,ndi

def calc_rsi(df,period=14):
    d=df["close"].diff()
    g=d.clip(lower=0).ewm(span=period,adjust=False).mean()
    l=(-d.clip(upper=0)).ewm(span=period,adjust=False).mean()
    return 100-(100/(1+g/l.replace(0,np.nan)))

def calc_macd(df):
    fast=df["close"].ewm(span=12,adjust=False).mean()
    slow=df["close"].ewm(span=26,adjust=False).mean()
    m=fast-slow; s=m.ewm(span=9,adjust=False).mean()
    return m,s

def get_signal(token, exchange, symbol):
    raw = fetch_candles(token, exchange, symbol)
    if raw is None or len(raw)<20: return None
    df5=raw; df15=resample_tf(raw,"15min")
    if len(df15)<5 or len(df5)<10: return None
    try:
        t15=int(calc_supertrend(df15).iloc[-1]); d5=int(calc_supertrend(df5).iloc[-1])
        close5=float(df5["close"].iloc[-1]); vwap_v=float(calc_vwap(df5).iloc[-1])
        adx_s,_,_=calc_adx(df15); adx_v=float(adx_s.iloc[-1])
        rsi_v=float(calc_rsi(df5).iloc[-1]); mc,ms=calc_macd(df5)
        macd_ab=float(mc.iloc[-1])>float(ms.iloc[-1]); macd_bl=float(mc.iloc[-1])<float(ms.iloc[-1])
        buy_s=sum([t15==1,d5==1,close5>vwap_v,macd_ab,rsi_v<70,adx_v>20])
        sel_s=sum([t15==-1,d5==-1,close5<vwap_v,macd_bl,rsi_v>30,adx_v>20])
        signal="BUY" if buy_s>=5 else ("SELL" if sel_s>=5 else None)
        return {"signal":signal,"close":close5,"adx":adx_v,"rsi":rsi_v,
                "buy_score":buy_s,"sell_score":sel_s}
    except Exception as e:
        logger.error(f"Signal [{symbol}]: {e}")
        return None

# ── Scanner ───────────────────────────────────────────────────────────────────
async def scan_and_alert(app):
    if not is_trading_time(): return
    if not angel_ready:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
            text=f"BOT2 STOCKS: Not connected\n{angel_error}\nSend /reconnect2"); return

    vix_val      = get_ltp(VIX_TOKEN,"NSE",VIX_SYMBOL) or 0.0
    hv           = vix_val > VIX_HIGH
    stock_expiry = last_expiry_month(3)
    done         = set()

    for sym, info in FO_STOCKS.items():
        if sym in done: continue
        result = get_signal(info["token"], info["exchange"], sym)
        if not result or not result["signal"]: continue

        ltp    = result["close"]
        signal = result["signal"]
        qty    = get_stock_qty(ltp)
        sl_pct = 0.015 if hv else 0.01
        sl_pts = round(ltp*sl_pct, 2)
        sl_p   = round(ltp-sl_pts if signal=="BUY" else ltp+sl_pts, 2)
        tgt_p  = round(ltp+sl_pts*TARGET_MULT if signal=="BUY" else ltp-sl_pts*TARGET_MULT, 2)
        score  = result["buy_score"] if signal=="BUY" else result["sell_score"]
        done.add(sym)

        # ── Equity leg ──────────────────────────────────────────────────────
        eq_leg = {"token":info["token"],"symbol":sym,"exchange":info["exchange"],
                  "action":signal,"ltp":ltp,"sl":sl_p,"target":tgt_p,
                  "quantity":qty,"trailing":False,"type":"EQUITY"}

        # ── Stock option leg — always with equity signal ─────────────────────
        opt_legs = []
        for gap in [5,10,25,50,100,250,500]:
            atm_s = round(ltp/gap)*gap
            if abs(atm_s-ltp)/ltp < 0.05:
                break
        opt_type = "CE" if signal=="BUY" else "PE"
        tok_s, sym_s, lot_s = get_stock_option_token(sym, atm_s, opt_type, stock_expiry)
        if tok_s:
            opt_ltp = get_ltp(tok_s,"NFO",sym_s) or 0.0
            if opt_ltp > 0:
                sl_o_pct = 0.35 if hv else 0.30
                sl_o     = round(opt_ltp*sl_o_pct, 2)
                tgt_o    = round(sl_o*TARGET_MULT, 2)
                opt_legs.append({
                    "token":tok_s,"symbol":sym_s,"exchange":"NFO",
                    "action":"BUY","ltp":opt_ltp,
                    "sl":round(opt_ltp-sl_o,2),"target":round(opt_ltp+tgt_o,2),
                    "quantity":lot_s,"trailing":False,"type":"STOCK_OPT"
                })

        # ── Send equity signal ───────────────────────────────────────────────
        eq_key = f"EQ_{sym}_{signal}_{int(now_ist().timestamp())}"
        pending_signals[eq_key] = {"symbol":sym,"legs":[eq_leg],"signal":signal}
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Approve",callback_data=f"approve_{eq_key}"),
            InlineKeyboardButton("Reject", callback_data=f"reject_{eq_key}")
        ]])
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,parse_mode="Markdown",reply_markup=kb,
            text=f"*BOT2 STOCKS | {signal} - {sym}* (EQUITY)\nLTP:Rs{ltp:.2f} Qty:{qty}\nSL:Rs{sl_p:.2f} ({int(sl_pct*100)}%) T:Rs{tgt_p:.2f}\nADX:{result['adx']:.1f} RSI:{result['rsi']:.1f} Score:{score}/6\n\nTap Approve to paper trade")
        await asyncio.sleep(0.3)

        # ── Send stock option signal ─────────────────────────────────────────
        if opt_legs:
            opt = opt_legs[0]
            opt_key = f"STKOPT_{sym}_{signal}_{int(now_ist().timestamp())}"
            pending_signals[opt_key] = {"symbol":sym,"legs":opt_legs,"signal":signal}
            kb2 = InlineKeyboardMarkup([[
                InlineKeyboardButton("Approve",callback_data=f"approve_{opt_key}"),
                InlineKeyboardButton("Reject", callback_data=f"reject_{opt_key}")
            ]])
            await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,parse_mode="Markdown",reply_markup=kb2,
                text=f"*BOT2 STOCKS | {signal} - {sym}* (STOCK OPTION)\n{opt['symbol']}\nLTP:Rs{opt['ltp']:.2f} Qty:{opt['quantity']}\nSL:Rs{opt['sl']:.2f} (30%) T:Rs{opt['target']:.2f}\nExpiry:{stock_expiry.strftime('%d %b %Y')}\n\nTap Approve to paper trade")
            await asyncio.sleep(0.3)

# ── Monitor ───────────────────────────────────────────────────────────────────
async def monitor_positions(app, positions, is_paper=True):
    if not positions or not angel_ready: return
    to_close=[]
    for key,trade in list(positions.items()):
        for leg in trade.get("legs",[]):
            ltp=get_ltp(leg["token"],leg["exchange"],leg["symbol"])
            if ltp is None: continue
            entry=leg["ltp"]; sl=leg["sl"]; tgt=leg["target"]; action=leg["action"]; qty=leg["quantity"]
            pnl=(ltp-entry)*qty if action=="BUY" else (entry-ltp)*qty
            sl_hit=(action=="BUY" and ltp<=sl) or (action=="SELL" and ltp>=sl)
            tgt_hit=(action=="BUY" and ltp>=tgt) or (action=="SELL" and ltp<=tgt)
            if sl_hit: to_close.append((key,"SL_HIT",ltp,pnl,leg))
            elif tgt_hit and not leg.get("trailing"):
                leg["trailing"]=True; leg["sl"]=entry
                new_tgt=round(ltp+(tgt-entry) if action=="BUY" else ltp-(entry-tgt),2); leg["target"]=new_tgt
                tag="PAPER: " if is_paper else ""
                await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,parse_mode="Markdown",
                    text=f"*BOT2 {tag}Target Hit - Trailing!*\n{leg['symbol']}\nLTP:Rs{ltp:.2f} SL->Rs{entry:.2f} T:Rs{new_tgt:.2f} P&L:Rs{pnl:.2f}")
            elif tgt_hit and leg.get("trailing"): to_close.append((key,"TARGET_HIT",ltp,pnl,leg))
    for key,reason,exit_price,pnl,leg in to_close:
        positions.pop(key,None)
        if not is_paper:
            exit_txn="SELL" if leg["action"]=="BUY" else "BUY"
            place_order(leg["token"],leg["exchange"],leg["symbol"],exit_txn,leg["quantity"])
        tag="PAPER " if is_paper else ""
        outcome="Target Hit" if reason=="TARGET_HIT" else "SL Hit"
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,parse_mode="Markdown",
            text=f"*BOT2 {tag}Closed - {outcome}*\n{leg['symbol']}\nEntry:Rs{leg['ltp']:.2f} Exit:Rs{exit_price:.2f} P&L:Rs{pnl:.2f}")
        record={"symbol":leg["symbol"],"action":leg["action"],"entry":leg["ltp"],"exit":exit_price,"pnl":pnl,"reason":reason}
        (paper_trades if is_paper else daily_trades).append(record)

async def square_off_all(app):
    global squaredoff_today
    if squaredoff_today: return
    squaredoff_today=True
    for positions,is_paper,label in [(paper_positions,True,"PAPER"),(active_positions,False,"REAL")]:
        if not positions: continue
        total=0.0
        for key,trade in list(positions.items()):
            for leg in trade.get("legs",[]):
                ltp=get_ltp(leg["token"],leg["exchange"],leg["symbol"]) or leg["ltp"]
                if not is_paper:
                    exit_txn="SELL" if leg["action"]=="BUY" else "BUY"
                    place_order(leg["token"],leg["exchange"],leg["symbol"],exit_txn,leg["quantity"])
                pnl=(ltp-leg["ltp"])*leg["quantity"] if leg["action"]=="BUY" else (leg["ltp"]-ltp)*leg["quantity"]
                total+=pnl
                (paper_trades if is_paper else daily_trades).append({"symbol":leg["symbol"],"action":leg["action"],"entry":leg["ltp"],"exit":ltp,"pnl":pnl,"reason":"SQUAREOFF"})
            positions.pop(key,None)
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,parse_mode="Markdown",
            text=f"*BOT2 {label} Square-Off*\nP&L: Rs{total:.2f}")

async def send_pnl(target, is_update=True):
    lines=[f"*BOT2 FO STOCKS P&L - {now_ist().strftime('%I:%M %p')}*\n"]
    for positions,label in [(paper_positions,"Paper"),(active_positions,"Real")]:
        if not positions: continue
        total=0.0; lines.append(f"*{label}:*")
        for key,trade in positions.items():
            for leg in trade.get("legs",[]):
                ltp=get_ltp(leg["token"],leg["exchange"],leg["symbol"]) or leg["ltp"]
                pnl=(ltp-leg["ltp"])*leg["quantity"] if leg["action"]=="BUY" else (leg["ltp"]-ltp)*leg["quantity"]
                total+=pnl; lines.append(f"{leg['symbol']} Rs{ltp:.2f} P&L:Rs{pnl:.2f}")
        lines.append(f"Total: Rs{total:.2f}\n")
    if not paper_positions and not active_positions: lines.append("No open positions.")
    msg="\n".join(lines)
    if is_update: await target.message.reply_text(msg,parse_mode="Markdown")
    else: await target.bot.send_message(chat_id=TELEGRAM_CHAT_ID,text=msg,parse_mode="Markdown")

# ── Commands ──────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    expiry=last_expiry_month(3)
    a_status="Connected" if angel_ready else f"Not Connected - {angel_error}"
    await update.message.reply_text(
        f"*BOT 2 - FO Stocks Bot*\nMode: PAPER\n\n"
        f"Stocks: {len(FO_STOCKS)} F&O eligible\n"
        f"Equity: Every day if score 5/6\n"
        f"Stock Options: Same time as equity signal\n"
        f"Monthly Expiry: {expiry.strftime('%d %b %Y')}\n"
        f"Angel One: {a_status}\n\n"
        f"/scan2 /pnl2 /status2 /reconnect2",
        parse_mode="Markdown")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    a_status="Connected" if angel_ready else f"Not Connected\n{angel_error}"
    expiry=last_expiry_month(3); dte=(expiry-date_ist()).days
    await update.message.reply_text(
        f"*BOT2 Status*\n"
        f"Time: {now_ist().strftime('%I:%M %p')} | {'Open' if is_trading_time() else 'Closed'}\n"
        f"Angel: {a_status}\n"
        f"Monthly Expiry: {expiry.strftime('%d %b %Y')} | DTE:{dte}\n"
        f"F&O Stocks: {len(FO_STOCKS)}\n"
        f"Paper:{len(paper_positions)} Pending:{len(pending_signals)}",
        parse_mode="Markdown")

async def cmd_reconnect(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global angel_ready, angel_error
    angel_ready=False; angel_error="Reconnecting..."
    await update.message.reply_text("BOT2: Reconnecting Angel One...")
    success = await asyncio.get_event_loop().run_in_executor(None, connect_angel)
    if success: await update.message.reply_text("BOT2: Angel One connected!")
    else: await update.message.reply_text(f"BOT2: Failed!\n{angel_error}")

async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not angel_ready:
        await update.message.reply_text(f"BOT2: Not connected\n{angel_error}"); return
    if not is_trading_time():
        await update.message.reply_text("BOT2: Market closed."); return
    await update.message.reply_text(f"BOT2: Scanning {len(FO_STOCKS)} F&O stocks...")
    await scan_and_alert(ctx.application)
    await update.message.reply_text("BOT2: Scan complete!")

async def cmd_pnl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await send_pnl(update,is_update=True)

async def handle_approval(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query=update.callback_query; await query.answer(); data=query.data
    if data.startswith("approve_"):
        key=data.replace("approve_","")
        if key not in pending_signals:
            await query.edit_message_text("Signal expired."); return
        trade=pending_signals.pop(key); legs=trade.get("legs",[]); success=True; order_ids=[]
        for leg in legs:
            resp=place_order(leg["token"],leg["exchange"],leg["symbol"],leg["action"],leg["quantity"])
            if resp and resp.get("status"): order_ids.append(resp.get("data",{}).get("orderid","N/A"))
            else: success=False; break
        if success:
            is_paper=any(str(oid).startswith("PAPER-") for oid in order_ids)
            (paper_positions if is_paper else active_positions)[key]=trade
            legs_text="\n".join([f"{l['action']} {l['symbol']} Entry:Rs{l['ltp']:.2f} SL:Rs{l['sl']:.2f} T:Rs{l['target']:.2f}" for l in legs])
            await query.edit_message_text(f"*BOT2 {'Paper' if is_paper else 'Real'} Trade Active!*\n\n{legs_text}\n\nMonitoring SL and Target.",parse_mode="Markdown")
        else: await query.edit_message_text("BOT2: Order failed.")
    elif data.startswith("reject_"):
        pending_signals.pop(data.replace("reject_",""),None)
        await query.edit_message_text("Signal rejected.")

# ── Jobs ──────────────────────────────────────────────────────────────────────
async def job_scan(ctx: ContextTypes.DEFAULT_TYPE):
    await scan_and_alert(ctx.application)

async def job_monitor(ctx: ContextTypes.DEFAULT_TYPE):
    await monitor_positions(ctx.application,paper_positions,is_paper=True)
    await monitor_positions(ctx.application,active_positions,is_paper=False)

async def job_pnl(ctx: ContextTypes.DEFAULT_TYPE):
    if is_trading_time(): await send_pnl(ctx.application,is_update=False)

async def job_squareoff(ctx: ContextTypes.DEFAULT_TYPE):
    if time_ist()>=SQUAREOFF_TIME and (active_positions or paper_positions):
        await square_off_all(ctx.application)

async def job_reconnect(ctx: ContextTypes.DEFAULT_TYPE):
    if not angel_ready:
        await asyncio.get_event_loop().run_in_executor(None, connect_angel)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    logger.info("BOT2 FO STOCKS starting...")
    connect_angel()
    logger.info(f"Angel status: {angel_ready} | {angel_error}")

    import threading
    t = threading.Thread(target=load_instruments, daemon=True)
    t.start()

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    for cmd,fn in [
        ("start2",     cmd_start),
        ("status2",    cmd_status),
        ("reconnect2", cmd_reconnect),
        ("scan2",      cmd_scan),
        ("pnl2",       cmd_pnl),
    ]:
        app.add_handler(CommandHandler(cmd,fn))
    app.add_handler(CallbackQueryHandler(handle_approval))

    jq=app.job_queue
    jq.run_repeating(job_scan,      interval=300,first=60)
    jq.run_repeating(job_monitor,   interval=30, first=90)
    jq.run_repeating(job_pnl,       interval=1800,first=120)
    jq.run_repeating(job_squareoff, interval=60, first=60)
    jq.run_repeating(job_reconnect, interval=300,first=300)

    logger.info("BOT2 polling started!")
    app.run_polling(allowed_updates=["message","callback_query"])

if __name__ == "__main__":
    main()
