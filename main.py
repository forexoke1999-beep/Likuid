#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""XAUUSD CFD Bybit bot — Shadow Fakeout + Trend Pullback + multi-timeframe /rencana.
Standard-library only. Uses closed Bybit linear candles; all historical results are simulations.
Live signals use M5 candles; historical /bt remains M15 and stops/targets/BE+/SL+ are evaluated against M1 candles for SL/TP/BE+/SL+ timing; recent structural break entries require a retest before another same-direction break; not a profit guarantee.
Commands: /status, /scan, /rencana [TF], /bt [days], /offset <number>,
/capital, /rekap, /be [on|off], /ichi [scan]. Paper capital + staged BE/SL+ tracker; no live orders.
"""
import os, sys, json, time, math, re, struct, zlib, threading, bisect
import html as _html
import random, traceback
from dataclasses import dataclass, asdict, field, fields as _dc_fields
from typing import Any, Optional
import urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

# Read plain KEY=VALUE .env without overriding variables already exported in shell.
def _load_simple_env(path='.env'):
    try:
        with open(path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and v and k not in os.environ:
                    os.environ[k] = v
    except OSError:
        pass
_load_simple_env()

HOSTS = [h.strip().rstrip('/') for h in os.getenv('BYBIT_HOSTS','https://api.bybit.id,https://api.bybit.com').split(',') if h.strip()]
SYMBOLS = [s.strip().upper() for s in os.getenv('SYMBOL','XAUTUSDT,XAUUSDT').split(',') if s.strip()]
TG_TOKEN = os.getenv('TG_TOKEN','').strip().strip('"').strip("'")
TG_CHAT = os.getenv('TG_CHAT_ID','').strip()
TG_TOPIC = os.getenv('TG_TOPIC_ID','').strip()
INTERVAL = 15
MIN_SCORE = int(os.getenv('MIN_SCORE','2'))
# Additional anti-chop filters for trend-continuation engines. Fakeout stays separate.
TREND_MIN_ADX = max(0.0, float(os.getenv('TREND_MIN_ADX','16.0')))
TREND_MIN_EMA_GAP_ATR = max(0.0, float(os.getenv('TREND_MIN_EMA_GAP_ATR','0.08')))
TREND_SLOPE_BARS = max(1, int(os.getenv('TREND_SLOPE_BARS','4')))
TREND_REQUIRE_BREAK = os.getenv('TREND_REQUIRE_BREAK','1') != '0'
# Structure-break engine: intentionally separate from ADX trend filter.
STRUCTURE_LOOKBACK = max(3, int(os.getenv('STRUCTURE_LOOKBACK','4')))
STRUCTURE_MIN_BODY_RATIO = min(0.9, max(0.25, float(os.getenv('STRUCTURE_MIN_BODY_RATIO','0.45'))))
STRUCTURE_MIN_BODY_ATR = max(0.2, float(os.getenv('STRUCTURE_MIN_BODY_ATR','0.65')))
STRUCTURE_MIN_RVOL = max(0.5, float(os.getenv('STRUCTURE_MIN_RVOL','1.10')))
STRUCTURE_MAX_EXTENSION_ATR = max(0.5, float(os.getenv('STRUCTURE_MAX_EXTENSION_ATR','1.50')))
STRUCTURE_MIN_SL_ATR = max(0.25, float(os.getenv('STRUCTURE_MIN_SL_ATR','0.65')))
STRUCTURE_RETEST_WINDOW = max(2, int(os.getenv('STRUCTURE_RETEST_WINDOW','8')))
STRUCTURE_RETEST_TOL_ATR = max(0.02, float(os.getenv('STRUCTURE_RETEST_TOL_ATR','0.15')))
RR = float(os.getenv('RR','2.0'))
SPREAD = float(os.getenv('SPREAD','0.30'))
ICHI_MODE = os.getenv('ICHI_MODE','off').lower()
CONFIRM = os.getenv('CONFIRM','retest').lower()
MAX_BARS = int(os.getenv('MAX_BARS','96'))
MAX_RPS = max(0.2, float(os.getenv('MAX_RPS','8')))
SCAN_DELAY = float(os.getenv('SCAN_DELAY','2'))
TRACK_SEC = float(os.getenv('TRACK_SEC','10'))
# Paper capital management ported from bybit_combo_bot.py. This bot remains signal/paper only.
CAPITAL_START_IDR = max(0.0, float(os.getenv('CAPITAL_START_IDR','1000000')))
CAPITAL_TOPUP_IDR = max(0.0, float(os.getenv('CAPITAL_TOPUP_IDR','500000')))
TRADE_LOT = max(0.0001, float(os.getenv('TRADE_LOT','0.01')))
USD_IDR = max(1.0, float(os.getenv('USD_IDR','16000')))
XAU_UNITS_PER_LOT = max(0.0001, float(os.getenv('XAU_UNITS_PER_LOT','100')))
PAPER_COST_XAU = max(0.0, float(os.getenv('PAPER_COST_XAU',str(SPREAD))))  # default follows historical spread assumption
BE_ENABLED = os.getenv('BE_ENABLED','1') != '0'
BE_TRIGGER_R = max(0.0, float(os.getenv('BE_TRIGGER_R','0.80')))
BE_OFFSET_R = max(0.0, float(os.getenv('BE_OFFSET_R','0.05')))
SLPLUS_TRIGGER_R = max(BE_TRIGGER_R, float(os.getenv('SLPLUS_TRIGGER_R','1.20')))
SLPLUS_LOCK_R = max(0.0, float(os.getenv('SLPLUS_LOCK_R','0.30')))
PAPER_TICK = max(0.000001, float(os.getenv('PAPER_TICK','0.01')))
STATE_FILE = os.getenv('STATE_FILE','xauusd_cfd_state.json')   # di Railway arahkan ke volume, mis. /data/xauusd_cfd_state.json
CAPITAL_FILE = os.getenv('CAPITAL_FILE','xauusd_cfd_capital.json')
# Ichimoku 4H otomatis (paper/alert saja, tanpa order live). Hasil riset: edge tipis, belum teruji live.
ICHI_ENABLE = os.getenv('ICHI_ENABLE','1') != '0'
ICHI_TF = '4h'
ICHI_MS = 14_400_000
ICHI_RISK_PCT = min(0.05, max(0.001, float(os.getenv('ICHI_RISK_PCT','0.01'))))
ICHI_SIDES = os.getenv('ICHI_SIDES','both').lower()      # both | long | short
ICHI_MAX_SL_PCT = max(0.005, float(os.getenv('ICHI_MAX_SL_PCT','0.10')))
ICHI_CAPITAL_FILE = os.getenv('ICHI_CAPITAL_FILE','xauusd_ichi_capital.json')
LEGACY_ENABLE = os.getenv('LEGACY_ENABLE','1') != '0'   # 0 = matikan sinyal engine awal; Scalper dan Ichimoku tetap jalan
WIB = timezone(timedelta(hours=7))
LOCK = threading.RLock()
STATE = {'offset': float(os.getenv('OFFSET','0')), 'active': [], 'last_ts': 0, 'tp': 0, 'sl': 0, 'tg_off': 0}
SYMBOL = None
_host_ok = None
_rl_lock = threading.Lock()
_rl_last = 0.0
_srv_off = 0

# Bybit V5 interval mapping. 1M is calendar month (not 1 minute).
TF = {
    '1m':('1',60_000,'1 menit'), '3m':('3',180_000,'3 menit'), '5m':('5',300_000,'5 menit'),
    '15m':('15',900_000,'15 menit'), '30m':('30',1_800_000,'30 menit'), '1h':('60',3_600_000,'1 jam'),
    '2h':('120',7_200_000,'2 jam'), '4h':('240',14_400_000,'4 jam'), '6h':('360',21_600_000,'6 jam'),
    '12h':('720',43_200_000,'12 jam'), '1d':('D',86_400_000,'1 hari'), '1w':('W',604_800_000,'1 minggu'),
    '1mo':('M',None,'1 bulan'),
}
TF_NATIVE = {v[0]: k for k,v in TF.items()}
TF_ALIASES = {'1min':'1m','3min':'3m','5min':'5m','15min':'15m','30min':'30m','60m':'1h','1hr':'1h','2hr':'2h','4hr':'4h','6hr':'6h','12hr':'12h','1day':'1d','1wk':'1w','1month':'1mo','1M':'1mo'}


def load_state():
    global BE_ENABLED
    try:
        with open(STATE_FILE, encoding='utf-8') as f: STATE.update(json.load(f))
    except Exception: pass
    try: STATE['offset'] = float(STATE.get('offset', 0.0))
    except Exception: STATE['offset'] = float(os.getenv('OFFSET','0'))
    if not isinstance(STATE.get('active'), list): STATE['active'] = []
    if not isinstance(STATE.get('seen_signals'), list): STATE['seen_signals'] = []
    # Preserve /be on|off across restarts; .env is used if no saved choice exists.
    if 'be_enabled' in STATE: BE_ENABLED = bool(STATE['be_enabled'])
    # Migrate active paper trades created by earlier bot versions.
    for a in STATE['active']:
        if not isinstance(a, dict): continue
        a.setdefault('initial_sl', a.get('sl', a.get('entry', 0.0)))
        a.setdefault('active_sl', a.get('sl', a.get('initial_sl', a.get('entry', 0.0))))
        a.setdefault('be_stage', 0)
        a.setdefault('stop_mode', 'INITIAL')
        a.setdefault('bars_held', 0)
        a.setdefault('lot', TRADE_LOT)
        a.setdefault('id', f"{a.get('sig_t', 0)}|{a.get('kind', 'XAU')}|{a.get('side', 'L')}")
        # Signal timestamp is the candle OPEN; do not manage the signal candle itself.
        a.setdefault('last_checked_ts', int(a.get('sig_t', 0)) + 900000 - 60000)


def save_state():
    try:
        with LOCK:
            tmp = STATE_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f: json.dump(STATE, f)
            os.replace(tmp, STATE_FILE)
    except Exception as e: print('save_state:', e)


def now_ms(): return int(time.time()*1000) + _srv_off


def _rp(value):
    return ('Rp ' + f"{int(round(float(value))):,}").replace(',', '.')


class CapitalTracker:
    """Realized paper P/L tracker, ported from bybit_combo_bot.py; never places orders."""
    def __init__(self, path):
        self.path = path
        self.data = {
            'balance': float(CAPITAL_START_IDR), 'initial_capital': float(CAPITAL_START_IDR),
            'realized_pnl_idr': 0.0, 'topups_idr': 0.0, 'topup_count': 0,
            'entries': 0, 'closed': 0, 'tp': 0, 'slp': 0, 'be': 0, 'sl': 0,
            'timeout': 0, 'win': 0, 'loss': 0, 'events': []
        }
        try:
            with open(path, encoding='utf-8') as f:
                saved = json.load(f)
            if isinstance(saved, dict): self.data.update(saved)
        except Exception: pass
        # Older capital files may not contain the newer outcome counters.
        for key in ('entries','closed','tp','slp','be','sl','timeout','win','loss','topup_count'):
            self.data.setdefault(key, 0)
        self.data.setdefault('events', [])

    def save(self):
        try:
            with LOCK:
                tmp = self.path + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as f: json.dump(self.data, f, indent=2)
                os.replace(tmp, self.path)
        except Exception as e: print('capital save:', e)

    def register_entry(self):
        with LOCK: self.data['entries'] = int(self.data.get('entries', 0)) + 1
        self.save()

    def settle(self, trade, status, exit_px, exit_ts):
        trade_id = str(trade.get('id') or f"{trade.get('sig_t')}|{trade.get('kind')}|{trade.get('side')}")
        with LOCK:
            if any(str(e.get('id')) == trade_id for e in self.data.get('events', [])):
                return None
            entry = float(trade['entry']); exit_px = float(exit_px)
            is_long = trade.get('side') == 'L'
            gross = (exit_px-entry) if is_long else (entry-exit_px)
            net_per_unit = gross - PAPER_COST_XAU
            lot = float(trade.get('lot', TRADE_LOT))
            units = lot * XAU_UNITS_PER_LOT
            pnl_usd = net_per_unit * units
            pnl_idr = round(pnl_usd * USD_IDR, 2)
            before = float(self.data.get('balance', CAPITAL_START_IDR))
            after = before + pnl_idr
            topup = 0.0
            # Same behavior as the combo bot: auto top-up only if paper balance reaches/breaches zero.
            if after <= 0 and CAPITAL_TOPUP_IDR > 0:
                topup = float(CAPITAL_TOPUP_IDR)
                after += topup
                self.data['topups_idr'] = float(self.data.get('topups_idr', 0.0)) + topup
                self.data['topup_count'] = int(self.data.get('topup_count', 0)) + 1
            initial_risk = abs(entry-float(trade.get('initial_sl', trade.get('sl', entry))))
            r_mult = net_per_unit / initial_risk if initial_risk > 0 else 0.0
            stage = int(trade.get('be_stage', 0) or 0)
            tag = status
            if status == 'SL':
                tag = 'SL+' if stage >= 2 else ('BE+' if stage == 1 else 'SL')
            self.data['balance'] = round(after, 2)
            self.data['realized_pnl_idr'] = round(float(self.data.get('realized_pnl_idr',0.0)) + pnl_idr, 2)
            self.data['closed'] = int(self.data.get('closed',0)) + 1
            if tag == 'TP': self.data['tp'] = int(self.data.get('tp',0)) + 1
            elif tag == 'SL+': self.data['slp'] = int(self.data.get('slp',0)) + 1
            elif tag == 'BE+': self.data['be'] = int(self.data.get('be',0)) + 1
            elif tag == 'SL': self.data['sl'] = int(self.data.get('sl',0)) + 1
            elif status == 'TIMEOUT': self.data['timeout'] = int(self.data.get('timeout',0)) + 1
            if pnl_idr > 0: self.data['win'] = int(self.data.get('win',0)) + 1
            elif pnl_idr < 0: self.data['loss'] = int(self.data.get('loss',0)) + 1
            ev = {'id':trade_id,'ts':exit_ts,'status':status,'tag':tag,'be_stage':stage,
                  'side':'BUY' if is_long else 'SELL','kind':trade.get('kind','?'),
                  'entry':entry,'exit':exit_px,'lot':lot,'r':round(r_mult,3),
                  'pnl_usd':round(pnl_usd,4),'pnl_idr':pnl_idr,
                  'balance_before':before,'balance_after':after,'topup_idr':topup}
            self.data.setdefault('events', []).append(ev)
            self.data['events'] = self.data['events'][-2000:]
        self.save()
        return ev

    def summary_text(self):
        with LOCK:
            d = json.loads(json.dumps(self.data))
        events = d.get('events', [])
        tp = sum(e.get('tag') == 'TP' for e in events)
        slp = sum(e.get('tag') == 'SL+' for e in events)
        be = sum(e.get('tag') == 'BE+' for e in events)
        sl = sum(e.get('tag') == 'SL' for e in events)
        to = sum(e.get('status') == 'TIMEOUT' for e in events)
        wins = sum(float(e.get('pnl_idr',0)) > 0 for e in events)
        losses = sum(float(e.get('pnl_idr',0)) < 0 for e in events)
        n = len(events); wr = wins/n*100 if n else 0.0
        return (f'<b>MODAL PAPER XAUUSD</b>\n'
                f"Modal awal : {_rp(d.get('initial_capital', CAPITAL_START_IDR))}\n"
                f"Saldo kini : {_rp(d.get('balance', CAPITAL_START_IDR))}\n"
                f"Realized P/L: {_rp(d.get('realized_pnl_idr',0))}\n"
                f"Trade selesai {n} | TP {tp} | SL+ {slp} | BE+ {be} | SL {sl} | Timeout {to}\n"
                f"Win P/L bersih: {wr:.1f}% ({wins} untung / {losses} rugi)\n"
                f"Sinyal terdaftar: {int(d.get('entries',0))} | Auto top-up: {_rp(d.get('topups_idr',0))} ({int(d.get('topup_count',0))}x)\n"
                f"Lot paper {TRADE_LOT:g} | Unit/lot {XAU_UNITS_PER_LOT:g} oz | USD/IDR {USD_IDR:g} | biaya {PAPER_COST_XAU:g} USD/oz.\n"
                f"Ini pembukuan simulasi, bukan saldo akun trading.")

    def recap_text(self, days=7):
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))
        with LOCK: evs = list(self.data.get('events', []))
        chosen=[]
        for e in evs:
            try:
                ts = datetime.fromisoformat(str(e.get('ts','')).replace('Z','+00:00'))
                if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff: chosen.append(e)
            except Exception: pass
        pnl=sum(float(e.get('pnl_idr',0) or 0) for e in chosen)
        wins=sum(float(e.get('pnl_idr',0) or 0)>0 for e in chosen)
        losses=sum(float(e.get('pnl_idr',0) or 0)<0 for e in chosen)
        tp=sum(e.get('tag')=='TP' for e in chosen); slp=sum(e.get('tag')=='SL+' for e in chosen)
        be=sum(e.get('tag')=='BE+' for e in chosen); sl=sum(e.get('tag')=='SL' for e in chosen)
        return (f'<b>REKAP PAPER XAUUSD — {max(1,int(days))} HARI</b>\n'
                f"Trade selesai {len(chosen)} | TP {tp} | SL+ {slp} | BE+ {be} | SL {sl}\n"
                f"P/L realized {_rp(pnl)} | {wins} untung / {losses} rugi\n"
                f"Saldo sekarang {_rp(self.data.get('balance', CAPITAL_START_IDR))}\n"
                f"Catatan: rekaman paper tracker, bukan order live.")


CAPITAL = CapitalTracker(CAPITAL_FILE)


def _throttle():
    global _rl_last
    with _rl_lock:
        wait = _rl_last + 1.0/MAX_RPS - time.monotonic()
        if wait > 0: time.sleep(wait)
        _rl_last = time.monotonic()


def api(path, params=None, timeout=15, tries=3):
    global _host_ok, _srv_off
    q = urllib.parse.urlencode(params or {})
    hosts = ([_host_ok] + [h for h in HOSTS if h != _host_ok]) if _host_ok else HOSTS[:]
    last = 'Tidak ada host Bybit yang dikonfigurasi'
    for attempt in range(tries):
        for host in hosts:
            _throttle()
            try:
                req = urllib.request.Request(f'{host}{path}?{q}', headers={'User-Agent':'Mozilla/5.0 xauusd-bot'})
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    data = json.loads(r.read().decode('utf-8'))
                if data.get('time'):
                    _srv_off = int(data['time']) - int(time.time()*1000)
                if data.get('retCode') == 0:
                    _host_ok = host
                    return data.get('result', {})
                last = f'{host}: retCode {data.get("retCode")} {data.get("retMsg")}'
            except urllib.error.HTTPError as e: last = f'{host}: HTTP {e.code}'
            except Exception as e: last = f'{host}: {e}'
        time.sleep(min(5.0, 0.8*(attempt+1)))
    raise RuntimeError(last)


def resolve_symbol():
    global SYMBOL
    if SYMBOL: return SYMBOL
    errors=[]
    for s in SYMBOLS:
        try:
            r=api('/v5/market/tickers', {'category':'linear','symbol':s})
            if r.get('list'): SYMBOL=s; return s
        except Exception as e: errors.append(str(e))
    raise RuntimeError('Symbol tidak ditemukan: '+' | '.join(errors))


def _parse_tf(raw):
    raw = (raw or '15m').strip()
    key = TF_ALIASES.get(raw, TF_ALIASES.get(raw.lower(), raw.lower()))
    if key not in TF:
        raise ValueError('Timeframe tidak valid. Gunakan: '+', '.join(TF.keys()))
    return key, TF[key]


def _bar_closed(ts, key, now=None):
    now = now_ms() if now is None else now
    ms = TF[key][1]
    if ms is not None: return ts + ms <= now
    dt = datetime.fromtimestamp(ts/1000, timezone.utc)
    y = dt.year + (1 if dt.month == 12 else 0); m = 1 if dt.month == 12 else dt.month+1
    end = datetime(y, m, 1, tzinfo=timezone.utc)
    return int(end.timestamp()*1000) <= now


def candle_close_ms(ts, tf_key='15m'):
    """Convert Bybit candle startTime to its actual close time for display only."""
    ms = TF[tf_key][1]
    if ms is not None:
        return int(ts) + ms
    dt = datetime.fromtimestamp(int(ts)/1000, timezone.utc)
    year = dt.year + (1 if dt.month == 12 else 0)
    month = 1 if dt.month == 12 else dt.month + 1
    return int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp()*1000)


def fetch_klines_tf(symbol, tf='15m', limit=300):
    key, (interval, _, _) = _parse_tf(tf)
    raw = api('/v5/market/kline', {'category':'linear','symbol':symbol,'interval':interval,'limit':min(1000,max(80,int(limit)))}, timeout=20).get('list',[])
    rows = sorted((int(r[0],),) + tuple(float(x) for x in r[1:6]) for r in raw)
    now=now_ms()
    return [r for r in rows if _bar_closed(r[0], key, now)]


def fetch_recent(symbol, n=300, tf='15m'): return fetch_klines_tf(symbol, tf, n)


def fetch_klines_history_tf(symbol, tf='1m', target=10000):
    """Fetch a paginated closed-candle history, newest window, sorted oldest->newest."""
    key, (interval, _, _) = _parse_tf(tf)
    target=max(80,int(target)); raw=[]; before=None
    pages=0
    while len(raw)<target and pages < max(2, int(math.ceil(target/1000))+3):
        limit=min(1000,target-len(raw))
        params={'category':'linear','symbol':symbol,'interval':interval,'limit':limit}
        if before is not None: params['end']=before-1
        pack=api('/v5/market/kline',params,timeout=20).get('list',[])
        if not pack: break
        rows=sorted((int(r[0]),)+tuple(float(x) for x in r[1:6]) for r in pack)
        new=[r for r in rows if not raw or r[0]<raw[0][0]]
        if not new: break
        raw=new+raw; before=raw[0][0]; pages+=1
        if len(pack)<limit: break
    now=now_ms()
    closed=[r for r in raw if _bar_closed(r[0],key,now)]
    return closed[-target:]


def fetch_oi_recent(symbol, n=24):
    try:
        rows=api('/v5/market/open-interest', {'category':'linear','symbol':symbol,'intervalTime':'15min','limit':n}).get('list',[])
        return {int(r['timestamp']):float(r['openInterest']) for r in rows}
    except Exception as e:
        print('OI gagal:',e); return {}


def trades_delta(symbol):
    try:
        trades=api('/v5/market/recent-trade',{'category':'linear','symbol':symbol,'limit':500}).get('list',[])
        b=sum(float(x['size']) for x in trades if x.get('side')=='Buy')
        s=sum(float(x['size']) for x in trades if x.get('side')=='Sell')
        return (b-s)/(b+s) if b+s else None
    except Exception as e: print('delta trade gagal:',e); return None


def book_imbalance(symbol):
    try:
        ob=api('/v5/market/orderbook',{'category':'linear','symbol':symbol,'limit':50})
        b=sum(float(x[1]) for x in ob.get('b',[])); a=sum(float(x[1]) for x in ob.get('a',[]))
        return (b-a)/(b+a) if b+a else None
    except Exception as e: print('orderbook gagal:',e); return None


def _ema(vals, period):
    if not vals: return []
    alpha=2/(period+1); out=[float(vals[0])]
    for x in vals[1:]: out.append(float(x)*alpha+out[-1]*(1-alpha))
    return out


def _rolling_mean(vals, p):
    n=len(vals); out=[None]*n; s=0.0
    for i,x in enumerate(vals):
        s += float(x)
        if i>=p: s -= float(vals[i-p])
        if i>=p-1: out[i]=s/p
    return out


def _rolling_std(vals,p,means):
    out=[None]*len(vals)
    for i in range(p-1,len(vals)):
        m=means[i]
        if m is not None: out[i]=math.sqrt(sum((vals[j]-m)**2 for j in range(i-p+1,i+1))/p)
    return out


def build(rows, oi=None):
    if not rows: raise ValueError('Data candle kosong')
    D={k:[r[i] for r in rows] for i,k in enumerate(('t','o','h','l','c','v'))}
    n=len(rows); h,l,c,o,v=D['h'],D['l'],D['c'],D['o'],D['v']
    tr=[]
    for i in range(n): tr.append(h[i]-l[i] if i==0 else max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    atr=[None]*n
    if n>=14:
        atr[13]=sum(tr[:14])/14
        for i in range(14,n): atr[i]=(atr[i-1]*13+tr[i])/14
    ema9,ema21,ema50,ema200=(_ema(c,p) for p in (9,21,50,200))
    m20=_rolling_mean(c,20); sd20=_rolling_std(c,20,m20)
    bb_up=[None if m20[i] is None else m20[i]+2*sd20[i] for i in range(n)]
    bb_dn=[None if m20[i] is None else m20[i]-2*sd20[i] for i in range(n)]
    gains=[0.0]+[max(c[i]-c[i-1],0.0) for i in range(1,n)]
    losses=[0.0]+[max(c[i-1]-c[i],0.0) for i in range(1,n)]
    ag=_rolling_mean(gains,14); al=_rolling_mean(losses,14); rsi=[None]*n
    for i in range(n):
        if ag[i] is not None and al[i] is not None:
            rsi[i]=100.0 if al[i]==0 and ag[i]>0 else (50.0 if al[i]==0 else 100-100/(1+ag[i]/al[i]))
    e12,e26=_ema(c,12),_ema(c,26); macd=[e12[i]-e26[i] for i in range(n)]; macd_sig=_ema(macd,9); mh=[macd[i]-macd_sig[i] for i in range(n)]
    vm=_rolling_mean(v,20); typical=[(h[i]+l[i]+c[i])/3 for i in range(n)]; pv=vv=0.0; vwap=[]
    # Correct daily VWAP: Bybit session rolls at 00:00 UTC (07:00 WIB), not across
    # the entire 1,000-bar download window. The old cumulative version was mislabelled
    # as VWAP and could bias signals toward a multi-day average.
    vwap_day=None
    for i in range(n):
        day=int(D['t'][i]//86_400_000)
        if day != vwap_day:
            pv=0.0; vv=0.0; vwap_day=day
        pv+=typical[i]*max(v[i],0); vv+=max(v[i],0); vwap.append(pv/vv if vv else c[i])
    dmp=[0.0]+[max(h[i]-h[i-1],0) for i in range(1,n)]
    dmn=[0.0]+[max(l[i-1]-l[i],0) for i in range(1,n)]
    atr_s=_rolling_mean(tr,14); p_s=_rolling_mean(dmp,14); n_s=_rolling_mean(dmn,14); dx=[0.0]*n
    for i in range(n):
        if atr_s[i] and p_s[i] is not None and n_s[i] is not None:
            dip=100*p_s[i]/atr_s[i]; din=100*n_s[i]/atr_s[i]; dx[i]=100*abs(dip-din)/(dip+din) if dip+din else 0.0
    adx=_rolling_mean(dx,14)
    # OI forward-fill by exact/nearest prior timestamp.
    oi_map=oi or {}; oi_vals=[]; last=None; ordered=sorted((int(t),float(x)) for t,x in oi_map.items()); p=0
    for ts in D['t']:
        while p<len(ordered) and ordered[p][0]<=ts: last=ordered[p][1]; p+=1
        oi_vals.append(last)
    D.update(atr=atr,ema9=ema9,ema21=ema21,ema50=ema50,ema200=ema200,bb_up=bb_up,bb_dn=bb_dn,
             rsi=rsi,macd=macd,macd_sig=macd_sig,macd_hist=mh,vol_ma=vm,vwap=vwap,adx=adx,oi=oi_vals)
    return D


def delta_bar(D,i):
    rng=D['h'][i]-D['l'][i]
    if rng<=0:return 0.0
    return D['v'][i]*((D['c'][i]-D['l'][i])-(D['h'][i]-D['c'][i]))/rng


def of_ratio(D,i,n=5):
    start=max(0,i-n+1); vol=sum(D['v'][k] for k in range(start,i+1))
    return sum(delta_bar(D,k) for k in range(start,i+1))/vol if vol>0 else 0.0


def fake_dir(D, i):
    if i<21 or D['atr'][i] is None:return None
    o,h,l,c=D['o'],D['h'],D['l'],D['c']; a=D['atr'][i]; rng=h[i]-l[i]
    if rng<0.6*a:return None
    sh=max(h[i-20:i]); sl=min(l[i-20:i]); upper=h[i]-max(o[i],c[i]); lower=min(o[i],c[i])-l[i]
    if h[i]>sh and c[i]<sh and upper>=0.5*rng:return ('S',h[i],sh)
    if l[i]<sl and c[i]>sl and lower>=0.5*rng:return ('L',l[i],sl)
    return None


def retest_setup(D, i, max_bos=8, max_rt=8):
    o,h,l,c=D['o'],D['h'],D['l'],D['c']
    for f in range(i-2,max(i-max_bos-max_rt-2,21),-1):
        fd=fake_dir(D,f)
        if not fd:continue
        side,ext,swept=fd; a=D['atr'][f]; tol=0.1*a
        if side=='S':
            level=min(l[f-5:f+1]); b=None
            for k in range(f+1,min(f+max_bos+1,i)):
                if c[k]>ext:break
                if c[k]<level:b=k;break
            if b is None or i-b>max_rt:continue
            if any(h[k]>=level-tol or c[k]>ext for k in range(b+1,i)):continue
            if h[i]>=level-tol and c[i]<level:return ('S',{'sweep':f,'bos':b,'retest':i,'level':level,'swept':swept,'ext':ext})
        else:
            level=max(h[f-5:f+1]); b=None
            for k in range(f+1,min(f+max_bos+1,i)):
                if c[k]<ext:break
                if c[k]>level:b=k;break
            if b is None or i-b>max_rt:continue
            if any(l[k]<=level+tol or c[k]<ext for k in range(b+1,i)):continue
            if l[i]<=level+tol and c[i]>level:return ('L',{'sweep':f,'bos':b,'retest':i,'level':level,'swept':swept,'ext':ext})
    return None


def _structure_retest_allows(D, i, side, look):
    """Avoid repeatedly chasing the same impulse. After a recent break, require a retest of its broken level before another entry in that direction."""
    h,l,o,c,v=D['h'],D['l'],D['o'],D['c'],D['v']
    atr=D['atr']; e9=D['ema9']; e21=D['ema21']; vwap=D['vwap']; mh=D['macd_hist']; r=D['rsi']
    first=max(look+2, i-STRUCTURE_RETEST_WINDOW)
    for k in range(i-1, first-1, -1):
        ak=atr[k]
        if ak is None or ak<=0: continue
        level=(max(h[k-look:k]) if side=='L' else min(l[k-look:k]))
        rng=max(h[k]-l[k],1e-9); br=abs(c[k]-o[k])/rng
        vv=v[max(0,k-20):k]; vm=sum(vv)/max(1,len(vv)); rv=v[k]/max(vm,1e-9)
        impulse=(rv>=STRUCTURE_MIN_RVOL or abs(c[k]-o[k])>=STRUCTURE_MIN_BODY_ATR*ak)
        strong=br>=STRUCTURE_MIN_BODY_RATIO and impulse
        ext=((c[k]-level)/ak if side=='L' else (level-c[k])/ak)
        mom=(mh[k]>mh[k-1] or (r[k] is not None and r[k-1] is not None and r[k]>r[k-1])) if side=='L' else (mh[k]<mh[k-1] or (r[k] is not None and r[k-1] is not None and r[k]<r[k-1]))
        if side=='L':
            event=(c[k]>level and c[k]>o[k] and c[k]>e9[k] and (c[k]>vwap[k] or c[k]>e21[k]) and strong and mom and 0<ext<=STRUCTURE_MAX_EXTENSION_ATR)
        else:
            event=(c[k]<level and c[k]<o[k] and c[k]<e9[k] and (c[k]<vwap[k] or c[k]<e21[k]) and strong and mom and 0<ext<=STRUCTURE_MAX_EXTENSION_ATR)
        if not event: continue
        tol=STRUCTURE_RETEST_TOL_ATR*(atr[i] or ak)
        retest=False
        for j in range(k+1,i):
            aj=atr[j] or ak
            if side=='L':
                # Pull back toward the broken ceiling, but do not accept a full failed reclaim.
                if l[j] <= level + max(tol,STRUCTURE_RETEST_TOL_ATR*aj) and c[j] >= level-0.25*aj:
                    retest=True; break
            else:
                # Rally back toward broken support, then current bar must break lower again.
                if h[j] >= level - max(tol,STRUCTURE_RETEST_TOL_ATR*aj) and c[j] <= level+0.25*aj:
                    retest=True; break
        return retest
    return True


def rencana_candidate(D,i,live=None, min_score=None):
    """Combined Shadow Fakeout + Radar-style Trend Pullback/momentum signals on a closed bar."""
    n=len(D['c']); min_score=MIN_SCORE if min_score is None else min_score
    if i<60 or i>=n:return None
    for k in ('atr','ema9','ema21','ema50','ema200','rsi','macd_hist','vol_ma','vwap'):
        if D.get(k) is None:return None
    a=D['atr'][i]; r=D['rsi'][i]; vm=D['vol_ma'][i]
    if a is None or a<=0 or r is None or vm is None or D['adx'][i] is None:return None
    c,o,h,l,v=D['c'],D['o'],D['h'],D['l'],D['v']; e9,e21,e50,e200=D['ema9'],D['ema21'],D['ema50'],D['ema200']; mh=D['macd_hist']
    picks=[]
    # Exact original Shadow Fakeout/retest signal.
    fake=None; setup=None
    if CONFIRM=='retest':
        rt=retest_setup(D,i)
        if rt: fake=rt[0]; setup=rt[1]
    else:
        fd=fake_dir(D,i)
        if fd:
            fake=fd[0]; setup={'sweep':i,'bos':None,'retest':None,'level':None,'swept':fd[2],'ext':fd[1]}
    if fake:
        d=1 if fake=='L' else -1
        raw=of_ratio(D,i)*d
        if live:
            pp=[raw]+[live[k]*d for k in ('tdelta','obi') if live.get(k) is not None]
            ofr=sum(pp)/len(pp)
        else:ofr=raw
        ofs=1 if ofr>0.10 else -1 if ofr< -0.10 else 0
        oi_s=0; oi0,oi4,oi2=D['oi'][i],D['oi'][i-4],D['oi'][i-2]
        if oi0 and oi4:
            chg=(oi0-oi4)/oi4; dp=(c[i]-c[i-4])*d
            if chg>0.002:oi_s=1 if dp>0 else -1
            # Open-interest contraction has no inherent buy/sell direction; keep it neutral.
            # Only OI expansion with price moving in the setup direction earns a directional score.
        score=2+ofs+oi_s
        if score>=min_score:
            extreme=setup['ext'] if setup else (l[i] if fake=='L' else h[i])
            dist=min(max(abs(c[i]-extreme)+0.1*a,0.8*a),3*a)
            picks.append({'side':'L' if fake=='L' else 'S','kind':'SHADOW FAKEOUT','trig':'FAKEOUT','score':score,'entry':c[i],'dist':dist,'sl':c[i]-d*dist,'tp':c[i]+d*RR*dist,'atr':a,'ofr':ofr,'oi':((oi0-oi4)/oi4 if oi0 and oi4 else 0.0),'setup':setup,'parts':{'base':2,'of':ofs,'oi':oi_s}})
    # Trend continuation is deliberately stricter than the old version:
    # avoid taking pullbacks during flat EMA/low-ADX chop or against EMA200/VWAP.
    slope_n = TREND_SLOPE_BARS
    slope_ok = i >= slope_n
    ema_gap = abs(e21[i] - e50[i])
    trend_quality = (D['adx'][i] is not None and D['adx'][i] >= TREND_MIN_ADX
                     and ema_gap >= TREND_MIN_EMA_GAP_ATR * a and slope_ok)
    bull = (trend_quality and e21[i] > e50[i] and c[i] > e50[i]
            and c[i] > e200[i] and c[i] > D['vwap'][i]
            and e50[i] > e50[i-slope_n])
    bear = (trend_quality and e21[i] < e50[i] and c[i] < e50[i]
            and c[i] < e200[i] and c[i] < D['vwap'][i]
            and e50[i] < e50[i-slope_n])
    macd_up=mh[i]>mh[i-1]; macd_dn=mh[i]<mh[i-1]; volok=v[i]>vm*0.9
    body=abs(c[i]-o[i]); rng=max(h[i]-l[i],1e-9); lower=min(o[i],c[i])-l[i]
    break_up = c[i] > h[i-1] if TREND_REQUIRE_BREAK else True
    break_dn = c[i] < l[i-1] if TREND_REQUIRE_BREAK else True
    of_now = of_ratio(D,i)
    # Exhaustion veto for trend-pullback entries: don't sell into the floor of a
    # recent flush or buy into the ceiling of an already extended leg. A confirmed
    # structural breakout/breakdown above remains eligible independently.
    ext_start=max(0,i-8)
    local_low=min(l[ext_start:i]) if i>ext_start else l[i]
    local_high=max(h[ext_start:i]) if i>ext_start else h[i]
    near_local_low=(c[i]-local_low) <= 0.35*a
    near_local_high=(local_high-c[i]) <= 0.35*a
    lower_band=D.get('bb_dn',[None]*n)[i]
    upper_band=D.get('bb_up',[None]*n)[i]
    sell_exhausted=(near_local_low and (r<=46 or (e50[i]-c[i])>=0.80*a
                     or (lower_band is not None and l[i]<=lower_band+0.15*a)))
    buy_exhausted=(near_local_high and (r>=55 or (c[i]-e50[i])>=0.80*a
                     or (upper_band is not None and h[i]>=upper_band-0.15*a)))

    # STRUCTURE BREAKOUT / BREAKDOWN (independent of ADX).
    # The old code required ADX >= 16 for nearly every trend engine. That blocked
    # the exact transition zones visible in the user's chart: a strong reversal
    # out of a flush and a confirmed support breakdown both began while ADX was low.
    # This engine only fires on a CLOSED candle that breaks a recent structure,
    # has a meaningful body, confirms short-term momentum, and is not too extended.
    look = STRUCTURE_LOOKBACK
    if i >= max(look + 2, 25):
        prior_high = max(h[i-look:i])
        prior_low = min(l[i-look:i])
        body_abs = abs(c[i]-o[i])
        body_ratio = body_abs / max(h[i]-l[i], 1e-9)
        vol_base_vals = v[max(0, i-20):i]  # baseline excludes the signal candle
        vol_base = sum(vol_base_vals) / max(1, len(vol_base_vals))
        rvol_now = v[i] / max(vol_base, 1e-9)
        volume_or_impulse = (rvol_now >= STRUCTURE_MIN_RVOL or
                             body_abs >= STRUCTURE_MIN_BODY_ATR * a)
        candle_strong = (body_ratio >= STRUCTURE_MIN_BODY_RATIO and volume_or_impulse)
        # BUY: confirmed close through local swing highs after upward reclaim.
        ext_up = (c[i]-prior_high) / max(a, 1e-9)
        momentum_up = (mh[i] > mh[i-1] or (r is not None and D['rsi'][i-1] is not None and r > D['rsi'][i-1]))
        long_break = (c[i] > prior_high and c[i] > o[i] and c[i] > e9[i]
                      and (c[i] > D['vwap'][i] or c[i] > e21[i])
                      and candle_strong and momentum_up
                      and 0 < ext_up <= STRUCTURE_MAX_EXTENSION_ATR
                      and _structure_retest_allows(D,i,'L',look))
        # SELL: mirror condition on a confirmed break below local lows. No ADX
        # requirement; require price below fast EMA and either VWAP or EMA21.
        ext_dn = (prior_low-c[i]) / max(a, 1e-9)
        momentum_dn = (mh[i] < mh[i-1] or (r is not None and D['rsi'][i-1] is not None and r < D['rsi'][i-1]))
        short_break = (c[i] < prior_low and c[i] < o[i] and c[i] < e9[i]
                       and (c[i] < D['vwap'][i] or c[i] < e21[i])
                       and candle_strong and momentum_dn
                       and 0 < ext_dn <= STRUCTURE_MAX_EXTENSION_ATR
                       and _structure_retest_allows(D,i,'S',look))
        if long_break and not short_break:
            dist = max((c[i]-prior_high) + 0.15*a, STRUCTURE_MIN_SL_ATR*a)
            dist = min(dist, 2.0*a)
            score = 4 + int(rvol_now >= 1.4) + int(of_now > 0.10)
            picks.append({'side':'L','kind':'STRUCTURE BREAKOUT','trig':'STRUCTURE_UP',
                          'score':score,'entry':c[i],'dist':dist,'sl':c[i]-dist,
                          'tp':c[i]+RR*dist,'atr':a,'ofr':of_now,'oi':0.0,'setup':None,
                          'parts':{'base':4,'of':score-4,'oi':0},'rvol':rvol_now,
                          'structure_level':prior_high})
        elif short_break and not long_break:
            dist = max((prior_low-c[i]) + 0.15*a, STRUCTURE_MIN_SL_ATR*a)
            dist = min(dist, 2.0*a)
            score = 4 + int(rvol_now >= 1.4) + int(of_now < -0.10)
            picks.append({'side':'S','kind':'STRUCTURE BREAKDOWN','trig':'STRUCTURE_DN',
                          'score':score,'entry':c[i],'dist':dist,'sl':c[i]+dist,
                          'tp':c[i]-RR*dist,'atr':a,'ofr':of_now,'oi':0.0,'setup':None,
                          'parts':{'base':4,'of':score-4,'oi':0},'rvol':rvol_now,
                          'structure_level':prior_low})

    # Do not accept a trend entry when candle orderflow materially contradicts it.
    of_buy_ok = of_now >= -0.02
    of_sell_ok = of_now <= 0.02
    if bull and not buy_exhausted and of_buy_ok and break_up and l[i]<=e21[i]+0.20*a and c[i]>=e21[i] and c[i]>o[i] and macd_up and 45<=r<=67:
        dist=min(max(1.5*a,c[i]*0.0015),3*a); d=1; score=3+(1 if of_now>0.1 else 0)
        if score>=min_score:picks.append({'side':'L','kind':'TREND PULLBACK','trig':'PULLBACK','score':score,'entry':c[i],'dist':dist,'sl':c[i]-dist,'tp':c[i]+RR*dist,'atr':a,'ofr':of_now,'oi':0.0,'setup':None,'parts':{'base':3,'of':score-3,'oi':0}})
    if bear and not sell_exhausted and of_sell_ok and break_dn and h[i]>=e21[i]-0.20*a and c[i]<=e21[i] and c[i]<o[i] and macd_dn and 33<=r<=57:
        dist=min(max(1.5*a,c[i]*0.0015),3*a); d=-1; score=3+(1 if of_now< -0.1 else 0)
        if score>=min_score:picks.append({'side':'S','kind':'TREND PULLBACK','trig':'PULLBACK','score':score,'entry':c[i],'dist':dist,'sl':c[i]-d*dist,'tp':c[i]+d*RR*dist,'atr':a,'ofr':of_now,'oi':0.0,'setup':None,'parts':{'base':3,'of':score-3,'oi':0}})
    # Radar long-B/hook is allowed only in qualified uptrend; a reversal candle alone is not enough.
    hammer=body/rng<0.40 and lower>=max(body,a*0.03)*1.7 and c[i]>e21[i] and r>43 and macd_up and c[i]>h[i-1]
    hook=e9[i-2]>e9[i-1] and e9[i]>e9[i-1] and c[i]>e9[i] and 45<r<64 and r>D['rsi'][i-1] and mh[i]>mh[i-2] and volok and c[i]>h[i-1]
    if (hammer or hook) and bull and of_buy_ok:
        score=3+(1 if of_now>0.1 else 0); d=1; dist=min(max(1.5*a,c[i]*0.0015),3*a)
        if score>=min_score:picks.append({'side':'L','kind':'RADAR LONG B/HOOK','trig':'RADAR_HOOK','score':score,'entry':c[i],'dist':dist,'sl':c[i]-dist,'tp':c[i]+RR*dist,'atr':a,'ofr':of_now,'oi':0.0,'setup':None,'parts':{'base':3,'of':score-3,'oi':0}})
    # Momentum breakout only in qualified uptrend, with genuine 20-bar break and volume expansion.
    prior_high=max(h[max(0,i-20):i])
    if c[i]>prior_high and c[i]>o[i] and v[i]>vm*1.5 and r<78 and bull and of_buy_ok:
        score=4+(1 if of_now>0.1 else 0); d=1; dist=min(max(1.5*a,c[i]*0.0015),3*a)
        if score>=min_score:picks.append({'side':'L','kind':'BREAKOUT','trig':'BREAKOUT','score':score,'entry':c[i],'dist':dist,'sl':c[i]-dist,'tp':c[i]+RR*dist,'atr':a,'ofr':of_now,'oi':0.0,'setup':None,'parts':{'base':4,'of':score-4,'oi':0}})
    if not picks:
        return None
    sides={p['side'] for p in picks}
    if len(sides)>1:
        # If a confirmed structure break contradicts a softer legacy/trend setup,
        # prefer the structural candle only when its score strictly wins; a tie remains WAIT.
        structures=[p for p in picks if p['kind'] in ('STRUCTURE BREAKOUT','STRUCTURE BREAKDOWN')]
        if not structures:return None
        structures.sort(key=lambda p:p['score'], reverse=True)
        best_struct=structures[0]
        other=max((p['score'] for p in picks if p['side']!=best_struct['side']), default=-1)
        if best_struct['score']<=other:return None
        picks=[p for p in picks if p['side']==best_struct['side']]
    # On the actual break candle, structure is the primary setup, not a weaker
    # pullback label that can disguise the reason for entry.
    priority={'STRUCTURE BREAKOUT':4,'STRUCTURE BREAKDOWN':4,'SHADOW FAKEOUT':3,
              'TREND PULLBACK':2,'RADAR LONG B/HOOK':1,'BREAKOUT':1}
    picks.sort(key=lambda p:(priority.get(p['kind'],0),p['score']),reverse=True)
    out=picks[0].copy(); out['t']=D['t'][i]; out['i']=i; out['session']=session_for(D['t'][i]//1000); out['live']=live or {}; out['dist']=max(out['dist'],1e-9)
    return out


def _nth_sunday(year, month, n):
    d=datetime(year,month,1,tzinfo=timezone.utc); return d.replace(day=1+(6-d.weekday())%7+7*(n-1))

def session_for(ts):
    dt=datetime.fromtimestamp(ts,timezone.utc); start=_nth_sunday(dt.year,3,2).replace(hour=7); end=_nth_sunday(dt.year,11,1).replace(hour=6)
    ny=dt+timedelta(hours=-4 if start<=dt<end else -5); m=ny.hour*60+ny.minute
    if 570<=m<=960:return 'NEW YORK'
    if m>=1200 or m<=120:return 'ASIA'
    if 180<=m<=690:return 'LONDON'
    return 'DEAD ZONE'

# Minimal bitmap font for labels in pure PNG renderer.
_GLYPHS={
'A':('01110','10001','11111','10001','10001'),'B':('11110','10001','11110','10001','11110'),'C':('01111','10000','10000','10000','01111'),'D':('11110','10001','10001','10001','11110'),'E':('11111','10000','11110','10000','11111'),'F':('11111','10000','11110','10000','10000'),'G':('01111','10000','10111','10001','01111'),'H':('10001','10001','11111','10001','10001'),'I':('11111','00100','00100','00100','11111'),'J':('00111','00010','00010','10010','01100'),'K':('10001','10010','11100','10010','10001'),'L':('10000','10000','10000','10000','11111'),'M':('10001','11011','10101','10001','10001'),'N':('10001','11001','10101','10011','10001'),'O':('01110','10001','10001','10001','01110'),'P':('11110','10001','11110','10000','10000'),'Q':('01110','10001','10101','10010','01101'),'R':('11110','10001','11110','10100','10010'),'S':('01111','10000','01110','00001','11110'),'T':('11111','00100','00100','00100','00100'),'U':('10001','10001','10001','10001','01110'),'V':('10001','10001','10001','01010','00100'),'W':('10001','10001','10101','11011','10001'),'X':('10001','01010','00100','01010','10001'),'Y':('10001','10001','01010','00100','00100'),'Z':('11111','00010','00100','01000','11111'),
'0':('01110','10001','10011','10101','01110'),'1':('00100','01100','00100','00100','01110'),'2':('01110','10001','00010','00100','11111'),'3':('11110','00001','01110','00001','11110'),'4':('10010','10010','11111','00010','00010'),'5':('11111','10000','11110','00001','11110'),'6':('01110','10000','11110','10001','01110'),'7':('11111','00001','00010','00100','00100'),'8':('01110','10001','01110','10001','01110'),'9':('01110','10001','01111','00001','01110'),'-':('00000','00000','11111','00000','00000'),':':('00000','00100','00000','00100','00000'),'.':('00000','00000','00000','00000','00100'),'/':('00001','00010','00100','01000','10000'),'+':('00000','00100','11111','00100','00000'),'%':('11001','00010','00100','01000','10011'),'>':('10000','01000','00100','01000','10000'),'<':('00001','00010','00100','00010','00001'),'_':('00000','00000','00000','00000','11111')}

class Canvas:
    def __init__(self,width=1200,height=720,color=(250,251,253)):
        self.width=width; self.height=height; self.pixels=bytearray(bytes(color)*(width*height))
    def pixel(self,x,y,col):
        if 0<=x<self.width and 0<=y<self.height:
            k=(int(y)*self.width+int(x))*3; self.pixels[k:k+3]=bytes(col)
    def rect(self,x1,y1,x2,y2,col):
        x1=max(0,min(self.width-1,int(x1))); x2=max(0,min(self.width-1,int(x2))); y1=max(0,min(self.height-1,int(y1))); y2=max(0,min(self.height-1,int(y2)))
        if x2<x1 or y2<y1:return
        row=bytes(col)*(x2-x1+1)
        for y in range(y1,y2+1):
            a=(y*self.width+x1)*3; self.pixels[a:a+len(row)]=row
    def line(self,x1,y1,x2,y2,col,width=1):
        x1,y1,x2,y2=map(int,(x1,y1,x2,y2)); dx=abs(x2-x1); sx=1 if x1<x2 else -1; dy=-abs(y2-y1); sy=1 if y1<y2 else -1; err=dx+dy
        while True:
            for o in range(-(width//2),width//2+1): self.pixel(x1+o,y1,col); self.pixel(x1,y1+o,col)
            if x1==x2 and y1==y2:break
            e2=2*err
            if e2>=dy:err+=dy;x1+=sx
            if e2<=dx:err+=dx;y1+=sy
    def dashed(self,x1,y,x2,col):
        for x in range(int(x1),int(x2),16): self.line(x,y,min(x+8,x2),y,col)
    def dotted(self,x1,y,x2,col):
        for x in range(int(x1),int(x2),7): self.line(x,y,min(x+2,x2),y,col)
    def circle(self,cx,cy,r,col):
        for y in range(int(cy-r),int(cy+r+1)):
            for x in range(int(cx-r),int(cx+r+1)):
                if (x-cx)**2+(y-cy)**2<=r*r:self.pixel(x,y,col)
    def label(self,x,y,text,col,scale=2,bg=None):
        text=str(text).upper(); x=int(x); y=int(y); step=6*scale
        if bg:self.rect(x-3,y-3,x+len(text)*step,y+7*scale,bg)
        cur=x
        for ch in text:
            if ch==' ':cur+=3*scale;continue
            glyph=_GLYPHS.get(ch)
            if glyph:
                for rr,row in enumerate(glyph):
                    for cc,b in enumerate(row):
                        if b=='1':self.rect(cur+cc*scale,y+rr*scale,cur+(cc+1)*scale-1,y+(rr+1)*scale-1,col)
            cur+=step
    def png(self):
        raw=b''.join(b'\x00'+bytes(self.pixels[y*self.width*3:(y+1)*self.width*3]) for y in range(self.height))
        def chunk(n,d):return struct.pack('>I',len(d))+n+d+struct.pack('>I',zlib.crc32(n+d)&0xffffffff)
        return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',self.width,self.height,8,2,0,0,0))+chunk(b'IDAT',zlib.compress(raw,6))+chunk(b'IEND',b'')


def _price_chart(D,end_i,tf_key='15m',off=0.0,deals=None,candidate=None,bars=110,diagnostic=False,pair_label='XAUUSD',src_label=None):
    deals=deals or []; ids=list(range(max(0,end_i-bars+1),end_i+1)); n=len(ids); cv=Canvas()
    if not ids: raise ValueError('Candle kosong')
    ts=candle_close_ms(D['t'][end_i], tf_key); label=TF[tf_key][0]  # close time for chart header
    tf_display=tf_key.upper()
    O=[D['o'][i]+off for i in ids]; H=[D['h'][i]+off for i in ids]; L=[D['l'][i]+off for i in ids]; C=[D['c'][i]+off for i in ids]; V=[D['v'][i] for i in ids]
    extra=[]
    if candidate:extra.extend([candidate['entry']+off,candidate['sl']+off,candidate['tp']+off]+[x[0]+off for x in candidate.get('extras',[])])
    # Include plotted EMA/BB levels in the visible price scale; otherwise a distant
    # EMA200 or band can draw through the title and status row.
    for key in ('ema9','ema21','ema50','ema200','bb_up','bb_dn','tenkan','kijun'):
        vals=D.get(key,[])
        for k in ids:
            if k < len(vals) and vals[k] is not None:
                extra.append(float(vals[k])+off)
    low=min(L+extra); high=max(H+extra); pad=max((high-low)*0.06,0.02); low-=pad; high+=pad
    left,top,right,bottom=80,108,920,535; prof_l,prof_r=960,1165; vol_top,vol_bottom=585,650
    step=(right-left)/max(1,n-1+12)
    def xx(k):return int(left+k*step)
    def yy(p):return int(bottom-(p-low)*(bottom-top)/max(high-low,1e-9))
    dt=datetime.fromtimestamp(ts/1000,WIB)
    cv.label(left,12,f'{pair_label} RENCANA {tf_display} {dt:%d-%m %H:%M}',(20,50,95),scale=2)
    cv.label(left,43,f'LAST {C[-1]:.2f} OFFSET {off:+.2f}',(25,75,125),scale=2)
    resolved=[d for d in deals if d['result'] in ('TP','SL','BE+','SL+','TIMEOUT')]
    tpc=sum(d['result']=='TP' for d in deals); slc=sum(d['result']=='SL' for d in deals)
    bec=sum(d['result']=='BE+' for d in deals); slpc=sum(d['result']=='SL+' for d in deals)
    toc=sum(d['result']=='TIMEOUT' for d in deals)
    net_chart=sum(float(d['r']) for d in deals if d.get('r') is not None)
    cv.label(left,68,f'SRC {src_label or SYMBOL or "BYBIT"} | SIM {len(deals)} TP {tpc} BE+ {bec} SL+ {slpc} SL {slc} | NET {net_chart:+.2f}R',(20,125,70),scale=1)
    for r in range(1,6):
        y=top+(bottom-top)*r//6; cv.line(left,y,right,y,(225,229,235))
    for k in range(1,9):
        x=left+(right-left)*k//9; cv.line(x,top,x,bottom,(239,241,245))
    for r in range(6):
        p=high-(high-low)*r/5; cv.label(3,yy(p)-5,f'{p:.2f}',(55,55,55),scale=1)
    # EMA ribbon and BB; each series only visible portion.
    for key,col,w in [('ema9',(220,173,45),2),('ema21',(225,105,30),3),('ema50',(35,105,175),3),('ema200',(150,70,130),3)]:
        vals=D.get(key,[])
        for k in range(1,n):
            a,b=vals[ids[k-1]],vals[ids[k]]; cv.line(xx(k-1),yy(a+off),xx(k),yy(b+off),col,w)
    # Ichimoku (hanya jika D membawa seri tenkan/kijun/cloud, mis. chart 4H)
    for key,col in [('cloud_top',(120,175,125)),('cloud_bot',(185,125,125)),('tenkan',(0,150,200)),('kijun',(205,40,120))]:
        vals=D.get(key)
        if vals:
            for k in range(1,n):
                a,b=vals[ids[k-1]],vals[ids[k]]
                if a is not None and b is not None:
                    y1,y2=yy(a+off),yy(b+off)
                    if top<=y1<=bottom and top<=y2<=bottom:cv.line(xx(k-1),y1,xx(k),y2,col,2)
    # bands as faint edges
    for key,col in [('bb_up',(175,155,185)),('bb_dn',(175,155,185))]:
        vals=D.get(key,[])
        for k in range(1,n):
            a,b=vals[ids[k-1]],vals[ids[k]]
            if a is not None and b is not None:cv.line(xx(k-1),yy(a+off),xx(k),yy(b+off),col,1)
    cw=max(2,int(step*0.30))
    for k in range(n):
        col=(22,145,82) if C[k]>=O[k] else (205,48,58); x=xx(k)
        cv.line(x,yy(H[k]),x,yy(L[k]),col,1); cv.rect(x-cw,yy(max(O[k],C[k])),x+cw,yy(min(O[k],C[k])),col)
    # Current candidate levels only if a real setup passes rules.
    if candidate:
        entry,sl,tp=candidate['entry']+off,candidate['sl']+off,candidate['tp']+off; xe=xx(n-1); x2=right+10
        cv.dashed(left,yy(entry),right,(35,35,35)); cv.dashed(left,yy(sl),right,(210,45,55)); cv.dashed(left,yy(tp),right,(20,145,75))
        cv.label(right-155,yy(entry)-14,f'ENTRY {entry:.2f}',(40,40,40),scale=1,bg=(250,251,253))
        cv.label(right-155,yy(sl)-14,f'SL {sl:.2f}',(210,45,55),scale=1,bg=(250,251,253))
        cv.label(right-155,yy(tp)-14,f'{candidate.get("tp_label","TP")} {tp:.2f}',(20,145,75),scale=1,bg=(250,251,253))
        for ex_p,ex_l in candidate.get('extras',[]):
            cv.dashed(left,yy(ex_p+off),right,(110,110,175)); cv.label(right-155,yy(ex_p+off)-14,f'{ex_l} {ex_p+off:.2f}',(80,80,170),scale=1,bg=(250,251,253))
        cv.label(left+8,top+5,('BUY ' if candidate['side']=='L' else 'SELL ')+candidate['kind'],(20,125,70) if candidate['side']=='L' else (190,40,50),scale=2,bg=(250,251,253))
    else:
        cv.label(left+8,top+5,'DIAGNOSTIC NO SIGNAL' if diagnostic else 'WAIT NO CURRENT SETUP',(185,45,55),scale=2,bg=(250,251,253))
    # Historical trade markers: show only the latest few visible trades so labels do not pile up.
    visible_deals = [d for d in deals if d.get('entry_i') in ids and d.get('exit_i') in ids][-4:]
    for d in visible_deals:
        ei=d.get('entry_i'); xi=d.get('exit_i')
        if ei is None or xi is None or ei not in ids or xi not in ids:continue
        ek=ei-ids[0]; x=xx(ek); yp=yy(d['entry']+off)
        isbuy=d['side']=='L'; col=(0,125,65) if isbuy else (195,40,50)
        marker={'STRUCTURE BREAKOUT':'BR','STRUCTURE BREAKDOWN':'BD',
                'TREND PULLBACK':'PB' if isbuy else 'PS',
                'SHADOW FAKEOUT':'SF','RADAR LONG B/HOOK':'RH','BREAKOUT':'BO'}.get(d.get('kind'), 'B' if isbuy else 'S')
        cv.circle(x,yp,4,col); cv.label(max(left,min(right-27,x-4)),max(top,min(bottom-10,yp-13)), marker,col,scale=1,bg=(250,251,253))
        if d['result'] in ('TP','SL','BE+','SL+','TIMEOUT'):
            xout=xx(xi-ids[0]); yout=yy(d['exit']+off)
            colors={'TP':(0,145,75),'SL':(210,35,45),'BE+':(20,135,170),'SL+':(125,80,190),'TIMEOUT':(105,105,105)}
            outcol=colors.get(d['result'],(105,105,105))
            out_label={'TIMEOUT':'TO','BE+':'BE+','SL+':'SL+','TP':'TP','SL':'SL'}.get(d['result'],d['result'])
            cv.line(x,yp,xout,yout,outcol,2); cv.circle(xout,yout,3,outcol); cv.label(max(left,min(right-24,xout-4)),max(top,min(bottom-10,yout+6)),out_label,outcol,scale=1,bg=(250,251,253))
    # Current bar marker
    cv.circle(xx(n-1),yy(C[-1]),5,(255,180,35))
    # Volume profile, point of control.
    bins=24; bsize=max((high-low)/bins,1e-9); profile=[0.0]*bins
    for k in range(n):
        b=max(0,min(bins-1,int((((H[k]+L[k]+C[k])/3)-low)/bsize))); profile[b]+=max(V[k],1.0)
    mx=max(profile) or 1
    for b,amt in enumerate(profile):
        p1=low+b*bsize; p2=p1+bsize; w=int(amt/mx*(prof_r-prof_l))
        col=(175,80,215) if b==profile.index(mx) else ((145,215,195) if p1<=C[-1] else (240,180,190))
        cv.rect(prof_r-w,yy(p2),prof_r,yy(p1),col)
    poc=low+(profile.index(mx)+0.5)*bsize; cv.dashed(left,yy(poc),right,(175,80,215)); cv.label(left+8,yy(poc)-15,f'POC {poc:.2f}',(145,45,175),scale=1,bg=(250,251,253)); cv.label(prof_l+10,top+4,'VOLUME',(20,85,90),scale=2)
    maxv=max(V) or 1; dstep=max(1,n//7)
    for k in range(n):
        hv=int(V[k]/maxv*(vol_bottom-vol_top-5)); col=(65,175,95) if C[k]>=O[k] else (225,105,110)
        cv.rect(xx(k)-max(1,cw//2),vol_bottom-hv,xx(k)+max(1,cw//2),vol_bottom,col)
    cv.line(left,vol_top,right,vol_top,(185,190,198)); cv.label(left+5,vol_top+4,'VOL',(80,80,80),scale=1)
    for k in range(0,n,dstep):cv.label(max(left,xx(k)-18),vol_bottom+12,datetime.fromtimestamp(D['t'][ids[k]]/1000,WIB).strftime('%d-%m %H:%M'),(70,70,70),scale=1)
    return cv.png()


def simulate_trades(D, start_i=None, end_i=None, max_hold=None):
    """Conservative M15 historical simulation with staged BE+/SL+ management.

    Signals are generated on a CLOSED bar; simulated entry is the next bar open.
    An SL update identified from bar j is only active from bar j+1. If the currently
    active stop is touched, it is processed before evaluating new BE/SL+ triggers.
    OHLC cannot reveal the path within a candle, so a stop is not upgraded using an
    excursion from a candle that also hit the old stop. If a new BE/SL+ trigger and
    its newly locked stop are both inside the same candle range, the simulator treats
    the locked stop as hit conservatively and marks the trade ambiguous. This avoids
    applying a hypothetical locked stop only on the next candle after price has already
    crossed it. Exit-bar MFE/MAE are reported separately, not counted as confirmed.
    This is a conservative M15 estimate,
    not a tick-accurate or broker-CFD backtest.
    """
    n = len(D['c'])
    if n < 2:
        return []
    signal_end = min(end_i if end_i is not None else n - 2, n - 2)
    data_end = min(signal_end + 1, n - 1)
    start = max(60, start_i if start_i is not None else max(60, n - 300))
    deals = []
    i = start
    hold = max(1, int(max_hold or MAX_BARS))
    min_cost_lock = max(SPREAD, PAPER_COST_XAU) + 2.0 * PAPER_TICK

    def stage_result(stage):
        return 'SL+' if stage >= 2 else ('BE+' if stage == 1 else 'SL')

    while i <= signal_end:
        sg = rencana_candidate(D, i, live=None)
        if not sg:
            i += 1
            continue

        direction = 1 if sg['side'] == 'L' else -1
        entry_i = i + 1
        if entry_i > data_end:
            break
        entry = float(D['o'][entry_i])
        dist = max(float(sg['dist']), 1e-9)
        initial_sl = entry - direction * dist
        active_sl = initial_sl
        tp = entry + direction * RR * dist
        stage = 0
        be_trigger_i = None
        slplus_trigger_i = None

        planned_exit_i = entry_i + hold - 1
        horizon_end = min(data_end, planned_exit_i)
        result = 'OPEN'
        reason = 'DATA_END_BEFORE_EXIT'
        exitpx = float(D['c'][horizon_end])
        exit_i = horizon_end
        ambiguous = False

        for j in range(entry_i, horizon_end + 1):
            op = float(D['o'][j]); hi = float(D['h'][j]); lo = float(D['l'][j])

            # Gaps past an already-active stop/target are filled conservatively.
            if j > entry_i:
                gap_stop = (direction == 1 and op <= active_sl) or (direction == -1 and op >= active_sl)
                gap_tp = (direction == 1 and op >= tp) or (direction == -1 and op <= tp)
                if gap_stop:
                    result, reason, exitpx, exit_i = stage_result(stage), 'GAP_THROUGH_ACTIVE_STOP', op, j
                    break
                if gap_tp:
                    result, reason, exitpx, exit_i = 'TP', 'GAP_THROUGH_TP', tp, j
                    break

            hit_stop = (lo <= active_sl) if direction == 1 else (hi >= active_sl)
            hit_tp = (hi >= tp) if direction == 1 else (lo <= tp)
            if hit_stop and hit_tp:
                result, reason, exitpx, exit_i = stage_result(stage), 'BOTH_LEVELS_SAME_CANDLE_STOP_FIRST', active_sl, j
                ambiguous = True
                break
            if hit_stop:
                result, reason, exitpx, exit_i = stage_result(stage), 'ACTIVE_STOP_TOUCHED', active_sl, j
                break
            if hit_tp:
                result, reason, exitpx, exit_i = 'TP', 'TP_TOUCHED', tp, j
                break

            # No active stop or target was touched in this candle. Only now can
            # the completed candle arm a new stop; it applies from next candle.
            if BE_ENABLED:
                favorable = (hi-entry) if direction == 1 else (entry-lo)
                old_stage = stage
                if favorable >= SLPLUS_TRIGGER_R * dist:
                    stage = 2
                    lock_dist = max(SLPLUS_LOCK_R * dist, min_cost_lock)
                    candidate_sl = entry + direction * lock_dist
                    if direction == 1:
                        active_sl = max(active_sl, _round_stop(candidate_sl, 'L'))
                    else:
                        active_sl = min(active_sl, _round_stop(candidate_sl, 'S'))
                    if slplus_trigger_i is None:
                        slplus_trigger_i = j
                    if be_trigger_i is None:
                        be_trigger_i = j
                elif favorable >= BE_TRIGGER_R * dist and stage < 1:
                    stage = 1
                    lock_dist = max(BE_OFFSET_R * dist, min_cost_lock)
                    candidate_sl = entry + direction * lock_dist
                    if direction == 1:
                        active_sl = max(active_sl, _round_stop(candidate_sl, 'L'))
                    else:
                        active_sl = min(active_sl, _round_stop(candidate_sl, 'S'))
                    if be_trigger_i is None:
                        be_trigger_i = j

                # If trigger and newly advanced stop both lie inside this bar's range,
                # OHLC cannot prove the order. Prefer the conservative stop-first model.
                if stage > old_stage:
                    trail_touched = (lo <= active_sl) if direction == 1 else (hi >= active_sl)
                    if trail_touched:
                        result, reason, exitpx, exit_i = stage_result(stage), 'TRAIL_STOP_AND_TRIGGER_SAME_CANDLE_CONSERVATIVE', active_sl, j
                        ambiguous = True
                        break

        if result == 'OPEN' and planned_exit_i <= data_end:
            result, reason = 'TIMEOUT', 'MAX_HOLD_CLOSE'
            exit_i = planned_exit_i
            exitpx = float(D['c'][exit_i])
        elif result == 'OPEN':
            exit_i = data_end
            exitpx = float(D['c'][exit_i])

        r_value = ((exitpx - entry) * direction - SPREAD) / dist
        r_value = r_value if result != 'OPEN' else None
        bars_held = max(1, exit_i - entry_i + 1)

        # Confirmed MFE/MAE only use full bars BEFORE an intrabar stop/TP exit.
        # The exit candle's extrema are separated because their order versus exit
        # is unknowable from M15 OHLC. TIMEOUT exits at close, so its candle is valid.
        if result in ('SL','BE+','SL+','TP'):
            confirmed_ids = range(entry_i, exit_i)
            exit_bar_only = True
        else:
            confirmed_ids = range(entry_i, exit_i + 1)
            exit_bar_only = False
        def excursions(indices):
            ids = list(indices)
            if not ids:
                return 0.0, 0.0
            if direction == 1:
                mf = max(float(D['h'][k])-entry for k in ids) / dist
                ma = max(entry-float(D['l'][k]) for k in ids) / dist
            else:
                mf = max(entry-float(D['l'][k]) for k in ids) / dist
                ma = max(float(D['h'][k])-entry for k in ids) / dist
            return max(0.0,mf), max(0.0,ma)
        mfe_r, mae_r = excursions(confirmed_ids)
        if exit_bar_only:
            exit_bar_mfe_r, exit_bar_mae_r = excursions([exit_i])
        else:
            exit_bar_mfe_r = exit_bar_mae_r = 0.0

        deals.append({
            't': D['t'][i], 'side': sg['side'], 'kind': sg['kind'], 'score': sg['score'],
            'signal_i': i, 'entry_i': entry_i, 'exit_i': exit_i,
            'signal_close': float(D['c'][i]), 'entry_gap': entry - float(D['c'][i]),
            'entry': entry, 'sl': initial_sl, 'initial_sl': initial_sl,
            'active_sl': active_sl, 'tp': tp, 'exit': exitpx,
            'result': result, 'reason': reason, 'r': r_value,
            'ambiguous': ambiguous, 'bars_held': bars_held,
            'mfe_r': mfe_r, 'mae_r': mae_r,
            'exit_bar_mfe_r': exit_bar_mfe_r, 'exit_bar_mae_r': exit_bar_mae_r,
            'be_stage': stage, 'be_trigger_i': be_trigger_i, 'slplus_trigger_i': slplus_trigger_i,
            'dist': dist,
        })
        # One position at a time. Resume only after trade closes/expires.
        i = max(i + 1, exit_i + 1)
    return deals


def simulate_trades_m1(D, rows_m1, start_i=None, end_i=None, max_hold=None, tf_key='15m'):
    """Generate signals on tf_key, but execute stops/targets/BE+/SL+ on CLOSED M1 OHLC.

    Stage updates are applied only from the next M1 candle. This fixes the previous
    backtest bug that could trigger BE/SL+ and hit the newly created stop inside the
    same candle, even though the code claimed the stop was active from the next bar.
    If an already-active SL and TP are both touched in the same M1 candle, SL-first is
    used and the trade is marked ambiguous.
    """
    n=len(D['c'])
    if n<2 or not rows_m1: return []
    ms=TF[tf_key][1]
    if ms is None or ms < 60_000: return simulate_trades(D,start_i,end_i,max_hold)
    t1=[int(r[0]) for r in rows_m1]
    signal_end=min(end_i if end_i is not None else n-2,n-2)
    data_end=min(signal_end+1,n-1)
    start=max(60,start_i if start_i is not None else max(60,n-300))
    hold=max(1,int(max_hold or MAX_BARS))
    sim_end_ms=candle_close_ms(D['t'][data_end],tf_key)
    cost=max(SPREAD,PAPER_COST_XAU)
    min_cost_lock=cost+2.0*PAPER_TICK
    def stage_result(stage): return 'SL+' if stage>=2 else ('BE+' if stage==1 else 'SL')
    deals=[]; i=start
    while i<=signal_end:
        sg=rencana_candidate(D,i,live=None)
        if not sg:
            i+=1; continue
        direction=1 if sg['side']=='L' else -1
        entry_i=i+1; entry_time=int(D['t'][entry_i])
        entry=float(D['o'][entry_i]); dist=max(float(sg['dist']),1e-9)
        initial_sl=entry-direction*dist; active_sl=initial_sl; tp=entry+direction*RR*dist
        stage=0; be_trigger_i=None; slplus_trigger_i=None; ambiguous=False
        horizon_end_ms=entry_time+hold*ms
        j0=bisect.bisect_left(t1,entry_time)
        jcap=bisect.bisect_left(t1,sim_end_ms)
        jh=bisect.bisect_left(t1,horizon_end_ms)
        jend=min(jcap,jh,len(rows_m1))
        # Entry candles must align across the signal timeframe and M1 feed.
        if j0>=len(rows_m1) or t1[j0] != entry_time:
            # Missing execution data is not converted into a synthetic win/loss.
            i+=1; continue
        if jend<=j0:
            i+=1; continue
        result='OPEN'; reason='DATA_END_BEFORE_EXIT'; exitpx=float(rows_m1[jend-1][4]); exit_j=jend-1
        for j in range(j0,jend):
            op,hi,lo,cl=map(float,(rows_m1[j][1],rows_m1[j][2],rows_m1[j][3],rows_m1[j][4]))
            # Gap fills against an already-active stop/TP, never against a stop armed later in this candle.
            if j>j0:
                gap_stop=(direction==1 and op<=active_sl) or (direction==-1 and op>=active_sl)
                gap_tp=(direction==1 and op>=tp) or (direction==-1 and op<=tp)
                if gap_stop:
                    result,reason,exitpx,exit_j=stage_result(stage),'GAP_THROUGH_ACTIVE_STOP',op,j; break
                if gap_tp:
                    result,reason,exitpx,exit_j='TP','GAP_THROUGH_TP',tp,j; break
            hit_stop=(lo<=active_sl) if direction==1 else (hi>=active_sl)
            hit_tp=(hi>=tp) if direction==1 else (lo<=tp)
            if hit_stop and hit_tp:
                result,reason,exitpx,exit_j=stage_result(stage),'BOTH_ACTIVE_LEVELS_SAME_M1_STOP_FIRST',active_sl,j
                ambiguous=True; break
            if hit_stop:
                result,reason,exitpx,exit_j=stage_result(stage),'ACTIVE_STOP_TOUCHED_M1',active_sl,j; break
            if hit_tp:
                result,reason,exitpx,exit_j='TP','TP_TOUCHED_M1',tp,j; break
            # A closed M1 candle can arm a new stop, but the new stop first becomes active next minute.
            if BE_ENABLED:
                favorable=(hi-entry) if direction==1 else (entry-lo)
                old_stage=stage
                if favorable>=SLPLUS_TRIGGER_R*dist:
                    stage=2
                    lock=max(SLPLUS_LOCK_R*dist,min_cost_lock)
                    proposed=entry+direction*lock
                    active_sl=max(active_sl,_round_stop(proposed,'L')) if direction==1 else min(active_sl,_round_stop(proposed,'S'))
                    if slplus_trigger_i is None: slplus_trigger_i=j
                    if be_trigger_i is None: be_trigger_i=j
                elif favorable>=BE_TRIGGER_R*dist and stage<1:
                    stage=1
                    lock=max(BE_OFFSET_R*dist,min_cost_lock)
                    proposed=entry+direction*lock
                    active_sl=max(active_sl,_round_stop(proposed,'L')) if direction==1 else min(active_sl,_round_stop(proposed,'S'))
                    if be_trigger_i is None: be_trigger_i=j
        if result=='OPEN':
            full_horizon=(horizon_end_ms<=sim_end_ms and jend==jh and jend>j0 and t1[jend-1]+60_000>=horizon_end_ms)
            exitpx=float(rows_m1[jend-1][4]); exit_j=jend-1
            if full_horizon:
                result,reason='TIMEOUT','MAX_HOLD_CLOSE_M1'
            else:
                reason='DATA_END_BEFORE_EXIT'
        pnl_gross=(exitpx-entry)*direction
        r_value=(pnl_gross-cost)/dist if result!='OPEN' else None
        bars_held=max(1,exit_j-j0+1)
        # Excursions use execution candles before exit; exit-minute excursions are diagnostic only.
        def excursion(a,b):
            if b<=a:return (0.0,0.0)
            if direction==1:
                mf=max(float(rows_m1[k][2])-entry for k in range(a,b))/dist
                ma=max(entry-float(rows_m1[k][3]) for k in range(a,b))/dist
            else:
                mf=max(entry-float(rows_m1[k][3]) for k in range(a,b))/dist
                ma=max(float(rows_m1[k][2])-entry for k in range(a,b))/dist
            return max(0.0,mf),max(0.0,ma)
        if result in ('SL','BE+','SL+','TP'):
            mfe,mae=excursion(j0,exit_j)
            exit_mfe,exit_mae=excursion(exit_j,exit_j+1)
        else:
            mfe,mae=excursion(j0,exit_j+1); exit_mfe=exit_mae=0.0
        exit_ts=t1[exit_j]
        exit_m15=max(0,min(n-1,bisect.bisect_right(D['t'],exit_ts)-1))
        deals.append({'t':D['t'][i],'side':sg['side'],'kind':sg['kind'],'score':sg['score'],
            'signal_i':i,'entry_i':entry_i,'exit_i':exit_m15,'exec_entry_i':j0,'exec_exit_i':exit_j,
            'signal_close':float(D['c'][i]),'entry_gap':entry-float(D['c'][i]),'entry':entry,
            'sl':initial_sl,'initial_sl':initial_sl,'active_sl':active_sl,'tp':tp,'exit':exitpx,
            'result':result,'reason':reason,'r':r_value,'ambiguous':ambiguous,'bars_held':bars_held,
            'mfe_r':mfe,'mae_r':mae,'exit_bar_mfe_r':exit_mfe,'exit_bar_mae_r':exit_mae,
            'be_stage':stage,'be_trigger_i':be_trigger_i,'slplus_trigger_i':slplus_trigger_i,
            'dist':dist,'exec_tf':'1m'})
        i=max(i+1,exit_m15+1)
    return deals


def backtest(D): return simulate_trades(D,start_i=60,end_i=len(D['c'])-2)

def _tf_millis_per_day(tf_key):
    ms=TF[tf_key][1]
    if ms:return 86_400_000/ms
    return 1/30

def run_backtest(days=30):
    sym=resolve_symbol(); tf='15m'; target=max(100,min(365*24*4,int(days*24*60/15)+220)); raw=[]; before=None
    while len(raw)<target:
        limit=min(1000,target-len(raw))
        params={'category':'linear','symbol':sym,'interval':'15','limit':limit}
        if before is not None:params['end']=before-1
        pack=api('/v5/market/kline',params,timeout=20).get('list',[])
        if not pack:break
        rows=sorted((int(r[0]),)+tuple(float(x) for x in r[1:6]) for r in pack)
        new=[r for r in rows if not raw or r[0]<raw[0][0]]
        if not new:break
        raw=new+raw; before=raw[0][0]
        if len(pack)<limit:break
    now=now_ms(); raw=[r for r in raw if r[0]+900000<=now]
    if len(raw)<80:return f'Backtest {days} hari: data candle kurang ({len(raw)}).'
    D=build(raw,fetch_oi_recent(sym,24)); cut=now-days*86400000
    exec_target=max(1000,int((D['t'][-1]+900000-D['t'][0])/60000)+5)
    exec_rows=fetch_klines_history_tf(sym,'1m',exec_target)
    all_deals=simulate_trades_m1(D,exec_rows,start_i=60,end_i=len(D['c'])-2,max_hold=MAX_BARS,tf_key='15m')
    deals=[d for d in all_deals if d['t']>=cut]
    done=[d for d in deals if d['result'] in ('TP','SL','BE+','SL+','TIMEOUT')]; wins=sum((d['r'] or 0)>0 for d in done); losses=sum((d['r'] or 0)<0 for d in done)
    gp=sum(d['r'] for d in done if (d['r'] or 0)>0); gl=-sum(d['r'] for d in done if (d['r'] or 0)<0); pf=gp/gl if gl else float('inf')
    net=sum(d['r'] for d in deals if d['r'] is not None)
    return (f'BACKTEST BYBIT {sym} M15 — {days} hari\n'
      f'Trade {len(deals)} | TP {sum(d["result"]=="TP" for d in deals)} | SL+ {sum(d["result"]=="SL+" for d in deals)} | BE+ {sum(d["result"]=="BE+" for d in deals)} | SL {sum(d["result"]=="SL" for d in deals)} | TIMEOUT {sum(d["result"]=="TIMEOUT" for d in deals)} | OPEN {sum(d["result"]=="OPEN" for d in deals)}\n'
      f'Ambigu (stop-first): {sum(d.get("ambiguous",False) for d in deals)} | Winrate closed {wins/len(done)*100:.1f}% | PF {pf:.2f} | Net {net:+.2f}R\n'
      f'Entry: open candle berikutnya; eksekusi/BE+/SL+ diperiksa pada M1. BE+ +{BE_TRIGGER_R:g}R→kunci min +{BE_OFFSET_R:g}R; SL+ +{SLPLUS_TRIGGER_R:g}R→kunci min +{SLPLUS_LOCK_R:g}R; biaya {max(SPREAD, PAPER_COST_XAU):g}. Hasil M1 masih simulasi Bybit, bukan order live/CFD broker.')


def _diagnostic_caption_full(D, i, deals, tf_key='15m', live=None, candidate=None):
    mtime=datetime.fromtimestamp(candle_close_ms(D['t'][i], tf_key)/1000,WIB); c=D['c'][i]; px=c+STATE['offset']; a=D['atr'][i] or 0
    completed=[d for d in deals if d['result'] in ('TP','SL','BE+','SL+','TIMEOUT')]
    tps=sum(d['result']=='TP' for d in deals); sls=sum(d['result']=='SL' for d in deals)
    bes=sum(d['result']=='BE+' for d in deals); slps=sum(d['result']=='SL+' for d in deals)
    timeouts=sum(d['result']=='TIMEOUT' for d in deals); opens=sum(d['result']=='OPEN' for d in deals)
    wins=sum((d['r'] or 0)>0 for d in completed)
    wr=f'{wins/len(completed)*100:.1f}%' if completed else 'N/A'
    net=sum(d['r'] for d in deals if d['r'] is not None)
    ambiguous=sum(d.get('ambiguous',False) for d in deals)
    live=live or {}; oi=D['oi'][i]; oi4=D['oi'][i-4] if i>=4 else None
    oi_chg=((oi-oi4)/oi4*100) if oi and oi4 else 0
    r=D['rsi'][i]; adx=D['adx'][i]; mh=D['macd_hist'][i]
    if candidate:
        side='BUY' if candidate['side']=='L' else 'SELL'
        setup=f'{side} — {candidate["kind"]} (score {candidate["score"]})'
        entry=candidate['entry']+STATE['offset']; sl=candidate['sl']+STATE['offset']; tp=candidate['tp']+STATE['offset']
        entry_text=f'Entry {entry:.2f} | SL {sl:.2f} | TP {tp:.2f}'
    else:
        setup='WAIT — belum ada setup valid'
        entry_text='Entry/SL/TP tidak dipaksakan tanpa setup valid.'
    text_out=(f'<b>RENCANA XAUUSD — {tf_key.upper()}</b>\n'
      f'Candle close: {mtime:%d-%m-%Y %H:%M} WIB | sumber Bybit: {SYMBOL}\n'
      f'Harga referensi + offset: {px:.2f} | ATR14: {a:.3f}\n'
      f'EMA9 {D["ema9"][i]+STATE["offset"]:.2f} | EMA21 {D["ema21"][i]+STATE["offset"]:.2f} | EMA50 {D["ema50"][i]+STATE["offset"]:.2f} | EMA200 {D["ema200"][i]+STATE["offset"]:.2f}\n'
      f'VWAP {D["vwap"][i]+STATE["offset"]:.2f} | RSI14 {r if r is not None else 0:.1f} | ADX {adx if adx is not None else 0:.1f} | MACD hist {mh:+.3f}\n'
      f'<b>Setup terkini:</b> {setup}\n{entry_text}\n\n'
      f'<b>Simulasi historis ({tf_key.upper()})</b>: {len(deals)} trade | TP {tps} | SL+ {slps} | BE+ {bes} | SL {sls} | TIMEOUT {timeouts} | OPEN {opens}\n'
      f'Win rate closed (hasil net > 0): {wr} | Net simulasi: {net:+.2f}R\n'
      f'BE/SL+ aktif: {"ON" if BE_ENABLED else "OFF"} | trigger BE +{BE_TRIGGER_R:g}R, SL+ +{SLPLUS_TRIGGER_R:g}R | lock minimal menutup biaya. Sinyal {tf_key.upper()} dieksekusi dengan candle M1; paper tracker live terpisah.\n'
      f'Ambigu (M1 stop-first): {ambiguous} | Entry = open candle timeframe berikutnya; biaya asumsi {max(SPREAD, PAPER_COST_XAU):g}; RR 1:{RR:g}. BE/SL+ yang terpicu pada candle baru aktif mulai candle M1 berikutnya.\n' 
      f'Orderflow candle {of_ratio(D,i):+.2f} | Delta trade {live.get("tdelta") if live.get("tdelta") is not None else "N/A"} | Orderbook {live.get("obi") if live.get("obi") is not None else "N/A"}\n'
      f'Filter trend pullback: ADX≥{TREND_MIN_ADX:g}, EMA gap≥{TREND_MIN_EMA_GAP_ATR:g} ATR, EMA50 slope {TREND_SLOPE_BARS} bar, break candle {"ON" if TREND_REQUIRE_BREAK else "OFF"}; veto exhaustion dekat swing.\n'
      f'Structure breakout/breakdown: lookback {STRUCTURE_LOOKBACK} bar, body≥{STRUCTURE_MIN_BODY_RATIO:.0%}, RVOL≥{STRUCTURE_MIN_RVOL:g} atau body≥{STRUCTURE_MIN_BODY_ATR:g} ATR; retest anti-chase aktif ({STRUCTURE_RETEST_WINDOW} bar).\n'
      f'OI perubahan 1 jam: {oi_chg:+.3f}% | Offset tampilan {STATE["offset"]:+.2f}\n'
      f'Simulasi strategi menggunakan candle Bybit, bukan histori order CFD broker / bukan order live. Bukan saran investasi.')
    # Compact per-trade audit: lets us check whether a TP was ever reached before the exit.
    if deals:
        lines=['<b>Audit trade terakhir (signal → entry next candle):</b>']
        for d in deals[-7:]:
            st=datetime.fromtimestamp(candle_close_ms(d['t'], tf_key)/1000,WIB).strftime('%d %H:%M')
            amb=' AMBIG' if d.get('ambiguous') else ''
            rv='OPEN' if d.get('r') is None else f'{d["r"]:+.2f}R'
            be_at = '-' if d.get('be_trigger_i') is None else f"{d['be_trigger_i']-d['entry_i']+1}"
            sp_at = '-' if d.get('slplus_trigger_i') is None else f"{d['slplus_trigger_i']-d['entry_i']+1}"
            extra = f" | exit-bar potential MFE/MAE {d.get('exit_bar_mfe_r',0):.2f}/{d.get('exit_bar_mae_r',0):.2f}R" if d.get('result') in ('TP','SL','BE+','SL+') else ''
            lines.append(f"{st} {'BUY' if d['side']=='L' else 'SELL'} {d['kind']}: sigC {d['signal_close']+STATE['offset']:.2f} -> E {d['entry']+STATE['offset']:.2f} (gap {d['entry_gap']:+.2f}) SL0 {d['sl']+STATE['offset']:.2f} SL aktif exit {d['active_sl']+STATE['offset']:.2f} TP {d['tp']+STATE['offset']:.2f} -> {d['result']}{amb} {rv} | BE bar {be_at}, SL+ bar {sp_at} | MFE/MAE pre-exit {d['mfe_r']:.2f}/{d['mae_r']:.2f}R{extra} | {d['bars_held']} bar eksekusi")
        text_out += '\n\n' + '\n'.join(lines)
    return text_out


def _tbl(rows):
    # Telegram HTML: karakter < > & di dalam tabel harus di-escape, kalau tidak sendPhoto/sendMessage 400.
    return '<pre>' + '\n'.join(_html.escape(str(r), quote=False) for r in rows) + '</pre>'


def _fmt_sig(v):
    if v is None: return 'NA'
    try: return f'{float(v):+.2f}'
    except Exception: return str(v)


def _diagnostic_caption(D, i, deals, tf_key='15m', live=None, candidate=None):
    """Caption ringkas berbentuk tabel. Versi panjang lama: _diagnostic_caption_full ('/rencana TF full')."""
    off = STATE['offset']; live = live or {}
    mtime = datetime.fromtimestamp(candle_close_ms(D['t'][i], tf_key)/1000, WIB)
    px = D['c'][i] + off; a = D['atr'][i] or 0
    cnt = lambda k: sum(d['result'] == k for d in deals)
    done = [d for d in deals if d['result'] in ('TP','SL','BE+','SL+','TIMEOUT')]
    wins = sum((d['r'] or 0) > 0 for d in done)
    wr = f'{wins/len(done)*100:.0f}%' if done else 'N/A'
    net = sum(d['r'] for d in deals if d['r'] is not None)
    r = D['rsi'][i] or 0; adx = D['adx'][i] or 0; mh = D['macd_hist'][i]
    oi = D['oi'][i]; oi4 = D['oi'][i-4] if i >= 4 else None
    oi_chg = ((oi-oi4)/oi4*100) if oi and oi4 else 0
    sep = '-'*31
    if candidate:
        side = 'BUY' if candidate['side'] == 'L' else 'SELL'
        e, s_, t = (candidate[k] + off for k in ('entry','sl','tp'))
        head = f'{"🟢" if side == "BUY" else "🔴"} <b>XAUUSD {tf_key.upper()} — {side} {candidate["kind"]}</b>'
        setup = [f'Entry   {e:.2f}', f'SL      {s_:.2f} ({s_-e:+.2f})', f'TP      {t:.2f} ({t-e:+.2f})', f'Skor {candidate["score"]}  RR 1:{RR:g}']
    else:
        head = f'⚪ <b>XAUUSD {tf_key.upper()} — WAIT</b>'
        setup = ['Belum ada setup valid']
    rows = [f'Harga   {px:.2f}  ATR {a:.2f}', f'Close   {mtime:%d-%m %H:%M} WIB', *setup, sep,
            f'EMA 9/21    {D["ema9"][i]+off:.1f}/{D["ema21"][i]+off:.1f}',
            f'EMA 50/200  {D["ema50"][i]+off:.1f}/{D["ema200"][i]+off:.1f}',
            f'RSI {r:.0f}  ADX {adx:.0f}  MACD {mh:+.2f}',
            f'Flow {of_ratio(D,i):+.2f} d {_fmt_sig(live.get("tdelta"))} ob {_fmt_sig(live.get("obi"))}',
            f'OI 1j   {oi_chg:+.2f}%', sep,
            f'{"Sim":<4}{"n":>3}{"TP":>4}{"BE+":>4}{"SL+":>4}{"SL":>4}{"TO":>4}',
            f'{tf_key.upper():<4}{len(deals):>3}{cnt("TP"):>4}{cnt("BE+"):>4}{cnt("SL+"):>4}{cnt("SL"):>4}{cnt("TIMEOUT"):>4}',
            f'Win {wr}  Net {net:+.2f}R']
    return f'{head}\n{_tbl(rows)}\nSimulasi candle {SYMBOL or "DEMO"}, bukan order live. /rencana {tf_key} full = detail.'


# /rencana chart and /scan use SAME selected timeframe for data/title/caption.
def do_rencana(args):
    full=any(str(x).lower()=='full' for x in args); args=[x for x in args if str(x).lower()!='full']
    raw_tf=args[0] if args else '15m'
    try: tf_key,(interval,_,tf_label)=_parse_tf(raw_tf)
    except ValueError as e:send(str(e));return
    send(f'🔎 Menyusun chart RENCANA XAUUSD {tf_label}...')
    try:
        sym=resolve_symbol(); rows=fetch_klines_tf(sym,tf_key,limit=1000)
        if len(rows)<80:raise RuntimeError(f'Candle closed hanya {len(rows)}; minimal 80 diperlukan untuk indikator.')
        D=build(rows,{})
        idx=len(rows)-1
        candidate=rencana_candidate(D,idx,live=None)
        sim_start=max(60,idx-500)
        sim_hold=min(MAX_BARS, max(8, int(96*_tf_millis_per_day(tf_key)/96)))
        if TF[tf_key][1] is not None and TF[tf_key][1] >= 60_000:
            exec_end_ms=candle_close_ms(D['t'][idx],tf_key)
            exec_start_ms=D['t'][min(sim_start+1,idx)]
            exec_target=max(1000,int((exec_end_ms-exec_start_ms)/60_000)+8)
            exec_rows=fetch_klines_history_tf(sym,'1m',exec_target)
            deals=simulate_trades_m1(D,exec_rows,start_i=sim_start,end_i=idx-1,max_hold=sim_hold,tf_key=tf_key)
        else:
            deals=simulate_trades(D,start_i=sim_start,end_i=idx-1,max_hold=sim_hold)
        # Signal TF and execution TF are now separate: levels are signalled on tf_key, managed on M1.
        png=_price_chart(D,idx,tf_key,STATE['offset'],deals,candidate,bars=110,diagnostic=not bool(candidate))
        caption=_diagnostic_caption(D,idx,deals,tf_key,{},candidate)
        full_text=_diagnostic_caption_full(D,idx,deals,tf_key,{},candidate) if full else None
        _send_png(png,caption,f'XAUUSD_RENCANA_{tf_key}_{datetime.fromtimestamp(D["t"][idx]/1000,WIB):%Y%m%d_%H%M}.png')
        if full_text: send(full_text,html_mode=True)
    except Exception as e:
        print('/rencana error:',e);send(f'<b>RENCANA ERROR</b>\n{str(e)[:450]}',html_mode=True)


# ───────── TELEGRAM ─────────
def _valid_token():
    if not TG_TOKEN:return False
    if any(ch in TG_TOKEN for ch in '\r\n\t '):
        print('TG_TOKEN tidak valid: terdeteksi whitespace/control char; periksa .env dan shell export.')
        return False
    return True

def tg(method,params,timeout=30):
    if not _valid_token():return None
    url=f'https://api.telegram.org/bot{TG_TOKEN}/{method}'
    data=urllib.parse.urlencode(params or {}).encode()
    req=urllib.request.Request(url,data=data if method!='getUpdates' or True else None,headers={'User-Agent':'XAUUSD-bot/1.0'})
    with urllib.request.urlopen(req,timeout=timeout) as r:return json.loads(r.read().decode('utf-8'))

_TLS = threading.local()
LEGACY_KIND_KEYS = {'SHADOW FAKEOUT': 'LEGACY_FAKEOUT', 'STRUCTURE BREAKOUT': 'LEGACY_BREAKOUT', 'STRUCTURE BREAKDOWN': 'LEGACY_BREAKDOWN',
                    'TREND PULLBACK': 'LEGACY_PULLBACK', 'RADAR LONG B/HOOK': 'LEGACY_RADAR'}
KIND_KEYS = {'COUNTER RETAIL': ('COUNTER_RETAIL',), 'SCALPER TREND': ('SCALPER',), 'ICHIMOKU 4H': ('ICHI_4H',), 'ICH M15': ('ICH_M15',)}


def _topic_env(key):
    v = (os.getenv('TG_TOPIC_' + key) or '').strip()
    return v if v.isdigit() else ''


def topic_for(*keys):
    """Topic pertama yang terisi dari TG_TOPIC_<KEY> menurut urutan kunci; jika kosong semua: TG_TOPIC_ID bawaan."""
    for k in keys:
        v = _topic_env(k)
        if v: return v
    return TG_TOPIC


def topic_for_kind(kind):
    if kind in LEGACY_KIND_KEYS: return topic_for(LEGACY_KIND_KEYS[kind], 'LEGACY')
    if kind in KIND_KEYS: return topic_for(*KIND_KEYS[kind])
    return topic_for('LEGACY')


def _eff_topic(topic=None):
    """Di dalam perintah: balas di topic tempat perintah diketik. Di luar perintah (alert otomatis): topic sinyalnya."""
    if getattr(_TLS, 'in_cmd', False):
        return str(getattr(_TLS, 'reply_topic', '') or TG_TOPIC or '')
    t = str(topic).strip() if topic not in (None, '') else ''
    return t or TG_TOPIC


def _with_reply(topic, fn, *a):
    _TLS.in_cmd = True; _TLS.reply_topic = str(topic or '')
    try: return fn(*a)
    finally: _TLS.in_cmd = False; _TLS.reply_topic = ''


def _run_cmd(text, thread_id=None):
    return _with_reply(thread_id, handle_cmd, text)


def topics_text():
    rows = [f'{"DEFAULT":<17}{TG_TOPIC or "-"}']
    keys = (['LEGACY', 'LEGACY_FAKEOUT', 'LEGACY_BREAKOUT', 'LEGACY_BREAKDOWN', 'LEGACY_PULLBACK', 'LEGACY_RADAR',
             'COUNTER_RETAIL', 'SCALPER', 'ICHI_4H', 'ICH_M15', 'SNIPER', 'PDF'] + [f'SNIPER_{p}' for p in LS_PAIRS] + [f'PDF_{p}' for p in LS_PAIRS])
    for k in keys: rows.append(f'{k:<17}{_topic_env(k) or "(ikut induk)"}')
    return ('<b>TOPIC TELEGRAM PER SINYAL</b>\n' + _tbl(rows) +
            '\nIsi lewat variabel TG_TOPIC_NAMA, contoh TG_TOPIC_ICH_M15. Induk: LEGACY_* ke LEGACY, SNIPER_* ke SNIPER, lalu DEFAULT (TG_TOPIC_ID). '
            'Ketik /topicid di dalam sebuah topic untuk melihat ID-nya.')


def send(text,html_mode=False,topic=None):
    clean=_html.unescape(re.sub(r'<[^>]+>','',text)) if html_mode else text
    print(clean)
    if not (TG_CHAT and _valid_token()):return
    for start in range(0,len(text),3900):
        p={'chat_id':TG_CHAT,'text':text[start:start+3900]}
        if html_mode:p['parse_mode']='HTML'
        _th=_eff_topic(topic)
        if _th:p['message_thread_id']=_th
        try:tg('sendMessage',p)
        except Exception as e:print('tg send:',e)

def _send_png(png,caption,filename='chart.png',topic=None):
    os.makedirs('charts',exist_ok=True)
    try:
        path=os.path.join('charts',filename)
        with open(path,'wb') as f:f.write(png)
        print('chart disimpan:',path)
    except Exception as e:print('chart simpan gagal:',e)
    if not (TG_CHAT and _valid_token()):send(caption,html_mode=True,topic=topic);return
    head,rest=caption,''
    if len(head)>1000:
        cut=head.rfind('\n',0,1000); cut= cut if cut>0 else 1000
        head,rest=caption[:cut],caption[cut+1:]
    boundary='----xauBot'+str(int(time.time()*1000))
    fields={'chat_id':TG_CHAT,'caption':head,'parse_mode':'HTML'}
    _th=_eff_topic(topic)
    if _th:fields['message_thread_id']=_th
    body=b''.join((f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n').encode() for k,v in fields.items())
    body+=(f'--{boundary}\r\nContent-Disposition: form-data; name="photo"; filename="{filename}"\r\nContent-Type: image/png\r\n\r\n').encode()+png+f'\r\n--{boundary}--\r\n'.encode()
    req=urllib.request.Request(f'https://api.telegram.org/bot{TG_TOKEN}/sendPhoto',data=body,headers={'Content-Type':f'multipart/form-data; boundary={boundary}'})
    try:
        with urllib.request.urlopen(req,timeout=60) as r:resp=json.loads(r.read().decode())
        if not resp.get('ok'):raise RuntimeError(resp.get('description','sendPhoto gagal'))
        print('chart berhasil dikirim:',filename)
    except Exception as e:
        print('kirim chart gagal:',e);send(caption,html_mode=True,topic=topic);return
    if rest:send(rest,html_mode=True,topic=topic)


def _signal_caption(sg,tf='15m'):
    side='BUY' if sg['side']=='L' else 'SELL'; off=STATE['offset']; dt=datetime.fromtimestamp(candle_close_ms(sg['t'], tf)/1000,WIB)
    e,s_,t=(sg[k]+off for k in ('entry','sl','tp'))
    rows=[f'TF/Sesi {tf.upper()} | {sg["session"]}', f'Waktu   {dt:%d-%m %H:%M} WIB', f'Entry   {e:.2f}',
          f'SL      {s_:.2f} ({s_-e:+.2f})', f'TP      {t:.2f} ({t-e:+.2f})',
          f'Skor {sg["score"]}  RR 1:{sg.get("rr", RR):g}  ATR {sg["atr"]:.2f}']
    if sg.get('z5'):
        z15, z5, o_ = sg['z15'], sg['z5'], STATE['offset']
        rows += ['-'*31, f'Trend H4 {"naik" if sg["side"] == "L" else "turun"}  garis {sg["tl"]+o_:.1f}',
                 f'FVG M15 {z15[0]+o_:.2f}-{z15[1]+o_:.2f}', f'FVG M5  {z5[0]+o_:.2f}-{z5[1]+o_:.2f}']
        if SCALP_TRAIL: rows.append(f'SL+ trail: +{SCALP_TRAIL_TRIGGER_R:g}R, tiap {SCALP_TRAIL_STEP_R:g}R')
    if sg.get('lvl') is not None:
        o_ = STATE['offset']
        rows += ['-'*31, f'Level   {sg["lvl"]+o_:.2f} (x{sg["n"]})', f'Sweep   {sg["pen"]:.2f} lewat level', 'Stop-hunt lalu close balik']
        if SCALP_TRAIL: rows.append(f'SL+ trail: +{SCALP_TRAIL_TRIGGER_R:g}R, tiap {SCALP_TRAIL_STEP_R:g}R')
    return (f'{"🟢" if side=="BUY" else "🔴"} <b>XAUUSD {side} — {sg["kind"]}</b>\n{_tbl(rows)}\n'
            f'Sumber {SYMBOL}. Simulasi/alert, bukan order live. Cek spread.')


def do_scan(force=True, announce_signal=True):
    # LIVE scan is M5. /bt remains the existing M15 historical test.
    tf_key='5m'; sym=resolve_symbol(); rows=fetch_recent(sym,300,tf=tf_key)
    if len(rows)<80:raise RuntimeError(f'Candle closed M5 kurang dari 80 (sekarang {len(rows)}).')
    oi,tdel,obi={},None,None
    with ThreadPoolExecutor(max_workers=3) as ex:
        f1=ex.submit(fetch_oi_recent,sym); f2=ex.submit(trades_delta,sym); f3=ex.submit(book_imbalance,sym)
        try:oi=f1.result()
        except Exception as e:print('oi:',e)
        try:tdel=f2.result()
        except Exception as e:print('delta:',e)
        try:obi=f3.result()
        except Exception as e:print('book:',e)
    D=build(rows,oi); idx=len(rows)-1; live={'tdelta':tdel,'obi':obi}
    candidate=rencana_candidate(D,idx,live=live) if LEGACY_ENABLE else None
    if not candidate: candidate=cr_live(sym,rows)
    if not candidate: candidate=scalp_live(sym,rows)
    sim_start=max(60,idx-180)
    exec_end_ms=candle_close_ms(D['t'][idx],tf_key)
    exec_start_ms=D['t'][min(sim_start+1,idx)]
    exec_target=max(1000,int((exec_end_ms-exec_start_ms)/60_000)+8)
    exec_rows=fetch_klines_history_tf(sym,'1m',exec_target)
    # MAX_BARS is historically expressed in M15 bars; scale horizon to M5 bars.
    deals=simulate_trades_m1(D,exec_rows,start_i=sim_start,end_i=idx-1,max_hold=MAX_BARS*3,tf_key=tf_key)
    png=_price_chart(D,idx,tf_key,STATE['offset'],deals,candidate,bars=110,diagnostic=not bool(candidate))
    caption=_diagnostic_caption(D,idx,deals,tf_key,live,candidate)
    _send_png(png,caption,f'XAUUSD_SCAN_{tf_key}_{datetime.fromtimestamp(D["t"][idx]/1000,WIB):%Y%m%d_%H%M}.png')
    with LOCK:STATE['last_ts']=D['t'][idx]
    save_state()
    if candidate:
        added=register(candidate)
        if added: print('paper tracker: signal registered', candidate['kind'], candidate['side'])
    return candidate,D,idx


def fp(x):return f'{x+STATE["offset"]:.2f}'

def _exit_tag(a, result):
    if result == 'SL':
        stage = int(a.get('be_stage', 0) or 0)
        return 'SL+' if stage >= 2 else ('BE+' if stage == 1 else 'SL')
    return result


def build_event(a,tag,px,capital_event=None):
    d=1 if a['side']=='L' else -1
    pnl=(px-a['entry'])*d
    initial_sl=float(a.get('initial_sl', a.get('sl', a['entry'])))
    risk=abs(float(a['entry'])-initial_sl)
    st=datetime.fromtimestamp(candle_close_ms(a['sig_t'], '15m')/1000,WIB)
    exit_kind=_exit_tag(a,tag)
    if exit_kind=='TP':emoji='✅';title='TP HIT'
    elif exit_kind=='SL+':emoji='🟢';title='SL+ — PROFIT TERKUNCI'
    elif exit_kind=='BE+':emoji='🟡';title='BE+ — PROFIT KECIL TERKUNCI'
    elif exit_kind=='SL':emoji='❌';title='SL HIT'
    else:emoji='⏱';title='TIMEOUT'+(' • profit' if pnl>0 else (' • rugi' if pnl<0 else ''))
    stop=float(a.get('active_sl',a.get('sl',0)))
    lines=[f'<b>{emoji} {title} — XAUUSD</b>',
           f'Side {"BUY" if d==1 else "SELL"} | Setup {a.get("kind","?")}',
           f'Entry {fp(a["entry"])} | Exit {fp(px)}',
           f'Hasil harga {pnl:+.2f} ({pnl/max(risk,1e-9):+.2f}R sebelum biaya)',
           f'Stop akhir {fp(stop)} | Mode {a.get("stop_mode","INITIAL")}']
    if capital_event:
        lines.append(f'Modal paper {_rp(capital_event.get("balance_after",0))} | P/L {_rp(capital_event.get("pnl_idr",0))}')
        if capital_event.get('topup_idr'): lines.append(f'Auto top-up {_rp(capital_event["topup_idr"])}')
    lines.append(f'Signal close {st:%d-%m %H:%M} WIB')
    lines.append('Paper tracker dari candle Bybit; bukan rekaman eksekusi CFD dan bukan order live.')
    return '\n'.join(lines)


def register(sg):
    signal_id=f"{int(sg['t'])}|{sg.get('kind','?')}|{sg['side']}"
    with LOCK:
        seen=STATE.setdefault('seen_signals', [])
        if signal_id in seen or any(a.get('id')==signal_id for a in STATE.get('active',[])):
            return False
        initial_sl=float(sg['sl']); entry=float(sg['entry'])
        risk=abs(entry-initial_sl)
        if risk <= 0: return False
        a={'id':signal_id,'side':sg['side'],'kind':sg['kind'],'entry':entry,
           'dist':risk,'initial_sl':initial_sl,'active_sl':initial_sl,'sl':initial_sl,
           'tp':float(sg['tp']),'be_stage':0,'stop_mode':'INITIAL','lot':TRADE_LOT,
           'bars_held':0,'ts':time.time(),'sig_t':int(sg['t']),
           # Do not inspect the 15m signal candle; first eligible 1m bar starts at candle close.
           'tf_ms':int(sg.get('tf_ms',300000)),'max_hold_m1':sg.get('max_hold_m1'),
           'last_checked_ts':int(sg['t'])+int(sg.get('tf_ms',300000))-60000}
        STATE['active'].append(a)
        seen.append(signal_id); STATE['seen_signals']=seen[-500:]
        if sg.get('zone_id'): STATE['scalp_seen']=(STATE.get('scalp_seen',[])+[sg['zone_id']])[-300:]
        CAPITAL.register_entry()
    save_state()
    return True


def _round_stop(value, side):
    tick=PAPER_TICK
    # Round a protective stop conservatively: long stops down, short stops up.
    if side=='L': return math.floor(value/tick+1e-10)*tick
    return math.ceil(value/tick-1e-10)*tick


def _manage_active_bar(a, op, hi, lo, cl):
    """Process one CLOSED 1m candle; stop/TP are checked before BE/SL+ triggers.

    A stop change caused by this candle only becomes active on the next candle, preventing
    intrabar look-ahead when OHLC does not reveal the price path.
    Returns (status, exit_price) when closed, otherwise None.
    """
    side=a.get('side','L'); entry=float(a['entry'])
    initial_sl=float(a.get('initial_sl',a.get('sl',entry)))
    active_sl=float(a.get('active_sl',initial_sl)); tp=float(a['tp'])
    risk=abs(entry-initial_sl)
    a['bars_held']=int(a.get('bars_held',0))+1
    if risk<=0:
        return ('TIMEOUT',cl)

    # Conservative sequence: if stop and target both appear in the same 1m bar, stop wins.
    if side=='L':
        if lo<=active_sl: return ('SL', float(op if op<active_sl else active_sl))
        if hi>=tp: return ('TP', float(tp))
        favorable=hi-entry
    else:
        if hi>=active_sl: return ('SL', float(op if op>active_sl else active_sl))
        if lo<=tp: return ('TP', float(tp))
        favorable=entry-lo

    if a.get('fixed'):
        pass
    elif a.get('kind') in SCALP_KINDS and SCALP_TRAIL:
        min_cost_lock=max(SPREAD, PAPER_COST_XAU) + 2.0*PAPER_TICK
        fav_r=favorable/risk
        if fav_r >= SCALP_TRAIL_TRIGGER_R:
            steps=math.floor((fav_r-SCALP_TRAIL_GAP_R)/SCALP_TRAIL_STEP_R+1e-9)
            lock_dist=max(steps*SCALP_TRAIL_STEP_R*risk, min_cost_lock)
            target=entry + (lock_dist if side=='L' else -lock_dist)
            new_sl=max(active_sl,target) if side=='L' else min(active_sl,target)
            if abs(new_sl-active_sl)>1e-12:
                a['active_sl']=_round_stop(new_sl,side); a['sl']=a['active_sl']
                a['be_stage']=max(int(a.get('be_stage',0) or 0), 2 if lock_dist>=0.3*risk else 1)
                a['stop_mode']='TRAIL'
    elif BE_ENABLED:
        stage=int(a.get('be_stage',0) or 0)
        new_stage=stage; new_sl=active_sl
        # Lock enough price distance to cover the configured spread/paper costs,
        # plus tick rounding. A nominal +0.05R is not necessarily net-positive
        # when the risk distance is small.
        min_cost_lock=max(SPREAD, PAPER_COST_XAU) + 2.0*PAPER_TICK
        if favorable >= SLPLUS_TRIGGER_R*risk:
            new_stage=2
            lock_dist=max(SLPLUS_LOCK_R*risk,min_cost_lock)
            target=entry + (lock_dist if side=='L' else -lock_dist)
            if side=='L': new_sl=max(new_sl,target)
            else: new_sl=min(new_sl,target)
        elif favorable >= BE_TRIGGER_R*risk and stage < 1:
            new_stage=1
            lock_dist=max(BE_OFFSET_R*risk,min_cost_lock)
            target=entry + (lock_dist if side=='L' else -lock_dist)
            if side=='L': new_sl=max(new_sl,target)
            else: new_sl=min(new_sl,target)
        if new_stage != stage or abs(new_sl-active_sl)>1e-12:
            a['active_sl']=_round_stop(new_sl,side)
            a['sl']=a['active_sl']
            a['be_stage']=new_stage
            a['stop_mode']='SL+' if new_stage>=2 else ('BE+' if new_stage==1 else 'INITIAL')
    # Match the combo-bot's max holding horizon: MAX_BARS is expressed in 15m bars.
    hold_lim = int(a.get('max_hold_m1') or (SCALP_MAX_BARS*5 if a.get('kind') in SCALP_KINDS else MAX_BARS*15))   # dalam candle M1
    if int(a.get('bars_held',0)) >= max(1,hold_lim):
        return ('TIMEOUT',float(cl))
    return None


def check_active():
    with LOCK:active=[a for a in STATE.get('active',[]) if isinstance(a,dict) and 'entry' in a]
    if not active:return
    sym=resolve_symbol()
    rows=api('/v5/market/kline',{'category':'linear','symbol':sym,'interval':'1','limit':200}).get('list',[])
    now=now_ms()
    # Bybit kline: [start, open, high, low, close, volume, turnover]. Ignore the still-open 1m candle.
    candles=sorted((int(r[0]),float(r[1]),float(r[2]),float(r[3]),float(r[4]))
                    for r in rows if len(r)>=5 and int(r[0])+60000<=now)
    done=set()
    for a in active:
        result=None; exit_px=None; exit_ts=None
        signal_close=int(a.get('sig_t',0))+int(a.get('tf_ms',900000))
        cursor=int(a.get('last_checked_ts',signal_close-60000))
        for ts,op,hi,lo,cl in candles:
            if ts < signal_close or ts <= cursor: continue
            outcome=_manage_active_bar(a,op,hi,lo,cl)
            a['last_checked_ts']=ts
            if outcome:
                result,exit_px=outcome; exit_ts=datetime.fromtimestamp((ts+60000)/1000,timezone.utc).isoformat()
                break
        if not result and candles and time.time()-float(a.get('ts',time.time())) > MAX_BARS*900:
            # Recovery fallback if the bot was offline longer than the holding horizon.
            result='TIMEOUT';exit_px=candles[-1][4]
            exit_ts=datetime.fromtimestamp((candles[-1][0]+60000)/1000,timezone.utc).isoformat()
        if result:
            ce=CAPITAL.settle(a,result,exit_px,exit_ts or datetime.now(timezone.utc).isoformat())
            send(build_event(a,result,exit_px,ce),html_mode=True,topic=topic_for_kind(a.get('kind')))
            done.add(a.get('id',a.get('ts')))
            with LOCK:
                if result=='TP':STATE['tp']=int(STATE.get('tp',0))+1
                elif result=='SL':
                    if int(a.get('be_stage',0))>=2: STATE['slp']=int(STATE.get('slp',0))+1
                    elif int(a.get('be_stage',0))==1: STATE['be']=int(STATE.get('be',0))+1
                    else: STATE['sl']=int(STATE.get('sl',0))+1
                elif result=='TIMEOUT':STATE['timeout']=int(STATE.get('timeout',0))+1
        else:
            # Preserve latest active stop, stage, and cursor after each tracker pass.
            pass
    with LOCK:STATE['active']=[a for a in STATE.get('active',[]) if a.get('id',a.get('ts')) not in done]
    save_state()


# ───────── ICHIMOKU 4H AUTO (paper/alert; tanpa order live) ─────────
# Aturan = hasil riset: TK cross + harga di luar cloud + konfirmasi chikou; SL di Kijun;
# exit saat close 4H menembus Kijun (tanpa TP tetap); satu posisi paper pada satu waktu.
ICHI_CAPITAL = CapitalTracker(ICHI_CAPITAL_FILE)


def _ichi_mid(D, n, k):
    return (max(D['h'][k-n+1:k+1]) + min(D['l'][k-n+1:k+1])) / 2


def _ichi_at(D, i):
    """Tenkan, Kijun, cloud top, cloud bottom di candle i (cloud = span yang dihitung 26 bar sebelumnya)."""
    t, k = _ichi_mid(D, 9, i), _ichi_mid(D, 26, i)
    j = i - 26
    sa = (_ichi_mid(D, 9, j) + _ichi_mid(D, 26, j)) / 2
    sb = _ichi_mid(D, 52, j)
    return t, k, max(sa, sb), min(sa, sb)


def _ichi_attach(D):
    n = len(D['c'])
    ten, kij, ctop, cbot = [None]*n, [None]*n, [None]*n, [None]*n
    for i in range(n):
        if i >= 8: ten[i] = _ichi_mid(D, 9, i)
        if i >= 25: kij[i] = _ichi_mid(D, 26, i)
        if i >= 78: ctop[i], cbot[i] = _ichi_at(D, i)[2:]
    D.update(tenkan=ten, kijun=kij, cloud_top=ctop, cloud_bot=cbot)


def ichi_setup(D, i):
    if i < 80: return None
    c = D['c'][i]
    t, k, ct, cb = _ichi_at(D, i)
    t0, k0 = _ichi_mid(D, 9, i-1), _ichi_mid(D, 26, i-1)
    ref = D['c'][i-26]                       # konfirmasi chikou: close sekarang vs harga 26 bar lalu
    if t0 <= k0 and t > k and c > ct and c > ref: side, sl = 'L', k*0.999
    elif t0 >= k0 and t < k and c < cb and c < ref: side, sl = 'S', k*1.001
    else: return None
    if (side == 'L' and sl >= c) or (side == 'S' and sl <= c): return None
    if abs(c-sl)/c > ICHI_MAX_SL_PCT: return None
    if (ICHI_SIDES == 'long' and side == 'S') or (ICHI_SIDES == 'short' and side == 'L'): return None
    return {'side': side, 'kind': 'ICHIMOKU 4H', 'entry': c, 'sl': sl, 'tp': k, 'tp_label': 'TRAIL',
            'score': '-', 't': D['t'][i], 'atr': D['atr'][i] or 0.0, 'session': '-',
            'tenkan': t, 'kijun': k, 'cloud_top': ct, 'cloud_bot': cb}


def _ichi_live_price(sym, fallback):
    try: return float(api('/v5/market/tickers', {'category': 'linear', 'symbol': sym})['list'][0]['lastPrice'])
    except Exception as e:
        print('ichi ticker:', e); return fallback


def _ichi_register(sg, sym):
    sid = f"ICHI|{int(sg['t'])}|{sg['side']}"
    entry = _ichi_live_price(sym, sg['entry'])
    sl = float(sg['sl']); d = abs(entry - sl)
    if d <= 0 or (sg['side'] == 'L' and sl >= entry) or (sg['side'] == 'S' and sl <= entry): return None
    with LOCK:
        seen = STATE.setdefault('ichi_seen', [])
        if sid in seen or STATE.get('ichi_active'): return None
        risk_usd = float(ICHI_CAPITAL.data.get('balance', CAPITAL_START_IDR)) / USD_IDR * ICHI_RISK_PCT
        lot = round(risk_usd / (d * XAU_UNITS_PER_LOT), 6)
        if lot <= 0: return None
        a = {'id': sid, 'side': sg['side'], 'kind': sg['kind'], 'entry': entry, 'initial_sl': sl,
             'active_sl': sl, 'sl': sl, 'lot': lot, 'be_stage': 0, 'sig_t': int(sg['t']),
             'ts': time.time(), 'last_checked_ts': int(sg['t']) + ICHI_MS - 60000}
        STATE.setdefault('ichi_active', []).append(a)
        STATE['ichi_seen'] = (seen + [sid])[-200:]
        ICHI_CAPITAL.register_entry()
    save_state()
    return a


def _ichi_caption(sg, a=None, note=''):
    off = STATE['offset']; side = 'BUY' if sg['side'] == 'L' else 'SELL'
    dt = datetime.fromtimestamp(candle_close_ms(sg['t'], '4h')/1000, WIB)
    e = (a['entry'] if a else sg['entry']) + off; s_ = (a['initial_sl'] if a else sg['sl']) + off
    sep = '-'*31
    rows = [f'Close 4H {dt:%d-%m %H:%M} WIB', f'Entry   {e:.2f}', f'SL      {s_:.2f} ({s_-e:+.2f})',
            f'Risiko  {abs(e-s_)/e*100:.2f}% dari harga', 'Exit    close 4H tembus Kijun', sep,
            f'Tenkan  {sg["tenkan"]+off:.2f}', f'Kijun   {sg["kijun"]+off:.2f}',
            f'Cloud   {sg["cloud_bot"]+off:.1f} - {sg["cloud_top"]+off:.1f}',
            f'Chikou  {"atas" if side == "BUY" else "bawah"} harga 26 bar lalu']
    if a: rows += [sep, f'Lot paper {a["lot"]:.6f} (risiko {ICHI_RISK_PCT*100:g}%)']
    return (f'{"🟢" if side == "BUY" else "🔴"} <b>XAUUSD {side} — ICHIMOKU 4H</b>\n{_tbl(rows)}\n'
            + (note or 'Alert/paper, bukan order live. Edge belum teruji live.'))


def _ichi_wait_caption(D, i):
    off = STATE['offset']; c = D['c'][i]; t, k, ct, cb = _ichi_at(D, i)
    dt = datetime.fromtimestamp(candle_close_ms(D['t'][i], '4h')/1000, WIB)
    pos = 'di atas cloud' if c > ct else ('di bawah cloud' if c < cb else 'di dalam cloud')
    rows = [f'Close 4H {dt:%d-%m %H:%M} WIB', f'Harga   {c+off:.2f}', f'Posisi  {pos}',
            f'TK      {"Tenkan > Kijun" if t > k else "Tenkan < Kijun"}',
            f'Tenkan  {t+off:.2f}', f'Kijun   {k+off:.2f}', f'Cloud   {cb+off:.1f} - {ct+off:.1f}']
    t0, k0 = _ichi_mid(D, 9, i-1), _ichi_mid(D, 26, i-1); ref = D['c'][i-26]
    ok = lambda v: 'OK' if v else '--'
    rows += ['-'*26, f'{"Syarat sinyal":<17}{"BUY":>4}{"SELL":>5}',
             f'{"TK cross baru":<17}{ok(t0 <= k0 and t > k):>4}{ok(t0 >= k0 and t < k):>5}',
             f'{"Close luar cloud":<17}{ok(c > ct):>4}{ok(c < cb):>5}',
             f'{"Chikou konfirmasi":<17}{ok(c > ref):>4}{ok(c < ref):>5}',
             'Sinyal butuh ketiganya OK', '-'*26]
    act = STATE.get('ichi_active', [])
    rows.append(f'Posisi paper aktif: {len(act)}')
    for a in act:
        rows += [f'{"BUY" if a["side"] == "L" else "SELL"} {a["entry"]+off:.2f} SL {a["active_sl"]+off:.2f}', f'  trail Kijun {k+off:.2f}']
    return f'⚪ <b>XAUUSD — ICHIMOKU 4H WAIT</b>\n{_tbl(rows)}\nTidak ada sinyal baru. Alert/paper saja.'


def _ichi_exit_caption(a, status, px, ev):
    d = 1 if a['side'] == 'L' else -1; pnl = (px - a['entry']) * d; risk = abs(a['entry'] - a['initial_sl'])
    emoji = '❌' if status == 'SL' else ('✅' if pnl > 0 else '🟡')
    title = 'SL HIT' if status == 'SL' else 'EXIT TRAIL KIJUN'
    rows = [f'Side    {"BUY" if d == 1 else "SELL"}  lot {a["lot"]:.6f}', f'Entry   {fp(a["entry"])}',
            f'Exit    {fp(px)} ({"SL" if status == "SL" else "trail"})',
            f'Hasil   {pnl:+.2f}  {pnl/max(risk,1e-9):+.2f}R', f'Bersih  {ev["r"]:+.2f}R setelah biaya',
            f'P/L     {_rp(ev["pnl_idr"])}', f'Saldo   {_rp(ev["balance_after"])}']
    return f'{emoji} <b>ICHIMOKU 4H — {title}</b>\n{_tbl(rows)}\nPaper tracker, bukan order live.'


def _close_ichi(a, status, px, ts_ms):
    ev = ICHI_CAPITAL.settle(a, status, px, datetime.fromtimestamp(ts_ms/1000, timezone.utc).isoformat())
    with LOCK:
        STATE['ichi_active'] = [x for x in STATE.get('ichi_active', []) if x.get('id') != a.get('id')]
    save_state()
    if ev: send(_ichi_exit_caption(a, status, px, ev), html_mode=True, topic=topic_for('ICHI_4H'))


def check_ichi_sl():
    """Cek SL dengan candle M1 yang sudah tutup (dipanggil tracker_loop)."""
    with LOCK: act = [a for a in STATE.get('ichi_active', []) if isinstance(a, dict)]
    if not act: return
    sym = resolve_symbol()
    rows = api('/v5/market/kline', {'category': 'linear', 'symbol': sym, 'interval': '1', 'limit': 200}).get('list', [])
    now = now_ms()
    candles = sorted((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]))
                     for r in rows if len(r) >= 5 and int(r[0]) + 60000 <= now)
    for a in act:
        start = int(a['sig_t']) + ICHI_MS; cur = int(a.get('last_checked_ts', start - 60000))
        for ts, op, hi, lo, cl in candles:
            if ts < start or ts <= cur: continue
            a['last_checked_ts'] = ts; sl = float(a['active_sl'])
            if a['side'] == 'L' and lo <= sl: _close_ichi(a, 'SL', float(min(op, sl)), ts + 60000); break
            if a['side'] == 'S' and hi >= sl: _close_ichi(a, 'SL', float(max(op, sl)), ts + 60000); break
    save_state()


def _ichi_manage(D, idx):
    """Jaring pengaman SL + exit trail Kijun berdasarkan candle 4H yang sudah tutup."""
    with LOCK: act = [a for a in STATE.get('ichi_active', []) if isinstance(a, dict)]
    for a in act:
        start = int(a['sig_t']) + ICHI_MS; long_ = a['side'] == 'L'; sl = float(a['active_sl'])
        for k in range(idx + 1):
            if D['t'][k] < start: continue
            op, hi, lo, cl = D['o'][k], D['h'][k], D['l'][k], D['c'][k]; end = D['t'][k] + ICHI_MS
            if (long_ and lo <= sl) or (not long_ and hi >= sl):
                _close_ichi(a, 'SL', float(min(op, sl) if long_ else max(op, sl)), end); break
            kj = D['kijun'][k]
            if kj is not None and ((long_ and cl < kj) or (not long_ and cl > kj)):
                _close_ichi(a, 'TRAIL', float(D['o'][k+1] if k < idx else cl), end); break


def ichi_job(force=False, entries=True):
    sym = resolve_symbol(); rows = fetch_klines_tf(sym, ICHI_TF, 500)
    if len(rows) < 100: raise RuntimeError(f'Candle 4H hanya {len(rows)}; minimal 100.')
    D = build(rows, {}); _ichi_attach(D); idx = len(rows) - 1; ts = D['t'][idx]
    _ichi_manage(D, idx)
    fresh = ts != STATE.get('ichi_last_ts'); sg = None
    if entries and (fresh or force):
        sg = ichi_setup(D, idx); a = None; note = ''
        if sg:
            a = _ichi_register(sg, sym)
            if not a: note = 'Sinyal tidak dicatat: posisi paper masih aktif / sudah pernah dicatat.'
        if force or (sg and a):
            png = _price_chart(D, idx, ICHI_TF, STATE['offset'], [], sg, bars=110, diagnostic=not bool(sg))
            cap = _ichi_caption(sg, a, note) if sg else _ichi_wait_caption(D, idx)
            _send_png(png, cap, f'XAUUSD_ICHI_4h_{datetime.fromtimestamp(ts/1000, WIB):%Y%m%d_%H%M}.png', topic=topic_for('ICHI_4H'))
    if entries and fresh:
        STATE['ichi_last_ts'] = ts; save_state()
    return sg, D, idx


def ichi_loop():
    _guard('ichi-init', lambda: ichi_job(force=False, entries=False))   # kelola posisi yang masih terbuka setelah restart
    while True:
        now = now_ms(); boundary = (now // ICHI_MS + 1) * ICHI_MS
        time.sleep(max(0, (boundary - now) / 1000) + SCAN_DELAY + 3)
        _guard('ichi', lambda: ichi_job(force=False))


def _ichi_status_text():
    d = ICHI_CAPITAL.data; ev = d.get('events', []); n = len(ev)
    wins = sum(float(e.get('pnl_idr', 0)) > 0 for e in ev); net_r = sum(float(e.get('r', 0)) for e in ev)
    rows = [f'Status   {"ON" if ICHI_ENABLE else "OFF"} | sisi {ICHI_SIDES}', f'Aktif    {len(STATE.get("ichi_active", []))}',
            f'Selesai  {n}  win {wins}', f'Net      {net_r:+.2f}R',
            f'Saldo    {_rp(d.get("balance", CAPITAL_START_IDR))}', f'Modal    {_rp(d.get("initial_capital", CAPITAL_START_IDR))}',
            f'Risiko   {ICHI_RISK_PCT*100:g}% per trade']
    return '<b>ICHIMOKU 4H — PAPER</b>\n' + _tbl(rows) + '\nPaper/alert saja, bukan order live.'


# ───────── SCALPER TREND: trendline H4 + FVG M15 + FVG M5 di dalam FVG M15 ─────────
# Tafsiran metode: (1) arah dari trendline H4, (2) cari FVG M15 searah tren yang belum termitigasi,
# (3) turun ke M5, cari FVG M5 searah yang berada di dalam FVG M15, (4) entry saat candle M5 retest
# FVG M5 lalu ditutup menolak zona. SL di balik FVG M5/ekor candle, TP = RR x jarak SL.
SCALP_ENABLE = os.getenv('SCALP_ENABLE', '0') != '0'   # default OFF: backtest tidak menunjukkan edge
SCALP_RR = max(0.5, float(os.getenv('SCALP_RR', str(RR))))
SCALP_SL_MIN = float(os.getenv('SCALP_SL_MIN_USD', '0.8'))     # SL terlalu sempit dimakan spread
SCALP_SL_MAX = float(os.getenv('SCALP_SL_MAX_USD', '3.0'))     # SL lebih lebar dari ini dilewati
SCALP_FVG15_BARS = 96      # FVG M15 dicari dalam 24 jam terakhir
SCALP_FVG5_BARS = 36       # FVG M5 dicari dalam 3 jam terakhir
SCALP_GAP15_ATR = 0.2
SCALP_GAP5_ATR = 0.15
SCALP_SL_BUF_ATR = 0.2
SCALP_MAX_BARS = 48        # batas waktu trade: 48 candle M5 (4 jam)
# SL+ bergeser terus: setelah profit >= TRIGGER (R), SL dinaikkan tiap kelipatan STEP (R) dengan jarak GAP (R)
# di belakang profit tertinggi; tidak pernah turun; tidak pernah di bawah entry + biaya.
SCALP_KINDS = ('SCALPER TREND', 'COUNTER RETAIL')
SCALP_TRAIL = os.getenv('SCALP_TRAIL', '1') != '0'
SCALP_TRAIL_TRIGGER_R = max(0.2, float(os.getenv('SCALP_TRAIL_TRIGGER_R', '0.8')))
SCALP_TRAIL_GAP_R = max(0.2, float(os.getenv('SCALP_TRAIL_GAP_R', '0.6')))
SCALP_TRAIL_STEP_R = max(0.05, float(os.getenv('SCALP_TRAIL_STEP_R', '0.2')))


def _atr_rows(rows, p=14):
    seg = rows[-(p+1):]
    if len(seg) < p + 1: return 0.0
    tr = [max(seg[k][2]-seg[k][3], abs(seg[k][2]-seg[k-1][4]), abs(seg[k][3]-seg[k-1][4])) for k in range(1, len(seg))]
    return sum(tr) / len(tr)


def _pivots(h, l, n=2):
    hi, lo = [], []
    for k in range(n, len(h) - n):
        if h[k] > max(h[k-n:k]) and h[k] >= max(h[k+1:k+n+1]): hi.append((k, h[k]))
        if l[k] < min(l[k-n:k]) and l[k] <= min(l[k+1:k+n+1]): lo.append((k, l[k]))
    return hi, lo


def h4_trend(rows4):
    """+1 naik / -1 turun / 0 netral. Trendline lewat 2 swing low (naik) atau 2 swing high (turun) terakhir;
    harga close terakhir belum menembus garis itu."""
    if len(rows4) < 40: return 0, None
    h = [r[2] for r in rows4]; l = [r[3] for r in rows4]; last = len(rows4) - 1; px = rows4[-1][4]
    hi, lo = _pivots(h, l, 2)
    if len(hi) < 2 or len(lo) < 2: return 0, None
    (i1, p1), (i2, p2) = lo[-2], lo[-1]; (j1, q1), (j2, q2) = hi[-2], hi[-1]
    if p2 > p1 and q2 > q1:
        line = p2 + (p2 - p1) / (i2 - i1) * (last - i2)
        if px > line: return 1, line
    if p2 < p1 and q2 < q1:
        line = q2 + (q2 - q1) / (j2 - j1) * (last - j2)
        if px < line: return -1, line
    return 0, None


def find_fvgs(rows, side, max_age, min_gap):
    """FVG 3-candle searah side ('L' bullish / 'S' bearish) yang belum termitigasi. Terbaru dulu."""
    n = len(rows); out = []
    for k in range(max(2, n - max_age), n):
        h0, l0 = rows[k-2][2], rows[k-2][3]; h2, l2 = rows[k][2], rows[k][3]
        if side == 'L' and l2 > h0 and (l2 - h0) >= min_gap: lo_, hi_ = h0, l2
        elif side == 'S' and h2 < l0 and (l0 - h2) >= min_gap: lo_, hi_ = h2, l0
        else: continue
        mitig = any((side == 'L' and rows[m][4] < lo_) or (side == 'S' and rows[m][4] > hi_) for m in range(k + 1, n))
        if not mitig: out.append({'k': k, 't': rows[k][0], 'lo': lo_, 'hi': hi_})
    return out[::-1]


def scalp_ctx(rows15, rows4):
    trend, tl = h4_trend(rows4)
    if trend == 0 or len(rows15) < 40: return None
    side = 'L' if trend == 1 else 'S'
    z15 = find_fvgs(rows15, side, SCALP_FVG15_BARS, SCALP_GAP15_ATR * _atr_rows(rows15))
    return {'side': side, 'tl': tl, 'z15': z15} if z15 else None


def scalp_candidate(rows5, ctx, seen=(), stats=None):
    if not ctx or len(rows5) < 60: return None
    i = len(rows5) - 1; side = ctx['side']; atr5 = _atr_rows(rows5)
    if atr5 <= 0: return None
    z5s = find_fvgs(rows5, side, SCALP_FVG5_BARS, SCALP_GAP5_ATR * atr5)
    t_, o, h, l, c = rows5[i][:5]; tol = 0.1 * atr5
    nested = retest = False; sl_flag = None; out = None
    for z15 in ctx['z15']:
        for z5 in z5s:
            zid = f"SC|{z5['t']}|{side}"
            if zid in seen or z5['t'] < z15['t'] or z5['k'] >= i: continue
            if not (z5['lo'] >= z15['lo'] - tol and z5['hi'] <= z15['hi'] + tol): continue
            nested = True
            if side == 'L' and l <= z5['hi'] and c > z5['lo'] and c > o:
                sl = min(z5['lo'], l) - SCALP_SL_BUF_ATR * atr5; risk = c - sl
            elif side == 'S' and h >= z5['lo'] and c < z5['hi'] and c < o:
                sl = max(z5['hi'], h) + SCALP_SL_BUF_ATR * atr5; risk = sl - c
            else: continue
            retest = True
            if risk < SCALP_SL_MIN: sl_flag = 'narrow'; continue
            if risk > SCALP_SL_MAX: sl_flag = 'wide'; continue
            tp = c + risk * SCALP_RR if side == 'L' else c - risk * SCALP_RR
            out = {'side': side, 'kind': 'SCALPER TREND', 'entry': c, 'sl': sl, 'tp': tp, 'score': '-',
                   't': t_, 'atr': atr5, 'session': session_for(t_ // 1000), 'dist': risk, 'i': i, 'live': {},
                   'tf_ms': 300_000, 'rr': SCALP_RR, 'zone_id': zid,
                   'z15': (z15['lo'], z15['hi']), 'z5': (z5['lo'], z5['hi']), 'tl': ctx['tl']}
            break
        if out: break
    if stats is not None:
        stats['nested'] += nested; stats['retest'] += retest
        if out: stats['signal'] += 1
        elif sl_flag: stats['sl_' + sl_flag] += 1
    return out


_H4_CACHE = {'ts': 0.0, 'rows': None}


def _h4_rows(sym):
    if _H4_CACHE['rows'] is None or time.time() - _H4_CACHE['ts'] > 600:
        _H4_CACHE['rows'] = fetch_klines_tf(sym, '4h', 150); _H4_CACHE['ts'] = time.time()
    return _H4_CACHE['rows']


def scalp_live(sym, rows5):
    if not SCALP_ENABLE: return None
    try:
        ctx = scalp_ctx(fetch_recent(sym, 200, tf='15m'), _h4_rows(sym))
        sg = scalp_candidate(rows5, ctx, STATE.get('scalp_seen', []))
        ket = f"tren H4 {'naik' if ctx['side'] == 'L' else 'turun'}, FVG M15 valid {len(ctx['z15'])}" if ctx else 'tidak ada tren H4 + FVG M15 valid'
        print(f"[scalper] {time.strftime('%H:%M:%S')} {ket} -> {'SINYAL' if sg else 'tunggu'}")
        return sg
    except Exception as e:
        print('scalper:', e); return None


def scalp_status_text():
    sym = resolve_symbol(); off = STATE['offset']
    rows5 = fetch_recent(sym, 300, tf='5m'); rows15 = fetch_recent(sym, 200, tf='15m'); rows4 = _h4_rows(sym)
    trend, tl = h4_trend(rows4); px = rows5[-1][4]
    rows = [f'Harga   {px+off:.2f}',
            f'Trend H4 {"naik" if trend == 1 else ("turun" if trend == -1 else "netral")}' + (f'  garis {tl+off:.1f}' if tl else '')]
    note = 'Sinyal hanya muncul saat candle M5 retest zona M5 lalu ditutup menolak.'
    if trend == 0:
        rows.append('Tunggu: trendline H4 belum valid')
    else:
        side = 'L' if trend == 1 else 'S'; atr5 = _atr_rows(rows5)
        z15 = find_fvgs(rows15, side, SCALP_FVG15_BARS, SCALP_GAP15_ATR * _atr_rows(rows15))
        z5 = find_fvgs(rows5, side, SCALP_FVG5_BARS, SCALP_GAP5_ATR * atr5)
        nested = [(a, b_) for a in z15 for b_ in z5 if b_['t'] >= a['t'] and b_['lo'] >= a['lo'] - 0.1*atr5 and b_['hi'] <= a['hi'] + 0.1*atr5]
        rows += [f'FVG M15 valid    {len(z15)}', f'FVG M5 valid     {len(z5)}', f'M5 di dlm M15     {len(nested)}']
        if nested:
            z = nested[0][1]; rows.append(f'Zona M5 {z["lo"]+off:.2f}-{z["hi"]+off:.2f}')
            rows.append('Tunggu retest + konfirmasi')
        elif z15: rows.append('Tunggu FVG M5 di dalam FVG M15')
        else: rows.append('Tunggu FVG M15 searah tren')
    rows.append(f'SL ${SCALP_SL_MIN:g}-${SCALP_SL_MAX:g} | RR 1:{SCALP_RR:g}')
    return '<b>SCALPER TREND — STATUS</b>\n' + _tbl(rows) + '\n' + note


def _m1_window(sym, start_ms, n_min):
    raw = api('/v5/market/kline', {'category': 'linear', 'symbol': sym, 'interval': '1', 'start': int(start_ms),
                                   'end': int(start_ms + n_min * 60_000), 'limit': 1000}, timeout=20).get('list', [])
    now = now_ms()
    return sorted((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in raw
                  if len(r) >= 5 and int(r[0]) + 60_000 <= now and int(r[0]) >= start_ms)


def _walk(candles, sg, trail):
    """Jalankan trade melalui _manage_active_bar (logika sama dengan tracker live)."""
    a = {'side': sg['side'], 'kind': sg.get('kind', 'SCALPER TREND'), 'entry': sg['entry'], 'initial_sl': sg['sl'],
         'active_sl': sg['sl'], 'sl': sg['sl'], 'tp': sg['tp'], 'be_stage': 0, 'bars_held': 0, 'max_hold_m1': sg.get('max_hold_m1')}
    if not trail: a['fixed'] = True
    last = None
    for ts, op, hi, lo, cl in candles:
        out = _manage_active_bar(a, op, hi, lo, cl); last = (ts, cl)
        if out: return out[0], out[1], ts + 60_000, a
    if last is None: return None
    return 'TIMEOUT', last[1], last[0] + 60_000, a


def run_scalp_backtest(days=14, wr_table=False):
    sym = resolve_symbol()
    rows5 = fetch_klines_history_tf(sym, '5m', days * 288 + 300)
    rows15 = fetch_klines_history_tf(sym, '15m', days * 96 + 120)
    rows4 = fetch_klines_history_tf(sym, '4h', days * 6 + 120)
    if len(rows5) < 400 or len(rows15) < 100 or len(rows4) < 60:
        return 'Data tidak cukup untuk backtest Scalper trend.'
    t15 = [r[0] for r in rows15]; t4 = [r[0] for r in rows4]
    cost = max(SPREAD, PAPER_COST_XAU); m1c = {}; fb = [0]

    def scan(trail, rr=None):
        ckey, ctx, tr_dir = None, None, 0; seen = []; res = []; busy_ms = -1
        stats = dict(nested=0, retest=0, signal=0, sl_narrow=0, sl_wide=0); cnt = dict(tot=0, trend=0, ctx=0)
        for i in range(60, len(rows5) - 1):
            close_ms = rows5[i][0] + 300_000
            if close_ms <= busy_ms: continue
            k15 = bisect.bisect_right(t15, close_ms - 900_000); k4 = bisect.bisect_right(t4, close_ms - 14_400_000)
            if k15 < 40 or k4 < 40: continue
            if (k15, k4) != ckey:
                ckey = (k15, k4); w15 = rows15[max(0, k15-200):k15]; w4 = rows4[max(0, k4-150):k4]
                tr_dir = h4_trend(w4)[0]; ctx = scalp_ctx(w15, w4)
            cnt['tot'] += 1; cnt['trend'] += (tr_dir != 0)
            if not ctx: continue
            cnt['ctx'] += 1
            sg = scalp_candidate(rows5[max(0, i-299):i+1], ctx, seen, stats)
            if not sg: continue
            seen.append(sg['zone_id'])
            if rr: sg['tp'] = sg['entry'] + sg['dist'] * rr if sg['side'] == 'L' else sg['entry'] - sg['dist'] * rr
            if close_ms not in m1c:
                try: m1c[close_ms] = _m1_window(sym, close_ms, SCALP_MAX_BARS * 5)
                except Exception as e: print('m1 window:', e); m1c[close_ms] = []
            cands = m1c[close_ms]
            if len(cands) < 5:
                fb[0] += 1; cands = [(r[0], r[1], r[2], r[3], r[4]) for r in rows5[i+1:i+1+SCALP_MAX_BARS]]
            w = _walk(cands, sg, trail)
            if not w: continue
            status, px, exit_ms, a = w
            gross = (px - sg['entry']) if sg['side'] == 'L' else (sg['entry'] - px)
            tag = 'TO' if status == 'TIMEOUT' else ('TP' if status == 'TP' else _exit_tag(a, 'SL'))
            res.append((sg['side'], (gross - cost) / sg['dist'], sg['dist'], tag, gross, close_ms)); busy_ms = exit_ms
        return res, stats, cnt

    tr_trades, stats, cnt = scan(True)
    fx_trades, _, _ = scan(False)

    def summ(tr):
        n = len(tr)
        if not n: return None
        rs = [t[1] for t in tr]; wn = [r for r in rs if r > 0]; ls = [-r for r in rs if r <= 0]
        pf = (sum(wn) / sum(ls)) if ls else 99.0
        cum = peak = dd = 0.0
        for r in rs:
            cum += r; peak = max(peak, cum); dd = max(dd, peak - cum)
        return n, len(wn) / n * 100, pf, sum(rs), dd, sum(t[2] for t in tr) / n

    if wr_table:
        rw = [f'{"TP":<5}{"n":>3}{"WR*":>6}{"PF":>6}{"Net":>8}']
        for rr in (0.2, 0.3, 0.5, 1.0, 1.5, 2.0):
            tr, _, _ = scan(False, rr); sm = summ(tr)
            if sm: rw.append(f'{rr:<5g}{sm[0]:>3}{sum(1 for t in tr if t[4] > 0)/sm[0]*100:>5.0f}%{sm[2]:>6.2f}{sm[3]:>+7.1f}R')
            else: rw.append(f'{rr:<5g}  0')
        tr, _, _ = scan(True); sm = summ(tr)
        if sm: rw.append(f'{"Trail":<5}{sm[0]:>3}{sum(1 for t in tr if t[4] > 0)/sm[0]*100:>5.0f}%{sm[2]:>6.2f}{sm[3]:>+7.1f}R')
        avg_r = sm[5] if sm else 1.0
        return (f'<b>WIN RATE vs HASIL — {days} HARI</b>\n' +
                _tbl([f'Data M5 {len(rows5)} M15 {len(rows15)} H4 {len(rows4)}', f'SL tetap | biaya {cost:g}/oz = {cost/avg_r:.2f}R', '-' * 28] + rw) +
                '\nTP dalam kelipatan R (SL = 1R). WR* = harga searah profit sebelum biaya; PF dan Net sudah memakai biaya. '
                'Jangan pilih baris terbaik dari data yang sama, itu overfitting.')
    sep = '-' * 28; tot = cnt['tot']; pc = lambda x: f'{x/tot*100:.0f}%' if tot else '-'
    funnel = [f'{"Candle dievaluasi":<19}{tot:>6}', f'{"Tren H4 valid":<19}{cnt["trend"]:>6} {pc(cnt["trend"])}',
              f'{"+ FVG M15 valid":<19}{cnt["ctx"]:>6} {pc(cnt["ctx"])}', f'{"+ FVG M5 di dalam":<19}{stats["nested"]:>6} {pc(stats["nested"])}',
              f'{"+ retest & tolak":<19}{stats["retest"]:>6}', f'{"SL <min / >maks":<19}{stats["sl_narrow"]:>3} /{stats["sl_wide"]:>3}',
              f'{"Sinyal":<19}{stats["signal"]:>6}']
    head = f'<b>SCALPER TREND — BACKTEST {days} HARI</b>\n'
    data = f'Data M5 {len(rows5)} M15 {len(rows15)} H4 {len(rows4)}'
    st, sf = summ(tr_trades), summ(fx_trades)
    if not st:
        return head + _tbl([data, 'Tidak ada trade.', sep] + funnel)
    gross_r = sum(t[4] / t[2] for t in tr_trades)
    net_at = lambda c: sum((t[4] - c) / t[2] for t in tr_trades)
    mid = rows5[len(rows5) // 2][0]
    h1 = [t for t in tr_trades if t[5] < mid]; h2 = [t for t in tr_trades if t[5] >= mid]
    line = lambda nm, sm: (f'{nm:<6}{sm[0]:>3}{sm[1]:>5.0f}%{sm[2]:>6.2f}{sm[3]:>+7.2f}R' if sm else f'{nm:<6}  0')
    tags = {k: sum(1 for t in tr_trades if t[3] == k) for k in ('TP', 'SL+', 'BE+', 'SL', 'TO')}
    rows = [data, f'{"Mode":<6}{"n":>3}{"Win":>6}{"PF":>6}{"Net":>8}', line('Trail', st), line('Fixed', sf),
            f'Trail: TP{tags["TP"]} SL+{tags["SL+"]} BE+{tags["BE+"]} SL{tags["SL"]} TO{tags["TO"]}',
            f'MaxDD  trail -{st[4]:.1f}R fixed -{(sf[4] if sf else 0):.1f}R',
            f'Rata2 SL ${st[5]:.2f} | biaya {cost/st[5]:.2f}R',
            f'Sebelum biaya {gross_r:+.1f}R',
            f'Net biaya $0.10 {net_at(0.10):+.1f}R', f'Net biaya $0.20 {net_at(0.20):+.1f}R', f'Net biaya $0.40 {net_at(0.40):+.1f}R',
            f'P1 {len(h1)}t {sum(t[1] for t in h1):+.1f}R  P2 {len(h2)}t {sum(t[1] for t in h2):+.1f}R',
            f'SL+: +{SCALP_TRAIL_TRIGGER_R:g}R lalu tiap {SCALP_TRAIL_STEP_R:g}R', sep] + funnel
    if fb[0]: rows.append(f'M1 kosong, pakai M5: {fb[0]} trade')
    note = '\nSampel kecil (kurang dari 30 trade), belum bisa disimpulkan.' if st[0] < 30 else ''
    return head + _tbl(rows) + '\nEksekusi dari candle M1; SL dan TP satu candle dihitung SL. Bukan jaminan hasil.' + note


# ───────── COUNTER RETAIL: stop-hunt / liquidity sweep ─────────
# Ide: ritel menaruh stop di bawah swing low dan di atas swing high yang kelihatan jelas (termasuk equal highs/lows).
# Harga sering menyapu level itu lalu berbalik. Sinyal: candle M5 menembus level M15 yang belum tersapu, lalu ditutup
# kembali di dalam dengan wick penolakan. Entry berlawanan dengan arah tembusan. Belum terbukti: wajib /bcr dulu.
CR_ENABLE = os.getenv('CR_ENABLE', '1') != '0'
CR_RR = max(0.5, float(os.getenv('CR_RR', str(RR))))
CR_SL_MIN = float(os.getenv('CR_SL_MIN_USD', '0.8'))
CR_SL_MAX = float(os.getenv('CR_SL_MAX_USD', '4.0'))
CR_LOOKBACK15 = 96         # level dicari dalam 24 jam M15 terakhir
CR_MIN_TOUCH = max(1, int(os.getenv('CR_MIN_TOUCH', '1')))   # 2 = hanya equal highs/lows
CR_TOL_ATR = 0.15          # toleransi pengelompokan level (x ATR M15)
CR_MIN_PEN_ATR = 0.1       # tembusan minimum (x ATR M5)
CR_MAX_PEN_ATR = 1.5       # tembusan lebih dari ini dianggap breakout, bukan sweep
CR_WICK_FRAC = 0.4         # wick penolakan minimal 40% dari range candle
CR_SL_BUF_ATR = 0.2


def cr_levels(rows15):
    """Level likuiditas M15 yang belum tersapu: {'H': [...], 'L': [...]}; tiap level {'p','n','t'}."""
    w = rows15[-(CR_LOOKBACK15 + 4):]
    if len(w) < 30: return None
    atr = _atr_rows(rows15)
    if atr <= 0: return None
    H = [r[2] for r in w]; L = [r[3] for r in w]
    hi, lo = _pivots(H, L, 2)

    def cluster(piv, side):
        res, used = [], set()
        for a, (ka, pa) in enumerate(piv):
            if a in used: continue
            grp = [(ka, pa)]; used.add(a)
            for b_, (kb, pb) in enumerate(piv):
                if b_ not in used and abs(pb - pa) <= CR_TOL_ATR * atr: grp.append((kb, pb)); used.add(b_)
            p = max(g[1] for g in grp) if side == 'H' else min(g[1] for g in grp)
            km = max(g[0] for g in grp)
            swept = any(H[m] > p for m in range(km + 1, len(w))) if side == 'H' else any(L[m] < p for m in range(km + 1, len(w)))
            if not swept and len(grp) >= CR_MIN_TOUCH: res.append({'p': p, 'n': len(grp), 't': w[km][0]})
        return res
    out = {'H': cluster(hi, 'H'), 'L': cluster(lo, 'L')}
    return out if (out['H'] or out['L']) else None


def cr_candidate(rows5, lv, seen=(), stats=None):
    if not lv or len(rows5) < 60: return None
    atr5 = _atr_rows(rows5)
    t_, o, h, l, c = rows5[-1][:5]; rng = h - l
    if atr5 <= 0 or rng <= 0: return None
    f = dict(swept=False, back=False, wick=False); sl_flag = None; out = None
    cands = [('S', x) for x in sorted(lv['H'], key=lambda x: -x['p'])] + [('B', x) for x in sorted(lv['L'], key=lambda x: x['p'])]
    for kind, lvl in cands:
        sell = kind == 'S'; p = lvl['p']
        lid = f"CR|{lvl['t']}|{p:.2f}|{kind}"
        if lid in seen: continue
        pen = (h - p) if sell else (p - l)
        if pen < CR_MIN_PEN_ATR * atr5 or pen > CR_MAX_PEN_ATR * atr5: continue
        f['swept'] = True
        if (sell and c >= p) or (not sell and c <= p): continue
        f['back'] = True
        wick = (h - max(o, c)) if sell else (min(o, c) - l)
        if wick < CR_WICK_FRAC * rng: continue
        f['wick'] = True
        sl = h + CR_SL_BUF_ATR * atr5 if sell else l - CR_SL_BUF_ATR * atr5
        risk = (sl - c) if sell else (c - sl)
        if risk < CR_SL_MIN: sl_flag = 'narrow'; continue
        if risk > CR_SL_MAX: sl_flag = 'wide'; continue
        tp = c - risk * CR_RR if sell else c + risk * CR_RR
        out = {'side': 'S' if sell else 'L', 'kind': 'COUNTER RETAIL', 'entry': c, 'sl': sl, 'tp': tp, 'score': f'x{lvl["n"]}',
               't': t_, 'atr': atr5, 'session': session_for(t_ // 1000), 'dist': risk, 'i': len(rows5) - 1, 'live': {},
               'tf_ms': 300_000, 'rr': CR_RR, 'zone_id': lid, 'lvl': p, 'n': lvl['n'], 'pen': pen}
        break
    if stats is not None:
        for k_, v_ in f.items(): stats[k_] += v_
        if out: stats['signal'] += 1
        elif sl_flag: stats['sl_' + sl_flag] += 1
    return out


def cr_live(sym, rows5):
    if not CR_ENABLE: return None
    try:
        lv = cr_levels(fetch_recent(sym, 200, tf='15m'))
        sg = cr_candidate(rows5, lv, STATE.get('scalp_seen', []))
        print(f"[counter-retail] {time.strftime('%H:%M:%S')} level H {len(lv['H']) if lv else 0} / L {len(lv['L']) if lv else 0} -> {'SINYAL' if sg else 'tunggu'}")
        return sg
    except Exception as e:
        print('counter-retail:', e); return None


def cr_status_text():
    sym = resolve_symbol(); off = STATE['offset']
    rows5 = fetch_recent(sym, 300, tf='5m'); lv = cr_levels(fetch_recent(sym, 200, tf='15m')); px = rows5[-1][4]
    rows = [f'Harga   {px+off:.2f}']
    if not lv: rows.append('Belum ada level likuiditas valid')
    else:
        up = sorted([x for x in lv['H'] if x['p'] > px], key=lambda x: x['p'])[:2]
        dn = sorted([x for x in lv['L'] if x['p'] < px], key=lambda x: -x['p'])[:2]
        rows += [f'High {x["p"]+off:.2f} x{x["n"]} (+{x["p"]-px:.2f})' for x in up] or ['High: tidak ada di atas']
        rows += [f'Low  {x["p"]+off:.2f} x{x["n"]} (-{px-x["p"]:.2f})' for x in dn] or ['Low: tidak ada di bawah']
    rows.append(f'SL ${CR_SL_MIN:g}-${CR_SL_MAX:g} | RR 1:{CR_RR:g}')
    return ('<b>COUNTER RETAIL — STATUS</b>\n' + _tbl(rows) +
            '\nSinyal saat candle M5 menyapu level lalu ditutup balik dengan wick penolakan.')


def crowd_text():
    sym = resolve_symbol(); rows = []
    r = api('/v5/market/account-ratio', {'category': 'linear', 'symbol': sym, 'period': '1h', 'limit': 24}).get('list', [])
    if r:
        buy = float(r[0]['buyRatio']) * 100; avg = sum(float(x['buyRatio']) for x in r) / len(r) * 100
        rows += [f'Akun long  {buy:.1f}%', f'Akun short {100-buy:.1f}%', f'Rata2 24j long {avg:.1f}%']
    else:
        rows.append(f'Bybit tidak memberi rasio akun utk {sym}')
    try:
        fr = api('/v5/market/funding/history', {'category': 'linear', 'symbol': sym, 'limit': 3}).get('list', [])
        if fr: rows.append(f'Funding {float(fr[0]["fundingRate"])*100:.4f}%')
    except Exception as e:
        print('funding:', e)
    return ('<b>POSISI AKUN BYBIT — ' + sym + '</b>\n' + _tbl(rows) +
            '\nHanya akun Bybit, bukan seluruh ritel. Info saja: belum dipakai di sinyal dan belum di-backtest.')


def run_cr_backtest(days=30):
    sym = resolve_symbol()
    rows5 = fetch_klines_history_tf(sym, '5m', days * 288 + 300)
    rows15 = fetch_klines_history_tf(sym, '15m', days * 96 + 200)
    if len(rows5) < 400 or len(rows15) < 150: return 'Data tidak cukup untuk backtest Counter Retail.'
    t15 = [r[0] for r in rows15]; cost = max(SPREAD, PAPER_COST_XAU); m1c = {}; fb = [0]

    def scan(trail):
        ckey, lv = None, None; seen = []; res = []; busy_ms = -1
        stats = dict(swept=0, back=0, wick=0, sl_narrow=0, sl_wide=0, signal=0); cnt = dict(tot=0, lv=0)
        for i in range(60, len(rows5) - 1):
            close_ms = rows5[i][0] + 300_000
            if close_ms <= busy_ms: continue
            k15 = bisect.bisect_right(t15, close_ms - 900_000)
            if k15 < 100: continue
            if k15 != ckey: ckey = k15; lv = cr_levels(rows15[max(0, k15-200):k15])
            cnt['tot'] += 1
            if not lv: continue
            cnt['lv'] += 1
            sg = cr_candidate(rows5[max(0, i-299):i+1], lv, seen, stats)
            if not sg: continue
            seen.append(sg['zone_id'])
            if close_ms not in m1c:
                try: m1c[close_ms] = _m1_window(sym, close_ms, SCALP_MAX_BARS * 5)
                except Exception as e: print('m1 window:', e); m1c[close_ms] = []
            cands = m1c[close_ms]
            if len(cands) < 5:
                fb[0] += 1; cands = [(r[0], r[1], r[2], r[3], r[4]) for r in rows5[i+1:i+1+SCALP_MAX_BARS]]
            w = _walk(cands, sg, trail)
            if not w: continue
            status, px, exit_ms, a = w
            gross = (px - sg['entry']) if sg['side'] == 'L' else (sg['entry'] - px)
            tag = 'TO' if status == 'TIMEOUT' else ('TP' if status == 'TP' else _exit_tag(a, 'SL'))
            res.append((sg['side'], (gross - cost) / sg['dist'], sg['dist'], tag, gross, close_ms)); busy_ms = exit_ms
        return res, stats, cnt

    tr_trades, stats, cnt = scan(True)
    fx_trades, _, _ = scan(False)

    def summ(tr):
        n = len(tr)
        if not n: return None
        rs = [t[1] for t in tr]; wn = [r for r in rs if r > 0]; ls = [-r for r in rs if r <= 0]
        pf = (sum(wn) / sum(ls)) if ls else 99.0
        cum = peak = dd = 0.0
        for r in rs:
            cum += r; peak = max(peak, cum); dd = max(dd, peak - cum)
        return n, len(wn) / n * 100, pf, sum(rs), dd, sum(t[2] for t in tr) / n

    sep = '-' * 28; tot = cnt['tot']; pc = lambda x: f'{x/tot*100:.0f}%' if tot else '-'
    funnel = [f'{"Candle dievaluasi":<19}{tot:>6}', f'{"Ada level likuid.":<19}{cnt["lv"]:>6} {pc(cnt["lv"])}',
              f'{"Sweep wick lewat":<19}{stats["swept"]:>6}', f'{"+ close balik":<19}{stats["back"]:>6}',
              f'{"+ wick tolak":<19}{stats["wick"]:>6}', f'{"SL <min / >maks":<19}{stats["sl_narrow"]:>3} /{stats["sl_wide"]:>3}',
              f'{"Sinyal":<19}{stats["signal"]:>6}']
    head = f'<b>COUNTER RETAIL — BACKTEST {days} HARI</b>\n'
    data = f'Data M5 {len(rows5)} M15 {len(rows15)}'
    st, sf = summ(tr_trades), summ(fx_trades)
    if not st: return head + _tbl([data, 'Tidak ada trade.', sep] + funnel)
    gross_r = sum(t[4] / t[2] for t in tr_trades); net_at = lambda c: sum((t[4] - c) / t[2] for t in tr_trades)
    mid = rows5[len(rows5) // 2][0]; h1 = [t for t in tr_trades if t[5] < mid]; h2 = [t for t in tr_trades if t[5] >= mid]
    line = lambda nm, sm: (f'{nm:<6}{sm[0]:>3}{sm[1]:>5.0f}%{sm[2]:>6.2f}{sm[3]:>+7.2f}R' if sm else f'{nm:<6}  0')
    tags = {k: sum(1 for t in tr_trades if t[3] == k) for k in ('TP', 'SL+', 'BE+', 'SL', 'TO')}
    nb = sum(1 for t in tr_trades if t[0] == 'L')
    rows = [data, f'{"Mode":<6}{"n":>3}{"Win":>6}{"PF":>6}{"Net":>8}', line('Trail', st), line('Fixed', sf),
            f'Trail: TP{tags["TP"]} SL+{tags["SL+"]} BE+{tags["BE+"]} SL{tags["SL"]} TO{tags["TO"]}',
            f'BUY {nb} / SELL {st[0]-nb}', f'MaxDD  trail -{st[4]:.1f}R fixed -{(sf[4] if sf else 0):.1f}R',
            f'Rata2 SL ${st[5]:.2f} | biaya {cost/st[5]:.2f}R', f'Sebelum biaya {gross_r:+.1f}R',
            f'Net biaya $0.10 {net_at(0.10):+.1f}R', f'Net biaya $0.20 {net_at(0.20):+.1f}R', f'Net biaya $0.40 {net_at(0.40):+.1f}R',
            f'P1 {len(h1)}t {sum(t[1] for t in h1):+.1f}R  P2 {len(h2)}t {sum(t[1] for t in h2):+.1f}R', sep] + funnel
    if fb[0]: rows.append(f'M1 kosong, pakai M5: {fb[0]} trade')
    note = '\nSampel kecil (kurang dari 30 trade), belum bisa disimpulkan.' if st[0] < 30 else ''
    return head + _tbl(rows) + '\nEksekusi dari candle M1; SL dan TP satu candle dihitung SL. Bukan jaminan hasil.' + note


# ───────── LIQUIDITY SNIPER 1M v2 — port lengkap dari bot.py ─────────
# Sinyal: SHADOW FAKEOUT 1M (sweep high/low 12 candle, reclaim, wick penolakan) + konfirmasi Ichimoku 1M.
# Ikut dibawa: manajemen posisi INIT -> BE+ -> trailing SL+ kontinu, model biaya (spread + fee), paper account
# (lot dari risiko % equity, leverage, margin call, top-up otomatis), caption, backtest trailing vs TP/SL murni.
# Semua env diberi awalan LS_ supaya tidak bentrok dengan BE_TRIGGER_R dll milik tracker utama.
def _lsf(name, default):
    try: return float((os.getenv(name) or '').strip() or default)
    except ValueError: return float(default)


def _lsb(name, default):
    v = (os.getenv(name) or '').strip().lower()
    return default if not v else v not in ('0', 'false', 'no', 'off')


LS_ENABLE = _lsb('LS_ENABLE', True)
LS_ALLOWED = ('XAUUSD', 'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT')
LS_PAIRS = [p for p in (x.strip().upper() for x in (os.getenv('LS_PAIRS') or 'XAUUSD,BTCUSDT').split(',')) if p in LS_ALLOWED] or ['XAUUSD', 'BTCUSDT']
LS_INTERVAL_MS = 60_000
LS_SWING = 12; LS_ATR_P = 14; LS_TENKAN = 9; LS_KIJUN = 26; LS_SENKOU_B = 52; LS_SHIFT = 26
LS_RR = _lsf('LS_RR_TARGET', 1.5); LS_MAX_HOLD = int(_lsf('LS_MAX_HOLD_BARS', 60))
LS_MAX_RISK_ATR = 3.0; LS_STOP_BUF = 0.08
LS_TRAILING = _lsb('LS_TRAILING', True)
LS_BE_TRIGGER_R = _lsf('LS_BE_TRIGGER_R', 0.8); LS_BE_OFFSET_R = _lsf('LS_BE_OFFSET_R', 0.1)
LS_TRAIL_START_R = _lsf('LS_TRAIL_START_R', 1.0); LS_TRAIL_GAP_R = _lsf('LS_TRAIL_GAP_R', 0.5)
LS_TRAIL_NOTIFY_R = _lsf('LS_TRAIL_NOTIFY_R', 0.25)
LS_START_IDR = _lsf('LS_PAPER_START_IDR', 1_000_000); LS_TOPUP_IDR = _lsf('LS_PAPER_TOPUP_IDR', 500_000)
LS_USDIDR = _lsf('LS_USDIDR', USD_IDR)
LS_LEVERAGE = max(1.0, _lsf('LS_LEVERAGE', 200)); LS_RISK_PCT = _lsf('LS_RISK_PCT', 2.0)
LS_MAX_RISK_PCT = _lsf('LS_MAX_RISK_PCT', 15.0); LS_MC_LEVEL = _lsf('LS_MC_LEVEL', 100.0)
LS_MAX_TOPUPS = int(_lsf('LS_MAX_TOPUPS', 0)); LS_COSTS = _lsb('LS_COSTS', True)
LS_POLL_OFFSET = _lsf('LS_POLL_OFFSET_S', 1.0); LS_CHART_BARS = int(_lsf('LS_CHART_BARS', 90)); LS_BT_COOLDOWN = 60
LS_DATA_DIR = (os.getenv('LS_DATA_DIR') or os.getenv('RAILWAY_VOLUME_MOUNT_PATH') or 'data').strip()
LS_STATE_FILE = os.path.join(LS_DATA_DIR, 'ls_paper_state.json')
# Engine: SNIPER = Shadow Fakeout + Ichimoku. PDF = Shadow Fakeout materi PDF (range -> sweep shadow panjang ->
# breakbody/BOS -> retest, TP zona lawan). Posisi, statistik, dan topic masing-masing terpisah.
LS_ENGINES = [e for e in (x.strip().upper() for x in (os.getenv('LS_ENGINES') or 'sniper,pdf').split(',')) if e in ('SNIPER', 'PDF')] or ['SNIPER', 'PDF']
LS_ENGINE_LABELS = {'SNIPER': 'Liquidity Sniper', 'PDF': 'PDF Shadow Fakeout'}
class _LSSpecs(dict):
    def __missing__(self, key):
        return {'unit': 'unit', 'min_qty': 0.01, 'step': 0.01, 'dec': 4, 'spread': 0.0, 'fee': 0.00055}


LS_SPECS = _LSSpecs({
    'XAUUSD': {'unit': 'oz', 'min_qty': 1.0, 'step': 1.0, 'dec': 2, 'spread': _lsf('LS_SPREAD_XAUUSD', 0.30), 'fee': _lsf('LS_FEE_XAUUSD', 0.0)},
    'BTCUSDT': {'unit': 'BTC', 'min_qty': 0.001, 'step': 0.001, 'dec': 1, 'spread': _lsf('LS_SPREAD_BTCUSDT', 0.20), 'fee': _lsf('LS_FEE_BTCUSDT', 0.00055)},
})
_LS_SPEC_DONE = set()


def _ls_log(msg):
    print(f"[{datetime.now(WIB):%H:%M:%S}] {msg}", flush=True)


@dataclass(frozen=True)
class LSCandle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    @property
    def time_utc(self):
        return datetime.fromtimestamp(self.ts / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


@dataclass
class LSSignal:
    symbol: str
    direction: str
    index: int
    candle_ts: int
    entry_reference: float
    stop: float
    target: float
    risk: float
    atr: float
    reason: str
    score: int
    sweep_level: float = 0.0
    engine: str = 'SNIPER'
    extra: dict = field(default_factory=dict)


@dataclass
class LSTrade:
    symbol: str
    direction: str
    signal_time: str
    entry_time: str
    exit_time: str
    entry: float
    stop: float
    target: float
    exit_price: float
    result_r: float
    outcome: str
    bars_held: int
    reason: str
    engine: str = 'SNIPER'


@dataclass
class LSPosition:
    pair: str
    direction: str
    entry: float
    stop: float
    init_stop: float
    target: float
    risk: float
    qty: float = 0.0
    margin: float = 0.0
    risk_usd: float = 0.0
    signal_ts: int = 0
    last_ts: int = 0
    bars: int = 0
    mfe: float = 0.0
    stage: str = 'INIT'
    notified_r: float = -9.0
    opened_at: float = 0.0
    reason: str = ''
    score: int = 0
    engine: str = 'SNIPER'
    max_hold: int = 0

    @property
    def d(self):
        return 1 if self.direction == 'BUY' else -1

    def r_of(self, price):
        return self.d * (price - self.entry) / self.risk

    @classmethod
    def from_dict(cls, data):
        names = {f.name for f in _dc_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in names})


@dataclass
class LSBarResult:
    closed: bool = False
    outcome: str = ''
    price: float = 0.0


def ls_normalize_pair(value):
    v = (value or '').strip().upper().replace('/', '').replace('-', '')
    if v in {'XAU', 'GOLD', 'XAUUSD', 'XAUUSD=X', 'XAUT', 'XAUTUSDT'}: return 'XAUUSD'
    if v in {'BTC', 'BTCUSDT', 'BTCUSD', 'BITCOIN'}: return 'BTCUSDT'
    if v in {'ETH', 'ETHUSDT', 'ETHUSD', 'ETHEREUM'}: return 'ETHUSDT'
    if v in {'SOL', 'SOLUSDT', 'SOLUSD', 'SOLANA'}: return 'SOLUSDT'
    if v in {'XRP', 'XRPUSDT', 'XRPUSD', 'RIPPLE'}: return 'XRPUSDT'
    if v in {'ALL', 'SEMUA', 'BOTH'}: return 'ALL'
    raise ValueError('Pair tidak dikenali. Pilih XAUUSD, BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, atau ALL.')


def _ls_fp(pair, x): return f"{x:.{LS_SPECS[pair]['dec']}f}"


def _ls_local(ts_ms): return datetime.fromtimestamp(ts_ms / 1000, WIB).strftime('%d-%m %H:%M') + ' WIB'


def _ls_rp(usd):
    v = usd * LS_USDIDR; n = int(round(abs(v))); t = f"{n:,}".replace(',', '.')
    return f"-Rp{t}" if (v < 0 and n) else f"Rp{t}"


def _ls_qty(pair, qty):
    if pair == 'XAUUSD': return f"{qty / 100:.2f} lot ({qty:g} oz)"
    if pair == 'BTCUSDT': return f"{qty:.3f} BTC"
    return f"{qty:g} {LS_SPECS[pair]['unit']}"


def _ls_pf(x): return '∞' if math.isinf(x) else f"{x:.2f}"


def ls_symbol(pair): return resolve_symbol() if pair == 'XAUUSD' else pair


def _ls_topic(pair, engine='SNIPER'):
    engine = str(engine).upper()
    return topic_for(f'{engine}_{pair}', engine)


def ls_ensure_spec(pair):
    """Pair kripto di luar XAU/BTC: ambil qtyStep, minOrderQty, tickSize dari Bybit sekali saja."""
    if pair in LS_SPECS or pair in _LS_SPEC_DONE: return
    _LS_SPEC_DONE.add(pair)
    try:
        it = (api('/v5/market/instruments-info', {'category': 'linear', 'symbol': pair}).get('list') or [None])[0]
        lot = it['lotSizeFilter']; tick = float(it['priceFilter']['tickSize'])
        dec = max(0, -int(math.floor(math.log10(tick)))) if tick > 0 else 4
        LS_SPECS[pair] = {'unit': it.get('baseCoin') or pair[:3], 'min_qty': float(lot['minOrderQty']), 'step': float(lot['qtyStep']),
                          'dec': dec, 'spread': _lsf(f'LS_SPREAD_{pair}', tick), 'fee': _lsf(f'LS_FEE_{pair}', 0.00055)}
        _ls_log(f"[{pair}] spesifikasi Bybit: min {LS_SPECS[pair]['min_qty']:g}, step {LS_SPECS[pair]['step']:g}, desimal {dec}")
    except Exception as exc:
        _ls_log(f'[{pair}] spesifikasi Bybit gagal ({exc}); memakai default konservatif')


def _ls_clean(candles, now_ms=None, interval_ms=None):
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    interval_ms = interval_ms or LS_INTERVAL_MS
    by_ts = {}
    for c in candles:
        if not all(math.isfinite(x) and x > 0 for x in (c.open, c.high, c.low, c.close)): continue
        if c.high < max(c.open, c.close, c.low) or c.low > min(c.open, c.close, c.high): continue
        if c.ts + interval_ms > now_ms: continue
        by_ts[c.ts] = c
    return [by_ts[k] for k in sorted(by_ts)]


def _ls_candles(rows, interval_ms=None):
    out = []
    for r in rows:
        try: out.append(LSCandle(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]) if len(r) > 5 else 0.0))
        except (TypeError, ValueError, IndexError): continue
    return _ls_clean(out, interval_ms=interval_ms)


def ls_fetch(pair, n=300):
    ls_ensure_spec(pair)
    rows = fetch_recent(ls_symbol(pair), n, tf='1m')
    candles = _ls_candles(rows)
    keep = {c.ts for c in candles}
    return [r for r in rows if int(r[0]) in keep], candles


def ls_history(pair, days):
    ls_ensure_spec(pair)
    return _ls_candles(fetch_klines_history_tf(ls_symbol(pair), '1m', days * 1440 + 300))


def _ls_mid(highs, lows, i, period):
    if i + 1 < period: return None
    return (max(highs[i - period + 1:i + 1]) + min(lows[i - period + 1:i + 1])) / 2.0


def ls_build_indicators(candles):
    n = len(candles)
    highs = [c.high for c in candles]; lows = [c.low for c in candles]; closes = [c.close for c in candles]
    tenkan = [None] * n; kijun = [None] * n; span_a_raw = [None] * n; span_b_raw = [None] * n
    cloud_a = [None] * n; cloud_b = [None] * n; tr = [0.0] * n; atr = [None] * n
    for i, c in enumerate(candles):
        tenkan[i] = _ls_mid(highs, lows, i, LS_TENKAN)
        kijun[i] = _ls_mid(highs, lows, i, LS_KIJUN)
        if tenkan[i] is not None and kijun[i] is not None:
            span_a_raw[i] = (tenkan[i] + kijun[i]) / 2.0
        span_b_raw[i] = _ls_mid(highs, lows, i, LS_SENKOU_B)
        prev_close = closes[i - 1] if i > 0 else c.close
        tr[i] = max(c.high - c.low, abs(c.high - prev_close), abs(c.low - prev_close))
        if i >= LS_ATR_P - 1:
            atr[i] = sum(tr[i - LS_ATR_P + 1:i + 1]) / LS_ATR_P
        source_i = i - LS_SHIFT          # Kumo yang tampil dihitung 26 bar sebelumnya (tanpa look-ahead)
        if source_i >= 0:
            cloud_a[i] = span_a_raw[source_i]; cloud_b[i] = span_b_raw[source_i]
    return {'tenkan': tenkan, 'kijun': kijun, 'cloud_a': cloud_a, 'cloud_b': cloud_b, 'atr': atr}


def ls_detect_signal(candles, i, indicators=None):
    """Shadow Fakeout 1M + konfirmasi Ichimoku; candle closed saja, tanpa look-ahead."""
    if i < max(LS_SWING + 2, LS_SENKOU_B - 1, LS_ATR_P + 2): return None
    if indicators is None: indicators = ls_build_indicators(candles[:i + 1])
    c = candles[i]; prev = candles[i - 1]
    atr = indicators['atr'][i]; tenkan = indicators['tenkan'][i]; kijun = indicators['kijun'][i]
    prev_tenkan = indicators['tenkan'][i - 1]; prev_kijun = indicators['kijun'][i - 1]
    cloud_a = indicators['cloud_a'][i]; cloud_b = indicators['cloud_b'][i]
    prev_cloud_a = indicators['cloud_a'][i - 1]; prev_cloud_b = indicators['cloud_b'][i - 1]
    if atr is None or tenkan is None or kijun is None or prev_tenkan is None or prev_kijun is None or atr <= 0: return None
    prior = candles[i - LS_SWING:i]
    prior_low = min(x.low for x in prior); prior_high = max(x.high for x in prior)
    candle_range = max(c.high - c.low, 1e-12)
    close_location = (c.close - c.low) / candle_range
    bullish_body = c.close > c.open and close_location >= 0.58
    bearish_body = c.close < c.open and close_location <= 0.42
    sweep_low = c.low < prior_low and c.close > prior_low
    sweep_high = c.high > prior_high and c.close < prior_high
    lower_wick = min(c.open, c.close) - c.low
    upper_wick = c.high - max(c.open, c.close)
    bullish_rejection = lower_wick >= max(abs(c.close - c.open) * 0.35, candle_range * 0.12)
    bearish_rejection = upper_wick >= max(abs(c.close - c.open) * 0.35, candle_range * 0.12)
    cross_up_tenkan = prev.close <= prev_tenkan and c.close > tenkan
    cross_down_tenkan = prev.close >= prev_tenkan and c.close < tenkan
    cross_up_kijun = prev.close <= prev_kijun and c.close > kijun
    cross_down_kijun = prev.close >= prev_kijun and c.close < kijun
    tk_turn_up = tenkan >= kijun and tenkan >= prev_tenkan and kijun >= prev_kijun
    tk_turn_down = tenkan <= kijun and tenkan <= prev_tenkan and kijun <= prev_kijun
    direction = ''; reason = ''; score = 0
    cloud_bull_confirm = False; cloud_bear_confirm = False
    if None not in (cloud_a, cloud_b, prev_cloud_a, prev_cloud_b):
        cloud_top = max(float(cloud_a), float(cloud_b)); cloud_bottom = min(float(cloud_a), float(cloud_b))
        prev_cloud_top = max(float(prev_cloud_a), float(prev_cloud_b)); prev_cloud_bottom = min(float(prev_cloud_a), float(prev_cloud_b))
        cloud_bull_confirm = c.close > cloud_top or (prev.close <= prev_cloud_top and c.close > cloud_top)
        cloud_bear_confirm = c.close < cloud_bottom or (prev.close >= prev_cloud_bottom and c.close < cloud_bottom)
    if sweep_low and bullish_body and bullish_rejection:
        score += 2
        if c.close > tenkan and (cross_up_tenkan or cross_up_kijun or tk_turn_up):
            score += 2; direction = 'BUY'
            reason = 'SHADOW FAKEOUT: sell-side sweep + reclaim/rejection + Ichimoku reversal'
        elif c.close > tenkan and c.close >= prev.close and tenkan >= prev_tenkan:
            score += 1; direction = 'BUY'
            reason = 'SHADOW FAKEOUT: sell-side sweep + reclaim/rejection + Tenkan momentum'
        if direction == 'BUY' and cloud_bull_confirm:
            score += 1; reason += ' + Kumo bullish confirmation'
    if sweep_high and bearish_body and bearish_rejection:
        score += 2
        if c.close < tenkan and (cross_down_tenkan or cross_down_kijun or tk_turn_down):
            score += 2; direction = 'SELL'
            reason = 'SHADOW FAKEOUT: buy-side sweep + reclaim/rejection + Ichimoku reversal'
        elif c.close < tenkan and c.close <= prev.close and tenkan <= prev_tenkan:
            score += 1; direction = 'SELL'
            reason = 'SHADOW FAKEOUT: buy-side sweep + reclaim/rejection + Tenkan momentum'
        if direction == 'SELL' and cloud_bear_confirm:
            score += 1; reason += ' + Kumo bearish confirmation'
    if not direction: return None
    buffer = max(atr * LS_STOP_BUF, abs(c.close) * 0.00001)
    if direction == 'BUY':
        stop = c.low - buffer; entry_ref = c.close; risk = entry_ref - stop
        target = entry_ref + LS_RR * risk; level = prior_low
    else:
        stop = c.high + buffer; entry_ref = c.close; risk = stop - entry_ref
        target = entry_ref - LS_RR * risk; level = prior_high
    if risk <= 0 or risk > LS_MAX_RISK_ATR * atr: return None
    return LSSignal(symbol='', direction=direction, index=i, candle_ts=c.ts, entry_reference=entry_ref, stop=stop,
                    target=target, risk=risk, atr=atr, reason=reason, score=score, sweep_level=level)


def ls_latest_closed_signal(pair, candles):
    inds = ls_build_indicators(candles)
    idx = len(candles) - 1
    sig = ls_detect_signal(candles, idx, inds) if idx >= 0 else None
    if sig: sig.symbol = pair
    c = candles[-1]
    return sig, {'price': c.close, 'time': c.time_utc, 'candle_ts': c.ts, 'count': len(candles)}


def _ls_blank_stats():
    return {'trades': 0, 'wins': 0, 'losses': 0, 'flats': 0, 'tp': 0, 'sl': 0, 'be': 0, 'slp': 0, 'timeout': 0,
            'gross_win': 0.0, 'gross_loss': 0.0, 'sum_r': 0.0, 'pnl': 0.0, 'peak_net': 0.0, 'max_dd': 0.0}


def ls_pos_key(pair, engine='SNIPER'): return f"{pair}|{str(engine).upper()}"


def _ls_hm(ts_ms): return datetime.fromtimestamp(ts_ms / 1000, WIB).strftime('%H:%M')


# ---- Engine PDF: Shadow Fakeout 1m (range -> sweep shadow panjang -> breakbody/BOS -> retest) -------------------
# Nama env sama dengan bot.py (MATERIAL_*). Pair kripto selain BTC wajib punya MATERIAL_<PAIR>_PIP_SIZE.
def ls_pdf_settings(pair):
    if pair == 'XAUUSD': pip = _lsf('MATERIAL_XAU_PIP_SIZE', 0.01)
    elif pair == 'BTCUSDT': pip = _lsf('MATERIAL_BTC_PIP_SIZE', 1.0)
    else:
        pip = _lsf(f'MATERIAL_{pair}_PIP_SIZE', 0.0)
        if pip <= 0: return None
    return {'range_lookback': max(10, int(_lsf('MATERIAL_RANGE_LOOKBACK', 24))),
            'setup_lookback': max(4, int(_lsf('MATERIAL_SETUP_LOOKBACK', 10))),
            'shadow_ratio': max(1.0, _lsf('MATERIAL_SHADOW_RATIO', 1.2)),
            'min_shadow_range': max(0.15, _lsf('MATERIAL_MIN_SHADOW_RANGE', 0.35)),
            'sl_pips': max(1.0, _lsf('MATERIAL_SL_PIPS', 50.0)), 'pip_size': max(1e-9, pip),
            'min_rr': max(1.0, _lsf('MATERIAL_MIN_RR', 1.5)), 'max_hold': max(10, int(_lsf('MATERIAL_MAX_HOLD_BARS', 120)))}


def ls_pdf_range_arrays(candles, lookback):
    """Low/high range `lookback` candle SEBELUM tiap index (untuk mempercepat backtest)."""
    n = len(candles); lows = [c.low for c in candles]; highs = [c.high for c in candles]
    lo = [None] * n; hi = [None] * n
    for k in range(lookback, n):
        lo[k] = min(lows[k - lookback:k]); hi[k] = max(highs[k - lookback:k])
    return lo, hi


def ls_detect_pdf_signal(candles, i, pair, settings=None, ranges=None, atr=None):
    """Setup terkonfirmasi memakai candle sampai index i saja; candle i = candle RETEST (entry di close-nya).
    Urutan ketat: (1) range sebelumnya, (2) sweep di luar range dengan shadow panjang lalu close kembali,
    (3) candle berikutnya body-reclaim level (breakbody/BOS), (4) candle i me-retest level dan bertahan.
    SL = ujung sweep +/- sl_pips*pip_size; TP = sisi lawan range -/+ buffer; wajib RR >= min_rr."""
    st = settings or ls_pdf_settings(pair)
    if st is None: return None
    rl, back = int(st['range_lookback']), int(st['setup_lookback'])
    if i < rl + 3 or i >= len(candles): return None
    pip_buffer = st['sl_pips'] * st['pip_size']
    found = None
    for s_i in range(max(rl, i - back), i - 1):
        if ranges is not None:
            range_low, range_high = ranges[0][s_i], ranges[1][s_i]
            if range_low is None or range_high is None: continue
        else:
            prior = candles[s_i - rl:s_i]
            if len(prior) < rl: continue
            range_low = min(x.low for x in prior); range_high = max(x.high for x in prior)
        range_size = max(range_high - range_low, pip_buffer * 2, 1e-9)
        sweep = candles[s_i]
        body = abs(sweep.close - sweep.open)
        lower = max(0.0, min(sweep.open, sweep.close) - sweep.low)
        upper = max(0.0, sweep.high - max(sweep.open, sweep.close))
        full = max(sweep.high - sweep.low, 1e-9)
        body_ref = max(body, range_size * 0.01, 1e-9)
        buy_sweep = (sweep.low < range_low and sweep.close > range_low
                     and lower >= body_ref * st['shadow_ratio'] and lower / full >= st['min_shadow_range'])
        sell_sweep = (sweep.high > range_high and sweep.close < range_high
                      and upper >= body_ref * st['shadow_ratio'] and upper / full >= st['min_shadow_range'])
        if not buy_sweep and not sell_sweep: continue
        side = 'BUY' if buy_sweep else 'SELL'
        level = range_low if side == 'BUY' else range_high
        b_i = None
        for k in range(s_i + 1, i):
            cand, prev_close = candles[k], candles[k - 1].close
            if side == 'BUY': ok = cand.close > level and (cand.open <= level or prev_close <= level)
            else: ok = cand.close < level and (cand.open >= level or prev_close >= level)
            if ok: b_i = k; break
        if b_i is None: continue
        retest = candles[i]
        tol = max(range_size * 0.08, pip_buffer * 0.5)
        if side == 'BUY':
            holds = (retest.low <= level + tol and retest.close > level and retest.close >= retest.open and retest.low > sweep.low)
            stop, target = sweep.low - pip_buffer, range_high - pip_buffer
        else:
            holds = (retest.high >= level - tol and retest.close < level and retest.close <= retest.open and retest.high < sweep.high)
            stop, target = sweep.high + pip_buffer, range_low + pip_buffer
        if not holds: continue
        entry = retest.close
        d = 1 if side == 'BUY' else -1
        risk, reward = d * (entry - stop), d * (target - entry)   # TP wajib di sisi profit
        if risk <= 0 or reward <= 0 or reward / risk < st['min_rr']: continue
        found = {'side': side, 'entry': entry, 'stop': stop, 'target': target, 'risk': risk, 'rr': reward / risk,
                 'level': level, 'range_low': range_low, 'range_high': range_high, 's_i': s_i, 'b_i': b_i}
    if found is None: return None
    s_i, b_i = found['s_i'], found['b_i']
    reasons = ['liquidity sweep dengan shadow panjang', 'breakbody/BOS terkonfirmasi', 'retest zona bertahan',
               f"SL di luar sweep + {pip_buffer:g} ({st['sl_pips']:g} pip)", 'TP menuju zona lawan']
    return LSSignal(symbol=pair, direction=found['side'], index=i, candle_ts=candles[i].ts, entry_reference=found['entry'],
                    stop=found['stop'], target=found['target'], risk=found['risk'],
                    atr=atr if (atr and atr > 0) else found['risk'],
                    reason='PDF SHADOW FAKEOUT: sweep shadow panjang → breakbody/BOS → retest bertahan',
                    score=5, sweep_level=found['level'], engine='PDF',
                    extra={'range_low': found['range_low'], 'range_high': found['range_high'], 's_i': s_i, 'b_i': b_i,
                           'sweep_ts': candles[s_i].ts, 'break_ts': candles[b_i].ts, 'rr': found['rr'],
                           'pip_buffer': pip_buffer, 'range_lookback': rl, 'reasons': reasons})


def ls_scan_engines(pair, candles, inds=None, engines=None):
    """Jalankan semua engine aktif pada candle closed terakhir."""
    inds = inds or ls_build_indicators(candles); i = len(candles) - 1; out = []
    for eng in (engines or LS_ENGINES):
        eng = eng.upper()
        if eng == 'SNIPER': sig = ls_detect_signal(candles, i, inds)
        elif eng == 'PDF': sig = ls_detect_pdf_signal(candles, i, pair, atr=inds['atr'][i])
        else: continue
        if sig is not None:
            sig.symbol = pair; sig.engine = eng; out.append(sig)
    return out


def _ls_better(d, a, b): return max(a, b) if d == 1 else min(a, b)


def ls_cost_per_unit(pair, entry, exit_price):
    """Biaya round-turn per 1 unit: spread + fee taker kedua sisi."""
    if not LS_COSTS: return 0.0
    sp = LS_SPECS[pair]
    return float(sp['spread']) + float(sp['fee']) * (entry + exit_price)


def ls_manage_bar(pos, bar, trailing=True, max_hold=None):
    """Proses SATU candle closed. SL dicek dengan nilai SEBELUM candle ini; bila SL & TP satu candle, SL menang."""
    max_hold = LS_MAX_HOLD if max_hold is None else max_hold
    d = pos.d; pos.bars += 1
    stop, target = pos.stop, pos.target
    stage_outcome = {'INIT': 'SL', 'BE': 'BE', 'TRAIL': 'SL+'}[pos.stage]
    if d == 1:
        stop_hit = bar.low <= stop; tp_hit = bar.high >= target; stop_fill = min(stop, bar.open)
    else:
        stop_hit = bar.high >= stop; tp_hit = bar.low <= target; stop_fill = max(stop, bar.open)
    if stop_hit: return LSBarResult(True, stage_outcome, stop_fill)
    if tp_hit: return LSBarResult(True, 'TP', target)
    fav = d * ((bar.high if d == 1 else bar.low) - pos.entry)
    if fav > pos.mfe: pos.mfe = fav
    if trailing and pos.risk > 0:
        mfe_r = pos.mfe / pos.risk; new_stop = pos.stop
        if mfe_r >= LS_BE_TRIGGER_R:
            new_stop = _ls_better(d, new_stop, pos.entry + d * LS_BE_OFFSET_R * pos.risk)
            if pos.stage == 'INIT': pos.stage = 'BE'
        if mfe_r >= LS_TRAIL_START_R:
            new_stop = _ls_better(d, new_stop, pos.entry + d * (mfe_r - LS_TRAIL_GAP_R) * pos.risk)
            pos.stage = 'TRAIL'
        if new_stop != pos.stop:
            pos.stop = new_stop
            if (d == 1 and bar.close <= new_stop) or (d == -1 and bar.close >= new_stop):
                return LSBarResult(True, {'BE': 'BE', 'TRAIL': 'SL+'}.get(pos.stage, 'SL'), new_stop)
    if pos.bars >= max_hold: return LSBarResult(True, 'TIMEOUT', bar.close)
    return LSBarResult()


def ls_net_r(pos, fill):
    return (pos.d * (fill - pos.entry) - ls_cost_per_unit(pos.pair, pos.entry, fill)) / pos.risk


def ls_backtest(candles, pair, rr=None, max_hold=None, trailing=True, engine='SNIPER'):
    """Satu posisi per waktu; sinyal di close -> entry OPEN candle berikutnya; SL menang bila bentrok.
    SNIPER: TP = rr x risk. PDF: TP = zona lawan (struktural) dari sinyal; satu setup hanya satu kali entry."""
    rr = LS_RR if rr is None else rr
    if len(candles) < 100: raise RuntimeError(f'Data tidak cukup untuk backtest: {len(candles)} candle (minimum 100).')
    engine = engine.upper(); indicators = ls_build_indicators(candles)
    st = ls_pdf_settings(pair) if engine == 'PDF' else None
    if engine == 'PDF' and st is None: raise RuntimeError(f'Engine PDF belum punya pip size untuk {pair}: isi MATERIAL_{pair}_PIP_SIZE.')
    ranges = ls_pdf_range_arrays(candles, int(st['range_lookback'])) if st else None
    hold = int(max_hold) if max_hold else (int(st['max_hold']) if st else LS_MAX_HOLD)
    i = int(st['range_lookback']) + int(st['setup_lookback']) + 3 if st else max(LS_SENKOU_B - 1, LS_SWING + 2, LS_ATR_P + 2)
    seen = set(); trades = []
    while i < len(candles) - 1:
        if st:
            sig = ls_detect_pdf_signal(candles, i, pair, st, ranges, indicators['atr'][i])
            if sig is not None and (sig.extra['sweep_ts'], sig.direction) in seen: sig = None
        else:
            sig = ls_detect_signal(candles, i, indicators)
        if sig is None: i += 1; continue
        entry_i = i + 1; entry = candles[entry_i].open
        d = 1 if sig.direction == 'BUY' else -1
        risk = d * (entry - sig.stop)
        if st:
            target = sig.target
            if risk <= 0 or d * (target - entry) <= 0: i += 1; continue
            seen.add((sig.extra['sweep_ts'], sig.direction))
        else:
            if risk <= 0 or risk > LS_MAX_RISK_ATR * sig.atr: i += 1; continue
            target = entry + d * rr * risk
        pos = LSPosition(pair=pair, direction=sig.direction, entry=entry, stop=sig.stop, init_stop=sig.stop,
                         target=target, risk=risk, signal_ts=candles[i].ts, last_ts=candles[i].ts, reason=sig.reason,
                         score=sig.score, engine=engine, max_hold=hold)
        exit_j = None; outcome, fill = 'TIMEOUT', candles[-1].close
        for j in range(entry_i, len(candles)):
            res = ls_manage_bar(pos, candles[j], trailing=trailing, max_hold=hold)
            if res.closed: exit_j, outcome, fill = j, res.outcome, res.price; break
        if exit_j is None: exit_j, outcome, fill = len(candles) - 1, 'TIMEOUT', candles[-1].close
        trades.append(LSTrade(symbol=pair, direction=sig.direction, signal_time=candles[i].time_utc,
                              entry_time=candles[entry_i].time_utc, exit_time=candles[exit_j].time_utc, entry=entry, stop=sig.stop,
                              target=target, exit_price=fill, result_r=ls_net_r(pos, fill), outcome=outcome,
                              bars_held=exit_j - entry_i + 1, reason=sig.reason, engine=engine))
        i = exit_j + 1
    wins = sum(1 for t in trades if t.result_r > 0); losses = sum(1 for t in trades if t.result_r < 0)
    flats = len(trades) - wins - losses; total = sum(t.result_r for t in trades)
    gp = sum(t.result_r for t in trades if t.result_r > 0); gl = -sum(t.result_r for t in trades if t.result_r < 0)
    equity = peak = max_dd = 0.0
    for t in trades:
        equity += t.result_r; peak = max(peak, equity); max_dd = max(max_dd, peak - equity)
    outcomes = {k: sum(1 for t in trades if t.outcome == k) for k in ('TP', 'SL+', 'BE', 'SL', 'TIMEOUT')}
    summary = {'symbol': pair, 'engine': engine, 'candles': len(candles), 'start': candles[0].time_utc, 'end': candles[-1].time_utc,
               'trades': len(trades), 'wins': wins, 'losses': losses, 'flats': flats,
               'winrate_pct': (wins / len(trades) * 100.0) if trades else 0.0, 'net_r': total,
               'avg_r': (total / len(trades)) if trades else 0.0, 'max_drawdown_r': max_dd,
               'profit_factor': (gp / gl) if gl > 0 else (math.inf if gp > 0 else 0.0),
               'outcomes': outcomes, 'rr': rr, 'max_hold': hold, 'trailing': trailing,
               'tp_mode': (f"TP zona lawan (RR>={st['min_rr']:g})" if st else f'TP {rr:.1f}R')}
    return trades, summary


def ls_format_backtest(sm, base=None):
    o = sm['outcomes']; label = LS_ENGINE_LABELS.get(sm.get('engine', 'SNIPER'), 'LIQUIDITY SNIPER').upper()
    lines = [f"BACKTEST {label} 1M — {sm['symbol']}",
             f"Data: {sm['start']} → {sm['end']} ({sm['candles']} candle valid)",
             f"Mode: {'BE+trailing SL+' if sm['trailing'] else 'TP/SL murni'} | {sm['tp_mode']} | maks hold {sm['max_hold']} bar | biaya {'ON' if LS_COSTS else 'OFF'}",
             f"Trade: {sm['trades']} | WR {sm['winrate_pct']:.1f}% ({sm['wins']}W/{sm['losses']}L/{sm['flats']} flat)",
             f"TP {o['TP']} | SL+ {o['SL+']} | BE {o['BE']} | SL {o['SL']} | Timeout {o['TIMEOUT']}",
             f"Net {sm['net_r']:+.2f}R | Avg {sm['avg_r']:+.3f}R | PF {_ls_pf(sm['profit_factor'])} | Max DD {sm['max_drawdown_r']:.2f}R"]
    if base is not None:
        lines.append(f"Pembanding TP/SL murni: Net {base['net_r']:+.2f}R | WR {base['winrate_pct']:.1f}% | "
                     f"PF {_ls_pf(base['profit_factor'])} | DD {base['max_drawdown_r']:.2f}R")
    lines += ['Entry OPEN candle berikutnya; SL menang bila SL & TP tersentuh satu candle. Belum ada slippage/latency/funding; spread+fee sesuai env.',
              'Simulasi historis, bukan jaminan hasil live.']
    return '\n'.join(lines)


def ls_trades_csv(trades):
    import csv as _csv, io as _io
    buf = _io.StringIO(); cols = [f.name for f in _dc_fields(LSTrade)]
    w = _csv.DictWriter(buf, fieldnames=cols); w.writeheader()
    for t in trades: w.writerow({k: getattr(t, k) for k in cols})
    return buf.getvalue().encode('utf-8')


class LSAccount:
    """Semua angka internal dalam USD; tampilan dikonversi ke rupiah dengan LS_USDIDR."""
    def __init__(self, path):
        self.path = path; self.lock = threading.RLock(); self.marks = {}; self.loaded = False; self._save_warned = False
        self._fresh(); self.load()

    def _fresh(self):
        start = LS_START_IDR / LS_USDIDR
        self.balance = start; self.deposits = start; self.topups = 0; self.created = time.time()
        self.positions = {}; self.closed = []; self.last_scanned = {}; self.last_setup = {}
        self.stats = _ls_blank_stats(); self.engine_stats = {}

    def reset(self):
        with self.lock:
            self._fresh(); self.save()

    def to_dict(self):
        return {'version': 2, 'created': self.created, 'balance': self.balance, 'deposits': self.deposits, 'topups': self.topups,
                'positions': {k: asdict(v) for k, v in self.positions.items()}, 'closed': self.closed[-100:],
                'stats': self.stats, 'engine_stats': self.engine_stats, 'last_scanned': self.last_scanned, 'last_setup': self.last_setup}

    def save(self):
        with self.lock:
            try:
                os.makedirs(os.path.dirname(self.path) or '.', exist_ok=True)
                tmp = self.path + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as fh: json.dump(self.to_dict(), fh)
                os.replace(tmp, self.path)
            except OSError as exc:
                if not self._save_warned:
                    self._save_warned = True; _ls_log(f'[ls-paper] GAGAL simpan state ({exc}); saldo hanya di memori')

    def load(self):
        if not os.path.exists(self.path): return
        try:
            with open(self.path, 'r', encoding='utf-8') as fh: data = json.load(fh)
            self.balance = float(data['balance']); self.deposits = float(data['deposits']); self.topups = int(data.get('topups', 0))
            self.created = float(data.get('created', time.time()))
            self.positions = {}
            for raw in (data.get('positions') or {}).values():      # kunci lama = pair saja -> dinormalkan
                p = LSPosition.from_dict(raw); self.positions[ls_pos_key(p.pair, p.engine)] = p
            self.closed = list(data.get('closed') or []); self.last_scanned = {k: int(v) for k, v in (data.get('last_scanned') or {}).items()}
            self.last_setup = {k: str(v) for k, v in (data.get('last_setup') or {}).items()}
            self.stats.update(data.get('stats') or {})
            self.engine_stats = {k: {**_ls_blank_stats(), **v} for k, v in (data.get('engine_stats') or {}).items()}
            self.loaded = True
            _ls_log(f'[ls-paper] state dimuat: saldo {_ls_rp(self.balance)}, {len(self.positions)} posisi terbuka')
        except Exception as exc:
            _ls_log(f'[ls-paper] state rusak, mulai baru ({exc})'); self._fresh()

    def mark_scanned(self, pair, ts):
        with self.lock:
            self.last_scanned[pair] = ts; self.save()

    def is_new_setup(self, pair, engine, key):
        """Satu setup PDF hanya boleh memicu satu sinyal (retest bisa berulang di candle berikutnya)."""
        with self.lock:
            k = f'{pair}|{engine}'
            if self.last_setup.get(k) == key: return False
            self.last_setup[k] = key; self.save(); return True

    def set_mark(self, pair, price):
        with self.lock: self.marks[pair] = price

    @staticmethod
    def floating(pos, price): return pos.d * (price - pos.entry) * pos.qty

    def equity(self):
        with self.lock:
            return self.balance + sum(self.floating(p, self.marks.get(p.pair, p.entry)) for p in self.positions.values())

    def used_margin(self):
        with self.lock: return sum(p.margin for p in self.positions.values())

    def net_pnl(self): return self.equity() - self.deposits

    def _topup(self):
        if LS_MAX_TOPUPS and self.topups >= LS_MAX_TOPUPS: return False
        dep = LS_TOPUP_IDR / LS_USDIDR
        self.balance += dep; self.deposits += dep; self.topups += 1
        return True

    def plan_size(self, pair, entry, risk_unit):
        spec = LS_SPECS[pair]; eq = self.equity(); free = eq - self.used_margin()
        if eq <= 0 or free <= 0 or risk_unit <= 0: return 0.0, 'margin'
        step, min_qty = float(spec['step']), float(spec['min_qty'])
        q_risk = eq * LS_RISK_PCT / 100.0 / risk_unit
        q_margin = free * LS_LEVERAGE * 0.95 / entry
        q = math.floor(min(q_risk, q_margin) / step + 1e-9) * step
        status = 'ok'
        if q < min_qty:
            if min_qty * entry / LS_LEVERAGE > free * 0.95: return 0.0, 'margin'
            q, status = min_qty, 'min_lot'
        q = round(q, 6)
        if q * risk_unit / eq * 100.0 > LS_MAX_RISK_PCT: return 0.0, 'risk'
        return q, status

    def open_from_signal(self, pair, sig, entry):
        info = {'skip': None, 'status': '', 'topups': []}
        with self.lock:
            key = ls_pos_key(pair, sig.engine)
            if key in self.positions:
                info['skip'] = f'posisi {pair} ({sig.engine}) masih terbuka'; return None, info
            risk_unit = abs(entry - sig.stop); qty, status = 0.0, 'margin'
            for _ in range(6):
                qty, status = self.plan_size(pair, entry, risk_unit)
                if qty > 0 or status != 'margin': break
                if not self._topup(): break
                info['topups'].append(f"💸 Modal tidak cukup untuk lot minimum → top-up otomatis +{_ls_rp(LS_TOPUP_IDR / LS_USDIDR)} ({self.topups}×)")
            if qty <= 0:
                info['skip'] = {'margin': 'modal/margin tidak cukup untuk lot minimum',
                                'risk': f'risiko lot minimum > {LS_MAX_RISK_PCT:.0f}% equity'}.get(status, 'ukuran lot 0')
                return None, info
            hold = int((ls_pdf_settings(pair) or {}).get('max_hold', LS_MAX_HOLD)) if sig.engine == 'PDF' else LS_MAX_HOLD
            pos = LSPosition(pair=pair, direction=sig.direction, entry=entry, stop=sig.stop, init_stop=sig.stop, target=sig.target,
                             risk=risk_unit, qty=qty, margin=qty * entry / LS_LEVERAGE, risk_usd=qty * risk_unit,
                             signal_ts=sig.candle_ts, last_ts=sig.candle_ts, opened_at=time.time(), reason=sig.reason, score=sig.score,
                             engine=sig.engine, max_hold=hold)
            self.positions[key] = pos; self.marks[pair] = entry; info['status'] = status
            info['risk_pct'] = pos.risk_usd / max(self.equity(), 1e-9) * 100.0
            self.save()
            return pos, info

    def advance_tagged(self, pair, candles):
        """Proses candle closed baru untuk SEMUA posisi pair ini (tiap engine). Return [(engine, pesan HTML)]."""
        out = []
        with self.lock:
            keys = [k for k, p in self.positions.items() if p.pair == pair]
            changed = False
            for key in keys:
                pos = self.positions.get(key)
                if pos is None: continue
                for bar in [c for c in candles if c.ts > pos.last_ts]:
                    changed = True; old_stage, old_stop = pos.stage, pos.stop
                    res = ls_manage_bar(pos, bar, trailing=LS_TRAILING, max_hold=pos.max_hold or LS_MAX_HOLD)
                    pos.last_ts = bar.ts
                    if res.closed:
                        out.append((pos.engine, self._close(pos, res.outcome, res.price)))
                        out.extend((pos.engine, m) for m in self._ensure_solvent()); break
                    if pos.stop != old_stop:
                        msg = self._trail_msg(pos, old_stage)
                        if msg: out.append((pos.engine, msg))
                    self.marks[pair] = bar.close
                    mc = self._margin_check(pos, bar)
                    if mc: out.append((pos.engine, mc))
            if candles and any(p.pair == pair for p in self.positions.values()): self.marks[pair] = candles[-1].close
            if changed: self.save()
        return out

    def advance(self, pair, candles):
        return [t for _, t in self.advance_tagged(pair, candles)]

    def _trail_msg(self, pos, old_stage):
        r_stop = pos.r_of(pos.stop); sp = _ls_fp(pos.pair, pos.stop)
        if pos.stage == 'BE' and old_stage == 'INIT':
            pos.notified_r = r_stop
            return f"🛡 <b>BE aktif</b> {pos.pair} {pos.direction} · {pos.engine}\nSL → <code>{sp}</code> ({r_stop:+.2f}R)"
        if pos.stage == 'TRAIL' and (old_stage != 'TRAIL' or r_stop - pos.notified_r >= LS_TRAIL_NOTIFY_R):
            pos.notified_r = r_stop
            return f"🔒 <b>SL+</b> {pos.pair} {pos.direction} · {pos.engine} → <code>{sp}</code> ({r_stop:+.2f}R terkunci)"
        return None

    def _margin_check(self, pos, bar):
        used = self.used_margin()
        if used <= 0: return None
        worst = bar.low if pos.direction == 'BUY' else bar.high
        eq = self.balance + self.floating(pos, worst) + sum(self.floating(p, self.marks.get(p.pair, p.entry)) for p in self.positions.values() if p is not pos)
        level = eq / used * 100.0
        if level > LS_MC_LEVEL: return None
        first_level, n = level, 0
        while level <= LS_MC_LEVEL and n < 20:
            if not self._topup(): break
            eq += LS_TOPUP_IDR / LS_USDIDR; level = eq / used * 100.0; n += 1
        if n == 0:
            return (f"⚠️ <b>MARGIN CALL</b> {pos.pair}: margin level {first_level:.0f}% ≤ {LS_MC_LEVEL:.0f}% "
                    f"tapi batas top-up ({LS_MAX_TOPUPS}×) habis")
        return (f"⚠️ <b>MARGIN CALL</b> {pos.pair}: margin level {first_level:.0f}% ≤ {LS_MC_LEVEL:.0f}%\n"
                f"Top-up otomatis +{_ls_rp(n * LS_TOPUP_IDR / LS_USDIDR)} ({self.topups}× total) → level {level:.0f}%\n"
                f"Saldo: <b>{_ls_rp(self.balance)}</b>")

    def _close(self, pos, outcome, fill):
        cost_u = ls_cost_per_unit(pos.pair, pos.entry, fill); gross = pos.d * (fill - pos.entry)
        pnl = (gross - cost_u) * pos.qty; r = (gross - cost_u) / pos.risk
        self.balance += pnl; self.positions.pop(ls_pos_key(pos.pair, pos.engine), None)
        key = {'TP': 'tp', 'SL': 'sl', 'BE': 'be', 'SL+': 'slp', 'TIMEOUT': 'timeout'}[outcome]
        es = self.engine_stats.setdefault(pos.engine, _ls_blank_stats())
        for s in (self.stats, es):
            s['trades'] += 1; s['sum_r'] += r; s['pnl'] += pnl; s[key] += 1
            if pnl > 0: s['wins'] += 1; s['gross_win'] += pnl
            elif pnl < 0: s['losses'] += 1; s['gross_loss'] += -pnl
            else: s['flats'] += 1
        s = self.stats
        net_all = self.net_pnl(); s['peak_net'] = max(s['peak_net'], net_all); s['max_dd'] = max(s['max_dd'], s['peak_net'] - net_all)
        self.closed.append({'t': time.time(), 'pair': pos.pair, 'engine': pos.engine, 'dir': pos.direction, 'outcome': outcome,
                            'entry': pos.entry, 'exit': fill, 'r': round(r, 3), 'pnl_usd': round(pnl, 4)})
        self.closed = self.closed[-100:]
        label = {'TP': '✅ TP KENA', 'SL': '❌ SL KENA', 'BE': '🛡 BE (stop break-even)', 'SL+': '🔒 SL+ (trailing profit)', 'TIMEOUT': '⏱ TIMEOUT'}[outcome]
        if outcome in ('BE', 'SL+') and pnl < 0: label = '🛡 Stop di profit kotor, tapi bersih minus (kena biaya)'
        pct = net_all / self.deposits * 100.0 if self.deposits else 0.0
        return (f"{label} — <b>{pos.pair} {pos.direction}</b> · {pos.engine}\n"
                f"Entry <code>{_ls_fp(pos.pair, pos.entry)}</code> → Exit <code>{_ls_fp(pos.pair, fill)}</code> ({r:+.2f}R bersih, {pos.bars} menit)\n"
                f"P/L: <b>{_ls_rp(pnl)}</b> (biaya spread+fee {_ls_rp(cost_u * pos.qty)}) | Saldo: <b>{_ls_rp(self.balance)}</b>\n"
                f"Net P/L total (setelah deposit): {_ls_rp(net_all)} ({pct:+.1f}%)")

    def _ensure_solvent(self):
        out = []; n = 0
        while self.balance <= 0 and n < 20:
            if not self._topup():
                out.append('💥 <b>Modal habis</b> dan batas top-up tercapai; paper trading berhenti sampai /reset confirm.'); break
            n += 1
        if n:
            out.append(f"⚠️ <b>MARGIN CALL / modal habis</b> → top-up otomatis +{_ls_rp(n * LS_TOPUP_IDR / LS_USDIDR)} ({self.topups}× total). Saldo: <b>{_ls_rp(self.balance)}</b>")
        return out

    def positions_text(self):
        with self.lock:
            if not self.positions: return 'Tidak ada posisi paper terbuka.'
            lines = []
            for p in self.positions.values():
                px = self.marks.get(p.pair, p.entry)
                lines.append(f"• <b>{p.pair} {p.direction}</b> · {p.engine} · {_ls_qty(p.pair, p.qty)}\n"
                             f"  Entry <code>{_ls_fp(p.pair, p.entry)}</code> | SL <code>{_ls_fp(p.pair, p.stop)}</code> ({p.r_of(p.stop):+.2f}R, {p.stage}) | TP <code>{_ls_fp(p.pair, p.target)}</code>\n"
                             f"  Harga <code>{_ls_fp(p.pair, px)}</code> | floating {_ls_rp(self.floating(p, px))} ({p.r_of(px):+.2f}R) | {p.bars} menit")
            return '\n'.join(lines)

    def summary_text(self):
        with self.lock:
            s = self.stats; eq = self.equity(); net = eq - self.deposits
            pct = net / self.deposits * 100.0 if self.deposits else 0.0
            n = int(s['trades']); wr = s['wins'] / n * 100.0 if n else 0.0
            pf = (s['gross_win'] / s['gross_loss']) if s['gross_loss'] > 0 else (math.inf if s['gross_win'] > 0 else 0.0)
            head = [
                '💼 <b>PAPER ACCOUNT — LIQUIDITY SNIPER</b>',
                f"Saldo: <b>{_ls_rp(self.balance)}</b> | Equity: {_ls_rp(eq)} (floating {_ls_rp(eq - self.balance)})",
                f"Total deposit: {_ls_rp(self.deposits)} (modal awal {_ls_rp(LS_START_IDR / LS_USDIDR)} + {self.topups}× top-up {_ls_rp(LS_TOPUP_IDR / LS_USDIDR)})",
                f"<b>Net P/L: {_ls_rp(net)} ({pct:+.1f}%)</b> ← saldo dikurangi semua deposit",
                f"Trade: {n} | W {int(s['wins'])} / L {int(s['losses'])} / flat {int(s['flats'])} | WR {wr:.1f}% | PF {_ls_pf(pf)}",
                f"TP {int(s['tp'])} | SL+ {int(s['slp'])} | BE {int(s['be'])} | SL {int(s['sl'])} | Timeout {int(s['timeout'])}",
                f"Total {s['sum_r']:+.2f}R | Max DD {_ls_rp(s['max_dd'])}"]
            for eng, es in sorted(self.engine_stats.items()):
                ne = int(es['trades'])
                if not ne: continue
                pfe = (es['gross_win'] / es['gross_loss']) if es['gross_loss'] > 0 else (math.inf if es['gross_win'] > 0 else 0.0)
                head.append(f"• {LS_ENGINE_LABELS.get(eng, eng)}: {ne} trade | WR {es['wins'] / ne * 100.0:.0f}% | PF {_ls_pf(pfe)} | {es['sum_r']:+.2f}R | {_ls_rp(es['pnl'])}")
            return '\n'.join(head + ['', self.positions_text()])


def ls_build_caption(pair, sig, entry, candle_ts, pos, info, account):
    dec = LS_SPECS[pair]['dec']; up = sig.direction == 'BUY'; p = lambda x: _ls_fp(pair, x)
    risk = abs(entry - sig.stop); reward = abs(sig.target - entry)
    is_pdf = sig.engine == 'PDF'; label = LS_ENGINE_LABELS.get(sig.engine, sig.engine)
    rr_text = f"{reward / risk:.1f}R, zona lawan" if is_pdf else f"{LS_RR:.1f}R"
    lines = [f"{'🟢' if up else '🔴'} <b>{sig.direction} {pair}</b> · {label} 1M",
             f"🕒 {_ls_local(candle_ts + LS_INTERVAL_MS)}" + ('' if is_pdf else f" · skor {sig.score}/5"), '',
             f"📍 Entry: <code>{p(entry)}</code>", f"🛑 SL: <code>{p(sig.stop)}</code> (-{risk:.{dec}f})",
             f"🎯 TP: <code>{p(sig.target)}</code> (+{reward:.{dec}f} · {rr_text})"]
    if LS_TRAILING:
        lines.append(f"🔒 BE di +{LS_BE_TRIGGER_R:.1f}R, lalu trailing SL+ dari +{LS_TRAIL_START_R:.1f}R (jarak {LS_TRAIL_GAP_R:.1f}R dari puncak)")
    if is_pdf:
        ex = sig.extra
        lines += ['', '🧠 ' + ' → '.join(_html.escape(x) for x in ex['reasons'][:3]),
                  f"Zona <code>{p(ex['range_low'])} – {p(ex['range_high'])}</code> · level reclaim <code>{p(sig.sweep_level)}</code>",
                  f"Sweep {_ls_hm(ex['sweep_ts'])} · breakbody {_ls_hm(ex['break_ts'])} · retest {_ls_hm(candle_ts)} WIB",
                  _html.escape(ex['reasons'][3]) + ' · TP = sisi lawan range', '']
    else:
        lines += ['', f"🧠 {_html.escape(sig.reason)}", f"Level disapu <code>{p(sig.sweep_level)}</code> · ATR14 {sig.atr:.{dec}f}", '']
    if pos is not None:
        tp_pnl = (reward - ls_cost_per_unit(pair, entry, sig.target)) * pos.qty
        lines.append(f"💼 Paper: {_ls_qty(pair, pos.qty)} · risiko {_ls_rp(pos.risk_usd)} ({info.get('risk_pct', 0.0):.1f}%)")
        lines.append(f"Jika TP ≈ {_ls_rp(tp_pnl)} · Saldo {_ls_rp(account.balance)}")
        if info.get('status') == 'min_lot': lines.append('⚠️ Lot minimum dipakai (modal kecil) — risiko di atas target.')
    else:
        lines.append(f"💼 Paper: SKIP — {_html.escape(str(info.get('skip')))}")
    return '\n'.join(lines)


def ls_format_signal(pair, candles):
    inds = ls_build_indicators(candles); found = {x.engine: x for x in ls_scan_engines(pair, candles, inds)}
    c = candles[-1]; p = lambda x: _ls_fp(pair, x)
    lines = [f'LIQUIDITY SNIPER 1M — {pair}', f'Harga close: {p(c.close)}', f'Data: {c.time_utc}']
    for eng in LS_ENGINES:
        sig = found.get(eng); label = LS_ENGINE_LABELS[eng]
        if sig is None:
            lines.append(f'[{label}] belum ada setup pada candle terakhir yang sudah close.'); continue
        rr = abs(sig.target - sig.entry_reference) / max(sig.risk, 1e-9)
        lines += [f'[{label}] SINYAL: {sig.direction}', f'  Entry acuan (close): {p(sig.entry_reference)}',
                  f'  SL: {p(sig.stop)} | TP: {p(sig.target)} ({rr:.1f}R)', f'  Konfirmasi: {sig.reason}']
    lines.append('Catatan: entry backtest memakai OPEN candle berikutnya; sinyal bukan instruksi transaksi.')
    return '\n'.join(lines)


LS_ACCOUNT = LSAccount(LS_STATE_FILE)
LS_LAST = dict(LS_ACCOUNT.last_scanned)
LS_BT_LOCK = threading.Lock()
LS_BT_LAST = {}
LS_STARTED = time.time()


def ls_handle_signal(pair, rows, candles, sig):
    c = candles[-1]; entry = c.close; topic = _ls_topic(pair, sig.engine)
    pos, info = LS_ACCOUNT.open_from_signal(pair, sig, entry)
    for line in info.get('topups', []): send(line, html_mode=True, topic=topic)
    caption = ls_build_caption(pair, sig, entry, c.ts, pos, info, LS_ACCOUNT)
    _ls_log(f"[{pair}] SINYAL {sig.engine} {sig.direction} entry {_ls_fp(pair, entry)} SL {_ls_fp(pair, sig.stop)} TP {_ls_fp(pair, sig.target)} | "
            f"paper: {('qty ' + str(pos.qty)) if pos else info.get('skip')}")
    try:
        D = build(rows, {}); idx = len(rows) - 1
        extras = [(sig.extra['range_high'], 'RANGE H'), (sig.extra['range_low'], 'RANGE L')] if sig.engine == 'PDF' else []
        cand = {'side': 'L' if sig.direction == 'BUY' else 'S', 'kind': 'PDF SHADOW FAKEOUT' if sig.engine == 'PDF' else 'SHADOW FAKEOUT',
                'entry': entry, 'sl': sig.stop, 'tp': sig.target, 'extras': extras, 'score': sig.score, 'i': idx, 'live': {}}
        png = _price_chart(D, idx, '1m', STATE['offset'] if pair == 'XAUUSD' else 0.0, [], cand, bars=LS_CHART_BARS,
                           diagnostic=False, pair_label=pair, src_label=ls_symbol(pair))
        _send_png(png, caption, f"LS_{sig.engine}_{pair}_1m_{datetime.fromtimestamp(c.ts / 1000, WIB):%Y%m%d_%H%M}.png", topic=topic)
    except Exception as exc:
        _ls_log(f'[{pair}] chart gagal ({exc}); kirim teks'); send(caption, html_mode=True, topic=topic)


def ls_process_pair(pair):
    rows, candles = ls_fetch(pair, 300)
    if len(candles) < 80: return
    latest = candles[-1]
    LS_ACCOUNT.set_mark(pair, latest.close)
    for eng, text in LS_ACCOUNT.advance_tagged(pair, candles): send(text, html_mode=True, topic=_ls_topic(pair, eng))
    if LS_LAST.get(pair) == latest.ts: return
    baseline = pair not in LS_LAST
    LS_LAST[pair] = latest.ts; LS_ACCOUNT.mark_scanned(pair, latest.ts)
    if baseline:
        _ls_log(f'[{pair}] baseline di candle {latest.time_utc}; sinyal dimulai dari candle berikutnya'); return
    age_s = time.time() - (latest.ts + LS_INTERVAL_MS) / 1000.0
    if age_s > 3 * LS_INTERVAL_MS / 1000.0:
        _ls_log(f'[{pair}] candle {latest.time_utc} sudah basi ({age_s:.0f}s); sinyal dilewati'); return
    inds = ls_build_indicators(candles)
    for sig in ls_scan_engines(pair, candles, inds):
        if sig.engine == 'PDF':   # retest bisa berulang di candle berikutnya -> satu setup satu sinyal
            key = f"{sig.direction}:{sig.extra.get('sweep_ts')}"
            if not LS_ACCOUNT.is_new_setup(pair, 'PDF', key):
                _ls_log(f'[{pair}] setup PDF yang sama sudah pernah dikirim; dilewati'); continue
        sig.symbol = pair; ls_handle_signal(pair, rows, candles, sig)


def ls_pair_loop(pair):
    _ls_log(f'[{pair}] Liquidity Sniper thread mulai')
    first = True
    while True:
        if not first:
            now = time.time(); target = (int(now // 60) + 1) * 60 + LS_POLL_OFFSET + random.uniform(0.0, 1.2)
            time.sleep(max(0.0, target - now))
        first = False
        expected = (int(time.time()) // 60 - 1) * 60_000           # open-ts candle terakhir yang pasti sudah close
        for _ in range(6):
            try:
                _, cs = ls_fetch(pair, 300)
                if cs and cs[-1].ts >= expected: break
            except Exception as exc:
                _ls_log(f'[{pair}] fetch: {exc}')
            time.sleep(1.5)
        try: ls_process_pair(pair)
        except Exception: _ls_log(f'[{pair}] error proses:\n{traceback.format_exc()}')


def _ls_send_doc(data, filename, caption):
    if not (TG_CHAT and _valid_token()): return
    boundary = '----xauBotDoc' + str(int(time.time() * 1000))
    fields = {'chat_id': TG_CHAT, 'caption': caption[:1000]}
    _th = _eff_topic(None)
    if _th: fields['message_thread_id'] = _th
    body = b''.join((f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n').encode() for k, v in fields.items())
    body += (f'--{boundary}\r\nContent-Disposition: form-data; name="document"; filename="{filename}"\r\nContent-Type: text/csv\r\n\r\n').encode() + data + f'\r\n--{boundary}--\r\n'.encode()
    req = urllib.request.Request(f'https://api.telegram.org/bot{TG_TOKEN}/sendDocument', data=body, headers={'Content-Type': f'multipart/form-data; boundary={boundary}'})
    try:
        with urllib.request.urlopen(req, timeout=60) as r: resp = json.loads(r.read().decode())
        if not resp.get('ok'): raise RuntimeError(resp.get('description', 'sendDocument gagal'))
    except Exception as exc:
        _ls_log(f'kirim dokumen gagal: {exc}')


def ls_backtest_job(pairs, days, release, engines=None):
    try:
        for pair in pairs:
            try:
                candles = ls_history(pair, min(days, 30))
                for eng in (engines or list(LS_ENGINES)):
                    try:
                        trades, s_on = ls_backtest(candles, pair, trailing=True, engine=eng)
                        _, s_off = ls_backtest(candles, pair, trailing=False, engine=eng)
                    except RuntimeError as exc:
                        send(f'BACKTEST {pair} [{eng}] dilewati: {exc}'); continue
                    send(ls_format_backtest(s_on, s_off))
                    if trades: _ls_send_doc(ls_trades_csv(trades), f'backtest_{pair}_{eng}.csv', f'Detail trade {pair} [{eng}]')
            except Exception as exc:
                send(f'BACKTEST {pair} GAGAL: {exc}')
    finally:
        if release is not None: release.release()


def ls_command(c, args):
    base = c.replace('/ls', '/', 1) if c.startswith('/ls') and c != '/lsbt' else c
    if base in ('/cek', '/signal'):
        try: pair = ls_normalize_pair(args[0]) if args else 'ALL'
        except ValueError: send('Pair salah. Gunakan /cek XAU, /cek BTC, atau /cek all'); return
        for p in (LS_PAIRS if pair == 'ALL' else [pair]):
            try:
                _, cs = ls_fetch(p, 300)
                if len(cs) < 80: send(f'Data {p} belum cukup ({len(cs)} candle).'); continue
                send(ls_format_signal(p, cs))
                if p == 'XAUUSD' and ICM_ENABLED:
                    _sy, _rw, _m15, _sg = icm_signal_now()
                    if _sg is not None: icm_send_alert(_sg, _rw, _sy)
            except Exception as exc:
                send(f'Gagal cek {p}: {exc}')
    elif base == '/saldo': send(LS_ACCOUNT.summary_text(), html_mode=True)
    elif base == '/posisi': send(LS_ACCOUNT.positions_text(), html_mode=True)
    elif base == '/reset':
        if args and args[0].lower() == 'confirm':
            LS_ACCOUNT.reset(); send(f'Paper account Liquidity Sniper di-reset. Saldo {_ls_rp(LS_ACCOUNT.balance)}.')
        else: send('Ini menghapus saldo, posisi, dan statistik paper Liquidity Sniper. Ketik: /reset confirm')
    elif base == '/stats':
        lines = [f"<b>LIQUIDITY SNIPER 1M</b> — uptime {(time.time() - LS_STARTED) / 3600:.1f} jam",
                 f"Engine aktif: {', '.join(LS_ENGINE_LABELS[e] for e in LS_ENGINES)} | TP sniper {LS_RR:.1f}R | "
                 + (f"BE +{LS_BE_TRIGGER_R:.1f}R, trailing {LS_TRAIL_START_R:.1f}R/{LS_TRAIL_GAP_R:.1f}R" if LS_TRAILING else 'tanpa trailing')
                 + f" | biaya {'ON' if LS_COSTS else 'OFF'}", '', '<b>Feed</b>']
        for p in LS_PAIRS:
            ts = LS_LAST.get(p)
            lines.append(f"{p}: " + (f"candle terakhir {_ls_local(ts)} ({(time.time() - ts / 1000) / 60:.0f} mnt lalu)" if ts else 'belum ada data'))
        lines += ['', f"Saldo paper {_ls_rp(LS_ACCOUNT.balance)} | Net P/L {_ls_rp(LS_ACCOUNT.net_pnl())} | {LS_ACCOUNT.topups}× top-up"]
        send('\n'.join(lines), html_mode=True)
    elif base == '/lsbt':
        days, pair, engines = 1, 'ALL', list(LS_ENGINES)
        for arg in args:
            if arg.isdigit(): days = max(1, min(int(arg), 30))
            elif arg.upper() in ('SNIPER', 'PDF'): engines = [arg.upper()]
            else:
                try: pair = ls_normalize_pair(arg)
                except ValueError: send('Format: /lsbt [hari 1-30] [XAU/BTC/ETH/SOL/XRP/ALL] [sniper/pdf]. Contoh: /lsbt 3 all pdf'); return
        wait = LS_BT_COOLDOWN - (time.time() - LS_BT_LAST.get('t', 0.0))
        if wait > 0: send(f'Tunggu {wait:.0f} detik lagi sebelum backtest berikutnya.'); return
        if not LS_BT_LOCK.acquire(blocking=False): send('Backtest lain masih berjalan. Tunggu laporannya selesai.'); return
        LS_BT_LAST['t'] = time.time(); pairs = LS_PAIRS if pair == 'ALL' else [pair]
        send(f"Backtest {', '.join(pairs)} {days} hari [{', '.join(engines)}] dimulai (simulasi, tanpa order).")
        threading.Thread(target=_with_reply, args=(getattr(_TLS, 'reply_topic', ''), ls_backtest_job, pairs, days, LS_BT_LOCK, engines), daemon=True).start()


# ───────── ICHIMOKU M15 (INDEPENDEN, XAUUSD) — port dari Liquidity Sniper v3 ─────────
# Dua mode: FAKEOUT (manipulasi menembus Kumo lalu ditolak, gaya AMD) dan BREAK (candle tembus Kumo dari sisi seberang).
# Skor konfirmasi 0-9, minimal ICH_M15_MIN_SCORE. Order LIMIT bertingkat (rung), SL 60-150 pip (1 pip = $0.10), TP1/TP2.
# Seperti di v3: ALERT SAJA, belum masuk saldo paper.
ICM_ENABLED = _lsb('ICH_M15_ENABLED', True)
ICM_SCORE_MIN = max(1, min(9, int(_lsf('ICH_M15_MIN_SCORE', 5))))
ICM_CTX_BARS = max(4, int(_lsf('ICH_M15_CTX_BARS', 8)))
ICM_CTX_MIN = max(1, min(ICM_CTX_BARS, int(_lsf('ICH_M15_CTX_MIN', 4))))
ICM_WICK_MIN = min(0.9, max(0.1, _lsf('ICH_M15_WICK_MIN', 0.35)))
ICM_WICK_ATR = max(0.0, _lsf('ICH_M15_WICK_ATR', 0.40))
ICM_ACCUM_BARS = max(5, int(_lsf('ICH_M15_ACCUM_BARS', 10)))
ICM_ACCUM_ATR = max(1.0, _lsf('ICH_M15_ACCUM_ATR', 4.0))
ICM_STEP_ATR = max(0.05, _lsf('ICH_M15_STEP_ATR', 0.25))
ICM_RUNGS = max(1, min(6, int(_lsf('ICH_M15_RUNGS', 3))))
ICM_TP1_R = max(0.5, _lsf('ICH_M15_TP1_R', 1.2))
ICM_TP2_R = max(ICM_TP1_R, _lsf('ICH_M15_TP2_R', 2.0))
ICM_SL_MIN_PIPS = max(1.0, _lsf('ICH_M15_SL_MIN_PIPS', 60))
ICM_SL_MAX_PIPS = max(ICM_SL_MIN_PIPS, _lsf('ICH_M15_SL_MAX_PIPS', 150))
ICM_PIP_XAU = max(0.001, _lsf('ICH_M15_PIP_XAU', 0.10))
ICM_MODES = tuple(x for x in (os.getenv('ICH_M15_MODES') or 'FAKEOUT,BREAK').upper().replace(' ', '').split(',') if x in ('FAKEOUT', 'BREAK')) or ('FAKEOUT', 'BREAK')
ICM_MIN_BARS = 82
ICM_MS = 900_000


@dataclass
class ICMSignal:
    direction: str
    mode: str
    candle_ts: int
    entry: float
    levels: tuple
    sl: float
    tp1: float
    tp2: float
    risk: float
    atr: float
    conf: list
    reason: str
    score: int


def _icm_mid(candles, i, period):
    if i + 1 < period: return None
    rows = candles[i - period + 1:i + 1]
    return (max(x.high for x in rows) + min(x.low for x in rows)) / 2.0


def icm_arrays(candles):
    n = len(candles)
    keys = ('tenkan', 'kijun', 'span_a', 'span_b', 'cloud_top', 'cloud_bot', 'atr')
    A = {k: [None] * n for k in keys}
    for i, c in enumerate(candles):
        A['tenkan'][i] = _icm_mid(candles, i, 9)
        A['kijun'][i] = _icm_mid(candles, i, 26)
        source_i = i - 26                  # Kumo yang tampil dihitung 26 bar sebelumnya
        if source_i >= 0:
            src_t, src_k = A['tenkan'][source_i], A['kijun'][source_i]
            A['span_a'][i] = (src_t + src_k) / 2.0 if src_t is not None and src_k is not None else None
            A['span_b'][i] = _icm_mid(candles, source_i, 52)
        if i >= 13:
            A['atr'][i] = sum(max(x.high - x.low, abs(x.high - candles[j-1].close) if j else x.high - x.low,
                                  abs(x.low - candles[j-1].close) if j else x.high - x.low)
                              for j, x in enumerate(candles[i-13:i+1], start=i-13)) / 14.0
        sa, sb = A['span_a'][i], A['span_b'][i]
        if sa is not None and sb is not None:
            A['cloud_top'][i], A['cloud_bot'][i] = max(sa, sb), min(sa, sb)
    A['pos'] = [0] * n
    for i, c in enumerate(candles):
        top, bot = A['cloud_top'][i], A['cloud_bot'][i]
        if top is not None and bot is not None:
            A['pos'][i] = 1 if c.close > top else (-1 if c.close < bot else 0)
    return A


def icm_core(candles, A, t, sell, mode):
    c = candles[t]; top = A['cloud_top'][t]; bot = A['cloud_bot'][t]; atr = A['atr'][t]
    if top is None or bot is None or atr is None or atr <= 0 or t < ICM_CTX_BARS + 1: return None
    pos = A['pos']; rng = c.high - c.low
    if rng <= 0: return None
    if mode == 'FAKEOUT':
        side_pos = -1 if sell else 1
        if sum(1 for q in pos[t-ICM_CTX_BARS:t] if q == side_pos) < ICM_CTX_MIN: return None
        if sell:
            if pos[t-1] > 0 or not (c.high > bot and c.close < bot): return None
            wick = c.high - max(c.open, c.close); edge, far, ext = bot, top, c.high
        else:
            if pos[t-1] < 0 or not (c.low < top and c.close > top): return None
            wick = min(c.open, c.close) - c.low; edge, far, ext = top, bot, c.low
        if wick < ICM_WICK_MIN * rng or wick < ICM_WICK_ATR * atr: return None
        return float(edge), float(far), float(ext)
    if sell:
        if not (pos[t] == -1 and pos[t-1] >= 0 and c.close < c.open and (bot - c.close) >= 0.15 * atr and abs(c.close - c.open) >= 0.5 * rng): return None
        return float(bot), float(top), float(top)
    if not (pos[t] == 1 and pos[t-1] <= 0 and c.close > c.open and (c.close - top) >= 0.15 * atr and abs(c.close - c.open) >= 0.5 * rng): return None
    return float(top), float(bot), float(bot)


def icm_confluence(candles, A, t, sell, mode, far):
    c = candles[t]; atr = float(A['atr'][t] or 0.0); rng = max(c.high - c.low, 1e-12); out = []
    n = ICM_ACCUM_BARS; pre = candles[t-n:t]; ph = max(x.high for x in pre); pl = min(x.low for x in pre)
    if mode == 'FAKEOUT':
        if (c.high > ph) if sell else (c.low < pl): out.append('sweep liquidity di ' + ('atas' if sell else 'bawah') + ' range akumulasi')
        if ph - pl <= ICM_ACCUM_ATR * atr: out.append(f'akumulasi rapat ({n} bar)')
        if (c.high > far) if sell else (c.low < far): out.append('manipulasi menembus seluruh awan')
        if ((c.close - c.low) / rng <= 0.35) if sell else ((c.close - c.low) / rng >= 0.65): out.append('close dekat ujung candle')
    else:
        came = A['pos'][max(0, t-6):t]
        if any(x == (1 if sell else -1) for x in came): out.append('tembus awan dari sisi seberang')
        if abs(c.close - c.open) >= 0.7 * rng: out.append('body dominan >=70%')
        if abs(c.close - c.open) >= atr: out.append('displacement >=1 ATR')
        if ((c.close - c.low) / rng <= 0.25) if sell else ((c.close - c.low) / rng >= 0.75): out.append('close di ujung candle')
    tk = A['tenkan'][t]; kj = A['kijun'][t]
    if tk is not None and kj is not None and ((tk < kj) if sell else (tk > kj)): out.append('Tenkan/Kijun searah')
    if t >= 3 and A['kijun'][t-3] is not None and kj is not None and ((kj < A['kijun'][t-3]) if sell else (kj > A['kijun'][t-3])): out.append('Kijun miring searah')
    if t >= 26 and ((c.close < candles[t-26].close) if sell else (c.close > candles[t-26].close)): out.append('Chikou searah')
    sa, sb = A['span_a'][t], A['span_b'][t]
    if sa is not None and sb is not None and ((sa < sb) if sell else (sa > sb)): out.append('awan searah')
    vm = sum(x.volume for x in candles[t-20:t]) / 20 if t >= 20 else 0.0
    if vm > 0 and c.volume >= 1.2 * vm: out.append('volume lonjakan')
    return out


def icm_detect(m15):
    """Candle M15 CLOSED terurut naik; evaluasi hanya bar terakhir. XAUUSD saja."""
    if not ICM_ENABLED or len(m15) < ICM_MIN_BARS: return None
    A = icm_arrays(m15); t = len(m15) - 1; found = None
    for mode in ICM_MODES:
        for sell in (True, False):
            core = icm_core(m15, A, t, sell, mode)
            if core is None: continue
            edge, far, ext = core
            conf = icm_confluence(m15, A, t, sell, mode, far)
            if len(conf) < ICM_SCORE_MIN: continue
            found = (mode, sell, edge, far, ext, conf); break
        if found: break
    if not found: return None
    mode, sell, edge, far, ext, conf = found; atr = float(A['atr'][t] or 0.0)
    if atr <= 0: return None
    side = 'SELL' if sell else 'BUY'; step = max(ICM_STEP_ATR * atr, 0.01)
    if sell: lim = min(far + 0.5 * step, ext) if mode == 'FAKEOUT' else far + 0.5 * step
    else: lim = max(far - 0.5 * step, ext) if mode == 'FAKEOUT' else far - 0.5 * step
    levels = []
    for k in range(ICM_RUNGS):
        px = edge + k * step if sell else edge - k * step
        if k and ((px > lim) if sell else (px < lim)): break
        levels.append(round(px, 2))
    levels = list(dict.fromkeys(levels))
    if not levels: return None
    entry = levels[0]; last = levels[-1]; buf = 0.15 * atr
    base = max(far, ext, last) if sell else min(far, ext, last)
    sl_raw = base + buf if sell else base - buf
    min_dist = ICM_SL_MIN_PIPS * ICM_PIP_XAU; max_dist = ICM_SL_MAX_PIPS * ICM_PIP_XAU
    dist = abs(sl_raw - entry)
    if dist > max_dist: return None
    if dist < min_dist: sl_raw = entry + min_dist if sell else entry - min_dist
    sl_raw = max(sl_raw, last + buf) if sell else min(sl_raw, last - buf)
    sl = round(sl_raw, 2); risk = abs(sl - entry)
    if risk <= 0: return None
    sign = -1.0 if sell else 1.0
    tp1 = round(entry + sign * ICM_TP1_R * risk, 2); tp2 = round(entry + sign * ICM_TP2_R * risk, 2)
    return ICMSignal(direction=side, mode=mode, candle_ts=m15[t].ts, entry=entry, levels=tuple(levels), sl=sl, tp1=tp1, tp2=tp2,
                     risk=risk, atr=atr, conf=conf, reason=f'ICHIMOKU M15 {mode} | ' + '; '.join(conf), score=len(conf))


def icm_signal_now():
    sym = resolve_symbol()
    rows = fetch_recent(sym, 200, tf='15m')
    m15 = _ls_candles(rows, interval_ms=ICM_MS)
    keep = {c.ts for c in m15}; rows = [r for r in rows if int(r[0]) in keep]
    return sym, rows, m15, (icm_detect(m15) if len(m15) >= ICM_MIN_BARS else None)


def icm_caption(sig):
    off = STATE['offset']; up = sig.direction == 'BUY'; p = lambda x: f'{x + off:.2f}'
    dt = datetime.fromtimestamp((sig.candle_ts + ICM_MS) / 1000, WIB)
    rows = [f'Mode   {sig.mode}', f'Skor   {sig.score}/9', f'Close  {dt:%d-%m %H:%M} WIB'] + [f'Limit{k} {p(v)}' for k, v in enumerate(sig.levels, 1)]
    rows += [f'SL     {p(sig.sl)}', f'TP1    {p(sig.tp1)} ({ICM_TP1_R:g}R)', f'TP2    {p(sig.tp2)} ({ICM_TP2_R:g}R)',
             f'Risiko {sig.risk:.2f} ({sig.risk / ICM_PIP_XAU:.0f} pip)']
    conf = '\n'.join('• ' + _html.escape(x) for x in sig.conf)
    return (f'{"🟢" if up else "🔴"} <b>XAUUSD · M15 · {sig.direction}</b> · ICHIMOKU M15\n{_tbl(rows)}\nKonfirmasi:\n{conf}\n'
            f'Order LIMIT bertingkat; engine independen, ALERT SAJA (belum masuk saldo paper). Harga sudah ditambah offset {off:+.2f}.')


def icm_send_alert(sig, rows, sym):
    cap = icm_caption(sig)
    try:
        D = build(rows, {}); idx = len(rows) - 1; up = sig.direction == 'BUY'
        extras = [(sig.tp2, 'TP2')] + [(v, f'LIMIT{k}') for k, v in enumerate(sig.levels[1:], 2)]
        cand = {'side': 'L' if up else 'S', 'kind': 'ICH M15', 'entry': sig.entry, 'sl': sig.sl, 'tp': sig.tp1, 'tp_label': 'TP1',
                'extras': extras, 'score': sig.score, 'i': idx, 'live': {}}
        png = _price_chart(D, idx, '15m', STATE['offset'], [], cand, bars=110, diagnostic=False, pair_label='XAUUSD', src_label=sym)
        _send_png(png, cap, f"XAUUSD_ICHM15_{datetime.fromtimestamp(sig.candle_ts / 1000, WIB):%Y%m%d_%H%M}.png", topic=topic_for('ICH_M15'))
    except Exception as exc:
        print('chart ICH M15 gagal:', exc); send(cap, html_mode=True, topic=topic_for('ICH_M15'))


def icm_job(force=False):
    sym, rows, m15, sig = icm_signal_now()
    if sig is None: return None
    if not force and sig.candle_ts <= int(STATE.get('icm_last_ts', 0) or 0): return sig
    STATE['icm_last_ts'] = sig.candle_ts; save_state()
    _ls_log(f'[ICH M15] {sig.direction} {sig.mode} limit {sig.entry:.2f} SL {sig.sl:.2f} TP1 {sig.tp1:.2f} TP2 {sig.tp2:.2f} skor {sig.score}')
    icm_send_alert(sig, rows, sym)
    return sig


def icm_loop():
    while True:
        now = now_ms(); boundary = (now // ICM_MS + 1) * ICM_MS
        time.sleep(max(0, (boundary - now) / 1000) + SCAN_DELAY + 2)
        _guard('ich-m15', icm_job)


def icm_status_text():
    sym, rows, m15, sig = icm_signal_now(); off = STATE['offset']
    if len(m15) < ICM_MIN_BARS: return f'Data M15 baru {len(m15)} bar; butuh minimal {ICM_MIN_BARS}.'
    A = icm_arrays(m15); t = len(m15) - 1; c = m15[t]
    top, bot, atr = A['cloud_top'][t], A['cloud_bot'][t], A['atr'][t] or 0.0
    pos = 'di atas Kumo' if A['pos'][t] == 1 else ('di bawah Kumo' if A['pos'][t] == -1 else 'di dalam Kumo')
    dt = datetime.fromtimestamp((c.ts + ICM_MS) / 1000, WIB)
    r = [f'Close  {dt:%d-%m %H:%M} WIB', f'Harga  {c.close + off:.2f}  ATR {atr:.2f}', f'Posisi {pos}',
         f'Kumo   {bot + off:.1f} - {top + off:.1f}', f'Mode   {",".join(ICM_MODES)} | skor min {ICM_SCORE_MIN}',
         f'SL     {ICM_SL_MIN_PIPS:g}-{ICM_SL_MAX_PIPS:g} pip | {ICM_RUNGS} limit', 'Setup  ' + (sig.direction + ' ' + sig.mode if sig else 'belum ada')]
    return '<b>ICHIMOKU M15 — STATUS</b>\n' + _tbl(r) + '\nSinyal hanya bila candle M15 terakhir memenuhi aturan FAKEOUT atau BREAK dan skor cukup.'


def handle_cmd(text):
    global BE_ENABLED
    cmd=text.strip().split()
    if not cmd or not cmd[0].startswith('/'):return
    c=cmd[0].split('@')[0].lower(); args=cmd[1:]
    if c in ('/start','/help'):
        send('Perintah:\n/status\n/scan — chart live M5 + histori simulasi\n/rencana [TF] — chart timeframe + histori\n/bt [hari] — backtest M15\n/offset <angka>\n/capital — modal paper dan P/L\n/rekap — hasil paper 7 hari\n/be [on|off] — BE/SL+ otomatis\n/ichi — status Ichimoku 4H paper | /ichi scan — chart sekarang\n/cr — status Counter Retail | /bcr [hari] — backtest | /crowd — posisi akun\n/cek [XAU|BTC|all] — setup Liquidity Sniper 1M | /saldo | /posisi | /stats\n/lsbt [hari] [XAU|BTC|ETH|SOL|XRP|all] [sniper|pdf] — backtest LS | /reset confirm — reset paper LS\n/ichm15 — Ichimoku M15 XAU (alert limit) | /topicid — ID topic ini | /topics — peta topic per sinyal\n/scalp — status Scalper trend | /bts [hari] [wr] — backtest\nTF valid: '+', '.join(TF.keys()))
    elif c=='/status':
        try:px=float(api('/v5/market/tickers',{'category':'linear','symbol':resolve_symbol()})['list'][0]['lastPrice'])+STATE['offset']
        except Exception as e:px=f'err {e}'
        send(f'Bybit {SYMBOL} {px} | offset tampilan {STATE["offset"]:+.2f}\nAktif {len(STATE.get("active",[]))} | TP {STATE.get("tp",0)} | SL+ {STATE.get("slp",0)} | BE+ {STATE.get("be",0)} | SL {STATE.get("sl",0)} | TO {STATE.get("timeout",0)}\nBE/SL+ {"ON" if BE_ENABLED else "OFF"} | Modal {_rp(CAPITAL.data.get('balance', CAPITAL_START_IDR))}\nMinScore {MIN_SCORE} RR {RR}')
    elif c=='/capital':
        send(CAPITAL.summary_text(),html_mode=True)
    elif c=='/rekap':
        send(CAPITAL.recap_text(7),html_mode=True)
    elif c=='/be':
        if args and args[0].lower() in ('on','off'):
            BE_ENABLED=args[0].lower()=='on'
            STATE['be_enabled']=BE_ENABLED;save_state()
            send(f'BE/SL+ otomatis {"AKTIF" if BE_ENABLED else "NONAKTIF"}. Berlaku untuk pengelolaan trade berikutnya; stop yang sudah dipindah tidak dikembalikan.')
        else:
            send(f'BE/SL+ {"ON" if BE_ENABLED else "OFF"}\nBE trigger +{BE_TRIGGER_R:.2f}R → stop kunci +{BE_OFFSET_R:.2f}R\nSL+ trigger +{SLPLUS_TRIGGER_R:.2f}R → stop kunci +{SLPLUS_LOCK_R:.2f}R\nPergerakan stop diproses dari candle 1m yang sudah tutup; stop baru berlaku mulai candle berikutnya.')
    elif c=='/offset':
        if not args:send('Format: /offset -5.00');return
        try:STATE['offset']=float(args[0].replace(',','.'));save_state();send(f'Offset diset {STATE["offset"]:+.2f}')
        except ValueError:send('Format: /offset -5.00')
    elif c in ('/bt','/backtest'):
        days=int(args[0]) if args and args[0].isdigit() else 30
        send('Backtest berjalan...');send(run_backtest(min(365,max(1,days))))
    elif c=='/scan':
        send('🔎 Scan XAUUSD LIVE M5 berjalan. Mengambil candle, indikator, dan histori...')
        try:do_scan(force=True,announce_signal=False)
        except Exception as e:print('/scan error:',e);send(f'<b>SCAN ERROR</b>\n{str(e)[:450]}',html_mode=True)
    elif c=='/rencana':do_rencana(args)
    elif c=='/scalp':
        try:send(scalp_status_text(),html_mode=True)
        except Exception as e:print('/scalp error:',e);send(f'<b>SCALP ERROR</b>\n{str(e)[:450]}',html_mode=True)
    elif c=='/topicid':
        tid=getattr(_TLS,'reply_topic','')
        send(f'Topic ID chat ini: <code>{tid}</code>\nIsi ke variabel TG_TOPIC_NAMA di Railway, contoh TG_TOPIC_ICH_M15.' if tid else 'Pesan ini bukan di dalam topic (General), jadi tidak punya topic ID. Ketik /topicid di dalam sebuah topic.',html_mode=True)
    elif c=='/topics':send(topics_text(),html_mode=True)
    elif c in ('/ichm15','/icm'):
        try:
            sym,rows_,m15_,sig_=icm_signal_now()
            if sig_ is not None:icm_send_alert(sig_,rows_,sym)
            else:send(icm_status_text(),html_mode=True)
        except Exception as e:print('/ichm15 error:',e);send(f'<b>ICH M15 ERROR</b>\n{str(e)[:450]}',html_mode=True)
    elif c in ('/cek','/lscek','/signal','/saldo','/lssaldo','/posisi','/lsposisi','/stats','/lsstats','/reset','/lsreset','/lsbt'):
        ls_command(c, args)
    elif c=='/cr':
        try:send(cr_status_text(),html_mode=True)
        except Exception as e:print('/cr error:',e);send(f'<b>CR ERROR</b>\n{str(e)[:450]}',html_mode=True)
    elif c=='/crowd':
        try:send(crowd_text(),html_mode=True)
        except Exception as e:print('/crowd error:',e);send(f'<b>CROWD ERROR</b>\n{str(e)[:450]}',html_mode=True)
    elif c=='/bcr':
        days=int(args[0]) if args and args[0].isdigit() else 30
        days=min(180,max(3,days)); send(f'Backtest Counter Retail {days} hari berjalan' + (' (bisa 1-3 menit)...' if days>45 else '...'))
        try:send(run_cr_backtest(days),html_mode=True)
        except Exception as e:print('/bcr error:',e);send(f'<b>BCR ERROR</b>\n{str(e)[:450]}',html_mode=True)
    elif c=='/bts':
        days=int(args[0]) if args and args[0].isdigit() else 14
        wr=any(str(x).lower()=='wr' for x in args)
        days=min(180,max(3,days)); send(f'Backtest Scalper trend {days} hari berjalan' + (' (periode panjang, bisa 1-3 menit)...' if days>45 else '...'))
        try:send(run_scalp_backtest(days,wr_table=wr),html_mode=True)
        except Exception as e:print('/bts error:',e);send(f'<b>BTS ERROR</b>\n{str(e)[:450]}',html_mode=True)
    elif c=='/ichi':
        if args and args[0].lower()=='scan':
            send('🔎 Scan Ichimoku 4H berjalan...')
            try:ichi_job(force=True)
            except Exception as e:print('/ichi error:',e);send(f'<b>ICHI ERROR</b>\n{str(e)[:450]}',html_mode=True)
        else:send(_ichi_status_text(),html_mode=True)
    else:send(f'Perintah {c} tidak dikenal. Ketik /help. (Modal paper: /capital)')


def _guard(name,fn):
    try:fn()
    except Exception as e:print(f'[{name}] error:',e)

def scanner_loop():
    # Scan only after each M5 candle closes; tracker continues to manage stops on M1.
    ms=5*60_000
    while True:
        now=now_ms(); boundary=(now//ms+1)*ms
        time.sleep(max(0,(boundary-now)/1000)+SCAN_DELAY)
        def job():
            tf_key='5m'; sym=resolve_symbol(); rows=fetch_recent(sym,300,tf=tf_key)
            if len(rows)<80:return
            D=build(rows,fetch_oi_recent(sym,24));idx=len(rows)-1
            if D['t'][idx]==STATE.get('last_ts'):return
            sg=rencana_candidate(D,idx,live={'tdelta':trades_delta(sym),'obi':book_imbalance(sym)}) if LEGACY_ENABLE else None
            if not sg: sg=cr_live(sym,rows)
            if not sg: sg=scalp_live(sym,rows)
            STATE['last_ts']=D['t'][idx];save_state()
            if sg:
                deals=simulate_trades(D,start_i=max(60,idx-180),end_i=idx-1,max_hold=MAX_BARS*3)
                png=_price_chart(D,idx,tf_key,STATE['offset'],deals,sg,bars=110)
                _send_png(png,_signal_caption(sg,tf_key),f'XAUUSD_SIGNAL_{tf_key}_{datetime.fromtimestamp(D["t"][idx]/1000,WIB):%Y%m%d_%H%M}.png',topic=topic_for_kind(sg['kind']))
                register(sg)
        _guard('scanner',job)

def tracker_loop():
    while True:time.sleep(max(3,TRACK_SEC));_guard('tracker',check_active);_guard('ichi-sl',check_ichi_sl)

def telegram_loop():
    if not (TG_CHAT and _valid_token()):print('Telegram polling tidak aktif: TG_TOKEN atau TG_CHAT_ID belum valid.');return
    while True:
        try:
            # getUpdates uses POST body, supports long polling.
            r=tg('getUpdates',{'offset':STATE.get('tg_off',0),'timeout':25},timeout=40)
            for u in (r or {}).get('result',[]):
                STATE['tg_off']=u['update_id']+1; m=u.get('message') or {}; chat=m.get('chat') or {}
                if str(chat.get('id'))==str(TG_CHAT):
                    text=m.get('text','')
                    threading.Thread(target=_guard,args=('command',lambda t=text,th=m.get('message_thread_id'):_run_cmd(t,th)),daemon=True).start()
            save_state()
        except Exception as e:print('[telegram] error:',e);time.sleep(5)

def live():
    load_state();sym=resolve_symbol()
    send(f'Bot XAUUSD referensi via Bybit aktif — live signal M5, sumber {sym} | backtest /bt tetap M15 | offset {STATE["offset"]:+.2f}\nScan tiap candle M5 close +{SCAN_DELAY:g}s • tracker M1 tiap {TRACK_SEC:g}s • BE/SL+ {"ON" if BE_ENABLED else "OFF"}\nModal paper {_rp(CAPITAL.data.get('balance', CAPITAL_START_IDR))} | lot {TRADE_LOT:g} | max {MAX_RPS:g} req/s\nCatatan: sinyal/pembukuan paper saja; bukan harga atau order CFD broker.'+(f'\nIchimoku 4H auto: ON (paper/alert) | sisi {ICHI_SIDES} | risiko {ICHI_RISK_PCT*100:g}%' if ICHI_ENABLE else '')+(f'\nScalper Trend: ON | H4 trendline > FVG M15 > FVG M5 | SL ${SCALP_SL_MIN:g}-${SCALP_SL_MAX:g} | RR 1:{SCALP_RR:g} | SL+ {"trail" if SCALP_TRAIL else "off"} | cek /scalp' if SCALP_ENABLE else '\nScalper Trend: OFF')+('' if LEGACY_ENABLE else '\nSinyal engine lama: OFF')+(f'\nCounter Retail (stop-hunt): ON | SL ${CR_SL_MIN:g}-${CR_SL_MAX:g} | RR 1:{CR_RR:g} | cek /cr' if CR_ENABLE else '\nCounter Retail: OFF')+(f'\nLiquidity Sniper 1M v2: ON | engine {",".join(LS_ENGINES)} | {",".join(LS_PAIRS)} | TP {LS_RR:g}R | BE +{LS_BE_TRIGGER_R:g}R, trail {LS_TRAIL_START_R:g}R/{LS_TRAIL_GAP_R:g}R | risiko {LS_RISK_PCT:g}% | /saldo' if LS_ENABLE else '\nLiquidity Sniper 1M v2: OFF')+(f'\nIchimoku M15 XAU (alert): ON | {",".join(ICM_MODES)} | skor ≥{ICM_SCORE_MIN} | SL {ICM_SL_MIN_PIPS:g}-{ICM_SL_MAX_PIPS:g} pip | /ichm15' if ICM_ENABLED else '\nIchimoku M15 XAU: OFF'))
    threads=[threading.Thread(target=scanner_loop,daemon=True),threading.Thread(target=tracker_loop,daemon=True)]
    if ICHI_ENABLE:threads.append(threading.Thread(target=ichi_loop,daemon=True))
    if ICM_ENABLED:threads.append(threading.Thread(target=icm_loop,name='ich-m15',daemon=True))
    if LS_ENABLE:
        for _p in LS_PAIRS:threads.append(threading.Thread(target=ls_pair_loop,args=(_p,),name=f'ls-{_p}',daemon=True))
    if TG_CHAT and _valid_token():threads.append(threading.Thread(target=telegram_loop,daemon=True))
    for t in threads:t.start()
    try:
        while True:time.sleep(3600)
    except KeyboardInterrupt:save_state();print('berhenti.')

def _synthetic_demo():
    import random
    random.seed(5); now=(int(time.time()*1000)//900000)*900000-700*900000; rows=[]; p=4100.0
    for i in range(700):
        o=p; c=o+0.3*math.sin(i/25)+random.gauss(0,2); h=max(o,c)+abs(random.gauss(0,1.3)); l=min(o,c)-abs(random.gauss(0,1.3)); rows.append((now+i*900000,o,h,l,c,random.uniform(300,2000)));p=c
    D=build(rows,{});idx=len(rows)-1
    # Synthetic M1 path solely for renderer smoke-test; not market data/backtest evidence.
    rows_m1=[]
    for ts,oo,hh,ll,cc,vv in rows:
        for m1 in range(15):
            f0=m1/15.0; f1=(m1+1)/15.0
            mo=oo+(cc-oo)*f0; mc=oo+(cc-oo)*f1
            mh=max(mo,mc); ml=min(mo,mc)
            if m1==7:
                mh=max(mh,hh); ml=min(ml,ll)
            rows_m1.append((ts+m1*60_000,mo,mh,ml,mc,vv/15.0))
    deals=simulate_trades_m1(D,rows_m1,start_i=300,end_i=idx-1,tf_key='15m');sg=rencana_candidate(D,idx)
    png=_price_chart(D,idx,'15m',STATE['offset'],deals,sg,bars=110,diagnostic=not bool(sg))
    cap=_diagnostic_caption(D,idx,deals,'15m',{},sg).replace('sumber Bybit: None','SUMBER DATA: SINTETIS DEMO').replace('Simulasi strategi menggunakan candle Bybit, bukan histori order CFD broker / bukan order live. Bukan saran investasi.','Demo menggunakan candle sintetis untuk uji tampilan saja; bukan backtest pasar dan bukan saran investasi.')
    _send_png(png,cap,'XAUUSD_DEMO.png')

if __name__=='__main__':
    mode=sys.argv[1].lower() if len(sys.argv)>1 else 'live'
    try:
        if mode=='backtest':print(run_backtest(int(sys.argv[2]) if len(sys.argv)>2 else 30))
        elif mode=='demo':_synthetic_demo()
        elif mode=='rencana':do_rencana(sys.argv[2:])
        elif mode=='scan':do_scan()
        elif mode=='ichi':ichi_job(force=True)
        elif mode=='bts':send(run_scalp_backtest(),html_mode=True)
        elif mode=='bcr':send(run_cr_backtest(),html_mode=True)
        elif mode=='lsbt':ls_backtest_job(['XAUUSD','BTCUSDT'] if len(sys.argv)<4 else [ls_normalize_pair(sys.argv[3])], int(sys.argv[2]) if len(sys.argv)>2 else 3, None)
        else:live()
    except KeyboardInterrupt:print('berhenti.')
    except Exception as e:print('FATAL:',e);raise
