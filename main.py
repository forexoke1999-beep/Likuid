#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MAIN.PY — LIQUIDITY SNIPER 1M v4.1

Replaces the old mixed signal architecture with the complete Liquidity Sniper stack:
- Main engine: Shadow Fakeout + Ichimoku M1, plus independent XAUUSD Ichimoku M15.
- All engine source is embedded in this single main.py (no engines/ folder required):
  Shadow Fakeout PDF (SFP), Trend Pullback M5 (TPB), and Liquidity Sniper v2 (LV2),
  each with its own state and Telegram topic.
- Pairs: XAUUSD, BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, NAS100, USOIL, GBPJPY.
  The selected main-engine pair list can be restricted with BOT_PAIRS.
- Paper-only execution: staged BE / SL+ trailing and Telegram SL-update alerts across
  the main, SFP, TPB, and LV2 paper trackers; TP/SL outcomes, paper balance, reports,
  charts, and backtests. No live exchange orders.

Gold and index/FX feeds are proxies where noted; they are not guaranteed to match a broker CFD.
This is a market-scanning and simulation bot, not a broker execution engine or profit guarantee.
"""
from __future__ import annotations

import argparse
import re
import csv
import binascii
import struct
import zlib
import json
import math
import os
import sys
import time
import threading
import traceback
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

# Load a simple local .env for Termux/local runs; Railway Variables remain authoritative.
def _load_simple_env(path: str) -> None:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        pass

_load_simple_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# Backward compatibility with the old main.py variable names.
for _new, _old in (
    ("TELEGRAM_BOT_TOKEN", "TG_TOKEN"),
    ("BOT_TELEGRAM_CHAT_ID", "TG_CHAT_ID"),
    ("LIQ_TOPIC_ID", "TG_TOPIC_ID"),
    ("BOT_OWNER_IDS", "TG_OWNER_IDS"),
):
    if not (os.getenv(_new) or "").strip() and (os.getenv(_old) or "").strip():
        os.environ[_new] = os.environ[_old].strip()

VERSION = "4.1.0-main"
INTERVAL_MS = 60_000
YAHOO_SYMBOL = "XAUUSD=X"
BYBIT_SYMBOL = "BTCUSDT"
BYBIT_XAU_SYMBOL = "XAUTUSDT"  # tokenized gold; not identical to broker XAUUSD CFD
BYBIT_CRYPTO_PAIRS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"}
# Yahoo Finance proxies; harga TIDAK identik dengan CFD broker. NAS100 & USOIL = futures, GBPJPY = spot FX.
YAHOO_SYMBOLS = {"NAS100": "NQ=F", "USOIL": "CL=F", "GBPJPY": "GBPJPY=X"}
PROXY_NOTE = {"NAS100": "Nasdaq 100 futures proxy", "USOIL": "Crude Oil WTI futures proxy", "GBPJPY": "spot FX"}
PAIR_DISPLAY = {"XAUUSD": "XAUUSD", "BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT", "SOLUSDT": "SOLUSDT", "XRPUSDT": "XRPUSDT", "NAS100": "NAS100", "USOIL": "USOIL", "GBPJPY": "GBPJPY"}
ALL_PAIRS = ["XAUUSD", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "NAS100", "USOIL", "GBPJPY"]
YAHOO_LIMITED_PAIRS = {"XAUUSD", "NAS100", "USOIL", "GBPJPY"}
PAIR_HELP = "XAU/BTC/ETH/SOL/XRP/NAS100/USOIL/GBPJPY"
BYBIT_CATEGORY = "linear"
BYBIT_BASE = "https://api.bybit.com"
YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
USER_AGENT = "Mozilla/5.0 (Linux; Android 13; Termux) AppleWebKit/537.36 Chrome/120 Safari/537.36"

# Strategy knobs (kept explicit so the user can backtest/change them)
SWING_LOOKBACK = 12          # prior bars, excludes current bar
ATR_PERIOD = 14
TENKAN_PERIOD = 9
KIJUN_PERIOD = 26
SENKOU_B_PERIOD = 52
CLOUD_SHIFT = 26
RR_TARGET = 1.5
MAX_HOLD_BARS = 60
MAX_RISK_ATR = 3.0
STOP_ATR_BUFFER = 0.08
SIGNAL_HISTORY_FOR_REPORT = 10

# Paper account settings. All amounts are simulated Indonesian rupiah (IDR).
PAPER_INITIAL_IDR = 1_000_000
PAPER_TOPUP_IDR = 500_000
PAPER_MC_RATIO = 0.10          # simulated margin call if equity <= 10% of total deposits
PAPER_RISK_PCT = 0.01          # simulated cash risk for a full -1R loss
BE_TRIGGER_R = 0.70            # move initial SL to break-even after +0.70R
LOCK_TRIGGER_R = 1.00          # start locking profit after +1.00R
LOCK_MIN_R = 0.20              # after +1R, protect at least +0.20R
TRAIL_ATR_MULT = 0.60          # trailing distance after +1R
TRAIL_R_MULT = 0.35            # minimum trailing distance, measured in initial R
APP_DIR = os.path.dirname(os.path.abspath(__file__))
_VOLUME_DIR = (os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or "").strip()
DATA_DIR = (os.getenv("BOT_DATA_DIR") or _VOLUME_DIR or APP_DIR).strip()
STATE_PATH = (os.getenv("PAPER_STATE_PATH") or os.path.join(DATA_DIR, "paper_state.json")).strip()
CHART_DIR = (os.getenv("CHART_DIR") or os.path.join(DATA_DIR, "charts")).strip()
OUTPUT_DIR = (os.getenv("OUTPUT_DIR") or os.path.join(DATA_DIR, "output")).strip()
for _directory in (DATA_DIR, os.path.dirname(os.path.abspath(STATE_PATH)), CHART_DIR, OUTPUT_DIR):
    try:
        os.makedirs(_directory, exist_ok=True)
    except OSError as _dir_error:
        print(f"[Storage] Tidak dapat membuat folder {_directory}: {_dir_error}", flush=True)
DATA_SCAN_INTERVAL_SEC = max(15, int(os.getenv("DATA_SCAN_INTERVAL_SEC", "60")))
STALE_SIGNAL_MIN = max(3, int(os.getenv("STALE_SIGNAL_MIN", "15")))
DATA_SOURCE_USED: dict[str, str] = {}


@dataclass(frozen=True)
class Candle:
    ts: int        # UTC Unix timestamp, milliseconds, candle open time
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    @property
    def time_utc(self) -> str:
        return datetime.fromtimestamp(self.ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


@dataclass
class Signal:
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
    # Engine metadata: the original 1M engine remains unchanged; ICH is independent.
    engine: str = "SHADOW FAKEOUT + ICHIMOKU 1M"
    target2: float = 0.0
    entry_levels: tuple[float, ...] = ()
    order_type: str = "NEXT_OPEN"


@dataclass
class Trade:
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


def http_json(url: str, timeout: int = 20) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
            return json.loads(raw)
        except urllib.error.HTTPError as exc:
            # HTTP 429 is a provider throttle, not a transient network error.
            # Retrying it three times for every index every minute worsens the block.
            if exc.code == 429:
                raise RuntimeError("HTTP 429: provider membatasi request; retry cepat dihentikan") from exc
            last_error = exc
            if attempt < 2:
                time.sleep(1.0 + attempt)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(1.0 + attempt)
    raise RuntimeError(f"Gagal mengambil data API: {last_error}")


def _clean_candles(candles: list[Candle], now_ms: Optional[int] = None) -> list[Candle]:
    """Sort, de-duplicate timestamps, reject invalid rows and unfinished bars."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    by_ts: dict[int, Candle] = {}
    for c in candles:
        vals = (c.open, c.high, c.low, c.close)
        if not all(math.isfinite(x) and x > 0 for x in vals):
            continue
        if c.high < max(c.open, c.close, c.low) or c.low > min(c.open, c.close, c.high):
            continue
        # Bar must have fully closed. A bar opened at 12:34 is closed at 12:35 UTC.
        if c.ts + INTERVAL_MS > now_ms:
            continue
        by_ts[c.ts] = c
    return [by_ts[k] for k in sorted(by_ts)]


def fetch_yahoo_symbol(symbol: str, days: int = 1) -> list[Candle]:
    """Fetch 1-minute OHLC candles from Yahoo Finance chart API (maximum 7 days)."""
    days = max(1, min(int(days), 7))
    now = int(time.time())
    start = now - days * 86400
    params = urllib.parse.urlencode({
        "period1": start, "period2": now, "interval": "1m",
        "includePrePost": "true", "events": "div,splits",
    })
    encoded = urllib.parse.quote(symbol, safe="=^.-")
    path = f"{encoded}?{params}"
    try:
        payload = http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{path}", timeout=25)
    except Exception as first_error:
        try:
            payload = http_json(f"https://query2.finance.yahoo.com/v8/finance/chart/{path}", timeout=25)
        except Exception as second_error:
            raise RuntimeError(f"Yahoo {symbol} query1 dan query2 gagal. query1={first_error}; query2={second_error}") from second_error
    chart = payload.get("chart", {})
    if chart.get("error"):
        raise RuntimeError(f"Yahoo Finance {symbol}: {chart['error']}")
    results = chart.get("result") or []
    if not results:
        raise RuntimeError(f"Yahoo Finance tidak mengembalikan candle untuk simbol {symbol}; feed/simbol 1m mungkin tidak tersedia.")
    result = results[0]
    stamps = result.get("timestamp") or []
    qlist = result.get("indicators", {}).get("quote") or []
    if not qlist:
        raise RuntimeError(f"Format data candle Yahoo {symbol} tidak lengkap (quote kosong).")
    q = qlist[0]
    opens, highs, lows, closes = q.get("open", []), q.get("high", []), q.get("low", []), q.get("close", [])
    volumes = q.get("volume", [])
    candles: list[Candle] = []
    for i, ts in enumerate(stamps):
        try:
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            if o is None or h is None or l is None or c is None:
                continue
            vol = volumes[i] if i < len(volumes) and volumes[i] is not None else 0
            candles.append(Candle(int(ts * 1000), float(o), float(h), float(l), float(c), float(vol or 0)))
        except (IndexError, TypeError, ValueError):
            continue
    cleaned = _clean_candles(candles)
    if len(cleaned) < 80:
        raise RuntimeError(f"Data Yahoo {symbol} terlalu sedikit: {len(cleaned)} candle closed. Minimal 80 dibutuhkan.")
    return cleaned


def fetch_yahoo_xau(days: int = 1) -> list[Candle]:
    """Compatibility wrapper for Yahoo XAUUSD=X."""
    return fetch_yahoo_symbol("XAUUSD=X", days)


def fetch_bybit_symbol(symbol: str, days: int = 1, category: str = "linear") -> list[Candle]:
    """Fetch closed 1-minute candles from Bybit V5, paginating backward."""
    days = max(1, min(int(days), 30))
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 86400 * 1000
    cursor_end = now_ms - 1
    all_candles: list[Candle] = []
    seen_min: Optional[int] = None
    max_pages = math.ceil(days * 1440 / 950) + 4
    for _ in range(max_pages):
        params = urllib.parse.urlencode({
            "category": category,
            "symbol": symbol,
            "interval": "1",
            "start": start_ms,
            "end": cursor_end,
            "limit": 1000,
        })
        payload = http_json(f"{BYBIT_BASE}/v5/market/kline?{params}", timeout=20)
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit API retCode={payload.get('retCode')}: {payload.get('retMsg', 'unknown error')}")
        rows = (payload.get("result") or {}).get("list") or []
        if not rows:
            break
        page: list[Candle] = []
        for row in rows:
            try:
                page.append(Candle(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])))
            except (IndexError, TypeError, ValueError):
                continue
        if not page:
            break
        all_candles.extend(page)
        page_min = min(c.ts for c in page)
        if seen_min is not None and page_min >= seen_min:
            # Guard against a server/pagination response that doesn't move backward.
            break
        seen_min = page_min
        if page_min <= start_ms or len(page) < 2:
            break
        cursor_end = page_min - 1
        time.sleep(0.10)  # small pause between public API pages
    cleaned = _clean_candles(all_candles, now_ms=now_ms)
    if len(cleaned) < 80:
        raise RuntimeError(f"Data Bybit {symbol} terlalu sedikit: {len(cleaned)} candle closed.")
    return [c for c in cleaned if c.ts >= start_ms]


def fetch_bybit_btc(days: int = 1) -> list[Candle]:
    return fetch_bybit_symbol(BYBIT_SYMBOL, days, BYBIT_CATEGORY)


def fetch_bybit_gold(days: int = 1) -> list[Candle]:
    """Try Bybit tokenized-gold spot candles; this is XAUTUSDT, not broker XAUUSD."""
    return fetch_bybit_symbol(BYBIT_XAU_SYMBOL, days, "spot")


def display_pair(pair: str) -> str:
    return PAIR_DISPLAY.get(normalize_pair(pair), str(pair))


def max_backtest_days(pair: str) -> int:
    return 7 if normalize_pair(pair) in YAHOO_LIMITED_PAIRS else 30


# Yahoo Finance can throttle shared cloud IPs. Cool down a symbol after 429 rather than
# hammering query1/query2 on every scan. Twelve Data is only used when its API key is set.
YAHOO_COOLDOWN_UNTIL: dict[str, float] = {}
TD_CACHE: dict[str, tuple[float, list[Candle], str]] = {}
TD_FAILURE_UNTIL: dict[str, float] = {}
TD_FAILURE_MESSAGE: dict[str, str] = {}
TD_API_KEY = os.getenv("TWELVEDATA_API_KEY", "").strip()
TD_SYMBOL_ENV = {
    "NAS100": os.getenv("TWELVEDATA_NAS100_SYMBOL", "NDX").strip(),
    "USOIL": os.getenv("TWELVEDATA_USOIL_SYMBOL", "WTI/USD").strip(),
    "GBPJPY": os.getenv("TWELVEDATA_GBPJPY_SYMBOL", "GBP/JPY").strip(),
}

def fetch_twelvedata_symbol(symbol: str, days: int = 1) -> list[Candle]:
    """Fetch 1m candles from Twelve Data. Futures symbol coverage depends on account/market listing."""
    if not TD_API_KEY or TD_API_KEY.lower().startswith("put_your_"):
        raise RuntimeError("TWELVEDATA_API_KEY belum diisi pada environment Railway")
    params = urllib.parse.urlencode({
        "symbol": symbol, "interval": "1min", "outputsize": min(5000, max(250, int(days) * 1440)),
        "timezone": "UTC", "apikey": TD_API_KEY,
    })
    payload = http_json(f"https://api.twelvedata.com/time_series?{params}", timeout=25)
    if payload.get("status") == "error" or payload.get("code") or payload.get("message"):
        msg = payload.get("message") or payload.get("status") or payload.get("code")
        raise RuntimeError(f"Twelve Data {symbol}: {msg}")
    rows = payload.get("values") or []
    if not rows:
        raise RuntimeError(f"Twelve Data {symbol}: tidak ada candle; simbol/plan mungkin tidak mendukung futures 1 menit")
    candles: list[Candle] = []
    for row in rows:
        try:
            # timezone=UTC requested; support both provider datetime forms.
            dt = datetime.strptime(str(row["datetime"]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            candles.append(Candle(int(dt.timestamp() * 1000), float(row["open"]), float(row["high"]),
                                  float(row["low"]), float(row["close"]), float(row.get("volume") or 0)))
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    cleaned = _clean_candles(candles)
    if len(cleaned) < 80:
        raise RuntimeError(f"Twelve Data {symbol}: hanya {len(cleaned)} candle closed; minimal 80")
    return cleaned

def fetch_index_candles(pair: str, days: int = 1) -> list[Candle]:
    """Yahoo first; on error try configured/alternate Twelve Data symbols (NAS100, USOIL, GBPJPY).

    There is no reliable unrestricted public 1m feed for these instruments. Symbol
    availability on Twelve Data must be verified against the user's account/plan.
    """
    yahoo_symbol = YAHOO_SYMBOLS[pair]
    yahoo_err = None
    now = time.time()
    if now < YAHOO_COOLDOWN_UNTIL.get(yahoo_symbol, 0.0):
        yahoo_err = RuntimeError(f"Yahoo {yahoo_symbol} dilewati sampai cooldown selesai setelah HTTP 429")
    else:
        try:
            data = fetch_yahoo_symbol(yahoo_symbol, min(days, 7))
            DATA_SOURCE_USED[pair] = f"Yahoo Finance {yahoo_symbol} ({PROXY_NOTE.get(pair, 'proxy')})"
            return data
        except Exception as exc:
            yahoo_err = exc
            if "429" in str(exc):
                YAHOO_COOLDOWN_UNTIL[yahoo_symbol] = now + 300.0
    if days == 1 and pair in TD_CACHE and now - TD_CACHE[pair][0] < 50:
        _, cached, source = TD_CACHE[pair]
        DATA_SOURCE_USED[pair] = source
        return cached
    if now < TD_FAILURE_UNTIL.get(pair, 0.0):
        raise RuntimeError(
            f"Yahoo {yahoo_symbol} gagal: {yahoo_err}. Twelve Data fallback sedang cooldown: "
            f"{TD_FAILURE_MESSAGE.get(pair, 'provider belum tersedia')}. Periksa key/simbol di Railway Variables."
        )

    configured = TD_SYMBOL_ENV.get(pair, "").strip()
    if not TD_API_KEY or TD_API_KEY.lower().startswith("put_your_"):
        message = "TWELVEDATA_API_KEY belum diisi pada Railway Variables; Yahoo kena rate limit dan tidak ada fallback tanpa kredensial yang dikonfigurasi"
        TD_FAILURE_MESSAGE[pair] = message
        TD_FAILURE_UNTIL[pair] = time.time() + 300.0
        raise RuntimeError(f"Yahoo {yahoo_symbol} gagal: {yahoo_err}. {message}. Tambahkan key, set simbol yang didukung, lalu redeploy.")
    # Default is the continuous-futures spelling used by some chart/data platforms.
    # Try common alternatives if the selected data account rejects that notation.
    alternates = {
        "NAS100": ["NDX", "NQ1!", "NQ=F", "NQ"],
        "USOIL": ["WTI/USD", "CL1!", "CL=F", "CL"],
        "GBPJPY": ["GBP/JPY", "GBPJPY"],
    }.get(pair, [])
    candidates = []
    for symbol in ([configured] if configured else []) + alternates:
        if symbol and symbol not in candidates:
            candidates.append(symbol)
    failures = []
    for symbol in candidates:
        try:
            data = fetch_twelvedata_symbol(symbol, days)
            source = f"Twelve Data {symbol} 1m ({PROXY_NOTE.get(pair, 'proxy')})"
            DATA_SOURCE_USED[pair] = source
            TD_FAILURE_UNTIL.pop(pair, None); TD_FAILURE_MESSAGE.pop(pair, None)
            if days == 1:
                TD_CACHE[pair] = (time.time(), data, source)
            return data
        except Exception as td_err:
            failures.append(f"{symbol}: {td_err}")
    failure_text = "; ".join(failures) if failures else "tidak ada simbol fallback"
    TD_FAILURE_MESSAGE[pair] = failure_text
    TD_FAILURE_UNTIL[pair] = time.time() + 300.0
    raise RuntimeError(
        f"Yahoo {yahoo_symbol} gagal: {yahoo_err}. Fallback Twelve Data gagal untuk [{failure_text}]. "
        "Periksa TWELVEDATA_API_KEY serta simbol kontrak dan akses 1 menit pada akun/plan; setelah mengubah Variables, redeploy service. "
        "Bot sengaja tidak membuat harga sintetis."
    ) from (RuntimeError(failure_text))


def fetch_candles(pair: str, days: int = 1) -> list[Candle]:
    pair = normalize_pair(pair)
    if pair == "XAUUSD":
        try:
            gold = fetch_bybit_gold(min(days, 30))
            if len(gold) >= 100:
                DATA_SOURCE_USED[pair] = "Bybit XAUTUSDT spot (tokenized gold; bukan CFD broker)"
                return gold
        except Exception as bybit_error:
            print(f"[Data XAU] Bybit XAUTUSDT gagal: {bybit_error}; fallback Yahoo XAUUSD=X: {bybit_error}", flush=True)
        data = fetch_yahoo_symbol("XAUUSD=X", min(days, 7))
        DATA_SOURCE_USED[pair] = "Yahoo Finance XAUUSD=X"
        return data
    if pair in BYBIT_CRYPTO_PAIRS:
        DATA_SOURCE_USED[pair] = f"Bybit {pair} linear"
        return fetch_bybit_symbol(pair, days, "linear")
    if pair in YAHOO_SYMBOLS:
        return fetch_index_candles(pair, days)
    raise ValueError("Pair tidak dikenali. Gunakan " + ", ".join(display_pair(x) for x in ALL_PAIRS) + " atau all.")


def normalize_pair(value: str) -> str:
    v = (value or "").strip().upper().replace("/", "").replace("-", "")
    aliases = {
        "XAU": "XAUUSD", "GOLD": "XAUUSD", "XAUUSD": "XAUUSD", "XAUUSD=X": "XAUUSD",
        "BTC": "BTCUSDT", "BTCUSDT": "BTCUSDT", "BTCUSD": "BTCUSDT", "BITCOIN": "BTCUSDT",
        "ETH": "ETHUSDT", "ETHUSDT": "ETHUSDT", "ETHEREUM": "ETHUSDT",
        "SOL": "SOLUSDT", "SOLUSDT": "SOLUSDT", "SOLANA": "SOLUSDT",
        "XRP": "XRPUSDT", "XRPUSDT": "XRPUSDT", "RIPPLE": "XRPUSDT",
        "NAS100": "NAS100", "NASDAQ": "NAS100", "NASDAQ100": "NAS100", "NDX": "NAS100", "NQ": "NAS100", "NQ=F": "NAS100",
        "USOIL": "USOIL", "OIL": "USOIL", "CRUDE": "USOIL", "CRUDEOIL": "USOIL", "WTI": "USOIL", "XTIUSD": "USOIL", "CL": "USOIL", "CL=F": "USOIL",
        "GBPJPY": "GBPJPY", "GJ": "GBPJPY", "GBPJPY=X": "GBPJPY",
        "ALL": "ALL", "SEMUA": "ALL", "BOTH": "ALL",
    }
    if v not in aliases:
        raise ValueError("Pair tidak dikenali. Pilih: XAU, BTC, ETH, SOL, XRP, NAS100, USOIL (Crude Oil), GBPJPY, atau all.")
    return aliases[v]


# BOT_PAIRS only controls the main scanner, /cek all, and /backtest all.
# The three independent engines use their own SFP_PAIRS / TPB_PAIRS / LV2_PAIRS.
_DEFAULT_ALL_PAIRS = ALL_PAIRS[:]
_raw_bot_pairs = (os.getenv("BOT_PAIRS") or "").strip()
if _raw_bot_pairs:
    _selected_pairs: list[str] = []
    for _item in _raw_bot_pairs.replace(";", ",").split(","):
        _item = _item.strip()
        if not _item:
            continue
        try:
            _pair_value = normalize_pair(_item)
        except ValueError as _pair_error:
            print(f"[Config] BOT_PAIRS item diabaikan: {_pair_error}", flush=True)
            continue
        if _pair_value == "ALL":
            _selected_pairs = _DEFAULT_ALL_PAIRS[:]
            break
        if _pair_value not in _selected_pairs:
            _selected_pairs.append(_pair_value)
    if _selected_pairs:
        ALL_PAIRS = _selected_pairs
    else:
        print("[Config] BOT_PAIRS tidak memuat pair valid; kembali ke pair default penuh.", flush=True)


def _rolling_mid(highs: list[float], lows: list[float], i: int, period: int) -> Optional[float]:
    if i + 1 < period:
        return None
    return (max(highs[i - period + 1:i + 1]) + min(lows[i - period + 1:i + 1])) / 2.0


def build_indicators(candles: list[Candle]) -> dict[str, list[Optional[float]]]:
    n = len(candles)
    highs = [c.high for c in candles]
    lows = [c.low for c in candles]
    closes = [c.close for c in candles]
    tenkan: list[Optional[float]] = [None] * n
    kijun: list[Optional[float]] = [None] * n
    span_a_raw: list[Optional[float]] = [None] * n
    span_b_raw: list[Optional[float]] = [None] * n
    cloud_a: list[Optional[float]] = [None] * n
    cloud_b: list[Optional[float]] = [None] * n
    tr: list[float] = [0.0] * n
    atr: list[Optional[float]] = [None] * n
    for i, c in enumerate(candles):
        tenkan[i] = _rolling_mid(highs, lows, i, TENKAN_PERIOD)
        kijun[i] = _rolling_mid(highs, lows, i, KIJUN_PERIOD)
        if tenkan[i] is not None and kijun[i] is not None:
            span_a_raw[i] = (tenkan[i] + kijun[i]) / 2.0
        span_b_raw[i] = _rolling_mid(highs, lows, i, SENKOU_B_PERIOD)
        prev_close = closes[i - 1] if i > 0 else c.close
        tr[i] = max(c.high - c.low, abs(c.high - prev_close), abs(c.low - prev_close))
        if i >= ATR_PERIOD - 1:
            atr[i] = sum(tr[i - ATR_PERIOD + 1:i + 1]) / ATR_PERIOD
        # Cloud currently visible on chart was calculated 26 bars earlier.
        # This alignment avoids forward-looking data leakage in historical tests.
        source_i = i - CLOUD_SHIFT
        if source_i >= 0:
            cloud_a[i] = span_a_raw[source_i]
            cloud_b[i] = span_b_raw[source_i]
    return {"tenkan": tenkan, "kijun": kijun, "cloud_a": cloud_a, "cloud_b": cloud_b, "atr": atr}


def detect_signal(candles: list[Candle], i: int, indicators: Optional[dict[str, list[Optional[float]]]] = None) -> Optional[Signal]:
    """Shadow Fakeout 1M + Ichimoku confirmation; closed candle only, no look-ahead."""
    if i < max(SWING_LOOKBACK + 2, SENKOU_B_PERIOD - 1, ATR_PERIOD + 2):
        return None
    if indicators is None:
        indicators = build_indicators(candles[:i + 1])
    c = candles[i]
    prev = candles[i - 1]
    atr = indicators["atr"][i]
    tenkan = indicators["tenkan"][i]
    kijun = indicators["kijun"][i]
    prev_tenkan = indicators["tenkan"][i - 1]
    prev_kijun = indicators["kijun"][i - 1]
    cloud_a = indicators["cloud_a"][i]
    cloud_b = indicators["cloud_b"][i]
    prev_cloud_a = indicators["cloud_a"][i - 1]
    prev_cloud_b = indicators["cloud_b"][i - 1]
    if atr is None or tenkan is None or kijun is None or prev_tenkan is None or prev_kijun is None or atr <= 0:
        return None
    prior = candles[i - SWING_LOOKBACK:i]
    prior_low = min(x.low for x in prior)
    prior_high = max(x.high for x in prior)
    candle_range = max(c.high - c.low, 1e-12)
    close_location = (c.close - c.low) / candle_range
    bullish_body = c.close > c.open and close_location >= 0.58
    bearish_body = c.close < c.open and close_location <= 0.42
    # SHADOW FAKEOUT: sweep external liquidity, reclaim the range, and reject the breakout.
    # A small tolerance avoids requiring a large wick; body must close back inside the range.
    sweep_low = c.low < prior_low and c.close > prior_low
    sweep_high = c.high > prior_high and c.close < prior_high
    lower_wick = min(c.open, c.close) - c.low
    upper_wick = c.high - max(c.open, c.close)
    bullish_rejection = lower_wick >= max(abs(c.close-c.open) * 0.35, candle_range * 0.12)
    bearish_rejection = upper_wick >= max(abs(c.close-c.open) * 0.35, candle_range * 0.12)
    # Sniper reversal: price breaks/reclaims Tenkan plus Kijun momentum improvement.
    cross_up_tenkan = prev.close <= prev_tenkan and c.close > tenkan
    cross_down_tenkan = prev.close >= prev_tenkan and c.close < tenkan
    cross_up_kijun = prev.close <= prev_kijun and c.close > kijun
    cross_down_kijun = prev.close >= prev_kijun and c.close < kijun
    tk_turn_up = tenkan >= kijun and tenkan >= prev_tenkan and kijun >= prev_kijun
    tk_turn_down = tenkan <= kijun and tenkan <= prev_tenkan and kijun <= prev_kijun

    direction = ""
    reason = ""
    score = 0
    cloud_bull_confirm = False
    cloud_bear_confirm = False
    if None not in (cloud_a, cloud_b, prev_cloud_a, prev_cloud_b):
        cloud_top = max(float(cloud_a), float(cloud_b))
        cloud_bottom = min(float(cloud_a), float(cloud_b))
        prev_cloud_top = max(float(prev_cloud_a), float(prev_cloud_b))
        prev_cloud_bottom = min(float(prev_cloud_a), float(prev_cloud_b))
        cloud_bull_confirm = c.close > cloud_top or (prev.close <= prev_cloud_top and c.close > cloud_top)
        cloud_bear_confirm = c.close < cloud_bottom or (prev.close >= prev_cloud_bottom and c.close < cloud_bottom)
    if sweep_low and bullish_body and bullish_rejection:
        score += 2  # failed downside breakout / sell-side liquidity sweep
        if c.close > tenkan and (cross_up_tenkan or cross_up_kijun or tk_turn_up):
            score += 2
            direction = "BUY"
            reason = "SHADOW FAKEOUT: sell-side sweep + reclaim/rejection + Ichimoku reversal"
        elif c.close > tenkan and c.close >= prev.close and tenkan >= prev_tenkan:
            # Adaptive fallback keeps the engine testable without requiring a full TK cross.
            score += 1
            direction = "BUY"
            reason = "SHADOW FAKEOUT: sell-side sweep + reclaim/rejection + Tenkan momentum"
        if direction == "BUY" and cloud_bull_confirm:
            score += 1
            reason += " + Kumo bullish confirmation"
    if sweep_high and bearish_body and bearish_rejection:
        score += 2
        if c.close < tenkan and (cross_down_tenkan or cross_down_kijun or tk_turn_down):
            score += 2
            direction = "SELL"
            reason = "SHADOW FAKEOUT: buy-side sweep + reclaim/rejection + Ichimoku reversal"
        elif c.close < tenkan and c.close <= prev.close and tenkan <= prev_tenkan:
            score += 1
            direction = "SELL"
            reason = "SHADOW FAKEOUT: buy-side sweep + reclaim/rejection + Tenkan momentum"
        if direction == "SELL" and cloud_bear_confirm:
            score += 1
            reason += " + Kumo bearish confirmation"
    if not direction:
        return None

    # Stop must sit beyond the sweep extreme; ATR adds a small noise buffer.
    buffer = max(atr * STOP_ATR_BUFFER, abs(c.close) * 0.00001)
    if direction == "BUY":
        stop = c.low - buffer
        # next-bar open is used during backtest; this close is only a reference estimate.
        entry_ref = c.close
        risk = entry_ref - stop
        target = entry_ref + RR_TARGET * risk
    else:
        stop = c.high + buffer
        entry_ref = c.close
        risk = stop - entry_ref
        target = entry_ref - RR_TARGET * risk
    if risk <= 0 or risk > MAX_RISK_ATR * atr:
        return None
    return Signal(
        symbol="", direction=direction, index=i, candle_ts=c.ts,
        entry_reference=entry_ref, stop=stop, target=target, risk=risk,
        atr=atr, reason=reason, score=score,
    )


# ------------------- Independent ICHIMOKU M15 engine (ported from bybit_combo_bot.py) -------------------
# It is not a filter for the original 1M Shadow Fakeout. It can emit its own alert.
ICH_ENABLED = os.getenv("ICH_M15_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
ICH_SCORE_MIN = max(1, min(9, int(os.getenv("ICH_M15_MIN_SCORE", "5"))))
ICH_CTX_BARS = max(4, int(os.getenv("ICH_M15_CTX_BARS", "8")))
ICH_CTX_MIN = max(1, min(ICH_CTX_BARS, int(os.getenv("ICH_M15_CTX_MIN", "4"))))
ICH_WICK_MIN = min(0.9, max(0.1, float(os.getenv("ICH_M15_WICK_MIN", "0.35"))))
ICH_WICK_ATR = max(0.0, float(os.getenv("ICH_M15_WICK_ATR", "0.40")))
ICH_ACCUM_BARS = max(5, int(os.getenv("ICH_M15_ACCUM_BARS", "10")))
ICH_ACCUM_ATR = max(1.0, float(os.getenv("ICH_M15_ACCUM_ATR", "4.0")))
ICH_STEP_ATR = max(0.05, float(os.getenv("ICH_M15_STEP_ATR", "0.25")))
ICH_RUNGS = max(1, min(6, int(os.getenv("ICH_M15_RUNGS", "3"))))
ICH_TP1_R = max(0.5, float(os.getenv("ICH_M15_TP1_R", "1.2")))
ICH_TP2_R = max(ICH_TP1_R, float(os.getenv("ICH_M15_TP2_R", "2.0")))
ICH_SL_MIN_PIPS = max(1.0, float(os.getenv("ICH_M15_SL_MIN_PIPS", "60")))
ICH_SL_MAX_PIPS = max(ICH_SL_MIN_PIPS, float(os.getenv("ICH_M15_SL_MAX_PIPS", "150")))
ICH_PIP_XAU = max(0.001, float(os.getenv("ICH_M15_PIP_XAU", "0.10")))
ICH_MODES = tuple(x for x in os.getenv("ICH_M15_MODES", "FAKEOUT,BREAK").upper().replace(" ", "").split(",") if x in {"FAKEOUT", "BREAK"}) or ("FAKEOUT", "BREAK")
ICH_MIN_M15_BARS = 82

def candles_to_m15(candles: list[Candle]) -> list[Candle]:
    """Aggregate CLOSED 1m candles to fully completed M15 OHLCV bars, UTC aligned."""
    grouped: dict[int, list[Candle]] = {}
    now_ms = int(time.time() * 1000)
    for c in candles:
        bucket = (c.ts // 900_000) * 900_000
        if bucket + 900_000 > now_ms:
            continue
        grouped.setdefault(bucket, []).append(c)
    out: list[Candle] = []
    for bucket in sorted(grouped):
        rows = sorted(grouped[bucket], key=lambda x: x.ts)
        # Avoid creating a 15m setup from an incomplete bucket/feed outage.
        if len(rows) < 12:
            continue
        out.append(Candle(bucket, rows[0].open, max(x.high for x in rows), min(x.low for x in rows), rows[-1].close, sum(x.volume for x in rows)))
    return out

def _ich_mid(candles: list[Candle], i: int, period: int) -> Optional[float]:
    if i + 1 < period:
        return None
    rows = candles[i - period + 1:i + 1]
    return (max(x.high for x in rows) + min(x.low for x in rows)) / 2.0

def _ich_m15_arrays(candles: list[Candle]) -> dict[str, list[Optional[float]]]:
    n = len(candles)
    keys = ("tenkan", "kijun", "span_a", "span_b", "cloud_top", "cloud_bot", "atr")
    A: dict[str, list[Optional[float]]] = {k: [None] * n for k in keys}
    for i, c in enumerate(candles):
        A["tenkan"][i] = _ich_mid(candles, i, 9)
        A["kijun"][i] = _ich_mid(candles, i, 26)
        a_raw = ((A["tenkan"][i] + A["kijun"][i]) / 2.0) if A["tenkan"][i] is not None and A["kijun"][i] is not None else None
        b_raw = _ich_mid(candles, i, 52)
        source_i = i - 26
        if source_i >= 0:
            src_t, src_k = A["tenkan"][source_i], A["kijun"][source_i]
            A["span_a"][i] = (src_t + src_k) / 2.0 if src_t is not None and src_k is not None else None
            A["span_b"][i] = _ich_mid(candles, source_i, 52)
        tr = c.high - c.low
        if i:
            prev_close = candles[i-1].close
            tr = max(tr, abs(c.high - prev_close), abs(c.low - prev_close))
        if i >= 13:
            A["atr"][i] = sum(max(x.high-x.low, abs(x.high-candles[j-1].close) if j else x.high-x.low, abs(x.low-candles[j-1].close) if j else x.high-x.low) for j, x in enumerate(candles[i-13:i+1], start=i-13)) / 14.0
        sa, sb = A["span_a"][i], A["span_b"][i]
        if sa is not None and sb is not None:
            A["cloud_top"][i], A["cloud_bot"][i] = max(sa, sb), min(sa, sb)
    A["pos"] = [0] * n
    for i, c in enumerate(candles):
        top, bot = A["cloud_top"][i], A["cloud_bot"][i]
        if top is not None and bot is not None:
            A["pos"][i] = 1 if c.close > top else (-1 if c.close < bot else 0)
    return A

def _ich_m15_core(candles: list[Candle], A: dict[str, list[Any]], t: int, sell: bool, mode: str):
    c = candles[t]; top=A["cloud_top"][t]; bot=A["cloud_bot"][t]; atr=A["atr"][t]
    if top is None or bot is None or atr is None or atr <= 0 or t < ICH_CTX_BARS + 1:
        return None
    pos=A["pos"]; rng=c.high-c.low
    if rng <= 0: return None
    if mode == "FAKEOUT":
        side_pos = -1 if sell else 1
        if sum(1 for q in pos[t-ICH_CTX_BARS:t] if q == side_pos) < ICH_CTX_MIN: return None
        if sell:
            if pos[t-1] > 0 or not (c.high > bot and c.close < bot): return None
            wick=c.high-max(c.open,c.close); edge,far,ext=bot,top,c.high
        else:
            if pos[t-1] < 0 or not (c.low < top and c.close > top): return None
            wick=min(c.open,c.close)-c.low; edge,far,ext=top,bot,c.low
        if wick < ICH_WICK_MIN*rng or wick < ICH_WICK_ATR*atr: return None
        return float(edge),float(far),float(ext)
    if sell:
        if not (pos[t] == -1 and pos[t-1] >= 0 and c.close < c.open and (bot-c.close) >= 0.15*atr and abs(c.close-c.open) >= 0.5*rng): return None
        return float(bot),float(top),float(top)
    if not (pos[t] == 1 and pos[t-1] <= 0 and c.close > c.open and (c.close-top) >= 0.15*atr and abs(c.close-c.open) >= 0.5*rng): return None
    return float(top),float(bot),float(bot)

def _ich_m15_confluence(candles: list[Candle], A: dict[str, list[Any]], t: int, sell: bool, mode: str, far: float) -> list[str]:
    c=candles[t]; atr=float(A["atr"][t] or 0.0); rng=max(c.high-c.low,1e-12); out=[]
    n=ICH_ACCUM_BARS; pre=candles[t-n:t]; ph=max(x.high for x in pre); pl=min(x.low for x in pre)
    if mode == "FAKEOUT":
        if (c.high > ph) if sell else (c.low < pl): out.append("sweep liquidity di "+("atas" if sell else "bawah")+" range akumulasi")
        if ph-pl <= ICH_ACCUM_ATR*atr: out.append(f"akumulasi rapat ({n} bar)")
        if (c.high > far) if sell else (c.low < far): out.append("manipulasi menembus seluruh awan")
        if ((c.close-c.low)/rng <= 0.35) if sell else ((c.close-c.low)/rng >= 0.65): out.append("close dekat ujung candle")
    else:
        came=A["pos"][max(0,t-6):t]
        if any(x == (1 if sell else -1) for x in came): out.append("tembus awan dari sisi seberang")
        if abs(c.close-c.open) >= 0.7*rng: out.append("body dominan >=70%")
        if abs(c.close-c.open) >= atr: out.append("displacement >=1 ATR")
        if ((c.close-c.low)/rng <= 0.25) if sell else ((c.close-c.low)/rng >= 0.75): out.append("close di ujung candle")
    tk=A["tenkan"][t]; kj=A["kijun"][t]
    if tk is not None and kj is not None and ((tk < kj) if sell else (tk > kj)): out.append("Tenkan/Kijun searah")
    if t >= 3 and A["kijun"][t-3] is not None and kj is not None and ((kj < A["kijun"][t-3]) if sell else (kj > A["kijun"][t-3])): out.append("Kijun miring searah")
    if t >= 26 and ((c.close < candles[t-26].close) if sell else (c.close > candles[t-26].close)): out.append("Chikou searah")
    sa,sb=A["span_a"][t],A["span_b"][t]
    if sa is not None and sb is not None and ((sa < sb) if sell else (sa > sb)): out.append("awan searah")
    vm=sum(x.volume for x in candles[t-20:t])/20 if t >= 20 else 0.0
    if vm > 0 and c.volume >= 1.2*vm: out.append("volume lonjakan")
    return out

def detect_ichimoku_m15_signal(candles_1m: list[Candle]) -> Optional[Signal]:
    """Independent M15 Ichimoku + FAKEOUT/AMD or BREAK cloud engine, XAUUSD only."""
    if not ICH_ENABLED: return None
    m15=candles_to_m15(candles_1m)
    if len(m15) < ICH_MIN_M15_BARS: return None
    A=_ich_m15_arrays(m15); t=len(m15)-1; found=None
    for mode in ICH_MODES:
        for sell in (True,False):
            core=_ich_m15_core(m15,A,t,sell,mode)
            if core is None: continue
            edge,far,ext=core
            conf=_ich_m15_confluence(m15,A,t,sell,mode,far)
            if len(conf) < ICH_SCORE_MIN: continue
            found=(mode,sell,edge,far,ext,conf); break
        if found: break
    if not found: return None
    mode,sell,edge,far,ext,conf=found; atr=float(A["atr"][t] or 0.0)
    if atr <= 0: return None
    side="SELL" if sell else "BUY"; step=max(ICH_STEP_ATR*atr,0.01)
    if sell:
        lim=min(far+0.5*step,ext) if mode=="FAKEOUT" else far+0.5*step
    else:
        lim=max(far-0.5*step,ext) if mode=="FAKEOUT" else far-0.5*step
    levels=[]
    for k in range(ICH_RUNGS):
        px=edge+k*step if sell else edge-k*step
        if k and ((px>lim) if sell else (px<lim)): break
        levels.append(round(px,2))
    levels=list(dict.fromkeys(levels))
    if not levels: return None
    entry=levels[0]; last=levels[-1]; buf=0.15*atr
    base=max(far,ext,last) if sell else min(far,ext,last)
    sl_raw=base+buf if sell else base-buf
    # Gold-specific SL bounds from the source bot: 60-150 pip where pip=$0.10.
    min_dist=ICH_SL_MIN_PIPS*ICH_PIP_XAU; max_dist=ICH_SL_MAX_PIPS*ICH_PIP_XAU
    dist=abs(sl_raw-entry)
    if dist > max_dist: return None
    if dist < min_dist: sl_raw=entry+min_dist if sell else entry-min_dist
    sl_raw=max(sl_raw,last+buf) if sell else min(sl_raw,last-buf)
    sl=round(sl_raw,2); risk=abs(sl-entry)
    if risk <= 0: return None
    sign=-1.0 if sell else 1.0
    tp1=round(entry+sign*ICH_TP1_R*risk,2); tp2=round(entry+sign*ICH_TP2_R*risk,2)
    ts=m15[t].ts
    return Signal(symbol="XAUUSD", direction=side, index=len(candles_1m)-1, candle_ts=ts,
                  entry_reference=entry, stop=sl, target=tp1, risk=risk, atr=atr,
                  reason=f"ICHIMOKU M15 {mode} | "+"; ".join(conf), score=len(conf),
                  engine="ICHIMOKU M15 (INDEPENDENT)", target2=tp2, entry_levels=tuple(levels), order_type="LIMIT")


def latest_closed_signal(pair: str, candles: list[Candle]) -> tuple[Optional[Signal], dict[str, Any]]:
    inds = build_indicators(candles)
    # Strict live mode checks the newest fully closed candle only; avoids stale entries.
    idx = len(candles) - 1
    sig = detect_signal(candles, idx, inds) if idx >= 0 else None
    if sig:
        sig.symbol = normalize_pair(pair)
    c = candles[-1]
    return sig, {"price": c.close, "time": c.time_utc, "candle_ts": c.ts, "count": len(candles)}


def backtest(candles: list[Candle], pair: str, rr: float = RR_TARGET, max_hold: int = MAX_HOLD_BARS) -> tuple[list[Trade], dict[str, Any]]:
    """One position at a time; next-bar OPEN entry; BE/SL+ trailing; SL-first on ambiguous bars."""
    if len(candles) < 100:
        raise RuntimeError(f"Data tidak cukup untuk backtest: {len(candles)} candle (minimum 100).")
    indicators = build_indicators(candles)
    trades: list[Trade] = []
    i = max(SENKOU_B_PERIOD - 1, SWING_LOOKBACK + 2, ATR_PERIOD + 2)
    while i < len(candles) - 1:
        sig = detect_signal(candles, i, indicators)
        if sig is None:
            i += 1
            continue
        entry_i = i + 1
        entry_candle = candles[entry_i]
        entry = entry_candle.open
        direction = sig.direction
        stop = sig.stop
        if direction == "BUY":
            if entry <= stop:
                i += 1
                continue
            risk = entry - stop
            target = entry + rr * risk
        else:
            if entry >= stop:
                i += 1
                continue
            risk = stop - entry
            target = entry - rr * risk
        if risk <= 0 or risk > MAX_RISK_ATR * sig.atr:
            i += 1
            continue
        max_favorable = entry
        outcome = "TIMEOUT"
        exit_i = min(len(candles) - 1, entry_i + max_hold - 1)
        exit_price = candles[exit_i].close
        current_stop = stop
        for j in range(entry_i, exit_i + 1):
            bar = candles[j]
            if direction == "BUY":
                stop_hit = bar.low <= current_stop
                target_hit = bar.high >= target
            else:
                stop_hit = bar.high >= current_stop
                target_hit = bar.low <= target
            if stop_hit:  # conservative: stop is assumed first if both levels are reached
                exit_price, exit_i = current_stop, j
                r_at_stop = ((exit_price - entry) / risk) if direction == "BUY" else ((entry - exit_price) / risk)
                outcome = "SL_PLUS" if r_at_stop > 1e-8 else ("BE" if r_at_stop >= -1e-8 else "SL")
                break
            if target_hit:
                exit_price, outcome, exit_i = target, "TP", j
                break
            # Only use this bar's high/low to update the stop for future candles (no same-bar lookahead).
            max_favorable = max(max_favorable, bar.high) if direction == "BUY" else min(max_favorable, bar.low, entry)
            favorable_r = ((max_favorable - entry) / risk) if direction == "BUY" else ((entry - max_favorable) / risk)
            if favorable_r >= BE_TRIGGER_R:
                be_stop = entry
                if direction == "BUY":
                    current_stop = max(current_stop, be_stop)
                else:
                    current_stop = min(current_stop, be_stop)
            if favorable_r >= LOCK_TRIGGER_R:
                atr_now = indicators["atr"][j] or sig.atr
                trail_dist = max(float(atr_now) * TRAIL_ATR_MULT, risk * TRAIL_R_MULT)
                if direction == "BUY":
                    candidate = max(entry + LOCK_MIN_R * risk, max_favorable - trail_dist)
                    current_stop = max(current_stop, candidate)
                else:
                    candidate = min(entry - LOCK_MIN_R * risk, max_favorable + trail_dist)
                    current_stop = min(current_stop, candidate)
        result_r = (exit_price - entry) / risk if direction == "BUY" else (entry - exit_price) / risk
        signal_candle = candles[i]
        trades.append(Trade(
            symbol=normalize_pair(pair), direction=direction,
            signal_time=signal_candle.time_utc, entry_time=entry_candle.time_utc,
            exit_time=candles[exit_i].time_utc, entry=entry, stop=current_stop,
            target=target, exit_price=exit_price, result_r=result_r,
            outcome=outcome, bars_held=exit_i - entry_i + 1, reason=sig.reason,
        ))
        i = exit_i + 1  # no overlapping positions for this pair

    wins = sum(1 for t in trades if t.result_r > 0)
    losses = sum(1 for t in trades if t.result_r < 0)
    flats = len(trades) - wins - losses
    net_r = sum(t.result_r for t in trades)
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in trades:
        equity += t.result_r
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    summary = {
        "symbol": normalize_pair(pair), "candles": len(candles),
        "start": candles[0].time_utc, "end": candles[-1].time_utc,
        "trades": len(trades), "wins": wins, "losses": losses, "flats": flats,
        "winrate_pct": (wins / len(trades) * 100.0) if trades else 0.0,
        "net_r": net_r, "avg_r": (net_r / len(trades)) if trades else 0.0,
        "max_drawdown_r": max_dd,
        "tp": sum(1 for t in trades if t.outcome == "TP"),
        "sl": sum(1 for t in trades if t.outcome == "SL"),
        "sl_plus": sum(1 for t in trades if t.outcome == "SL_PLUS"),
        "be": sum(1 for t in trades if t.outcome == "BE"),
        "timeout": sum(1 for t in trades if t.outcome == "TIMEOUT"),
        "rr": rr, "max_hold": max_hold,
    }
    # Translate the R-results into the same simulated IDR paper-money model used by live paper trades.
    paper_balance = float(PAPER_INITIAL_IDR)
    paper_deposits = float(PAPER_INITIAL_IDR)
    paper_peak = paper_balance
    paper_max_dd = 0.0
    paper_topups = 0
    for trade in trades:
        risk_cash = max(paper_balance, 0.0) * PAPER_RISK_PCT
        paper_balance += risk_cash * float(trade.result_r)
        if paper_balance <= paper_deposits * PAPER_MC_RATIO:
            paper_balance += float(PAPER_TOPUP_IDR)
            paper_deposits += float(PAPER_TOPUP_IDR)
            paper_topups += 1
        paper_peak = max(paper_peak, paper_balance)
        paper_max_dd = max(paper_max_dd, paper_peak - paper_balance)
    summary.update({
        "paper_balance_end_idr": paper_balance,
        "paper_deposits_idr": paper_deposits,
        "paper_net_idr": paper_balance - paper_deposits,
        "paper_topups": paper_topups,
        "paper_max_drawdown_idr": paper_max_dd,
    })
    return trades, summary

def format_signal(pair: str, candles: list[Candle]) -> str:
    sig, info = latest_closed_signal(pair, candles)
    title = f"{display_pair(pair)} | {DATA_SOURCE_USED.get(normalize_pair(pair), 'public feed')}"
    lines = [f"LIQUIDITY SNIPER 1M — {title}", f"Harga candle close: {info['price']:.5f}", f"Data: {info['time']}"]
    if not sig:
        lines += ["Setup: BELUM ADA pada candle terakhir yang sudah close.",
                  "Pola menunggu liquidity sweep + reclaim + konfirmasi Ichimoku."]
        return "\n".join(lines)
    sig.symbol = normalize_pair(pair)
    lines += [f"SINYAL: {sig.direction}", f"Entry acuan (close): {sig.entry_reference:.5f}",
              f"SL: {sig.stop:.5f}", f"TP ({RR_TARGET:.1f}R): {sig.target:.5f}",
              f"Risk harga: {sig.risk:.5f} | ATR(14): {sig.atr:.5f}",
              f"Konfirmasi: {sig.reason}", f"Score: {sig.score}/5"]
    lines += ["Catatan: entry backtest memakai OPEN candle berikutnya; sinyal bukan instruksi transaksi."]
    return "\n".join(lines)


def format_backtest(summary: dict[str, Any]) -> str:
    return "\n".join([
        f"BACKTEST LIQUIDITY SNIPER 1M — {display_pair(summary['symbol'])}",
        f"Data: {summary['start']} → {summary['end']}",
        f"Candle valid: {summary['candles']}",
        f"Trade closed: {summary['trades']}",
        f"Win rate (trade R positif): {summary['winrate_pct']:.2f}% ({summary['wins']} W / {summary['losses']} L / {summary['flats']} flat)",
        f"TP: {summary['tp']} | SL: {summary['sl']} | SL+: {summary.get('sl_plus', 0)} | BE: {summary.get('be', 0)} | Timeout: {summary['timeout']}",
        f"Net hasil: {summary['net_r']:+.2f}R | R rata-rata: {summary['avg_r']:+.3f}R",
        f"Max drawdown: {summary['max_drawdown_r']:.2f}R",
        f"Paper balance akhir: {rupiah(summary.get('paper_balance_end_idr', PAPER_INITIAL_IDR))} | Total deposit: {rupiah(summary.get('paper_deposits_idr', PAPER_INITIAL_IDR))} | P/L bersih: {rupiah(summary.get('paper_net_idr', 0))}",
        f"Auto top-up MC simulasi: {summary.get('paper_topups', 0)} x {rupiah(PAPER_TOPUP_IDR)} | Max DD paper: {rupiah(summary.get('paper_max_drawdown_idr', 0))}",
        f"Model: next-bar OPEN entry | risk paper {PAPER_RISK_PCT*100:.1f}%/trade | TP {summary['rr']:.1f}R | BE +{BE_TRIGGER_R:.2f}R | SL+ trailing setelah +{LOCK_TRIGGER_R:.1f}R | max hold {summary['max_hold']} candle | SL-first jika level sama-sama tersentuh",
        "Belum memasukkan spread, komisi, slippage, funding, latency, atau perbedaan feed broker/futures proxy.",
        "Hasil historis simulasi; bukan jaminan hasil live.",
    ])


def save_trades_csv(trades: list[Trade], pair: str, outdir: str = ".") -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(outdir, f"backtest_{normalize_pair(pair)}_{stamp}.csv")
    fields = list(Trade.__dataclass_fields__.keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for trade in trades:
            writer.writerow(trade.__dict__)
    return path


def run_backtest_pair(pair: str, days: int, save: bool = True) -> tuple[str, Optional[str]]:
    pair = normalize_pair(pair)
    max_days = max_backtest_days(pair)
    days = max(1, min(days, max_days))
    candles = fetch_candles(pair, days)
    trades, summary = backtest(candles, pair)
    report = format_backtest(summary)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = save_trades_csv(trades, pair, OUTPUT_DIR) if save else None
    if csv_path:
        report += f"\nDetail trade CSV: {csv_path}"
    return report, csv_path


# --------------------------- Chart rendering (stdlib-only PNG) ---------------------------
# Tiny 3x5 bitmap font so charts remain dependency-free in Termux.
FONT = {
"A":["010","101","111","101","101"],"B":["110","101","110","101","110"],"C":["011","100","100","100","011"],
"D":["110","101","101","101","110"],"E":["111","100","110","100","111"],"F":["111","100","110","100","100"],
"G":["011","100","101","101","011"],"H":["101","101","111","101","101"],"I":["111","010","010","010","111"],
"J":["001","001","001","101","010"],"K":["101","101","110","101","101"],"L":["100","100","100","100","111"],
"M":["101","111","111","101","101"],"N":["101","111","111","111","101"],"O":["010","101","101","101","010"],
"P":["110","101","110","100","100"],"Q":["010","101","101","111","011"],"R":["110","101","110","101","101"],
"S":["011","100","010","001","110"],"T":["111","010","010","010","010"],"U":["101","101","101","101","111"],
"V":["101","101","101","101","010"],"W":["101","101","111","111","101"],"X":["101","101","010","101","101"],
"Y":["101","101","010","010","010"],"Z":["111","001","010","100","111"],
"0":["111","101","101","101","111"],"1":["010","110","010","010","111"],"2":["110","001","010","100","111"],
"3":["110","001","010","001","110"],"4":["101","101","111","001","001"],"5":["111","100","110","001","110"],
"6":["011","100","110","101","010"],"7":["111","001","010","010","010"],"8":["010","101","010","101","010"],
"9":["010","101","011","001","110"],"-":["000","000","111","000","000"],
".":["000","000","000","000","010"],":":["000","010","000","010","000"],"/":["001","001","010","100","100"],
"+":["000","010","111","010","000"]," ":["000","000","000","000","000"],"_":["000","000","000","000","111"],
"=":["000","111","000","111","000"],"%":["101","001","010","100","101"],"(":["001","010","010","010","001"],")":["100","010","010","010","100"],
}


def _png_chunk(tag: bytes, data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", binascii.crc32(tag + data) & 0xffffffff)


def _write_png(path: str, width: int, height: int, pixels: bytearray) -> None:
    raw = b"".join(b"\x00" + pixels[y * width * 3:(y + 1) * width * 3] for y in range(height))
    png = b"\x89PNG\r\n\x1a\n"
    png += _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
    png += _png_chunk(b"IDAT", zlib.compress(raw, 6)) + _png_chunk(b"IEND", b"")
    with open(path, "wb") as f:
        f.write(png)


def _draw_chart_bitmap(candles: list[Candle], pair: str, sig: Optional[Signal], path: Optional[str] = None,
                       position: Optional[dict[str, Any]] = None) -> str:
    """Dependency-free white-background fallback chart."""
    os.makedirs(CHART_DIR, exist_ok=True)
    if path is None:
        path = os.path.join(CHART_DIR, f"{normalize_pair(pair)}_{int(time.time())}_{(sig.direction if sig else 'CHECK')}.png")
    width, height = 1000, 620
    bg = (255, 255, 255)
    pixels = bytearray(bg * (width * height))

    def pixel(x: int, y: int, color: tuple[int, int, int]) -> None:
        if 0 <= x < width and 0 <= y < height:
            k = (y * width + x) * 3
            pixels[k:k + 3] = bytes(color)

    def line(x1: int, y1: int, x2: int, y2: int, color: tuple[int, int, int], thick: int = 1, dashed: bool = False) -> None:
        dx, dy = abs(x2-x1), abs(y2-y1)
        sx, sy = (1 if x1 < x2 else -1), (1 if y1 < y2 else -1)
        err, step = dx-dy, 0
        while True:
            if not dashed or (step // 6) % 2 == 0:
                for ox in range(-(thick//2), thick//2 + 1):
                    for oy in range(-(thick//2), thick//2 + 1):
                        pixel(x1 + ox, y1 + oy, color)
            if x1 == x2 and y1 == y2:
                break
            e2 = 2 * err
            if e2 > -dy:
                err -= dy; x1 += sx
            if e2 < dx:
                err += dx; y1 += sy
            step += 1

    def rect(x1: int, y1: int, x2: int, y2: int, color: tuple[int, int, int]) -> None:
        xa, xb = sorted((int(x1), int(x2)))
        ya, yb = sorted((int(y1), int(y2)))
        xa, xb = max(0, xa), min(width - 1, xb)
        ya, yb = max(0, ya), min(height - 1, yb)
        if xa > xb or ya > yb:
            return
        row = bytes(color) * (xb-xa+1)
        for yy in range(ya, yb+1):
            k = (yy * width + xa) * 3
            pixels[k:k + len(row)] = row

    def text(x: int, y: int, value: str, color: tuple[int, int, int], scale: int = 2) -> None:
        cursor = x
        for ch in str(value).upper():
            glyph = FONT.get(ch, FONT[" "])
            for gy, row in enumerate(glyph):
                for gx, bit in enumerate(row):
                    if bit == "1":
                        rect(cursor + gx*scale, y + gy*scale, cursor + gx*scale + scale - 1, y + gy*scale + scale - 1, color)
            cursor += 4 * scale
            if cursor > width - 12:
                break

    sample = candles[-100:]
    if not sample:
        raise RuntimeError("Tidak ada candle untuk chart.")
    inds = build_indicators(candles)
    start_idx = len(candles) - len(sample)
    pair_label = display_pair(pair)
    last = sample[-1]
    accent = (19, 111, 138)
    navy = (25, 43, 65)
    muted = (100, 116, 139)
    grid = (229, 234, 240)
    bull = (0, 139, 120)
    bear = (207, 70, 87)
    tenkan_col = (43, 112, 222)
    kijun_col = (221, 137, 40)
    span_a_col = (119, 184, 151)
    span_b_col = (207, 139, 151)
    extra_levels: list[float] = []
    if sig:
        extra_levels.extend([sig.entry_reference, sig.stop, sig.target])
    if position:
        extra_levels.extend([float(position.get("entry", 0)), float(position.get("stop", 0)), float(position.get("target", 0))])
    positives = [x for x in extra_levels if x > 0 and math.isfinite(x)]
    lo = min([c.low for c in sample] + positives)
    hi = max([c.high for c in sample] + positives)
    pad = max((hi-lo)*0.07, abs(hi)*0.00001, 1e-8)
    lo, hi = lo-pad, hi+pad
    left, right, top, bottom = 76, 925, 95, 505
    def yprice(value: float) -> int:
        return int(bottom - (value-lo)/(hi-lo)*(bottom-top))

    # white title band and subtle teal accent
    rect(0, 0, width-1, height-1, (255,255,255))
    rect(0, 0, 8, 73, accent)
    engine_title = sig.engine if sig else "SHADOW FAKEOUT + ICHIMOKU 1M"
    text(28, 17, f"{pair_label}  |  {engine_title}", navy, 3)
    direction_label = (sig.direction if sig else "WAIT")
    status_color = bull if direction_label == "BUY" else bear if direction_label == "SELL" else muted
    text(28, 49, f"CLOSE {last.close:.5f}  |  {last.time_utc}  |  SIGNAL {direction_label}", muted, 2)
    line(28, 76, width-28, 76, (215, 223, 232), 1)

    # grid and price axis
    for fraction in (0, .2, .4, .6, .8, 1):
        yy = int(bottom - fraction*(bottom-top))
        line(left, yy, right, yy, grid, 1)
        val = lo + fraction*(hi-lo)
        label = f"{val:.5f}" if abs(val) < 10 else (f"{val:.2f}" if abs(val) < 1000 else f"{val:.1f}")
        text(right+9, yy-5, label, muted, 1)
    for j in range(0, len(sample), 10):
        xx = int(left + (j+.5)*(right-left)/max(len(sample), 1))
        line(xx, top, xx, bottom, (242, 245, 248), 1)
    line(left, top, left, bottom, (198, 208, 218), 1)
    line(left, bottom, right, bottom, (198, 208, 218), 1)
    step = (right-left) / max(len(sample), 1)
    x_of = lambda j: int(left + (j+.5)*step)

    # Kumo edges, then Tenkan/Kijun. All indicator values are aligned to the closed candle.
    for key, color in (("cloud_a", span_a_col), ("cloud_b", span_b_col), ("tenkan", tenkan_col), ("kijun", kijun_col)):
        prev = None
        for j in range(len(sample)):
            gi = start_idx+j
            value = inds[key][gi]
            if value is None:
                prev = None
                continue
            point = (x_of(j), yprice(float(value)))
            if prev:
                line(prev[0], prev[1], point[0], point[1], color, 2 if key in ("tenkan", "kijun") else 1)
            prev = point

    # candle bodies and wicks
    for j, c in enumerate(sample):
        xx = x_of(j)
        yo, yh, yl, yc = map(yprice, (c.open, c.high, c.low, c.close))
        col = bull if c.close >= c.open else bear
        line(xx, yh, xx, yl, col, 1)
        rect(xx-2, min(yo,yc), xx+2, max(yo,yc) if abs(yc-yo)>1 else min(yo,yc)+1, col)

    levels = []
    if sig:
        levels = [(sig.entry_reference, accent, "ENTRY"), (sig.stop, bear, "SL"), (sig.target, bull, "TP")]
    elif position:
        levels = [(float(position["entry"]), accent, "ENTRY"), (float(position["stop"]), bear, "SL"), (float(position["target"]), bull, "TP")]
    for price, color, label in levels:
        yy = yprice(price)
        line(left, yy, right, yy, color, 2, True)
        rect(left+4, yy-7, left+42, yy+7, color)
        text(left+8, yy-4, label, (255,255,255), 1)

    # Footer line and compact legend
    line(28, 535, width-28, 535, (215, 223, 232), 1)
    text(30, 551, "TENKAN", tenkan_col, 2)
    line(95, 558, 125, 558, tenkan_col, 2)
    text(145, 551, "KIJUN", kijun_col, 2)
    line(195, 558, 225, 558, kijun_col, 2)
    text(245, 551, "KUMO A/B", muted, 2)
    line(330, 558, 360, 558, span_a_col, 2); line(366, 558, 396, 558, span_b_col, 2)
    text(420, 551, f"{direction_label}  |  {sig.score}/5" if sig else "NO VALID SETUP", status_color, 2)
    text(30, 585, "1M CLOSED CANDLES  |  PUBLIC DATA FEED  |  PAPER ONLY - NO REAL ORDERS", muted, 1)
    _write_png(path, width, height, pixels)
    return path


def draw_chart(candles: list[Candle], pair: str, sig: Optional[Signal], path: Optional[str] = None,
               position: Optional[dict[str, Any]] = None) -> str:
    """Professional white chart, using Pillow when available and stdlib renderer otherwise."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        return _draw_chart_bitmap(candles, pair, sig, path, position)

    os.makedirs(CHART_DIR, exist_ok=True)
    if path is None:
        path = os.path.join(CHART_DIR, f"{normalize_pair(pair)}_{int(time.time())}_{(sig.direction if sig else 'CHECK')}.png")
    if not candles:
        raise RuntimeError("Tidak ada candle untuk chart.")

    W, H = 1280, 760
    img = Image.new("RGB", (W, H), "#FFFFFF")
    d = ImageDraw.Draw(img)

    def get_font(size: int, bold: bool = False):
        candidates = []
        if bold:
            candidates += [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
                "/usr/share/fonts/dejavu-sans-fonts/DejaVuSans-Bold.ttf",
            ]
        else:
            candidates += [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
                "/usr/share/fonts/dejavu-sans-fonts/DejaVuSans.ttf",
            ]
        for candidate in candidates:
            try:
                return ImageFont.truetype(candidate, size=size)
            except (OSError, ValueError):
                pass
        try:
            return ImageFont.load_default(size=size)
        except TypeError:
            return ImageFont.load_default()

    f_title = get_font(30, True)
    f_sub = get_font(16)
    f_axis = get_font(13)
    f_legend = get_font(15, True)
    f_footer = get_font(12)

    navy = "#172B4D"
    muted = "#65758B"
    teal = "#0D8B7A"
    red = "#CE4657"
    blue = "#2E6FDE"
    orange = "#D58A2A"
    span_a_color = "#8CBDAC"
    span_b_color = "#D2A0A7"
    grid = "#E8EDF2"
    border = "#CBD5E1"

    sample = candles[-100:]
    start_idx = len(candles) - len(sample)
    indicators = build_indicators(candles)
    last = sample[-1]
    pair_name = display_pair(pair)
    direction = sig.direction if sig else "WAIT"
    status_color = teal if direction == "BUY" else red if direction == "SELL" else muted

    # Header
    d.rectangle((0, 0, W, 8), fill=teal)
    engine_title = sig.engine if sig else "SHADOW FAKEOUT + ICHIMOKU"
    d.text((34, 24), f"{pair_name}  |  {engine_title}", fill=navy, font=f_title)
    d.text((36, 70), f"TIMEFRAME  1M     LAST CLOSE  {last.close:.5f}     {last.time_utc}", fill=muted, font=f_sub)
    # Signal pill
    pill_text = f"{direction}" if not sig else f"{sig.direction}  |  SCORE {sig.score}/5"
    box = d.textbbox((0, 0), pill_text, font=f_legend)
    pill_w = box[2] - box[0] + 34
    d.rounded_rectangle((W-pill_w-32, 28, W-32, 66), radius=10, fill=status_color)
    d.text((W-pill_w-15, 38), pill_text, fill="#FFFFFF", font=f_legend)
    d.line((32, 106, W-32, 106), fill=border, width=1)

    # Plot area
    left, right, top, bottom = 82, 1150, 140, 620
    level_prices: list[float] = []
    if sig:
        level_prices += [sig.entry_reference, sig.stop, sig.target]
    if position:
        level_prices += [float(position.get("entry", 0)), float(position.get("stop", 0)), float(position.get("target", 0))]
    level_prices = [v for v in level_prices if v > 0 and math.isfinite(v)]
    lo = min([c.low for c in sample] + level_prices)
    hi = max([c.high for c in sample] + level_prices)
    pad = max((hi-lo)*0.08, abs(hi)*0.00002, 1e-8)
    lo, hi = lo-pad, hi+pad

    def xy(index: int, value: float) -> tuple[int, int]:
        xx = int(left + (index + 0.5) * (right-left) / max(1, len(sample)))
        yy = int(bottom - ((value-lo)/(hi-lo))*(bottom-top))
        return xx, yy

    def yprice(value: float) -> int:
        return int(bottom - ((value-lo)/(hi-lo))*(bottom-top))

    # Soft panel and grid
    d.rounded_rectangle((left-14, top-10, right+8, bottom+13), radius=9, fill="#FFFFFF", outline=border, width=1)
    for fraction in (0, .2, .4, .6, .8, 1):
        yy = int(bottom - fraction*(bottom-top))
        d.line((left, yy, right, yy), fill=grid, width=1)
        val = lo + fraction*(hi-lo)
        fmt = f"{val:.5f}" if abs(val) < 10 else (f"{val:.2f}" if abs(val) < 1000 else f"{val:.1f}")
        d.text((right+15, yy-8), fmt, fill=muted, font=f_axis)
    for j in range(0, len(sample), 10):
        xx = xy(j, lo)[0]
        d.line((xx, top, xx, bottom), fill="#F3F6F9", width=1)

    # Kumo background quads, before other technical lines/candles
    cloud_a_pts, cloud_b_pts = [], []
    for j in range(len(sample)):
        gi = start_idx+j
        a, b = indicators["cloud_a"][gi], indicators["cloud_b"][gi]
        cloud_a_pts.append(xy(j, float(a)) if a is not None else None)
        cloud_b_pts.append(xy(j, float(b)) if b is not None else None)
    for j in range(1, len(sample)):
        a0, a1 = cloud_a_pts[j-1], cloud_a_pts[j]
        b0, b1 = cloud_b_pts[j-1], cloud_b_pts[j]
        va0, va1 = indicators["cloud_a"][start_idx+j-1], indicators["cloud_a"][start_idx+j]
        vb0, vb1 = indicators["cloud_b"][start_idx+j-1], indicators["cloud_b"][start_idx+j]
        if None not in (a0, a1, b0, b1, va0, va1, vb0, vb1):
            fill = "#EEF7F1" if float(va0) >= float(vb0) and float(va1) >= float(vb1) else "#FCF0F1"
            d.polygon([a0, a1, b1, b0], fill=fill)

    # Draw Tenkan / Kijun / Kumo boundaries
    for key, color, thick in (("cloud_a", span_a_color, 1), ("cloud_b", span_b_color, 1),
                              ("tenkan", blue, 3), ("kijun", orange, 3)):
        points = []
        for j in range(len(sample)):
            value = indicators[key][start_idx+j]
            points.append(xy(j, float(value)) if value is not None else None)
        for j in range(1, len(points)):
            if points[j-1] is not None and points[j] is not None:
                d.line((points[j-1], points[j]), fill=color, width=thick)

    # Candles on top of the indicators
    body_w = max(3, min(7, int((right-left)/len(sample)*0.62)))
    for j, c in enumerate(sample):
        xx = xy(j, c.close)[0]
        yo, yh, yl, yc = (yprice(c.open), yprice(c.high), yprice(c.low), yprice(c.close))
        col = teal if c.close >= c.open else red
        d.line((xx, yh, xx, yl), fill=col, width=2)
        top_body, bottom_body = min(yo, yc), max(yo, yc)
        if bottom_body-top_body < 2:
            bottom_body = top_body+2
        d.rectangle((xx-body_w//2, top_body, xx+body_w//2, bottom_body), fill=col, outline=col)

    # Entry / SL / TP levels with labels
    levels = []
    if sig:
        levels = [(sig.entry_reference, teal, "ENTRY"), (sig.stop, red, "SL"), (sig.target, "#168B55", "TP")]
    elif position:
        levels = [(float(position["entry"]), teal, "ENTRY"), (float(position["stop"]), red, "SL"), (float(position["target"]), "#168B55", "TP")]
    for price, color, label in levels:
        yy = yprice(price)
        for xx in range(left, right, 12):
            d.line((xx, yy, min(xx+7, right), yy), fill=color, width=2)
        d.rounded_rectangle((left+7, yy-12, left+63, yy+12), radius=5, fill=color)
        d.text((left+13, yy-8), label, fill="#FFFFFF", font=f_axis)

    # Time axis, chart legend, and disclaimer
    for j in (0, 24, 49, 74, 99):
        if j < len(sample):
            xx = xy(j, lo)[0]
            t = datetime.fromtimestamp(sample[j].ts/1000, timezone.utc).strftime("%H:%M")
            d.text((xx-17, bottom+22), t, fill=muted, font=f_axis)
    legend_y = 680
    def legend(x: int, color: str, label: str, line_style: bool = True) -> int:
        d.line((x, legend_y+9, x+30, legend_y+9), fill=color, width=3 if line_style else 7)
        d.text((x+39, legend_y), label, fill=navy, font=f_legend)
        return x + 39 + int(d.textlength(label, font=f_legend)) + 28
    lx = 42
    lx = legend(lx, blue, "TENKAN")
    lx = legend(lx, orange, "KIJUN")
    lx = legend(lx, span_a_color, "KUMO A")
    lx = legend(lx, span_b_color, "KUMO B")
    d.text((42, 718), "SHADOW FAKEOUT  |  CLOSED 1M CANDLES  |  PAPER ONLY - NO REAL ORDERS", fill=muted, font=f_footer)
    d.text((W-310, 718), f"DATA: {DATA_SOURCE_USED.get(normalize_pair(pair), 'PUBLIC FEED')[:36]}", fill=muted, font=f_footer)

    img.save(path, format="PNG", optimize=True)
    return path


# --------------------------- Paper account ---------------------------
def rupiah(value: float) -> str:
    return "Rp" + f"{int(round(value)):,.0f}".replace(",", ".")


def _default_paper_state() -> dict[str, Any]:
    return {
        "version": 1, "balance_idr": float(PAPER_INITIAL_IDR),
        "total_deposits_idr": float(PAPER_INITIAL_IDR), "topups": 0,
        "open_positions": {}, "pending": {}, "last_bar_ts": {}, "last_signal_ts": {}, "last_ich_signal_ts": {},
        "closed_trades": [], "deposit_log": [{"time": datetime.now(timezone.utc).isoformat(), "amount_idr": PAPER_INITIAL_IDR, "reason": "modal awal"}],
    }


class PaperLedger:
    """Persistent IDR paper wallet; there are no exchange orders or real funds."""
    def __init__(self, path: str = STATE_PATH):
        self.path = path
        self.lock = threading.RLock()
        self.state = self._load()

    def _load(self) -> dict[str, Any]:
        if not os.path.exists(self.path):
            data = _default_paper_state()
            self._write(data)
            return data
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            default = _default_paper_state()
            for key, val in default.items():
                data.setdefault(key, val)
            return data
        except (OSError, json.JSONDecodeError) as exc:
            backup = self.path + ".corrupt"
            try:
                os.replace(self.path, backup)
            except OSError:
                pass
            print(f"[Paper] State rusak ({exc}); membuat state baru. Backup: {backup}", flush=True)
            data = _default_paper_state()
            self._write(data)
            return data

    def _write(self, data: dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def save(self) -> None:
        with self.lock:
            self._write(self.state)

    def balance(self) -> float:
        return float(self.state["balance_idr"])

    def queue_signal(self, pair: str, sig: Signal) -> bool:
        with self.lock:
            pair = normalize_pair(pair)
            if pair in self.state["open_positions"] or pair in self.state["pending"]:
                return False
            self.state["pending"][pair] = {
                "symbol": pair, "direction": sig.direction, "signal_ts": sig.candle_ts,
                "signal_time": datetime.fromtimestamp(sig.candle_ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                "entry_reference": sig.entry_reference, "stop": sig.stop, "atr": sig.atr,
                "reason": sig.reason, "score": sig.score,
            }
            self.state["last_signal_ts"][pair] = sig.candle_ts
            self.save()
            return True

    def note_signal_seen(self, pair: str, sig: Signal) -> None:
        with self.lock:
            self.state["last_signal_ts"][normalize_pair(pair)] = sig.candle_ts
            self.save()

    def process_pair(self, pair: str, candles: list[Candle]) -> list[dict[str, Any]]:
        """Process newly closed candles for paper entries, SL/TP and trailing SL."""
        events: list[dict[str, Any]] = []
        pair = normalize_pair(pair)
        if not candles:
            return events
        with self.lock:
            last_ts = int(self.state["last_bar_ts"].get(pair, 0) or 0)
            if last_ts <= 0:
                # Do not retroactively paper-trade the entire downloaded history on first boot.
                self.state["last_bar_ts"][pair] = candles[-1].ts
                self.save()
                return events
            bars = [c for c in candles if c.ts > last_ts]
            if not bars:
                return events
            indicators = build_indicators(candles)
            index_by_ts = {c.ts: i for i, c in enumerate(candles)}
            for bar in bars:
                pending = self.state["pending"].get(pair)
                if pending and bar.ts > int(pending["signal_ts"]) and pair not in self.state["open_positions"]:
                    direction = pending["direction"]
                    entry = bar.open  # confirmed historical open of the candle after the signal candle
                    stop = float(pending["stop"])
                    valid = entry > stop if direction == "BUY" else entry < stop
                    risk = entry - stop if direction == "BUY" else stop - entry
                    balance = float(self.state["balance_idr"])
                    if valid and risk > 0 and balance > 0 and risk <= MAX_RISK_ATR * float(pending["atr"]):
                        target = entry + RR_TARGET * risk if direction == "BUY" else entry - RR_TARGET * risk
                        pos = {
                            "symbol": pair, "direction": direction, "signal_ts": int(pending["signal_ts"]),
                            "signal_time": pending["signal_time"], "entry_ts": bar.ts,
                            "entry_time": bar.time_utc, "entry": entry, "stop": stop,
                            "initial_stop": stop, "target": target, "risk_price": risk,
                            "atr": float(pending["atr"]), "risk_cash_idr": balance * PAPER_RISK_PCT,
                            "reason": pending["reason"], "score": int(pending["score"]),
                            "max_favorable": entry, "bars_held": 0, "last_ts": 0,
                        }
                        self.state["open_positions"][pair] = pos
                        events.append({"type": "OPEN", "position": dict(pos)})
                    else:
                        events.append({"type": "SKIP", "symbol": pair, "reason": "Harga open candle berikutnya melewati SL / risk terlalu besar / saldo nol."})
                    self.state["pending"].pop(pair, None)

                pos = self.state["open_positions"].get(pair)
                if pos and bar.ts >= int(pos["entry_ts"]) and int(pos.get("last_ts", 0)) < bar.ts:
                    direction = pos["direction"]
                    stop = float(pos["stop"])
                    target = float(pos["target"])
                    entry = float(pos["entry"])
                    risk = float(pos["risk_price"])
                    stop_hit = bar.low <= stop if direction == "BUY" else bar.high >= stop
                    target_hit = bar.high >= target if direction == "BUY" else bar.low <= target
                    # Conservative tie handling: stop takes precedence if stop and target are both touched.
                    if stop_hit:
                        exit_price = stop
                        result_r = (exit_price - entry) / risk if direction == "BUY" else (entry - exit_price) / risk
                        outcome = "SL_PLUS" if result_r > 1e-8 else ("BE" if result_r >= -1e-8 else "SL")
                        events.append(self._close(pair, pos, bar, exit_price, result_r, outcome))
                        continue
                    if target_hit:
                        events.append(self._close(pair, pos, bar, target, RR_TARGET, "TP"))
                        continue

                    # No exit on this candle: use the favorable extreme to ratchet the stop for subsequent candles.
                    old_stop = float(pos["stop"])
                    if direction == "BUY":
                        pos["max_favorable"] = max(float(pos["max_favorable"]), bar.high)
                        favorable_r = (float(pos["max_favorable"]) - entry) / risk
                    else:
                        old_best = float(pos["max_favorable"])
                        pos["max_favorable"] = min(old_best, bar.low, entry)
                        favorable_r = (entry - float(pos["max_favorable"])) / risk
                    if favorable_r >= BE_TRIGGER_R:
                        if direction == "BUY":
                            pos["stop"] = max(float(pos["stop"]), entry)
                        else:
                            pos["stop"] = min(float(pos["stop"]), entry)
                    if favorable_r >= LOCK_TRIGGER_R:
                        ix = index_by_ts.get(bar.ts)
                        atr_now = indicators["atr"][ix] if ix is not None else None
                        trail_dist = max(float(atr_now or pos["atr"]) * TRAIL_ATR_MULT, risk * TRAIL_R_MULT)
                        if direction == "BUY":
                            candidate = max(entry + LOCK_MIN_R * risk, float(pos["max_favorable"]) - trail_dist)
                            pos["stop"] = max(float(pos["stop"]), candidate)
                        else:
                            candidate = min(entry - LOCK_MIN_R * risk, float(pos["max_favorable"]) + trail_dist)
                            pos["stop"] = min(float(pos["stop"]), candidate)
                    pos["bars_held"] = int(pos.get("bars_held", 0)) + 1
                    pos["last_ts"] = bar.ts
                    if abs(float(pos["stop"]) - old_stop) > max(1e-9, risk * 0.001):
                        events.append({"type": "TRAIL", "position": dict(pos), "old_stop": old_stop, "new_stop": float(pos["stop"]), "favorable_r": favorable_r, "bar": bar})
                    if int(pos["bars_held"]) >= MAX_HOLD_BARS:
                        exit_price = bar.close
                        result_r = (exit_price - entry) / risk if direction == "BUY" else (entry - exit_price) / risk
                        outcome = "TIMEOUT"
                        events.append(self._close(pair, pos, bar, exit_price, result_r, outcome))
                    else:
                        self.state["open_positions"][pair] = pos
                self.state["last_bar_ts"][pair] = bar.ts
                self.save()
            self.state["last_bar_ts"][pair] = candles[-1].ts
            self.save()
        return events

    def _close(self, pair: str, pos: dict[str, Any], bar: Candle, exit_price: float, result_r: float, outcome: str) -> dict[str, Any]:
        risk_cash = float(pos["risk_cash_idr"])
        pnl = risk_cash * float(result_r)
        self.state["balance_idr"] = float(self.state["balance_idr"]) + pnl
        closed = {
            "symbol": pair, "direction": pos["direction"], "signal_time": pos["signal_time"],
            "entry_time": pos["entry_time"], "exit_time": bar.time_utc,
            "entry": float(pos["entry"]), "stop": float(pos["stop"]), "target": float(pos["target"]),
            "exit_price": float(exit_price), "result_r": float(result_r), "outcome": outcome,
            "pnl_idr": float(pnl), "balance_after_idr": float(self.state["balance_idr"]),
            "bars_held": int(pos.get("bars_held", 0)) + (0 if outcome == "TIMEOUT" else 1), "reason": pos["reason"],
        }
        self.state["closed_trades"].append(closed)
        self.state["closed_trades"] = self.state["closed_trades"][-1000:]
        self.state["open_positions"].pop(pair, None)
        event = {"type": "CLOSE", "trade": closed, "topup_idr": 0}
        # Simulated MC rule: auto-inject Rp500,000 when balance falls to <=10% of all deposits so far.
        mc_line = float(self.state["total_deposits_idr"]) * PAPER_MC_RATIO
        if float(self.state["balance_idr"]) <= mc_line:
            topup = float(PAPER_TOPUP_IDR)
            self.state["balance_idr"] = float(self.state["balance_idr"]) + topup
            self.state["total_deposits_idr"] = float(self.state["total_deposits_idr"]) + topup
            self.state["topups"] = int(self.state.get("topups", 0)) + 1
            self.state["deposit_log"].append({"time": datetime.now(timezone.utc).isoformat(), "amount_idr": topup, "reason": "auto top-up paper MC"})
            event["topup_idr"] = topup
            event["balance_after_topup_idr"] = float(self.state["balance_idr"])
        self.state["last_bar_ts"][pair] = bar.ts
        self.save()
        return event

    def report(self) -> str:
        with self.lock:
            s = self.state
            balance = float(s["balance_idr"])
            deposits = float(s["total_deposits_idr"])
            pnl = balance - deposits
            closed = s["closed_trades"]
            wins = sum(1 for t in closed if float(t.get("pnl_idr", 0)) > 0)
            losses = sum(1 for t in closed if float(t.get("pnl_idr", 0)) < 0)
            count = lambda outcome: sum(1 for trade in closed if str(trade.get("outcome", "")).upper() == outcome)
            lines = [
                "PAPER ACCOUNT — LIQUIDITY SNIPER M1",
                f"Saldo/equity simulasi: {rupiah(balance)}",
                f"Total deposit (modal awal + top-up): {rupiah(deposits)}",
                f"P/L bersih vs total deposit: {rupiah(pnl)}",
                f"Modal awal: {rupiah(PAPER_INITIAL_IDR)} | Auto top-up MC: {rupiah(PAPER_TOPUP_IDR)} x {int(s.get('topups', 0))}",
                f"Risk per trade: {PAPER_RISK_PCT*100:.1f}% dari saldo saat entry | BE +{BE_TRIGGER_R:.2f}R | SL+ trailing mulai +{LOCK_TRIGGER_R:.1f}R",
                f"Trade selesai: {len(closed)} | TP {count('TP')} | SL+ {count('SL_PLUS') + count('SL+')} | BE {count('BE')} | SL {count('SL')} | Timeout {count('TIMEOUT')}",
                f"P/L trade: {wins} positif / {losses} negatif",
                f"MC simulasi: equity <= {PAPER_MC_RATIO*100:.0f}% total deposit, lalu top-up otomatis Rp500.000.",
                "Catatan: ini buku besar simulasi, bukan saldo broker; tidak ada order nyata.",
            ]
            opens = s["open_positions"]
            for pair, pos in opens.items():
                entry = float(pos["entry"])
                initial_stop = float(pos.get("initial_stop", pos.get("stop", entry)))
                initial_risk = abs(entry - initial_stop)
                stop = float(pos["stop"])
                stop_r = ((stop - entry) / initial_risk if pos["direction"] == "BUY" else (entry - stop) / initial_risk) if initial_risk > 0 else 0.0
                stage = "SL+" if stop_r > 1e-8 else ("BE" if stop_r >= -1e-8 else "INITIAL SL")
                lines.append(f"OPEN {pair} {pos['direction']} | entry {entry:.5f} | SL {stop:.5f} ({stage}, kunci {stop_r:+.2f}R) | TP {float(pos['target']):.5f} | risk {rupiah(float(pos['risk_cash_idr']))}")
            for pair, pending in s["pending"].items():
                lines.append(f"PENDING {pair} {pending['direction']} | menunggu OPEN candle berikutnya")
            return "\n".join(lines)

    def reset(self) -> None:
        with self.lock:
            self.state = _default_paper_state()
            self.save()


# --------------------------- Telegram integration ---------------------------
class TelegramBot:
    def __init__(self, token: str, allowed_chat_id: Optional[str] = None, auto_scan: bool = True):
        self.token = token.strip()
        self.base = f"https://api.telegram.org/bot{self.token}/"
        self.allowed_chat_id = str(allowed_chat_id).strip() if allowed_chat_id else None
        self.owner_ids = {x.strip() for x in os.getenv("BOT_OWNER_IDS", "").replace(";", ",").split(",") if x.strip().isdigit()}
        self.auto_scan = auto_scan
        self.offset: Optional[int] = None
        self.backtest_lock = threading.Lock()
        self.scan_lock = threading.Lock()
        self.paper = PaperLedger()
        # Topic untuk alert bot liquid utama (opsional). Kosong = General / tanpa topic.
        try:
            self.main_thread_id: Optional[int] = int(os.getenv("LIQ_TOPIC_ID", "").strip() or 0) or None
        except ValueError:
            self.main_thread_id = None
        self.engines: dict[str, Any] = {}

    def call(self, method: str, params: dict[str, Any], timeout: int = 35) -> dict[str, Any]:
        body = urllib.parse.urlencode(params).encode("utf-8")
        req = urllib.request.Request(self.base + method, data=body, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except Exception as exc:
            raise RuntimeError(f"Telegram API {method} gagal: {exc}") from exc
        if not payload.get("ok"):
            raise RuntimeError(f"Telegram API {method}: {payload}")
        return payload

    def _thread_for(self, chat_id: Any, thread: Optional[int]) -> Optional[int]:
        if thread:
            return thread
        if self.main_thread_id and self.allowed_chat_id and str(chat_id) == self.allowed_chat_id:
            return self.main_thread_id
        return None

    def send(self, chat_id: Any, text: str, thread: Optional[int] = None) -> None:
        text = str(text)
        chunks = [text[i:i + 3900] for i in range(0, len(text), 3900)] or [""]
        thread = self._thread_for(chat_id, thread)
        for chunk in chunks:
            params: dict[str, Any] = {"chat_id": str(chat_id), "text": chunk}
            if thread:
                params["message_thread_id"] = str(thread)
            try:
                self.call("sendMessage", params, timeout=20)
            except Exception as exc:
                if thread and ("thread" in str(exc).lower() or "topic" in str(exc).lower()):
                    params.pop("message_thread_id", None)
                    try:
                        self.call("sendMessage", params, timeout=20)
                        continue
                    except Exception as exc2:
                        exc = exc2
                print(f"[Telegram send error] {exc}", flush=True)

    def send_photo(self, chat_id: Any, photo_path: str, caption: str, thread: Optional[int] = None) -> None:
        """Telegram sendPhoto via multipart/form-data, without external packages."""
        if not os.path.isfile(photo_path):
            self.send(chat_id, caption + "\n[Chart file tidak ditemukan]", thread)
            return
        boundary = "----TermuxPaperBot" + str(int(time.time() * 1000))
        body = bytearray()
        def field(name: str, value: str) -> None:
            body.extend(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n".encode())
        field("chat_id", str(chat_id))
        thread = self._thread_for(chat_id, thread)
        if thread:
            field("message_thread_id", str(thread))
        field("caption", caption[:1000])
        with open(photo_path, "rb") as f:
            image_data = f.read()
        body.extend(f"--{boundary}\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"chart.png\"\r\nContent-Type: image/png\r\n\r\n".encode())
        body.extend(image_data)
        body.extend(f"\r\n--{boundary}--\r\n".encode())
        req = urllib.request.Request(self.base + "sendPhoto", data=bytes(body), headers={
            "User-Agent": USER_AGENT, "Content-Type": f"multipart/form-data; boundary={boundary}",
        })
        try:
            with urllib.request.urlopen(req, timeout=35) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
            if not payload.get("ok"):
                raise RuntimeError(str(payload))
        except Exception as exc:
            print(f"[Telegram photo error] {exc}", flush=True)
            self.send(chat_id, caption + "\nChart gagal terkirim; gambar tersimpan lokal: " + photo_path, thread)

    def authorized_message(self, message: dict[str, Any]) -> bool:
        """Owner IDs always pass. In groups, current Telegram creators/admins can use commands.
        Private chats are restricted to BOT_OWNER_IDS.
        """
        sender = message.get("from") or {}
        user_id = str(sender.get("id", ""))
        if user_id and user_id in self.owner_ids:
            return True
        chat = message.get("chat") or {}
        chat_type = chat.get("type")
        if chat_type in {"group", "supergroup"} and user_id:
            try:
                payload = self.call("getChatMember", {"chat_id": str(chat.get("id")), "user_id": user_id}, timeout=12)
                status = (payload.get("result") or {}).get("status")
                return status in {"creator", "administrator"}
            except Exception as exc:
                print(f"[Security] Gagal verifikasi admin Telegram user={user_id}: {exc}", flush=True)
                return False
        return False

    def delete_unauthorized_command(self, message: dict[str, Any]) -> None:
        """Try removing a non-admin's command in a group; bot needs Delete Messages admin permission."""
        chat = message.get("chat") or {}
        msg_id = message.get("message_id")
        if chat.get("type") not in {"group", "supergroup"}:
            return
        if not msg_id or not chat.get("id"):
            return
        try:
            self.call("deleteMessage", {"chat_id": str(chat["id"]), "message_id": str(msg_id)}, timeout=10)
            print(f"[Security] Command anggota non-admin dihapus; chat={chat.get('id')}", flush=True)
        except Exception as exc:
            print(f"[Security] Command non-admin ditolak tetapi tidak dapat dihapus. Pastikan bot admin dengan izin Delete Messages. err={exc}", flush=True)

    def send_signal_chart(self, chat_id: Any, pair: str, candles: list[Candle], sig: Optional[Signal],
                          paper_queued: Optional[bool] = None, thread: Optional[int] = None) -> None:
        last = candles[-1]
        source = DATA_SOURCE_USED.get(normalize_pair(pair), "public feed")
        label = display_pair(pair)
        if sig:
            paper_status = "QUEUED" if paper_queued else ("SKIP" if paper_queued is False else "ALERT")
            is_ich = sig.engine.startswith("ICHIMOKU M15")
            tf = "M15" if is_ich else "1M"
            entry_label = "LIMIT 1" if sig.order_type == "LIMIT" else "ENTRY*"
            score_max = 9 if is_ich else 5
            status_line = ("ALERT ONLY · independent engine; belum dimasukkan ke saldo paper" if is_ich
                           else f"{paper_status} · BE +{BE_TRIGGER_R:.2f}R / SL+ trailing +{LOCK_TRIGGER_R:.1f}R")
            table = [
                f"{'🟢' if sig.direction == 'BUY' else '🔴'} {label} · {tf} · {sig.direction}",
                f"ENGINE │ {sig.engine} │ score {sig.score}/{score_max}",
                f"CLOSE │ {last.time_utc}",
                "┌────────┬────────────────┐",
                f"│ {entry_label:<6} │ {sig.entry_reference:.5f}",
                *([f"│ LIMITS │ {' / '.join(f'{x:.2f}' for x in sig.entry_levels)}"] if is_ich else []),
                f"│ SL     │ {sig.stop:.5f}",
                f"│ TP1    │ {sig.target:.5f}",
            ]
            if is_ich and sig.target2:
                table.append(f"│ TP2    │ {sig.target2:.5f}")
            table += ["└────────┴────────────────┘", f"WHY │ {sig.reason[:160]}",
                      f"PAPER │ {status_line}", f"DATA │ {source[:75]}"]
            if not is_ich:
                table.append("*Ref candle close; paper entry pada open candle berikutnya.")
            caption = "\n".join(table)
        else:
            caption = "\n".join([
                f"{label} · 1M · MONITOR",
                f"CLOSE │ {last.time_utc}",
                "┌────────┬────────────────┐",
                f"│ PRICE  │ {last.close:.5f}",
                "│ SETUP  │ WAIT",
                "└────────┴────────────────┘",
                "CHECK │ Liquidity sweep + reclaim + Ichimoku",
                f"DATA │ {source[:75]}",
            ])
        chart = draw_chart(candles, pair, sig)
        self.send_photo(chat_id, chart, caption[:1000], thread)


    def handle_message(self, message: dict[str, Any]) -> None:
        chat_id = (message.get("chat") or {}).get("id")
        text_in = (message.get("text") or "").strip()
        thread = message.get("message_thread_id") if message.get("is_topic_message") else None
        if chat_id is None or not text_in.startswith("/"):
            # When BotFather privacy mode is disabled, ignore normal group chatter and never log it.
            return
        sender_id = str((message.get("from") or {}).get("id", "?"))
        print(f"[Telegram] command attempt chat_id={chat_id} user_id={sender_id} cmd={text_in.split()[0][:40]!r}", flush=True)
        # All slash commands are owner/admin-only, including unknown commands. Non-admin
        # group commands are automatically deleted when Telegram permissions allow it.
        if not self.authorized_message(message):
            self.delete_unauthorized_command(message)
            return
        parts = text_in.split()
        command = parts[0].split("@", 1)[0].lower()
        args = parts[1:]
        # Safe command aliases for users migrating from the previous main.py.
        if command == "/scan":
            command = "/cek"
            args = args or ["all"]
        elif command in {"/capital", "/rekap"}:
            command = "/paper"
        elif command == "/status":
            command = "/engines"
        elif command == "/ichi":
            command, args = "/cek", ["XAU"]
        if command == "/rencana":
            self.send(chat_id, "Engine /rencana multi-timeframe lama telah diganti dengan Liquidity Sniper. Gunakan /cek [PAIR] untuk sinyal dan /tpb cek untuk Trend Pullback M5. Untuk histori gunakan /backtest [hari] [pair|all].", thread)
            return
        if command == "/offset":
            self.send(chat_id, "Command /offset dari arsitektur Bybit XAU lama tidak berlaku untuk semua feed Liquidity Sniper. Sumber/jenis feed aktif ditampilkan pada caption sinyal; konfigurasi engine-specific tersedia di Railway Variables.", thread)
            return
        if command == "/be":
            self.send(chat_id, f"Main paper engine BE aktif otomatis pada +{BE_TRIGGER_R:.2f}R dan trailing SL+ mulai +{LOCK_TRIGGER_R:.2f}R. Untuk engine TPB, gunakan /tpb be on atau /tpb be off. Untuk LV2, atur LV2_TRAILING dan LV2_BE_TRIGGER_R di Railway Variables.", thread)
            return
        if command in {"/start", "/help"}:
            self.send(chat_id, help_text(), thread)
        elif command in {"/cek", "/signal"}:
            pair = "ALL"
            if args:
                try:
                    pair = normalize_pair(args[0])
                except ValueError:
                    self.send(chat_id, "Pair salah. Gunakan /cek " + PAIR_HELP + "/all", thread)
                    return
            pairs = ALL_PAIRS[:] if pair == "ALL" else [pair]
            for p in pairs:
                try:
                    data = fetch_candles(p, 1)
                    sig, _info = latest_closed_signal(p, data)
                    self.send_signal_chart(chat_id, p, data, sig, None, thread)
                    if p == "XAUUSD":
                        ich_sig = detect_ichimoku_m15_signal(data)
                        if ich_sig is not None:
                            self.send_signal_chart(chat_id, p, data, ich_sig, None, thread)
                        else:
                            print("[ICH M15 XAUUSD] tidak ada setup pada candle M15 terakhir yang closed", flush=True)
                except Exception as exc:
                    self.send(chat_id, f"Gagal cek {p}: {exc}", thread)
        elif command in {"/backtest", "/bt"}:
            days = 1
            pair = "ALL"
            for arg in args:
                if arg.isdigit():
                    days = max(1, min(int(arg), 30))
                else:
                    try:
                        pair = normalize_pair(arg)
                    except ValueError:
                        self.send(chat_id, "Format: /backtest [hari] [" + PAIR_HELP + "/all]. Contoh: /backtest 1 all", thread)
                        return
            if not self.backtest_lock.acquire(blocking=False):
                self.send(chat_id, "Backtest lain masih berjalan. Tunggu sampai laporan selesai.", thread)
                return
            pairs = ALL_PAIRS[:] if pair == "ALL" else [pair]
            self.send(chat_id, f"Backtest {', '.join(pairs)} {days} hari dimulai. Data 1M sedang diambil; hanya simulasi.", thread)
            threading.Thread(target=self._backtest_job, args=(chat_id, pairs, days, thread), daemon=True).start()
        elif command == "/paper":
            self.send(chat_id, self.paper.report(), thread)
        elif command == "/paper_reset":
            if args != ["KONFIRM"]:
                self.send(chat_id, "Reset hanya buku simulasi. Untuk benar-benar reset saldo dan menghapus posisi/trade paper, kirim: /paper_reset KONFIRM", thread)
            else:
                self.paper.reset()
                self.send(chat_id, "Paper account direset. Saldo simulasi kembali Rp1.000.000; riwayat paper dihapus. Tidak memengaruhi akun exchange.", thread)
        elif command == "/stats":
            self.send(chat_id, self.paper.report() + "\n\nEngine: Shadow Fakeout M1 + independent Ichimoku M15 XAUUSD. Ichimoku M15 is a separate alert and is not mixed into the existing paper balance. No real orders.", thread)
        elif command == "/id":
            self.send(chat_id, "\n".join([
                f"chat_id: {chat_id}",
                f"message_thread_id (topic): {message.get('message_thread_id') if message.get('is_topic_message') else 'tidak ada — kirim /id DI DALAM topic yang diinginkan'}",
                f"user_id: {sender_id}",
                "Isi Railway Variables: BOT_TELEGRAM_CHAT_ID=<chat_id>, lalu SFP_TOPIC_ID / TPB_TOPIC_ID / LV2_TOPIC_ID / LIQ_TOPIC_ID = <message_thread_id> topic masing-masing.",
            ]), thread)
        elif command == "/engines":
            self.send(chat_id, self.engines_report(), thread)
        elif command in {"/sfp", "/tpb", "/v2"}:
            engine = self.engines.get({"/sfp": "sfp", "/tpb": "tpb", "/v2": "lv2"}[command])
            if engine is None:
                self.send(chat_id, f"Engine untuk {command} tidak aktif (cek {command[1:].upper()}_ENABLED dan log Railway).", thread)
            else:
                threading.Thread(target=self._run_engine_command, args=(engine, args, chat_id, thread), daemon=True).start()
        else:
            self.send(chat_id, "Perintah tidak dikenal. Kirim /help", thread)

    def _run_engine_command(self, engine: Any, args: list[str], chat_id: Any, thread: Optional[int]) -> None:
        try:
            engine.handle_command(args, chat_id, thread)
        except Exception as exc:
            self.send(chat_id, f"Perintah engine gagal: {exc}", thread)

    def engines_report(self) -> str:
        if not self.engines:
            return "Tidak ada engine aktif."
        out = []
        for key in ("sfp", "tpb", "lv2"):
            eng = self.engines.get(key)
            if eng is not None:
                try:
                    out.append(re.sub(r"<[^>]+>", "", eng.status_text()))
                except Exception as exc:
                    out.append(f"{key}: status gagal ({exc})")
        return "\n\n".join(out)

    def _backtest_job(self, chat_id: Any, pairs: list[str], days: int, thread: Optional[int] = None) -> None:
        try:
            for pair in pairs:
                try:
                    d = min(days, max_backtest_days(pair))
                    report, _csv_path = run_backtest_pair(pair, d, save=True)
                    self.send(chat_id, report, thread)
                except Exception as exc:
                    self.send(chat_id, f"BACKTEST {pair} GAGAL: {exc}", thread)
        finally:
            self.backtest_lock.release()

    def _send_paper_event(self, event: dict[str, Any]) -> None:
        if not self.allowed_chat_id:
            print(f"[Paper event] {event}", flush=True)
            return
        kind = event.get("type")
        if kind == "OPEN":
            p = event["position"]
            text_msg = "\n".join([
                f"🧪 PAPER OPEN — {p['symbol']} {p['direction']}",
                f"Entry: {float(p['entry']):.5f} | SL: {float(p['stop']):.5f} | TP: {float(p['target']):.5f}",
                f"Risk simulasi: {rupiah(float(p['risk_cash_idr']))} ({PAPER_RISK_PCT*100:.1f}% saldo saat entry)",
                f"Modal paper sekarang: {rupiah(self.paper.balance())}",
                f"Reason: {p['reason']}",
            ])
        elif kind == "TRAIL":
            p = event["position"]
            entry = float(p["entry"])
            initial_risk = abs(entry - float(p.get("initial_stop", p.get("stop", entry))))
            stop = float(event["new_stop"])
            lock_r = ((stop - entry) / initial_risk if p["direction"] == "BUY" else (entry - stop) / initial_risk) if initial_risk > 0 else 0.0
            stage = "SL+" if lock_r > 1e-8 else ("BE" if lock_r >= -1e-8 else "SL")
            text_msg = "\n".join([
                f"🔒 PAPER {stage} UPDATE — {p['symbol']} {p['direction']}",
                f"SL: {float(event['old_stop']):.5f} → {stop:.5f} | status {stage} ({lock_r:+.2f}R terkunci)",
                f"Pergerakan favorable: +{float(event['favorable_r']):.2f}R | Entry {entry:.5f}",
                f"TP tetap: {float(p['target']):.5f} | Modal: {rupiah(self.paper.balance())}",
                "Stop hanya dirapatkan, tidak pernah dilonggarkan. Update berlaku mulai candle berikutnya.",
            ])
        elif kind == "CLOSE":
            t = event["trade"]
            outcome_label = {"SL_PLUS": "SL+", "SL+": "SL+", "BE": "BE", "SL": "SL", "TP": "TP", "TIMEOUT": "TIMEOUT"}.get(str(t["outcome"]).upper(), str(t["outcome"]))
            text_msg = "\n".join([
                f"{'✅' if float(t['pnl_idr']) > 0 else '❌' if float(t['pnl_idr']) < 0 else '➖'} PAPER CLOSED — {t['symbol']} {t['direction']} | {outcome_label}",
                f"Entry {float(t['entry']):.5f} → Exit {float(t['exit_price']):.5f}",
                f"Hasil: {float(t['result_r']):+.2f}R | P/L: {rupiah(float(t['pnl_idr']))}",
                f"Modal paper: {rupiah(float(t['balance_after_idr']))}",
            ])
            if event.get("topup_idr"):
                text_msg += f"\n🆘 SIMULASI MC: auto top-up {rupiah(float(event['topup_idr']))}. Saldo sesudah top-up: {rupiah(float(event['balance_after_topup_idr']))}."
        else:
            text_msg = f"Paper {event.get('symbol', '')}: {event.get('reason', 'event')}"
        self.send(self.allowed_chat_id, text_msg)

    def auto_scan_once(self) -> None:
        """Fetch new closed 1m bars, manage paper positions, and alert fresh signals with charts."""
        if not self.scan_lock.acquire(blocking=False):
            return
        try:
            for pair in ALL_PAIRS:
                try:
                    candles = fetch_candles(pair, 1)
                    if not candles:
                        continue
                    # Process active/pending paper trades first, using only newly closed bars.
                    for event in self.paper.process_pair(pair, candles):
                        self._send_paper_event(event)
                    # Pasar tutup / feed basi (NAS100, USOIL, GBPJPY, XAU akhir pekan): jangan buat sinyal baru
                    stale_min = (time.time() * 1000 - candles[-1].ts) / 60_000
                    if stale_min > STALE_SIGNAL_MIN:
                        continue
                    # Independent XAU M15 Ichimoku engine runs regardless of the 1M result.
                    if pair == "XAUUSD":
                        try:
                            ich_sig = detect_ichimoku_m15_signal(candles)
                            if ich_sig is not None:
                                seen = int(self.paper.state.setdefault("last_ich_signal_ts", {}).get(pair, 0) or 0)
                                if ich_sig.candle_ts > seen:
                                    self.paper.state["last_ich_signal_ts"][pair] = ich_sig.candle_ts
                                    self.paper.save()
                                    if self.allowed_chat_id:
                                        self.send_signal_chart(self.allowed_chat_id, pair, candles, ich_sig, None)
                                    else:
                                        print(f"[Auto ICH M15 {pair}] {ich_sig.direction} LIMIT={ich_sig.entry_reference:.2f} SL={ich_sig.stop:.2f} TP1={ich_sig.target:.2f} TP2={ich_sig.target2:.2f}", flush=True)
                        except Exception as exc:
                            print(f"[Auto ICH M15 XAUUSD] {exc}", flush=True)
                    sig, _info = latest_closed_signal(pair, candles)
                    if sig is None:
                        continue
                    prev_sig_ts = int(self.paper.state["last_signal_ts"].get(pair, 0) or 0)
                    if sig.candle_ts <= prev_sig_ts:
                        continue
                    sig.symbol = pair
                    queued = self.paper.queue_signal(pair, sig)
                    if not queued:
                        self.paper.note_signal_seen(pair, sig)
                    if self.allowed_chat_id:
                        self.send_signal_chart(self.allowed_chat_id, pair, candles, sig, queued)
                    else:
                        chart = draw_chart(candles, pair, sig)
                        print(f"[Auto signal {pair}] {sig.direction} entry_ref={sig.entry_reference:.5f} SL={sig.stop:.5f} TP={sig.target:.5f}; chart={chart}; paper_queued={queued}", flush=True)
                    # Do not nest ICH under the 1M signal test: this engine can signal independently.
                except Exception as exc:
                    print(f"[Auto scan {pair}] {exc}", flush=True)
        finally:
            self.scan_lock.release()

    def run(self) -> None:
        print(f"Liquidity Sniper bot v{VERSION} berjalan: live market scan + paper account; tidak ada order exchange.", flush=True)
        print(f"Paper state tersimpan di: {STATE_PATH}", flush=True)
        if not self.owner_ids:
            print("PERINGATAN: BOT_OWNER_IDS kosong. Admin grup masih bisa diverifikasi di grup, tetapi akses owner dari private chat terkunci. Isi BOT_OWNER_IDS dengan user ID Telegram owner.", flush=True)
        if self.auto_scan and not self.allowed_chat_id:
            print("BOT_TELEGRAM_CHAT_ID belum diatur: sinyal/chart hanya tercatat di terminal; set ID chat untuk push Telegram.", flush=True)
        if os.getenv("ENGINES_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}:
            try:
                from engines.manager import start_engines
                self.engines = start_engines(self.token, self.allowed_chat_id)
                print(f"Engine independen aktif: {', '.join(self.engines) or 'tidak ada'}", flush=True)
            except Exception:
                print("[Engines] gagal memulai:\n" + traceback.format_exc(), flush=True)
        next_scan = 0.0
        while True:
            try:
                params: dict[str, Any] = {"timeout": 20, "allowed_updates": json.dumps(["message"])}
                if self.offset is not None:
                    params["offset"] = self.offset
                updates = self.call("getUpdates", params, timeout=30).get("result", [])
                for update in updates:
                    self.offset = int(update.get("update_id", 0)) + 1
                    message = update.get("message")
                    if message:
                        self.handle_message(message)
                now = time.monotonic()
                if self.auto_scan and now >= next_scan:
                    threading.Thread(target=self.auto_scan_once, daemon=True).start()
                    next_scan = now + DATA_SCAN_INTERVAL_SEC
            except KeyboardInterrupt:
                print("Bot dihentikan.", flush=True)
                return
            except Exception as exc:
                print(f"[Telegram polling] {exc}", flush=True)
                time.sleep(3)


def help_text() -> str:
    return "\n".join([
        f"LIQUIDITY SNIPER 1M v{VERSION}",
        "Main: Shadow Fakeout + Ichimoku M1; ICHIMOKU M15 XAU terpisah.",
        "Engines independen: SFP PDF M1, Trend Pullback M5, Liquidity Sniper v2; state + topic masing-masing.",
        "Pair: XAUUSD, BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, NAS100, USOIL (Crude Oil), GBPJPY.",
        "Feeds: Bybit linear untuk crypto; XAUT/Yahoo untuk emas; NAS100/USOIL/GBPJPY: Yahoo lalu Twelve Data jika API key tersedia.",
        "Engine: Shadow Fakeout 1M (existing) + ICHIMOKU M15 XAU (independent alert; TP1/TP2; tidak masuk saldo paper).",
        "Paper: modal Rp1.000.000; risiko 1%; MC simulasi + top-up Rp500.000; stop dikelola per candle closed.",
        "SL update: Main BE +0.70R → trailing SL+ +1.00R; SFP BE +1.00R → SL+ +1.20R; TPB BE+ +0.80R → SL+ +1.20R; LV2 BE+ +0.80R → trailing SL+ +1.00R.",
        "Security: command hanya owner (BOT_OWNER_IDS) atau creator/admin grup; command member dicoba hapus otomatis.",
        "",
        "/cek all — chart/status seluruh pair",
        "/cek XAU | BTC | ETH | SOL | XRP | NAS100 | USOIL | GBPJPY",
        "/backtest 1 all — backtest satu hari semua pair",
        "/backtest 3 ETH — backtest tiga hari ETH",
        "/paper — saldo dan posisi paper | /stats — status ringkas",
        "/paper_reset KONFIRM — reset buku simulasi saja",
        "",
        "ENGINE INDEPENDEN (masing-masing topic sendiri):",
        "/id — tampilkan chat_id & message_thread_id topic (untuk isi *_TOPIC_ID)",
        "/engines — status ketiga engine",
        "/sfp [status|cek|backtest N] — SHADOW FAKEOUT PDF 1M + BE/SL+ dengan notifikasi update SL",
        "/tpb [status|cek|capital|rekap|be|offset|bt] — TREND PULLBACK M5 + notifikasi BE+/SL+",
        "/v2 [cek|saldo|posisi|backtest|stats|reset] — LIQUIDITY SNIPER v2 + update trailing SL+",
        "",
        "Bot tidak mengirim order nyata. Futures/FX Yahoo bukan harga CFD broker yang presisi.",
        "Tambahkan bot sebagai admin grup dengan Delete Messages untuk menghapus command anggota.",
    ])


def cli_main(args: argparse.Namespace) -> int:
    if args.backtest is not None:
        pairs = ALL_PAIRS[:] if normalize_pair(args.pair) == "ALL" else [normalize_pair(args.pair)]
        code = 0
        for pair in pairs:
            try:
                days = min(args.backtest, max_backtest_days(pair))
                report, _path = run_backtest_pair(pair, days, save=True)
                print("\n" + report + "\n", flush=True)
            except Exception as exc:
                code = 1
                print(f"BACKTEST {pair} GAGAL: {exc}", file=sys.stderr, flush=True)
        return code
    if args.signal:
        pairs = ALL_PAIRS[:] if normalize_pair(args.pair) == "ALL" else [normalize_pair(args.pair)]
        code = 0
        for pair in pairs:
            try:
                candles = fetch_candles(pair, 1)
                sig, _info = latest_closed_signal(pair, candles)
                chart_path = draw_chart(candles, pair, sig)
                print((format_signal(pair, candles) + f"\nChart: {chart_path}\n") if sig else (f"{pair}: belum ada sinyal. Harga {candles[-1].close:.5f}. Chart: {chart_path}\n"))
                if pair == "XAUUSD":
                    ich_sig = detect_ichimoku_m15_signal(candles)
                    if ich_sig:
                        print(f"ICHIMOKU M15 INDEPENDENT: {ich_sig.direction} LIMIT {ich_sig.entry_reference:.2f} | SL {ich_sig.stop:.2f} | TP1 {ich_sig.target:.2f} | TP2 {ich_sig.target2:.2f} | {ich_sig.reason}")
                    else:
                        print("ICHIMOKU M15 INDEPENDENT: belum ada setup pada M15 terakhir yang closed")
            except Exception as exc:
                code = 1
                print(f"CEK {pair} GAGAL: {exc}", file=sys.stderr)
        return code
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_TOKEN") or "").strip()
    if not token:
        print(help_text())
        print("\nTelegram token belum diatur. Untuk bot live alert + paper account, set TELEGRAM_BOT_TOKEN (atau TG_TOKEN lama), BOT_TELEGRAM_CHAT_ID (atau TG_CHAT_ID lama), dan BOT_OWNER_IDS lalu jalankan python main.py")
        return 2
    auto = os.getenv("BOT_AUTO_SCAN", "1").strip().lower() not in {"0", "false", "no", "off"}
    chat_id = (os.getenv("BOT_TELEGRAM_CHAT_ID") or os.getenv("TG_CHAT_ID") or "").strip() or None
    TelegramBot(token, chat_id, auto_scan=auto).run()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Liquidity Sniper multi-engine Telegram alerts + paper tracking; no live orders.")
    parser.add_argument("--backtest", type=int, metavar="DAYS", help="backtest N days from 1m candles (Yahoo feeds max 7 days; Bybit crypto max 30 days)")
    parser.add_argument("--signal", action="store_true", help="check latest signal and save chart locally")
    parser.add_argument("--pair", default="all", help="XAU, BTC, ETH, SOL, XRP, NAS100, USOIL, GBPJPY, or all (default all)")
    parser.add_argument("pair_pos", nargs="?", default=None, help="optional pair argument")
    args = parser.parse_args()
    if args.pair_pos:
        args.pair = args.pair_pos
    try:
        return cli_main(args)
    except ValueError as exc:
        print(f"Input salah: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


# ============================================================================
# SELF-CONTAINED ENGINE BUNDLE
# All shared feed, Telegram/chart utilities, SFP, TPB, LV2 and manager source
# is stored in this main.py and installed as in-memory modules at startup.
# No separate engines/ directory is needed.
# ============================================================================
_EMBEDDED_ENGINE_SOURCES = {
    "common": r'''
# -*- coding: utf-8 -*-
"""Utilitas bersama untuk semua engine: env, state JSON, Telegram (topic-aware), chart."""
from __future__ import annotations

import html
import io
import json
import os
import queue
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

try:  # chart opsional; tanpa Pillow sinyal dikirim sebagai teks
    from PIL import Image, ImageDraw, ImageFont
except Exception:  # pragma: no cover
    Image = ImageDraw = ImageFont = None

UA = "Mozilla/5.0 (X11; Linux x86_64) liquidity-sniper/4.0"
WIB = timezone(timedelta(hours=7))


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z {msg}", flush=True)


# --------------------------------------------------------------------------- env
def env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def env_float(name: str, default: float) -> float:
    try:
        return float(env_str(name) or default)
    except ValueError:
        return float(default)


def env_int(name: str, default: int) -> int:
    try:
        return int(float(env_str(name) or default))
    except ValueError:
        return int(default)


def env_bool(name: str, default: bool) -> bool:
    v = env_str(name).lower()
    return default if not v else v not in {"0", "false", "no", "off"}


def env_topic(name: str) -> Optional[int]:
    """Topic ID Telegram (message_thread_id). Kosong/invalid -> None (kirim ke General)."""
    raw = env_str(name)
    if not raw:
        return None
    try:
        value = int(raw)
        return value if value > 0 else None
    except ValueError:
        log(f"[env] {name}={raw!r} bukan angka; diabaikan")
        return None


def data_dir() -> str:
    return env_str("BOT_DATA_DIR") or env_str("RAILWAY_VOLUME_MOUNT_PATH") or "data"


def state_path(filename: str) -> str:
    return os.path.join(data_dir(), filename)


_FILE_LOCK = threading.Lock()


def load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return default
    except (OSError, ValueError) as exc:
        backup = path + ".corrupt"
        try:
            os.replace(path, backup)
        except OSError:
            pass
        log(f"[state] {path} rusak ({exc}); dipindah ke {backup}")
        return default


def save_json(path: str, data: Any) -> None:
    with _FILE_LOCK:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False)
            os.replace(tmp, path)
        except OSError as exc:
            log(f"[state] gagal simpan {path}: {exc}")


def strip_tags(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", text))


def esc(text: Any) -> str:
    return html.escape(str(text), quote=False)


def rupiah(value: float) -> str:
    n = int(round(abs(value)))
    s = f"{n:,}".replace(",", ".")
    return f"-Rp{s}" if (value < 0 and n) else f"Rp{s}"


# --------------------------------------------------------------------------- Telegram
class TelegramError(RuntimeError):
    def __init__(self, code: int, desc: str, retry_after: float = 0.0):
        super().__init__(f"Telegram {code}: {desc}")
        self.code = code
        self.desc = desc
        self.retry_after = retry_after


def build_multipart(params: dict[str, Any], file_field: str, filename: str, data: bytes,
                    mime: str) -> tuple[bytes, str]:
    boundary = "----sniper" + uuid.uuid4().hex
    out = io.BytesIO()
    for key, value in params.items():
        out.write((f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n'
                   "Content-Type: text/plain; charset=utf-8\r\n\r\n").encode("utf-8"))
        out.write(str(value).encode("utf-8"))
        out.write(b"\r\n")
    out.write((f'--{boundary}\r\nContent-Disposition: form-data; name="{file_field}"; filename="{filename}"\r\n'
               f"Content-Type: {mime}\r\n\r\n").encode("utf-8"))
    out.write(data)
    out.write(b"\r\n")
    out.write(f"--{boundary}--\r\n".encode("utf-8"))
    return out.getvalue(), f"multipart/form-data; boundary={boundary}"


class TelegramHub:
    """Satu antrean kirim untuk SEMUA engine (token sama): >=1.1 dtk antar pesan,
    maks 18/menit, patuh retry_after. Topic ID ditentukan per pesan."""

    _instances: dict[str, "TelegramHub"] = {}
    _guard = threading.Lock()

    @classmethod
    def get(cls, token: str) -> "TelegramHub":
        with cls._guard:
            hub = cls._instances.get(token)
            if hub is None:
                hub = cls(token)
                cls._instances[token] = hub
            return hub

    def __init__(self, token: str):
        self.token = token.strip()
        self.api = env_str("TELEGRAM_API_BASE", "https://api.telegram.org").rstrip("/")
        self.q: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=400)
        self.min_gap = 1.1
        self.max_per_min = 18
        self._last = 0.0
        self._sent: deque = deque()
        threading.Thread(target=self._worker, name="tg-hub", daemon=True).start()

    def call(self, method: str, params: dict[str, Any], file: Optional[tuple[str, str, bytes, str]] = None,
             timeout: int = 30) -> dict[str, Any]:
        url = f"{self.api}/bot{self.token}/{method}"
        headers = {"User-Agent": UA}
        if file is not None:
            body, ctype = build_multipart(params, *file)
            headers["Content-Type"] = ctype
        else:
            body = urllib.parse.urlencode(params).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            try:
                payload = json.loads(exc.read().decode("utf-8", errors="replace"))
            except Exception:
                payload = {"ok": False, "error_code": exc.code, "description": f"HTTP {exc.code}"}
        except Exception as exc:
            raise TelegramError(0, f"jaringan: {type(exc).__name__}") from None
        if not payload.get("ok"):
            raise TelegramError(int(payload.get("error_code", 0) or 0), str(payload.get("description", "")),
                                float((payload.get("parameters") or {}).get("retry_after", 0) or 0))
        return payload

    def enqueue(self, job: dict[str, Any]) -> None:
        if not job.get("chat"):
            log("[tg] chat id belum diatur; pesan tidak dikirim: "
                + strip_tags(str(job.get("text") or job.get("caption") or ""))[:120])
            return
        try:
            self.q.put_nowait(job)
        except queue.Full:
            log("[tg] antrean penuh, pesan dibuang")

    def _worker(self) -> None:
        while True:
            job = self.q.get()
            try:
                self._deliver(job)
            except Exception as exc:
                log(f"[tg] error worker: {type(exc).__name__}: {exc}")

    def _pace(self) -> None:
        wait = self.min_gap - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        while True:
            now = time.monotonic()
            while self._sent and now - self._sent[0] > 60:
                self._sent.popleft()
            if len(self._sent) < self.max_per_min:
                break
            time.sleep(max(0.5, 60 - (now - self._sent[0]) + 0.2))
        self._last = time.monotonic()
        self._sent.append(self._last)

    def _deliver(self, job: dict[str, Any]) -> None:
        for attempt in range(5):
            self._pace()
            try:
                params: dict[str, Any] = {"chat_id": job["chat"]}
                if job.get("thread"):
                    params["message_thread_id"] = job["thread"]
                if job["kind"] == "text":
                    params["text"] = job["text"][:4096]
                    params["disable_web_page_preview"] = "true"
                    if job["html"]:
                        params["parse_mode"] = "HTML"
                    self.call("sendMessage", params)
                elif job["kind"] == "photo":
                    params["caption"] = job["caption"][:1024]
                    if job["html"]:
                        params["parse_mode"] = "HTML"
                    self.call("sendPhoto", params, file=("photo", "chart.png", job["data"], "image/png"), timeout=60)
                else:
                    params["caption"] = job.get("caption", "")
                    self.call("sendDocument", params,
                              file=("document", job["filename"], job["data"], "text/csv"), timeout=60)
                return
            except TelegramError as exc:
                desc = exc.desc.lower()
                if exc.code == 429:
                    time.sleep(min(exc.retry_after or 5.0, 60.0) + 1.0)
                    continue
                if job.get("html") and "parse entities" in desc:
                    job = dict(job)
                    job["html"] = False
                    for key in ("text", "caption"):
                        if key in job:
                            job[key] = strip_tags(job[key])
                    continue
                if job.get("thread") and ("thread" in desc or "topic" in desc):
                    log(f"[tg] topic {job['thread']} tidak valid ({exc.desc}); kirim ke General")
                    job = dict(job)
                    job["thread"] = None
                    continue
                if job["kind"] == "photo" and "caption is too long" in desc:
                    long_text = job["caption"]
                    job = dict(job)
                    job["caption"] = ""
                    try:
                        self.q.put_nowait({"kind": "text", "chat": job["chat"], "thread": job.get("thread"),
                                           "text": long_text, "html": job["html"]})
                    except queue.Full:
                        pass
                    continue
                if exc.code in (0, 500, 502, 503, 504):
                    time.sleep(2 ** attempt)
                    continue
                log(f"[tg] gagal kirim: {exc}")
                return
        log("[tg] gagal kirim setelah 5 percobaan")


class TelegramView:
    """Pandangan hub untuk satu engine: chat + topic default milik engine itu."""

    def __init__(self, hub: TelegramHub, chat_id: Optional[str], thread_id: Optional[int]):
        self.hub = hub
        self.chat_id = str(chat_id).strip() if chat_id else None
        self.thread_id = thread_id

    def _dest(self, chat: Any, thread: Optional[int]) -> tuple[Optional[str], Optional[int]]:
        if chat is not None:
            return str(chat), thread
        return self.chat_id, (thread if thread is not None else self.thread_id)

    def send_text(self, text: str, chat: Any = None, thread: Optional[int] = None, html_mode: bool = True) -> None:
        c, t = self._dest(chat, thread)
        self.hub.enqueue({"kind": "text", "chat": c, "thread": t, "text": text, "html": html_mode})

    def send_photo(self, png: bytes, caption: str, chat: Any = None, thread: Optional[int] = None) -> None:
        c, t = self._dest(chat, thread)
        self.hub.enqueue({"kind": "photo", "chat": c, "thread": t, "data": png, "caption": caption, "html": True})

    def send_document(self, data: bytes, filename: str, caption: str = "", chat: Any = None,
                      thread: Optional[int] = None) -> None:
        c, t = self._dest(chat, thread)
        self.hub.enqueue({"kind": "doc", "chat": c, "thread": t, "data": data, "filename": filename,
                          "caption": caption, "html": False})

    def send_signal(self, png: Optional[bytes], caption: str) -> None:
        if png:
            self.send_photo(png, caption)
        else:
            self.send_text(caption)


# --------------------------------------------------------------------------- chart generik
_FONTS: dict[int, Any] = {}


def _font(size: int) -> Any:
    if ImageFont is None:
        return None
    if size in _FONTS:
        return _FONTS[size]
    try:
        font = ImageFont.truetype("DejaVuSans.ttf", size)
    except Exception:
        try:
            font = ImageFont.load_default(size=size)
        except Exception:
            font = ImageFont.load_default()
    _FONTS[size] = font
    return font


def render_levels_chart(title: str, candles: list[Any], *, side: str, entry: float, sl: float,
                        targets: list[tuple[str, float]], zone: Optional[tuple[float, float]] = None,
                        lines: Optional[list[tuple[str, list[Optional[float]], tuple[int, int, int]]]] = None,
                        markers: Optional[list[tuple[int, str]]] = None, bars: int = 90,
                        digits: int = 2) -> Optional[bytes]:
    """Chart candlestick putih + garis Entry/SL/TP. `candles` bertipe .ts .open .high .low .close (ms).

    zone    : (low, high) area range (mis. range Shadow Fakeout)
    lines   : [(label, nilai_per_candle_sejajar_dgn_candles, warna)] mis. EMA21/EMA50
    markers : [(ts_ms_candle, label)] penanda vertikal (SWEEP/BOS/RETEST)
    """
    if Image is None or not candles:
        return None
    n_total = len(candles)
    start = max(0, n_total - bars)
    view = candles[start:]
    W, H = 1100, 640
    ml, mr, mt, mb = 20, 130, 60, 40
    img = Image.new("RGB", (W, H), (255, 255, 255))
    d = ImageDraw.Draw(img)
    levels = [entry, sl] + [p for _, p in targets] + ([zone[0], zone[1]] if zone else [])
    hi = max([c.high for c in view] + levels)
    lo = min([c.low for c in view] + levels)
    pad = (hi - lo) * 0.06 or 1.0
    hi += pad
    lo -= pad

    def y(price: float) -> float:
        return mt + (hi - price) / (hi - lo) * (H - mt - mb)

    cw = (W - ml - mr) / max(len(view), 1)

    def x(i: int) -> float:
        return ml + (i + 0.5) * cw

    # grid
    for k in range(6):
        price = lo + (hi - lo) * k / 5
        d.line([(ml, y(price)), (W - mr, y(price))], fill=(235, 235, 235), width=1)
        d.text((W - mr + 6, y(price) - 8), f"{price:.{digits}f}", fill=(120, 120, 120), font=_font(13))
    if zone:
        d.rectangle([(ml, y(zone[1])), (W - mr, y(zone[0]))], fill=(240, 244, 255))
        d.text((ml + 6, y(zone[1]) + 2), "RANGE/ZONA", fill=(90, 110, 190), font=_font(12))
    # lines (EMA dll.)
    for label, series, color in (lines or []):
        seg = series[start:start + len(view)]
        pts = [(x(i), y(v)) for i, v in enumerate(seg) if v is not None]
        if len(pts) > 1:
            d.line(pts, fill=color, width=2)
    # candles
    for i, c in enumerate(view):
        up = c.close >= c.open
        col = (22, 163, 74) if up else (220, 38, 38)
        d.line([(x(i), y(c.high)), (x(i), y(c.low))], fill=col, width=1)
        top, bot = y(max(c.open, c.close)), y(min(c.open, c.close))
        if bot - top < 1:
            bot = top + 1
        d.rectangle([(x(i) - cw * 0.34, top), (x(i) + cw * 0.34, bot)], fill=col)
    # markers
    ts_index = {c.ts: i for i, c in enumerate(view)}
    for ts, label in (markers or []):
        i = ts_index.get(ts)
        if i is None:
            continue
        d.line([(x(i), mt), (x(i), H - mb)], fill=(180, 180, 200), width=1)
        d.text((x(i) - 18, mt - 18), label, fill=(80, 80, 140), font=_font(12))

    def hline(price: float, color: tuple[int, int, int], label: str) -> None:
        yy = y(price)
        for xx in range(ml, W - mr, 10):
            d.line([(xx, yy), (xx + 5, yy)], fill=color, width=2)
        text = f"{label} {price:.{digits}f}"
        tw = d.textlength(text, font=_font(13))
        d.rectangle([(W - mr + 2, yy - 9), (W - mr + 8 + tw, yy + 9)], fill=color)
        d.text((W - mr + 5, yy - 8), text, fill=(255, 255, 255), font=_font(13))

    hline(entry, (37, 99, 235), "ENTRY")
    hline(sl, (220, 38, 38), "SL")
    for label, price in targets:
        hline(price, (22, 163, 74), label)
    # header + footer
    up = side.upper() in {"BUY", "L"}
    d.rectangle([(0, 0), (W, 40)], fill=(22, 163, 74) if up else (220, 38, 38))
    d.text((14, 9), title, fill=(255, 255, 255), font=_font(20))
    t0 = datetime.fromtimestamp(view[0].ts / 1000, WIB).strftime("%d-%m %H:%M")
    t1 = datetime.fromtimestamp(view[-1].ts / 1000, WIB).strftime("%d-%m %H:%M")
    d.text((ml, H - 28), f"{t0} WIB", fill=(120, 120, 120), font=_font(13))
    d.text((W - mr - 130, H - 28), f"{t1} WIB", fill=(120, 120, 120), font=_font(13))
    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


# --------------------------------------------------------------------------- sesi pasar
def _nth_sunday(year: int, month: int, n: int) -> datetime:
    d = datetime(year, month, 1, tzinfo=timezone.utc)
    return d.replace(day=1 + (6 - d.weekday()) % 7 + 7 * (n - 1))


def ny_minutes(ts_sec: float) -> int:
    """Menit sejak 00:00 waktu New York (memperhitungkan DST AS)."""
    dt = datetime.fromtimestamp(ts_sec, timezone.utc)
    start = _nth_sunday(dt.year, 3, 2).replace(hour=7)
    end = _nth_sunday(dt.year, 11, 1).replace(hour=6)
    ny = dt + timedelta(hours=-4 if start <= dt < end else -5)
    return ny.hour * 60 + ny.minute


def session_for(ts_sec: float) -> str:
    m = ny_minutes(ts_sec)
    if 570 <= m <= 960:
        return "NEW YORK"
    if m >= 1200 or m <= 120:
        return "ASIA"
    if 180 <= m <= 690:
        return "LONDON"
    return "DEAD ZONE"


def ny_datetime(ts_sec: float) -> datetime:
    """Waktu New York (DST AS dihitung manual; tidak butuh tzdata di server)."""
    dt = datetime.fromtimestamp(ts_sec, timezone.utc)
    start = _nth_sunday(dt.year, 3, 2).replace(hour=7)
    end = _nth_sunday(dt.year, 11, 1).replace(hour=6)
    return (dt + timedelta(hours=-4 if start <= dt < end else -5)).replace(tzinfo=None)
''',
    "data": r'''
# -*- coding: utf-8 -*-
"""Feed candle bersama untuk engine SFP & TPB: Bybit linear (utama) + Yahoo (cadangan).

Hanya candle yang SUDAH CLOSE yang dikembalikan. Ada cache singkat dan cooldown per sumber
supaya dua engine yang membaca pair yang sama tidak menggandakan request.
"""
from __future__ import annotations

import json
import math
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional

from .common import UA, env_str, log


@dataclass(frozen=True)
class Candle:
    ts: int          # UTC ms, waktu OPEN candle
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


TF_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000}
BYBIT_INTERVAL = {"1m": "1", "5m": "5", "15m": "15"}
YAHOO_INTERVAL = {"1m": ("1m", "5d"), "5m": ("5m", "5d"), "15m": ("15m", "5d")}

BYBIT_HOSTS = [h.strip().rstrip("/") for h in
               env_str("BYBIT_HOSTS", "https://api.bybit.com,https://api.bytick.com,https://api.bybit.id").split(",")
               if h.strip()]

# Nama pair internal engine -> sumber. Bisa di-override lewat env.
SOURCES: dict[str, dict[str, object]] = {
    "XAUUSD": {
        "bybit": [s.strip().upper() for s in env_str("XAU_BYBIT_SYMBOLS", "XAUUSDT,XAUTUSDT").split(",") if s.strip()],
        "yahoo": env_str("XAU_YAHOO_SYMBOL", "GC=F"),
        "label": "XAUUSD",
    },
    "BTCUSD": {
        "bybit": [env_str("BTC_BYBIT_SYMBOL", "BTCUSDT").upper()],
        "yahoo": env_str("BTC_YAHOO_SYMBOL", "BTC-USD"),
        "label": "BTCUSD",
    },
}

_CACHE: dict[tuple[str, str], tuple[float, list[Candle], str]] = {}
_RESOLVED: dict[str, str] = {}
_BLOCKED_UNTIL: dict[str, float] = {}
_LOCK = threading.Lock()
_HOST_OK = [0]


def _http_json(url: str, timeout: int = 20) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            raise RuntimeError("HTTP 429") from None
        raise RuntimeError(f"HTTP {exc.code}") from None


def clean(candles: list[Candle], tf: str, now_ms: Optional[int] = None) -> list[Candle]:
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    span = TF_MS[tf]
    by_ts: dict[int, Candle] = {}
    for c in candles:
        vals = (c.open, c.high, c.low, c.close)
        if not all(math.isfinite(v) and v > 0 for v in vals):
            continue
        if c.high < max(c.open, c.close, c.low) or c.low > min(c.open, c.close, c.high):
            continue
        if c.ts + span > now_ms:      # candle belum close
            continue
        by_ts[c.ts] = c
    return [by_ts[k] for k in sorted(by_ts)]


def _bybit(symbol: str, tf: str, limit: int) -> list[Candle]:
    params = urllib.parse.urlencode({"category": "linear", "symbol": symbol,
                                     "interval": BYBIT_INTERVAL[tf], "limit": max(50, min(int(limit), 1000))})
    last: Optional[Exception] = None
    for k in range(len(BYBIT_HOSTS)):
        idx = (_HOST_OK[0] + k) % len(BYBIT_HOSTS)
        try:
            payload = _http_json(f"{BYBIT_HOSTS[idx]}/v5/market/kline?{params}")
            if payload.get("retCode") != 0:
                raise RuntimeError(f"Bybit retCode={payload.get('retCode')} {payload.get('retMsg', '')}")
            rows = (payload.get("result") or {}).get("list") or []
            out = []
            for r in rows:
                try:
                    out.append(Candle(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
                except (IndexError, TypeError, ValueError):
                    continue
            _HOST_OK[0] = idx
            return out
        except Exception as exc:
            last = exc
    raise RuntimeError(f"Bybit {symbol}: {last}")


def _yahoo(symbol: str, tf: str) -> list[Candle]:
    interval, rng = YAHOO_INTERVAL[tf]
    path = urllib.parse.quote(symbol, safe="=-^.")
    last: Optional[Exception] = None
    for host in ("query1", "query2"):
        try:
            payload = _http_json(f"https://{host}.finance.yahoo.com/v8/finance/chart/{path}"
                                 f"?interval={interval}&range={rng}&includePrePost=true", timeout=25)
            res = ((payload.get("chart") or {}).get("result") or [None])[0]
            if not res:
                raise RuntimeError("tidak ada hasil")
            q = (res.get("indicators") or {}).get("quote", [{}])[0]
            out = []
            for i, t in enumerate(res.get("timestamp") or []):
                try:
                    o, h, l, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
                    if None in (o, h, l, c):
                        continue
                    v = (q.get("volume") or [0] * (i + 1))[i] or 0.0
                    out.append(Candle(int(t) * 1000, float(o), float(h), float(l), float(c), float(v)))
                except (KeyError, IndexError, TypeError, ValueError):
                    continue
            return out
        except Exception as exc:
            last = exc
    raise RuntimeError(f"Yahoo {symbol}: {last}")


def fetch_candles(pair: str, tf: str, limit: int = 300, ttl: float = 12.0) -> tuple[list[Candle], str]:
    """Return (candle closed, nama sumber). Raise RuntimeError bila semua sumber gagal."""
    if pair not in SOURCES:
        raise ValueError(f"Pair tidak didukung engine: {pair}")
    if tf not in TF_MS:
        raise ValueError(f"Timeframe tidak didukung: {tf}")
    key = (pair, tf)
    now = time.time()
    with _LOCK:
        hit = _CACHE.get(key)
        if hit and now - hit[0] < ttl and len(hit[1]) >= min(limit, 80):
            return list(hit[1]), hit[2]
    src = SOURCES[pair]
    errors: list[str] = []
    symbols = list(src["bybit"])  # type: ignore[arg-type]
    if pair in _RESOLVED and _RESOLVED[pair] in symbols:
        symbols.remove(_RESOLVED[pair])
        symbols.insert(0, _RESOLVED[pair])
    if now >= _BLOCKED_UNTIL.get("bybit", 0.0):
        for sym in symbols:
            try:
                rows = clean(_bybit(sym, tf, limit), tf)
                if len(rows) >= 60:
                    _RESOLVED[pair] = sym
                    source = f"Bybit {sym} (linear)"
                    with _LOCK:
                        _CACHE[key] = (time.time(), rows, source)
                    return list(rows), source
                errors.append(f"Bybit {sym}: hanya {len(rows)} candle")
            except Exception as exc:
                errors.append(str(exc))
                if "429" in str(exc):
                    _BLOCKED_UNTIL["bybit"] = time.time() + 60
                    break
    else:
        errors.append("Bybit cooldown")
    ysym = str(src["yahoo"])
    if now >= _BLOCKED_UNTIL.get("yahoo:" + ysym, 0.0):
        try:
            rows = clean(_yahoo(ysym, tf), tf)
            if len(rows) >= 60:
                source = f"Yahoo {ysym}"
                with _LOCK:
                    _CACHE[key] = (time.time(), rows, source)
                return list(rows), source
            errors.append(f"Yahoo {ysym}: hanya {len(rows)} candle")
        except Exception as exc:
            errors.append(str(exc))
            if "429" in str(exc):
                _BLOCKED_UNTIL["yahoo:" + ysym] = time.time() + 300
    else:
        errors.append(f"Yahoo {ysym} cooldown")
    log(f"[data] {pair} {tf} gagal: " + " | ".join(errors))
    raise RuntimeError(f"Data {pair} {tf} gagal: " + " | ".join(errors))


def fetch_history(pair: str, tf: str, days: int) -> tuple[list[Candle], str]:
    """Riwayat candle closed (untuk backtest). Bybit dipaginasi mundur; Yahoo maks ~5 hari."""
    days = max(1, int(days))
    span = TF_MS[tf]
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 86_400_000
    src = SOURCES[pair]
    errors: list[str] = []
    for sym in list(src["bybit"]):  # type: ignore[arg-type]
        try:
            end = now_ms
            allc: list[Candle] = []
            seen_min: Optional[int] = None
            for _ in range(math.ceil(days * 86_400_000 / span / 950) + 4):
                params = urllib.parse.urlencode({"category": "linear", "symbol": sym, "interval": BYBIT_INTERVAL[tf],
                                                 "start": start_ms, "end": end, "limit": 1000})
                payload = None
                for k in range(len(BYBIT_HOSTS)):
                    try:
                        payload = _http_json(f"{BYBIT_HOSTS[(_HOST_OK[0] + k) % len(BYBIT_HOSTS)]}/v5/market/kline?{params}")
                        _HOST_OK[0] = (_HOST_OK[0] + k) % len(BYBIT_HOSTS)
                        break
                    except Exception as exc:
                        errors.append(str(exc))
                if not payload or payload.get("retCode") != 0:
                    break
                rows = (payload.get("result") or {}).get("list") or []
                if not rows:
                    break
                page = []
                for r in rows:
                    try:
                        page.append(Candle(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
                    except (IndexError, TypeError, ValueError):
                        continue
                if not page:
                    break
                allc.extend(page)
                page_min = min(c.ts for c in page)
                if seen_min is not None and page_min >= seen_min:
                    break
                seen_min = page_min
                if page_min <= start_ms or len(page) < 2:
                    break
                end = page_min - 1
                time.sleep(0.1)
            rows_c = [c for c in clean(allc, tf) if c.ts >= start_ms]
            if len(rows_c) >= 100:
                return rows_c, f"Bybit {sym} (linear)"
        except Exception as exc:
            errors.append(str(exc))
    ysym = str(src["yahoo"])
    try:
        rows_c = clean(_yahoo(ysym, tf), tf)
        if len(rows_c) >= 100:
            return rows_c, f"Yahoo {ysym} (maks ~5 hari)"
    except Exception as exc:
        errors.append(str(exc))
    raise RuntimeError(f"Riwayat {pair} {tf} gagal: " + " | ".join(errors[-4:]))
''',
    "sfp": r'''
# -*- coding: utf-8 -*-
"""ENGINE 1 — SHADOW FAKEOUT PDF 1M (port dari bot1.py: material_entry_at / make_material_signal).

Urutan setup (ketat, tanpa look-ahead, candle closed saja):
  1. bangun range sebelumnya,
  2. liquidity sweep dengan shadow panjang di luar range (fakeout),
  3. candle BODY kembali masuk range (breakbody / BOS),
  4. entry hanya bila candle berikutnya retest zona yang di-reclaim dan bertahan.
SL = di luar sweep + N pip (referensi PDF 50 pip), TP = sisi lawan range (min RR 1.5).

Independen: state sfp_state.json sendiri, Telegram topic SFP_TOPIC_ID sendiri.
Hanya sinyal + pelacak hasil (TP / TP_BE / SL); tidak ada order nyata.
"""
from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from . import data as mdata
from .common import (WIB, TelegramView, env_bool, env_float, env_int, env_str, esc, load_json, log,
                     ny_datetime, render_levels_chart, save_json, session_for, state_path)
from .data import Candle

PAIRS = [p.strip().upper() for p in env_str("SFP_PAIRS", "XAUUSD,BTCUSD").split(",") if p.strip() in mdata.SOURCES] \
    or ["XAUUSD", "BTCUSD"]
DISPLAY = {"XAUUSD": "SPOT Xauusd", "BTCUSD": "BTCUSD"}
SCAN_SEC = max(10, env_int("SFP_SCAN_SEC", 15))
COOLDOWN_MIN = max(1, env_int("SFP_COOLDOWN_MIN", 30))
REJECTION_COOLDOWN_MIN = max(1, env_int("SFP_REJECTION_COOLDOWN_MIN", 30))
BE_TRIGGER_R = max(0.25, env_float("SFP_BE_TRIGGER_R", 1.0))
SLPLUS_TRIGGER_R = max(BE_TRIGGER_R, env_float("SFP_SLPLUS_TRIGGER_R", 1.20))
SLPLUS_LOCK_R = max(0.0, env_float("SFP_SLPLUS_LOCK_R", 0.20))
SLPLUS_GAP_R = max(0.05, env_float("SFP_SLPLUS_GAP_R", 0.50))
SLPLUS_NOTIFY_R = max(0.05, env_float("SFP_SLPLUS_NOTIFY_R", 0.20))
ALLOW_XAU_24_7 = env_bool("SFP_ALLOW_XAU_24_7", False)
XAU_OFFSET = env_float("SFP_XAU_OFFSET", 0.0)          # offset futures->spot (hanya untuk sumber Yahoo)
MAX_STALE_MIN = max(5.0, env_float("SFP_MAX_STALE_MIN", 90.0))
EXPIRE_HOURS = max(1.0, env_float("SFP_EXPIRE_HOURS", 24.0))
BACKTEST_COST_R = max(0.0, env_float("SFP_BACKTEST_COST_R", 0.0))
STATE_FILE = state_path("sfp_state.json")


# ------------------------------------------------------------------ utilitas pasar
def xau_market_status(ts_sec: Optional[float] = None) -> tuple[bool, str]:
    """Jam emas tradisional (NY): tutup Jumat 17:00 - Minggu 18:00, jeda harian 17:00-18:00, libur utama."""
    cur = ny_datetime(ts_sec if ts_sec is not None else time.time())
    d = cur.date()
    easter = _easter(d.year)
    holidays = {datetime(d.year, 1, 1).date(), datetime(d.year, 12, 25).date(),
                (datetime(easter.year, easter.month, easter.day) - _days(2)).date()}
    if d in holidays:
        return False, f"libur pasar emas ({d.isoformat()})"
    for raw in env_str("XAU_CLOSED_DATES").split(","):
        if raw.strip() == d.isoformat():
            return False, f"tanggal ditutup via XAU_CLOSED_DATES ({d.isoformat()})"
    wd, minute = cur.weekday(), cur.hour * 60 + cur.minute
    if wd == 5:
        return False, "akhir pekan (Sabtu)"
    if wd == 6 and minute < 18 * 60:
        return False, "akhir pekan (Minggu sebelum buka 18:00 New York)"
    if wd == 4 and minute >= 17 * 60:
        return False, "akhir pekan (Jumat setelah tutup 17:00 New York)"
    if wd in {0, 1, 2, 3, 4} and 17 * 60 <= minute < 18 * 60:
        return False, "jeda harian pasar emas (17:00-18:00 New York)"
    return True, "pasar emas tradisional buka"


def _days(n: int):
    from datetime import timedelta
    return timedelta(days=n)


def _easter(year: int):
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return datetime(year, month, day)


def fmt_price(pair: str, value: float) -> str:
    return f"{value:,.2f}"


def fmt_wib(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, WIB).strftime("%d-%m-%Y %H:%M") + " WIB"


def _ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    alpha = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * alpha + out[-1] * (1 - alpha))
    return out


def _atr(candles: list[Candle], length: int = 14) -> list[float]:
    trs = []
    for i, c in enumerate(candles):
        pc = candles[i - 1].close if i else c.close
        trs.append(max(c.high - c.low, abs(c.high - pc), abs(c.low - pc)))
    return _ema(trs, max(1, length))


def _range(c: Candle) -> float:
    return max(c.high - c.low, 1e-9)


# ------------------------------------------------------------------ logika sinyal (port persis)
def material_settings(pair: str) -> dict[str, float | int]:
    """Parameter model PDF. 50 pip di PDF hanya contoh; pip size dibuat configurable per instrumen."""
    pip_size = env_float("SFP_PIP_XAU", 0.01) if pair == "XAUUSD" else env_float("SFP_PIP_BTC", 1.0)
    return {
        "range_lookback": max(10, env_int("SFP_RANGE_LOOKBACK", 24)),
        "setup_lookback": max(4, env_int("SFP_SETUP_LOOKBACK", 10)),
        "shadow_ratio": max(1.0, env_float("SFP_SHADOW_RATIO", 1.2)),
        "min_shadow_range": max(0.15, env_float("SFP_MIN_SHADOW_RANGE", 0.35)),
        "sl_pips": max(1.0, env_float("SFP_SL_PIPS", 50.0)),
        "pip_size": max(1e-9, pip_size),
        "min_rr": max(1.0, env_float("SFP_MIN_RR", 1.5)),
        "max_hold_bars": max(10, env_int("SFP_MAX_HOLD_BARS", 120)),
        "cost_r": BACKTEST_COST_R,
    }


def _shadow(c: Candle) -> tuple[float, float, float, float]:
    body = abs(c.close - c.open)
    lower = max(0.0, min(c.open, c.close) - c.low)
    upper = max(0.0, c.high - max(c.open, c.close))
    return body, lower, upper, _range(c)


def material_entry_at(candles: list[Candle], index: int, pair: str,
                      settings: Optional[dict[str, float | int]] = None) -> Optional[dict[str, Any]]:
    settings = settings or material_settings(pair)
    range_lookback = int(settings["range_lookback"])
    setup_lookback = int(settings["setup_lookback"])
    if index < range_lookback + 3 or index >= len(candles):
        return None
    pip_buffer = float(settings["sl_pips"]) * float(settings["pip_size"])
    candidate_start = max(range_lookback, index - setup_lookback)
    candidates: list[dict[str, Any]] = []
    for sweep_index in range(candidate_start, index - 1):
        prior = candles[sweep_index - range_lookback:sweep_index]
        if len(prior) < range_lookback:
            continue
        range_low = min(x.low for x in prior)
        range_high = max(x.high for x in prior)
        range_size = max(range_high - range_low, pip_buffer * 2, 1e-9)
        sweep = candles[sweep_index]
        body, lower_shadow, upper_shadow, full_range = _shadow(sweep)
        body_reference = max(body, range_size * 0.01, 1e-9)
        ratio = float(settings["shadow_ratio"])
        min_shadow_range = float(settings["min_shadow_range"])
        is_buy = (sweep.low < range_low and sweep.close > range_low
                  and lower_shadow >= body_reference * ratio
                  and lower_shadow / max(full_range, 1e-9) >= min_shadow_range)
        is_sell = (sweep.high > range_high and sweep.close < range_high
                   and upper_shadow >= body_reference * ratio
                   and upper_shadow / max(full_range, 1e-9) >= min_shadow_range)
        if not is_buy and not is_sell:
            continue
        side = "BUY" if is_buy else "SELL"
        reclaim = range_low if side == "BUY" else range_high
        breakout_index: Optional[int] = None
        for k in range(sweep_index + 1, index):
            p = candles[k]
            prev_close = candles[k - 1].close
            if side == "BUY":
                ok = p.close > reclaim and (p.open <= reclaim or prev_close <= reclaim)
            else:
                ok = p.close < reclaim and (p.open >= reclaim or prev_close >= reclaim)
            if ok:
                breakout_index = k
                break
        if breakout_index is None:
            continue
        retest = candles[index]
        tol = max(range_size * 0.08, pip_buffer * 0.5)
        if side == "BUY":
            holds = (retest.low <= reclaim + tol and retest.close > reclaim
                     and retest.close >= retest.open and retest.low > sweep.low)
            stop_loss = sweep.low - pip_buffer
            take_profit = range_high - pip_buffer
        else:
            holds = (retest.high >= reclaim - tol and retest.close < reclaim
                     and retest.close <= retest.open and retest.high < sweep.high)
            stop_loss = sweep.high + pip_buffer
            take_profit = range_low + pip_buffer
        if not holds:
            continue
        entry = retest.close
        risk = abs(entry - stop_loss)
        reward = abs(take_profit - entry)
        rr = reward / max(risk, 1e-9)
        if risk <= 0 or reward <= 0 or rr < float(settings["min_rr"]):
            continue
        candidates.append({"side": side, "entry": entry, "sl": stop_loss, "tp": take_profit, "risk": risk, "rr": rr,
                           "range_low": range_low, "range_high": range_high, "sweep_index": sweep_index,
                           "breakout_index": breakout_index, "ts": retest.ts})
    return candidates[-1] if candidates else None


@dataclass
class SfpSignal:
    pair: str
    side: str
    entry: float
    sl: float
    tp: float
    ts: int                      # open-ts candle retest (ms)
    rr: float
    range_low: float
    range_high: float
    sweep_ts: int
    bos_ts: int
    session: str
    sl_pips: float
    pip_size: float
    offset_applied: float = 0.0
    reasons: list[str] = field(default_factory=list)

    @property
    def risk(self) -> float:
        return abs(self.entry - self.sl)


def make_signal(pair: str, candles: list[Candle]) -> Optional[SfpSignal]:
    settings = material_settings(pair)
    minimum = int(settings["range_lookback"]) + int(settings["setup_lookback"]) + 3
    if len(candles) < minimum:
        return None
    e = material_entry_at(candles, len(candles) - 1, pair, settings)
    if e is None:
        return None
    pips = float(settings["sl_pips"])
    pip_dist = pips * float(settings["pip_size"])
    return SfpSignal(
        pair=pair, side=str(e["side"]), entry=float(e["entry"]), sl=float(e["sl"]), tp=float(e["tp"]),
        ts=int(e["ts"]), rr=float(e["rr"]), range_low=float(e["range_low"]), range_high=float(e["range_high"]),
        sweep_ts=candles[int(e["sweep_index"])].ts, bos_ts=candles[int(e["breakout_index"])].ts,
        session=session_for(int(e["ts"]) / 1000), sl_pips=pips, pip_size=float(settings["pip_size"]),
        reasons=["liquidity sweep dengan shadow panjang", "breakbody/BOS terkonfirmasi", "retest zona bertahan",
                 f"SL di luar invalidasi + {pip_dist:g} ({pips:g} pip)", "TP menuju zona lawan"])


def signal_caption(sg: SfpSignal, source: str) -> str:
    up = sg.side == "BUY"
    p = lambda v: fmt_price(sg.pair, v)  # noqa: E731
    bar = "━━━━━━━━━━━━━━━━━━━━"
    rows = [
        bar,
        f"{'🟢' if up else '🔴'} <b>PDF Shadow Fakeout + Pattern/BOS + Retest | {sg.side}</b>",
        f"{DISPLAY.get(sg.pair, sg.pair)} • TF 1m",
        bar,
        f"Entry    : <code>{p(sg.entry)}</code>",
        f"Target   : <code>{p(sg.tp)}</code> (+{abs(sg.tp - sg.entry):.2f} · {sg.rr:.2f}R)",
        f"SL       : <code>{p(sg.sl)}</code> (-{sg.risk:.2f})",
        bar,
        f"Session  : {sg.session}",
        f"Candle   : {fmt_wib(sg.ts + 60_000)} (close)",
        f"SNR      : Demand {p(sg.range_low)} | Supply {p(sg.range_high)}",
        "Pattern  : Shadow Fakeout, Liquidity Sweep, Breakbody/BOS, Retest",
        "Dasar    : zona lawan setelah sweep + breakbody + retest",
        f"Alasan   : {esc('; '.join(sg.reasons))}",
        "Konfirmasi:",
        f"ZONE: {p(sg.range_low)} - {p(sg.range_high)}",
        f"SWEEP: {fmt_wib(sg.sweep_ts)}",
        f"BREAKBODY: {fmt_wib(sg.bos_ts)}",
        f"RR: {sg.rr:.2f}R",
        f"SL_RULE: invalidasi + {sg.sl_pips:g} pip",
        "TP_RULE: zona lawan",
        f"BE: SL ke entry setelah +{BE_TRIGGER_R:g}R (kena BE dicatat TP_BE)",
        f"SL+: trailing mulai +{SLPLUS_TRIGGER_R:g}R; kunci minimal +{SLPLUS_LOCK_R:g}R; gap {SLPLUS_GAP_R:g}R",
        bar,
        f"Sumber {esc(source)}. Analisis, bukan jaminan profit. Cek spread & berita sebelum entry.",
    ]
    return "\n".join(rows)


def rejection_warning(item: dict[str, Any], candles: list[Candle]) -> Optional[tuple[str, str]]:
    """Peringatan dini dari rejection candle 1m saat posisi sedang profit (port dari bot1)."""
    if str(item.get("status", "OPEN")) != "OPEN" or len(candles) < 6:
        return None
    closed = candles
    candle, previous = closed[-1], closed[-2]
    entry, side = float(item["entry"]), str(item["side"])
    target = float(item.get("tp", 0.0) or 0.0)
    if target > 0 and any((b.high >= target if side == "BUY" else b.low <= target)
                          for b in closed if b.ts > int(item["ts"])):
        return None
    in_profit = candle.close > entry if side == "BUY" else candle.close < entry
    if not in_profit:
        return None
    body = abs(candle.close - candle.open)
    size = _range(candle)
    atr_value = max(_atr(closed, 14)[-1], entry * 0.0001)
    recent_high = max(b.high for b in closed[-6:-1])
    recent_low = min(b.low for b in closed[-6:-1])
    if side == "BUY":
        wick = candle.high - max(candle.open, candle.close)
        bearish_body = candle.close < candle.open and candle.close < previous.close
        swept = candle.high > recent_high and candle.close < recent_high
        rejected = (size >= atr_value * 0.45 and wick >= max(body * 1.15, size * 0.25)
                    and (bearish_body or swept) and candle.close < candle.high - size * 0.35)
        protected = entry + max(entry * 0.00005, min(candle.close - entry, atr_value * 0.2))
        label = "seller rejection setelah buyer push"
    else:
        wick = min(candle.open, candle.close) - candle.low
        bullish_body = candle.close > candle.open and candle.close > previous.close
        swept = candle.low < recent_low and candle.close > recent_low
        rejected = (size >= atr_value * 0.45 and wick >= max(body * 1.15, size * 0.25)
                    and (bullish_body or swept) and candle.close > candle.low + size * 0.35)
        protected = entry - max(entry * 0.00005, min(entry - candle.close, atr_value * 0.2))
        label = "buyer rejection setelah seller push"
    if not rejected:
        return None
    severity = "PARAH" if size >= atr_value * 1.25 else "AWAL"
    pair = str(item["pair"])
    text = (f"⚠️ <b>PERINGATAN REJECTION 1M {severity}</b> | {esc(pair)} 1m\n"
            f"{esc(label.capitalize())} di {fmt_price(pair, candle.close)}.\n"
            f"Segera take profit/close sebagian dan lindungi profit dengan SL minimal "
            f"{fmt_price(pair, protected)}.\n"
            "Peringatan memakai candle 1m agar tidak terlambat. Bot hanya memberi peringatan; tidak ada eksekusi order.")
    return f"{pair}:1m:{severity}", text


# ------------------------------------------------------------------ backtest
def backtest(pair: str, candles: list[Candle]) -> dict[str, Any]:
    """Backtest SFP with the same conservative BE/SL+ ratchet used by the paper tracker.

    The stop active before a candle is checked first. Any stop update derived from that
    candle's high/low only applies from the next candle, avoiding same-bar look-ahead.
    """
    settings = material_settings(pair)
    warmup = int(settings["range_lookback"]) + int(settings["setup_lookback"]) + 3
    max_hold = int(settings["max_hold_bars"])
    cost_r = float(settings["cost_r"])
    total = wins = losses = timeouts = sl_plus = be = 0
    net_r = 0.0
    active: Optional[dict[str, Any]] = None
    rows: list[dict[str, Any]] = []

    for i in range(warmup, len(candles)):
        c = candles[i]
        if active is not None:
            side = active["side"]
            entry = float(active["entry"])
            risk = max(float(active["risk"]), 1e-12)
            stop = float(active.get("active_sl", active["sl"]))
            tp = float(active["tp"])
            # Conservative order: current stop wins when SL/TP touch in the same candle.
            stop_hit = c.low <= stop if side == "BUY" else c.high >= stop
            tp_hit = c.high >= tp if side == "BUY" else c.low <= tp
            raw_r = 0.0
            outcome = ""
            if stop_hit:
                raw_r = (stop - entry) / risk if side == "BUY" else (entry - stop) / risk
                stage = active.get("sl_stage", "INIT")
                outcome = "SL_PLUS" if stage == "SL+" or raw_r > 1e-8 else ("TP_BE" if stage == "BE" or raw_r >= -1e-8 else "SL")
            elif tp_hit:
                raw_r = active["rr"]
                outcome = "TP"
            elif i - active["index"] >= max_hold:
                raw_r = (c.close - entry) / risk if side == "BUY" else (entry - c.close) / risk
                raw_r = max(-1.0, raw_r)
                outcome = "TIMEOUT"

            if outcome:
                result_r = raw_r - cost_r
                net_r += result_r
                rows.append({"outcome": outcome, "r": result_r})
                if outcome == "TP":
                    wins += 1
                elif outcome == "SL_PLUS":
                    sl_plus += 1
                    if result_r > 0: wins += 1
                    elif result_r < 0: losses += 1
                elif outcome == "TP_BE":
                    be += 1
                    if result_r > 0: wins += 1
                    elif result_r < 0: losses += 1
                elif outcome == "SL":
                    losses += 1
                else:
                    timeouts += 1
                    if result_r > 0: wins += 1
                    elif result_r < 0: losses += 1
                active = None
                continue

            # Update favorable extreme and ratchet only forward for the next candle.
            if side == "BUY":
                active["best_price"] = max(float(active.get("best_price", entry)), c.high)
                favorable = (float(active["best_price"]) - entry) / risk
            else:
                active["best_price"] = min(float(active.get("best_price", entry)), c.low, entry)
                favorable = (entry - float(active["best_price"])) / risk
            current = float(active.get("active_sl", active["sl"]))
            stage = active.get("sl_stage", "INIT")
            if favorable >= BE_TRIGGER_R and stage == "INIT":
                current = entry
                stage = "BE"
            if favorable >= SLPLUS_TRIGGER_R:
                if side == "BUY":
                    trail = float(active["best_price"]) - SLPLUS_GAP_R * risk
                    floor = entry + SLPLUS_LOCK_R * risk
                    candidate = max(floor, trail)
                    current = max(current, candidate)
                else:
                    trail = float(active["best_price"]) + SLPLUS_GAP_R * risk
                    ceiling = entry - SLPLUS_LOCK_R * risk
                    candidate = min(ceiling, trail)
                    current = min(current, candidate)
                stage = "SL+"
            active["active_sl"] = current
            active["sl_stage"] = stage
            continue

        e = material_entry_at(candles, i, pair, settings)
        if e is not None:
            total += 1
            active = {**e, "index": i, "active_sl": float(e["sl"]), "sl_stage": "INIT",
                      "best_price": float(e["entry"])}

    decided = wins + losses
    gp = sum(max(0.0, r["r"]) for r in rows)
    gl = abs(sum(min(0.0, r["r"]) for r in rows))
    return {"pair": pair, "total": total, "wins": wins, "losses": losses, "timeouts": timeouts,
            "sl_plus": sl_plus, "be": be, "open": 1 if active else 0,
            "win_rate": wins / decided * 100 if decided else 0.0,
            "hit_rate": wins / total * 100 if total else 0.0, "net_r": net_r,
            "pf": gp / gl if gl else (float("inf") if gp else 0.0)}


# ------------------------------------------------------------------ engine
class ShadowFakeoutPdfEngine:
    key = "sfp"
    title = "SHADOW FAKEOUT PDF 1M"

    def __init__(self, tg: TelegramView):
        self.tg = tg
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.state: dict[str, Any] = load_json(STATE_FILE, {})
        for k, v in (("last_signal", {}), ("last_sent_at", {}), ("alert_history", {}), ("trades", []),
                     ("counts", {"TP": 0, "TP_BE": 0, "SL_PLUS": 0, "SL": 0, "EXPIRED": 0})):
            self.state.setdefault(k, v)
        self.last_error: dict[str, str] = {}
        self.last_scan: dict[str, str] = {}
        self.thread: Optional[threading.Thread] = None

    def save(self) -> None:
        save_json(STATE_FILE, self.state)

    # ---- data
    def candles(self, pair: str, limit: int = 600) -> tuple[list[Candle], str]:
        rows, source = mdata.fetch_candles(pair, "1m", limit=limit)
        if pair == "XAUUSD" and XAU_OFFSET and source.startswith("Yahoo"):
            rows = [Candle(c.ts, c.open + XAU_OFFSET, c.high + XAU_OFFSET, c.low + XAU_OFFSET,
                           c.close + XAU_OFFSET, c.volume) for c in rows]
        return rows, source

    def scannable(self, pair: str, rows: list[Candle]) -> tuple[bool, str]:
        if pair == "XAUUSD" and not ALLOW_XAU_24_7:
            ok, reason = xau_market_status()
            if not ok:
                return False, f"XAUUSD tidak discan: {reason}"
        if not rows:
            return False, "data kosong"
        age_min = (time.time() * 1000 - rows[-1].ts) / 60_000
        if age_min > MAX_STALE_MIN:
            return False, f"candle terakhir stale {age_min:.0f} menit"
        return True, "ok"

    # ---- pelacakan posisi (BE / TP / SL)
    def settle_open(self, pair: str, rows: list[Candle]) -> list[str]:
        """Update SFP paper positions using closed candles only.

        The stop already active before a candle is tested first. BE / SL+ updates
        derived from that candle take effect on the following candle, so a future
        stop is never used to claim a fill earlier in the same candle.
        """
        messages: list[str] = []
        changed = False
        for item in self.state["trades"]:
            if item.get("status") != "OPEN" or item.get("pair") != pair:
                continue
            entry = float(item["entry"])
            initial_sl = float(item.get("initial_sl", item.get("sl", entry)))
            tp = float(item["tp"])
            risk = abs(entry - initial_sl)
            side = str(item["side"])
            if risk <= 0:
                continue
            # Migrate open positions made by previous versions.
            item.setdefault("initial_sl", initial_sl)
            legacy_stage = "BE" if item.get("be_armed") else "INIT"
            item.setdefault("sl_stage", legacy_stage)
            item.setdefault("active_sl", float(item.get("be_price", entry)) if item.get("be_armed") else initial_sl)
            item.setdefault("best_price", entry)
            item.setdefault("last_notified_lock_r", 0.0)
            cursor = int(item.get("last_ts", item["ts"]))

            for c in rows:
                if c.ts <= cursor:
                    continue
                item["last_ts"] = c.ts
                changed = True
                active_sl = float(item.get("active_sl", initial_sl))
                stage = str(item.get("sl_stage", "INIT"))
                hit_stop = c.low <= active_sl if side == "BUY" else c.high >= active_sl
                hit_tp = c.high >= tp if side == "BUY" else c.low <= tp

                # SL wins same-candle ties. Exit labels reflect the stop that was active.
                if hit_stop:
                    if stage == "SL+":
                        status = "SL_PLUS"
                        label = "🔒 <b>SL+ HIT</b>"
                        detail = f"SL trailing tersentuh di {fmt_price(pair, active_sl)}; profit terkunci +{((active_sl-entry)/risk if side == 'BUY' else (entry-active_sl)/risk):.2f}R."
                    elif stage == "BE":
                        status = "TP_BE"
                        label = "🟢 <b>BE HIT</b>"
                        detail = f"SL breakeven tersentuh di {fmt_price(pair, active_sl)}; bukan loss harga sebelum biaya."
                    else:
                        status = "SL"
                        label = "❌ <b>SL HIT</b>"
                        detail = f"SL awal tersentuh di {fmt_price(pair, active_sl)}."
                    self._close(item, status, c.ts, messages, f"{label} {esc(pair)} 1m: {detail}")
                    break
                if hit_tp:
                    self._close(item, "TP", c.ts, messages, f"✅ <b>TP HIT</b> {esc(pair)} 1m: target tersentuh di {fmt_price(pair, tp)}.")
                    break

                # Only after testing the old stop/TP may this candle improve protection.
                if side == "BUY":
                    item["best_price"] = max(float(item.get("best_price", entry)), c.high)
                    favorable_r = (float(item["best_price"]) - entry) / risk
                else:
                    item["best_price"] = min(float(item.get("best_price", entry)), c.low, entry)
                    favorable_r = (entry - float(item["best_price"])) / risk
                old_sl = active_sl
                old_stage = stage

                if favorable_r >= BE_TRIGGER_R and stage == "INIT":
                    active_sl = entry
                    stage = "BE"

                if favorable_r >= SLPLUS_TRIGGER_R:
                    if side == "BUY":
                        trail = float(item["best_price"]) - SLPLUS_GAP_R * risk
                        locked_floor = entry + SLPLUS_LOCK_R * risk
                        active_sl = max(active_sl, locked_floor, trail)
                    else:
                        trail = float(item["best_price"]) + SLPLUS_GAP_R * risk
                        locked_ceiling = entry - SLPLUS_LOCK_R * risk
                        active_sl = min(active_sl, locked_ceiling, trail)
                    stage = "SL+"

                # Monotonic stop: a BUY SL can only rise; a SELL SL can only fall.
                if side == "BUY":
                    active_sl = max(float(item.get("active_sl", initial_sl)), active_sl)
                else:
                    active_sl = min(float(item.get("active_sl", initial_sl)), active_sl)
                item["active_sl"] = active_sl
                item["sl_stage"] = stage
                item["be_armed"] = stage in {"BE", "SL+"}
                item["be_price"] = entry if item["be_armed"] else 0.0

                if active_sl != old_sl:
                    locked_r = (active_sl - entry) / risk if side == "BUY" else (entry - active_sl) / risk
                    if stage == "BE" and old_stage != "BE":
                        messages.append(f"🛡 <b>SL UPDATE — BE</b> {esc(pair)} {side}\nSL: {fmt_price(pair, old_sl)} → {fmt_price(pair, active_sl)} (entry / 0.00R). Update berlaku mulai candle berikutnya.")
                    elif stage == "SL+":
                        last_notified = float(item.get("last_notified_lock_r", 0.0))
                        if old_stage != "SL+" or locked_r - last_notified >= SLPLUS_NOTIFY_R:
                            messages.append(f"🔒 <b>SL+ UPDATE</b> {esc(pair)} {side}\nSL: {fmt_price(pair, old_sl)} → {fmt_price(pair, active_sl)} | profit terkunci +{locked_r:.2f}R\nFavorable: +{favorable_r:.2f}R. Stop hanya dirapatkan; perubahan berlaku candle berikutnya.")
                            item["last_notified_lock_r"] = locked_r

            if item.get("status") == "OPEN" and time.time() - float(item.get("opened_at", time.time())) > EXPIRE_HOURS * 3600:
                self._close(item, "EXPIRED", rows[-1].ts if rows else 0, messages,
                            f"⏱ <b>EXPIRED</b> {esc(pair)} 1m: tidak menyentuh TP/SL dalam {EXPIRE_HOURS:g} jam; dilepas dari pelacak.")
                changed = True
        if changed:
            self.save()
        return messages

    def _close(self, item: dict[str, Any], status: str, ts: int, messages: list[str], text: str) -> None:
        item["status"] = status
        item["closed_ts"] = ts
        self.state["counts"][status] = int(self.state["counts"].get(status, 0)) + 1
        messages.append(text)

    def rejection_alerts(self, pair: str, rows: list[Candle]) -> list[str]:
        out: list[str] = []
        for item in self.state["trades"]:
            if item.get("status") != "OPEN" or item.get("pair") != pair:
                continue
            warning = rejection_warning(item, rows)
            if not warning:
                continue
            key, text = warning
            hist = self.state["alert_history"].get(key, {})
            if hist.get("ts") == rows[-1].ts:
                continue
            if time.time() - float(hist.get("sent_at", 0)) < REJECTION_COOLDOWN_MIN * 60:
                continue
            self.state["alert_history"][key] = {"ts": rows[-1].ts, "sent_at": time.time()}
            out.append(text)
        if out:
            self.save()
        return out

    # ---- scan
    def process_pair(self, pair: str) -> None:
        rows, source = self.candles(pair)
        ok, reason = self.scannable(pair, rows)
        self.last_scan[pair] = f"{datetime.now(timezone.utc):%H:%M:%S}Z {reason}"
        # Pelacakan posisi tetap jalan walau scan sinyal baru ditahan (mis. emas tutup)
        with self.lock:
            for text in self.settle_open(pair, rows):
                self.tg.send_text(text)
        if not ok:
            return
        with self.lock:
            for text in self.rejection_alerts(pair, rows):
                self.tg.send_text(text)
            sg = make_signal(pair, rows)
            if sg is None:
                return
            if int(self.state["last_signal"].get(pair, 0)) == sg.ts:
                return
            if time.time() - float(self.state["last_sent_at"].get(pair, 0)) < COOLDOWN_MIN * 60:
                return
            self.state["last_signal"][pair] = sg.ts
            self.state["last_sent_at"][pair] = time.time()
            self.state["trades"].append({
                "id": f"{pair}|{sg.ts}|{sg.side}", "pair": pair, "side": sg.side, "entry": sg.entry, "sl": sg.sl,
                "tp": sg.tp, "ts": sg.ts, "rr": sg.rr, "status": "OPEN", "be_armed": False, "be_price": 0.0,
                "initial_sl": sg.sl, "active_sl": sg.sl, "sl_stage": "INIT", "best_price": sg.entry,
                "last_notified_lock_r": 0.0, "opened_at": time.time(), "last_ts": sg.ts})
            self.state["trades"] = self.state["trades"][-150:]
            self.save()
        log(f"[SFP] {pair} {sg.side} entry {sg.entry:.2f} SL {sg.sl:.2f} TP {sg.tp:.2f} RR {sg.rr:.2f}")
        self.tg.send_signal(self.chart(pair, rows, sg), signal_caption(sg, source))

    def chart(self, pair: str, rows: list[Candle], sg: SfpSignal) -> Optional[bytes]:
        try:
            return render_levels_chart(
                f"{DISPLAY.get(pair, pair)} 1m — SHADOW FAKEOUT PDF {sg.side}", rows, side=sg.side, entry=sg.entry,
                sl=sg.sl, targets=[("TP", sg.tp)], zone=(sg.range_low, sg.range_high),
                markers=[(sg.sweep_ts, "SWEEP"), (sg.bos_ts, "BOS"), (sg.ts, "RETEST")], bars=90)
        except Exception:
            log("[SFP] chart gagal:\n" + traceback.format_exc())
            return None

    def loop(self) -> None:
        log(f"[SFP] engine mulai | pair={','.join(PAIRS)} | topic={self.tg.thread_id}")
        while not self.stop.is_set():
            for pair in PAIRS:
                try:
                    self.process_pair(pair)
                    self.last_error.pop(pair, None)
                except Exception as exc:
                    self.last_error[pair] = f"{type(exc).__name__}: {exc}"[:160]
                    log(f"[SFP] {pair} error: {exc}")
            self.stop.wait(SCAN_SEC)

    def start(self) -> None:
        self.thread = threading.Thread(target=self.loop, name="engine-sfp", daemon=True)
        self.thread.start()
        s = material_settings("XAUUSD")
        self.tg.send_text(
            f"🟢 <b>{self.title} online</b>\nPair: {', '.join(PAIRS)} | TF 1m\n"
            f"Range {int(s['range_lookback'])} bar | setup {int(s['setup_lookback'])} bar | shadow ≥ {float(s['shadow_ratio']):g}× body | "
            f"SL {float(s['sl_pips']):g} pip | min RR {float(s['min_rr']):g}\nBE +{BE_TRIGGER_R:g}R | SL+ mulai +{SLPLUS_TRIGGER_R:g}R (gap {SLPLUS_GAP_R:g}R) | cooldown {COOLDOWN_MIN} menit. "
            "Sinyal & simulasi saja, bukan order nyata.")

    # ---- perintah
    def stats_line(self) -> str:
        c = self.state["counts"]
        wins = c.get("TP", 0) + c.get("TP_BE", 0) + c.get("SL_PLUS", 0)
        decided = wins + c.get("SL", 0)
        wr = f"{wins / decided * 100:.0f}%" if decided else "N/A"
        open_n = sum(1 for t in self.state["trades"] if t.get("status") == "OPEN")
        return (f"Hasil: TP {c.get('TP', 0)} | SL+ {c.get('SL_PLUS', 0)} | BE {c.get('TP_BE', 0)} | SL {c.get('SL', 0)} | "
                f"Expired {c.get('EXPIRED', 0)} | Open {open_n} | Win {wr}")

    def status_text(self) -> str:
        lines = [f"<b>{self.title}</b> — topic {self.tg.thread_id or 'General'}", self.stats_line()]
        for pair in PAIRS:
            err = self.last_error.get(pair)
            lines.append(f"{pair}: " + (f"⚠️ {esc(err)}" if err else esc(self.last_scan.get(pair, "belum scan"))))
        return "\n".join(lines)

    def handle_command(self, args: list[str], chat: Any, thread: Optional[int]) -> None:
        sub = (args[0].lower() if args else "status")
        if sub in {"status", "stats"}:
            self.tg.send_text(self.status_text(), chat=chat, thread=thread)
        elif sub in {"cek", "check"}:
            for pair in PAIRS:
                try:
                    rows, source = self.candles(pair)
                    ok, reason = self.scannable(pair, rows)
                    sg = make_signal(pair, rows) if ok else None
                    if sg:
                        self.tg.send_signal(self.chart(pair, rows, sg), signal_caption(sg, source))
                    else:
                        self.tg.send_text(f"{pair} 1m: " + ("WAIT — belum ada setup PDF pada candle terakhir. "
                                          f"Harga {rows[-1].close:,.2f}" if ok else reason), chat=chat, thread=thread)
                except Exception as exc:
                    self.tg.send_text(f"{pair} gagal: {esc(exc)}", chat=chat, thread=thread)
        elif sub in {"backtest", "bt"}:
            days = int(args[1]) if len(args) > 1 and args[1].isdigit() else 3
            days = max(1, min(days, 14))
            self.tg.send_text(f"Backtest SFP {days} hari dimulai (simulasi)…", chat=chat, thread=thread)
            threading.Thread(target=self._bt_job, args=(days, chat, thread), daemon=True).start()
        else:
            self.tg.send_text("Perintah: /sfp status | /sfp cek | /sfp backtest [hari]", chat=chat, thread=thread)

    def _bt_job(self, days: int, chat: Any, thread: Optional[int]) -> None:
        lines = [f"<b>BACKTEST {self.title} — {days} hari</b>",
                 "Entry di close candle retest; SL menang bila SL & TP di satu candle.", ""]
        for pair in PAIRS:
            try:
                rows, source = mdata.fetch_history(pair, "1m", days)
                r = backtest(pair, rows)
                lines.append(f"{pair} ({esc(source)}, {len(rows)} candle): entry {r['total']} | TP/menang {r['wins']} | SL+ {r['sl_plus']} | BE {r['be']} | SL/rugi {r['losses']} | "
                             f"TO {r['timeouts']} | open {r['open']} | WR tertutup {r['win_rate']:.1f}% | "
                             f"TP/entry {r['hit_rate']:.1f}% | net {r['net_r']:+.2f}R | PF {r['pf']:.2f}")
            except Exception as exc:
                lines.append(f"{pair}: GAGAL — {esc(exc)}")
        lines.append("\nHasil historis bukan jaminan profit.")
        self.tg.send_text("\n".join(lines), chat=chat, thread=thread)
''',
    "tpb": r'''
# -*- coding: utf-8 -*-
"""ENGINE 2 — TREND PULLBACK M5 (port dari main.py: rencana_candidate -> cabang 'TREND PULLBACK').

Syarat (candle M5 closed):
  tren berkualitas : ADX >= TPB_MIN_ADX, jarak EMA21-EMA50 >= TPB_MIN_EMA_GAP_ATR x ATR, slope EMA50 N bar
  BUY  : EMA21 > EMA50, close > EMA50 / EMA200 / VWAP harian, EMA50 naik; pullback menyentuh EMA21 (low <= EMA21 + 0.2 ATR),
         close >= EMA21, candle hijau, break high candle sebelumnya, MACD hist naik, RSI 45-67, order-flow candle >= -0.02
  SELL : cermin di atas (RSI 33-57, break low candle sebelumnya, flow <= +0.02)
  veto exhaustion: tidak SELL di dasar flush / tidak BUY di puncak leg yang sudah ekstensi
SL = 1.5 ATR (min 0.15% harga, maks 3 ATR); TP = RR x jarak SL (default 2.0R).

Pelacak paper: candle M1 setelah sinyal close, BE+ (0.80R) -> SL+ (1.20R), modal paper Rp (lot 0.01),
auto top-up bila saldo <= 0. Independen: state tpb_state.json + tpb_capital.json, topic TPB_TOPIC_ID.
"""
from __future__ import annotations

import math
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Optional

from . import data as mdata
from .common import (WIB, TelegramView, env_bool, env_float, env_int, env_str, esc, load_json, log, render_levels_chart,
                     rupiah, save_json, session_for, state_path)
from .data import Candle

PAIRS = [p.strip().upper() for p in env_str("TPB_PAIRS", "XAUUSD").split(",") if p.strip() in mdata.SOURCES] or ["XAUUSD"]
TF = "5m"
TF_MS = 300_000
MIN_SCORE = env_int("TPB_MIN_SCORE", 2)
RR = env_float("TPB_RR", 2.0)
MIN_ADX = max(0.0, env_float("TPB_MIN_ADX", 16.0))
MIN_EMA_GAP_ATR = max(0.0, env_float("TPB_MIN_EMA_GAP_ATR", 0.08))
SLOPE_BARS = max(1, env_int("TPB_SLOPE_BARS", 4))
REQUIRE_BREAK = env_bool("TPB_REQUIRE_BREAK", True)
SPREAD = max(0.0, env_float("TPB_SPREAD", 0.30))
OFFSET = env_float("TPB_OFFSET", 0.0)
SCAN_DELAY = max(1.0, env_float("TPB_SCAN_DELAY", 4.0))
TRACK_SEC = max(5.0, env_float("TPB_TRACK_SEC", 10.0))
MAX_BARS = max(1, env_int("TPB_MAX_BARS", 96))                    # dalam bar M15 (sama seperti main.py) -> *15 candle M1
CAPITAL_START_IDR = max(0.0, env_float("TPB_CAPITAL_START_IDR", 1_000_000))
CAPITAL_TOPUP_IDR = max(0.0, env_float("TPB_CAPITAL_TOPUP_IDR", 500_000))
TRADE_LOT = max(0.0001, env_float("TPB_LOT", 0.01))
USD_IDR = max(1.0, env_float("TPB_USD_IDR", 16_000))
UNITS_PER_LOT = max(0.0001, env_float("TPB_UNITS_PER_LOT", 100))
PAPER_COST = max(0.0, env_float("TPB_PAPER_COST", SPREAD))
BE_ENABLED_DEFAULT = env_bool("TPB_BE_ENABLED", True)
BE_TRIGGER_R = max(0.0, env_float("TPB_BE_TRIGGER_R", 0.80))
BE_OFFSET_R = max(0.0, env_float("TPB_BE_OFFSET_R", 0.05))
SLPLUS_TRIGGER_R = max(BE_TRIGGER_R, env_float("TPB_SLPLUS_TRIGGER_R", 1.20))
SLPLUS_LOCK_R = max(0.0, env_float("TPB_SLPLUS_LOCK_R", 0.30))
SLPLUS_TRAIL_GAP_R = max(0.05, env_float("TPB_SLPLUS_TRAIL_GAP_R", 0.50))
SLPLUS_NOTIFY_R = max(0.05, env_float("TPB_SLPLUS_NOTIFY_R", 0.20))
TICK = max(1e-6, env_float("TPB_TICK", 0.01))
STATE_FILE = state_path("tpb_state.json")
CAPITAL_FILE = state_path("tpb_capital.json")
DISPLAY = {"XAUUSD": "XAUUSD", "BTCUSD": "BTCUSD"}


# ------------------------------------------------------------------ indikator (port build())
def _ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    alpha = 2 / (period + 1)
    out = [float(vals[0])]
    for x in vals[1:]:
        out.append(float(x) * alpha + out[-1] * (1 - alpha))
    return out


def _rolling_mean(vals: list[float], p: int) -> list[Optional[float]]:
    n = len(vals)
    out: list[Optional[float]] = [None] * n
    s = 0.0
    for i, x in enumerate(vals):
        s += float(x)
        if i >= p:
            s -= float(vals[i - p])
        if i >= p - 1:
            out[i] = s / p
    return out


def _rolling_std(vals: list[float], p: int, means: list[Optional[float]]) -> list[Optional[float]]:
    out: list[Optional[float]] = [None] * len(vals)
    for i in range(p - 1, len(vals)):
        m = means[i]
        if m is not None:
            out[i] = math.sqrt(sum((vals[j] - m) ** 2 for j in range(i - p + 1, i + 1)) / p)
    return out


def build(candles: list[Candle]) -> dict[str, list]:
    if not candles:
        raise ValueError("Data candle kosong")
    D: dict[str, list] = {"t": [c.ts for c in candles], "o": [c.open for c in candles], "h": [c.high for c in candles],
                          "l": [c.low for c in candles], "c": [c.close for c in candles],
                          "v": [c.volume for c in candles]}
    n = len(candles)
    h, l, c, v = D["h"], D["l"], D["c"], D["v"]
    tr = [h[i] - l[i] if i == 0 else max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(n)]
    atr: list[Optional[float]] = [None] * n
    if n >= 14:
        atr[13] = sum(tr[:14]) / 14
        for i in range(14, n):
            atr[i] = (atr[i - 1] * 13 + tr[i]) / 14
    ema9, ema21, ema50, ema200 = (_ema(c, p) for p in (9, 21, 50, 200))
    m20 = _rolling_mean(c, 20)
    sd20 = _rolling_std(c, 20, m20)
    bb_up = [None if m20[i] is None else m20[i] + 2 * sd20[i] for i in range(n)]
    bb_dn = [None if m20[i] is None else m20[i] - 2 * sd20[i] for i in range(n)]
    gains = [0.0] + [max(c[i] - c[i - 1], 0.0) for i in range(1, n)]
    losses = [0.0] + [max(c[i - 1] - c[i], 0.0) for i in range(1, n)]
    ag, al = _rolling_mean(gains, 14), _rolling_mean(losses, 14)
    rsi: list[Optional[float]] = [None] * n
    for i in range(n):
        if ag[i] is not None and al[i] is not None:
            rsi[i] = 100.0 if al[i] == 0 and ag[i] > 0 else (50.0 if al[i] == 0 else 100 - 100 / (1 + ag[i] / al[i]))
    e12, e26 = _ema(c, 12), _ema(c, 26)
    macd = [e12[i] - e26[i] for i in range(n)]
    macd_sig = _ema(macd, 9)
    mh = [macd[i] - macd_sig[i] for i in range(n)]
    vm = _rolling_mean(v, 20)
    typical = [(h[i] + l[i] + c[i]) / 3 for i in range(n)]
    pv = vv = 0.0
    vwap: list[float] = []
    vwap_day = None
    for i in range(n):                       # VWAP harian, reset 00:00 UTC
        day = int(D["t"][i] // 86_400_000)
        if day != vwap_day:
            pv = vv = 0.0
            vwap_day = day
        pv += typical[i] * max(v[i], 0)
        vv += max(v[i], 0)
        vwap.append(pv / vv if vv else c[i])
    dmp = [0.0] + [max(h[i] - h[i - 1], 0) for i in range(1, n)]
    dmn = [0.0] + [max(l[i - 1] - l[i], 0) for i in range(1, n)]
    atr_s, p_s, n_s = _rolling_mean(tr, 14), _rolling_mean(dmp, 14), _rolling_mean(dmn, 14)
    dx = [0.0] * n
    for i in range(n):
        if atr_s[i] and p_s[i] is not None and n_s[i] is not None:
            dip = 100 * p_s[i] / atr_s[i]
            din = 100 * n_s[i] / atr_s[i]
            dx[i] = 100 * abs(dip - din) / (dip + din) if dip + din else 0.0
    adx = _rolling_mean(dx, 14)
    D.update(atr=atr, ema9=ema9, ema21=ema21, ema50=ema50, ema200=ema200, bb_up=bb_up, bb_dn=bb_dn, rsi=rsi,
             macd=macd, macd_sig=macd_sig, macd_hist=mh, vol_ma=vm, vwap=vwap, adx=adx)
    return D


def delta_bar(D: dict, i: int) -> float:
    rng = D["h"][i] - D["l"][i]
    if rng <= 0:
        return 0.0
    return D["v"][i] * ((D["c"][i] - D["l"][i]) - (D["h"][i] - D["c"][i])) / rng


def of_ratio(D: dict, i: int, n: int = 5) -> float:
    start = max(0, i - n + 1)
    vol = sum(D["v"][k] for k in range(start, i + 1))
    return sum(delta_bar(D, k) for k in range(start, i + 1)) / vol if vol > 0 else 0.0


# ------------------------------------------------------------------ sinyal (cabang TREND PULLBACK persis)
def trend_pullback(D: dict, i: int, min_score: Optional[int] = None) -> Optional[dict[str, Any]]:
    n = len(D["c"])
    min_score = MIN_SCORE if min_score is None else min_score
    if i < 60 or i >= n:
        return None
    a, r, vm = D["atr"][i], D["rsi"][i], D["vol_ma"][i]
    if a is None or a <= 0 or r is None or vm is None or D["adx"][i] is None:
        return None
    c, o, h, l, v = D["c"], D["o"], D["h"], D["l"], D["v"]
    e21, e50, e200 = D["ema21"], D["ema50"], D["ema200"]
    mh = D["macd_hist"]
    slope_ok = i >= SLOPE_BARS
    ema_gap = abs(e21[i] - e50[i])
    trend_quality = D["adx"][i] >= MIN_ADX and ema_gap >= MIN_EMA_GAP_ATR * a and slope_ok
    bull = (trend_quality and e21[i] > e50[i] and c[i] > e50[i] and c[i] > e200[i] and c[i] > D["vwap"][i]
            and e50[i] > e50[i - SLOPE_BARS])
    bear = (trend_quality and e21[i] < e50[i] and c[i] < e50[i] and c[i] < e200[i] and c[i] < D["vwap"][i]
            and e50[i] < e50[i - SLOPE_BARS])
    macd_up, macd_dn = mh[i] > mh[i - 1], mh[i] < mh[i - 1]
    break_up = c[i] > h[i - 1] if REQUIRE_BREAK else True
    break_dn = c[i] < l[i - 1] if REQUIRE_BREAK else True
    of_now = of_ratio(D, i)
    # veto exhaustion
    ext_start = max(0, i - 8)
    local_low = min(l[ext_start:i]) if i > ext_start else l[i]
    local_high = max(h[ext_start:i]) if i > ext_start else h[i]
    near_local_low = (c[i] - local_low) <= 0.35 * a
    near_local_high = (local_high - c[i]) <= 0.35 * a
    lower_band, upper_band = D["bb_dn"][i], D["bb_up"][i]
    sell_exhausted = (near_local_low and (r <= 46 or (e50[i] - c[i]) >= 0.80 * a
                                          or (lower_band is not None and l[i] <= lower_band + 0.15 * a)))
    buy_exhausted = (near_local_high and (r >= 55 or (c[i] - e50[i]) >= 0.80 * a
                                          or (upper_band is not None and h[i] >= upper_band - 0.15 * a)))
    of_buy_ok = of_now >= -0.02
    of_sell_ok = of_now <= 0.02
    pick: Optional[dict[str, Any]] = None
    dist = min(max(1.5 * a, c[i] * 0.0015), 3 * a)
    if (bull and not buy_exhausted and of_buy_ok and break_up and l[i] <= e21[i] + 0.20 * a and c[i] >= e21[i]
            and c[i] > o[i] and macd_up and 45 <= r <= 67):
        score = 3 + (1 if of_now > 0.1 else 0)
        if score >= min_score:
            pick = {"side": "L", "score": score, "entry": c[i], "dist": dist, "sl": c[i] - dist, "tp": c[i] + RR * dist}
    elif (bear and not sell_exhausted and of_sell_ok and break_dn and h[i] >= e21[i] - 0.20 * a and c[i] <= e21[i]
          and c[i] < o[i] and macd_dn and 33 <= r <= 57):
        score = 3 + (1 if of_now < -0.1 else 0)
        if score >= min_score:
            pick = {"side": "S", "score": score, "entry": c[i], "dist": dist, "sl": c[i] + dist, "tp": c[i] - RR * dist}
    if pick is None:
        return None
    pick.update(kind="TREND PULLBACK", trig="PULLBACK", atr=a, ofr=of_now, t=D["t"][i], i=i,
                session=session_for(D["t"][i] / 1000), rr=RR, adx=D["adx"][i], rsi=r)
    return pick


# ------------------------------------------------------------------ modal paper
class CapitalTracker:
    """Pembukuan P/L paper realized (tidak ada order nyata)."""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.data: dict[str, Any] = {
            "balance": float(CAPITAL_START_IDR), "initial_capital": float(CAPITAL_START_IDR), "realized_pnl_idr": 0.0,
            "topups_idr": 0.0, "topup_count": 0, "entries": 0, "events": []}
        saved = load_json(path, None)
        if isinstance(saved, dict):
            self.data.update(saved)

    def save(self) -> None:
        with self.lock:
            save_json(self.path, self.data)

    def register_entry(self) -> None:
        with self.lock:
            self.data["entries"] = int(self.data.get("entries", 0)) + 1
        self.save()

    def settle(self, trade: dict[str, Any], status: str, exit_px: float, exit_ts: str) -> Optional[dict[str, Any]]:
        trade_id = str(trade["id"])
        with self.lock:
            if any(str(e.get("id")) == trade_id for e in self.data["events"]):
                return None
            entry, exit_px = float(trade["entry"]), float(exit_px)
            is_long = trade["side"] == "L"
            gross = (exit_px - entry) if is_long else (entry - exit_px)
            net_per_unit = gross - PAPER_COST
            lot = float(trade.get("lot", TRADE_LOT))
            pnl_usd = net_per_unit * lot * UNITS_PER_LOT
            pnl_idr = round(pnl_usd * USD_IDR, 2)
            before = float(self.data["balance"])
            after = before + pnl_idr
            topup = 0.0
            if after <= 0 and CAPITAL_TOPUP_IDR > 0:
                topup = float(CAPITAL_TOPUP_IDR)
                after += topup
                self.data["topups_idr"] = float(self.data.get("topups_idr", 0.0)) + topup
                self.data["topup_count"] = int(self.data.get("topup_count", 0)) + 1
            initial_risk = abs(entry - float(trade.get("initial_sl", entry)))
            r_mult = net_per_unit / initial_risk if initial_risk > 0 else 0.0
            stage = int(trade.get("be_stage", 0) or 0)
            tag = status
            if status == "SL":
                tag = "SL+" if stage >= 2 else ("BE+" if stage == 1 else "SL")
            self.data["balance"] = round(after, 2)
            self.data["realized_pnl_idr"] = round(float(self.data["realized_pnl_idr"]) + pnl_idr, 2)
            ev = {"id": trade_id, "ts": exit_ts, "status": status, "tag": tag, "be_stage": stage,
                  "side": "BUY" if is_long else "SELL", "kind": trade.get("kind", "?"), "entry": entry, "exit": exit_px,
                  "lot": lot, "r": round(r_mult, 3), "pnl_usd": round(pnl_usd, 4), "pnl_idr": pnl_idr,
                  "balance_before": before, "balance_after": after, "topup_idr": topup}
            self.data["events"].append(ev)
            self.data["events"] = self.data["events"][-2000:]
        self.save()
        return ev

    def summary_text(self) -> str:
        with self.lock:
            d = dict(self.data)
            events = list(d["events"])
        cnt = lambda tag: sum(e.get("tag") == tag for e in events)  # noqa: E731
        wins = sum(float(e.get("pnl_idr", 0)) > 0 for e in events)
        losses = sum(float(e.get("pnl_idr", 0)) < 0 for e in events)
        n = len(events)
        wr = wins / n * 100 if n else 0.0
        return (f"<b>MODAL PAPER — TREND PULLBACK</b>\nModal awal : {rupiah(d['initial_capital'])}\n"
                f"Saldo kini : {rupiah(d['balance'])}\nRealized P/L: {rupiah(d['realized_pnl_idr'])}\n"
                f"Trade selesai {n} | TP {cnt('TP')} | SL+ {cnt('SL+')} | BE+ {cnt('BE+')} | SL {cnt('SL')} | "
                f"Timeout {sum(e.get('status') == 'TIMEOUT' for e in events)}\n"
                f"Win P/L bersih: {wr:.1f}% ({wins} untung / {losses} rugi)\n"
                f"Sinyal terdaftar: {int(d.get('entries', 0))} | Auto top-up: {rupiah(d.get('topups_idr', 0))} ({int(d.get('topup_count', 0))}x)\n"
                f"Lot paper {TRADE_LOT:g} | Unit/lot {UNITS_PER_LOT:g} oz | USD/IDR {USD_IDR:g} | biaya {PAPER_COST:g} USD/oz.\n"
                "Ini pembukuan simulasi, bukan saldo akun trading.")

    def recap_text(self, days: int = 7) -> str:
        cutoff = datetime.now(timezone.utc).timestamp() - max(1, days) * 86400
        chosen = []
        with self.lock:
            for e in self.data["events"]:
                try:
                    ts = datetime.fromisoformat(str(e.get("ts", "")).replace("Z", "+00:00"))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    if ts.timestamp() >= cutoff:
                        chosen.append(e)
                except Exception:
                    pass
            bal = self.data["balance"]
        cnt = lambda tag: sum(e.get("tag") == tag for e in chosen)  # noqa: E731
        pnl = sum(float(e.get("pnl_idr", 0) or 0) for e in chosen)
        wins = sum(float(e.get("pnl_idr", 0) or 0) > 0 for e in chosen)
        losses = sum(float(e.get("pnl_idr", 0) or 0) < 0 for e in chosen)
        return (f"<b>REKAP PAPER TREND PULLBACK — {max(1, days)} HARI</b>\n"
                f"Trade selesai {len(chosen)} | TP {cnt('TP')} | SL+ {cnt('SL+')} | BE+ {cnt('BE+')} | SL {cnt('SL')}\n"
                f"P/L realized {rupiah(pnl)} | {wins} untung / {losses} rugi\nSaldo sekarang {rupiah(bal)}\n"
                "Catatan: rekaman paper tracker, bukan order live.")


# ------------------------------------------------------------------ manajemen posisi (port _manage_active_bar)
def _round_stop(value: float, side: str) -> float:
    if side == "L":
        return math.floor(value / TICK + 1e-10) * TICK
    return math.ceil(value / TICK - 1e-10) * TICK


def manage_bar(a: dict[str, Any], op: float, hi: float, lo: float, cl: float, be_enabled: bool) -> Optional[tuple[str, float]]:
    """Process one closed M1 candle. Current stop is tested first; any stop update
    calculated from this candle applies only to following candles (no look-ahead).
    SL+ trails the best favorable price continuously after its trigger.
    """
    side = a["side"]
    entry = float(a["entry"])
    initial_sl = float(a.get("initial_sl", entry))
    active_sl = float(a.get("active_sl", initial_sl))
    tp = float(a["tp"])
    risk = abs(entry - initial_sl)
    a["bars_held"] = int(a.get("bars_held", 0)) + 1
    if risk <= 0:
        return ("TIMEOUT", cl)
    # Conservative fill ordering: stop before target when both are touched.
    if side == "L":
        if lo <= active_sl:
            return ("SL", float(op if op < active_sl else active_sl))
        if hi >= tp:
            return ("TP", float(tp))
        a["best_price"] = max(float(a.get("best_price", entry)), hi, entry)
        favorable = float(a["best_price"]) - entry
    else:
        if hi >= active_sl:
            return ("SL", float(op if op > active_sl else active_sl))
        if lo <= tp:
            return ("TP", float(tp))
        a["best_price"] = min(float(a.get("best_price", entry)), lo, entry)
        favorable = entry - float(a["best_price"])

    if be_enabled:
        stage = int(a.get("be_stage", 0) or 0)
        new_stage, new_sl = stage, active_sl
        min_cost_lock = max(SPREAD, PAPER_COST) + 2.0 * TICK
        favorable_r = favorable / risk
        if favorable_r >= SLPLUS_TRIGGER_R:
            new_stage = 2
            trail_gap = max(SLPLUS_TRAIL_GAP_R * risk, min_cost_lock)
            if side == "L":
                candidate = max(entry + SLPLUS_LOCK_R * risk, float(a["best_price"]) - trail_gap)
                new_sl = max(new_sl, candidate)
            else:
                candidate = min(entry - SLPLUS_LOCK_R * risk, float(a["best_price"]) + trail_gap)
                new_sl = min(new_sl, candidate)
        elif favorable_r >= BE_TRIGGER_R and stage < 1:
            new_stage = 1
            lock = max(BE_OFFSET_R * risk, min_cost_lock)
            target = entry + (lock if side == "L" else -lock)
            new_sl = max(new_sl, target) if side == "L" else min(new_sl, target)
        if new_stage != stage or abs(new_sl - active_sl) > 1e-12:
            # Monotonic protection: BUY stop rises, SELL stop falls; never loosen it.
            rounded = _round_stop(new_sl, side)
            a["active_sl"] = max(active_sl, rounded) if side == "L" else min(active_sl, rounded)
            a["sl"] = a["active_sl"]
            a["be_stage"] = new_stage
            a["stop_mode"] = "SL+" if new_stage >= 2 else ("BE+" if new_stage == 1 else "INITIAL")
    if int(a["bars_held"]) >= max(1, int(a.get("max_hold_m1") or MAX_BARS * 15)):
        return ("TIMEOUT", float(cl))
    return None


# ------------------------------------------------------------------ backtest ringkas M5
def backtest(pair: str, candles: list[Candle]) -> dict[str, Any]:
    """Backtest M5 pendekatan: entry open candle berikutnya, SL menang bila SL & TP satu candle,
    BE+/SL+ diproses per candle M5 (lebih kasar dari pelacak live yang memakai M1)."""
    D = build(candles)
    n = len(candles)
    trades: list[float] = []
    kinds = {"TP": 0, "SL": 0, "BE+": 0, "SL+": 0, "TIMEOUT": 0}
    i = 61
    while i < n - 2:
        sg = trend_pullback(D, i)
        if not sg:
            i += 1
            continue
        entry = candles[i + 1].open
        sl, tp = sg["sl"], sg["tp"]
        side = sg["side"]
        risk = abs(entry - sl)
        if risk <= 0 or (side == "L" and not (sl < entry < tp)) or (side == "S" and not (tp < entry < sl)):
            i += 1
            continue
        trade = {"side": side, "entry": entry, "initial_sl": sl, "active_sl": sl, "tp": tp, "bars_held": 0,
                 "max_hold_m1": MAX_BARS * 3}
        result, px, j = "TIMEOUT", candles[min(i + 1 + MAX_BARS * 3, n - 1)].close, i + 1
        while j < n:
            c = candles[j]
            out = manage_bar(trade, c.open, c.high, c.low, c.close, BE_ENABLED_DEFAULT)
            if out:
                result, px = out
                if result == "SL":
                    stage = int(trade.get("be_stage", 0))
                    result = "SL+" if stage >= 2 else ("BE+" if stage == 1 else "SL")
                break
            j += 1
        gross = (px - entry) if side == "L" else (entry - px)
        trades.append((gross - SPREAD) / risk)
        kinds[result] = kinds.get(result, 0) + 1
        i = j + 1
    wins = sum(1 for r in trades if r > 0)
    gp = sum(r for r in trades if r > 0)
    gl = abs(sum(r for r in trades if r < 0))
    return {"pair": pair, "n": len(trades), "wins": wins, "kinds": kinds, "net_r": sum(trades),
            "wr": wins / len(trades) * 100 if trades else 0.0, "pf": gp / gl if gl else (float("inf") if gp else 0.0)}


# ------------------------------------------------------------------ engine
class TrendPullbackEngine:
    key = "tpb"
    title = "TREND PULLBACK M5"

    def __init__(self, tg: TelegramView):
        self.tg = tg
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.capital = CapitalTracker(CAPITAL_FILE)
        self.state: dict[str, Any] = load_json(STATE_FILE, {})
        for k, v in (("active", []), ("seen", []), ("last_ts", {}), ("counts", {"TP": 0, "SL+": 0, "BE+": 0, "SL": 0, "TIMEOUT": 0}),
                     ("offset", OFFSET), ("be_enabled", BE_ENABLED_DEFAULT)):
            self.state.setdefault(k, v)
        self.last_error: dict[str, str] = {}
        self.last_scan: dict[str, str] = {}

    def save(self) -> None:
        with self.lock:
            save_json(STATE_FILE, self.state)

    @property
    def offset(self) -> float:
        return float(self.state.get("offset", 0.0))

    def fp(self, v: float) -> str:
        return f"{v + self.offset:,.2f}"

    # ---- caption & chart
    def signal_caption(self, pair: str, sg: dict[str, Any], D: dict, source: str) -> str:
        i = sg["i"]
        off = self.offset
        side = "BUY" if sg["side"] == "L" else "SELL"
        e, s_, t = sg["entry"] + off, sg["sl"] + off, sg["tp"] + off
        close_dt = datetime.fromtimestamp((sg["t"] + TF_MS) / 1000, WIB)
        sep = "-" * 31
        rows = [f"TF/Sesi {TF.upper()} | {sg['session']}", f"Waktu   {close_dt:%d-%m %H:%M} WIB", f"Entry   {e:.2f}",
                f"SL      {s_:.2f} ({s_ - e:+.2f})", f"TP      {t:.2f} ({t - e:+.2f})",
                f"Skor {sg['score']}  RR 1:{sg['rr']:g}  ATR {sg['atr']:.2f}", sep,
                f"EMA 9/21    {D['ema9'][i] + off:.1f}/{D['ema21'][i] + off:.1f}",
                f"EMA 50/200  {D['ema50'][i] + off:.1f}/{D['ema200'][i] + off:.1f}",
                f"VWAP {D['vwap'][i] + off:.1f}  RSI {sg['rsi']:.0f}  ADX {sg['adx']:.0f}",
                f"MACD hist {D['macd_hist'][i]:+.3f}  Flow {sg['ofr']:+.2f}", sep,
                f"BE+ +{BE_TRIGGER_R:g}R (kunci +{BE_OFFSET_R:g}R) | SL+ trailing dari +{SLPLUS_TRIGGER_R:g}R "
                f"(kunci min +{SLPLUS_LOCK_R:g}R, gap {SLPLUS_TRAIL_GAP_R:g}R)",
                f"Filter: ADX>={MIN_ADX:g}, gap EMA>={MIN_EMA_GAP_ATR:g} ATR, slope EMA50 {SLOPE_BARS} bar"]
        body = "\n".join(esc(r) for r in rows)
        return (f"{'🟢' if side == 'BUY' else '🔴'} <b>{DISPLAY.get(pair, pair)} {side} — {sg['kind']}</b>\n<pre>{body}</pre>\n"
                f"Sumber {esc(source)}. Simulasi/alert, bukan order live. Cek spread.")

    def chart(self, pair: str, rows: list[Candle], D: dict, sg: dict[str, Any]) -> Optional[bytes]:
        try:
            off = self.offset
            sh = lambda arr: [None if v is None else v + off for v in arr]  # noqa: E731
            shifted = [Candle(c.ts, c.open + off, c.high + off, c.low + off, c.close + off, c.volume) for c in rows]
            side = "BUY" if sg["side"] == "L" else "SELL"
            return render_levels_chart(
                f"{DISPLAY.get(pair, pair)} M5 — {sg['kind']} {side}", shifted, side=side, entry=sg["entry"] + off,
                sl=sg["sl"] + off, targets=[("TP", sg["tp"] + off)],
                lines=[("EMA21", sh(D["ema21"]), (37, 99, 235)), ("EMA50", sh(D["ema50"]), (234, 138, 0)),
                       ("EMA200", sh(D["ema200"]), (120, 120, 120)), ("VWAP", sh(D["vwap"]), (147, 51, 234))],
                markers=[(sg["t"], "SIGNAL")], bars=110)
        except Exception:
            log("[TPB] chart gagal:\n" + traceback.format_exc())
            return None

    # ---- registrasi & pelacakan
    def register(self, pair: str, sg: dict[str, Any]) -> bool:
        sid = f"{pair}|{int(sg['t'])}|{sg['kind']}|{sg['side']}"
        with self.lock:
            if sid in self.state["seen"] or any(a.get("id") == sid for a in self.state["active"]):
                return False
            entry, sl = float(sg["entry"]), float(sg["sl"])
            if abs(entry - sl) <= 0:
                return False
            self.state["active"].append({
                "id": sid, "pair": pair, "side": sg["side"], "kind": sg["kind"], "entry": entry, "dist": abs(entry - sl),
                "initial_sl": sl, "active_sl": sl, "sl": sl, "tp": float(sg["tp"]), "be_stage": 0, "stop_mode": "INITIAL",
                "lot": TRADE_LOT, "bars_held": 0, "ts": time.time(), "sig_t": int(sg["t"]), "tf_ms": TF_MS,
                "best_price": entry, "last_notified_lock_r": 0.0, "last_checked_ts": int(sg["t"]) + TF_MS - 60_000})
            self.state["seen"] = (self.state["seen"] + [sid])[-500:]
            self.capital.register_entry()
        self.save()
        return True

    def event_text(self, a: dict[str, Any], result: str, px: float, ev: Optional[dict[str, Any]]) -> str:
        d = 1 if a["side"] == "L" else -1
        pnl = (px - a["entry"]) * d
        risk = abs(float(a["entry"]) - float(a.get("initial_sl", a["entry"])))
        stage = int(a.get("be_stage", 0) or 0)
        kind = result
        if result == "SL":
            kind = "SL+" if stage >= 2 else ("BE+" if stage == 1 else "SL")
        title = {"TP": "✅ TP HIT", "SL+": "🟢 SL+ — PROFIT TERKUNCI", "BE+": "🟡 BE+ — PROFIT KECIL TERKUNCI",
                 "SL": "❌ SL HIT"}.get(kind, "⏱ TIMEOUT" + (" • profit" if pnl > 0 else (" • rugi" if pnl < 0 else "")))
        stop = float(a.get("active_sl", a.get("sl", 0)))
        lines = [f"<b>{title} — {esc(a.get('pair', 'XAUUSD'))}</b>", f"Side {'BUY' if d == 1 else 'SELL'} | Setup {esc(a.get('kind', '?'))}",
                 f"Entry {self.fp(a['entry'])} | Exit {self.fp(px)}",
                 f"Hasil harga {pnl:+.2f} ({pnl / max(risk, 1e-9):+.2f}R sebelum biaya)",
                 f"Stop akhir {self.fp(stop)} | Mode {a.get('stop_mode', 'INITIAL')}"]
        if ev:
            lines.append(f"Modal paper {rupiah(ev.get('balance_after', 0))} | P/L {rupiah(ev.get('pnl_idr', 0))}")
            if ev.get("topup_idr"):
                lines.append(f"Auto top-up {rupiah(ev['topup_idr'])}")
        close_dt = datetime.fromtimestamp((int(a["sig_t"]) + TF_MS) / 1000, WIB)
        lines.append(f"Signal close {close_dt:%d-%m %H:%M} WIB")
        lines.append("Paper tracker dari candle Bybit; bukan rekaman eksekusi CFD dan bukan order live.")
        return "\n".join(lines)

    def check_active(self) -> None:
        with self.lock:
            active = [a for a in self.state["active"] if isinstance(a, dict) and "entry" in a]
        if not active:
            return
        done: set[str] = set()
        by_pair: dict[str, list[Candle]] = {}
        for a in active:
            pair = a.get("pair", "XAUUSD")
            if pair not in by_pair:
                try:
                    by_pair[pair] = mdata.fetch_candles(pair, "1m", limit=1000, ttl=8.0)[0]
                except Exception as exc:
                    log(f"[TPB] tracker {pair}: {exc}")
                    by_pair[pair] = []
            candles = by_pair[pair]
            result = exit_px = exit_ts = None
            signal_close = int(a["sig_t"]) + int(a.get("tf_ms", TF_MS))
            cursor = int(a.get("last_checked_ts", signal_close - 60_000))
            for c in candles:
                if c.ts < signal_close or c.ts <= cursor:
                    continue
                with self.lock:
                    old_sl = float(a.get("active_sl", a.get("sl", a.get("entry", 0.0))))
                    old_stage = int(a.get("be_stage", 0) or 0)
                    out = manage_bar(a, c.open, c.high, c.low, c.close, bool(self.state.get("be_enabled", True)))
                    a["last_checked_ts"] = c.ts
                    new_sl = float(a.get("active_sl", a.get("sl", old_sl)))
                    new_stage = int(a.get("be_stage", 0) or 0)
                    initial_risk = abs(float(a.get("entry", 0.0)) - float(a.get("initial_sl", a.get("sl", 0.0))))
                    side_sign = 1.0 if a.get("side") == "L" else -1.0
                    locked_r = side_sign * (new_sl - float(a.get("entry", 0.0))) / initial_risk if initial_risk > 0 else 0.0
                    last_notified = float(a.get("last_notified_lock_r", 0.0))
                    should_notify = (not out and abs(new_sl - old_sl) > max(1e-9, TICK * 0.5)
                                     and (new_stage != old_stage or locked_r - last_notified >= SLPLUS_NOTIFY_R))
                    if should_notify:
                        stage_text = "SL+" if new_stage >= 2 else ("BE+" if new_stage == 1 else "SL")
                        emoji = "🔒" if new_stage >= 2 else "🛡"
                        self.tg.send_text(
                            f"{emoji} <b>SL UPDATE — {stage_text}</b> {esc(a.get('pair', pair))} "
                            f"{'BUY' if a.get('side') == 'L' else 'SELL'}\n"
                            f"SL: {self.fp(old_sl)} → {self.fp(new_sl)} | profit terkunci {locked_r:+.2f}R\n"
                            f"Stop hanya dirapatkan; perubahan berlaku mulai candle berikutnya."
                        )
                        a["last_notified_lock_r"] = locked_r
                if out:
                    result, exit_px = out
                    exit_ts = datetime.fromtimestamp((c.ts + 60_000) / 1000, timezone.utc).isoformat()
                    break
            if not result and candles and time.time() - float(a.get("ts", time.time())) > MAX_BARS * 900:
                result, exit_px = "TIMEOUT", candles[-1].close        # pemulihan bila bot lama mati
                exit_ts = datetime.fromtimestamp((candles[-1].ts + 60_000) / 1000, timezone.utc).isoformat()
            if result:
                ev = self.capital.settle(a, result, float(exit_px), exit_ts or datetime.now(timezone.utc).isoformat())
                self.tg.send_text(self.event_text(a, result, float(exit_px), ev))
                done.add(a["id"])
                with self.lock:
                    stage = int(a.get("be_stage", 0))
                    key = result if result != "SL" else ("SL+" if stage >= 2 else ("BE+" if stage == 1 else "SL"))
                    self.state["counts"][key] = int(self.state["counts"].get(key, 0)) + 1
        with self.lock:
            self.state["active"] = [a for a in self.state["active"] if a.get("id") not in done]
        self.save()

    # ---- scan M5
    def scan_pair(self, pair: str) -> None:
        rows, source = mdata.fetch_candles(pair, TF, limit=300, ttl=3.0)
        if len(rows) < 80:
            raise RuntimeError(f"Candle closed M5 kurang dari 80 (sekarang {len(rows)})")
        idx = len(rows) - 1
        last_ts = int(self.state["last_ts"].get(pair, 0))
        if rows[idx].ts == last_ts:
            return
        age_min = (time.time() * 1000 - rows[idx].ts) / 60_000
        self.state["last_ts"][pair] = rows[idx].ts
        self.save()
        self.last_scan[pair] = f"{datetime.now(timezone.utc):%H:%M:%S}Z candle {datetime.fromtimestamp(rows[idx].ts / 1000, WIB):%H:%M} WIB"
        if age_min > 30:
            log(f"[TPB] {pair} candle basi {age_min:.0f} menit; sinyal dilewati")
            return
        D = build(rows)
        sg = trend_pullback(D, idx)
        if not sg:
            return
        if not self.register(pair, sg):
            return
        log(f"[TPB] {pair} {sg['side']} entry {sg['entry']:.2f} SL {sg['sl']:.2f} TP {sg['tp']:.2f}")
        self.tg.send_signal(self.chart(pair, rows, D, sg), self.signal_caption(pair, sg, D, source))

    def scan_loop(self) -> None:
        while not self.stop.is_set():
            for pair in PAIRS:
                try:
                    self.scan_pair(pair)
                    self.last_error.pop(pair, None)
                except Exception as exc:
                    self.last_error[pair] = f"{type(exc).__name__}: {exc}"[:160]
                    log(f"[TPB] scan {pair}: {exc}")
            now = time.time() * 1000
            boundary = (now // TF_MS + 1) * TF_MS
            # tidur sampai sesaat setelah candle M5 berikutnya close; bila data telat, coba lagi tiap 20 dtk
            delay = (max(0.0, (boundary - now) / 1000.0) + SCAN_DELAY) if self._caught_up() else 20.0
            self.stop.wait(delay)

    def _caught_up(self) -> bool:
        """True bila semua pair sudah memproses candle M5 terakhir yang pasti close."""
        expected = (int(time.time() * 1000) // TF_MS - 1) * TF_MS
        return all(int(self.state["last_ts"].get(p, 0)) >= expected for p in PAIRS)

    def track_loop(self) -> None:
        while not self.stop.is_set():
            self.stop.wait(TRACK_SEC)
            try:
                self.check_active()
            except Exception:
                log("[TPB] tracker error:\n" + traceback.format_exc())

    def start(self) -> None:
        threading.Thread(target=self.scan_loop, name="engine-tpb-scan", daemon=True).start()
        threading.Thread(target=self.track_loop, name="engine-tpb-track", daemon=True).start()
        log(f"[TPB] engine mulai | pair={','.join(PAIRS)} | topic={self.tg.thread_id}")
        self.tg.send_text(
            f"🟢 <b>{self.title} online</b>\nPair: {', '.join(PAIRS)} | scan tiap candle M5 close +{SCAN_DELAY:g}s | tracker M1 tiap {TRACK_SEC:g}s\n"
            f"RR 1:{RR:g} | filter ADX≥{MIN_ADX:g}, EMA gap≥{MIN_EMA_GAP_ATR:g} ATR | BE/SL+ {'ON' if self.state['be_enabled'] else 'OFF'}\n"
            f"Modal paper {rupiah(self.capital.data['balance'])} | lot {TRADE_LOT:g}. Sinyal/pembukuan paper saja.")

    # ---- perintah
    def status_text(self) -> str:
        c = self.state["counts"]
        lines = [f"<b>{self.title}</b> — topic {self.tg.thread_id or 'General'}",
                 f"Aktif {len(self.state['active'])} | TP {c.get('TP', 0)} | SL+ {c.get('SL+', 0)} | BE+ {c.get('BE+', 0)} | "
                 f"SL {c.get('SL', 0)} | TO {c.get('TIMEOUT', 0)}",
                 f"BE/SL+ {'ON' if self.state['be_enabled'] else 'OFF'} | Modal {rupiah(self.capital.data['balance'])} | "
                 f"offset {self.offset:+.2f} | RR {RR:g}"]
        for pair in PAIRS:
            err = self.last_error.get(pair)
            lines.append(f"{pair}: " + (f"⚠️ {esc(err)}" if err else esc(self.last_scan.get(pair, "belum scan"))))
        return "\n".join(lines)

    def diagnostic(self, pair: str) -> tuple[Optional[bytes], str]:
        rows, source = mdata.fetch_candles(pair, TF, limit=300, ttl=3.0)
        D = build(rows)
        i = len(rows) - 1
        sg = trend_pullback(D, i)
        if sg:
            return self.chart(pair, rows, D, sg), self.signal_caption(pair, sg, D, source)
        off = self.offset
        r, adx = D["rsi"][i] or 0, D["adx"][i] or 0
        sep = "-" * 31
        tbl = [f"Harga   {D['c'][i] + off:.2f}  ATR {(D['atr'][i] or 0):.2f}",
               f"Close   {datetime.fromtimestamp((D['t'][i] + TF_MS) / 1000, WIB):%d-%m %H:%M} WIB", "Belum ada setup valid", sep,
               f"EMA 9/21    {D['ema9'][i] + off:.1f}/{D['ema21'][i] + off:.1f}",
               f"EMA 50/200  {D['ema50'][i] + off:.1f}/{D['ema200'][i] + off:.1f}",
               f"VWAP {D['vwap'][i] + off:.1f}  RSI {r:.0f}  ADX {adx:.0f}  MACD {D['macd_hist'][i]:+.2f}",
               f"Flow {of_ratio(D, i):+.2f}"]
        return None, (f"⚪ <b>{DISPLAY.get(pair, pair)} M5 — WAIT</b>\n<pre>" + "\n".join(esc(x) for x in tbl) +
                      f"</pre>\nSumber {esc(source)}. Simulasi, bukan order live.")

    def handle_command(self, args: list[str], chat: Any, thread: Optional[int]) -> None:
        sub = args[0].lower() if args else "status"
        if sub == "status":
            self.tg.send_text(self.status_text(), chat=chat, thread=thread)
        elif sub in {"cek", "scan", "check"}:
            for pair in PAIRS:
                try:
                    png, cap = self.diagnostic(pair)
                    if png:
                        self.tg.send_photo(png, cap, chat=chat, thread=thread)
                    else:
                        self.tg.send_text(cap, chat=chat, thread=thread)
                except Exception as exc:
                    self.tg.send_text(f"{pair} gagal: {esc(exc)}", chat=chat, thread=thread)
        elif sub in {"capital", "modal"}:
            self.tg.send_text(self.capital.summary_text(), chat=chat, thread=thread)
        elif sub == "rekap":
            days = int(args[1]) if len(args) > 1 and args[1].isdigit() else 7
            self.tg.send_text(self.capital.recap_text(days), chat=chat, thread=thread)
        elif sub == "be":
            if len(args) > 1 and args[1].lower() in {"on", "off"}:
                self.state["be_enabled"] = args[1].lower() == "on"
                self.save()
                self.tg.send_text(f"BE/SL+ otomatis {'AKTIF' if self.state['be_enabled'] else 'NONAKTIF'} untuk pengelolaan berikutnya.",
                                  chat=chat, thread=thread)
            else:
                self.tg.send_text(f"BE/SL+ {'ON' if self.state['be_enabled'] else 'OFF'}\nBE trigger +{BE_TRIGGER_R:.2f}R → kunci +{BE_OFFSET_R:.2f}R\n"
                                  f"SL+ trigger +{SLPLUS_TRIGGER_R:.2f}R → kunci +{SLPLUS_LOCK_R:.2f}R\n"
                                  "Stop diproses dari candle M1 closed; stop baru berlaku mulai candle berikutnya.", chat=chat, thread=thread)
        elif sub == "offset":
            try:
                self.state["offset"] = float(args[1].replace(",", "."))
                self.save()
                self.tg.send_text(f"Offset diset {self.offset:+.2f}", chat=chat, thread=thread)
            except (IndexError, ValueError):
                self.tg.send_text("Format: /tpb offset -5.00", chat=chat, thread=thread)
        elif sub in {"bt", "backtest"}:
            days = int(args[1]) if len(args) > 1 and args[1].isdigit() else 14
            days = max(3, min(days, 60))
            self.tg.send_text(f"Backtest TREND PULLBACK M5 {days} hari dimulai (pendekatan, simulasi)…", chat=chat, thread=thread)
            threading.Thread(target=self._bt_job, args=(days, chat, thread), daemon=True).start()
        else:
            self.tg.send_text("Perintah: /tpb status | cek | capital | rekap [hari] | be [on|off] | offset <n> | bt [hari]",
                              chat=chat, thread=thread)

    def _bt_job(self, days: int, chat: Any, thread: Optional[int]) -> None:
        lines = [f"<b>BACKTEST {self.title} — {days} hari</b>",
                 "Entry open candle M5 berikutnya; SL menang bila SL&TP satu candle; BE/SL+ per candle M5 (lebih kasar dari live).", ""]
        for pair in PAIRS:
            try:
                rows, source = mdata.fetch_history(pair, TF, days)
                r = backtest(pair, rows)
                k = r["kinds"]
                lines.append(f"{pair} ({esc(source)}, {len(rows)} candle): trade {r['n']} | TP {k.get('TP', 0)} | SL+ {k.get('SL+', 0)} | "
                             f"BE+ {k.get('BE+', 0)} | SL {k.get('SL', 0)} | TO {k.get('TIMEOUT', 0)} | win {r['wr']:.0f}% | "
                             f"net {r['net_r']:+.2f}R | PF {r['pf']:.2f}")
            except Exception as exc:
                lines.append(f"{pair}: GAGAL — {esc(exc)}")
        lines.append("\nHasil historis bukan jaminan profit.")
        self.tg.send_text("\n".join(lines), chat=chat, thread=thread)
''',
    "liq_v2": r'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ENGINE 3 — LIQUIDITY SNIPER 1M v2.0 (port LENGKAP dari bot.py).

Strategi: SHADOW FAKEOUT 1M (sweep, reclaim, rejection) + Ichimoku Tenkan/Kijun/Kumo.
Semua detail bot.py dipertahankan: live paralel satu thread per pair + cache candle, anti rate-limit
(limiter per host, cooldown + backoff, failover Bybit -> Yahoo), sinyal sebagai chart PNG + caption
(alasan, entry, SL, TP, lot, risiko), paper account (modal Rp1.000.000, margin call -> top-up otomatis,
BE+ + trailing SL+), backtest trailing vs TP/SL murni, watchdog feed.

Perubahan agar independen di dalam bot liquid v4:
- semua env strategi memakai awalan LV2_ (mis. LV2_RR_TARGET, LV2_PAIRS, LV2_BE_TRIGGER_R)
- state file sendiri: paper_state_v2.json
- Telegram lewat hub bersama; kirim ke topic LV2_TOPIC_ID
- tidak ada polling Telegram sendiri; perintah lewat bot utama: /v2 cek | saldo | posisi | backtest | stats | reset
"""
from __future__ import annotations

import argparse
import csv
import html
import io
import json
import math
import os
import queue
import random
import re
import signal as signal_mod
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from dataclasses import dataclass, asdict, fields as dc_fields
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from .common import (TelegramError, TelegramHub, TelegramView, build_multipart, data_dir as _shared_data_dir,
                     env_topic as _env_topic)

try:  # chart opsional
    from PIL import Image, ImageDraw, ImageFont
except Exception:  # pragma: no cover
    Image = ImageDraw = ImageFont = None

VERSION = "2.0.0"


# ----------------------------------------------------------------------------
# Konfigurasi (semua bisa dioverride lewat environment variable di Railway)
# ----------------------------------------------------------------------------
_SHARED_ENV = {"DATA_DIR", "RAILWAY_VOLUME_MOUNT_PATH", "TELEGRAM_API_BASE", "BYBIT_BASES", "TZ_LABEL",
               "TZ_OFFSET_HOURS", "TELEGRAM_BOT_TOKEN", "BOT_TELEGRAM_CHAT_ID"}


def _env(name: str, default: str = "") -> str:
    """Env strategi engine ini memakai awalan LV2_ supaya tidak bentrok dengan engine lain."""
    if name in _SHARED_ENV:
        return (os.getenv(name) or default).strip()
    return (os.getenv("LV2_" + name) or default).strip()


def env_float(name: str, default: float) -> float:
    try:
        return float(_env(name) or default)
    except ValueError:
        return float(default)


def env_int(name: str, default: int) -> int:
    try:
        return int(float(_env(name) or default))
    except ValueError:
        return int(default)


def env_bool(name: str, default: bool) -> bool:
    v = _env(name).lower()
    return default if not v else v not in {"0", "false", "no", "off"}


INTERVAL_MS = 60_000
YAHOO_XAU = "XAUUSD=X"
YAHOO_BTC = "BTC-USD"
BYBIT_BTC = "BTCUSDT"
BYBIT_XAUT = "XAUTUSDT"  # emas tokenized; bukan feed XAUUSD broker
BYBIT_BASES = [b.strip().rstrip("/") for b in
               _env("BYBIT_BASES", "https://api.bybit.com,https://api.bytick.com").split(",") if b.strip()]
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36"

# Strategi (sama seperti v1.1)
SWING_LOOKBACK = 12
ATR_PERIOD = 14
TENKAN_PERIOD = 9
KIJUN_PERIOD = 26
SENKOU_B_PERIOD = 52
CLOUD_SHIFT = 26
RR_TARGET = env_float("RR_TARGET", 1.5)
MAX_HOLD_BARS = env_int("MAX_HOLD_BARS", 60)
MAX_RISK_ATR = 3.0
STOP_ATR_BUFFER = 0.08

# BE + trailing (satuan R = jarak entry ke SL awal)
TRAILING_ON = env_bool("TRAILING", True)
BE_TRIGGER_R = env_float("BE_TRIGGER_R", 0.8)    # profit puncak >= 0.8R -> SL ke BE+
BE_OFFSET_R = env_float("BE_OFFSET_R", 0.1)      # BE digeser +0.1R supaya menutup biaya
TRAIL_START_R = env_float("TRAIL_START_R", 1.0)  # mulai trailing kontinu di 1.0R
TRAIL_GAP_R = env_float("TRAIL_GAP_R", 0.5)      # SL = puncak profit - 0.5R
TRAIL_NOTIFY_R = env_float("TRAIL_NOTIFY_R", 0.25)  # kabari Telegram tiap SL naik >= 0.25R

# Paper account
PAPER_START_IDR = env_float("PAPER_START_IDR", 1_000_000)
PAPER_TOPUP_IDR = env_float("PAPER_TOPUP_IDR", 500_000)
USDIDR = env_float("USDIDR", 17_900)       # kurs statis untuk konversi P/L ke rupiah
LEVERAGE = max(1.0, env_float("LEVERAGE", 200))
RISK_PCT = env_float("RISK_PCT", 2.0)       # target risiko per trade (% equity)
MAX_RISK_PCT = env_float("MAX_RISK_PCT", 15.0)  # lot minimum yang risikonya lebih besar -> skip
MC_LEVEL = env_float("MC_LEVEL", 100.0)     # margin level (%) yang dianggap margin call
MAX_TOPUPS = env_int("MAX_TOPUPS", 0)       # 0 = tak terbatas
COSTS_ON = env_bool("COSTS", True)          # spread + fee ikut dihitung

# Feed
BOOT_CANDLES = min(1000, env_int("BOOT_CANDLES", 700))
CACHE_MAX = 1500
POLL_OFFSET_S = env_float("POLL_OFFSET_S", 1.0)
STALE_ALERT_S = env_int("STALE_ALERT_S", 300)
SWITCH_BACK_S = 600
CHART_BARS = env_int("CHART_BARS", 90)
BT_COOLDOWN_S = 60

DATA_DIR = _shared_data_dir()
STATE_FILE = os.path.join(DATA_DIR, "paper_state_v2.json")

TZ_HOURS = env_float("TZ_OFFSET_HOURS", 7.0)
TZ_LABEL = _env("TZ_LABEL", "WIB")
LOCAL_TZ = timezone(timedelta(hours=TZ_HOURS))

# Spesifikasi instrumen untuk paper account (qty dalam satuan dasar: oz / BTC)
SPECS: dict[str, dict[str, Any]] = {
    "XAUUSD": {"unit": "oz", "min_qty": 1.0, "step": 1.0, "dec": 2,        # 0.01 lot = 1 oz
               "spread": env_float("SPREAD_XAUUSD", 0.30), "fee": env_float("FEE_XAUUSD", 0.0)},
    "BTCUSDT": {"unit": "BTC", "min_qty": 0.001, "step": 0.001, "dec": 1,
                "spread": env_float("SPREAD_BTCUSDT", 0.20), "fee": env_float("FEE_BTCUSDT", 0.00055)},
}
ACTIVE_PAIRS = [p for p in (x.strip().upper() for x in _env("PAIRS", "XAUUSD,BTCUSDT").split(","))
                if p in SPECS] or list(SPECS)


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z {msg}", flush=True)


# ----------------------------------------------------------------------------
# Model data & format
# ----------------------------------------------------------------------------
@dataclass(frozen=True)
class Candle:
    ts: int        # UTC ms, waktu OPEN candle
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    @property
    def time_utc(self) -> str:
        return datetime.fromtimestamp(self.ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


@dataclass
class Signal:
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


@dataclass
class Trade:
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


def fmt_price(pair: str, x: float) -> str:
    return f"{x:.{SPECS[pair]['dec']}f}"


def fmt_local(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, LOCAL_TZ).strftime("%d-%m %H:%M") + f" {TZ_LABEL}"


def rp(usd: float) -> str:
    v = usd * USDIDR
    n = int(round(abs(v)))
    s = f"{n:,}".replace(",", ".")
    return f"-Rp{s}" if (v < 0 and n) else f"Rp{s}"


def fmt_qty(pair: str, qty: float) -> str:
    if pair == "XAUUSD":
        return f"{qty / 100:.2f} lot ({qty:g} oz)"
    return f"{qty:.3f} BTC"


def strip_tags(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", text))


def normalize_pair(value: str) -> str:
    v = (value or "").strip().upper().replace("/", "").replace("-", "")
    if v in {"XAU", "GOLD", "XAUUSD", "XAUUSD=X", "XAUT", "XAUTUSDT"}:
        return "XAUUSD"
    if v in {"BTC", "BTCUSDT", "BTCUSD", "BITCOIN"}:
        return "BTCUSDT"
    if v in {"ALL", "SEMUA", "BOTH"}:
        return "ALL"
    raise ValueError("Pair tidak dikenali. Pilih XAUUSD, BTCUSDT, atau ALL.")


# ----------------------------------------------------------------------------
# Data bersih + indikator + sinyal (logika sama seperti v1.1)
# ----------------------------------------------------------------------------
def _clean_candles(candles: list[Candle], now_ms: Optional[int] = None) -> list[Candle]:
    """Urutkan, buang duplikat/invalid, dan buang candle yang belum close."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    by_ts: dict[int, Candle] = {}
    for c in candles:
        vals = (c.open, c.high, c.low, c.close)
        if not all(math.isfinite(x) and x > 0 for x in vals):
            continue
        if c.high < max(c.open, c.close, c.low) or c.low > min(c.open, c.close, c.high):
            continue
        if c.ts + INTERVAL_MS > now_ms:  # candle 12:34 baru closed pada 12:35
            continue
        by_ts[c.ts] = c
    return [by_ts[k] for k in sorted(by_ts)]


def _rolling_mid(highs: list[float], lows: list[float], i: int, period: int) -> Optional[float]:
    if i + 1 < period:
        return None
    return (max(highs[i - period + 1:i + 1]) + min(lows[i - period + 1:i + 1])) / 2.0


def build_indicators(candles: list[Candle]) -> dict[str, list[Optional[float]]]:
    n = len(candles)
    highs = [c.high for c in candles]
    lows = [c.low for c in candles]
    closes = [c.close for c in candles]
    tenkan: list[Optional[float]] = [None] * n
    kijun: list[Optional[float]] = [None] * n
    span_a_raw: list[Optional[float]] = [None] * n
    span_b_raw: list[Optional[float]] = [None] * n
    cloud_a: list[Optional[float]] = [None] * n
    cloud_b: list[Optional[float]] = [None] * n
    tr: list[float] = [0.0] * n
    atr: list[Optional[float]] = [None] * n
    for i, c in enumerate(candles):
        tenkan[i] = _rolling_mid(highs, lows, i, TENKAN_PERIOD)
        kijun[i] = _rolling_mid(highs, lows, i, KIJUN_PERIOD)
        if tenkan[i] is not None and kijun[i] is not None:
            span_a_raw[i] = (tenkan[i] + kijun[i]) / 2.0
        span_b_raw[i] = _rolling_mid(highs, lows, i, SENKOU_B_PERIOD)
        prev_close = closes[i - 1] if i > 0 else c.close
        tr[i] = max(c.high - c.low, abs(c.high - prev_close), abs(c.low - prev_close))
        if i >= ATR_PERIOD - 1:
            atr[i] = sum(tr[i - ATR_PERIOD + 1:i + 1]) / ATR_PERIOD
        # Kumo yang tampil di chart dihitung 26 bar sebelumnya (tanpa look-ahead).
        source_i = i - CLOUD_SHIFT
        if source_i >= 0:
            cloud_a[i] = span_a_raw[source_i]
            cloud_b[i] = span_b_raw[source_i]
    return {"tenkan": tenkan, "kijun": kijun, "cloud_a": cloud_a, "cloud_b": cloud_b, "atr": atr}


def detect_signal(candles: list[Candle], i: int,
                  indicators: Optional[dict[str, list[Optional[float]]]] = None) -> Optional[Signal]:
    """Shadow Fakeout 1M + konfirmasi Ichimoku; candle closed saja, tanpa look-ahead."""
    if i < max(SWING_LOOKBACK + 2, SENKOU_B_PERIOD - 1, ATR_PERIOD + 2):
        return None
    if indicators is None:
        indicators = build_indicators(candles[:i + 1])
    c = candles[i]
    prev = candles[i - 1]
    atr = indicators["atr"][i]
    tenkan = indicators["tenkan"][i]
    kijun = indicators["kijun"][i]
    prev_tenkan = indicators["tenkan"][i - 1]
    prev_kijun = indicators["kijun"][i - 1]
    cloud_a = indicators["cloud_a"][i]
    cloud_b = indicators["cloud_b"][i]
    prev_cloud_a = indicators["cloud_a"][i - 1]
    prev_cloud_b = indicators["cloud_b"][i - 1]
    if atr is None or tenkan is None or kijun is None or prev_tenkan is None or prev_kijun is None or atr <= 0:
        return None
    prior = candles[i - SWING_LOOKBACK:i]
    prior_low = min(x.low for x in prior)
    prior_high = max(x.high for x in prior)
    candle_range = max(c.high - c.low, 1e-12)
    close_location = (c.close - c.low) / candle_range
    bullish_body = c.close > c.open and close_location >= 0.58
    bearish_body = c.close < c.open and close_location <= 0.42
    # Sweep likuiditas eksternal, close kembali ke dalam range, ada penolakan (wick).
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

    direction = ""
    reason = ""
    score = 0
    cloud_bull_confirm = False
    cloud_bear_confirm = False
    if None not in (cloud_a, cloud_b, prev_cloud_a, prev_cloud_b):
        cloud_top = max(float(cloud_a), float(cloud_b))
        cloud_bottom = min(float(cloud_a), float(cloud_b))
        prev_cloud_top = max(float(prev_cloud_a), float(prev_cloud_b))
        prev_cloud_bottom = min(float(prev_cloud_a), float(prev_cloud_b))
        cloud_bull_confirm = c.close > cloud_top or (prev.close <= prev_cloud_top and c.close > cloud_top)
        cloud_bear_confirm = c.close < cloud_bottom or (prev.close >= prev_cloud_bottom and c.close < cloud_bottom)
    if sweep_low and bullish_body and bullish_rejection:
        score += 2
        if c.close > tenkan and (cross_up_tenkan or cross_up_kijun or tk_turn_up):
            score += 2
            direction = "BUY"
            reason = "SHADOW FAKEOUT: sell-side sweep + reclaim/rejection + Ichimoku reversal"
        elif c.close > tenkan and c.close >= prev.close and tenkan >= prev_tenkan:
            score += 1
            direction = "BUY"
            reason = "SHADOW FAKEOUT: sell-side sweep + reclaim/rejection + Tenkan momentum"
        if direction == "BUY" and cloud_bull_confirm:
            score += 1
            reason += " + Kumo bullish confirmation"
    if sweep_high and bearish_body and bearish_rejection:
        score += 2
        if c.close < tenkan and (cross_down_tenkan or cross_down_kijun or tk_turn_down):
            score += 2
            direction = "SELL"
            reason = "SHADOW FAKEOUT: buy-side sweep + reclaim/rejection + Ichimoku reversal"
        elif c.close < tenkan and c.close <= prev.close and tenkan <= prev_tenkan:
            score += 1
            direction = "SELL"
            reason = "SHADOW FAKEOUT: buy-side sweep + reclaim/rejection + Tenkan momentum"
        if direction == "SELL" and cloud_bear_confirm:
            score += 1
            reason += " + Kumo bearish confirmation"
    if not direction:
        return None

    buffer = max(atr * STOP_ATR_BUFFER, abs(c.close) * 0.00001)
    if direction == "BUY":
        stop = c.low - buffer
        entry_ref = c.close
        risk = entry_ref - stop
        target = entry_ref + RR_TARGET * risk
        level = prior_low
    else:
        stop = c.high + buffer
        entry_ref = c.close
        risk = stop - entry_ref
        target = entry_ref - RR_TARGET * risk
        level = prior_high
    if risk <= 0 or risk > MAX_RISK_ATR * atr:
        return None
    return Signal(symbol="", direction=direction, index=i, candle_ts=c.ts,
                  entry_reference=entry_ref, stop=stop, target=target, risk=risk,
                  atr=atr, reason=reason, score=score, sweep_level=level)


def latest_closed_signal(pair: str, candles: list[Candle]) -> tuple[Optional[Signal], dict[str, Any]]:
    inds = build_indicators(candles)
    idx = len(candles) - 1
    sig = detect_signal(candles, idx, inds) if idx >= 0 else None
    if sig:
        sig.symbol = normalize_pair(pair)
    c = candles[-1]
    return sig, {"price": c.close, "time": c.time_utc, "candle_ts": c.ts, "count": len(candles)}


# ----------------------------------------------------------------------------
# Manajemen posisi: SL awal -> BE+ -> trailing SL+ (dipakai live DAN backtest)
# ----------------------------------------------------------------------------
@dataclass
class Position:
    pair: str
    direction: str
    entry: float
    stop: float          # SL yang sedang aktif (bergeser)
    init_stop: float
    target: float
    risk: float          # |entry - SL awal| dalam harga = 1R
    qty: float = 0.0
    margin: float = 0.0
    risk_usd: float = 0.0
    signal_ts: int = 0
    last_ts: int = 0     # open-ts candle terakhir yang sudah diproses
    bars: int = 0
    mfe: float = 0.0     # profit puncak (satuan harga)
    stage: str = "INIT"  # INIT -> BE -> TRAIL
    notified_r: float = -9.0
    opened_at: float = 0.0
    reason: str = ""
    score: int = 0

    @property
    def d(self) -> int:
        return 1 if self.direction == "BUY" else -1

    def r_of(self, price: float) -> float:
        return self.d * (price - self.entry) / self.risk

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Position":
        names = {f.name for f in dc_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in names})


@dataclass
class BarResult:
    closed: bool = False
    outcome: str = ""
    price: float = 0.0


def _better(d: int, a: float, b: float) -> float:
    """Ratchet: stop hanya boleh bergerak ke arah profit."""
    return max(a, b) if d == 1 else min(a, b)


def cost_per_unit(pair: str, entry: float, exit_price: float) -> float:
    """Biaya round-turn per 1 unit: spread + fee taker kedua sisi."""
    if not COSTS_ON:
        return 0.0
    sp = SPECS[pair]
    return float(sp["spread"]) + float(sp["fee"]) * (entry + exit_price)


def manage_bar(pos: Position, bar: Candle, trailing: bool = True, max_hold: int = MAX_HOLD_BARS) -> BarResult:
    """Proses SATU candle closed. Stop dicek dengan nilai SEBELUM candle ini (tanpa look-ahead);
    bila SL & TP tersentuh di candle yang sama, SL dimenangkan (konservatif)."""
    d = pos.d
    pos.bars += 1
    stop, target = pos.stop, pos.target
    stage_outcome = {"INIT": "SL", "BE": "BE", "TRAIL": "SL+"}[pos.stage]
    if d == 1:
        stop_hit = bar.low <= stop
        tp_hit = bar.high >= target
        stop_fill = min(stop, bar.open)   # gap menembus stop -> fill di open (lebih buruk)
    else:
        stop_hit = bar.high >= stop
        tp_hit = bar.low <= target
        stop_fill = max(stop, bar.open)
    if stop_hit:
        return BarResult(True, stage_outcome, stop_fill)
    if tp_hit:
        return BarResult(True, "TP", target)

    fav = d * ((bar.high if d == 1 else bar.low) - pos.entry)
    if fav > pos.mfe:
        pos.mfe = fav
    if trailing and pos.risk > 0:
        mfe_r = pos.mfe / pos.risk
        new_stop = pos.stop
        if mfe_r >= BE_TRIGGER_R:
            new_stop = _better(d, new_stop, pos.entry + d * BE_OFFSET_R * pos.risk)
            if pos.stage == "INIT":
                pos.stage = "BE"
        if mfe_r >= TRAIL_START_R:
            new_stop = _better(d, new_stop, pos.entry + d * (mfe_r - TRAIL_GAP_R) * pos.risk)
            pos.stage = "TRAIL"
        if new_stop != pos.stop:
            pos.stop = new_stop
            # Stop baru dibentuk dari high/low candle ini; kalau close sudah menembusnya,
            # harga pasti melewati stop itu setelah puncak -> kena di candle yang sama.
            if (d == 1 and bar.close <= new_stop) or (d == -1 and bar.close >= new_stop):
                return BarResult(True, {"BE": "BE", "TRAIL": "SL+"}.get(pos.stage, "SL"), new_stop)
    if pos.bars >= max_hold:
        return BarResult(True, "TIMEOUT", bar.close)
    return BarResult()


def net_r(pos: Position, fill: float) -> float:
    return (pos.d * (fill - pos.entry) - cost_per_unit(pos.pair, pos.entry, fill)) / pos.risk


# ----------------------------------------------------------------------------
# Backtest (memakai manage_bar yang sama dengan live)
# ----------------------------------------------------------------------------
def backtest(candles: list[Candle], pair: str, rr: float = RR_TARGET, max_hold: int = MAX_HOLD_BARS,
             trailing: bool = True) -> tuple[list[Trade], dict[str, Any]]:
    """Satu posisi per waktu; sinyal di close -> entry OPEN candle berikutnya; SL menang bila bentrok."""
    if len(candles) < 100:
        raise RuntimeError(f"Data tidak cukup untuk backtest: {len(candles)} candle (minimum 100).")
    pair = normalize_pair(pair)
    indicators = build_indicators(candles)
    trades: list[Trade] = []
    i = max(SENKOU_B_PERIOD - 1, SWING_LOOKBACK + 2, ATR_PERIOD + 2)
    while i < len(candles) - 1:
        sig = detect_signal(candles, i, indicators)
        if sig is None:
            i += 1
            continue
        entry_i = i + 1
        entry = candles[entry_i].open
        d = 1 if sig.direction == "BUY" else -1
        risk = d * (entry - sig.stop)
        if risk <= 0 or risk > MAX_RISK_ATR * sig.atr:
            i += 1
            continue
        target = entry + d * rr * risk
        pos = Position(pair=pair, direction=sig.direction, entry=entry, stop=sig.stop, init_stop=sig.stop,
                       target=target, risk=risk, signal_ts=candles[i].ts, last_ts=candles[i].ts,
                       reason=sig.reason, score=sig.score)
        exit_j: Optional[int] = None
        outcome, fill = "TIMEOUT", candles[-1].close
        for j in range(entry_i, len(candles)):
            res = manage_bar(pos, candles[j], trailing=trailing, max_hold=max_hold)
            if res.closed:
                exit_j, outcome, fill = j, res.outcome, res.price
                break
        if exit_j is None:  # data habis sebelum posisi selesai
            exit_j, outcome, fill = len(candles) - 1, "TIMEOUT", candles[-1].close
        result_r = net_r(pos, fill)
        trades.append(Trade(
            symbol=pair, direction=sig.direction, signal_time=candles[i].time_utc,
            entry_time=candles[entry_i].time_utc, exit_time=candles[exit_j].time_utc,
            entry=entry, stop=sig.stop, target=target, exit_price=fill, result_r=result_r,
            outcome=outcome, bars_held=exit_j - entry_i + 1, reason=sig.reason))
        i = exit_j + 1

    wins = sum(1 for t in trades if t.result_r > 0)
    losses = sum(1 for t in trades if t.result_r < 0)
    flats = len(trades) - wins - losses
    total = sum(t.result_r for t in trades)
    gp = sum(t.result_r for t in trades if t.result_r > 0)
    gl = -sum(t.result_r for t in trades if t.result_r < 0)
    equity = peak = max_dd = 0.0
    for t in trades:
        equity += t.result_r
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    outcomes = {k: sum(1 for t in trades if t.outcome == k) for k in ("TP", "SL+", "BE", "SL", "TIMEOUT")}
    summary = {
        "symbol": pair, "candles": len(candles), "start": candles[0].time_utc, "end": candles[-1].time_utc,
        "trades": len(trades), "wins": wins, "losses": losses, "flats": flats,
        "winrate_pct": (wins / len(trades) * 100.0) if trades else 0.0,
        "net_r": total, "avg_r": (total / len(trades)) if trades else 0.0,
        "max_drawdown_r": max_dd, "profit_factor": (gp / gl) if gl > 0 else (math.inf if gp > 0 else 0.0),
        "outcomes": outcomes, "rr": rr, "max_hold": max_hold, "trailing": trailing,
    }
    return trades, summary


def _pf(x: float) -> str:
    return "∞" if math.isinf(x) else f"{x:.2f}"


def format_backtest(s: dict[str, Any], base: Optional[dict[str, Any]] = None) -> str:
    o = s["outcomes"]
    lines = [
        f"BACKTEST LIQUIDITY SNIPER 1M — {s['symbol']}",
        f"Data: {s['start']} → {s['end']} ({s['candles']} candle valid)",
        f"Mode: {'BE+trailing SL+' if s['trailing'] else 'TP/SL murni'} | TP {s['rr']:.1f}R | "
        f"biaya {'ON' if COSTS_ON else 'OFF'}",
        f"Trade: {s['trades']} | WR {s['winrate_pct']:.1f}% ({s['wins']}W/{s['losses']}L/{s['flats']} flat)",
        f"TP {o['TP']} | SL+ {o['SL+']} | BE {o['BE']} | SL {o['SL']} | Timeout {o['TIMEOUT']}",
        f"Net {s['net_r']:+.2f}R | Avg {s['avg_r']:+.3f}R | PF {_pf(s['profit_factor'])} | "
        f"Max DD {s['max_drawdown_r']:.2f}R",
    ]
    if base is not None:
        lines.append(f"Pembanding TP/SL murni: Net {base['net_r']:+.2f}R | WR {base['winrate_pct']:.1f}% | "
                     f"PF {_pf(base['profit_factor'])} | DD {base['max_drawdown_r']:.2f}R")
    lines += [
        "Entry OPEN candle berikutnya; SL menang bila SL & TP tersentuh satu candle. "
        "Belum ada slippage/latency/funding; spread+fee sesuai env.",
        "Simulasi historis, bukan jaminan hasil live.",
    ]
    return "\n".join(lines)


def trades_csv_bytes(trades: list[Trade]) -> bytes:
    buf = io.StringIO()
    names = list(Trade.__dataclass_fields__.keys())
    writer = csv.DictWriter(buf, fieldnames=names)
    writer.writeheader()
    for t in trades:
        writer.writerow(t.__dict__)
    return buf.getvalue().encode("utf-8")


# ----------------------------------------------------------------------------
# HTTP + limiter + provider data (anti rate-limit)
# ----------------------------------------------------------------------------
class Throttled(Exception):
    """Server menolak karena rate limit / blokir sementara (403/418/429/503)."""

    def __init__(self, code: int, retry_after: Optional[float] = None):
        super().__init__(f"HTTP {code}")
        self.code = code
        self.retry_after = retry_after


class Limiter:
    """Jarak minimum antar request ke satu host; thread-safe."""

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.min_interval
        delay = start - now
        if delay > 0:
            time.sleep(delay)


BYBIT_LIMITER = Limiter(0.30)   # jauh di bawah batas IP Bybit
YAHOO_LIMITER = Limiter(8.0)    # Yahoo gampang 429, beri jarak lebar


def http_get_json(url: str, limiter: Optional[Limiter] = None, timeout: int = 15) -> dict[str, Any]:
    if limiter is not None:
        limiter.wait()
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 418, 429, 503):
            retry_after = None
            try:
                header = exc.headers.get("Retry-After")
                retry_after = float(header) if header else None
            except (TypeError, ValueError, AttributeError):
                retry_after = None
            raise Throttled(exc.code, retry_after) from exc
        raise


class Provider:
    """Satu sumber data. Gagal/ditolak -> masuk cooldown eksponensial (circuit breaker)."""
    name = "?"

    def __init__(self) -> None:
        self.blocked_until = 0.0
        self.fail_streak = 0
        self.last_error = ""

    def available(self) -> bool:
        return time.time() >= self.blocked_until

    def ok(self) -> None:
        self.fail_streak = 0
        self.last_error = ""

    def fail(self, err: Any, throttled: bool = False, retry_after: Optional[float] = None) -> None:
        self.fail_streak += 1
        self.last_error = str(err)[:120]
        base = 45.0 if throttled else 6.0
        delay = base * (2 ** min(self.fail_streak - 1, 5))
        if retry_after:
            delay = max(delay, float(retry_after))
        delay = min(delay, 900.0) + random.uniform(0.0, 2.0)
        self.blocked_until = time.time() + delay
        log(f"[feed] {self.name} cooldown {delay:.0f}s ({'throttled' if throttled else 'error'}: {self.last_error})")

    def cooldown_left(self) -> float:
        return max(0.0, self.blocked_until - time.time())

    def recent(self, limit: int) -> list[Candle]:
        raise NotImplementedError


def _parse_bybit(rows: list[Any]) -> list[Candle]:
    out: list[Candle] = []
    for row in rows:
        try:
            out.append(Candle(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])))
        except (IndexError, TypeError, ValueError):
            continue
    return out


class BybitProvider(Provider):
    def __init__(self, symbol: str, category: str):
        super().__init__()
        self.symbol = symbol
        self.category = category
        self.name = f"Bybit {symbol} ({category})"
        self.base_i = 0

    def kline(self, **extra: Any) -> list[Candle]:
        params = {"category": self.category, "symbol": self.symbol, "interval": "1", **extra}
        query = urllib.parse.urlencode(params)
        last_exc: Optional[Exception] = None
        for k in range(len(BYBIT_BASES)):
            idx = (self.base_i + k) % len(BYBIT_BASES)
            try:
                payload = http_get_json(f"{BYBIT_BASES[idx]}/v5/market/kline?{query}", BYBIT_LIMITER, 15)
                code = payload.get("retCode")
                if code in (10006, 10018):
                    raise Throttled(429)
                if code != 0:
                    raise RuntimeError(f"Bybit retCode={code}: {payload.get('retMsg', '')}")
                self.base_i = idx  # domain yang berhasil dijadikan default
                return _parse_bybit((payload.get("result") or {}).get("list") or [])
            except Exception as exc:  # coba domain cadangan
                last_exc = exc
        assert last_exc is not None
        raise last_exc

    def recent(self, limit: int) -> list[Candle]:
        return _clean_candles(self.kline(limit=max(1, min(int(limit), 1000))))


class YahooProvider(Provider):
    def __init__(self, symbol: str):
        super().__init__()
        self.symbol = symbol
        self.name = f"Yahoo {symbol}"

    def chart(self, start_s: int, end_s: int) -> list[Candle]:
        params = urllib.parse.urlencode({"period1": int(start_s), "period2": int(end_s), "interval": "1m",
                                         "includePrePost": "true", "events": "div,splits"})
        path = f"{urllib.parse.quote(self.symbol, safe='=-')}?{params}"
        payload: Optional[dict[str, Any]] = None
        last_exc: Optional[Exception] = None
        for host in ("query1", "query2"):
            try:
                payload = http_get_json(f"https://{host}.finance.yahoo.com/v8/finance/chart/{path}", YAHOO_LIMITER, 20)
                break
            except Exception as exc:
                last_exc = exc
        if payload is None:
            assert last_exc is not None
            raise last_exc
        chart = payload.get("chart", {})
        if chart.get("error"):
            raise RuntimeError(f"Yahoo: {chart['error']}")
        results = chart.get("result") or []
        if not results:
            raise RuntimeError(f"Yahoo tidak mengembalikan candle {self.symbol}")
        res = results[0]
        stamps = res.get("timestamp") or []
        qlist = res.get("indicators", {}).get("quote") or []
        if not qlist:
            raise RuntimeError("Yahoo: quote kosong")
        q = qlist[0]
        opens, highs, lows, closes = q.get("open", []), q.get("high", []), q.get("low", []), q.get("close", [])
        vols = q.get("volume", [])
        out: list[Candle] = []
        for i, ts in enumerate(stamps):
            try:
                o, h, l, c = opens[i], highs[i], lows[i], closes[i]
                if o is None or h is None or l is None or c is None:
                    continue
                v = vols[i] if i < len(vols) and vols[i] is not None else 0
                out.append(Candle(int(ts * 1000), float(o), float(h), float(l), float(c), float(v or 0)))
            except (IndexError, TypeError, ValueError):
                continue
        return _clean_candles(out)

    def recent(self, limit: int) -> list[Candle]:
        now = int(time.time())
        minutes = min(max(int(limit), 10) + 5, 7 * 1440)
        return self.chart(now - minutes * 60, now)


# Registry bersama: cooldown dipakai live feed DAN backtest sehingga tidak saling menabrak.
PROVIDERS: dict[str, list[Provider]] = {
    "XAUUSD": [BybitProvider(BYBIT_XAUT, "spot"), YahooProvider(YAHOO_XAU)],
    "BTCUSDT": [BybitProvider(BYBIT_BTC, "linear"), YahooProvider(YAHOO_BTC)],
}


class CandleFeed:
    """Cache candle closed per pair. Satu thread pemilik per feed; pembaca memakai snapshot()."""

    def __init__(self, pair: str, providers: list[Provider]):
        self.pair = pair
        self.providers = providers
        self.cache: list[Candle] = []
        self.lock = threading.Lock()
        self.active: Optional[Provider] = None
        self.switch_back_at = 0.0

    def snapshot(self) -> list[Candle]:
        with self.lock:
            return list(self.cache)

    def latest_ts(self) -> int:
        with self.lock:
            return self.cache[-1].ts if self.cache else 0

    def _choose(self) -> Optional[Provider]:
        now = time.time()
        primary = self.providers[0]
        if (self.active is not None and self.active is not primary
                and now >= self.switch_back_at and primary.available()):
            return primary  # coba kembali ke sumber utama (dengan histeresis)
        if self.active is not None and self.active.available():
            return self.active
        for p in self.providers:
            if p.available():
                return p
        return None

    def _bootstrap(self, prov: Provider) -> None:
        rows = prov.recent(BOOT_CANDLES)
        if len(rows) < 80:
            raise RuntimeError(f"{prov.name}: hanya {len(rows)} candle, minimal 80")
        with self.lock:
            self.cache = rows[-CACHE_MAX:]

    def _merge(self, rows: list[Candle]) -> int:
        with self.lock:
            last = self.cache[-1].ts if self.cache else 0
            add = [c for c in sorted(rows, key=lambda x: x.ts) if c.ts > last]
            if add:
                self.cache.extend(add)
                if len(self.cache) > CACHE_MAX:
                    del self.cache[:len(self.cache) - CACHE_MAX]
            return len(add)

    def refresh(self, expected_ts: int) -> str:
        """Ambil candle terbaru. Return: 'ok' | 'blocked' | 'error'. Tidak pernah melempar."""
        prov = self._choose()
        if prov is None:
            return "blocked"
        try:
            if prov is not self.active or not self.snapshot():
                self._bootstrap(prov)
                changed = self.active is not None and prov is not self.active
                self.active = prov
                if prov is not self.providers[0]:
                    self.switch_back_at = time.time() + SWITCH_BACK_S
                prov.ok()
                log(f"[{self.pair}] feed {'pindah ke' if changed else 'siap:'} {prov.name} "
                    f"({len(self.snapshot())} candle)")
                return "ok"
            last = self.latest_ts()
            missing = max(0, (expected_ts - last) // INTERVAL_MS)
            if missing > 900:                      # gap terlalu lebar -> bangun ulang
                self._bootstrap(prov)
                prov.ok()
                return "ok"
            limit = 6 if missing <= 3 else min(missing + 4, 1000)
            rows = prov.recent(limit)
        except Throttled as exc:
            prov.fail(exc, throttled=True, retry_after=exc.retry_after)
            return "blocked"
        except Exception as exc:
            prov.fail(exc)
            return "error"
        prov.ok()
        self._merge(rows)
        return "ok"

    def status_line(self) -> str:
        ts = self.latest_ts()
        age = f"{time.time() - ts / 1000 - 60:.0f}s" if ts else "-"
        name = self.active.name if self.active else "-"
        cds = [f"{p.name}: cooldown {p.cooldown_left():.0f}s" for p in self.providers if not p.available()]
        return f"{self.pair}: {name} | umur candle {age} | {len(self.snapshot())} candle" + \
            (f" | {'; '.join(cds)}" if cds else "")


# ----------------------------------------------------------------------------
# Riwayat panjang untuk backtest (di-cache supaya tidak membombardir API)
# ----------------------------------------------------------------------------
_HISTORY_CACHE: dict[tuple[str, int], tuple[float, list[Candle]]] = {}
_HISTORY_LOCK = threading.Lock()
HISTORY_TTL_S = 600


def _history_from(prov: Provider, days: int) -> list[Candle]:
    now_ms = int(time.time() * 1000)
    if isinstance(prov, BybitProvider):
        days = max(1, min(days, 30))
        start_ms = now_ms - days * 86400 * 1000
        cursor = now_ms - 1
        allc: list[Candle] = []
        seen_min: Optional[int] = None
        for _ in range(math.ceil(days * 1440 / 950) + 4):
            page = prov.kline(start=start_ms, end=cursor, limit=1000)
            if not page:
                break
            allc.extend(page)
            page_min = min(c.ts for c in page)
            if seen_min is not None and page_min >= seen_min:
                break
            seen_min = page_min
            if page_min <= start_ms or len(page) < 2:
                break
            cursor = page_min - 1
        return [c for c in _clean_candles(allc, now_ms) if c.ts >= start_ms]
    days = max(1, min(days, 7))
    assert isinstance(prov, YahooProvider)
    return prov.chart(int(time.time()) - days * 86400, int(time.time()))


def fetch_history(pair: str, days: int = 1) -> list[Candle]:
    pair = normalize_pair(pair)
    key = (pair, days)
    with _HISTORY_LOCK:
        hit = _HISTORY_CACHE.get(key)
        if hit and time.time() - hit[0] < HISTORY_TTL_S:
            return list(hit[1])
    errors: list[str] = []
    for prov in PROVIDERS[pair]:
        if not prov.available():
            errors.append(f"{prov.name}: cooldown {prov.cooldown_left():.0f}s")
            continue
        try:
            rows = _history_from(prov, days)
            if len(rows) < 100:
                raise RuntimeError(f"hanya {len(rows)} candle")
            prov.ok()
            with _HISTORY_LOCK:
                _HISTORY_CACHE[key] = (time.time(), rows)
            return list(rows)
        except Throttled as exc:
            prov.fail(exc, throttled=True, retry_after=exc.retry_after)
            errors.append(f"{prov.name}: {exc}")
        except Exception as exc:
            prov.fail(exc)
            errors.append(f"{prov.name}: {exc}")
    raise RuntimeError("Data " + pair + " gagal diambil — " + " | ".join(errors))


# ----------------------------------------------------------------------------
# Paper account: modal, lot, margin call + top-up otomatis, BE/trailing, statistik
# ----------------------------------------------------------------------------
class PaperAccount:
    """Semua angka internal dalam USD; tampilan dikonversi ke rupiah dengan USDIDR."""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.marks: dict[str, float] = {}
        self.loaded = False
        self._save_warned = False
        self._fresh()
        self.load()

    # ---- state ------------------------------------------------------------
    def _fresh(self) -> None:
        start = PAPER_START_IDR / USDIDR
        self.balance = start
        self.deposits = start
        self.topups = 0
        self.created = time.time()
        self.positions: dict[str, Position] = {}
        self.closed: list[dict[str, Any]] = []
        self.last_scanned: dict[str, int] = {}
        self.stats: dict[str, float] = {
            "trades": 0, "wins": 0, "losses": 0, "flats": 0, "tp": 0, "sl": 0, "be": 0, "slp": 0,
            "timeout": 0, "gross_win": 0.0, "gross_loss": 0.0, "sum_r": 0.0, "peak_net": 0.0, "max_dd": 0.0}

    def reset(self) -> None:
        with self.lock:
            self._fresh()
            self.save()

    def to_dict(self) -> dict[str, Any]:
        return {"version": 1, "created": self.created, "balance": self.balance, "deposits": self.deposits,
                "topups": self.topups, "positions": {k: asdict(v) for k, v in self.positions.items()},
                "closed": self.closed[-100:], "stats": self.stats, "last_scanned": self.last_scanned}

    def save(self) -> None:
        with self.lock:
            try:
                os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
                tmp = self.path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(self.to_dict(), fh)
                os.replace(tmp, self.path)
            except OSError as exc:
                if not self._save_warned:
                    self._save_warned = True
                    log(f"[paper] GAGAL simpan state ({exc}); saldo hanya di memori")

    def load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            self.balance = float(data["balance"])
            self.deposits = float(data["deposits"])
            self.topups = int(data.get("topups", 0))
            self.created = float(data.get("created", time.time()))
            self.positions = {k: Position.from_dict(v) for k, v in (data.get("positions") or {}).items()}
            self.closed = list(data.get("closed") or [])
            self.last_scanned = {k: int(v) for k, v in (data.get("last_scanned") or {}).items()}
            self.stats.update(data.get("stats") or {})
            self.loaded = True
            log(f"[paper] state dimuat: saldo {rp(self.balance)}, {len(self.positions)} posisi terbuka")
        except Exception as exc:
            log(f"[paper] state rusak, mulai baru ({exc})")
            self._fresh()

    def mark_scanned(self, pair: str, ts: int) -> None:
        with self.lock:
            self.last_scanned[pair] = ts
            self.save()

    def set_mark(self, pair: str, price: float) -> None:
        with self.lock:
            self.marks[pair] = price

    # ---- hitungan ---------------------------------------------------------
    @staticmethod
    def floating(pos: Position, price: float) -> float:
        return pos.d * (price - pos.entry) * pos.qty

    def equity(self) -> float:
        with self.lock:
            return self.balance + sum(self.floating(p, self.marks.get(p.pair, p.entry))
                                      for p in self.positions.values())

    def used_margin(self) -> float:
        with self.lock:
            return sum(p.margin for p in self.positions.values())

    def net_pnl(self) -> float:
        return self.equity() - self.deposits

    def _topup(self) -> bool:
        if MAX_TOPUPS and self.topups >= MAX_TOPUPS:
            return False
        dep = PAPER_TOPUP_IDR / USDIDR
        self.balance += dep
        self.deposits += dep
        self.topups += 1
        return True

    # ---- buka posisi ------------------------------------------------------
    def plan_size(self, pair: str, entry: float, risk_unit: float) -> tuple[float, str]:
        spec = SPECS[pair]
        eq = self.equity()
        free = eq - self.used_margin()
        if eq <= 0 or free <= 0 or risk_unit <= 0:
            return 0.0, "margin"
        step, min_qty = float(spec["step"]), float(spec["min_qty"])
        q_risk = eq * RISK_PCT / 100.0 / risk_unit
        q_margin = free * LEVERAGE * 0.95 / entry
        q = math.floor(min(q_risk, q_margin) / step + 1e-9) * step
        status = "ok"
        if q < min_qty:
            if min_qty * entry / LEVERAGE > free * 0.95:
                return 0.0, "margin"
            q, status = min_qty, "min_lot"
        q = round(q, 6)
        if q * risk_unit / eq * 100.0 > MAX_RISK_PCT:
            return 0.0, "risk"
        return q, status

    def open_from_signal(self, pair: str, sig: Signal, entry: float) -> tuple[Optional[Position], dict[str, Any]]:
        info: dict[str, Any] = {"skip": None, "status": "", "topups": []}
        with self.lock:
            if pair in self.positions:
                info["skip"] = f"posisi {pair} masih terbuka"
                return None, info
            risk_unit = abs(entry - sig.stop)
            qty, status = 0.0, "margin"
            for _ in range(6):
                qty, status = self.plan_size(pair, entry, risk_unit)
                if qty > 0 or status != "margin":
                    break
                if not self._topup():
                    break
                info["topups"].append(f"💸 Modal tidak cukup untuk lot minimum → top-up otomatis "
                                      f"+{rp(PAPER_TOPUP_IDR / USDIDR)} ({self.topups}×)")
            if qty <= 0:
                info["skip"] = {"margin": "modal/margin tidak cukup untuk lot minimum",
                                "risk": f"risiko lot minimum > {MAX_RISK_PCT:.0f}% equity"}.get(status, "ukuran lot 0")
                return None, info
            pos = Position(pair=pair, direction=sig.direction, entry=entry, stop=sig.stop, init_stop=sig.stop,
                           target=sig.target, risk=risk_unit, qty=qty, margin=qty * entry / LEVERAGE,
                           risk_usd=qty * risk_unit, signal_ts=sig.candle_ts, last_ts=sig.candle_ts,
                           opened_at=time.time(), reason=sig.reason, score=sig.score)
            self.positions[pair] = pos
            self.marks[pair] = entry
            info["status"] = status
            info["risk_pct"] = pos.risk_usd / max(self.equity(), 1e-9) * 100.0
            self.save()
            return pos, info

    # ---- kelola posisi per candle ----------------------------------------
    def advance(self, pair: str, candles: list[Candle]) -> list[str]:
        """Proses candle closed baru untuk posisi pair ini. Return daftar pesan Telegram (HTML)."""
        out: list[str] = []
        with self.lock:
            pos = self.positions.get(pair)
            if pos is None:
                return out
            todo = [c for c in candles if c.ts > pos.last_ts]
            if not todo:
                return out
            for bar in todo:
                old_stage = pos.stage
                old_stop = pos.stop
                res = manage_bar(pos, bar, trailing=TRAILING_ON, max_hold=MAX_HOLD_BARS)
                pos.last_ts = bar.ts
                if res.closed:
                    out.append(self._close(pos, res.outcome, res.price))
                    out.extend(self._ensure_solvent())
                    break
                if pos.stop != old_stop:
                    msg = self._trail_msg(pos, old_stage)
                    if msg:
                        out.append(msg)
                self.marks[pair] = bar.close
                mc = self._margin_check(pos, bar)
                if mc:
                    out.append(mc)
            if pair in self.positions:
                self.marks[pair] = todo[-1].close
            self.save()
        return out

    def _trail_msg(self, pos: Position, old_stage: str) -> Optional[str]:
        r_stop = pos.r_of(pos.stop)
        sp = fmt_price(pos.pair, pos.stop)
        if pos.stage == "BE" and old_stage == "INIT":
            pos.notified_r = r_stop
            return f"🛡 <b>BE aktif</b> {pos.pair} {pos.direction}\nSL → <code>{sp}</code> ({r_stop:+.2f}R)"
        if pos.stage == "TRAIL" and (old_stage != "TRAIL" or r_stop - pos.notified_r >= TRAIL_NOTIFY_R):
            pos.notified_r = r_stop
            return f"🔒 <b>SL+</b> {pos.pair} {pos.direction} → <code>{sp}</code> ({r_stop:+.2f}R terkunci)"
        return None

    def _margin_check(self, pos: Position, bar: Candle) -> Optional[str]:
        used = self.used_margin()
        if used <= 0:
            return None
        worst = bar.low if pos.direction == "BUY" else bar.high
        eq = self.balance + self.floating(pos, worst) + sum(
            self.floating(p, self.marks.get(p.pair, p.entry)) for p in self.positions.values() if p is not pos)
        level = eq / used * 100.0
        if level > MC_LEVEL:
            return None
        first_level, n = level, 0
        while level <= MC_LEVEL and n < 20:
            if not self._topup():
                break
            eq += PAPER_TOPUP_IDR / USDIDR
            level = eq / used * 100.0
            n += 1
        if n == 0:
            return (f"⚠️ <b>MARGIN CALL</b> {pos.pair}: margin level {first_level:.0f}% ≤ {MC_LEVEL:.0f}% "
                    f"tapi batas top-up ({MAX_TOPUPS}×) habis")
        return (f"⚠️ <b>MARGIN CALL</b> {pos.pair}: margin level {first_level:.0f}% ≤ {MC_LEVEL:.0f}%\n"
                f"Top-up otomatis +{rp(n * PAPER_TOPUP_IDR / USDIDR)} ({self.topups}× total) → level {level:.0f}%\n"
                f"Saldo: <b>{rp(self.balance)}</b>")

    def _close(self, pos: Position, outcome: str, fill: float) -> str:
        cost_u = cost_per_unit(pos.pair, pos.entry, fill)
        gross = pos.d * (fill - pos.entry)
        pnl = (gross - cost_u) * pos.qty
        r = (gross - cost_u) / pos.risk
        self.balance += pnl
        del self.positions[pos.pair]
        s = self.stats
        s["trades"] += 1
        s["sum_r"] += r
        key = {"TP": "tp", "SL": "sl", "BE": "be", "SL+": "slp", "TIMEOUT": "timeout"}[outcome]
        s[key] += 1
        if pnl > 0:
            s["wins"] += 1
            s["gross_win"] += pnl
        elif pnl < 0:
            s["losses"] += 1
            s["gross_loss"] += -pnl
        else:
            s["flats"] += 1
        net_all = self.net_pnl()
        s["peak_net"] = max(s["peak_net"], net_all)
        s["max_dd"] = max(s["max_dd"], s["peak_net"] - net_all)
        self.closed.append({"t": time.time(), "pair": pos.pair, "dir": pos.direction, "outcome": outcome,
                            "entry": pos.entry, "exit": fill, "r": round(r, 3), "pnl_usd": round(pnl, 4)})
        self.closed = self.closed[-100:]
        label = {"TP": "✅ TP KENA", "SL": "❌ SL KENA", "BE": "🛡 BE (stop break-even)",
                 "SL+": "🔒 SL+ (trailing profit)", "TIMEOUT": "⏱ TIMEOUT"}[outcome]
        if outcome in ("BE", "SL+") and pnl < 0:
            label = "🛡 Stop di profit kotor, tapi bersih minus (kena biaya)"
        pct = net_all / self.deposits * 100.0 if self.deposits else 0.0
        return (f"{label} — <b>{pos.pair} {pos.direction}</b>\n"
                f"Entry <code>{fmt_price(pos.pair, pos.entry)}</code> → Exit <code>{fmt_price(pos.pair, fill)}</code> "
                f"({r:+.2f}R bersih, {pos.bars} menit)\n"
                f"P/L: <b>{rp(pnl)}</b> (biaya spread+fee {rp(cost_u * pos.qty)}) | Saldo: <b>{rp(self.balance)}</b>\n"
                f"Net P/L total (setelah deposit): {rp(net_all)} ({pct:+.1f}%)")

    def _ensure_solvent(self) -> list[str]:
        """Saldo habis/negatif setelah loss -> top-up otomatis (jika masih diizinkan)."""
        out: list[str] = []
        n = 0
        while self.balance <= 0 and n < 20:
            if not self._topup():
                out.append("💥 <b>Modal habis</b> dan batas top-up tercapai; paper trading berhenti sampai /reset.")
                break
            n += 1
        if n:
            out.append(f"⚠️ <b>MARGIN CALL / modal habis</b> → top-up otomatis +{rp(n * PAPER_TOPUP_IDR / USDIDR)} "
                       f"({self.topups}× total). Saldo: <b>{rp(self.balance)}</b>")
        return out

    # ---- laporan ----------------------------------------------------------
    def positions_text(self) -> str:
        with self.lock:
            if not self.positions:
                return "Tidak ada posisi paper terbuka."
            lines = []
            for p in self.positions.values():
                px = self.marks.get(p.pair, p.entry)
                lines.append(
                    f"• <b>{p.pair} {p.direction}</b> {fmt_qty(p.pair, p.qty)}\n"
                    f"  Entry <code>{fmt_price(p.pair, p.entry)}</code> | SL <code>{fmt_price(p.pair, p.stop)}</code>"
                    f" ({p.r_of(p.stop):+.2f}R, {p.stage}) | TP <code>{fmt_price(p.pair, p.target)}</code>\n"
                    f"  Harga <code>{fmt_price(p.pair, px)}</code> | floating {rp(self.floating(p, px))} "
                    f"({p.r_of(px):+.2f}R) | {p.bars} menit")
            return "\n".join(lines)

    def summary_text(self) -> str:
        with self.lock:
            s = self.stats
            eq = self.equity()
            net = eq - self.deposits
            pct = net / self.deposits * 100.0 if self.deposits else 0.0
            n = int(s["trades"])
            wr = s["wins"] / n * 100.0 if n else 0.0
            pf = (s["gross_win"] / s["gross_loss"]) if s["gross_loss"] > 0 else (math.inf if s["gross_win"] > 0 else 0.0)
            return "\n".join([
                "💼 <b>PAPER ACCOUNT</b>",
                f"Saldo: <b>{rp(self.balance)}</b> | Equity: {rp(eq)} (floating {rp(eq - self.balance)})",
                f"Total deposit: {rp(self.deposits)} (modal awal {rp(PAPER_START_IDR / USDIDR)} + "
                f"{self.topups}× top-up {rp(PAPER_TOPUP_IDR / USDIDR)})",
                f"<b>Net P/L: {rp(net)} ({pct:+.1f}%)</b> ← saldo dikurangi semua deposit",
                f"Trade: {n} | W {int(s['wins'])} / L {int(s['losses'])} / flat {int(s['flats'])} | "
                f"WR {wr:.1f}% | PF {_pf(pf)}",
                f"TP {int(s['tp'])} | SL+ {int(s['slp'])} | BE {int(s['be'])} | SL {int(s['sl'])} | "
                f"Timeout {int(s['timeout'])}",
                f"Total {s['sum_r']:+.2f}R | Max DD {rp(s['max_dd'])}",
                "",
                self.positions_text(),
            ])


# ----------------------------------------------------------------------------
# Chart PNG (Pillow) + caption sinyal
# ----------------------------------------------------------------------------
_FONTS: dict[int, Any] = {}


def _font(size: int) -> Any:
    if size in _FONTS:
        return _FONTS[size]
    try:
        font = ImageFont.truetype("DejaVuSans.ttf", size)
    except Exception:
        try:
            font = ImageFont.load_default(size=size)  # Pillow >= 10.1
        except Exception:
            font = ImageFont.load_default()
    _FONTS[size] = font
    return font


def render_chart(pair: str, candles: list[Candle], inds: dict[str, list[Optional[float]]], sig: Signal,
                 entry: float, n_bars: Optional[int] = None) -> Optional[bytes]:
    """PNG candlestick 1M + Kumo/Tenkan/Kijun + level sweep + zona Entry/SL/TP. None bila gagal."""
    if Image is None or not candles:
        return None
    try:
        return _render_chart(pair, candles, inds, sig, entry, n_bars or CHART_BARS)
    except Exception as exc:
        log(f"[chart] gagal render: {type(exc).__name__}: {exc}")
        return None


def _render_chart(pair: str, candles: list[Candle], inds: dict[str, list[Optional[float]]], sig: Signal,
                  entry: float, n_bars: int) -> bytes:
    S = 2                                  # supersampling untuk garis/bentuk; teks digambar setelah downscale
    W, H = 1100, 660
    L, R, T, B = 14, 140, 74, 46
    n_total = len(candles)
    n_win = min(n_bars, n_total)
    i0 = n_total - n_win
    win = candles[i0:]
    future = 16
    slot_w = (W - L - R) / (n_win + future)
    plot_h = H - T - B
    stop, target = sig.stop, sig.target
    lo = min(min(c.low for c in win), stop, target, entry)
    hi = max(max(c.high for c in win), stop, target, entry)
    pad = (hi - lo) * 0.08 or 1.0
    lo -= pad
    hi += pad

    def X(k: float) -> float:
        return L + (k + 0.5) * slot_w

    def Y(p: float) -> float:
        return T + (hi - p) / (hi - lo) * plot_h

    def Yc(p: float) -> float:
        return Y(min(max(p, lo), hi))

    def P(x: float, y: float) -> tuple[float, float]:
        return (x * S, y * S)

    BG, GRID = (13, 17, 23, 255), (32, 39, 50, 255)
    UP, DN = (38, 166, 154, 255), (239, 83, 80, 255)
    base = Image.new("RGBA", (W * S, H * S), BG)
    d = ImageDraw.Draw(base)
    ov = Image.new("RGBA", (W * S, H * S), (0, 0, 0, 0))
    od = ImageDraw.Draw(ov)

    ticks = [lo + (hi - lo) * t / 5 for t in range(6)]
    for p in ticks:
        d.line([P(L, Y(p)), P(W - R, Y(p))], fill=GRID, width=S)

    ca, cb = inds["cloud_a"], inds["cloud_b"]            # Kumo
    for k in range(n_win - 1):
        g = i0 + k
        a1, b1, a2, b2 = ca[g], cb[g], ca[g + 1], cb[g + 1]
        if a1 is None or b1 is None or a2 is None or b2 is None:
            continue
        col = (38, 166, 154, 46) if (a1 + a2) >= (b1 + b2) else (239, 83, 80, 46)
        od.polygon([P(X(k), Yc(max(a1, b1))), P(X(k + 1), Yc(max(a2, b2))),
                    P(X(k + 1), Yc(min(a2, b2))), P(X(k), Yc(min(a1, b1)))], fill=col)

    for key, col in (("tenkan", (255, 167, 38, 255)), ("kijun", (66, 165, 245, 255))):
        arr = inds[key]
        pts = [P(X(k), Yc(arr[i0 + k])) for k in range(n_win) if arr[i0 + k] is not None]
        if len(pts) > 1:
            d.line(pts, fill=col, width=S)

    bw = max(1.5, slot_w * 0.62)                          # candle
    for k, c in enumerate(win):
        x = X(k)
        col = UP if c.close >= c.open else DN
        d.line([P(x, Y(c.high)), P(x, Y(c.low))], fill=col, width=S)
        top, bot = Y(max(c.open, c.close)), Y(min(c.open, c.close))
        if bot - top < 1:
            bot = top + 1
        d.rectangle([P(x - bw / 2, top), P(x + bw / 2, bot)], fill=col)

    xs, xr = X(n_win - 1), W - R                          # zona + garis Entry/SL/TP
    od.rectangle([P(xs - slot_w / 2, T), P(xs + slot_w / 2, H - B)], fill=(255, 255, 255, 16))
    od.rectangle([P(xs, Y(max(entry, target))), P(xr, Y(min(entry, target)))], fill=(38, 166, 154, 40))
    od.rectangle([P(xs, Y(max(entry, stop))), P(xr, Y(min(entry, stop)))], fill=(239, 83, 80, 40))
    for price, col in ((entry, (200, 206, 214, 255)), (target, UP), (stop, DN)):
        d.line([P(xs, Y(price)), P(xr, Y(price))], fill=col, width=S * 2)

    ys = Yc(sig.sweep_level)                              # level likuiditas yang disapu
    x0 = X(max(0, n_win - 1 - SWING_LOOKBACK))
    xx = x0
    while xx < xs:
        d.line([P(xx, ys), P(min(xx + 9, xs), ys)], fill=(255, 213, 79, 255), width=S)
        xx += 15

    up = sig.direction == "BUY"                           # penanda candle sinyal
    last = win[-1]
    if up:
        ym = Y(last.low) + 8
        tri = [P(xs, ym), P(xs - 7, ym + 14), P(xs + 7, ym + 14)]
    else:
        ym = Y(last.high) - 8
        tri = [P(xs, ym), P(xs - 7, ym - 14), P(xs + 7, ym - 14)]
    d.polygon(tri, fill=UP if up else DN)

    img = Image.alpha_composite(base, ov)
    resample = getattr(Image, "Resampling", Image).LANCZOS
    img = img.resize((W, H), resample).convert("RGB")
    td = ImageDraw.Draw(img)
    f_big, f_mid, f_small = _font(26), _font(15), _font(13)
    muted = (140, 150, 165)

    td.text((L + 4, 14), f"{pair} · 1M · {sig.direction}", font=f_big, fill=(255, 255, 255))
    td.text((L + 4, 48), f"Liquidity Sniper 1M · close {fmt_local(sig.candle_ts + INTERVAL_MS)} · "
                         f"skor {sig.score}/5 · target {RR_TARGET:.1f}R", font=f_small, fill=muted)
    for p in ticks:
        td.text((W - R + 8, Y(p) - 8), fmt_price(pair, p), font=f_small, fill=(110, 120, 135))

    def tag(price: float, label: str, bg: tuple[int, int, int]) -> None:
        text = f"{label} {fmt_price(pair, price)}"
        y = Y(price)
        w = td.textlength(text, font=f_mid)
        td.rectangle([W - R + 2, y - 12, W - R + 2 + w + 12, y + 12], fill=bg)
        td.text((W - R + 8, y - 8), text, font=f_mid, fill=(255, 255, 255))

    tag(target, "TP", (30, 136, 124))
    tag(entry, "ENTRY", (96, 106, 120))
    tag(stop, "SL", (200, 60, 58))
    td.text((x0, ys - 18), "level likuiditas disapu", font=f_small, fill=(255, 213, 79))

    lx = L + 4
    for col, label in (((255, 167, 38), "Tenkan"), ((66, 165, 245), "Kijun"),
                       ((38, 166, 154), "Kumo"), ((255, 213, 79), "Level sweep")):
        td.rectangle([lx, H - 27, lx + 14, H - 23], fill=col)
        td.text((lx + 20, H - 35), label, font=f_small, fill=muted)
        lx += 20 + int(td.textlength(label, font=f_small)) + 22
    td.text((W - R - 250, H - 35), "Simulasi/paper — bukan ajakan transaksi", font=f_small, fill=(90, 100, 115))

    buf = io.BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()


def build_caption(pair: str, sig: Signal, entry: float, candle_ts: int, pos: Optional[Position],
                  info: dict[str, Any], account: PaperAccount) -> str:
    dec = SPECS[pair]["dec"]
    up = sig.direction == "BUY"
    p = lambda x: fmt_price(pair, x)  # noqa: E731
    risk = abs(entry - sig.stop)
    reward = abs(sig.target - entry)
    lines = [
        f"{'🟢' if up else '🔴'} <b>{sig.direction} {pair}</b> · Liquidity Sniper 1M",
        f"🕒 {fmt_local(candle_ts + INTERVAL_MS)} · skor {sig.score}/5",
        "",
        f"📍 Entry: <code>{p(entry)}</code>",
        f"🛑 SL: <code>{p(sig.stop)}</code> (-{risk:.{dec}f})",
        f"🎯 TP: <code>{p(sig.target)}</code> (+{reward:.{dec}f} · {RR_TARGET:.1f}R)",
    ]
    if TRAILING_ON:
        lines.append(f"🔒 BE di +{BE_TRIGGER_R:.1f}R, lalu trailing SL+ dari +{TRAIL_START_R:.1f}R "
                     f"(jarak {TRAIL_GAP_R:.1f}R dari puncak)")
    lines += ["", f"🧠 {html.escape(sig.reason)}",
              f"Level disapu <code>{p(sig.sweep_level)}</code> · ATR14 {sig.atr:.{dec}f}", ""]
    if pos is not None:
        tp_pnl = (reward - cost_per_unit(pair, entry, sig.target)) * pos.qty
        lines.append(f"💼 Paper: {fmt_qty(pair, pos.qty)} · risiko {rp(pos.risk_usd)} ({info.get('risk_pct', 0.0):.1f}%)")
        lines.append(f"Jika TP ≈ {rp(tp_pnl)} · Saldo {rp(account.balance)}")
        if info.get("status") == "min_lot":
            lines.append("⚠️ Lot minimum dipakai (modal kecil) — risiko di atas target.")
    else:
        lines.append(f"💼 Paper: SKIP — {html.escape(str(info.get('skip')))}")
    return "\n".join(lines)


def format_signal(pair: str, candles: list[Candle]) -> str:
    sig, info = latest_closed_signal(pair, candles)
    p = lambda x: fmt_price(pair, x)  # noqa: E731
    lines = [f"LIQUIDITY SNIPER 1M — {pair}", f"Harga close: {p(info['price'])}", f"Data: {info['time']}"]
    if not sig:
        lines += ["Setup: BELUM ADA pada candle terakhir yang sudah close.",
                  "Pola menunggu liquidity sweep + reclaim + konfirmasi Ichimoku."]
        return "\n".join(lines)
    lines += [f"SINYAL: {sig.direction}", f"Entry acuan (close): {p(sig.entry_reference)}", f"SL: {p(sig.stop)}",
              f"TP ({RR_TARGET:.1f}R): {p(sig.target)}", f"Risk harga: {p(sig.risk)} | ATR(14): {p(sig.atr)}",
              f"Konfirmasi: {sig.reason}", f"Score: {sig.score}/5",
              "Catatan: entry backtest memakai OPEN candle berikutnya; sinyal bukan instruksi transaksi."]
    return "\n".join(lines)


# ----------------------------------------------------------------------------
# Telegram: antrean kirim + pacing + retry_after (anti rate-limit sisi Telegram)
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Engine live: satu thread per pair, cache bersama, paper account, perintah Telegram
# ----------------------------------------------------------------------------
def help_text() -> str:
    return "\n".join([
        f"LIQUIDITY SNIPER 1M v{VERSION}",
        "Strategi: SHADOW FAKEOUT 1M (sweep, reclaim, rejection) + Ichimoku Tenkan/Kijun/Kumo.",
        "Live otomatis: sinyal dikirim sebagai chart + caption (alasan, entry, SL, TP, lot).",
        "Paper account: BE di +0.8R lalu trailing SL+, margin call -> top-up otomatis.",
        "",
        "/v2 cek [XAU|BTC|all] — cek setup candle terakhir (dari cache, tanpa panggil API)",
        "/v2 saldo — saldo, equity, deposit, net P/L, statistik",
        "/v2 posisi — posisi paper yang terbuka",
        "/v2 backtest [hari] [XAU|BTC|all] — backtest (bandingkan trailing vs TP/SL murni)",
        "/v2 stats — status feed + engine",
        "/v2 reset confirm — reset paper account ke modal awal",
        "",
        "XAU 1M Yahoo (fallback) maksimal 7 hari; Bybit sampai 30 hari.",
        "Tidak mengirim order sungguhan dan tidak menjamin profit.",
    ])


class Bot:
    def __init__(self, tg: Telegram, account: PaperAccount, auto: bool = True):
        self.tg = tg
        self.account = account
        self.auto = auto
        self.stop = threading.Event()
        self.started = time.time()
        self.feeds = {p: CandleFeed(p, PROVIDERS[p]) for p in ACTIVE_PAIRS}
        self.last_scanned: dict[str, int] = dict(account.last_scanned)
        self.stale_alerted: dict[str, bool] = {}
        self.backtest_lock = threading.Lock()
        self.last_bt: dict[str, float] = {}
        self.offset: Optional[int] = None
        self._last_beat = 0.0

    # ---- loop per pair (paralel) ---------------------------------------------
    def _sleep_until_next_minute(self) -> None:
        now = time.time()
        target = (int(now // 60) + 1) * 60 + POLL_OFFSET_S + random.uniform(0.0, 1.2)
        self.stop.wait(max(0.0, target - now))

    def pair_loop(self, pair: str) -> None:
        feed = self.feeds[pair]
        log(f"[{pair}] thread feed mulai")
        first = True
        while not self.stop.is_set():
            if not first:
                self._sleep_until_next_minute()
                if self.stop.is_set():
                    break
            first = False
            expected = (int(time.time()) // 60 - 1) * 60_000   # open-ts candle terakhir yang pasti sudah close
            # Bybit: boleh retry cepat menunggu candle baru muncul; Yahoo (fallback) cukup 1x/menit.
            attempts = 6 if (feed.active is None or isinstance(feed.active, BybitProvider)) else 1
            for _ in range(attempts):
                status = feed.refresh(expected)
                if status == "blocked" or feed.latest_ts() >= expected:
                    break
                if self.stop.wait(1.5):
                    return
            try:
                self.process_pair(pair)
            except Exception:
                log(f"[{pair}] error proses:\n{traceback.format_exc()}")
            try:
                self._watchdog(pair, feed)
            except Exception:
                log(f"[{pair}] error watchdog:\n{traceback.format_exc()}")

    def process_pair(self, pair: str) -> None:
        candles = self.feeds[pair].snapshot()
        if len(candles) < 80:
            return
        latest = candles[-1]
        self.account.set_mark(pair, latest.close)
        for text in self.account.advance(pair, candles):     # BE / trailing / TP / SL / margin call
            self.tg.send_text(text)
        if self.last_scanned.get(pair) == latest.ts:
            return
        baseline = pair not in self.last_scanned
        self.last_scanned[pair] = latest.ts
        self.account.mark_scanned(pair, latest.ts)
        if baseline:
            log(f"[{pair}] baseline di candle {latest.time_utc}; sinyal dimulai dari candle berikutnya")
            return
        if not self.auto:
            return
        age_s = time.time() - (latest.ts + INTERVAL_MS) / 1000.0
        if age_s > 3 * INTERVAL_MS / 1000.0:
            log(f"[{pair}] candle {latest.time_utc} sudah basi ({age_s:.0f}s); sinyal dilewati")
            return
        inds = build_indicators(candles)
        sig = detect_signal(candles, len(candles) - 1, inds)
        if sig is not None:
            sig.symbol = pair
            self.handle_signal(pair, candles, inds, sig)

    def handle_signal(self, pair: str, candles: list[Candle], inds: dict[str, list[Optional[float]]],
                      sig: Signal) -> None:
        c = candles[-1]
        entry = c.close
        pos, info = self.account.open_from_signal(pair, sig, entry)
        for line in info.get("topups", []):
            self.tg.send_text(line)
        png = render_chart(pair, candles, inds, sig, entry)
        caption = build_caption(pair, sig, entry, c.ts, pos, info, self.account)
        log(f"[{pair}] SINYAL {sig.direction} entry {fmt_price(pair, entry)} SL {fmt_price(pair, sig.stop)} "
            f"TP {fmt_price(pair, sig.target)} | paper: {('qty ' + str(pos.qty)) if pos else info.get('skip')}")
        if png:
            self.tg.send_photo(png, caption)
        else:
            self.tg.send_text(caption)

    def _watchdog(self, pair: str, feed: CandleFeed) -> None:
        ts = feed.latest_ts()
        now = time.time()
        age = now - ts / 1000.0 if ts else None
        stale = (age is not None and age > STALE_ALERT_S) or (ts == 0 and now - self.started > STALE_ALERT_S)
        if stale and not self.stale_alerted.get(pair):
            self.stale_alerted[pair] = True
            self.tg.send_text(f"⚠️ <b>Feed {pair} macet</b> (>{STALE_ALERT_S // 60} menit tanpa candle baru).\n"
                              f"{html.escape(feed.status_line())}")
        elif not stale and self.stale_alerted.get(pair):
            self.stale_alerted[pair] = False
            self.tg.send_text(f"✅ Feed {pair} pulih.")
        if now - self._last_beat > 900:
            self._last_beat = now
            log("[hb] " + " || ".join(f.status_line() for f in self.feeds.values()))

    # ---- perintah Telegram ------------------------------------------------------
    def authorized(self, chat_id: Any) -> bool:
        return self.tg.chat_id is None or str(chat_id) == self.tg.chat_id

    def reply(self, chat_id: Any, thread: Optional[int], text: str, html_mode: bool = False) -> None:
        self.tg.send_text(text, chat=chat_id, thread=thread, html_mode=html_mode)

    def handle_message(self, message: dict[str, Any]) -> None:
        chat_id = (message.get("chat") or {}).get("id")
        text_in = (message.get("text") or "").strip()
        thread = message.get("message_thread_id") if message.get("is_topic_message") else None
        if chat_id is None or not text_in.startswith("/"):
            return
        log(f"[cmd] chat={chat_id} {text_in[:60]!r}")
        if not self.authorized(chat_id):
            self.reply(chat_id, thread, "Akses ditolak. BOT_TELEGRAM_CHAT_ID membatasi penggunaan bot ini.")
            return
        parts = text_in.split()
        command = parts[0].split("@", 1)[0].lower()
        args = parts[1:]
        if command in {"/start", "/help"}:
            self.reply(chat_id, thread, help_text())
        elif command in {"/cek", "/signal"}:
            try:
                pair = normalize_pair(args[0]) if args else "ALL"
            except ValueError:
                self.reply(chat_id, thread, "Pair salah. Gunakan /cek XAU, /cek BTC, atau /cek all")
                return
            for p in (list(self.feeds) if pair == "ALL" else [pair]):
                feed = self.feeds.get(p)
                candles = feed.snapshot() if feed else []
                if len(candles) < 80:
                    self.reply(chat_id, thread, f"Feed {p} belum siap (cache {len(candles)} candle). Coba lagi sebentar.")
                    continue
                self.reply(chat_id, thread, format_signal(p, candles))
        elif command == "/saldo":
            self.reply(chat_id, thread, self.account.summary_text(), html_mode=True)
        elif command == "/posisi":
            self.reply(chat_id, thread, self.account.positions_text(), html_mode=True)
        elif command == "/reset":
            if self.tg.chat_id is None:
                self.reply(chat_id, thread, "Set BOT_TELEGRAM_CHAT_ID dulu sebelum memakai /reset.")
            elif args and args[0].lower() == "confirm":
                self.account.reset()
                self.reply(chat_id, thread, f"Paper account di-reset. Saldo {rp(self.account.balance)}.")
            else:
                self.reply(chat_id, thread, "Ini menghapus saldo, posisi, dan statistik paper. Ketik: /reset confirm")
        elif command in {"/backtest", "/bt"}:
            days, pair = 1, "ALL"
            for arg in args:
                if arg.isdigit():
                    days = max(1, min(int(arg), 30))
                else:
                    try:
                        pair = normalize_pair(arg)
                    except ValueError:
                        self.reply(chat_id, thread, "Format: /backtest [hari 1-30] [XAU/BTC/ALL]. Contoh: /backtest 3 all")
                        return
            wait = BT_COOLDOWN_S - (time.time() - self.last_bt.get(str(chat_id), 0.0))
            if wait > 0:
                self.reply(chat_id, thread, f"Tunggu {wait:.0f} detik lagi sebelum backtest berikutnya.")
                return
            if not self.backtest_lock.acquire(blocking=False):
                self.reply(chat_id, thread, "Backtest lain masih berjalan. Tunggu laporannya selesai.")
                return
            self.last_bt[str(chat_id)] = time.time()
            pairs = list(self.feeds) if pair == "ALL" else [pair]
            self.reply(chat_id, thread, f"Backtest {', '.join(pairs)} {days} hari dimulai (simulasi, tanpa order).")
            threading.Thread(target=self._backtest_job, args=(chat_id, thread, pairs, days), daemon=True).start()
        elif command == "/stats":
            lines = [f"<b>LIQUIDITY SNIPER 1M v{VERSION}</b> — uptime {(time.time() - self.started) / 3600:.1f} jam",
                     f"Strategi: Shadow Fakeout 1M + Ichimoku | TP {RR_TARGET:.1f}R | "
                     + (f"BE +{BE_TRIGGER_R:.1f}R, trailing {TRAIL_START_R:.1f}R/{TRAIL_GAP_R:.1f}R" if TRAILING_ON else "tanpa trailing")
                     + f" | biaya {'ON' if COSTS_ON else 'OFF'}",
                     "", "<b>Feed</b>"]
            lines += [html.escape(f.status_line()) for f in self.feeds.values()]
            lines += ["", f"Saldo paper {rp(self.account.balance)} | Net P/L {rp(self.account.net_pnl())} | "
                          f"{self.account.topups}× top-up"]
            self.reply(chat_id, thread, "\n".join(lines), html_mode=True)
        else:
            self.reply(chat_id, thread, "Perintah tidak dikenal. Kirim /help")

    def _backtest_job(self, chat_id: Any, thread: Optional[int], pairs: list[str], days: int) -> None:
        try:
            for pair in pairs:
                try:
                    d = min(days, 7 if pair == "XAUUSD" else 30)
                    candles = fetch_history(pair, d)
                    trades, s_on = backtest(candles, pair, trailing=True)
                    _, s_off = backtest(candles, pair, trailing=False)
                    self.reply(chat_id, thread, format_backtest(s_on, s_off))
                    if trades:
                        self.tg.send_document(trades_csv_bytes(trades), f"backtest_{pair}.csv",
                                              f"Detail trade {pair}", chat=chat_id, thread=thread)
                except Exception as exc:
                    self.reply(chat_id, thread, f"BACKTEST {pair} GAGAL: {exc}")
        finally:
            self.backtest_lock.release()

    # ---- jalan ----------------------------------------------------------------------
    def startup_text(self) -> str:
        persistent = bool(os.getenv("BOT_DATA_DIR") or os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or os.getenv("DATA_DIR"))
        lines = [f"🟢 <b>Liquidity Sniper 1M v{VERSION} (engine LV2) online</b>",
                 f"Pair: {', '.join(self.feeds)} | TP {RR_TARGET:.1f}R | "
                 + (f"BE +{BE_TRIGGER_R:.1f}R → trailing SL+" if TRAILING_ON else "tanpa trailing"),
                 f"Paper: saldo {rp(self.account.balance)} | Net P/L {rp(self.account.net_pnl())} | "
                 f"{len(self.account.positions)} posisi terbuka"]
        if not persistent:
            lines.append("⚠️ Tanpa Volume: saldo paper reset tiap redeploy/restart. Pasang Volume (mount /data).")
        return "\n".join(lines)

    def start(self) -> None:
        """Jalankan thread feed per pair (tanpa polling Telegram; perintah lewat bot utama /v2 ...)."""
        log(f"Liquidity Sniper v2 engine start | pairs={','.join(self.feeds)} | auto={self.auto} | data={DATA_DIR}")
        if not self.tg.chat_id:
            log("PERINGATAN: BOT_TELEGRAM_CHAT_ID belum diatur — sinyal LV2 hanya tercatat di log.")
        for pair in self.feeds:
            threading.Thread(target=self.pair_loop, args=(pair,), name=f"lv2-feed-{pair}", daemon=True).start()
        self.tg.send_text(self.startup_text())

    # ---- antarmuka seragam dengan engine lain (dipanggil bot utama)
    key = "lv2"
    title = "LIQUIDITY SNIPER 1M v2"

    def status_text(self) -> str:
        lines = [f"<b>{self.title}</b> — topic {self.tg.thread_id or 'General'}"]
        lines += [html.escape(f.status_line()) for f in self.feeds.values()]
        lines.append(f"Saldo paper {rp(self.account.balance)} | Net P/L {rp(self.account.net_pnl())} | "
                     f"{self.account.topups}× top-up | {len(self.account.positions)} posisi terbuka")
        return "\n".join(lines)

    def handle_command(self, args: list[str], chat: Any, thread: Optional[int]) -> None:
        sub = args[0].lower() if args else "stats"
        sub = {"status": "stats", "balance": "saldo", "bt": "backtest", "check": "cek"}.get(sub, sub)
        self.handle_message({"chat": {"id": chat}, "text": "/" + sub + (" " + " ".join(args[1:]) if len(args) > 1 else ""),
                             "message_thread_id": thread, "is_topic_message": bool(thread)})


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def build_engine(token: str, chat_id: Optional[str]) -> "Bot":
    """Dipanggil bot utama: buat engine LV2 dengan topic LV2_TOPIC_ID (belum dijalankan)."""
    hub = TelegramHub.get(token)
    tg = TelegramView(hub, chat_id, _env_topic("LV2_TOPIC_ID"))
    account = PaperAccount(STATE_FILE)
    return Bot(tg, account, auto=env_bool("AUTO_SCAN", True))


def run_live() -> int:
    print("Engine ini dijalankan oleh liquidity_sniper_bot.py. Uji data: python -m engines.liq_v2 --backtest 1 --pair all")
    return 2


def cli_main(args: argparse.Namespace) -> int:
    pair_arg = normalize_pair(args.pair)
    pairs = list(ACTIVE_PAIRS) if pair_arg == "ALL" else [pair_arg]
    if args.backtest is not None:
        code = 0
        for pair in pairs:
            try:
                days = min(args.backtest, 7 if pair == "XAUUSD" else 30)
                candles = fetch_history(pair, days)
                trades, s_on = backtest(candles, pair, trailing=True)
                _, s_off = backtest(candles, pair, trailing=False)
                print("\n" + format_backtest(s_on, s_off) + "\n", flush=True)
                os.makedirs(DATA_DIR, exist_ok=True)
                path = os.path.join(DATA_DIR, f"backtest_{pair}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                with open(path, "wb") as fh:
                    fh.write(trades_csv_bytes(trades))
                print(f"Detail trade CSV: {path}")
            except Exception as exc:
                code = 1
                print(f"BACKTEST {pair} GAGAL: {exc}", file=sys.stderr, flush=True)
        return code
    if args.signal:
        code = 0
        for pair in pairs:
            try:
                print(format_signal(pair, fetch_history(pair, 1)) + "\n")
            except Exception as exc:
                code = 1
                print(f"CEK {pair} GAGAL: {exc}", file=sys.stderr)
        return code
    return run_live()


def main() -> int:
    parser = argparse.ArgumentParser(description="Liquidity Sniper 1M — bot Telegram live + paper account (tanpa order).")
    parser.add_argument("--backtest", type=int, metavar="DAYS", help="backtest N hari data 1M (XAU max 7 hari)")
    parser.add_argument("--signal", action="store_true", help="cek sinyal terbaru lalu keluar")
    parser.add_argument("--pair", default="all", help="XAU, BTC, atau all (default all)")
    parser.add_argument("pair_pos", nargs="?", default=None, help="opsional: pair setelah argumen")
    args = parser.parse_args()
    if args.pair_pos:
        args.pair = args.pair_pos
    try:
        return cli_main(args)
    except ValueError as exc:
        print(f"Input salah: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
''',
    "manager": r'''
# -*- coding: utf-8 -*-
"""Menyalakan ketiga engine independen. Kegagalan satu engine tidak mematikan yang lain."""
from __future__ import annotations

import traceback
from typing import Any, Optional

from .common import TelegramHub, TelegramView, env_bool, env_topic, log

ENGINE_LABELS = {
    "sfp": ("SFP", "SHADOW FAKEOUT PDF 1M", "SFP_TOPIC_ID"),
    "tpb": ("TPB", "TREND PULLBACK M5", "TPB_TOPIC_ID"),
    "lv2": ("LV2", "LIQUIDITY SNIPER 1M v2", "LV2_TOPIC_ID"),
}


def start_engines(token: str, chat_id: Optional[str]) -> dict[str, Any]:
    engines: dict[str, Any] = {}
    hub = TelegramHub.get(token)

    def boot(key: str, factory) -> None:
        prefix = ENGINE_LABELS[key][0]
        if not env_bool(f"{prefix}_ENABLED", True):
            log(f"[engines] {key} dimatikan ({prefix}_ENABLED=0)")
            return
        try:
            engine = factory()
            engine.start()
            engines[key] = engine
        except Exception:
            log(f"[engines] {key} GAGAL start:\n{traceback.format_exc()}")

    def make_sfp():
        from .sfp import ShadowFakeoutPdfEngine
        return ShadowFakeoutPdfEngine(TelegramView(hub, chat_id, env_topic("SFP_TOPIC_ID")))

    def make_tpb():
        from .tpb import TrendPullbackEngine
        return TrendPullbackEngine(TelegramView(hub, chat_id, env_topic("TPB_TOPIC_ID")))

    def make_lv2():
        from .liq_v2 import build_engine
        return build_engine(token, chat_id)

    boot("sfp", make_sfp)
    boot("tpb", make_tpb)
    boot("lv2", make_lv2)
    return engines
''',
}


def _install_embedded_engines() -> None:
    """Register all extra engines as modules in memory, all sourced from main.py."""
    import types

    # Reuse the same persistent volume path as the main paper ledger.
    os.environ.setdefault("BOT_DATA_DIR", DATA_DIR)
    package = types.ModuleType("engines")
    package.__package__ = "engines"
    package.__path__ = []
    sys.modules["engines"] = package

    # Relative imports work because each virtual submodule is registered before use.
    for name in ("common", "data", "sfp", "tpb", "liq_v2", "manager"):
        fullname = "engines." + name
        module = types.ModuleType(fullname)
        module.__package__ = "engines"
        module.__file__ = f"{globals().get('__file__', 'main.py')}::<embedded {fullname}>"
        sys.modules[fullname] = module
        setattr(package, name, module)
        try:
            exec(compile(_EMBEDDED_ENGINE_SOURCES[name], module.__file__, "exec"), module.__dict__)
        except Exception:
            sys.modules.pop(fullname, None)
            raise

    print("[Embedded engines] common/data + SFP BE/SL+ + TPB continuous SL+ + LV2 trailing + manager loaded", flush=True)


_install_embedded_engines()


if __name__ == "__main__":
    raise SystemExit(main())
