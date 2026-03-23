"""
₿  Binance USDT-M Futures · Engulfing Candle Scanner  v4
═══════════════════════════════════════════════════════════
PATTERN TYPES:
  BULLISH      : C1 bearish · C2 bullish · full range engulf · C2.open < C1.close
  BEARISH      : C1 bullish · C2 bearish · full range engulf · C2.open > C1.close
  BULL-CONT    : C1 bullish · C2 bullish · full range engulf · C2.open < C1.close (gap-down)
  BEAR-CONT    : C1 bearish · C2 bearish · full range engulf · C2.open > C1.close (gap-up)

FVG CHECK (last 10 candles):
  BULLISH / BULL-CONT : C2.low touches (enters) a Bullish FVG
  BEARISH / BEAR-CONT : C2.high touches (enters) a Bearish FVG

FVG Definition:
  Bullish FVG  : gap between candle[i-1].high and candle[i+1].low  (candle[i] is the impulse)
                 valid when candle[i+1].low > candle[i-1].high
  Bearish FVG  : gap between candle[i-1].low  and candle[i+1].high (candle[i] is the impulse)
                 valid when candle[i+1].high < candle[i-1].low

No API key required — Binance public REST API.
"""

import io
import time
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import namedtuple

# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Binance Futures · Engulfing Scanner v4",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Space+Grotesk:wght@400;600;700;800&display=swap');

html,body,[class*="css"]{font-family:'Space Grotesk',sans-serif;}
.stApp{background:#04060e;color:#dde4f0;}
.stApp>header{background:transparent!important;}

[data-testid="stSidebar"]{background:#070b16!important;border-right:1px solid #0f1a2e;}
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span{color:#7a8fac!important;font-size:.82rem;}
[data-testid="stSidebar"] h3{color:#f0b429!important;font-family:'JetBrains Mono',monospace!important;
  font-size:.72rem!important;letter-spacing:.2em!important;text-transform:uppercase!important;}

[data-testid="metric-container"]{background:#080d1c!important;border:1px solid #0f1a2e!important;
  border-radius:12px!important;padding:.8rem 1.1rem!important;}
[data-testid="metric-container"] label{font-family:'JetBrains Mono',monospace!important;
  font-size:.6rem!important;color:#2d4060!important;letter-spacing:.15em!important;text-transform:uppercase!important;}
[data-testid="metric-container"] [data-testid="stMetricValue"]{font-family:'Space Grotesk',sans-serif!important;
  font-weight:800!important;font-size:1.9rem!important;color:#dde4f0!important;line-height:1.1!important;}

.stProgress>div>div{background:linear-gradient(90deg,#f0b429,#f97316)!important;border-radius:99px!important;}

.stButton>button{background:linear-gradient(135deg,#1a1200,#2d1f00)!important;color:#f0b429!important;
  border:1px solid #3d2d00!important;border-radius:9px!important;font-family:'JetBrains Mono',monospace!important;
  font-weight:700!important;font-size:.83rem!important;letter-spacing:.1em!important;
  padding:.55rem 1.4rem!important;width:100%!important;transition:all .18s ease!important;}
.stButton>button:hover{border-color:#f0b429!important;box-shadow:0 0 22px #f0b42926!important;}

[data-testid="stDownloadButton"]>button{background:linear-gradient(135deg,#003322,#004d33)!important;
  color:#34d399!important;border:1px solid #065f46!important;border-radius:9px!important;
  font-family:'JetBrains Mono',monospace!important;font-weight:700!important;
  font-size:.8rem!important;width:100%!important;}

/* ── Signal cards ── */
.bull-card{background:linear-gradient(135deg,#020d06,#051508);border:1px solid #16532822;
  border-left:4px solid #22c55e;border-radius:12px;padding:1rem 1.25rem;margin:.4rem 0;
  font-family:'JetBrains Mono',monospace;animation:cardIn .3s cubic-bezier(.34,1.56,.64,1) both;}
.bear-card{background:linear-gradient(135deg,#0d0202,#150505);border:1px solid #65161622;
  border-left:4px solid #ef4444;border-radius:12px;padding:1rem 1.25rem;margin:.4rem 0;
  font-family:'JetBrains Mono',monospace;animation:cardIn .3s cubic-bezier(.34,1.56,.64,1) both;}
.bullcont-card{background:linear-gradient(135deg,#020a0d,#041520);border:1px solid #1e4a6022;
  border-left:4px solid #38bdf8;border-radius:12px;padding:1rem 1.25rem;margin:.4rem 0;
  font-family:'JetBrains Mono',monospace;animation:cardIn .3s cubic-bezier(.34,1.56,.64,1) both;}
.bearcont-card{background:linear-gradient(135deg,#0d0208,#150215);border:1px solid #6516582;
  border-left:4px solid #e879f9;border-radius:12px;padding:1rem 1.25rem;margin:.4rem 0;
  font-family:'JetBrains Mono',monospace;animation:cardIn .3s cubic-bezier(.34,1.56,.64,1) both;}

@keyframes cardIn{from{opacity:0;transform:translateY(-8px) scale(.97)}to{opacity:1;transform:translateY(0) scale(1)}}

/* FVG badge */
.fvg-badge{display:inline-block;background:#1a0d00;color:#f97316;font-size:.6rem;
  padding:.1rem .55rem;border-radius:4px;font-weight:700;letter-spacing:.08em;
  border:1px solid #7c2d1222;font-family:'JetBrains Mono',monospace;margin-left:.4rem;}

.log-row{background:#070b16;border:1px solid #0c1526;border-radius:6px;
  padding:.32rem .8rem;margin:.16rem 0;font-family:'JetBrains Mono',monospace;
  font-size:.7rem;color:#253650;}
.log-hit{color:#7a8fac!important;border-color:#162035!important;}
.log-err{color:#4a1515!important;}

.badge-bull{display:inline-block;background:#0a2916;color:#4ade80;font-size:.63rem;
  padding:.12rem .6rem;border-radius:5px;font-weight:700;letter-spacing:.08em;
  font-family:'JetBrains Mono',monospace;}
.badge-bear{display:inline-block;background:#2d0808;color:#f87171;font-size:.63rem;
  padding:.12rem .6rem;border-radius:5px;font-weight:700;letter-spacing:.08em;
  font-family:'JetBrains Mono',monospace;}
.badge-bullcont{display:inline-block;background:#071a2d;color:#38bdf8;font-size:.63rem;
  padding:.12rem .6rem;border-radius:5px;font-weight:700;letter-spacing:.08em;
  font-family:'JetBrains Mono',monospace;}
.badge-bearcont{display:inline-block;background:#1f0728;color:#e879f9;font-size:.63rem;
  padding:.12rem .6rem;border-radius:5px;font-weight:700;letter-spacing:.08em;
  font-family:'JetBrains Mono',monospace;}

.sec-hdr{font-family:'JetBrains Mono',monospace;font-size:.6rem;letter-spacing:.25em;
  text-transform:uppercase;color:#f0b429;border-bottom:1px solid #0f1a2e;
  padding-bottom:.3rem;margin:1rem 0 .5rem 0;}

.def-box{background:#080d1c;border:1px solid #0f1a2e;border-radius:12px;padding:1rem 1.25rem;margin:.5rem 0;}

.res-pane{max-height:65vh;overflow-y:auto;padding-right:4px;
  scrollbar-width:thin;scrollbar-color:#1a2e4a #070b16;}
.res-pane::-webkit-scrollbar{width:4px;}
.res-pane::-webkit-scrollbar-track{background:#070b16;}
.res-pane::-webkit-scrollbar-thumb{background:#1a2e4a;border-radius:4px;}

.live-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:#34d399;
  animation:pulse 1.5s ease-in-out infinite;vertical-align:middle;margin-right:5px;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS & TYPES
# ══════════════════════════════════════════════════════════════════════════════
# Binance futures (fapi) is geo-blocked on some cloud IPs.
# We auto-detect which base URL works and fall back to spot (api) if needed.
FAPI_BASE = "https://fapi.binance.com"
SAPI_BASE = "https://api.binance.com"

TF_MAP = {"1H": "1h", "4H": "4h", "1D": "1d", "1W": "1w"}

# Gap tolerance: crypto candles open at almost exactly the prior close.
# A pure strict < / > gap check would miss nearly everything.
# We allow up to GAP_TOL of the C1 body as tolerance (0.05 = 5%).
GAP_TOL = 0.0   # set to 0 = strict; increase to e.g. 0.002 if still no results

# kind: BULLISH | BEARISH | BULL-CONT | BEAR-CONT
Signal = namedtuple("Signal", [
    "kind", "symbol", "tf", "dt",
    "c1_open", "c1_high", "c1_low", "c1_close",
    "c2_open", "c2_high", "c2_low", "c2_close",
    "body_pct", "candle_idx", "total_candles",
    "fvg_touched",
    "fvg_top", "fvg_bot",
])

# ══════════════════════════════════════════════════════════════════════════════
# HTTP SESSION
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 EngulfingScanner/4.1"})
    a = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount("https://", a)
    return s

SESSION = get_session()

# ══════════════════════════════════════════════════════════════════════════════
# BINANCE API  —  auto-selects fapi vs api base, retries with backoff
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=120, show_spinner=False)
def _detect_base() -> str:
    """
    Try fapi first. If it fails (geo-block / timeout) fall back to spot api.
    Cached for 2 minutes so we don't re-probe on every symbol fetch.
    """
    for base in (FAPI_BASE, SAPI_BASE):
        try:
            r = SESSION.get(base + "/fapi/v1/ping", timeout=5)
            if r.status_code == 200:
                return base
        except Exception:
            pass
    return FAPI_BASE   # last resort — will surface errors downstream


def _raw_get(url: str, params: dict | None, retries: int = 4) -> dict | list | None:
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=10)
            used = int(resp.headers.get("X-MBX-USED-WEIGHT-1M", 0))
            if used > 1100:
                time.sleep(4)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                time.sleep(8 * (attempt + 1))
            elif resp.status_code == 418:
                time.sleep(65)
            elif resp.status_code in (403, 451):
                # geo-block — no point retrying same URL
                return None
            else:
                time.sleep(1.5 * (attempt + 1))
        except requests.exceptions.Timeout:
            time.sleep(2 * (attempt + 1))
        except requests.exceptions.RequestException:
            time.sleep(1 + attempt)
    return None


def api_get(endpoint: str, params: dict | None = None) -> dict | list | None:
    base = _detect_base()
    result = _raw_get(base + endpoint, params)
    if result is None and base == FAPI_BASE:
        # Auto-fallback: try spot endpoint (klines are identical for USDT pairs)
        spot_ep = endpoint.replace("/fapi/v1/", "/api/v3/")
        result = _raw_get(SAPI_BASE + spot_ep, params)
    return result


@st.cache_data(ttl=300, show_spinner=False)
def get_all_symbols() -> tuple[list[str], str]:
    """Returns (symbols, error_msg). error_msg is '' on success."""
    data = api_get("/fapi/v1/exchangeInfo")
    if not data:
        # Fallback: fetch spot symbols and filter for USDT perpetual-like pairs
        data2 = api_get("/api/v3/exchangeInfo")
        if not data2:
            return [], "❌ Cannot reach Binance API — check network / firewall."
        syms = sorted(
            s["symbol"] for s in data2.get("symbols", [])
            if s.get("status") == "TRADING"
            and s["symbol"].endswith("USDT")
            and s.get("quoteAsset") == "USDT"
        )
        return syms, "⚠️ Using spot symbols (futures API unreachable)"
    syms = sorted(
        s["symbol"] for s in data.get("symbols", [])
        if s["status"] == "TRADING"
        and s["contractType"] == "PERPETUAL"
        and s["quoteAsset"] == "USDT"
    )
    return syms, ""


def fetch_klines(symbol: str, interval: str, limit: int) -> tuple[list[dict] | None, str]:
    """Returns (candles, error_str). error_str is '' on success."""
    # Try futures endpoint first, then spot
    for endpoint, base in (
        ("/fapi/v1/klines", _detect_base()),
        ("/api/v3/klines",  SAPI_BASE),
    ):
        url = base + endpoint
        data = _raw_get(url, {"symbol": symbol, "interval": interval, "limit": limit})
        if data and isinstance(data, list) and len(data) > 0:
            candles = [
                {
                    "dt":    datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
                    "open":  float(k[1]),
                    "high":  float(k[2]),
                    "low":   float(k[3]),
                    "close": float(k[4]),
                }
                for k in data
            ]
            return candles, ""
    return None, f"fetch failed for {symbol}"

# ══════════════════════════════════════════════════════════════════════════════
# FVG DETECTION
# ══════════════════════════════════════════════════════════════════════════════
def find_fvgs(candles: list[dict], lookback: int = 10) -> tuple[list[tuple], list[tuple]]:
    """
    Scan the `lookback` candles immediately BEFORE the last candle (C2 of the
    engulfing pair) and return all FVG zones found in that window.

    FVGs are 3-candle formations:
        Bullish FVG : candle[i+1].low > candle[i-1].high  → zone = (candle[i-1].high, candle[i+1].low)
        Bearish FVG : candle[i+1].high < candle[i-1].low  → zone = (candle[i+1].high, candle[i-1].low)

    The last candle in `candles` is C2 (the engulfer). FVG search excludes C2
    so zones are strictly pre-existing.

    Returns:
        bull_fvgs : list of (bot, top)
        bear_fvgs : list of (bot, top)
    """
    bull_fvgs: list[tuple[float, float]] = []
    bear_fvgs: list[tuple[float, float]] = []

    n = len(candles)
    # Exclude the last candle (C2). Search window ends at index n-2 (C1).
    # A 3-candle FVG centred on index i needs i-1 and i+1, so i runs from 1 to n-3 inclusive.
    # We only look back `lookback` candles from C1, so i starts at max(1, n-2-lookback).
    i_end   = n - 2          # C1 position (last candle before C2)
    i_start = max(1, i_end - lookback + 1)

    for i in range(i_start, i_end + 1):
        if i + 1 >= n - 1:   # don't let i+1 reach C2
            break
        prev = candles[i - 1]
        nxt  = candles[i + 1]

        # Bullish FVG: gap above prev, below nxt
        if nxt["low"] > prev["high"]:
            bull_fvgs.append((prev["high"], nxt["low"]))

        # Bearish FVG: gap below prev, above nxt
        if nxt["high"] < prev["low"]:
            bear_fvgs.append((nxt["high"], prev["low"]))

    return bull_fvgs, bear_fvgs


def check_fvg_touch(
    c2_low: float,
    c2_high: float,
    bull_fvgs: list[tuple],
    bear_fvgs: list[tuple],
    pattern_kind: str,
) -> tuple[bool, float | None, float | None]:
    """
    For BULLISH / BULL-CONT : check if C2.low is inside (or at boundary of) any bullish FVG.
      → C2.low <= fvg_top  AND  C2.low >= fvg_bot  (wick enters the zone from above)
      Also accept if C2.low is below fvg_bot but C2.high is above fvg_bot (wick passes through).
      Simplest correct check: C2.low <= fvg_top  (wick reaches at least the top of the gap)
      combined with C2.low <= fvg_top to confirm touch — we just need the wick to be
      at or below fvg_top (meaning it entered the gap).

    For BEARISH / BEAR-CONT : check if C2.high is inside any bearish FVG.
      → C2.high >= fvg_bot  AND  C2.high <= fvg_top

    "Wick anywhere inside FVG" means:
      Bull FVG touch: C2.low  <= fvg_top  AND  C2.high >= fvg_bot  (range overlaps zone)
      Bear FVG touch: C2.high >= fvg_bot  AND  C2.low  <= fvg_top  (same overlap logic)
    """
    if pattern_kind in ("BULLISH", "BULL-CONT"):
        for (bot, top) in bull_fvgs:
            # Candle range [c2_low, c2_high] overlaps FVG zone [bot, top]
            if c2_low <= top and c2_high >= bot:
                return True, top, bot
        return False, None, None

    else:  # BEARISH or BEAR-CONT
        for (bot, top) in bear_fvgs:
            if c2_low <= top and c2_high >= bot:
                return True, top, bot
        return False, None, None

# ══════════════════════════════════════════════════════════════════════════════
# ENGULFING LOGIC
# ══════════════════════════════════════════════════════════════════════════════
def detect_engulfing(
    candles: list[dict],
    symbol: str,
    tf: str,
    fvg_lookback: int = 10,
    eng_lookback: int = 10,
) -> list[Signal]:
    """
    Detect four engulfing pattern types:

    BULLISH    : C1 bearish, C2 bullish,  C2 engulfs C1 range, C2.open < C1.close
    BEARISH    : C1 bullish, C2 bearish,  C2 engulfs C1 range, C2.open > C1.close
    BULL-CONT  : C1 bullish, C2 bullish,  C2 engulfs C1 range, C2.open < C1.close (gap-down open)
    BEAR-CONT  : C1 bearish, C2 bearish,  C2 engulfs C1 range, C2.open > C1.close (gap-up open)

    Additionally checks FVG touch for each signal.
    """
    found = []
    n = len(candles)

    # Only examine the last `eng_lookback` consecutive candle pairs.
    # The very last candle (candles[-1]) is the most recent complete candle = C2.
    scan_start = max(1, n - eng_lookback)

    for i in range(scan_start, n):
        c1 = candles[i - 1]
        c2 = candles[i]

        c1_o, c1_h, c1_l, c1_c = c1["open"], c1["high"], c1["low"], c1["close"]
        c2_o, c2_h, c2_l, c2_c = c2["open"], c2["high"], c2["low"], c2["close"]

        # Shared range condition: C2 fully engulfs C1's high-low range
        range_engulf = (c2_l < c1_l) and (c2_h > c1_h)
        if not range_engulf:
            continue

        kind = None
        # Tolerance for gap: allow C2.open to be within GAP_TOL * C1_body of C1.close.
        # In crypto, candles open at exact prior close so strict < / > misses everything.
        # GAP_TOL=0 means exact touch is still valid (open == close counts as gap-down/up).
        c1_body = abs(c1_c - c1_o) if c1_o != c1_c else abs(c1_h - c1_l) * 0.1
        tol = GAP_TOL * c1_body

        # ── BULLISH REVERSAL ─────────────────────────────────────────────────
        # C1 bearish · C2 bullish · C2.open <= C1.close + tol
        if c1_c < c1_o and c2_o < c2_c and c2_o <= c1_c + tol:
            kind = "BULLISH"

        # ── BEARISH REVERSAL ─────────────────────────────────────────────────
        # C1 bullish · C2 bearish · C2.open >= C1.close - tol
        elif c1_c > c1_o and c2_o > c2_c and c2_o >= c1_c - tol:
            kind = "BEARISH"

        # ── BULL CONTINUATION ────────────────────────────────────────────────
        # C1 bullish · C2 bullish · C2.open <= C1.close + tol (gap-down open)
        elif c1_c > c1_o and c2_o < c2_c and c2_o <= c1_c + tol:
            kind = "BULL-CONT"

        # ── BEAR CONTINUATION ────────────────────────────────────────────────
        # C1 bearish · C2 bearish · C2.open >= C1.close - tol (gap-up open)
        elif c1_c < c1_o and c2_o > c2_c and c2_o >= c1_c - tol:
            kind = "BEAR-CONT"

        if kind is None:
            continue

        # ── FVG check ────────────────────────────────────────────────────────
        # Pass all candles up to and including C2 (index i).
        # find_fvgs internally excludes C2 (last element) from FVG formation
        # and looks back fvg_lookback candles before it.
        bull_fvgs, bear_fvgs = find_fvgs(candles[: i + 1], lookback=fvg_lookback)
        fvg_hit, fvg_top, fvg_bot = check_fvg_touch(c2_l, c2_h, bull_fvgs, bear_fvgs, kind)

        body = round(abs(c2_c - c2_o) / c2_o * 100, 3)

        found.append(Signal(
            kind=kind, symbol=symbol, tf=tf, dt=c2["dt"],
            c1_open=c1_o, c1_high=c1_h, c1_low=c1_l, c1_close=c1_c,
            c2_open=c2_o, c2_high=c2_h, c2_low=c2_l, c2_close=c2_c,
            body_pct=body, candle_idx=i, total_candles=n,
            fvg_touched=fvg_hit, fvg_top=fvg_top, fvg_bot=fvg_bot,
        ))

    return found


def scan_symbol(args: tuple):
    symbol, interval, limit, tf, fvg_lookback, eng_lookback = args
    candles, err = fetch_klines(symbol, interval, limit)
    if candles is None or len(candles) < 3:
        return symbol, [], True, err or "no data"
    sigs = detect_engulfing(candles, symbol, tf, fvg_lookback, eng_lookback)
    return symbol, sigs, False, ""

# ══════════════════════════════════════════════════════════════════════════════
# HTML / DISPLAY HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def fp(v: float) -> str:
    if v >= 10_000: return f"{v:,.2f}"
    if v >= 1:      return f"{v:.4f}"
    return f"{v:.6f}"


_CARD_META = {
    "BULLISH":   ("bull-card",     "badge-bull",     "#4ade80", "▲ BULLISH ENGULF"),
    "BEARISH":   ("bear-card",     "badge-bear",     "#f87171", "▼ BEARISH ENGULF"),
    "BULL-CONT": ("bullcont-card", "badge-bullcont", "#38bdf8", "↑ BULL CONTINUATION"),
    "BEAR-CONT": ("bearcont-card", "badge-bearcont", "#e879f9", "↓ BEAR CONTINUATION"),
}


def signal_card(sig: Signal) -> str:
    card_cls, badge_cls, col, label = _CARD_META[sig.kind]
    dt_str = sig.dt.strftime("%d %b %Y  %H:%M UTC")

    fvg_html = ""
    if sig.fvg_touched:
        zone = f"{fp(sig.fvg_bot)} – {fp(sig.fvg_top)}" if sig.fvg_top else ""
        fvg_html = f'<span class="fvg-badge">⚡ FVG {zone}</span>'

    rows = [
        ("C1 High",  fp(sig.c1_high),  "C1 Low",   fp(sig.c1_low)),
        ("C1 Open",  fp(sig.c1_open),  "C1 Close", fp(sig.c1_close)),
        ("C2 High",  fp(sig.c2_high),  "C2 Low",   fp(sig.c2_low)),
        ("C2 Open",  fp(sig.c2_open),  "C2 Close", fp(sig.c2_close)),
    ]

    cells = ""
    for l1, v1, l2, v2 in rows:
        cells += (
            f'<div><div style="font-size:.58rem;color:#2d4060;text-transform:uppercase;'
            f'letter-spacing:.08em;">{l1}</div>'
            f'<div style="color:{col};font-weight:600;font-size:.78rem;margin-top:.05rem;">{v1}</div></div>'
            f'<div><div style="font-size:.58rem;color:#2d4060;text-transform:uppercase;'
            f'letter-spacing:.08em;">{l2}</div>'
            f'<div style="color:{col};font-weight:600;font-size:.78rem;margin-top:.05rem;">{v2}</div></div>'
        )

    return f"""
<div class="{card_cls}">
  <div style="display:flex;justify-content:space-between;align-items:center;gap:.5rem;flex-wrap:wrap;">
    <span style="color:#f1f5f9;font-weight:800;font-size:1.05rem;
                 font-family:'Space Grotesk',sans-serif;letter-spacing:-.01em;">{sig.symbol}</span>
    <div style="display:flex;gap:.4rem;align-items:center;flex-wrap:wrap;">
      <span class="{badge_cls}">{label}</span>
      {fvg_html}
      <span style="font-size:.6rem;color:#2d4060;font-family:'JetBrains Mono',monospace;">
        {sig.tf} · candle&nbsp;{sig.candle_idx}/{sig.total_candles}
      </span>
    </div>
  </div>
  <div style="color:#2d4060;font-size:.67rem;margin-top:.28rem;font-family:'JetBrains Mono',monospace;">
    📅 {dt_str} &nbsp;·&nbsp; Body&nbsp;<span style="color:{col};">{sig.body_pct}%</span>
  </div>
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:.5rem;margin-top:.65rem;">
    {cells}
  </div>
</div>"""


def log_row_html(sym: str, kind: str, n: int = 0) -> str:
    if kind == "hit":
        return f'<div class="log-row log-hit">✦ {sym} &nbsp;·&nbsp; {n} signal(s)</div>'
    if kind == "err":
        return f'<div class="log-row log-err">✗ {sym}</div>'
    return f'<div class="log-row">· {sym}</div>'


def build_csv(signals: list[Signal]) -> bytes:
    buf = io.StringIO()
    rows = [{
        "Type":           s.kind,
        "Symbol":         s.symbol,
        "Timeframe":      s.tf,
        "Datetime (UTC)": s.dt.strftime("%Y-%m-%d %H:%M"),
        "C1 High": s.c1_high, "C1 Low":  s.c1_low,
        "C1 Open": s.c1_open, "C1 Close": s.c1_close,
        "C2 High": s.c2_high, "C2 Low":  s.c2_low,
        "C2 Open": s.c2_open, "C2 Close": s.c2_close,
        "Body %":  s.body_pct,
        "FVG Touch":  s.fvg_touched,
        "FVG Top":    s.fvg_top,
        "FVG Bot":    s.fvg_bot,
    } for s in signals]
    pd.DataFrame(rows).to_csv(buf, index=False)
    return buf.getvalue().encode()

# ══════════════════════════════════════════════════════════════════════════════
# DEFINITION DIAGRAM
# ══════════════════════════════════════════════════════════════════════════════
DIAGRAM_SVG = """
<svg width="100%" viewBox="0 0 900 520" xmlns="http://www.w3.org/2000/svg"
     style="font-family:'JetBrains Mono',monospace;">
  <defs>
    <marker id="da" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
      <path d="M2 1L8 5L2 9" fill="none" stroke="context-stroke" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    </marker>
  </defs>

  <!-- ══ TOP ROW ══ -->

  <!-- ── Panel 1: BULLISH REVERSAL ── x=10 -->
  <rect x="10" y="8" width="205" height="240" rx="10" fill="#051508" stroke="#22c55e" stroke-width="0.6" stroke-opacity="0.4"/>
  <text x="112" y="26" text-anchor="middle" font-size="10" font-weight="700" fill="#4ade80">▲ BULLISH REVERSAL</text>
  <text x="112" y="39" text-anchor="middle" font-size="8" fill="#3d6040">C1 bear · C2 bull · gap-down</text>
  <!-- C1 red -->
  <line x1="72"  y1="55" x2="72"  y2="68" stroke="#ef4444" stroke-width="2"/>
  <rect x="58"   y="68" width="28" height="75" rx="2" fill="#ef4444" opacity="0.85"/>
  <line x1="72"  y1="143" x2="72" y2="162" stroke="#ef4444" stroke-width="2"/>
  <!-- C2 green -->
  <line x1="138" y1="48" x2="138" y2="60" stroke="#22c55e" stroke-width="2"/>
  <rect x="124"  y="60" width="28" height="100" rx="2" fill="#22c55e" opacity="0.85"/>
  <line x1="138" y1="160" x2="138" y2="180" stroke="#22c55e" stroke-width="2"/>
  <!-- C1 close dashed -->
  <line x1="54" y1="143" x2="178" y2="143" stroke="#ef4444" stroke-width="0.7" stroke-dasharray="4 3" stroke-opacity="0.6"/>
  <!-- C2 open marker -->
  <line x1="120" y1="160" x2="178" y2="160" stroke="#22c55e" stroke-width="0.7" stroke-dasharray="4 3" stroke-opacity="0.6"/>
  <!-- gap arrow -->
  <line x1="172" y1="145" x2="172" y2="158" stroke="#facc15" stroke-width="1.4" marker-start="url(#da)" marker-end="url(#da)"/>
  <text x="54" y="147" font-size="8" fill="#ef4444" text-anchor="end">C1.cls</text>
  <text x="54" y="164" font-size="8" fill="#22c55e" text-anchor="end">C2.opn</text>
  <text x="112" y="47" font-size="8" fill="#22c55e" text-anchor="middle">C2.high &gt; C1.high</text>
  <text x="112" y="192" font-size="8" fill="#22c55e" text-anchor="middle">C2.low &lt; C1.low</text>
  <text x="72" y="47" text-anchor="middle" font-size="10" font-weight="600" fill="#ef4444">C1</text>
  <text x="138" y="40" text-anchor="middle" font-size="10" font-weight="600" fill="#22c55e">C2</text>
  <!-- rule box -->
  <rect x="18" y="206" width="189" height="34" rx="4" fill="#0a2916" stroke="#22c55e" stroke-width="0.4" stroke-opacity="0.4"/>
  <text x="112" y="218" text-anchor="middle" font-size="8" fill="#4ade80">C1.cls &lt; C1.opn · C2.opn &lt; C2.cls</text>
  <text x="112" y="231" text-anchor="middle" font-size="8" fill="#4ade80">C2.opn &lt; C1.cls · range ↑↓</text>

  <!-- ── Panel 2: BEARISH REVERSAL ── x=225 -->
  <rect x="225" y="8" width="205" height="240" rx="10" fill="#150505" stroke="#ef4444" stroke-width="0.6" stroke-opacity="0.4"/>
  <text x="327" y="26" text-anchor="middle" font-size="10" font-weight="700" fill="#f87171">▼ BEARISH REVERSAL</text>
  <text x="327" y="39" text-anchor="middle" font-size="8" fill="#604040">C1 bull · C2 bear · gap-up</text>
  <!-- C1 green -->
  <line x1="287" y1="55" x2="287" y2="68" stroke="#22c55e" stroke-width="2"/>
  <rect x="273"  y="68" width="28" height="75" rx="2" fill="#22c55e" opacity="0.85"/>
  <line x1="287" y1="143" x2="287" y2="162" stroke="#22c55e" stroke-width="2"/>
  <!-- C2 red -->
  <line x1="353" y1="48" x2="353" y2="58" stroke="#ef4444" stroke-width="2"/>
  <rect x="339"  y="58" width="28" height="112" rx="2" fill="#ef4444" opacity="0.85"/>
  <line x1="353" y1="170" x2="353" y2="185" stroke="#ef4444" stroke-width="2"/>
  <!-- C1 close dashed (top of bullish body = y=68) -->
  <line x1="269" y1="68" x2="390" y2="68" stroke="#22c55e" stroke-width="0.7" stroke-dasharray="4 3" stroke-opacity="0.6"/>
  <!-- C2 open marker y=58 -->
  <line x1="335" y1="58" x2="390" y2="58" stroke="#ef4444" stroke-width="0.7" stroke-dasharray="4 3" stroke-opacity="0.6"/>
  <!-- gap arrow -->
  <line x1="385" y1="60" x2="385" y2="66" stroke="#facc15" stroke-width="1.4" marker-start="url(#da)" marker-end="url(#da)"/>
  <text x="269" y="64" font-size="8" fill="#22c55e" text-anchor="end">C1.cls</text>
  <text x="269" y="56" font-size="8" fill="#ef4444" text-anchor="end">C2.opn</text>
  <text x="327" y="47" font-size="8" fill="#f87171" text-anchor="middle">C2.high &gt; C1.high</text>
  <text x="327" y="198" font-size="8" fill="#f87171" text-anchor="middle">C2.low &lt; C1.low</text>
  <text x="287" y="47" text-anchor="middle" font-size="10" font-weight="600" fill="#22c55e">C1</text>
  <text x="353" y="40" text-anchor="middle" font-size="10" font-weight="600" fill="#ef4444">C2</text>
  <rect x="233" y="206" width="189" height="34" rx="4" fill="#2d0808" stroke="#ef4444" stroke-width="0.4" stroke-opacity="0.4"/>
  <text x="327" y="218" text-anchor="middle" font-size="8" fill="#f87171">C1.cls &gt; C1.opn · C2.opn &gt; C2.cls</text>
  <text x="327" y="231" text-anchor="middle" font-size="8" fill="#f87171">C2.opn &gt; C1.cls · range ↑↓</text>

  <!-- ── Panel 3: BULL CONTINUATION ── x=440 -->
  <rect x="440" y="8" width="205" height="240" rx="10" fill="#020a0d" stroke="#38bdf8" stroke-width="0.6" stroke-opacity="0.4"/>
  <text x="542" y="26" text-anchor="middle" font-size="10" font-weight="700" fill="#38bdf8">↑ BULL CONTINUATION</text>
  <text x="542" y="39" text-anchor="middle" font-size="8" fill="#1e4060">C1 bull · C2 bull · gap-down</text>
  <!-- C1 green -->
  <line x1="502" y1="65" x2="502" y2="75" stroke="#22c55e" stroke-width="2"/>
  <rect x="488"  y="75" width="28" height="75" rx="2" fill="#22c55e" opacity="0.85"/>
  <line x1="502" y1="150" x2="502" y2="168" stroke="#22c55e" stroke-width="2"/>
  <!-- C2 green (larger) -->
  <line x1="568" y1="50" x2="568" y2="63" stroke="#38bdf8" stroke-width="2"/>
  <rect x="554"  y="63" width="28" height="112" rx="2" fill="#38bdf8" opacity="0.85"/>
  <line x1="568" y1="175" x2="568" y2="190" stroke="#38bdf8" stroke-width="2"/>
  <!-- C1 close dashed = bottom of bullish = y=150 -->
  <line x1="484" y1="150" x2="605" y2="150" stroke="#22c55e" stroke-width="0.7" stroke-dasharray="4 3" stroke-opacity="0.6"/>
  <!-- C2 open marker y=175 (below C1.close) -->
  <line x1="550" y1="175" x2="605" y2="175" stroke="#38bdf8" stroke-width="0.7" stroke-dasharray="4 3" stroke-opacity="0.6"/>
  <!-- gap arrow -->
  <line x1="599" y1="152" x2="599" y2="173" stroke="#facc15" stroke-width="1.4" marker-start="url(#da)" marker-end="url(#da)"/>
  <text x="484" y="154" font-size="8" fill="#22c55e" text-anchor="end">C1.cls</text>
  <text x="484" y="179" font-size="8" fill="#38bdf8" text-anchor="end">C2.opn</text>
  <text x="542" y="47" font-size="8" fill="#38bdf8" text-anchor="middle">C2.high &gt; C1.high</text>
  <text x="542" y="205" font-size="8" fill="#38bdf8" text-anchor="middle">C2.low &lt; C1.low</text>
  <text x="502" y="57" text-anchor="middle" font-size="10" font-weight="600" fill="#22c55e">C1</text>
  <text x="568" y="42" text-anchor="middle" font-size="10" font-weight="600" fill="#38bdf8">C2</text>
  <rect x="448" y="206" width="189" height="34" rx="4" fill="#071a2d" stroke="#38bdf8" stroke-width="0.4" stroke-opacity="0.4"/>
  <text x="542" y="218" text-anchor="middle" font-size="8" fill="#38bdf8">C1.cls &gt; C1.opn · C2.opn &lt; C2.cls</text>
  <text x="542" y="231" text-anchor="middle" font-size="8" fill="#38bdf8">C2.opn &lt; C1.cls · range ↑↓</text>

  <!-- ── Panel 4: BEAR CONTINUATION ── x=655 -->
  <rect x="655" y="8" width="205" height="240" rx="10" fill="#0d0208" stroke="#e879f9" stroke-width="0.6" stroke-opacity="0.4"/>
  <text x="757" y="26" text-anchor="middle" font-size="10" font-weight="700" fill="#e879f9">↓ BEAR CONTINUATION</text>
  <text x="757" y="39" text-anchor="middle" font-size="8" fill="#4a1560">C1 bear · C2 bear · gap-up</text>
  <!-- C1 red -->
  <line x1="717" y1="68" x2="717" y2="80" stroke="#ef4444" stroke-width="2"/>
  <rect x="703"  y="80" width="28" height="75" rx="2" fill="#ef4444" opacity="0.85"/>
  <line x1="717" y1="155" x2="717" y2="172" stroke="#ef4444" stroke-width="2"/>
  <!-- C2 purple/red (larger) -->
  <line x1="783" y1="48" x2="783" y2="63" stroke="#e879f9" stroke-width="2"/>
  <rect x="769"  y="63" width="28" height="120" rx="2" fill="#e879f9" opacity="0.85"/>
  <line x1="783" y1="183" x2="783" y2="198" stroke="#e879f9" stroke-width="2"/>
  <!-- C1 close = top of red body = y=80 -->
  <line x1="699" y1="80" x2="820" y2="80" stroke="#ef4444" stroke-width="0.7" stroke-dasharray="4 3" stroke-opacity="0.6"/>
  <!-- C2 open y=63 (above C1.close = above y=80, so y=63 is higher = gap-up) -->
  <line x1="765" y1="63" x2="820" y2="63" stroke="#e879f9" stroke-width="0.7" stroke-dasharray="4 3" stroke-opacity="0.6"/>
  <!-- gap arrow -->
  <line x1="815" y1="65" x2="815" y2="78" stroke="#facc15" stroke-width="1.4" marker-start="url(#da)" marker-end="url(#da)"/>
  <text x="699" y="84" font-size="8" fill="#ef4444" text-anchor="end">C1.cls</text>
  <text x="699" y="61" font-size="8" fill="#e879f9" text-anchor="end">C2.opn</text>
  <text x="757" y="47" font-size="8" fill="#e879f9" text-anchor="middle">C2.high &gt; C1.high</text>
  <text x="757" y="212" font-size="8" fill="#e879f9" text-anchor="middle">C2.low &lt; C1.low</text>
  <text x="717" y="60" text-anchor="middle" font-size="10" font-weight="600" fill="#ef4444">C1</text>
  <text x="783" y="40" text-anchor="middle" font-size="10" font-weight="600" fill="#e879f9">C2</text>
  <rect x="663" y="206" width="189" height="34" rx="4" fill="#1f0728" stroke="#e879f9" stroke-width="0.4" stroke-opacity="0.4"/>
  <text x="757" y="218" text-anchor="middle" font-size="8" fill="#e879f9">C1.cls &lt; C1.opn · C2.opn &gt; C2.cls</text>
  <text x="757" y="231" text-anchor="middle" font-size="8" fill="#e879f9">C2.opn &gt; C1.cls · range ↑↓</text>

  <!-- ══ BOTTOM ROW: FVG explanation ══ -->
  <rect x="10" y="268" width="880" height="244" rx="10" fill="#080d1c" stroke="#f97316" stroke-width="0.6" stroke-opacity="0.35"/>
  <text x="450" y="288" text-anchor="middle" font-size="11" font-weight="700" fill="#f97316">⚡ FAIR VALUE GAP (FVG) — TOUCH CHECK</text>
  <text x="450" y="302" text-anchor="middle" font-size="9" fill="#5a3a10">Scans last 10 candles · Wick must overlap FVG zone</text>

  <!-- Bullish FVG panel -->
  <rect x="28" y="312" width="390" height="185" rx="8" fill="#020d06" stroke="#22c55e" stroke-width="0.5" stroke-opacity="0.3"/>
  <text x="223" y="328" text-anchor="middle" font-size="10" font-weight="700" fill="#4ade80">Bullish FVG — Checked for BULLISH &amp; BULL-CONT</text>

  <!-- 3 candles forming FVG -->
  <!-- Prev (i-1) -->
  <line x1="90" y1="355" x2="90" y2="365" stroke="#ef4444" stroke-width="1.5"/>
  <rect x="78" y="365" width="24" height="50" rx="2" fill="#ef4444" opacity="0.8"/>
  <line x1="90" y1="415" x2="90" y2="430" stroke="#ef4444" stroke-width="1.5"/>
  <text x="90" y="345" text-anchor="middle" font-size="8" fill="#7a8fac">i-1</text>
  <!-- prev.high label -->
  <text x="74" y="369" font-size="8" fill="#22c55e" text-anchor="end">prev.high</text>

  <!-- Impulse candle (i) -->
  <line x1="135" y1="340" x2="135" y2="352" stroke="#22c55e" stroke-width="1.5"/>
  <rect x="123" y="352" width="24" height="65" rx="2" fill="#22c55e" opacity="0.8"/>
  <line x1="135" y1="417" x2="135" y2="432" stroke="#22c55e" stroke-width="1.5"/>
  <text x="135" y="333" text-anchor="middle" font-size="8" fill="#7a8fac">i (impulse)</text>

  <!-- Next (i+1) -->
  <line x1="180" y1="347" x2="180" y2="358" stroke="#22c55e" stroke-width="1.5"/>
  <rect x="168" y="358" width="24" height="55" rx="2" fill="#22c55e" opacity="0.8"/>
  <line x1="180" y1="413" x2="180" y2="425" stroke="#22c55e" stroke-width="1.5"/>
  <text x="180" y="340" text-anchor="middle" font-size="8" fill="#7a8fac">i+1</text>
  <!-- next.low label -->
  <text x="74" y="362" font-size="8" fill="#22c55e" text-anchor="end">next.low</text>

  <!-- FVG zone shaded between prev.high(365) and next.low(358) — actually gap is prev.high < next.low -->
  <!-- Let's show it clearly: prev.high = y=365, next.low = y=358, gap shaded -->
  <rect x="78" y="358" width="118" height="7" rx="1" fill="#f97316" opacity="0.25"/>
  <line x1="74" y1="358" x2="210" y2="358" stroke="#f97316" stroke-width="0.8" stroke-dasharray="3 2" stroke-opacity="0.8"/>
  <line x1="74" y1="365" x2="210" y2="365" stroke="#f97316" stroke-width="0.8" stroke-dasharray="3 2" stroke-opacity="0.8"/>
  <text x="215" y="360" font-size="8" fill="#f97316">FVG top (next.low)</text>
  <text x="215" y="368" font-size="8" fill="#f97316">FVG bot (prev.high)</text>
  <text x="215" y="377" font-size="8" fill="#f0b429">gap = next.low &gt; prev.high</text>

  <!-- Engulfer C2 touching the FVG -->
  <line x1="285" y1="338" x2="285" y2="350" stroke="#22c55e" stroke-width="1.5"/>
  <rect x="273" y="350" width="24" height="60" rx="2" fill="#22c55e" opacity="0.75"/>
  <line x1="285" y1="410" x2="285" y2="438" stroke="#22c55e" stroke-width="2.5" stroke-linecap="round"/>
  <!-- C2 low wick touching FVG -->
  <line x1="260" y1="438" x2="320" y2="438" stroke="#22c55e" stroke-width="0.6" stroke-dasharray="3 2" stroke-opacity="0.5"/>
  <text x="327" y="441" font-size="8" fill="#22c55e">C2.low (wick)</text>
  <!-- FVG zone at this x -->
  <line x1="260" y1="358" x2="320" y2="358" stroke="#f97316" stroke-width="0.8" stroke-dasharray="3 2" stroke-opacity="0.7"/>
  <line x1="260" y1="365" x2="320" y2="365" stroke="#f97316" stroke-width="0.8" stroke-dasharray="3 2" stroke-opacity="0.7"/>
  <rect x="260" y="358" width="60" height="7" rx="1" fill="#f97316" opacity="0.2"/>
  <text x="285" y="330" text-anchor="middle" font-size="8" fill="#7a8fac">C2 engulfer</text>
  <!-- touch arrow -->
  <line x1="285" y1="410" x2="285" y2="366" stroke="#facc15" stroke-width="1.2" stroke-dasharray="3 2" stroke-opacity="0.8"/>
  <text x="327" y="362" font-size="8" fill="#f97316">FVG zone</text>
  <text x="160" y="475" text-anchor="middle" font-size="8.5" fill="#7a8fac">✓  C2.low enters FVG zone → wick touches bullish imbalance</text>
  <text x="223" y="490" text-anchor="middle" font-size="8" fill="#3d6040">Overlap condition: C2.low ≤ FVG.top  AND  C2.high ≥ FVG.bot</text>

  <!-- Bearish FVG panel -->
  <rect x="468" y="312" width="400" height="185" rx="8" fill="#150505" stroke="#ef4444" stroke-width="0.5" stroke-opacity="0.3"/>
  <text x="668" y="328" text-anchor="middle" font-size="10" font-weight="700" fill="#f87171">Bearish FVG — Checked for BEARISH &amp; BEAR-CONT</text>

  <!-- 3 candles forming bearish FVG -->
  <line x1="530" y1="345" x2="530" y2="358" stroke="#22c55e" stroke-width="1.5"/>
  <rect x="518" y="358" width="24" height="50" rx="2" fill="#22c55e" opacity="0.8"/>
  <line x1="530" y1="408" x2="530" y2="425" stroke="#22c55e" stroke-width="1.5"/>
  <text x="530" y="338" text-anchor="middle" font-size="8" fill="#7a8fac">i-1</text>
  <text x="514" y="412" font-size="8" fill="#ef4444" text-anchor="end">prev.low</text>

  <!-- Impulse bearish -->
  <line x1="575" y1="338" x2="575" y2="350" stroke="#ef4444" stroke-width="1.5"/>
  <rect x="563" y="350" width="24" height="70" rx="2" fill="#ef4444" opacity="0.8"/>
  <line x1="575" y1="420" x2="575" y2="435" stroke="#ef4444" stroke-width="1.5"/>
  <text x="575" y="330" text-anchor="middle" font-size="8" fill="#7a8fac">i (impulse)</text>

  <!-- Next (i+1) -->
  <line x1="620" y1="355" x2="620" y2="368" stroke="#ef4444" stroke-width="1.5"/>
  <rect x="608" y="368" width="24" height="55" rx="2" fill="#ef4444" opacity="0.8"/>
  <line x1="620" y1="423" x2="620" y2="437" stroke="#ef4444" stroke-width="1.5"/>
  <text x="620" y="347" text-anchor="middle" font-size="8" fill="#7a8fac">i+1</text>
  <text x="514" y="406" font-size="8" fill="#ef4444" text-anchor="end">next.high</text>

  <!-- Bearish FVG zone: next.high(368) to prev.low(408) — gap when next.high < prev.low -->
  <rect x="518" y="368" width="118" height="40" rx="1" fill="#f97316" opacity="0.12"/>
  <line x1="514" y1="368" x2="658" y2="368" stroke="#f97316" stroke-width="0.8" stroke-dasharray="3 2" stroke-opacity="0.8"/>
  <line x1="514" y1="408" x2="658" y2="408" stroke="#f97316" stroke-width="0.8" stroke-dasharray="3 2" stroke-opacity="0.8"/>
  <text x="662" y="371" font-size="8" fill="#f97316">FVG top (prev.low)</text>
  <text x="662" y="411" font-size="8" fill="#f97316">FVG bot (next.high)</text>
  <text x="662" y="390" font-size="8" fill="#f0b429">gap = next.high &lt; prev.low</text>

  <!-- Engulfer C2 high touching bearish FVG -->
  <line x1="750" y1="345" x2="750" y2="358" stroke="#ef4444" stroke-width="2.5" stroke-linecap="round"/>
  <rect x="738" y="358" width="24" height="75" rx="2" fill="#ef4444" opacity="0.75"/>
  <line x1="750" y1="433" x2="750" y2="448" stroke="#ef4444" stroke-width="1.5"/>
  <text x="750" y="336" text-anchor="middle" font-size="8" fill="#7a8fac">C2 engulfer</text>
  <!-- C2 high wick at y=345 touching FVG zone 368-408 — let's put wick at y=372 inside zone -->
  <line x1="750" y1="345" x2="750" y2="375" stroke="#ef4444" stroke-width="2.5"/>
  <line x1="720" y1="358" x2="790" y2="358" stroke="#ef4444" stroke-width="0.6" stroke-dasharray="3 2" stroke-opacity="0.5"/>
  <text x="795" y="361" font-size="8" fill="#ef4444">C2.high (wick)</text>
  <line x1="720" y1="368" x2="790" y2="368" stroke="#f97316" stroke-width="0.8" stroke-dasharray="3 2" stroke-opacity="0.7"/>
  <line x1="720" y1="408" x2="790" y2="408" stroke="#f97316" stroke-width="0.8" stroke-dasharray="3 2" stroke-opacity="0.7"/>
  <rect x="720" y="368" width="70" height="40" rx="1" fill="#f97316" opacity="0.15"/>
  <line x1="750" y1="358" x2="750" y2="367" stroke="#facc15" stroke-width="1.2" stroke-dasharray="3 2" stroke-opacity="0.8"/>
  <text x="600" y="475" text-anchor="middle" font-size="8.5" fill="#7a8fac">✓  C2.high enters FVG zone → wick touches bearish imbalance</text>
  <text x="668" y="490" text-anchor="middle" font-size="8" fill="#604040">Overlap condition: C2.high ≥ FVG.bot  AND  C2.low ≤ FVG.top</text>
</svg>
"""

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### ₿ Scanner Config")
    st.markdown("---")
    tf_choice = st.selectbox("⏱ Timeframe", ["1H", "4H", "1D", "1W"], index=2)
    lookback  = st.selectbox("🔭 Candles to scan", [5, 10, 15, 20], index=1)

    st.markdown("---")
    st.markdown("### 🌐 Universe")
    with st.spinner("Loading Binance symbols…"):
        ALL_SYMBOLS, _sym_err = get_all_symbols()
    if _sym_err:
        if "❌" in _sym_err:
            st.error(_sym_err)
        else:
            st.warning(_sym_err)
    if not ALL_SYMBOLS:
        st.error("Cannot load symbols — check internet / Binance availability.")

    universe = st.radio("Scan", ["All USDT Perpetuals", "Custom symbols"], index=0)
    if universe == "Custom symbols":
        raw     = st.text_area("Symbols (comma/newline)", value="BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT", height=110)
        valid   = set(ALL_SYMBOLS)
        tickers = [t.strip().upper() for t in raw.replace("\n", ",").split(",") if t.strip()]
        tickers = [t for t in tickers if t in valid] or tickers
    else:
        tickers = ALL_SYMBOLS

    st.caption(f"**{len(tickers)}** symbols selected")

    st.markdown("---")
    st.markdown("### 🔽 Filters")
    show_bull      = st.checkbox("🟢 Bullish Reversal",    value=True)
    show_bear      = st.checkbox("🔴 Bearish Reversal",    value=True)
    show_bullcont  = st.checkbox("🔵 Bull Continuation",   value=True)
    show_bearcont  = st.checkbox("🟣 Bear Continuation",   value=True)
    fvg_only       = st.checkbox("⚡ FVG touch only",       value=False,
                                  help="Show only signals where C2 wick touches an FVG")

    st.markdown("---")
    st.markdown("### ⚡ Speed")
    workers  = st.slider("Parallel workers", 1, 25, 12)
    delay_ms = st.slider("Batch delay (ms)", 0, 400, 80, step=20)

    st.markdown("---")
    run_btn  = st.button("▶  SCAN NOW")
    stop_btn = st.button("⏹  STOP SCAN")
    if stop_btn:
        st.session_state["_stop"] = True

# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div style="margin-bottom:.3rem;">
  <span style="font-family:'Space Grotesk',sans-serif;font-weight:800;font-size:2rem;
    letter-spacing:-.04em;background:linear-gradient(115deg,#f0b429 0%,#f97316 45%,#ef4444 100%);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;">
    ₿ Binance Futures · Engulfing Scanner <span style="font-size:1rem;opacity:.5">v4</span>
  </span>
</div>
<div style="font-family:'JetBrains Mono',monospace;font-size:.67rem;color:#2d4060;
  letter-spacing:.18em;text-transform:uppercase;margin-bottom:.8rem;">
  USDT-M Perpetuals · 4 Pattern Types · FVG Touch Check · No API Key
</div>
""", unsafe_allow_html=True)

h1, h2 = st.columns([1, 4])
with h1:
    st.markdown(
        '<span style="background:#080d1c;border:1px solid #0a5c3a;border-radius:999px;'
        'padding:.18rem .75rem;font-family:\'JetBrains Mono\',monospace;font-size:.65rem;'
        'color:#34d399;letter-spacing:.12em;">'
        '<span class="live-dot"></span>BINANCE LIVE</span>',
        unsafe_allow_html=True,
    )
with h2:
    st.markdown(
        f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:.72rem;color:#2d4060;">'
        f'{len(tickers)} symbols &nbsp;·&nbsp; {tf_choice} &nbsp;·&nbsp; '
        f'last {lookback} candles &nbsp;·&nbsp; {workers} workers &nbsp;·&nbsp; '
        f'FVG lookback 10</span>',
        unsafe_allow_html=True,
    )

# ── Definition diagram ─────────────────────────────────────────────────────
with st.expander("📐 Pattern & FVG Definitions — click to expand", expanded=False):
    st.markdown("""
<div class="def-box">
  <div style="font-family:'JetBrains Mono',monospace;font-size:.62rem;color:#f0b429;
    letter-spacing:.2em;text-transform:uppercase;margin-bottom:.6rem;">Pattern Rules</div>
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:.8rem;">
    <div>
      <div style="color:#4ade80;font-size:.72rem;font-weight:700;margin-bottom:.3rem;">▲ Bullish Reversal</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:.68rem;color:#7a8fac;line-height:1.9;">
        C1 bearish → C1.cls &lt; C1.opn<br>C2 bullish → C2.opn &lt; C2.cls<br>
        C2.low &lt; C1.low<br>C2.high &gt; C1.high<br>
        <span style="color:#4ade80;">C2.opn &lt; C1.cls</span>
      </div>
    </div>
    <div>
      <div style="color:#f87171;font-size:.72rem;font-weight:700;margin-bottom:.3rem;">▼ Bearish Reversal</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:.68rem;color:#7a8fac;line-height:1.9;">
        C1 bullish → C1.cls &gt; C1.opn<br>C2 bearish → C2.opn &gt; C2.cls<br>
        C2.low &lt; C1.low<br>C2.high &gt; C1.high<br>
        <span style="color:#f87171;">C2.opn &gt; C1.cls</span>
      </div>
    </div>
    <div>
      <div style="color:#38bdf8;font-size:.72rem;font-weight:700;margin-bottom:.3rem;">↑ Bull Continuation</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:.68rem;color:#7a8fac;line-height:1.9;">
        C1 bullish → C1.cls &gt; C1.opn<br>C2 bullish → C2.opn &lt; C2.cls<br>
        C2.low &lt; C1.low<br>C2.high &gt; C1.high<br>
        <span style="color:#38bdf8;">C2.opn &lt; C1.cls (gap-down)</span>
      </div>
    </div>
    <div>
      <div style="color:#e879f9;font-size:.72rem;font-weight:700;margin-bottom:.3rem;">↓ Bear Continuation</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:.68rem;color:#7a8fac;line-height:1.9;">
        C1 bearish → C1.cls &lt; C1.opn<br>C2 bearish → C2.opn &gt; C2.cls<br>
        C2.low &lt; C1.low<br>C2.high &gt; C1.high<br>
        <span style="color:#e879f9;">C2.opn &gt; C1.cls (gap-up)</span>
      </div>
    </div>
  </div>
  <div style="margin-top:.8rem;padding-top:.6rem;border-top:1px solid #0f1a2e;
    font-family:'JetBrains Mono',monospace;font-size:.68rem;color:#7a8fac;line-height:1.9;">
    <span style="color:#f97316;">⚡ FVG Check</span> &nbsp;·&nbsp;
    Bullish/Bull-Cont → C2.low touches a Bullish FVG (prev.high &lt; next.low gap) in last 10 candles<br>
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
    Bearish/Bear-Cont → C2.high touches a Bearish FVG (next.high &lt; prev.low gap) in last 10 candles<br>
    Touch = C2 wick range [C2.low, C2.high] overlaps FVG zone [bot, top]
  </div>
</div>
""", unsafe_allow_html=True)
    st.markdown(DIAGRAM_SVG, unsafe_allow_html=True)

st.markdown("")

# ══════════════════════════════════════════════════════════════════════════════
# STAT METRICS
# ══════════════════════════════════════════════════════════════════════════════
mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
m_total   = mc1.empty()
m_scanned = mc2.empty()
m_bull    = mc3.empty()
m_bear    = mc4.empty()
m_cont    = mc5.empty()
m_fvg     = mc6.empty()

def upd(total, scanned, bull, bear, cont, fvg_hits):
    m_total.metric("Symbols",      total)
    m_scanned.metric("Scanned",    f"{scanned}/{total}")
    m_bull.metric("🟢 Bullish Rev", bull)
    m_bear.metric("🔴 Bearish Rev", bear)
    m_cont.metric("🔵🟣 Cont",       cont)
    m_fvg.metric("⚡ FVG Touch",    fvg_hits)

upd(len(tickers), 0, 0, 0, 0, 0)

prog_bar   = st.progress(0.0)
status_txt = st.empty()

# ── Results layout ─────────────────────────────────────────────────────────
st.markdown('<div class="sec-hdr">Scan output</div>', unsafe_allow_html=True)
log_col, res_col = st.columns([1, 2], gap="medium")
with log_col:
    st.markdown('<div class="sec-hdr" style="font-size:.55rem;">Progress log</div>', unsafe_allow_html=True)
    log_ph = st.empty()
with res_col:
    st.markdown('<div class="sec-hdr" style="font-size:.55rem;">Signals found</div>', unsafe_allow_html=True)
    res_ph = st.empty()

# ══════════════════════════════════════════════════════════════════════════════
# RUN SCAN
# ══════════════════════════════════════════════════════════════════════════════
_SHOW_MAP = {
    "BULLISH":   lambda: show_bull,
    "BEARISH":   lambda: show_bear,
    "BULL-CONT": lambda: show_bullcont,
    "BEAR-CONT": lambda: show_bearcont,
}

if run_btn:
    st.session_state["_stop"] = False

    interval  = TF_MAP[tf_choice]
    FVG_LB    = 10  # fixed per user preference
    # We need: lookback candle-pairs to scan for engulfing + FVG_LB extra candles
    # before each pair so FVG detection has history + 2 buffer.
    # fetch enough so that even the earliest engulf pair (candles[0],candles[1])
    # has FVG_LB candles before it.  Total = FVG_LB + lookback + 2
    limit     = FVG_LB + lookback + 5
    delay_s   = delay_ms / 1000.0
    total     = len(tickers)

    log_lines: list[str]   = []
    result_html: str       = ""
    all_sigs: list[Signal] = []
    bull_count = bear_count = cont_count = fvg_count = error_count = scanned = 0

    status_txt.info(f"🔍 Scanning **{total}** USDT-M perpetuals on **{tf_choice}** — 4 pattern types + FVG check…")

    for batch_start in range(0, total, workers):
        if st.session_state.get("_stop"):
            status_txt.warning("⏹ Scan stopped.")
            break

        batch = tickers[batch_start : batch_start + workers]
        args  = [(sym, interval, limit, tf_choice, FVG_LB, lookback) for sym in batch]

        with ThreadPoolExecutor(max_workers=len(batch)) as pool:
            futs = {pool.submit(scan_symbol, a): a[0] for a in args}

            for fut in as_completed(futs):
                if st.session_state.get("_stop"):
                    break

                sym, sigs, had_error, err_msg = fut.result()
                scanned += 1

                if had_error:
                    error_count += 1
                    log_lines.append(log_row_html(sym, "err"))
                elif sigs:
                    log_lines.append(log_row_html(sym, "hit", len(sigs)))
                    for sig in sigs:
                        all_sigs.append(sig)
                        if sig.kind == "BULLISH":    bull_count += 1
                        elif sig.kind == "BEARISH":  bear_count += 1
                        else:                         cont_count += 1
                        if sig.fvg_touched:           fvg_count  += 1

                        # Visibility filter
                        visible = _SHOW_MAP.get(sig.kind, lambda: True)()
                        if fvg_only and not sig.fvg_touched:
                            visible = False

                        if visible:
                            result_html = signal_card(sig) + result_html
                else:
                    log_lines.append(log_row_html(sym, "ok"))

                log_ph.markdown("".join(log_lines[-30:]), unsafe_allow_html=True)
                res_ph.markdown(
                    f'<div class="res-pane">{result_html}</div>'
                    if result_html else
                    '<div class="log-row" style="color:#1a2e45;">No signals yet…</div>',
                    unsafe_allow_html=True,
                )
                prog_bar.progress(min(scanned / total, 1.0))
                upd(total, scanned, bull_count, bear_count, cont_count, fvg_count)

        if delay_s > 0 and not st.session_state.get("_stop"):
            time.sleep(delay_s)

    # ── Done ─────────────────────────────────────────────────────────────────
    prog_bar.progress(1.0)
    if not st.session_state.get("_stop"):
        status_txt.success(
            f"✅ Complete — **{scanned}** symbols · "
            f"**{bull_count}** bull rev · **{bear_count}** bear rev · "
            f"**{cont_count}** continuations · **{fvg_count}** FVG touches · "
            f"**{error_count}** errors"
        )

    if not result_html:
        # ── Diagnostic: tell user exactly WHY nothing showed ──────────────────
        diag_parts = []
        if error_count == scanned:
            diag_parts.append(
                "🔴 **All symbol fetches failed.** Binance may be blocking this server's IP. "
                "Try switching to 'Custom symbols' with a small list (e.g. BTCUSDT, ETHUSDT) "
                "to confirm connectivity, or re-deploy the app."
            )
        elif error_count > scanned * 0.5:
            diag_parts.append(
                f"⚠️ **{error_count}/{scanned} fetches failed** — possible rate-limit or geo-block. "
                "Reduce parallel workers and try again."
            )
        elif len(all_sigs) > 0 and not result_html:
            diag_parts.append(
                f"🔍 **{len(all_sigs)} signal(s) found but all filtered out.** "
                "Uncheck '⚡ FVG touch only' or enable more pattern types in the sidebar."
            )
        else:
            diag_parts.append(
                f"ℹ️ **0 patterns matched** across {scanned} symbols on **{tf_choice}** "
                f"with {lookback}-candle lookback. "
                "Try a higher candle count (15–20), a different timeframe, or a longer lookback."
            )
        for msg in diag_parts:
            st.warning(msg)

    # ── Export ────────────────────────────────────────────────────────────────
    if all_sigs:
        st.markdown('<div class="sec-hdr">Export results</div>', unsafe_allow_html=True)
        tbl_col, dl_col = st.columns([3, 1], gap="medium")

        with tbl_col:
            df_show = pd.DataFrame([{
                "Type":       s.kind,
                "Symbol":     s.symbol,
                "Datetime":   s.dt.strftime("%d %b %Y %H:%M"),
                "FVG Touch":  "⚡ YES" if s.fvg_touched else "—",
                "FVG Zone":   f"{fp(s.fvg_bot)}–{fp(s.fvg_top)}" if s.fvg_touched and s.fvg_top else "—",
                "C1 High": s.c1_high, "C1 Low":  s.c1_low,
                "C1 Open": s.c1_open, "C1 Close": s.c1_close,
                "C2 High": s.c2_high, "C2 Low":  s.c2_low,
                "C2 Open": s.c2_open, "C2 Close": s.c2_close,
                "Body %":  s.body_pct,
            } for s in all_sigs])

            def colour_type(v):
                if v == "BULLISH":   return "color: #4ade80"
                if v == "BEARISH":   return "color: #f87171"
                if v == "BULL-CONT": return "color: #38bdf8"
                if v == "BEAR-CONT": return "color: #e879f9"
                return ""

            st.dataframe(
                df_show.style.applymap(colour_type, subset=["Type"]),
                use_container_width=True, height=300,
            )

        with dl_col:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
            st.download_button(
                label="⬇ Download CSV",
                data=build_csv(all_sigs),
                file_name=f"binance_engulfing_v4_{tf_choice}_{ts}.csv",
                mime="text/csv",
            )
            st.markdown(
                f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:.7rem;'
                f'color:#2d4060;margin-top:.6rem;line-height:1.9;">'
                f'Total &nbsp;&nbsp; : {len(all_sigs)}<br>'
                f'Bull Rev &nbsp;: {bull_count}<br>'
                f'Bear Rev &nbsp;: {bear_count}<br>'
                f'Continuations: {cont_count}<br>'
                f'FVG Touches : {fvg_count}</div>',
                unsafe_allow_html=True,
            )
