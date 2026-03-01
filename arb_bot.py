"""
╔══════════════════════════════════════════════════════════════════════╗
║  DEMINIA BOT V.2 —  Production Ready                                  ║
║  1.  Odds Staleness + Slippage Guard   9.  Profitability Guard        ║
║  2.  Max/Min Odds Filter              10.  CLV Benchmark + Settlement ║
║  3.  Alert Cooldown + Multi-chat      11.  Manual Settle (/settle)    ║
║  4.  P&L Tracker + /trades command   12.  Sport Rotation              ║
║  5.  Turso persistent DB (sync+async) 13.  Thread-safe _data_lock     ║
║  6.  Scanner asyncio.Event wakeup     14.  Dashboard Force Settle UI  ║
║  7.  Line Movement (Steam + RLM)      15.  Kelly Criterion stake      ║
║  8.  Soccer 3-way Arb (1X2 calc)     16.  Signal TTL + WAL DB        ║
╠══════════════════════════════════════════════════════════════════════╣
║  17. Auth guard all slash cmds   19. Bankroll auto-stop guard          ║
║  18. Bearer-only dashboard auth  20. VB slippage refetch guard         ║
║  21. opp_log confirmed-after-exec 22. 3-way post-round profit guard    ║
║  23. Live odds in TradeRecord    24. MANUAL_REVIEW no-spam alert       ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio, json, logging, os, random, re, signal, sqlite3, threading, time, uuid  # re already imported at top (Q6)
import urllib.request, urllib.error
# v10-6: ใช้ Turso HTTP REST API ตรงๆ — ไม่พึ่ง libsql_client
_TURSO_API = "http"  # always http mode
_libsql_mod = None
HAS_TURSO = True  # จะ check จริงตอน turso_init
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass, field
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

import aiohttp
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════
def _d(k,v): return Decimal(os.getenv(k,v))
def _s(k,v): return os.getenv(k,v)
def _i(k,v): return int(os.getenv(k,str(v)))

ODDS_API_KEY    = _s("ODDS_API_KEY",    "")
TELEGRAM_TOKEN  = _s("TELEGRAM_TOKEN",  "")
CHAT_ID         = _s("CHAT_ID",         "")

# Validate required credentials at startup
for _env_name, _env_val in [("ODDS_API_KEY", ODDS_API_KEY), ("TELEGRAM_TOKEN", TELEGRAM_TOKEN), ("CHAT_ID", CHAT_ID)]:
    if not _env_val:
        raise RuntimeError(f"Missing required env var: {_env_name} — set it in Railway Variables")
EXTRA_CHAT_IDS  = [c.strip() for c in _s("EXTRA_CHAT_IDS","").split(",") if c.strip()]  # 9. multi-chat
PORT            = _i("PORT",            8080)
DB_PATH         = _s("DB_PATH",         "/tmp/arb_bot.db")   # local fallback
TURSO_URL       = _s("TURSO_URL",       "")   # libsql://your-db.turso.io
TURSO_TOKEN     = _s("TURSO_TOKEN",     "")   # eyJ...
USE_TURSO       = bool(TURSO_URL and TURSO_TOKEN)
DASHBOARD_TOKEN = _s("DASHBOARD_TOKEN", "")   # ตั้งใน Railway เพื่อป้องกัน dashboard

TOTAL_STAKE_THB = _d("TOTAL_STAKE_THB","10000")
USD_TO_THB      = _d("USD_TO_THB",     "35")
TOTAL_STAKE     = TOTAL_STAKE_THB / USD_TO_THB

MIN_PROFIT_PCT  = _d("MIN_PROFIT_PCT",  "0.015")
SCAN_INTERVAL   = _i("SCAN_INTERVAL",   300)
AUTO_SCAN_START = _s("AUTO_SCAN_START","true").lower() == "true"
QUOTA_WARN_AT   = _i("QUOTA_WARN_AT",   50)

# Webhook (ใส่ใน Railway Variables)
WEBHOOK_URL     = _s("WEBHOOK_URL", "")
WEBHOOK_PATH    = "/webhook"
USE_WEBHOOK     = bool(WEBHOOK_URL and "railway.app" in (WEBHOOK_URL or ""))

# Kelly Criterion
KELLY_FRACTION  = _d("KELLY_FRACTION", "0.25")   # คงไว้ที่ 0.25 เพื่อความปลอดภัย
BANKROLL_THB    = _d("BANKROLL_THB", "100000")  # v10-13: default 100k — ตั้ง env BANKROLL_THB เองถ้าใช้เงินจริงมากกว่านี้
USE_KELLY       = _s("USE_KELLY", "true").lower() == "true"
MIN_KELLY_STAKE = _d("MIN_KELLY_STAKE", "10000") # บังคับขั้นต่ำ 10,000 บาท
MAX_KELLY_STAKE = _d("MAX_KELLY_STAKE", "50000") # เพดานสูงสุดต่อรอบ
MAX_DAILY_LOSS_THB = _d("MAX_DAILY_LOSS_THB", "0")  # 0 = ไม่จำกัด

# 1. Odds staleness — ไม่รับ odds ที่เก่ากว่านี้ (นาที)
MAX_ODDS_AGE_MIN   = _i("MAX_ODDS_AGE_MIN",  5)
# 2. Max/Min odds filter
MAX_ODDS_ALLOWED   = _d("MAX_ODDS_ALLOWED",  "15")   # กรอง odds > 15 ออก
MIN_ODDS_ALLOWED   = _d("MIN_ODDS_ALLOWED",  "1.05") # กรอง odds < 1.05 ออก
# 3. Alert cooldown per event (นาที)
ALERT_COOLDOWN_MIN = _i("ALERT_COOLDOWN_MIN", 30)
# 5. Max stake per bookmaker (THB) — 0 = ไม่จำกัด
MAX_STAKE_PINNACLE = _d("MAX_STAKE_PINNACLE", "0")
MAX_STAKE_1XBET    = _d("MAX_STAKE_1XBET",    "0")
MAX_STAKE_DAFABET  = _d("MAX_STAKE_DAFABET",  "0")
# 7. Line movement threshold
LINE_MOVE_THRESHOLD = _d("LINE_MOVE_THRESHOLD", "0.05")  # 5%
# 9. Multi-chat
ALL_CHAT_IDS = list(dict.fromkeys([CHAT_ID] + EXTRA_CHAT_IDS))  # B7: dedupe
# Polymarket liquidity filters
POLY_MIN_LIQUIDITY      = float(os.getenv("POLY_MIN_LIQUIDITY",      "200"))   # USD
POLY_MIN_DRAW_LIQUIDITY = float(os.getenv("POLY_MIN_DRAW_LIQUIDITY",  "100"))   # USD — Draw market มัก liquidity ต่ำกว่า
RLM_MIN_LIQUIDITY_USD   = float(os.getenv("RLM_MIN_LIQUIDITY_USD",  "10000"))  # USD — RLM signal

_SPORTS_DEFAULT = (
    "basketball_nba,basketball_euroleague,basketball_ncaab,"
    "americanfootball_nfl,"
    "soccer_epl,soccer_uefa_champs_league,soccer_spain_la_liga,soccer_germany_bundesliga,"
    "soccer_italy_serie_a,soccer_france_ligue_one,soccer_fifa_world_cup,"
    "baseball_mlb,"
    "icehockey_nhl,"
    "cricket_test_match,cricket_odi,"
    "mma_mixed_martial_arts"
)
SPORTS     = [s.strip() for s in _s("SPORTS",_SPORTS_DEFAULT).split(",") if s.strip()]
BOOKMAKERS      = _s("BOOKMAKERS","pinnacle,onexbet,dafabet")
SIGNAL_TTL_SEC  = _i("SIGNAL_TTL_SEC", 900)   # F7: constant — อ่านครั้งเดียวตอน startup

SPORT_EMOJI = {
    "basketball_nba":"🏀","basketball_euroleague":"🏀","basketball_ncaab":"🏀",
    "americanfootball_nfl":"🏈","americanfootball_nfl_super_bowl_winner":"🏈",
    "soccer_epl":"⚽","soccer_uefa_champs_league":"⚽",
    "soccer_spain_la_liga":"⚽","soccer_germany_bundesliga":"⚽",
    "soccer_italy_serie_a":"⚽","soccer_france_ligue_one":"⚽",
    "soccer_fifa_world_cup":"⚽",
    "tennis_atp_wimbledon":"🎾","tennis_wta":"🎾",
    "baseball_mlb":"⚾",
    "icehockey_nhl":"🏒",
    "cricket_test_match":"🏏","cricket_odi":"🏏",
    "mma_mixed_martial_arts":"🥊",
    "esports_csgo":"🎮","esports_dota2":"🎮","esports_lol":"🎮",
}

# กีฬาที่ควรเน้น H2H/Moneyline (Sharp money เข้ามากที่ตลาดนี้)
H2H_FOCUS_SPORTS = {
    "basketball_nba", "basketball_euroleague", "basketball_ncaab",
    "tennis_atp_wimbledon", "tennis_wta",
    "americanfootball_nfl",
    "baseball_mlb", "icehockey_nhl",
}

# กีฬา 3-way (Home/Draw/Away) — ต้องประมวลผลต่างกัน
THREE_WAY_SPORTS_PREFIX = "soccer"  # sport_key ที่ขึ้นต้นด้วยนี้คือ 3-way market

KALSHI_API_KEY    = _s("KALSHI_API_KEY",    "")
KALSHI_API_SECRET = _s("KALSHI_API_SECRET", "")  # reserved for signed/private endpoints
USE_KALSHI        = bool(KALSHI_API_KEY)  # secret optional for read-only endpoints
SEEN_TTL_SEC      = int(os.getenv("SEEN_TTL_SEC", str(4 * 3600)))  # default 4h

# 6. Commission แบบ dynamic (อ่านจาก env ได้)
COMMISSION = {
    "polymarket": _d("FEE_POLYMARKET","0.02"),
    "pinnacle":   _d("FEE_PINNACLE",  "0.00"),
    "onexbet":    _d("FEE_1XBET",     "0.00"),
    "1xbet":      _d("FEE_1XBET",     "0.00"),
    "dafabet":    _d("FEE_DAFABET",   "0.00"),
    "kalshi":     _d("FEE_KALSHI",    "0.07"),  # Kalshi ~7% maker/taker
    "stake":      _d("FEE_STAKE",     "0.00"),
    "cloudbet":   _d("FEE_CLOUDBET",  "0.00"),
}

MAX_STAKE_KALSHI   = _d("MAX_STAKE_KALSHI",   "0")
MAX_STAKE_STAKE    = _d("MAX_STAKE_STAKE",    "0")
MAX_STAKE_CLOUDBET = _d("MAX_STAKE_CLOUDBET", "0")
MAX_STAKE_MAP = {
    "pinnacle":  MAX_STAKE_PINNACLE,
    "onexbet":   MAX_STAKE_1XBET,
    "1xbet":     MAX_STAKE_1XBET,
    "dafabet":   MAX_STAKE_DAFABET,
    "kalshi":    MAX_STAKE_KALSHI,
    "stake":     MAX_STAKE_STAKE,
    "cloudbet":  MAX_STAKE_CLOUDBET,
}


# ══════════════════════════════════════════════════════════════════
#  DATA MODELS
# ══════════════════════════════════════════════════════════════════
@dataclass
class OddsLine:
    bookmaker:  str
    outcome:    str
    odds:       Decimal
    odds_raw:   Decimal
    market_url: str  = ""
    raw:        dict = field(default_factory=dict)
    last_update: str = ""

@dataclass
class ArbOpportunity:
    signal_id:  str
    sport:      str
    event:      str
    commence:   str
    leg1:       OddsLine
    leg2:       OddsLine
    profit_pct: Decimal
    stake1:     Decimal
    stake2:     Decimal
    leg3:       Optional["OddsLine"] = None   # 3-way: Draw leg
    stake3:     Optional[Decimal]   = None   # 3-way: Draw stake
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status:     str = "pending"

@dataclass
class LineMovement:
    event:       str
    sport:       str
    bookmaker:   str
    outcome:     str
    odds_before: Decimal
    odds_after:  Decimal
    pct_change:  Decimal
    direction:   str   # "UP" | "DOWN"
    is_steam:    bool  # True = หลายเว็บขยับพร้อมกัน
    is_rlm:      bool  # True = Reverse Line Movement
    ts:          str   = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

@dataclass
class TradeRecord:
    """4. P&L Tracker"""
    signal_id:   str
    event:       str
    sport:       str
    leg1_bm:     str
    leg2_bm:     str
    leg1_team:   str         # ชื่อทีม/นักกีฬาที่วาง leg1
    leg2_team:   str         # ชื่อทีม/นักกีฬาที่วาง leg2
    leg1_odds:   float
    leg2_odds:   float
    stake1_thb:  int
    stake2_thb:  int
    profit_pct:  float
    status:      str    # confirmed | rejected
    clv_leg1:    Optional[float] = None
    clv_leg2:    Optional[float] = None
    actual_profit_thb: Optional[int] = None
    settled_at:  Optional[str] = None
    created_at:  str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    commence_time: str = ""   # v10-2: เวลาแข่งจริง เพื่อ restore settlement ถูกต้อง
    # 3-way fields (Optional — None = 2-way trade)
    leg3_bm:     Optional[str]   = None
    leg3_team:   Optional[str]   = None
    leg3_odds:   Optional[float] = None
    stake3_thb:  Optional[int]   = None


# ══════════════════════════════════════════════════════════════════
#  STATE
# ══════════════════════════════════════════════════════════════════
_main_loop: Optional[asyncio.AbstractEventLoop] = None  # ref to main loop for cross-thread calls
_scan_wakeup: Optional[asyncio.Event] = None  # v10-1: ปลุก scanner_loop ทันทีเมื่อ config เปลี่ยน
_scan_in_progress: bool = False  # B6: ป้องกัน scan overlap (legacy — replaced by _scan_lock)
_scan_lock: Optional[asyncio.Lock] = None  # B1: asyncio-safe scan mutex
_now_last_ts: float = 0  # A5: timestamp ที่กด /now ล่าสุด
_bot_start_ts: float = time.time()  # D2: uptime
_last_error: str = ""  # D2: last error message
_db_write_halted: bool = False  # Fix 4: set True when Turso fails 3x

@dataclass
class ValueBetSignal:
    """Value Bet signal จาก Line Movement — รอ Confirm ก่อนวาง"""
    signal_id:   str
    event:       str
    sport:       str
    bookmaker:   str       # Soft book ที่จะวาง
    outcome:     str       # ฝั่งที่แทง
    true_odds:   float     # Pinnacle/Sharp (ก่อนขยับ)
    soft_odds:   float     # Soft book (หลังขยับ)
    grade:       str       # A / B / C
    rec_stake_thb: int     # Kelly stake ที่แนะนำ
    edge_pct:    float     # edge (%)
    commence_time: str = ""
    created_at:  str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


pending:           dict[str, tuple]           = {}  # sid -> (ArbOpportunity, created_at_ts)
_pending_vb:       dict[str, tuple]           = {}  # sid -> (ValueBetSignal, created_at_ts)
seen_signals:      dict[str, float]          = {}  # key -> timestamp (TTL 4h)
auto_scan:         bool                      = AUTO_SCAN_START
scan_count:        int                       = 0
last_scan_time:    str                       = "ยังไม่ได้สแกน"
api_remaining:     int                       = 500
api_used_session:  int                       = 0
quota_warned:      bool                      = False
opportunity_log:   list[dict]                = []
trade_records:     list[TradeRecord]         = []   # 4. P&L
_app:              Optional[Application]     = None

# 3. Alert cooldown
alert_cooldown:    dict[str, datetime]       = {}   # event_key → last_alert_time

_shutdown_event = threading.Event()


# ══════════════════════════════════════════════════════════════════
#  💾 PERSISTENT STORAGE (SQLite)
# ══════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════
#  💾 DATABASE LAYER  (Turso cloud หรือ SQLite local fallback)
# ══════════════════════════════════════════════════════════════════

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS trade_records (
    signal_id TEXT PRIMARY KEY, event TEXT, sport TEXT,
    leg1_bm TEXT, leg2_bm TEXT,
    leg1_team TEXT DEFAULT '', leg2_team TEXT DEFAULT '',
    leg1_odds REAL, leg2_odds REAL,
    stake1_thb INTEGER, stake2_thb INTEGER, profit_pct REAL, status TEXT,
    clv_leg1 REAL, clv_leg2 REAL, actual_profit_thb INTEGER,
    settled_at TEXT, created_at TEXT,
    commence_time TEXT DEFAULT '',
    leg3_bm TEXT DEFAULT NULL, leg3_team TEXT DEFAULT NULL,
    leg3_odds REAL DEFAULT NULL, stake3_thb INTEGER DEFAULT NULL
);
CREATE TABLE IF NOT EXISTS opportunity_log (
    id TEXT PRIMARY KEY, event TEXT, sport TEXT, profit_pct REAL,
    leg1_bm TEXT, leg1_odds REAL, leg2_bm TEXT, leg2_odds REAL,
    stake1_thb INTEGER, stake2_thb INTEGER, created_at TEXT,
    status TEXT DEFAULT 'pending',
    leg3_bm TEXT DEFAULT NULL, stake3_thb INTEGER DEFAULT NULL,
    total_stake_thb INTEGER DEFAULT NULL
);
CREATE TABLE IF NOT EXISTS line_movements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event TEXT, sport TEXT, bookmaker TEXT, outcome TEXT,
    odds_before REAL, odds_after REAL, pct_change REAL,
    direction TEXT, is_steam INTEGER, is_rlm INTEGER, ts TEXT
);
CREATE TABLE IF NOT EXISTS bot_state (
    key TEXT PRIMARY KEY, value TEXT
);
"""

# ── Turso HTTP REST API (v10-6) ───────────────────────────────────
_turso_url:   str = ""
_turso_token: str = ""
_turso_ok:    bool = False

def _turso_http(statements: list) -> list:
    """POST to Turso /v2/pipeline — returns list of result rows per statement"""
    body = json.dumps({"requests": [
        {"type": "execute", "stmt": {
            "sql": s["sql"],
            "args": [{"type": _turso_val_type(v), "value": _turso_val_json(v)} for v in s.get("args", [])]
        }} for s in statements
    ] + [{"type": "close"}]}).encode()
    req = urllib.request.Request(
        f"{_turso_url}/v2/pipeline",
        data=body,
        headers={"Authorization": f"Bearer {_turso_token}", "Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as he:
        err_body = he.read().decode(errors="replace")[:500]
        log.error(f"[DB] Turso HTTP {he.code}: {err_body}")
        raise RuntimeError(f"Turso HTTP {he.code}: {err_body[:200]}") from he  # B9: removed raw body log
    data = json.loads(raw)
    # DEBUG: log first response item type to verify format
    if data.get("results"):
        first = data["results"][0]
        log.debug(f"[DB] Turso raw result[0] keys={list(first.keys())} type={first.get('type')}")
    results = []
    for item in data.get("results", []):
        itype = item.get("type")
        if itype == "error":
            msg = item.get("error", {}).get("message") or str(item)
            raise RuntimeError(msg)
        # Turso /v2/pipeline returns {"type":"ok","response":{"type":"execute","result":{...}}}
        if itype == "ok":
            rs = item.get("response", {}).get("result", {})
            rows = [tuple(v.get("value") for v in row) for row in rs.get("rows", [])]
            results.append(rows)
    return results

def _turso_val_type(v) -> str:
    if v is None:              return "null"
    if isinstance(v, bool):    return "integer"  # bool before int
    if isinstance(v, int):     return "integer"
    if isinstance(v, (float, Decimal)): return "float"  # Decimal → f64
    if isinstance(v, bytes):   return "blob"
    return "text"

def _turso_val(v):
    """Turso /v2/pipeline: integer/text/blob → string; float/Decimal → native JSON number (f64)."""
    if v is None:              return None
    if isinstance(v, bool):    return str(int(v))   # True->'1', False->'0'  (integer type)
    if isinstance(v, int):     return str(v)         # integer type → string
    if isinstance(v, Decimal): return float(v)       # float type → native number (not string)
    if isinstance(v, float):   return v              # float type → native number (not string)
    if isinstance(v, bytes):   return v.hex()
    return str(v)

def norm_bm_key(name: str) -> str:
    """Normalize bookmaker display name → API key for CLV lookup."""
    s = (name or "").lower().strip()
    if s in ("1xbet", "onexbet", "1x bet"):
        return "onexbet"
    if s in ("dafabet",):
        return "dafabet"
    if s in ("stake", "stake.com"):
        return "stake"
    if s in ("cloudbet",):
        return "cloudbet"
    return s


def _turso_val_json(v):
    """Turso /v2/pipeline: return native JSON type for integer/float, string for text/blob, None for null.
    Some Turso versions reject string-encoded numerics for REAL/INTEGER columns."""
    if v is None:              return None
    if isinstance(v, bool):    return int(v)         # True->1, False->0
    if isinstance(v, int):     return v
    if isinstance(v, Decimal): return float(v)
    if isinstance(v, float):   return v
    if isinstance(v, bytes):   return v.hex()
    return str(v)

async def turso_init():
    global _turso_url, _turso_token, _turso_ok
    url   = os.environ.get("TURSO_URL",   TURSO_URL).strip()
    token = os.environ.get("TURSO_TOKEN", TURSO_TOKEN).strip()
    log.info(f"[DB] TURSO_URL={'set ('+url[:40]+'...)' if url else 'NOT SET'}")
    log.info(f"[DB] TURSO_TOKEN={'set ('+token[:8]+'...)' if token else 'NOT SET'}")
    if not url or not token:
        log.warning("[DB] Turso not configured — using SQLite /tmp fallback")
        db_init_local()
        return
    _turso_url   = url.replace("libsql://", "https://").replace("wss://", "https://")
    _turso_token = token
    log.info(f"[DB] Turso endpoint → {_turso_url[:50]}")
    try:
        loop = asyncio.get_running_loop()
        def _init():
            stmts = [{"sql": s.strip()} for s in CREATE_TABLES_SQL.strip().split(";") if s.strip()]
            stmts.append({"sql": "SELECT COUNT(*) FROM trade_records"})
            results = _turso_http(stmts)
            count = results[-1][0][0] if results and results[-1] else 0
            return count
        count = await loop.run_in_executor(None, _init)
        _turso_ok = True
        log.info(f"[DB] Turso HTTP connected ✅ | trade_records={count}")
    except Exception as e:
        log.error(f"[DB] Turso init failed: {e!r} — fallback to SQLite")
        _turso_ok = False
        db_init_local()

async def turso_exec(sql: str, params: tuple = ()):
    """Execute write query (Turso HTTP หรือ SQLite fallback)"""
    global auto_scan, _db_write_halted, _turso_ok
    # Fix 1: early bail if writes are halted
    if _db_write_halted:
        log.error("[DB] writes halted — skipping")
        return
    if _turso_ok:
        for attempt in range(3):
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, lambda: _turso_http(
                    [{"sql": sql, "args": list(params)}]
                ))
                return
            except Exception as e:
                emsg = str(e).lower()
                # benign migration errors — skip retry, no warning
                if "duplicate column" in emsg or "already exists" in emsg:
                    log.debug(f"[DB] turso_exec migration (ok): {e}")
                    return
                if attempt < 2:
                    log.warning(f"[DB] turso_exec attempt {attempt+1} failed: {e!r}")
                    await asyncio.sleep(1.5 ** attempt)
                else:
                    log.error(f"[DB] turso_exec failed 3x: {e!r} — halting writes")
                    # Fix 2: Turso ล่ม 3 ครั้ง → หยุด auto_scan + return ทันที ไม่เขียน SQLite
                    auto_scan = False
                    if _app:
                        try:
                            asyncio.get_running_loop().create_task(
                                _app.bot.send_message(
                                    chat_id=CHAT_ID,
                                    text=f"🚨 *DB CRITICAL*: Turso write failed 3x\n`{str(e)[:120]}`\n❌ *Auto scan หยุดแล้ว* — หยุดเขียน DB ทั้งหมด",
                                    parse_mode="Markdown"
                                )
                            )
                        except Exception:
                            pass
                    _db_write_halted = True
                    _turso_ok = False  # Fix 1: prevent re-entry into Turso block
                    return  # ไม่ fallback ไป SQLite — ป้องกัน split-brain
    # SQLite-only mode (ใช้เฉพาะตอน Turso init fail ตั้งแต่แรก หรือไม่ได้ตั้ง Turso)
    if not _turso_ok and not _db_write_halted:
        try:
            with sqlite3.connect(DB_PATH, timeout=10) as con:
                con.execute(sql, params)
                con.commit()
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e) or "already exists" in str(e):
                pass
            else:
                log.error(f"[DB] sqlite_exec: {e}")
        except Exception as e:
            log.error(f"[DB] sqlite_exec: {e}")

async def turso_query(sql: str, params: tuple = ()) -> list:
    """Execute read query (Turso HTTP หรือ SQLite fallback)"""
    # Fix 5: if Turso was live and then died, refuse SQLite fallback read
    if _db_write_halted:
        log.error("[DB] reads halted after Turso failure — refusing SQLite fallback")
        return []
    if _turso_ok:
        try:
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, lambda: _turso_http(
                [{"sql": sql, "args": list(params)}]
            ))
            return results[0] if results else []
        except Exception as e:
            log.error(f"[DB] turso_query: {e!r}")
            return []  # Fix 1: no SQLite fallback when Turso is/was active
    # SQLite fallback — only when Turso was never configured
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as con:
            rows = con.execute(sql, params).fetchall()
        return rows
    except Exception as e:
        log.error(f"[DB] sqlite_query: {e}")
        return []

# ── SQLite local init (fallback) ──────────────────────────────────
def db_init_local():
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as con:
            con.execute("PRAGMA journal_mode=WAL;")  # ลด database-locked errors ใน 24/7 mode
            con.execute("PRAGMA synchronous=NORMAL;")  # เร็วขึ้น ยังปลอดภัย
            for stmt in CREATE_TABLES_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    con.execute(stmt)
            con.commit()
        log.info(f"[DB] SQLite local at {DB_PATH} (WAL mode)")
    except Exception as e:
        log.error(f"[DB] local init: {e}")

def db_init():
    # SQLite always initialized as fallback (Turso init happens later async)
    db_init_local()

# ── Write helpers ─────────────────────────────────────────────────
# #33 Thread-safe db_save_* — ใช้ get_event_loop แทน get_running_loop
# เพราะ db_save_* อาจถูกเรียกจาก dashboard thread (ไม่ใช่ asyncio thread)
def _schedule_coro(coro):
    """Schedule coroutine onto the main asyncio loop from any thread safely."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(coro)
    except RuntimeError:
        # เรียกจาก non-asyncio thread (เช่น dashboard HTTP thread)
        if _main_loop and not _main_loop.is_closed():
            asyncio.run_coroutine_threadsafe(coro, _main_loop)
        else:
            log.warning("[DB] _schedule_coro: no event loop available")

def db_save_trade(t: "TradeRecord"):
    _schedule_coro(_async_save_trade(t))

async def _async_save_trade(t: "TradeRecord"):
    await turso_exec(
        "INSERT OR REPLACE INTO trade_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (t.signal_id,t.event,t.sport,t.leg1_bm,t.leg2_bm,
         t.leg1_team,t.leg2_team,
         t.leg1_odds,t.leg2_odds,t.stake1_thb,t.stake2_thb,
         t.profit_pct,t.status,t.clv_leg1,t.clv_leg2,
         t.actual_profit_thb,t.settled_at,t.created_at,
         t.commence_time,
         t.leg3_bm,t.leg3_team,t.leg3_odds,t.stake3_thb)
    )

def db_save_opportunity(opp: dict):
    _schedule_coro(_async_save_opp(opp))

async def _async_save_opp(opp: dict):
    # B5: migration ย้ายไป db_load_all startup แล้ว — ไม่ต้อง ALTER TABLE ทุก insert
    await turso_exec(
        """INSERT OR REPLACE INTO opportunity_log
           (id,event,sport,profit_pct,leg1_bm,leg1_odds,leg2_bm,leg2_odds,
            stake1_thb,stake2_thb,created_at,status,leg3_bm,stake3_thb,total_stake_thb)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (opp["id"],opp["event"],opp["sport"],opp["profit_pct"],
         opp["leg1_bm"],opp["leg1_odds"],opp["leg2_bm"],opp["leg2_odds"],
         opp["stake1_thb"],opp["stake2_thb"],opp["created_at"],opp["status"],
         opp.get("leg3_bm"), opp.get("stake3_thb"), opp.get("total_stake_thb"))
    )

def db_update_opp_status(signal_id: str, status: str):
    _schedule_coro(turso_exec("UPDATE opportunity_log SET status=? WHERE id=?", (status, signal_id)))

def db_save_line_movement(lm: "LineMovement"):
    _schedule_coro(_async_save_lm(lm))

async def _async_save_lm(lm: "LineMovement"):
    await turso_exec(
        """INSERT INTO line_movements
           (event,sport,bookmaker,outcome,odds_before,odds_after,
            pct_change,direction,is_steam,is_rlm,ts)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (lm.event,lm.sport,lm.bookmaker,lm.outcome,
         float(lm.odds_before),float(lm.odds_after),float(lm.pct_change),
         lm.direction,int(lm.is_steam),int(lm.is_rlm),lm.ts)
    )

def db_save_state(key: str, value: str):
    _schedule_coro(turso_exec("INSERT OR REPLACE INTO bot_state VALUES (?,?)", (key, value)))

async def db_load_state_async(key: str, default: str = "") -> str:
    rows = await turso_query("SELECT value FROM bot_state WHERE key=?", (key,))
    return rows[0][0] if rows else default

def db_load_state(key: str, default: str = "") -> str:
    """Sync version (ใช้ SQLite local เท่านั้น สำหรับ startup)"""
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as con:
            row = con.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
        return row[0] if row else default
    except Exception:
        return default

async def db_load_all() -> tuple[list, list, list]:
    """โหลดทุกอย่างจาก DB (async)"""
    try:
        # migrate DB schema — เพิ่มคอลัมน์ใหม่ถ้ายังไม่มี (ไม่พังถ้ามีอยู่แล้ว)
        for _col, _sql in [
            ("commence_time", "ALTER TABLE trade_records ADD COLUMN commence_time TEXT DEFAULT ''"),
            ("leg3_bm",   "ALTER TABLE trade_records ADD COLUMN leg3_bm TEXT DEFAULT NULL"),
            ("leg3_team", "ALTER TABLE trade_records ADD COLUMN leg3_team TEXT DEFAULT NULL"),
            ("leg3_odds", "ALTER TABLE trade_records ADD COLUMN leg3_odds REAL DEFAULT NULL"),
            ("stake3_thb","ALTER TABLE trade_records ADD COLUMN stake3_thb INTEGER DEFAULT NULL"),
            # B5: opportunity_log 3-way migration ย้ายมาไว้ที่นี่ — ยิงแค่ครั้งเดียวตอน startup
            ("opp_leg3_bm",        "ALTER TABLE opportunity_log ADD COLUMN leg3_bm TEXT DEFAULT NULL"),
            ("opp_stake3_thb",     "ALTER TABLE opportunity_log ADD COLUMN stake3_thb INTEGER DEFAULT NULL"),
            ("opp_total_stake_thb","ALTER TABLE opportunity_log ADD COLUMN total_stake_thb INTEGER DEFAULT NULL"),
        ]:
            try: await turso_exec(_sql)
            except Exception: pass  # column exists already

        trades_rows = await turso_query(
            "SELECT * FROM trade_records ORDER BY created_at DESC LIMIT 500")  # B1: DESC newest-first, reversed below → oldest-first in memory
        trades = []
        for r in trades_rows:
            n = len(r)
            # col order: 0=signal_id,1=event,2=sport,3=leg1_bm,4=leg2_bm,
            #            5=leg1_team,6=leg2_team,7=leg1_odds,8=leg2_odds,
            #            9=stake1_thb,10=stake2_thb,11=profit_pct,12=status,
            #            13=clv_leg1,14=clv_leg2,15=actual_profit_thb,
            #            16=settled_at,17=created_at,18=commence_time
            #            19=leg3_bm,20=leg3_team,21=leg3_odds,22=stake3_thb
            if n >= 18:
                trades.append(TradeRecord(
                    signal_id=r[0],event=r[1],sport=r[2],leg1_bm=r[3],leg2_bm=r[4],
                    leg1_team=r[5] or "",leg2_team=r[6] or "",
                    leg1_odds=float(r[7] or 0),leg2_odds=float(r[8] or 0),
                    stake1_thb=int(float(r[9] or 0)),stake2_thb=int(float(r[10] or 0)),
                    profit_pct=float(r[11] or 0),status=r[12],
                    clv_leg1=float(r[13]) if r[13] is not None else None,
                    clv_leg2=float(r[14]) if r[14] is not None else None,
                    actual_profit_thb=int(float(r[15])) if r[15] is not None else None,
                    settled_at=r[16],created_at=r[17],
                    commence_time=r[18] if n >= 19 else "",
                    leg3_bm=r[19] if n >= 20 else None,
                    leg3_team=r[20] if n >= 21 else None,
                    leg3_odds=float(r[21]) if n >= 22 and r[21] is not None else None,
                    stake3_thb=int(float(r[22])) if n >= 23 and r[22] is not None else None))
            else:
                # DB เก่า — ไม่มี leg1_team/leg2_team
                ev = r[1] if n>1 else ""
                parts = ev.split(" vs ")
                trades.append(TradeRecord(
                    signal_id=r[0],event=ev,sport=r[2],leg1_bm=r[3],leg2_bm=r[4],
                    leg1_team=parts[0] if parts else "",
                    leg2_team=parts[1] if len(parts)>1 else "",
                    leg1_odds=float(r[5] or 0),leg2_odds=float(r[6] or 0),
                    stake1_thb=int(float(r[7] or 0)),stake2_thb=int(float(r[8] or 0)),
                    profit_pct=float(r[9] or 0),status=r[10],
                    clv_leg1=float(r[11]) if r[11] is not None else None,
                    clv_leg2=float(r[12]) if r[12] is not None else None,
                    actual_profit_thb=int(float(r[13])) if r[13] is not None else None,
                    settled_at=r[14],created_at=r[15]))

        opps_rows = await turso_query(
            "SELECT * FROM opportunity_log ORDER BY created_at DESC LIMIT 50")  # newest 50; reversed below → oldest-first in memory
        opps = []
        for r in opps_rows:
            n = len(r)
            opps.append({
                "id":         r[0], "event":     r[1],  "sport":     r[2],
                "profit_pct": float(r[3] or 0),
                "leg1_bm":    r[4],  "leg1_odds": float(r[5] or 0),
                "leg2_bm":    r[6],  "leg2_odds": float(r[7] or 0),
                "stake1_thb": int(float(r[8] or 0)),
                "stake2_thb": int(float(r[9] or 0)),
                "created_at": r[10], "status":    r[11],
                # R8: 3-way fields (columns 12-14 added by migration)
                "leg3_bm":         r[12] if n > 12 else None,
                "stake3_thb":      int(float(r[13])) if n > 13 and r[13] is not None else None,
                "total_stake_thb": int(float(r[14])) if n > 14 and r[14] is not None else None,
            })

        lm_rows = await turso_query(
            "SELECT * FROM line_movements ORDER BY ts DESC LIMIT 200")  # B1: DESC newest-first, reversed below
        lms = [LineMovement(
            event=r[1],sport=r[2],bookmaker=r[3],outcome=r[4],
            odds_before=Decimal(str(r[5])),odds_after=Decimal(str(r[6])),
            pct_change=Decimal(str(r[7])),direction=r[8],
            is_steam=bool(int(r[9] or 0)),is_rlm=bool(int(r[10] or 0)),ts=r[11])
               for r in lm_rows]

        trades.reverse()  # B1: loaded DESC → reverse to oldest-first
        opps.reverse()    # loaded DESC → reverse to oldest-first
        lms.reverse()     # B1: loaded DESC → reverse to oldest-first
        log.info(f"[DB] loaded: trades={len(trades)}, opps={len(opps)}, moves={len(lms)}")
        return trades, opps, lms
    except Exception as e:
        log.error(f"[DB] load_all: {e}", exc_info=True)
        return [], [], []

def save_snapshot():
    db_save_state("scan_count",     str(scan_count))
    db_save_state("auto_scan",      str(auto_scan))
    db_save_state("last_scan_time", last_scan_time)
    db_save_state("api_remaining",  str(api_remaining))

# 7/10/11. Line movement tracking
odds_history:      dict[str, dict]           = defaultdict(dict)  # event+outcome → {bm: odds}
line_movements:    list[LineMovement]        = []   # ประวัติ line move
steam_tracker:     dict[str, list]           = defaultdict(list)  # event → [(bm, ts, direction)]

# 12. CLV tracking — odds ตอนปิด
closing_odds:      dict[str, dict]           = {}   # event+outcome → {bm: final_odds}

# Thread-safety lock — dashboard thread อ่าน global lists พร้อมกับ asyncio เขียน
import threading as _threading
_data_lock = _threading.Lock()


# ══════════════════════════════════════════════════════════════════
#  QUOTA TRACKER
# ══════════════════════════════════════════════════════════════════
async def update_quota(remaining: int):
    global api_remaining, api_used_session, quota_warned, auto_scan
    api_remaining     = remaining
    api_used_session += 1
    should_warn = remaining <= QUOTA_WARN_AT and not quota_warned
    critical    = remaining <= 10
    if should_warn or critical:
        quota_warned = True
        level = "🔴 *CRITICAL*" if critical else "⚠️ *WARNING*"
        msg = (f"{level} — Odds API Quota\n"
               f"Credits เหลือ: *{remaining}*\n"
               f"{'🛑 หยุด scan อัตโนมัติ!' if critical else f'แจ้งเตือนที่ {QUOTA_WARN_AT}'}\n"
               f"อัพเกรด: https://the-odds-api.com")
        if _app:
            for cid in ALL_CHAT_IDS:
                try: await _app.bot.send_message(chat_id=cid, text=msg, parse_mode="Markdown")
                except Exception: pass
        if critical:
            auto_scan = False


# ══════════════════════════════════════════════════════════════════
#  FUZZY MATCH
# ══════════════════════════════════════════════════════════════════
TEAM_ALIASES = {
    "lakers":"Los Angeles Lakers","la lakers":"Los Angeles Lakers",
    "clippers":"LA Clippers","warriors":"Golden State Warriors",
    "celtics":"Boston Celtics","heat":"Miami Heat","nets":"Brooklyn Nets",
    "bulls":"Chicago Bulls","sa spurs":"San Antonio Spurs","kings":"Sacramento Kings",
    "nuggets":"Denver Nuggets","suns":"Phoenix Suns","bucks":"Milwaukee Bucks",
    "sixers":"Philadelphia 76ers","76ers":"Philadelphia 76ers",
    "knicks":"New York Knicks","mavs":"Dallas Mavericks",
    "rockets":"Houston Rockets","raptors":"Toronto Raptors",
    "yankees":"New York Yankees","red sox":"Boston Red Sox",
    "dodgers":"Los Angeles Dodgers","cubs":"Chicago Cubs","astros":"Houston Astros",
    "navi":"Natus Vincere","faze":"FaZe Clan","g2":"G2 Esports",
    "liquid":"Team Liquid","og":"OG","secret":"Team Secret",
    # Soccer — EPL
    "man utd":"Manchester United","man united":"Manchester United","mufc":"Manchester United",
    "man city":"Manchester City","mcfc":"Manchester City",
    "arsenal":"Arsenal","gunners":"Arsenal","afc":"Arsenal",
    "liverpool":"Liverpool","reds":"Liverpool","lfc":"Liverpool",
    "chelsea":"Chelsea","blues":"Chelsea","cfc":"Chelsea",
    "spurs":"Tottenham Hotspur","tottenham":"Tottenham Hotspur","thfc":"Tottenham Hotspur",
    "newcastle":"Newcastle United","nufc":"Newcastle United",
    "villa":"Aston Villa","avfc":"Aston Villa",
    "west ham":"West Ham United","hammers":"West Ham United",
    "everton":"Everton","toffees":"Everton",
    # Soccer — La Liga / Bundesliga / UCL
    "barca":"FC Barcelona","barcelona":"FC Barcelona","fcb":"FC Barcelona",
    "real":"Real Madrid","rmcf":"Real Madrid",
    "atletico":"Atletico Madrid","atleti":"Atletico Madrid",
    "bayern":"Bayern Munich","fcb munich":"Bayern Munich",
    "dortmund":"Borussia Dortmund","bvb":"Borussia Dortmund",
    "psg":"Paris Saint-Germain","paris":"Paris Saint-Germain",
    "juve":"Juventus","juventus":"Juventus",
    "inter":"Inter Milan","internazionale":"Inter Milan",
    "milan":"AC Milan","acm":"AC Milan",
    # NFL
    "chiefs":"Kansas City Chiefs","kc":"Kansas City Chiefs",
    "eagles":"Philadelphia Eagles","philly":"Philadelphia Eagles",
    "49ers":"San Francisco 49ers","niners":"San Francisco 49ers",
    "bills":"Buffalo Bills","cowboys":"Dallas Cowboys",
    "ravens":"Baltimore Ravens","packers":"Green Bay Packers",
    "lions":"Detroit Lions","dolphins":"Miami Dolphins",
    "bengals":"Cincinnati Bengals","rams":"Los Angeles Rams",
    "chargers":"Los Angeles Chargers","steelers":"Pittsburgh Steelers",
    "bears":"Chicago Bears","patriots":"New England Patriots",
    "commanders":"Washington Commanders","giants":"New York Giants",
    "jets":"New York Jets","texans":"Houston Texans",
    "broncos":"Denver Broncos","seahawks":"Seattle Seahawks",
    "vikings":"Minnesota Vikings","saints":"New Orleans Saints",
}

def normalize_team(name: str) -> str:
    n = name.lower().strip()
    return re.sub(r"\s+"," ", re.sub(r"[^\w\s]","",n))

def fuzzy_match(a: str, b: str, threshold: float = 0.6) -> bool:
    na = normalize_team(TEAM_ALIASES.get(normalize_team(a), a))
    nb = normalize_team(TEAM_ALIASES.get(normalize_team(b), b))
    if na == nb: return True
    sw = {"the","fc","cf","sc","ac","de","city","united","of","and"}
    ta = set(na.split()) - sw
    tb = set(nb.split()) - sw
    if not ta or not tb: return False
    j = len(ta&tb)/len(ta|tb)
    return j >= threshold or (na in nb) or (nb in na) or (na[:5]==nb[:5] and len(na)>=5)


# ══════════════════════════════════════════════════════════════════
#  7/10/11. LINE MOVEMENT DETECTOR
# ══════════════════════════════════════════════════════════════════
async def detect_line_movements(odds_by_sport: dict, poly_markets: list | None = None):
    """
    เปรียบเทียบ odds ใหม่กับ history
    ตรวจจับ: Line Move, Steam Move, Reverse Line Movement
    พร้อมจัดเกรดสัญญาณ (A/B/C) และวิเคราะห์จังหวะเวลา
    """
    new_movements: list[tuple[LineMovement, dict]] = []  # (lm, context)
    now = datetime.now(timezone.utc)

    for sport, events in odds_by_sport.items():
        await asyncio.sleep(0)  # yield ให้ event loop ไปทำงานอื่น (Telegram, etc.) ได้ระหว่างสปอร์ต
        for event in events:
            home  = event.get("home_team","")
            away  = event.get("away_team","")
            ename = f"{home} vs {away}"
            commence = event.get("commence_time","")

            for bm in event.get("bookmakers",[]):
                bk = bm.get("key","")
                bn = bm.get("title", bk)
                for mkt in bm.get("markets",[]):
                    if mkt.get("key") != "h2h": continue
                    for out in mkt.get("outcomes",[]):
                        outcome  = out.get("name","")
                        new_odds = Decimal(str(out.get("price",1)))
                        hist_key = f"{ename}|{outcome}"

                        if bk in odds_history.get(hist_key, {}):
                            old_odds = odds_history[hist_key][bk]
                            if old_odds > 0:
                                pct = (new_odds - old_odds) / old_odds
                                if abs(pct) >= LINE_MOVE_THRESHOLD:
                                    direction = "UP 📈" if pct > 0 else "DOWN 📉"

                                    # 11. Steam: หลายเว็บขยับพร้อมกันภายใน 5 นาที
                                    steam_key = f"{ename}|{outcome}|{direction}"
                                    steam_tracker[steam_key].append((bk, now))
                                    # ลบ entry เก่ากว่า 5 นาที
                                    steam_tracker[steam_key] = [
                                        (b,t) for b,t in steam_tracker[steam_key]
                                        if (now-t).total_seconds() < 300
                                    ]
                                    # dedupe: นับเฉพาะ unique bookmakers
                                    unique_bms = {b for b, _ in steam_tracker[steam_key]}
                                    num_bm_moved = len(unique_bms)
                                    is_steam = num_bm_moved >= 2

                                    # 10. RLM: odds ขยับ反向กับ public bet
                                    # ถ้า odds ลง (favourite กลายเป็น underdog) = sharp money เดิน
                                    is_sharp_move = pct < -LINE_MOVE_THRESHOLD and bk == "pinnacle"

                                    lm = LineMovement(
                                        event=ename, sport=sport,
                                        bookmaker=bn, outcome=outcome,
                                        odds_before=old_odds, odds_after=new_odds,
                                        pct_change=pct, direction=direction,
                                        is_steam=is_steam, is_rlm=is_sharp_move,
                                    )
                                    # C5: liquidity จาก Polymarket จริง
                                    liq_usd = 0.0
                                    if poly_markets:
                                        poly_match = find_polymarket(ename, poly_markets)
                                        if poly_match:
                                            liq_usd = float(poly_match.get("_liquidity", 0) or 0)

                                                            # C4: สร้าง all_odds dict — odds ของ outcome นี้จากทุก bm
                                    _hist_for_outcome = odds_history.get(hist_key, {})
                                    all_odds_for_ctx: dict[str, float] = {
                                        norm_bm_key(_bk): float(_o)
                                        for _bk, _o in _hist_for_outcome.items()
                                        if isinstance(_o, (int, float, Decimal))
                                    }
                                    all_odds_for_ctx[norm_bm_key(bk)] = float(new_odds)  # อัพเดทค่าล่าสุด

                                    # C4: หา sharp_odds (Pinnacle) vs soft_odds (bm นี้)
                                    sharp_odds_val = float(all_odds_for_ctx.get("pinnacle", 0))
                                    soft_odds_val  = float(new_odds)

                                    ctx = {
                                        "commence_time": commence,
                                        "num_bm_moved": num_bm_moved,
                                        "bm_key": bk,
                                        "liquidity_usd": liq_usd,
                                        "all_odds": all_odds_for_ctx,
                                        "sharp_odds": sharp_odds_val,
                                        "soft_odds": soft_odds_val,
                                    }
                                    new_movements.append((lm, ctx))
                                    with _data_lock:
                                        line_movements.append(lm)
                                    db_save_line_movement(lm)  # 💾
                                    log.info(f"[LineMove] {ename} | {bn} {outcome} {float(old_odds):.3f}→{float(new_odds):.3f} ({pct:.1%}) {'🌊STEAM' if is_steam else ''} {'🔄Sharp' if is_sharp_move else ''}")

                        # อัพเดท history
                        if hist_key not in odds_history:
                            odds_history[hist_key] = {}
                        odds_history[hist_key][bk] = new_odds

    # ส่ง Telegram alert สำหรับ line movements
    if new_movements and _app:
        await send_line_move_alerts(new_movements)

    # จำกัด history
    with _data_lock:
        if len(line_movements) > 200:
            line_movements[:] = line_movements[-200:]


async def send_line_move_alerts(movements: list[tuple[LineMovement, dict]]):
    """
    ส่ง alert สำหรับ Line Movement พร้อม:
    - Signal Grade (A/B/C)
    - Time-of-Move analysis
    - Direct betting links
    - Liquidity check
    """
    for lm, ctx in movements:
        commence_time = ctx.get("commence_time", "")
        num_bm_moved  = ctx.get("num_bm_moved", 1)
        bm_key        = ctx.get("bm_key", "")

        # จัดเกรดสัญญาณ — ส่ง liquidity จาก ctx ถ้ามี
        liquidity_usd = ctx.get("liquidity_usd", 0)
        grade, grade_emoji, reasons = grade_signal(
            lm, liquidity_usd=liquidity_usd,
            commence_time=commence_time,
            num_bm_moved=num_bm_moved,
        )

        # Header ตามประเภท
        tags = []
        if lm.is_rlm:   tags.append("🔄 *REVERSE LINE MOVEMENT*")
        if lm.is_steam:  tags.append("🌊 *STEAM MOVE*")
        if not tags:      tags.append("📊 *Line Movement*")

        pct_str = f"+{lm.pct_change:.1%}" if lm.pct_change > 0 else f"{lm.pct_change:.1%}"
        sport_emoji = SPORT_EMOJI.get(lm.sport, "🏆")

        # เวลาแข่ง
        time_info = ""
        if commence_time:
            try:
                ct   = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
                ct_th = ct + timedelta(hours=7)  # แปลงเป็น UTC+7
                mins = (ct - datetime.now(timezone.utc)).total_seconds() / 60
                date_str = ct_th.strftime("%d/%m/%Y %H:%M")
                if mins <= 0:
                    time_info = f"🟢 เริ่มแล้ว ({date_str} น. ไทย)"
                elif mins < 60:
                    time_info = f"⏰ เริ่มใน {int(mins)} นาที — {date_str} น. ไทย"
                elif mins < 1440:
                    h = int(mins // 60)
                    m = int(mins % 60)
                    time_info = f"📅 {date_str} น. ไทย (อีก {h}ชม.{m}น.)"
                else:
                    days = int(mins // 1440)
                    time_info = f"📅 {date_str} น. ไทย (อีก {days} วัน)"
            except Exception:
                pass

        msg = (
            f"{'  '.join(tags)}\n"
            f"{grade_emoji} *เกรด {grade}* {'— 🔥 สัญญาณแข็ง!' if grade == 'A' else '— สัญญาณพอใช้' if grade == 'B' else ''}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{sport_emoji} {md_escape(lm.event)}\n"
            f"📡 {md_escape(lm.bookmaker)} — *{md_escape(lm.outcome)}*\n"
            f"📉 `{float(lm.odds_before):.3f}` → `{float(lm.odds_after):.3f}` ({pct_str}) {lm.direction}\n"
        )
        if time_info:
            msg += f"{time_info}\n"

        # แสดงเหตุผลของเกรด
        msg += f"\n📋 *วิเคราะห์สัญญาณ:*\n"
        for reason in reasons:
            msg += f"  {reason}\n"

        # คำแนะนำสำหรับ Grade A/B
        vb_sid = None
        keyboard = None
        if grade in ("A", "B") and (lm.is_rlm or lm.is_steam):
            target = lm.outcome
            if lm.pct_change < 0:
                msg += (f"\n💡 *แนะนำ:* เดิมพัน *{md_escape(target)}* (odds ลง = เงินใหญ่เดิน)\n"
                        f"Soft books ยังไม่ตาม → โอกาส value bet!\n")
            else:
                msg += (f"\n💡 *สังเกต:* odds ขึ้น → อาจเป็น value ฝั่งตรงข้าม\n")

            # Dynamic Kelly Payout Calculator
            # R2/B9: เฉพาะ soft books เท่านั้น — Pinnacle ไม่แสดง Kelly section แต่ยัง send alert
            _soft_books = {"onexbet", "1xbet", "dafabet", "stake", "cloudbet", "betway", "unibet", "bwin"}
            _bm_key_norm = norm_bm_key(ctx.get("bm_key", lm.bookmaker))
            sharp_ctx = ctx.get("sharp_odds", 0.0)
            true_odds = sharp_ctx if sharp_ctx > 1.0 else float(lm.odds_before)
            soft_odds = float(lm.odds_after)     # ราคา Soft book ที่จะวาง
            _is_soft_with_edge = (
                _bm_key_norm != "pinnacle"
                and _bm_key_norm in _soft_books
                and sharp_ctx > 1.0
                and sharp_ctx < soft_odds  # soft ยังค้างราคาดีกว่า sharp
            )
            rec_stake_thb, edge_pct = (Decimal("0"), 0.0)
            if _is_soft_with_edge:
                rec_stake_thb, edge_pct = calc_valuebet_kelly(true_odds, soft_odds, grade)
            rec_stake_int = int(rec_stake_thb)
            # H6: apply per-book cap ตั้งแต่สร้าง signal — alert กับ confirm แสดงตัวเลขเดียวกัน
            if _is_soft_with_edge and rec_stake_thb > 0:
                rec_stake_thb = apply_vb_book_cap(rec_stake_thb, lm.bookmaker)
            rec_stake_int = int(rec_stake_thb)
            if _is_soft_with_edge and rec_stake_int > 0 and edge_pct > 0:
                payout = int(rec_stake_int * soft_odds)
                profit = payout - rec_stake_int
                fraction_map = {"A": "30%", "B": "15%", "C": "5%"}
                msg += (
                    f"\n💰 *Dynamic Kelly Stake — เกรด {grade} ({fraction_map.get(grade,'?')} Kelly):*\n"
                    f"  Edge: *+{edge_pct:.2f}%* | วาง *฿{rec_stake_int:,}* ที่ `{soft_odds:.3f}`\n"
                    f"  ได้คืน *฿{payout:,}* (กำไร +฿{profit:,})\n"
                )
                # สร้าง ValueBetSignal รอ Confirm
                vb_sid = str(uuid.uuid4())[:8]
                vb = ValueBetSignal(
                    signal_id=vb_sid, event=lm.event, sport=lm.sport,
                    bookmaker=lm.bookmaker, outcome=lm.outcome,
                    true_odds=true_odds, soft_odds=soft_odds,
                    grade=grade, rec_stake_thb=rec_stake_int,
                    edge_pct=edge_pct, commence_time=commence_time,
                )
                with _data_lock:
                    _pending_vb[vb_sid] = (vb, time.time())
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton(f"✅ Confirm Value Bet ฿{rec_stake_int:,}", callback_data=f"vb_confirm:{vb_sid}"),
                    InlineKeyboardButton("❌ Skip", callback_data=f"vb_reject:{vb_sid}"),
                ]])
            else:
                msg += f"\n⚠️ *ระบบคำนวณแล้ว: No Value / Edge ต่ำ* (edge={edge_pct:.2f}%) — ควรรอดูท่าที\n"

            # Direct betting links + odds ทุกเว็บ
            all_odds_ctx: dict | None = ctx.get("all_odds")
            msg += f"\n🔗 *วางเดิมพันได้ที่:*\n"
            msg += build_betting_links(lm.event, lm.outcome, lm.sport, lm.odds_after, bm_key, all_odds_ctx)
            msg += "\n"

        # H2H Focus note
        if lm.sport in H2H_FOCUS_SPORTS:
            msg += f"\n🎯 _กีฬานี้ Sharp money เน้นตลาด H2H — สัญญาณน่าเชื่อถือ_"

        for cid in ALL_CHAT_IDS:
            try:
                await _app.bot.send_message(
                    chat_id=cid, text=msg, parse_mode="Markdown",
                    reply_markup=keyboard if keyboard else None,
                )
                await asyncio.sleep(0.3)
            except Exception as e:
                log.error(f"[LineMove] alert error: {e}")


# ══════════════════════════════════════════════════════════════════
#  12. CLV TRACKER
# ══════════════════════════════════════════════════════════════════
def update_clv(event: str, outcome: str, bookmaker: str, final_odds: Decimal):
    """บันทึก closing odds เพื่อคำนวณ CLV"""
    key = f"{event}|{outcome}"
    # S3: closing_odds อ่าน/เขียนข้าม thread (asyncio loop + dashboard thread) — ต้องใช้ lock
    with _data_lock:
        if key not in closing_odds:
            closing_odds[key] = {}
        closing_odds[key][norm_bm_key(bookmaker)] = final_odds


def calc_clv(trade: TradeRecord) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """
    CLV = (odds_got / closing_odds - 1) × 100%
    L2: ใช้ Pinnacle closing line เป็น benchmark กลาง — ไม่ใช้ per-book lookup
    ทำให้ CLV comparable ข้าม book (รวม alt markets ที่ไม่มี closing data ผ่าน Odds API)
    Returns: (clv_leg1, clv_leg2, clv_leg3)  — clv_leg3 = None for 2-way trades
    """
    def _clv(event, outcome, odds_got):
        key = f"{event}|{outcome}"
        # S3: snapshot under lock to avoid race with update_clv (dashboard thread vs asyncio)
        with _data_lock:
            bm_data = dict(closing_odds.get(key, {}))
        # M4: strict Pinnacle benchmark — ถ้าไม่มี Pinnacle closing line → None
        co = bm_data.get("pinnacle")
        if co and co > 0:
            return round((float(odds_got) / float(co) - 1) * 100, 2)
        return None

    clv1 = _clv(trade.event, trade.leg1_team or trade.leg1_bm, trade.leg1_odds)
    clv2 = _clv(trade.event, trade.leg2_team or trade.leg2_bm, trade.leg2_odds)
    clv3 = None
    if trade.leg3_team and trade.leg3_bm and trade.leg3_odds:
        clv3 = _clv(trade.event, trade.leg3_team, trade.leg3_odds)
    return clv1, clv2, clv3


# ══════════════════════════════════════════════════════════════════
#  SIGNAL GRADING SYSTEM (RLM + Steam)
# ══════════════════════════════════════════════════════════════════
def classify_move_time(move_ts: str, commence_time: str = "") -> tuple[str, str, float]:
    """
    จำแนกจังหวะเวลาของ Line Movement
    Returns: (label, description, confidence_boost 0.0-1.0)

    - เช้าตรู่ (00:00-08:00 UTC) → Sharp analyst money (แม่นยำสูง)
    - ก่อนแข่ง ≤15 นาที → Insider / injury news (แม่นยำที่สุด)
    - ช่วงกลางวัน (08:00-20:00) → อาจเป็นกระแสหน้าตั๋ว (ปานกลาง)
    - กลางคืน (20:00-00:00) → ผสม
    """
    try:
        ts = datetime.fromisoformat(move_ts.replace("Z","+00:00"))
    except Exception:
        ts = datetime.now(timezone.utc)

    # เช็คเวลาก่อนแข่ง
    if commence_time:
        try:
            ct = datetime.fromisoformat(commence_time.replace("Z","+00:00"))
            mins_to_start = (ct - ts).total_seconds() / 60
            if 0 < mins_to_start <= 15:
                return "PRE-MATCH", "⏰ ก่อนแข่ง ≤15 นาที — วงในหรือข่าวบาดเจ็บ", 1.0
            if 0 < mins_to_start <= 60:
                return "CLOSE", f"⏰ เหลือ {int(mins_to_start)} นาที — สัญญาณแรง", 0.7
        except Exception:
            pass

    hour = ts.hour
    if 0 <= hour < 8:
        return "EARLY", "🌅 เช้าตรู่ — Sharp analyst money", 0.8
    elif 8 <= hour < 20:
        return "MIDDAY", "☀️ กลางวัน — อาจเป็นกระแสหน้าตั๋ว", 0.3
    else:
        return "NIGHT", "🌙 กลางคืน — สัญญาณผสม", 0.5


def grade_signal(lm: LineMovement, liquidity_usd: float = 0,
                 commence_time: str = "", num_bm_moved: int = 1) -> tuple[str, str, list[str]]:
    """
    จัดเกรดสัญญาณ RLM/Steam
    Returns: (grade, grade_emoji, reasons)

    Grade A: RLM + (Steam หรือ High Liquidity) + จังหวะดี
    Grade B: RLM หรือ Steam อย่างเดียว + liquidity พอใช้
    Grade C: Line Move ธรรมดา
    """
    score = 0.0
    reasons = []

    # RLM = +3 คะแนน
    if lm.is_rlm:
        score += 3.0
        reasons.append("🔄 RLM — Pinnacle odds ลง (Sharp money)")

    # Steam = +2 คะแนน
    if lm.is_steam:
        score += 2.0
        reasons.append(f"🌊 Steam Move — {num_bm_moved} เว็บขยับพร้อมกัน")

    # Liquidity
    if liquidity_usd >= RLM_MIN_LIQUIDITY_USD:
        score += 2.0
        reasons.append(f"💰 High Liquidity (${liquidity_usd:,.0f})")
    elif liquidity_usd >= 5000:
        score += 1.0
        reasons.append(f"💵 Medium Liquidity (${liquidity_usd:,.0f})")
    elif liquidity_usd > 0 and liquidity_usd < 5000:
        score -= 1.0
        reasons.append(f"⚠️ Low Liquidity (${liquidity_usd:,.0f}) — อาจเป็นสัญญาณปลอม")

    # Time-of-Move
    time_label, time_desc, time_boost = classify_move_time(lm.ts, commence_time)
    score += time_boost * 2  # max +2 คะแนน
    reasons.append(time_desc)

    # H2H Focus — กีฬาที่ Sharp เข้ามาก
    if lm.sport in H2H_FOCUS_SPORTS:
        score += 0.5
        reasons.append(f"🎯 H2H Focus Sport — Sharp money เข้ามาก")

    # ขนาดการขยับ — ยิ่งแรงยิ่งดี
    abs_pct = abs(float(lm.pct_change))
    if abs_pct >= 0.15:
        score += 1.0
        reasons.append(f"📊 ขยับแรง {abs_pct:.1%}")
    elif abs_pct >= 0.10:
        score += 0.5

    # จัดเกรด
    if score >= 6.0:
        return "A", "🅰️", reasons
    elif score >= 3.5:
        return "B", "🅱️", reasons
    else:
        return "C", "🅲", reasons


def sport_to_path(sport_key: str) -> dict:
    """แปลง sport_key เป็น path สำหรับแต่ละ bookmaker
    K5: เพิ่ม stake/cloudbet keys
    """
    s = sport_key.lower()
    if "basketball" in s:
        return {"pinnacle": "basketball", "1xbet": "basketball", "dafabet": "sports/basketball",
                "stake": "basketball", "cloudbet": "basketball"}
    if "soccer" in s:
        return {"pinnacle": "soccer", "1xbet": "football", "dafabet": "sports/football",
                "stake": "soccer", "cloudbet": "soccer"}
    if "americanfootball" in s:
        return {"pinnacle": "american-football", "1xbet": "american-football", "dafabet": "sports/american-football",
                "stake": "american-football", "cloudbet": "american-football"}
    if "baseball" in s:
        return {"pinnacle": "baseball", "1xbet": "baseball", "dafabet": "sports/baseball",
                "stake": "baseball", "cloudbet": "baseball"}
    if "icehockey" in s:
        return {"pinnacle": "ice-hockey", "1xbet": "ice-hockey", "dafabet": "sports/ice-hockey",
                "stake": "ice-hockey", "cloudbet": "ice-hockey"}
    if "cricket" in s:
        return {"pinnacle": "cricket", "1xbet": "cricket", "dafabet": "sports/cricket",
                "stake": "cricket", "cloudbet": "cricket"}
    if "tennis" in s:
        return {"pinnacle": "tennis", "1xbet": "tennis", "dafabet": "sports/tennis",
                "stake": "tennis", "cloudbet": "tennis"}
    if "mma" in s or "martial" in s:
        return {"pinnacle": "mixed-martial-arts", "1xbet": "mixed-martial-arts", "dafabet": "sports/mma",
                "stake": "mma", "cloudbet": "mma"}
    return {"pinnacle": "sports", "1xbet": "sports", "dafabet": "sports",
            "stake": "sports", "cloudbet": "sports"}


def build_betting_links(event_name: str, outcome: str, sport: str,
                        odds: Decimal, bookmaker_key: str = "",
                        all_odds: dict | None = None) -> str:
    """สร้างลิงค์ตรงไปหน้า betting พร้อมแสดง odds ของแต่ละเว็บ

    all_odds: dict {bookmaker_lower -> odds_float} สำหรับแสดง odds ต่อท้ายลิงก์
    """
    links = []
    parts = event_name.split(" vs ")
    sp = sport_to_path(sport)

    def _odds_tag(bm_key: str) -> str:
        if not all_odds: return ""
        o = all_odds.get(bm_key)
        return f" `{float(o):.3f}`" if o else ""

    links.append(f"  🔵 [Pinnacle](https://www.pinnacle.com/en/{sp['pinnacle']}){_odds_tag('pinnacle')}")
    links.append(f"  🟠 [1xBet](https://1xbet.com/en/line/{sp['1xbet']}){_odds_tag('onexbet')}")
    links.append(f"  🟢 [Dafabet](https://www.dafabet.com/en/{sp['dafabet']}){_odds_tag('dafabet')}")

    # เว็บอื่นที่มีใน all_odds แต่ไม่ได้ hardcode ไว้
    if all_odds:
        known = {"pinnacle", "onexbet", "1xbet", "dafabet", "polymarket", "kalshi"}
        for bm, o in sorted(all_odds.items()):
            if bm not in known:
                links.append(f"  📌 {bm.title()} — `{float(o):.3f}`")

    # Polymarket
    if parts:
        search_q = parts[0].replace(" ", "+")
        links.append(f"  🟣 [Polymarket](https://polymarket.com/search?query={search_q}){_odds_tag('polymarket')}")

    # Kalshi
    links.append(f"  🔵 [Kalshi](https://kalshi.com/sports){_odds_tag('kalshi')}")

    # Stake.com + Cloudbet (ถ้าตั้ง EXTRA_BOOKMAKERS)
    extra = _s("EXTRA_BOOKMAKERS", "")
    if "stake" in extra:
        links.append(f"  ⚫ [Stake.com](https://stake.com/sports){_odds_tag('stake')}")
    if "cloudbet" in extra:
        links.append(f"  ☁️ [Cloudbet](https://www.cloudbet.com/en/sports){_odds_tag('cloudbet')}")

    return "\n".join(links)


# ══════════════════════════════════════════════════════════════════
#  ASYNC FETCH
# ══════════════════════════════════════════════════════════════════
async def async_fetch_odds(session: aiohttp.ClientSession, sport_key: str) -> list[dict]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY, "regions": "eu,uk,au",
        "markets": "h2h", "oddsFormat": "decimal",
        "bookmakers": BOOKMAKERS,
    }
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            remaining = int(r.headers.get("x-requests-remaining", api_remaining))
            data = await r.json(content_type=None)
            await update_quota(remaining)
            if isinstance(data, list):
                log.info(f"[OddsAPI] {sport_key} | events={len(data)} | remaining={remaining}")
                return data
            log.warning(f"[OddsAPI] {sport_key}: {data.get('message','?')}")
            return []
    except Exception as e:
        log.error(f"[OddsAPI] {sport_key}: {e}")
        return []

async def fetch_poly_market_detail(session: aiohttp.ClientSession, condition_id: str) -> dict:
    """ดึง orderbook depth + liquidity จริงของ market"""
    try:
        # ดึง market depth
        async with session.get(
            f"https://clob.polymarket.com/book",
            params={"token_id": condition_id},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200: return {}
            book = await r.json(content_type=None)
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            # คำนวณ liquidity top 3 levels
            bid_liq = sum(float(b.get("size",0)) for b in bids[:3])
            ask_liq = sum(float(a.get("size",0)) for a in asks[:3])
            best_bid = float(bids[0]["price"]) if bids else 0
            best_ask = float(asks[0]["price"]) if asks else 0
            spread   = best_ask - best_bid if best_bid and best_ask else 0
            return {
                "bid_liquidity": bid_liq,
                "ask_liquidity": ask_liq,
                "best_bid":      best_bid,
                "best_ask":      best_ask,
                "spread":        spread,
                "mid_price":     (best_bid + best_ask) / 2 if best_bid and best_ask else 0,
            }
    except Exception as e:
        log.debug(f"[Poly orderbook] {condition_id}: {e}")
        return {}


async def _http_get_with_retry(session: aiohttp.ClientSession, url: str,
                               params: dict | None = None, max_tries: int = 3,
                               label: str = "") -> tuple[int, any]:
    """INFO1: Retry with exponential backoff for 429/502/503.
    Returns (status, json_data). Returns (status, None) after all retries."""
    for attempt in range(max_tries):
        try:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                status = r.status
                if status == 200:
                    data = await r.json(content_type=None)
                    return status, data
                if status in (429, 502, 503) and attempt < max_tries - 1:
                    wait = 1.5 ** attempt + random.uniform(0, 0.5)
                    log.warning(f"[Fetch] {label} HTTP {status} — retry {attempt+1}/{max_tries-1} in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                return status, None
        except Exception as e:
            if attempt < max_tries - 1:
                wait = 1.5 ** attempt
                log.debug(f"[Fetch] {label} error: {e} — retry in {wait:.1f}s")
                await asyncio.sleep(wait)
            else:
                log.warning(f"[Fetch] {label} failed after {max_tries} tries: {e}")
    return 0, None


async def async_fetch_polymarket(session: aiohttp.ClientSession) -> list[dict]:
    """ดึง Polymarket sports events ผ่าน Gamma API (แม่นยำกว่า CLOB /markets)"""
    try:
        # Step 1: ดึง sports series list จาก Gamma API
        gamma_markets: list[dict] = []
        _st, _data = await _http_get_with_retry(
            session, "https://gamma-api.polymarket.com/events",
            params={"active": "true", "closed": "false", "limit": 100,
                    "tag_slug": "sports", "order": "volume24hr", "ascending": "false"},
            label="Polymarket/Gamma",
        )
        if _st == 200 and isinstance(_data, list):
            gamma_markets = _data

        if not gamma_markets:
            # fallback ถ้า Gamma ไม่ตอบ — ลอง CLOB เดิม (with retry)
            _st2, _data2 = await _http_get_with_retry(
                session, "https://clob.polymarket.com/markets",
                params={"active": True, "closed": False, "tag_slug": "sports"},
                label="Polymarket/CLOB",
            )
            raw = _data2.get("data", []) if isinstance(_data2, dict) else []
            enriched = []
            for m in raw[:80]:
                tokens = m.get("tokens", [])
                if len(tokens) < 2: continue
                fee_rate = float(m.get("maker_base_fee", 0)) + float(m.get("taker_base_fee", 200))
                volume_24h = float(m.get("volume_num_24hr", 0) or 0)
                total_vol  = float(m.get("volume", 0) or 0)
                if volume_24h < 500 and total_vol < 5000: continue
                p_a = float(tokens[0].get("price", 0))
                p_b = float(tokens[1].get("price", 0))
                if p_a <= 0.01 or p_b <= 0.01: continue
                m["_fee_pct"]    = fee_rate / 10000
                m["_volume_24h"] = volume_24h
                m["_liquidity"]  = min(volume_24h, total_vol / 30)
                    # R5: infer _sport เหมือน Gamma path
                _slug_fb = m.get("slug", "").lower()
                _sport_fb = ""
                for _sp_fb in ("soccer", "football", "basketball", "mma", "tennis", "baseball", "hockey"):
                    if _sp_fb in _slug_fb:
                        _sport_fb = _sp_fb
                        break
                m["_sport"] = _sport_fb
                enriched.append(m)
            log.info(f"[Polymarket] CLOB fallback: {len(enriched)} markets")
            return enriched

        # Step 2: แปลง Gamma events → format เดิมที่ find_polymarket ใช้
        enriched = []
        for ev in gamma_markets[:100]:
            markets_in_ev = ev.get("markets", [])
            # เอาเฉพาะ binary market (2 outcomes)
            binary = [m for m in markets_in_ev if len(m.get("outcomes", [])) == 2
                      or len(m.get("tokens", [])) == 2]
            if not binary:
                # Gamma event อาจเก็บ tokens ที่ระดับ event
                tokens = ev.get("tokens", [])
                if len(tokens) == 2:
                    binary = [ev]
            for m in binary:
                tokens = m.get("tokens", [])
                # Gamma API: tokens อาจอยู่ที่ market level หรือ event level
                if not tokens:
                    # สร้าง synthetic tokens จาก outcomes + prices
                    outcomes = m.get("outcomes", [])
                    out_prices = m.get("outcomePrices", m.get("outcome_prices", []))
                    if len(outcomes) == 2 and len(out_prices) == 2:
                        tokens = [
                            {"outcome": outcomes[0], "price": out_prices[0],
                             "token_id": m.get("clobTokenIds", ["",""])[0] if m.get("clobTokenIds") else ""},
                            {"outcome": outcomes[1], "price": out_prices[1],
                             "token_id": m.get("clobTokenIds", ["",""])[1] if m.get("clobTokenIds") else ""},
                        ]
                if len(tokens) < 2: continue

                p_a = float(tokens[0].get("price", 0) or 0)
                p_b = float(tokens[1].get("price", 0) or 0)
                if p_a <= 0.01 or p_b <= 0.01: continue

                volume_24h = float(m.get("volume24hr", ev.get("volume24hr", 0)) or 0)
                total_vol  = float(m.get("volume",    ev.get("volume",    0)) or 0)
                if volume_24h < 200 and total_vol < 2000: continue

                fee_rate = float(m.get("makerBaseFee", 0) or 0) + float(m.get("takerBaseFee", 200) or 200)

                # Infer sport from event tags or slug for Yes/No guard
                ev_tags = [t.get("slug", "") for t in ev.get("tags", [])]
                ev_sport = next((tg for tg in ev_tags if tg in (
                    "soccer", "football", "basketball", "mma", "tennis",
                    "baseball", "hockey", "rugby",
                )), "")
                if not ev_sport:
                    _slug = m.get("slug", ev.get("slug", "")).lower()
                    for _sp in ("soccer", "football", "basketball", "mma", "tennis"):
                        if _sp in _slug:
                            ev_sport = _sp
                            break
                enriched.append({
                    "question":        m.get("question", ev.get("title", ev.get("question", ""))),
                    "slug":            m.get("slug", ev.get("slug", "")),
                    "tokens":          tokens,
                    "_fee_pct":        fee_rate / 10000,
                    "_volume_24h":     volume_24h,
                    "_liquidity":      max(volume_24h, total_vol / 30),
                    "_gamma":          True,
                    "_sport":          ev_sport,
                })

        # Step 3: enrich top 20 ด้วย real orderbook liquidity
        enriched.sort(key=lambda x: x.get("_liquidity", 0), reverse=True)
        top, rest = enriched[:20], enriched[20:]
        async def _empty_book(): return {}
        book_tasks = [
            fetch_poly_market_detail(session, m["tokens"][0].get("token_id", ""))
            if m["tokens"][0].get("token_id") else _empty_book()
            for m in top
        ]
        books = await asyncio.gather(*book_tasks, return_exceptions=True)
        for m, book in zip(top, books):
            if isinstance(book, dict) and book:
                real_liq = book.get("bid_liquidity", 0) + book.get("ask_liquidity", 0)
                if real_liq > 0:
                    m["_liquidity"] = real_liq
                m["_orderbook"] = book
        enriched = top + rest

        log.info(f"[Polymarket] Gamma API: events={len(gamma_markets)} | filtered={len(enriched)} | enriched={len(top)}")
        return enriched

    except Exception as e:
        log.warning(f"[Polymarket] {e}")
        return []


# ══════════════════════════════════════════════════════════════════
#  KALSHI FETCH
# ══════════════════════════════════════════════════════════════════
async def fetch_kalshi_market_detail(session: aiohttp.ClientSession, ticker: str) -> dict:
    """G1: ดึง yes_bid/yes_ask ล่าสุดจาก Kalshi REST API ตาม ticker
    Returns dict เดียวกับ fetch_poly_market_detail: {mid_price, ...}
    ticker เช่น 'KXNBA-23NOV16-T225.5' — ไม่ใช่ synthetic '_yes'/'_no'
    """
    # strip synthetic suffix ที่เราต่อเองตอน build enriched
    real_ticker = ticker.removesuffix("_yes").removesuffix("_no")
    try:
        _st, _data = await _http_get_with_retry(
            session,
            f"https://trading-api.kalshi.com/trade-api/v2/markets/{real_ticker}",
            label=f"Kalshi/detail/{real_ticker}",
        )
        if _st != 200 or not isinstance(_data, dict): return {}
        m = _data.get("market", _data)  # API v2 wraps in 'market'
        yes_bid = float(m.get("yes_bid", 0) or 0) / 100
        yes_ask = float(m.get("yes_ask", 0) or 0) / 100
        if yes_bid <= 0 or yes_ask <= 0: return {}
        yes_mid = (yes_bid + yes_ask) / 2
        no_mid  = 1 - yes_mid
        # return format เดียวกับ fetch_poly_market_detail
        return {
            "mid_price":     yes_mid,
            "no_mid_price":  no_mid,
            "best_bid":      yes_bid,
            "best_ask":      yes_ask,
            "spread":        yes_ask - yes_bid,
        }
    except Exception as e:
        log.debug(f"[Kalshi/detail] {real_ticker}: {e}")
        return {}
async def async_fetch_kalshi(session: aiohttp.ClientSession) -> list[dict]:
    """ดึง Kalshi sports markets — binary prediction markets (US regulated)
    Docs: https://trading-api.kalshi.com/trade-api/v2
    """
    if not USE_KALSHI:
        return []
    try:
        sport_categories = ["sports", "basketball", "football", "soccer", "baseball", "mma"]
        all_markets: list[dict] = []
        for cat in sport_categories:
            _st, _data = await _http_get_with_retry(
                session, "https://trading-api.kalshi.com/trade-api/v2/markets",
                params={"status": "open", "category": cat, "limit": 100},
                label=f"Kalshi/{cat}",
            )
            if _st == 200 and isinstance(_data, dict):
                all_markets.extend(_data.get("markets", []))

        enriched = []
        seen_ids = set()
        for m in all_markets:
            mid = m.get("ticker", "")
            if mid in seen_ids: continue
            seen_ids.add(mid)

            # Kalshi binary: yes_bid/yes_ask ใน cents (0-100)
            yes_bid = float(m.get("yes_bid", 0) or 0) / 100
            yes_ask = float(m.get("yes_ask", 0) or 0) / 100
            if yes_bid <= 0 or yes_ask <= 0: continue
            no_bid  = 1 - yes_ask
            no_ask  = 1 - yes_bid
            if no_bid <= 0.01 or yes_bid <= 0.01: continue

            # mid price → decimal odds
            yes_mid = (yes_bid + yes_ask) / 2
            no_mid  = (no_bid  + no_ask)  / 2

            volume = float(m.get("volume", 0) or 0)
            open_interest = float(m.get("open_interest", 0) or 0)
            liquidity = max(volume / 30, open_interest / 10)
            if liquidity < 500: continue  # กรอง thin markets

            title    = m.get("title", "")
            subtitle = m.get("subtitle", "")
            question = f"{title} — {subtitle}" if subtitle else title

            enriched.append({
                "question":    question,
                "slug":        mid,
                "tokens": [
                    {"outcome": "Yes", "price": yes_mid, "token_id": f"{mid}_yes"},
                    {"outcome": "No",  "price": no_mid,  "token_id": f"{mid}_no"},
                ],
                "_fee_pct":    float(COMMISSION.get("kalshi", Decimal("0.07"))),
                "_volume_24h": float(m.get("volume_24h", 0) or 0),
                "_liquidity":  liquidity,
                "_kalshi":     True,
                "_market_url": f"https://kalshi.com/markets/{mid}",
                "_ticker":     mid,
            })

        log.info(f"[Kalshi] markets={len(all_markets)} | filtered={len(enriched)}")
        return enriched

    except Exception as e:
        log.warning(f"[Kalshi] {e}")
        return []


# ══════════════════════════════════════════════════════════════════
#  STAKE.COM + CLOUDBET FETCH (ผ่าน The Odds API — ถ้ามี)
# ══════════════════════════════════════════════════════════════════
async def async_fetch_extra_books(session: aiohttp.ClientSession, sport_key: str) -> list[dict]:
    """ดึง odds จาก Stake.com และ Cloudbet ผ่าน The Odds API
    ต้องเพิ่ม stake,cloudbet ใน EXTRA_BOOKMAKERS env var
    J2: อ่าน x-requests-remaining และเรียก update_quota()
    """
    extra = _s("EXTRA_BOOKMAKERS", "")
    if not extra:
        return []
    try:
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
        params = {
            "apiKey": ODDS_API_KEY, "regions": "eu,uk,au,us",
            "markets": "h2h", "oddsFormat": "decimal",
            "bookmakers": extra,
        }
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            # J2: อัปเดต quota เหมือน main odds
            remaining = int(r.headers.get("x-requests-remaining", api_remaining))
            await update_quota(remaining)
            if r.status != 200: return []
            data = await r.json(content_type=None)
            if isinstance(data, list):
                log.debug(f"[ExtraBooks] {sport_key} extra={extra} events={len(data)} remaining={remaining}")
                return data
        return []
    except Exception as e:
        log.debug(f"[ExtraBooks] {sport_key}: {e}")
        return []


async def _fetch_extra_books_sem(session: aiohttp.ClientSession, sport: str) -> list[dict]:
    """J2: wrap async_fetch_extra_books ด้วย Semaphore ตัวเดียวกับ main odds"""
    sem = _ODDS_API_SEM
    if sem is None:
        return await async_fetch_extra_books(session, sport)
    async with sem:
        return await async_fetch_extra_books(session, sport)

_ODDS_API_SEM: Optional[asyncio.Semaphore] = None  # B8: จำกัด concurrent Odds API requests

async def _fetch_odds_sem(session: aiohttp.ClientSession, sport: str) -> list[dict]:
    """B8: wrap async_fetch_odds ด้วย Semaphore เพื่อไม่ให้ burst เกิน 5 concurrent"""
    sem = _ODDS_API_SEM
    if sem is None:
        return await async_fetch_odds(session, sport)
    async with sem:
        return await async_fetch_odds(session, sport)

async def fetch_all_async(sports: list[str]) -> tuple[dict, list]:
    # F5: _ODDS_API_SEM สร้างใน post_init() แล้ว — lazy-init ออก
    async with aiohttp.ClientSession() as session:
        n = len(sports)
        results = await asyncio.gather(
            *[_fetch_odds_sem(session, s) for s in sports],
            async_fetch_polymarket(session),
            async_fetch_kalshi(session),
            # J2: ใช้ _fetch_extra_books_sem (ผ่าน semaphore + quota)
            *([_fetch_extra_books_sem(session, s) for s in sports] if _s("EXTRA_BOOKMAKERS","") else []),
        )
    odds_by_sport = {s: results[i] for i, s in enumerate(sports)}
    poly_markets  = results[n]       # Polymarket
    kalshi_markets = results[n + 1]  # Kalshi

    # Q4: Merge extra bookmaker events into odds_by_sport — fuzzy match ชื่อทีม
    if _s("EXTRA_BOOKMAKERS", ""):
        for i, s in enumerate(sports):
            extra_events = results[n + 2 + i]
            if not extra_events: continue
            std_events = odds_by_sport[s]
            for ev in extra_events:
                ev_name = f"{ev.get('home_team','')} vs {ev.get('away_team','')}"
                merged = False
                for std_ev in std_events:
                    std_name = f"{std_ev.get('home_team','')} vs {std_ev.get('away_team','')}"
                    if fuzzy_match(ev_name, std_name, 0.80):
                        existing_bms = {bm["key"] for bm in std_ev.get("bookmakers", [])}
                        for bm in ev.get("bookmakers", []):
                            if bm["key"] not in existing_bms:
                                std_ev.setdefault("bookmakers", []).append(bm)
                        merged = True
                        break
                if not merged:
                    std_events.append(ev)

    # Combine Polymarket + Kalshi into single alt-markets list
    all_alt_markets = list(poly_markets) + list(kalshi_markets)
    return odds_by_sport, all_alt_markets


# ══════════════════════════════════════════════════════════════════
#  SLIPPAGE + ARB
# ══════════════════════════════════════════════════════════════════
def apply_slippage(odds: Decimal, bm: str) -> Decimal:
    com = next((v for k,v in COMMISSION.items() if k in bm.lower()), Decimal("0"))
    return (odds * (Decimal("1") - com)).quantize(Decimal("0.001"))

def calc_arb(odds_a: Decimal, odds_b: Decimal):
    inv_a, inv_b = Decimal("1")/odds_a, Decimal("1")/odds_b
    margin = inv_a + inv_b
    if margin >= 1: return Decimal("0"), Decimal("0"), Decimal("0")
    profit = (Decimal("1") - margin) / margin
    s_a = (TOTAL_STAKE * inv_a / margin).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return profit, s_a, (TOTAL_STAKE - s_a).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

def calc_arb_3way(odds_h: Decimal, odds_d: Decimal, odds_a: Decimal, total: Optional[Decimal] = None):
    """คำนวณ 3-way arb (soccer 1X2): Home / Draw / Away
    C12: รับ total param — ถ้าไม่ส่งมาใช้ TOTAL_STAKE (backward compat)
    Returns: (profit_pct, stake_h, stake_d, stake_a) ทั้งหมดใน USD
    """
    _total = total if total is not None else TOTAL_STAKE
    inv_h = Decimal("1") / odds_h
    inv_d = Decimal("1") / odds_d
    inv_a = Decimal("1") / odds_a
    margin = inv_h + inv_d + inv_a
    if margin >= 1:
        return Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")
    profit = (Decimal("1") - margin) / margin
    s_h = (_total * inv_h / margin).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    s_d = (_total * inv_d / margin).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    s_a = (_total * inv_a / margin).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return profit, s_h, s_d, s_a


def calc_arb_fixed(odds_a: Decimal, odds_b: Decimal, total: Decimal):
    """Calc arb with custom total stake (ใช้หลัง cap)"""
    inv_a, inv_b = Decimal("1")/odds_a, Decimal("1")/odds_b
    margin = inv_a + inv_b
    if margin >= 1: return Decimal("0"), Decimal("0"), Decimal("0")
    profit = (Decimal("1") - margin) / margin
    s_a = (total * inv_a / margin).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return profit, s_a, (total - s_a).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

def natural_round(amount: Decimal) -> Decimal:
    """
    Natural Rounding — ปัด stake ให้ดูเป็นธรรมชาติ ไม่ให้บ่อนสงสัย
    < 50,000  → ปัดเป็นทวีคูณ 500  (เช่น 10,230 → 10,000 หรือ 10,500)
    >= 50,000 → ปัดเป็นทวีคูณ 1,000 (เช่น 52,300 → 52,000)
    + random jitter ±1 step เพื่อให้ไม่ซ้ำกันทุกครั้ง
    ใช้สำหรับ Kelly total stake sizing — apply MIN/MAX KELLY clamp
    """
    step = Decimal("500") if amount < Decimal("50000") else Decimal("1000")
    base = (amount // step) * step
    jitter = step if random.random() < 0.5 else Decimal("0")
    result = base + jitter
    # R7: clamp — Kelly stake ไม่ควรเกิน MAX_KELLY_STAKE
    result = min(result, MAX_KELLY_STAKE)
    result = max(result, MIN_KELLY_STAKE)
    return result


def natural_round_leg(amount: Decimal) -> Decimal:
    """H3: per-leg rounding — ไม่ได้ floor ขึ้น MIN_KELLY_STAKE
    ใช้ใน execute_both() เท่านั้น — per-leg stake ควร round ไม่ควรถูก force ขึ้นเป็น 10,000
    """
    step = Decimal("500") if amount < Decimal("50000") else Decimal("1000")
    base = (amount // step) * step
    jitter = step if random.random() < 0.5 else Decimal("0")
    result = base + jitter
    # clamp เฉพาะ MAX — ไม่ floor MIN (ไม่งั้น stake ต่อ leg)
    result = min(result, MAX_KELLY_STAKE)
    return max(result, Decimal("100"))  # floor ต่ำมาก 100 บาทเพื่อกัน 0


def get_current_bankroll() -> Decimal:
    """Fix 8: Return BANKROLL_THB + cumulative actual PNL from settled trades."""
    with _data_lock:
        actual_pnl = sum(
            t.actual_profit_thb for t in trade_records
            if t.actual_profit_thb is not None
        )
    return BANKROLL_THB + Decimal(str(actual_pnl))


def calc_kelly_stake(odds_a: Decimal, odds_b: Decimal, profit_pct: Decimal) -> Decimal:
    """
    Kelly Criterion สำหรับ Arbitrage
    ใน arb จริงๆ edge = profit_pct (guaranteed)
    Kelly = edge / odds_range → แต่ใช้ fractional Kelly เพื่อความปลอดภัย

    Full Kelly = (edge) / (1 - 1/max_odds)
    Fractional = Full Kelly × KELLY_FRACTION

    H4: NOTE — สำหรับ 3-way arb ฟังก์ชันนี้รับเฉพาะ odds_a, odds_b (2 legs)
    และใช้ min_prob = min(1/odds_a, 1/odds_b) ซึ่งเป็น conservative approximation
    ไม่ได้คำนวณ true 3-way Kelly — stake ที่ได้จะต่ำกว่า optimal เล็กน้อย (safe side)
    """
    if not USE_KELLY:
        return TOTAL_STAKE  # USD

    edge = float(profit_pct)  # guaranteed edge
    min_prob = float(min(Decimal("1")/odds_a, Decimal("1")/odds_b))
    if min_prob >= 1 or edge <= 0:
        return TOTAL_STAKE  # USD

    full_kelly = edge / (1 - min_prob)
    frac_kelly = full_kelly * float(KELLY_FRACTION)

    # Fix 8: use live bankroll (BANKROLL_THB + actual PNL)
    current_bankroll = get_current_bankroll()
    kelly_thb  = Decimal(str(frac_kelly)) * current_bankroll
    kelly_thb  = max(MIN_KELLY_STAKE, min(MAX_KELLY_STAKE, kelly_thb))
    kelly_thb  = natural_round(kelly_thb)  # พรางตัว — ปัดเป็นเลขกลม 500/1000
    kelly_thb  = max(MIN_KELLY_STAKE, kelly_thb)  # ตรวจ MIN อีกรอบหลัง round
    kelly_usd  = kelly_thb / USD_TO_THB

    log.info(f"[Kelly] edge={edge:.2%} full={full_kelly:.3f} frac={frac_kelly:.3f} bankroll=฿{int(current_bankroll):,} stake=฿{int(kelly_thb):,} (${float(kelly_usd):.0f})")
    return kelly_usd


def calc_valuebet_kelly(true_odds: float, soft_odds: float, grade: str) -> tuple[Decimal, float]:
    """
    Kelly Criterion สำหรับ Value Betting (แทงหน้าเดียว) พร้อม Dynamic Fraction ตามเกรด

    Args:
        true_odds : ราคา Sharp/Pinnacle ก่อนขยับ (estimate true probability)
        soft_odds : ราคา Soft book ที่จะวาง (หลังขยับ)
        grade     : "A" / "B" / "C" จาก grade_signal()

    Returns:
        (rec_stake_thb, edge_pct) — stake แนะนำเป็นบาท, edge เป็น %
        stake = 0 ถ้าไม่มี value
    """
    # Dynamic fraction ตามความคมสัญญาณ
    fraction_map = {"A": 0.30, "B": 0.15, "C": 0.05}
    fraction = fraction_map.get(grade, 0.05)

    if true_odds <= 1.0 or soft_odds <= 1.0:
        return Decimal("0"), 0.0

    true_prob = 1.0 / true_odds
    edge = (true_prob * soft_odds) - 1.0

    if edge <= 0:
        return Decimal("0"), round(edge * 100, 2)

    # Full Kelly สำหรับ directional bet: f* = edge / (b) where b = soft_odds - 1
    full_kelly = edge / (soft_odds - 1.0)
    frac_kelly = full_kelly * fraction

    if not USE_KELLY:
        stake_thb = MIN_KELLY_STAKE
    else:
        current_bankroll = get_current_bankroll()
        stake_thb = Decimal(str(frac_kelly)) * current_bankroll
        stake_thb = max(MIN_KELLY_STAKE, min(MAX_KELLY_STAKE, stake_thb))
        stake_thb = natural_round(stake_thb)
        stake_thb = max(MIN_KELLY_STAKE, stake_thb)

    log.info(f"[DynamicKelly] Grade {grade} | edge={edge:.2%} | full={full_kelly:.3f} frac={frac_kelly:.3f} | stake=฿{int(stake_thb):,}")
    return stake_thb, round(edge * 100, 2)


def apply_vb_book_cap(stake_thb: Decimal, bookmaker: str) -> Decimal:
    """G5: apply per-bookmaker max stake cap สำหรับ Value Bet (stake เป็น THB)"""
    bm  = bookmaker.lower()
    cap = next((v for k, v in MAX_STAKE_MAP.items() if k in bm), Decimal("0"))
    if cap > 0 and stake_thb > cap:
        log.debug(f"[VB-BookCap] {bookmaker}: ฿{int(stake_thb):,} → capped ฿{int(cap):,}")
        return cap
    return stake_thb


def apply_max_stake(stake: Decimal, bookmaker: str) -> Decimal:
    """5. จำกัด stake ตาม MAX_STAKE ของแต่ละเว็บ"""
    bm  = bookmaker.lower()
    cap = next((v for k,v in MAX_STAKE_MAP.items() if k in bm), Decimal("0"))
    if cap > 0:
        stake_thb = stake * USD_TO_THB
        if stake_thb > cap:
            return (cap / USD_TO_THB).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return stake


# ══════════════════════════════════════════════════════════════════
#  SCAN
# ══════════════════════════════════════════════════════════════════
def is_stale(commence_time: str, last_update: str = "") -> bool:
    """1. เช็ค odds staleness
    - แมตช์เริ่มไปแล้วเกิน 3 ชั่วโมง → stale
    - last_update ของ odds เก่าเกิน MAX_ODDS_AGE_MIN นาที → stale
    """
    now = datetime.now(timezone.utc)
    try:
        ct = datetime.fromisoformat(commence_time.replace("Z","+00:00"))
        if ct < now - timedelta(hours=3):
            return True
    except Exception:
        pass
    # ตรวจ odds age จาก last_update (OddsAPI ส่งมาใน market/outcome)
    if last_update:
        try:
            lu = datetime.fromisoformat(last_update.replace("Z","+00:00"))
            if (now - lu).total_seconds() > MAX_ODDS_AGE_MIN * 60:
                return True
        except Exception:
            pass
    return False

def is_valid_odds(odds: Decimal) -> bool:
    """2. กรอง odds ที่ผิดปกติ"""
    return MIN_ODDS_ALLOWED <= odds <= MAX_ODDS_ALLOWED

def make_cooldown_key(event: str, bm1: str, bm2: str, market_type: str = "2way", bm_draw: str = "") -> str:
    """สร้าง cooldown key ที่ consistent สำหรับทั้ง read/write
    L4: 3-way รวม draw bm เพื่อป้องกันผสม key กับ setup ที่ต่าง draw bm
    """
    if market_type == "3way" and bm_draw:
        return f"3way|{event}|{bm1}|{bm_draw}|{bm2}"
    return f"{market_type}|{event}|{bm1}|{bm2}"

def is_on_cooldown(event: str, bm1: str, bm2: str, market_type: str = "2way", bm_draw: str = "") -> bool:
    """3. เช็ค alert cooldown"""
    key  = make_cooldown_key(event, bm1, bm2, market_type, bm_draw)
    last = alert_cooldown.get(key)
    if last and (datetime.now(timezone.utc) - last).total_seconds() < ALERT_COOLDOWN_MIN * 60:
        return True
    return False

def find_draw_market(event_name: str, alt_markets: list) -> Optional[dict]:
    """ค้นหา Draw/X market บน Polymarket หรือ Kalshi สำหรับ soccer 3-way
    ค้นหา market ที่พูดถึง Draw หรือ X ของ event นี้
    Returns: dict เดียวกับ find_polymarket แต่สำหรับ outcome Draw
    """
    parts = [p.strip() for p in event_name.replace(" vs ", "|").split("|")]
    if len(parts) < 2: return None
    ta, tb = parts[0], parts[1]

    DRAW_KEYWORDS = ("draw", " x ", "tie", "no winner", "drawn")

    best, best_score = None, 0
    for m in alt_markets:
        tokens = m.get("tokens", [])
        if len(tokens) < 2: continue
        liquidity = m.get("_liquidity", 0)
        if liquidity < POLY_MIN_DRAW_LIQUIDITY: continue

        title = m.get("question", "").lower()
        # ต้องพูดถึงทีมทั้งสอง + คำว่า draw/x/tie
        has_ta  = fuzzy_match(ta, title, 0.3)
        has_tb  = fuzzy_match(tb, title, 0.3)
        has_draw = any(kw in title for kw in DRAW_KEYWORDS)
        if not (has_ta and has_tb and has_draw): continue

        score = (1 if has_ta else 0) + (1 if has_tb else 0) + min(3, liquidity / 10000)
        if score > best_score:
            best_score, best = score, m

    if not best: return None

    tokens  = best.get("tokens", [])
    # B4: เลือก token ตาม label (Yes/Draw/Tie) แทน index 0 ที่ไม่รับประกัน order
    _draw_labels = ("yes", "draw", "tie")
    yes_token = next(
        (t for t in tokens if str(t.get("outcome", "")).strip().lower() in _draw_labels),
        None,
    )
    if yes_token is None:
        return None  # ไม่มี Draw/Yes token ที่ชัดเจน

    pa = Decimal(str(yes_token.get("price", 0) or 0))
    if pa <= 0.01: return None

    fee_pct = Decimal(str(best.get("_fee_pct", 0.02)))
    liq_usd = best.get("_liquidity", 0)
    est_stake_usd = float(MIN_KELLY_STAKE) / float(USD_TO_THB)
    impact_ratio = min(est_stake_usd / liq_usd, 0.10) if liq_usd > 0 else 0.05
    impact_adj = Decimal(str(1 - impact_ratio * 0.5))

    odds_raw = (Decimal("1") / pa).quantize(Decimal("0.001"))
    odds_eff = (odds_raw * (Decimal("1") - fee_pct) * impact_adj).quantize(Decimal("0.001"))

    is_kalshi = best.get("_kalshi", False)
    slug = best.get("slug", "")
    if is_kalshi:
        ticker = best.get("_ticker", slug)
        market_url = best.get("_market_url", f"https://kalshi.com/markets/{ticker}")
        bm_name = "Kalshi"
    else:
        market_url = f"https://polymarket.com/event/{slug}"
        bm_name = "Polymarket"

    return {
        "market_url": market_url,
        "bookmaker":  bm_name,
        "fee_pct":    float(fee_pct),
        "liquidity":  liq_usd,
        "outcome":    "Draw",
        "odds_raw":   odds_raw,
        "odds":       odds_eff,
        "token_id":   yes_token.get("token_id", ""),
    }


def find_polymarket(event_name: str, poly_markets: list) -> Optional[dict]:
    parts = [p.strip() for p in event_name.replace(" vs ","|").split("|")]
    if len(parts) < 2: return None
    ta, tb = parts[0], parts[1]
    best, best_score = None, 0

    for m in poly_markets:
        tokens = m.get("tokens",[])
        if len(tokens) < 2: continue

        # ✅ Liquidity check — กรอง market ที่ thin เกินไป
        liquidity = m.get("_liquidity", 0)
        if liquidity < POLY_MIN_LIQUIDITY:
            continue

        title = m.get("question","")
        if fuzzy_match(ta, title, 0.3) and fuzzy_match(tb, title, 0.3):
            # Score = keyword match + liquidity bonus
            kw_score = sum(1 for t in (normalize_team(ta).split()+normalize_team(tb).split()) if t in title.lower())
            liq_bonus = min(3, liquidity / 10000)  # liquidity สูง = score สูงกว่า
            score = kw_score + liq_bonus
            if score > best_score:
                best_score, best = score, m

    if not best: return None

    tokens   = best.get("tokens",[])
    if len(tokens) < 2: return None
    # E1: find Yes/No tokens by label — API does not guarantee tokens[0]=Yes
    _tok_yes = next((t for t in tokens if t.get("outcome","").lower() == "yes"), None)
    _tok_no  = next((t for t in tokens if t.get("outcome","").lower() == "no"),  None)
    # ⚠️ G2: Yes/No guard — parse proposition ก่อน map team เพื่อกัน swap เงียบ
    out_a = tokens[0].get("outcome","").lower()
    out_b = tokens[1].get("outcome","").lower()
    if out_a in ("yes","no") or out_b in ("yes","no"):
        question = best.get("question","").lower()
        sport_name = best.get("_sport", "").lower()
        # N4: narrow to strict soccer only — "football" alone covers American football (NFL)
        is_soccer_like = sport_name == "soccer" or "association football" in sport_name
        if is_soccer_like:
            log.debug(f"[PolyYesNo] skip Yes/No soccer market: {best.get('question','')[:60]}")
            return None
        # parse proposition: เช่น "Will Celtics beat Lakers?" → Yes = Celtics, No = Lakers
        ta_norm = normalize_team(ta)
        tb_norm = normalize_team(tb)
        q_lower  = question
        # ตรวจว่า ta หรือ tb ปรากฏใน question
        ta_in_q = fuzzy_match(ta_norm, q_lower, 0.4) or any(w in q_lower for w in ta_norm.split() if len(w)>3)
        tb_in_q = fuzzy_match(tb_norm, q_lower, 0.4) or any(w in q_lower for w in tb_norm.split() if len(w)>3)
        if not (ta_in_q and tb_in_q):
            log.debug(f"[PolyYesNo] skip — can't parse both teams from question: {best.get('question','')[:60]}")
            return None
        # E2: allowlist approach — only accept clear outright winner propositions
        # blocklist is incomplete; allowlist is safer
        _non_h2h_terms = (
            "cover", "spread", "handicap", "over", "under", "total",
            "advance", "qualify", "series", "map ", "set ", "quarter",
            "period", "first half", "game 1", "game 2", "lift the trophy",
        )
        if any(x in q_lower for x in _non_h2h_terms):
            log.debug(f"[PolyYesNo] skip non-H2H proposition: {best.get('question','')[:60]}")
            return None
        # H5: parse ว่า Yes = ทีมไหน — รับ beat/defeat/'win against' + fallback 'will X win'
        # Q6: re imported at top-level
        _pat = re.search(r'will\s+(.*?)\s+(?:beat|defeat|win against)', q_lower)
        if not _pat:
            # G4: fallback — 'Will X win?' format (single team, treated as Yes=X)
            _pat = re.search(r'will\s+(.*?)\s+win\b', q_lower)
        if not _pat:
            log.debug(f"[PolyYesNo] skip — can't parse Yes team from: {best.get('question','')[:60]}")
            return None
        subject = _pat.group(1).strip()
        if fuzzy_match(subject, tb_norm, 0.5):
            yes_team, no_team = tb, ta  # Yes = away
        elif fuzzy_match(subject, ta_norm, 0.5):
            yes_team, no_team = ta, tb  # Yes = home
        else:
            log.debug(f"[PolyYesNo] skip — subject '{subject}' doesn't match either team")
            return None
        # E1: build tokens_mapped using label-resolved Yes/No tokens (not positional index)
        if _tok_yes is None or _tok_no is None:
            log.debug(f"[PolyYesNo] skip — no Yes/No tokens found in market")
            return None
        tokens_mapped = [
            {**_tok_yes, "outcome": yes_team},
            {**_tok_no,  "outcome": no_team},
        ]
        tokens = tokens_mapped
        log.debug(f"[PolyYesNo] mapped Yes={yes_team} No={no_team} from: {best.get('question','')[:60]}")
    pa       = Decimal(str(tokens[0].get("price",0)))
    pb       = Decimal(str(tokens[1].get("price",0)))
    if pa <= 0 or pb <= 0: return None

    # ✅ ใช้ fee จริงจาก API แทน hardcode 2%
    fee_pct  = Decimal(str(best.get("_fee_pct", 0.02)))
    liq_usd  = best.get("_liquidity", 0)
    vol_24h  = best.get("_volume_24h", 0)

    # #26 Impact Cost — ถ้า liquidity บาง stake ใหญ่จะกิน spread
    # ประมาณ stake ที่จะวาง (Kelly min ÷ USD_TO_THB เป็น USD)
    est_stake_usd = float(MIN_KELLY_STAKE) / float(USD_TO_THB)
    if liq_usd > 0:
        # impact = stake / liquidity (สัดส่วน orderbook ที่จะกิน)
        impact_ratio = min(est_stake_usd / liq_usd, 0.10)  # cap 10%
    else:
        impact_ratio = 0.05  # default 5% ถ้าไม่รู้ liquidity
    # แปลง impact เป็น odds penalty (ยิ่ง impact มาก ยิ่ง odds ลด)
    impact_adj = Decimal(str(1 - impact_ratio * 0.5))  # max -5% odds

    def poly_odds(p: Decimal) -> tuple[Decimal, Decimal]:
        odds_raw = (Decimal("1") / p).quantize(Decimal("0.001"))
        # fee + impact cost
        odds_eff = (odds_raw * (Decimal("1") - fee_pct) * impact_adj).quantize(Decimal("0.001"))
        return odds_raw, odds_eff

    slug    = best.get("slug","")
    odds_raw_a, odds_a = poly_odds(pa)
    odds_raw_b, odds_b = poly_odds(pb)

    if impact_ratio > 0.03:
        log.info(f"[PolyImpact] {best.get('question','?')[:40]} liq=${liq_usd:.0f} impact={impact_ratio:.1%} adj={float(impact_adj):.3f}")

    is_kalshi = best.get("_kalshi", False)
    if is_kalshi:
        ticker = best.get("_ticker", slug)
        market_url = best.get("_market_url", f"https://kalshi.com/markets/{ticker}")
        bm_name = "Kalshi"
    else:
        market_url = f"https://polymarket.com/event/{slug}"
        bm_name = "Polymarket"

    return {
        "market_url":   market_url,
        "bookmaker":    bm_name,
        "fee_pct":      float(fee_pct),
        "liquidity":    liq_usd,
        "volume_24h":   vol_24h,
        "impact_ratio": impact_ratio,
        "team_a": {"name": tokens[0].get("outcome", ta),
                   "odds_raw": odds_raw_a, "odds": odds_a,
                   "token_id": tokens[0].get("token_id", "")},
        "team_b": {"name": tokens[1].get("outcome", tb),
                   "odds_raw": odds_raw_b, "odds": odds_b,
                   "token_id": tokens[1].get("token_id", "")},
    }

def scan_all(odds_by_sport: dict, poly_markets: list) -> list[ArbOpportunity]:
    found = []
    for sport_key, events in odds_by_sport.items():
        for event in events:
            home       = event.get("home_team","")
            away       = event.get("away_team","")
            event_name = f"{home} vs {away}"
            commence   = event.get("commence_time","").replace("T"," ").rstrip("Z")  # B10: keep full ISO, no [:16]

            # 1. Staleness check
            if is_stale(event.get("commence_time","")):
                log.debug(f"[Stale] {event_name}")
                continue

            is_soccer = sport_key.startswith(THREE_WAY_SPORTS_PREFIX)
            best: dict[str, OddsLine] = {}
            for bm in event.get("bookmakers",[]):
                bk, bn = bm.get("key",""), bm.get("title", bm.get("key",""))
                for mkt in bm.get("markets",[]):
                    if mkt.get("key") != "h2h": continue
                    mkt_last_update = mkt.get("last_update", "")
                    for out in mkt.get("outcomes",[]):
                        name     = out.get("name","")
                        nl = name.lower()
                        # กรอง "no contest"/"nc" เสมอ
                        if nl in ("no contest", "nc"): continue
                        is_draw = nl in ("draw", "tie")
                        # กรอง Draw สำหรับกีฬา 2-way — soccer เก็บ Draw ไว้
                        if is_draw and not is_soccer: continue
                        odds_raw = Decimal(str(out.get("price",1)))
                        # 2. Odds filter
                        if not is_valid_odds(odds_raw): continue
                        # 1b. Odds staleness check ด้วย last_update จริง
                        if is_stale(event.get("commence_time",""), mkt_last_update):
                            log.debug(f"[Stale-odds] {event_name} {bn} last_update={mkt_last_update}")
                            continue
                        odds_eff = apply_slippage(odds_raw, bk)
                        # Normalize Draw → "Draw" เพื่อให้ merge ได้ถูกต้อง
                        key_name = "Draw" if is_draw else name
                        if key_name not in best or odds_eff > best[key_name].odds:
                            best[key_name] = OddsLine(bookmaker=bn, outcome=key_name,
                                                      odds=odds_eff, odds_raw=odds_raw,
                                                      raw={"bm_key":bk,"event_id":event.get("id","")},
                                                      last_update=mkt_last_update or commence)

            poly = find_polymarket(event_name, poly_markets)
            if poly:
                for side, team in [("team_a",home),("team_b",away)]:
                    p = poly[side]
                    if not is_valid_odds(p["odds"]): continue
                    matched = next((k for k in best if fuzzy_match(p["name"],k)), team)
                    if matched not in best or p["odds"] > best[matched].odds:
                        bm_name = poly.get("bookmaker", "Polymarket")
                        best[matched] = OddsLine(bookmaker=bm_name, outcome=matched,
                                                 odds=p["odds"], odds_raw=p["odds_raw"],
                                                 market_url=poly["market_url"],
                                                 raw={"token_id": p["token_id"]})

            # Soccer 3-way: ค้นหา Draw market บน alt-markets ถ้ายังไม่มีใน best
            if is_soccer:
                draw_mkt = find_draw_market(event_name, poly_markets)
                if draw_mkt and is_valid_odds(draw_mkt["odds"]):
                    if "Draw" not in best or draw_mkt["odds"] > best["Draw"].odds:
                        best["Draw"] = OddsLine(
                            bookmaker=draw_mkt["bookmaker"],
                            outcome="Draw",
                            odds=draw_mkt["odds"],
                            odds_raw=draw_mkt["odds_raw"],
                            market_url=draw_mkt["market_url"],
                            raw={"token_id": draw_mkt["token_id"]},
                        )

            if is_soccer:
                # ══ Soccer 3-way branch: ตรวจเฉพาะ Home/Draw/Away ครบครัน ══
                # เพิ่ม 2-way arb สำหรับ soccer ด้วย — ถ้า Draw ไม่ครบ
                home_key = next((k for k in best if k.lower() not in ("draw",) and fuzzy_match(k, home, 0.4)), None)
                away_key = next((k for k in best if k.lower() not in ("draw",) and fuzzy_match(k, away, 0.4)), None)
                draw_key = "Draw" if "Draw" in best else None

                if home_key and away_key and draw_key:
                    # True 3-way arb: 1/H + 1/D + 1/A < 1
                    bh, bd, ba = best[home_key], best[draw_key], best[away_key]
                    bms = {bh.bookmaker, bd.bookmaker, ba.bookmaker}
                    if len(bms) >= 2:
                        if not is_on_cooldown(event_name, bh.bookmaker, ba.bookmaker, "3way", bd.bookmaker):
                            profit3, sh, sd, sa = calc_arb_3way(bh.odds, bd.odds, ba.odds)
                            if profit3 >= MIN_PROFIT_PCT:
                                # Kelly — scale total stake by edge
                                kelly_total = calc_kelly_stake(bh.odds, ba.odds, profit3)  # approx 2-leg Kelly
                                if kelly_total != TOTAL_STAKE:
                                    ratio = kelly_total / TOTAL_STAKE
                                    sh = (sh * ratio).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                                    sd = (sd * ratio).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                                    sa = (sa * ratio).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                                    # BugA: คำนวณ profit จาก scaled stakes โดยตรง ไม่ใช้ TOTAL_STAKE
                                    _tt3 = sh + sd + sa
                                    _w3_min = min(sh*bh.odds, sd*bd.odds, sa*ba.odds)
                                    profit3 = (_w3_min - _tt3) / _tt3 if _tt3 > 0 else Decimal("0")
                                # apply_max_stake per leg — cap ถ้าเกิน
                                sh_cap = apply_max_stake(sh, bh.bookmaker)
                                sd_cap = apply_max_stake(sd, bd.bookmaker)
                                sa_cap = apply_max_stake(sa, ba.bookmaker)
                                if sh_cap < sh or sd_cap < sd or sa_cap < sa:
                                    # ใช้ stake ต่ำสุดเป็น limit และ rescale ทั้ง 3 legs
                                    min_ratio = min(
                                        sh_cap / sh if sh > 0 else Decimal("1"),
                                        sd_cap / sd if sd > 0 else Decimal("1"),
                                        sa_cap / sa if sa > 0 else Decimal("1"),
                                    )
                                    sh = (sh * min_ratio).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                                    sd = (sd * min_ratio).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                                    sa = (sa * min_ratio).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                                    # BugA: recompute profit จาก scaled stakes
                                    _tt3 = sh + sd + sa
                                    _w3_min = min(sh*bh.odds, sd*bd.odds, sa*ba.odds)
                                    profit3 = (_w3_min - _tt3) / _tt3 if _tt3 > 0 else Decimal("0")
                                if profit3 >= MIN_PROFIT_PCT:
                                    opp = ArbOpportunity(
                                        signal_id=str(uuid.uuid4())[:8], sport=sport_key,
                                        event=event_name, commence=commence,
                                        leg1=bh, leg2=ba, leg3=bd,
                                        profit_pct=profit3, stake1=sh, stake2=sa, stake3=sd,
                                    )
                                    found.append(opp)
                                    alert_cooldown[make_cooldown_key(event_name, bh.bookmaker, ba.bookmaker, "3way", bd.bookmaker)] = datetime.now(timezone.utc)
                                    log.info(f"[ARB-3WAY] {event_name} H={float(bh.odds):.3f} D={float(bd.odds):.3f} A={float(ba.odds):.3f} profit={profit3:.2%} stake=({float(sh):.2f},{float(sd):.2f},{float(sa):.2f})")
                elif home_key and away_key and not draw_key:
                    # M1: Soccer h2h คือ 1X2 — ถ้าไม่มี draw_key อาจเป็นแค่ข้อมูลหลุด — อย่า scan 2-way false arb
                    if "soccer" in sport_key.lower():
                        log.debug(f"[Scan] skip soccer 2-way fallback (no draw_key) for {event_name} — 1X2 market, draw may be missing from feed")
                    else:
                        # ไม่มี Draw — cross-book H vs A เท่านั้น (ไม่ใช่ 1X2 ครบ 3 หน้า)
                        bh, ba = best[home_key], best[away_key]
                        if bh.bookmaker != ba.bookmaker:
                            if not is_on_cooldown(event_name, bh.bookmaker, ba.bookmaker):
                                profit, s_h, s_a = calc_arb(bh.odds, ba.odds)
                                if profit >= MIN_PROFIT_PCT:
                                    kelly_total = calc_kelly_stake(bh.odds, ba.odds, profit)
                                    if kelly_total != TOTAL_STAKE:
                                        profit, s_h, s_a = calc_arb_fixed(bh.odds, ba.odds, kelly_total)
                                    # rebalance หลัง cap (เหมือน non-soccer branch)
                                    s_h_cap = apply_max_stake(s_h, bh.bookmaker)
                                    s_a_cap = apply_max_stake(s_a, ba.bookmaker)
                                    if s_h_cap != s_h or s_a_cap != s_a:
                                        # I20: แปลง cap stake เป็น THB ก่อนคำนวณ new_total (USD)
                                        if s_h_cap < s_h:
                                            limited_thb = s_h_cap * USD_TO_THB
                                            ratio = Decimal("1") / bh.odds
                                            margin = Decimal("1")/bh.odds + Decimal("1")/ba.odds
                                            new_total = (limited_thb / USD_TO_THB) / ratio * margin
                                            profit, s_h, s_a = calc_arb_fixed(bh.odds, ba.odds, new_total)
                                        else:
                                            limited_thb = s_a_cap * USD_TO_THB
                                            ratio = Decimal("1") / ba.odds
                                            margin = Decimal("1")/bh.odds + Decimal("1")/ba.odds
                                            new_total = (limited_thb / USD_TO_THB) / ratio * margin
                                            profit, s_h, s_a = calc_arb_fixed(bh.odds, ba.odds, new_total)
                                    if profit >= MIN_PROFIT_PCT:
                                        opp = ArbOpportunity(
                                            signal_id=str(uuid.uuid4())[:8], sport=sport_key,
                                            event=event_name, commence=commence,
                                            leg1=bh, leg2=ba,
                                            profit_pct=profit, stake1=s_h, stake2=s_a,
                                        )
                                        found.append(opp)
                                        alert_cooldown[make_cooldown_key(event_name, bh.bookmaker, ba.bookmaker)] = datetime.now(timezone.utc)
                                        log.info(f"[ARB-2WAY] {event_name} H vs A profit={profit:.2%}")
            else:
                # ══ Non-soccer: 2-way arb เหมือนเดิม ══
                outcomes = list(best.keys())
                for i in range(len(outcomes)):
                    for j in range(i+1, len(outcomes)):
                        a, b = outcomes[i], outcomes[j]
                        if best[a].bookmaker == best[b].bookmaker: continue
                        # 3. Cooldown check
                        if is_on_cooldown(event_name, best[a].bookmaker, best[b].bookmaker): continue  # 2way default
                        profit, s_a, s_b = calc_arb(best[a].odds, best[b].odds)
                        if profit >= MIN_PROFIT_PCT:
                            # Kelly — ปรับ total stake ตาม edge
                            kelly_total = calc_kelly_stake(best[a].odds, best[b].odds, profit)
                            if kelly_total != TOTAL_STAKE:
                                profit, s_a, s_b = calc_arb_fixed(best[a].odds, best[b].odds, kelly_total)
                            # 5. Apply max stake — recalc ใหม่ถ้าถูก cap
                            s_a_capped = apply_max_stake(s_a, best[a].bookmaker)
                            s_b_capped = apply_max_stake(s_b, best[b].bookmaker)
                            if s_a_capped != s_a or s_b_capped != s_b:
                                if s_a_capped < s_a:
                                    limited = s_a_capped * USD_TO_THB
                                    ratio   = Decimal("1") / best[a].odds
                                    margin  = Decimal("1")/best[a].odds + Decimal("1")/best[b].odds
                                    new_total = (limited / USD_TO_THB) / ratio * margin
                                    profit, s_a, s_b = calc_arb_fixed(best[a].odds, best[b].odds, new_total)
                                else:
                                    limited = s_b_capped * USD_TO_THB
                                    ratio   = Decimal("1") / best[b].odds
                                    margin  = Decimal("1")/best[a].odds + Decimal("1")/best[b].odds
                                    new_total = (limited / USD_TO_THB) / ratio * margin
                                    profit, s_a, s_b = calc_arb_fixed(best[a].odds, best[b].odds, new_total)
                                if profit < MIN_PROFIT_PCT:
                                    log.debug(f"[ARB] {event_name} skipped after cap — profit={profit:.2%}")
                                    continue
                            else:
                                s_a, s_b = s_a_capped, s_b_capped
                            opp = ArbOpportunity(
                                signal_id=str(uuid.uuid4())[:8], sport=sport_key,
                                event=event_name, commence=commence,
                                leg1=best[a], leg2=best[b],
                                profit_pct=profit, stake1=s_a, stake2=s_b,
                            )
                            found.append(opp)
                            alert_cooldown[make_cooldown_key(event_name, best[a].bookmaker, best[b].bookmaker)] = datetime.now(timezone.utc)
                            log.info(f"[ARB] {event_name} | profit={profit:.2%}")
    # Scan summary per sport
    sport_counts: dict[str, int] = {}
    for opp in found:
        sport_counts[opp.sport] = sport_counts.get(opp.sport, 0) + 1
    for sk, cnt in sport_counts.items():
        log.info(f"[ScanSummary] {sk} opps={cnt}")
    return found


# ══════════════════════════════════════════════════════════════════
#  SEND ALERT
# ══════════════════════════════════════════════════════════════════
async def send_alert(opp: ArbOpportunity):
    with _data_lock:
        pending[opp.signal_id] = (opp, time.time())  # A2: store with timestamp

    # ── คำนวณ mins_to_start ก่อนใช้ ──
    try:
        commence_dt = parse_commence(opp.commence)
        mins_to_start = (commence_dt - datetime.now(timezone.utc)).total_seconds() / 60
    except Exception:
        mins_to_start = 999

    _s3_thb = int(opp.stake3 * USD_TO_THB) if opp.stake3 else None
    _tt_thb = int(opp.stake1*USD_TO_THB) + int(opp.stake2*USD_TO_THB) + (_s3_thb or 0)
    entry = {
        "id": opp.signal_id, "event": opp.event, "sport": opp.sport,
        "profit_pct": float(opp.profit_pct),
        "leg1_bm": opp.leg1.bookmaker, "leg1_odds": float(opp.leg1.odds),
        "leg2_bm": opp.leg2.bookmaker, "leg2_odds": float(opp.leg2.odds),
        "stake1_thb": int(opp.stake1*USD_TO_THB),
        "stake2_thb": int(opp.stake2*USD_TO_THB),
        "leg3_bm": opp.leg3.bookmaker if opp.leg3 else None,
        "stake3_thb": _s3_thb,
        "total_stake_thb": _tt_thb,
        "created_at": opp.created_at, "status": "pending",
        "mins_to_start": round(mins_to_start) if mins_to_start < 9999 else 9999,
    }
    with _data_lock:
        opportunity_log.append(entry)
        if len(opportunity_log) > 100: opportunity_log.pop(0)
    db_save_opportunity(entry)   # 💾 save to DB

    emoji = SPORT_EMOJI.get(opp.sport,"🏆")

    urgent = mins_to_start <= 120 and mins_to_start > 0
    closing_soon = mins_to_start <= 30 and mins_to_start > 0

    if closing_soon:
        urgency_tag = "🔴 *CLOSING SOON* — CLV สูงสุด!"
        urgency_note = f"⏰ เหลือ *{int(mins_to_start)} นาที* — ราคาใกล้ปิด CLV แม่นที่สุด"
    elif urgent:
        urgency_tag = "🟡 *แข่งเร็วๆ นี้* — CLV ดี"
        urgency_note = f"⏰ เหลือ *{int(mins_to_start)} นาที* — ยังได้ closing line ที่ดี"
    else:
        urgency_tag = ""
        urgency_note = ""

    s1 = (opp.stake1*USD_TO_THB).quantize(Decimal("1"))
    s2 = (opp.stake2*USD_TO_THB).quantize(Decimal("1"))
    s3_alert = (opp.stake3*USD_TO_THB).quantize(Decimal("1")) if opp.stake3 else None
    w1 = (opp.stake1*opp.leg1.odds*USD_TO_THB).quantize(Decimal("1"))
    w2 = (opp.stake2*opp.leg2.odds*USD_TO_THB).quantize(Decimal("1"))
    w3_alert = (opp.stake3*opp.leg3.odds*USD_TO_THB).quantize(Decimal("1")) if opp.stake3 and opp.leg3 else None
    is_3way_alert = s3_alert is not None
    tt = s1 + s2 + (s3_alert if is_3way_alert else Decimal("0"))

    # แปลงเวลาแข่งเป็น UTC+7 พร้อม countdown
    try:
        _ct    = parse_commence(opp.commence)
        _ct_th = _ct + timedelta(hours=7)
        _date_str = _ct_th.strftime("%d/%m/%Y %H:%M") + " น. ไทย"
        if mins_to_start <= 0:
            _countdown = "🟢 เริ่มแล้ว"
        elif mins_to_start < 60:
            _countdown = f"⏰ อีก {int(mins_to_start)} นาที"
        else:
            _h = int(mins_to_start // 60); _m = int(mins_to_start % 60)
            _countdown = f"⏰ อีก {_h}ชม.{_m}น."
        commence_line = f"📅 *{_date_str}* ({_countdown})"
    except Exception:
        commence_line = f"📅 {opp.commence} UTC"

    urgent_prefix = f"{urgency_tag}\n" if urgency_tag else ""
    arb_type = "3-WAY ARB" if is_3way_alert else "ARB FOUND"
    _bm1 = "🏠 " + opp.leg1.bookmaker[:10]
    _bm2 = "🟠 " + opp.leg2.bookmaker[:10]
    _bm3 = "⚪ " + (opp.leg3.bookmaker[:10] if opp.leg3 else "")
    _pfx_blue = "🔵 " + opp.leg1.bookmaker[:10]
    if is_3way_alert:
        table_rows = (
            f"{_bm1:<12} {opp.leg1.outcome[:15]:<15} {float(opp.leg1.odds):>5.3f} {'฿'+str(int(s1)):>8} {'฿'+str(int(w1)):>8}\n"
            f"{_bm3:<12} {'Draw'[:15]:<15} {float(opp.leg3.odds):>5.3f} {'฿'+str(int(s3_alert)):>8} {'฿'+str(int(w3_alert)):>8}\n"
            f"{_bm2:<12} {opp.leg2.outcome[:15]:<15} {float(opp.leg2.odds):>5.3f} {'฿'+str(int(s2)):>8} {'฿'+str(int(w2)):>8}\n"
        )
        outcome_lines = (
            f"   {md_escape(str(opp.leg1.outcome))} → ฿{int(w1):,} *(+฿{int(w1-tt):,})*\n"
            f"   Draw → ฿{int(w3_alert):,} *(+฿{int(w3_alert-tt):,})*\n"
            f"   {md_escape(str(opp.leg2.outcome))} → ฿{int(w2):,} *(+฿{int(w2-tt):,})*"
        )
    else:
        table_rows = (
            f"{_pfx_blue:<12} {opp.leg1.outcome[:15]:<15} {float(opp.leg1.odds):>5.3f} {'฿'+str(int(s1)):>8} {'฿'+str(int(w1)):>8}\n"
            f"{_bm2:<12} {opp.leg2.outcome[:15]:<15} {float(opp.leg2.odds):>5.3f} {'฿'+str(int(s2)):>8} {'฿'+str(int(w2)):>8}\n"
        )
        outcome_lines = (
            f"   {md_escape(str(opp.leg1.outcome))} → ฿{int(w1):,} *(+฿{int(w1-tt):,})*\n"
            f"   {md_escape(str(opp.leg2.outcome))} → ฿{int(w2):,} *(+฿{int(w2-tt):,})*"
        )
    msg = (
        f"{urgent_prefix}"
        f"{emoji} *{arb_type} — {opp.profit_pct:.2%}* _(หลัง fee)_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{commence_line}  {urgency_note}\n"
        f"🏆 {md_escape(opp.event)}\n"
        f"💵 ทุน: *฿{int(tt):,}* {'_(เป็น Kelly)_' if USE_KELLY else ''}  |  Credits: {api_remaining}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"{'\u0e0a\u0e48\u0e2d\u0e07\u0e17\u0e32\u0e07':<12} {'\u0e1d\u0e31\u0e48\u0e07':<15} {'Odds':>5} {'\u0e27\u0e32\u0e07':>8} {'\u0e44\u0e14\u0e49':>8}\n"
        f"{'\u2500'*51}\n"
        f"{table_rows}"
        f"{'\u2500'*51}\n"
        f"{'\u0e23\u0e27\u0e21':<34} {'\u0e3f'+str(int(tt)):>8}\n"
        f"```\n"
        f"📊 ไม่ว่าใครชนะ\n"
        f"{outcome_lines}\n"
        f"🔗 {' | '.join(u for u in [opp.leg1.market_url, opp.leg2.market_url, (opp.leg3.market_url if opp.leg3 else None)] if u) or '\u2014'}\n"
        f"🆔 `{opp.signal_id}`"
    )
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{opp.signal_id}"),
        InlineKeyboardButton("❌ Reject",  callback_data=f"reject:{opp.signal_id}"),
    ]])
    # 9. Multi-chat (rate-limited)
    for cid in ALL_CHAT_IDS:
        try:
            await _app.bot.send_message(chat_id=cid, text=msg, parse_mode="Markdown",
                                        reply_markup=keyboard if cid==CHAT_ID else None)
            await asyncio.sleep(1.5)  # C10: ป้องกัน Telegram Flood (20 msg/min limit per chat)
        except Exception as e:
            log.error(f"[Alert] chat {cid}: {e}")


def md_escape(text: str) -> str:
    """B3: escape Telegram legacy-Markdown special chars in dynamic text.
    Legacy Markdown only treats _ * backtick [ as special, plus backslash."""
    for ch in ("\\", "_", "*", "`", "["):
        text = text.replace(ch, f"\\{ch}")
    return text


def _parse_settled_at(s: str) -> datetime:
    """P5: Parse settled_at string — UTC-aware datetime (safe for naive strings)"""
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def parse_commence(raw: str) -> datetime:
    """Fix 1: Parse commence_time string — handles all formats robustly.
    Returns UTC-aware datetime. Never appends ':00+00:00' blindly."""
    s = raw.strip().replace(" ", "T")
    # Already has tz info (+00:00 / Z)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "+" in s[10:] or (len(s) > 10 and s[-3] == ":"):
        dt = datetime.fromisoformat(s)
    else:
        # No tz — assume UTC
        dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ══════════════════════════════════════════════════════════════════
#  SLIPPAGE GUARD — Re-fetch live odds ก่อน execute
# ══════════════════════════════════════════════════════════════════
_refetch_cache: dict[str, tuple[float, list]] = {}  # C10: sport -> (ts, events)

async def refetch_live_odds(opp: ArbOpportunity) -> tuple[Decimal, Decimal, bool, bool]:
    """
    Re-fetch ราคาล่าสุดจาก API ก่อนยืนยันการเดิมพัน
    J1: mixed-source arb — fetch ทั้ง standard + extra feeds ถ้า legs มาจากต่าง source
    O1: fail-closed — Returns: (live1, live2, found1, found2)
    ถ้า found=False แสดงว่า market suspended / mapping miss — caller ต้อง abort
    """
    _bookmakers_norm = [norm_bm_key(b) for b in BOOKMAKERS.split(",") if b.strip()]
    _extra_norm      = [norm_bm_key(b) for b in _s("EXTRA_BOOKMAKERS","").split(",") if b.strip()]
    def _is_extra(bm: str) -> bool:
        k = norm_bm_key(bm)
        return k not in _bookmakers_norm and k in _extra_norm

    leg1_extra = _is_extra(opp.leg1.bookmaker)
    leg2_extra = _is_extra(opp.leg2.bookmaker)
    need_standard = not leg1_extra or not leg2_extra  # อย่างน้อย 1 leg เป็น standard
    need_extra    = leg1_extra or leg2_extra           # อย่างน้อย 1 leg เป็น extra

    try:
        now_ts = time.time()
        # C10: cache แยกตาม feed (15s TTL)
        std_events   = []
        extra_events = []
        # L1: ใช้ semaphore wrappers — ไม่ยิง direct
        async with aiohttp.ClientSession() as session:
            if need_standard:
                _ck = opp.sport
                _cts, _cev = _refetch_cache.get(_ck, (0, []))
                if now_ts - _cts < 15 and _cev:
                    std_events = _cev
                else:
                    std_events = await _fetch_odds_sem(session, opp.sport)
                    _refetch_cache[_ck] = (now_ts, std_events)
            if need_extra and _extra_norm:
                _ck2 = f"{opp.sport}__extra"
                _cts2, _cev2 = _refetch_cache.get(_ck2, (0, []))
                if now_ts - _cts2 < 15 and _cev2:
                    extra_events = _cev2
                else:
                    extra_events = await _fetch_extra_books_sem(session, opp.sport)
                    _refetch_cache[_ck2] = (now_ts, extra_events)

        if leg1_extra != leg2_extra:
            log.debug(f"[SlippageGuard] mixed-source refetch: leg1={'extra' if leg1_extra else 'std'} leg2={'extra' if leg2_extra else 'std'}")

        def _lookup_leg(leg, is_extra_leg, std_ev, extra_ev) -> tuple[Decimal, bool]:
            """หา live odds ของ leg จาก feed ที่ถูก — คืน (price, found)"""
            source_events = extra_ev if is_extra_leg else std_ev
            leg_key = leg.raw.get("bm_key", "") if leg.raw else ""
            for event in source_events:
                ename = f"{event.get('home_team','')} vs {event.get('away_team','')}"
                if not fuzzy_match(ename, opp.event, 0.7): continue
                for bm in event.get("bookmakers", []):
                    bk = bm.get("key","")
                    if not (bk == leg_key or leg.bookmaker.lower() in bk.lower()): continue
                    for mkt in bm.get("markets", []):
                        if mkt.get("key") != "h2h": continue
                        for out in mkt.get("outcomes", []):
                            if fuzzy_match(out.get("name",""), leg.outcome, 0.8):
                                return apply_slippage(Decimal(str(out.get("price", 1))), bk), True
            return leg.odds, False  # O1: not found — caller must abort

        live1, found1 = _lookup_leg(opp.leg1, leg1_extra, std_events, extra_events)
        live2, found2 = _lookup_leg(opp.leg2, leg2_extra, std_events, extra_events)
        return live1, live2, found1, found2
    except Exception as e:
        log.warning(f"[SlippageGuard] re-fetch failed: {e}")
    return opp.leg1.odds, opp.leg2.odds, False, False


async def refetch_valuebet_odds(vb: "ValueBetSignal") -> tuple[float, bool]:
    """B2: ดึง live odds ของ soft book ก่อน VB confirm
    G3: รวม EXTRA_BOOKMAKERS path — ถ้า bm ไม่อยู่ใน BOOKMAKERS ให้ fetch extra feed แทน
    K3: ใช้ semaphore wrapper เหมือน scan หลัก
    O1: fail-closed — Returns: (live_price, found)
    ถ้า found=False → market suspended หรือ mapping miss — caller ต้อง abort
    """
    def _search_events(events: list, bm_key_norm: str) -> float | None:
        for event in events:
            ename = f"{event.get('home_team','')} vs {event.get('away_team','')}"
            if not fuzzy_match(ename, vb.event, 0.7):
                continue
            for bm in event.get("bookmakers", []):
                bk = bm.get("key", "")
                if norm_bm_key(bk) != bm_key_norm:
                    continue
                for mkt in bm.get("markets", []):
                    if mkt.get("key") != "h2h": continue
                    for out in mkt.get("outcomes", []):
                        if fuzzy_match(out.get("name", ""), vb.outcome, 0.8):
                            return float(out.get("price", vb.soft_odds))
        return None

    try:
        bm_key_norm = norm_bm_key(vb.bookmaker)
        # ตรวจว่า bm อยู่ใน BOOKMAKERS หรือ EXTRA
        bookmakers_norm = [norm_bm_key(b) for b in BOOKMAKERS.split(",") if b.strip()]
        extra_norm      = [norm_bm_key(b) for b in _s("EXTRA_BOOKMAKERS","").split(",") if b.strip()]
        use_extra = bm_key_norm not in bookmakers_norm and bm_key_norm in extra_norm

        cache_key = f"{vb.sport}{'__extra' if use_extra else ''}"
        cached_ts, cached_events = _refetch_cache.get(cache_key, (0, []))
        if time.time() - cached_ts < 15 and cached_events:
            events = cached_events
        else:
            # K3: ใช้ semaphore wrapper — ไม่ยิง direct
            async with aiohttp.ClientSession() as session:
                if use_extra:
                    events = await _fetch_extra_books_sem(session, vb.sport)
                    log.debug(f"[VBGuard] refetch via extra feed for {vb.bookmaker}")
                else:
                    events = await _fetch_odds_sem(session, vb.sport)
            _refetch_cache[cache_key] = (time.time(), events)
        result = _search_events(events, bm_key_norm)
        if result is not None:
            return result, True
    except Exception as e:
        log.warning(f"[VBGuard] refetch failed: {e}")
    return vb.soft_odds, False  # O1: not found — caller must abort


# ══════════════════════════════════════════════════════════════════
#  EXECUTE
# ══════════════════════════════════════════════════════════════════
async def execute_both(opp: ArbOpportunity) -> str:
    is_3way = opp.leg3 is not None and opp.stake3 is not None

    # 🛡️ Slippage Guard — 2-way only (ไม่ใช้ calc_arb 2-way ตัดสิน 3-way)
    orig_profit = opp.profit_pct
    live_profit = orig_profit  # default
    slippage_warn = ""
    # P1: unified per-leg refetch helper — routes ตาม source (polymarket/kalshi/extra/std)
    # fail-closed: คืน (price, found=False) ถ้าหาไม่เจอ — caller ต้อง abort
    _bookmakers_norm_ex = [norm_bm_key(b) for b in BOOKMAKERS.split(",") if b.strip()]
    _extra_norm_ex      = [norm_bm_key(b) for b in _s("EXTRA_BOOKMAKERS","").split(",") if b.strip()]
    async def _refetch_leg(leg, sport: str, label: str) -> tuple[Decimal, bool]:
        _bm  = leg.bookmaker.lower()
        _tok = leg.raw.get("token_id", "") if leg.raw else ""
        # 1) Polymarket / Kalshi — CLOB / REST API
        # Q1: ใช้ ask side แทน mid_price — conservative execution price (buyer pays ask)
        if _tok and ("polymarket" in _bm or "kalshi" in _bm):
            try:
                async with aiohttp.ClientSession() as _s:
                    _book = (await fetch_kalshi_market_detail(_s, _tok)
                             if "kalshi" in _bm
                             else await fetch_poly_market_detail(_s, _tok))
                _is_no = _tok.endswith("_no")
                if _book:
                    # Q1: prefer ask price (executable) over mid — more conservative
                    if "kalshi" in _bm:
                        # Kalshi: yes_ask / (1-yes_ask) ฝั่ง no
                        _ask_raw = _book.get("best_ask", 0)
                        if _is_no:
                            _no_ask = 1.0 - (_book.get("best_bid", 0) or _ask_raw)
                            _exec_p = _no_ask if _no_ask > 0 else (1.0 - _ask_raw)
                        else:
                            _exec_p = _ask_raw
                    else:
                        # Polymarket: best_ask ของ token นั้น
                        _exec_p = _book.get("best_ask", 0)
                        if _is_no:
                            # No token ask = 1 - Yes bid
                            _yes_bid = _book.get("best_bid", 0)
                            _exec_p = (1.0 - _yes_bid) if _yes_bid > 0 else _exec_p
                    if _exec_p > 0.01:
                        _price = apply_slippage(Decimal("1") / Decimal(str(_exec_p)), _bm)
                        log.info(f"[SlippageGuard] {label} CLOB ask refetch: {float(_price):.3f} (ask={_exec_p:.4f})")
                        return _price, True
                log.warning(f"[SlippageGuard] {label} CLOB empty response")
                return leg.odds, False
            except Exception as _e:
                log.warning(f"[SlippageGuard] {label} CLOB failed: {_e}")
                return leg.odds, False
        # 2) Sportsbook (standard or extra) — Odds API feed
        _is_extra = norm_bm_key(leg.bookmaker) not in _bookmakers_norm_ex
        _bm_key   = leg.raw.get("bm_key", "") if leg.raw else ""
        _ck = f"{sport}{'__extra' if _is_extra else ''}"
        now_ts = time.time()
        _cts, _cev = _refetch_cache.get(_ck, (0, []))
        try:
            if now_ts - _cts < 15 and _cev:
                _events = _cev
            else:
                async with aiohttp.ClientSession() as _s2:
                    _events = (await _fetch_extra_books_sem(_s2, sport)
                               if _is_extra else await _fetch_odds_sem(_s2, sport))
                _refetch_cache[_ck] = (time.time(), _events)
        except Exception as _ef:
            log.warning(f"[SlippageGuard] {label} feed fetch failed: {_ef}")
            return leg.odds, False
        for _ev in _events:
            _en = f"{_ev.get('home_team','')} vs {_ev.get('away_team','')}"
            if not fuzzy_match(_en, opp.event, 0.7): continue
            for _bm2 in _ev.get("bookmakers", []):
                _bk = _bm2.get("key", "")
                if not (_bk == _bm_key or leg.bookmaker.lower() in _bk.lower()): continue
                for _mkt in _bm2.get("markets", []):
                    if _mkt.get("key") != "h2h": continue
                    for _out in _mkt.get("outcomes", []):
                        if fuzzy_match(_out.get("name", ""), leg.outcome, 0.8):
                            _price = apply_slippage(Decimal(str(_out.get("price", 1))), _bk)
                            log.info(f"[SlippageGuard] {label} feed refetch: {float(_price):.3f}")
                            return _price, True
        log.warning(f"[SlippageGuard] {label} not found in feed (market suspended?)")
        return leg.odds, False

    if is_3way:
        # P1: per-leg refetch — ทุก leg รวม leg3(Draw) — fail-closed ทั้งหมด
        live1, _f1 = await _refetch_leg(opp.leg1, opp.sport, "leg1")
        live2, _f2 = await _refetch_leg(opp.leg2, opp.sport, "leg2")
        live3, _f3 = await _refetch_leg(opp.leg3, opp.sport, "leg3(Draw)")
        _missing = [lg for lg, fnd in [(opp.leg1.bookmaker,_f1),(opp.leg2.bookmaker,_f2),(opp.leg3.bookmaker,_f3)] if not fnd]
        if _missing:
            log.warning(f"[SlippageGuard-3way] ABORT {opp.event} — live odds unavailable: {_missing}")
            raise ValueError(
                f"🚫 *ABORT: Live odds unavailable*\n"
                f"ดึงราคา live ไม่ได้: {', '.join(_missing)}\n"
                f"_(market อาจถูก suspend หรือ mapping miss — รอ signal ใหม่)_"
            )
        live_profit3, _, _, _ = calc_arb_3way(live1, live3, live2)  # H, D, A
        drop_too_much_3 = (orig_profit > 0 and
            float(orig_profit - live_profit3) / float(orig_profit) > 0.50)
        # M3: abort ถ้า live < 0, drop > 50%, หรือ ต่ำกว่า MIN_PROFIT_PCT
        if live_profit3 < Decimal("0") or drop_too_much_3 or live_profit3 < MIN_PROFIT_PCT:
            _reason3 = (
                "ติดลบ" if live_profit3 < 0
                else f"ต่ำกว่า threshold {float(MIN_PROFIT_PCT):.1%}" if live_profit3 < MIN_PROFIT_PCT
                else "profit ลด >50%"
            )
            log.warning(f"[SlippageGuard-3way] ABORT {opp.event} — live={live_profit3:.2%} (was {float(orig_profit):.2%}) reason={_reason3}")
            raise ValueError(
                f"🚫 *ABORT: Odds Dropped (3-way)*\n"
                f"ราคาเปลี่ยนขณะรอยืนยัน\n"
                f"คาด: *{float(orig_profit):.2%}* → จริง: *{float(live_profit3):.2%}* ({_reason3})\n"
                f"_(กด Confirm ใหม่ถ้าต้องการลองอีกครั้ง หรือรอ signal ใหม่)_"
            )
        live_profit = live_profit3
        profit_drop_3 = float(opp.profit_pct - live_profit3) / float(opp.profit_pct) if opp.profit_pct > 0 else 0
        if profit_drop_3 > 0.30:
            slippage_warn = f"\n⚠️ *Slippage Alert*: profit ลดลง {profit_drop_3:.0%} (คาด {float(opp.profit_pct):.2%} → จริง {float(live_profit3):.2%})"
    else:
        # P1: per-leg refetch — routes ตาม source (polymarket/kalshi/extra/std) — fail-closed
        live1, _f1 = await _refetch_leg(opp.leg1, opp.sport, "leg1")
        live2, _f2 = await _refetch_leg(opp.leg2, opp.sport, "leg2")
        _missing = [lg for lg, fnd in [(opp.leg1.bookmaker,_f1),(opp.leg2.bookmaker,_f2)] if not fnd]
        if _missing:
            log.warning(f"[SlippageGuard] ABORT {opp.event} — live odds unavailable: {_missing}")
            raise ValueError(
                f"🚫 *ABORT: Live odds unavailable*\n"
                f"ดึงราคา live ไม่ได้: {', '.join(_missing)}\n"
                f"_(market อาจถูก suspend หรือ mapping miss — รอ signal ใหม่)_"
            )
        live_profit, _, _ = calc_arb(live1, live2)
        drop_too_much = (orig_profit > 0 and
                        float(orig_profit - live_profit) / float(orig_profit) > 0.50)
        # M3/N1: abort ถ้า live < 0, drop > 50%, หรือ ต่ำกว่า MIN_PROFIT_PCT
        if live_profit < Decimal("0") or drop_too_much or live_profit < MIN_PROFIT_PCT:
            _reason = (
                "ติดลบ" if live_profit < 0
                else f"ต่ำกว่า threshold {float(MIN_PROFIT_PCT):.1%}" if live_profit < MIN_PROFIT_PCT
                else "profit ลด >50%"
            )
            log.warning(f"[SlippageGuard] ABORT {opp.event} — live profit={live_profit:.2%} (was {float(orig_profit):.2%}) reason={_reason}")
            raise ValueError(
                f"🚫 *ABORT: Odds Dropped*\n"
                f"ราคาเปลี่ยนขณะรอยืนยัน\n"
                f"คาด: *{float(orig_profit):.2%}* → จริง: *{float(live_profit):.2%}* ({_reason})\n"
                f"_(กด Confirm ใหม่ถ้าต้องการลองอีกครั้ง หรือรอ signal ใหม่)_"
            )

    s1_raw = (opp.stake1*USD_TO_THB).quantize(Decimal("1"))
    s2_raw = (opp.stake2*USD_TO_THB).quantize(Decimal("1"))
    s3_raw = (opp.stake3*USD_TO_THB).quantize(Decimal("1")) if opp.stake3 else None
    # H3: natural_round_leg() — per-leg, ไม่ floor MIN_KELLY_STAKE
    # G4: re-apply per-book cap หลัง round (ป้องกัน jitter ดันเกิน cap)
    s1 = apply_max_stake(natural_round_leg(s1_raw)/USD_TO_THB, opp.leg1.bookmaker)*USD_TO_THB
    s2 = apply_max_stake(natural_round_leg(s2_raw)/USD_TO_THB, opp.leg2.bookmaker)*USD_TO_THB
    s1 = s1.quantize(Decimal("1"))
    s2 = s2.quantize(Decimal("1"))
    s3 = None
    if s3_raw is not None and opp.leg3:
        s3 = (apply_max_stake(natural_round_leg(s3_raw)/USD_TO_THB, opp.leg3.bookmaker)*USD_TO_THB).quantize(Decimal("1"))
    # BugB: is_3way already set above — do NOT reassign (s3 cannot become None from natural_round)
    # O3/P4: budget clamp — loop ลด leg ใหญ่สุดซ้ำจน actual_total เข้า tolerance จริง
    _target_total = (opp.stake1 + opp.stake2 + (opp.stake3 or Decimal("0"))) * USD_TO_THB
    _tolerance = _target_total * Decimal("0.05")
    _step_c = Decimal("500") if _target_total < Decimal("50000") else Decimal("1000")
    _clamp_iters = 0
    while True:
        _actual_total = s1 + s2 + (s3 or Decimal("0"))
        if _actual_total <= _target_total + _tolerance or _clamp_iters >= 10:
            break
        _clamp_iters += 1
        if s3 is not None and s3 >= s2 and s3 >= s1:
            s3 = max(s3 - _step_c, Decimal("100")).quantize(Decimal("1"))
        elif s2 >= s1:
            s2 = max(s2 - _step_c, Decimal("100")).quantize(Decimal("1"))
        else:
            s1 = max(s1 - _step_c, Decimal("100")).quantize(Decimal("1"))
    if _clamp_iters > 0:
        log.info(f"[BudgetClamp] clamped {_clamp_iters}x: total {float(_actual_total):.0f} → {float(s1+s2+(s3 or Decimal('0'))):.0f} (target {float(_target_total):.0f})")


    if is_3way:
        # B10/D3: ใช้ live odds (after slippage) สำหรับ payout display
        _leg1_disp = live1 if 'live1' in locals() else opp.leg1.odds
        _leg2_disp = live2 if 'live2' in locals() else opp.leg2.odds
        _leg3_disp = live3 if 'live3' in locals() else opp.leg3.odds
        w1 = (s1 * _leg1_disp).quantize(Decimal("1"))
        w2 = (s2 * _leg2_disp).quantize(Decimal("1"))
        w3 = (s3 * _leg3_disp).quantize(Decimal("1"))
        tt = s1 + s2 + s3
        # D3/N2: post-rounding profitability guard for 3-way
        rounded_profit_3 = (min(w1, w2, w3) - tt) / tt if tt > 0 else Decimal("0")
        if rounded_profit_3 < MIN_PROFIT_PCT:
            _rp3_reason = "ติดลบ" if rounded_profit_3 < 0 else f"ต่ำกว่า threshold {float(MIN_PROFIT_PCT):.1%}"
            log.warning(f"[ProfitGuard-3way] ABORT {opp.event} — 3-way profit {float(rounded_profit_3):.2%} after rounding ({_rp3_reason})")
            raise ValueError(
                f"🚫 *Abort: 3-way profit ต่ำกว่า threshold หลัง rounding* ({float(rounded_profit_3):.2%} < {float(MIN_PROFIT_PCT):.1%})\n"
                f"edge บางเกินไปสำหรับทุนนี้ — รอ signal ใหม่"
            )
    else:
        # C5/C13: ใช้ live odds (after slippage) แทน odds_raw สำหรับ guard + payout display
        _od1 = live1 if 'live1' in locals() else opp.leg1.odds
        _od2 = live2 if 'live2' in locals() else opp.leg2.odds
        # v10-3: Profitability Guard — rebalance s2 ถ้า rounding ทำให้ arb หาย
        w1 = (s1 * _od1).quantize(Decimal("1"))
        w2 = (s2 * _od2).quantize(Decimal("1"))
        tt = s1 + s2
        rounded_profit = (min(w1, w2) - tt) / tt if tt > 0 else Decimal("0")
        if rounded_profit < Decimal("0"):
            # rebalance: หา s2 ที่ทำให้ w2 >= w1 (worst-case break-even)
            s2_rebalanced = (w1 / _od2).quantize(Decimal("1"), rounding=ROUND_DOWN) + 1
            # H4: re-apply cap หลัง rebalance — s2_rebalanced อาจเกิน cap
            s2_rebalanced = (apply_max_stake(s2_rebalanced/USD_TO_THB, opp.leg2.bookmaker)*USD_TO_THB).quantize(Decimal("1"))
            # D2: ใช้ _od2 ตลอด — ไม่ย้อนกลับไป odds_raw
            rebalanced_profit = (min(w1, (s2_rebalanced * _od2).quantize(Decimal("1"))) - (s1 + s2_rebalanced)) / (s1 + s2_rebalanced)
            if rebalanced_profit >= Decimal("0"):
                s2 = s2_rebalanced
                log.info(f"[ProfitGuard] rebalanced s2: {int(s2_raw)} -> {int(s2)} | profit: {float(rounded_profit):.3%} -> {float(rebalanced_profit):.3%}")
            else:
                log.warning(f"[ProfitGuard] ABORT {opp.event} — arb lost after rounding (profit={float(rounded_profit):.3%})")
                raise ValueError(
                    f"Abort: arb profit ติดลบหลัง natural rounding ({float(rounded_profit):.2%})\n"
                    f"ทุนน้อยเกินไปสำหรับ edge นี้ — รอ signal ใหม่ที่ profit สูงกว่า"
                )
        # D2: ลบ w1/w2 รอบสอง (odds_raw) ออก — ใช้ w1/w2 จากรอบแรก (live odds) ต่อไป
        # recalc หลัง rebalance ถ้า s2 เปลี่ยน
        w1 = (s1 * _od1).quantize(Decimal("1"))
        w2 = (s2 * _od2).quantize(Decimal("1"))
        tt = s1 + s2
        # N2: final threshold check หลัง rebalance
        _final_profit_2way = (min(w1, w2) - tt) / tt if tt > 0 else Decimal("0")
        if _final_profit_2way < MIN_PROFIT_PCT:
            _fp2_reason = "ติดลบ" if _final_profit_2way < 0 else f"ต่ำกว่า threshold {float(MIN_PROFIT_PCT):.1%}"
            log.warning(f"[ProfitGuard-2way] ABORT {opp.event} — final profit {float(_final_profit_2way):.2%} after rebalance ({_fp2_reason})")
            raise ValueError(
                f"🚫 *Abort: profit ต่ำกว่า threshold หลัง rounding/rebalance* ({float(_final_profit_2way):.2%} < {float(MIN_PROFIT_PCT):.1%})\n"
                f"รอ signal ใหม่ที่ profit สูงกว่า"
            )
        w3 = None

    # D4: บันทึก live odds ที่ใช้จริง (after slippage) แทน odds_raw
    _save_od1 = float(_leg1_disp if is_3way else _od1) if is_3way or '_od1' in locals() else float(opp.leg1.odds)
    _save_od2 = float(_leg2_disp if is_3way else _od2) if is_3way or '_od2' in locals() else float(opp.leg2.odds)
    # E8: fallback ใช้ opp.leg3.odds (after slippage) แทน odds_raw
    _save_od3 = float(_leg3_disp) if is_3way and '_leg3_disp' in locals() else (float(opp.leg3.odds) if is_3way else None)
    tr = TradeRecord(
        signal_id=opp.signal_id, event=opp.event, sport=opp.sport,
        leg1_bm=opp.leg1.bookmaker, leg2_bm=opp.leg2.bookmaker,
        leg1_team=opp.leg1.outcome,
        leg2_team=opp.leg2.outcome,
        leg1_odds=_save_od1, leg2_odds=_save_od2,
        stake1_thb=int(s1), stake2_thb=int(s2),
        profit_pct=float(live_profit), status="confirmed",
        commence_time=opp.commence,
        leg3_bm=opp.leg3.bookmaker if is_3way else None,
        leg3_team=opp.leg3.outcome if is_3way else None,
        leg3_odds=_save_od3,
        stake3_thb=int(s3) if is_3way else None,
    )
    with _data_lock:
        trade_records.append(tr)
    db_save_trade(tr)            # save to DB
    register_for_settlement(tr, opp.commence)  # auto settle
    register_closing_watch(opp)               # #39 CLV watch ตอนเจอ opp ใหม่
    # D1: opportunity_log update ทำใน button_handler หลัง execute สำเร็จ (ไม่ทำซ้ำที่นี่)
    db_update_opp_status(opp.signal_id, "confirmed")  # save to DB

    sp = sport_to_path(opp.sport)
    # E3: steps() รับ display_odds เพื่อโชว์ live odds แทน odds_raw
    def steps(leg, stake, display_odds=None):
        bm  = leg.bookmaker.lower()
        eid = leg.raw.get("event_id","")
        bk  = leg.raw.get("bm_key", bm)
        cap = apply_max_stake(stake/USD_TO_THB, leg.bookmaker)*USD_TO_THB
        cap_note = f"\n  ⚠️ Capped ที่ ฿{int(cap):,}" if cap < stake else ""
        show_odds = display_odds if display_odds is not None else leg.odds_raw
        if "kalshi" in bm:
            link = leg.market_url or "https://kalshi.com"
            usd_amt = round(stake / float(USD_TO_THB), 2)
            return f"  🔗 [เปิด Kalshi]({link})\n  2. เลือก *{leg.outcome}*\n  3. วาง *${usd_amt}* (≈฿{int(stake)}){cap_note}"
        elif "polymarket" in bm:
            link = leg.market_url or "https://polymarket.com"
            usdc_amt = round(stake / float(USD_TO_THB), 2)
            return f"  🔗 [เปิด Polymarket]({link})\n  2. เลือก *{leg.outcome}*\n  3. วาง *{usdc_amt} USDC* (≈฿{int(stake)}){cap_note}"
        elif "pinnacle" in bk:
            path = sp["pinnacle"]
            link = f"https://www.pinnacle.com/en/{path}/matchup/{eid}" if eid else f"https://www.pinnacle.com/en/{path}"
            return f"  🔗 [เปิด Pinnacle]({link})\n  2. เลือก *{leg.outcome}* @ {show_odds}\n  3. วาง ฿{int(stake)}{cap_note}"
        elif "onexbet" in bk or "1xbet" in bm:
            path = sp["1xbet"]
            link = f"https://1xbet.com/en/line/{path}/{eid}" if eid else f"https://1xbet.com/en/line/{path}"
            return f"  🔗 [เปิด 1xBet]({link})\n  2. เลือก *{leg.outcome}* @ {show_odds}\n  3. วาง ฿{int(stake)}{cap_note}"
        elif "dafabet" in bk:
            path = sp["dafabet"]
            return f"  🔗 [เปิด Dafabet](https://www.dafabet.com/en/{path})\n  2. ค้นหา *{leg.outcome}*\n  3. วาง ฿{int(stake)}{cap_note}"
        return f"  1. เปิด {leg.bookmaker}\n  2. เลือก *{leg.outcome}* @ {show_odds}\n  3. วาง ฿{int(stake)}{cap_note}"

    if is_3way:
        # E3: ส่ง live odds เข้า steps()
        return (
            f"📋 *วางเงิน 3-WAY — {md_escape(opp.event)}*{slippage_warn}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏠 *Home — {md_escape(opp.leg1.bookmaker)}*\n{steps(opp.leg1, s1, _leg1_disp)}\n\n"
            f"⚪ Draw — *{md_escape(opp.leg3.bookmaker)}*\n{steps(opp.leg3, s3, _leg3_disp)}\n\n"
            f"🟠 *Away — {md_escape(opp.leg2.bookmaker)}*\n{steps(opp.leg2, s2, _leg2_disp)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💵 ทุน ฿{int(tt):,}  _(คาด profit: {float(opp.profit_pct):.2%})_\n"
            f"   {md_escape(opp.leg1.outcome)} ชนะ → ฿{int(w1):,}\n"
            f"   Draw → ฿{int(w3):,}\n"
            f"   {md_escape(opp.leg2.outcome)} ชนะ → ฿{int(w2):,}"
        )
    # E3: 2-way — ส่ง _od1/_od2 เข้า steps()
    return (
        f"📋 *วางเงิน — {md_escape(opp.event)}*{slippage_warn}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔵 *{md_escape(opp.leg1.bookmaker)}*\n{steps(opp.leg1, s1, _od1)}\n\n"
        f"🟠 *{md_escape(opp.leg2.bookmaker)}*\n{steps(opp.leg2, s2, _od2)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 ทุน ฿{int(tt):,}  _(Live profit: {float(live_profit):.2%})_\n"
        f"   {md_escape(str(opp.leg1.outcome))} ชนะ → ฿{int(w1):,} (+฿{int(w1-tt):,})\n"
        f"   {md_escape(str(opp.leg2.outcome))} ชนะ → ฿{int(w2):,} (+฿{int(w2-tt):,})"
    )


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════
def is_owner(update: Update) -> bool:
    """C3: ตรวจว่า user เป็น owner (ตาม OWNER_USER_ID)"""
    owner = os.getenv("OWNER_USER_ID", "").strip()
    if not owner:
        return True  # ไม่ได้ตั้ง = ไม่บังคับ
    user = update.effective_user
    return bool(user and str(user.id) == owner)

async def require_owner(update: Update) -> bool:
    """C3: Guard — คืน False และแจ้ง ⛔ ถ้าไม่ใช่ owner"""
    if not is_owner(update):
        if update.message:
            await update.message.reply_text("⛔ Not authorized")
        return False
    return True

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # v10-6: เช็ค user_id โดยตรง — ถ้าไม่ได้ตั้ง OWNER_USER_ID ให้ผ่านได้ (ไม่ fallback CHAT_ID)
    _owner_id = os.getenv("OWNER_USER_ID", "").strip()
    if _owner_id and str(query.from_user.id) != _owner_id:
        await query.answer("⛔ Not authorized", show_alert=True)
        return
    try: action, sid = query.data.split(":",1)
    except Exception: return

    SIGNAL_TTL = SIGNAL_TTL_SEC  # F7: ใช้ constant แทน os.getenv
    orig = query.message.text

    # ── Value Bet confirm/reject ──────────────────────────────────
    if action in ("vb_confirm", "vb_reject"):
        with _data_lock:
            vb_entry = _pending_vb.get(sid)
        if not vb_entry:
            try: await query.edit_message_text(orig+"\n\n⚠️ Value Bet signal หมดอายุแล้ว")
            except Exception: pass
            return
        vb, vb_ts = vb_entry
        if time.time() - vb_ts > SIGNAL_TTL:
            with _data_lock:
                _pending_vb.pop(sid, None)
            try: await query.edit_message_text(orig+f"\n\n⏰ *Signal expired* ({int((time.time()-vb_ts)//60)}m ago)", parse_mode="Markdown")
            except Exception: pass
            return
        # J6: kick-off guard — ไม่ confirm VB หลังแม็ตช์เริ่ม
        if vb.commence_time:
            try:
                _vb_cdt = parse_commence(vb.commence_time)
                if datetime.now(timezone.utc) > _vb_cdt + timedelta(minutes=5):
                    with _data_lock:
                        _pending_vb.pop(sid, None)
                    try: await query.edit_message_text(orig+"\n\n⏰ *แม็ตช์เริ่มแล้ว — signal หมดอายุ*", parse_mode="Markdown")
                    except Exception: pass
                    return
            except Exception:
                pass
        if action == "vb_reject":
            with _data_lock:
                _pending_vb.pop(sid, None)
            try: await query.edit_message_text(orig+"\n\n❌ *Value Bet Skipped*", parse_mode="Markdown")
            except Exception: pass
            return
        # B2: VB slippage guard — refetch live odds ก่อน confirm
        try: await query.edit_message_text(orig+"\n\n⏳ *ตรวจราคาล่าสุด...*", parse_mode="Markdown")
        except Exception: pass
        live_soft_odds, _vb_found = await refetch_valuebet_odds(vb)
        _, live_edge = calc_valuebet_kelly(vb.true_odds, live_soft_odds, vb.grade)
        _retry_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Retry",   callback_data=f"vb_confirm:{sid}"),
            InlineKeyboardButton("❌ Skip",    callback_data=f"vb_reject:{sid}"),
        ]])
        # O1: fail-closed — abort ถ้าหา live odds ไม่เจอ
        if not _vb_found:
            try:
                await query.edit_message_text(
                    orig + (
                        f"\n\n🚫 *ABORT: Live odds unavailable*\n"
                        f"ดึงราคา {vb.bookmaker} live ไม่ได้ (market อาจถูก suspend)\n"
                        f"_(กด Retry ถ้าจะลองใหม่)_"
                    ),
                    parse_mode="Markdown", reply_markup=_retry_kb
                )
            except Exception: pass
            return
        if live_soft_odds < vb.soft_odds * 0.98 or live_edge <= 0:  # tolerance 2%
            # K4: ไม่ pop — ยังเก็บ signal ไว้ให้ retry ได้ (one-shot abort ไม่ pop)
            try:
                await query.edit_message_text(
                    orig + (
                        f"\n\n🚫 *ABORT: Value เปลี่ยนแล้ว*\n"
                        f"คาด `{vb.soft_odds:.3f}` → จริง `{live_soft_odds:.3f}`\n"
                        f"edge ใหม่ = {live_edge:.2f}%\n"
                        f"_(กด Retry ถ้าจะลองใหม่)_"
                    ),
                    parse_mode="Markdown",
                    reply_markup=_retry_kb,
                )
            except Exception: pass
            return
        # E2: ใช้ executed odds/edge ที่ผ่าน guard แล้ว — ไม่ย้อนไป vb.soft_odds
        executed_soft_odds = live_soft_odds
        executed_edge_pct  = live_edge
        # recalc stake ตาม live edge
        # F1: return order คือ (stake, edge) — ไม่ใช่ (edge, stake)
        executed_stake_dec, executed_edge_recalc = calc_valuebet_kelly(vb.true_odds, executed_soft_odds, vb.grade)
        _raw_stake = Decimal(str(int(executed_stake_dec))) if executed_stake_dec else Decimal(str(int(vb.rec_stake_thb)))
        # G5: apply per-book cap (MAX_STAKE_* env) ก่อนส่ง Telegram
        executed_stake_thb = int(apply_vb_book_cap(_raw_stake, vb.bookmaker))
        # vb_confirm — แจ้ง Telegram พร้อม step-by-step
        sp = sport_to_path(vb.sport)
        bm_lower = vb.bookmaker.lower()
        if "pinnacle" in bm_lower:
            link = f"https://www.pinnacle.com/en/{sp['pinnacle']}"
        elif "1xbet" in bm_lower or "onexbet" in bm_lower:
            link = f"https://1xbet.com/en/line/{sp['1xbet']}"
        elif "dafabet" in bm_lower:
            link = f"https://www.dafabet.com/en/{sp['dafabet']}"
        # J4: stake.com / cloudbet — เพิ่ม mapping แทน fallback Pinnacle
        elif "stake" in bm_lower:
            link = f"https://stake.com/sports/{sp.get('stake', sp.get('pinnacle', 'soccer'))}"
        elif "cloudbet" in bm_lower:
            link = f"https://www.cloudbet.com/en/sports/{sp.get('cloudbet', sp.get('pinnacle', 'soccer'))}"
        else:
            link = f"https://www.pinnacle.com/en/{sp['pinnacle']}"
        confirm_msg = (
            f"✅ *Value Bet CONFIRMED — เกรด {vb.grade}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🏆 {md_escape(vb.event)}\n"
            f"📡 {md_escape(vb.bookmaker)} — *{md_escape(vb.outcome)}*\n"
            f"💰 Edge: *+{executed_edge_pct:.2f}%* | Stake: *฿{executed_stake_thb:,}* @ `{executed_soft_odds:.3f}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 *วิธีวาง:*\n"
            f"  1. [เปิด {md_escape(vb.bookmaker)}]({link})\n"
            f"  2. ค้นหา *{md_escape(vb.event)}*\n"
            f"  3. เลือก *{md_escape(vb.outcome)}* @ `{executed_soft_odds:.3f}`\n"
            f"  4. วาง *฿{executed_stake_thb:,}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"_(ยืนยันแล้ว — วางด้วยมือ)_"
        )
        # K4: pop เมื่อ confirm สำเร็จแล้ว
        with _data_lock:
            _pending_vb.pop(sid, None)
        try: await query.edit_message_text(orig+f"\n\n✅ *Value Bet Confirmed* ฿{executed_stake_thb:,}", parse_mode="Markdown")
        except Exception: pass
        for cid in ALL_CHAT_IDS:
            try:
                await _app.bot.send_message(chat_id=cid, text=confirm_msg, parse_mode="Markdown")
            except Exception as e:
                log.error(f"[VB] confirm msg error: {e}")
        # E2: บันทึก TradeRecord ด้วย executed odds/edge (ไม่ใช้ vb.soft_odds เดิม)
        tr_vb = TradeRecord(
            signal_id=vb.signal_id, event=vb.event, sport=vb.sport,
            leg1_bm=vb.bookmaker, leg2_bm="-",
            leg1_team=vb.outcome, leg2_team="-",
            leg1_odds=float(executed_soft_odds), leg2_odds=0.0,
            stake1_thb=executed_stake_thb, stake2_thb=0,
            profit_pct=float(executed_edge_pct / 100.0), status="confirmed",
            commence_time=vb.commence_time,
        )
        with _data_lock:
            trade_records.append(tr_vb)
        db_save_trade(tr_vb)
        if vb.commence_time:
            register_for_settlement(tr_vb, vb.commence_time)
            # E3: CLV watch for VB trades — adapt TradeRecord to ArbOpportunity interface
            from types import SimpleNamespace as _NS
            register_closing_watch(_NS(event=tr_vb.event, sport=tr_vb.sport, commence=tr_vb.commence_time))
        return

    # ── Arb confirm/reject ────────────────────────────────────────
    with _data_lock:
        entry = pending.get(sid)
    if not entry:
        try: await query.edit_message_text(orig+"\n\n⚠️ หมดอายุ")
        except Exception: pass
        return
    opp, sig_ts = entry
    if time.time() - sig_ts > SIGNAL_TTL:
        with _data_lock:
            pending.pop(sid, None)
        try: await query.edit_message_text(orig+f"\n\n⏰ *Signal expired* ({int((time.time()-sig_ts)//60)}m ago)", parse_mode="Markdown")
        except Exception: pass
        return
    if action == "reject":  # D1: set rejected immediately; confirmed only after execute_both succeeds
        with _data_lock:
            pending.pop(sid, None)  # B2: pop ตอน reject จริง
        _is_3w = opp.leg3 is not None and opp.stake3 is not None
        tr_rej = TradeRecord(
            signal_id=sid, event=opp.event, sport=opp.sport,
            leg1_bm=opp.leg1.bookmaker, leg2_bm=opp.leg2.bookmaker,
            leg1_team=opp.leg1.outcome,
            leg2_team=opp.leg2.outcome,
            leg1_odds=float(opp.leg1.odds_raw), leg2_odds=float(opp.leg2.odds_raw),
            stake1_thb=int(opp.stake1*USD_TO_THB), stake2_thb=int(opp.stake2*USD_TO_THB),
            profit_pct=float(opp.profit_pct), status="rejected",
            leg3_bm=opp.leg3.bookmaker if _is_3w else None,
            leg3_team=opp.leg3.outcome if _is_3w else None,
            leg3_odds=float(opp.leg3.odds_raw) if _is_3w else None,
            stake3_thb=int(opp.stake3*USD_TO_THB) if _is_3w else None,
        )
        with _data_lock:
            trade_records.append(tr_rej)
        db_save_trade(tr_rej)    # 
        with _data_lock:  # D1: update opp_log to rejected
            for _e in opportunity_log:
                if _e["id"] == sid:
                    _e["status"] = "rejected"; break
        db_update_opp_status(sid, "rejected")  # 
        try: await query.edit_message_text(orig+"\n\n❌ *REJECTED*", parse_mode="Markdown")
        except Exception: pass  # C8
        return
    try: await query.edit_message_text(orig+"\n\n⏳ *กำลังตรวจราคาล่าสุด...*", parse_mode="Markdown")
    except Exception: pass
    try:
        result = await execute_both(opp)
        with _data_lock:
            pending.pop(sid, None)  # B2: pop หลัง execute สำเร็จเท่านั้น
            # D1: set confirmed ใน opp_log หลัง execute สำเร็จเท่านั้น
            for _e in opportunity_log:
                if _e["id"] == sid:
                    _e["status"] = "confirmed"; break
        try: await query.edit_message_text(orig+"\n\n✅ *CONFIRMED*\n\n"+result, parse_mode="Markdown")
        except Exception: pass  # C8
    except ValueError as abort_msg:
        # B2: ไม่ pop pending ถ้า execute abort — user ยังกดอีกครั้งได้
        # K2: ส่ง reply_markup ปุ่ม Confirm กลับไปพร้อม abort message เพื่อให้กดได้
        _abort_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Confirm อีกครั้ง", callback_data=f"confirm:{sid}"),
            InlineKeyboardButton("❌ Reject",           callback_data=f"reject:{sid}"),
        ]])
        try:
            await query.edit_message_text(
                orig+"\n\n"+str(abort_msg)+"\n\n_กด Confirm อีกครั้งได้_",
                parse_mode="Markdown",
                reply_markup=_abort_kb,
            )
        except Exception: pass
    except Exception as e:  # C4: generic catch — network/parse/decimal errors
        log.error(f"[Confirm] execute_both failed: {e}", exc_info=True)
        # L3: ส่ง reply_markup กลับไปเหมือน abort path — ให้ user กด retry ได้
        _err_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Retry",   callback_data=f"confirm:{sid}"),
            InlineKeyboardButton("❌ Reject",   callback_data=f"reject:{sid}"),
        ]])
        try:
            await query.edit_message_text(
                orig + "\n\n❌ *Execution failed*\nระบบขัดข้องระหว่างยืนยัน กรุณาลองใหม่",
                parse_mode="Markdown",
                reply_markup=_err_kb,
            )
        except Exception: pass


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update): return  # C3
    global auto_scan, quota_warned
    args = context.args
    if not args:
        s = "🟢" if auto_scan else "🔴"
        await update.message.reply_text(f"Auto scan: {s}\n/scan on หรือ /scan off")
        return
    if args[0].lower()=="on":
        auto_scan=True; quota_warned=False
        with _data_lock:
            seen_signals.clear()  # reset TTL timers เมื่อเปิด scan ใหม่
            # B11: clear เฉพาะ expired VB signals — ไม่ลบที่ยังไม่หมดอายุ
            _vb_ttl_now = SIGNAL_TTL_SEC  # F7
            _now_vb = time.time()
            _expired_vb = [k for k, (_, ts) in _pending_vb.items() if _now_vb - ts > _vb_ttl_now]
            for k in _expired_vb:
                del _pending_vb[k]
        await update.message.reply_text(f"🟢 *Auto scan เปิด* — ทุก {SCAN_INTERVAL}s", parse_mode="Markdown")
    elif args[0].lower()=="off":
        auto_scan=False
        await update.message.reply_text("🔴 *Auto scan ปิด*", parse_mode="Markdown")


async def cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """4. /pnl — ดู P&L summary"""
    if not await require_owner(update): return  # C3
    with _data_lock:  # v10-12
        confirmed = [t for t in trade_records if t.status=="confirmed"]
        rejected  = [t for t in trade_records if t.status=="rejected"]

    # I1/I3: separate arb vs VB; est_profit = unsettled arb only (VB edge != guaranteed profit)
    def _is_vb(t): return t.stake2_thb == 0 and t.leg2_team == "-"
    arb_confirmed = [t for t in confirmed if not _is_vb(t)]
    vb_confirmed  = [t for t in confirmed if _is_vb(t)]
    settled       = [t for t in confirmed if t.actual_profit_thb is not None]
    unsettled_arb = [t for t in arb_confirmed if t.actual_profit_thb is None]
    actual_profit = sum(t.actual_profit_thb for t in settled)
    # I3: est_profit = unsettled arb only
    est_profit    = sum(t.profit_pct * (t.stake1_thb + t.stake2_thb + (t.stake3_thb or 0))
                        for t in unsettled_arb)
    # VB expected value (edge * stake) — shown separately
    vb_ev         = sum(t.profit_pct * t.stake1_thb for t in vb_confirmed
                        if t.actual_profit_thb is None)

    win_trades = [t for t in settled if t.actual_profit_thb >= 0]
    lose_trades= [t for t in settled if t.actual_profit_thb < 0]
    win_rate   = len(win_trades)/len(settled)*100 if settled else 0
    confirm_rate = len(confirmed)/(len(confirmed)+len(rejected))*100 if (confirmed or rejected) else None
    confirm_str  = f"{confirm_rate:.0f}%" if confirm_rate is not None else "N/A"

    # CLV summary
    clv_values = []
    for t in confirmed:
        c1, c2, c3 = calc_clv(t)
        if c1 is not None: clv_values.append(c1)
        if c2 is not None: clv_values.append(c2)
        if c3 is not None: clv_values.append(c3)
    avg_clv = sum(clv_values)/len(clv_values) if clv_values else None
    clv_str = f"{avg_clv:+.2f}%" if avg_clv is not None else "ยังไม่มีข้อมูล"

    vb_line = (f"\n🎯 VB EV         : ฿{int(vb_ev):+,} _({len(vb_confirmed)} VB, edge-based)_"
               if vb_confirmed else "")
    await update.message.reply_text(
        f"💰 *P&L Summary*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Confirmed   : {len(confirmed)} ({len(arb_confirmed)} arb + {len(vb_confirmed)} VB)\n"
        f"  └ Confirm rate : {confirm_str} ({len(confirmed)} / {len(confirmed)+len(rejected)})\n"
        f"  └ Settled : {len(settled)} | Unsettled arb: {len(unsettled_arb)}\n"
        f"  └ *Settled Win Rate*: {win_rate:.0f}% ({len(win_trades)}W/{len(lose_trades)}L)\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Actual P&L  : *฿{actual_profit:+,}*\n"
        f"📊 Arb Est.    : ฿{int(est_profit):,} _(unsettled arb only)_"
        f"{vb_line}\n"
        f"📈 CLV avg     : {clv_str}\n"
        f"_(CLV บวก = เอาชนะตลาด)_",
        parse_mode="Markdown",
    )


async def cmd_lines(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """7. /lines — ดู line movements ล่าสุด"""
    if not await require_owner(update): return  # C3
    with _data_lock:  # v10-12
        recent = list(line_movements[-10:])[::-1]
    if not recent:
        await update.message.reply_text("ยังไม่มี line movement ที่น่าสนใจ")
        return
    lines_text = ""
    for lm in recent:
        tags = ""
        if lm.is_steam: tags += "🌊"
        if lm.is_rlm:   tags += "🔄"
        pct = f"{lm.pct_change:+.1%}"
        lines_text += f"{tags} {md_escape(lm.event[:25])} {md_escape(lm.bookmaker)} {pct}\n"
    await update.message.reply_text(
        f"📊 *Line Movements ล่าสุด*\n━━━━━━━━━━━━━━━━━━\n{lines_text}\n"
        f"🌊=Steam 🔄=RLM",
        parse_mode="Markdown",
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update): return  # C3
    s = "🟢 เปิด" if auto_scan else "🔴 ปิด"
    qpct = min(100, int(api_remaining/5))
    qbar = "█"*int(qpct/5)+"░"*(20-int(qpct/5))
    with _data_lock:  # v10-12 — Fix 4: also snapshot lens under lock
        confirmed    = len([t for t in trade_records if t.status=="confirmed"])
        settled      = len([t for t in trade_records if t.status=="confirmed" and t.actual_profit_thb is not None])
        actual_pnl   = sum(t.actual_profit_thb for t in trade_records
                           if t.status=="confirmed" and t.actual_profit_thb is not None)
        _pending_ct  = len(pending)
        _unsettled_ct = len(_pending_settlement)
        _lm_ct       = len(line_movements)
    # C13: rotation info
    rotation_size = int(os.getenv("SPORT_ROTATION_SIZE", "0"))
    if rotation_size > 0 and len(SPORTS) > rotation_size:
        rot_str = f"Rotation    : {_sport_rotation_idx}/{len(SPORTS)} (batch {rotation_size})\n"
    else:
        rot_str = f"Sports      : {len(SPORTS)} (all)\n"
    await update.message.reply_text(
        f"📊 *Deminia Bot V.2*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Auto scan   : {s} ({SCAN_INTERVAL}s)\n"
        f"สแกนไปแล้ว  : {scan_count} รอบ\n"
        f"ล่าสุด      : {last_scan_time}\n"
        f"{rot_str}"
        f"รอ confirm  : {_pending_ct} | trade: {confirmed} | unsettled: {_unsettled_ct}\n"
        f"Settled P&L : ฿{actual_pnl:+,} ({settled} เกม)\n"
        f"Line moves  : {_lm_ct} events\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Min profit  : {MIN_PROFIT_PCT:.1%} | Max odds: {MAX_ODDS_ALLOWED}\n"
        f"ทุน/trade   : ฿{int(TOTAL_STAKE_THB):,} | Bankroll: ฿{int(get_current_bankroll()):,}\n"
        f"Kelly       : {'✅ ON' if USE_KELLY else '❌ OFF'} (f={KELLY_FRACTION})\n"
        f"Cooldown    : {ALERT_COOLDOWN_MIN}m | Staleness: {MAX_ODDS_AGE_MIN}m\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Credits: *{api_remaining}*/500\n"
        f"[{qbar}]\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"/scan on·off | /now | /pnl | /lines",
        parse_mode="Markdown",
    )


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update): return  # C3
    global _now_last_ts
    # C1: ใช้ _scan_lock.locked() แทน dead _scan_in_progress flag
    if _scan_lock and _scan_lock.locked():
        await update.message.reply_text("⏳ *กำลัง scan อยู่แล้ว* — รอสักครู่", parse_mode="Markdown")
        return
    # A5: Rate limit /now — ไม่ให้กดซ้ำภายใน NOW_COOLDOWN_SEC
    NOW_COOLDOWN = int(os.getenv("NOW_COOLDOWN_SEC", "60"))
    elapsed = time.time() - _now_last_ts
    if elapsed < NOW_COOLDOWN:
        remaining = int(NOW_COOLDOWN - elapsed)
        await update.message.reply_text(
            f"⏳ รออีก *{remaining}s* ก่อนใช้ /now อีกครั้ง", parse_mode="Markdown"
        )
        return
    _now_last_ts = time.time()
    await update.message.reply_text("🔍 *กำลังสแกน...*", parse_mode="Markdown")
    count = await do_scan()
    msg = f"✅ พบ *{count}* opportunity" if count else f"✅ ไม่พบ > {MIN_PROFIT_PCT:.1%}"
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """v10-11: /trades — แสดง trade list รายละเอียด 10 รายการล่าสุด"""
    if not await require_owner(update): return  # C3
    with _data_lock:
        recent = [t for t in trade_records if t.status == "confirmed"][-10:][::-1]
    if not recent:
        await update.message.reply_text("ยังไม่มี confirmed trade")
        return
    lines = []
    for i, t in enumerate(recent, 1):
        settled = f"✅ ฿{t.actual_profit_thb:+,}" if t.actual_profit_thb is not None else "⏳ รอผล"
        ct_th = ""
        if t.commence_time:
            try:
                _ct = parse_commence(t.commence_time)
                ct_th = (_ct + timedelta(hours=7)).strftime("%d/%m %H:%M")
            except Exception:
                pass
        is_vb = (not t.stake2_thb) and t.leg2_team == "-"  # P6: handle None from old schema
        is_3w = t.stake3_thb is not None and t.stake3_thb > 0
        if is_vb:
            stake_str = f"฿{t.stake1_thb:,} (ValueBet)"
        elif is_3w:
            stake_str = f"฿{t.stake1_thb:,}+฿{t.stake2_thb:,}+฿{t.stake3_thb:,} (3-way)"
        else:
            stake_str = f"฿{t.stake1_thb:,}+฿{t.stake2_thb:,}"
        lines.append(
            f"{i}. {md_escape(t.event[:28])}\n"
            f"   {SPORT_EMOJI.get(t.sport,'🏆')} {md_escape(t.leg1_bm)} vs {md_escape(t.leg2_bm)} | profit {t.profit_pct:.1%}\n"
            f"   {stake_str} | {ct_th} | {settled}"
        )
    await update.message.reply_text(
        f"📋 *Confirmed Trades ({len(recent)} ล่าสุด)*\n━━━━━━━━━━━━━━━━━━\n" + "\n\n".join(lines),
        parse_mode="Markdown",
    )


async def cmd_settle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """v10-10: /settle <signal_id> <leg1|leg2|draw|void>
    Manual settle สำหรับ MANUAL_REVIEW หรือ DRAW
    ตัวอย่าง: /settle abc12345 leg1
    """
    if not await require_owner(update): return  # C3
    args = context.args
    if len(args) < 2:
        # Fix 5: snapshot _pending_settlement before reading
        with _data_lock:
            ps_snap = dict(_pending_settlement)
        if not ps_snap:
            await update.message.reply_text("ไม่มี trade ที่รอ settle")
            return
        lines = []
        for sid, (t, dt) in list(ps_snap.items())[:40]:  # limit 40 เพื่อไม่เกิน Telegram 4096 chars
            dt_th = (dt + timedelta(hours=7)).strftime("%d/%m %H:%M") if dt else "?"
            lines.append(f"`{sid}` — {md_escape(t.event[:30])} ({dt_th})")
        await update.message.reply_text(
            f"⏳ *Trade รอ settle* ({len(ps_snap)} รายการ)\n"
            + "\n".join(lines)
            + "\n\nใช้: `/settle <signal_id> <leg1|leg2|draw|void>`",
            parse_mode="Markdown",
        )
        return

    sid    = args[0].strip()
    result = args[1].strip().lower()
    if result not in ("leg1", "leg2", "draw", "void"):
        await update.message.reply_text("result ต้องเป็น: leg1 / leg2 / draw / void")
        return

    # K1: get-validate-pop — peek first, validate, then pop on commit
    with _data_lock:
        entry = _pending_settlement.get(sid) or _manual_review_pending.get(sid)
    if not entry:
        await update.message.reply_text(f"ไม่พบ signal_id `{sid}` ใน pending settlement", parse_mode="Markdown")
        return

    t, _ = entry
    # J2: guard status — only confirmed trades can be settled
    if t.status != "confirmed":
        await update.message.reply_text(
            f"⚠️ Trade `{sid}` status=`{t.status}` — settle ได้เฉพาะ confirmed trades",
            parse_mode="Markdown"
        )
        return
    # C8: double-settle guard
    if t.actual_profit_thb is not None or t.settled_at is not None:
        await update.message.reply_text(
            f"⚠️ Trade นี้ settle ไปแล้ว (P&L: ฿{t.actual_profit_thb:+,})",
            parse_mode="Markdown"
        )
        return
    # K1: validation passed — now pop from whichever queue holds it
    with _data_lock:
        _pending_settlement.pop(sid, None)
        _manual_review_pending.pop(sid, None)
    tt = t.stake1_thb + t.stake2_thb + (t.stake3_thb or 0)

    if result == "leg1":
        payout = int(t.leg1_odds * t.stake1_thb)
        actual = payout - tt
    elif result == "leg2":
        payout = int(t.leg2_odds * t.stake2_thb)
        actual = payout - tt
    elif result == "draw":
        # 3-way: leg3 = Draw leg (bookmaker=draw bm, outcome="Draw")
        # fallback: leg1 or leg2 ถ้าชื่อ == "draw"
        if t.leg3_team and t.leg3_team.lower() == "draw" and t.leg3_odds and t.stake3_thb:
            payout = int(t.leg3_odds * t.stake3_thb)
            actual = payout - tt
        elif t.leg1_team and t.leg1_team.lower() == "draw":
            payout = int(t.leg1_odds * t.stake1_thb)
            actual = payout - tt
        elif t.leg2_team and t.leg2_team.lower() == "draw":
            payout = int(t.leg2_odds * t.stake2_thb)
            actual = payout - tt
        else:
            actual = 0  # 2-way sport: refund
    else:  # void
        actual = 0

    t.actual_profit_thb = actual
    t.settled_at = datetime.now(timezone.utc).isoformat()
    t.status = "confirmed"
    # C7: update trade_records in-memory ด้วย เพื่อให้ /pnl เห็นผล settle ทันที
    with _data_lock:
        for idx, rec in enumerate(trade_records):
            if rec.signal_id == t.signal_id:
                trade_records[idx] = t
                break
    db_save_trade(t)

    emoji = "✅" if actual >= 0 else "❌"
    await update.message.reply_text(
        f"{emoji} *Manual Settle*\n`{t.event}`\n"
        f"ผล: *{result.upper()}* | P&L: *฿{actual:+,}*\n"
        f"(settled_at: {t.settled_at[:16]})",
        parse_mode="Markdown",
    )


# ══════════════════════════════════════════════════════════════════
#  SCAN CORE
# ══════════════════════════════════════════════════════════════════
_sport_rotation_idx = 0  # v10-14: pointer สำหรับ rotation

async def do_scan() -> int:
    global scan_count, last_scan_time, _sport_rotation_idx, _scan_in_progress, auto_scan, _last_error
    # B1: asyncio.Lock — กัน race จาก /now + dashboard + scanner_loop พร้อมกัน
    if _scan_lock is None:
        log.warning("[Scan] lock not initialized")
        return 0
    # B2: non-blocking acquire — asyncio is single-threaded so locked()+acquire() is atomic
    # (no other coroutine can run between these two lines without an explicit await)
    if _scan_lock.locked():
        log.debug("[Scan] already running — skip")
        return 0
    await _scan_lock.acquire()
    try:
        # C9: Bankroll guard — หยุด scan ถ้าทุนหมด
        _cur_bankroll = get_current_bankroll()
        if _cur_bankroll < MIN_KELLY_STAKE and auto_scan:
            auto_scan = False
            log.warning(f"[Bankroll] ทุนเหลือ \u0e3f{int(_cur_bankroll):,} < MIN \u0e3f{int(MIN_KELLY_STAKE):,} — หยุด scan")
            if _app:
                asyncio.get_running_loop().create_task(
                    _app.bot.send_message(
                        chat_id=CHAT_ID,
                        text=(
                            f"🚨 *Bankroll Alert*\n"
                            f"ทุนเหลือ *\u0e3f{int(_cur_bankroll):,}* น้อยกว่า MIN \u0e3f{int(MIN_KELLY_STAKE):,}\n"
                            f"❌ Auto scan หยุดอัตโนมัติ — เติมทุนหรือทบทวนระบบ"
                        ),
                        parse_mode="Markdown"
                    )
                )
            return 0

        # A4: Daily Loss Limit
        if MAX_DAILY_LOSS_THB > 0:
            # C7: ใช้ UTC+7 แทน UTC เพื่อ reset ตรงกับวันใช้งานจริง
            today = (datetime.now(timezone.utc) + timedelta(hours=7)).date()
            with _data_lock:
                daily_loss = sum(
                    t.actual_profit_thb for t in trade_records
                    if t.actual_profit_thb is not None
                    and t.settled_at
                    and (_parse_settled_at(t.settled_at) + timedelta(hours=7)).date() == today
                )
            if daily_loss <= -int(MAX_DAILY_LOSS_THB):
                if auto_scan:
                    auto_scan = False
                    log.warning(f"[DailyLoss] ขาดทุนวันนี้ ฿{abs(daily_loss):,} เกิน MAX ฿{int(MAX_DAILY_LOSS_THB):,} — หยุด scan")
                    if _app:
                        asyncio.get_running_loop().create_task(
                            _app.bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"🚨 *Daily Loss Limit*\nขาดทุนวันนี้ *฿{abs(daily_loss):,}* เกินสูงสุด\n❌ Auto scan หยุดอัตโนมัติ",
                                parse_mode="Markdown"
                            )
                        )
                return 0

        # v10-14: Sport Rotation — scan sports เป็นกลุ่มๆ ประหยัด quota
        rotation_size = int(os.getenv("SPORT_ROTATION_SIZE", "0"))
        if rotation_size > 0 and len(SPORTS) > rotation_size:
            batch = SPORTS[_sport_rotation_idx: _sport_rotation_idx + rotation_size]
            if not batch:  # wrap around
                _sport_rotation_idx = 0
                batch = SPORTS[:rotation_size]
            _sport_rotation_idx = (_sport_rotation_idx + rotation_size) % len(SPORTS)
            scan_sports = batch
            log.debug(f"[Rotation] scanning {scan_sports} ({_sport_rotation_idx}/{len(SPORTS)})")
        else:
            scan_sports = SPORTS  # scan ทั้งหมด (default)
        odds_by_sport, poly_markets = await fetch_all_async(scan_sports)

        # B7: await detect_line_movements ไม่ใช้ create_task — ป้องกัน race condition
        await detect_line_movements(odds_by_sport, poly_markets)

        all_opps = scan_all(odds_by_sport, poly_markets)
        sent = 0
        _SEEN_TTL = SEEN_TTL_SEC  # อ่านจาก env SEEN_TTL_SEC (default 4h)
        _now_ts = time.time()
        for opp in sorted(all_opps, key=lambda x: x.profit_pct, reverse=True):
            _mtype = "3way" if (opp.leg3 is not None) else "2way"
            _l3bm  = opp.leg3.bookmaker if opp.leg3 else "-"
            key = f"{_mtype}|{opp.event}|{opp.leg1.bookmaker}|{opp.leg2.bookmaker}|{_l3bm}"
            with _data_lock:
                last_seen = seen_signals.get(key, 0)
                is_new = (_now_ts - last_seen) > _SEEN_TTL
                if is_new:
                    seen_signals[key] = _now_ts
            if is_new:
                await send_alert(opp)
                await asyncio.sleep(1)
                sent += 1
        with _data_lock:
            # prune expired entries เพื่อไม่ให้ dict โต
            expired = [k for k, ts in seen_signals.items() if (_now_ts - ts) > _SEEN_TTL]
            for k in expired:
                del seen_signals[k]
        scan_count    += 1
        last_scan_time = datetime.now(timezone.utc).strftime("%d/%m %H:%M UTC")
        _last_error = ""  # F4: เคลียร์ error เก่าหลัง scan สำเร็จ
        save_snapshot()   # 💾 บันทึก state
        return sent
    except Exception as e:
        _last_error = f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {e!r}"
        log.error(f"[Scan] error: {e}", exc_info=True)
        return 0
    finally:
        _scan_lock.release()  # B2: always release manually-acquired lock


# track events ที่รอดึง closing line
_closing_line_watch: dict[str, dict] = {}  # event_key → {sport, commence_dt, done}

async def watch_closing_lines():
    """ดึง closing line อัตโนมัติ 1 นาทีก่อนแข่ง"""
    while True:
        try:
            now = datetime.now(timezone.utc)
            to_fetch = []

            with _data_lock:
                watch_snapshot = list(_closing_line_watch.items())

            for key, info in watch_snapshot:
                if info.get("done"): continue
                mins_left = (info["commence_dt"] - now).total_seconds() / 60
                if -10 <= mins_left <= 1:
                    to_fetch.append((key, info))  # fetch window: 1 min before to 10 min after
                elif mins_left < -10:
                    # Fix 3: event is well past — mark done to stop retrying
                    with _data_lock:
                        if key in _closing_line_watch:
                            _closing_line_watch[key]["done"] = True
                    log.info(f"[CLV] auto-expired stale watch: {info['event']}")

            if api_remaining <= 5:
                log.warning("[Quota] Credits ต่ำเกินไป — ระงับ CLV fetch ชั่วคราว")
                await asyncio.sleep(300)
                continue
            if to_fetch:
                async with aiohttp.ClientSession() as session:
                    for key, info in to_fetch:
                        sport  = info["sport"]
                        # L6: fetch both standard + extra feeds for closing lines
                        std_ev   = await _fetch_odds_sem(session, sport)
                        extra_ev = await _fetch_extra_books_sem(session, sport) if _s("EXTRA_BOOKMAKERS", "") else []
                        # R2: fuzzy merge extra events (same logic as fetch_all_async)
                        events = list(std_ev)
                        for _ex_ev in extra_ev:
                            _ex_name = f"{_ex_ev.get('home_team','')} vs {_ex_ev.get('away_team','')}"
                            _merged = False
                            for _st_ev in events:
                                _st_name = f"{_st_ev.get('home_team','')} vs {_st_ev.get('away_team','')}"
                                if fuzzy_match(_ex_name, _st_name, 0.80):
                                    _exist_bms = {_b["key"] for _b in _st_ev.get("bookmakers", [])}
                                    for _b in _ex_ev.get("bookmakers", []):
                                        if _b["key"] not in _exist_bms:
                                            _st_ev.setdefault("bookmakers", []).append(_b)
                                    _merged = True
                                    break
                            if not _merged:
                                events.append(_ex_ev)
                        matched_any = False       # S4: aggregate flags แทน per-event
                        pinnacle_found_any = False  # S4: ถ้ามี Pinnacle ใน event ใดก็ตาม → True
                        for event in events:
                            ename = f"{event.get('home_team','')} vs {event.get('away_team','')}"
                            # E6: fuzzy match — ตรวจ token overlap แทน string ตรง ทน alias/punctuation
                            _tgt = info["event"].lower()
                            _src = ename.lower()
                            _tgt_tokens = set(re.split(r'[\s\-_/]+', _tgt))
                            _src_tokens = set(re.split(r'[\s\-_/]+', _src))
                            _overlap = len(_tgt_tokens & _src_tokens) / max(len(_tgt_tokens), 1)
                            if _overlap < 0.6 and ename != info["event"]: continue
                            _ev_pinnacle = False
                            for bm in event.get("bookmakers", []):
                                bk = bm.get("key","")
                                for mkt in bm.get("markets",[]):
                                    if mkt.get("key") != "h2h": continue
                                    for out in mkt.get("outcomes",[]):
                                        price = Decimal(str(out.get("price",1)))
                                        # M2: canonical key; N3: normalize Draw/Tie outcome
                                        _out_name = out.get("name", "")
                                        _norm_name = "Draw" if str(_out_name).strip().lower() in ("draw", "tie", "x") else _out_name
                                        update_clv(info["event"], _norm_name, bk, price)
                                        if bk == "pinnacle":
                                            _ev_pinnacle = True
                            matched_any = True
                            if _ev_pinnacle:
                                pinnacle_found_any = True
                            if not _ev_pinnacle:
                                log.warning(f"[CLV] ⚠️ Pinnacle closing line missing for {ename} — CLV benchmark unreliable")
                            log.info(f"[CLV] closing line saved: {ename} (pinnacle={'✅' if _ev_pinnacle else '❌'})")
                        # S4/R3: mark done ต่อเมื่อ match >=1 event AND มี Pinnacle ใน event ใดก็ตาม
                        if matched_any and pinnacle_found_any:
                            with _data_lock:
                                if key in _closing_line_watch:
                                    _closing_line_watch[key]["done"] = True
                        elif matched_any and not pinnacle_found_any:
                            log.warning(f"[CLV] Pinnacle line missing for {info['event']} — will retry next window")
                        else:
                            log.warning(f"[CLV] fetch returned no match for {info['event']} — will retry")
        except Exception as e:
            log.error(f"[CLV] watch_closing_lines crash: {e}", exc_info=True)

        await asyncio.sleep(30)


def register_closing_watch(opp: "ArbOpportunity"):
    """เพิ่ม event เข้า watchlist สำหรับ closing line"""
    try:
        commence_dt = parse_commence(opp.commence)
        key = f"{opp.event}|{opp.sport}"
        with _data_lock:
            if key not in _closing_line_watch:
                _closing_line_watch[key] = {
                    "event":       opp.event,
                    "sport":       opp.sport,
                    "commence_dt": commence_dt,
                    "done":        False,
                }
                log.info(f"[CLV] watching closing line: {opp.event}")
    except Exception as e:
        log.debug(f"[CLV] register watch: {e}")



# ══════════════════════════════════════════════════════════════════
#  🏆 AUTO SETTLEMENT — ดึงผลการแข่งขันอัตโนมัติ
# ══════════════════════════════════════════════════════════════════
# track trades ที่รอ settle: signal_id → (trade, commence_dt)
_pending_settlement: dict[str, tuple] = {}   # signal_id → (TradeRecord, datetime)
_settle_alerted: set[str] = set()            # B6: signal_ids ที่แจ้ง postponed ไปแล้ว
_manual_review_alerted: set[str] = set()     # D5: signal_ids ที่แจ้ง MANUAL_REVIEW ไปแล้ว
_manual_review_pending: dict[str, tuple] = {}  # K3: trades ที่ parse_winner() = MANUAL_REVIEW — แยกออกจาก _pending_settlement เพื่อให้ queue สะอาด


def _settle_hint(trade: TradeRecord) -> str:
    """I4: return smart /settle hint based on trade type"""
    sid = trade.signal_id
    is_vb   = trade.stake2_thb == 0 and trade.leg2_team == "-"
    is_3way = bool(trade.leg3_team)
    if is_vb:
        return f"`/settle {sid} leg1` หรือ `/settle {sid} void`"
    elif is_3way:
        return f"`/settle {sid} leg1|leg2|draw|void`"
    else:
        return f"`/settle {sid} leg1` หรือ `/settle {sid} leg2` หรือ `/settle {sid} void`"


def register_for_settlement(trade: TradeRecord, commence: str):
    """เพิ่ม trade เข้า queue รอ settle — จะยิง API ก็ต่อเมื่อเลยเวลาเตะ +2h"""
    try:
        dt = parse_commence(commence)
        with _data_lock:
            _pending_settlement[trade.signal_id] = (trade, dt)
        settle_after = dt + timedelta(hours=2)
        log.info(f"[Settle] registered: {trade.event} | kick={dt.strftime('%d/%m %H:%M')} UTC | check after {settle_after.strftime('%d/%m %H:%M')} UTC")
    except Exception as e:
        log.debug(f"[Settle] register error: {e} (commence={commence})", exc_info=True)


async def fetch_scores(sport: str, session: Optional[aiohttp.ClientSession] = None) -> list[dict]:
    """ดึงผลการแข่งขัน (scores) จาก Odds API"""
    async def _fetch(s: aiohttp.ClientSession):
        async with s.get(
            f"https://api.the-odds-api.com/v4/sports/{sport}/scores",
            params={"apiKey": ODDS_API_KEY, "daysFrom": 3},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            remaining = int(r.headers.get("x-requests-remaining", api_remaining))
            await update_quota(remaining)
            data = await r.json(content_type=None)
            return data if isinstance(data, list) else []
    try:
        if session:
            return await _fetch(session)
        async with aiohttp.ClientSession() as s:
            return await _fetch(s)
    except Exception as e:
        log.error(f"[Settle] fetch_scores {sport}: {e}")
        return []


def parse_winner(event: dict, sport: str = "") -> Optional[str]:
    """
    แกะผลจาก scores endpoint — คืนชื่อทีมที่ชนะ
    #28 Sport-specific logic:
    - NBA/NFL/MLB/Soccer: ใช้คะแนนสูงสุด
    - Soccer draw: คืน "DRAW" — caller จะ mark เป็น manual review
    - MMA/Tennis: scores schema ต่างกัน → log + manual review
    - ไม่มี scores หรือ schema ผิด → คืน None (needs manual review)
    """
    if not event.get("completed", False):
        return None
    scores = event.get("scores")
    if not scores:
        log.warning(f"[Settle] no scores for completed event: {event.get('id','?')} sport={sport}")
        return None

    sport_lower = sport.lower()

    # MMA — scores อาจเป็น method (KO/TKO/Decision) ไม่ใช่ตัวเลข
    if "mma" in sport_lower:
        try:
            sorted_scores = sorted(scores, key=lambda x: float(x.get("score", 0)), reverse=True)
            winner = sorted_scores[0]["name"]
            log.info(f"[Settle] MMA result: {winner} (scores={scores})")
            return winner
        except Exception:
            log.warning(f"[Settle] MMA scores schema unknown: {scores} — needs manual review")
            return "MANUAL_REVIEW"

    # Tennis — scores เป็น sets (e.g. "6-4 7-5") ไม่ใช่ integer
    if "tennis" in sport_lower:
        try:
            # นับ sets ที่ชนะ
            set_wins = {}
            for s in scores:
                name = s.get("name","")
                score_str = str(s.get("score","0"))
                # รูปแบบ "6-4 7-5" → นับ sets
                sets_won = sum(1 for pair in score_str.split() if "-" in pair
                               and int(pair.split("-")[0]) > int(pair.split("-")[1]))
                set_wins[name] = sets_won
            if set_wins:
                winner = max(set_wins, key=set_wins.get)
                log.info(f"[Settle] Tennis result: {winner} sets={set_wins}")
                return winner
        except Exception:
            log.warning(f"[Settle] Tennis scores schema unknown: {scores} — needs manual review")
            return "MANUAL_REVIEW"

    # Soccer — อาจเสมอ (arb scan กรอง draw ออกแล้ว แต่ผลจริงอาจเสมอ)
    if "soccer" in sport_lower:
        try:
            sorted_scores = sorted(scores, key=lambda x: float(x.get("score", 0)), reverse=True)
            if float(sorted_scores[0].get("score", 0)) == float(sorted_scores[-1].get("score", 0)):
                log.info(f"[Settle] Soccer draw — {event.get('home_team','')} vs {event.get('away_team','')}")
                return "DRAW"
            return sorted_scores[0]["name"]
        except Exception:
            return "MANUAL_REVIEW"

    # Default: NBA/NFL/MLB/EuroLeague — numeric score
    try:
        sorted_scores = sorted(scores, key=lambda x: float(x.get("score", 0)), reverse=True)
        return sorted_scores[0]["name"]
    except Exception:
        log.warning(f"[Settle] Unknown scores schema sport={sport}: {scores}")
        return "MANUAL_REVIEW"


def calc_actual_pnl(trade: TradeRecord, winner: str) -> int:
    """
    คำนวณกำไร/ขาดทุนจริง รองรับ 2-way, 3-way และ ValueBet (1-leg)
    """
    # C3: Value Bet guard — 1-leg trade (stake2=0, leg2_team="-")
    is_valuebet = (not trade.stake2_thb) and trade.leg2_team == "-"  # P6: handle None from old schema
    if is_valuebet:
        if fuzzy_match(winner, trade.leg1_team or "", threshold=0.5):
            return int(trade.stake1_thb * trade.leg1_odds) - trade.stake1_thb
        else:
            return -trade.stake1_thb

    total_staked = trade.stake1_thb + trade.stake2_thb + (trade.stake3_thb or 0)

    # F6: กรอง legs ที่ stake=0 ออก — ป้องกัน payout ผิดกรณีข้อมูลเสียหาย
    legs = [
        leg for leg in [
            {"team": trade.leg1_team or "", "odds": trade.leg1_odds, "stake": trade.stake1_thb},
            {"team": trade.leg2_team or "", "odds": trade.leg2_odds, "stake": trade.stake2_thb},
            ({"team": trade.leg3_team, "odds": trade.leg3_odds, "stake": trade.stake3_thb}
             if trade.leg3_team and trade.leg3_odds and trade.stake3_thb else None),
        ] if leg is not None and leg["stake"] > 0
    ]

    # หา leg ที่ match winner
    # Q5/F2: fuzzy 0.65 กัน Man Utd/Man City ambiguous; substring fallback รับ short-form names เช่น 'Celtics' → 'Boston Celtics'
    _winner_norm = normalize_team(winner)
    def _leg_match(leg):
        t = leg["team"]
        return (fuzzy_match(winner, t, threshold=0.65)
                or _winner_norm in normalize_team(t)
                or normalize_team(t) in _winner_norm)
    matched   = [leg for leg in legs if _leg_match(leg)]
    unmatched = [leg for leg in legs if not _leg_match(leg)]

    if len(matched) == 1:
        payout = matched[0]["stake"] * matched[0]["odds"]
        log.info(f"[Settle] {trade.event} → '{matched[0]['team']}' won")
    else:
        # ambiguous — worst-case payout
        payout = min(leg["stake"] * leg["odds"] for leg in legs)
        log.warning(f"[Settle] {trade.event} — winner '{winner}' ambiguous — using worst-case")

    return int(payout - total_staked)


async def settle_completed_trades():
    """
    Loop ตรวจสอบผลการแข่งขัน ทุก 5 นาที
    เมื่อแข่งเสร็จ → คำนวณ actual P&L → แจ้ง Telegram → บันทึก DB
    """
    await asyncio.sleep(60)  # รอ bot start ก่อน
    log.info("[Settle] auto settlement loop started")

    while True:
        try:
            with _data_lock:
                ps_snapshot = dict(_pending_settlement)
            if not ps_snapshot:
                await asyncio.sleep(300)
                continue

            now = datetime.now(timezone.utc)
            # #37 กรองเฉพาะ trades ที่เลยเวลาเตะ +2h แล้ว — ไม่ยิง API ก่อนถึงเวลา
            ready = {
                sid: (trade, cdt)
                for sid, (trade, cdt) in ps_snapshot.items()
                if now >= cdt + timedelta(hours=2)
            }
            if not ready:
                earliest = min(cdt for _, cdt in ps_snapshot.values())
                wait_min = max(0, int((earliest + timedelta(hours=2) - now).total_seconds() / 60))
                log.debug(f"[Settle] {len(ps_snapshot)} trade(s) waiting — earliest ready in {wait_min}m")
                await asyncio.sleep(300)
                continue

            if api_remaining <= 5:
                log.warning("[Quota] Credits ต่ำเกินไป — ระงับ settle fetch ชั่วคราว")
                await asyncio.sleep(300)
                continue

            # รวม sports ที่ต้องดึงผล (เฉพาะที่ ready)
            sports_needed = set(trade.sport for trade, _ in ready.values())
            all_scores: dict[str, list] = {}

            async with aiohttp.ClientSession() as session:
                for sport in sports_needed:
                    scores = await fetch_scores(sport, session=session)
                    all_scores[sport] = scores
                    await asyncio.sleep(1)  # ไม่ spam API

            settled_ids = []
            for signal_id, (trade, _cdt) in list(ready.items()):
                # หา event ที่ตรงกัน
                sport_scores = all_scores.get(trade.sport, [])
                matched_event = None

                for ev in sport_scores:
                    home = ev.get("home_team", "")
                    away = ev.get("away_team", "")
                    ev_name = f"{home} vs {away}"
                    # R6/Issue29: guard ก่อน split — event อาจไม่มี " vs " (Tennis/MMA)
                    _ev_parts = trade.event.split(" vs ")
                    if len(_ev_parts) < 2:
                        # ไม่สามารถ auto-settle ได้ — format ไม่ตรง
                        log.debug(f"[Settle] skip {trade.signal_id} — event has no ' vs ' separator: {trade.event!r}")
                        break
                    _home_part = _ev_parts[0].strip()
                    _away_part = _ev_parts[-1].strip()
                    if fuzzy_match(home, _home_part, 0.5) and \
                       fuzzy_match(away, _away_part, 0.5):
                        matched_event = ev
                        break

                if not matched_event:
                    # H2: zombie deadman — if trade has no API match after 72h past commence, alert once and remove
                    try:
                        _ct_zombie = parse_commence(trade.commence_time or "")
                        _zombie_elapsed = (datetime.now(timezone.utc) - _ct_zombie).total_seconds()
                        if _zombie_elapsed > 72 * 3600 and signal_id not in _manual_review_alerted:
                            _manual_review_alerted.add(signal_id)
                            log.warning(f"[Settle] {trade.event} — no API match after 72h, marking MANUAL_REVIEW")
                            if _app:
                                asyncio.get_running_loop().create_task(
                                    _app.bot.send_message(
                                        chat_id=CHAT_ID,
                                        text=(
                                            f"⚠️ *Zombie Trade* `{md_escape(trade.event)}`\n"
                                            f"ไม่พบ event ใน API หลัง 72h — settle ด้วยตนเอง:\n"
                                            + _settle_hint(trade)
                                        ),
                                        parse_mode="Markdown"
                                    )
                                )
                            settled_ids.append(signal_id)  # remove from pending loop
                    except Exception:
                        pass
                    continue
                if not matched_event.get("completed", False):
                    # ยังไม่เสร็จ — เช็คว่านานเกิน 6 ชั่วโมงไหม (อาจ postponed)
                    try:
                        ct = parse_commence(matched_event.get("commence_time",""))
                        _elapsed = (datetime.now(timezone.utc) - ct).total_seconds()
                        if 6 * 3600 < _elapsed < 72 * 3600:  # Issue36: alert 6h–72h only; after 72h = postponed → manual
                            log.warning(f"[Settle] {trade.event} — เกิน 6h ยังไม่เสร็จ (postponed?)")
                            # B6: แจ้งครั้งเดียว — ไม่ spam ทุกรอบ
                            if _app and signal_id not in _settle_alerted:
                                _settle_alerted.add(signal_id)
                                asyncio.get_running_loop().create_task(
                                    _app.bot.send_message(
                                        chat_id=CHAT_ID,
                                        text=(
                                            f"⏰ *Postponed?* `{md_escape(trade.event)}`\n"
                                            f"เลยเวลาแข่งกว่า 6h แต่ยังไม่ completed\n"
                                            f"แนะนำ settle ด้วยตนเอง: "
                                            + _settle_hint(trade)
                                        ),
                                        parse_mode="Markdown"
                                    )
                                )
                    except Exception:
                        pass
                    continue

                # แมตช์เสร็จแล้ว!
                winner = parse_winner(matched_event, sport=trade.sport)
                if not winner:
                    continue

                # #28 Handle special outcomes
                if winner == "DRAW":
                    # 3-way: leg3 = Draw; 2-way: refund
                    tt_draw = trade.stake1_thb + trade.stake2_thb + (trade.stake3_thb or 0)
                    if trade.leg3_team and trade.leg3_team.lower() == "draw" and trade.leg3_odds and trade.stake3_thb:
                        actual_draw = int(trade.leg3_odds * trade.stake3_thb) - tt_draw
                        draw_label = f"P&L: *฿{actual_draw:+,}*"
                        log.info(f"[Settle] {trade.event} — DRAW (leg3), profit={actual_draw:+,}")
                    elif trade.leg1_team and trade.leg1_team.lower() == "draw":
                        actual_draw = int(trade.leg1_odds * trade.stake1_thb) - tt_draw
                        draw_label = f"P&L: *฿{actual_draw:+,}*"
                        log.info(f"[Settle] {trade.event} — DRAW (leg1), profit={actual_draw:+,}")
                    elif trade.leg2_team and trade.leg2_team.lower() == "draw":
                        actual_draw = int(trade.leg2_odds * trade.stake2_thb) - tt_draw
                        draw_label = f"P&L: *฿{actual_draw:+,}*"
                        log.info(f"[Settle] {trade.event} — DRAW (leg2), profit={actual_draw:+,}")
                    else:
                        actual_draw = 0
                        draw_label = "P&L: *฿0* (refund)"
                        log.info(f"[Settle] {trade.event} — DRAW (no draw leg), profit=0")
                    trade.actual_profit_thb = actual_draw
                    trade.settled_at = datetime.now(timezone.utc).isoformat()
                    db_save_trade(trade)
                    with _data_lock:
                        for idx, rec in enumerate(trade_records):
                            if rec.signal_id == trade.signal_id:
                                trade_records[idx] = trade
                                break
                    for cid in ALL_CHAT_IDS:
                        try:
                            await _app.bot.send_message(chat_id=cid, parse_mode="Markdown",
                                text=f"🤝 *DRAW — {draw_label}*\n`{trade.event}`\n"
                                     f"เกมเสมอ — กรุณาตรวจสอบว่าเว็บ refund เงินหรือเป่า")
                        except Exception: pass
                    settled_ids.append(signal_id)
                    continue

                if winner == "MANUAL_REVIEW":
                    log.warning(f"[Settle] {trade.event} — schema unknown, needs manual review")
                    # K3: move trade to _manual_review_pending — remove from auto-settle queue
                    with _data_lock:
                        _manual_review_pending[signal_id] = _pending_settlement.pop(signal_id, (trade, _cdt))
                    settled_ids.append(signal_id)  # mark as removed from pending loop
                    # D5: แจ้งครั้งเดียว — ไม่ spam ทุก 5 นาที
                    if _app and signal_id not in _manual_review_alerted:
                        _manual_review_alerted.add(signal_id)
                        for cid in ALL_CHAT_IDS:
                            try:
                                await _app.bot.send_message(chat_id=cid, parse_mode="Markdown",
                                    text=f"⚠️ *Manual Review Required*\n`{md_escape(trade.event)}`\n"
                                         f"ระบบ settle อัตโนมัติไม่รองรับ schema ของกีฬานี้ ({md_escape(trade.sport)})\n"
                                         f"กรุณาตรวจสอบผลเองใน Dashboard\n"
                                         + _settle_hint(trade))
                            except Exception: pass
                    continue

                # G8: double-settle guard — ป้องกัน race กับ /settle command
                if trade.actual_profit_thb is not None or trade.settled_at is not None:
                    log.info(f"[Settle] {trade.signal_id} already settled — skip auto-settle")
                    settled_ids.append(signal_id)
                    continue
                # คำนวณ P&L จริง
                actual_profit = calc_actual_pnl(trade, winner)
                total_staked  = trade.stake1_thb + trade.stake2_thb + (trade.stake3_thb or 0)
                emoji_result  = "✅" if actual_profit >= 0 else "❌"
                sport_emoji   = SPORT_EMOJI.get(trade.sport, "🏆")

                # อัพเดท trade record + in-memory
                trade.actual_profit_thb = actual_profit
                trade.settled_at        = datetime.now(timezone.utc).isoformat()
                db_save_trade(trade)
                with _data_lock:
                    for idx, rec in enumerate(trade_records):
                        if rec.signal_id == trade.signal_id:
                            trade_records[idx] = trade
                            break
                settled_ids.append(signal_id)
                # E7: เคลียร signal ออกจาก alert sets หลัง settle เสร็จ
                _settle_alerted.discard(signal_id)
                _manual_review_alerted.discard(signal_id)

                log.info(f"[Settle] {trade.event} | winner={winner} | profit=฿{actual_profit:+,}")

                # แจ้ง Telegram
                msg = (
                    f"{sport_emoji} *SETTLED* \u2014 {md_escape(trade.event)}\n"
                    f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                    f"\U0001f3c6 \u0e1c\u0e39\u0e49\u0e0a\u0e19\u0e30 : *{md_escape(winner)}*\n"
                    f"\U0001f4b5 \u0e27\u0e32\u0e07\u0e44\u0e1b  : \u0e3f{total_staked:,}\n"
                    f"\U0001f4ca \u0e01\u0e33\u0e44\u0e23\u0e08\u0e23\u0e34\u0e07: {emoji_result} *\u0e3f{actual_profit:+,}*\n"
                    f"\U0001f4c8 ROI     : *{actual_profit/total_staked*100:+.2f}%*\n"
                    f"\U0001f194 `{signal_id}`"
                )
                if _app:
                    for cid in ALL_CHAT_IDS:
                        try:
                            await _app.bot.send_message(
                                chat_id=cid, text=msg, parse_mode="Markdown")
                        except Exception as e:
                            log.error(f"[Settle] notify {cid}: {e}")

            # ลบ trades ที่ settle แล้ว
            with _data_lock:
                for sid in settled_ids:
                    _pending_settlement.pop(sid, None)

        except Exception as e:
            log.error(f"[Settle] crash in loop: {e}", exc_info=True)

        await asyncio.sleep(300)  # เช็คทุก 5 นาที


def periodic_cleanup():
    """ทำความสะอาด memory — เรียกทุกรอบ scan เพื่อป้องกัน leak ใน 24/7"""
    now = datetime.now(timezone.utc)
    with _data_lock:
        # trim trade_records ใน memory (DB ยังเก็บทั้งหมด)
        if len(trade_records) > 500:
            trade_records[:] = trade_records[-500:]
        # ลบ cooldown entries ที่หมดอายุ
        expired = [k for k, v in alert_cooldown.items()
                   if (now - v).total_seconds() > ALERT_COOLDOWN_MIN * 60 * 2]
        for k in expired:
            del alert_cooldown[k]
        # trim odds_history — เก็บแค่ 500 keys ล่าสุด
        if len(odds_history) > 500:
            keys_to_remove = list(odds_history.keys())[:-500]
            for k in keys_to_remove:
                del odds_history[k]
        # trim steam_tracker — ลบ entries เก่า
        expired_steam = [k for k, v in steam_tracker.items() if not v]
        for k in expired_steam:
            del steam_tracker[k]
        # trim closing_odds — ลบ done entries
        done_clw = [k for k, v in _closing_line_watch.items() if v.get("done")]
        for k in done_clw:
            del _closing_line_watch[k]
        if len(closing_odds) > 500:  # S3: already inside _data_lock block
            keys_to_remove = list(closing_odds.keys())[:-500]
            for k in keys_to_remove:
                del closing_odds[k]
        # S6/Issue32: prune seen_signals ใน periodic_cleanup ด้วย (ป้องกัน leak เมื่อ scan lock ค้าง)
        _now_ts_pc = time.time()
        _seen_exp = [k for k, ts in seen_signals.items() if (_now_ts_pc - ts) > SEEN_TTL_SEC]
        for k in _seen_exp:
            del seen_signals[k]
        # trim _refetch_cache — ลบ entries ที่เกิน 30 วินาที
        now_ts = time.time()
        expired_rc = [k for k, (ts, _) in _refetch_cache.items() if now_ts - ts > 30]
        for k in expired_rc:
            del _refetch_cache[k]
        # trim _pending_vb — ลบ Value Bet signals ที่หมดอายุ (> SIGNAL_TTL)
        _vb_ttl = SIGNAL_TTL_SEC  # F7
        expired_vb = [k for k, (_, ts) in _pending_vb.items() if now_ts - ts > _vb_ttl]
        for k in expired_vb:
            del _pending_vb[k]
    # Issue35/41: snapshot + clear alert sets inside lock — consistent view, no conceptual race
    with _data_lock:
        _trade_snap = list(trade_records)
        _settled_in_cleanup = {s.signal_id for s in _trade_snap if s.actual_profit_thb is not None}
        _settle_alerted        -= _settled_in_cleanup
        _manual_review_alerted -= _settled_in_cleanup
        # K3: prune _manual_review_pending for trades that got settled externally
        for _sid in list(_manual_review_pending):
            if _sid in _settled_in_cleanup:
                del _manual_review_pending[_sid]


async def scanner_loop():
    global _scan_wakeup
    _scan_wakeup = asyncio.Event()
    await asyncio.sleep(3)
    log.info(f"[Scanner] v2.0 | interval={SCAN_INTERVAL}s | sports={len(SPORTS)}")
    while True:
        # S7: log auto_scan state every loop to trace silent stops
        log.info(f"[Scanner] tick | auto_scan={auto_scan} | db_halted={_db_write_halted} | api={api_remaining}")
        if auto_scan:
            try: await do_scan()
            except Exception as e: log.error(f"[Scanner] {e}")
        else:
            log.warning("[Scanner] auto_scan=False — skipping scan this tick")
        periodic_cleanup()
        # D1: pending TTL cleanup — ลบ signal เก่าเกิน TTL หรือเลยเวลาแข่งแล้ว
        _ttl = SIGNAL_TTL_SEC  # F7
        _now_ts = time.time()
        _now_dt = datetime.now(timezone.utc)
        expired = []
        with _data_lock:
            pending_snapshot = list(pending.items())
        for _sid, (_opp, _ts) in pending_snapshot:
            if _now_ts - _ts > _ttl:
                expired.append(_sid)
                continue
            try:
                if _opp.commence:  # S5/Issue31: guard empty string ก่อน parse
                    _cdt = parse_commence(_opp.commence)
                    if _now_dt > _cdt + timedelta(minutes=5):  # เลยเวลาแข่ง +5m
                        expired.append(_sid)
            except Exception:
                pass
        if expired:
            with _data_lock:
                for _sid in expired:
                    pending.pop(_sid, None)
                    # R1: opportunity_log เก็บเป็น dict — ใช้ .get("id") ไม่ใช่ getattr
                    for opp_rec in opportunity_log:
                        if opp_rec.get("id") == _sid:
                            opp_rec["status"] = "expired"
                            break
            log.info(f"[Pending] expired {len(expired)} signal(s)")
            # S1: split-brain guard — ถ้า writes halted ห้าม fallback SQLite
            if _db_write_halted:
                pass  # skip persist entirely — Turso halted, SQLite would cause split-brain
            elif _turso_ok:
                async def _expire_turso(_sids=list(expired)):
                    for _sid in _sids:
                        try:
                            await turso_exec("UPDATE opportunity_log SET status='expired' WHERE id=?", (_sid,))
                        except Exception: pass
                asyncio.get_running_loop().create_task(_expire_turso())
            else:
                # SQLite-only mode (Turso never configured)
                try:
                    with sqlite3.connect(DB_PATH, timeout=2) as _con:
                        for _sid in expired:
                            _con.execute("UPDATE opportunity_log SET status='expired' WHERE id=?", (_sid,))
                        _con.commit()
                except Exception as _qe:
                    log.debug(f"[Pending] expire DB update failed: {_qe}")
        # v10-1: รอแบบ ถ้า apply_runtime_config เปลี่ยน interval/auto_scan จะปลุก event นี้เพื่อตื่นทันที
        _scan_wakeup.clear()
        try:
            await asyncio.wait_for(_scan_wakeup.wait(), timeout=SCAN_INTERVAL)
            log.info("[Scanner] woken up by config change")
        except asyncio.TimeoutError:
            pass


async def keep_alive_ping():
    """#31 Render keep-alive — self-ping /health ทุก 14 นาที เพื่อกัน Render free tier sleep"""
    await asyncio.sleep(60)  # รอ bot start ก่อน
    # I22: ใช้ 127.0.0.1 แทน localhost — Railway/Render container policy
    url = f"http://127.0.0.1:{PORT}/health"
    log.info(f"[KeepAlive] self-ping loop started → {url}")
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    log.debug(f"[KeepAlive] ping {r.status}")
        except Exception as e:
            log.debug(f"[KeepAlive] ping failed: {e}")
        await asyncio.sleep(14 * 60)  # ทุก 14 นาที


# ══════════════════════════════════════════════════════════════════
#  8. DASHBOARD
# ══════════════════════════════════════════════════════════════════
_DASH_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
try:
    with open(_DASH_PATH, "r", encoding="utf-8") as _f:
        DASHBOARD_HTML = _f.read()
    log.info(f"[Dashboard] loaded from {_DASH_PATH}")
except FileNotFoundError:
    log.warning("[Dashboard] dashboard.html not found — using empty fallback")
    DASHBOARD_HTML = "<h1>dashboard.html not found</h1>"



_stats_cache: dict = {"data": None, "ts": 0}
_stats_cache_lock = threading.Lock()  # B3: protect _stats_cache read/write across ThreadingHTTPServer threads

def calc_stats_cached() -> dict:
    """calc_stats พร้อม cache 15 วินาที — ลดภาระ CPU ตอน dashboard refresh"""
    # G1/Issue43: short-lock check, compute outside lock, short-lock write
    # avoids blocking dashboard threads during heavy calc_stats() computation
    with _stats_cache_lock:
        if time.time() - _stats_cache["ts"] < 15 and _stats_cache["data"] is not None:
            return _stats_cache["data"]
    result = calc_stats()
    with _stats_cache_lock:
        _stats_cache["data"] = result
        _stats_cache["ts"]   = time.time()
    return result

def calc_stats() -> dict:
    """คำนวณสถิติทั้งหมดสำหรับ /api/stats"""
    # v10-12: snapshot ด้วย lock ก่อนประมวลผล
    with _data_lock:
        confirmed    = [t for t in trade_records if t.status == "confirmed"]
        rejected     = [t for t in trade_records if t.status == "rejected"]
        rlm_moves    = [m for m in line_movements if m.is_rlm]
        steam_moves  = [m for m in line_movements if m.is_steam]
        lm_snap      = list(line_movements)
        tr_snap      = list(trade_records[-30:])

    # ── Win Rate ──────────────────────────────────────────────────

    # เชื่อม RLM กับ trade ที่เกิดขึ้นหลังสัญญาณ (ภายใน 30 นาที)
    # C2: แยก signal_conversion_rate (signal → confirmed) vs settled_win_rate (actual outcome)
    # G9: helper กัน naive vs aware datetime TypeError
    def _parse_ts(s: str) -> datetime:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def signal_conversion_rate(moves):
        """วัด % ของ line-move signals ที่ถูก confirm เป็น trade ภายใน 30 นาที"""
        if not moves: return None, len(moves)
        converted = 0
        total_m = 0
        for m in moves:
            try: m_ts = _parse_ts(m.ts)
            except Exception: continue
            total_m += 1
            for t in confirmed:
                try: t_ts = _parse_ts(t.created_at)
                except Exception: continue
                if abs((t_ts - m_ts).total_seconds()) < 1800 and (m.event in t.event or t.event in m.event):
                    converted += 1
                    break
        return (converted / total_m * 100 if total_m > 0 else None), total_m

    def signal_win_rate(moves):
        """C2: วัด % ที่ trade ที่ match สัญญาณนั้น settled ด้วยกำไร (actual_profit_thb >= 0)"""
        if not moves: return None, len(moves)
        wins = 0
        total = 0
        settled_confirmed = [t for t in confirmed if t.actual_profit_thb is not None]
        for m in moves:
            try: m_ts = _parse_ts(m.ts)
            except Exception: continue
            for t in settled_confirmed:
                try: t_ts = _parse_ts(t.created_at)
                except Exception: continue
                if abs((t_ts - m_ts).total_seconds()) < 1800 and (m.event in t.event or t.event in m.event):
                    total += 1
                    if t.actual_profit_thb >= 0:
                        wins += 1
                    break
        return (wins / total * 100 if total > 0 else None), len(moves)

    rlm_conv,   rlm_cnt   = signal_conversion_rate(rlm_moves)
    steam_conv, steam_cnt = signal_conversion_rate(steam_moves)
    rlm_wr,   _   = signal_win_rate(rlm_moves)
    steam_wr, _   = signal_win_rate(steam_moves)
    arb_total    = len(confirmed) + len(rejected)
    confirm_rate = (len(confirmed) / arb_total * 100) if arb_total > 0 else None

    # I2/I3: define once — used in ROI, P&L, and est_profit
    arb_confirmed = [t for t in confirmed if not (t.stake2_thb == 0 and t.leg2_team == "-")]

    # ── Sharp vs Public ───────────────────────────────────────────
    sharp_count  = len(rlm_moves) + len(steam_moves)
    public_count = max(0, len(lm_snap) - sharp_count)

    # ── Bookmaker Accuracy ────────────────────────────────────────
    # วัดจาก: ถ้า Pinnacle ขยับ odds ฝั่งไหน แล้ว outcome นั้นชนะบ่อยแค่ไหน
    # ใช้ line_movements เพื่อดูว่า bookmaker ไหน "รู้ก่อน" (odds ลดลง = favourite จริง)
    bm_correct = defaultdict(int)
    bm_total   = defaultdict(int)
    for m in lm_snap:
        bm_total[m.bookmaker] += 1
        # ถ้า odds ลด = เว็บเชื่อว่าจะชนะมากขึ้น = "sharp signal"
        if m.pct_change < -0.03:
            bm_correct[m.bookmaker] += 1
    # D7: เปลี่ยนชื่อ metric ให้ตรงความหมาย — นี่คือ % ของ moves ที่ดู "sharp" ไม่ใช่ accuracy vs ผลจริง
    bm_sharp_move_rate = {bm: bm_correct[bm]/bm_total[bm]
                          for bm in bm_total if bm_total[bm] >= 3}

    # ── ROI per Sport ─────────────────────────────────────────────
    # I2: ROI = arb only (profit_pct * stake is meaningful for arb, not VB edge)
    sport_profit = defaultdict(float)
    sport_stake  = defaultdict(float)
    for t in arb_confirmed:
        est = t.profit_pct * (t.stake1_thb + t.stake2_thb + (t.stake3_thb or 0))
        sport_profit[t.sport] += est
        sport_stake[t.sport]  += (t.stake1_thb + t.stake2_thb + (t.stake3_thb or 0))
    roi_by_sport = {s: sport_profit[s]/sport_stake[s]
                    for s in sport_stake if sport_stake[s] > 0}

    # ── CLV Summary ───────────────────────────────────────────────
    clv_values = []
    for t in confirmed:
        c1, c2, c3 = calc_clv(t)
        if c1 is not None: clv_values.append(c1)
        if c2 is not None: clv_values.append(c2)
        if c3 is not None: clv_values.append(c3)
    avg_clv = sum(clv_values)/len(clv_values) if clv_values else None
    clv_positive = len([c for c in clv_values if c > 0])
    clv_negative = len([c for c in clv_values if c < 0])
    best_clv     = max(clv_values) if clv_values else None

    # ── P&L ───────────────────────────────────────────────────────
    # I3: est_profit = unsettled arb only (arb_confirmed defined above)
    unsettled_arb_snap = [t for t in arb_confirmed if t.actual_profit_thb is None]
    est_profit = sum(t.profit_pct*(t.stake1_thb+t.stake2_thb+(t.stake3_thb or 0)) for t in unsettled_arb_snap)
    avg_profit = (sum(t.profit_pct for t in arb_confirmed)/len(arb_confirmed)*100) if arb_confirmed else None

    # ── Trade records สำหรับ table ────────────────────────────────
    trade_list = []
    for t in tr_snap:  # C6: ใช้ tr_snap (snapshot ใน lock) ไม่ใช่ trade_records โดยตรง
        c1, c2, c3 = calc_clv(t)
        trade_list.append({
            "signal_id": t.signal_id, "event": t.event, "sport": t.sport,
            "leg1_bm": t.leg1_bm, "leg2_bm": t.leg2_bm,
            "leg1_odds": t.leg1_odds, "leg2_odds": t.leg2_odds,
            "stake1_thb": t.stake1_thb, "stake2_thb": t.stake2_thb,
            "profit_pct": t.profit_pct, "status": t.status,
            "clv_leg1": c1, "clv_leg2": c2, "clv_leg3": c3,
            "leg3_bm": t.leg3_bm, "leg3_team": t.leg3_team,
            "leg3_odds": t.leg3_odds, "stake3_thb": t.stake3_thb,
            "created_at": t.created_at,
        })

    return {
        "rlm_conversion_rate":  rlm_conv,
        "rlm_win_rate":          rlm_wr,
        "rlm_count":             rlm_cnt,
        "steam_conversion_rate": steam_conv,
        "steam_win_rate":        steam_wr,
        "steam_count":           steam_cnt,
        "confirm_rate":    confirm_rate,  # L5: ลบ arb_win_rate (ทำให้สับสน) — ใช้ confirm_rate เอก

        "confirmed_trades":len(confirmed),
        "sharp_count":     sharp_count,
        "public_count":    public_count,
        "bm_sharp_move_rate": bm_sharp_move_rate,
        "roi_by_sport":    roi_by_sport,
        "clv": {
            "avg":      round(avg_clv,2) if avg_clv is not None else None,
            "positive": clv_positive,
            "negative": clv_negative,
            "best":     round(best_clv,2) if best_clv is not None else None,
        },
        "pnl": {
            "confirmed":  len(confirmed),
            "rejected":   len(rejected),
            "est_profit": round(est_profit),
            "avg_profit": round(avg_profit,2) if avg_profit else None,
            "avg_clv":    round(avg_clv,2) if avg_clv is not None else None,
        },
        "trade_records": trade_list,
    }



def apply_runtime_config(key: str, value: str) -> tuple[bool, str]:
    """ปรับ config runtime โดยไม่ต้อง redeploy"""
    global auto_scan, MIN_PROFIT_PCT, SCAN_INTERVAL, MAX_ODDS_ALLOWED
    global MIN_ODDS_ALLOWED, ALERT_COOLDOWN_MIN, TOTAL_STAKE_THB, TOTAL_STAKE
    global KELLY_FRACTION, USE_KELLY, QUOTA_WARN_AT

    try:
        if key == "auto_scan":
            auto_scan = value.lower() in ("true","1","on")
            if _scan_wakeup: _scan_wakeup.set()  # v10-1: ปลุก loop ทันที
            return True, f"auto_scan = {auto_scan}"
        elif key == "min_profit_pct":
            MIN_PROFIT_PCT = Decimal(value)
            return True, f"MIN_PROFIT_PCT = {MIN_PROFIT_PCT:.3f}"
        elif key == "scan_interval":
            SCAN_INTERVAL = int(value)
            if _scan_wakeup: _scan_wakeup.set()  # v10-1: ปลุก loop ให้ใช้ interval ใหม่ทันที
            return True, f"SCAN_INTERVAL = {SCAN_INTERVAL}s"
        elif key == "max_odds":
            MAX_ODDS_ALLOWED = Decimal(value)
            return True, f"MAX_ODDS_ALLOWED = {MAX_ODDS_ALLOWED}"
        elif key == "min_odds":
            MIN_ODDS_ALLOWED = Decimal(value)
            return True, f"MIN_ODDS_ALLOWED = {MIN_ODDS_ALLOWED}"
        elif key == "cooldown":
            ALERT_COOLDOWN_MIN = int(value)
            return True, f"ALERT_COOLDOWN_MIN = {ALERT_COOLDOWN_MIN}m"
        elif key == "total_stake":
            TOTAL_STAKE_THB = Decimal(value)
            TOTAL_STAKE     = TOTAL_STAKE_THB / USD_TO_THB
            return True, f"TOTAL_STAKE_THB = ฿{int(TOTAL_STAKE_THB):,}"
        elif key == "kelly_fraction":
            KELLY_FRACTION = Decimal(value)
            return True, f"KELLY_FRACTION = {KELLY_FRACTION}"
        elif key == "use_kelly":
            USE_KELLY = value.lower() in ("true","1","on")
            return True, f"USE_KELLY = {USE_KELLY}"
        elif key == "scan_now":
            # trigger scan ทันที — ส่งผ่าน _main_loop เพราะ HTTP thread ไม่มี running loop
            if _main_loop and _main_loop.is_running():
                asyncio.run_coroutine_threadsafe(do_scan(), _main_loop)
            return True, "scan triggered"
        elif key == "clear_seen":
            with _data_lock:
                seen_signals.clear()  # clear dict — reset all TTL timers
            return True, "seen_signals cleared"
        else:
            return False, f"unknown key: {key}"
    except Exception as e:
        return False, str(e)

class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def _check_auth(self) -> bool:
        """ตรวจ Dashboard token (ถ้าตั้งไว้)
        J7: รองรับทั้ง Bearer header และ ?token= query param
        """
        if not DASHBOARD_TOKEN:
            # I5: allow only if ALLOW_INSECURE_DASHBOARD=true is explicitly set
            _ALLOW_INSECURE = os.getenv("ALLOW_INSECURE_DASHBOARD", "").lower() == "true"
            if _ALLOW_INSECURE:
                return True
            # block and return 401 — production-safe by default
            self.send_response(401)
            body = b'{"error":"DASHBOARD_TOKEN not set. Set ALLOW_INSECURE_DASHBOARD=true to allow unauthenticated access."}'
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)
            return False
        # C6: Bearer header
        auth = self.headers.get("Authorization", "")
        if auth == f"Bearer {DASHBOARD_TOKEN}":
            return True
            # P2: Bearer-only — ?token= ถูกปิดเพื่อกัน token ละหลายใน browser history / logs
        self.send_response(401)
        body = b'{"error":"unauthorized"}'
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)
        return False

    def do_POST(self):
        """รับ POST จาก Dashboard UI Controls"""
        if not self._check_auth(): return
        # Fix 2: strip query params so ?token=xxx doesn't break path matching
        from urllib.parse import urlparse as _urlparse
        _clean = _urlparse(self.path).path
        if _clean == "/api/control":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body   = json.loads(self.rfile.read(length))
                key    = body.get("key","")
                value  = str(body.get("value",""))
                ok, msg = apply_runtime_config(key, value)
                # save ลง DB ด้วย
                # R5: one-shot actions ไม่ควร save เป็น persistent cfg_*
                _non_persistent = {"scan_now", "clear_seen"}
                if ok and key not in _non_persistent:
                    db_save_state(f"cfg_{key}", value)
                    # G6: clear stats cache เมื่อ config เปลี่ยน
                    with _stats_cache_lock:
                        _stats_cache["data"] = None
                        _stats_cache["ts"]   = 0
                resp = json.dumps({"ok": ok, "msg": msg}).encode()
                self.send_response(200 if ok else 400)
                self.send_header("Content-Type","application/json")
                self.send_header("Content-Length",len(resp))
                self.end_headers()
                self.wfile.write(resp)
            except Exception as e:
                err = json.dumps({"ok":False,"msg":str(e)}).encode()
                self.send_response(500)
                self.send_header("Content-Type","application/json")
                self.send_header("Content-Length",len(err))
                self.end_headers()
                self.wfile.write(err)
        elif _clean == "/api/settle":
            # v10-9: Manual Settlement จาก Dashboard
            try:
                length = int(self.headers.get("Content-Length", 0))
                body   = json.loads(self.rfile.read(length))
                sid    = body.get("signal_id", "").strip()
                result = body.get("result", "").strip().lower()  # leg1|leg2|draw|void
                if not sid or result not in ("leg1","leg2","draw","void"):
                    raise ValueError("signal_id and result (leg1/leg2/draw/void) required")
                # K2: get-validate-pop — peek from pending or manual_review, validate, then pop
                with _data_lock:
                    entry = _pending_settlement.get(sid) or _manual_review_pending.get(sid)
                if not entry:
                    raise ValueError(f"signal_id '{sid}' not found in pending settlement")
                t, _ = entry
                # J2: status guard
                if t.status != "confirmed":
                    raise ValueError(f"Trade {sid} status='{t.status}' — only confirmed trades can be settled")
                # Fix 2: prevent re-settling an already-settled trade
                if t.actual_profit_thb is not None or t.settled_at is not None:
                    raise ValueError(f"signal_id '{sid}' already settled (P&L={t.actual_profit_thb:+,})")
                # K2: validation passed — pop now
                with _data_lock:
                    _pending_settlement.pop(sid, None)
                    _manual_review_pending.pop(sid, None)
                tt = t.stake1_thb + t.stake2_thb + (t.stake3_thb or 0)
                if result == "leg1":
                    actual = int(t.leg1_odds * t.stake1_thb) - tt
                elif result == "leg2":
                    actual = int(t.leg2_odds * t.stake2_thb) - tt
                elif result == "draw":
                    # 3-way: leg3 = Draw; fallback leg1/leg2
                    if t.leg3_team and t.leg3_team.lower() == "draw" and t.leg3_odds and t.stake3_thb:
                        actual = int(t.leg3_odds * t.stake3_thb) - tt
                    elif t.leg1_team and t.leg1_team.lower() == "draw":
                        actual = int(t.leg1_odds * t.stake1_thb) - tt
                    elif t.leg2_team and t.leg2_team.lower() == "draw":
                        actual = int(t.leg2_odds * t.stake2_thb) - tt
                    else:
                        actual = 0
                else:  # void
                    actual = 0
                t.actual_profit_thb = actual
                t.settled_at = datetime.now(timezone.utc).isoformat()
                t.status = "confirmed"
                db_save_trade(t)
                # A3: update in-memory trade_records
                with _data_lock:
                    for idx, rec in enumerate(trade_records):
                        if rec.signal_id == t.signal_id:
                            trade_records[idx] = t
                            break
                resp = json.dumps({"ok": True, "msg": f"Settled {result.upper()} | P&L: {actual:+,}", "actual": actual}).encode()
                self.send_response(200)
                self.send_header("Content-Type","application/json")
                self.send_header("Content-Length",len(resp))
                self.end_headers()
                self.wfile.write(resp)
            except Exception as e:
                err = json.dumps({"ok": False, "msg": str(e)}).encode()
                self.send_response(400)
                self.send_header("Content-Type","application/json")
                self.send_header("Content-Length",len(err))
                self.end_headers()
                self.wfile.write(err)
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        # Health check endpoint สำหรับ Railway (ไม่ต้อง auth) — D2: richer
        from urllib.parse import urlparse as _up_health
        _req_path = _up_health(self.path).path
        # E4: /ready = readiness probe — returns 503 when DB is halted (for Railway/Render health checks)
        if _req_path == "/ready":
            body = json.dumps({"ready": not _db_write_halted}).encode()
            self.send_response(200 if not _db_write_halted else 503)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",len(body))
            self.end_headers()
            self.wfile.write(body)
            return
        if _req_path == "/health":
            uptime_s = int(time.time() - _bot_start_ts)
            uptime_str = f"{uptime_s//3600}h{(uptime_s%3600)//60}m"
            # C5: /health is public (Railway check) — minimal response only, no internal telemetry
            health = {
                "status":  "ok" if not _db_write_halted else "degraded",
                "uptime":  uptime_str,
            }
            body = json.dumps(health).encode()
            # G2: /health = liveness (process alive) → always 200; /ready = readiness → 503 when DB halted
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        # strip query params for path matching
        from urllib.parse import urlparse
        clean_path = urlparse(self.path).path

        # J4/C3: protect ALL non-health paths — both /api/* and HTML page
        # _check_auth() blocks if no token and ALLOW_INSECURE_DASHBOARD!=true
        if not self._check_auth(): return

        if clean_path == "/api/state":
            with _data_lock:
                confirmed  = [t for t in trade_records if t.status=="confirmed"]
                rejected   = [t for t in trade_records if t.status=="rejected"]
                lm_snap    = list(line_movements[-50:])
                opp_snap   = list(opportunity_log[-50:])
                tr_snap    = list(trade_records[-30:])
                ps_snap    = list(_pending_settlement.values())
                mr_snap    = list(_manual_review_pending.values())  # K3: manual review queue
                pending_ct = len(pending)
            # J3/I3: est_profit = unsettled arb only (matches calc_stats logic)
            _arb_confirmed = [t for t in confirmed if not (t.stake2_thb == 0 and t.leg2_team == "-")]
            _unsettled_arb = [t for t in _arb_confirmed if t.actual_profit_thb is None]
            est_profit = sum(t.profit_pct*(t.stake1_thb+t.stake2_thb+(t.stake3_thb or 0)) for t in _unsettled_arb)
            clv_values = []
            for t in confirmed:
                c1, c2, c3 = calc_clv(t)
                if c1 is not None: clv_values.append(c1)
                if c2 is not None: clv_values.append(c2)
                if c3 is not None: clv_values.append(c3)
            avg_clv = sum(clv_values)/len(clv_values) if clv_values else None

            lm_list = [{"event":m.event,"bookmaker":m.bookmaker,"outcome":m.outcome,
                        "odds_before":float(m.odds_before),"odds_after":float(m.odds_after),
                        "pct_change":float(m.pct_change),"direction":m.direction,
                        "is_steam":m.is_steam,"is_rlm":m.is_rlm,"ts":m.ts}
                       for m in lm_snap]

            # serialize trade_records for dashboard Force Settle UI
            def _tr_to_dict(t: TradeRecord) -> dict:
                c1, c2, c3 = calc_clv(t)  # C8: use computed CLV not stale field
                return {
                    "signal_id":  t.signal_id,
                    "event":      t.event,
                    "sport":      t.sport,
                    "leg1_bm":    t.leg1_bm,
                    "leg2_bm":    t.leg2_bm,
                    "leg1_team":  t.leg1_team,
                    "leg2_team":  t.leg2_team,
                    "leg1_odds":  t.leg1_odds,
                    "leg2_odds":  t.leg2_odds,
                    "stake1_thb": t.stake1_thb,
                    "stake2_thb": t.stake2_thb,
                    "profit_pct": t.profit_pct,
                    "status":     t.status,
                    "clv_leg1":   c1,
                    "clv_leg2":   c2,
                    "clv_leg3":   c3,
                    "actual_profit_thb": t.actual_profit_thb,
                    "settled_at": t.settled_at,
                    "created_at": t.created_at,
                    "commence_time": t.commence_time,
                    "leg3_bm":    t.leg3_bm,
                    "leg3_team":  t.leg3_team,
                    "leg3_odds":  t.leg3_odds,
                    "stake3_thb": t.stake3_thb,
                }
            tr_list = [_tr_to_dict(t) for t in tr_snap]
            # D6: snapshot _pending_vb ใต้ _data_lock กัน race condition
            with _data_lock:
                _vb_snap = list(_pending_vb.items())

            data = {
                "auto_scan":       auto_scan,
                "scan_count":      scan_count,
                "last_scan_time":  last_scan_time,
                "pending_count":   pending_ct,
                "api_remaining":   api_remaining,
                "quota_warn_at":   QUOTA_WARN_AT,
                "total_stake_thb": int(TOTAL_STAKE_THB),
                "min_profit_pct":  float(MIN_PROFIT_PCT),
                "max_odds":        float(MAX_ODDS_ALLOWED),
                "scan_interval":   SCAN_INTERVAL,
                "db_mode":         "turso" if _turso_ok else ("halted" if _db_write_halted else "sqlite"),
                "db_write_halted": _db_write_halted,
                "line_move_count": len(lm_snap),
                "confirmed_trades":len(confirmed),
                "manual_review_count": len(mr_snap),  # K3: separate from unsettled_trades count
                "opportunities":   opp_snap,
                "line_movements":  lm_list,
                "trade_records":   tr_list,
                "unsettled_trades": [
                    {
                        "signal_id":  t.signal_id,
                        "event":      t.event,
                        "leg1_bm":    t.leg1_bm,
                        "leg2_bm":    t.leg2_bm,
                        "profit_pct": t.profit_pct,
                        "stake1_thb": t.stake1_thb,
                        "stake2_thb": t.stake2_thb,
                        "created_at": t.created_at,
                        "commence_time": t.commence_time,
                        # 3-way fields
                        "leg3_bm":    t.leg3_bm,
                        "leg3_team":  t.leg3_team,
                        "leg3_odds":  t.leg3_odds,
                        "stake3_thb": t.stake3_thb,
                    }
                    for t, _dt in ps_snap
                ],
                "pnl": {
                    "confirmed":  len(confirmed),
                    "rejected":   len(rejected),
                    "est_profit": round(est_profit),
                    "avg_clv":    round(avg_clv,2) if avg_clv is not None else None,
                },
                "pending_valuebets": [
                    {
                        "signal_id": sid,
                        "event":     vb.event,
                        "bookmaker": vb.bookmaker,
                        "outcome":   vb.outcome,
                        "grade":     vb.grade,
                        "edge_pct":  vb.edge_pct,
                        "stake":     vb.rec_stake_thb,
                        "soft_odds": vb.soft_odds,
                    }
                    for sid, (vb, _ts) in _vb_snap
                ],
            }
            body = json.dumps(data, default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",len(body))
            self.end_headers()
            self.wfile.write(body)
        elif clean_path == "/api/stats":
            body = json.dumps(calc_stats_cached(), default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",len(body))
            self.end_headers()
            self.wfile.write(body)
        else:
            body = DASHBOARD_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type","text/html; charset=utf-8")
            self.send_header("Content-Length",len(body))
            self.end_headers()
            self.wfile.write(body)


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Thread-per-request HTTP server — prevents Dashboard from blocking"""
    daemon_threads = True

def start_dashboard():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), DashboardHandler)
    log.info(f"[Dashboard] http://0.0.0.0:{PORT} (ThreadingHTTPServer)")
    server.serve_forever()


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if "Conflict" in str(context.error):
        log.warning("[Bot] Conflict — รอ instance เก่าหายไป")
        return
    log.error(f"[Bot] {context.error}")


async def post_init(app: Application):
    global trade_records, opportunity_log, line_movements, scan_count, auto_scan, last_scan_time, api_remaining, _main_loop, _scan_lock, _ODDS_API_SEM
    # B1: สร้าง asyncio.Lock ใน event loop ที่ถูกต้อง
    _scan_lock    = asyncio.Lock()
    # F5: สร้าง Semaphore ที่นี่เลย — ไม่ lazy-init ใน fetch_all_async (กัน race condition)
    _ODDS_API_SEM = asyncio.Semaphore(5)
    # #33 บันทึก main event loop สำหรับ cross-thread db saves
    _main_loop = asyncio.get_running_loop()

    # ── init DB ──
    db_init()                     # SQLite local (sync, fallback)
    await turso_init()            # Turso cloud (async)

    # โหลด bot state จาก Turso (persistent) → fallback local SQLite
    if _turso_ok:
        scan_count     = int(await db_load_state_async("scan_count", "0"))
        last_scan_time = await db_load_state_async("last_scan_time", "ยังไม่ได้สแกน")
        api_remaining  = int(await db_load_state_async("api_remaining", "500"))
        saved_scan     = await db_load_state_async("auto_scan", "")
    else:
        scan_count     = int(db_load_state("scan_count", "0"))
        last_scan_time = db_load_state("last_scan_time", "ยังไม่ได้สแกน")
        api_remaining  = int(db_load_state("api_remaining", "500"))
        saved_scan     = db_load_state("auto_scan", "")
    if saved_scan:
        auto_scan = saved_scan.lower() == "true"

    # Q2: restore runtime config keys saved via dashboard /api/control
    _cfg_keys = ["min_profit_pct", "scan_interval", "max_odds", "min_odds",
                 "cooldown", "total_stake", "kelly_fraction", "use_kelly"]
    for _ck in _cfg_keys:
        _cv = (await db_load_state_async(f"cfg_{_ck}", "")) if _turso_ok else db_load_state(f"cfg_{_ck}", "")
        if _cv:
            ok, msg = apply_runtime_config(_ck, _cv)
            log.info(f"[Config] restored cfg_{_ck}={_cv} → {msg}" if ok else f"[Config] cfg_{_ck} restore failed: {msg}")

    # โหลด records จาก DB (Turso หรือ SQLite)
    loaded_trades, loaded_opps, lms = await db_load_all()
    trade_records.extend(loaded_trades)
    opportunity_log.extend(loaded_opps)
    line_movements.extend(lms)

    db_mode = "☁️ Turso" if _turso_ok else "💾 SQLite local (data resets on deploy!)"
    log.info(f"[DB] {db_mode} | trades={len(trade_records)}, opps={len(opportunity_log)}, moves={len(line_movements)}, scans={scan_count}")
    if not _turso_ok:
        log.warning("[DB] ⚠️ Running WITHOUT Turso — all stats will reset on next deploy")

    # restore pending settlement — trades ที่ confirmed แต่ยังไม่มีผล
    # #34 เรียก register_closing_watch ด้วยเพื่อให้ CLV tracking ทำงานหลัง restart
    for t in trade_records:
        if t.status == "confirmed" and t.actual_profit_thb is None and t.settled_at is None:
            # v10-2: ใช้ commence_time จริง (ไม่ต้องเดาจาก created_at+3h อีกต่อไป)
            try:
                ct_str = t.commence_time or ""
                if ct_str:
                    commence_dt = parse_commence(ct_str)
                else:
                    # O2: ใช้ created_at + 24h แทน now + 24h — กัน zombie drift หลัง restart
                    _base_dt = datetime.now(timezone.utc)
                    if t.created_at:
                        try:
                            _base_dt = parse_commence(t.created_at) if isinstance(t.created_at, str) else t.created_at.replace(tzinfo=timezone.utc) if not t.created_at.tzinfo else t.created_at
                        except Exception:
                            pass
                    commence_dt = _base_dt + timedelta(hours=24)
                    log.warning(f"[Settle] trade {t.signal_id} ไม่มี commence_time — settle check ที่ created_at+24h ({commence_dt.isoformat()})")
            except Exception:
                commence_dt = datetime.now(timezone.utc)
            _pending_settlement[t.signal_id] = (t, commence_dt)
            # restore CLV watch
            try:
                key = f"{t.event}|{t.sport}"
                if key not in _closing_line_watch:
                    _already_past = datetime.now(timezone.utc) > commence_dt + timedelta(hours=3)
                    _closing_line_watch[key] = {
                        "event":       t.event,
                        "sport":       t.sport,
                        "commence_dt": commence_dt,
                        "done":        _already_past,  # Issue34: skip fetch for stale matches
                    }
            except Exception:
                pass
    log.info(f"[Settle] restored {len(_pending_settlement)} unsettled trades | CLV watch={len(_closing_line_watch)}")

    app.add_error_handler(error_handler)
    threading.Thread(target=start_dashboard, daemon=True).start()

    is_restored = len(trade_records) > 0 or scan_count > 0
    db_mode_str  = "☁️ Turso ✅" if _turso_ok else "⚠️ SQLite (resets on deploy)"
    restore_note = f"♻️ {db_mode_str}: {len(trade_records)} trades, {scan_count} scans" if is_restored else f"🆕 {db_mode_str}: fresh start"

    await app.bot.send_message(
        chat_id=CHAT_ID, parse_mode="Markdown",
        text=(
            "🤖 *Deminia Bot V.2 — Production Ready*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{restore_note}\n"
            f"Sports    : {' '.join([SPORT_EMOJI.get(s,'🏆') for s in SPORTS])}\n"
            f"Min profit: {MIN_PROFIT_PCT:.1%} | Max odds: {MAX_ODDS_ALLOWED}\n"
            f"ทุน/trade : ฿{int(TOTAL_STAKE_THB):,} | Kelly: {'✅' if USE_KELLY else '❌'}\n"
            f"Auto scan : {'🟢 เปิด' if auto_scan else '🔴 ปิด'} (ทุก {SCAN_INTERVAL}s)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"/scan /now /pnl /lines /status /trades /settle"
        ),
    )
    asyncio.create_task(scanner_loop())
    asyncio.create_task(watch_closing_lines())  # 📌 auto CLV
    asyncio.create_task(settle_completed_trades())  # 🏆 auto settle
    if os.getenv("KEEP_ALIVE", "true").lower() in ("true","1","yes"):  # v10-15: optional
        asyncio.create_task(keep_alive_ping())


def handle_shutdown(signum, frame):
    """G7: save state — SQLite sync ก่อนเสมอ (fast, signal-safe), Turso เฉพาะ loop ไม่วิ่ง"""
    log.info("[Shutdown] กำลังบันทึก state...")
    state_pairs = [
        ("scan_count",     str(scan_count)),
        ("auto_scan",      str(auto_scan)),
        ("last_scan_time", last_scan_time),
        ("api_remaining",  str(api_remaining)),
    ]
    # 1) SQLite local ก่อนเสมอ — sync-safe ใน signal handler
    try:
        with sqlite3.connect(DB_PATH, timeout=3) as con:
            for k, v in state_pairs:
                con.execute("INSERT OR REPLACE INTO bot_state(key,value) VALUES(?,?)", (k, v))
            con.commit()
        log.info("[Shutdown] saved to SQLite")
    except Exception as ex:
        log.error(f"[Shutdown] sqlite save failed: {ex}")
    # 2) Turso — P3: ยิง sync เสมอ ไม่เช็ค loop (ใช้ urllib sync ตรงๆ กัน state rollback)
    _url   = _turso_url or TURSO_URL.replace("libsql://", "https://").replace("wss://", "https://")
    _token = _turso_token or TURSO_TOKEN
    if _turso_ok and _url and _token:
        try:
            stmts = [{"sql": "INSERT OR REPLACE INTO bot_state(key,value) VALUES(?,?)",
                      "args": [k, v]} for k, v in state_pairs]
            _req = urllib.request.Request(  # F4: removed unused _body
                f"{_url}/v2/pipeline",
                data=json.dumps({"requests": [{"type":"execute","stmt":{"sql":s["sql"],"args":[{"type":"text","value":v} for v in s["args"]]}} for s in stmts] + [{"type":"close"}]}).encode(),
                headers={"Authorization": f"Bearer {_token}", "Content-Type": "application/json"},
                method="POST"
            )
            urllib.request.urlopen(_req, timeout=5)
            log.info("[Shutdown] saved to Turso (sync). Bye!")
        except Exception as ex:
            log.error(f"[Shutdown] turso sync save failed: {ex}")
    os._exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT,  handle_shutdown)
    # J5/H3: warn about auth mode on startup
    if not DASHBOARD_TOKEN:
        _insecure = os.getenv("ALLOW_INSECURE_DASHBOARD", "").lower() == "true"
        if _insecure:
            log.warning("[Security] DASHBOARD_TOKEN not set + ALLOW_INSECURE_DASHBOARD=true — dashboard fully open!")
        else:
            log.warning("[Security] DASHBOARD_TOKEN not set — dashboard returns 401 until token is set (or ALLOW_INSECURE_DASHBOARD=true).")

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(CommandHandler("scan",   cmd_scan))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("now",    cmd_now))
    app.add_handler(CommandHandler("pnl",    cmd_pnl))
    app.add_handler(CommandHandler("lines",  cmd_lines))
    app.add_handler(CommandHandler("trades", cmd_trades))   # v10-11
    app.add_handler(CommandHandler("settle", cmd_settle))   # v10-10
    _app = app

    # Railway/Render: ใช้ polling เสมอ (single-port compatible)
    log.info("[Bot] Polling mode (Railway/Render single-port compatible)")
    app.run_polling(drop_pending_updates=True)
