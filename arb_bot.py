"""
╔══════════════════════════════════════════════════════════════════════╗
║  ARB BOT v8.0  —  Sharp Edition                                      ║
║  1.  Odds Staleness Check    7.  Line Movement Alert (Pinnacle)     ║
║  2.  Max Odds Filter         8.  Dashboard History Chart            ║
║  3.  Alert Cooldown          9.  Multi-chat Support                 ║
║  4.  P&L Tracker             10. Reverse Line Movement (RLM)        ║
║  5.  Max Stake per Book      11. Steam Move Alert                   ║
║  6.  Dynamic Commission      12. CLV Tracker                        ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio, json, logging, os, re, signal, sqlite3, threading, time, uuid
try:
    import libsql_client as turso_client
    HAS_TURSO = True
except ImportError:
    HAS_TURSO = False
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass, field
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler

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
BANKROLL_THB    = _d("BANKROLL_THB", "2000000")  # แนะนำให้ตั้งไว้ที่ 2 ล้านเพื่อให้ Kelly คำนวณได้ยอดหมื่นต้นๆ
USE_KELLY       = _s("USE_KELLY", "true").lower() == "true"
MIN_KELLY_STAKE = _d("MIN_KELLY_STAKE", "10000") # บังคับขั้นต่ำ 10,000 บาท
MAX_KELLY_STAKE = _d("MAX_KELLY_STAKE", "50000") # เพดานสูงสุดต่อรอบ

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
ALL_CHAT_IDS = [CHAT_ID] + EXTRA_CHAT_IDS

_SPORTS_DEFAULT = (
    "basketball_nba,basketball_euroleague,basketball_ncaab,"
    "americanfootball_nfl,"
    "soccer_epl,soccer_uefa_champs_league,soccer_spain_la_liga,soccer_germany_bundesliga,"
    "soccer_fifa_world_cup,"
    "baseball_mlb,mma_mixed_martial_arts"
)
SPORTS     = [s.strip() for s in _s("SPORTS",_SPORTS_DEFAULT).split(",") if s.strip()]
BOOKMAKERS = _s("BOOKMAKERS","pinnacle,onexbet,dafabet")

SPORT_EMOJI = {
    "basketball_nba":"🏀","basketball_euroleague":"🏀","basketball_ncaab":"🏀",
    "americanfootball_nfl":"🏈","americanfootball_nfl_super_bowl_winner":"🏈",
    "soccer_epl":"⚽","soccer_uefa_champs_league":"⚽",
    "soccer_spain_la_liga":"⚽","soccer_germany_bundesliga":"⚽",
    "soccer_fifa_world_cup":"⚽",
    "tennis_atp_wimbledon":"🎾","tennis_wta":"🎾",
    "baseball_mlb":"⚾","mma_mixed_martial_arts":"🥊",
    "esports_csgo":"🎮","esports_dota2":"🎮","esports_lol":"🎮",
}

# กีฬาที่ควรเน้น H2H/Moneyline (Sharp money เข้ามากที่ตลาดนี้)
H2H_FOCUS_SPORTS = {
    "basketball_nba", "basketball_euroleague", "basketball_ncaab",
    "tennis_atp_wimbledon", "tennis_wta",
    "americanfootball_nfl",
}

# 6. Commission แบบ dynamic (อ่านจาก env ได้)
COMMISSION = {
    "polymarket": _d("FEE_POLYMARKET","0.02"),
    "pinnacle":   _d("FEE_PINNACLE",  "0.00"),
    "onexbet":    _d("FEE_1XBET",     "0.00"),
    "1xbet":      _d("FEE_1XBET",     "0.00"),
    "dafabet":    _d("FEE_DAFABET",   "0.00"),
}

MAX_STAKE_MAP = {
    "pinnacle": MAX_STAKE_PINNACLE,
    "onexbet":  MAX_STAKE_1XBET,
    "1xbet":    MAX_STAKE_1XBET,
    "dafabet":  MAX_STAKE_DAFABET,
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


# ══════════════════════════════════════════════════════════════════
#  STATE
# ══════════════════════════════════════════════════════════════════
pending:           dict[str, ArbOpportunity] = {}
seen_signals:      set[str]                  = set()
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
    settled_at TEXT, created_at TEXT
);
CREATE TABLE IF NOT EXISTS opportunity_log (
    id TEXT PRIMARY KEY, event TEXT, sport TEXT, profit_pct REAL,
    leg1_bm TEXT, leg1_odds REAL, leg2_bm TEXT, leg2_odds REAL,
    stake1_thb INTEGER, stake2_thb INTEGER, created_at TEXT,
    status TEXT DEFAULT 'pending'
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

# ── Turso async client (ใช้ถ้ามี TURSO_URL) ──────────────────────
_turso: Optional[object] = None

async def turso_init():
    global _turso
    if not USE_TURSO or not HAS_TURSO:
        return
    try:
        _turso = turso_client.create_client(
            url=TURSO_URL, auth_token=TURSO_TOKEN
        )
        for stmt in CREATE_TABLES_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                await _turso.execute(stmt)
        log.info("[DB] Turso connected ✅")
    except Exception as e:
        log.error(f"[DB] Turso init failed: {e} — fallback to SQLite")
        _turso = None

async def turso_exec(sql: str, params: tuple = ()):
    """Execute write query (Turso หรือ SQLite)"""
    if _turso:
        try:
            await _turso.execute(sql, list(params))
            return
        except Exception as e:
            log.error(f"[DB] turso_exec: {e}")
    # SQLite fallback
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as con:
            con.execute(sql, params)
            con.commit()
    except Exception as e:
        log.error(f"[DB] sqlite_exec: {e}")

async def turso_query(sql: str, params: tuple = ()) -> list:
    """Execute read query (Turso หรือ SQLite)"""
    if _turso:
        try:
            rs = await _turso.execute(sql, list(params))
            return [tuple(row.values()) for row in rs.rows]
        except Exception as e:
            log.error(f"[DB] turso_query: {e}")
    # SQLite fallback
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
            for stmt in CREATE_TABLES_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    con.execute(stmt)
            con.commit()
        log.info(f"[DB] SQLite local at {DB_PATH}")
    except Exception as e:
        log.error(f"[DB] local init: {e}")

def db_init():
    if not USE_TURSO:
        db_init_local()

# ── Write helpers ─────────────────────────────────────────────────
def db_save_trade(t: "TradeRecord"):
    asyncio.get_running_loop().create_task(_async_save_trade(t))

async def _async_save_trade(t: "TradeRecord"):
    await turso_exec(
        "INSERT OR REPLACE INTO trade_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (t.signal_id,t.event,t.sport,t.leg1_bm,t.leg2_bm,
         t.leg1_team,t.leg2_team,
         t.leg1_odds,t.leg2_odds,t.stake1_thb,t.stake2_thb,
         t.profit_pct,t.status,t.clv_leg1,t.clv_leg2,
         t.actual_profit_thb,t.settled_at,t.created_at)
    )

def db_save_opportunity(opp: dict):
    asyncio.get_running_loop().create_task(_async_save_opp(opp))

async def _async_save_opp(opp: dict):
    await turso_exec(
        "INSERT OR REPLACE INTO opportunity_log VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (opp["id"],opp["event"],opp["sport"],opp["profit_pct"],
         opp["leg1_bm"],opp["leg1_odds"],opp["leg2_bm"],opp["leg2_odds"],
         opp["stake1_thb"],opp["stake2_thb"],opp["created_at"],opp["status"])
    )

def db_update_opp_status(signal_id: str, status: str):
    asyncio.get_running_loop().create_task(
        turso_exec("UPDATE opportunity_log SET status=? WHERE id=?", (status, signal_id))
    )

def db_save_line_movement(lm: "LineMovement"):
    asyncio.get_running_loop().create_task(_async_save_lm(lm))

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
    asyncio.get_running_loop().create_task(
        turso_exec("INSERT OR REPLACE INTO bot_state VALUES (?,?)", (key, value))
    )

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
        trades_rows = await turso_query(
            "SELECT * FROM trade_records ORDER BY created_at DESC LIMIT 500")
        trades = []
        for r in trades_rows:
            # รองรับ DB เก่า (16 col) และใหม่ (18 col)
            if len(r) >= 18:
                trades.append(TradeRecord(
                    signal_id=r[0],event=r[1],sport=r[2],leg1_bm=r[3],leg2_bm=r[4],
                    leg1_team=r[5] or "",leg2_team=r[6] or "",
                    leg1_odds=r[7],leg2_odds=r[8],stake1_thb=r[9],stake2_thb=r[10],
                    profit_pct=r[11],status=r[12],clv_leg1=r[13],clv_leg2=r[14],
                    actual_profit_thb=r[15],settled_at=r[16],created_at=r[17]))
            else:
                # DB เก่า — ไม่มี leg1_team/leg2_team
                ev = r[1] if len(r)>1 else ""
                parts = ev.split(" vs ")
                trades.append(TradeRecord(
                    signal_id=r[0],event=ev,sport=r[2],leg1_bm=r[3],leg2_bm=r[4],
                    leg1_team=parts[0] if parts else "",
                    leg2_team=parts[1] if len(parts)>1 else "",
                    leg1_odds=r[5],leg2_odds=r[6],stake1_thb=r[7],stake2_thb=r[8],
                    profit_pct=r[9],status=r[10],clv_leg1=r[11],clv_leg2=r[12],
                    actual_profit_thb=r[13],settled_at=r[14],created_at=r[15]))

        opps_rows = await turso_query(
            "SELECT * FROM opportunity_log ORDER BY created_at DESC LIMIT 100")
        opps = [{"id":r[0],"event":r[1],"sport":r[2],"profit_pct":r[3],
                 "leg1_bm":r[4],"leg1_odds":r[5],"leg2_bm":r[6],"leg2_odds":r[7],
                 "stake1_thb":r[8],"stake2_thb":r[9],"created_at":r[10],"status":r[11]}
                for r in opps_rows]

        lm_rows = await turso_query(
            "SELECT * FROM line_movements ORDER BY ts DESC LIMIT 200")
        lms = [LineMovement(
            event=r[1],sport=r[2],bookmaker=r[3],outcome=r[4],
            odds_before=Decimal(str(r[5])),odds_after=Decimal(str(r[6])),
            pct_change=Decimal(str(r[7])),direction=r[8],
            is_steam=bool(r[9]),is_rlm=bool(r[10]),ts=r[11])
               for r in lm_rows]

        log.info(f"[DB] loaded: trades={len(trades)}, opps={len(opps)}, moves={len(lms)}")
        return trades, opps, lms
    except Exception as e:
        log.error(f"[DB] load_all: {e}")
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
async def detect_line_movements(odds_by_sport: dict):
    """
    เปรียบเทียบ odds ใหม่กับ history
    ตรวจจับ: Line Move, Steam Move, Reverse Line Movement
    พร้อมจัดเกรดสัญญาณ (A/B/C) และวิเคราะห์จังหวะเวลา
    """
    new_movements: list[tuple[LineMovement, dict]] = []  # (lm, context)
    now = datetime.now(timezone.utc)

    for sport, events in odds_by_sport.items():
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
                                    num_bm_moved = len(steam_tracker[steam_key])
                                    is_steam = num_bm_moved >= 2

                                    # 10. RLM: odds ขยับ反向กับ public bet
                                    # ถ้า odds ลง (favourite กลายเป็น underdog) = sharp money เดิน
                                    is_rlm = pct < -LINE_MOVE_THRESHOLD and bk == "pinnacle"

                                    lm = LineMovement(
                                        event=ename, sport=sport,
                                        bookmaker=bn, outcome=outcome,
                                        odds_before=old_odds, odds_after=new_odds,
                                        pct_change=pct, direction=direction,
                                        is_steam=is_steam, is_rlm=is_rlm,
                                    )
                                    ctx = {
                                        "commence_time": commence,
                                        "num_bm_moved": num_bm_moved,
                                        "bm_key": bk,
                                    }
                                    new_movements.append((lm, ctx))
                                    line_movements.append(lm)
                                    db_save_line_movement(lm)  # 💾
                                    log.info(f"[LineMove] {ename} | {bn} {outcome} {float(old_odds):.3f}→{float(new_odds):.3f} ({pct:.1%}) {'🌊STEAM' if is_steam else ''} {'🔄RLM' if is_rlm else ''}")

                        # อัพเดท history
                        if hist_key not in odds_history:
                            odds_history[hist_key] = {}
                        odds_history[hist_key][bk] = new_odds

    # ส่ง Telegram alert สำหรับ line movements
    if new_movements and _app:
        await send_line_move_alerts(new_movements)

    # จำกัด history
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

        # จัดเกรดสัญญาณ
        grade, grade_emoji, reasons = grade_signal(
            lm, liquidity_usd=0,
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
                ct = datetime.fromisoformat(commence_time.replace("Z","+00:00"))
                mins = (ct - datetime.now(timezone.utc)).total_seconds() / 60
                if mins > 0:
                    if mins < 60:
                        time_info = f"⏰ เริ่มใน {int(mins)} นาที"
                    else:
                        time_info = f"📅 {commence_time[:16].replace('T',' ')} UTC"
            except Exception:
                pass

        msg = (
            f"{'  '.join(tags)}\n"
            f"{grade_emoji} *เกรด {grade}* {'— 🔥 สัญญาณแข็ง!' if grade == 'A' else '— สัญญาณพอใช้' if grade == 'B' else ''}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{sport_emoji} `{lm.event}`\n"
            f"📡 {lm.bookmaker} — *{lm.outcome}*\n"
            f"📉 `{float(lm.odds_before):.3f}` → `{float(lm.odds_after):.3f}` ({pct_str}) {lm.direction}\n"
        )
        if time_info:
            msg += f"{time_info}\n"

        # แสดงเหตุผลของเกรด
        msg += f"\n📋 *วิเคราะห์สัญญาณ:*\n"
        for reason in reasons:
            msg += f"  {reason}\n"

        # คำแนะนำสำหรับ Grade A/B
        if grade in ("A", "B") and (lm.is_rlm or lm.is_steam):
            action = "BET" if lm.pct_change < 0 else "FADE"
            target = lm.outcome
            if lm.pct_change < 0:
                msg += (f"\n💡 *แนะนำ:* เดิมพัน *{target}* (odds ลง = เงินใหญ่เดิน)\n"
                        f"Soft books ยังไม่ตาม → โอกาส value bet!\n")
            else:
                msg += (f"\n💡 *สังเกต:* odds ขึ้น → อาจเป็น value ฝั่งตรงข้าม\n")

            # Direct betting links
            msg += f"\n🔗 *วางเดิมพันได้ที่:*\n"
            msg += build_betting_links(lm.event, lm.outcome, lm.sport, lm.odds_after, bm_key)
            msg += "\n"

        # H2H Focus note
        if lm.sport in H2H_FOCUS_SPORTS:
            msg += f"\n🎯 _กีฬานี้ Sharp money เน้นตลาด H2H — สัญญาณน่าเชื่อถือ_"

        for cid in ALL_CHAT_IDS:
            try:
                await _app.bot.send_message(chat_id=cid, text=msg, parse_mode="Markdown")
                await asyncio.sleep(0.3)
            except Exception as e:
                log.error(f"[LineMove] alert error: {e}")


# ══════════════════════════════════════════════════════════════════
#  12. CLV TRACKER
# ══════════════════════════════════════════════════════════════════
def update_clv(event: str, outcome: str, bookmaker: str, final_odds: Decimal):
    """บันทึก closing odds เพื่อคำนวณ CLV"""
    key = f"{event}|{outcome}"
    if key not in closing_odds:
        closing_odds[key] = {}
    closing_odds[key][bookmaker.lower()] = final_odds


def calc_clv(trade: TradeRecord) -> tuple[Optional[float], Optional[float]]:
    """
    CLV = (odds_got / closing_odds - 1) × 100%
    บวก = เอาชนะตลาด | ลบ = แพ้ตลาด
    """
    def _clv(event, outcome, bm, odds_got):
        key = f"{event}|{outcome}"
        co  = closing_odds.get(key, {}).get(bm.lower())
        if co and co > 0:
            return round((float(odds_got) / float(co) - 1) * 100, 2)
        return None

    clv1 = _clv(trade.event, trade.leg1_bm, trade.leg1_bm, trade.leg1_odds)
    clv2 = _clv(trade.event, trade.leg2_bm, trade.leg2_bm, trade.leg2_odds)
    return clv1, clv2


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


def build_betting_links(event_name: str, outcome: str, sport: str,
                        odds: Decimal, bookmaker_key: str = "") -> str:
    """สร้างลิงค์ตรงไปหน้า betting สำหรับ RLM/Steam signal"""
    links = []
    parts = event_name.split(" vs ")

    # Pinnacle
    pin_sport = "basketball" if "basketball" in sport else \
                "soccer" if "soccer" in sport else \
                "american-football" if "americanfootball" in sport else \
                "baseball" if "baseball" in sport else \
                "tennis" if "tennis" in sport else \
                "mixed-martial-arts" if "mma" in sport else "sports"
    links.append(f"  🔵 [Pinnacle](https://www.pinnacle.com/en/{pin_sport})")

    # 1xBet
    xbet_sport = "basketball" if "basketball" in sport else \
                 "soccer" if "soccer" in sport else \
                 "american-football" if "americanfootball" in sport else \
                 "baseball" if "baseball" in sport else \
                 "tennis" if "tennis" in sport else \
                 "mixed-martial-arts" if "mma" in sport else "sports"
    links.append(f"  🟠 [1xBet](https://1xbet.com/en/line/{xbet_sport})")

    # Dafabet
    links.append(f"  🟢 [Dafabet](https://www.dafabet.com/en/sports)")

    # Polymarket (ถ้าเป็นกีฬาที่มี market)
    if parts:
        search_q = parts[0].replace(" ", "+")
        links.append(f"  🟣 [Polymarket](https://polymarket.com/search?query={search_q})")

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


async def async_fetch_polymarket(session: aiohttp.ClientSession) -> list[dict]:
    """ดึง Polymarket markets พร้อม liquidity จริง"""
    try:
        # Step 1: ดึง sports markets เท่านั้น
        async with session.get(
            "https://clob.polymarket.com/markets",
            params={"active": True, "closed": False, "tag_slug": "sports"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            data = await r.json(content_type=None)
            markets = data.get("data", [])

        if not markets:
            # fallback — ดึงทั้งหมดถ้า tag ไม่ work
            async with session.get(
                "https://clob.polymarket.com/markets",
                params={"active": True, "closed": False},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                data = await r.json(content_type=None)
                markets = data.get("data", [])

        # Step 2: ดึง fee จริง (Polymarket fee 2% standard แต่บาง market ต่างกัน)
        enriched = []
        for m in markets[:80]:  # limit 80 เพื่อไม่ให้ช้า
            tokens = m.get("tokens", [])
            if len(tokens) < 2: continue

            # ดึง fee rate จาก market data
            fee_rate = float(m.get("maker_base_fee", 0)) + float(m.get("taker_base_fee", 200))
            fee_pct  = fee_rate / 10000  # basis points → decimal

            # ดึง volume 24h เป็น proxy ของ liquidity
            volume_24h = float(m.get("volume_num_24hr", 0) or 0)
            total_vol  = float(m.get("volume", 0) or 0)

            # กรอง market ที่ volume ต่ำเกินไป (< $500 USD)
            MIN_VOLUME = 500
            if volume_24h < MIN_VOLUME and total_vol < MIN_VOLUME * 10:
                continue

            # คำนวณ mid price จาก token prices
            p_a = float(tokens[0].get("price", 0))
            p_b = float(tokens[1].get("price", 0))
            if p_a <= 0.01 or p_b <= 0.01: continue  # กรอง odds ที่สูงเกิน (>100x)

            m["_fee_pct"]    = fee_pct
            m["_volume_24h"] = volume_24h
            m["_liquidity"]  = min(volume_24h, total_vol / 30)  # est. daily liquidity
            enriched.append(m)

        log.info(f"[Polymarket] markets={len(markets)} | filtered={len(enriched)} | sports only")
        return enriched

    except Exception as e:
        log.debug(f"[Polymarket] {e}")
        return []

async def fetch_all_async(sports: list[str]) -> tuple[dict, list]:
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[async_fetch_odds(session, s) for s in sports],
            async_fetch_polymarket(session),
        )
    return {s: results[i] for i,s in enumerate(sports)}, results[-1]


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
    """
    import random
    step = Decimal("500") if amount < Decimal("50000") else Decimal("1000")
    # ปัดลงก่อน แล้วสุ่ม +0 หรือ +1 step (50/50)
    base = (amount // step) * step
    jitter = step if random.random() < 0.5 else Decimal("0")
    return base + jitter


def calc_kelly_stake(odds_a: Decimal, odds_b: Decimal, profit_pct: Decimal) -> Decimal:
    """
    Kelly Criterion สำหรับ Arbitrage
    ใน arb จริงๆ edge = profit_pct (guaranteed)
    Kelly = edge / odds_range → แต่ใช้ fractional Kelly เพื่อความปลอดภัย

    Full Kelly = (edge) / (1 - 1/max_odds)
    Fractional = Full Kelly × KELLY_FRACTION
    """
    if not USE_KELLY:
        return TOTAL_STAKE

    edge = float(profit_pct)  # guaranteed edge
    # Kelly stake as fraction of bankroll
    # สำหรับ arb: f* = edge / (1 - min_implied_prob)
    min_prob = float(min(Decimal("1")/odds_a, Decimal("1")/odds_b))
    if min_prob >= 1 or edge <= 0:
        return TOTAL_STAKE

    full_kelly = edge / (1 - min_prob)
    frac_kelly = full_kelly * float(KELLY_FRACTION)

    # Kelly stake in THB
    kelly_thb  = Decimal(str(frac_kelly)) * BANKROLL_THB
    kelly_thb  = max(MIN_KELLY_STAKE, min(MAX_KELLY_STAKE, kelly_thb))
    kelly_thb  = natural_round(kelly_thb)  # พรางตัว — ปัดเป็นเลขกลม 500/1000
    kelly_thb  = max(MIN_KELLY_STAKE, kelly_thb)  # ตรวจ MIN อีกรอบหลัง round

    log.info(f"[Kelly] edge={edge:.2%} full={full_kelly:.3f} frac={frac_kelly:.3f} stake=฿{int(kelly_thb):,} (natural_round)")
    return kelly_thb


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
def is_stale(commence_time: str) -> bool:
    """1. เช็ค odds staleness"""
    try:
        ct = datetime.fromisoformat(commence_time.replace("Z","+00:00"))
        # ถ้าแมตช์เริ่มไปแล้วเกิน 3 ชั่วโมง ถือว่า stale
        if ct < datetime.now(timezone.utc) - timedelta(hours=3):
            return True
    except Exception:
        pass
    return False

def is_valid_odds(odds: Decimal) -> bool:
    """2. กรอง odds ที่ผิดปกติ"""
    return MIN_ODDS_ALLOWED <= odds <= MAX_ODDS_ALLOWED

def is_on_cooldown(event: str, bm1: str, bm2: str) -> bool:
    """3. เช็ค alert cooldown"""
    key      = f"{event}|{bm1}|{bm2}"
    last     = alert_cooldown.get(key)
    if last and (datetime.now(timezone.utc) - last).total_seconds() < ALERT_COOLDOWN_MIN * 60:
        return True
    return False

# Minimum liquidity USD สำหรับ Polymarket (ตั้งใน Railway)
POLY_MIN_LIQUIDITY     = float(os.getenv("POLY_MIN_LIQUIDITY", "1000"))
# Liquidity ขั้นต่ำสำหรับ RLM signal — ต่ำกว่านี้ถือว่าสัญญาณปลอม
RLM_MIN_LIQUIDITY_USD  = float(os.getenv("RLM_MIN_LIQUIDITY_USD", "10000"))

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
    pa       = Decimal(str(tokens[0].get("price",0)))
    pb       = Decimal(str(tokens[1].get("price",0)))
    if pa <= 0 or pb <= 0: return None

    # ✅ ใช้ fee จริงจาก API แทน hardcode 2%
    fee_pct  = Decimal(str(best.get("_fee_pct", 0.02)))
    liq_usd  = best.get("_liquidity", 0)
    vol_24h  = best.get("_volume_24h", 0)

    def poly_odds(p: Decimal) -> tuple[Decimal, Decimal]:
        odds_raw = (Decimal("1") / p).quantize(Decimal("0.001"))
        # ใช้ fee จริง แทน hardcode
        odds_eff = (odds_raw * (Decimal("1") - fee_pct)).quantize(Decimal("0.001"))
        return odds_raw, odds_eff

    slug    = best.get("slug","")
    odds_raw_a, odds_a = poly_odds(pa)
    odds_raw_b, odds_b = poly_odds(pb)

    return {
        "market_url": f"https://polymarket.com/event/{slug}",
        "fee_pct":    float(fee_pct),
        "liquidity":  liq_usd,
        "volume_24h": vol_24h,
        "team_a": {"name": tokens[0].get("outcome",ta),
                   "odds_raw": odds_raw_a, "odds": odds_a,
                   "token_id": tokens[0].get("token_id","")},
        "team_b": {"name": tokens[1].get("outcome",tb),
                   "odds_raw": odds_raw_b, "odds": odds_b,
                   "token_id": tokens[1].get("token_id","")},
    }

def scan_all(odds_by_sport: dict, poly_markets: list) -> list[ArbOpportunity]:
    found = []
    for sport_key, events in odds_by_sport.items():
        for event in events:
            home       = event.get("home_team","")
            away       = event.get("away_team","")
            event_name = f"{home} vs {away}"
            commence   = event.get("commence_time","")[:16].replace("T"," ")

            # 1. Staleness check
            if is_stale(event.get("commence_time","")):
                log.debug(f"[Stale] {event_name}")
                continue

            best: dict[str, OddsLine] = {}
            for bm in event.get("bookmakers",[]):
                bk, bn = bm.get("key",""), bm.get("title", bm.get("key",""))
                for mkt in bm.get("markets",[]):
                    if mkt.get("key") != "h2h": continue
                    for out in mkt.get("outcomes",[]):
                        name     = out.get("name","")
                        # กรอง Draw/Tie
                        if name.lower() in ("draw","tie","no contest","nc"): continue
                        odds_raw = Decimal(str(out.get("price",1)))
                        # 2. Odds filter
                        if not is_valid_odds(odds_raw): continue
                        odds_eff = apply_slippage(odds_raw, bk)
                        if name not in best or odds_eff > best[name].odds:
                            best[name] = OddsLine(bookmaker=bn, outcome=name,
                                                  odds=odds_eff, odds_raw=odds_raw,
                                                  raw={"bm_key":bk,"event_id":event.get("id","")},
                                                  last_update=commence)

            poly = find_polymarket(event_name, poly_markets)
            if poly:
                for side, team in [("team_a",home),("team_b",away)]:
                    p = poly[side]
                    if not is_valid_odds(p["odds"]): continue
                    matched = next((k for k in best if fuzzy_match(p["name"],k)), team)
                    if matched not in best or p["odds"] > best[matched].odds:
                        best[matched] = OddsLine(bookmaker="Polymarket", outcome=matched,
                                                 odds=p["odds"], odds_raw=p["odds_raw"],
                                                 market_url=poly["market_url"],
                                                 raw={"token_id":p["token_id"]})

            outcomes = list(best.keys())
            for i in range(len(outcomes)):
                for j in range(i+1, len(outcomes)):
                    a, b = outcomes[i], outcomes[j]
                    if best[a].bookmaker == best[b].bookmaker: continue
                    # 3. Cooldown check
                    if is_on_cooldown(event_name, best[a].bookmaker, best[b].bookmaker): continue
                    profit, s_a, s_b = calc_arb(best[a].odds, best[b].odds)
                    if profit >= MIN_PROFIT_PCT:
                        # Kelly — ปรับ total stake ตาม edge
                        kelly_total = calc_kelly_stake(best[a].odds, best[b].odds, profit)
                        if kelly_total != TOTAL_STAKE:
                            profit, s_a, s_b = calc_arb_fixed(best[a].odds, best[b].odds,
                                                               kelly_total / USD_TO_THB)
                        # 5. Apply max stake — recalc ใหม่ถ้าถูก cap
                        s_a_capped = apply_max_stake(s_a, best[a].bookmaker)
                        s_b_capped = apply_max_stake(s_b, best[b].bookmaker)
                        # ถ้า cap ทำให้ stake เปลี่ยน → recalculate อีกรอบ
                        if s_a_capped != s_a or s_b_capped != s_b:
                            # หา limited stake แล้ว recalc ให้สมดุล
                            if s_a_capped < s_a:
                                # Leg A ถูก cap → จำกัด total stake แล้ว recalc
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
                            # ตรวจสอบอีกครั้งว่ายังกำไรอยู่ไหม
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
                        # บันทึก cooldown
                        alert_cooldown[f"{event_name}|{best[a].bookmaker}|{best[b].bookmaker}"] = datetime.now(timezone.utc)
                        log.info(f"[ARB] {event_name} | profit={profit:.2%}")
    return found


# ══════════════════════════════════════════════════════════════════
#  SEND ALERT
# ══════════════════════════════════════════════════════════════════
async def send_alert(opp: ArbOpportunity):
    pending[opp.signal_id] = opp

    # ── คำนวณ mins_to_start ก่อนใช้ ──
    try:
        commence_dt = datetime.fromisoformat(
            opp.commence.replace(" ","T") + ":00+00:00"
        )
        mins_to_start = (commence_dt - datetime.now(timezone.utc)).total_seconds() / 60
    except Exception:
        mins_to_start = 999

    entry = {
        "id": opp.signal_id, "event": opp.event, "sport": opp.sport,
        "profit_pct": float(opp.profit_pct),
        "leg1_bm": opp.leg1.bookmaker, "leg1_odds": float(opp.leg1.odds),
        "leg2_bm": opp.leg2.bookmaker, "leg2_odds": float(opp.leg2.odds),
        "stake1_thb": int(opp.stake1*USD_TO_THB),
        "stake2_thb": int(opp.stake2*USD_TO_THB),
        "created_at": opp.created_at, "status": "pending",
        "mins_to_start": round(mins_to_start) if mins_to_start < 9999 else 9999,
    }
    opportunity_log.append(entry)
    db_save_opportunity(entry)   # 💾 save to DB
    if len(opportunity_log) > 100: opportunity_log.pop(0)

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
    w1 = (opp.stake1*opp.leg1.odds*USD_TO_THB).quantize(Decimal("1"))
    w2 = (opp.stake2*opp.leg2.odds*USD_TO_THB).quantize(Decimal("1"))
    tt = s1 + s2  # ใช้ stake จริง (ไม่ใช่ TOTAL_STAKE_THB) — สำคัญมากเมื่อใช้ Kelly

    urgent_prefix = f"{urgency_tag}\n" if urgency_tag else ""
    msg = (
        f"{urgent_prefix}"
        f"{emoji} *ARB FOUND — {opp.profit_pct:.2%}* _(หลัง fee)_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 {opp.commence} UTC {urgency_note}\n"
        f"🏆 `{opp.event}`\n"
        f"💵 ทุน: *฿{int(tt):,}* {'_(Kelly)_' if USE_KELLY else ''}  |  Credits: {api_remaining}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"{'ช่องทาง':<12} {'ฝั่ง':<15} {'Odds':>5} {'วาง':>8} {'ได้':>8}\n"
        f"{'─'*51}\n"
        f"{'🔵 '+opp.leg1.bookmaker:<12} {opp.leg1.outcome:<15} {float(opp.leg1.odds):>5.3f} {'฿'+str(int(s1)):>8} {'฿'+str(int(w1)):>8}\n"
        f"{'🟠 '+opp.leg2.bookmaker:<12} {opp.leg2.outcome:<15} {float(opp.leg2.odds):>5.3f} {'฿'+str(int(s2)):>8} {'฿'+str(int(w2)):>8}\n"
        f"{'─'*51}\n"
        f"{'รวม':<34} {'฿'+str(int(tt)):>8}\n"
        f"```\n"
        f"📊 ไม่ว่าใครชนะ\n"
        f"   {opp.leg1.outcome} → ฿{int(w1):,} *(+฿{int(w1-tt):,})*\n"
        f"   {opp.leg2.outcome} → ฿{int(w2):,} *(+฿{int(w2-tt):,})*\n"
        f"🔗 {opp.leg1.market_url or '—'}\n"
        f"🆔 `{opp.signal_id}`"
    )
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{opp.signal_id}"),
        InlineKeyboardButton("❌ Reject",  callback_data=f"reject:{opp.signal_id}"),
    ]])
    # 9. Multi-chat
    for cid in ALL_CHAT_IDS:
        try:
            await _app.bot.send_message(chat_id=cid, text=msg, parse_mode="Markdown",
                                        reply_markup=keyboard if cid==CHAT_ID else None)
        except Exception as e:
            log.error(f"[Alert] chat {cid}: {e}")


# ══════════════════════════════════════════════════════════════════
#  EXECUTE
# ══════════════════════════════════════════════════════════════════
async def execute_both(opp: ArbOpportunity) -> str:
    s1_raw = (opp.stake1*USD_TO_THB).quantize(Decimal("1"))
    s2_raw = (opp.stake2*USD_TO_THB).quantize(Decimal("1"))
    # 🎭 Natural rounding — ป้องกันโดนจับว่าใช้บอท
    s1 = natural_round(s1_raw)
    s2 = natural_round(s2_raw)
    w1 = (opp.stake1*opp.leg1.odds*USD_TO_THB).quantize(Decimal("1"))
    w2 = (opp.stake2*opp.leg2.odds*USD_TO_THB).quantize(Decimal("1"))
    tt = s1 + s2  # ใช้ stake จริง (ไม่ใช่ TOTAL_STAKE_THB)

    # บันทึก trade
    tr = TradeRecord(
        signal_id=opp.signal_id, event=opp.event, sport=opp.sport,
        leg1_bm=opp.leg1.bookmaker, leg2_bm=opp.leg2.bookmaker,
        leg1_team=opp.leg1.outcome,   # ✅ ชื่อทีม/นักกีฬาจริง
        leg2_team=opp.leg2.outcome,   # ✅ ชื่อทีม/นักกีฬาจริง
        leg1_odds=float(opp.leg1.odds_raw), leg2_odds=float(opp.leg2.odds_raw),
        stake1_thb=int(s1), stake2_thb=int(s2),
        profit_pct=float(opp.profit_pct), status="confirmed",
    )
    trade_records.append(tr)
    db_save_trade(tr)            # 💾 save to DB
    register_for_settlement(tr, opp.commence)  # 🏆 auto settle
    # อัพเดท opportunity_log
    for entry in opportunity_log:
        if entry["id"] == opp.signal_id:
            entry["status"] = "confirmed"
    db_update_opp_status(opp.signal_id, "confirmed")  # 💾

    def steps(leg, stake):
        bm  = leg.bookmaker.lower()
        eid = leg.raw.get("event_id","")
        bk  = leg.raw.get("bm_key", bm)
        cap = apply_max_stake(stake/USD_TO_THB, leg.bookmaker)*USD_TO_THB
        cap_note = f"\n  ⚠️ Capped ที่ ฿{int(cap):,}" if cap < stake else ""
        if "polymarket" in bm:
            link = leg.market_url or "https://polymarket.com"
            return f"  🔗 [เปิด Polymarket]({link})\n  2. เลือก *{leg.outcome}*\n  3. วาง ฿{int(stake)} USDC{cap_note}"
        elif "pinnacle" in bk:
            link = f"https://www.pinnacle.com/en/mixed-martial-arts/matchup/{eid}" if eid else "https://www.pinnacle.com"
            return f"  🔗 [เปิด Pinnacle]({link})\n  2. เลือก *{leg.outcome}* @ {leg.odds_raw}\n  3. วาง ฿{int(stake)}{cap_note}"
        elif "onexbet" in bk or "1xbet" in bm:
            link = f"https://1xbet.com/en/line/mixed-martial-arts/{eid}" if eid else "https://1xbet.com/en/line/mixed-martial-arts"
            return f"  🔗 [เปิด 1xBet]({link})\n  2. เลือก *{leg.outcome}* @ {leg.odds_raw}\n  3. วาง ฿{int(stake)}{cap_note}"
        elif "dafabet" in bk:
            return f"  🔗 [เปิด Dafabet](https://www.dafabet.com/en/sports/mma)\n  2. ค้นหา *{leg.outcome}*\n  3. วาง ฿{int(stake)}{cap_note}"
        return f"  1. เปิด {leg.bookmaker}\n  2. เลือก *{leg.outcome}* @ {leg.odds_raw}\n  3. วาง ฿{int(stake)}{cap_note}"

    return (
        f"📋 *วางเงิน — {opp.event}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔵 *{opp.leg1.bookmaker}*\n{steps(opp.leg1, s1)}\n\n"
        f"🟠 *{opp.leg2.bookmaker}*\n{steps(opp.leg2, s2)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 ทุน ฿{int(tt):,}\n"
        f"   {opp.leg1.outcome} ชนะ → ฿{int(w1):,} (+฿{int(w1-tt):,})\n"
        f"   {opp.leg2.outcome} ชนะ → ฿{int(w2):,} (+฿{int(w2-tt):,})"
    )


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try: action, sid = query.data.split(":",1)
    except Exception: return
    opp = pending.pop(sid, None)
    if not opp:
        await query.edit_message_text(query.message.text+"\n\n⚠️ หมดอายุ")
        return
    for entry in opportunity_log:
        if entry["id"] == sid: entry["status"] = action
    orig = query.message.text
    if action == "reject":
        tr_rej = TradeRecord(
            signal_id=sid, event=opp.event, sport=opp.sport,
            leg1_bm=opp.leg1.bookmaker, leg2_bm=opp.leg2.bookmaker,
            leg1_team=opp.leg1.outcome,
            leg2_team=opp.leg2.outcome,
            leg1_odds=float(opp.leg1.odds_raw), leg2_odds=float(opp.leg2.odds_raw),
            stake1_thb=int(opp.stake1*USD_TO_THB), stake2_thb=int(opp.stake2*USD_TO_THB),
            profit_pct=float(opp.profit_pct), status="rejected",
        )
        trade_records.append(tr_rej)
        db_save_trade(tr_rej)    # 💾
        db_update_opp_status(sid, "rejected")  # 💾
        await query.edit_message_text(orig+"\n\n❌ *REJECTED*", parse_mode="Markdown")
        return
    await query.edit_message_text(orig+"\n\n⏳ *กำลังเตรียม...*", parse_mode="Markdown")
    result = await execute_both(opp)
    await query.edit_message_text(orig+"\n\n✅ *CONFIRMED*\n\n"+result, parse_mode="Markdown")


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global auto_scan, quota_warned
    args = context.args
    if not args:
        s = "🟢" if auto_scan else "🔴"
        await update.message.reply_text(f"Auto scan: {s}\n/scan on หรือ /scan off")
        return
    if args[0].lower()=="on":
        auto_scan=True; quota_warned=False; seen_signals.clear()
        await update.message.reply_text(f"🟢 *Auto scan เปิด* — ทุก {SCAN_INTERVAL}s", parse_mode="Markdown")
    elif args[0].lower()=="off":
        auto_scan=False
        await update.message.reply_text("🔴 *Auto scan ปิด*", parse_mode="Markdown")


async def cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """4. /pnl — ดู P&L summary"""
    confirmed = [t for t in trade_records if t.status=="confirmed"]
    rejected  = [t for t in trade_records if t.status=="rejected"]
    total_profit = sum(t.profit_pct * (t.stake1_thb+t.stake2_thb) for t in confirmed)

    # CLV summary
    clv_values = []
    for t in confirmed:
        c1, c2 = calc_clv(t)
        if c1 is not None: clv_values.append(c1)
        if c2 is not None: clv_values.append(c2)
    avg_clv = sum(clv_values)/len(clv_values) if clv_values else None

    clv_str = f"{avg_clv:+.2f}%" if avg_clv is not None else "ยังไม่มีข้อมูล"
    # actual P&L จาก settled trades
    settled   = [t for t in confirmed if t.actual_profit_thb is not None]
    unsettled = [t for t in confirmed if t.actual_profit_thb is None]
    actual_profit = sum(t.actual_profit_thb for t in settled)
    win_trades    = [t for t in settled if t.actual_profit_thb >= 0]
    lose_trades   = [t for t in settled if t.actual_profit_thb < 0]
    win_rate      = len(win_trades)/len(settled)*100 if settled else 0

    await update.message.reply_text(
        f"💰 *P&L Summary*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Confirmed   : {len(confirmed)} trades\n"
        f"  └ Settled : {len(settled)} | Unsettled: {len(unsettled)}\n"
        f"  └ Win/Lose: {len(win_trades)}W / {len(lose_trades)}L ({win_rate:.0f}%)\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💵 Actual P&L  : *฿{actual_profit:+,}*\n"
        f"📊 Est. Profit : ฿{int(total_profit):,} _(ยังไม่ settle)_\n"
        f"📈 CLV avg     : {clv_str}\n"
        f"_(CLV บวก = เอาชนะตลาด)_",
        parse_mode="Markdown",
    )


async def cmd_lines(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """7. /lines — ดู line movements ล่าสุด"""
    recent = line_movements[-10:][::-1]
    if not recent:
        await update.message.reply_text("ยังไม่มี line movement ที่น่าสนใจ")
        return
    lines_text = ""
    for lm in recent:
        tags = ""
        if lm.is_steam: tags += "🌊"
        if lm.is_rlm:   tags += "🔄"
        pct = f"{lm.pct_change:+.1%}"
        lines_text += f"{tags} `{lm.event[:25]}` {lm.bookmaker} {pct}\n"
    await update.message.reply_text(
        f"📊 *Line Movements ล่าสุด*\n━━━━━━━━━━━━━━━━━━\n{lines_text}\n"
        f"🌊=Steam 🔄=RLM",
        parse_mode="Markdown",
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = "🟢 เปิด" if auto_scan else "🔴 ปิด"
    qpct = min(100, int(api_remaining/5))
    qbar = "█"*int(qpct/5)+"░"*(20-int(qpct/5))
    confirmed = len([t for t in trade_records if t.status=="confirmed"])
    await update.message.reply_text(
        f"📊 *ARB BOT v8.0*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Auto scan   : {s} ({SCAN_INTERVAL}s)\n"
        f"สแกนไปแล้ว  : {scan_count} รอบ\n"
        f"ล่าสุด      : {last_scan_time}\n"
        f"รอ confirm  : {len(pending)} | trade: {confirmed} | unsettled: {len(_pending_settlement)}\n"
        f"Line moves  : {len(line_movements)} events\n"
        f"Min profit  : {MIN_PROFIT_PCT:.1%} | Max odds: {MAX_ODDS_ALLOWED}\n"
        f"Cooldown    : {ALERT_COOLDOWN_MIN}m | Staleness: {MAX_ODDS_AGE_MIN}m\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Credits: *{api_remaining}*/500\n"
        f"[{qbar}]\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"/scan on·off | /now | /pnl | /lines",
        parse_mode="Markdown",
    )


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 *กำลังสแกน...*", parse_mode="Markdown")
    count = await do_scan()
    msg = f"✅ พบ *{count}* opportunity" if count else f"✅ ไม่พบ > {MIN_PROFIT_PCT:.1%}"
    await update.message.reply_text(msg, parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════════
#  SCAN CORE
# ══════════════════════════════════════════════════════════════════
async def do_scan() -> int:
    global scan_count, last_scan_time
    odds_by_sport, poly_markets = await fetch_all_async(SPORTS)

    # 7/10/11. Detect line movements (async, ไม่ block)
    asyncio.create_task(detect_line_movements(odds_by_sport))

    all_opps = scan_all(odds_by_sport, poly_markets)
    sent = 0
    for opp in sorted(all_opps, key=lambda x: x.profit_pct, reverse=True):
        key = f"{opp.event}|{opp.leg1.bookmaker}|{opp.leg2.bookmaker}"
        if key not in seen_signals:
            seen_signals.add(key)
            await send_alert(opp)
            await asyncio.sleep(1)
            sent += 1
    if len(seen_signals) > 500: seen_signals.clear()
    scan_count    += 1
    last_scan_time = datetime.now(timezone.utc).strftime("%d/%m %H:%M UTC")
    save_snapshot()   # 💾 บันทึก state
    return sent


# track events ที่รอดึง closing line
_closing_line_watch: dict[str, dict] = {}  # event_key → {sport, commence_dt, done}

async def watch_closing_lines():
    """ดึง closing line อัตโนมัติ 1 นาทีก่อนแข่ง"""
    while True:
        try:
            now = datetime.now(timezone.utc)
            to_fetch = []

            for key, info in list(_closing_line_watch.items()):
                if info.get("done"): continue
                mins_left = (info["commence_dt"] - now).total_seconds() / 60
                if mins_left <= 1:
                    to_fetch.append((key, info))
                    _closing_line_watch[key]["done"] = True

            if to_fetch:
                async with aiohttp.ClientSession() as session:
                    for key, info in to_fetch:
                        sport = info["sport"]
                        events = await async_fetch_odds(session, sport)
                        for event in events:
                            ename = f"{event.get('home_team','')} vs {event.get('away_team','')}"
                            if ename != info["event"]: continue
                            for bm in event.get("bookmakers", []):
                                bk = bm.get("key","")
                                for mkt in bm.get("markets",[]):
                                    if mkt.get("key") != "h2h": continue
                                    for out in mkt.get("outcomes",[]):
                                        update_clv(ename, out["name"], bk,
                                                   Decimal(str(out.get("price",1))))
                            log.info(f"[CLV] closing line saved: {ename}")
        except Exception as e:
            log.error(f"[CLV] watch_closing_lines crash: {e}", exc_info=True)

        await asyncio.sleep(30)


def register_closing_watch(opp: "ArbOpportunity"):
    """เพิ่ม event เข้า watchlist สำหรับ closing line"""
    try:
        commence_dt = datetime.fromisoformat(
            opp.commence.replace(" ","T") + ":00+00:00"
        )
        key = f"{opp.event}|{opp.sport}"
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
# track trades ที่รอ settle
_pending_settlement: dict[str, TradeRecord] = {}   # signal_id → trade


def register_for_settlement(trade: TradeRecord, commence: str):
    """เพิ่ม trade เข้า queue รอ settle หลังแข่งเสร็จ"""
    try:
        dt = datetime.fromisoformat(commence.replace(" ", "T") + ":00+00:00")
        _pending_settlement[trade.signal_id] = trade
        log.info(f"[Settle] registered: {trade.event} | {dt.strftime('%d/%m %H:%M')} UTC")
    except Exception as e:
        log.debug(f"[Settle] register error: {e}")


async def fetch_scores(sport: str, session: Optional[aiohttp.ClientSession] = None) -> list[dict]:
    """ดึงผลการแข่งขัน (scores) จาก Odds API"""
    async def _fetch(s: aiohttp.ClientSession):
        async with s.get(
            f"https://api.the-odds-api.com/v4/sports/{sport}/scores",
            params={"apiKey": ODDS_API_KEY, "daysFrom": 1},
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


def parse_winner(event: dict) -> Optional[str]:
    """แกะผลจาก scores endpoint — คืนชื่อทีมที่ชนะ"""
    scores = event.get("scores")
    if not scores:
        return None
    if not event.get("completed", False):
        return None
    # scores = [{"name": "TeamA", "score": "110"}, {"name": "TeamB", "score": "98"}]
    try:
        sorted_scores = sorted(scores, key=lambda x: float(x.get("score", 0)), reverse=True)
        return sorted_scores[0]["name"]  # ทีมที่ได้คะแนนสูงสุด
    except Exception:
        return None


def calc_actual_pnl(trade: TradeRecord, winner: str) -> int:
    """
    คำนวณกำไร/ขาดทุนจริง โดยใช้ชื่อทีมที่บันทึกไว้ใน trade

    arb ที่ดี → กำไรไม่ว่าใครชนะ
    แต่ถ้า stake ถูก cap หรือ odds เปลี่ยนก่อนวาง → อาจมีผิดพลาดได้
    """
    total_staked = trade.stake1_thb + trade.stake2_thb

    # match winner กับ leg1_team หรือ leg2_team (fuzzy)
    match_leg1 = fuzzy_match(winner, trade.leg1_team, threshold=0.5)
    match_leg2 = fuzzy_match(winner, trade.leg2_team, threshold=0.5)

    if match_leg1 and not match_leg2:
        # leg1 ชนะ → ได้ payout จาก stake1
        payout = trade.stake1_thb * trade.leg1_odds
        log.info(f"[Settle] {trade.event} → leg1 won ({trade.leg1_team})")
    elif match_leg2 and not match_leg1:
        # leg2 ชนะ → ได้ payout จาก stake2
        payout = trade.stake2_thb * trade.leg2_odds
        log.info(f"[Settle] {trade.event} → leg2 won ({trade.leg2_team})")
    else:
        # match ทั้งคู่หรือไม่ match เลย — ใช้ leg ที่ให้ payout สูงกว่า (conservative)
        payout1 = trade.stake1_thb * trade.leg1_odds
        payout2 = trade.stake2_thb * trade.leg2_odds
        payout  = min(payout1, payout2)  # worst case
        log.warning(f"[Settle] {trade.event} — winner '{winner}' ambiguous "
                    f"(leg1={trade.leg1_team}, leg2={trade.leg2_team}) using worst-case")

    profit = int(payout - total_staked)
    return profit


async def settle_completed_trades():
    """
    Loop ตรวจสอบผลการแข่งขัน ทุก 5 นาที
    เมื่อแข่งเสร็จ → คำนวณ actual P&L → แจ้ง Telegram → บันทึก DB
    """
    await asyncio.sleep(60)  # รอ bot start ก่อน
    log.info("[Settle] auto settlement loop started")

    while True:
        try:
            if not _pending_settlement:
                await asyncio.sleep(300)
                continue

            # รวม sports ที่ต้องดึงผล
            sports_needed = set(t.sport for t in _pending_settlement.values())
            all_scores: dict[str, list] = {}

            async with aiohttp.ClientSession() as session:
                for sport in sports_needed:
                    scores = await fetch_scores(sport, session=session)
                    all_scores[sport] = scores
                    await asyncio.sleep(1)  # ไม่ spam API

            settled_ids = []
            for signal_id, trade in list(_pending_settlement.items()):
                # หา event ที่ตรงกัน
                sport_scores = all_scores.get(trade.sport, [])
                matched_event = None

                for ev in sport_scores:
                    home = ev.get("home_team", "")
                    away = ev.get("away_team", "")
                    ev_name = f"{home} vs {away}"
                    if fuzzy_match(home, trade.event.split(" vs ")[0], 0.5) and \
                       fuzzy_match(away, trade.event.split(" vs ")[-1], 0.5):
                        matched_event = ev
                        break

                if not matched_event:
                    continue
                if not matched_event.get("completed", False):
                    # ยังไม่เสร็จ — เช็คว่านานเกิน 6 ชั่วโมงไหม (อาจ postponed)
                    try:
                        ct = datetime.fromisoformat(
                            matched_event.get("commence_time","").replace("Z","+00:00"))
                        if (datetime.now(timezone.utc) - ct).total_seconds() > 6 * 3600:
                            log.warning(f"[Settle] {trade.event} — เกิน 6h ยังไม่เสร็จ (postponed?)")
                    except Exception:
                        pass
                    continue

                # แมตช์เสร็จแล้ว!
                winner = parse_winner(matched_event)
                if not winner:
                    continue

                # คำนวณ P&L จริง
                actual_profit = calc_actual_pnl(trade, winner)
                total_staked  = trade.stake1_thb + trade.stake2_thb
                emoji_result  = "✅" if actual_profit >= 0 else "❌"
                sport_emoji   = SPORT_EMOJI.get(trade.sport, "🏆")

                # อัพเดท trade record
                trade.actual_profit_thb = actual_profit
                trade.settled_at        = datetime.now(timezone.utc).isoformat()
                db_save_trade(trade)
                settled_ids.append(signal_id)

                log.info(f"[Settle] {trade.event} | winner={winner} | profit=฿{actual_profit:+,}")

                # แจ้ง Telegram
                msg = (
                    f"{sport_emoji} *SETTLED* \u2014 {trade.event}\n"
                    f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                    f"\U0001f3c6 \u0e1c\u0e39\u0e49\u0e0a\u0e19\u0e30 : *{winner}*\n"
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
            for sid in settled_ids:
                _pending_settlement.pop(sid, None)

        except Exception as e:
            log.error(f"[Settle] crash in loop: {e}", exc_info=True)

        await asyncio.sleep(300)  # เช็คทุก 5 นาที


def periodic_cleanup():
    """ทำความสะอาด memory — เรียกทุกรอบ scan เพื่อป้องกัน leak ใน 24/7"""
    now = datetime.now(timezone.utc)
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
    if len(closing_odds) > 500:
        keys_to_remove = list(closing_odds.keys())[:-500]
        for k in keys_to_remove:
            del closing_odds[k]


async def scanner_loop():
    await asyncio.sleep(3)
    log.info(f"[Scanner] v8.0 | interval={SCAN_INTERVAL}s | sports={len(SPORTS)}")
    while True:
        if auto_scan:
            try: await do_scan()
            except Exception as e: log.error(f"[Scanner] {e}")
        periodic_cleanup()
        await asyncio.sleep(SCAN_INTERVAL)


# ══════════════════════════════════════════════════════════════════
#  8. DASHBOARD (ปรับปรุงใหม่พร้อมกราฟ + Line Movement section)
# ══════════════════════════════════════════════════════════════════
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>ARB BOT v8.0</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#e6edf3;font-family:'Segoe UI',sans-serif;padding:16px}
h1{color:#58a6ff;font-size:1.3rem;margin-bottom:2px}
.sub{color:#8b949e;font-size:.75rem;margin-bottom:12px}
.tabs{display:flex;gap:4px;margin-bottom:16px;border-bottom:1px solid #30363d;padding-bottom:0}
.tab{padding:8px 16px;cursor:pointer;color:#8b949e;font-size:.85rem;border-radius:6px 6px 0 0;border:1px solid transparent;border-bottom:none;margin-bottom:-1px}
.tab.active{color:#e6edf3;background:#161b22;border-color:#30363d;border-bottom-color:#161b22}
.tab:hover:not(.active){color:#e6edf3;background:#1c2128}
.page{display:none}.page.active{display:block}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin-bottom:12px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px}
.card .lbl{color:#8b949e;font-size:.68rem;text-transform:uppercase;letter-spacing:.5px}
.card .val{font-size:1.4rem;font-weight:700;margin-top:2px}
.card .sub2{color:#8b949e;font-size:.7rem;margin-top:2px}
.green{color:#3fb950}.red{color:#f85149}.yellow{color:#d29922}.blue{color:#58a6ff}.purple{color:#bc8cff}.orange{color:#fb8f44}
.quota-bar{background:#21262d;border-radius:3px;height:5px;overflow:hidden;margin:4px 0}
.quota-fill{height:100%;border-radius:3px;transition:width .3s}
.qtext{color:#8b949e;font-size:.7rem;margin-bottom:12px}
.sec{color:#8b949e;font-size:.7rem;text-transform:uppercase;letter-spacing:.5px;margin:12px 0 6px}
table{width:100%;border-collapse:collapse;background:#161b22;border-radius:8px;overflow:hidden;margin-bottom:12px}
th{background:#21262d;color:#8b949e;font-size:.68rem;text-transform:uppercase;padding:7px 10px;text-align:left}
td{padding:7px 10px;border-top:1px solid #21262d;font-size:.8rem}
tr:hover td{background:#1c2128}
.badge{display:inline-block;padding:1px 6px;border-radius:10px;font-size:.65rem;font-weight:600}
.bp{background:#1f3d5c;color:#58a6ff}.bc{background:#1a3a2a;color:#3fb950}.br{background:#3d1f1f;color:#f85149}
.brlm{background:#2d1f4a;color:#bc8cff}.bsteam{background:#1a2d4a;color:#58a6ff}
.profit{color:#3fb950;font-weight:700}.loss{color:#f85149;font-weight:700}
.cw{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px;margin-bottom:10px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.three{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px}
.wr-ring{display:flex;align-items:center;gap:12px;padding:8px 0}
.ring{width:64px;height:64px;flex-shrink:0}
@media(max-width:600px){.two,.three{grid-template-columns:1fr}}
</style>
</head>
<body>
<h1>🤖 ARB BOT v8.0</h1>
<div class="sub">รีเฟรชทุก 20 วินาที — Production Ready + Sharp Money Analytics</div>

<div class="tabs">
  <div class="tab active" onclick="switchTab('overview')">📊 Overview</div>
  <div class="tab" onclick="switchTab('linelog')">📡 Line Moves</div>
  <div class="tab" onclick="switchTab('stats')">🔬 Statistics</div>
  <div class="tab" onclick="switchTab('trades')">💰 Trades</div>
  <div class="tab" onclick="switchTab('controls')">⚙️ Controls</div>
</div>

<!-- ═══ TAB 1: OVERVIEW ═══ -->
<div id="page-overview" class="page active">
  <div class="grid" id="overviewCards"></div>
  <div class="quota-bar"><div class="quota-fill" id="qFill"></div></div>
  <div class="qtext" id="qText"></div>
  <div class="two">
    <div class="cw"><div class="sec">📈 Profit Opportunity History</div><canvas id="profitChart" height="150"></canvas></div>
    <div class="cw"><div class="sec">📡 Line Movement Flow</div><canvas id="lineFlowChart" height="150"></canvas></div>
  </div>
  <div class="sec">📋 Opportunity Log ล่าสุด</div>
  <table>
    <thead><tr><th>Event</th><th>Leg 1</th><th>Leg 2</th><th>Profit</th><th>เวลา</th><th>Status</th></tr></thead>
    <tbody id="oppBody"></tbody>
  </table>
</div>

<!-- ═══ TAB 2: LINE MOVES ═══ -->
<div id="page-linelog" class="page">
  <div class="two">
    <div class="cw"><div class="sec">💧 Price Flow — odds เปลี่ยนตามเวลา</div><canvas id="priceFlowChart" height="180"></canvas></div>
    <div class="cw"><div class="sec">🌊 Steam vs 🔄 RLM Count</div><canvas id="signalTypeChart" height="180"></canvas></div>
  </div>
  <div class="sec">🌊 Line Movement Log</div>
  <table>
    <thead><tr><th>Event</th><th>Bookmaker</th><th>Outcome</th><th>Before→After</th><th>Change</th><th>Signal</th><th>เวลา</th></tr></thead>
    <tbody id="lineBody"></tbody>
  </table>
</div>

<!-- ═══ TAB 3: STATISTICS ═══ -->
<div id="page-stats" class="page">
  <!-- Win Rate Section -->
  <div class="three">
    <div class="cw">
      <div class="sec">🔄 RLM Win Rate</div>
      <div class="wr-ring">
        <svg class="ring" viewBox="0 0 36 36">
          <circle cx="18" cy="18" r="15.9" fill="none" stroke="#21262d" stroke-width="3"/>
          <circle cx="18" cy="18" r="15.9" fill="none" stroke="#bc8cff" stroke-width="3"
            stroke-dasharray="0 100" stroke-linecap="round" transform="rotate(-90 18 18)"
            id="rlmRing"/>
        </svg>
        <div>
          <div class="val purple" id="rlmWR">—</div>
          <div class="sub2" id="rlmCount">0 signals</div>
          <div class="sub2" style="font-size:.65rem;margin-top:2px">Pinnacle ขยับ = sharp money</div>
        </div>
      </div>
    </div>
    <div class="cw">
      <div class="sec">🌊 Steam Win Rate</div>
      <div class="wr-ring">
        <svg class="ring" viewBox="0 0 36 36">
          <circle cx="18" cy="18" r="15.9" fill="none" stroke="#21262d" stroke-width="3"/>
          <circle cx="18" cy="18" r="15.9" fill="none" stroke="#58a6ff" stroke-width="3"
            stroke-dasharray="0 100" stroke-linecap="round" transform="rotate(-90 18 18)"
            id="steamRing"/>
        </svg>
        <div>
          <div class="val blue" id="steamWR">—</div>
          <div class="sub2" id="steamCount">0 signals</div>
          <div class="sub2" style="font-size:.65rem;margin-top:2px">หลายเว็บขยับพร้อมกัน</div>
        </div>
      </div>
    </div>
    <div class="cw">
      <div class="sec">📊 Overall Arb Win Rate</div>
      <div class="wr-ring">
        <svg class="ring" viewBox="0 0 36 36">
          <circle cx="18" cy="18" r="15.9" fill="none" stroke="#21262d" stroke-width="3"/>
          <circle cx="18" cy="18" r="15.9" fill="none" stroke="#3fb950" stroke-width="3"
            stroke-dasharray="0 100" stroke-linecap="round" transform="rotate(-90 18 18)"
            id="arbRing"/>
        </svg>
        <div>
          <div class="val green" id="arbWR">—</div>
          <div class="sub2" id="arbCount">0 trades</div>
          <div class="sub2" style="font-size:.65rem;margin-top:2px">Confirmed / total</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Sharp vs Public + Bookmaker Accuracy -->
  <div class="two">
    <div class="cw">
      <div class="sec">💎 Sharp vs Public Money</div>
      <canvas id="sharpPublicChart" height="160"></canvas>
      <div id="sharpNote" style="color:#8b949e;font-size:.72rem;margin-top:6px"></div>
    </div>
    <div class="cw">
      <div class="sec">🎯 Bookmaker Accuracy (vs closing line)</div>
      <canvas id="bmAccChart" height="160"></canvas>
    </div>
  </div>

  <!-- ROI per Sport -->
  <div class="cw">
    <div class="sec">💹 ROI ต่อ Sport</div>
    <canvas id="roiChart" height="130"></canvas>
  </div>

  <!-- CLV Summary -->
  <div class="grid" id="clvCards"></div>
</div>

<!-- ═══ TAB 4: TRADES ═══ -->
<div id="page-trades" class="page">
  <div class="grid" id="pnlCards"></div>
  <div class="sec">📝 Trade History</div>
  <table>
    <thead><tr><th>Event</th><th>Sport</th><th>Leg 1</th><th>Leg 2</th><th>Profit%</th><th>ทุน</th><th>CLV</th><th>Status</th><th>เวลา</th></tr></thead>
    <tbody id="tradeBody"></tbody>
  </table>
</div>

<!-- ═══ TAB 5: CONTROLS ═══ -->
<div id="page-controls" class="page">
  <div class="two">
    <!-- Quick Actions -->
    <div class="cw">
      <div class="sec">⚡ Quick Actions</div>
      <div style="display:flex;flex-direction:column;gap:8px;margin-top:4px">
        <button class="btn-green" onclick="action('scan_now','true')">🔍 Scan Now</button>
        <button class="btn-blue"  onclick="toggleScan()">🔄 Toggle Auto Scan</button>
        <button class="btn-gray"  onclick="action('clear_seen','true')">🗑️ Clear Seen Signals</button>
      </div>
    </div>
    <!-- Scan Settings -->
    <div class="cw">
      <div class="sec">📡 Scan Settings</div>
      <div class="ctrl-row">
        <label>Min Profit %</label>
        <input type="number" id="cfg_min_profit" step="0.1" min="0.5" max="10" placeholder="1.5">
        <button onclick="save('min_profit_pct', document.getElementById('cfg_min_profit').value/100)">Save</button>
      </div>
      <div class="ctrl-row">
        <label>Scan Interval (s)</label>
        <input type="number" id="cfg_interval" step="60" min="60" max="3600" placeholder="300">
        <button onclick="save('scan_interval', document.getElementById('cfg_interval').value)">Save</button>
      </div>
      <div class="ctrl-row">
        <label>Alert Cooldown (m)</label>
        <input type="number" id="cfg_cooldown" step="5" min="5" max="120" placeholder="30">
        <button onclick="save('cooldown', document.getElementById('cfg_cooldown').value)">Save</button>
      </div>
    </div>
  </div>
  <div class="two">
    <!-- Odds Settings -->
    <div class="cw">
      <div class="sec">📊 Odds Filter</div>
      <div class="ctrl-row">
        <label>Max Odds</label>
        <input type="number" id="cfg_max_odds" step="1" min="2" max="50" placeholder="15">
        <button onclick="save('max_odds', document.getElementById('cfg_max_odds').value)">Save</button>
      </div>
      <div class="ctrl-row">
        <label>Min Odds</label>
        <input type="number" id="cfg_min_odds" step="0.05" min="1.01" max="2" placeholder="1.05">
        <button onclick="save('min_odds', document.getElementById('cfg_min_odds').value)">Save</button>
      </div>
    </div>
    <!-- Stake Settings -->
    <div class="cw">
      <div class="sec">💵 Stake / Kelly</div>
      <div class="ctrl-row">
        <label>Total Stake (฿)</label>
        <input type="number" id="cfg_stake" step="1000" min="1000" placeholder="10000">
        <button onclick="save('total_stake', document.getElementById('cfg_stake').value)">Save</button>
      </div>
      <div class="ctrl-row">
        <label>Kelly Fraction</label>
        <input type="number" id="cfg_kelly" step="0.05" min="0.1" max="1" placeholder="0.25">
        <button onclick="save('kelly_fraction', document.getElementById('cfg_kelly').value)">Save</button>
      </div>
      <div class="ctrl-row">
        <label>Use Kelly</label>
        <select id="cfg_use_kelly">
          <option value="true">ON</option>
          <option value="false">OFF</option>
        </select>
        <button onclick="save('use_kelly', document.getElementById('cfg_use_kelly').value)">Save</button>
      </div>
    </div>
  </div>
  <div id="ctrl-msg" style="margin-top:8px;padding:10px;border-radius:6px;display:none;font-size:.85rem"></div>
  <div class="cw" style="margin-top:12px">
    <div class="sec">📋 Current Config</div>
    <div id="cfg-display" style="font-size:.8rem;color:#8b949e;line-height:1.8"></div>
  </div>
</div>

<style>
.btn-green{background:#1a3a2a;color:#3fb950;border:1px solid #3fb950;padding:10px 16px;border-radius:6px;cursor:pointer;font-size:.85rem;width:100%}
.btn-blue{background:#1f3d5c;color:#58a6ff;border:1px solid #58a6ff;padding:10px 16px;border-radius:6px;cursor:pointer;font-size:.85rem;width:100%}
.btn-gray{background:#21262d;color:#8b949e;border:1px solid #30363d;padding:10px 16px;border-radius:6px;cursor:pointer;font-size:.85rem;width:100%}
.ctrl-row{display:grid;grid-template-columns:140px 1fr auto;gap:6px;align-items:center;margin-bottom:8px}
.ctrl-row label{color:#8b949e;font-size:.78rem}
.ctrl-row input,.ctrl-row select{background:#0d1117;border:1px solid #30363d;border-radius:4px;color:#e6edf3;padding:5px 8px;font-size:.82rem;width:100%}
.ctrl-row button{background:#21262d;color:#58a6ff;border:1px solid #30363d;border-radius:4px;padding:5px 10px;cursor:pointer;font-size:.78rem;white-space:nowrap}
.ctrl-row button:hover{background:#1f3d5c}
</style>

<script>
let charts = {};
let currentTab = 'overview';
let lastData = null;

function switchTab(name) {
  currentTab = name;
  document.querySelectorAll('.tab').forEach((t,i) => {
    const names = ['overview','linelog','stats','trades','controls'];
    t.classList.toggle('active', names[i]===name);
  });
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.getElementById('page-'+name).classList.add('active');
  if (lastData) { renderTab(name, lastData); if(name==='controls') renderControls(lastData); }
}

function mkChart(id, type, labels, datasets, opts={}) {
  const ctx = document.getElementById(id);
  if (!ctx) return;
  if (charts[id]) charts[id].destroy();
  charts[id] = new Chart(ctx, {
    type,
    data:{labels, datasets},
    options:{
      responsive:true,
      plugins:{legend:{display:opts.legend??false,labels:{color:'#8b949e',font:{size:10}}},
               tooltip:{callbacks:{label: ctx => opts.tooltipFn ? opts.tooltipFn(ctx) : ctx.formattedValue}}},
      scales: type==='pie'||type==='doughnut' ? {} : {
        x:{ticks:{color:'#8b949e',font:{size:9}},grid:{color:'#21262d'}},
        y:{ticks:{color:'#8b949e',font:{size:9}},grid:{color:'#21262d'},
           ...(opts.yTitle?{title:{display:true,text:opts.yTitle,color:'#8b949e',font:{size:9}}}:{})}
      }
    }
  });
}

function setRing(id, pct, color) {
  const el = document.getElementById(id);
  if (el) el.setAttribute('stroke-dasharray', `${pct} ${100-pct}`);
}

function renderTab(tab, d) {
  const s = d.stats || {};
  const lm = d.line_movements || [];
  const opps = d.opportunities || [];
  const trades = d.trade_records || [];

  if (tab === 'overview') {
    const qPct = Math.round((d.api_remaining/500)*100);
    const qColor = qPct>30?'#3fb950':qPct>10?'#d29922':'#f85149';
    document.getElementById('overviewCards').innerHTML = `
      <div class="card"><div class="lbl">Auto Scan</div><div class="val ${d.auto_scan?'green':'red'}">${d.auto_scan?'🟢 ON':'🔴 OFF'}</div></div>
      <div class="card"><div class="lbl">สแกน</div><div class="val blue">${d.scan_count} รอบ</div></div>
      <div class="card"><div class="lbl">รอ Confirm</div><div class="val yellow">${d.pending_count}</div></div>
      <div class="card"><div class="lbl">API Credits</div><div class="val" style="color:${qColor}">${d.api_remaining}</div></div>
      <div class="card"><div class="lbl">Line Moves</div><div class="val purple">${d.line_move_count}</div></div>
      <div class="card"><div class="lbl">Trades</div><div class="val green">${d.confirmed_trades}</div></div>
    `;
    document.getElementById('qFill').style.cssText=`width:${qPct}%;background:${qColor}`;
    document.getElementById('qText').textContent=`Credits ${d.api_remaining}/500 (${qPct}%) | เตือนที่ ${d.quota_warn_at} | ${d.last_scan_time}`;

    const pLabels = opps.slice(-20).map(o=>o.event.split(' vs ')[0].substring(0,8));
    const pData   = opps.slice(-20).map(o=>+(o.profit_pct*100).toFixed(2));
    mkChart('profitChart','bar',pLabels,[{label:'Profit%',data:pData,
      backgroundColor:pData.map(v=>v>2?'#3fb950':v>1?'#d29922':'#58a6ff'),borderRadius:3}],{yTitle:'%'});

    const lmLabels = lm.slice(-20).map(m=>m.event.split(' vs ')[0].substring(0,6));
    const lmData   = lm.slice(-20).map(m=>+(m.pct_change*100).toFixed(2));
    mkChart('lineFlowChart','bar',lmLabels,[{label:'Move%',data:lmData,
      backgroundColor:lmData.map(v=>v<0?'#f85149':'#3fb950'),borderRadius:3}],{yTitle:'%'});

    const oppRows = opps.slice(-15).reverse().map(o=>{
      const bc=o.status==='pending'?'bp':o.status==='confirmed'?'bc':'br';
      const bl=o.status==='pending'?'รอ':o.status==='confirmed'?'✅':'❌';
      const t=new Date(o.created_at).toLocaleTimeString('th-TH',{hour:'2-digit',minute:'2-digit'});
      return `<tr><td>${o.event}</td><td>${o.leg1_bm} @${o.leg1_odds.toFixed(2)}</td>
        <td>${o.leg2_bm} @${o.leg2_odds.toFixed(2)}</td>
        <td class="profit">+${(o.profit_pct*100).toFixed(2)}%</td>
        <td>${t}</td>
        <td><span class="badge ${bc}">${bl}</span>${o.mins_to_start<=30&&o.mins_to_start>0?' <span class="badge" style="background:#3d1a1a;color:#f85149">🔴CLOSING</span>':o.mins_to_start<=120&&o.mins_to_start>0?' <span class="badge" style="background:#2d2a1a;color:#d29922">⏰120m</span>':''}</td></tr>`;
    }).join('');
    document.getElementById('oppBody').innerHTML = oppRows||'<tr><td colspan="6" style="text-align:center;color:#8b949e;padding:20px">ยังไม่พบ opportunity</td></tr>';
  }

  if (tab === 'linelog') {
    // Price Flow Chart — odds ของ event ล่าสุดตามเวลา
    const byEvent = {};
    lm.forEach(m => {
      const k = m.event.substring(0,20);
      if (!byEvent[k]) byEvent[k] = [];
      byEvent[k].push({t: new Date(m.ts).toLocaleTimeString('th-TH',{hour:'2-digit',minute:'2-digit'}),
                       v: m.odds_after, bm: m.bookmaker});
    });
    const topEvents = Object.keys(byEvent).slice(-4);
    const colors = ['#58a6ff','#3fb950','#d29922','#bc8cff'];
    const flowDatasets = topEvents.map((ev,i)=>({
      label: ev, borderColor: colors[i], backgroundColor: colors[i]+'33',
      data: byEvent[ev].map(p=>p.v), fill:false, tension:0.3, pointRadius:3,
    }));
    const flowLabels = topEvents.length ? byEvent[topEvents[0]].map(p=>p.t) : [];
    mkChart('priceFlowChart','line',flowLabels,flowDatasets,{legend:true,yTitle:'Odds'});

    // Signal type donut
    const rlmCount   = lm.filter(m=>m.is_rlm).length;
    const steamCount = lm.filter(m=>m.is_steam).length;
    const normalCount= lm.length - rlmCount - steamCount;
    mkChart('signalTypeChart','doughnut',
      ['🔄 RLM','🌊 Steam','📊 Normal'],
      [{data:[rlmCount,steamCount,normalCount],backgroundColor:['#bc8cff','#58a6ff','#30363d'],borderWidth:0}],
      {legend:true});

    const lmRows = lm.slice(-20).reverse().map(m=>{
      const pct=(m.pct_change*100).toFixed(1);
      const sign=m.pct_change>0?'+':'';
      const tags=(m.is_steam?'<span class="badge bsteam">🌊Steam</span> ':'')+(m.is_rlm?'<span class="badge brlm">🔄RLM</span>':'');
      const t=new Date(m.ts).toLocaleTimeString('th-TH',{hour:'2-digit',minute:'2-digit'});
      return `<tr><td>${m.event}</td><td>${m.bookmaker}</td><td>${m.outcome}</td>
        <td>${m.odds_before.toFixed(3)}→${m.odds_after.toFixed(3)}</td>
        <td style="color:${m.pct_change<0?'#f85149':'#3fb950'}">${sign}${pct}%</td>
        <td>${tags||'—'}</td><td>${t}</td></tr>`;
    }).join('');
    document.getElementById('lineBody').innerHTML = lmRows||'<tr><td colspan="7" style="text-align:center;color:#8b949e;padding:20px">ยังไม่มีข้อมูล</td></tr>';
  }

  if (tab === 'stats') {
    // RLM Win Rate
    const rlmWR  = s.rlm_win_rate ?? null;
    const rlmCnt = s.rlm_count ?? 0;
    const steamWR  = s.steam_win_rate ?? null;
    const steamCnt = s.steam_count ?? 0;
    const arbWR    = s.arb_win_rate ?? null;
    const arbCnt   = s.confirmed_trades ?? 0;

    document.getElementById('rlmWR').textContent   = rlmWR!==null ? rlmWR.toFixed(1)+'%' : '—';
    document.getElementById('rlmCount').textContent = rlmCnt+' signals';
    document.getElementById('steamWR').textContent  = steamWR!==null ? steamWR.toFixed(1)+'%' : '—';
    document.getElementById('steamCount').textContent= steamCnt+' signals';
    document.getElementById('arbWR').textContent    = arbWR!==null ? arbWR.toFixed(1)+'%' : '—';
    document.getElementById('arbCount').textContent  = arbCnt+' trades';

    if (rlmWR!==null)   setRing('rlmRing', rlmWR, '#bc8cff');
    if (steamWR!==null) setRing('steamRing', steamWR, '#58a6ff');
    if (arbWR!==null)   setRing('arbRing', arbWR, '#3fb950');

    // Sharp vs Public
    const sharpCount  = s.sharp_count ?? 0;
    const publicCount = s.public_count ?? 0;
    mkChart('sharpPublicChart','doughnut',
      ['💎 Sharp (RLM+Steam)', '📢 Public (Normal)'],
      [{data:[sharpCount,publicCount],backgroundColor:['#bc8cff','#30363d'],borderWidth:0}],
      {legend:true});
    document.getElementById('sharpNote').textContent =
      `Sharp signals: ${sharpCount} | Public: ${publicCount} | Sharp ratio: ${sharpCount+publicCount>0?((sharpCount/(sharpCount+publicCount))*100).toFixed(1):'—'}%`;

    // Bookmaker accuracy
    const bmNames  = Object.keys(s.bm_accuracy ?? {});
    const bmScores = bmNames.map(k=>(s.bm_accuracy[k]*100).toFixed(1));
    mkChart('bmAccChart','bar',bmNames,
      [{label:'Accuracy%',data:bmScores,
        backgroundColor:bmScores.map(v=>v>60?'#3fb950':v>40?'#d29922':'#f85149'),borderRadius:3}],
      {yTitle:'%'});

    // ROI per sport
    const sportNames = Object.keys(s.roi_by_sport ?? {});
    const sportROI   = sportNames.map(k=>+(s.roi_by_sport[k]*100).toFixed(2));
    mkChart('roiChart','bar',sportNames.map(n=>n.split('_').pop().toUpperCase()),
      [{label:'ROI%',data:sportROI,
        backgroundColor:sportROI.map(v=>v>0?'#3fb950':'#f85149'),borderRadius:3}],
      {yTitle:'%'});

    // CLV cards
    const clv = s.clv ?? {};
    document.getElementById('clvCards').innerHTML = `
      <div class="card"><div class="lbl">CLV avg</div>
        <div class="val ${(clv.avg??0)>=0?'green':'red'}">${clv.avg!==null&&clv.avg!==undefined?clv.avg.toFixed(2)+'%':'—'}</div>
        <div class="sub2">เอาชนะตลาด = บวก</div></div>
      <div class="card"><div class="lbl">CLV บวก</div><div class="val green">${clv.positive??0} trades</div></div>
      <div class="card"><div class="lbl">CLV ลบ</div><div class="val red">${clv.negative??0} trades</div></div>
      <div class="card"><div class="lbl">Best CLV</div><div class="val green">${clv.best!==undefined?'+'+clv.best.toFixed(2)+'%':'—'}</div></div>
    `;
  }

  if (tab === 'trades') {
    const p = d.pnl ?? {};
    document.getElementById('pnlCards').innerHTML = `
      <div class="card"><div class="lbl">Confirmed</div><div class="val green">${p.confirmed??0}</div></div>
      <div class="card"><div class="lbl">Rejected</div><div class="val red">${p.rejected??0}</div></div>
      <div class="card"><div class="lbl">Est. Profit</div><div class="val green">฿${(p.est_profit??0).toLocaleString()}</div></div>
      <div class="card"><div class="lbl">Avg Profit%</div><div class="val blue">${p.avg_profit?p.avg_profit.toFixed(2)+'%':'—'}</div></div>
      <div class="card"><div class="lbl">CLV avg</div><div class="val ${(p.avg_clv??0)>=0?'green':'red'}">${p.avg_clv!==null&&p.avg_clv!==undefined?p.avg_clv.toFixed(2)+'%':'—'}</div></div>
    `;
    const tRows = trades.slice(-30).reverse().map(t=>{
      const bc=t.status==='confirmed'?'bc':'br';
      const tm=new Date(t.created_at).toLocaleTimeString('th-TH',{hour:'2-digit',minute:'2-digit'});
      const clvStr=(t.clv_leg1!==null&&t.clv_leg1!==undefined)?`${t.clv_leg1>0?'+':''}${t.clv_leg1.toFixed(1)}%`:'—';
      return `<tr>
        <td>${t.event}</td><td>${t.sport.split('_').pop().toUpperCase()}</td>
        <td>${t.leg1_bm} @${t.leg1_odds.toFixed(2)}</td>
        <td>${t.leg2_bm} @${t.leg2_odds.toFixed(2)}</td>
        <td class="profit">+${(t.profit_pct*100).toFixed(2)}%</td>
        <td>฿${t.stake1_thb.toLocaleString()}/฿${t.stake2_thb.toLocaleString()}</td>
        <td style="color:${(t.clv_leg1??0)>=0?'#3fb950':'#f85149'}">${clvStr}</td>
        <td><span class="badge ${bc}">${t.status==='confirmed'?'✅ Confirmed':'❌ Rejected'}</span></td>
        <td>${tm}</td></tr>`;
    }).join('');
    document.getElementById('tradeBody').innerHTML = tRows||'<tr><td colspan="9" style="text-align:center;color:#8b949e;padding:20px">ยังไม่มี trades</td></tr>';
  }
}

const _qs = new URLSearchParams(location.search);
const _tk = _qs.get('token') || '';
function apiUrl(path) { return _tk ? path + '?token=' + encodeURIComponent(_tk) : path; }
setInterval(() => location.reload(), 20000);

async function action(key, value) {
  const r = await fetch(apiUrl('/api/control'), {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({key, value})
  });
  const d = await r.json();
  showMsg(d.msg, d.ok);
  if (d.ok) load();
}

async function save(key, value) {
  if (!value && value !== false) { showMsg('กรุณากรอกค่า','error'); return; }
  await action(key, value);
}

async function toggleScan() {
  const r = await fetch(apiUrl('/api/state'));
  const d = await r.json();
  await action('auto_scan', (!d.auto_scan).toString());
}

function showMsg(msg, ok) {
  const el = document.getElementById('ctrl-msg');
  el.textContent = ok ? '✅ ' + msg : '❌ ' + msg;
  el.style.display = 'block';
  el.style.background = ok ? '#1a3a2a' : '#3d1f1f';
  el.style.color = ok ? '#3fb950' : '#f85149';
  setTimeout(() => el.style.display='none', 3000);
}

function renderControls(d) {
  document.getElementById('cfg-display').innerHTML = `
    Auto Scan: <b style="color:${d.auto_scan?'#3fb950':'#f85149'}">${d.auto_scan?'ON':'OFF'}</b><br>
    Min Profit: <b>${(d.min_profit_pct*100).toFixed(2)}%</b><br>
    Scan Interval: <b>${d.scan_interval}s</b><br>
    Total Stake: <b>฿${d.total_stake_thb.toLocaleString()}</b><br>
    API Credits: <b>${d.api_remaining}/500</b>
  `;
}

async function load() {
  const [stateRes, statsRes] = await Promise.all([
    fetch(apiUrl('/api/state')), fetch(apiUrl('/api/stats'))
  ]);
  const state = await stateRes.json();
  const stats = await statsRes.json();
  const data  = {...state, stats};
  data.trade_records = stats.trade_records || [];
  lastData = data;
  renderTab(currentTab, data);
}
load();
</script>
</body>
</html>"""


_stats_cache: dict = {"data": None, "ts": 0}

def calc_stats_cached() -> dict:
    """calc_stats พร้อม cache 15 วินาที — ลดภาระ CPU ตอน dashboard refresh"""
    if time.time() - _stats_cache["ts"] < 15 and _stats_cache["data"] is not None:
        return _stats_cache["data"]
    result = calc_stats()
    _stats_cache["data"] = result
    _stats_cache["ts"]   = time.time()
    return result

def calc_stats() -> dict:
    """คำนวณสถิติทั้งหมดสำหรับ /api/stats"""
    confirmed = [t for t in trade_records if t.status == "confirmed"]
    rejected  = [t for t in trade_records if t.status == "rejected"]

    # ── Win Rate ──────────────────────────────────────────────────
    # RLM win rate: นับ line movements ที่เป็น RLM
    rlm_moves   = [m for m in line_movements if m.is_rlm]
    steam_moves = [m for m in line_movements if m.is_steam]

    # เชื่อม RLM กับ trade ที่เกิดขึ้นหลังสัญญาณ (ภายใน 30 นาที)
    def signal_win_rate(moves):
        if not moves or not confirmed: return None, len(moves)
        wins = 0
        total = 0
        for m in moves:
            m_ts = datetime.fromisoformat(m.ts.replace("Z","+00:00")) if "Z" in m.ts else datetime.fromisoformat(m.ts)
            for t in confirmed:
                t_ts = datetime.fromisoformat(t.created_at)
                if abs((t_ts - m_ts).total_seconds()) < 1800:  # 30 นาที
                    if m.event in t.event or t.event in m.event:
                        total += 1
                        wins  += 1  # confirmed = win (arb)
                        break
        return (wins/total*100 if total > 0 else None), len(moves)

    rlm_wr,   rlm_cnt   = signal_win_rate(rlm_moves)
    steam_wr, steam_cnt = signal_win_rate(steam_moves)
    arb_total = len(confirmed) + len(rejected)
    arb_wr    = (len(confirmed) / arb_total * 100) if arb_total > 0 else None

    # ── Sharp vs Public ───────────────────────────────────────────
    sharp_count  = len(rlm_moves) + len(steam_moves)
    public_count = max(0, len(line_movements) - sharp_count)

    # ── Bookmaker Accuracy ────────────────────────────────────────
    # วัดจาก: ถ้า Pinnacle ขยับ odds ฝั่งไหน แล้ว outcome นั้นชนะบ่อยแค่ไหน
    # ใช้ line_movements เพื่อดูว่า bookmaker ไหน "รู้ก่อน" (odds ลดลง = favourite จริง)
    bm_correct = defaultdict(int)
    bm_total   = defaultdict(int)
    for m in line_movements:
        bm_total[m.bookmaker] += 1
        # ถ้า odds ลด = เว็บเชื่อว่าจะชนะมากขึ้น = "sharp signal"
        if m.pct_change < -0.03:
            bm_correct[m.bookmaker] += 1
    bm_accuracy = {bm: bm_correct[bm]/bm_total[bm]
                   for bm in bm_total if bm_total[bm] >= 3}

    # ── ROI per Sport ─────────────────────────────────────────────
    sport_profit = defaultdict(float)
    sport_stake  = defaultdict(float)
    for t in confirmed:
        est = t.profit_pct * (t.stake1_thb + t.stake2_thb)
        sport_profit[t.sport] += est
        sport_stake[t.sport]  += (t.stake1_thb + t.stake2_thb)
    roi_by_sport = {s: sport_profit[s]/sport_stake[s]
                    for s in sport_stake if sport_stake[s] > 0}

    # ── CLV Summary ───────────────────────────────────────────────
    clv_values = []
    for t in confirmed:
        c1, c2 = calc_clv(t)
        if c1 is not None: clv_values.append(c1)
        if c2 is not None: clv_values.append(c2)
    avg_clv = sum(clv_values)/len(clv_values) if clv_values else None
    clv_positive = len([c for c in clv_values if c > 0])
    clv_negative = len([c for c in clv_values if c < 0])
    best_clv     = max(clv_values) if clv_values else None

    # ── P&L ───────────────────────────────────────────────────────
    est_profit = sum(t.profit_pct*(t.stake1_thb+t.stake2_thb) for t in confirmed)
    avg_profit = (sum(t.profit_pct for t in confirmed)/len(confirmed)*100) if confirmed else None

    # ── Trade records สำหรับ table ────────────────────────────────
    trade_list = []
    for t in trade_records[-50:]:
        c1, c2 = calc_clv(t)
        trade_list.append({
            "signal_id": t.signal_id, "event": t.event, "sport": t.sport,
            "leg1_bm": t.leg1_bm, "leg2_bm": t.leg2_bm,
            "leg1_odds": t.leg1_odds, "leg2_odds": t.leg2_odds,
            "stake1_thb": t.stake1_thb, "stake2_thb": t.stake2_thb,
            "profit_pct": t.profit_pct, "status": t.status,
            "clv_leg1": c1, "clv_leg2": c2,
            "created_at": t.created_at,
        })

    return {
        "rlm_win_rate":    rlm_wr,
        "rlm_count":       rlm_cnt,
        "steam_win_rate":  steam_wr,
        "steam_count":     steam_cnt,
        "arb_win_rate":    arb_wr,
        "confirmed_trades":len(confirmed),
        "sharp_count":     sharp_count,
        "public_count":    public_count,
        "bm_accuracy":     bm_accuracy,
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
            return True, f"auto_scan = {auto_scan}"
        elif key == "min_profit_pct":
            MIN_PROFIT_PCT = Decimal(value)
            return True, f"MIN_PROFIT_PCT = {MIN_PROFIT_PCT:.3f}"
        elif key == "scan_interval":
            SCAN_INTERVAL = int(value)
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
            # trigger scan ทันที
            asyncio.get_running_loop().create_task(do_scan())
            return True, "scan triggered"
        elif key == "clear_seen":
            seen_signals.clear()
            return True, "seen_signals cleared"
        else:
            return False, f"unknown key: {key}"
    except Exception as e:
        return False, str(e)

class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def _check_auth(self) -> bool:
        """ตรวจ Dashboard token (ถ้าตั้งไว้)"""
        if not DASHBOARD_TOKEN:
            return True  # ไม่ได้ตั้ง token = ไม่บังคับ auth
        auth = self.headers.get("Authorization", "")
        # รองรับทั้ง header และ query param ?token=xxx
        from urllib.parse import urlparse, parse_qs
        qs_token = parse_qs(urlparse(self.path).query).get("token", [""])[0]
        if auth == f"Bearer {DASHBOARD_TOKEN}" or qs_token == DASHBOARD_TOKEN:
            return True
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
        if self.path == "/api/control":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body   = json.loads(self.rfile.read(length))
                key    = body.get("key","")
                value  = str(body.get("value",""))
                ok, msg = apply_runtime_config(key, value)
                # save ลง DB ด้วย
                if ok:
                    db_save_state(f"cfg_{key}", value)
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
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        # Health check endpoint สำหรับ Railway (ไม่ต้อง auth)
        if self.path == "/health":
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        if not self._check_auth(): return

        # strip query params for path matching
        from urllib.parse import urlparse
        clean_path = urlparse(self.path).path

        if clean_path == "/api/state":
            confirmed = [t for t in trade_records if t.status=="confirmed"]
            rejected  = [t for t in trade_records if t.status=="rejected"]
            est_profit = sum(t.profit_pct*(t.stake1_thb+t.stake2_thb) for t in confirmed)
            clv_values = []
            for t in confirmed:
                c1,c2 = calc_clv(t)
                if c1 is not None: clv_values.append(c1)
                if c2 is not None: clv_values.append(c2)
            avg_clv = sum(clv_values)/len(clv_values) if clv_values else None

            lm_list = [{"event":m.event,"bookmaker":m.bookmaker,"outcome":m.outcome,
                        "odds_before":float(m.odds_before),"odds_after":float(m.odds_after),
                        "pct_change":float(m.pct_change),"direction":m.direction,
                        "is_steam":m.is_steam,"is_rlm":m.is_rlm,"ts":m.ts}
                       for m in line_movements[-50:]]

            data = {
                "auto_scan":       auto_scan,
                "scan_count":      scan_count,
                "last_scan_time":  last_scan_time,
                "pending_count":   len(pending),
                "api_remaining":   api_remaining,
                "quota_warn_at":   QUOTA_WARN_AT,
                "total_stake_thb": int(TOTAL_STAKE_THB),
                "min_profit_pct":  float(MIN_PROFIT_PCT),
                "scan_interval":   SCAN_INTERVAL,
                "line_move_count": len(line_movements),
                "confirmed_trades":len(confirmed),
                "opportunities":   opportunity_log[-50:],
                "line_movements":  lm_list,
                "pnl": {
                    "confirmed":  len(confirmed),
                    "rejected":   len(rejected),
                    "est_profit": round(est_profit),
                    "avg_clv":    round(avg_clv,2) if avg_clv is not None else None,
                },
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


def start_dashboard():
    server = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    log.info(f"[Dashboard] http://0.0.0.0:{PORT}")
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
    global trade_records, opportunity_log, line_movements, scan_count, auto_scan, last_scan_time, api_remaining

    # ── init DB ──
    db_init()                     # SQLite local (sync, fallback)
    await turso_init()            # Turso cloud (async)

    # โหลด bot state จาก Turso (persistent) → fallback local SQLite
    if _turso is not None:
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

    # โหลด records จาก DB (Turso หรือ SQLite)
    loaded_trades, loaded_opps, lms = await db_load_all()
    trade_records.extend(loaded_trades)
    opportunity_log.extend(loaded_opps)
    line_movements.extend(lms)

    db_mode = "☁️ Turso" if (_turso is not None) else "💾 SQLite local"
    log.info(f"[DB] {db_mode} | trades={len(trade_records)}, opps={len(opportunity_log)}, moves={len(line_movements)}, scans={scan_count}")

    # restore pending settlement — trades ที่ confirmed แต่ยังไม่มีผล
    for t in trade_records:
        if t.status == "confirmed" and t.actual_profit_thb is None and t.settled_at is None:
            _pending_settlement[t.signal_id] = t
    log.info(f"[Settle] restored {len(_pending_settlement)} unsettled trades")

    app.add_error_handler(error_handler)
    threading.Thread(target=start_dashboard, daemon=True).start()

    is_restored = len(trade_records) > 0 or scan_count > 0
    db_mode_str  = "☁️ Turso" if (_turso is not None) else "💾 SQLite"
    restore_note = f"♻️ {db_mode_str}: {len(trade_records)} trades, {scan_count} scans" if is_restored else f"🆕 {db_mode_str}: fresh start"

    await app.bot.send_message(
        chat_id=CHAT_ID, parse_mode="Markdown",
        text=(
            "🤖 *ARB BOT v8.0 — Production Ready*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💾 Persistent Storage + Health Check\n"
            f"{restore_note}\n"
            f"Sports    : {' '.join([SPORT_EMOJI.get(s,'🏆') for s in SPORTS])}\n"
            f"Min profit: {MIN_PROFIT_PCT:.1%} | Max odds: {MAX_ODDS_ALLOWED}\n"
            f"ทุน/trade : ฿{int(TOTAL_STAKE_THB):,} | Cooldown: {ALERT_COOLDOWN_MIN}m\n"
            f"Auto scan : {'🟢 เปิด' if auto_scan else '🔴 ปิด'} (ทุก {SCAN_INTERVAL}s)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"/scan on·off | /now | /pnl | /lines | /status"
        ),
    )
    asyncio.create_task(scanner_loop())
    asyncio.create_task(watch_closing_lines())  # 📌 auto CLV
    asyncio.create_task(settle_completed_trades())  # 🏆 auto settle


def handle_shutdown(signum, frame):
    """Graceful shutdown — บันทึก state ก่อนปิด"""
    log.info("[Shutdown] กำลังบันทึก state...")
    save_snapshot()
    log.info("[Shutdown] saved. Bye!")
    os._exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT,  handle_shutdown)

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
    _app = app

    # Railway expose ได้แค่ 1 port — ใช้ polling เสมอ
    # Dashboard อยู่ที่ PORT, bot ใช้ polling (stable กว่าใน Railway)
    log.info("[Bot] Polling mode (Railway single-port compatible)")
    app.run_polling(drop_pending_updates=True)
