"""
╔══════════════════════════════════════════════════════════════════════╗
║  ARB BOT v6.0  —  Complete Edition                                  ║
║  1.  Odds Staleness Check    7.  Line Movement Alert (Pinnacle)     ║
║  2.  Max Odds Filter         8.  Dashboard History Chart            ║
║  3.  Alert Cooldown          9.  Multi-chat Support                 ║
║  4.  P&L Tracker             10. Reverse Line Movement (RLM)        ║
║  5.  Max Stake per Book      11. Steam Move Alert                   ║
║  6.  Dynamic Commission      12. CLV Tracker                        ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio, json, logging, os, re, signal, sqlite3, threading, uuid
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

ODDS_API_KEY    = _s("ODDS_API_KEY",    "3eb65e34745253e9240627121408823c")
TELEGRAM_TOKEN  = _s("TELEGRAM_TOKEN",  "8517689298:AAEgHOYN-zAOwsJ4LMYGQkLeZPTComJP4A8")
CHAT_ID         = _s("CHAT_ID",         "6415456688")
EXTRA_CHAT_IDS  = [c.strip() for c in _s("EXTRA_CHAT_IDS","").split(",") if c.strip()]  # 9. multi-chat
PORT            = _i("PORT",            8080)
DB_PATH         = _s("DB_PATH",         "/tmp/arb_bot.db")   # local fallback
TURSO_URL       = _s("TURSO_URL",       "")   # libsql://your-db.turso.io
TURSO_TOKEN     = _s("TURSO_TOKEN",     "")   # eyJ...
USE_TURSO       = bool(TURSO_URL and TURSO_TOKEN)

TOTAL_STAKE_THB = _d("TOTAL_STAKE_THB","10000")
USD_TO_THB      = _d("USD_TO_THB",     "35")
TOTAL_STAKE     = TOTAL_STAKE_THB / USD_TO_THB

MIN_PROFIT_PCT  = _d("MIN_PROFIT_PCT",  "0.015")
SCAN_INTERVAL   = _i("SCAN_INTERVAL",   300)
AUTO_SCAN_START = _s("AUTO_SCAN_START","true").lower() == "true"
QUOTA_WARN_AT   = _i("QUOTA_WARN_AT",   50)

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

_SPORTS_DEFAULT = "basketball_nba,baseball_mlb,mma_mixed_martial_arts"
SPORTS     = [s.strip() for s in _s("SPORTS",_SPORTS_DEFAULT).split(",") if s.strip()]
BOOKMAKERS = _s("BOOKMAKERS","pinnacle,onexbet,dafabet")

SPORT_EMOJI = {
    "basketball_nba":"🏀","basketball_euroleague":"🏀",
    "tennis_atp_wimbledon":"🎾","tennis_wta":"🎾",
    "baseball_mlb":"⚾","mma_mixed_martial_arts":"🥊",
    "esports_csgo":"🎮","esports_dota2":"🎮","esports_lol":"🎮",
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
    leg1_odds:   float
    leg2_odds:   float
    stake1_thb:  int
    stake2_thb:  int
    profit_pct:  float
    status:      str    # confirmed | rejected
    clv_leg1:    Optional[float] = None   # 12. CLV
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
    leg1_bm TEXT, leg2_bm TEXT, leg1_odds REAL, leg2_odds REAL,
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
        con = sqlite3.connect(DB_PATH)
        con.execute(sql, params)
        con.commit()
        con.close()
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
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(sql, params).fetchall()
        con.close()
        return rows
    except Exception as e:
        log.error(f"[DB] sqlite_query: {e}")
        return []

# ── SQLite local init (fallback) ──────────────────────────────────
def db_init_local():
    try:
        con = sqlite3.connect(DB_PATH)
        for stmt in CREATE_TABLES_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
        con.commit()
        con.close()
        log.info(f"[DB] SQLite local at {DB_PATH}")
    except Exception as e:
        log.error(f"[DB] local init: {e}")

def db_init():
    if not USE_TURSO:
        db_init_local()

# ── Write helpers ─────────────────────────────────────────────────
def db_save_trade(t: "TradeRecord"):
    asyncio.get_event_loop().create_task(_async_save_trade(t))

async def _async_save_trade(t: "TradeRecord"):
    await turso_exec(
        "INSERT OR REPLACE INTO trade_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (t.signal_id,t.event,t.sport,t.leg1_bm,t.leg2_bm,
         t.leg1_odds,t.leg2_odds,t.stake1_thb,t.stake2_thb,
         t.profit_pct,t.status,t.clv_leg1,t.clv_leg2,
         t.actual_profit_thb,t.settled_at,t.created_at)
    )

def db_save_opportunity(opp: dict):
    asyncio.get_event_loop().create_task(_async_save_opp(opp))

async def _async_save_opp(opp: dict):
    await turso_exec(
        "INSERT OR REPLACE INTO opportunity_log VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (opp["id"],opp["event"],opp["sport"],opp["profit_pct"],
         opp["leg1_bm"],opp["leg1_odds"],opp["leg2_bm"],opp["leg2_odds"],
         opp["stake1_thb"],opp["stake2_thb"],opp["created_at"],opp["status"])
    )

def db_update_opp_status(signal_id: str, status: str):
    asyncio.get_event_loop().create_task(
        turso_exec("UPDATE opportunity_log SET status=? WHERE id=?", (status, signal_id))
    )

def db_save_line_movement(lm: "LineMovement"):
    asyncio.get_event_loop().create_task(_async_save_lm(lm))

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
    asyncio.get_event_loop().create_task(
        turso_exec("INSERT OR REPLACE INTO bot_state VALUES (?,?)", (key, value))
    )

async def db_load_state_async(key: str, default: str = "") -> str:
    rows = await turso_query("SELECT value FROM bot_state WHERE key=?", (key,))
    return rows[0][0] if rows else default

def db_load_state(key: str, default: str = "") -> str:
    """Sync version (ใช้ SQLite local เท่านั้น สำหรับ startup)"""
    try:
        con = sqlite3.connect(DB_PATH)
        row = con.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
        con.close()
        return row[0] if row else default
    except:
        return default

async def db_load_all() -> tuple[list, list, list]:
    """โหลดทุกอย่างจาก DB (async)"""
    try:
        trades_rows = await turso_query(
            "SELECT * FROM trade_records ORDER BY created_at DESC LIMIT 500")
        trades = []
        for r in trades_rows:
            trades.append(TradeRecord(
                signal_id=r[0],event=r[1],sport=r[2],leg1_bm=r[3],leg2_bm=r[4],
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
                except: pass
        if critical:
            auto_scan = False


# ══════════════════════════════════════════════════════════════════
#  FUZZY MATCH
# ══════════════════════════════════════════════════════════════════
TEAM_ALIASES = {
    "lakers":"Los Angeles Lakers","la lakers":"Los Angeles Lakers",
    "clippers":"LA Clippers","warriors":"Golden State Warriors",
    "celtics":"Boston Celtics","heat":"Miami Heat","nets":"Brooklyn Nets",
    "bulls":"Chicago Bulls","spurs":"San Antonio Spurs","kings":"Sacramento Kings",
    "nuggets":"Denver Nuggets","suns":"Phoenix Suns","bucks":"Milwaukee Bucks",
    "sixers":"Philadelphia 76ers","76ers":"Philadelphia 76ers",
    "knicks":"New York Knicks","mavs":"Dallas Mavericks",
    "rockets":"Houston Rockets","raptors":"Toronto Raptors",
    "yankees":"New York Yankees","red sox":"Boston Red Sox",
    "dodgers":"Los Angeles Dodgers","cubs":"Chicago Cubs","astros":"Houston Astros",
    "navi":"Natus Vincere","faze":"FaZe Clan","g2":"G2 Esports",
    "liquid":"Team Liquid","og":"OG","secret":"Team Secret",
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
    """
    new_movements: list[LineMovement] = []
    now = datetime.now(timezone.utc)

    for sport, events in odds_by_sport.items():
        for event in events:
            home  = event.get("home_team","")
            away  = event.get("away_team","")
            ename = f"{home} vs {away}"

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
                                        if (now-t).seconds < 300
                                    ]
                                    is_steam = len(steam_tracker[steam_key]) >= 2

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
                                    new_movements.append(lm)
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


async def send_line_move_alerts(movements: list[LineMovement]):
    """ส่ง alert สำหรับ Line Movement"""
    for lm in movements:
        tags = []
        if lm.is_steam: tags.append("🌊 *STEAM MOVE*")
        if lm.is_rlm:   tags.append("🔄 *REVERSE LINE MOVEMENT*")
        if not tags:     tags.append("📊 *Line Movement*")

        pct_str = f"+{lm.pct_change:.1%}" if lm.pct_change > 0 else f"{lm.pct_change:.1%}"
        msg = (
            f"{'  '.join(tags)}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🏆 `{lm.event}`\n"
            f"📡 {lm.bookmaker} — {lm.outcome}\n"
            f"📉 {float(lm.odds_before):.3f} → {float(lm.odds_after):.3f} ({pct_str}) {lm.direction}\n"
        )
        if lm.is_rlm:
            msg += (f"\n💡 *Sharp Money Signal*\n"
                    f"Pinnacle ขยับ odds ลงแรง = มีเงินใหญ่เดิน\n"
                    f"Soft books ยังไม่ตาม → โอกาส value bet!")
        if lm.is_steam:
            msg += f"\n⚡ หลายเว็บขยับพร้อมกัน = สัญญาณแข็งแกร่ง"

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

async def async_fetch_polymarket(session: aiohttp.ClientSession) -> list[dict]:
    try:
        async with session.get(
            "https://clob.polymarket.com/markets",
            params={"active":True,"closed":False},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            data = await r.json(content_type=None)
            return data.get("data",[])
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
    except:
        pass
    return False

def is_valid_odds(odds: Decimal) -> bool:
    """2. กรอง odds ที่ผิดปกติ"""
    return MIN_ODDS_ALLOWED <= odds <= MAX_ODDS_ALLOWED

def is_on_cooldown(event: str, bm1: str, bm2: str) -> bool:
    """3. เช็ค alert cooldown"""
    key      = f"{event}|{bm1}|{bm2}"
    last     = alert_cooldown.get(key)
    if last and (datetime.now(timezone.utc) - last).seconds < ALERT_COOLDOWN_MIN * 60:
        return True
    return False

def find_polymarket(event_name: str, poly_markets: list) -> Optional[dict]:
    parts = [p.strip() for p in event_name.replace(" vs ","|").split("|")]
    if len(parts) < 2: return None
    ta, tb = parts[0], parts[1]
    best, best_score = None, 0
    for m in poly_markets:
        tokens = m.get("tokens",[])
        if len(tokens) < 2: continue
        title = m.get("question","")
        if fuzzy_match(ta, title, 0.3) and fuzzy_match(tb, title, 0.3):
            score = sum(1 for t in (normalize_team(ta).split()+normalize_team(tb).split()) if t in title.lower())
            if score > best_score:
                best_score, best = score, m
    if not best: return None
    tokens = best.get("tokens",[])
    pa = Decimal(str(tokens[0].get("price",0)))
    pb = Decimal(str(tokens[1].get("price",0)))
    if pa <= 0 or pb <= 0: return None
    slug = best.get("slug","")
    return {
        "market_url": f"https://polymarket.com/event/{slug}",
        "team_a": {"name": tokens[0].get("outcome",ta),
                   "odds_raw": (Decimal("1")/pa).quantize(Decimal("0.001")),
                   "odds": apply_slippage((Decimal("1")/pa).quantize(Decimal("0.001")),"polymarket"),
                   "token_id": tokens[0].get("token_id","")},
        "team_b": {"name": tokens[1].get("outcome",tb),
                   "odds_raw": (Decimal("1")/pb).quantize(Decimal("0.001")),
                   "odds": apply_slippage((Decimal("1")/pb).quantize(Decimal("0.001")),"polymarket"),
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
                        # 5. Apply max stake
                        s_a = apply_max_stake(s_a, best[a].bookmaker)
                        s_b = apply_max_stake(s_b, best[b].bookmaker)
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
    entry = {
        "id": opp.signal_id, "event": opp.event, "sport": opp.sport,
        "profit_pct": float(opp.profit_pct),
        "leg1_bm": opp.leg1.bookmaker, "leg1_odds": float(opp.leg1.odds),
        "leg2_bm": opp.leg2.bookmaker, "leg2_odds": float(opp.leg2.odds),
        "stake1_thb": int(opp.stake1*USD_TO_THB),
        "stake2_thb": int(opp.stake2*USD_TO_THB),
        "created_at": opp.created_at, "status": "pending",
    }
    opportunity_log.append(entry)
    db_save_opportunity(entry)   # 💾 save to DB
    if len(opportunity_log) > 100: opportunity_log.pop(0)

    emoji = SPORT_EMOJI.get(opp.sport,"🏆")
    s1 = (opp.stake1*USD_TO_THB).quantize(Decimal("1"))
    s2 = (opp.stake2*USD_TO_THB).quantize(Decimal("1"))
    w1 = (opp.stake1*opp.leg1.odds*USD_TO_THB).quantize(Decimal("1"))
    w2 = (opp.stake2*opp.leg2.odds*USD_TO_THB).quantize(Decimal("1"))
    tt = TOTAL_STAKE_THB.quantize(Decimal("1"))

    msg = (
        f"{emoji} *ARB FOUND — {opp.profit_pct:.2%}* _(หลัง fee)_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 {opp.commence} UTC\n"
        f"🏆 `{opp.event}`\n"
        f"💵 ทุน: *฿{int(tt):,}*  |  Credits: {api_remaining}\n"
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
    s1 = (opp.stake1*USD_TO_THB).quantize(Decimal("1"))
    s2 = (opp.stake2*USD_TO_THB).quantize(Decimal("1"))
    w1 = (opp.stake1*opp.leg1.odds*USD_TO_THB).quantize(Decimal("1"))
    w2 = (opp.stake2*opp.leg2.odds*USD_TO_THB).quantize(Decimal("1"))
    tt = TOTAL_STAKE_THB.quantize(Decimal("1"))

    # บันทึก trade
    tr = TradeRecord(
        signal_id=opp.signal_id, event=opp.event, sport=opp.sport,
        leg1_bm=opp.leg1.bookmaker, leg2_bm=opp.leg2.bookmaker,
        leg1_odds=float(opp.leg1.odds_raw), leg2_odds=float(opp.leg2.odds_raw),
        stake1_thb=int(s1), stake2_thb=int(s2),
        profit_pct=float(opp.profit_pct), status="confirmed",
    )
    trade_records.append(tr)
    db_save_trade(tr)            # 💾 save to DB
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
    except: return
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
    await update.message.reply_text(
        f"💰 *P&L Summary*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Confirmed   : {len(confirmed)} trades\n"
        f"Rejected    : {len(rejected)} trades\n"
        f"Est. Profit : ฿{total_profit:,.0f}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📈 CLV avg  : {clv_str}\n"
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
        f"📊 *ARB BOT v6.0*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Auto scan   : {s} ({SCAN_INTERVAL}s)\n"
        f"สแกนไปแล้ว  : {scan_count} รอบ\n"
        f"ล่าสุด      : {last_scan_time}\n"
        f"รอ confirm  : {len(pending)} | trade: {confirmed}\n"
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


async def scanner_loop():
    await asyncio.sleep(3)
    log.info(f"[Scanner] v6.0 | interval={SCAN_INTERVAL}s | sports={len(SPORTS)}")
    while True:
        if auto_scan:
            try: await do_scan()
            except Exception as e: log.error(f"[Scanner] {e}")
        await asyncio.sleep(SCAN_INTERVAL)


# ══════════════════════════════════════════════════════════════════
#  8. DASHBOARD (ปรับปรุงใหม่พร้อมกราฟ + Line Movement section)
# ══════════════════════════════════════════════════════════════════
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta http-equiv="refresh" content="20">
<title>ARB BOT v6.0</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#e6edf3;font-family:'Segoe UI',sans-serif;padding:20px}
h1{color:#58a6ff;font-size:1.4rem;margin-bottom:2px}
.sub{color:#8b949e;font-size:.8rem;margin-bottom:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:16px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px}
.card .label{color:#8b949e;font-size:.7rem;text-transform:uppercase;letter-spacing:.5px}
.card .value{font-size:1.5rem;font-weight:700;margin-top:4px}
.green{color:#3fb950}.red{color:#f85149}.yellow{color:#d29922}.blue{color:#58a6ff}.purple{color:#bc8cff}
.quota-wrap{margin-bottom:16px}
.quota-bar{background:#21262d;border-radius:4px;height:6px;overflow:hidden}
.quota-fill{height:100%;border-radius:4px;transition:width .3s}
.quota-text{color:#8b949e;font-size:.72rem;margin-top:4px}
.section{color:#8b949e;font-size:.75rem;text-transform:uppercase;letter-spacing:.5px;margin:16px 0 8px}
table{width:100%;border-collapse:collapse;background:#161b22;border-radius:8px;overflow:hidden;margin-bottom:16px}
th{background:#21262d;color:#8b949e;font-size:.7rem;text-transform:uppercase;padding:8px 12px;text-align:left}
td{padding:8px 12px;border-top:1px solid #21262d;font-size:.82rem}
tr:hover td{background:#1c2128}
.badge{display:inline-block;padding:2px 7px;border-radius:10px;font-size:.68rem;font-weight:600}
.bp{background:#1f3d5c;color:#58a6ff}.bc{background:#1a3a2a;color:#3fb950}.br{background:#3d1f1f;color:#f85149}
.profit{color:#3fb950;font-weight:700}
.steam{color:#58a6ff}.rlm{color:#bc8cff}
.chart-wrap{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin-bottom:16px}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:12px}
@media(max-width:600px){.two-col{grid-template-columns:1fr}}
</style>
</head>
<body>
<h1>🤖 ARB BOT v6.0</h1>
<div class="sub">รีเฟรชทุก 20 วินาที — Fuzzy Match + Async + Slippage + Line Movement + CLV</div>

<div class="grid" id="stats"></div>
<div class="quota-wrap">
  <div class="quota-bar"><div class="quota-fill" id="qFill"></div></div>
  <div class="quota-text" id="qText"></div>
</div>

<div class="two-col">
  <div class="chart-wrap">
    <div class="section">📈 Profit Opportunity History</div>
    <canvas id="profitChart" height="140"></canvas>
  </div>
  <div class="chart-wrap">
    <div class="section">📡 Line Movements</div>
    <canvas id="lineChart" height="140"></canvas>
  </div>
</div>

<div class="section">🌊 Line Movement Log</div>
<table>
  <thead><tr><th>Event</th><th>Bookmaker</th><th>Outcome</th><th>Before</th><th>After</th><th>Change</th><th>Type</th><th>เวลา</th></tr></thead>
  <tbody id="lineBody"></tbody>
</table>

<div class="section">📋 Opportunity Log</div>
<table>
  <thead><tr><th>Event</th><th>Leg 1</th><th>Leg 2</th><th>Profit</th><th>ทุน</th><th>เวลา</th><th>Status</th></tr></thead>
  <tbody id="oppBody"></tbody>
</table>

<div class="section">💰 P&L Summary</div>
<div id="pnl" class="card" style="margin-bottom:16px"></div>

<script>
let profitChart, lineChart;

function initCharts(opps, moves) {
  // Profit chart
  const labels = opps.slice(-20).map(o => o.event.split(' vs ')[0].substring(0,10));
  const data   = opps.slice(-20).map(o => +(o.profit_pct*100).toFixed(2));
  const ctx1   = document.getElementById('profitChart').getContext('2d');
  if (profitChart) profitChart.destroy();
  profitChart = new Chart(ctx1, {
    type:'bar',
    data:{labels, datasets:[{label:'Profit %', data,
      backgroundColor: data.map(v => v>2?'#3fb950':v>1?'#d29922':'#58a6ff'),
      borderRadius:4}]},
    options:{plugins:{legend:{display:false}},scales:{
      x:{ticks:{color:'#8b949e',font:{size:9}},grid:{color:'#21262d'}},
      y:{ticks:{color:'#8b949e',font:{size:9}},grid:{color:'#21262d'},
         title:{display:true,text:'%',color:'#8b949e',font:{size:9}}}
    }}
  });

  // Line movement chart
  const lmLabels = moves.slice(-15).map(m => m.event.split(' vs ')[0].substring(0,8));
  const lmData   = moves.slice(-15).map(m => +(m.pct_change*100).toFixed(2));
  const ctx2     = document.getElementById('lineChart').getContext('2d');
  if (lineChart) lineChart.destroy();
  lineChart = new Chart(ctx2, {
    type:'bar',
    data:{labels:lmLabels, datasets:[{label:'Move %', data:lmData,
      backgroundColor: lmData.map(v => v<0?'#f85149':'#3fb950'),
      borderRadius:4}]},
    options:{plugins:{legend:{display:false}},scales:{
      x:{ticks:{color:'#8b949e',font:{size:9}},grid:{color:'#21262d'}},
      y:{ticks:{color:'#8b949e',font:{size:9}},grid:{color:'#21262d'},
         title:{display:true,text:'%',color:'#8b949e',font:{size:9}}}
    }}
  });
}

async function load() {
  const r = await fetch('/api/state');
  const d = await r.json();

  const qPct   = Math.round((d.api_remaining/500)*100);
  const qColor = qPct>30?'#3fb950':qPct>10?'#d29922':'#f85149';
  const scanC  = d.auto_scan?'green':'red';

  document.getElementById('stats').innerHTML = `
    <div class="card"><div class="label">Auto Scan</div><div class="value ${scanC}">${d.auto_scan?'🟢 ON':'🔴 OFF'}</div></div>
    <div class="card"><div class="label">สแกน</div><div class="value blue">${d.scan_count} รอบ</div></div>
    <div class="card"><div class="label">รอ Confirm</div><div class="value yellow">${d.pending_count}</div></div>
    <div class="card"><div class="label">API Credits</div><div class="value" style="color:${qColor}">${d.api_remaining}</div></div>
    <div class="card"><div class="label">Line Moves</div><div class="value purple">${d.line_move_count}</div></div>
    <div class="card"><div class="label">Trades</div><div class="value green">${d.confirmed_trades}</div></div>
  `;
  document.getElementById('qFill').style.cssText = `width:${qPct}%;background:${qColor}`;
  document.getElementById('qText').textContent = `Credits ${d.api_remaining}/500 (${qPct}%) | เตือนที่ ${d.quota_warn_at} | สแกนล่าสุด ${d.last_scan_time}`;

  initCharts(d.opportunities||[], d.line_movements||[]);

  // Line movement table
  const lmRows = (d.line_movements||[]).slice(-15).reverse().map(m => {
    const pct  = (m.pct_change*100).toFixed(1);
    const sign = m.pct_change>0?'+':'';
    const tags = (m.is_steam?'<span class="steam">🌊Steam</span> ':'')+(m.is_rlm?'<span class="rlm">🔄RLM</span>':'');
    const t    = new Date(m.ts).toLocaleTimeString('th-TH',{hour:'2-digit',minute:'2-digit'});
    return `<tr><td>${m.event}</td><td>${m.bookmaker}</td><td>${m.outcome}</td>
      <td>${m.odds_before.toFixed(3)}</td><td>${m.odds_after.toFixed(3)}</td>
      <td style="color:${m.pct_change<0?'#f85149':'#3fb950'}">${sign}${pct}%</td>
      <td>${tags||'—'}</td><td>${t}</td></tr>`;
  }).join('');
  document.getElementById('lineBody').innerHTML = lmRows||'<tr><td colspan="8" style="text-align:center;color:#8b949e;padding:20px">ยังไม่มีข้อมูล</td></tr>';

  // Opportunity table
  const oppRows = (d.opportunities||[]).slice(-20).reverse().map(o => {
    const bc   = o.status==='pending'?'bp':o.status==='confirmed'?'bc':'br';
    const bl   = o.status==='pending'?'รอ':o.status==='confirmed'?'✅':'❌';
    const t    = new Date(o.created_at).toLocaleTimeString('th-TH',{hour:'2-digit',minute:'2-digit'});
    return `<tr><td>${o.event}</td><td>${o.leg1_bm} @${o.leg1_odds.toFixed(2)}</td>
      <td>${o.leg2_bm} @${o.leg2_odds.toFixed(2)}</td>
      <td class="profit">+${(o.profit_pct*100).toFixed(2)}%</td>
      <td>฿${o.stake1_thb.toLocaleString()}/฿${o.stake2_thb.toLocaleString()}</td>
      <td>${t}</td><td><span class="badge ${bc}">${bl}</span></td></tr>`;
  }).join('');
  document.getElementById('oppBody').innerHTML = oppRows||'<tr><td colspan="7" style="text-align:center;color:#8b949e;padding:20px">ยังไม่พบ opportunity</td></tr>';

  // P&L
  const p = d.pnl;
  document.getElementById('pnl').innerHTML = `
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px">
      <div><div class="label">Confirmed</div><div class="value green">${p.confirmed}</div></div>
      <div><div class="label">Rejected</div><div class="value red">${p.rejected}</div></div>
      <div><div class="label">Est. Profit</div><div class="value green">฿${p.est_profit.toLocaleString()}</div></div>
      <div><div class="label">CLV avg</div><div class="value ${p.avg_clv>=0?'green':'red'}">${p.avg_clv!==null?p.avg_clv.toFixed(2)+'%':'—'}</div></div>
    </div>`;
}
load();
</script>
</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def do_GET(self):
        # Health check endpoint สำหรับ Railway
        if self.path == "/health":
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/api/state":
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

    # โหลด bot state จาก local SQLite ก่อน (เร็ว)
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

    app.add_error_handler(error_handler)
    threading.Thread(target=start_dashboard, daemon=True).start()

    is_restored = len(trade_records) > 0 or scan_count > 0
    db_mode_str  = "☁️ Turso" if (_turso is not None) else "💾 SQLite"
    restore_note = f"♻️ {db_mode_str}: {len(trade_records)} trades, {scan_count} scans" if is_restored else f"🆕 {db_mode_str}: fresh start"

    await app.bot.send_message(
        chat_id=CHAT_ID, parse_mode="Markdown",
        text=(
            "🤖 *ARB BOT v7.0 — Production Ready*\n"
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


def handle_shutdown(signum, frame):
    """Graceful shutdown — บันทึก state ก่อนปิด"""
    log.info("[Shutdown] กำลังบันทึก state...")
    save_snapshot()
    log.info("[Shutdown] saved. Bye!")
    os._exit(0)


if __name__ == "__main__":
    # Graceful shutdown handlers
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
    app.run_polling(drop_pending_updates=True)
