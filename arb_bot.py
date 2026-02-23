"""
╔══════════════════════════════════════════════════════════════════╗
║            ARB BOT v4.0  —  Polymarket vs Betting Sites         ║
║  Sports : NBA 🏀 | Tennis 🎾 | MLB ⚾ | UFC 🥊 | CS2/Dota2 🎮  ║
║  NEW    : Fuzzy Match + Async Fetch + Slippage Calculator       ║
║  Mode   : Semi-auto  —  Telegram ✅ Confirm / ❌ Reject          ║
╚══════════════════════════════════════════════════════════════════╝
คำสั่ง Telegram:
  /scan on   — เปิด auto scan
  /scan off  — ปิด auto scan
  /now       — สแกนทันที 1 รอบ
  /status    — ดูสถานะทั้งหมด
"""

import asyncio
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

load_dotenv()

# ══════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════
def _d(k, v): return Decimal(os.getenv(k, v))
def _s(k, v): return os.getenv(k, v)
def _i(k, v): return int(os.getenv(k, str(v)))

ODDS_API_KEY    = _s("ODDS_API_KEY",    "0205bd80de0af87de624e134b8c38db3")
TELEGRAM_TOKEN  = _s("TELEGRAM_TOKEN",  "8517689298:AAEgHOYN-zAOwsJ4LMYGQkLeZPTComJP4A8")
CHAT_ID         = _s("CHAT_ID",         "6415456688")

TOTAL_STAKE_THB = _d("TOTAL_STAKE_THB", "10000")
USD_TO_THB      = _d("USD_TO_THB",      "35")
TOTAL_STAKE     = TOTAL_STAKE_THB / USD_TO_THB

MIN_PROFIT_PCT  = _d("MIN_PROFIT_PCT",  "0.015")
SCAN_INTERVAL   = _i("SCAN_INTERVAL",   300)
AUTO_SCAN_START = _s("AUTO_SCAN_START", "true").lower() == "true"

_SPORTS_DEFAULT = "basketball_nba,baseball_mlb,mma_mixed_martial_arts,esports_csgo,esports_dota2"
SPORTS = [s.strip() for s in _s("SPORTS", _SPORTS_DEFAULT).split(",") if s.strip()]
BOOKMAKERS = _s("BOOKMAKERS", "pinnacle,onexbet,dafabet")

SPORT_EMOJI = {
    "basketball_nba":         "🏀",
    "basketball_euroleague":  "🏀",
    "tennis_atp_wimbledon":   "🎾",
    "tennis_wta":             "🎾",
    "baseball_mlb":           "⚾",
    "mma_mixed_martial_arts": "🥊",
    "esports_csgo":           "🎮",
    "esports_dota2":          "🎮",
    "esports_lol":            "🎮",
}

# ── ค่าคอมมิชชั่น / slippage แต่ละเว็บ ───────────────────────────
# ใส่เป็น Decimal เช่น 0.02 = 2%
COMMISSION = {
    "polymarket": Decimal("0.02"),   # Polymarket ค่า fee 2%
    "pinnacle":   Decimal("0.00"),   # Pinnacle ไม่มีคอม (margin ใน odds อยู่แล้ว)
    "1xbet":      Decimal("0.00"),
    "onexbet":    Decimal("0.00"),
    "dafabet":    Decimal("0.00"),
}


# ══════════════════════════════════════════════════════════════════
#  DATA MODELS
# ══════════════════════════════════════════════════════════════════
@dataclass
class OddsLine:
    bookmaker:  str
    outcome:    str
    odds:       Decimal          # decimal odds หลัง slippage
    odds_raw:   Decimal          # decimal odds ก่อน slippage
    market_url: str  = ""
    raw:        dict = field(default_factory=dict)

@dataclass
class ArbOpportunity:
    signal_id:   str
    sport:       str
    event:       str
    commence:    str
    leg1:        OddsLine
    leg2:        OddsLine
    profit_pct:  Decimal
    stake1:      Decimal
    stake2:      Decimal


# ══════════════════════════════════════════════════════════════════
#  STATE
# ══════════════════════════════════════════════════════════════════
pending:        dict[str, ArbOpportunity] = {}
seen_signals:   set[str]                  = set()
auto_scan:      bool                      = AUTO_SCAN_START
scan_count:     int                       = 0
last_scan_time: str                       = "ยังไม่ได้สแกน"
_app:           Optional[Application]     = None


# ══════════════════════════════════════════════════════════════════
#  🆕 UPGRADE 1: FUZZY NAME MATCHING
# ══════════════════════════════════════════════════════════════════

# Alias dictionary — ชื่อย่อ → ชื่อเต็ม
TEAM_ALIASES: dict[str, str] = {
    # NBA
    "lakers":       "Los Angeles Lakers",
    "la lakers":    "Los Angeles Lakers",
    "lal":          "Los Angeles Lakers",
    "clippers":     "LA Clippers",
    "la clippers":  "LA Clippers",
    "warriors":     "Golden State Warriors",
    "gsw":          "Golden State Warriors",
    "celtics":      "Boston Celtics",
    "bos":          "Boston Celtics",
    "heat":         "Miami Heat",
    "mia":          "Miami Heat",
    "nets":         "Brooklyn Nets",
    "bkn":          "Brooklyn Nets",
    "bulls":        "Chicago Bulls",
    "chi":          "Chicago Bulls",
    "spurs":        "San Antonio Spurs",
    "sas":          "San Antonio Spurs",
    "kings":        "Sacramento Kings",
    "sac":          "Sacramento Kings",
    "nuggets":      "Denver Nuggets",
    "den":          "Denver Nuggets",
    "suns":         "Phoenix Suns",
    "phx":          "Phoenix Suns",
    "bucks":        "Milwaukee Bucks",
    "mil":          "Milwaukee Bucks",
    "sixers":       "Philadelphia 76ers",
    "76ers":        "Philadelphia 76ers",
    "phi":          "Philadelphia 76ers",
    "knicks":       "New York Knicks",
    "nyk":          "New York Knicks",
    "mavs":         "Dallas Mavericks",
    "dal":          "Dallas Mavericks",
    "rockets":      "Houston Rockets",
    "hou":          "Houston Rockets",
    "raptors":      "Toronto Raptors",
    "tor":          "Toronto Raptors",
    # MLB
    "yankees":      "New York Yankees",
    "ny yankees":   "New York Yankees",
    "red sox":      "Boston Red Sox",
    "bos red sox":  "Boston Red Sox",
    "dodgers":      "Los Angeles Dodgers",
    "la dodgers":   "Los Angeles Dodgers",
    "cubs":         "Chicago Cubs",
    "chi cubs":     "Chicago Cubs",
    "astros":       "Houston Astros",
    "hou astros":   "Houston Astros",
    # Esports
    "navi":         "Natus Vincere",
    "na`vi":        "Natus Vincere",
    "faze":         "FaZe Clan",
    "faze clan":    "FaZe Clan",
    "g2":           "G2 Esports",
    "liquid":       "Team Liquid",
    "t1":           "T1",
    "eg":           "Evil Geniuses",
    "og":           "OG",
    "secret":       "Team Secret",
    "lgd":          "PSG.LGD",
}


def normalize_team(name: str) -> str:
    """แปลงชื่อทีมให้เป็นมาตรฐาน lowercase ไม่มี punctuation"""
    n = name.lower().strip()
    n = re.sub(r"[^\w\s]", "", n)   # ลบ punctuation
    n = re.sub(r"\s+", " ", n)      # ลด whitespace
    return n


def resolve_alias(name: str) -> str:
    """ค้นหา alias → ชื่อเต็ม"""
    key = normalize_team(name)
    return TEAM_ALIASES.get(key, name)


def fuzzy_match(name_a: str, name_b: str, threshold: float = 0.6) -> bool:
    """
    เช็คว่า 2 ชื่อทีมคือทีมเดียวกันไหม
    วิธี: token overlap + alias resolution
    """
    # Resolve aliases ก่อน
    a = normalize_team(resolve_alias(name_a))
    b = normalize_team(resolve_alias(name_b))

    if a == b:
        return True

    # Token overlap score
    tokens_a = set(a.split())
    tokens_b = set(b.split())

    # ลบ stopwords
    stopwords = {"the", "fc", "cf", "sc", "ac", "de", "city", "united", "of", "and"}
    tokens_a -= stopwords
    tokens_b -= stopwords

    if not tokens_a or not tokens_b:
        return False

    intersection = tokens_a & tokens_b
    union        = tokens_a | tokens_b
    jaccard      = len(intersection) / len(union)

    # Substring check (เช่น "Lakers" อยู่ใน "Los Angeles Lakers")
    substring = (a in b) or (b in a)

    # ตัวอักษรแรก 5 ตัวตรงกัน
    prefix = a[:5] == b[:5] and len(a) >= 5

    return jaccard >= threshold or substring or prefix


def match_team_to_outcome(poly_team: str, bm_outcomes: list[str]) -> Optional[str]:
    """หา outcome ใน betting site ที่ตรงกับ poly_team"""
    for out in bm_outcomes:
        if fuzzy_match(poly_team, out):
            return out
    return None


# ══════════════════════════════════════════════════════════════════
#  🆕 UPGRADE 2: ASYNC FETCH
# ══════════════════════════════════════════════════════════════════

async def async_fetch_odds(session: aiohttp.ClientSession, sport_key: str) -> list[dict]:
    """ดึง Odds API แบบ async"""
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey":     ODDS_API_KEY,
        "regions":    "eu,uk,au",
        "markets":    "h2h",
        "oddsFormat": "decimal",
        "bookmakers": BOOKMAKERS,
    }
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            remaining = r.headers.get("x-requests-remaining", "?")
            data = await r.json(content_type=None)
            if isinstance(data, list):
                log.info(f"[OddsAPI] {sport_key} | events={len(data)} | remaining={remaining}")
                return data
            log.warning(f"[OddsAPI] {sport_key} unexpected: {data}")
            return []
    except Exception as e:
        log.error(f"[OddsAPI] {sport_key}: {e}")
        return []


async def async_fetch_polymarket(session: aiohttp.ClientSession) -> list[dict]:
    """ดึง Polymarket markets แบบ async"""
    try:
        async with session.get(
            "https://clob.polymarket.com/markets",
            params={"active": True, "closed": False},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            data = await r.json(content_type=None)
            return data.get("data", [])
    except Exception as e:
        log.debug(f"[Polymarket] fetch: {e}")
        return []


async def fetch_all_async(sports: list[str]) -> tuple[dict[str, list], list[dict]]:
    """
    🚀 ดึงข้อมูลทุกแหล่งพร้อมกัน (parallel)
    return: (odds_by_sport, poly_markets)
    """
    async with aiohttp.ClientSession() as session:
        # สร้าง tasks ทั้งหมดพร้อมกัน
        odds_tasks  = [async_fetch_odds(session, sport) for sport in sports]
        poly_task   = async_fetch_polymarket(session)

        # รันพร้อมกัน!
        results = await asyncio.gather(*odds_tasks, poly_task)

    poly_markets = results[-1]
    odds_by_sport = {sport: results[i] for i, sport in enumerate(sports)}

    return odds_by_sport, poly_markets


# ══════════════════════════════════════════════════════════════════
#  🆕 UPGRADE 3: SLIPPAGE CALCULATOR
# ══════════════════════════════════════════════════════════════════

def apply_slippage(odds: Decimal, bookmaker: str) -> Decimal:
    """
    หักค่า commission/slippage ออกจาก odds จริง
    effective_odds = odds * (1 - commission)
    เช่น odds=2.00, commission=2% → effective=1.96
    """
    bm_key = bookmaker.lower()
    # หา commission
    commission = Decimal("0")
    for key, val in COMMISSION.items():
        if key in bm_key:
            commission = val
            break
    return (odds * (Decimal("1") - commission)).quantize(Decimal("0.001"))


def calc_arb(odds_a: Decimal, odds_b: Decimal) -> tuple[Decimal, Decimal, Decimal]:
    """คำนวณ arb หลัง slippage"""
    inv_a  = Decimal("1") / odds_a
    inv_b  = Decimal("1") / odds_b
    margin = inv_a + inv_b
    if margin >= Decimal("1"):
        return Decimal("0"), Decimal("0"), Decimal("0")
    profit_pct = (Decimal("1") - margin) / margin
    stake_a = (TOTAL_STAKE * inv_a / margin).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    stake_b = (TOTAL_STAKE - stake_a).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return profit_pct, stake_a, stake_b


# ══════════════════════════════════════════════════════════════════
#  SCAN ALL SPORTS (ใช้ async fetch + fuzzy match + slippage)
# ══════════════════════════════════════════════════════════════════

def find_polymarket(event_name: str, poly_markets: list[dict]) -> Optional[dict]:
    """
    Match event กับ Polymarket โดยใช้ fuzzy matching
    """
    parts = [p.strip() for p in event_name.replace(" vs ", "|").split("|")]
    if len(parts) < 2:
        return None
    team_a, team_b = parts[0], parts[1]

    best_match = None
    best_score = 0.0

    for m in poly_markets:
        title  = m.get("question", "")
        tokens = m.get("tokens", [])
        if len(tokens) < 2:
            continue

        # fuzzy match ชื่อทีมทั้งสองกับ title
        match_a = fuzzy_match(team_a, title, threshold=0.3)
        match_b = fuzzy_match(team_b, title, threshold=0.3)

        if match_a and match_b:
            # คำนวณ score จาก token overlap
            ta = normalize_team(team_a)
            tb = normalize_team(team_b)
            tl = normalize_team(title)
            score = sum(1 for tok in (ta.split() + tb.split()) if tok in tl)
            if score > best_score:
                best_score = score
                best_match = m

    if not best_match:
        return None

    tokens = best_match.get("tokens", [])
    prob_a = Decimal(str(tokens[0].get("price", 0)))
    prob_b = Decimal(str(tokens[1].get("price", 0)))
    if prob_a <= 0 or prob_b <= 0:
        return None

    return {
        "market_url": f"https://polymarket.com/event/{best_match.get('slug','')}",
        "team_a": {
            "name":     tokens[0].get("outcome", team_a),
            "odds_raw": (Decimal("1") / prob_a).quantize(Decimal("0.001")),
            "odds":     apply_slippage((Decimal("1") / prob_a).quantize(Decimal("0.001")), "polymarket"),
            "token_id": tokens[0].get("token_id", ""),
        },
        "team_b": {
            "name":     tokens[1].get("outcome", team_b),
            "odds_raw": (Decimal("1") / prob_b).quantize(Decimal("0.001")),
            "odds":     apply_slippage((Decimal("1") / prob_b).quantize(Decimal("0.001")), "polymarket"),
            "token_id": tokens[1].get("token_id", ""),
        },
    }


def scan_all(odds_by_sport: dict[str, list], poly_markets: list[dict]) -> list[ArbOpportunity]:
    """สแกนหา arb จากข้อมูลที่ดึงมาแล้ว"""
    found: list[ArbOpportunity] = []

    for sport_key, events in odds_by_sport.items():
        for event in events:
            home       = event.get("home_team", "")
            away       = event.get("away_team", "")
            event_name = f"{home} vs {away}"
            commence   = event.get("commence_time", "")[:16].replace("T", " ")

            # ── รวบรวม best odds จาก betting sites + apply slippage ──
            best: dict[str, OddsLine] = {}
            for bm in event.get("bookmakers", []):
                bm_key  = bm.get("key", "")
                bm_name = bm.get("title", bm_key)
                for mkt in bm.get("markets", []):
                    if mkt.get("key") != "h2h":
                        continue
                    for out in mkt.get("outcomes", []):
                        name      = out.get("name", "")
                        odds_raw  = Decimal(str(out.get("price", 1)))
                        odds_eff  = apply_slippage(odds_raw, bm_key)
                        if name not in best or odds_eff > best[name].odds:
                            best[name] = OddsLine(
                                bookmaker = bm_name,
                                outcome   = name,
                                odds      = odds_eff,
                                odds_raw  = odds_raw,
                                raw       = {"bm_key": bm_key, "event_id": event.get("id", "")},
                            )

            # ── Polymarket fuzzy match + slippage ──
            poly = find_polymarket(event_name, poly_markets)
            if poly:
                for side, team_name in [("team_a", home), ("team_b", away)]:
                    p     = poly[side]
                    # fuzzy match ชื่อ outcome
                    matched = match_team_to_outcome(p["name"], list(best.keys()))
                    key     = matched if matched else team_name
                    if key not in best or p["odds"] > best[key].odds:
                        best[key] = OddsLine(
                            bookmaker  = "Polymarket",
                            outcome    = key,
                            odds       = p["odds"],
                            odds_raw   = p["odds_raw"],
                            market_url = poly["market_url"],
                            raw        = {"token_id": p["token_id"]},
                        )

            # ── เช็ค arb ทุก pair ──
            outcomes = list(best.keys())
            for i in range(len(outcomes)):
                for j in range(i + 1, len(outcomes)):
                    oc_a, oc_b = outcomes[i], outcomes[j]
                    if best[oc_a].bookmaker == best[oc_b].bookmaker:
                        continue
                    profit_pct, s_a, s_b = calc_arb(best[oc_a].odds, best[oc_b].odds)
                    if profit_pct >= MIN_PROFIT_PCT:
                        found.append(ArbOpportunity(
                            signal_id  = str(uuid.uuid4())[:8],
                            sport      = sport_key,
                            event      = event_name,
                            commence   = commence,
                            leg1       = best[oc_a],
                            leg2       = best[oc_b],
                            profit_pct = profit_pct,
                            stake1     = s_a,
                            stake2     = s_b,
                        ))
                        log.info(
                            f"[ARB] {event_name} | "
                            f"{best[oc_a].bookmaker}({oc_a}@{best[oc_a].odds_raw}→{best[oc_a].odds}) vs "
                            f"{best[oc_b].bookmaker}({oc_b}@{best[oc_b].odds_raw}→{best[oc_b].odds}) | "
                            f"profit={profit_pct:.2%}"
                        )
    return found


# ══════════════════════════════════════════════════════════════════
#  SEND TELEGRAM ALERT
# ══════════════════════════════════════════════════════════════════
async def send_alert(opp: ArbOpportunity):
    pending[opp.signal_id] = opp
    emoji = SPORT_EMOJI.get(opp.sport, "🏆")

    stake1_thb = (opp.stake1 * USD_TO_THB).quantize(Decimal("1"))
    stake2_thb = (opp.stake2 * USD_TO_THB).quantize(Decimal("1"))
    win1_thb   = (opp.stake1 * opp.leg1.odds * USD_TO_THB).quantize(Decimal("1"))
    win2_thb   = (opp.stake2 * opp.leg2.odds * USD_TO_THB).quantize(Decimal("1"))
    total_thb  = TOTAL_STAKE_THB.quantize(Decimal("1"))
    profit1    = win1_thb - total_thb
    profit2    = win2_thb - total_thb

    # แสดง slippage ถ้ามี
    def slip_note(leg: OddsLine) -> str:
        if leg.odds != leg.odds_raw:
            return f" (raw {leg.odds_raw} → after fee {leg.odds})"
        return ""

    msg = (
        f"{emoji} *ARB FOUND — {opp.profit_pct:.2%} profit* _(หลังหัก fee แล้ว)_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 {opp.commence} UTC\n"
        f"🏆 `{opp.event}`\n"
        f"💵 ทุนรวม: *฿{int(total_thb):,}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"{'ช่องทาง':<12} {'ฝั่ง':<16} {'Odds':>5} {'วาง':>8} {'ได้คืน':>9}\n"
        f"{'─'*53}\n"
        f"{'🔵 '+opp.leg1.bookmaker:<12} {opp.leg1.outcome:<16} "
        f"{float(opp.leg1.odds):>5.3f} "
        f"{'฿'+str(int(stake1_thb)):>8} {'฿'+str(int(win1_thb)):>9}\n"
        f"{'🟠 '+opp.leg2.bookmaker:<12} {opp.leg2.outcome:<16} "
        f"{float(opp.leg2.odds):>5.3f} "
        f"{'฿'+str(int(stake2_thb)):>8} {'฿'+str(int(win2_thb)):>9}\n"
        f"{'─'*53}\n"
        f"{'รวม':<35} {'฿'+str(int(total_thb)):>8}\n"
        f"```\n"
        f"📊 *ไม่ว่าใครชนะ (หลัง fee)*\n"
        f"   {opp.leg1.outcome} ชนะ → ฿{int(win1_thb):,} *(+฿{int(profit1):,})*"
        f"{slip_note(opp.leg1)}\n"
        f"   {opp.leg2.outcome} ชนะ → ฿{int(win2_thb):,} *(+฿{int(profit2):,})*"
        f"{slip_note(opp.leg2)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 {opp.leg1.market_url or '—'}\n"
        f"🆔 `{opp.signal_id}`"
    )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅  Confirm & Execute", callback_data=f"confirm:{opp.signal_id}"),
        InlineKeyboardButton("❌  Reject",            callback_data=f"reject:{opp.signal_id}"),
    ]])

    await _app.bot.send_message(
        chat_id      = CHAT_ID,
        text         = msg,
        parse_mode   = "Markdown",
        reply_markup = keyboard,
    )
    log.info(f"[Telegram] Alert: {opp.signal_id} {opp.event} profit={opp.profit_pct:.2%}")


# ══════════════════════════════════════════════════════════════════
#  EXECUTE  (Manual steps หลังกด Confirm)
# ══════════════════════════════════════════════════════════════════
def manual_steps(leg: OddsLine, stake_thb: Decimal) -> str:
    bm = leg.bookmaker.lower()
    commission = Decimal("0")
    for key, val in COMMISSION.items():
        if key in bm:
            commission = val
            break
    fee_note = f"\n  ⚠️ Fee {commission:.1%} หักแล้วใน odds" if commission > 0 else ""

    if bm == "polymarket":
        return (
            f"  1. เปิด: {leg.market_url}\n"
            f"  2. เลือก: *{leg.outcome}*\n"
            f"  3. วาง: ฿{int(stake_thb)} USDC\n"
            f"  4. Token: `{leg.raw.get('token_id','—')}`{fee_note}"
        )
    elif "pinnacle" in bm:
        return (
            f"  1. เปิด Pinnacle → ค้นหา event\n"
            f"  2. เลือก: *{leg.outcome}* @ {leg.odds_raw}{fee_note}\n"
            f"  3. วาง: ฿{int(stake_thb)}"
        )
    else:
        return (
            f"  1. เปิด {leg.bookmaker} → ค้นหา event\n"
            f"  2. เลือก: *{leg.outcome}* @ {leg.odds_raw}{fee_note}\n"
            f"  3. วาง: ฿{int(stake_thb)}"
        )


async def execute_both(opp: ArbOpportunity) -> str:
    stake1_thb = (opp.stake1 * USD_TO_THB).quantize(Decimal("1"))
    stake2_thb = (opp.stake2 * USD_TO_THB).quantize(Decimal("1"))
    win1_thb   = (opp.stake1 * opp.leg1.odds * USD_TO_THB).quantize(Decimal("1"))
    win2_thb   = (opp.stake2 * opp.leg2.odds * USD_TO_THB).quantize(Decimal("1"))
    total_thb  = TOTAL_STAKE_THB.quantize(Decimal("1"))

    return (
        f"📋 *วิธีวางเงิน — {opp.event}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔵 *LEG 1 — {opp.leg1.bookmaker}*\n"
        f"{manual_steps(opp.leg1, stake1_thb)}\n\n"
        f"🟠 *LEG 2 — {opp.leg2.bookmaker}*\n"
        f"{manual_steps(opp.leg2, stake2_thb)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 ทุนรวม : ฿{int(total_thb):,}\n"
        f"   {opp.leg1.outcome} ชนะ → ฿{int(win1_thb):,} (+฿{int(win1_thb-total_thb):,})\n"
        f"   {opp.leg2.outcome} ชนะ → ฿{int(win2_thb):,} (+฿{int(win2_thb-total_thb):,})"
    )


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, signal_id = query.data.split(":", 1)
    except ValueError:
        return
    opp = pending.pop(signal_id, None)
    if opp is None:
        await query.edit_message_text(query.message.text + "\n\n⚠️ Signal หมดอายุแล้ว")
        return
    original = query.message.text
    if action == "reject":
        await query.edit_message_text(original + "\n\n❌ *REJECTED*", parse_mode="Markdown")
        return
    await query.edit_message_text(original + "\n\n⏳ *กำลังเตรียม...*", parse_mode="Markdown")
    result = await execute_both(opp)
    await query.edit_message_text(original + "\n\n✅ *CONFIRMED*\n\n" + result, parse_mode="Markdown")


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global auto_scan
    args = context.args
    if not args:
        state = "🟢 เปิดอยู่" if auto_scan else "🔴 ปิดอยู่"
        await update.message.reply_text(f"Auto scan: {state}\nใช้ /scan on หรือ /scan off")
        return
    if args[0].lower() == "on":
        auto_scan = True
        seen_signals.clear()
        await update.message.reply_text(f"🟢 *Auto scan เปิดแล้ว* — ทุก {SCAN_INTERVAL}s", parse_mode="Markdown")
    elif args[0].lower() == "off":
        auto_scan = False
        await update.message.reply_text("🔴 *Auto scan ปิดแล้ว*\nใช้ /now เพื่อสแกน manual", parse_mode="Markdown")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = "🟢 เปิด" if auto_scan else "🔴 ปิด"
    fee_summary = " | ".join([f"{k}={v:.0%}" for k, v in COMMISSION.items() if v > 0]) or "ไม่มี"
    await update.message.reply_text(
        f"📊 *ARB BOT v4.0 Status*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Auto scan   : {state}\n"
        f"Interval    : {SCAN_INTERVAL}s\n"
        f"สแกนไปแล้ว  : {scan_count} รอบ\n"
        f"สแกนล่าสุด  : {last_scan_time}\n"
        f"รอ confirm  : {len(pending)} รายการ\n"
        f"Min profit  : {MIN_PROFIT_PCT:.1%} _(หลัง fee)_\n"
        f"ทุน/trade   : ฿{int(TOTAL_STAKE_THB):,}\n"
        f"Sports      : {len(SPORTS)} รายการ\n"
        f"Fees        : {fee_summary}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✨ v4.0: Fuzzy Match + Async + Slippage\n"
        f"/scan on · /scan off · /now · /status",
        parse_mode="Markdown",
    )


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 *กำลังสแกน (async)...*", parse_mode="Markdown")
    count = await do_scan()
    if count == 0:
        await update.message.reply_text(f"✅ สแกนเสร็จ — ไม่พบ opportunity > {MIN_PROFIT_PCT:.1%} (หลัง fee)")
    else:
        await update.message.reply_text(f"✅ พบ *{count}* opportunity ดูด้านบนครับ", parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════════
#  SCAN CORE
# ══════════════════════════════════════════════════════════════════
async def do_scan() -> int:
    global scan_count, last_scan_time

    # 🚀 ดึงทุกแหล่งพร้อมกัน
    odds_by_sport, poly_markets = await fetch_all_async(SPORTS)
    log.info(f"[Scanner] Polymarket markets={len(poly_markets)}")

    all_opps = scan_all(odds_by_sport, poly_markets)

    sent = 0
    for opp in sorted(all_opps, key=lambda x: x.profit_pct, reverse=True):
        key = f"{opp.event}|{opp.leg1.bookmaker}|{opp.leg2.bookmaker}"
        if key not in seen_signals:
            seen_signals.add(key)
            await send_alert(opp)
            await asyncio.sleep(1)
            sent += 1

    if len(seen_signals) > 500:
        seen_signals.clear()

    scan_count    += 1
    last_scan_time = datetime.now(timezone.utc).strftime("%d/%m %H:%M UTC")
    log.info(f"[Scanner] #{scan_count} done | found={len(all_opps)} | sent={sent}")
    return sent


async def scanner_loop():
    await asyncio.sleep(3)
    log.info(f"[Scanner] v4.0 started | interval={SCAN_INTERVAL}s | sports={len(SPORTS)}")
    while True:
        if auto_scan:
            try:
                await do_scan()
            except Exception as e:
                log.error(f"[Scanner] {e}")
        else:
            log.info("[Scanner] Paused")
        await asyncio.sleep(SCAN_INTERVAL)


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════
async def post_init(app: Application):
    await app.bot.send_message(
        chat_id    = CHAT_ID,
        parse_mode = "Markdown",
        text       = (
            "🤖 *ARB BOT v4.0 — Started!*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"✨ Fuzzy Match + Async Fetch + Slippage Calc\n"
            f"Sports    : {' | '.join([SPORT_EMOJI.get(s,'🏆') for s in SPORTS])}\n"
            f"Min profit: {MIN_PROFIT_PCT:.1%} _(หลัง fee)_\n"
            f"ทุน/trade : ฿{int(TOTAL_STAKE_THB):,}\n"
            f"Auto scan : {'🟢 เปิด' if auto_scan else '🔴 ปิด'} (ทุก {SCAN_INTERVAL}s)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"/scan on · /scan off · /now · /status"
        ),
    )
    asyncio.create_task(scanner_loop())


if __name__ == "__main__":
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

    _app = app
    app.run_polling(drop_pending_updates=True)
