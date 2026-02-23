"""
╔══════════════════════════════════════════════════════════════════╗
║            ARB BOT v5.0  —  Polymarket vs Betting Sites         ║
║  NEW v5: Web Dashboard + API Quota Alert + Railway Variables    ║
║  v4.0 : Fuzzy Match + Async Fetch + Slippage Calculator        ║
╚══════════════════════════════════════════════════════════════════╝
Railway Variables ที่ต้องตั้ง:
  ODDS_API_KEY      = your_key
  TELEGRAM_TOKEN    = your_token
  CHAT_ID           = your_chat_id
  TOTAL_STAKE_THB   = 10000
  USD_TO_THB        = 35
  MIN_PROFIT_PCT    = 0.015
  SCAN_INTERVAL     = 300
  AUTO_SCAN_START   = true
  SPORTS            = basketball_nba,baseball_mlb,mma_mixed_martial_arts
  BOOKMAKERS        = pinnacle,onexbet,dafabet
  QUOTA_WARN_AT     = 50        (แจ้งเตือนเมื่อ credits เหลือน้อยกว่านี้)
  PORT              = 8080      (Web dashboard port)
"""

import asyncio
import json
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass, field, asdict
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler

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
#  ⚙️  CONFIG — ทุก param อ่านจาก Railway Variables / .env
# ══════════════════════════════════════════════════════════════════
def _d(k, v): return Decimal(os.getenv(k, v))
def _s(k, v): return os.getenv(k, v)
def _i(k, v): return int(os.getenv(k, str(v)))

ODDS_API_KEY    = _s("ODDS_API_KEY",    "3eb65e34745253e9240627121408823c")
TELEGRAM_TOKEN  = _s("TELEGRAM_TOKEN",  "8517689298:AAEgHOYN-zAOwsJ4LMYGQkLeZPTComJP4A8")
CHAT_ID         = _s("CHAT_ID",         "6415456688")
PORT            = _i("PORT",            8080)

TOTAL_STAKE_THB = _d("TOTAL_STAKE_THB", "10000")
USD_TO_THB      = _d("USD_TO_THB",      "35")
TOTAL_STAKE     = TOTAL_STAKE_THB / USD_TO_THB

MIN_PROFIT_PCT  = _d("MIN_PROFIT_PCT",  "0.015")
SCAN_INTERVAL   = _i("SCAN_INTERVAL",   300)
AUTO_SCAN_START = _s("AUTO_SCAN_START", "true").lower() == "true"
QUOTA_WARN_AT   = _i("QUOTA_WARN_AT",   50)   # แจ้งเตือนเมื่อ credits เหลือน้อยกว่านี้

_SPORTS_DEFAULT = "basketball_nba,baseball_mlb,mma_mixed_martial_arts"
SPORTS     = [s.strip() for s in _s("SPORTS", _SPORTS_DEFAULT).split(",") if s.strip()]
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

COMMISSION = {
    "polymarket": Decimal("0.02"),
    "pinnacle":   Decimal("0.00"),
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
    odds:       Decimal
    odds_raw:   Decimal
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
    created_at:  str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status:      str = "pending"   # pending | confirmed | rejected


# ══════════════════════════════════════════════════════════════════
#  STATE
# ══════════════════════════════════════════════════════════════════
pending:          dict[str, ArbOpportunity] = {}
seen_signals:     set[str]                  = set()
auto_scan:        bool                      = AUTO_SCAN_START
scan_count:       int                       = 0
last_scan_time:   str                       = "ยังไม่ได้สแกน"
api_remaining:    int                       = 500      # credits คงเหลือ
api_used_session: int                       = 0        # ใช้ไปใน session นี้
quota_warned:     bool                      = False    # ส่งเตือนแล้วหรือยัง
opportunity_log:  list[dict]               = []        # ประวัติ opportunity ทั้งหมด
_app:             Optional[Application]    = None


# ══════════════════════════════════════════════════════════════════
#  🆕 QUOTA TRACKER
# ══════════════════════════════════════════════════════════════════
async def update_quota(remaining: int):
    """อัพเดท quota และส่งเตือนถ้าใกล้หมด"""
    global api_remaining, api_used_session, quota_warned, auto_scan

    api_remaining     = remaining
    api_used_session += 1

    # แจ้งเตือนตามเกณฑ์
    thresholds = [200, 100, QUOTA_WARN_AT, 20, 10, 5, 1]
    for t in thresholds:
        if remaining <= t and api_remaining > t:
            break
    
    should_warn = remaining <= QUOTA_WARN_AT and not quota_warned
    critical    = remaining <= 10

    if should_warn or critical:
        quota_warned = True
        level  = "🔴 *CRITICAL*" if critical else "⚠️ *WARNING*"
        msg = (
            f"{level} — Odds API Quota\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Credits เหลือ : *{remaining}*\n"
            f"ใช้ไปแล้ว    : {api_used_session} (session นี้)\n"
            f"ตั้งเตือนที่  : {QUOTA_WARN_AT}\n\n"
            f"{'🛑 หยุด scan ชั่วคราว!' if critical else '💡 พิจารณา /scan off หรืออัพเกรด plan'}\n"
            f"อัพเกรดที่ : https://the-odds-api.com"
        )
        if _app:
            try:
                await _app.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")
            except Exception as e:
                log.error(f"[Quota] ส่ง alert ไม่ได้: {e}")

        if critical and auto_scan:
            auto_scan = False
            log.warning("[Quota] Credits เหลือน้อยมาก — หยุด auto scan อัตโนมัติ")
            if _app:
                await _app.bot.send_message(
                    chat_id    = CHAT_ID,
                    text       = "🛑 *Auto scan ถูกหยุดอัตโนมัติ* เพราะ credits เหลือ ≤10\nใช้ /scan on เพื่อเปิดใหม่",
                    parse_mode = "Markdown",
                )


# ══════════════════════════════════════════════════════════════════
#  FUZZY NAME MATCHING
# ══════════════════════════════════════════════════════════════════
TEAM_ALIASES: dict[str, str] = {
    "lakers": "Los Angeles Lakers", "la lakers": "Los Angeles Lakers",
    "clippers": "LA Clippers", "warriors": "Golden State Warriors",
    "celtics": "Boston Celtics", "heat": "Miami Heat",
    "nets": "Brooklyn Nets", "bulls": "Chicago Bulls",
    "spurs": "San Antonio Spurs", "kings": "Sacramento Kings",
    "nuggets": "Denver Nuggets", "suns": "Phoenix Suns",
    "bucks": "Milwaukee Bucks", "sixers": "Philadelphia 76ers",
    "76ers": "Philadelphia 76ers", "knicks": "New York Knicks",
    "mavs": "Dallas Mavericks", "rockets": "Houston Rockets",
    "raptors": "Toronto Raptors", "yankees": "New York Yankees",
    "red sox": "Boston Red Sox", "dodgers": "Los Angeles Dodgers",
    "cubs": "Chicago Cubs", "astros": "Houston Astros",
    "navi": "Natus Vincere", "na`vi": "Natus Vincere",
    "faze": "FaZe Clan", "g2": "G2 Esports",
    "liquid": "Team Liquid", "og": "OG", "secret": "Team Secret",
}

def normalize_team(name: str) -> str:
    n = name.lower().strip()
    n = re.sub(r"[^\w\s]", "", n)
    return re.sub(r"\s+", " ", n)

def fuzzy_match(a: str, b: str, threshold: float = 0.6) -> bool:
    na = normalize_team(TEAM_ALIASES.get(normalize_team(a), a))
    nb = normalize_team(TEAM_ALIASES.get(normalize_team(b), b))
    if na == nb: return True
    ta = set(na.split()) - {"the","fc","cf","sc","ac","de","city","united","of","and"}
    tb = set(nb.split()) - {"the","fc","cf","sc","ac","de","city","united","of","and"}
    if not ta or not tb: return False
    jaccard = len(ta & tb) / len(ta | tb)
    return jaccard >= threshold or (na in nb) or (nb in na) or (na[:5] == nb[:5] and len(na) >= 5)


# ══════════════════════════════════════════════════════════════════
#  ASYNC FETCH
# ══════════════════════════════════════════════════════════════════
async def async_fetch_odds(session: aiohttp.ClientSession, sport_key: str) -> list[dict]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY, "regions": "eu,uk,au",
        "markets": "h2h", "oddsFormat": "decimal", "bookmakers": BOOKMAKERS,
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
            params={"active": True, "closed": False},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            data = await r.json(content_type=None)
            return data.get("data", [])
    except Exception as e:
        log.debug(f"[Polymarket] {e}")
        return []

async def fetch_all_async(sports: list[str]) -> tuple[dict, list]:
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[async_fetch_odds(session, s) for s in sports],
            async_fetch_polymarket(session),
        )
    return {s: results[i] for i, s in enumerate(sports)}, results[-1]


# ══════════════════════════════════════════════════════════════════
#  SLIPPAGE + ARB CALC
# ══════════════════════════════════════════════════════════════════
def apply_slippage(odds: Decimal, bookmaker: str) -> Decimal:
    bm  = bookmaker.lower()
    com = next((v for k, v in COMMISSION.items() if k in bm), Decimal("0"))
    return (odds * (Decimal("1") - com)).quantize(Decimal("0.001"))

def calc_arb(odds_a: Decimal, odds_b: Decimal):
    inv_a, inv_b = Decimal("1")/odds_a, Decimal("1")/odds_b
    margin = inv_a + inv_b
    if margin >= 1: return Decimal("0"), Decimal("0"), Decimal("0")
    profit = (Decimal("1") - margin) / margin
    s_a = (TOTAL_STAKE * inv_a / margin).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return profit, s_a, (TOTAL_STAKE - s_a).quantize(Decimal("0.01"), rounding=ROUND_DOWN)


# ══════════════════════════════════════════════════════════════════
#  SCAN
# ══════════════════════════════════════════════════════════════════
def find_polymarket(event_name: str, poly_markets: list) -> Optional[dict]:
    parts = [p.strip() for p in event_name.replace(" vs ", "|").split("|")]
    if len(parts) < 2: return None
    ta, tb = parts[0], parts[1]
    best, best_score = None, 0
    for m in poly_markets:
        tokens = m.get("tokens", [])
        if len(tokens) < 2: continue
        title = m.get("question", "")
        if fuzzy_match(ta, title, 0.3) and fuzzy_match(tb, title, 0.3):
            score = sum(1 for t in (normalize_team(ta).split() + normalize_team(tb).split()) if t in title.lower())
            if score > best_score:
                best_score, best = score, m
    if not best: return None
    tokens = best.get("tokens", [])
    pa = Decimal(str(tokens[0].get("price", 0)))
    pb = Decimal(str(tokens[1].get("price", 0)))
    if pa <= 0 or pb <= 0: return None
    slug = best.get("slug", "")
    return {
        "market_url": f"https://polymarket.com/event/{slug}",
        "team_a": {"name": tokens[0].get("outcome", ta),
                   "odds_raw": (Decimal("1")/pa).quantize(Decimal("0.001")),
                   "odds": apply_slippage((Decimal("1")/pa).quantize(Decimal("0.001")), "polymarket"),
                   "token_id": tokens[0].get("token_id","")},
        "team_b": {"name": tokens[1].get("outcome", tb),
                   "odds_raw": (Decimal("1")/pb).quantize(Decimal("0.001")),
                   "odds": apply_slippage((Decimal("1")/pb).quantize(Decimal("0.001")), "polymarket"),
                   "token_id": tokens[1].get("token_id","")},
    }

def scan_all(odds_by_sport: dict, poly_markets: list) -> list[ArbOpportunity]:
    found = []
    for sport_key, events in odds_by_sport.items():
        for event in events:
            home       = event.get("home_team", "")
            away       = event.get("away_team", "")
            event_name = f"{home} vs {away}"
            commence   = event.get("commence_time", "")[:16].replace("T", " ")
            best: dict[str, OddsLine] = {}

            for bm in event.get("bookmakers", []):
                bk, bn = bm.get("key",""), bm.get("title", bm.get("key",""))
                for mkt in bm.get("markets", []):
                    if mkt.get("key") != "h2h": continue
                    for out in mkt.get("outcomes", []):
                        name     = out.get("name","")
                        # กรอง Draw/Tie ออก — ใช้แค่ 2 ฝั่งหลัก
                        if name.lower() in ("draw", "tie", "no contest", "nc"):
                            continue
                        odds_raw = Decimal(str(out.get("price", 1)))
                        odds_eff = apply_slippage(odds_raw, bk)
                        if name not in best or odds_eff > best[name].odds:
                            best[name] = OddsLine(bookmaker=bn, outcome=name,
                                                  odds=odds_eff, odds_raw=odds_raw,
                                                  raw={"bm_key": bk, "event_id": event.get("id","")})

            poly = find_polymarket(event_name, poly_markets)
            if poly:
                for side, team in [("team_a", home), ("team_b", away)]:
                    p = poly[side]
                    matched = next((k for k in best if fuzzy_match(p["name"], k)), team)
                    if matched not in best or p["odds"] > best[matched].odds:
                        best[matched] = OddsLine(bookmaker="Polymarket", outcome=matched,
                                                 odds=p["odds"], odds_raw=p["odds_raw"],
                                                 market_url=poly["market_url"],
                                                 raw={"token_id": p["token_id"]})

            outcomes = list(best.keys())
            for i in range(len(outcomes)):
                for j in range(i+1, len(outcomes)):
                    a, b = outcomes[i], outcomes[j]
                    if best[a].bookmaker == best[b].bookmaker: continue
                    profit, s_a, s_b = calc_arb(best[a].odds, best[b].odds)
                    if profit >= MIN_PROFIT_PCT:
                        opp = ArbOpportunity(
                            signal_id=str(uuid.uuid4())[:8], sport=sport_key,
                            event=event_name, commence=commence,
                            leg1=best[a], leg2=best[b],
                            profit_pct=profit, stake1=s_a, stake2=s_b,
                        )
                        found.append(opp)
                        log.info(f"[ARB] {event_name} | profit={profit:.2%}")
    return found


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM ALERT
# ══════════════════════════════════════════════════════════════════
async def send_alert(opp: ArbOpportunity):
    pending[opp.signal_id] = opp
    # เก็บใน log
    opportunity_log.append({
        "id": opp.signal_id, "event": opp.event, "sport": opp.sport,
        "profit_pct": float(opp.profit_pct),
        "leg1_bm": opp.leg1.bookmaker, "leg1_odds": float(opp.leg1.odds),
        "leg2_bm": opp.leg2.bookmaker, "leg2_odds": float(opp.leg2.odds),
        "stake1_thb": int(opp.stake1 * USD_TO_THB),
        "stake2_thb": int(opp.stake2 * USD_TO_THB),
        "created_at": opp.created_at, "status": "pending",
    })
    if len(opportunity_log) > 100:
        opportunity_log.pop(0)

    emoji = SPORT_EMOJI.get(opp.sport, "🏆")
    s1 = (opp.stake1 * USD_TO_THB).quantize(Decimal("1"))
    s2 = (opp.stake2 * USD_TO_THB).quantize(Decimal("1"))
    w1 = (opp.stake1 * opp.leg1.odds * USD_TO_THB).quantize(Decimal("1"))
    w2 = (opp.stake2 * opp.leg2.odds * USD_TO_THB).quantize(Decimal("1"))
    tt = TOTAL_STAKE_THB.quantize(Decimal("1"))

    msg = (
        f"{emoji} *ARB FOUND — {opp.profit_pct:.2%}* _(หลัง fee)_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 {opp.commence} UTC\n"
        f"🏆 `{opp.event}`\n"
        f"💵 ทุน: *฿{int(tt):,}*  |  API credits: {api_remaining}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"{'ช่องทาง':<12} {'ฝั่ง':<15} {'Odds':>5} {'วาง':>8} {'ได้':>8}\n"
        f"{'─'*51}\n"
        f"{'🔵 '+opp.leg1.bookmaker:<12} {opp.leg1.outcome:<15} {float(opp.leg1.odds):>5.3f} {'฿'+str(int(s1)):>8} {'฿'+str(int(w1)):>8}\n"
        f"{'🟠 '+opp.leg2.bookmaker:<12} {opp.leg2.outcome:<15} {float(opp.leg2.odds):>5.3f} {'฿'+str(int(s2)):>8} {'฿'+str(int(w2)):>8}\n"
        f"{'─'*51}\n"
        f"{'รวม':<34} {'฿'+str(int(tt)):>8}\n"
        f"```\n"
        f"📊 *ไม่ว่าใครชนะ*\n"
        f"   {opp.leg1.outcome} ชนะ → ฿{int(w1):,} *(+฿{int(w1-tt):,})*\n"
        f"   {opp.leg2.outcome} ชนะ → ฿{int(w2):,} *(+฿{int(w2-tt):,})*\n"
        f"🔗 {opp.leg1.market_url or '—'}\n"
        f"🆔 `{opp.signal_id}`"
    )
    await _app.bot.send_message(
        chat_id=CHAT_ID, text=msg, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{opp.signal_id}"),
            InlineKeyboardButton("❌ Reject",  callback_data=f"reject:{opp.signal_id}"),
        ]])
    )


# ══════════════════════════════════════════════════════════════════
#  EXECUTE
# ══════════════════════════════════════════════════════════════════
async def execute_both(opp: ArbOpportunity) -> str:
    s1 = (opp.stake1 * USD_TO_THB).quantize(Decimal("1"))
    s2 = (opp.stake2 * USD_TO_THB).quantize(Decimal("1"))
    w1 = (opp.stake1 * opp.leg1.odds * USD_TO_THB).quantize(Decimal("1"))
    w2 = (opp.stake2 * opp.leg2.odds * USD_TO_THB).quantize(Decimal("1"))
    tt = TOTAL_STAKE_THB.quantize(Decimal("1"))

    def steps(leg, stake):
        bm  = leg.bookmaker.lower()
        eid = leg.raw.get("event_id", "")
        bk  = leg.raw.get("bm_key", bm)

        # สร้าง deep link แต่ละเว็บ
        if "polymarket" in bm:
            link = leg.market_url or "https://polymarket.com"
            return (f"  🔗 [เปิด Polymarket ตรงนี้เลย]({link})\n"
                    f"  2. เลือก *{leg.outcome}*\n"
                    f"  3. วาง ฿{int(stake)} USDC")

        elif "pinnacle" in bk:
            # Pinnacle deep link ไปหน้า sport
            sport_path = {
                "basketball_nba":         "basketball/nba",
                "baseball_mlb":           "baseball/mlb",
                "mma_mixed_martial_arts": "mixed-martial-arts",
                "esports_csgo":           "esports/cs2",
                "esports_dota2":          "esports/dota-2",
            }
            # ถ้ามี event_id ใช้ลิงค์ตรง ถ้าไม่มีใช้ sport
            if eid:
                link = f"https://www.pinnacle.com/en/mixed-martial-arts/matchup/{eid}"
            else:
                link = "https://www.pinnacle.com/en/mixed-martial-arts"
            return (f"  🔗 [เปิด Pinnacle ตรงนี้เลย]({link})\n"
                    f"  2. เลือก *{leg.outcome}* @ {leg.odds_raw}\n"
                    f"  3. วาง ฿{int(stake)}")

        elif "onexbet" in bk or "1xbet" in bm:
            # 1xBet deep link ด้วย event_id
            if eid:
                link = f"https://1xbet.com/en/line/mixed-martial-arts/{eid}"
            else:
                link = "https://1xbet.com/en/line/mixed-martial-arts"
            return (f"  🔗 [เปิด 1xBet ตรงนี้เลย]({link})\n"
                    f"  2. เลือก *{leg.outcome}* @ {leg.odds_raw}\n"
                    f"  3. วาง ฿{int(stake)}")

        elif "dafabet" in bk:
            link = "https://www.dafabet.com/en/sports/mma"
            return (f"  🔗 [เปิด Dafabet]({link})\n"
                    f"  2. ค้นหา *{leg.outcome}*\n"
                    f"  3. วาง ฿{int(stake)}")

        else:
            return (f"  1. เปิด {leg.bookmaker}\n"
                    f"  2. เลือก *{leg.outcome}* @ {leg.odds_raw}\n"
                    f"  3. วาง ฿{int(stake)}")

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
    try: action, sid = query.data.split(":", 1)
    except: return
    opp = pending.pop(sid, None)
    if not opp:
        await query.edit_message_text(query.message.text + "\n\n⚠️ หมดอายุแล้ว")
        return
    # อัพเดท log
    for entry in opportunity_log:
        if entry["id"] == sid:
            entry["status"] = action
    orig = query.message.text
    if action == "reject":
        await query.edit_message_text(orig + "\n\n❌ *REJECTED*", parse_mode="Markdown")
        return
    await query.edit_message_text(orig + "\n\n⏳ *กำลังเตรียม...*", parse_mode="Markdown")
    result = await execute_both(opp)
    await query.edit_message_text(orig + "\n\n✅ *CONFIRMED*\n\n" + result, parse_mode="Markdown")


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global auto_scan, quota_warned
    args = context.args
    if not args:
        s = "🟢 เปิด" if auto_scan else "🔴 ปิด"
        await update.message.reply_text(f"Auto scan: {s}\nใช้ /scan on หรือ /scan off")
        return
    if args[0].lower() == "on":
        auto_scan = True; quota_warned = False; seen_signals.clear()
        await update.message.reply_text(f"🟢 *Auto scan เปิด* — ทุก {SCAN_INTERVAL}s", parse_mode="Markdown")
    elif args[0].lower() == "off":
        auto_scan = False
        await update.message.reply_text("🔴 *Auto scan ปิด*", parse_mode="Markdown")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = "🟢 เปิด" if auto_scan else "🔴 ปิด"
    quota_bar = "█" * min(20, int(api_remaining / 25)) + "░" * max(0, 20 - int(api_remaining / 25))
    await update.message.reply_text(
        f"📊 *ARB BOT v5.0*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Auto scan   : {s}  ({SCAN_INTERVAL}s)\n"
        f"สแกนไปแล้ว  : {scan_count} รอบ\n"
        f"ล่าสุด      : {last_scan_time}\n"
        f"รอ confirm  : {len(pending)} รายการ\n"
        f"Min profit  : {MIN_PROFIT_PCT:.1%}\n"
        f"ทุน/trade   : ฿{int(TOTAL_STAKE_THB):,}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 API Credits\n"
        f"  เหลือ  : *{api_remaining}* / 500\n"
        f"  [{quota_bar}]\n"
        f"  แจ้งเตือนที่ : {QUOTA_WARN_AT}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"/scan on · /scan off · /now · /status",
        parse_mode="Markdown",
    )


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 *สแกน...*", parse_mode="Markdown")
    count = await do_scan()
    msg = f"✅ พบ *{count}* opportunity" if count else f"✅ ไม่พบ opportunity > {MIN_PROFIT_PCT:.1%}"
    await update.message.reply_text(msg, parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════════
#  SCAN CORE
# ══════════════════════════════════════════════════════════════════
async def do_scan() -> int:
    global scan_count, last_scan_time
    odds_by_sport, poly_markets = await fetch_all_async(SPORTS)
    log.info(f"[Scanner] Polymarket={len(poly_markets)}")
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
    return sent


async def scanner_loop():
    await asyncio.sleep(3)
    log.info(f"[Scanner] v5.0 started | interval={SCAN_INTERVAL}s")
    while True:
        if auto_scan:
            try: await do_scan()
            except Exception as e: log.error(f"[Scanner] {e}")
        await asyncio.sleep(SCAN_INTERVAL)


# ══════════════════════════════════════════════════════════════════
#  🆕 WEB DASHBOARD  (built-in HTTP server ไม่ต้องลง Flask)
# ══════════════════════════════════════════════════════════════════
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="30">
<title>ARB BOT v5.0</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:#0d1117; color:#e6edf3; font-family:'Segoe UI',sans-serif; padding:20px; }
  h1 { color:#58a6ff; font-size:1.5rem; margin-bottom:4px; }
  .sub { color:#8b949e; font-size:.85rem; margin-bottom:20px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin-bottom:24px; }
  .card { background:#161b22; border:1px solid #30363d; border-radius:10px; padding:16px; }
  .card .label { color:#8b949e; font-size:.75rem; text-transform:uppercase; letter-spacing:.5px; }
  .card .value { font-size:1.6rem; font-weight:700; margin-top:4px; }
  .green { color:#3fb950; } .red { color:#f85149; } .yellow { color:#d29922; } .blue { color:#58a6ff; }
  .quota-bar { background:#21262d; border-radius:4px; height:8px; margin-top:8px; overflow:hidden; }
  .quota-fill { height:100%; border-radius:4px; transition:width .3s; }
  table { width:100%; border-collapse:collapse; background:#161b22; border-radius:10px; overflow:hidden; }
  th { background:#21262d; color:#8b949e; font-size:.75rem; text-transform:uppercase; padding:10px 14px; text-align:left; }
  td { padding:10px 14px; border-top:1px solid #21262d; font-size:.85rem; }
  tr:hover td { background:#1c2128; }
  .badge { display:inline-block; padding:2px 8px; border-radius:12px; font-size:.7rem; font-weight:600; }
  .badge-pending  { background:#1f3d5c; color:#58a6ff; }
  .badge-confirm  { background:#1a3a2a; color:#3fb950; }
  .badge-rejected { background:#3d1f1f; color:#f85149; }
  .profit { color:#3fb950; font-weight:700; }
  .section-title { color:#8b949e; font-size:.8rem; text-transform:uppercase; letter-spacing:.5px; margin:20px 0 10px; }
</style>
</head>
<body>
<h1>🤖 ARB BOT v5.0</h1>
<div class="sub">Real-time arbitrage scanner — รีเฟรชทุก 30 วินาที</div>

<div class="grid" id="stats"></div>
<div class="quota-bar"><div class="quota-fill" id="quotaFill"></div></div>
<div style="color:#8b949e;font-size:.75rem;margin-top:4px;" id="quotaText"></div>

<div class="section-title">📋 Opportunity Log (ล่าสุด 20 รายการ)</div>
<table>
  <thead><tr>
    <th>Event</th><th>Sport</th><th>Leg 1</th><th>Leg 2</th>
    <th>Profit</th><th>ทุน</th><th>เวลา</th><th>Status</th>
  </tr></thead>
  <tbody id="tbody"></tbody>
</table>

<script>
async function load() {
  const r = await fetch('/api/state');
  const d = await r.json();

  // Stats cards
  const scanColor = d.auto_scan ? 'green' : 'red';
  const qPct = Math.round((d.api_remaining / 500) * 100);
  const qColor = qPct > 30 ? 'green' : qPct > 10 ? 'yellow' : 'red';
  document.getElementById('stats').innerHTML = `
    <div class="card"><div class="label">Auto Scan</div>
      <div class="value ${scanColor}">${d.auto_scan ? '🟢 ON' : '🔴 OFF'}</div></div>
    <div class="card"><div class="label">สแกนไปแล้ว</div>
      <div class="value blue">${d.scan_count}</div></div>
    <div class="card"><div class="label">รอ Confirm</div>
      <div class="value yellow">${d.pending_count}</div></div>
    <div class="card"><div class="label">API Credits</div>
      <div class="value ${qColor}">${d.api_remaining}</div></div>
    <div class="card"><div class="label">ทุน/trade</div>
      <div class="value">฿${d.total_stake_thb.toLocaleString()}</div></div>
    <div class="card"><div class="label">Min Profit</div>
      <div class="value green">${(d.min_profit_pct * 100).toFixed(1)}%</div></div>
  `;
  document.getElementById('quotaFill').style.width = qPct + '%';
  document.getElementById('quotaFill').style.background = qPct > 30 ? '#3fb950' : qPct > 10 ? '#d29922' : '#f85149';
  document.getElementById('quotaText').textContent = `API Credits: ${d.api_remaining}/500 (${qPct}%) — แจ้งเตือนที่ ${d.quota_warn_at} | สแกนล่าสุด: ${d.last_scan_time}`;

  // Table
  const rows = (d.opportunities || []).slice(-20).reverse().map(o => {
    const badge = o.status === 'pending' ? 'badge-pending' : o.status === 'confirm' ? 'badge-confirm' : 'badge-rejected';
    const label = o.status === 'pending' ? 'รอ' : o.status === 'confirm' ? 'ยืนยัน' : 'ปฏิเสธ';
    const t = new Date(o.created_at).toLocaleTimeString('th-TH', {hour:'2-digit',minute:'2-digit'});
    return `<tr>
      <td>${o.event}</td>
      <td>${o.sport.split('_').pop().toUpperCase()}</td>
      <td>${o.leg1_bm} @${o.leg1_odds.toFixed(2)}</td>
      <td>${o.leg2_bm} @${o.leg2_odds.toFixed(2)}</td>
      <td class="profit">+${(o.profit_pct*100).toFixed(2)}%</td>
      <td>฿${o.stake1_thb.toLocaleString()} / ฿${o.stake2_thb.toLocaleString()}</td>
      <td>${t}</td>
      <td><span class="badge ${badge}">${label}</span></td>
    </tr>`;
  }).join('');
  document.getElementById('tbody').innerHTML = rows || '<tr><td colspan="8" style="text-align:center;color:#8b949e;padding:30px">ยังไม่พบ opportunity</td></tr>';
}
load();
</script>
</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass  # ปิด access log

    def do_GET(self):
        if self.path == "/api/state":
            data = {
                "auto_scan":       auto_scan,
                "scan_count":      scan_count,
                "last_scan_time":  last_scan_time,
                "pending_count":   len(pending),
                "api_remaining":   api_remaining,
                "api_used":        api_used_session,
                "quota_warn_at":   QUOTA_WARN_AT,
                "total_stake_thb": int(TOTAL_STAKE_THB),
                "min_profit_pct":  float(MIN_PROFIT_PCT),
                "scan_interval":   SCAN_INTERVAL,
                "sports":          SPORTS,
                "opportunities":   opportunity_log[-50:],
            }
            body = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)
        else:
            body = DASHBOARD_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(body))
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
    err = str(context.error)
    if "Conflict" in err:
        log.warning("[Bot] Conflict — รอ instance เก่าหายไป")
        return
    log.error(f"[Bot] {context.error}")


async def post_init(app: Application):
    app.add_error_handler(error_handler)
    # เริ่ม dashboard thread
    t = threading.Thread(target=start_dashboard, daemon=True)
    t.start()
    await app.bot.send_message(
        chat_id=CHAT_ID, parse_mode="Markdown",
        text=(
            "🤖 *ARB BOT v5.0 — Started!*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"✨ Dashboard + Quota Alert + Fuzzy + Async\n"
            f"Sports    : {' '.join([SPORT_EMOJI.get(s,'🏆') for s in SPORTS])}\n"
            f"Min profit: {MIN_PROFIT_PCT:.1%}  |  ทุน: ฿{int(TOTAL_STAKE_THB):,}\n"
            f"Auto scan : {'🟢 เปิด' if auto_scan else '🔴 ปิด'} (ทุก {SCAN_INTERVAL}s)\n"
            f"Dashboard : port {PORT}\n"
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
