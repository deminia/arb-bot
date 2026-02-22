"""
╔══════════════════════════════════════════════════════════════════╗
║            ARB BOT v3.0  —  Polymarket vs Betting Sites         ║
║  Sports : NBA 🏀 | Tennis 🎾 | MLB ⚾ | UFC 🥊 | CS2/Dota2 🎮  ║
║  Mode   : Semi-auto  —  Telegram ✅ Confirm / ❌ Reject          ║
║  Config : แก้ใน .env ไฟล์เดียว                                  ║
╚══════════════════════════════════════════════════════════════════╝

วิธีรัน:
  pip install -r requirements.txt
  python arb_bot.py

คำสั่ง Telegram:
  /scan on   — เปิด auto scan
  /scan off  — ปิด auto scan
  /now       — สแกนทันที 1 รอบ (manual)
  /status    — ดูสถานะทั้งหมด
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass, field
from typing import Optional

import requests
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

# โหลด .env
load_dotenv()

# ══════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG  —  อ่านจาก .env  (แก้ใน .env ไฟล์เดียวพอ)
# ══════════════════════════════════════════════════════════════════
def _d(key: str, default: str) -> Decimal:
    return Decimal(os.getenv(key, default))

def _s(key: str, default: str) -> str:
    return os.getenv(key, default)

def _i(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))

# API Keys
ODDS_API_KEY   = _s("ODDS_API_KEY",   "0205bd80de0af87de624e134b8c38db3")
TELEGRAM_TOKEN = _s("TELEGRAM_TOKEN", "8517689298:AAEgHOYN-zAOwsJ4LMYGQkLeZPTComJP4A8")
CHAT_ID        = _s("CHAT_ID",        "6415456688")

# เงิน
TOTAL_STAKE_THB = _d("TOTAL_STAKE_THB", "10000")   # ทุนรวมต่อ 1 arb (บาท)
USD_TO_THB      = _d("USD_TO_THB",      "35")       # อัตราแลก
TOTAL_STAKE     = TOTAL_STAKE_THB / USD_TO_THB      # คำนวณอัตโนมัติ

# Scan
MIN_PROFIT_PCT = _d("MIN_PROFIT_PCT", "0.015")      # กำไรขั้นต่ำ (0.015 = 1.5%)
SCAN_INTERVAL  = _i("SCAN_INTERVAL",  300)          # วินาที ต่อรอบ
AUTO_SCAN_START = _s("AUTO_SCAN_START", "true").lower() == "true"

# Bookmakers (key ของ Odds API)
BOOKMAKERS = _s("BOOKMAKERS", "pinnacle,onexbet,dafabet")

# Sports — แก้ใน .env เป็น comma-separated
# ตัวอย่าง: SPORTS=basketball_nba,baseball_mlb,esports_csgo
_SPORTS_DEFAULT = "basketball_nba,baseball_mlb,mma_mixed_martial_arts,esports_csgo,esports_dota2"
SPORTS = [s.strip() for s in _s("SPORTS", _SPORTS_DEFAULT).split(",") if s.strip()]

# Sport emoji map
SPORT_EMOJI = {
    "basketball_nba":        "🏀",
    "basketball_euroleague": "🏀",
    "tennis_atp_wimbledon":  "🎾",
    "tennis_wta":            "🎾",
    "baseball_mlb":          "⚾",
    "mma_mixed_martial_arts":"🥊",
    "esports_csgo":          "🎮",
    "esports_dota2":         "🎮",
    "esports_lol":           "🎮",
}


# ══════════════════════════════════════════════════════════════════
#  DATA MODELS
# ══════════════════════════════════════════════════════════════════
@dataclass
class OddsLine:
    bookmaker:  str
    outcome:    str
    odds:       Decimal
    market_url: str  = ""
    raw:        dict = field(default_factory=dict)

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
#  FETCH ODDS  —  The Odds API
# ══════════════════════════════════════════════════════════════════
def fetch_odds(sport_key: str) -> list[dict]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey":     ODDS_API_KEY,
        "regions":    "eu,uk,au",
        "markets":    "h2h",
        "oddsFormat": "decimal",
        "bookmakers": BOOKMAKERS,
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        remaining = r.headers.get("x-requests-remaining", "?")
        data = r.json()
        if isinstance(data, list):
            log.info(f"[OddsAPI] {sport_key} | events={len(data)} | remaining={remaining}")
            return data
        log.warning(f"[OddsAPI] Unexpected response: {data}")
        return []
    except Exception as e:
        log.error(f"[OddsAPI] {sport_key} error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════
#  FETCH POLYMARKET ODDS
# ══════════════════════════════════════════════════════════════════
def fetch_polymarket_odds(event_name: str) -> Optional[dict]:
    try:
        r = requests.get(
            "https://clob.polymarket.com/markets",
            params={"active": True, "closed": False},
            timeout=15,
        )
        markets = r.json().get("data", [])
        parts = [p.strip().lower() for p in event_name.lower().replace(" vs ", "|").split("|")]
        if len(parts) < 2:
            return None
        team_a, team_b = parts[0], parts[1]

        for m in markets:
            title = m.get("question", "").lower()
            # match ด้วย 5 ตัวอักษรแรกของชื่อทีม
            if team_a[:5] in title and team_b[:5] in title:
                tokens = m.get("tokens", [])
                if len(tokens) < 2:
                    continue
                prob_a = Decimal(str(tokens[0].get("price", 0)))
                prob_b = Decimal(str(tokens[1].get("price", 0)))
                if prob_a <= 0 or prob_b <= 0:
                    continue
                return {
                    "market_url": f"https://polymarket.com/event/{m.get('slug','')}",
                    "team_a": {
                        "name":     tokens[0].get("outcome", team_a),
                        "odds":     (Decimal("1") / prob_a).quantize(Decimal("0.001")),
                        "token_id": tokens[0].get("token_id", ""),
                    },
                    "team_b": {
                        "name":     tokens[1].get("outcome", team_b),
                        "odds":     (Decimal("1") / prob_b).quantize(Decimal("0.001")),
                        "token_id": tokens[1].get("token_id", ""),
                    },
                }
    except Exception as e:
        log.debug(f"[Polymarket] {e}")
    return None


# ══════════════════════════════════════════════════════════════════
#  CALCULATE ARB
# ══════════════════════════════════════════════════════════════════
def calc_arb(odds_a: Decimal, odds_b: Decimal) -> tuple[Decimal, Decimal, Decimal]:
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
#  SCAN ONE SPORT
# ══════════════════════════════════════════════════════════════════
def scan_sport(sport_key: str) -> list[ArbOpportunity]:
    events = fetch_odds(sport_key)
    found  = []

    for event in events:
        home       = event.get("home_team", "")
        away       = event.get("away_team", "")
        event_name = f"{home} vs {away}"
        commence   = event.get("commence_time", "")[:16].replace("T", " ")

        # ── รวบรวม best odds จาก betting sites ──
        best: dict[str, OddsLine] = {}
        for bm in event.get("bookmakers", []):
            bm_key  = bm.get("key", "")
            bm_name = bm.get("title", bm_key)
            for mkt in bm.get("markets", []):
                if mkt.get("key") != "h2h":
                    continue
                for out in mkt.get("outcomes", []):
                    name  = out.get("name", "")
                    price = Decimal(str(out.get("price", 1)))
                    if name not in best or price > best[name].odds:
                        best[name] = OddsLine(
                            bookmaker = bm_name,
                            outcome   = name,
                            odds      = price,
                            raw       = {"bm_key": bm_key, "event_id": event.get("id", "")},
                        )

        # ── เพิ่ม Polymarket ──
        poly = fetch_polymarket_odds(event_name)
        if poly:
            for side, team_name in [("team_a", home), ("team_b", away)]:
                p_odds = poly[side]["odds"]
                tok    = poly[side]["token_id"]
                murl   = poly["market_url"]
                out_name = team_name
                if out_name not in best or p_odds > best[out_name].odds:
                    best[out_name] = OddsLine(
                        bookmaker  = "Polymarket",
                        outcome    = out_name,
                        odds       = p_odds,
                        market_url = murl,
                        raw        = {"token_id": tok},
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
                        f"{best[oc_a].bookmaker}({oc_a}@{best[oc_a].odds}) vs "
                        f"{best[oc_b].bookmaker}({oc_b}@{best[oc_b].odds}) | "
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

    msg = (
        f"{emoji} *ARB FOUND — {opp.profit_pct:.2%} profit*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 {opp.commence} UTC\n"
        f"🏆 `{opp.event}`\n"
        f"💵 ทุนรวม: *฿{int(total_thb):,}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"{'ช่องทาง':<12} {'ฝั่ง':<16} {'Odds':>5} {'วาง':>8} {'ได้คืน':>9}\n"
        f"{'─'*53}\n"
        f"{'🔵 '+opp.leg1.bookmaker:<12} {opp.leg1.outcome:<16} "
        f"{float(opp.leg1.odds):>5.2f} "
        f"{'฿'+str(int(stake1_thb)):>8} {'฿'+str(int(win1_thb)):>9}\n"
        f"{'🟠 '+opp.leg2.bookmaker:<12} {opp.leg2.outcome:<16} "
        f"{float(opp.leg2.odds):>5.2f} "
        f"{'฿'+str(int(stake2_thb)):>8} {'฿'+str(int(win2_thb)):>9}\n"
        f"{'─'*53}\n"
        f"{'รวม':<35} {'฿'+str(int(total_thb)):>8}\n"
        f"```\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *ไม่ว่าใครชนะ*\n"
        f"   {opp.leg1.outcome} ชนะ → ฿{int(win1_thb):,} *(+฿{int(profit1):,})*\n"
        f"   {opp.leg2.outcome} ชนะ → ฿{int(win2_thb):,} *(+฿{int(profit2):,})*\n"
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
    log.info(f"[Telegram] Alert sent: {opp.signal_id} {opp.event}")


# ══════════════════════════════════════════════════════════════════
#  EXECUTE LEGS  (Manual steps หลังกด Confirm)
# ══════════════════════════════════════════════════════════════════
def manual_steps(leg: OddsLine, stake_thb: Decimal) -> str:
    bm = leg.bookmaker.lower()
    if bm == "polymarket":
        return (
            f"  1. เปิด: {leg.market_url}\n"
            f"  2. เลือก: *{leg.outcome}*\n"
            f"  3. วาง: ฿{int(stake_thb)} USDC\n"
            f"  4. Token: `{leg.raw.get('token_id','—')}`"
        )
    elif "pinnacle" in bm:
        return (
            f"  1. เปิด Pinnacle → ค้นหา event\n"
            f"  2. เลือก: *{leg.outcome}* @ {leg.odds}\n"
            f"  3. วาง: ฿{int(stake_thb)}\n"
            f"  4. Event ID: `{leg.raw.get('event_id','—')}`"
        )
    else:
        return (
            f"  1. เปิด {leg.bookmaker} → ค้นหา event\n"
            f"  2. เลือก: *{leg.outcome}* @ {leg.odds}\n"
            f"  3. วาง: ฿{int(stake_thb)}"
        )

async def execute_both(opp: ArbOpportunity) -> str:
    stake1_thb = (opp.stake1 * USD_TO_THB).quantize(Decimal("1"))
    stake2_thb = (opp.stake2 * USD_TO_THB).quantize(Decimal("1"))
    win1_thb   = (opp.stake1 * opp.leg1.odds * USD_TO_THB).quantize(Decimal("1"))
    win2_thb   = (opp.stake2 * opp.leg2.odds * USD_TO_THB).quantize(Decimal("1"))
    total_thb  = TOTAL_STAKE_THB.quantize(Decimal("1"))

    return (
        f"📋 *วิธีวางเงิน*\n"
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
#  TELEGRAM — BUTTON CALLBACK
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
        await query.edit_message_text(
            query.message.text + "\n\n⚠️ Signal หมดอายุหรือถูก execute แล้ว"
        )
        return

    original = query.message.text

    if action == "reject":
        await query.edit_message_text(original + "\n\n❌ *REJECTED*", parse_mode="Markdown")
        log.info(f"[Bot] Rejected: {signal_id}")
        return

    await query.edit_message_text(original + "\n\n⏳ *กำลังเตรียม...*", parse_mode="Markdown")
    result = await execute_both(opp)
    await query.edit_message_text(
        original + "\n\n✅ *CONFIRMED*\n\n" + result,
        parse_mode="Markdown",
    )
    log.info(f"[Bot] Confirmed: {signal_id}")


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM — COMMANDS
# ══════════════════════════════════════════════════════════════════
async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global auto_scan
    args = context.args
    if not args:
        state = "🟢 เปิดอยู่" if auto_scan else "🔴 ปิดอยู่"
        await update.message.reply_text(f"Auto scan: {state}\nใช้ /scan on หรือ /scan off")
        return
    cmd = args[0].lower()
    if cmd == "on":
        auto_scan = True
        seen_signals.clear()
        await update.message.reply_text(
            f"🟢 *Auto scan เปิดแล้ว*\nสแกนทุก {SCAN_INTERVAL}s",
            parse_mode="Markdown",
        )
    elif cmd == "off":
        auto_scan = False
        await update.message.reply_text(
            "🔴 *Auto scan ปิดแล้ว*\nใช้ /now เพื่อสแกน manual",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text("❓ ใช้ /scan on หรือ /scan off")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = "🟢 เปิด" if auto_scan else "🔴 ปิด"
    await update.message.reply_text(
        f"📊 *Bot Status*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Auto scan   : {state}\n"
        f"Interval    : {SCAN_INTERVAL}s\n"
        f"สแกนไปแล้ว  : {scan_count} รอบ\n"
        f"สแกนล่าสุด  : {last_scan_time}\n"
        f"รอ confirm  : {len(pending)} รายการ\n"
        f"Min profit  : {MIN_PROFIT_PCT:.1%}\n"
        f"ทุน/trade   : ฿{int(TOTAL_STAKE_THB):,}\n"
        f"อัตราแลก    : 1 USD = ฿{USD_TO_THB}\n"
        f"Sports      : {len(SPORTS)} รายการ\n"
        f"Bookmakers  : {BOOKMAKERS}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"/scan on · /scan off · /now · /status",
        parse_mode="Markdown",
    )


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 *กำลังสแกน...*", parse_mode="Markdown")
    count = await do_scan()
    if count == 0:
        await update.message.reply_text(
            f"✅ สแกนเสร็จ — ไม่พบ opportunity ที่กำไร > {MIN_PROFIT_PCT:.1%}",
        )
    else:
        await update.message.reply_text(f"✅ พบ *{count}* opportunity ดูด้านบนครับ", parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════════
#  SCAN CORE
# ══════════════════════════════════════════════════════════════════
async def do_scan() -> int:
    global scan_count, last_scan_time

    all_opps: list[ArbOpportunity] = []
    for sport in SPORTS:
        opps = scan_sport(sport)
        all_opps.extend(opps)
        await asyncio.sleep(0.5)

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

    scan_count     += 1
    last_scan_time  = datetime.now(timezone.utc).strftime("%d/%m %H:%M UTC")
    log.info(f"[Scanner] #{scan_count} done | sent={sent} | auto={auto_scan}")
    return sent


async def scanner_loop():
    await asyncio.sleep(3)
    log.info(f"[Scanner] Started | interval={SCAN_INTERVAL}s | sports={len(SPORTS)}")
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
async def main():
    global _app

    _app = Application.builder().token(TELEGRAM_TOKEN).build()
    _app.add_handler(CallbackQueryHandler(button_handler))
    _app.add_handler(CommandHandler("scan",   cmd_scan))
    _app.add_handler(CommandHandler("status", cmd_status))
    _app.add_handler(CommandHandler("now",    cmd_now))

    await _app.initialize()
    await _app.start()
    await _app.bot.send_message(
        chat_id    = CHAT_ID,
        parse_mode = "Markdown",
        text       = (
            "🤖 *ARB BOT v3.0 — Started!*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Sports     : {' | '.join([SPORT_EMOJI.get(s,'🏆')+' '+s.split('_')[-1].upper() for s in SPORTS])}\n"
            f"Min profit : {MIN_PROFIT_PCT:.1%}\n"
            f"ทุน/trade  : ฿{int(TOTAL_STAKE_THB):,}\n"
            f"Auto scan  : {'🟢 เปิด' if auto_scan else '🔴 ปิด'} (ทุก {SCAN_INTERVAL}s)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📌 *คำสั่ง*\n"
            f"/scan on  — เปิด auto scan\n"
            f"/scan off — ปิด auto scan\n"
            f"/now      — สแกนทันที 1 รอบ\n"
            f"/status   — ดูสถานะทั้งหมด"
        ),
    )

    asyncio.create_task(scanner_loop())
    await _app.updater.start_polling()
    await _app.updater.idle()


if __name__ == "__main__":
    asyncio.run(main())
