#!/usr/bin/env python3
# edge_context_patch.py
# Adds Edge Context (news + LLM) section to arb_bot.py Telegram alerts.
# Usage: python edge_context_patch.py  (run from same dir as arb_bot.py)
import sys, shutil, ast
from pathlib import Path

BOT_FILE = Path("arb_bot.py")
if not BOT_FILE.exists():
    print("ERROR: arb_bot.py not found"); sys.exit(1)

backup = BOT_FILE.with_suffix(".py.bak")
shutil.copy2(BOT_FILE, backup)
print(f"[+] Backup -> {backup}")
src = BOT_FILE.read_text(encoding="utf-8")
changed = False

EC_BLOCK       = '\n\n# ─────────────────────────────────────────────────────────────────────\n# Edge Context Module  (auto-patched)\n# ─────────────────────────────────────────────────────────────────────\nEDGE_CONTEXT_ENABLED = os.getenv("EDGE_CONTEXT_ENABLED", "true").lower() == "true"\nEDGE_CONTEXT_TIMEOUT = int(os.getenv("EDGE_CONTEXT_TIMEOUT", "25"))\n_ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")\n_EDGE_CLAUDE_MODEL   = "claude-sonnet-4-20250514"\n_edge_cache: dict = {}  # event_key -> (ctx_text, ts)\n_EDGE_CACHE_TTL = 1800  # 30 min\n\n\nclass EdgeContextModule:\n    def __init__(self):\n        if not _ANTHROPIC_API_KEY:\n            log.warning("[EdgeCtx] ANTHROPIC_API_KEY not set")\n        try:\n            import anthropic as _ant\n            self._client = _ant.Anthropic(api_key=_ANTHROPIC_API_KEY) if _ANTHROPIC_API_KEY else None\n        except ImportError:\n            log.error("[EdgeCtx] anthropic package not installed — pip install anthropic")\n            self._client = None\n\n    def _search_news(self, event: str, sport: str) -> str:\n        if not self._client: return "N/A (no API key or package)"\n        try:\n            resp = self._client.messages.create(\n                model=_EDGE_CLAUDE_MODEL, max_tokens=800,\n                tools=[{"type": "web_search_20250305", "name": "web_search"}],\n                messages=[{"role": "user", "content": (\n                    f"Search latest pre-match news for: {event} ({sport}). "\n                    "Focus on injuries, suspensions, lineup, recent form (last 3 games), "\n                    "motivation, fatigue. Return 3-4 bullet points. "\n                    "If nothing significant found reply: No significant news found."\n                )}]\n            )\n            parts = [b.text for b in resp.content if hasattr(b, "text")]\n            return ("\\n".join(parts)).strip() or "No news retrieved."\n        except Exception as e:\n            log.warning(f"[EdgeCtx] news search: {e}")\n            return f"News error: {e}"\n\n    def _interpret(self, event, sport, profit_pct,\n                   l1_bm, l1_out, l1_odds, l2_bm, l2_out, l2_odds, l3_bm, news) -> str:\n        if not self._client: return "N/A"\n        legs = f"  Leg1: {l1_bm} -> {l1_out} @ {l1_odds:.3f}\\n  Leg2: {l2_bm} -> {l2_out} @ {l2_odds:.3f}"\n        if l3_bm: legs += f"\\n  Leg3: {l3_bm} -> Draw"\n        prompt = (\n            f"You are a sharp betting analyst reviewing an arb signal.\\n\\n"\n            f"ARB SIGNAL:\\n- Event: {event}\\n- Sport: {sport}\\n- Profit: {profit_pct:.2%}\\n{legs}\\n\\n"\n            f"LATEST NEWS:\\n{news}\\n\\n"\n            "Answer concisely (max 4 lines):\\n"\n            f"SOFT_SIDE: which bookmaker ({l1_bm} or {l2_bm}) is mispriced and why?\\n"\n            "NEWS_EDGE: does news explain the mispricing? yes/no/partial + reason\\n"\n            "VERDICT: STRONG / MODERATE / WEAK / ARB_ONLY\\n\\n"\n            "No hallucination. If no clear reason, say so."\n        )\n        try:\n            resp = self._client.messages.create(\n                model=_EDGE_CLAUDE_MODEL, max_tokens=250,\n                messages=[{"role": "user", "content": prompt}]\n            )\n            return resp.content[0].text.strip()\n        except Exception as e:\n            log.warning(f"[EdgeCtx] interpret: {e}")\n            return f"LLM error: {e}"\n\n    def build_context(self, opp) -> str:\n        global _edge_cache\n        now_ts = time.time()\n        if opp.event in _edge_cache:\n            txt, ts = _edge_cache[opp.event]\n            if (now_ts - ts) < _EDGE_CACHE_TTL:\n                return txt\n        news   = self._search_news(opp.event, opp.sport)\n        interp = self._interpret(\n            opp.event, opp.sport, float(opp.profit_pct),\n            opp.leg1.bookmaker, str(opp.leg1.outcome), float(opp.leg1.odds),\n            opp.leg2.bookmaker, str(opp.leg2.outcome), float(opp.leg2.odds),\n            opp.leg3.bookmaker if opp.leg3 else None, news\n        )\n        sep    = "\\u2501" * 26\n        result = f"\\n{sep}\\n\\u26a1 *Edge Context*\\n\\U0001f4f0 {news}\\n\\n\\U0001f916 {interp}"\n        _edge_cache[opp.event] = (result, now_ts)\n        _edge_cache = {k: v for k, v in _edge_cache.items() if (now_ts - v[1]) < _EDGE_CACHE_TTL}\n        return result\n\n\n_edge_ctx = EdgeContextModule()\n\n'
GET_EDGE_BLOCK = '\n\nasync def get_edge_context(opp) -> str:\n    # Async wrapper: run EdgeContextModule in thread pool (anthropic is sync)\n    if not EDGE_CONTEXT_ENABLED or not _ANTHROPIC_API_KEY:\n        return ""\n    try:\n        loop = asyncio.get_running_loop()\n        return await asyncio.wait_for(\n            loop.run_in_executor(None, _edge_ctx.build_context, opp),\n            timeout=EDGE_CONTEXT_TIMEOUT\n        )\n    except asyncio.TimeoutError:\n        log.warning(f"[EdgeCtx] timeout for {opp.event}")\n        sep = "\\u2501" * 26\n        return f"\\n{sep}\\n\\u26a1 *Edge Context* \\u2014 \\u23f1 timeout"\n    except Exception as e:\n        log.warning(f"[EdgeCtx] {e}")\n        return ""\n\n\n'

# 0. import anthropic
if "import anthropic" not in src:
    src = src.replace("import aiohttp", "import aiohttp\nimport anthropic", 1)
    print("[+] Added: import anthropic"); changed = True
else:
    print("[-] Skip: import anthropic exists")

# 1. EdgeContextModule block
if "class EdgeContextModule" not in src:
    anchor = "def _i(k,v): return int(os.getenv(k,str(v)))"
    if anchor in src:
        src = src.replace(anchor, anchor + EC_BLOCK, 1)
        print("[+] Added: EdgeContextModule"); changed = True
    else:
        print("[!] WARN: anchor for EdgeContextModule not found")
else:
    print("[-] Skip: EdgeContextModule exists")

# 2. get_edge_context() async wrapper
if "async def get_edge_context" not in src:
    anchor2 = "async def send_alert(opp: ArbOpportunity):"
    if anchor2 in src:
        src = src.replace(anchor2, GET_EDGE_BLOCK + anchor2, 1)
        print("[+] Added: get_edge_context()"); changed = True
    else:
        print("[!] WARN: anchor for get_edge_context not found")
else:
    print("[-] Skip: get_edge_context exists")

# 3a. await edge_ctx in send_alert (unique anchor: confirm:{opp.signal_id})
UNIQUE_KB   = '    keyboard = InlineKeyboardMarkup([[\n        InlineKeyboardButton("\u2705 Confirm", callback_data=f"confirm:{opp.signal_id}"),'
KB_WITH_EDGE = "    # \u26a1 Edge Context\n    edge_ctx = await get_edge_context(opp)\n\n" + UNIQUE_KB
if "edge_ctx = await get_edge_context" not in src:
    if UNIQUE_KB in src:
        src = src.replace(UNIQUE_KB, KB_WITH_EDGE, 1)
        print("[+] Added: edge_ctx call in send_alert"); changed = True
    else:
        print("[!] WARN: unique keyboard anchor not found")
else:
    print("[-] Skip: edge_ctx call exists")

# 3b. {edge_ctx} before signal_id in msg
MSG_ANCHOR = '        f"\U0001f194 `{opp.signal_id}`"'
MSG_NEW    = '        f"{edge_ctx}\\n"\n        f"\U0001f194 `{opp.signal_id}`"'
if "{edge_ctx}" not in src:
    if MSG_ANCHOR in src:
        src = src.replace(MSG_ANCHOR, MSG_NEW, 1)
        print("[+] Added: {edge_ctx} in msg"); changed = True
    else:
        print("[!] WARN: signal_id anchor not found")
else:
    print("[-] Skip: {edge_ctx} exists")

# syntax check
try:
    ast.parse(src)
    print("[\u2713] Syntax OK")
except SyntaxError as e:
    print(f"[\u2717] Syntax ERROR: {e} — restoring backup")
    shutil.copy2(backup, BOT_FILE)
    sys.exit(1)

if changed:
    BOT_FILE.write_text(src, encoding="utf-8")
    print("\n[\u2713] Patch applied!")
else:
    print("\n[\u2713] Already up to date")

print("""
Add these to Railway Variables (or .env):
  ANTHROPIC_API_KEY    = sk-ant-api03-...
  EDGE_CONTEXT_ENABLED = true
  EDGE_CONTEXT_TIMEOUT = 25
""")
