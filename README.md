# Edge Context Module

An add-on for `arb_bot.py` — appends news search and LLM interpretation to the arb alerts the bot already sends, without touching the existing arb detection logic.

---

## Concept

When the arb scanner finds a signal as usual, before sending the Telegram alert the bot now searches for the latest news on that match (injuries, lineup, form, motivation) using Claude's web search, then sends all of that to an LLM to interpret which side looks "soft" (mispriced) and whether the news explains the price gap. The result is appended to the existing alert as a new `⚡ Edge Context` section.

Everything runs automatically during the normal auto-scan loop. No extra commands needed.

---

## Flow

```
auto scan (existing)
    ↓
arb signal found (existing)
    ↓
send_alert(opp)
    ↓
  edge_ctx = await get_edge_context(opp)   ← new
    │
    ├─ EdgeContextModule._search_news()    → news search via Claude web_search
    └─ EdgeContextModule._interpret()      → LLM analyzes gap + news
    ↓
existing Telegram alert + ⚡ Edge Context section
```

---

## Installation

### 1. Install dependency

```bash
pip install anthropic
```

### 2. Run the patch

Place `edge_context_patch.py` in the same directory as `arb_bot.py`, then run:

```bash
python edge_context_patch.py
```

The patch will:
- Back up `arb_bot.py` to `arb_bot.py.bak` before making changes
- Add `import anthropic`
- Add the `EdgeContextModule` class
- Add the `get_edge_context()` function
- Modify `send_alert()` to call edge context and append it to the message
- Validate syntax with `ast.parse` — if anything breaks, it restores the backup automatically

The patch is idempotent — safe to run multiple times; already-patched sections are skipped.

### 3. Add environment variables

In Railway Variables or `.env`:

```bash
ANTHROPIC_API_KEY=sk-ant-api03-...     # from console.anthropic.com
EDGE_CONTEXT_ENABLED=true              # true/false — toggle without redeploying code
EDGE_CONTEXT_TIMEOUT=25                # seconds — skip if Claude takes longer than this
```

### 4. Run the bot as usual

```bash
python arb_bot.py
```

---

## Example Alert Output

```
🟡 *ARB FOUND — 2.40%* _(after fees)_
━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 16/06/2026 21:00 (Thai time) (⏰ in 1h30m)
🏆 Liverpool vs Arsenal
💵 Stake: ฿10,000 | Credits: 480
━━━━━━━━━━━━━━━━━━━━━━━━━━
Book          Side            Odds    Stake     Return
───────────────────────────────────────────────
🔵 Bet365     Liverpool      2.10   ฿4,800   ฿10,080
🟠 Pinnacle   Arsenal        2.15   ฿5,200   ฿11,180
───────────────────────────────────────────────
Total                          ฿10,000
📊 Whoever wins
   Liverpool → ฿10,080 (+฿80)
   Arsenal → ฿11,180 (+฿1,180)
🔗 [links]
━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ *Edge Context*
📰 • Arsenal missing 2 key defenders due to injury (news 4h ago)
   • Liverpool won 4 of last 5 matches
🤖 SOFT_SIDE: Bet365 — Liverpool odds look high relative to recent form
   NEWS_EDGE: partial — Arsenal's injury news doesn't appear fully priced in yet
   VERDICT: MODERATE
━━━━━━━━━━━━━━━━━━━━━━━━━━
🆔 `abc123`
```

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `EDGE_CONTEXT_ENABLED` | `true` | Enable/disable the entire feature |
| `EDGE_CONTEXT_TIMEOUT` | `25` | Timeout (seconds) for news search + LLM per signal |
| `ANTHROPIC_API_KEY` | - | Required — if unset, the feature is skipped (normal alerts still send) |

The model used is `claude-sonnet-4-20250514`, hardcoded inside `EdgeContextModule`. Change `_EDGE_CLAUDE_MODEL` if you want a different model.

---

## Caching & Rate Limiting

- News results and LLM interpretation are cached for 30 minutes per match (key = `opp.event`)
- If the same arb signal recurs (e.g. odds shift slightly but it's the same match), the cached result is reused instead of re-searching
- The cache is pruned automatically on every `build_context()` call

---

## Failure Behavior

| Scenario | Result |
|---|---|
| `ANTHROPIC_API_KEY` not set | Alert sends normally, no `⚡ Edge Context` section |
| `EDGE_CONTEXT_ENABLED=false` | Same as above |
| Claude web search/LLM error | Section shows a short error message, but the main alert still sends successfully |
| Timeout exceeds `EDGE_CONTEXT_TIMEOUT` | Section shows "⏱ timeout" instead |

**Edge Context never causes the main alert (arb signal) to fail to send** — all exceptions are caught and a fallback string is returned.

---

## Notes

- This patch only modifies `send_alert()`. It does not affect `scan_all()`, `execute_both()`, settlement logic, or Kelly sizing
- The LLM prompt is constrained to avoid hallucination — if no news explains the gap, it says so directly
- To roll back, copy `arb_bot.py.bak` back over `arb_bot.py`
