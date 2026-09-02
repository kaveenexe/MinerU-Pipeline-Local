#!/usr/bin/env python3
"""
kpi_extractor.py
Extracts KPIs from financial reports via Gemini Flash or NVIDIA NIM.

Two AI backends supported (set AI_BACKEND in .env):
  gemini  — Google Gemini Flash (default)
  nvidia  — NVIDIA NIM (OpenAI-compatible, e.g. deepseek-ai/deepseek-v3)

Two data sources are supported:
  1. File-based  — reads content_list_v2.json produced by MinerU (original path)
  2. DB-based    — reads report_blocks table when v2 JSON files have been deleted

Tier 1 KPIs (fixed keys, cross-company comparable):
  revenue, gross_profit, profit_before_tax, profit_after_tax,
  eps_basic, total_assets, total_equity, operating_cashflow

Tier 2 KPIs (sector-specific, AI-decided, stored with is_custom=True)

Usage (standalone — file source):
  python kpi_extractor.py --v2 path/to/content_list_v2.json --symbol JKH.N0000 --period Q3-2025

Usage (standalone — DB source, requires .env with DB creds):
  python kpi_extractor.py --db-report-id 123 --symbol JKH.N0000 --period Q3-2025
"""

import os
import re
import time
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── AI Backend selection ───────────────────────────────────────────────────────
AI_BACKEND    = os.getenv("AI_BACKEND", "gemini").lower().strip()
NIM_MAX_RPM   = int(os.getenv("NIM_MAX_RPM", 38))   # NVIDIA free tier = 40 rpm
_nim_req_times: list[float] = []                      # rolling window for rate limiting

if AI_BACKEND == "nvidia":
    from openai import OpenAI as _OpenAI
    _nim_client = _OpenAI(
        base_url="https://integrate.api.nvidia.com/v1",
        api_key=os.getenv("NVIDIA_API_KEY"),
    )
    NVIDIA_MODEL = os.getenv("NVIDIA_MODEL", "deepseek-ai/deepseek-v3")
    print(f"[AI] Backend: NVIDIA NIM  model={NVIDIA_MODEL}  max_rpm={NIM_MAX_RPM}")
else:
    import google.generativeai as genai
    genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
    _model = genai.GenerativeModel("gemini-2.5-flash")
    print(f"[AI] Backend: Gemini Flash")

# ─── Statement heading patterns ───────────────────────────────────────────────
INCOME_PATTERNS = [
    "income statement",
    "statement of comprehensive income",
    "statement of profit or loss",
    "profit or loss",
    "condensed interim income",
    "statement of income",
    "comprehensive income",
    "profit or loss and other comprehensive income",  # common combined heading
    "income and expenditure",                          # some older reports
    "results of operations",
]

BALANCE_PATTERNS = [
    "statement of financial position",
    "balance sheet",
    "financial position",
    "assets and liabilities",
    "condensed interim statement of financial position",
    "interim statement of financial position",
    "consolidated statement of financial position",
    "statements of financial position",               # plural variant
    "financial position as at",                       # with date suffix
    "position as at",                                 # truncated variant
]

CASHFLOW_PATTERNS = [
    "statement of cash flows",
    "cash flow statement",
    "cash flows",
    "condensed interim statement of cash flows",      # same pattern as balance
    "cash flows from operating",                      # partial match from table body
    "cash generated from",                            # alternative phrasing
]

def classify_heading(text: str) -> str | None:
    """Return statement type or None."""
    t = text.lower().strip()
    if any(p in t for p in INCOME_PATTERNS):  return "income_statement"
    if any(p in t for p in BALANCE_PATTERNS): return "balance_sheet"
    if any(p in t for p in CASHFLOW_PATTERNS): return "cash_flow"
    return None

def extract_text_from_content(content_block) -> str:
    """Pull plain text out of a v2 content block (title / paragraph)."""
    if isinstance(content_block, dict):
        for key in ("title_content", "paragraph_content", "list_items"):
            val = content_block.get(key)
            if val:
                if isinstance(val, list):
                    parts = []
                    for item in val:
                        if isinstance(item, dict):
                            parts.append(item.get("content", ""))
                        elif isinstance(item, str):
                            parts.append(item)
                    return " ".join(parts)
    return ""

def get_table_caption_text(table_content: dict) -> str:
    """Extract flat text from table_caption list in v2 format."""
    captions = table_content.get("table_caption", [])
    parts = []
    for cap in captions:
        if isinstance(cap, dict):
            parts.append(cap.get("content", ""))
        elif isinstance(cap, str):
            parts.append(cap)
    return " ".join(parts)

def build_tagged_tables(v2_path: Path) -> list[dict]:
    """
    Walk content_list_v2.json pages in reading order.
    Tag each table block with the nearest financial statement heading
    (from table_caption first, then preceding heading blocks).
    Returns list of dicts: {html, statement_type, table_type, page_idx, caption}
    """
    print(f"      [AI] Reading: {v2_path.name}")
    with open(v2_path, "r", encoding="utf-8", errors="replace") as f:
        raw = json.load(f)

    # Normalise: hybrid returns list-of-pages; pipeline returns flat list
    if raw and isinstance(raw[0], list):
        pages = raw
        print(f"      [AI] Format: multi-page ({len(pages)} pages)")
    else:
        pages = [raw]   # wrap flat list as single "page"
        print(f"      [AI] Format: flat list ({len(raw)} blocks)")

    tagged = []
    current_statement = None  # tracks heading context as we walk

    for page_blocks in pages:
        for blk in page_blocks:
            btype = blk.get("type", "")
            content = blk.get("content", {})

            # Update heading context from title / paragraph blocks
            if btype in ("title", "paragraph"):
                heading_text = extract_text_from_content(content)
                stmt = classify_heading(heading_text)
                if stmt:
                    if stmt != current_statement:
                        print(f"      [AI] Heading detected → {stmt}: \"{heading_text[:60]}\"")
                    current_statement = stmt

            elif btype == "table":
                html = content.get("html", "") or content.get("table_body", "")
                if not html:
                    continue

                # Prefer table_caption for statement detection
                caption_text = get_table_caption_text(content)
                stmt_from_caption = classify_heading(caption_text)
                statement_type = stmt_from_caption or current_statement

                tagged.append({
                    "html":           html,
                    "statement_type": statement_type,
                    "table_type":     content.get("table_type", "simple_table"),
                    "page_idx":       blk.get("bbox", [0,0,0,0]),  # bbox[1] = y = page hint
                    "caption":        caption_text,
                })

    # Summary of tagged tables
    by_type = {}
    for t in tagged:
        key = t["statement_type"] or "unknown"
        by_type[key] = by_type.get(key, 0) + 1
    summary = ", ".join(f"{k}={v}" for k, v in by_type.items()) or "none"
    print(f"      [AI] Tables found: {len(tagged)} total ({summary})")

    return tagged


def build_tagged_tables_from_db(cur, report_db_id: int) -> list[dict]:
    """
    DB-based equivalent of build_tagged_tables().

    Walks report_blocks rows in block_index order for the given report_db_id.
    Uses content_text of title/paragraph blocks for heading context,
    and content_html of table blocks as the HTML fed to Gemini.

    Returns the same list[dict] shape as build_tagged_tables():
      {html, statement_type, table_type, caption}
    """
    print(f"      [AI] Reading report_blocks from DB (report_db_id={report_db_id})")
    cur.execute("""
        SELECT block_type, content_text, content_html, table_type
        FROM   report_blocks
        WHERE  report_db_id = %s
        ORDER  BY block_index ASC
    """, (report_db_id,))
    rows = cur.fetchall()
    print(f"      [AI] Fetched {len(rows)} blocks from DB")

    tagged = []
    current_statement = None

    for block_type, content_text, content_html, table_type in rows:
        btype = block_type or ""

        # ── Heading context ───────────────────────────────────────────────────
        if btype in ("title", "paragraph"):
            stmt = classify_heading(content_text or "")
            if stmt and stmt != current_statement:
                preview = (content_text or "")[:60]
                print(f"      [AI] Heading detected → {stmt}: \"{preview}\"")
                current_statement = stmt

        # ── Table block ───────────────────────────────────────────────────────
        elif btype == "table":
            html = (content_html or "").strip()
            if not html:
                continue

            # Try to detect statement type from the HTML itself (e.g. caption text)
            stmt_from_html = classify_heading(html[:300])
            statement_type = stmt_from_html or current_statement

            tagged.append({
                "html":           html,
                "statement_type": statement_type,
                "table_type":     table_type or "simple_table",
                "caption":        "",
            })

    # Summary
    by_type = {}
    for t in tagged:
        key = t["statement_type"] or "unknown"
        by_type[key] = by_type.get(key, 0) + 1
    summary = ", ".join(f"{k}={v}" for k, v in by_type.items()) or "none"
    print(f"      [AI] Tables found: {len(tagged)} total ({summary})")

    return tagged


# ─── Tier 1 KPI definitions ───────────────────────────────────────────────────
TIER1_DEFINITIONS = {
    "revenue":            "Top-line income. For banks: Gross Income. For insurance: Gross Written Premium. For others: Revenue/Turnover/Net Sales.",
    "gross_profit":       "Gross Profit or Gross Income after direct costs. Null if not separately shown.",
    "profit_before_tax":  "Profit/Income Before Income Tax / PBT.",
    "profit_after_tax":   "Net Profit / Profit for the period / Profit After Tax (attributable to equity holders).",
    "eps_basic":          "Basic Earnings Per Share (Rs.). Null if not in report.",
    "total_assets":       "Total Assets from the Balance Sheet / Statement of Financial Position.",
    "total_equity":       "Total Equity (shareholders equity). From Balance Sheet.",
    "operating_cashflow": "Net cash from / (used in) operating activities. From Cash Flow Statement.",
}

SYSTEM_PROMPT = """You are a financial data extraction assistant.
You will receive one or more HTML tables from a Sri Lankan company's quarterly financial report.
Each table is labeled with its statement type (income_statement / balance_sheet / cash_flow / unknown).

RULES:
1. Extract TIER 1 KPIs using ONLY the fixed JSON keys listed. Map company-specific labels to the
   correct key (e.g., "Gross Income" → revenue, "Profit After Tax" → profit_after_tax).
2. Prefer CONSOLIDATED / GROUP figures over Bank standalone when both exist in the same table.
3. Extract the most recent QUARTERLY (3-month period) figure, NOT the year-to-date or annual figure.
   If only annual figures are available, use those and set period_type="annual".
   Default period_type="quarterly".
4. Numbers may use Sri Lankan format: commas as thousand separators, brackets = negative.
   Return raw numbers as plain integers or floats (no commas, no brackets — use minus sign).
   Values may be in Rs. 000 (thousands) — return as-is, do not multiply.
5. If a KPI is not found, return null. Never guess or hallucinate.
6. TIER 2: also extract any other KPIs you find that are meaningful for investors
   (e.g., Net Interest Income, NPL Ratio, Gross Written Premium breakdown, EBITDA).
   Use snake_case keys. These go in the "tier2" object.
7. Return ONLY valid JSON. No explanation, no markdown fences.

Output format:
{
  "period_type": "quarterly",
  "currency_unit": "Rs. 000",
  "tier1": {
    "revenue": null,
    "gross_profit": null,
    "profit_before_tax": null,
    "profit_after_tax": null,
    "eps_basic": null,
    "total_assets": null,
    "total_equity": null,
    "operating_cashflow": null
  },
  "tier2": {}
}
"""


def build_prompt(tagged_tables: list[dict]) -> str:
    """Build the user message from tagged financial tables."""
    parts = []
    for i, t in enumerate(tagged_tables):
        stmt = t["statement_type"] or "unknown"
        cap = t["caption"] or "(no caption)"
        ttype = t["table_type"]

        # Using triple quotes allows the line break
        parts.append(
            f"""--- TABLE {i + 1} | statement: {stmt} | type: {ttype} | caption: {cap} ---
{t['html']}"""
        )
    return "\n\n".join(parts)


def _nim_rate_limit() -> None:
    """Block if we are about to exceed NIM_MAX_RPM requests per minute."""
    now = time.monotonic()
    # Keep only timestamps from the last 60 s
    _nim_req_times[:] = [t for t in _nim_req_times if now - t < 60]
    if len(_nim_req_times) >= NIM_MAX_RPM:
        wait = 60 - (now - _nim_req_times[0]) + 0.5
        print(f"      [AI] NIM rate limit reached — waiting {wait:.1f}s...")
        time.sleep(wait)
    _nim_req_times.append(time.monotonic())


def call_ai(prompt: str) -> dict:
    """Send prompt to the configured AI backend and return parsed JSON."""
    prompt_chars = len(prompt)
    backend_label = AI_BACKEND.upper()
    print(f"      [AI] Sending to {backend_label} — prompt size: {prompt_chars:,} chars (~{prompt_chars // 4:,} tokens)")
    print(f"      [AI] Waiting for {backend_label} response...")

    if AI_BACKEND == "nvidia":
        _nim_rate_limit()
        response = _nim_client.chat.completions.create(
            model=NVIDIA_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            temperature=0,
            max_tokens=2048,
        )
        raw = response.choices[0].message.content.strip()
    else:
        response = _model.generate_content(
            [SYSTEM_PROMPT, prompt],
            generation_config={"temperature": 0},
        )
        raw = response.text.strip()

    print(f"      [AI] Response received — {len(raw):,} chars")

    # Strip markdown fences if model adds them anyway
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"```\s*$",         "", raw, flags=re.MULTILINE)

    # Safety: extract the first valid JSON object in case of surrounding text
    match = re.search(r'\{[\s\S]*\}', raw)
    if match:
        raw = match.group(0)

    parsed = json.loads(raw)
    print(f"      [AI] JSON parsed successfully")
    return parsed

# Tier 1 KPI threshold — if fewer than this many are found on pass 1, retry with all tables
_T1_RETRY_THRESHOLD = 3

# Log file for reports that fail validation even after correction
VALIDATION_LOG = Path("kpi_validation_errors.log")


# ─── Accounting Validation ────────────────────────────────────────────────────
def validate_tier1(tier1: dict) -> list[dict]:
    """
    Run accounting sanity checks on extracted Tier 1 KPIs.
    Returns a list of violation dicts: {id, message}.
    An empty list means all checks passed.

    Rules:
      1. PAT ≤ PBT          (tax reduces profit)
      2. Gross Profit ≤ Revenue
      3. Total Equity ≤ Total Assets
      4. Total Assets > 0
      5. Revenue > 0
      6. EPS sign matches PAT sign
    """
    violations = []
    pat = tier1.get("profit_after_tax")
    pbt = tier1.get("profit_before_tax")
    rev = tier1.get("revenue")
    gp  = tier1.get("gross_profit")
    ta  = tier1.get("total_assets")
    eq  = tier1.get("total_equity")
    eps = tier1.get("eps_basic")

    # Rule 1: PAT ≤ PBT
    if pat is not None and pbt is not None:
        if pat > pbt:
            violations.append({
                "id": "pat_gt_pbt",
                "message": (
                    f"profit_after_tax ({pat:,.2f}) > profit_before_tax ({pbt:,.2f}). "
                    f"PAT must be ≤ PBT — income tax reduces profit "
                    f"(deferred tax credits are a rare exception but unlikely here)."
                ),
            })

    # Rule 2: Gross Profit ≤ Revenue
    if gp is not None and rev is not None:
        if gp > rev:
            violations.append({
                "id": "gp_gt_revenue",
                "message": (
                    f"gross_profit ({gp:,.2f}) > revenue ({rev:,.2f}). "
                    f"Gross profit is revenue minus cost of sales and cannot exceed revenue."
                ),
            })

    # Rule 3: Total Equity ≤ Total Assets
    if eq is not None and ta is not None:
        if eq > ta:
            violations.append({
                "id": "equity_gt_assets",
                "message": (
                    f"total_equity ({eq:,.2f}) > total_assets ({ta:,.2f}). "
                    f"Equity cannot exceed total assets (Assets = Equity + Liabilities)."
                ),
            })

    # Rule 4: Total Assets > 0
    if ta is not None and ta <= 0:
        violations.append({
            "id": "assets_not_positive",
            "message": f"total_assets ({ta:,.2f}) is ≤ 0. Total assets must always be a positive number.",
        })

    # Rule 5: Revenue > 0
    if rev is not None and rev <= 0:
        violations.append({
            "id": "revenue_not_positive",
            "message": f"revenue ({rev:,.2f}) is ≤ 0. Revenue should be a positive value.",
        })

    # Rule 6: EPS sign matches PAT sign
    if eps is not None and pat is not None and pat != 0:
        if (pat > 0 and eps < 0) or (pat < 0 and eps > 0):
            violations.append({
                "id": "eps_sign_mismatch",
                "message": (
                    f"eps_basic ({eps}) has the opposite sign to profit_after_tax ({pat:,.2f}). "
                    f"EPS should be positive when profit is positive and negative when it is negative."
                ),
            })

    return violations


def _build_correction_prompt(original_prompt: str, violations: list[dict], tier1: dict) -> str:
    """Build a correction prompt that tells Gemini exactly which rules were broken."""
    violation_lines = "\n".join(f"  - [{v['id']}] {v['message']}" for v in violations)
    current_values  = "\n".join(
        f"  {k}: {v}" for k, v in tier1.items() if v is not None
    )
    return (
        f"Your previous extraction contained the following ACCOUNTING ERRORS:\n\n"
        f"{violation_lines}\n\n"
        f"Your extracted values that caused these errors:\n{current_values}\n\n"
        f"Please re-examine the financial tables carefully. "
        f"Correct ONLY the values involved in the errors above. "
        f"Return the full JSON response with the corrected values.\n\n"
        f"--- ORIGINAL TABLES ---\n{original_prompt}"
    )


def _log_validation_failure(
    symbol: str, period: str, violations: list[dict], tier1: dict
) -> None:
    """Append a structured failure record to VALIDATION_LOG."""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"\n{'=' * 70}",
        f"TIMESTAMP  : {now}",
        f"TICKER     : {symbol}",
        f"PERIOD     : {period}",
        f"STATUS     : VALIDATION_FAILED_AFTER_RETRY",
        "VIOLATIONS :",
    ]
    for v in violations:
        lines.append(f"  [{v['id']}] {v['message']}")
    lines.append("TIER1 VALUES AT FAILURE:")
    for k, v in tier1.items():
        lines.append(f"  {k}: {v}")
    lines.append("")

    with open(VALIDATION_LOG, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"      [VAL] ⚠ Failure logged → {VALIDATION_LOG}")


def _validate_and_correct(
    final_prompt: str,
    tier1: dict,
    tier2: dict,
    result: dict,
    symbol: str,
    period: str,
) -> tuple[dict, dict, dict]:
    """
    Shared validation + correction helper used by both extract_kpis() and
    extract_kpis_from_db().

    1. Runs validate_tier1() on the current tier1.
    2. If violations found → asks Gemini to correct them.
    3. If still violations after correction → logs to VALIDATION_LOG.
    4. Returns (tier1, tier2, result) — possibly corrected.
    """
    violations = validate_tier1(tier1)

    if not violations:
        print(f"      [VAL] ✓ All accounting checks passed")
        return tier1, tier2, result

    print(f"      [VAL] ⚠ {len(violations)} violation(s) detected:")
    for v in violations:
        print(f"        [{v['id']}] {v['message']}")
    print(f"      [VAL] Sending correction prompt to Gemini...")

    correction_prompt = _build_correction_prompt(final_prompt, violations, tier1)
    try:
        result_c     = call_ai(correction_prompt)
        tier1_c      = result_c.get("tier1", {})
        tier2_c      = result_c.get("tier2", {})
        violations_c = validate_tier1(tier1_c)

        if not violations_c:
            print(f"      [VAL] ✓ Correction accepted — all checks now pass")
            return tier1_c, tier2_c, result_c
        else:
            print(f"      [VAL] ✗ {len(violations_c)} violation(s) remain after correction — logging")
            _log_validation_failure(symbol, period, violations_c, tier1_c)
            # Still return the corrected values (may be partially better)
            return tier1_c, tier2_c, result_c

    except Exception as exc:
        print(f"      [VAL] ✗ Correction call failed ({exc}) — keeping original values")
        return tier1, tier2, result


def extract_kpis(v2_path: Path, symbol: str = "", period: str = "") -> dict:
    """
    Main entry point — two-pass extraction.

    Pass 1: send only tables whose statement type was recognised from headings.
    Pass 2: if Tier 1 result is sparse (< _T1_RETRY_THRESHOLD non-null), retry
            with ALL tables so that reports with non-standard headings are covered.

    Returns: {"symbol": ..., "period": ..., "tier1": {...}, "tier2": {...}}
    """
    print(f"      [AI] ── KPI Extraction: {symbol} / {period} ──")
    tagged = build_tagged_tables(v2_path)

    # ── Pass 1: filtered tables ───────────────────────────────────────────────
    financial_tables = [t for t in tagged if t["statement_type"] is not None]
    print(f"      [AI] Financial tables after filter: {len(financial_tables)} / {len(tagged)}")

    # Fallback within pass 1: if nothing tagged, try complex_tables
    if not financial_tables:
        financial_tables = [t for t in tagged if t.get("table_type") == "complex_table"]
        if financial_tables:
            print(f"      [AI] Fallback: using {len(financial_tables)} complex_table(s) (no statement headings found)")

    if not financial_tables:
        print(f"      [AI] ✗ No financial tables found — skipping {AI_BACKEND.upper()} call")
        return {"symbol": symbol, "period": period, "tier1": {}, "tier2": {}, "error": "no_financial_tables_found"}

    prompt = build_prompt(financial_tables)
    result = call_ai(prompt)
    final_prompt = prompt   # track which prompt produced the accepted result

    tier1 = result.get("tier1", {})
    tier2 = result.get("tier2", {})
    t1_found = {k: v for k, v in tier1.items() if v is not None}

    # ── Pass 2: retry with all tables if Tier 1 is sparse ────────────────────
    all_tables_sent = len(financial_tables) == len(tagged)
    if len(t1_found) < _T1_RETRY_THRESHOLD and not all_tables_sent:
        print(
            f"      [AI] ⚠ Sparse Tier1 ({len(t1_found)}/8) — "
            f"retrying with all {len(tagged)} tables (headings may not have matched)..."
        )
        prompt2 = build_prompt(tagged)
        result2 = call_ai(prompt2)

        tier1_2  = result2.get("tier1", {})
        t1_found2 = {k: v for k, v in tier1_2.items() if v is not None}

        # Accept pass-2 result only if it improved Tier 1
        if len(t1_found2) > len(t1_found):
            print(f"      [AI] ✓ Pass 2 improved Tier1: {len(t1_found)} → {len(t1_found2)} KPIs")
            result       = result2
            tier1        = tier1_2
            tier2        = result2.get("tier2", {})
            t1_found     = t1_found2
            final_prompt = prompt2
        else:
            print(f"      [AI] Pass 2 did not improve Tier1 — keeping pass 1 result")

    # ── Accounting validation + correction ───────────────────────────────────
    tier1, tier2, result = _validate_and_correct(final_prompt, tier1, tier2, result, symbol, period)
    t1_found = {k: v for k, v in tier1.items() if v is not None}

    # ── Final log ─────────────────────────────────────────────────────────────
    t1_null = [k for k, v in tier1.items() if v is None]
    print(f"      [AI] Tier1 extracted ({len(t1_found)}/8): {list(t1_found.keys())}")
    if t1_null:
        print(f"      [AI] Tier1 nulls: {t1_null}")
    print(f"      [AI] Tier2 extracted ({len(tier2)}): {list(tier2.keys())[:8]}{'...' if len(tier2) > 8 else ''}")
    print(f"      [AI] Period type: {result.get('period_type', '?')} | Currency: {result.get('currency_unit', '?')}")

    return {
        "symbol":        symbol,
        "period":        period,
        "period_type":   result.get("period_type", "quarterly"),
        "currency_unit": result.get("currency_unit", ""),
        "tier1":         tier1,
        "tier2":         tier2,
    }


def extract_kpis_from_db(cur, report_db_id: int, symbol: str = "", period: str = "") -> dict:
    """
    DB-based two-pass KPI extraction.
    Same logic as extract_kpis() but uses build_tagged_tables_from_db()
    instead of reading a file from disk.

    Use this when content_list_v2.json files have been deleted.
    """
    print(f"      [AI] ── KPI Extraction (DB source): {symbol} / {period} ──")
    tagged = build_tagged_tables_from_db(cur, report_db_id)

    # ── Pass 1: filtered tables ───────────────────────────────────────────────
    financial_tables = [t for t in tagged if t["statement_type"] is not None]
    print(f"      [AI] Financial tables after filter: {len(financial_tables)} / {len(tagged)}")

    # Fallback within pass 1: if nothing tagged, try complex_tables
    if not financial_tables:
        financial_tables = [t for t in tagged if t.get("table_type") == "complex_table"]
        if financial_tables:
            print(f"      [AI] Fallback: using {len(financial_tables)} complex_table(s)")

    if not financial_tables:
        print(f"      [AI] ✗ No financial tables found — skipping {AI_BACKEND.upper()} call")
        return {"symbol": symbol, "period": period, "tier1": {}, "tier2": {}, "error": "no_financial_tables_found"}

    prompt = build_prompt(financial_tables)
    result = call_ai(prompt)
    final_prompt = prompt   # track which prompt produced the accepted result

    tier1    = result.get("tier1", {})
    tier2    = result.get("tier2", {})
    t1_found = {k: v for k, v in tier1.items() if v is not None}

    # ── Pass 2: retry with all tables if Tier 1 is sparse ────────────────────
    all_tables_sent = len(financial_tables) == len(tagged)
    if len(t1_found) < _T1_RETRY_THRESHOLD and not all_tables_sent:
        print(
            f"      [AI] ⚠ Sparse Tier1 ({len(t1_found)}/8) — "
            f"retrying with all {len(tagged)} tables..."
        )
        prompt2   = build_prompt(tagged)
        result2   = call_ai(prompt2)
        tier1_2   = result2.get("tier1", {})
        t1_found2 = {k: v for k, v in tier1_2.items() if v is not None}
        if len(t1_found2) > len(t1_found):
            print(f"      [AI] ✓ Pass 2 improved Tier1: {len(t1_found)} → {len(t1_found2)} KPIs")
            result       = result2
            tier1        = tier1_2
            tier2        = result2.get("tier2", {})
            t1_found     = t1_found2
            final_prompt = prompt2
        else:
            print(f"      [AI] Pass 2 did not improve Tier1 — keeping pass 1 result")

    # ── Accounting validation + correction ───────────────────────────────────
    tier1, tier2, result = _validate_and_correct(final_prompt, tier1, tier2, result, symbol, period)
    t1_found = {k: v for k, v in tier1.items() if v is not None}

    # ── Final log ─────────────────────────────────────────────────────────────
    t1_null = [k for k, v in tier1.items() if v is None]
    print(f"      [AI] Tier1 extracted ({len(t1_found)}/8): {list(t1_found.keys())}")
    if t1_null:
        print(f"      [AI] Tier1 nulls: {t1_null}")
    print(f"      [AI] Tier2 extracted ({len(tier2)}): {list(tier2.keys())[:8]}{'...' if len(tier2) > 8 else ''}")
    print(f"      [AI] Period type: {result.get('period_type', '?')} | Currency: {result.get('currency_unit', '?')}")

    return {
        "symbol":        symbol,
        "period":        period,
        "period_type":   result.get("period_type", "quarterly"),
        "currency_unit": result.get("currency_unit", ""),
        "tier1":         tier1,
        "tier2":         tier2,
    }


# ─── CLI standalone test ──────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    import mysql.connector
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="Standalone KPI extractor test")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--v2",           help="Path to content_list_v2.json (file source)")
    src.add_argument("--db-report-id", type=int, metavar="ID",
                     help="report_blocks.report_db_id to read from DB (DB source)")
    parser.add_argument("--symbol", default="TEST")
    parser.add_argument("--period", default="Q1-2025")
    args = parser.parse_args()

    if args.v2:
        result = extract_kpis(Path(args.v2), args.symbol, args.period)
    else:
        # DB source — connect, build tagged tables, run same two-pass logic
        _db_cfg = {
            "host":     os.getenv("DB_HOST", "localhost"),
            "port":     int(os.getenv("DB_PORT", 3306)),
            "user":     os.getenv("DB_USER", "root"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME", "mineru_cse"),
        }
        _conn = mysql.connector.connect(**_db_cfg)
        _cur  = _conn.cursor()
        result = extract_kpis_from_db(_cur, args.db_report_id, args.symbol, args.period)
        _cur.close()
        _conn.close()

    print(json.dumps(result, indent=2))
