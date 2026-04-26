#!/usr/bin/env python3
"""
kpi_extractor.py
Reads content_list_v2.json from a MinerU output folder,
identifies financial statement tables, and extracts KPIs via Gemini Flash.

Tier 1 KPIs (fixed keys, cross-company comparable):
  revenue, gross_profit, profit_before_tax, profit_after_tax,
  eps_basic, total_assets, total_equity, operating_cashflow

Tier 2 KPIs (sector-specific, AI-decided, stored with is_custom=True)

Usage (standalone test):
  python kpi_extractor.py --v2 path/to/content_list_v2.json --symbol JKH.N0000 --period Q3-2025
"""

import os
import re
import json
import argparse
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
_model = genai.GenerativeModel("gemini-2.5-flash")

# ─── Statement heading patterns ───────────────────────────────────────────────
INCOME_PATTERNS = [
    "income statement", "statement of comprehensive income",
    "statement of profit or loss", "profit or loss",
    "condensed interim income", "statement of income",
    "comprehensive income",
]
BALANCE_PATTERNS = [
    "statement of financial position", "balance sheet",
    "financial position", "assets and liabilities",
]
CASHFLOW_PATTERNS = [
    "statement of cash flows", "cash flow statement",
    "cash flows",
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


def call_gemini(prompt: str) -> dict:
    """Send prompt to Gemini Flash and parse JSON response."""
    prompt_chars = len(prompt)
    print(f"      [AI] Sending to Gemini — prompt size: {prompt_chars:,} chars (~{prompt_chars // 4:,} tokens)")
    print(f"      [AI] Waiting for Gemini response...")
    response = _model.generate_content(
        [SYSTEM_PROMPT, prompt],
        generation_config={"temperature": 0},
    )
    raw = response.text.strip()
    print(f"      [AI] Response received — {len(raw):,} chars")
    # Strip markdown fences if model adds them anyway
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"```\s*$", "", raw, flags=re.MULTILINE)
    parsed = json.loads(raw)
    print(f"      [AI] JSON parsed successfully")
    return parsed

# Tier 1 KPI threshold — if fewer than this many are found on pass 1, retry with all tables
_T1_RETRY_THRESHOLD = 3


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
        print(f"      [AI] ✗ No financial tables found — skipping Gemini call")
        return {"symbol": symbol, "period": period, "tier1": {}, "tier2": {}, "error": "no_financial_tables_found"}

    prompt = build_prompt(financial_tables)
    result = call_gemini(prompt)

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
        result2 = call_gemini(prompt2)

        tier1_2  = result2.get("tier1", {})
        t1_found2 = {k: v for k, v in tier1_2.items() if v is not None}

        # Accept pass-2 result only if it improved Tier 1
        if len(t1_found2) > len(t1_found):
            print(f"      [AI] ✓ Pass 2 improved Tier1: {len(t1_found)} → {len(t1_found2)} KPIs")
            result   = result2
            tier1    = tier1_2
            tier2    = result2.get("tier2", {})
            t1_found = t1_found2
        else:
            print(f"      [AI] Pass 2 did not improve Tier1 — keeping pass 1 result")

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--v2",     required=True, help="Path to content_list_v2.json")
    parser.add_argument("--symbol", default="TEST")
    parser.add_argument("--period", default="Q1-2025")
    args = parser.parse_args()

    result = extract_kpis(Path(args.v2), args.symbol, args.period)
    print(json.dumps(result, indent=2))
