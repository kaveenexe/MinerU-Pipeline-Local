#!/usr/bin/env python3
"""
CSE Quarterly Report Pipeline
Fetches quarterly reports from CSE API, extracts with MinerU, stores in MySQL.

Usage:
  python pipeline.py                        # Run full pipeline
  python pipeline.py --status               # Show progress summary
  python pipeline.py --delete ABAN.N0000    # Delete all data for ticker
  python pipeline.py --limit 8             # Override quarterly report count
  python pipeline.py --companies path.xlsx  # Custom Excel file path
  python pipeline.py --retry-failed         # Retry all failed reports
"""

import os
import sys
import json
import signal
import shutil
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

import requests
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from kpi_extractor import extract_kpis

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "root"),
}
DB_NAME          = os.getenv("DB_NAME", "mineru_cse")
PDF_BASE_URL     = os.getenv("PDF_BASE_URL", "https://cdn.cse.lk/")
PDF_DIR          = Path(os.getenv("PDF_DIR", "pdfs"))
OUTPUT_DIR       = Path(os.getenv("OUTPUT_DIR", "output"))
QUARTERLY_LIMIT  = int(os.getenv("QUARTERLY_LIMIT", 4))
CSE_API_BASE     = os.getenv("CSE_API_BASE", "https://www.cse.lk/api/financials")

# ── Graceful Shutdown ─────────────────────────────────────────────────────────
_shutdown = False

def get_db_connection(with_db=True):
    cfg = {**DB_CONFIG}
    if with_db:
        cfg["database"] = DB_NAME
    return mysql.connector.connect(**cfg)


def purge_all():
    """
    Nuclear reset: drops all pipeline data.
    - Truncates all pipeline tables (financial_kpis, report_blocks, reports, pipeline_state, companies)
    - Deletes the entire /pdfs and /output folders
    """
    import shutil

    print("\n[!] PURGE ALL — this will delete everything from the DB and disk.")
    confirm = input("    Type YES to confirm: ").strip()
    if confirm != "YES":
        print("    Aborted.")
        return

    # ── Database ─────────────────────────────────────────────────────────────
    conn = get_db_connection()
    cur  = conn.cursor()

    tables = ["financial_kpis", "report_blocks", "reports", "pipeline_state", "companies"]
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    for tbl in tables:
        cur.execute(f"TRUNCATE TABLE `{tbl}`")
        print(f"  [✓] Truncated: {tbl}")
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    conn.commit()
    cur.close()
    conn.close()

    # ── Disk ─────────────────────────────────────────────────────────────────
    for folder in [PDF_DIR, OUTPUT_DIR]:
        if folder.exists():
            shutil.rmtree(folder)
            folder.mkdir(exist_ok=True)          # recreate empty
            print(f"  [✓] Cleared folder: {folder}")
        else:
            print(f"  [skip] Folder not found: {folder}")

    print("\n[✓] Purge complete. DB is empty, folders are cleared.")


def _handle_signal(sig, frame):
    global _shutdown
    print("\n\n[!] Ctrl+C detected — finishing current step then stopping safely...")
    _shutdown = True

signal.signal(signal.SIGINT, _handle_signal)


# ── Database ──────────────────────────────────────────────────────────────────
def get_conn(with_db=True):
    cfg = DB_CONFIG.copy()
    if with_db:
        cfg["database"] = DB_NAME
    return mysql.connector.connect(**cfg)


def init_database():
    conn = get_conn(with_db=False)
    cur  = conn.cursor()

    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    cur.execute(f"USE `{DB_NAME}`")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            name       VARCHAR(255),
            symbol     VARCHAR(50) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            company_symbol  VARCHAR(50)  NOT NULL,
            cse_report_id   INT          NOT NULL,
            file_text       VARCHAR(500),
            manual_date     BIGINT,
            uploaded_date   BIGINT,
            pdf_url         VARCHAR(500),
            pdf_local_path  VARCHAR(500),
            md_content      LONGTEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_report (company_symbol, cse_report_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS report_blocks (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            report_db_id  INT          NOT NULL,
            block_index   INT          NOT NULL,
            block_type    VARCHAR(50),
            text_level    INT          DEFAULT NULL,
            content_text  LONGTEXT,
            content_html  LONGTEXT,
            img_path      VARCHAR(500),
            table_type    VARCHAR(50)  DEFAULT NULL,
            table_source  VARCHAR(50)  DEFAULT NULL,
            page_number   INT,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_rid  (report_db_id),
            INDEX idx_type (block_type),
            INDEX idx_page (page_number),
            FOREIGN KEY (report_db_id) REFERENCES reports(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            company_symbol  VARCHAR(50) NOT NULL,
            cse_report_id   INT         NOT NULL,
            file_text       VARCHAR(500),
            pdf_url         VARCHAR(500),
            stage           ENUM(
                                'queued','pdf_downloaded','mineru_extracted',
                                'completed','failed','deleted'
                            ) DEFAULT 'queued',
            error_message   TEXT,
            attempts        INT DEFAULT 0,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_state (company_symbol, cse_report_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


    cur.execute("""
        CREATE TABLE IF NOT EXISTS financial_kpis (
            id             INT AUTO_INCREMENT PRIMARY KEY,
            company_symbol VARCHAR(50)  NOT NULL,
            report_db_id   INT          NOT NULL,
            period         VARCHAR(50),
            period_type    ENUM('quarterly','annual') DEFAULT 'quarterly',
            currency_unit  VARCHAR(30)  DEFAULT 'LKR',
            metric         VARCHAR(100) NOT NULL,
            value          DECIMAL(24,4),
            is_custom      TINYINT(1)   DEFAULT 0,
            extracted_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol_period (company_symbol, period),
            INDEX idx_metric        (metric),
            UNIQUE KEY uq_kpi (company_symbol, period, metric)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    conn.commit()

    # ── Safe migrations: add columns that may be missing from older schema ────
    migrations = [
        ("report_blocks", "table_type",   "ALTER TABLE report_blocks ADD COLUMN table_type  VARCHAR(50)  DEFAULT NULL AFTER img_path"),
        ("report_blocks", "table_source", "ALTER TABLE report_blocks ADD COLUMN table_source VARCHAR(50)  DEFAULT NULL AFTER table_type"),
        ("report_blocks", "page_number",  "ALTER TABLE report_blocks ADD COLUMN page_number  INT          DEFAULT NULL AFTER table_source"),
    ]
    for tbl, col, sql in migrations:
        cur.execute(f"""
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
        """, (DB_NAME, tbl, col))
        if cur.fetchone()[0] == 0:
            cur.execute(sql)
            print(f"  [migrate] Added column: {tbl}.{col}")
    conn.commit()
    cur.close()
    conn.close()

    print(f"[✓] Database `{DB_NAME}` ready")


# ── Pipeline State Helpers ────────────────────────────────────────────────────
def get_state(cur, symbol, cse_id):
    cur.execute(
        "SELECT stage FROM pipeline_state WHERE company_symbol=%s AND cse_report_id=%s",
        (symbol, cse_id)
    )
    row = cur.fetchone()
    return row[0] if row else None


def set_state(cur, symbol, cse_id, stage, file_text="", pdf_url="", error=None):
    cur.execute("""
        INSERT INTO pipeline_state
            (company_symbol, cse_report_id, file_text, pdf_url, stage, error_message, attempts)
        VALUES (%s, %s, %s, %s, %s, %s, 1)
        ON DUPLICATE KEY UPDATE
            stage         = VALUES(stage),
            error_message = VALUES(error_message),
            attempts      = attempts + 1,
            updated_at    = CURRENT_TIMESTAMP
    """, (symbol, cse_id, file_text, pdf_url, stage, error))


# ── CSE API ───────────────────────────────────────────────────────────────────
def fetch_quarterly_reports(symbol, limit):
    try:
        resp = requests.post(f"{CSE_API_BASE}?symbol={symbol}", timeout=30)
        resp.raise_for_status()
        quarters = resp.json().get("infoQuarterlyData", [])
        quarters.sort(key=lambda x: x.get("uploadedDate", 0), reverse=True)
        return quarters[:limit]
    except Exception as e:
        print(f"  [!] API error for {symbol}: {e}")
        return []


# ── PDF Download ──────────────────────────────────────────────────────────────
def download_pdf(symbol, report):
    url      = PDF_BASE_URL + report["path"]
    dest_dir = PDF_DIR / symbol
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{report['id']}_{Path(report['path']).name}"
    dest     = dest_dir / filename

    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=16384):
            f.write(chunk)
    return str(dest)


# ── MinerU Extraction ─────────────────────────────────────────────────────────
def run_mineru(pdf_path):
    out_dir = OUTPUT_DIR / Path(pdf_path).stem
    out_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["VIRTUAL_VRAM_SIZE"]         = os.getenv("VIRTUAL_VRAM_SIZE", "6")
    env["MINERU_HYBRID_BATCH_RATIO"] = os.getenv("MINERU_HYBRID_BATCH_RATIO", "2")
    env["PYTORCH_CUDA_ALLOC_CONF"]   = "max_split_size_mb:512"

    result = subprocess.run(
        ["mineru", "-p", str(pdf_path), "-o", str(out_dir), "-b", "hybrid-auto-engine"],
        env=env,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        snippet = (result.stderr or result.stdout or "")[-600:]
        raise RuntimeError(f"MinerU exited {result.returncode}: {snippet}")
    return out_dir


# ── Output File Locator ───────────────────────────────────────────────────────
def find_output_files(pdf_path):
    """
    MinerU creates: OUTPUT_DIR/<pdf_stem>/auto/<pdf_stem>.md
                    OUTPUT_DIR/<pdf_stem>/auto/<pdf_stem>_content_list.json
                    OUTPUT_DIR/<pdf_stem>/auto/<pdf_stem>_content_list_v2.json
    """
    base = OUTPUT_DIR / Path(pdf_path).stem

    content_list    = next(base.rglob("*_content_list.json"), None)
    content_list_v2 = next(base.rglob("*_content_list_v2.json"), None)
    md_file         = next(base.rglob("*.md"), None)

    return content_list, content_list_v2, md_file


# ── MySQL Storage ─────────────────────────────────────────────────────────────
def store_report(conn, cur, symbol, report, pdf_local_path):
    content_list_path, content_list_v2_path, md_path = find_output_files(pdf_local_path)

    if not content_list_path:
        raise RuntimeError(
            f"content_list.json not found under {OUTPUT_DIR / Path(pdf_local_path).stem}"
        )

    md_content = ""
    if md_path and md_path.exists():
        md_content = md_path.read_text(encoding="utf-8", errors="replace")

    # Build table_type lookup from content_list_v2 {block_index: table_type}
    v2_table_types = {}
    if content_list_v2_path and content_list_v2_path.exists():
        with open(content_list_v2_path, "r", encoding="utf-8", errors="replace") as f:
            v2_raw = json.load(f)
        # Flatten if list-of-pages
        if v2_raw and isinstance(v2_raw[0], list):
            v2_blocks = [blk for page in v2_raw for blk in page]
        else:
            v2_blocks = v2_raw
        for i, blk in enumerate(v2_blocks):
            if blk.get("type") == "table":
                ttype = blk.get("content", {}).get("table_type", None)
                v2_table_types[i] = ttype

    # Upsert report row
    cur.execute("""
        INSERT INTO reports
            (company_symbol, cse_report_id, file_text, manual_date,
             uploaded_date, pdf_url, pdf_local_path, md_content)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            md_content     = VALUES(md_content),
            pdf_local_path = VALUES(pdf_local_path),
            updated_at     = CURRENT_TIMESTAMP
    """, (
        symbol,
        report["id"],
        report.get("fileText", ""),
        report.get("manualDate"),
        report.get("uploadedDate"),
        PDF_BASE_URL + report["path"],
        pdf_local_path,
        md_content,
    ))

    report_db_id = cur.lastrowid
    if report_db_id == 0:
        cur.execute(
            "SELECT id FROM reports WHERE company_symbol=%s AND cse_report_id=%s",
            (symbol, report["id"])
        )
        report_db_id = cur.fetchone()[0]

    # Clear old blocks (supports re-processing)
    cur.execute("DELETE FROM report_blocks WHERE report_db_id=%s", (report_db_id,))

    # Parse and insert blocks
    with open(content_list_path, "r", encoding="utf-8", errors="replace") as f:
        raw = json.load(f)
    # hybrid-auto-engine returns list-of-pages (list of lists); pipeline returns flat list
    if raw and isinstance(raw[0], list):
        blocks = [blk for page in raw for blk in page]
    else:
        blocks = raw

    rows = []
    for idx, blk in enumerate(blocks):
        btype      = blk.get("type", "unknown")
        table_type = v2_table_types.get(idx) if btype == "table" else None
        rows.append((
            report_db_id,
            idx,
            btype,
            blk.get("text_level"),
            blk.get("text") or blk.get("img_caption") or "",
            blk.get("table_body", ""),
            blk.get("img_path", ""),
            table_type,
            blk.get("page_idx", 0),
        ))

    cur.executemany("""
        INSERT INTO report_blocks
            (report_db_id, block_index, block_type, text_level,
             content_text, content_html, img_path, table_type, page_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, rows)

    conn.commit()
    return len(rows)


# ── Delete Ticker ─────────────────────────────────────────────────────────────
def delete_ticker(symbol):
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("SELECT pdf_local_path FROM reports WHERE company_symbol=%s", (symbol,))
    pdf_paths = [r[0] for r in cur.fetchall() if r[0]]

    cur.execute("DELETE FROM reports WHERE company_symbol=%s", (symbol,))
    cur.execute("DELETE FROM pipeline_state WHERE company_symbol=%s", (symbol,))
    cur.execute("DELETE FROM companies WHERE symbol=%s", (symbol,))
    conn.commit()

    # Delete PDF folder
    ticker_pdf_dir = PDF_DIR / symbol
    if ticker_pdf_dir.exists():
        shutil.rmtree(ticker_pdf_dir)
        print(f"  [✓] Removed PDF folder: {ticker_pdf_dir}")

    # Delete MinerU output folders
    for pdf_path in pdf_paths:
        out_dir = OUTPUT_DIR / Path(pdf_path).stem
        if out_dir.exists():
            shutil.rmtree(out_dir)
            print(f"  [✓] Removed output:    {out_dir}")

    cur.close()
    conn.close()
    print(f"[✓] All data deleted for: {symbol}")


# ── Status Report ─────────────────────────────────────────────────────────────
def show_status():
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("SELECT stage, COUNT(*) FROM pipeline_state GROUP BY stage")
    rows = cur.fetchall()

    cur.execute("SELECT COUNT(DISTINCT company_symbol) FROM pipeline_state WHERE stage != 'deleted'")
    companies = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM reports")
    total_reports = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM report_blocks")
    total_blocks = cur.fetchone()[0]

    print("\n╔══════════════════════════════╗")
    print("║    CSE Pipeline Status       ║")
    print("╠══════════════════════════════╣")
    total = sum(c for _, c in rows)
    for stage, count in sorted(rows, key=lambda x: x[1], reverse=True):
        bar = "█" * int(count / max(total, 1) * 20)
        print(f"  {stage:<20} {count:>5}  {bar}")
    print(f"  {'─'*38}")
    print(f"  {'TOTAL REPORTS':<20} {total:>5}")
    print(f"  {'COMPANIES':<20} {companies:>5}")
    print(f"  {'DB REPORTS':<20} {total_reports:>5}")
    print(f"  {'DB BLOCKS':<20} {total_blocks:>5}")
    print("╚══════════════════════════════╝\n")

    cur.close()
    conn.close()


# ── Retry Failed ──────────────────────────────────────────────────────────────
def retry_failed():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        "UPDATE pipeline_state SET stage='queued', error_message=NULL "
        "WHERE stage='failed'"
    )
    count = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"[✓] Reset {count} failed reports to 'queued'. Run pipeline again to retry.")


# ── Main Pipeline ─────────────────────────────────────────────────────────────
def store_kpis(conn, cur, symbol, report_db_id, kpi_result):
    """Upsert Tier1 + Tier2 KPIs into financial_kpis table."""
    period       = kpi_result.get("period", "")
    period_type  = kpi_result.get("period_type", "quarterly")
    currency_unit = kpi_result.get("currency_unit", "")

    rows = []
    for metric, value in kpi_result.get("tier1", {}).items():
        if value is not None:
            rows.append((symbol, report_db_id, period, period_type, currency_unit, metric, float(value), 0))

    for metric, value in kpi_result.get("tier2", {}).items():
        if value is not None:
            try:
                rows.append((symbol, report_db_id, period, period_type, currency_unit, metric, float(value), 1))
            except (TypeError, ValueError):
                pass  # skip non-numeric tier2 values

    if rows:
        cur.executemany("""
            INSERT INTO financial_kpis
                (company_symbol, report_db_id, period, period_type, currency_unit, metric, value, is_custom)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE value = VALUES(value), period_type = VALUES(period_type)
        """, rows)


def run_kpi_only(companies_xlsx, limit):
    """
    Re-run Gemini KPI extraction on all completed reports
    (reports that already have MinerU output but no/incomplete KPI data).
    """
    import pandas as pd
    df  = pd.read_excel(companies_xlsx)
    df.columns = [c.strip() for c in df.columns]
    symbols = df["Symbol"].dropna().str.strip().tolist()

    conn = get_conn()
    cur  = conn.cursor()

    total = 0
    for symbol in symbols:
        cur.execute("""
            SELECT r.id, r.cse_report_id, r.pdf_local_path
            FROM   reports r
            WHERE  r.company_symbol = %s
            ORDER  BY r.uploaded_date DESC
            LIMIT  %s
        """, (symbol, limit))
        rep_rows = cur.fetchall()
        if not rep_rows:
            continue

        for report_db_id, cse_report_id, pdf_local_path in rep_rows:
            # Skip if KPI already extracted (has at least one tier1 row)
            cur.execute("""
                SELECT COUNT(*) FROM financial_kpis
                WHERE company_symbol=%s AND report_db_id=%s AND is_custom=0
            """, (symbol, report_db_id))
            kpi_count = cur.fetchone()[0]
            if kpi_count > 0:
                print(f"  [skip] {symbol} report {cse_report_id} — {kpi_count} KPIs already present")
                continue

            if not pdf_local_path:
                print(f"  [!] {symbol} report {cse_report_id} — no pdf_local_path, skipping")
                continue

            _, v2_path, _ = find_output_files(pdf_local_path)
            if not v2_path or not v2_path.exists():
                print(f"  [!] {symbol} report {cse_report_id} — content_list_v2.json not found")
                continue

            print(f"  → KPI extraction: {symbol} report {cse_report_id}")
            try:
                period_label = f"report-{cse_report_id}"
                kpi_result   = extract_kpis(v2_path, symbol, period_label)
                store_kpis(conn, cur, symbol, report_db_id, kpi_result)
                conn.commit()
                t1 = sum(1 for v in kpi_result.get("tier1", {}).values() if v is not None)
                t2 = len(kpi_result.get("tier2", {}))
                print(f"    [✓] {t1} Tier1, {t2} Tier2 KPIs stored")
                total += 1
            except Exception as e:
                print(f"    [✗] Failed: {e}")

    cur.close()
    conn.close()
    print(f"\n[✓] KPI extraction complete — {total} reports processed")


def run_pipeline(companies_xlsx, quarterly_limit, skip_kpi=False):
    global _shutdown

    df = pd.read_excel(companies_xlsx)
    df.columns = [c.strip() for c in df.columns]
    symbols = df["Symbol"].dropna().str.strip().tolist()
    print(f"[✓] Loaded {len(symbols)} companies | limit={quarterly_limit} quarters each\n")

    PDF_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)

    conn = get_conn()
    cur  = conn.cursor()
    total = len(symbols)

    for ci, symbol in enumerate(symbols, 1):
        if _shutdown:
            break

        print(f"[{ci:>3}/{total}] {symbol}")

        reports = fetch_quarterly_reports(symbol, quarterly_limit)
        if not reports:
            print(f"  [!] No quarterly data — skipping")
            continue

        cur.execute("INSERT IGNORE INTO companies (symbol) VALUES (%s)", (symbol,))
        conn.commit()

        for report in reports:
            if _shutdown:
                break

            cse_id    = report["id"]
            file_text = report.get("fileText", "")
            pdf_url   = PDF_BASE_URL + report["path"]
            state     = get_state(cur, symbol, cse_id)

            if state in ("completed", "deleted"):
                print(f"  [skip] {file_text[:65]} ({state})")
                continue

            print(f"  ↳ {file_text[:70]}")

            if state is None:
                set_state(cur, symbol, cse_id, "queued", file_text, pdf_url)
                conn.commit()
                state = "queued"

            # ── Step 1: Download PDF ──────────────────────────
            pdf_local_path = None
            if state == "queued":
                try:
                    print(f"    [1/3] Downloading...")
                    pdf_local_path = download_pdf(symbol, report)
                    set_state(cur, symbol, cse_id, "pdf_downloaded", file_text, pdf_url)
                    conn.commit()
                    state = "pdf_downloaded"
                    print(f"    [✓] Saved: {pdf_local_path}")
                except Exception as e:
                    set_state(cur, symbol, cse_id, "failed", file_text, pdf_url, str(e))
                    conn.commit()
                    print(f"    [✗] Download failed: {e}")
                    continue
            else:
                cur.execute(
                    "SELECT pdf_local_path FROM reports "
                    "WHERE company_symbol=%s AND cse_report_id=%s",
                    (symbol, cse_id)
                )
                row = cur.fetchone()
                if row and row[0] and Path(row[0]).exists():
                    pdf_local_path = row[0]
                else:
                    try:
                        print(f"    [1/3] Re-downloading (file missing)...")
                        pdf_local_path = download_pdf(symbol, report)
                        state = "pdf_downloaded"
                    except Exception as e:
                        set_state(cur, symbol, cse_id, "failed", file_text, pdf_url, str(e))
                        conn.commit()
                        print(f"    [✗] Re-download failed: {e}")
                        continue

            # ── Step 2: MinerU Extraction ─────────────────────
            if state in ("queued", "pdf_downloaded"):
                try:
                    print(f"    [2/3] Running MinerU...")
                    run_mineru(pdf_local_path)
                    set_state(cur, symbol, cse_id, "mineru_extracted", file_text, pdf_url)
                    conn.commit()
                    state = "mineru_extracted"
                    print(f"    [✓] Extraction complete")
                except Exception as e:
                    set_state(cur, symbol, cse_id, "failed", file_text, pdf_url, str(e))
                    conn.commit()
                    print(f"    [✗] MinerU failed: {e}")
                    continue

            # ── Step 3: Store in MySQL ────────────────────────
            if state in ("queued", "pdf_downloaded", "mineru_extracted"):
                try:
                    print(f"    [3/3] Storing in MySQL...")
                    n = store_report(conn, cur, symbol, report, pdf_local_path)
                    set_state(cur, symbol, cse_id, "completed", file_text, pdf_url)
                    conn.commit()
                    print(f"    [✓] Stored {n} blocks")

                    if skip_kpi:
                        print(f"    [skip] KPI step skipped (--no-kpi flag)")
                    else:
    # ── Step 4: KPI extraction via Gemini ──────────────
                        _, v2_path, _ = find_output_files(pdf_local_path)
                        if v2_path and v2_path.exists():
                            try:
                                print(f"    [4/4] Extracting KPIs via Gemini...")
                                period_label = f"report-{cse_id}"
                                # Get the report DB id for FK
                                cur.execute(
                                    "SELECT id FROM reports WHERE company_symbol=%s AND cse_report_id=%s",
                                    (symbol, cse_id)
                                )
                                row = cur.fetchone()
                                report_db_id = row[0] if row else 0
                                kpi_result = extract_kpis(v2_path, symbol, period_label)
                                store_kpis(conn, cur, symbol, report_db_id, kpi_result)
                                conn.commit()
                                t1 = sum(1 for v in kpi_result.get("tier1", {}).values() if v is not None)
                                t2 = len(kpi_result.get("tier2", {}))
                                print(f"    [✓] KPIs: {t1} Tier1, {t2} Tier2")
                            except Exception as ke:
                                print(f"    [!] KPI extraction failed (non-fatal): {ke}")
                        else:
                            print(f"    [!] content_list_v2.json not found, skipping KPI step")
                except Exception as e:
                    set_state(cur, symbol, cse_id, "failed", file_text, pdf_url, str(e))
                    conn.commit()
                    print(f"    [✗] Storage failed: {e}")
                    continue

    cur.close()
    conn.close()

    if _shutdown:
        print("\n[!] Pipeline paused safely. Run again to resume from where you left off.")
    else:
        print("\n[✓] Pipeline complete!")


# ── Entry Point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="CSE Quarterly Report Extraction Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--delete",       metavar="TICKER",  help="Delete all data for a ticker")
    parser.add_argument("--status",       action="store_true", help="Show pipeline progress")
    parser.add_argument("--retry-failed", action="store_true", help="Reset failed reports to queued")
    parser.add_argument("--limit",        type=int, default=QUARTERLY_LIMIT,
                        help=f"Quarterly reports per company (default: {QUARTERLY_LIMIT})")
    parser.add_argument("--ticker",        metavar="TICKER",
                        help="Run pipeline for a single ticker only (e.g. HNB.N0000)")
    parser.add_argument("--no-kpi",        action="store_true",
                        help="Skip Gemini KPI extraction step (faster for testing)")
    parser.add_argument("--kpi-only",      action="store_true",
                        help="Re-run Gemini KPI extraction on completed reports that have no KPI data yet")
    parser.add_argument("--purge-all",    action="store_true",
                        help="DANGER: wipe all DB tables and delete /pdfs + /output folders")
    parser.add_argument("--companies",    default="companies.xlsx",
                        help="Path to companies Excel file (default: companies.xlsx)")
    args = parser.parse_args()

    init_database()

    if args.kpi_only:
        companies_file = args.companies
        if args.ticker:
            import tempfile, pandas as pd
            tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
            pd.DataFrame({"Symbol": [args.ticker]}).to_excel(tmp.name, index=False)
            companies_file = tmp.name
        run_kpi_only(companies_file, args.limit)
        return

    if args.purge_all:
        purge_all()
        return

    if args.ticker:
        # Single-ticker run — create a temp one-row companies source
        import tempfile, pandas as pd
        tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        pd.DataFrame({"Symbol": [args.ticker]}).to_excel(tmp.name, index=False)
        run_pipeline(tmp.name, args.limit, skip_kpi=args.no_kpi)  # single ticker
        return

    if args.delete:
        delete_ticker(args.delete)
    elif args.status:
        show_status()
    elif args.retry_failed:
        retry_failed()
    else:
        run_pipeline(args.companies, args.limit, skip_kpi=args.no_kpi)


if __name__ == "__main__":
    main()
