# CSE Pipeline Database Schema

This document describes the SQL database structure for the CSE (Colombo Stock Exchange) Quarterly Report Pipeline. The database stores metadata, extracted content, and processed financial KPIs from company reports.

## Database Overview
The database uses **MySQL** (InnoDB engine) with `utf8mb4` encoding to support a wide range of characters.

---

## 1. `companies`
Stores the master list of companies whose reports are being processed.

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `INT` | Primary Key, Auto-incremented. |
| `name` | `VARCHAR(255)` | The full name of the company (e.g., John Keells Holdings PLC). But all the values are null in the database |
| `symbol` | `VARCHAR(50)` | Unique stock ticker symbol (e.g., `JKH.N0000`). |
| `created_at` | `TIMESTAMP` | Record creation timestamp. |

---

## 2. `reports`
Stores metadata and the high-level extracted content for each specific quarterly report.

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `INT` | Primary Key, Auto-incremented. |
| `company_symbol` | `VARCHAR(50)` | Foreign link to `companies.symbol`. |
| `cse_report_id` | `INT` | The unique ID assigned by the CSE API. |
| `file_text` | `VARCHAR(500)` | Descriptive text for the file (often contains the quarter/year). There is no specific format in this. Many reports use different formats* |
| `manual_date` | `BIGINT` | Unix timestamp of the report's manual date. |
| `uploaded_date` | `BIGINT` | Unix timestamp of when the report was uploaded to CSE. |
| `pdf_url` | `VARCHAR(500)` | Source URL of the PDF on the CSE CDN. |
| `pdf_local_path` | `VARCHAR(500)` | Path to the downloaded PDF on the local file system. |
| `md_content` | `LONGTEXT` | The full extracted content of the report in Markdown format. |
| `created_at` | `TIMESTAMP` | Record creation timestamp. |
| `updated_at` | `TIMESTAMP` | Last update timestamp (auto-updates). |

**Constraints:**
- Unique Key: `(company_symbol, cse_report_id)` ensures no duplicate reports per company.

**NOTE:**
*Many reports use different formats for the file_text. As examples:
- 'Interim Financial Statements for the Quarter ended 31st December 2025'
- 'Interim financial statements 30th June 2025'
- 'Interim Financial Statements for the six months ended June 30,2025'
- 'QUARTERLY FINANCIAL STATEMENT AS OF 30TH JUNE 2025'
- 'INTERIM FINANCIAL STATEMENTS FOR THE QUARTER ENDED 30TH JUNE 2025'
- and more.
---

## 3. `report_blocks`
Stores the granular elements (text, tables, images) extracted from each report by MinerU.

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `INT` | Primary Key, Auto-incremented. |
| `report_db_id` | `INT` | Foreign Key referencing `reports.id`. |
| `block_index` | `INT` | The sequence index of the block within the document. |
| `block_type` | `VARCHAR(50)` | Type of block (e.g., `title`, `paragraph`, `table`, `image`, `page-number`). |
| `text_level` | `INT` | Hierarchy level for headings/titles. (all are null in this case)|
| `content_text` | `LONGTEXT` | Plain text content or image caption. |
| `content_html` | `LONGTEXT` | HTML representation of the block (primarily for tables). |
| `img_path` | `VARCHAR(500)` | Local path to any extracted images. |
| `table_type` | `VARCHAR(50)` | MinerU's classification (e.g., `simple_table`, `complex_table`). |
| `table_source` | `VARCHAR(50)` | Source classification (internal use). (all null)|
| `page_number` | `INT` | The page number in the original PDF where the block was found. |
| `created_at` | `TIMESTAMP` | Record creation timestamp. |

**Indexes:**
- `idx_rid`: Optimized for filtering blocks by report.
- `idx_type`: Optimized for filtering by block type (e.g., fetching all tables).
- `idx_page`: Optimized for page-based retrieval.

---

## 4. `pipeline_state`
Tracks the progress and status of each report as it moves through the extraction pipeline.

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `INT` | Primary Key, Auto-incremented. |
| `company_symbol` | `VARCHAR(50)` | Stock ticker symbol. |
| `cse_report_id` | `INT` | CSE's unique report ID. |
| `file_text` | `VARCHAR(500)` | Report description. |
| `pdf_url` | `VARCHAR(500)` | PDF source URL. |
| `stage` | `ENUM` | Current pipeline stage: `queued`, `pdf_downloaded`, `mineru_extracted`, `completed`, `failed`, `deleted`. |
| `error_message` | `TEXT` | Stores the stack trace or error message if a stage fails. |
| `attempts` | `INT` | Number of times the pipeline has attempted to process this report. |
| `created_at` | `TIMESTAMP` | Record creation timestamp. |
| `updated_at` | `TIMESTAMP` | Last status update timestamp. |

**Constraints:**
- Unique Key: `(company_symbol, cse_report_id)` tracking state per report.

---

## 5. `financial_kpis`
Stores the structured financial metrics extracted from reports using Gemini AI.

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `INT` | Primary Key, Auto-incremented. |
| `company_symbol` | `VARCHAR(50)` | Stock ticker symbol. |
| `report_db_id` | `INT` | Foreign Key referencing `reports.id`. |
| `period` | `VARCHAR(50)` | The financial period (Same as `reports.cse_report_id`). |
| `period_type` | `ENUM` | Classification: `quarterly` or `annual`. (In this case all are in quarterly and annual will add in future) |
| `currency_unit` | `VARCHAR(30)` | Currency and unit (e.g., `LKR`, `Rs. 000`). |
| `metric` | `VARCHAR(100)` | Name of the KPI (e.g., `revenue`, `profit_after_tax`). |
| `value` | `DECIMAL(24,4)` | The numeric value extracted. Supports large numbers. |
| `is_custom` | `TINYINT(1)` | `0` for Tier 1 (standard-defined) KPIs, `1` for Tier 2 (custom/AI-detected) KPIs. |
| `extracted_at` | `TIMESTAMP` | Timestamp of when the KPI was extracted. |

**Tier 1 Standard Metrics:**
- `revenue`, `gross_profit`, `profit_before_tax`, `profit_after_tax`, `eps_basic`, `total_assets`, `total_equity`, `operating_cashflow`.

**Indexes:**
- `idx_symbol_period`: Optimized for company performance analysis over time.
- `idx_metric`: Optimized for benchmarking a specific metric across companies.
- Unique Key: `uq_kpi (company_symbol, period, metric)` prevents duplicate KPI records for the same period.
