# CSE Quarterly Report Pipeline

## Setup

1. Copy both files into `D:\MyProjects\Scripts\mineru\`
2. Edit `.env` — set your MySQL password if different from `root`
3. Place `companies.xlsx` in the same folder
4. Install dependencies:
   ```
   uv pip install -r requirements.txt
   ```

## Usage

```powershell
# Run the full pipeline (all 287 companies, 4 quarters each)
python pipeline.py

# Check progress anytime
python pipeline.py --status

# Delete a ticker (removes DB rows + PDFs + MinerU output)
python pipeline.py --delete ABAN.N0000

# Retry any failed reports
python pipeline.py --retry-failed

# Test with just 1 company first (edit companies.xlsx to 1 row)
python pipeline.py --limit 1

# Extract more past quarters (e.g. 8 instead of 4)
python pipeline.py --limit 8
```

## How Resume Works

Each report goes through these stages tracked in `pipeline_state` table:
```
queued → pdf_downloaded → mineru_extracted → completed
                                           ↘ failed (retryable)
```
If you Ctrl+C or the script crashes, restart it — it skips `completed`
and resumes from the exact stage each report was at.

## Folder Structure

```
mineru/
├── pipeline.py
├── .env
├── companies.xlsx
├── requirements.txt
├── pdfs/
│   ├── ABAN.N0000/
│   │   ├── 49958_731_1769508614541.pdf
│   │   └── ...
│   └── JKH.N0000/
│       └── ...
└── output/
    ├── 49958_731_1769508614541/
    │   ├── auto/
    │   │   ├── 49958_731_1769508614541.md
    │   │   ├── content_list.json
    │   │   └── images/
    └── ...
```

## MySQL Tables

- `companies`      — ticker + name
- `reports`        — one row per PDF, includes full `.md` content
- `report_blocks`  — each MinerU block (text/table/image) as a row
- `pipeline_state` — tracks extraction stage for resume/retry

## Migrate to CeylonStreet Later

```sql
-- Export from mineru_cse, import into ceylonstreet DB
-- The schema is designed to be portable — just migrate the 4 tables
```
