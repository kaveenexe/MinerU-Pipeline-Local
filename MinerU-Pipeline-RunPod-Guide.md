# MinerU Pipeline on RunPod — Complete Setup & Operations Guide

> **Git commit**: `0bdb7a1` | **Repo**: https://github.com/kaveenexe/MinerU-Pipeline-Local  
> **GPU**: RTX 3090 24GB VRAM | **Purpose**: CSE quarterly PDF extraction → MySQL

---

## Architecture Overview

The pipeline uses **three separate processes** that must all be running simultaneously:

```
pipeline.py (workers)
    └─► mineru-api (task server :8000)
            └─► lmdeploy VLM server (:30000)
                    └─► RTX 3090 GPU
```

| Process | What it does | Port |
|---|---|---|
| `lmdeploy` | Serves MinerU2.5-Pro VLM model for table/figure understanding | 30000 |
| `mineru-api` | Task queue that orchestrates MinerU PDF extraction | 8000 |
| `pipeline.py` | Downloads PDFs, calls mineru-api, stores results in MySQL | — |

---

## Prerequisites

- RunPod pod with **RTX 3090 24GB**
- Pod volume: **≥30 GB** (models + venv + temp files)
- Container disk: **≥25 GB**
- MySQL database (Railway or similar) with credentials ready

---

## One-Time Setup

### 1. Clone the Repository

```bash
cd /workspace
git clone https://github.com/kaveenexe/MinerU-Pipeline-Local.git
cd MinerU-Pipeline-Local
git checkout 0bdb7a1
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

> **Note**: This installs MinerU, lmdeploy, and all pipeline dependencies. Takes ~10–15 minutes.

### 4. Configure Environment

Create `.env` in the project root:

```env
# Database
DB_HOST=your-db-host
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your-password
DB_NAME=railway

# MinerU Settings
VIRTUAL_VRAM_SIZE=6
MINERU_HYBRID_BATCH_RATIO=3
PYTORCH_ALLOC_CONF=max_split_size_mb:256,expandable_segments:True

# Paths
PDF_DIR=/workspace/MinerU-Pipeline-Local/pdfs
OUTPUT_DIR=/workspace/MinerU-Pipeline-Local/output
QUARTERLY_LIMIT=4

# CSE API
PDF_BASE_URL=https://cdn.cse.lk/
CSE_API_BASE=https://www.cse.lk/api/financials
GOOGLE_API_KEY=your-gemini-api-key

# VLM Server — must point to lmdeploy port 30000, NOT mineru-api port 8000
MINERU_VL_SERVER=http://127.0.0.1:30000
```

### 5. Set HuggingFace Cache to Pod Volume

```bash
export HF_HUB_CACHE=/workspace/models
echo 'export HF_HUB_CACHE=/workspace/models' >> ~/.bashrc
```

### 6. Download MinerU VLM Model

```bash
source venv/bin/activate
mineru-models-download
# When prompted: select "vlm"
# Downloads to /workspace/models (~3 GB, takes ~10–15 minutes)
```

Verify:

```bash
ls /workspace/models/
# Should show hub/ directory with MinerU2.5-Pro model
```

### 7. Add venv to PATH Permanently

```bash
echo 'source /workspace/MinerU-Pipeline-Local/venv/bin/activate' >> ~/.bashrc
source ~/.bashrc
```

---

## Every Time You Start the Pod

Open **3 separate terminal tabs** in RunPod's web UI. Each command runs in its own tab.

### Terminal 1 — Start lmdeploy VLM Server

```bash
source /workspace/MinerU-Pipeline-Local/venv/bin/activate

PYTORCH_ALLOC_CONF=expandable_segments:True \
HF_HUB_CACHE=/workspace/models \
nohup lmdeploy serve api_server opendatalab/MinerU2.5-Pro-2604-1.2B \
  --server-port 30000 \
  --backend pytorch \
  --device cuda \
  --cache-max-entry-count 0.35 \
  --max-batch-size 8 \
  --log-level INFO \
  > /workspace/MinerU-Pipeline-Local/lmdeploy.log 2>&1 &

echo "lmdeploy PID: $!"
tail -f /workspace/MinerU-Pipeline-Local/lmdeploy.log
```

**Wait** until you see this before proceeding:
```
Uvicorn running on http://0.0.0.0:30000
```

### Terminal 2 — Start mineru-api Task Server

```bash
source /workspace/MinerU-Pipeline-Local/venv/bin/activate

nohup env MINERU_VL_SERVER=http://127.0.0.1:30000 \
  mineru-api --host 127.0.0.1 --port 8000 --enable-vlm-preload false \
  > /workspace/MinerU-Pipeline-Local/mineru-api.log 2>&1 &

echo "mineru-api PID: $!"
tail -f /workspace/MinerU-Pipeline-Local/mineru-api.log
```

**Wait** until you see:
```
Start MinerU FastAPI Service: http://127.0.0.1:8000
```

### Terminal 3 — Run the Pipeline

```bash
source /workspace/MinerU-Pipeline-Local/venv/bin/activate
cd /workspace/MinerU-Pipeline-Local

nohup python -u pipeline.py --workers 2 --no-kpi \
  >> pipeline.log 2>&1 &

echo "Pipeline PID: $!"
tail -f pipeline.log
```

> `>>` appends to the log (preserves history). `-u` forces unbuffered output so logs appear in real time.

---

## Pipeline Commands Reference

### Check Progress

```bash
cd /workspace/MinerU-Pipeline-Local && source venv/bin/activate
python pipeline.py --status
```

### Run Full Pipeline (All Companies)

```bash
nohup python -u pipeline.py --workers 2 --no-kpi >> pipeline.log 2>&1 &
```

### Run for Specific Tickers (Testing / Partial)

```bash
# Single ticker
python pipeline.py --ticker HNB.N0000 --workers 1 --no-kpi

# Multiple tickers with 2 workers
python pipeline.py --tickers HNB.N0000 COMB.N0000 --workers 2 --no-kpi
```

### Retry Failed Reports

```bash
python pipeline.py --retry-failed
nohup python -u pipeline.py --workers 2 --no-kpi >> pipeline.log 2>&1 &
```

### Change Quarterly Limit

```bash
# Extract 8 reports per company instead of default 4
python pipeline.py --workers 2 --no-kpi --limit 8
```

### Run KPI Extraction Only

```bash
# Re-run Gemini KPI extraction on already-extracted reports
python pipeline.py --kpi-only
```

### Delete All Data for a Ticker

```bash
python pipeline.py --delete HNB.N0000
```

### Nuclear Reset (Danger — wipes everything)

```bash
python pipeline.py --purge-all
```

---

## Monitoring

### Watch Live Logs

```bash
tail -f /workspace/MinerU-Pipeline-Local/pipeline.log
tail -f /workspace/MinerU-Pipeline-Local/lmdeploy.log
tail -f /workspace/MinerU-Pipeline-Local/mineru-api.log
```

### Check All 3 Processes Are Running

```bash
ps aux | grep -E "lmdeploy|mineru-api|pipeline" | grep -v grep
```

### Monitor GPU Memory

```bash
watch -n 2 nvidia-smi
```

### Check Disk Space

```bash
df -h /workspace
du -sh /workspace/models /workspace/MinerU-Pipeline-Local/venv
```

---

## Recovery After Crash or Internet Drop

All processes use `nohup` — they survive tab closes and disconnects. If the pod itself restarts, all three processes need to be restarted.

### Recovery Steps

```bash
# 1. Check what state the pipeline is in
cd /workspace/MinerU-Pipeline-Local && source venv/bin/activate
python pipeline.py --status

# 2. Reset any failed or stuck reports
python pipeline.py --retry-failed

# 3. Restart Terminal 1 (lmdeploy) — wait for port 30000 ready
# 4. Restart Terminal 2 (mineru-api) — wait for port 8000 ready

# 5. Resume pipeline (appends to existing log)
nohup python -u pipeline.py --workers 2 --no-kpi >> pipeline.log 2>&1 &
tail -f pipeline.log
```

---

## Performance Settings

### Optimal Settings for RTX 3090

| Setting | Value | Reason |
|---|---|---|
| `--workers 2` | 2 parallel workers | Processes 2 tickers simultaneously |
| `VIRTUAL_VRAM_SIZE=6` | 6 GB per worker | Safe headroom alongside lmdeploy |
| `MINERU_HYBRID_BATCH_RATIO=3` | Batch multiplier | Matches 6 GB VRAM allocation |
| `--cache-max-entry-count 0.35` | lmdeploy KV cache | ~8.5 GB KV cache, ~11 GB total |
| `--max-batch-size 8` | lmdeploy batching | Handles 2 concurrent workers |

### GPU Memory Budget

```
lmdeploy model weights (1.2B):   ~2.5 GB
lmdeploy KV cache (0.35 × 24):   ~8.5 GB
Worker 1 pipeline models:        ~4–5 GB
Worker 2 pipeline models:        ~4–5 GB
─────────────────────────────────────────
Total:                           ~19–21 GB  ✓ within 24 GB
```

### Expected Throughput

- ~2–3 minutes per PDF
- ~4 PDFs per ticker (default quarterly limit)
- ~9–10 minutes per 2-ticker batch with 2 workers
- 287 companies × 4 reports = ~1,148 PDFs total → **~22 hours full run**

---

## Common Errors & Fixes

### `[Errno 2] No such file or directory: 'mineru'`

venv not in PATH of forked subprocess. Fix in `pipeline.py` at module level:

```python
import shutil
MINERU_BIN = (
    shutil.which("mineru")
    or "/workspace/MinerU-Pipeline-Local/venv/bin/mineru"
)
```

Use `MINERU_BIN` instead of `"mineru"` in `subprocess.run(...)`.

### `Failed to get model name from http://127.0.0.1:8000`

`.env` has wrong `MINERU_VL_SERVER`. Change to port **30000**:

```env
MINERU_VL_SERVER=http://127.0.0.1:30000
```

Then restart mineru-api.

### `Python type CMySQLConnection cannot be converted`

MySQL C extension is not thread-safe inside forked workers. Fix in `get_conn()`:

```python
def get_conn(with_db=True):
    cfg = DB_CONFIG.copy()
    if with_db:
        cfg["database"] = DB_NAME
    cfg["use_pure"] = True   # forces pure Python connector — thread-safe
    return mysql.connector.connect(**cfg)
```

### CUDA Out of Memory / Fragmentation Error

Add to lmdeploy startup command:
```bash
PYTORCH_ALLOC_CONF=expandable_segments:True
```
And reduce lmdeploy cache: `--cache-max-entry-count 0.30`

### `No module named 'mineru.api'`

This import path does not exist in MinerU 3.1.11. Always use the subprocess approach with `hybrid-http-client` backend in `run_mineru()`. Do not import `mineru.api` directly.

### `lmdeploy: error: unrecognized arguments: --port`

lmdeploy 0.11.x uses `--server-port`, not `--port`. Always start lmdeploy directly — never through `mineru-openai-server` which passes the wrong flag.

### Pipeline Stuck on `[2/3] Running MinerU...` for Hours

A large/complex PDF caused lmdeploy to hang indefinitely. Add a timeout to `run_mineru()`:

```python
try:
    result = subprocess.run(
        [MINERU_BIN, "-p", str(pdf_path), "-o", str(out_dir),
         "-b", "hybrid-http-client", "--device", "cuda"],
        env=env,
        capture_output=True,
        text=True,
        timeout=600   # kill after 10 minutes
    )
except subprocess.TimeoutExpired:
    raise RuntimeError("MinerU timed out after 10 minutes — PDF may be too large")
```

---

## Disk Space Management

### Space Requirements

| Item | Location | Size |
|---|---|---|
| MinerU VLM model | `/workspace/models/` | ~3 GB |
| MinerU pipeline models (auto-downloaded) | `/workspace/models/` | ~8–10 GB |
| Python venv | `venv/` | ~5–7 GB |
| Active PDFs (temporary) | `pdfs/` | ~1–2 GB |
| **Total needed** | `/workspace` | **~20–22 GB** |

**Recommended: 30 GB pod volume minimum.**

### Free Up Space (Run While Pipeline Is Idle)

```bash
rm -rf /workspace/MinerU-Pipeline-Local/pdfs/*
rm -rf /workspace/MinerU-Pipeline-Local/output/*
pip cache purge
find /workspace/models -name "*.incomplete" -delete
find /workspace/models -name "tmp*" -delete
```

### Expand Pod Volume

RunPod allows expanding volume size while the pod is running — no reinstall needed. Go to RunPod dashboard → your pod → expand volume.

---

## Cost Reference

| Resource | Rate | Full 22-hour run |
|---|---|---|
| RTX 3090 Community Cloud | ~$0.22/hr | ~$4.84 |
| Pod volume 30 GB | $0.07/GB/month | ~$0.07 |
| **Total** | | **~$5** |

Stop the pod when not running — container disk costs more when stopped.

---

## File Structure

```
/workspace/
├── models/                                    ← HuggingFace cache (persistent, do not delete)
│   └── hub/
│       └── models--opendatalab--MinerU2.5-Pro-2604-1.2B/
└── MinerU-Pipeline-Local/
    ├── .env                                   ← All configuration (never commit)
    ├── pipeline.py                            ← Main pipeline script
    ├── kpi_extractor.py                       ← Gemini KPI extraction
    ├── companies.xlsx                         ← CSE company list
    ├── requirements.txt                       ← Python dependencies
    ├── venv/                                  ← Python virtual environment
    ├── pdfs/                                  ← Downloaded PDFs (temp, auto-cleaned)
    ├── output/                                ← MinerU output (temp, auto-cleaned)
    ├── pipeline.log                           ← Pipeline run log
    ├── mineru-api.log                         ← mineru-api server log
    └── lmdeploy.log                           ← lmdeploy server log
```
