Here are the instructions to give your IDE:

---

**Goal:** Add NVIDIA NIM as an optional AI backend to `kpi_extractor.py`, selectable via a config flag, while keeping Gemini fully intact.

---

**Step 1 — Install dependency**

Add `openai` to requirements if not already there:
```
pip install openai
```

---

**Step 2 — Add to `.env`**
```
NVIDIA_API_KEY=nvapi-xxxxxxxxxxxx
AI_BACKEND=gemini        # switch to "nvidia" to use NIM
NVIDIA_MODEL=deepseek-ai/deepseek-v3
```

---

**Step 3 — At the top of the file, after existing imports and Gemini setup**

Read `AI_BACKEND` from env. If it's `"nvidia"`, initialize an OpenAI client pointed at `https://integrate.api.nvidia.com/v1` with `NVIDIA_API_KEY`. Keep the existing Gemini `_model` initialization inside an `else` block so both are never loaded simultaneously.

---

**Step 4 — Modify `call_gemini()` — rename it to `call_ai()`**

The function logic splits on `AI_BACKEND`:
- If `"nvidia"`: call `client.chat.completions.create()` with `messages=[{"role":"system",...}, {"role":"user",...}]`, `temperature=0`, `max_tokens=2048`. Extract response from `response.choices[0].message.content`
- If `"gemini"`: keep existing `_model.generate_content()` logic exactly as-is

After getting `raw` in either branch, apply the same existing fence-stripping regex, then add this JSON safety extract after it:
```python
match = re.search(r'\{[\s\S]*\}', raw)
if match:
    raw = match.group(0)
```

Then parse and return as before.

---

**Step 5 — Update all callers**

Replace every call to `call_gemini(prompt)` in the file with `call_ai(prompt)`. There are exactly 4 call sites — 2 in `extract_kpis()` and 2 in `extract_kpis_from_db()`.

---

**Step 6 — Update log messages**

Replace `"Sending to Gemini"` and `"Waiting for Gemini response..."` log strings to dynamically use the backend name, e.g. `f"Sending to {AI_BACKEND.upper()}"`.

NVIDIA NIM free tier is 40 requests per minute. Gemini has no such limit so rate limiting should only activate when `AI_BACKEND=nvidia`.