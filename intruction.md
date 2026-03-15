# GitHub Universe Scraping Instructions

This file is a practical runbook for scraping and rebuilding data.

---

## 1) Command order (recommended)

1. **Run scraper (normal/hybrid first)**
2. **Run scraper deep (optional, for more coverage)**
3. **Run build** to regenerate `data/index.json` and `data/countries/*.json`
4. **Serve locally** to preview UI

---

## 2) Core commands + purpose

### A) Normal scrape (seed + search, safe append)
Purpose:
- Adds new users/repositories
- Uses seed files when available (`--seed-dir data/seeds`)
- Keeps existing data and deduplicates by login

```bash
source .env && python3 scraper.py --token "$GH_TOKEN" --countries PH,JP --seed-dir data/seeds
```

---

### B) Deep scrape (more coverage, slower)
Purpose:
- Expands data further using follower × year buckets
- Best for maximizing data per country

```bash
source .env && python3 scraper.py --token "$GH_TOKEN" --countries PH,JP --seed-dir data/seeds --deep
```

---

### C) Add workers for speed
Purpose:
- Parallel country processing
- Recommended range: `--workers 2` (or `3` max)

```bash
source .env && python3 scraper.py --token "$GH_TOKEN" --countries PH,JP --seed-dir data/seeds --deep --workers 2
```

---

### D) Resume mode
Purpose:
- Skip countries marked done in `data/raw/_progress.json`
- Useful after interrupted runs

```bash
source .env && python3 scraper.py --token "$GH_TOKEN" --countries PH,JP --seed-dir data/seeds --deep --resume
```

Note:
- If a country is already marked done, `--resume` will skip it.
- To force re-run that country, run without `--resume`.

---

### E) Seed-only mode (optional)
Purpose:
- Enrich only seed logins
- Skip search buckets

```bash
source .env && python3 scraper.py --token "$GH_TOKEN" --countries PH,JP --seed-dir data/seeds --seed-only
```

---

### F) Build output JSON for UI
Purpose:
- Regenerates:
  - `data/index.json`
  - `data/countries/PH.json`
  - `data/countries/JP.json`

```bash
python3 build.py
```

---

### G) Run locally
Purpose:
- Preview app in browser

```bash
python3 -m http.server 8081
```

Open:
- http://127.0.0.1:8081

---

## 3) Practical flows

### Flow 1: New country data (PH/JP not yet scraped)
```bash
source .env && python3 scraper.py --token "$GH_TOKEN" --countries PH,JP --seed-dir data/seeds --workers 2
source .env && python3 scraper.py --token "$GH_TOKEN" --countries PH,JP --seed-dir data/seeds --deep --workers 2
python3 build.py
python3 -m http.server 8081
```

### Flow 2: Existing data, grow further
```bash
source .env && python3 scraper.py --token "$GH_TOKEN" --countries PH,JP --seed-dir data/seeds --deep --workers 2
python3 build.py
```

### Flow 3: Interrupted run, continue
```bash
source .env && python3 scraper.py --token "$GH_TOKEN" --countries PH,JP --seed-dir data/seeds --deep --workers 2 --resume
python3 build.py
```

---

## 4) Country list

- Full supported country list is in [countries_list.md](countries_list.md).
- Use any country codes from that file in `--countries` (comma-separated).

---

## 5) Notes

- `--deep` increases data but takes much longer.
- `--workers 2` is usually best balance for speed/stability.
- If using multiple terminals/tokens, do not overlap the same country code at the same time.
- Run `build.py` after scraper jobs finish.
