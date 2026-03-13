# GitHub Universe — Data Pipeline

Scrapes public GitHub repositories by country/city and builds static JSON
files that power the 3-level constellation visualization.

## Setup

### 1. Create a GitHub Personal Access Token

1. Go to https://github.com/settings/tokens/new
2. Select **Fine-grained token** (recommended) or Classic
3. Scopes needed: `public_repo`, `read:user`  
   *(no write access needed — read-only is fine)*
4. Copy the token → add it as a repository secret named `GH_SCRAPER_TOKEN`

### 2. Install dependencies

```bash
pip install requests
```

### 3. Run the scraper

```bash
# All 195 countries (takes 2-4 hours with a token)
python scraper.py --token ghp_xxx

# Just a few countries to test
python scraper.py --token ghp_xxx --countries US,DE,PH,SG

# Resume an interrupted run
python scraper.py --token ghp_xxx --resume

# Dry run — just prints the plan
python scraper.py --token ghp_xxx --dry-run
```

### 4. Build the static JSON

```bash
python build.py

# With options
python build.py --top-cities 8 --top-repos 30 --min-stars 5
```

### 5. Serve the visualization

```bash
# Any static file server works
python -m http.server 8080
# then open http://localhost:8080/
```

## Output structure

```
data/
  index.json              ← global summary (all countries, ~200KB)
  countries/
    US.json               ← full data for USA (cities + repos)
    DE.json
    PH.json
    ...                   ← one file per country (~50-200KB each)
```

### index.json shape
```json
{
  "version": 2,
  "builtAt": "2026-03-14T...",
  "totalCountries": 142,
  "totalRepos": 284000,
  "totalStars": 18400000,
  "topLang": "JavaScript",
  "countries": [
    {
      "code": "US",
      "name": "United States",
      "flag": "🇺🇸",
      "continent": "NA",
      "repos": 42000,
      "stars": 3200000,
      "topLang": "JavaScript",
      "cities": ["New York", "San Francisco", "Seattle"]
    }
  ]
}
```

### data/countries/US.json shape
```json
{
  "code": "US",
  "cities": [
    {
      "name": "San Francisco",
      "repoCount": 420,
      "stars": 840000,
      "topLang": "TypeScript",
      "repos": [
        {
          "full_name": "vercel/next.js",
          "stars": 120000,
          "language": "TypeScript",
          "description": "...",
          "url": "https://github.com/vercel/next.js"
        }
      ]
    }
  ]
}
```

## GitHub Actions (auto-update weekly)

The workflow in `.github/workflows/update.yml` runs every Sunday at 02:00 UTC.

**Required secret:** `GH_SCRAPER_TOKEN` — add in  
*Settings → Secrets and variables → Actions → New repository secret*

**Manual trigger:** Go to *Actions → Update GitHub Universe Data → Run workflow*  
You can specify specific countries, adjust city/repo counts, or resume a run.

## Rate limits

| Mode | Requests/hour | Time for all 195 countries |
|------|--------------|---------------------------|
| No token | 60 | ~days (not recommended) |
| Token (Classic) | 5,000 | ~3-4 hours |
| Token (Fine-grained) | 5,000 | ~3-4 hours |

The scraper automatically sleeps when rate limits are hit and resumes.
The `--resume` flag skips already-completed countries if the run is interrupted.

## Updating the visualization

Once `data/index.json` and `data/countries/*.json` exist, update the
visualization to load from static files instead of live API:

```javascript
// In github-universe.html, replace the ghFetch calls with:
const index = await fetch('data/index.json').then(r => r.json());

// Per country (on click):
const country = await fetch(`data/countries/${code}.json`).then(r => r.json());
```
