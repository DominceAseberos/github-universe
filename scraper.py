#!/usr/bin/env python3
"""
GitHub Universe Scraper
=======================
Scrapes public GitHub users by country, collects their repos,
and saves raw data as JSON files ready for build.py to aggregate.

Usage:
    python scraper.py --token ghp_xxx
    python scraper.py --token ghp_xxx --countries US,DE,PH
    python scraper.py --token ghp_xxx --resume          # continue interrupted run
    python scraper.py --token ghp_xxx --workers 3       # parallel countries

Output:
    data/raw/{COUNTRY_CODE}.jsonl   — one JSON object per line (user + repos)
    data/raw/_progress.json         — tracks which countries are done
    data/raw/_meta.json             — run stats
"""

import os
import sys
import json
import time
import argparse
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
RAW_DIR      = Path("data/raw")
PROGRESS_FILE = RAW_DIR / "_progress.json"
META_FILE     = RAW_DIR / "_meta.json"

# GitHub search returns max 1000 results per query (10 pages × 100)
MAX_USERS_PER_COUNTRY = 1000
USERS_PER_PAGE        = 100
REPOS_PER_USER        = 10    # top repos per user (sorted by stars)
REPO_PAGES            = 1     # pages of repos per user

# Rate limit safety — stop fetching if remaining drops below this
RATE_LIMIT_BUFFER = 50

# All 195 countries with their search-friendly names and ISO codes
COUNTRIES = [
    ("US", "United States"), ("CN", "China"), ("IN", "India"),
    ("DE", "Germany"), ("GB", "United Kingdom"), ("BR", "Brazil"),
    ("FR", "France"), ("CA", "Canada"), ("RU", "Russia"), ("JP", "Japan"),
    ("KR", "South Korea"), ("AU", "Australia"), ("NL", "Netherlands"),
    ("PL", "Poland"), ("ES", "Spain"), ("IT", "Italy"), ("SE", "Sweden"),
    ("CH", "Switzerland"), ("UA", "Ukraine"), ("AR", "Argentina"),
    ("ID", "Indonesia"), ("TR", "Turkey"), ("PT", "Portugal"),
    ("PK", "Pakistan"), ("MX", "Mexico"), ("CZ", "Czech Republic"),
    ("BE", "Belgium"), ("DK", "Denmark"), ("SG", "Singapore"),
    ("FI", "Finland"), ("NO", "Norway"), ("AT", "Austria"),
    ("IL", "Israel"), ("TW", "Taiwan"), ("NG", "Nigeria"),
    ("RO", "Romania"), ("GR", "Greece"), ("HU", "Hungary"),
    ("EG", "Egypt"), ("VN", "Vietnam"), ("IR", "Iran"),
    ("CO", "Colombia"), ("ZA", "South Africa"), ("PH", "Philippines"),
    ("TH", "Thailand"), ("NZ", "New Zealand"), ("MY", "Malaysia"),
    ("CL", "Chile"), ("BD", "Bangladesh"), ("MA", "Morocco"),
    ("PE", "Peru"), ("BG", "Bulgaria"), ("HR", "Croatia"),
    ("SK", "Slovakia"), ("RS", "Serbia"), ("KE", "Kenya"),
    ("BY", "Belarus"), ("LT", "Lithuania"), ("EE", "Estonia"),
    ("DZ", "Algeria"), ("IE", "Ireland"), ("LK", "Sri Lanka"),
    ("TN", "Tunisia"), ("JO", "Jordan"), ("LB", "Lebanon"),
    ("GE", "Georgia"), ("GH", "Ghana"), ("KZ", "Kazakhstan"),
    ("EC", "Ecuador"), ("BO", "Bolivia"), ("VE", "Venezuela"),
    ("UY", "Uruguay"), ("NP", "Nepal"), ("LV", "Latvia"),
    ("SI", "Slovenia"), ("LU", "Luxembourg"), ("CR", "Costa Rica"),
    ("CM", "Cameroon"), ("ET", "Ethiopia"), ("AE", "UAE"),
    ("SA", "Saudi Arabia"), ("KW", "Kuwait"), ("CY", "Cyprus"),
    ("PA", "Panama"), ("DO", "Dominican Republic"), ("GT", "Guatemala"),
    ("HN", "Honduras"), ("SV", "El Salvador"), ("NI", "Nicaragua"),
    ("PY", "Paraguay"), ("UG", "Uganda"), ("ZM", "Zambia"),
    ("ZW", "Zimbabwe"), ("AM", "Armenia"), ("AZ", "Azerbaijan"),
    ("MD", "Moldova"), ("BA", "Bosnia"), ("MK", "North Macedonia"),
    ("AL", "Albania"), ("ME", "Montenegro"), ("IS", "Iceland"),
    ("MT", "Malta"), ("BH", "Bahrain"), ("QA", "Qatar"),
    ("OM", "Oman"), ("BN", "Brunei"), ("KH", "Cambodia"),
    ("MN", "Mongolia"), ("PS", "Palestine"), ("MM", "Myanmar"),
    ("LA", "Laos"), ("KG", "Kyrgyzstan"), ("TJ", "Tajikistan"),
    ("AF", "Afghanistan"), ("LR", "Liberia"), ("SL", "Sierra Leone"),
    ("GN", "Guinea"), ("ML", "Mali"), ("NE", "Niger"),
    ("TD", "Chad"), ("BJ", "Benin"), ("TG", "Togo"),
    ("BF", "Burkina Faso"), ("MR", "Mauritania"), ("BI", "Burundi"),
    ("BW", "Botswana"), ("NA", "Namibia"), ("GA", "Gabon"),
    ("MV", "Maldives"), ("FJ", "Fiji"), ("PG", "Papua New Guinea"),
    ("WS", "Samoa"), ("VU", "Vanuatu"), ("TO", "Tonga"),
    ("JM", "Jamaica"), ("TT", "Trinidad"), ("BB", "Barbados"),
    ("AD", "Andorra"), ("LI", "Liechtenstein"), ("MC", "Monaco"),
    ("SM", "San Marino"), ("XK", "Kosovo"), ("BZ", "Belize"),
    ("SR", "Suriname"), ("GY", "Guyana"), ("LY", "Libya"),
    ("SD", "Sudan"), ("SO", "Somalia"), ("YE", "Yemen"),
    ("IQ", "Iraq"), ("SY", "Syria"), ("CU", "Cuba"),
    ("HT", "Haiti"), ("RW", "Rwanda"), ("AO", "Angola"),
    ("MZ", "Mozambique"), ("MG", "Madagascar"), ("TZ", "Tanzania"),
    ("CI", "Ivory Coast"), ("SN", "Senegal"), ("KE", "Kenya"),
]
# dedupe by code
seen = set()
COUNTRIES = [(c, n) for c, n in COUNTRIES if c not in seen and not seen.add(c)]

LANG_COLORS = {
    "JavaScript": "#f0c040", "TypeScript": "#4a9ff5", "Python": "#4ec94e",
    "Java": "#b07219", "C++": "#b06be0", "C#": "#178600", "C": "#555555",
    "PHP": "#4f5d95", "Ruby": "#ff5370", "Go": "#26c6da", "Rust": "#ff6b3d",
    "Swift": "#ff9f43", "Kotlin": "#9c6af7", "Dart": "#40c4ff",
    "Shell": "#89e051", "HTML": "#e34c26", "CSS": "#563d7c", "Vue": "#42b883",
    "R": "#198ce7", "Scala": "#c22d40", "Haskell": "#5d4f85",
}

# ── GitHub API Client ─────────────────────────────────────────────────────────
class GitHubClient:
    BASE = "https://api.github.com"

    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        self._lock = threading.Lock()
        self.rate_remaining  = 5000
        self.rate_reset_at   = 0
        self.search_remaining = 30
        self.search_reset_at  = 0
        self.total_requests  = 0

    def _update_rate(self, resp: requests.Response):
        with self._lock:
            self.rate_remaining  = int(resp.headers.get("X-RateLimit-Remaining", self.rate_remaining))
            self.rate_reset_at   = int(resp.headers.get("X-RateLimit-Reset", self.rate_reset_at))
            self.total_requests += 1
            if "search" in resp.url:
                self.search_remaining = int(resp.headers.get("X-RateLimit-Remaining", self.search_remaining))
                self.search_reset_at  = int(resp.headers.get("X-RateLimit-Reset", self.search_reset_at))

    def _wait_for_rate(self, is_search=False):
        remaining = self.search_remaining if is_search else self.rate_remaining
        reset_at  = self.search_reset_at  if is_search else self.rate_reset_at
        if remaining <= RATE_LIMIT_BUFFER:
            wait = max(0, reset_at - time.time()) + 2
            log.warning(f"Rate limit low ({remaining} left) — sleeping {wait:.0f}s")
            time.sleep(wait)

    def get(self, path: str, params: dict = None, retries: int = 5) -> dict | list | None:
        url = self.BASE + path
        is_search = "/search/" in path
        for attempt in range(retries):
            self._wait_for_rate(is_search)
            try:
                resp = self.session.get(url, params=params, timeout=20)
                self._update_rate(resp)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code in (403, 429):
                    retry_after = int(resp.headers.get("Retry-After", "61"))
                    reset_at = int(resp.headers.get("X-RateLimit-Reset", time.time() + 61))
                    wait = max(retry_after, reset_at - time.time()) + 2
                    log.warning(f"Rate limited — sleeping {wait:.0f}s (attempt {attempt+1}/{retries})")
                    time.sleep(min(wait, 120))
                    continue

                if resp.status_code == 422:
                    # Search result window exhausted (>1000 results)
                    return None

                if resp.status_code in (404, 451):
                    return None

                if resp.status_code >= 500:
                    wait = 10 * (attempt + 1)
                    log.warning(f"Server error {resp.status_code} — retry in {wait}s")
                    time.sleep(wait)
                    continue

                log.error(f"Unexpected {resp.status_code} for {url}")
                return None

            except requests.RequestException as e:
                wait = 10 * (attempt + 1)
                log.warning(f"Request error: {e} — retry in {wait}s")
                time.sleep(wait)

        log.error(f"Failed after {retries} retries: {url}")
        return None


# ── Progress tracking ─────────────────────────────────────────────────────────
def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            pass
    return {"done": [], "started": str(datetime.now(timezone.utc))}

def save_progress(progress: dict):
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))

def load_meta() -> dict:
    if META_FILE.exists():
        try:
            return json.loads(META_FILE.read_text())
        except Exception:
            pass
    return {"total_users": 0, "total_repos": 0, "countries_done": 0, "started": str(datetime.now(timezone.utc))}

def save_meta(meta: dict):
    META_FILE.write_text(json.dumps(meta, indent=2))


# ── City extraction ───────────────────────────────────────────────────────────
# Common noise words to strip from location strings
NOISE = {
    "remote", "worldwide", "global", "earth", "world", "internet",
    "everywhere", "anywhere", "nomad", "online", "home", "the", "and",
}

def extract_city(location: str | None) -> str | None:
    """
    Try to extract a city name from a free-text location string.
    e.g. "Berlin, Germany" -> "Berlin"
         "San Francisco, CA, USA" -> "San Francisco"
         "Germany" -> None  (country-only, skip)
    """
    if not location:
        return None
    loc = location.strip()
    if not loc or len(loc) < 2:
        return None

    # Split on comma — first part is usually city
    parts = [p.strip() for p in loc.split(",")]
    candidate = parts[0]

    # Strip leading @, #, emoji, numbers
    import re
    candidate = re.sub(r"^[@#\d\s]+", "", candidate).strip()
    candidate = re.sub(r"[^\w\s\-\.]", "", candidate).strip()

    words = candidate.lower().split()
    if not words:
        return None

    # Skip if all noise words
    if all(w in NOISE for w in words):
        return None

    # Skip if looks like a country (single word, starts with capital, > 4 chars)
    # This is heuristic — we'd over-skip but that's fine
    if len(words) == 1 and len(candidate) > 3:
        # Check against country names
        country_names = {n.lower() for _, n in COUNTRIES}
        if candidate.lower() in country_names:
            return None

    # Title-case the result
    return " ".join(w.capitalize() for w in candidate.split())


# ── Per-country scraper ───────────────────────────────────────────────────────
def scrape_country(client: GitHubClient, code: str, name: str) -> dict:
    """
    Fetch up to MAX_USERS_PER_COUNTRY users from this country,
    get their repos, extract city from location.
    Returns a summary dict and writes raw JSONL file.
    """
    out_file = RAW_DIR / f"{code}.jsonl"
    lines_written = 0
    city_counts: dict[str, int] = {}
    lang_counts: dict[str, int] = {}
    total_stars = 0
    total_repos_collected = 0

    log.info(f"[{code}] Starting — {name}")

    with open(out_file, "w", encoding="utf-8") as fh:
        for page in range(1, (MAX_USERS_PER_COUNTRY // USERS_PER_PAGE) + 1):
            result = client.get("/search/users", params={
                "q": f"location:{name} type:user",
                "per_page": USERS_PER_PAGE,
                "page": page,
                "sort": "repositories",
                "order": "desc",
            })
            if not result:
                break

            items = result.get("items", [])
            if not items:
                break

            for user in items:
                login = user["login"]

                # Get full user profile for location
                profile = client.get(f"/users/{login}") or {}
                location = profile.get("location") or user.get("location")
                city = extract_city(location)

                # Get user's top repos
                repos_raw = client.get(f"/users/{login}/repos", params={
                    "sort": "stars",
                    "per_page": REPOS_PER_USER,
                    "type": "owner",
                }) or []

                repos = []
                for r in repos_raw:
                    if r.get("fork"):
                        continue  # skip forks
                    lang = r.get("language") or "Unknown"
                    stars = r.get("stargazers_count", 0)
                    repos.append({
                        "id":          r["id"],
                        "name":        r["name"],
                        "full_name":   r["full_name"],
                        "description": (r.get("description") or "")[:120],
                        "language":    lang,
                        "stars":       stars,
                        "forks":       r.get("forks_count", 0),
                        "topics":      r.get("topics", [])[:5],
                        "url":         r.get("html_url", ""),
                        "pushed_at":   r.get("pushed_at", ""),
                    })
                    lang_counts[lang] = lang_counts.get(lang, 0) + 1
                    total_stars += stars

                if city:
                    city_counts[city] = city_counts.get(city, 0) + 1

                total_repos_collected += len(repos)

                record = {
                    "login":    login,
                    "location": location,
                    "city":     city,
                    "country":  code,
                    "repos":    repos,
                }
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                lines_written += 1

            log.info(
                f"[{code}] page {page}/{MAX_USERS_PER_COUNTRY//USERS_PER_PAGE} "
                f"— {lines_written} users, {total_repos_collected} repos "
                f"— rate: {client.rate_remaining} left"
            )

            if len(items) < USERS_PER_PAGE:
                break  # last page

            # Small polite delay between pages
            time.sleep(0.3)

    summary = {
        "code":       code,
        "name":       name,
        "users":      lines_written,
        "repos":      total_repos_collected,
        "stars":      total_stars,
        "top_cities": sorted(city_counts.items(), key=lambda x: -x[1])[:20],
        "top_langs":  sorted(lang_counts.items(), key=lambda x: -x[1])[:15],
        "scraped_at": str(datetime.now(timezone.utc)),
    }
    log.info(f"[{code}] Done — {lines_written} users, {total_repos_collected} repos, top city: {summary['top_cities'][:1]}")
    return summary


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="GitHub Universe Scraper")
    parser.add_argument("--token",    required=True,  help="GitHub Personal Access Token")
    parser.add_argument("--countries",default="",     help="Comma-separated ISO codes to scrape (default: all)")
    parser.add_argument("--resume",   action="store_true", help="Skip already-completed countries")
    parser.add_argument("--workers",  type=int, default=1, help="Parallel workers (keep ≤3 to avoid rate limits)")
    parser.add_argument("--dry-run",  action="store_true", help="Print plan without scraping")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    progress = load_progress() if args.resume else {"done": [], "started": str(datetime.now(timezone.utc))}
    meta     = load_meta()

    # Filter countries
    target = COUNTRIES
    if args.countries:
        codes = {c.strip().upper() for c in args.countries.split(",")}
        target = [(c, n) for c, n in COUNTRIES if c in codes]
        if not target:
            log.error(f"No matching countries for: {args.countries}")
            sys.exit(1)

    if args.resume:
        done_set = set(progress.get("done", []))
        skipped  = [c for c, _ in target if c in done_set]
        target   = [(c, n) for c, n in target if c not in done_set]
        log.info(f"Resuming — skipping {len(skipped)} already-done countries")

    log.info(f"Scraping {len(target)} countries with {args.workers} worker(s)")
    log.info(f"Max {MAX_USERS_PER_COUNTRY} users × {REPOS_PER_USER} repos each")

    if args.dry_run:
        for code, name in target:
            print(f"  {code}  {name}")
        return

    client = GitHubClient(args.token)

    # Verify token
    me = client.get("/user")
    if not me:
        log.error("Token invalid or expired")
        sys.exit(1)
    log.info(f"Authenticated as @{me.get('login')} — {client.rate_remaining} req remaining")

    def process(code_name):
        code, name = code_name
        try:
            summary = scrape_country(client, code, name)
            with threading.Lock():
                progress["done"].append(code)
                save_progress(progress)
                meta["total_users"]      += summary["users"]
                meta["total_repos"]      += summary["repos"]
                meta["countries_done"]   += 1
                meta["updated_at"]        = str(datetime.now(timezone.utc))
                save_meta(meta)
            return summary
        except Exception as e:
            log.error(f"[{code}] Failed: {e}", exc_info=True)
            return None

    start = time.time()
    if args.workers == 1:
        for item in target:
            process(item)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(process, item): item for item in target}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    log.info(f"Completed: {result['code']} — {result['repos']} repos")

    elapsed = time.time() - start
    log.info(f"All done in {elapsed/60:.1f} min — {meta['total_repos']} repos across {meta['countries_done']} countries")
    log.info(f"Total API requests: {client.total_requests}")


if __name__ == "__main__":
    main()
