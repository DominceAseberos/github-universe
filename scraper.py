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
    python scraper.py --token ghp_xxx --only-with-repos # keep users with repos only
    python scraper.py --token ghp_xxx --skip-seen       # skip users already in data/raw/{CODE}.jsonl
    python scraper.py --token ghp_xxx --fresh           # wipe existing data and start from scratch

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

# Rate limit safety thresholds
CORE_RATE_BUFFER   = 20   # pause core API below this (out of 5000/hr)
SEARCH_RATE_BUFFER = 3    # pause search API below this (out of 30/min)

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
            self.total_requests += 1
            remaining = int(resp.headers.get("X-RateLimit-Remaining", -1))
            reset_at  = int(resp.headers.get("X-RateLimit-Reset", 0))
            if remaining == -1:
                return
            if "/search/" in resp.url:
                # Search API: 30 req/min separate bucket
                self.search_remaining = remaining
                self.search_reset_at  = reset_at
            else:
                # Core API: 5000 req/hr bucket
                self.rate_remaining = remaining
                self.rate_reset_at  = reset_at

    def _wait_for_rate(self, is_search=False):
        if is_search:
            if self.search_remaining <= SEARCH_RATE_BUFFER:
                wait = max(0, self.search_reset_at - time.time()) + 2
                log.warning(f"Search rate limit low ({self.search_remaining} left) — sleeping {wait:.0f}s")
                time.sleep(wait)
        else:
            if self.rate_remaining <= CORE_RATE_BUFFER:
                wait = max(0, self.rate_reset_at - time.time()) + 2
                log.warning(f"Core rate limit low ({self.rate_remaining} left) — sleeping {wait:.0f}s")
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


def load_seen_logins(code: str) -> set[str]:
    out_file = RAW_DIR / f"{code}.jsonl"
    if not out_file.exists():
        return set()
    seen_logins: set[str] = set()
    with open(out_file, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                continue
            login = record.get("login")
            if isinstance(login, str) and login:
                seen_logins.add(login.lower())
    return seen_logins


# ── City extraction ───────────────────────────────────────────────────────────
# Common noise words to strip from location strings
NOISE = {
    "remote", "worldwide", "global", "earth", "world", "internet",
    "everywhere", "anywhere", "nomad", "online", "home", "the", "and",
}

def _clean_location_token(token: str) -> str:
    import re
    cleaned = token.strip()
    cleaned = re.sub(r"^[@#\d\s]+", "", cleaned).strip()
    cleaned = re.sub(r"[^\w\s\-\.]", "", cleaned).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned

def _is_country_like(token: str, country_names: set[str], country_codes: set[str]) -> bool:
    value = token.lower().strip()
    return value in country_names or value in country_codes

def _contains_country_reference(value: str, country_names: set[str], country_codes: set[str]) -> bool:
    padded = f" {value.lower()} "
    for c in country_names:
        if f" {c} " in padded:
            return True
    for c in country_codes:
        if f" {c} " in padded:
            return True
    return False

# ── Region → city fallbacks (last resort when extract_city() returns None) ────
# Maps lowercase keywords found anywhere in the location string → canonical city.
REGION_CITY_FALLBACK: dict[str, list[tuple[list[str], str]]] = {
    "PH": [
        (["metro manila", "ncr", "national capital region"], "Manila"),
        (["quezon city"], "Quezon City"),
        (["makati", "taguig", "pasig", "mandaluyong", "marikina", "pasay",
          "paranaque", "parañaque", "caloocan", "navotas", "muntinlupa",
          "las piñas", "las pinas", "valenzuela", "pateros", "san juan"],
         "Manila"),
        (["luzon"], "Manila"),
        (["mindanao"], "Davao"),
        (["visayas", "visayan"], "Cebu"),
        (["iloilo"], "Iloilo City"),
        (["bacolod", "negros"], "Bacolod"),
        (["cagayan de oro", "cdo"], "Cagayan de Oro"),
        (["zamboanga"], "Zamboanga"),
        (["general santos", "gensan"], "General Santos"),
        (["ilocos"], "Laoag"),
        (["bicol", "bicolandia", "legazpi"], "Legazpi"),
        (["palawan", "puerto princesa"], "Puerto Princesa"),
        (["bohol", "tagbilaran"], "Tagbilaran"),
        (["leyte", "tacloban"], "Tacloban"),
        (["pampanga", "angeles", "clark"], "Angeles"),
        (["bulacan", "malolos"], "Malolos"),
        (["cavite", "bacoor", "dasmariñas", "dasmarinas"], "Dasmarinas"),
        (["laguna", "calamba", "los baños", "los banos"], "Calamba"),
        (["batangas"], "Batangas"),
        (["rizal", "antipolo"], "Antipolo"),
        (["iligan"], "Iligan"),
        (["cotabato"], "Cotabato"),
    ],
    "DE": [
        (["munich", "münchen", "muenchen", "bavaria", "bayern", "augsburg",
          "nuremberg", "nürnberg", "nuernberg", "regensburg", "ingolstadt",
          "würzburg", "wuerzburg", "erlangen", "fürth", "fuerth"], "Munich"),
        (["cologne", "köln", "koeln", "north rhine", "nordrhein",
          "westphalia", "westfalen", "nrw", "düsseldorf", "dusseldorf",
          "dortmund", "essen", "duisburg", "bochum", "bonn", "münster",
          "muenster", "wuppertal", "bielefeld", "aachen", "krefeld",
          "mönchengladbach", "moenchengladbach", "oberhausen", "hagen"],
         "Cologne"),
        (["frankfurt", "hesse", "hessen", "wiesbaden", "darmstadt",
          "kassel", "offenbach", "gießen", "giessen", "marburg"],
         "Frankfurt"),
        (["stuttgart", "bad.-württ", "badenwürtt", "bw",
          "württemberg", "wuerttemberg",
          "karlsruhe", "carlsruhe", "mannheim", "freiburg", "heidelberg",
          "ulm", "heilbronn", "pforzheim", "reutlingen", "tübingen",
          "tuebingen", "konstanz", "sindelfingen", "ravensburg"],
         "Stuttgart"),
        (["saxony", "sachsen", "dresden", "leipzig", "chemnitz", "zwickau",
          "görlitz", "goerlitz", "bautzen", "plauen"], "Dresden"),
        (["lower saxony", "niedersachsen", "hannover", "hanover",
          "braunschweig", "wolfsburg", "göttingen", "goettingen",
          "osnabrück", "osnabrueck", "oldenburg", "hildesheim"],
         "Hannover"),
        (["thuringia", "thüringen", "thueringen", "erfurt", "jena",
          "weimar", "gera", "gotha", "eisenach"], "Erfurt"),
        (["brandenberg", "brandenburg", "potsdam", "cottbus"],
         "Potsdam"),
        (["mecklenburg", "rostock", "schwerin", "greifswald", "stralsund",
          "neubrandenburg"], "Rostock"),
        (["rhineland-palatinate", "rheinland-pfalz", "rheinland",
          "mainz", "koblenz", "trier", "kaiserslautern", "ludwigshafen"],
         "Mainz"),
        (["saarland", "saarbrücken", "saarbruecken", "saarlouis"],
         "Saarbrücken"),
        (["schleswig-holstein", "schleswig", "holstein", "kiel",
          "lübeck", "luebeck", "flensburg", "neumünster", "neumuenster"],
         "Kiel"),
        (["saxony-anhalt", "sachsen-anhalt", "magdeburg", "halle",
          "dessau", "merseburg"], "Magdeburg"),
        (["bremen", "bremerhaven"], "Bremen"),
    ],
    "SG": [
        (["singapore", "jurong", "tampines", "woodlands", "ang mo kio",
          "bedok", "toa payoh", "bishan", "bukit timah", "bukit batok",
          "choa chu kang", "clementi", "geylang", "hougang", "pasir ris",
          "punggol", "queenstown", "sembawang", "sengkang", "serangoon",
          "yishun", "novena", "orchard", "marina", "kallang",
          "marine parade", "changi", "tengah", "central"], "Singapore"),
    ],
}


def apply_region_fallback(location: str, country_code: str | None) -> str | None:
    """Return a canonical city if `location` contains a known region keyword."""
    if not country_code:
        return None
    mapping = REGION_CITY_FALLBACK.get(country_code.upper())
    if not mapping:
        return None
    loc_lower = location.lower()
    for keywords, city in mapping:
        for kw in keywords:
            if kw in loc_lower:
                return city
    return None


def _normalize_city(city: str, country_code: str | None) -> str:
    """If the extracted city is itself a region name, map it to its capital."""
    mapped = apply_region_fallback(city, country_code)
    return mapped if mapped else city


def sanitize_location(location: str | None) -> str | None:
    if not location:
        return None
    import re
    import unicodedata
    cleaned = "".join(
        ch for ch in location
        if unicodedata.category(ch) not in {"So", "Cs"}
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;/|-")
    return cleaned or None

def extract_city(location: str | None, country_code: str | None = None) -> str | None:
    """
    Try to extract a city name from a free-text location string.
    e.g. "Berlin, Germany" -> "Berlin"
         "San Francisco, CA, USA" -> "San Francisco"
         "Germany" -> None  (country-only, skip)
    Falls back to REGION_CITY_FALLBACK if country_code is given and
    normal extraction fails (e.g. "Luzon, Philippines" -> "Manila").
    """
    if not location:
        return None
    loc = location.strip()
    if not loc or len(loc) < 2:
        return None

    import re
    country_names = {n.lower() for _, n in COUNTRIES}
    country_codes = {c.lower() for c, _ in COUNTRIES}

    # Try structured separators first (comma/slash/pipe/semicolon/dash).
    parts = [p.strip() for p in re.split(r",|/|\||;|\s+-\s+", loc) if p.strip()]
    for part in parts:
        candidate = _clean_location_token(part)
        if not candidate:
            continue
        lower_candidate = candidate.lower()
        if any(sep in lower_candidate for sep in (" and ", " or ", " und ", " & ")) and _contains_country_reference(lower_candidate, country_names, country_codes):
            continue
        candidate_parts = candidate.split()
        if len(candidate_parts) >= 2:
            tail = candidate_parts[-1].lower()
            if tail in country_codes or tail in country_names:
                candidate = " ".join(candidate_parts[:-1]).strip()
                if not candidate:
                    continue
        words = candidate.lower().split()
        if not words:
            continue
        if words[-1] in {"and", "or", "und", "&"}:
            continue
        if all(w in NOISE for w in words):
            continue
        if _is_country_like(candidate, country_names, country_codes):
            continue
        return _normalize_city(candidate.title(), country_code)

    # Fallback: handle compact forms like "Berlin Germany" or "Nuremberg DE".
    normalized = _clean_location_token(loc)
    if not normalized:
        return None
    lower = normalized.lower()

    # Remove trailing country name/code and retry.
    for country_name in sorted(country_names, key=len, reverse=True):
        suffix = f" {country_name}"
        if lower.endswith(suffix):
            prefix = normalized[: -len(suffix)].strip(" ,-/")
            prefix = _clean_location_token(prefix)
            if prefix and not _is_country_like(prefix, country_names, country_codes):
                words = prefix.lower().split()
                if words and not all(w in NOISE for w in words):
                    return _normalize_city(prefix.title(), country_code)

    for cc in country_codes:
        suffix = f" {cc}"
        if lower.endswith(suffix):
            prefix = normalized[: -len(suffix)].strip(" ,-/")
            prefix = _clean_location_token(prefix)
            if prefix and not _is_country_like(prefix, country_names, country_codes):
                words = prefix.lower().split()
                if words and not all(w in NOISE for w in words):
                    return _normalize_city(prefix.title(), country_code)

    # Last resort: region → city fallback for known countries
    if location and country_code:
        fallback = apply_region_fallback(location, country_code)
        if fallback:
            return fallback

    return None


# ── Follower-range buckets ─────────────────────────────────────────────────────
# Each bucket is a separate GitHub search query (max 1000 results each).
# Normal mode:  5 follower buckets  → up to  5,000 unique users/country
# Deep mode:   5 × 19 year buckets  → up to 95,000 unique users/country
FOLLOWER_BUCKETS = [
    "followers:>=500",    # power users
    "followers:100..499", # active community members
    "followers:10..99",   # regular devs
    "followers:1..9",     # casual users
    "followers:0",        # newcomers / private accounts
]

# GitHub launched in 2008; split by year for fine-grained windows
CREATED_YEAR_BUCKETS = [
    f"created:{y}-01-01..{y}-12-31" for y in range(2008, 2027)
]


def build_query_buckets(deep: bool) -> list[str]:
    """
    Return list of extra query filter strings (appended to the base query).
    Normal : one filter per follower range  →  5 buckets
    Deep   : follower × created-year grid  → 95 buckets
    """
    if not deep:
        return FOLLOWER_BUCKETS
    buckets = []
    for f in FOLLOWER_BUCKETS:
        for d in CREATED_YEAR_BUCKETS:
            buckets.append(f"{f} {d}")
    return buckets


# ── Per-country scraper ───────────────────────────────────────────────────────
def scrape_country(
    client: GitHubClient,
    code: str,
    name: str,
    only_with_repos: bool = False,
    skip_seen: bool = True,
    deep: bool = False,
) -> dict:
    """
    Scrape users from `name` (country) across query buckets.
    Normal mode : 5 follower buckets → up to   5,000 unique users/country
    Deep mode   : 5×19 year grid    → up to  95,000 unique users/country
    Already-seen logins are always skipped (deduplicated across buckets).
    """
    out_file = RAW_DIR / f"{code}.jsonl"
    city_counts: dict[str, int] = {}
    lang_counts: dict[str, int] = {}
    total_stars = 0
    total_repos_collected = 0
    lines_written = 0
    users_skipped_no_repos = 0
    users_skipped_seen = 0
    seen_logins = load_seen_logins(code) if skip_seen else set()

    buckets = build_query_buckets(deep)
    mode_str = f"{'append+dedupe' if skip_seen else 'fresh'}, {'deep' if deep else 'normal'}, {len(buckets)} buckets"
    log.info(f"[{code}] Starting — {name} ({mode_str})")
    if not skip_seen:
        log.warning(f"[{code}] --fresh mode: existing data will be overwritten!")

    open_mode = "a" if skip_seen else "w"
    with open(out_file, open_mode, encoding="utf-8") as fh:

        for bucket_idx, followers_filter in enumerate(buckets):
            query = f"location:{name} type:user {followers_filter}"
            bucket_new = 0
            log.info(f"[{code}] bucket {bucket_idx+1}/{len(buckets)}: {followers_filter}")

            for page in range(1, (MAX_USERS_PER_COUNTRY // USERS_PER_PAGE) + 1):
                result = client.get("/search/users", params={
                    "q": query,
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
                    login_key = login.lower()
                    if login_key in seen_logins:
                        users_skipped_seen += 1
                        continue

                    # Fetch full profile for reliable location data
                    user_profile = client.get(f"/users/{login}") or {}
                    location_raw = user_profile.get("location") or user.get("location")
                    location = sanitize_location(location_raw)
                    city = extract_city(location, country_code=code)

                    # Get user's top repos
                    repos_raw = client.get(f"/users/{login}/repos", params={
                        "sort": "stars",
                        "per_page": REPOS_PER_USER,
                        "type": "owner",
                    }) or []

                    repos = []
                    for r in repos_raw:
                        if r.get("fork"):
                            continue
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

                    if only_with_repos and not repos:
                        users_skipped_no_repos += 1
                        time.sleep(0.3)
                        continue

                    total_repos_collected += len(repos)

                    record = {
                        "login":    login,
                        "location": location,
                        "city":     city,
                        "country":  code,
                        "repos":    repos,
                    }
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                    fh.flush()
                    lines_written += 1
                    bucket_new += 1
                    seen_logins.add(login_key)
                    time.sleep(0.3)

                log.info(
                    f"[{code}] bucket {bucket_idx+1} page {page}/10 "
                    f"— +{bucket_new} new this bucket, {lines_written} total "
                    f"— core: {client.rate_remaining} left, search: {client.search_remaining} left"
                )

                if len(items) < USERS_PER_PAGE:
                    break

                time.sleep(1)

            log.info(f"[{code}] bucket {bucket_idx+1}/{len(buckets)} done — {bucket_new} new users added")

    summary = {
        "code":       code,
        "name":       name,
        "users":      lines_written,
        "repos":      total_repos_collected,
        "stars":      total_stars,
        "skipped_no_repos": users_skipped_no_repos,
        "skipped_seen": users_skipped_seen,
        "top_cities": sorted(city_counts.items(), key=lambda x: -x[1])[:20],
        "top_langs":  sorted(lang_counts.items(), key=lambda x: -x[1])[:15],
        "scraped_at": str(datetime.now(timezone.utc)),
    }
    log.info(
        f"[{code}] Done — {lines_written} new users, {total_repos_collected} repos, "
        f"skipped-seen: {users_skipped_seen}, top city: {summary['top_cities'][:1]}"
    )
    return summary


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="GitHub Universe Scraper")
    parser.add_argument("--token",    required=True,  help="GitHub Personal Access Token")
    parser.add_argument("--countries",default="",     help="Comma-separated ISO codes to scrape (default: all)")
    parser.add_argument("--resume",   action="store_true", help="Skip already-completed countries")
    parser.add_argument("--workers",  type=int, default=1, help="Parallel workers (keep ≤3 to avoid rate limits)")
    parser.add_argument("--dry-run",  action="store_true", help="Print plan without scraping")
    parser.add_argument("--only-with-repos", action="store_true", help="Only keep users with at least 1 non-fork repo")
    parser.add_argument("--skip-seen", action="store_true", default=True, help="(default) Append mode: skip users already in data/raw/{CODE}.jsonl")
    parser.add_argument("--fresh",     action="store_true", help="Wipe existing country data and start from scratch")
    parser.add_argument("--deep",      action="store_true", help="Deep mode: 5 follower × 19 year buckets = up to 95,000 users/country")
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

    # --fresh overrides --skip-seen
    effective_skip_seen = not args.fresh
    effective_buckets = build_query_buckets(args.deep)
    log.info(f"Query plan: {'deep' if args.deep else 'normal'} mode — {len(effective_buckets)} buckets/country, up to {len(effective_buckets) * MAX_USERS_PER_COUNTRY:,} users/country")

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
            summary = scrape_country(
                client,
                code,
                name,
                only_with_repos=args.only_with_repos,
                skip_seen=effective_skip_seen,
                deep=args.deep,
            )
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