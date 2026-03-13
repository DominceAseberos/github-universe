#!/usr/bin/env python3
"""
GitHub Universe Builder
=======================
Reads raw JSONL files from data/raw/ (produced by scraper.py)
and builds static JSON files that the visualization loads directly.

Output structure:
    data/index.json              — global summary + all countries
    data/countries/{CODE}.json   — per-country with cities and repos

Usage:
    python build.py
    python build.py --top-cities 8     # cities per country (default 6)
    python build.py --top-repos  30    # repos per city (default 20)
    python build.py --min-stars  0     # min stars to include a repo
    python build.py --countries US,DE  # only rebuild specific countries
"""

import json
import argparse
import logging
import sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

RAW_DIR      = Path("data/raw")
OUT_DIR      = Path("data")
COUNTRY_DIR  = OUT_DIR / "countries"

CONTINENT_MAP = {
    "US":"NA","CN":"AS","IN":"AS","DE":"EU","GB":"EU","BR":"SA","FR":"EU",
    "CA":"NA","RU":"EU","JP":"AS","KR":"AS","AU":"OC","NL":"EU","PL":"EU",
    "ES":"EU","IT":"EU","SE":"EU","CH":"EU","UA":"EU","AR":"SA","ID":"AS",
    "TR":"AS","PT":"EU","PK":"AS","MX":"NA","CZ":"EU","BE":"EU","DK":"EU",
    "SG":"AS","FI":"EU","NO":"EU","AT":"EU","IL":"AS","TW":"AS","NG":"AF",
    "RO":"EU","GR":"EU","HU":"EU","EG":"AF","VN":"AS","IR":"AS","CO":"SA",
    "ZA":"AF","PH":"AS","TH":"AS","NZ":"OC","MY":"AS","CL":"SA","BD":"AS",
    "MA":"AF","PE":"SA","BG":"EU","HR":"EU","SK":"EU","RS":"EU","KE":"AF",
    "BY":"EU","LT":"EU","EE":"EU","DZ":"AF","IE":"EU","LK":"AS","TN":"AF",
    "JO":"AS","LB":"AS","GE":"AS","GH":"AF","KZ":"AS","EC":"SA","BO":"SA",
    "VE":"SA","UY":"SA","NP":"AS","LV":"EU","SI":"EU","LU":"EU","CR":"NA",
    "CM":"AF","ET":"AF","AE":"AS","SA":"AS","KW":"AS","CY":"EU","PA":"NA",
    "DO":"NA","GT":"NA","HN":"NA","SV":"NA","NI":"NA","PY":"SA","UG":"AF",
    "ZM":"AF","ZW":"AF","AM":"AS","AZ":"AS","MD":"EU","BA":"EU","MK":"EU",
    "AL":"EU","ME":"EU","IS":"EU","MT":"EU","BH":"AS","QA":"AS","OM":"AS",
    "BN":"AS","KH":"AS","MN":"AS","PS":"AS","MM":"AS","LA":"AS","KG":"AS",
    "TJ":"AS","AF":"AS","LR":"AF","SL":"AF","GN":"AF","ML":"AF","NE":"AF",
    "TD":"AF","BJ":"AF","TG":"AF","BF":"AF","MR":"AF","BI":"AF","BW":"AF",
    "NA":"AF","GA":"AF","MV":"AS","FJ":"OC","PG":"OC","WS":"OC","VU":"OC",
    "TO":"OC","JM":"NA","TT":"NA","BB":"NA","AD":"EU","LI":"EU","MC":"EU",
    "SM":"EU","XK":"EU","BZ":"NA","SR":"SA","GY":"SA","LY":"AF","SD":"AF",
    "SO":"AF","YE":"AS","IQ":"AS","SY":"AS","CU":"NA","HT":"NA","RW":"AF",
    "AO":"AF","MZ":"AF","MG":"AF","TZ":"AF","CI":"AF","SN":"AF",
}

COUNTRY_NAMES = {
    "US":"United States","CN":"China","IN":"India","DE":"Germany","GB":"United Kingdom",
    "BR":"Brazil","FR":"France","CA":"Canada","RU":"Russia","JP":"Japan","KR":"South Korea",
    "AU":"Australia","NL":"Netherlands","PL":"Poland","ES":"Spain","IT":"Italy",
    "SE":"Sweden","CH":"Switzerland","UA":"Ukraine","AR":"Argentina","ID":"Indonesia",
    "TR":"Turkey","PT":"Portugal","PK":"Pakistan","MX":"Mexico","CZ":"Czech Republic",
    "BE":"Belgium","DK":"Denmark","SG":"Singapore","FI":"Finland","NO":"Norway",
    "AT":"Austria","IL":"Israel","TW":"Taiwan","NG":"Nigeria","RO":"Romania",
    "GR":"Greece","HU":"Hungary","EG":"Egypt","VN":"Vietnam","IR":"Iran",
    "CO":"Colombia","ZA":"South Africa","PH":"Philippines","TH":"Thailand",
    "NZ":"New Zealand","MY":"Malaysia","CL":"Chile","BD":"Bangladesh","MA":"Morocco",
    "PE":"Peru","BG":"Bulgaria","HR":"Croatia","SK":"Slovakia","RS":"Serbia",
    "KE":"Kenya","BY":"Belarus","LT":"Lithuania","EE":"Estonia","DZ":"Algeria",
    "IE":"Ireland","LK":"Sri Lanka","TN":"Tunisia","JO":"Jordan","LB":"Lebanon",
    "GE":"Georgia","GH":"Ghana","KZ":"Kazakhstan","EC":"Ecuador","BO":"Bolivia",
    "VE":"Venezuela","UY":"Uruguay","NP":"Nepal","LV":"Latvia","SI":"Slovenia",
    "LU":"Luxembourg","CR":"Costa Rica","AE":"UAE","SA":"Saudi Arabia","PS":"Palestine",
}

FLAGS = {
    "US":"🇺🇸","CN":"🇨🇳","IN":"🇮🇳","DE":"🇩🇪","GB":"🇬🇧","BR":"🇧🇷","FR":"🇫🇷",
    "CA":"🇨🇦","RU":"🇷🇺","JP":"🇯🇵","KR":"🇰🇷","AU":"🇦🇺","NL":"🇳🇱","PL":"🇵🇱",
    "ES":"🇪🇸","IT":"🇮🇹","SE":"🇸🇪","CH":"🇨🇭","UA":"🇺🇦","AR":"🇦🇷","ID":"🇮🇩",
    "TR":"🇹🇷","PT":"🇵🇹","PK":"🇵🇰","MX":"🇲🇽","CZ":"🇨🇿","BE":"🇧🇪","DK":"🇩🇰",
    "SG":"🇸🇬","FI":"🇫🇮","NO":"🇳🇴","AT":"🇦🇹","IL":"🇮🇱","TW":"🇹🇼","NG":"🇳🇬",
    "RO":"🇷🇴","GR":"🇬🇷","HU":"🇭🇺","EG":"🇪🇬","VN":"🇻🇳","IR":"🇮🇷","CO":"🇨🇴",
    "ZA":"🇿🇦","PH":"🇵🇭","TH":"🇹🇭","NZ":"🇳🇿","MY":"🇲🇾","CL":"🇨🇱","BD":"🇧🇩",
    "MA":"🇲🇦","PE":"🇵🇪","BG":"🇧🇬","HR":"🇭🇷","SK":"🇸🇰","RS":"🇷🇸","KE":"🇰🇪",
    "BY":"🇧🇾","LT":"🇱🇹","EE":"🇪🇪","DZ":"🇩🇿","IE":"🇮🇪","LK":"🇱🇰","TN":"🇹🇳",
    "JO":"🇯🇴","LB":"🇱🇧","GE":"🇬🇪","GH":"🇬🇭","KZ":"🇰🇿","AE":"🇦🇪","SA":"🇸🇦",
    "PS":"🇵🇸","SG":"🇸🇬",
}

LANG_COLORS = {
    "JavaScript":"#f0c040","TypeScript":"#4a9ff5","Python":"#4ec94e","Java":"#b07219",
    "C++":"#b06be0","C#":"#178600","C":"#555555","PHP":"#4f5d95","Ruby":"#ff5370",
    "Go":"#26c6da","Rust":"#ff6b3d","Swift":"#ff9f43","Kotlin":"#9c6af7","Dart":"#40c4ff",
    "Shell":"#89e051","HTML":"#e34c26","CSS":"#563d7c","Vue":"#42b883","R":"#198ce7",
    "Scala":"#c22d40","Haskell":"#5d4f85","Unknown":"#6a7a8c",
}


def read_raw(code: str) -> list[dict]:
    """Read all lines from a raw JSONL file."""
    path = RAW_DIR / f"{code}.jsonl"
    if not path.exists():
        return []
    records = []
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                log.warning(f"[{code}] Bad JSON on line {i+1}: {e}")
    return records


def build_country(code: str, top_cities: int, top_repos: int, min_stars: int) -> dict | None:
    records = read_raw(code)
    if not records:
        log.warning(f"[{code}] No raw data found — skipping")
        return None

    log.info(f"[{code}] Building from {len(records)} users…")

    # Aggregate by city
    city_buckets: dict[str, list] = defaultdict(list)
    no_city_repos = []
    global_lang_counts: dict[str, int] = defaultdict(int)
    global_stars = 0
    global_repos = 0

    for record in records:
        city = record.get("city")
        repos = record.get("repos", [])
        for repo in repos:
            if repo.get("stars", 0) < min_stars:
                continue
            global_lang_counts[repo.get("language","Unknown")] += 1
            global_stars += repo.get("stars", 0)
            global_repos += 1
            if city:
                city_buckets[city].append(repo)
            else:
                no_city_repos.append(repo)

    # Sort cities by number of repos, take top N
    sorted_cities = sorted(city_buckets.items(), key=lambda x: -len(x[1]))[:top_cities]

    cities_out = []
    for city_name, city_repos in sorted_cities:
        # Deduplicate repos by id
        seen = {}
        for r in city_repos:
            rid = r["id"]
            if rid not in seen or r["stars"] > seen[rid]["stars"]:
                seen[rid] = r
        deduped = sorted(seen.values(), key=lambda x: -x["stars"])[:top_repos]

        city_lang_counts: dict[str, int] = defaultdict(int)
        for r in deduped:
            city_lang_counts[r.get("language","Unknown")] += 1

        cities_out.append({
            "name":      city_name,
            "repos":     deduped,
            "repoCount": len(deduped),
            "stars":     sum(r["stars"] for r in deduped),
            "topLang":   max(city_lang_counts, key=city_lang_counts.get, default="Unknown"),
            "langs":     dict(sorted(city_lang_counts.items(), key=lambda x: -x[1])[:8]),
        })

    # Top language globally for this country
    top_lang = max(global_lang_counts, key=global_lang_counts.get, default="Unknown")

    country_doc = {
        "code":       code,
        "name":       COUNTRY_NAMES.get(code, code),
        "flag":       FLAGS.get(code, "🏳"),
        "continent":  CONTINENT_MAP.get(code, "UN"),
        "users":      len(records),
        "totalRepos": global_repos,
        "totalStars": global_stars,
        "topLang":    top_lang,
        "langs":      dict(sorted(global_lang_counts.items(), key=lambda x: -x[1])[:10]),
        "cities":     cities_out,
        "builtAt":    str(datetime.now(timezone.utc)),
    }

    # Write per-country file
    COUNTRY_DIR.mkdir(parents=True, exist_ok=True)
    out_path = COUNTRY_DIR / f"{code}.json"
    out_path.write_text(json.dumps(country_doc, ensure_ascii=False, separators=(",",":")))
    log.info(f"[{code}] → {out_path} ({global_repos} repos, {len(cities_out)} cities, top: {top_lang})")

    return country_doc


def build_index(country_docs: list[dict]) -> dict:
    """Build global index.json from all country summaries."""
    # Global lang aggregation
    global_langs: dict[str, int] = defaultdict(int)
    for doc in country_docs:
        for lang, cnt in doc.get("langs", {}).items():
            global_langs[lang] += cnt

    total_repos = sum(d["totalRepos"] for d in country_docs)
    total_stars = sum(d["totalStars"] for d in country_docs)
    total_users = sum(d["users"]      for d in country_docs)
    top_lang    = max(global_langs, key=global_langs.get, default="Unknown")

    # Country summary cards (no repos — keep index.json small)
    countries_summary = []
    for doc in sorted(country_docs, key=lambda d: -d["totalRepos"]):
        countries_summary.append({
            "code":      doc["code"],
            "name":      doc["name"],
            "flag":      doc["flag"],
            "continent": doc["continent"],
            "users":     doc["users"],
            "repos":     doc["totalRepos"],
            "stars":     doc["totalStars"],
            "topLang":   doc["topLang"],
            "cities":    [c["name"] for c in doc.get("cities", [])],
            "builtAt":   doc["builtAt"],
        })

    index = {
        "version":     2,
        "builtAt":     str(datetime.now(timezone.utc)),
        "totalCountries": len(country_docs),
        "totalRepos":  total_repos,
        "totalStars":  total_stars,
        "totalUsers":  total_users,
        "topLang":     top_lang,
        "topLangColor": LANG_COLORS.get(top_lang, "#aaa"),
        "langs":       dict(sorted(global_langs.items(), key=lambda x: -x[1])[:20]),
        "langColors":  LANG_COLORS,
        "countries":   countries_summary,
    }

    out_path = OUT_DIR / "index.json"
    out_path.write_text(json.dumps(index, ensure_ascii=False, separators=(",",":")))
    size_kb = out_path.stat().st_size / 1024
    log.info(f"index.json written — {size_kb:.1f} KB, {len(countries_summary)} countries")
    return index


def main():
    parser = argparse.ArgumentParser(description="GitHub Universe Builder")
    parser.add_argument("--top-cities", type=int, default=6,  help="Cities per country (default 6)")
    parser.add_argument("--top-repos",  type=int, default=20, help="Repos per city (default 20)")
    parser.add_argument("--min-stars",  type=int, default=0,  help="Min stars to include a repo")
    parser.add_argument("--countries",  default="",           help="Comma-separated codes (default: all)")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    COUNTRY_DIR.mkdir(parents=True, exist_ok=True)

    # Find raw files to process
    if args.countries:
        codes = [c.strip().upper() for c in args.countries.split(",")]
    else:
        codes = sorted(p.stem for p in RAW_DIR.glob("*.jsonl") if not p.stem.startswith("_"))

    if not codes:
        log.error("No raw data files found in data/raw/ — run scraper.py first")
        sys.exit(1)

    log.info(f"Building {len(codes)} countries (top_cities={args.top_cities}, top_repos={args.top_repos}, min_stars={args.min_stars})")

    country_docs = []
    for code in codes:
        doc = build_country(code, args.top_cities, args.top_repos, args.min_stars)
        if doc:
            country_docs.append(doc)

    if not country_docs:
        log.error("No country data built — check raw files")
        sys.exit(1)

    # Rebuild index from ALL existing country files (not just this run)
    all_country_files = list(COUNTRY_DIR.glob("*.json"))
    all_docs = []
    for p in all_country_files:
        try:
            all_docs.append(json.loads(p.read_text()))
        except Exception as e:
            log.warning(f"Could not read {p}: {e}")

    index = build_index(all_docs)

    log.info(
        f"Done — {index['totalCountries']} countries, "
        f"{index['totalRepos']:,} repos, "
        f"{index['totalStars']:,} stars"
    )
    log.info(f"Files ready in {OUT_DIR}/")


if __name__ == "__main__":
    main()
