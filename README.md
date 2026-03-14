# GitHub Universe — Current Setup

[![Data Update Workflow](https://github.com/DominceAseberos/github-universe/actions/workflows/update.yml/badge.svg)](https://github.com/DominceAseberos/github-universe/actions/workflows/update.yml)
![Country Coverage](https://img.shields.io/badge/Coverage-3%2F195-3b82f6)

Interactive GitHub data universe with a static-data pipeline and 4-level drill-down visualization.

## Country coverage progress

Progress: `3 / 195` countries (`1.54%`)

`[█░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░]`

## Project purpose

This project maps public GitHub activity into an explorable universe:
- Level 0: Countries (galaxy view)
- Level 1: Cities (country view)
- Level 2: Users (population field)
- Level 3: Repositories (repo field)

## Current active system

### Data pipeline
- Scraper supports query-bucket partitioning to avoid repeated top-result caps.
- Deep scraping mode (`--deep`) increases country coverage with follower and year buckets.
- Resume mode is supported for interrupted runs.
- Builder compiles static outputs into:
  - `data/index.json`
  - `data/countries/{CODE}.json`

### Visualization
- Active-country filtering: only countries present in built data are shown.
- Wide spread layouts are enabled across levels with dense-view de-crowding.
- Country nodes use seeded SVG mini-universe styling (unique per country).
- Context-aware search with dropdown navigation to matching entities.
- Adaptive performance mode (`auto/high/mid/low`) with manual override.
- Live FPS HUD displays frame rate + active performance mode.

### Current dataset snapshot
- Active built countries in this repo: `PH`, `DE`, `SG`.
- Overview metrics are sourced from `data/index.json`.

## Deployment state

- Frontend is static (no runtime backend required for visualization).
- Vercel-compatible static deployment is configured.
- Weekly data refresh workflow is present via GitHub Actions.

## Next planned features

- Expand country coverage (priority): scale from current `PH/DE/SG` to more countries via deep scrape + rebuild cycles.
- Add country coverage progress tracking in overview (covered vs target countries).
- Deterministic packed layouts per selected city/user for stable re-entry visuals.
- Optional “labels on hover only” mode for very dense datasets.
- Stronger label collision culling with priority-based rendering.
- UI control for density/spread tuning in settings.
- Shareable view-state links (selected level/node + camera/filter state).
- Data freshness badge/details based on latest build timestamp.
