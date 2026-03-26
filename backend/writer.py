import json
from pathlib import Path
from .models import CountrySummary

class JsonWriter:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.country_dir = out_dir / "countries"
        self.country_dir.mkdir(parents=True, exist_ok=True)

    def write_country(self, country: CountrySummary):
        out_path = self.country_dir / f"{country.code}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(country.__dict__, f, ensure_ascii=False, separators=(",", ":"))

    def write_index(self, countries: List[CountrySummary]):
        index_path = self.out_dir / "index.json"
        # Simplified: just dump all countries
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump([c.__dict__ for c in countries], f, ensure_ascii=False, separators=(",", ":"))
