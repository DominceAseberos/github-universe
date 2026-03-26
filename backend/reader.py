from pathlib import Path
from typing import List, Dict
import json
from .models import User, Repo

class RawDataReader:
    def __init__(self, raw_dir: Path):
        self.raw_dir = raw_dir

    def read_country(self, code: str) -> List[Dict]:
        path = self.raw_dir / f"{code}.jsonl"
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
                except json.JSONDecodeError:
                    pass
        return records
