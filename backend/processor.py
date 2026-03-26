from typing import List, Dict
from .models import User, Repo, City, CountrySummary
from collections import defaultdict
from datetime import datetime, timezone

class CountryBuilder:
    def build_country(self, code: str, records: List[Dict], country_meta: Dict) -> CountrySummary:
        # This is a simplified placeholder. Real logic would aggregate users, cities, repos, etc.
        users = [User(login=rec.get('login', 'unknown'), location=rec.get('location'),
                     repos=[Repo(**repo) for repo in rec.get('repos', [])]) for rec in records]
        # Dummy aggregation for demonstration
        total_repos = sum(len(u.repos) for u in users)
        total_stars = sum(r.stars for u in users for r in u.repos)
        cities = []  # Would be filled with real city aggregation
        langs = defaultdict(int)
        for u in users:
            for r in u.repos:
                langs[r.language] += 1
        top_lang = max(langs, key=langs.get, default="Unknown")
        return CountrySummary(
            code=code,
            name=country_meta.get('name', code),
            flag=country_meta.get('flag', ''),
            continent=country_meta.get('continent', ''),
            users=len(users),
            total_repos=total_repos,
            total_stars=total_stars,
            top_lang=top_lang,
            langs=dict(langs),
            cities=cities,
            built_at=str(datetime.now(timezone.utc)),
        )
