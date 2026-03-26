from dataclasses import dataclass, field
from typing import List, Dict, Optional

@dataclass
class Repo:
    id: int
    name: str
    stars: int
    language: str

@dataclass
class User:
    login: str
    location: Optional[str]
    repos: List[Repo] = field(default_factory=list)

@dataclass
class City:
    name: str
    users: List[User] = field(default_factory=list)
    repos: List[Repo] = field(default_factory=list)

@dataclass
class CountrySummary:
    code: str
    name: str
    flag: str
    continent: str
    users: int
    total_repos: int
    total_stars: int
    top_lang: str
    langs: Dict[str, int]
    cities: List[City]
    built_at: str
