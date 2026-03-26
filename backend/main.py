from pathlib import Path
from .config import RAW_DIR, OUT_DIR, COUNTRY_META
from .reader import RawDataReader
from .processor import CountryBuilder
from .writer import JsonWriter

# Entry point for backend build pipeline
def main():
    reader = RawDataReader(RAW_DIR)
    builder = CountryBuilder()
    writer = JsonWriter(OUT_DIR)

    # Example: process all countries in COUNTRY_META
    countries = []
    for code, meta in COUNTRY_META.items():
        records = reader.read_country(code)
        if not records:
            continue
        country = builder.build_country(code, records, meta)
        writer.write_country(country)
        countries.append(country)
    writer.write_index(countries)

if __name__ == "__main__":
    main()
