"""Ingest tourism data from UNdata (UNWTO source).

UNdata provides UNWTO tourism statistics via Statistical Yearbook CSV files.
No authentication required.

Data contains tourist/visitor arrivals and tourism expenditure by country and year.
"""

from subsets_utils import get, save_raw_file

# UNdata Statistical Yearbook tourism dataset
# SYB67 (November 2024) contains arrivals and expenditure in a single file
TOURISM_URL = "https://data.un.org/_Docs/SYB/CSV/SYB67_176_202411_Tourist-Visitors%20Arrival%20and%20Expenditure.csv"


def run():
    """Fetch UNWTO tourism data from UNdata Statistical Yearbook."""
    print("Fetching UNWTO Tourism data from UNdata...")

    print("  Downloading Statistical Yearbook tourism data (SYB67)...")
    response = get(TOURISM_URL, timeout=120)
    response.raise_for_status()

    content = response.text
    row_count = content.count("\n") - 1

    save_raw_file(content, "tourism_arrivals_expenditure", extension="csv")
    print(f"  Saved {row_count:,} records")
