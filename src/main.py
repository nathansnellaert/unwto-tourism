"""UNWTO Tourism Data Connector - fetches international tourism statistics.

Data sources:
- UNdata (UNWTO): https://data.un.org/

Datasets:
- International tourist arrivals
- Tourism receipts
- Tourism expenditure
"""

import argparse
import os

os.environ["RUN_ID"] = os.getenv("RUN_ID", "local-run")

from subsets_utils import validate_environment
from ingest import undata_tourism as ingest_tourism


def main():
    parser = argparse.ArgumentParser(description="UNWTO Tourism Data Connector")
    parser.add_argument(
        "--ingest-only", action="store_true", help="Only fetch data from API"
    )
    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Only transform existing raw data",
    )
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_tourism.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("Transforms not yet implemented")


if __name__ == "__main__":
    main()
