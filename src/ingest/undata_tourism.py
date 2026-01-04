"""Ingest tourism data from UNdata (UNWTO source).

UNdata provides UNWTO tourism statistics in CSV format.
No authentication required.
"""

from subsets_utils import get, save_raw_file, load_state, save_state

# UNdata tourism datasets (UNWTO data)
# These are direct download links from UNdata
DATASETS = {
    "arrivals": {
        "url": "https://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=group_code:580&Format=csv&c=2,3,4,5,6,7&s=_crEngNameOrderBy:asc,yr:desc",
        "name": "International Tourist Arrivals",
        "desc": "Tourist arrivals by country and year",
    },
    "receipts": {
        "url": "https://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=group_code:600&Format=csv&c=2,3,4,5,6,7&s=_crEngNameOrderBy:asc,yr:desc",
        "name": "Tourism Receipts",
        "desc": "International tourism receipts in USD millions",
    },
    "expenditure": {
        "url": "https://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=group_code:610&Format=csv&c=2,3,4,5,6,7&s=_crEngNameOrderBy:asc,yr:desc",
        "name": "Tourism Expenditure",
        "desc": "International tourism expenditure in USD millions",
    },
}


def run():
    """Fetch all UNWTO tourism datasets from UNdata."""
    print("Fetching UNWTO Tourism data from UNdata...")

    state = load_state("unwto_tourism")
    completed = set(state.get("completed", []))

    pending = [(k, v) for k, v in DATASETS.items() if k not in completed]

    if not pending:
        print("All datasets up to date")
        return

    print(f"  Datasets to fetch: {len(pending)}")

    for i, (dataset_key, dataset_info) in enumerate(pending, 1):
        print(f"\n[{i}/{len(pending)}] Fetching {dataset_info['name']}...")

        try:
            response = get(dataset_info["url"], timeout=120)
            response.raise_for_status()

            content = response.text
            row_count = content.count("\n") - 1

            save_raw_file(content, f"unwto_{dataset_key}", extension="csv")
            print(f"    Saved {row_count:,} records")

            completed.add(dataset_key)
            save_state("unwto_tourism", {"completed": list(completed)})

        except Exception as e:
            print(f"    Error: {e}")

    print(f"\nIngested {len(completed)} datasets")
