import json
import os
from pathlib import Path
from deltalake import DeltaTable
from .environment import get_data_dir, is_cloud_mode
from .r2 import get_delta_table_uri, get_storage_options


def _normalize_metadata(metadata: dict) -> str:
    """Normalize metadata to a canonical JSON string for comparison."""
    # Sort keys and use consistent formatting
    return json.dumps(metadata, sort_keys=True, separators=(',', ':'))


def _metadata_changed(dt: DeltaTable, new_metadata: dict) -> bool:
    """Check if metadata has changed from what's stored in the Delta table."""
    try:
        current_description = dt.metadata().description
        if not current_description:
            return True
        current_metadata = json.loads(current_description)
        return _normalize_metadata(current_metadata) != _normalize_metadata(new_metadata)
    except (json.JSONDecodeError, AttributeError):
        return True


def sync_metadata(dataset_name: str, metadata: dict, force: bool = False):
    """Sync metadata to a Delta table. Skips write if metadata is unchanged.

    Args:
        dataset_name: Name of the dataset/table
        metadata: Metadata dict (must contain 'id' and 'title')
        force: If True, always write even if metadata is unchanged
    """
    if 'id' not in metadata:
        raise ValueError("Missing required field: 'id'")
    if 'title' not in metadata:
        raise ValueError("Missing required field: 'title'")

    # Make a copy to avoid mutating the original
    metadata = metadata.copy()

    connector_url = os.environ.get('GITHUB_CONNECTOR_URL')
    if connector_url:
        metadata['connector_url'] = connector_url

    if is_cloud_mode():
        table_uri = get_delta_table_uri(dataset_name)
        dt = DeltaTable(table_uri, storage_options=get_storage_options())
    else:
        table_path = Path(get_data_dir()) / "subsets" / dataset_name
        dt = DeltaTable(str(table_path))

    # Validate column descriptions against actual schema
    if 'column_descriptions' in metadata:
        schema = dt.schema().to_pyarrow() if hasattr(dt.schema(), 'to_pyarrow') else dt.schema().to_arrow()
        actual_columns = {field.name for field in schema}
        col_descs = json.loads(metadata['column_descriptions']) if isinstance(
            metadata['column_descriptions'], str
        ) else metadata['column_descriptions']
        invalid = set(col_descs.keys()) - actual_columns
        if invalid:
            raise ValueError(f"Invalid columns in descriptions: {sorted(invalid)}")

    print(f"Syncing metadata for {dataset_name}")

    # Check if metadata has changed
    if not force and not _metadata_changed(dt, metadata):
        print(f"  → Metadata unchanged, skipping write")
        return

    dt.alter.set_table_description(json.dumps(metadata))
    print(f"  → Metadata updated")


def publish(dataset_name: str, metadata: dict):
    """Deprecated: Use sync_metadata() instead. This always writes (force=True)."""
    import warnings
    warnings.warn("publish() is deprecated, use sync_metadata() instead", DeprecationWarning, stacklevel=2)
    sync_metadata(dataset_name, metadata, force=True)
