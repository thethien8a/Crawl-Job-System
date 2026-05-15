"""Track which bronze objects have been ingested into the silver layer.

Object stores (S3, MinIO) do not support atomic append. To avoid
read-modify-write on the manifest, each ETL run writes one new shard at
``<silver_bucket>/_manifest/processed_<timestamp>.jsonl``. On read, all
shards are merged into a single set of processed bronze keys.
"""
import json
from collections.abc import Iterable
from datetime import datetime

_MANIFEST_DIR = "_manifest"
_SHARD_PATTERN = "processed_*.jsonl"


def _shards_glob(silver_bucket: str) -> str:
    return f"{silver_bucket}/{_MANIFEST_DIR}/{_SHARD_PATTERN}"


def load_processed_keys(fs, silver_bucket: str) -> set[str]:
    """Return every bronze key already ingested into silver."""
    shards = fs.glob(_shards_glob(silver_bucket))
    processed: set[str] = set()
    for shard in shards:
        with fs.open(shard, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                processed.add(json.loads(line)["bronze_key"])
    return processed


def append_processed_keys(
    fs, silver_bucket: str, entries: Iterable[dict]
) -> str | None:
    """Write a new manifest shard recording keys ingested in this run.

    ``entries`` items should be dicts containing at least ``bronze_key``.
    Returns the shard path, or ``None`` when there are no entries.
    """
    entries = list(entries)
    if not entries:
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    shard_path = f"{silver_bucket}/{_MANIFEST_DIR}/processed_{timestamp}.jsonl"

    payload = "\n".join(json.dumps(e) for e in entries) + "\n"
    with fs.open(shard_path, "w") as f:
        f.write(payload)
    return shard_path
