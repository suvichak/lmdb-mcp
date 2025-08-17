import json
from typing import Any, Optional

import lmdb
from fastmcp import FastMCP


def _open_env(db_path: str, readonly: bool = True) -> lmdb.Environment:
    """Open an LMDB environment."""
    return lmdb.open(db_path, readonly=readonly, max_dbs=1, map_size=10485760)


server = FastMCP(
    "lmdb-mcp",
    instructions="Tools for navigating generic LMDB key/value stores",
)


@server.tool()
def search(
    db_path: str,
    field: str,
    value: Any,
    page: int = 1,
) -> dict:
    """Search entries where a JSON field matches a value.

    Args:
        db_path: Path to the LMDB environment.
        field: JSON key to match.
        value: Desired value for the key.
        page: 1-indexed page number (10 results per page).

    Returns:
        Mapping with "results" and optional "next_page".
    """
    env = _open_env(db_path)
    matched: list[dict[str, Any]] = []
    with env.begin() as txn:
        for key, raw in txn.cursor():
            data = json.loads(raw)
            if data.get(field) == value:
                matched.append({"key": key.decode(), "value": data})
    limit = 10
    offset = (page - 1) * limit
    page_results = matched[offset : offset + limit]
    next_page = page + 1 if offset + limit < len(matched) else None
    return {"results": page_results, "next_page": next_page}


@server.tool()
def get_row(db_path: str, key: str) -> dict:
    """Retrieve the JSON value for an exact key.

    Args:
        db_path: Path to the LMDB environment.
        key: Row identifier to fetch.

    Returns:
        Mapping with "key" and "value" (or None if missing).
    """
    env = _open_env(db_path)
    with env.begin() as txn:
        raw = txn.get(key.encode())
    if raw is None:
        return {"key": key, "value": None}
    return {"key": key, "value": json.loads(raw)}


@server.tool()
def list_keys(db_path: str, page: int = 1) -> dict:
    """List available keys with pagination.

    Args:
        db_path: Path to the LMDB environment.
        page: 1-indexed page number (200 keys per page).

    Returns:
        Mapping with "keys" and optional "next_page".
    """
    env = _open_env(db_path)
    with env.begin() as txn:
        keys = [k.decode() for k, _ in txn.cursor()]
    limit = 200
    offset = (page - 1) * limit
    page_keys = keys[offset : offset + limit]
    next_page = page + 1 if offset + limit < len(keys) else None
    return {"keys": page_keys, "next_page": next_page}


@server.tool()
def count(
    db_path: str,
    prefix: str,
    column: str,
    value: Any,
) -> dict:
    """Count records with a key prefix and JSON column match.

    Args:
        db_path: Path to the LMDB environment.
        prefix: Key prefix to scan.
        column: JSON key to inspect.
        value: Desired value for the column.

    Returns:
        Mapping with "count".
    """
    env = _open_env(db_path)
    total = 0
    with env.begin() as txn:
        for key, raw in txn.cursor():
            key_str = key.decode()
            if not key_str.startswith(prefix):
                continue
            data = json.loads(raw)
            if data.get(column) == value:
                total += 1
    return {"count": total}


@server.tool()
def create_record(db_path: str, key: str, value: dict) -> dict:
    """Create a new record in the database.

    Args:
        db_path: Path to the LMDB environment.
        key: Key for the new record.
        value: JSON object to store.

    Returns:
        Mapping indicating whether the record was created.
    """
    env = _open_env(db_path, readonly=False)
    with env.begin(write=True) as txn:
        existing = txn.get(key.encode())
        if existing is not None:
            return {"created": False, "error": "key exists"}
        txn.put(key.encode(), json.dumps(value).encode())
    return {"created": True}


@server.tool()
def set_value(
    db_path: str,
    key: str,
    column: str,
    value: Any,
) -> dict:
    """Set a JSON column to a target value.

    Args:
        db_path: Path to the LMDB environment.
        key: Row identifier to update.
        column: JSON key to modify.
        value: New value for the column.

    Returns:
        Mapping indicating whether the row was updated.
    """
    env = _open_env(db_path, readonly=False)
    with env.begin(write=True) as txn:
        raw = txn.get(key.encode())
        if raw is None:
            return {"updated": False, "error": "key not found"}
        data = json.loads(raw)
        data[column] = value
        txn.put(key.encode(), json.dumps(data).encode())
    return {"updated": True}


@server.tool()
def set_columns(db_path: str, key: str, updates: dict) -> dict:
    """Update multiple JSON columns for a record.

    Args:
        db_path: Path to the LMDB environment.
        key: Row identifier to update.
        updates: Mapping of columns to new values.

    Returns:
        Mapping indicating whether the row was updated.
    """
    env = _open_env(db_path, readonly=False)
    with env.begin(write=True) as txn:
        raw = txn.get(key.encode())
        if raw is None:
            return {"updated": False, "error": "key not found"}
        data = json.loads(raw)
        data.update(updates)
        txn.put(key.encode(), json.dumps(data).encode())
    return {"updated": True}


@server.tool()
def next_pending(
    db_path: str,
    column: str,
    after_key: Optional[str] = None,
) -> dict:
    """Return the next row where a column equals ``1``.

    Args:
        db_path: Path to the LMDB environment.
        column: JSON key to check for value ``1``.
        after_key: Start scanning after this key.

    Returns:
        Mapping with "key" and "value" or None if not found.
    """
    env = _open_env(db_path)
    with env.begin() as txn:
        cursor = txn.cursor()
        if after_key:
            found = cursor.set_range(after_key.encode())
            if found and cursor.key().decode() == after_key:
                cursor.next()
        else:
            cursor.first()
        while cursor.key() is not None:
            key = cursor.key().decode()
            data = json.loads(cursor.value())
            if data.get(column) == 1:
                return {"key": key, "value": data}
            if not cursor.next():
                break
    return {"key": None, "value": None}


if __name__ == "__main__":
    server.run()
