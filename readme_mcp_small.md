# LMDB MCP quick start

Functions:
1. `search(db_path, field, value, page)` – find records with JSON field/value (10 per page).
2. `get_row(db_path, key)` – fetch JSON document for a key.
3. `list_keys(db_path, page)` – list keys, 200 per page.
4. `count(db_path, prefix, column, value)` – count matching prefix + column.
5. `set_value(db_path, key, column, value)` – update a single field.
6. `create_record(db_path, key, value)` – insert a new JSON record.
7. `set_columns(db_path, key, updates)` – update multiple fields.
8. `next_pending(db_path, column, after_key)` – next record where column==1.

Run the MCP server:
```
python -m lmdb_mcp.server
```
