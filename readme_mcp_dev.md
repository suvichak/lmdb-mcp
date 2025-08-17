# LMDB MCP Development

## Setup
```
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .'[test]'
```

## Run tests
```
pytest -q
```

## Running the server
```
python -m lmdb_mcp.server
```

## Continuing development
* Add new tools in `lmdb_mcp/server.py` using `@server.tool`.
* Update tests under `tests/` for new behaviours.
