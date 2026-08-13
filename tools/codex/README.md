# Codex Compression

A simple content-defined chunking + deduplication compressor (Codex) implemented in Python.

Usage (from the repository root):

1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r tools/codex/requirements.txt
```

2. Compress a directory

```bash
python -m tools.codex.codex.cli compress --input ./data --store ./codex_store
```

3. List files in the codex

```bash
python -m tools.codex.codex.cli list --store ./codex_store
```

4. Extract a file

```bash
python -m tools.codex.codex.cli extract --store ./codex_store --file path/to/file --dest ./out
```
