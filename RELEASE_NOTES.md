# Release v0.1.0 — XYZ Knowledge Library — Codex

Initial release (patched) by Tobias Gordon Carson.

Highlights:
- Core engine implementing V1→V4 → X → Y → Z pipeline
- Persistence fixes: canonical fingerprint, pattern_store, delta_store persisted and restored
- Stable content hashing computed post-veracity
- Optional Zstd dictionary training and use (train_zstd_dictionary)
- Packaging: pyproject.toml for building a wheel