"""Core codex engine: chunking, dedup store, sqlite index, compress/decompress"""
import os
import json
import hashlib
import sqlite3
from pathlib import Path
from typing import Iterator, Tuple, List

try:
    import brotli
except Exception:
    brotli = None


class GearRabin:
    """A simple Gear-like rolling hash for content-defined chunking."""

    def __init__(self):
        import random
        random.seed(0xC0FFEE)
        self.table = [random.getrandbits(64) for _ in range(256)]
        self.h = 0

    def reset(self):
        self.h = 0

    def slide(self, b: int):
        # Gear hash: h = (h << 1) + table[b]
        self.h = ((self.h << 1) + self.table[b]) & ((1 << 64) - 1)
        return self.h


class CodexEngine:
    def __init__(self, store: Path, min_size=2 * 1024, avg_size=8 * 1024, max_size=64 * 1024):
        self.store = Path(store)
        self.chunks_dir = self.store / "chunks"
        self.db_path = self.store / "codex.db"
        self.min_size = min_size
        self.avg_size = avg_size
        self.max_size = max_size
        # mask bit count to approximate avg size
        # choose mask so probability ~1/avg_size
        # mask = (1<<k)-1 where 2^k ~= avg_size => k = log2(avg_size)
        import math
        k = max(1, int(math.log2(avg_size)))
        self.mask = (1 << k) - 1
        self._ensure_store()
        self._init_db()

    def _ensure_store(self):
        self.store.mkdir(parents=True, exist_ok=True)
        self.chunks_dir.mkdir(parents=True, exist_ok=True)

    def _init_db(self):
        self.conn = sqlite3.connect(str(self.db_path))
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS chunks (
                id TEXT PRIMARY KEY,
                size INTEGER,
                csize INTEGER,
                compressed INTEGER,
                path TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                metadata TEXT
            )
            """
        )
        self.conn.commit()

    def _chunker(self, fh) -> Iterator[bytes]:
        gear = GearRabin()
        buf = bytearray()
        while True:
            chunk = fh.read(8192)
            if not chunk:
                if buf:
                    yield bytes(buf)
                break
            for b in chunk:
                buf.append(b)
                h = gear.slide(b)
                if len(buf) >= self.min_size and (h & self.mask) == 0:
                    yield bytes(buf)
                    buf = bytearray()
                    gear.reset()
                elif len(buf) >= self.max_size:
                    yield bytes(buf)
                    buf = bytearray()
                    gear.reset()

    def _store_chunk(self, data: bytes) -> Tuple[str, int, int, int, Path]:
        sha = hashlib.sha256(data).hexdigest()
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM chunks WHERE id=?", (sha,))
        if cur.fetchone():
            # already stored
            cur.execute("SELECT size,csize,compressed,path FROM chunks WHERE id=?", (sha,))
            row = cur.fetchone()
            return sha, row[0], row[1], row[2], Path(row[3])

        # compress with brotli if available
        compressed = 0
        cdata = data
        if brotli is not None:
            try:
                cand = brotli.compress(data)
                if len(cand) < len(data):
                    cdata = cand
                    compressed = 1
            except Exception:
                compressed = 0
                cdata = data

        # write chunk file
        filename = self.chunks_dir / sha
        with open(filename, "wb") as fh:
            fh.write(cdata)

        cur.execute(
            "INSERT OR REPLACE INTO chunks(id,size,csize,compressed,path) VALUES(?,?,?,?,?)",
            (sha, len(data), len(cdata), compressed, str(filename)),
        )
        self.conn.commit()
        return sha, len(data), len(cdata), compressed, filename

    def compress_file(self, path: Path, base_dir: Path) -> None:
        rel = str(path.relative_to(base_dir))
        print(f"Compressing {rel}")
        chunks_meta = []
        total_in = 0
        total_out = 0
        with open(path, "rb") as fh:
            for chunk in self._chunker(fh):
                total_in += len(chunk)
                sha, size, csize, compressed, fpath = self._store_chunk(chunk)
                chunks_meta.append({"id": sha, "size": size, "csize": csize, "compressed": compressed})
                total_out += csize

        meta = {"chunks": chunks_meta}
        cur = self.conn.cursor()
        cur.execute("INSERT OR REPLACE INTO files(path, metadata) VALUES(?,?)", (rel, json.dumps(meta)))
        self.conn.commit()
        print(f"File {rel}: raw={total_in} compressed_store={total_out} ratio={(total_in/(total_out+1)):.2f}")

    def compress_directory(self, input_dir: Path) -> None:
        input_dir = input_dir.resolve()
        for root, dirs, files in os.walk(input_dir):
            for f in files:
                p = Path(root) / f
                try:
                    self.compress_file(p, input_dir)
                except Exception as e:
                    print(f"Error compressing {p}: {e}")

    def list_files(self) -> List[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT path FROM files ORDER BY path")
        return [r[0] for r in cur.fetchall()]

    def extract_file(self, file_path: str, dest: Path) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT metadata FROM files WHERE path=?", (file_path,))
        row = cur.fetchone()
        if not row:
            raise FileNotFoundError(f"File {file_path} not found in codex")
        meta = json.loads(row[0])
        dest = dest
        if dest.is_dir():
            dest.mkdir(parents=True, exist_ok=True)
            outp = dest / Path(file_path).name
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            outp = dest
        with open(outp, "wb") as outfh:
            for c in meta.get("chunks", []):
                sha = c["id"]
                cur.execute("SELECT path,compressed FROM chunks WHERE id=?", (sha,))
                crow = cur.fetchone()
                if not crow:
                    raise FileNotFoundError(f"Chunk {sha} missing")
                path = Path(crow[0])
                compressed = crow[1]
                with open(path, "rb") as chf:
                    b = chf.read()
                    if compressed:
                        if brotli is None:
                            raise RuntimeError("Chunk is compressed but brotli is not available")
                        b = brotli.decompress(b)
                    outfh.write(b)
        print(f"Extracted {file_path} -> {outp}")

    def inspect(self):
        cur = self.conn.cursor()
        cur.execute("SELECT count(*) FROM chunks")
        nchunks = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM files")
        nfiles = cur.fetchone()[0]
        total_raw = 0
        total_store = 0
        cur.execute("SELECT SUM(size), SUM(csize) FROM chunks")
        row = cur.fetchone()
        if row:
            total_raw = row[0] or 0
            total_store = row[1] or 0
        print(f"Chunks: {nchunks}, Files: {nfiles}")
        print(f"Raw bytes in chunks: {total_raw}, Stored bytes: {total_store}")

