"""Basic test for codex compress/extract"""
import tempfile
import os
from pathlib import Path
from tools.codex.codex.engine import CodexEngine


def test_basic_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        inp = Path(td) / "input"
        inp.mkdir()
        f = inp / "hello.txt"
        f.write_text("hello world\n" * 1000)

        store = Path(td) / "codex_store"
        engine = CodexEngine(store)
        engine.compress_directory(inp)
        files = engine.list_files()
        assert "hello.txt" in files
        outdir = Path(td) / "out"
        engine.extract_file("hello.txt", outdir)
        outf = outdir / "hello.txt"
        assert outf.exists()
        txt = outf.read_text()
        assert "hello world" in txt


if __name__ == '__main__':
    test_basic_roundtrip()
    print('ok')
