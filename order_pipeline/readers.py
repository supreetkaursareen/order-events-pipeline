"""Streaming readers for CSV and JSON-Lines. Never materialise the whole file."""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from . import config as C


class SchemaError(Exception):
    """File-level schema problem (e.g. a required column is missing)."""


@dataclass(frozen=True)
class Record:
    record_no: int            # 1-based ordinal of the data record (header excluded)
    line_no: int              # physical line where the record ends (diagnostics)
    data: Optional[dict]      # None when the record is malformed
    raw: str                  # source text kept for the reject path
    error: Optional[str]      # parse-level error, if any


def sha256_file(path, block: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(block)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def detect_format(path) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in (".jsonl", ".ndjson"):
        return "jsonl"
    raise ValueError(
        f"unsupported file type {suffix!r} for {path} (use .csv, .jsonl or .ndjson)"
    )


class RecordReader:
    """Context manager yielding `Record`s one at a time.

    CSV rows whose field count differs from the header, and JSONL lines that
    are not a JSON object, are yielded as malformed records (data=None) rather
    than raising, so the loader can quarantine them.
    A missing required column raises SchemaError when the file is opened.
    """

    def __init__(self, path, required=C.REQUIRED_COLUMNS):
        self.path = Path(path)
        self.fmt = detect_format(path)
        self.required = tuple(required)
        self.warnings: list[str] = []
        self.empty = False
        self._f = None
        self._columns: list[str] = []

    # -- context management -------------------------------------------------
    def __enter__(self):
        self._f = open(self.path, "r", encoding="utf-8-sig", errors="replace", newline="")
        if self.fmt == "csv":
            self._open_csv()
        else:
            self._check_jsonl_schema()
        return self

    def __exit__(self, *exc):
        if self._f:
            self._f.close()

    # -- schema checks ------------------------------------------------------
    def _open_csv(self):
        self._reader = csv.reader(self._f)
        header = next(self._reader, None)
        while header is not None and not any(h.strip() for h in header):
            header = next(self._reader, None)
        if header is None:
            self.empty = True
            return
        self._columns = [h.strip().lower() for h in header]
        self._check_columns(set(self._columns))

    def _check_jsonl_schema(self):
        # Peek at the first parseable object only; then rewind.
        seen_any = False
        for line in self._f:
            if not line.strip():
                continue
            seen_any = True
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                self._check_columns({str(k).strip().lower() for k in obj})
                break
        self.empty = not seen_any
        self._f.seek(0)

    def _check_columns(self, cols: set):
        missing = [c for c in self.required if c not in cols]
        if missing:
            raise SchemaError(f"missing required column(s): {', '.join(missing)}")
        extra = sorted(cols - set(self.required))
        if extra:
            self.warnings.append(f"ignoring unexpected column(s): {', '.join(extra)}")

    # -- iteration ----------------------------------------------------------
    def __iter__(self) -> Iterator[Record]:
        if self.empty:
            return
        if self.fmt == "csv":
            yield from self._iter_csv()
        else:
            yield from self._iter_jsonl()

    def _iter_csv(self):
        n = 0
        ncols = len(self._columns)
        for row in self._reader:
            if not row:                       # blank line: not a record
                continue
            n += 1
            raw = json.dumps(row, ensure_ascii=False)
            if len(row) != ncols:
                yield Record(n, self._reader.line_num, None, raw,
                             f"expected {ncols} fields, got {len(row)}")
            else:
                yield Record(n, self._reader.line_num, dict(zip(self._columns, row)), raw, None)

    def _iter_jsonl(self):
        n = 0
        for line_no, line in enumerate(self._f, start=1):
            text = line.strip()
            if not text:
                continue
            n += 1
            raw = text[:2000]
            try:
                obj = json.loads(text)
            except json.JSONDecodeError as e:
                yield Record(n, line_no, None, raw, f"invalid JSON: {e.msg}")
                continue
            if not isinstance(obj, dict):
                yield Record(n, line_no, None, raw, "JSON value is not an object")
                continue
            yield Record(n, line_no, {str(k).strip().lower(): v for k, v in obj.items()}, raw, None)
