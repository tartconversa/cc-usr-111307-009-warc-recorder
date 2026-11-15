"""归档索引：SQLite 保存 member 定位与抓取关系。

一行对应一个 gzip member（一条 WARC 记录），关键列：
segment + compressed_offset + compressed_length 可直接 seek 定位；
fetch_seq + hop 表达一次逻辑抓取及其重定向每一跳。

索引是派生物：任何时候都能通过 rebuild() 从段文件重新扫描生成，
因此索引写入失败、索引文件丢失都不会让归档失效。
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .records import parse_http_message
from .segments import list_segment_names, scan_segment, read_member_record

INDEX_FILENAME = "index.sqlite"
_SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    record_id         TEXT PRIMARY KEY,
    segment           TEXT NOT NULL,
    compressed_offset INTEGER NOT NULL,
    compressed_length INTEGER NOT NULL,
    record_type       TEXT NOT NULL,
    url               TEXT,
    fetch_seq         INTEGER,
    hop               INTEGER,
    status_code       INTEGER,
    truncated         TEXT,
    block_digest      TEXT,
    payload_digest    TEXT,
    concurrent_to     TEXT,
    refers_to         TEXT
);
CREATE INDEX IF NOT EXISTS idx_fetch ON records(fetch_seq, hop);
CREATE INDEX IF NOT EXISTS idx_url   ON records(url);
CREATE INDEX IF NOT EXISTS idx_seg   ON records(segment, compressed_offset);
"""


@dataclass
class IndexRow:
    record_id: str
    segment: str
    compressed_offset: int
    compressed_length: int
    record_type: str
    url: Optional[str]
    fetch_seq: Optional[int]
    hop: Optional[int]
    status_code: Optional[int]
    truncated: Optional[str]
    block_digest: Optional[str]
    payload_digest: Optional[str]
    concurrent_to: Optional[str]
    refers_to: Optional[str]

    @classmethod
    def from_sqlite(cls, row: sqlite3.Row) -> "IndexRow":
        return cls(**row)


class IndexStore:
    def __init__(self, archive_dir: str | Path, read_only: bool = False):
        self.archive_dir = Path(archive_dir)
        self.path = self.archive_dir / INDEX_FILENAME
        self.read_only = read_only
        if read_only:
            if not self.path.exists():
                raise FileNotFoundError("索引不存在: %s" % self.path)
            self._db = sqlite3.connect("file:%s?mode=ro" % self.path, uri=True)
        else:
            self.archive_dir.mkdir(parents=True, exist_ok=True)
            self._db = sqlite3.connect(self.path)
            self._db.row_factory = sqlite3.Row
            self._db.executescript(_SCHEMA)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.commit()
        self._db.row_factory = sqlite3.Row

    # ------------------------------------------------------------------ #
    # 写入
    # ------------------------------------------------------------------ #
    def add_record(self, member, record, *, status_code: Optional[int] = None) -> None:
        """登记一条已落盘记录。member 是 segments.MemberInfo。"""
        if self.read_only:
            raise RuntimeError("只读索引不能写入")
        h = {k.lower(): v for k, v in record.headers}
        self._db.execute(
            "INSERT OR REPLACE INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                member.record_id,
                member.segment,
                member.compressed_offset,
                member.compressed_length,
                member.record_type,
                h.get("warc-target-uri"),
                _int(h.get("x-fetch-seq")),
                _int(h.get("x-fetch-hop")),
                status_code,
                h.get("warc-truncated"),
                h.get("warc-block-digest"),
                h.get("warc-payload-digest"),
                h.get("warc-concurrent-to"),
                h.get("warc-refers-to"),
            ),
        )
        # 每条记录事务提交：进程被杀时最多丢最后一条的索引登记，段数据不受影响。
        self._db.commit()

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get(self, record_id: str) -> Optional[IndexRow]:
        cur = self._db.execute("SELECT * FROM records WHERE record_id=?", (record_id,))
        row = cur.fetchone()
        return IndexRow.from_sqlite(row) if row else None

    def by_fetch(self, fetch_seq: int) -> list[IndexRow]:
        cur = self._db.execute(
            "SELECT * FROM records WHERE fetch_seq=? ORDER BY hop, rowid", (fetch_seq,))
        return [IndexRow.from_sqlite(r) for r in cur.fetchall()]

    def latest_fetch_seq(self) -> Optional[int]:
        cur = self._db.execute("SELECT MAX(fetch_seq) AS m FROM records")
        return cur.fetchone()["m"]

    def by_url(self, url: str) -> list[IndexRow]:
        cur = self._db.execute(
            "SELECT * FROM records WHERE url=? ORDER BY fetch_seq, hop, rowid", (url,))
        return [IndexRow.from_sqlite(r) for r in cur.fetchall()]

    def all_responses(self) -> list[IndexRow]:
        cur = self._db.execute(
            "SELECT * FROM records WHERE record_type='response' ORDER BY fetch_seq, hop")
        return [IndexRow.from_sqlite(r) for r in cur.fetchall()]

    def count(self) -> int:
        return self._db.execute("SELECT COUNT(*) AS c FROM records").fetchone()["c"]

    def close(self) -> None:
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ #
    # 重建：从段文件扫描，索引文件丢失/损坏后仍可恢复全部定位信息
    # ------------------------------------------------------------------ #
    def rebuild(self) -> dict:
        """扫描归档目录下所有段，重建索引内容。

        前置条件：各段已经过 recover_segment 处理（Archive 打开时会自动做），
        残尾 member 不在扫描结果里，因此不完整记录永远不会被登记为可回放。
        重建在单事务内完成，中途失败保留旧索引。
        """
        if self.read_only:
            raise RuntimeError("只读索引不能重建")
        rows: list[tuple] = []
        scanned_members = 0
        for name in list_segment_names(self.archive_dir):
            segment_path = self.archive_dir / name
            result = scan_segment(segment_path)
            if result.tail_offset is not None:
                raise RuntimeError(
                    "段 %s 存在未处理残尾（%s），请先 recover_segment"
                    % (name, result.tail_reason))
            for member in result.members:
                record = read_member_record(segment_path,
                                            member.compressed_offset,
                                            member.compressed_length)
                h = {k.lower(): v for k, v in record.headers}
                status_code = None
                if record.record_type == "response":
                    message = parse_http_message(record.block)
                    status_code = _int(message.start_line.split(" ", 2)[1])
                rows.append((
                    member.record_id,
                    name,
                    member.compressed_offset,
                    member.compressed_length,
                    record.record_type,
                    h.get("warc-target-uri"),
                    _int(h.get("x-fetch-seq")),
                    _int(h.get("x-fetch-hop")),
                    status_code,
                    h.get("warc-truncated"),
                    h.get("warc-block-digest"),
                    h.get("warc-payload-digest"),
                    h.get("warc-concurrent-to"),
                    h.get("warc-refers-to"),
                ))
                scanned_members += 1
        with self._db:  # 单事务
            self._db.execute("DELETE FROM records")
            self._db.executemany(
                "INSERT OR REPLACE INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
        return {"segments": len(list_segment_names(self.archive_dir)),
                "members": scanned_members,
                "index_rows": len(rows)}


def _int(value) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
