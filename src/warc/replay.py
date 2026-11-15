"""离线回放：不访问原站，从归档读取同一批字节。

回放入口按索引里的 segment + 压缩偏移 + 压缩长度 seek 到单个 gzip member，
只解压这一个 member（不展开整段），再做两级校验：
1. gzip 流完整性由 segments.read_member_record 保证（CRC32/ISIZE + 边界）；
2. WARC-Block-Digest 校验整个内嵌 HTTP 块，WARC-Payload-Digest 校验实体。

截断记录（WARC-Truncated）默认抛出 TruncatedResponseError，
明确拒绝当完整页使用；需要审计残体时可传 allow_truncated=True。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .indexstore import IndexStore, IndexRow
from .records import (
    digest_b32sha256,
    header_get,
    parse_http_message,
)
from .segments import read_member_record


class ReplayError(Exception):
    """回放数据缺失或完整性校验失败。"""


class TruncatedResponseError(ReplayError):
    """响应在捕获时被截断，不能作为完整页使用。"""

    def __init__(self, row: IndexRow, reason: Optional[str]):
        super().__init__(
            "记录 %s 是截断响应（WARC-Truncated=%s，URL=%s），拒绝作为完整页"
            % (row.record_id, reason, row.url))
        self.row = row
        self.reason = reason


@dataclass
class ReplayedResponse:
    """离线回放结果。entity 与线上捕获的实体逐字节相同。"""

    row: IndexRow
    status_code: int
    reason: str
    headers: list[tuple[str, str]]
    entity: bytes
    truncated: Optional[str]
    block_digest: Optional[str]
    payload_digest: Optional[str]
    request_record: object
    metadata: dict

    @property
    def url(self) -> Optional[str]:
        return self.row.url


class ReplayStore:
    def __init__(self, archive_dir: str | Path):
        self.archive_dir = Path(archive_dir)
        self.index = IndexStore(self.archive_dir, read_only=True)

    def close(self) -> None:
        self.index.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ #
    def fetch_rows(self, fetch_seq: int) -> list[IndexRow]:
        return self.index.by_fetch(fetch_seq)

    def response_rows_for_url(self, url: str) -> list[IndexRow]:
        return [r for r in self.index.by_url(url) if r.record_type == "response"]

    def all_response_rows(self) -> list[IndexRow]:
        return self.index.all_responses()

    def read_record(self, row: IndexRow):
        """按索引定位一个 gzip member 并解析（不触碰段内其他 member）。"""
        return read_member_record(
            self.archive_dir / row.segment,
            row.compressed_offset, row.compressed_length)

    # ------------------------------------------------------------------ #
    def replay(self, row: IndexRow, *, verify: bool = True,
               allow_truncated: bool = False) -> ReplayedResponse:
        if row.record_type != "response":
            raise ReplayError("记录 %s 不是 response（%s）"
                              % (row.record_id, row.record_type))
        reason = row.truncated
        if reason and not allow_truncated:
            raise TruncatedResponseError(row, reason)

        record = self.read_record(row)  # 内部已校验 WARC 块摘要
        message = parse_http_message(record.block)
        pieces = message.start_line.split(" ", 2)
        status_code = int(pieces[1])
        reason_text = pieces[2] if len(pieces) > 2 else ""

        if verify:
            self._verify_digests(row, record, message.body)
        # 回放形态必须带实体长度且无 Transfer-Encoding。
        if header_get(message.headers, "Transfer-Encoding") is not None:
            raise ReplayError("回放响应仍带 Transfer-Encoding，记录 %s"
                              % row.record_id)
        declared_len = header_get(message.headers, "Content-Length")
        if declared_len is None or int(declared_len) != len(message.body):
            raise ReplayError(
                "回放响应 Content-Length 与实体长度冲突，记录 %s" % row.record_id)

        # 同跳 request / metadata 通过并发关系定位。
        request_row = self.index.get(row.concurrent_to) if row.concurrent_to else None
        request_record = self.read_record(request_row) if request_row else None
        metadata = self._load_metadata(row)
        return ReplayedResponse(
            row=row, status_code=status_code, reason=reason_text,
            headers=message.headers, entity=message.body,
            truncated=reason,
            block_digest=row.block_digest, payload_digest=row.payload_digest,
            request_record=request_record, metadata=metadata)

    def replay_url(self, url: str, *, verify: bool = True,
                   allow_truncated: bool = False) -> ReplayedResponse:
        rows = self.response_rows_for_url(url)
        if not rows:
            raise ReplayError("URL 没有可回放 response: %s" % url)
        return self.replay(rows[-1], verify=verify,
                           allow_truncated=allow_truncated)

    def replay_fetch(self, fetch_seq: int, *, verify: bool = True,
                     allow_truncated: bool = False) -> list[ReplayedResponse]:
        out = []
        for row in self.fetch_rows(fetch_seq):
            if row.record_type == "response":
                out.append(self.replay(
                    row, verify=verify, allow_truncated=allow_truncated))
        return out

    # ------------------------------------------------------------------ #
    def _verify_digests(self, row: IndexRow, record, entity: bytes) -> None:
        declared_block = header_get(record.headers, "WARC-Block-Digest")
        actual_block = "sha256:" + digest_b32sha256(record.block)
        if declared_block != actual_block:
            raise ReplayError(
                "块摘要不符 记录 %s: %s != %s"
                % (row.record_id, declared_block, actual_block))
        declared_payload = header_get(record.headers, "WARC-Payload-Digest")
        actual_payload = "sha256:" + digest_b32sha256(entity)
        if declared_payload != actual_payload:
            raise ReplayError(
                "实体摘要不符 记录 %s: %s != %s"
                % (row.record_id, declared_payload, actual_payload))
        # 索引登记值与记录头交叉核对。
        if row.block_digest and row.block_digest != actual_block:
            raise ReplayError("索引块摘要与记录头不一致: %s" % row.record_id)
        if row.payload_digest and row.payload_digest != actual_payload:
            raise ReplayError("索引实体摘要与记录头不一致: %s" % row.record_id)

    def _load_metadata(self, response_row: IndexRow) -> dict:
        # metadata 以 WARC-Refers-To 指向 response。
        cur = self.index._db.execute(
            "SELECT * FROM records WHERE refers_to=?", (response_row.record_id,))
        data = cur.fetchone()
        if data is None:
            return {}
        meta_row = IndexRow.from_sqlite(data)
        record = self.read_record(meta_row)
        return json.loads(record.block.decode("utf-8"))
