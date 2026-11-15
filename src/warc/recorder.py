"""抓取编排：把每次 HTTP 交换写成 request/response/metadata 三条 WARC 记录。

归档保证（对应需求）：
- 一次逻辑抓取的重定向每一跳独立归档，通过 WARC 记录 ID 相连
  （同跳 request 与 response 用 WARC-Concurrent-To，metadata 用
   WARC-Concurrent-To + WARC-Refers-To；跨跳顺序由 X-Fetch-Seq/
   X-Fetch-Hop 与 metadata 内的 fetch/hop 结构表达）；
- 失败（连不上、无响应头）只写 request + 错误 metadata，不伪造 response；
- 截断响应写 response 但带 WARC-Truncated，且内嵌 Content-Length
  等于实际收到的实体长度，不补造成功正文；
- 相同正文不做去重：每次状态、头、记录 ID 都独立保存。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from .capture import CaptureError, build_capture_request, capture_raw
from .indexstore import IndexStore
from .records import (
    build_metadata_record,
    build_request_record,
    build_response_record,
    build_warcinfo_record,
    header_get,
)
from .segments import (
    SegmentWriter,
    list_segment_names,
    read_member_record,
    recover_segment,
    scan_segment,
)

REDIRECT_STATUSES = {301, 302, 303, 307, 308}
DEFAULT_MAX_HOPS = 5
DEFAULT_SEGMENT_LIMIT = 8 * 1024 * 1024


@dataclass
class Hop:
    """一次逻辑抓取中的一跳。"""

    hop: int
    url: str
    request_id: str
    response_id: Optional[str]
    metadata_id: str
    status_code: Optional[int]
    truncated: Optional[str]
    error: Optional[str]
    payload_digest: Optional[str]
    block_digest: Optional[str]


@dataclass
class FetchResult:
    fetch_seq: int
    url: str
    hops: list[Hop] = field(default_factory=list)
    ok: bool = False
    final_status: Optional[int] = None
    truncated: Optional[str] = None
    error: Optional[str] = None

    @property
    def final_hop(self) -> Optional[Hop]:
        return self.hops[-1] if self.hops else None


class Recorder:
    """采集器对接的归档门面：打开目录、抓取、落盘、登记索引。"""

    def __init__(self, archive_dir: str | Path,
                 segment_limit: int = DEFAULT_SEGMENT_LIMIT,
                 timeout: float = 15.0):
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.segment_limit = segment_limit
        self.timeout = timeout
        # 打开即恢复：隔离所有段尾残尾，不完整 member 绝不进入索引。
        self.recovery_reports = self._recover_segments()
        self.index = IndexStore(self.archive_dir)
        self._reconcile_index()
        self._next_seq = (self.index.latest_fetch_seq() or 0) + 1
        self._writer: Optional[SegmentWriter] = None
        self._segment_seq = self._initial_segment_seq()

    # ------------------------------------------------------------------ #
    # 段管理与崩溃恢复
    # ------------------------------------------------------------------ #
    def _recover_segments(self) -> list[dict]:
        reports = []
        for name in list_segment_names(self.archive_dir):
            reports.append(recover_segment(self.archive_dir / name))
        return reports

    def _reconcile_index(self) -> None:
        """按段内实际存在的完整 member 对账索引。

        崩溃可能发生在“member 已落盘、索引未登记”或“残尾已隔离、
        索引仍残留旧行”两种窗口。这里以段扫描结果为准：
        删除指向不存在/不完整 member 的行，缺失行则触发整库重建
        （重建会重新解析全部记录并补齐状态码、摘要等列）。
        """
        valid_keys: set[tuple] = set()
        for name in list_segment_names(self.archive_dir):
            scan = scan_segment(self.archive_dir / name)
            if scan.tail_offset is not None:
                # _recover_segments 已处理；兜底防御。
                raise RuntimeError("段 %s 仍有未隔离残尾" % name)
            for member in scan.members:
                valid_keys.add((member.segment, member.compressed_offset,
                                member.compressed_length))
        rows = self.index._db.execute(
            "SELECT record_id, segment, compressed_offset, compressed_length "
            "FROM records").fetchall()
        stale = [r[0] for r in rows
                 if (r[1], r[2], r[3]) not in valid_keys]
        if stale:
            with self.index._db:
                self.index._db.executemany(
                    "DELETE FROM records WHERE record_id=?",
                    [(rid,) for rid in stale])
        if len(rows) - len(stale) != len(valid_keys):
            # member 多于索引：有落盘成功但未登记的记录，整库重建补齐。
            self.index.rebuild()

    def _initial_segment_seq(self) -> int:
        existing = list_segment_names(self.archive_dir)
        if not existing:
            return 0
        # 文件名 segment-NNNN.warc.gz，取最大编号 +1，绝不复用旧段。
        highest = -1
        for name in existing:
            stem = name[: -len(".warc.gz")]
            if stem.startswith("segment-"):
                try:
                    highest = max(highest, int(stem.split("-", 1)[1]))
                except ValueError:
                    continue
        return highest + 1

    def _segment_name(self, seq: int) -> str:
        return "segment-%04d.warc.gz" % seq

    def _ensure_writer(self) -> SegmentWriter:
        if self._writer is None:
            name = self._segment_name(self._segment_seq)
            self._writer = SegmentWriter(self.archive_dir, name)
            # 每个新段第一条是 warcinfo。
            info = build_warcinfo_record({
                "software": "warc-recorder/0.1 (python stdlib)",
                "format": "WARC File Format 1.1",
                "conformsTo": "ISO 28500:2017",
                "filename": name,
                "description": "request/response/metadata per HTTP hop; "
                               "payload bytes retain Content-Encoding",
            })
            self._write(info, is_warcinfo=True)
        elif self._writer.bytes_written >= self.segment_limit:
            # 轮换：密封旧段，开新段。
            self._writer.close()
            self._segment_seq += 1
            self._writer = None
            return self._ensure_writer()
        return self._writer

    def _write(self, record, *, is_warcinfo: bool = False,
               status_code: Optional[int] = None):
        writer = self._ensure_writer()
        member = writer.write_record(record)
        self.index.add_record(
            member, record,
            status_code=None if is_warcinfo else status_code)
        return member

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        self.index.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ #
    # 抓取与逐跳归档
    # ------------------------------------------------------------------ #
    def fetch(self, url: str, method: str = "GET",
              extra_headers=None, body: bytes = b"",
              max_hops: int = DEFAULT_MAX_HOPS,
              fetch_seq: Optional[int] = None) -> FetchResult:
        """执行一次逻辑抓取（含重定向链），全部跳数归档后返回结果。"""
        seq = fetch_seq if fetch_seq is not None else self._next_seq
        if fetch_seq is None:
            self._next_seq += 1
        result = FetchResult(fetch_seq=seq, url=url)
        current_url = url
        hop_index = 0
        visited: set[str] = set()
        current_method, current_body = method, body
        previous_request_id: Optional[str] = None

        while True:
            if hop_index >= max_hops:
                result.error = "超过最大跳数 %d" % max_hops
                break
            if current_url in visited:
                result.error = "重定向循环: %s" % current_url
                break
            visited.add(current_url)
            hop = self._archive_one_hop(
                current_url, current_method, extra_headers,
                current_body, seq, hop_index, previous_request_id)
            result.hops.append(hop)
            previous_request_id = hop.request_id
            if hop.error is not None or hop.response_id is None:
                result.error = hop.error or "无响应"
                break
            if hop.status_code not in REDIRECT_STATUSES:
                result.ok = hop.truncated is None
                result.final_status = hop.status_code
                result.truncated = hop.truncated
                break
            location = self._hop_location(hop)
            if not location:
                result.error = "重定向响应缺少 Location（hop %d）" % hop_index
                break
            current_url = urljoin(current_url, location)
            # 303 后按惯例改用 GET；307/308 保留方法与请求体。
            if hop.status_code == 303:
                current_method, current_body = "GET", b""
            hop_index += 1
        return result

    def _archive_one_hop(self, url, method, extra_headers, body,
                         seq, hop_index,
                         previous_request_id: Optional[str] = None) -> Hop:
        # 先按线上实际发送的字节构造请求（含 Host/Accept-Encoding/Connection）。
        wire = build_capture_request(url, method, extra_headers, body)
        request_record = build_request_record(
            url, wire.method, wire.headers, wire.body, seq, hop_index)
        try:
            _, raw = capture_raw(
                url, method, extra_headers=extra_headers,
                body=body, timeout=self.timeout)
        except CaptureError as exc:
            # 线上没有响应：只落 request + 错误 metadata，绝不伪造 response。
            self._write(request_record)
            meta_payload = {
                "fetch_seq": seq,
                "hop": hop_index,
                "url": url,
                "capture": "failed",
                "error": str(exc),
                "request_id": request_record.record_id,
                "response_id": None,
                "previous_hop_request_id": previous_request_id,
            }
            metadata = build_metadata_record(
                url, meta_payload, seq, hop_index,
                request_record.record_id, None)
            self._write(metadata)
            return Hop(hop_index, url, request_record.record_id, None,
                       metadata.record_id, None, None, str(exc), None, None)

        # 正常捕获（含截断）：response 内嵌回放形态，线上传输事实进 metadata。
        truncated = raw.truncation
        response_record = build_response_record(
            url, raw.status_line, raw.headers, raw.entity, seq, hop_index,
            request_record.record_id, truncated=truncated,
            ip_address=raw.peer_ip)
        payload_digest = response_record.header("WARC-Payload-Digest")
        block_digest = response_record.header("WARC-Block-Digest")
        meta_payload = {
            "fetch_seq": seq,
            "hop": hop_index,
            "url": url,
            "capture": "truncated" if truncated else "complete",
            "request_id": request_record.record_id,
            "response_id": response_record.record_id,
            "previous_hop_request_id": previous_request_id,
            "peer_ip": raw.peer_ip,
            "framing": raw.framing,
            "status_code": raw.status_code,
            "original_headers": [list(item) for item in raw.headers],
            "declared_content_length": raw.declared_length,
            "entity_length": len(raw.entity),
            "entity_digest": payload_digest,
            "block_digest": block_digest,
            "truncated": truncated,
            "chunk_boundaries": raw.chunk_boundaries,
            "trailers": [list(item) for item in raw.trailers],
            "location": header_get(raw.headers, "Location"),
        }
        # request 先写，再写并发的 response，最后写 metadata。
        self._write(request_record)
        self._write(response_record, status_code=raw.status_code)
        metadata = build_metadata_record(
            url, meta_payload, seq, hop_index,
            request_record.record_id, response_record.record_id)
        self._write(metadata)
        return Hop(hop_index, url, request_record.record_id,
                   response_record.record_id, metadata.record_id,
                   raw.status_code, truncated, None,
                   payload_digest, block_digest)

    def _hop_location(self, hop: Hop) -> Optional[str]:
        record = self._read_record_by_id(hop.metadata_id)
        payload = json.loads(record.block.decode("utf-8"))
        return payload.get("location")

    def _read_record_by_id(self, record_id: str):
        row = self.index.get(record_id)
        if row is None:
            raise RuntimeError("索引缺少记录 %s" % record_id)
        return read_member_record(
            self.archive_dir / row.segment,
            row.compressed_offset, row.compressed_length)

    def rebuild_index(self) -> dict:
        """索引丢失或怀疑漂移时，从全部段重建。"""
        return self.index.rebuild()
