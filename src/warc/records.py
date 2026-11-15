"""WARC/1.1 记录与内嵌 HTTP 消息。

记录格式遵循 WARC ISO 28500（WARC/1.1）：
    WARC/1.1\\r\\n
    WARC-Name: value\\r\\n
    ...
    \\r\\n
    <block>
    \\r\\n\\r\\n

两种摘要严格分开：
- 块摘要 WARC-Block-Digest：覆盖整个内嵌 HTTP 消息（状态行/请求行 + 头 + 实体）的字节；
- 实体摘要 WARC-Payload-Digest：仅覆盖 HTTP 传输解帧后的实体字节。
  若响应带 Content-Encoding，实体仍是压缩字节（gzip 不解压），摘要即压缩字节的摘要。
"""
from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Optional
from urllib.parse import urlsplit

WARC_VERSION = "WARC/1.1"
CRLF = b"\r\n"
WARC_TYPES = ("warcinfo", "response", "request", "metadata", "revisit", "resource")

# 头部按顺序保存（同名头、原始顺序都不能丢），元素为 (name, value)。
HeaderList = list[tuple[str, str]]


def digest_b32sha256(data: bytes) -> str:
    """WARC 规范使用的未填充 base32 小写 sha256 摘要。"""
    return base64.b32encode(hashlib.sha256(data).digest()).decode("ascii").rstrip("=").lower()


def warc_now() -> str:
    """W3C ISO 8601 秒级 UTC 时间，例如 2026-11-15T09:30:00Z。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _encode_headers(headers: HeaderList) -> bytes:
    out = bytearray()
    for name, value in headers:
        # WARC 头值不允许裸 CR/LF。
        if any(ch in value for ch in "\r\n") or any(ch in name for ch in "\r\n "):
            raise ValueError("WARC 头部名/值不允许 CR/LF: %r" % name)
        out += name.encode("latin-1")
        out += b": "
        out += value.encode("latin-1")
        out += CRLF
    return bytes(out)


@dataclass
class WARCRecord:
    """一条 WARC 记录（已序列化结构，block 为完整内嵌内容）。"""

    record_type: str
    headers: HeaderList
    block: bytes
    record_id: Optional[str] = None
    content_type: Optional[str] = None

    def header(self, name: str) -> Optional[str]:
        """按大小写不敏感方式取第一个同名 WARC 头。"""
        return header_get(self.headers, name)

    def to_bytes(self) -> bytes:
        """序列化为磁盘上的 WARC 记录字节（不含 gzip 封装）。

        WARC-Record-ID / Content-Type / WARC-Content-Length 自动补齐，
        Content-Length 是 block 自身的字节数，与 gzip member 的压缩长度无关。
        """
        if self.record_type not in WARC_TYPES:
            raise ValueError("未知 WARC 类型: %s" % self.record_type)
        rid = self.record_id or new_record_id()
        self.record_id = rid
        # WARC/1.1 记录以裸起始行 WARC/1.1 开始，WARC-Version 不是头字段。
        warc_headers: HeaderList = [("WARC-Type", self.record_type)]
        warc_headers.append(("WARC-Record-ID", rid))
        warc_headers.append(("WARC-Date", self.header("WARC-Date") or warc_now()))
        if self.content_type:
            warc_headers.append(("Content-Type", self.content_type))
        # 调用方自定义头（WARC-Target-URI、WARC-Concurrent-To 等），
        # 跳过与固定头重复的名字。
        fixed = {n.lower() for n, _ in warc_headers}
        for name, value in self.headers:
            if name.lower() not in fixed:
                warc_headers.append((name, value))
        warc_headers.append(("WARC-Content-Length", str(len(self.block))))
        return (
            b"WARC/1.1\r\n"
            + _encode_headers(warc_headers)
            + CRLF
            + self.block
            + CRLF
            + CRLF
        )


def new_record_id() -> str:
    import uuid

    return "<urn:uuid:%s>" % uuid.uuid4()


# --------------------------------------------------------------------------- #
# HTTP 头工具
# --------------------------------------------------------------------------- #

def header_get(headers: HeaderList, name: str, default: Optional[str] = None) -> Optional[str]:
    name = name.lower()
    for key, value in headers:
        if key.lower() == name:
            return value
    return default


def header_remove(headers: HeaderList, names: Iterable[str]) -> HeaderList:
    drop = {n.lower() for n in names}
    return [(k, v) for k, v in headers if k.lower() not in drop]


# HTTP 逐跳头（RFC 7230 6.1），回放重写时一律移除。
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade",
}


def _http_head_line(line: bytes) -> bytes:
    return line.rstrip(b"\r\n")


def build_request_block(method: str, target: str, headers: HeaderList,
                        body: bytes = b"", version: str = "HTTP/1.1") -> bytes:
    """构造实际发往对端的请求行+头+体，保证归档的就是线上发送的字节序列。

    target 用 origin-form（/path?query），与套接字请求一致。
    """
    out = bytearray()
    out += f"{method} {target} {version}".encode("latin-1") + CRLF
    out += _encode_headers(headers)
    out += CRLF
    out += body
    return bytes(out)


def build_response_block(status_line: str, headers: HeaderList, entity: bytes) -> bytes:
    """构造内嵌响应块：状态行 + 头（空行）+ 实体字节。"""
    out = bytearray()
    out += status_line.encode("latin-1") + CRLF
    out += _encode_headers(headers)
    out += CRLF
    out += entity
    return bytes(out)


@dataclass
class HTTPMessage:
    """解析后的内嵌 HTTP 消息。"""

    start_line: str
    headers: HeaderList
    body: bytes
    head_bytes: bytes  # 起始行+头部+空行的原始字节，块摘要复核时可重建


def parse_http_message(block: bytes) -> HTTPMessage:
    """按 RFC 7230 切分起始行、头与消息体。只解析，不做任何解压。"""
    sep = block.find(CRLF + CRLF)
    if sep < 0:
        raise ValueError("HTTP 消息缺少头部结束空行")
    head = block[: sep + 2]
    body = block[sep + 4:]
    lines = block[:sep].split(CRLF)
    start_line = lines[0].decode("latin-1")
    headers: HeaderList = []
    for raw in lines[1:]:
        if not raw:
            continue
        if raw[:1] in (b" ", b"\t"):
            # 折叠头，按 RFC 7230 归并到上一个头。
            if not headers:
                raise ValueError("非法折叠头")
            k, v = headers[-1]
            headers[-1] = (k, v + " " + raw.decode("latin-1").strip())
            continue
        colon = raw.find(b":")
        if colon <= 0:
            raise ValueError("非法 HTTP 头: %r" % raw)
        headers.append((
            raw[:colon].decode("latin-1"),
            raw[colon + 1:].decode("latin-1").strip(),
        ))
    return HTTPMessage(start_line, headers, body, head)


def rewrite_response_for_replay(headers: HeaderList, entity_len: int) -> HeaderList:
    """把捕获时的传输头改写为可直接回放的实体头。

    规则（任务硬性要求）：
    - 移除 Transfer-Encoding 及全部逐跳头；
    - 写入等于实际实体字节数的 Content-Length；
    - 已存在的 Content-Length 一律替换，杜绝同一条响应同时携带
      Transfer-Encoding 与 Content-Length、或两个冲突长度的情况。
    """
    kept = header_remove(headers, HOP_BY_HOP | {"content-length"})
    kept.append(("Content-Length", str(entity_len)))
    return kept


# --------------------------------------------------------------------------- #
# WARC 记录构建
# --------------------------------------------------------------------------- #

@dataclass
class RecordBundle:
    """一次 HTTP 交换产出的 request/response/metadata 三条记录及其关联 ID。"""

    request: WARCRecord
    response: Optional[WARCRecord]
    metadata: WARCRecord
    request_id: str
    response_id: Optional[str]
    metadata_id: str


def _common_warc_headers(url: str, fetch_seq: int, hop: int,
                         extra: Optional[HeaderList] = None) -> HeaderList:
    """自定义 WARC 扩展头。

    X-Fetch-Seq / X-Fetch-Hop 是本归档的自定义扩展头（WARC 允许 X- 前缀），
    作用是让索引重建只扫描记录头即可拿到抓取序号，不必解析 metadata 块。
    """
    headers: HeaderList = [
        ("WARC-Target-URI", url),
        ("WARC-Date", warc_now()),
        ("X-Fetch-Seq", str(fetch_seq)),
        ("X-Fetch-Hop", str(hop)),
    ]
    if extra:
        headers.extend(extra)
    return headers


def build_request_record(url: str, method: str, req_headers: HeaderList,
                         req_body: bytes, fetch_seq: int, hop: int,
                         concurrent_to: Optional[str] = None) -> WARCRecord:
    block = build_request_block(method, _origin_form(url), req_headers, req_body)
    extra: HeaderList = []
    if concurrent_to:
        extra.append(("WARC-Concurrent-To", concurrent_to))
    record = WARCRecord(
        record_type="request",
        headers=_common_warc_headers(url, fetch_seq, hop, extra),
        block=block,
        content_type="application/http;msgtype=request",
    )
    record.to_bytes()  # 预生成 record_id
    return record


def build_response_record(url: str, status_line: str, http_headers: HeaderList,
                          entity: bytes, fetch_seq: int, hop: int,
                          request_id: str,
                          truncated: Optional[str] = None,
                          ip_address: Optional[str] = None) -> WARCRecord:
    """归档响应记录。

    entity 是传输解帧后的实体：Content-Encoding 压缩字节原样保留。
    block 内嵌的是已重写的回放形态（无 Transfer-Encoding，Content-Length
    等于实体长度）；捕获时的原始传输头与分块边界保存在 metadata。
    Payload-Digest 覆盖实体字节，Block-Digest 覆盖整个内嵌 HTTP 块。
    """
    replayed_headers = rewrite_response_for_replay(http_headers, len(entity))
    block = build_response_block(status_line, replayed_headers, entity)
    extra: HeaderList = [
        ("WARC-Concurrent-To", request_id),
        ("WARC-Block-Digest", "sha256:" + digest_b32sha256(block)),
        ("WARC-Payload-Digest", "sha256:" + digest_b32sha256(entity)),
    ]
    if ip_address:
        extra.append(("WARC-IP-Address", ip_address))
    if truncated:
        # WARC 1.1 标准截断标记：disconnect/error/length/time。
        extra.append(("WARC-Truncated", truncated))
    record = WARCRecord(
        record_type="response",
        headers=_common_warc_headers(url, fetch_seq, hop, extra),
        block=block,
        content_type="application/http;msgtype=response",
    )
    record.to_bytes()
    return record


def build_metadata_record(url: str, payload: dict, fetch_seq: int, hop: int,
                          request_id: str,
                          response_id: Optional[str]) -> WARCRecord:
    import json

    block = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    extra: HeaderList = [("WARC-Concurrent-To", request_id)]
    if response_id:
        extra.append(("WARC-Refers-To", response_id))
    extra.append(("WARC-Block-Digest", "sha256:" + digest_b32sha256(block)))
    record = WARCRecord(
        record_type="metadata",
        headers=_common_warc_headers(url, fetch_seq, hop, extra),
        block=block,
        content_type="application/json",
    )
    record.to_bytes()
    return record


def build_warcinfo_record(payload: dict) -> WARCRecord:
    import json

    lines = []
    for key in sorted(payload):
        lines.append("%s: %s" % (key, payload[key]))
    block = ("\r\n".join(lines)).encode("utf-8")
    record = WARCRecord(
        record_type="warcinfo",
        headers=[("WARC-Filename", payload.get("filename", ""))],
        block=block,
        content_type="application/warc-fields",
    )
    record.to_bytes()
    return record


def _origin_form(url: str) -> str:
    parts = urlsplit(url)
    target = parts.path or "/"
    if parts.query:
        target += "?" + parts.query
    return target


# --------------------------------------------------------------------------- #
# WARC 记录解析（索引重建与回放共用）
# --------------------------------------------------------------------------- #

@dataclass
class ParsedWARC:
    record: WARCRecord
    block_digest_ok: Optional[bool] = field(default=None)


def parse_warc_record(data: bytes, verify_digest: bool = True) -> WARCRecord:
    """从一条完整 WARC 记录字节还原结构，并校验 WARC-Content-Length 与块摘要。"""
    sep = data.find(CRLF + CRLF)
    if sep < 0:
        raise ValueError("WARC 记录缺少头部空行")
    header_lines = data[:sep].split(CRLF)
    if header_lines[0] != b"WARC/1.1":
        raise ValueError("不是 WARC/1.1 记录: %r" % header_lines[0][:20])
    headers: HeaderList = []
    for raw in header_lines[1:]:
        colon = raw.find(b":")
        if colon <= 0:
            raise ValueError("非法 WARC 头: %r" % raw)
        headers.append((raw[:colon].decode("latin-1"),
                        raw[colon + 1:].decode("latin-1").strip()))
    declared_len = int(header_get(headers, "WARC-Content-Length", "-1"))
    block_start = sep + 4
    block = data[block_start:]
    if not block.endswith(CRLF + CRLF):
        raise ValueError("WARC 记录缺少结尾 CRLF CRLF")
    block = block[:-4]
    if declared_len != len(block):
        raise ValueError(
            "WARC-Content-Length=%d 与块实际字节 %d 不符" % (declared_len, len(block)))
    record = WARCRecord(
        record_type=header_get(headers, "WARC-Type") or "",
        headers=headers,
        block=block,
        record_id=header_get(headers, "WARC-Record-ID"),
        content_type=header_get(headers, "Content-Type"),
    )
    if verify_digest:
        declared = header_get(headers, "WARC-Block-Digest")
        if declared and declared.startswith("sha256:"):
            actual = "sha256:" + digest_b32sha256(block)
            if actual != declared:
                raise ValueError("块摘要校验失败: %s != %s" % (declared, actual))
    return record
