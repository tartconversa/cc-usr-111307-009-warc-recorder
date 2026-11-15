"""原始 HTTP/1.1 采集（标准库 socket，禁用任何自动解压）。

与 http.client/requests 的关键区别：
- 不识别也不解压 Content-Encoding，gzip 实体的压缩字节原样归档；
- 自己解帧 chunked，把每个分块边界与 trailers 完整记录下来；
- 连接在声明长度到达前断开时返回 truncation='disconnect' 的 RawResponse，
  已收到的部分实体保留，绝不补造成功正文。

支持的传输帧：Content-Length 与 chunked。其余情况（无长度且非 1xx/204/304）
按 read-until-close 处理并在元数据中标注 framing='read-until-close'。
"""
from __future__ import annotations

import socket
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlsplit

from .records import HeaderList, build_request_block, header_get

DEFAULT_TIMEOUT = 15.0
_DEFAULT_USER_AGENT = "warc-recorder/0.1 (+std.socket)"
_NO_BODY_STATUSES = {204, 304}


class CaptureError(Exception):
    """建连或读取响应头失败：线上没有产生任何可归档的响应。"""


@dataclass
class RawResponse:
    """一次 HTTP 交换的线上事实。

    entity 是传输解帧后的实体字节（Content-Encoding 压缩原样保留）。
    chunk_boundaries/trailers 只有 chunked 响应才有。
    truncation 非 None 即表示响应不完整，回放侧必须拒绝当完整页使用。
    """

    version: str
    status_code: int
    reason: str
    headers: HeaderList          # 线上原始响应头，顺序与大小写保留
    entity: bytes
    framing: str                 # content-length | chunked | read-until-close | none
    chunk_boundaries: list[dict] = field(default_factory=list)
    trailers: HeaderList = field(default_factory=list)
    declared_length: Optional[int] = None
    truncation: Optional[str] = None  # disconnect | length | chunk-encoding
    peer_ip: Optional[str] = None

    @property
    def status_line(self) -> str:
        return "%s %d %s" % (self.version, self.status_code, self.reason)


@dataclass
class _WireRequest:
    method: str
    url: str
    headers: HeaderList
    body: bytes
    block: bytes


def build_capture_request(url: str, method: str = "GET",
                          extra_headers: Optional[HeaderList] = None,
                          body: bytes = b"") -> _WireRequest:
    """构造线上请求。默认带 Host、Accept-Encoding、Connection: close。

    显式发送 Accept-Encoding: gzip，让服务器可以返回压缩实体；
    压缩字节由本模块原样保留，不做透明解压。
    """
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise CaptureError("仅支持 http/https: %s" % url)
    headers: HeaderList = [
        ("Host", parts.netloc),
        ("Accept-Encoding", "gzip"),
        ("User-Agent", _DEFAULT_USER_AGENT),
        ("Connection", "close"),
    ]
    if body and header_get(headers, "Content-Length") is None:
        headers.append(("Content-Length", str(len(body))))
    if extra_headers:
        names = {k.lower() for k, _ in extra_headers}
        headers = [(k, v) for k, v in headers if k.lower() not in names]
        headers.extend(extra_headers)
    target = parts.path or "/"
    if parts.query:
        target += "?" + parts.query
    block = build_request_block(method, target, headers, body)
    return _WireRequest(method, url, headers, body, block)


class _LineSocket:
    """带缓冲的套接字：readline 读头，read 按字节数读体。"""

    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._buf = bytearray()

    def readline(self, limit: int = 65536) -> bytes:
        while b"\r\n" not in self._buf:
            if len(self._buf) > limit:
                raise CaptureError("响应行超过 %d 字节" % limit)
            chunk = self._sock.recv(4096)
            if not chunk:
                if self._buf:
                    return bytes(self._buf)
                return b""
            self._buf.extend(chunk)
        idx = self._buf.index(b"\r\n") + 2
        line = bytes(self._buf[:idx])
        del self._buf[:idx]
        return line

    def read(self, size: int) -> bytes:
        while len(self._buf) < size:
            chunk = self._sock.recv(min(65536, size - len(self._buf)))
            if not chunk:
                break
            self._buf.extend(chunk)
        take = min(size, len(self._buf))
        data = bytes(self._buf[:take])
        del self._buf[:take]
        return data

    def read_available(self) -> bytes:
        data = bytes(self._buf)
        self._buf.clear()
        chunks = [data]
        while True:
            chunk = self._sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)


def capture_raw(url: str, method: str = "GET",
                extra_headers: Optional[HeaderList] = None,
                body: bytes = b"",
                timeout: float = DEFAULT_TIMEOUT) -> tuple[_WireRequest, RawResponse]:
    """执行一次原始 HTTP 抓取，返回 (线上请求, 线上响应事实)。

    建连失败、响应头缺失/非法抛 CaptureError（无响应可归档，
    由 recorder 只落 request + 错误 metadata）。
    响应头之后的中途断开不抛异常，返回带 truncation 的 RawResponse。
    """
    request = build_capture_request(url, method, extra_headers, body)
    parts = urlsplit(url)
    host = parts.hostname
    port = parts.port or (443 if parts.scheme == "https" else 80)
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
    except OSError as exc:
        raise CaptureError("建连失败 %s:%d: %s" % (host, port, exc)) from exc
    peer_ip, _ = sock.getpeername()
    try:
        sock.settimeout(timeout)
        if parts.scheme == "https":
            import ssl

            ctx = ssl.create_default_context()
            sock = ctx.wrap_socket(sock, server_hostname=host)
        try:
            sock.sendall(request.block)
        except OSError as exc:
            raise CaptureError("发送请求失败: %s" % exc) from exc
        line_sock = _LineSocket(sock)
        response = _read_response(line_sock, peer_ip)
        return request, response
    except (socket.timeout, TimeoutError) as exc:
        raise CaptureError("读取超时: %s" % exc) from exc
    finally:
        try:
            sock.close()
        except OSError:
            pass


def _read_response(line_sock: _LineSocket, peer_ip: str) -> RawResponse:
    status_raw = line_sock.readline()
    if not status_raw:
        raise CaptureError("连接关闭前未返回任何响应字节")
    status_line = status_raw.rstrip(b"\r\n").decode("latin-1")
    pieces = status_line.split(" ", 2)
    if len(pieces) < 2 or not pieces[0].startswith("HTTP/"):
        raise CaptureError("非法状态行: %r" % status_line)
    version, status_text = pieces[0], pieces[1]
    reason = pieces[2] if len(pieces) > 2 else ""
    try:
        status_code = int(status_text)
    except ValueError as exc:
        raise CaptureError("非法状态码: %r" % status_line) from exc

    headers = _read_headers(line_sock)
    te = (header_get(headers, "Transfer-Encoding") or "").lower()
    declared = header_get(headers, "Content-Length")

    if status_code in _NO_BODY_STATUSES or 100 <= status_code < 200:
        return RawResponse(version, status_code, reason, headers, b"",
                           "none", peer_ip=peer_ip)
    if "chunked" in te:
        entity, chunks, trailers, trunc = _read_chunked(line_sock)
        return RawResponse(version, status_code, reason, headers, entity,
                           "chunked", chunks, trailers,
                           truncation=trunc, peer_ip=peer_ip)
    if declared is not None:
        try:
            length = int(declared)
        except ValueError as exc:
            raise CaptureError("非法 Content-Length: %r" % declared) from exc
        entity = line_sock.read(length)
        trunc = "disconnect" if len(entity) < length else None
        return RawResponse(version, status_code, reason, headers, entity,
                           "content-length", declared_length=length,
                           truncation=trunc, peer_ip=peer_ip)
    # 无传输长度：读到连接关闭（HTTP/1.1 + Connection: close 的合法情形）。
    entity = line_sock.read_available()
    return RawResponse(version, status_code, reason, headers, entity,
                       "read-until-close", peer_ip=peer_ip)


def _read_headers(line_sock: _LineSocket) -> HeaderList:
    headers: HeaderList = []
    while True:
        line = line_sock.readline()
        if not line:
            raise CaptureError("响应头读取中途连接关闭")
        if line in (b"\r\n", b"\n"):
            return headers
        raw = line.rstrip(b"\r\n")
        if raw[:1] in (b" ", b"\t") and headers:
            key, value = headers[-1]
            headers[-1] = (key, value + " " + raw.decode("latin-1").strip())
            continue
        colon = raw.find(b":")
        if colon <= 0:
            raise CaptureError("非法响应头: %r" % raw)
        headers.append((raw[:colon].decode("latin-1"),
                        raw[colon + 1:].decode("latin-1").strip()))


def _read_chunked(line_sock: _LineSocket):
    """解析 RFC 7230 chunked，返回 (实体, 分块边界, trailers, 截断原因)。

    分块边界记录偏移（相对于解帧实体）与长度，使 metadata 能完整
    复现线上的分块结构；trailers 原样保存。任何一步遇到提前 EOF
    都标记截断，已解出的实体保留。
    """
    entity = bytearray()
    chunks: list[dict] = []
    trailers: HeaderList = []
    truncation: Optional[str] = None
    while True:
        size_line = line_sock.readline()
        if not size_line:
            truncation = "disconnect"
            break
        size_text = size_line.rstrip(b"\r\n").split(b";", 1)[0].strip()
        try:
            size = int(size_text, 16)
        except ValueError as exc:
            raise CaptureError("非法 chunk-size: %r" % size_line) from exc
        if size == 0:
            trailers, truncation = _read_trailers(line_sock, truncation)
            break
        data = line_sock.read(size)
        if len(data) < size:
            entity.extend(data)
            chunks.append({"offset": len(entity) - len(data),
                           "length": len(data), "declared": size,
                           "complete": False})
            truncation = "disconnect"
            break
        crlf = line_sock.read(2)
        if crlf != b"\r\n":
            entity.extend(data)
            chunks.append({"offset": len(entity) - len(data),
                           "length": len(data), "declared": size,
                           "complete": False})
            truncation = "disconnect"
            break
        chunks.append({"offset": len(entity), "length": size,
                       "declared": size, "complete": True})
        entity.extend(data)
    return bytes(entity), chunks, trailers, truncation


def _read_trailers(line_sock: _LineSocket, prior_truncation: Optional[str]):
    trailers: HeaderList = []
    while True:
        line = line_sock.readline()
        if not line:
            return trailers, prior_truncation or "disconnect"
        if line in (b"\r\n", b"\n"):
            return trailers, prior_truncation
        raw = line.rstrip(b"\r\n")
        colon = raw.find(b":")
        if colon <= 0:
            return trailers, "chunk-encoding"
        trailers.append((raw[:colon].decode("latin-1"),
                         raw[colon + 1:].decode("latin-1").strip()))
