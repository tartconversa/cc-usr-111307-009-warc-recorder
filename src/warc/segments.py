"""WARC 归档段：每条 WARC 记录一个独立 gzip member。

磁盘布局（segment，后缀 .warc.gz）：

    [gzip member: warcinfo][gzip member: record][gzip member: record] ...

member 之间互相独立，索引保存每个 member 的压缩偏移与压缩长度，
回放时 seek 到偏移、只读这一个 member 并解压，不触碰段内其余字节，
因此不需要先解压整个大段。

崩溃恢复：写入中断可能在段尾留下半个 member（gzip 流没有正常结束、
CRC32/ISIZE 无法验证）。scan_segment 能精确定位最后一个完整 member 的
边界，recover_segment 把残尾隔离到 .quarantine 证据文件并截断段，
恢复后不完整记录绝不会进入索引、不会被当成可回放数据。
"""
from __future__ import annotations

import gzip
import os
import zlib
from dataclasses import dataclass
from pathlib import Path

from .records import WARCRecord, parse_warc_record

_GZIP_WBITS = 16 + zlib.MAX_WBITS
_SCAN_CHUNK = 64 * 1024
SEGMENT_SUFFIX = ".warc.gz"


@dataclass
class MemberInfo:
    """一个 gzip member 的定位信息（索引一行的核心）。"""

    segment: str
    compressed_offset: int
    compressed_length: int
    record_id: str
    record_type: str


@dataclass
class ScanResult:
    members: list[MemberInfo]
    # None 表示段完整；否则给出残尾起始偏移与原因（truncated/crc/format）。
    tail_offset: int | None
    tail_reason: str | None


class SegmentWriter:
    """向单个段追加 gzip member。一个 member 就是一条 WARC 记录。"""

    def __init__(self, directory: str | Path, name: str):
        self.directory = Path(directory)
        self.name = name
        self.path = self.directory / name
        self.directory.mkdir(parents=True, exist_ok=True)
        # ab：恢复场景下段可能已存在且密封完好，但正常轮换总是换新文件名。
        self._fh = open(self.path, "ab")
        self.bytes_written = self._fh.tell()

    def write_record(self, record: WARCRecord) -> MemberInfo:
        """把一条 WARC 记录压成独立 gzip member 落盘，逐条 fsync。"""
        offset = self._fh.tell()
        # mtime=0 使 member 头确定，便于比对与测试。
        with gzip.GzipFile(filename="", mode="wb", fileobj=self._fh, mtime=0) as gz:
            gz.write(record.to_bytes())
        self._fh.flush()
        os.fsync(self._fh.fileno())
        end = self._fh.tell()
        length = end - offset
        if length <= 0:
            raise RuntimeError("gzip member 写入长度异常")
        self.bytes_written = end
        return MemberInfo(self.name, offset, length,
                          record.record_id or "", record.record_type)

    def close(self) -> None:
        self._fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _new_decompressor() -> zlib.Decompress:
    return zlib.decompressobj(_GZIP_WBITS)


def scan_segment(path: str | Path) -> ScanResult:
    """流式扫描段，定位每个完整 gzip member 的精确边界。

    不依赖外部工具，不把整个段读进内存。zlib 在 gzip 流结束时会校验
    CRC32 与 ISIZE；校验失败或流未结束即 EOF，分别判定为 crc / truncated，
    残尾边界作为 tail_offset 返回。
    """
    path = Path(path)
    members: list[MemberInfo] = []
    decomp = _new_decompressor()
    out = bytearray()
    member_start = 0
    member_fed = 0      # 当前 member 已喂给解压器的字节数
    pending = b""

    with open(path, "rb") as fh:
        while True:
            if not pending:
                chunk = fh.read(_SCAN_CHUNK)
                if not chunk:
                    break
                pending = chunk
            try:
                produced = decomp.decompress(pending)
            except zlib.error as exc:
                # gzip 魔数错误、CRC32 不符等：残尾从当前 member 起点开始。
                return ScanResult(members, member_start, "crc: %s" % exc)
            out.extend(produced)
            member_fed += len(pending)
            if decomp.eof:
                # 本次喂入里属于当前 member 的字节数；剩下的属于下一个 member。
                consumed = member_fed - len(decomp.unused_data)
                member_end = member_start + consumed
                try:
                    out.extend(decomp.flush())
                    record = parse_warc_record(bytes(out))
                except (ValueError, zlib.error) as exc:
                    # gzip 完整但里面不是合法 WARC（写了一半被当作收尾）。
                    return ScanResult(members, member_start,
                                      "warc: %s" % exc)
                members.append(MemberInfo(
                    path.name, member_start, member_end - member_start,
                    record.record_id or "", record.record_type))
                pending = decomp.unused_data
                member_start = member_end
                decomp = _new_decompressor()
                out = bytearray()
                member_fed = 0
            else:
                # 当前 member 还没结束，继续读盘。
                pending = b""

    if member_fed > 0 or out:
        # 段尾是半个 member：gzip 流从未正常结束。
        return ScanResult(members, member_start, "truncated")
    return ScanResult(members, None, None)


def read_member_record(path: str | Path, offset: int, length: int) -> WARCRecord:
    """随机读取一个 gzip member 并解析为 WARC 记录。

    严格校验：读到的压缩字节必须恰好构成一个完整 member
    （无多余尾字节、流正常结束且 CRC 通过），否则报错——
    这保证索引里的偏移/长度确实能独立定位一条记录。
    """
    path = Path(path)
    with open(path, "rb") as fh:
        fh.seek(offset)
        comp = fh.read(length)
    if len(comp) != length:
        raise ValueError("member 读取不完整: 需要 %d 字节，实得 %d"
                         % (length, len(comp)))
    decomp = _new_decompressor()
    try:
        raw = decomp.decompress(comp)
        raw += decomp.flush()
    except zlib.error as exc:
        raise ValueError("member 解压失败（偏移 %d）: %s" % (offset, exc)) from exc
    if not decomp.eof or decomp.unused_data:
        raise ValueError("member 边界错误：压缩范围不是恰好一条记录（偏移 %d）"
                         % offset)
    return parse_warc_record(raw)


def recover_segment(path: str | Path) -> dict:
    """识别并隔离段尾残尾，使段只保留完整 member。

    返回恢复报告。残尾原样复制到 <segment>.quarantine（保留证据，不删除），
    段文件被截断到最后一个完整 member 边界。完整段不动。
    """
    path = Path(path)
    result = scan_segment(path)
    if result.tail_offset is None:
        return {"segment": path.name, "status": "clean",
                "members": len(result.members), "quarantined_bytes": 0}
    quarantine = path.with_name(path.name + ".quarantine")
    with open(path, "rb") as fh:
        fh.seek(result.tail_offset)
        tail = fh.read()
    # 残尾先写证据文件并 fsync，再截断原段。
    with open(quarantine, "wb") as qf:
        qf.write(tail)
        qf.flush()
        os.fsync(qf.fileno())
    with open(path, "r+b") as fh:
        fh.truncate(result.tail_offset)
        fh.flush()
        os.fsync(fh.fileno())
    return {"segment": path.name, "status": "recovered",
            "reason": result.tail_reason,
            "members_kept": len(result.members),
            "quarantined_bytes": len(tail),
            "quarantine": quarantine.name}


def list_segment_names(directory: str | Path) -> list[str]:
    directory = Path(directory)
    if not directory.exists():
        return []
    return sorted(p.name for p in directory.iterdir()
                  if p.is_file() and p.name.endswith(SEGMENT_SUFFIX))
