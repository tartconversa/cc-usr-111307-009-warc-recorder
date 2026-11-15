"""WARC/1.1 归档与离线回放（纯标准库）。

模块划分：
- records: WARC/1.1 记录与内嵌 HTTP 消息的构建、解析、摘要
- segments: 多 gzip member 归档段，随机定位读取与崩溃恢复
- indexstore: SQLite 索引（段/压缩偏移/记录ID/URL/抓取序号）
- capture: 原始套接字 HTTP/1.1 采集，不做自动解压
- recorder: 抓取编排，重定向逐跳归档，失败/截断显式标记
- replay: 离线回放，按 member 定位，校验块摘要与实体摘要
"""
from .records import (
    WARC_VERSION,
    WARCRecord,
    build_request_record,
    build_response_record,
    build_metadata_record,
    build_warcinfo_record,
    parse_warc_record,
    digest_b32sha256,
    HeaderList,
    build_request_block,
    build_response_block,
    parse_http_message,
    rewrite_response_for_replay,
    header_get,
    header_remove,
)
from .segments import (
    SegmentWriter,
    MemberInfo,
    scan_segment,
    read_member_record,
    recover_segment,
    list_segment_names,
)
from .indexstore import IndexStore
from .capture import CaptureError, RawResponse, capture_raw, build_capture_request
from .recorder import Recorder, FetchResult, Hop
from .replay import ReplayStore, ReplayedResponse, ReplayError, TruncatedResponseError

__all__ = [
    "WARC_VERSION", "WARCRecord",
    "build_request_record", "build_response_record", "build_metadata_record",
    "build_warcinfo_record", "parse_warc_record", "digest_b32sha256",
    "HeaderList", "build_request_block", "build_response_block",
    "parse_http_message", "rewrite_response_for_replay",
    "header_get", "header_remove",
    "SegmentWriter", "MemberInfo", "scan_segment", "read_member_record",
    "recover_segment", "list_segment_names",
    "IndexStore",
    "CaptureError", "RawResponse", "capture_raw", "build_capture_request",
    "Recorder", "FetchResult", "Hop",
    "ReplayStore", "ReplayedResponse", "ReplayError", "TruncatedResponseError",
]
