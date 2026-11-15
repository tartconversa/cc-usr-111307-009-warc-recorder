"""WARC 记录与内嵌 HTTP 消息的单元测试。"""
import unittest

from src.warc.records import (
    WARCRecord,
    build_request_record,
    build_response_record,
    build_metadata_record,
    digest_b32sha256,
    parse_warc_record,
    parse_http_message,
    rewrite_response_for_replay,
    header_get,
)


class RecordFormatTests(unittest.TestCase):
    def test_roundtrip_and_content_length(self):
        entity = "<html>中文</html>".encode("utf-8")
        rec = build_response_record(
            "http://x/page", "HTTP/1.1 200 OK",
            [("Content-Type", "text/html; charset=utf-8"),
             ("Content-Length", "999")],
            entity, fetch_seq=1, hop=0, request_id="<urn:uuid:req-1>")
        raw = rec.to_bytes()
        # WARC/1.1 起始行与记录结尾双 CRLF。
        self.assertTrue(raw.startswith(b"WARC/1.1\r\n"))
        self.assertTrue(raw.endswith(b"\r\n\r\n"))
        parsed = parse_warc_record(raw)
        self.assertEqual(parsed.record_type, "response")
        self.assertEqual(parsed.header("WARC-Concurrent-To"), "<urn:uuid:req-1>")
        # Content-Length 等于 block 字节数（含多字节中文）。
        declared = int(parsed.header("WARC-Content-Length"))
        self.assertEqual(declared, len(parsed.block))
        # 块摘要与实体摘要是两个不同字段，分别覆盖整块与实体。
        self.assertTrue(parsed.header("WARC-Block-Digest").startswith("sha256:"))
        self.assertEqual(
            parsed.header("WARC-Payload-Digest"),
            "sha256:" + digest_b32sha256(entity))
        self.assertNotEqual(
            parsed.header("WARC-Block-Digest"),
            parsed.header("WARC-Payload-Digest"))

    def test_embedded_response_has_no_conflicting_length(self):
        rec = build_response_record(
            "http://x/c", "HTTP/1.1 200 OK",
            [("Transfer-Encoding", "chunked"),
             ("Content-Length", "7"),
             ("Trailer", "X-Source-End"),
             ("Connection", "close")],
            b"abcdefg", 0, 0, "<urn:uuid:r>")
        message = parse_http_message(rec.block)
        names = {k.lower() for k, _ in message.headers}
        self.assertNotIn("transfer-encoding", names)
        self.assertNotIn("connection", names)
        self.assertNotIn("trailer", names)
        self.assertEqual(header_get(message.headers, "Content-Length"), "7")
        self.assertEqual(len([k for k, _ in message.headers
                              if k.lower() == "content-length"]), 1)

    def test_rewrite_truncated_entity_uses_actual_length(self):
        rewritten = rewrite_response_for_replay(
            [("Content-Length", "100"), ("Content-Encoding", "gzip")], 43)
        self.assertEqual(header_get(rewritten, "Content-Length"), "43")
        self.assertEqual(header_get(rewritten, "Content-Encoding"), "gzip")

    def test_request_record_contains_wire_request(self):
        rec = build_request_record(
            "http://x/a?q=1", "GET",
            [("Host", "x"), ("Accept-Encoding", "gzip")], b"", 2, 1)
        parsed = parse_warc_record(rec.to_bytes())
        self.assertTrue(parsed.block.startswith(b"GET /a?q=1 HTTP/1.1\r\n"))
        self.assertIn(b"Accept-Encoding: gzip\r\n", parsed.block)
        self.assertEqual(parsed.header("X-Fetch-Seq"), "2")
        self.assertEqual(parsed.header("X-Fetch-Hop"), "1")

    def test_metadata_record_json_block(self):
        rec = build_metadata_record(
            "http://x", {"framing": "chunked", "chunks": [{"offset": 0, "length": 7}]},
            1, 0, "<urn:uuid:req>", "<urn:uuid:resp>")
        parsed = parse_warc_record(rec.to_bytes())
        self.assertEqual(parsed.header("Content-Type"), "application/json")
        self.assertEqual(parsed.header("WARC-Refers-To"), "<urn:uuid:resp>")
        self.assertIn(b'"framing": "chunked"', parsed.block)

    def test_bad_block_digest_detected(self):
        rec = build_metadata_record("http://x", {"a": 1}, 0, 0,
                                    "<urn:uuid:q>", None)
        raw = bytearray(rec.to_bytes())
        raw[-8:-6] = b"zz"  # 破坏 block 尾部
        with self.assertRaises(ValueError):
            parse_warc_record(bytes(raw))

    def test_gzip_entity_digest_covers_compressed_bytes(self):
        import gzip
        text = "资源甲".encode("utf-8")
        compressed = gzip.compress(text, mtime=0)
        rec = build_response_record(
            "http://x/compressed", "HTTP/1.1 200 OK",
            [("Content-Encoding", "gzip"),
             ("Content-Length", str(len(compressed)))],
            compressed, 0, 0, "<urn:uuid:r>")
        parsed = parse_warc_record(rec.to_bytes())
        # 实体摘要针对压缩字节，不等于明文摘要。
        self.assertEqual(
            parsed.header("WARC-Payload-Digest"),
            "sha256:" + digest_b32sha256(compressed))
        self.assertNotEqual(
            parsed.header("WARC-Payload-Digest"),
            "sha256:" + digest_b32sha256(text))
        message = parse_http_message(parsed.block)
        self.assertEqual(message.body, compressed)
        self.assertEqual(gzip.decompress(message.body), text)


if __name__ == "__main__":
    unittest.main()
