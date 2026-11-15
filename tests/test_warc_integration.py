"""端到端：本机演练源 -> 原始采集 -> WARC 归档 -> 离线回放。

覆盖中文正文、gzip 实体、chunked+trailer、302 逐跳、中途断开、
索引重建、崩溃半 member 恢复与段轮换。全程只访问 127.0.0.1。
"""
import gzip
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src import source
from src.warc import (
    IndexStore,
    Recorder,
    ReplayStore,
    TruncatedResponseError,
    digest_b32sha256,
)
from src.warc.records import parse_http_message
from src.warc.segments import recover_segment, scan_segment, list_segment_names


class CaptureReplayIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = source.make_server(0)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = "http://127.0.0.1:%d" % cls.server.server_port
        cls.expected_html = source.DATA["content"].encode("utf-8")

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join()
        cls.server.server_close()

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _open(self, **kwargs):
        return Recorder(self.dir, timeout=10, **kwargs)

    # ---------------------------------------------------------------- #
    def test_chinese_page_capture_and_replay_bytes_match(self):
        with self._open() as rec:
            result = rec.fetch(self.base + "/page")
            self.assertTrue(result.ok)
            self.assertEqual(result.final_status, 200)
            seq = result.fetch_seq
            hop = result.final_hop
            self.assertIsNone(hop.truncated)
            # 线上捕获时即算出的实体摘要。
            live_digest = hop.payload_digest

        # 完全离线：原服务无需参与（这里只通过归档目录构造 ReplayStore）。
        with ReplayStore(self.dir) as replay:
            rows = replay.response_rows_for_url(self.base + "/page")
            self.assertEqual(len(rows), 1)
            replayed = replay.replay(rows[0])
            self.assertEqual(replayed.status_code, 200)
            self.assertEqual(replayed.entity, self.expected_html)
            self.assertIn("资源甲", replayed.entity.decode("utf-8"))
            # 离线实体重算摘要 == 线上捕获摘要。
            self.assertEqual(
                replayed.payload_digest,
                "sha256:" + digest_b32sha256(self.expected_html))
            self.assertEqual(replayed.payload_digest, live_digest)
            # 请求记录可通过并发关系取回。
            self.assertIsNotNone(replayed.request_record)
            self.assertTrue(
                replayed.request_record.block.startswith(b"GET /page HTTP/1.1"))

    def test_gzip_entity_kept_compressed(self):
        with self._open() as rec:
            result = rec.fetch(self.base + "/compressed")
            self.assertTrue(result.ok)
        with ReplayStore(self.dir) as replay:
            replayed = replay.replay_url(self.base + "/compressed")
            # 实体仍是 gzip 压缩字节，没有被透明解压。
            self.assertEqual(replayed.entity[:2], b"\x1f\x8b")
            self.assertEqual(gzip.decompress(replayed.entity), self.expected_html)
            headers = {k.lower(): v for k, v in replayed.headers}
            self.assertEqual(headers["content-encoding"], "gzip")
            self.assertEqual(int(headers["content-length"]), len(replayed.entity))
            # 实体摘要针对压缩字节。
            self.assertEqual(
                replayed.payload_digest,
                "sha256:" + digest_b32sha256(replayed.entity))

    def test_chunked_boundaries_and_trailers_saved_to_metadata(self):
        with self._open() as rec:
            result = rec.fetch(self.base + "/chunked")
            self.assertTrue(result.ok)
        with ReplayStore(self.dir) as replay:
            replayed = replay.replay_url(self.base + "/chunked")
            # 回放响应没有 Transfer-Encoding，只有实体长度。
            names = {k.lower() for k, v in replayed.headers}
            self.assertNotIn("transfer-encoding", names)
            self.assertEqual(
                int(dict(replayed.headers)["Content-Length"]),
                len(replayed.entity))
            self.assertEqual(replayed.entity, self.expected_html)
            # 分块边界与 trailers 在 metadata 中完整保留。
            meta = replayed.metadata
            self.assertEqual(meta["framing"], "chunked")
            self.assertTrue(all(c["complete"] for c in meta["chunk_boundaries"]))
            # 7 字节一块，边界偏移可还原线上分块。
            rebuilt = bytearray()
            for chunk in meta["chunk_boundaries"]:
                rebuilt += replayed.entity[
                    chunk["offset"]: chunk["offset"] + chunk["length"]]
            self.assertEqual(bytes(rebuilt), self.expected_html)
            self.assertIn(["X-Source-End", "yes"], meta["trailers"])

    def test_redirect_every_hop_archived_and_linked(self):
        with self._open() as rec:
            result = rec.fetch(self.base + "/jump")
            self.assertTrue(result.ok)
            self.assertEqual(result.final_status, 200)
            seq = result.fetch_seq
            # 两跳：/jump (302) -> /page (200)，每跳三条记录 + 段首 warcinfo。
            self.assertEqual(len(result.hops), 2)
            self.assertEqual(result.hops[0].status_code, 302)
            self.assertEqual(result.hops[1].status_code, 200)
            self.assertIsNotNone(result.hops[0].response_id)
            self.assertIsNotNone(result.hops[1].response_id)
        with ReplayStore(self.dir) as replay:
            rows = replay.fetch_rows(seq)
            responses = [r for r in rows if r.record_type == "response"]
            self.assertEqual([r.status_code for r in responses], [302, 200])
            self.assertEqual([r.hop for r in responses], [0, 1])
            # 两跳 URL 不同，302 响应体也独立归档（没有因为空体而省略）。
            first = replay.replay(responses[0])
            self.assertEqual(first.status_code, 302)
            self.assertEqual(first.metadata["location"], "/page")
            second = replay.replay(responses[1])
            self.assertEqual(second.entity, self.expected_html)
            # 同跳 request/response 并发关系存在且指向对应 request。
            self.assertEqual(responses[0].concurrent_to,
                             result.hops[0].request_id)

    def test_identical_body_still_separately_archived(self):
        with self._open() as rec:
            r1 = rec.fetch(self.base + "/page")
            r2 = rec.fetch(self.base + "/page")
        self.assertNotEqual(r1.final_hop.response_id, r2.final_hop.response_id)
        with ReplayStore(self.dir) as replay:
            rows = replay.response_rows_for_url(self.base + "/page")
            self.assertEqual(len(rows), 2)
            self.assertNotEqual(rows[0].record_id, rows[1].record_id)
            for row in rows:
                replay.replay(row)  # 两次都能独立回放

    def test_truncated_response_marked_and_rejected_as_full_page(self):
        with self._open() as rec:
            result = rec.fetch(self.base + "/truncated")
            # 抓取流程本身不抛错，但明确不是完整成功页。
            self.assertFalse(result.ok)
            self.assertEqual(result.truncated, "disconnect")
            self.assertEqual(result.final_status, 200)
        with ReplayStore(self.dir) as replay:
            rows = replay.response_rows_for_url(self.base + "/truncated")
            row = rows[0]
            self.assertEqual(row.truncated, "disconnect")
            # 默认拒绝当完整页使用。
            with self.assertRaises(TruncatedResponseError):
                replay.replay(row)
            # 审计场景可显式取回残体，但长度小于声明长度。
            partial = replay.replay(row, allow_truncated=True)
            self.assertTrue(len(partial.entity) < len(self.expected_html))
            self.assertEqual(
                partial.metadata["declared_content_length"],
                len(self.expected_html))
            self.assertEqual(
                partial.metadata["entity_length"], len(partial.entity))
            # 内嵌响应没有伪造长度：Content-Length 等于实到字节。
            message = parse_http_message(
                replay.read_record(row).block)
            self.assertEqual(
                int(dict(message.headers)["Content-Length"]),
                len(partial.entity))

    def test_chunked_truncation_keeps_partial_entity_and_marks_record(self):
        # 通过 __control 让源在 chunked 写出过程中切断：
        # 直接用 capture 层构造一个声明 chunked 但提前关连接的最小服务。
        import socket
        import threading

        body = self.expected_html
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        port = server_sock.getsockname()[1]

        def serve_once():
            conn, _ = server_sock.accept()
            conn.recv(4096)
            head = (
                "HTTP/1.1 200 OK\r\n"
                "Transfer-Encoding: chunked\r\n"
                "Connection: close\r\n\r\n"
            ).encode("latin-1")
            conn.sendall(head)
            conn.sendall(("%x\r\n" % 7).encode() + body[:7] + b"\r\n")
            # 第二个分块只发一半就关连接。
            conn.sendall(("%x\r\n" % 7).encode() + body[7:10])
            conn.close()
            server_sock.close()

        t = threading.Thread(target=serve_once, daemon=True)
        t.start()
        with self._open() as rec:
            result = rec.fetch("http://127.0.0.1:%d/stream" % port)
            t.join(timeout=5)
            self.assertFalse(result.ok)
            self.assertEqual(result.truncated, "disconnect")
        with ReplayStore(self.dir) as replay:
            row = replay.response_rows_for_url(
                "http://127.0.0.1:%d/stream" % port)[0]
            with self.assertRaises(TruncatedResponseError):
                replay.replay(row)
            partial = replay.replay(row, allow_truncated=True)
            # 已完整收到的第一个分块保留，第二个残块也保留但标记不完整。
            self.assertTrue(partial.entity.startswith(body[:7]))
            chunks = partial.metadata["chunk_boundaries"]
            self.assertFalse(chunks[-1]["complete"])
            self.assertTrue(chunks[0]["complete"])

    def test_redirect_hops_linked_through_record_ids(self):
        with self._open() as rec:
            result = rec.fetch(self.base + "/jump")
            seq = result.fetch_seq
        with ReplayStore(self.dir) as replay:
            rows = replay.fetch_rows(seq)
            metas = [r for r in rows if r.record_type == "metadata"]
            payloads = [
                json.loads(replay.read_record(r).block.decode("utf-8"))
                for r in metas]
            first, second = payloads
            self.assertIsNone(first["previous_hop_request_id"])
            self.assertEqual(
                second["previous_hop_request_id"], first["request_id"])
            # 同跳并发关系与 refers-to 完整。
            self.assertEqual(first["response_id"],
                             [r for r in rows if r.record_type == "response"
                              and r.hop == 0][0].record_id)

    def test_connection_failure_leaves_request_and_error_metadata_only(self):
        with self._open() as rec:
            # 端口 1 通常拒连；失败也必须消耗一个抓取序号。
            result = rec.fetch("http://127.0.0.1:1/nope")
            self.assertFalse(result.ok)
            self.assertIsNotNone(result.error)
            hop = result.final_hop
            self.assertIsNone(hop.response_id)
        with ReplayStore(self.dir) as replay:
            rows = replay.fetch_rows(result.fetch_seq)
            types = sorted(r.record_type for r in rows)
            self.assertEqual(types, ["metadata", "request"])
            meta = replay.read_record(
                [r for r in rows if r.record_type == "metadata"][0])
            payload = json.loads(meta.block.decode("utf-8"))
            self.assertEqual(payload["capture"], "failed")
            self.assertTrue(payload["error"])

    # ---------------------------------------------------------------- #
    def test_reopen_reconciles_stale_and_missing_index_rows(self):
        with self._open() as rec:
            rec.fetch(self.base + "/page")
            segment = list_segment_names(self.dir)[0]
            good_count = rec.index.count()
        seg_path = self.dir / segment
        # 漂移 1：索引残留一行指向段尾之外的假 member。
        conn = sqlite3.connect(self.dir / "index.sqlite")
        conn.execute(
            "INSERT OR REPLACE INTO records VALUES "
            "('rid-ghost',?,999999,42,'response','http://ghost',99,0,"
            "200,NULL,NULL,NULL,NULL,NULL)", (segment,))
        conn.commit()
        conn.close()
        # 漂移 2：段尾追加半个 member（落盘成功但索引未登记，且本身不完整）。
        with open(seg_path, "ab") as fh:
            fh.write(gzip.compress(b"half")[:-5])
        # 重新打开：残尾隔离、ghost 行清除，完整 member 一行不少。
        with Recorder(self.dir, timeout=10) as rec2:
            self.assertEqual(rec2.index.count(), good_count)
            self.assertIsNone(rec2.index.get("rid-ghost"))
            # 追加抓取仍正常并可回放。
            result = rec2.fetch(self.base + "/compressed")
            self.assertTrue(result.ok)
        with ReplayStore(self.dir) as replay:
            replay.replay_url(self.base + "/compressed")
            self.assertEqual(
                replay.replay_url(self.base + "/page").entity,
                self.expected_html)

    def test_index_rebuild_from_segments(self):
        with self._open() as rec:
            for path in ("/page", "/compressed", "/jump"):
                rec.fetch(self.base + path)
            before = rec.index.count()
        # 模拟索引文件丢失：删除后从段完整重建。
        db = self.dir / "index.sqlite"
        for suffix in ("", "-wal", "-shm"):
            p = self.dir / ("index.sqlite" + suffix)
            if p.exists():
                p.unlink()
        self.assertFalse(db.exists())
        with IndexStore(self.dir) as fresh:
            report = fresh.rebuild()
            self.assertEqual(fresh.count(), before)
        # 重建后的索引仍可精确定位并回放。
        with ReplayStore(self.dir) as replay:
            replayed = replay.replay_url(self.base + "/page")
            self.assertEqual(replayed.entity, self.expected_html)
            self.assertGreaterEqual(report["segments"], 1)

    def test_half_member_crash_then_append_and_rebuild(self):
        # 第一段：正常抓取后，人为在段尾追加半个 gzip member 模拟崩溃。
        with self._open() as rec:
            rec.fetch(self.base + "/page")
            segment = list_segment_names(self.dir)[0]
        seg_path = self.dir / segment
        good_size = seg_path.stat().st_size
        with open(seg_path, "ab") as fh:
            fh.write(gzip.compress(b"never finished warc block")[:-9])
            fh.flush()
        scanned = scan_segment(seg_path)
        self.assertIsNotNone(scanned.tail_offset)

        # 重新打开归档：残尾被隔离，不完整记录不进索引；追加写入进入新段。
        with Recorder(self.dir, timeout=10) as rec:
            reports = [r for r in rec.recovery_reports if r["status"] == "recovered"]
            self.assertEqual(len(reports), 1)
            self.assertTrue((self.dir / (segment + ".quarantine")).exists())
            # 旧段完整 member 仍可回放。
            before_rows = rec.index.count()
            result = rec.fetch(self.base + "/chunked")
            self.assertTrue(result.ok)
        self.assertEqual(len(list_segment_names(self.dir)), 2)
        with ReplayStore(self.dir) as replay:
            # 残尾没有被登记成任何记录。
            self.assertEqual(
                len(replay.response_rows_for_url("")), 0)
            replayed = replay.replay_url(self.base + "/chunked")
            self.assertEqual(replayed.entity, self.expected_html)
            page_rows = replay.response_rows_for_url(self.base + "/page")
            self.assertEqual(len(page_rows), 1)

        # 再删索引重建：结果与现有索引行数一致，且全部可回放。
        with IndexStore(self.dir) as idx:
            expected = idx.count()
        for suffix in ("", "-wal", "-shm"):
            p = self.dir / ("index.sqlite" + suffix)
            if p.exists():
                p.unlink()
        with IndexStore(self.dir) as rebuilt:
            rebuilt.rebuild()
            self.assertEqual(rebuilt.count(), expected)
        with ReplayStore(self.dir) as replay:
            for row in replay.all_response_rows():
                replay.replay(row, allow_truncated=True)

    def test_segment_rotation_by_size(self):
        # 极小轮换阈值：每条记录都可能触发新段。
        with self._open(segment_limit=64) as rec:
            rec.fetch(self.base + "/page")
            rec.fetch(self.base + "/compressed")
        names = list_segment_names(self.dir)
        self.assertGreater(len(names), 1)
        # 每个段都以 warcinfo 开头。
        from src.warc.segments import read_member_record
        for name in names:
            scan = scan_segment(self.dir / name)
            self.assertIsNone(scan.tail_offset)
            first = read_member_record(
                self.dir / name,
                scan.members[0].compressed_offset,
                scan.members[0].compressed_length)
            self.assertEqual(first.record_type, "warcinfo")
        # 跨段记录照样全部可回放。
        with ReplayStore(self.dir) as replay:
            self.assertEqual(
                replay.replay_url(self.base + "/page").entity,
                self.expected_html)

    def test_sqlite_index_columns(self):
        with self._open() as rec:
            result = rec.fetch(self.base + "/jump")
            seq = result.fetch_seq
        db = sqlite3.connect(self.dir / "index.sqlite")
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT * FROM records WHERE fetch_seq=? ORDER BY hop, record_type",
            (seq,)).fetchall()
        # 两跳 * (request/response/metadata) = 6 行。
        self.assertEqual(len(rows), 6)
        for row in rows:
            self.assertTrue(row["segment"].endswith(".warc.gz"))
            self.assertGreaterEqual(row["compressed_offset"], 0)
            self.assertGreater(row["compressed_length"], 0)
            self.assertTrue(row["record_id"].startswith("<urn:uuid:"))
            self.assertTrue(row["url"].startswith(
                "http://127.0.0.1:%d/" % self.server.server_port))
        db.close()


if __name__ == "__main__":
    unittest.main()
