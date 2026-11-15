"""gzip member 段：随机定位、半 member 识别与隔离恢复。"""
import gzip
import tempfile
import unittest
from pathlib import Path

from src.warc.records import WARCRecord
from src.warc.segments import (
    SegmentWriter,
    scan_segment,
    read_member_record,
    recover_segment,
)


def make_record(mark: str) -> WARCRecord:
    return WARCRecord(
        record_type="metadata",
        headers=[("WARC-Date", "2026-11-15T00:00:00Z")],
        block=("记录-%s" % mark).encode("utf-8"),
        content_type="application/json",
    )


class SegmentTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_members_are_independent_gzip_members(self):
        with SegmentWriter(self.dir, "segment-0000.warc.gz") as writer:
            infos = [writer.write_record(make_record(str(i))) for i in range(4)]
        path = self.dir / "segment-0000.warc.gz"
        data = path.read_bytes()
        # 每个 member 以独立 gzip 魔数开头，共 4 个 member。
        self.assertEqual(data.count(b"\x1f\x8b"), 4)
        # 偏移单调递增，压缩长度为正。
        offsets = [m.compressed_offset for m in infos]
        self.assertEqual(offsets, sorted(offsets))
        self.assertTrue(all(m.compressed_length > 0 for m in infos))
        # 不展开整段：直接定位第三个 member 即可读出对应记录。
        target = infos[2]
        record = read_member_record(path, target.compressed_offset,
                                    target.compressed_length)
        self.assertEqual(record.block, "记录-2".encode("utf-8"))

    def test_wrong_member_range_is_rejected(self):
        with SegmentWriter(self.dir, "segment-0000.warc.gz") as writer:
            infos = [writer.write_record(make_record(str(i))) for i in range(3)]
        path = self.dir / "segment-0000.warc.gz"
        # 故意多读一个字节：member 边界校验必须报错，而不是静默读到下一条。
        with self.assertRaises(ValueError):
            read_member_record(path, infos[1].compressed_offset,
                               infos[1].compressed_length + 1)
        # 偏移错位同样报错。
        with self.assertRaises(ValueError):
            read_member_record(path, infos[1].compressed_offset + 1,
                               infos[1].compressed_length)

    def test_truncated_tail_detected_and_quarantined(self):
        with SegmentWriter(self.dir, "segment-0000.warc.gz") as writer:
            infos = [writer.write_record(make_record(str(i))) for i in range(3)]
        path = self.dir / "segment-0000.warc.gz"
        data = path.read_bytes()
        # 模拟写入中断：砍掉最后一个 member 的尾部字节，留下半个 member。
        path.write_bytes(data[:-7])
        result = scan_segment(path)
        self.assertEqual(len(result.members), 2)
        self.assertEqual(result.tail_offset, infos[2].compressed_offset)
        self.assertIsNotNone(result.tail_reason)

        report = recover_segment(path)
        self.assertEqual(report["status"], "recovered")
        self.assertEqual(report["members_kept"], 2)
        self.assertEqual(report["quarantined_bytes"],
                         len(data) - infos[2].compressed_offset - 7)
        quarantine = path.with_name(path.name + ".quarantine")
        self.assertTrue(quarantine.exists())
        # 恢复后再扫描：干净，无残尾；两条完整 member 仍可随机读取。
        clean = scan_segment(path)
        self.assertIsNone(clean.tail_offset)
        self.assertEqual(len(clean.members), 2)
        record = read_member_record(path, infos[0].compressed_offset,
                                    infos[0].compressed_length)
        self.assertEqual(record.block, "记录-0".encode("utf-8"))

    def test_corrupt_crc_is_reported(self):
        with SegmentWriter(self.dir, "segment-0000.warc.gz") as writer:
            info = writer.write_record(make_record("x"))
        path = self.dir / "segment-0000.warc.gz"
        data = bytearray(path.read_bytes())
        # 翻转 gzip 压缩数据区一个字节，破坏 CRC32。
        data[info.compressed_offset + 20] ^= 0xFF
        path.write_bytes(bytes(data))
        result = scan_segment(path)
        self.assertEqual(result.members, [])
        self.assertEqual(result.tail_offset, info.compressed_offset)
        self.assertTrue(result.tail_reason.startswith("crc"))

    def test_complete_segment_scans_clean(self):
        with SegmentWriter(self.dir, "segment-0000.warc.gz") as writer:
            writer.write_record(make_record("a"))
            writer.write_record(make_record("b"))
        result = scan_segment(self.dir / "segment-0000.warc.gz")
        self.assertIsNone(result.tail_offset)
        self.assertEqual(len(result.members), 2)

    def test_gzip_tool_can_still_read_concatenated_members(self):
        # 互操作性：标准 gzip 工具按多 member 流解压，得到全部记录拼接体。
        with SegmentWriter(self.dir, "segment-0000.warc.gz") as writer:
            for i in range(3):
                writer.write_record(make_record(str(i)))
        path = self.dir / "segment-0000.warc.gz"
        with gzip.open(path, "rb") as fh:
            blob = fh.read()
        for i in range(3):
            self.assertIn(("记录-%d" % i).encode("utf-8"), blob)


if __name__ == "__main__":
    unittest.main()
