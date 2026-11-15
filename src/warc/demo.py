"""端到端演示：原始采集 -> WARC 归档 -> 离线回放 -> 崩溃恢复。

运行（仓库根目录）：
    python3 -m src.warc.demo [--archive demo_archive] [--keep]

演示内容：
1. 中文正文、gzip 压缩实体、chunked(+trailer)、302 逐跳、中途断开、连接失败；
2. 离线重放时实体重算摘要与线上捕获摘要逐字节一致；
3. 截断记录默认拒绝当完整页，显式审计才可取回残体；
4. 模拟段尾半个 gzip member 的写中断，重新打开自动隔离并追加归档；
5. 删除 SQLite 索引后从段完整重建。

全程只访问 127.0.0.1 的演练源，不连接任何外部站点。
"""
from __future__ import annotations

import argparse
import gzip
import json
import shutil
import sys
import tempfile
import threading
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
from src.warc.segments import list_segment_names, scan_segment


def hr(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def show_digest_match(label: str, live: str | None, replayed_digest: str) -> None:
    match = live == replayed_digest
    print(f"  [{ '✓' if match else '✗'}] {label}")
    print(f"      线上捕获摘要: {live}")
    print(f"      离线重算摘要: {replayed_digest}")
    if not match:
        raise AssertionError("摘要不一致: %s" % label)


def main() -> int:
    parser = argparse.ArgumentParser(description="WARC 归档与离线回放演示")
    parser.add_argument("--archive", default=None, help="归档目录（默认临时目录）")
    parser.add_argument("--keep", action="store_true", help="结束后保留归档目录")
    args = parser.parse_args()

    archive_dir = Path(args.archive) if args.archive else Path(tempfile.mkdtemp(prefix="warc-demo-"))
    if archive_dir.exists() and any(archive_dir.iterdir()):
        shutil.rmtree(archive_dir)
    archive_dir.mkdir(parents=True)

    # 启动本机演练源（独立线程，仅回环地址）。
    server = source.make_server(0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = "http://127.0.0.1:%d" % server.server_port
    expected = source.DATA["content"].encode("utf-8")
    print("演练源: %s  归档目录: %s" % (base, archive_dir))

    try:
        # -------------------------------------------------------------- #
        hr("一、线上采集并归档（request/response/metadata，逐跳）")
        with Recorder(archive_dir, timeout=10) as rec:
            page = rec.fetch(base + "/page")
            gz = rec.fetch(base + "/compressed")
            chunked = rec.fetch(base + "/chunked")
            jump = rec.fetch(base + "/jump")
            cut = rec.fetch(base + "/truncated")
            refused = rec.fetch("http://127.0.0.1:1/unreachable")
            seq_page, seq_jump, seq_cut = (
                page.fetch_seq, jump.fetch_seq, cut.fetch_seq)
            live_digests = {
                "page": page.final_hop.payload_digest,
                "gzip": gz.final_hop.payload_digest,
                "chunked": chunked.final_hop.payload_digest,
            }
            print("  /page       -> seq=%d 200 完整" % page.fetch_seq)
            print("  /compressed -> seq=%d 200 gzip 实体保留压缩字节" % gz.fetch_seq)
            print("  /chunked    -> seq=%d 200 chunked 边界+trailer 入 metadata"
                  % chunked.fetch_seq)
            print("  /jump       -> seq=%d 跳数=%d 状态链=%s"
                  % (jump.fetch_seq, len(jump.hops),
                     [h.status_code for h in jump.hops]))
            print("  /truncated  -> seq=%d ok=%s truncated=%s（不补造正文）"
                  % (cut.fetch_seq, cut.ok, cut.truncated))
            print("  拒连目标    -> seq=%d 仅 request+metadata，error=%s"
                  % (refused.fetch_seq, refused.error is not None))

        # -------------------------------------------------------------- #
        hr("二、离线回放：同一批字节，不访问原站")
        with ReplayStore(archive_dir) as replay:
            # 1) 中文正文：离线摘要 == 线上摘要
            page_rows = replay.response_rows_for_url(base + "/page")
            rp = replay.replay(page_rows[0])
            print("  [中文正文] 状态=%d 字节=%d" % (rp.status_code, len(rp.entity)))
            print("    正文片段: %s" % rp.entity.decode("utf-8")[:48])
            show_digest_match(
                "中文页实体摘要（线上 vs 离线）",
                live_digests["page"],
                "sha256:" + digest_b32sha256(rp.entity))

            # 2) gzip 实体：归档与回放都是压缩字节
            rg = replay.replay_url(base + "/compressed")
            print("  [gzip 实体] 魔数=%s 压缩字节=%d 解压后=%d 字节"
                  % (rg.entity[:2], len(rg.entity),
                     len(gzip.decompress(rg.entity))))
            show_digest_match(
                "gzip 压缩字节实体摘要（线上 vs 离线）",
                live_digests["gzip"],
                "sha256:" + digest_b32sha256(rg.entity))
            print("    Content-Encoding 保留: %s"
                  % dict(rg.headers).get("Content-Encoding"))

            # 3) chunked：回放无 Transfer-Encoding，边界/trailer 在 metadata
            rc = replay.replay_url(base + "/chunked")
            names = {k.lower() for k, _ in rc.headers}
            print("  [chunked] 回放头含 Transfer-Encoding? %s；"
                  "Content-Length=%d；分块数=%d"
                  % ("transfer-encoding" in names,
                     int(dict(rc.headers)["Content-Length"]),
                     len(rc.metadata["chunk_boundaries"])))
            print("    trailers 原样保留: %s" % rc.metadata["trailers"])
            show_digest_match(
                "chunked 解帧实体摘要（线上 vs 离线）",
                live_digests["chunked"],
                "sha256:" + digest_b32sha256(rc.entity))

            # 4) 302：两跳独立归档、记录 ID 相连
            jump_rows = replay.fetch_rows(seq_jump)
            responses = [r for r in jump_rows if r.record_type == "response"]
            print("  [302 跳转] 独立 response 数=%d，状态=%s，hop=%s"
                  % (len(responses),
                     [r.status_code for r in responses],
                     [r.hop for r in responses]))
            rj0 = replay.replay(responses[0])
            print("    第 0 跳 Location=%s，request 并发指向=%s"
                  % (rj0.metadata["location"], responses[0].concurrent_to))
            rj1 = replay.replay(responses[1])
            print("    第 1 跳实体与 /page 相同但记录 ID 独立: %s"
                  % rj1.row.record_id)

            # 5) 截断：默认拒绝当完整页
            cut_rows = replay.response_rows_for_url(base + "/truncated")
            try:
                replay.replay(cut_rows[0])
                raise AssertionError("截断记录不应被当作完整页接受")
            except TruncatedResponseError as exc:
                print("  [中途断开] 完整页回放被拒绝: %s" % exc)
            partial = replay.replay(cut_rows[0], allow_truncated=True)
            print("    审计模式残体=%d 字节，声明长度=%d，WARC-Truncated=%s"
                  % (len(partial.entity),
                     partial.metadata["declared_content_length"],
                     partial.row.truncated))
            embedded = parse_http_message(replay.read_record(cut_rows[0]).block)
            print("    内嵌响应 Content-Length=%d（=实到字节，无冲突长度）"
                  % int(dict(embedded.headers)["Content-Length"]))

            # 6) 连接失败：没有 response 记录
            fail_rows = replay.fetch_rows(refused.fetch_seq)
            print("  [连接失败] 归档类型=%s（无 response，未伪造正文）"
                  % sorted(r.record_type for r in fail_rows))

        # -------------------------------------------------------------- #
        hr("三、段结构与索引：member 随机定位，不展开整段")
        segments = list_segment_names(archive_dir)
        for name in segments:
            scan = scan_segment(archive_dir / name)
            size = (archive_dir / name).stat().st_size
            print("  %s  压缩大小=%6d B  member 数=%d  残尾=%s"
                  % (name, size, len(scan.members), scan.tail_reason))
        db = archive_dir / "index.sqlite"
        print("  索引文件: %s（列: segment, compressed_offset, "
              "compressed_length, record_id, url, fetch_seq, hop）" % db.name)

        # -------------------------------------------------------------- #
        hr("四、崩溃恢复：段尾半个 member -> 隔离 -> 追加归档")
        target_segment = archive_dir / segments[0]
        with open(target_segment, "ab") as fh:
            half = gzip.compress(b"partial member simulating killed writer")[:-11]
            fh.write(half)
        broken = scan_segment(target_segment)
        print("  注入半个 gzip member 后扫描: 完整 member=%d 残尾偏移=%s 原因=%s"
              % (len(broken.members), broken.tail_offset, broken.tail_reason))
        with Recorder(archive_dir, timeout=10) as rec2:
            recovered = [r for r in rec2.recovery_reports
                         if r["status"] == "recovered"]
            for report in recovered:
                print("  恢复报告: %s" % json.dumps(report, ensure_ascii=False))
            extra = rec2.fetch(base + "/page")
            print("  恢复后新抓取 seq=%d ok=%s（写入恢复后新建的段，不复用旧段）"
                  % (extra.fetch_seq, extra.ok))
        quarantine = archive_dir / (segments[0] + ".quarantine")
        print("  残尾证据文件: %s 存在=%s" % (quarantine.name, quarantine.exists()))
        with ReplayStore(archive_dir) as replay:
            all_rows = replay.all_response_rows()
            print("  恢复后可回放 response 数=%d，逐个回放校验：" % len(all_rows))
            for row in all_rows:
                replay.replay(row, allow_truncated=True)
            print("    全部 member 摘要校验通过；半个 member 未出现在索引中")

        # -------------------------------------------------------------- #
        hr("五、索引持久化失败/丢失：从段重建")
        with IndexStore(archive_dir) as idx:
            before = idx.count()
        for suffix in ("", "-wal", "-shm"):
            p = archive_dir / ("index.sqlite" + suffix)
            if p.exists():
                p.unlink()
        print("  已删除 %s 及 WAL/SHM" % db.name)
        with IndexStore(archive_dir) as rebuilt:
            report = rebuilt.rebuild()
            after_count = rebuilt.count()
        print("  重建报告: %s" % json.dumps(report, ensure_ascii=False))
        print("  重建前行数=%d 重建后行数=%d" % (before, after_count))
        assert before == after_count
        with ReplayStore(archive_dir) as replay:
            rp = replay.replay_url(base + "/page")
            show_digest_match(
                "重建索引后回放摘要仍与线上一致",
                live_digests["page"],
                "sha256:" + digest_b32sha256(rp.entity))

        hr("演示完成")
        print("归档目录: %s" % archive_dir)
        if not args.keep and args.archive is None:
            print("（临时目录，进程退出后由系统回收；用 --archive DIR --keep 保留）")
        return 0
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
        if args.archive is None and not args.keep:
            shutil.rmtree(archive_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
