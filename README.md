# 网页采集原始响应的可回放归档

项目保存供方接口协议、演练数据、回环地址服务，以及把采集过程落为
WARC/1.1 归档并支持离线回放的纯标准库实现。所有示例记录均使用本机演练资源，
不连接实际供应平台。

运行环境为 Python 3.10 或以上，仅使用标准库。

```sh
python -m unittest discover -s tests -v
python -m compileall -q src tests
python -m src.source --port 8765
python -m src.warc.demo --archive demo_archive --keep
```

前两条命令分别检查行为及语法；第三条启动本机服务（Ctrl+C 结束）；
第四条跑端到端演示（自动启停本机服务，可删去 `--keep` 让目录自动清理）。

## 演练源（src/source.py）

GET /page 为 UTF-8 正文，/compressed 为 gzip 实体，/chunked 采用 7 字节
分块并携带 X-Source-End trailer，/jump 跳转至 /page，/truncated 声明完整
长度但提前关闭。服务不产生归档文件。fixtures/source.json 保留正文。

GET /__state 可查看当前演练状态与已收到的请求。POST /__control 接受 JSON，
修改该服务实例的演练状态；此入口只用于本机供方模拟，不属于生产接口。
服务端状态不跨进程保留。

## WARC 归档设计（src/warc/）

### 记录格式

按 WARC/1.1（ISO 28500）输出，一次 HTTP 交换写三条记录：

| 记录 | Content-Type | 关键字段 |
| --- | --- | --- |
| request | application/http;msgtype=request | 实际发往对端的请求行+头+体 |
| response | application/http;msgtype=response | 状态行、响应头、实体字节 |
| metadata | application/json | 线上传输事实（分块边界、trailers、原始头、截断原因等） |

- 每段首记录为 warcinfo；同跳 request/response 用 `WARC-Concurrent-To`
  相连，metadata 同时带 `WARC-Concurrent-To` 与 `WARC-Refers-To`，
  重定向跨跳在 metadata 的 `previous_hop_request_id` 中显式相连，
  抓取与跳序号由扩展头 `X-Fetch-Seq`、`X-Fetch-Hop` 表达。
- 两种摘要分开：`WARC-Block-Digest` 覆盖整个内嵌 HTTP 块；
  `WARC-Payload-Digest` 只覆盖传输解帧后的实体字节。摘要为
  base32 未填充小写 sha256。`WARC-Content-Length` 是内嵌块自身字节数，
  与 gzip member 的压缩长度无关。
- 归档对象是 HTTP 传输解帧后的实体：`Content-Encoding: gzip` 的压缩字节
  原样保留并参与实体摘要，不使用任何 HTTP 库自动解压后的正文冒充原响应。
- 传输帧限定 Content-Length 与 chunked。chunked 的分块边界（相对实体的
  偏移/长度）与 trailers 保存在 metadata；内嵌回放响应移除
  Transfer-Encoding 及逐跳头，写入等于实体长度的 Content-Length，
  不存在同时携带冲突长度的响应。
- 重定向每一跳独立归档（各自的状态、头、记录 ID），相同正文不去重。
- 连接失败/无响应头：只写 request + 错误 metadata，不伪造 response；
  中途断开：写 response 但带标准 `WARC-Truncated: disconnect`，
  内嵌 Content-Length 等于实际收到的字节，不补造成功正文。

### 段与索引

- 段文件 `segment-NNNN.warc.gz` 由多个**独立 gzip member** 拼接而成，
  一条 WARC 记录一个 member，写入逐条 fsync；按压缩大小轮换，新段不复用旧段。
- SQLite 索引 `index.sqlite` 每行一个 member，给出 segment、压缩偏移、
  压缩长度、记录 ID、URL、抓取序号/跳号、状态码与截断标记。
- 回放按索引 seek 到单个 member，只读并解压这一个 member（不展开整段），
  并校验 gzip CRC32/ISIZE、WARC 块摘要与实体摘要。
- 崩溃恢复：段尾半个 member（gzip 流未正常结束或 CRC 错误）可被
  `scan_segment` 精确定位，`recover_segment` 把残尾隔离到
  `.quarantine` 证据文件并截断段；不完整记录不会进入索引、不可回放。
  打开归档目录时自动执行恢复。
- 索引是派生物：删除或损坏后可用 `IndexStore.rebuild()` 从段重新扫描生成，
  重建在单事务内完成。

## 采集器如何接入

写路径（在线采集，单条调用即完成 request/response/metadata 落盘与索引）：

```python
from src.warc import Recorder

with Recorder("./archive") as rec:          # 打开即做段恢复
    result = rec.fetch("http://127.0.0.1:8765/page")
    if result.ok:
        print(result.final_hop.payload_digest)   # 线上捕获时的实体摘要
    if result.truncated:                          # 截断显式标记
        print(result.truncated)
```

读路径（离线抽取，不访问原站；业务抽取代码只读 ReplayedResponse）：

```python
from src.warc import ReplayStore, TruncatedResponseError

with ReplayStore("./archive") as replay:
    for row in replay.all_response_rows():
        try:
            resp = replay.replay(row)         # 双摘要校验，截断默认拒绝
        except TruncatedResponseError:
            continue                          # 不完整页不进入业务抽取
        html = resp.entity                    # 与线上同一批字节
```

崩溃恢复与索引重建：

```python
with Recorder("./archive") as rec:
    print(rec.recovery_reports)               # 段尾残尾隔离报告
    print(rec.rebuild_index())                # 索引丢失后从段重建
```

## 模块与测试

- `src/warc/records.py`：WARC 记录、内嵌 HTTP 构建/解析、双摘要、回放头重写
- `src/warc/segments.py`：多 gzip member 段、流式扫描、随机定位、残尾隔离
- `src/warc/indexstore.py`：SQLite 索引与重建
- `src/warc/capture.py`：原始套接字 HTTP/1.1 采集（保留压缩字节与 chunked 边界）
- `src/warc/recorder.py`：抓取编排、重定向逐跳归档、失败/截断标记、段轮换
- `src/warc/replay.py`：离线回放、摘要校验、截断拒绝
- `src/warc/demo.py`：端到端演示
- `tests/test_warc_records.py`、`tests/test_warc_segments.py`、
  `tests/test_warc_integration.py`：记录格式、member 边界/恢复、全链路

目录文字的业务抽取不在本项目范围内；归档只负责让后续抽取能在离线状态下
重新读取与线上捕获逐字节一致的实体。
