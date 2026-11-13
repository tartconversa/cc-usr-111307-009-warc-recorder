# 网页采集原始响应的可回放归档

项目保存供方接口协议、演练数据与回环地址服务。所有示例记录均使用本机演练资源，不连接实际供应平台。

运行环境为 Python 3.10 或以上，仅使用标准库。

```sh
python -m unittest discover -s tests -v
python -m compileall -q src tests
python -m src.source --port 8765
```

前两条命令分别检查供方源协议行为及语法；第三条启动本机服务，使用 Ctrl+C 结束。

GET /page为UTF-8正文，/compressed为gzip实体，/chunked采用7字节分块并携带X-Source-End trailer，/jump跳转至/page，/truncated声明完整长度但提前关闭。服务不产生归档文件。fixtures/source.json保留正文。

GET /__state 可查看当前演练状态与已收到的请求。POST /__control 接受JSON，修改该服务实例的演练状态；此入口只用于本机供方模拟，不属于生产接口。服务端状态不跨进程保留。

src/source.py 是供方源实现；fixtures/source.json 保存协议示例数据；tests/test_source.py 检查供方源自身的响应。
