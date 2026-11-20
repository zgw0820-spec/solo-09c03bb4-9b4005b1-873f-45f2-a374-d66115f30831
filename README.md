# 机房救援 TFTP：镜像下发 + 启动日志回传

给无法从本地盘启动的机器做两件事，Python asyncio 实现，`docker compose`
一键启动两个服务：

| 服务 | 端口 | 说明 |
|---|---|---|
| `tftp-server` | UDP 69（宿主映射 6969/udp） | asyncio TFTP 服务端：RRQ 只下发固定镜像 `rescue.img`；WRQ 接收 `<工单号>.log` 启动日志并归档 |
| `api` | TCP 8000 | FastAPI。打开 **http://localhost:8000/docs** 在 Swagger 里发起**真实 UDP** 传输：`POST /download` 下载镜像并核对 SHA-256，`POST /upload` 回传日志，`GET /logs/...` 按工单号取包 |

## 启动

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000/docs
```

归档目录通过命名卷 `log-data` 在两个容器间共享：服务端把成品写进
`/data/archive`，API 从同一目录读取；半成品放在 `/data/staging`，
API 永远看不到它。

## Swagger 操作

- `POST /upload`（真实 UDP WRQ，参数都有默认值）
  - `work_order`：工单号，线上文件名固定为 `<work_order>.log`
  - `payload_size`：日志字节数（0 可测空块；默认 1200 落在非整块）
  - `data_loss`：模拟丢弃出向 DATA 的概率
  - `drop_final_ack`：吞掉末块的第一个 ACK，验证同端口末块 ACK 重传
  - `seed` / `timeout` / `max_retries`
- `GET /logs`：列出已归档日志（半成品永不出现）
- `GET /logs/{work_order}/info`：单个日志的大小与 SHA-256
- `GET /logs/{work_order}/download`：按工单号取包（二进制下载）
- 原 `POST /download`、`GET /image/info` 救援镜像下载保持不变

## WRQ 回传协议行为（对应需求）

- **文件名与模式**：只接受 `<工单号>.log`（工单号限 `[A-Za-z0-9_-]{1,64}`，
  拒绝路径穿越/子目录），`octet` 模式，固定每块 512 字节，纯 RFC 1350
  停等，不做选项协商（带选项的 WRQ 回 ERROR 8）。
- **同工单只接纳一路**：`ArchiveStore` 按工单号预留；进行中的第二路 WRQ
  在 69 端口直接回 **ERROR 2**，不开新传输端口。
- **已归档禁止覆盖**：成品以 `os.replace` 原子落盘，重名 WRQ 回
  **ERROR 6**，归档字节保持不变；落盘前再做一次存在性兜底检查。
- **完整落盘后才开放取包并确认末块**：数据先写私有 `.part`，末块到达时
  `flush + fsync`，再原子 rename 进归档，**之后**才发末块 ACK；
  API 只从归档目录取包，因此永远取不到半成品。
- **写盘失败回 TFTP ERROR**：写或 rename 抛 `OSError` 时回 **ERROR 3**，
  清理 `.part`、释放工单，工单号可立即重新使用。
- **中断超时清理**：空闲超过 `UPLOAD_TIMEOUT * UPLOAD_MAX_RETRIES` 即
  删除半成品、注销会话、释放工单号（测试验证可重新上传）。
- **末块 ACK 丢失**：在现有重试时限内由**原传输端口**重传末块 ACK
  （1 次初始 + 至多 `UPLOAD_MAX_RETRIES` 次重传），重发的末块 DATA 也会
  再收到同一 ACK；成品只写一次，重试窗口关闭后释放端口。
- **块号回绕**：16 位块号按 65536 取模，超长日志按序继续。
- 零字节日志与长度恰为 512 整数倍的日志都以一个空 DATA 块收尾。

环境变量：`UPLOAD_ARCHIVE_DIR`、`UPLOAD_STAGING_DIR`、`UPLOAD_TIMEOUT`
（默认 2.0s）、`UPLOAD_MAX_RETRIES`（默认 5）。

## 本地验证（无需 Docker）

```bash
python3 tests/e2e.py         # 48 项：镜像下载（基本/丢包/重复 RRQ/重试耗尽/并发/回绕）
python3 tests/e2e_upload.py  # 79 项：WRQ 回传全量
```

上传套件覆盖：基本上传与空收尾块、整 512 倍与零字节、同号并发只留一路、
已归档禁覆盖、非法文件名、末块 ACK 丢失同端口重传、末块 DATA 重发只写一次、
中断超时清理与工单复用、rename 失败回 ERROR 3、DATA 丢包重传，以及
FastAPI 的 `/upload`、`/logs`、info、download 接口（ASGI 内联真实 UDP）。

## 布局

```
common/protocol.py     共享线格式：RRQ/WRQ/DATA/ACK/OACK/ERROR 编解码、块号回绕
common/imagegen.py     确定性镜像生成（seed+size -> bytes / sha256）
server/tftp_server.py  69 端口前门 + RRQ 下载会话，分发 WRQ
server/upload.py       WRQ 上传会话、工单号预留、staging/archive 原子归档
api/tftp_client.py     asyncio TFTP 客户端：RRQ 下载 + WRQ 上传（丢包模拟）
api/main.py            FastAPI：/docs、/download、/upload、/logs*
tests/e2e.py           下载端到端验证（48 项）
tests/e2e_upload.py    上传端到端验证（79 项）
Dockerfile.server / Dockerfile.api / docker-compose.yml
```

## 已知限制

- 下载仍只服务一个固定镜像；上传只接受标准 512 字节块的纯 RFC 1350 WRQ，
  不协商 blksize/windowsize（这是有意的，停等语义最简单可靠）。
- 客户端按序发送；服务端对乱序块重确认最高连续块，不做乱序缓存。
- 归档为本地共享卷；多实例水平部署时需要换成共享存储并加分布式预留。
