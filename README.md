# diaggate — TCP 诊断网关 + 2～4 个虚拟设备

一个不接真实硬件的诊断网关。TCP 客户端提交 `设备 ID / 请求 ID / ≤512 字节载荷`，
网关按自定义 8 字节帧协议把请求分片送往虚拟设备，处理基于信用（额度）的流控、
取消、乱序、长度不符和超时，并返回完整的帧轨迹与结构化错误。

## 运行

```bash
go run ./cmd/diaggate -addr 127.0.0.1:7000 -devices 3 -timeout 2s -device-delay 20ms
```

## 测试

```bash
go test ./...            # 功能测试
go test -race ./...      # 竞态检测
go test -count=5 ./...   # 重复运行，排查 flaky
```

测试不依赖外部服务；虚拟设备、传输（`net.Pipe`）、时钟和延迟都可以注入。

## 整体结构

```
TCP 客户端  ── 8 字节客户端头 + 载荷 ──►  Server (tcp.go)
                                              │ Gateway.RoundTrip (gateway.go)
                                              ▼
                          8 字节设备帧协议 (protocol.go, frameconn.go)
                              ┌───────────┬───────────┬───────────┐
                              ▼           ▼           ▼           ▼
                          虚拟设备 1  虚拟设备 2  (设备 3)    (设备 4)   virtual.go
```

- 每台设备**至多一个未完成请求**：`deviceSlot.callMu` 串行化对同一设备的调用。
- **不同设备完全并行**：各设备有独立的锁、传输和 goroutine。
- 每次调用使用一条独立的设备传输，结束即关闭。

## 设备侧 8 字节帧协议

所有帧都是小端（little-endian）8 字节头，随后最多 63 字节载荷：

| 偏移 | 长度 | 含义 |
|----|----|----|
| 0  | 1 | 协议版本（=1） |
| 1  | 1 | 低 4 位：帧类型；最高位 `0x80`：epoch 位 |
| 2  | 1 | 设备 ID |
| 3-4 | 2 | 请求 ID（首帧/会话级帧）或循环序号（续帧） |
| 5-6 | 2 | 首帧记录的**总载荷长度**；续帧复用为循环序号 |
| 7  | 1 | 本帧载荷长度（0..63） |

帧类型：

| 类型 | 名称 | 说明 |
|----|----|----|
| 1 | `first` | 首帧，携带总长度与前 63 字节 |
| 2 | `cont`  | 续帧，请求 ID 字段放循环 uint16 序号（从 1 开始，65535 后回绕到 0） |
| 3 | `grant` | 设备授予的发送额度，1 字节载荷，取值 0..4 |
| 4 | `cancel`| 取消本会话，载荷为空 |
| 5 | `error` | 结构化错误，载荷为 JSON |
| 6 | `end`   | 响应结束标记，载荷为空，`total` 回显总长度 |

### 分片与流控

- 载荷上限 `MaxPayload = 512`，每片 `ChunkSize = 63`。
- 首帧总是被接受；**续帧必须先获得额度**。
- 设备每次授予 1～4 帧额度（`grant.payload[0]`）。授 0 表示
  **额度耗尽（quota_exhausted）**，只终止当前会话。
- 网关持有一个本地额度计数，按授予数量发送续帧，用完即停（流控停顿）。

### 会话归属与“迟到帧”防护

- 每次调用在该设备上翻转一次 **epoch 位**，随首帧发出。
- 续帧的请求 ID 字段被序号占用，因此设备与网关都按 **epoch** 关联续帧；
  会话级帧（first/end/error/cancel/grant）按 epoch + 请求 ID 关联。
- 收到 epoch 或请求 ID 不匹配的帧一律**丢弃并记入轨迹**，绝不投递给当前会话。
- 每次调用使用独立传输，调用结束即关闭，被取消请求的迟到响应物理上无法到达
  下一次调用；即使共用传输，epoch 位也会将其挡在会话之外。

### 只终止所属会话的失败

取消、额度耗尽、乱序帧、长度不符、超时都只影响当前 `设备 + 请求 + epoch`：

| 情况 | 处理 |
|----|----|
| 调用方取消 | 发 `cancel`，返回 `canceled` |
| 超时 | 注入时钟到期，发 `cancel`，返回 `timeout` |
| 额度为 0 | 返回 `quota_exhausted` |
| 续帧序号不对/缺失/重复 | 返回 `out_of_order` |
| 总长度与各片长度不符 / `end` 不符 | 返回 `length_mismatch` |
| 设备发 `error` 帧 | 原样返回结构化错误 |
| 传输断开 | 返回 `device_closed` |

设备故障不会阻止其它设备继续推进（测试中逐一验证）。

## 客户端 TCP 协议（与设备帧协议相互独立）

8 字节大端头：

| 偏移 | 长度 | 含义 |
|----|----|----|
| 0-1 | 2 | 魔数 `DG` |
| 2  | 1 | 操作：1=request，2=cancel |
| 3  | 1 | 设备 ID |
| 4-5 | 2 | 请求 ID（大端） |
| 6-7 | 2 | 载荷长度（大端，0..512） |

其后是载荷。响应头同形：第 3 字节为状态（1 OK / 2 error / 3 canceled /
4 protocol），载荷为响应字节或 JSON 错误信封。

## 结构化错误

`ProtocolError`（`errors.go`）包含稳定的机器可读 `code`、`message`、
`device_id`、`request_id`，同时实现 `error` 接口，可用 `errors.As` 判定。

```go
var pe *ProtocolError
if errors.As(err, &pe) {
    switch pe.Code {
    case CodeTimeout:        // "timeout"
    case CodeQuotaExhausted: // "quota_exhausted"
    case CodeOutOfOrder:     // "out_of_order"
    case CodeLengthMismatch: // "length_mismatch"
    case CodeCanceled:       // "canceled"
    }
}
```

## 帧轨迹

`Tracer` 接口记录每一帧的方向、设备、请求、epoch、类型、总长度、本帧长度、
十六进制载荷以及丢弃原因/备注：

- `MemoryTracer`：测试用，可随时取回全部事件。
- `LoggingTracer`：命令行运行时逐帧打印。

## 注入点（供测试）

- `Clock` / `Timer`：`FakeClock` 手动推进时间，`WaitForTimers` 等待定时器注册，
  可确定性地触发超时而不真正 sleep。
- `VirtualDeviceConfig.Respond`：可阻塞注入设备延迟，也可故意忽略取消来制造
  “迟到响应”。
- `VirtualDeviceConfig.GrantPolicy`：返回 0 制造额度耗尽，阻塞制造流控停顿，
  分批返回 1/2/3/4 控制分片节奏。
- `VirtualDeviceConfig.TransformResponse`：交换/重复/篡改响应帧，制造乱序。
- `GatewayConfig.Dial`：可替换为自定义 `FrameTransport`（例如手写脚本）。

## 测试覆盖

| 测试 | 覆盖点 |
|----|----|
| `TestCrossDeviceInterleaving` | 4 台设备交错并行推进 |
| `TestSequenceWraparound` | 续帧序号 65535 → 0 → 1 回绕 |
| `TestLateFrameAfterCancel` | 取消后复用同一请求 ID，迟到响应不被下一个请求误收 |
| `TestReadFramesDropsForeignEpoch` | 旧 epoch 帧在路由层被丢弃 |
| `TestFlowControlPause` | 额度用尽时停顿，追加额度后继续 |
| `TestQuotaExhaustion` | 授 0 终止本会话，不影响其它设备 |
| `TestTimeoutIsSessionScoped` | 超时只终止所属设备 |
| `TestOutOfOrderAndLengthMismatch` | 乱序、长度不符 |
| `TestEmptyPayload` | 空载荷仍有 `end`，不挂起 |
| `TestTCPClientServer` / `TestTCPCancelSendsStructuredError` | 外部 TCP 协议、超大载荷、取消后连接可复用 |
