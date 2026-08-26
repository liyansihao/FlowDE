# FlowDE

FlowDE 是一个可复用的多店铺任务调度层，用于把同一套选品或发布引擎安全地分配给多家店铺。它只负责商家编排，不包含任何真实店铺资料、商品数据、供应商链接、利润记录或平台凭证。

## 核心机制

- 多主店并发运行，每个主店拥有固定分片，避免同一候选被多个店铺同时处理。
- 主店达到每日额度或平台返回额度限制后，自动交给可用备用店。
- 每个备用店同一时间只接管一个分片，避免重复占用。
- 每店每日额度独立计算；示例配置为 100 个，并支持按平台时区和任意本地重置时间切换账期。
- 本地创建计数只用于观察。只有平台或 ERP 明确返回当日额度用完，才会停用该店并切换备用店。
- 每家店铺有独立状态目录、审计账本和安全停止信号。
- 候选队列暂时为空时使用独立复查间隔，避免高频空转，也不会把等待误报成整个流程已暂停。
- 多店启动和接口限流重试按分片错峰，避免十家店在同一秒集中请求共享接口。
- 共享候选池只指定一个分片刷新，其余分片等待并复用同一快照，减少重复扫描。
- 汇总状态同时报告运行、额度受限和候选队列等待的店铺数，适合多店铺看板直接读取。
- 不依赖浏览器；真正的平台或 ERP 调用由外部引擎适配器实现。

## 当前运行版同步

本仓库的 `v1.2` 调度逻辑已按 2026-08-26 正在运行的 Flow E/F 同步，包含：十主店分片、备用店接管、平台额度权威判断、非零点额度重置、共享候选池单领导者、启动错峰、接口限流退避和安全停止。

公开仓库保留完整调度机制，但不包含真实店铺 ID、仓库 ID、平台令牌、毛子 ERP 私有传输层、商品记录、供应商链接、图片、利润记录、日志和运行状态。这些内容继续由私有配置及业务引擎适配器提供。

## 快速开始

需要 Node.js 20 或更高版本。

```bash
cp config.example.json config.json
npm test
npm start
npm run status
npm run stop
```

`config.json` 和所有运行状态都已被 `.gitignore` 排除。示例配置使用虚构店铺，可以直接配合 `adapters/example-engine.mjs` 验证主备和额度机制，不会访问任何外部平台。

## 接入自己的发布引擎

在私有的 `config.json` 中把 `engine` 指向你的 Node.js 适配器。FlowDE 会使用 `node <engine> run` 启动它，并传入以下环境变量：

| 环境变量 | 含义 |
| --- | --- |
| `FLOWDE_RUNTIME_STATE_DIR` | 当前店铺的独立状态目录 |
| `FLOWDE_STORE_ID` / `FLOWDE_STORE_NAME` | 当前店铺标识 |
| `FLOWDE_SLOT_INDEX` / `FLOWDE_SLOT_COUNT` | 固定候选分片 |
| `FLOWDE_MAX_CREATIONS` | 本轮最多允许创建的数量 |
| `FLOWDE_DAILY_LIMIT` | 当前店铺每日额度 |
| `FLOWDE_DAILY_TIMEZONE` | 日额度时区 |
| `FLOWDE_DAILY_RESET_LOCAL` | 平台时区内的额度重置时间 |
| `FLOWDE_IS_STANDBY` | 当前是否作为备用店接管 |
| `FLOWDE_REPLACING_STORE_ID` | 被接管的主店 ID |
| `FLOWDE_REFRESH_SHARED_POOL` | 是否由当前分片刷新共享候选池（仅一个分片为 `1`） |
| `FLOWDE_RUN_STARTED_AT` | 本次调度启动时间，用于识别新快照 |
| `FLOWDE_SHARED_POOL_WAIT_MS` | 非领导分片等待共享候选池的最长时间 |

调度配置中的 `empty_queue_pause_seconds` 控制适配器返回 `candidate-queue-complete` 且本轮扫描数为 0 时的复查间隔，默认 300 秒。普通轮次仍使用 `cycle_pause_seconds`。

适配器应在 `FLOWDE_RUNTIME_STATE_DIR` 中维护：

- `status.json`：包含 `pid`、`phase`、`stop_reason` 和可选的 `submission_blocker`。
- `audit.jsonl`：每次创建尝试一行，至少包含 `at`、`item_id`（或 `offer_id` / `sku`）、`state` 和 `submission_attempted`。
- `stop.requested`：检测到该文件或收到 `SIGTERM` 时安全收尾。

当平台额度用完时，适配器应返回以下任一标准信号：

```json
{
  "stop_reason": "platform-daily-creation-limit",
  "submission_blocker": {
    "type": "platform-daily-creation-limit"
  }
}
```

额度拦截必须来自平台或 ERP 的明确响应。即使本地审计计数达到示例上限，FlowDE 也不会自行宣布额度已满；这样可以避免重复记录或跨流程记录导致店铺被误停。

当共享接口触发限流时，适配器可以返回：

```json
{
  "stop_reason": "api-rate-limit",
  "runtime_blocker": {
    "type": "api-rate-limit",
    "retry_after_at": "2026-08-26T10:00:00.000Z"
  }
}
```

FlowDE 会采用服务端时间、配置的最低退避时间和分片错峰中的最大安全等待值，不会把限流 SKU 误判为选品失败。

当当前分片暂时没有新候选时，适配器可以返回：

```json
{
  "stop_reason": "candidate-queue-complete",
  "counts": { "scanned": 0 }
}
```

## 数据安全

公开仓库只包含调度机制、示例配置、示例适配器和测试。真实配置、凭证、运行状态、商品图片、表格、CSV、JSONL、日志和本地业务数据默认全部忽略。平台密钥应由适配器从环境变量或密钥管理服务读取，不能写入仓库。

## 边界

FlowDE 不替代业务引擎里的类目合规、同款判断、利润门槛、库存或物流验证。这些门槛必须由你的私有引擎适配器继续严格执行；调度层只决定“哪家店处理哪个分片、今天还能创建多少、何时切换备用店”。
