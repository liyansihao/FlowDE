# FlowDE

FlowDE 是一个可复用的多店铺任务调度层，用于把同一套选品或发布引擎安全地分配给多家店铺。它只负责商家编排，不包含任何真实店铺资料、商品数据、供应商链接、利润记录或平台凭证。

## 核心机制

- 多主店并发运行，每个主店拥有固定分片，避免同一候选被多个店铺同时处理。
- 主店达到每日额度或平台返回额度限制后，自动交给可用备用店。
- 每个备用店同一时间只接管一个分片，避免重复占用。
- 每店每日额度独立计算；示例配置为 100 个，并在 `Asia/Shanghai` 的 00:00 切换到新账期。
- 平台返回的额度拦截优先于本地计数，程序不会为了速度绕过平台限制。
- 每家店铺有独立状态目录、审计账本和安全停止信号。
- 不依赖浏览器；真正的平台或 ERP 调用由外部引擎适配器实现。

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
| `FLOWDE_IS_STANDBY` | 当前是否作为备用店接管 |
| `FLOWDE_REPLACING_STORE_ID` | 被接管的主店 ID |

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

## 数据安全

公开仓库只包含调度机制、示例配置、示例适配器和测试。真实配置、凭证、运行状态、商品图片、表格、CSV、JSONL、日志和本地业务数据默认全部忽略。平台密钥应由适配器从环境变量或密钥管理服务读取，不能写入仓库。

## 边界

FlowDE 不替代业务引擎里的类目合规、同款判断、利润门槛、库存或物流验证。这些门槛必须由你的私有引擎适配器继续严格执行；调度层只决定“哪家店处理哪个分片、今天还能创建多少、何时切换备用店”。
