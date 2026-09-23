# M3 历史图表 —— 设计与规格说明

版本：v1（草案，待用户复核）
日期：2026-09-24
关联：`C:\Users\Administrator\Desktop\EVE欧服市场客户端-开发方案v3.1.md` §3.3 / §5 / §6 / §8（里程碑 M3）
前置里程碑：M0 / M0.5 / M1 / M2 / M2b 已完成（daemon 12 子命令、Tauri 壳 7 命令、三栏市场浏览器、分类树与站点字典）

---

## 0. 实现现状校正（2026-09-24 复核，先读这节）

本 spec 初稿按"M3 从零"写；复核代码后发现**上个会话已把 emd-core 的 M3 数据层+逻辑层建完**，以下条目以代码为准，覆盖正文相应处：

**已完成（在 `emd-core`，但 `market/history.rs` 尚未在 `market/mod.rs` 里 `mod history;` 接线，当前不参与编译）：**
- 迁移 v4 已存在：`market_history`、`watchlist`、`history_scope`（L1 活跃记忆）、`history_log`（回填台账）；列名用实测的 `highest`/`lowest`（**非**正文 §3 写的 `high`/`low`）。见 `store/schema.rs`。
- `market/history.rs` 已实现：`fetch_one` 的幂等闸门是 **`sync_state` 的逐 URL `Expires`**（**非**正文 §4.2 设想的"查 `market_history` 当日 date 行"，实现选了更正确的方案）；404 视为"确认无历史"，记 7 天复核周期（`note_history_absent`）；`backfill` 并发 16 + 连续 20 个 404 熔断（`ALL_MISSING_ABORT`，防星域 ID 写错爆刷）；`history_due`（过 11:20 UTC 且当日未成功过）；`Estimate`；`prune_history`（留 425 天）；`PassReport` 台账。
- 实测事实（写进 history.rs 模块头，覆盖正文）：一次 history 请求返回 **418 天滚动窗口**（**非** §3.3 估的 365），"回填一年"与"每日增量"是同一次请求；`ESI_WINDOW_DAYS=418`。
- 🔴 **附录 D.4 已由代码实测结掉**：交叉实验证明 history 不占 `market-order` 组令牌（40×200 与 5×404 期间 orders 的 `X-Ratelimit-Remaining` 只按自身每笔 2 递减）。正文 §4.1 / §1.2 里"D.4 未决、须先小样本观察"的表述作废——但每个 404 仍会扣**全局** `X-Esi-Error-Limit-Remain`，故 404-as-absent 与熔断逻辑保留。

**未完成（本 spec 剩余实现对象）：**
- `market/mod.rs` 加 `mod history;` 并 `pub use`，让上述逻辑真正编译进 crate。
- 第 2 节的单一采集编排器 `emd_core::collector` + `collector.lock` 单实例锁（当前 `lib.rs` 无 collector、`scheduler.rs` 仍纯 T1、daemon/app 各自跑旧胶水）。
- T3 挂载：编排器在 `history_due` 时调 `history::backfill`（复用已实现的 L0/L1 目标选取）。
- daemon / app 接入 collector、`history` 子命令、`get_history`/watchlist 命令、status 查看者回落。
- 前端 `echarts` 依赖 + 蜡烛图页签（正文 §6 全部未做）。

---

## 1. 目标与范围

### 1.1 本刀做什么
M3 交付"动态波动分析可用"（§8 判据），即：自选清单（L0）的**日线历史**自动回填 + 每日增量，前端用蜡烛图呈现区间波动并支持缩放与切换。

### 1.2 本刀明确不做什么（延后）
- **分钟线 `ticker_intraday`**：延后。§3.3 红线——全类型全站每 6 分钟写是 16 亿行/天（物理不可能），只能做受限版，属独立数据源与独立表，另案。
- **L2 全量日线回填（15,801 类型 × 7 区域 ≈ 11 万请求 ≈ 7 小时，仅手动）**：延后。受附录 D.4 约束（history 端点无 `X-Ratelimit-*`，全量前须先小样本观察错误限额）。本刀只做 L0 小样本（≤1200/日），天然规避该未决项。
- 跨站历史价差曲线：ESI `history` 无站点维度（§3.3 固有限制），永远不做，UI 不得暗示。

### 1.3 顺带闭合的已知缺口
**双采集器冲突**：当前 `emd-daemon serve` 与 `emd-app` 的 `spawn_collector` 各自在同一默认库上跑一套 `Scheduler`（都是 T1 6 分钟节拍），彼此无协调、无单实例锁。M3 引入 T3 历史采集前先把它修掉，避免历史采集叠加成更乱的并发。

---

## 2. 架构：单一采集编排器 + 单实例锁

### 2.1 编排器下沉到 emd-core
新增 `emd_core::collector`，把"造 `Scheduler` → 拥有 T1 节拍与 T3 日补 → 跑常驻循环"收敛成**唯一实现**。`emd-daemon`（`serve`/`round`）与 `emd-app`（`spawn_collector`）都改为调用它，删除各自的重复胶水。

职责边界：
- `emd_core::collector`：进程级采集编排（锁、模式判定、T1+T3 循环、状态转发）。
- `emd_core::scheduler`：T1 单轮与节拍（现有逻辑基本不动，被 collector 持有）。
- `emd_core::history`：T3 日线拉取（见 §4）。

### 2.2 单实例锁决定"采集者 / 查看者"
数据目录放一把 OS 级独占锁文件：`%LOCALAPPDATA%\EveMarketDesk\collector.lock`。

- **抢到锁的进程 = 采集者**：跑 T1（6 分钟）+ T3（每日历史）。
- **抢不到的进程 = 查看者**：不开任何采集循环，仅以只读连接打开同一份 WAL 库展示数据。

Windows 实现：以 `CreateFileW` + `dwSharedMode = 0`（或 Rust 侧等价的文件独占打开）持有一个进程存活期的句柄；进程退出句柄自动关闭，锁随之释放，无需清理残留锁文件。抽象成 `emd_core::collector::InstanceLock::acquire(dir) -> Option<Guard>`，`Guard` drop 即释放。非 Windows 平台用 `flock` 兜底（本项目目标平台仅 Windows，但保持核心库可编译）。

效果：`emd serve` 与 Tauri 壳可同时开着，网络请求只有一份；§7"关窗口也继续攒数据、重启即续传"由持锁常驻的 daemon 满足。

### 2.3 状态回传（查看者如何看到采集者的进度）
采集者把 `RoundState`、`next_in`、`snapshot_lm`、令牌余量持续写入共享库（`round_log` / `meta` / `esi_health`，均为现有表）。查看者从库读这些做展示，**不直连采集者进程**（避免新增 IPC/端口，符合"不注册服务"硬约束）。`get_status` 现依赖内存 `latest` watch channel——查看者模式下该 channel 为空，需回落到读 `meta` + `round_log` 最近一轮（见 §5.4）。

---

## 3. 数据模型（迁移 v4）

`crates/emd-core/src/store/schema.rs` 的 `MIGRATIONS` 追加版本 4：

```sql
-- 自选清单：T3 日线回填的目标集。默认灌入 Top 1200 高流动类型（§3.3 L0）。
CREATE TABLE watchlist (
    type_id   INTEGER PRIMARY KEY,
    note      TEXT,
    added_at  INTEGER NOT NULL
);

-- 日线历史。(region,type,date) 天然幂等：同一天重复拉到就 UPSERT 覆盖，绝不逐轮累积。
CREATE TABLE market_history (
    region_id   INTEGER NOT NULL,
    type_id     INTEGER NOT NULL,
    date        TEXT    NOT NULL,   -- 'YYYY-MM-DD'，ESI 原值
    average     REAL,
    high        REAL,
    low         REAL,
    volume      INTEGER,
    order_count INTEGER,
    PRIMARY KEY (region_id, type_id, date)
);
CREATE INDEX ix_mh_type_date ON market_history (type_id, date DESC);
```

### 3.1 容量口径
- `market_history` 主键含 date 是**故意的**——它是 §5 允许的"每轮取一个代表值"归档路径，不是 6 分钟追加表。
- 一年上界（L0）：1200 类型 × 365 天 ≈ 44 万行（The Forge 单 region），远低于 §3.3/§5 红线。
- `watchlist` 是纯用户集，不随时间膨胀。

### 3.2 迁移测试（沿用 `schema.rs` 现有断言风格）
- 新增断言：`market_history` 主键含 `date`（确认它是按日归档，而非被误建成快照式有界表），且**不含**逐轮 ts 累积。
- `versions_are_unique_and_ascending` 的长度断言从 `3` 改为 `4`。

---

## 4. History 拉取（新 `emd_core::history` 模块）

### 4.1 端点与字段
`GET /v1/markets/{region}/history?type_id={t}`（附录 A.4：单类型实测 418 行、gzip 7,925 B、0.96 s）。响应元素字段 `{average, date, high, low, order_count, volume}` → 直接映射 `market_history`。

复用 `EsiClient::get_json`（自带 UA/gzip/退避/预算预检）。**注意**：history 响应**不返回 `X-Ratelimit-*` 头**，令牌走本地估算（`Watermark::cost_of`），且服务端水位 `note_server_remaining` 不触发——这是已接受的行为，靠"每日一次 + 规模受 watchlist 约束"自限。

### 4.2 幂等闸门是 DB，不是缓存
`EsiClient` 内存缓存把 TTL 夹在 `[5s, 360s]`（`MAX_MEM_TTL`），而 history 的 `Expires` 是"次日 11:05 UTC"，会被夹到 360s。因此**不能靠内存缓存保证"每天一次"**。真正的闸门：拉取前查 `market_history` 里"该 (region, type) 是否已有今日 date 行"，有则跳过。

> 这里的"今日 date"指 ESI 数据日（A.4：当日数据次日出现），实现上以"本地日历日是否已对该类型发起过拉取"为准，用 `meta` 记一个 `last_history_pass_date` 防止同一天反复扫全清单；单类型仍做一次"该 region+date 是否已在库"的确认。

### 4.3 触发模型：幂等日一次 + 启动追赶（对齐用户选择）
- 编排器在**启动时**、以及**每个 T1 轮结束后**，各调一次 `run_history_pass()`。
- `run_history_pass()` 判定"今天是否已补完"：若 `meta.last_history_pass_date == 今日`，直接返回（0 请求）。否则挑出 `watchlist` 中当日 `market_history` 尚缺的 (region,type)，分批拉取（并发沿用 16、令牌预算受 §3.1 的 T3 上限 ≤1200/日 约束；跨窗口自然续补，不一次憋完）。
- 错过 11:20 UTC 不敏感：桌面机可能那时没开，"启动追赶"保证开机即补当天缺口。

### 4.4 冷启动合规
遵守 §7"冷启动不得立刻全量爆刷"：`run_history_pass` 与 T1 共用同一 `EsiClient` 的缓存/预算，未到点不发；启动首轮 T1 之前不抢跑历史。

---

## 5. Tauri 命令层（emd-app）

沿用 `impl AppState` 双层（命令体可脱离 Tauri 运行时测试，规避 `tauri::test 不可用`）。

### 5.1 新增命令
- `get_history(type_id: u32, region_id: Option<u32>) -> Vec<HistoryBar>`：读 `market_history`，按 date 升序返回。`region_id` 默认 `REGION_FORGE`。`HistoryBar { date, average, high, low, volume, order_count }`（前端蜡烛 open=昨日 average、close=当日 average）。
- 自选读写（M3 至少需要看，写入可后置但先留命令位）：
  - `get_watchlist() -> Vec<u32>`
  - `add_watch(type_id)` / `remove_watch(type_id)`（写 `watchlist`）

### 5.2 采集落点改造
`spawn_collector` 改为：先 `InstanceLock::acquire`；抢到→调用 `emd_core::collector` 编排器（T1+T3）；抢不到→跳过采集，进入查看者模式。`run()` 里据此决定是否显示"本窗口为只读（采集由另一进程持有）"状态。

### 5.3 emd-daemon 改造
`run_scheduler` 改为调用同一个 `emd_core::collector`；`serve` 走"抢锁→采集者"路径。可选新增 `history` 子命令手动跑一次 `run_history_pass`（便于验收与排障），并同步扩展 `parse_args` 的 `enum Command`、`print_usage` 与 `command_words_map` 测试。

### 5.4 status 在查看者模式回落
`get_status` 内 `latest` channel 为空时，改读 `meta` + `round_log` 最近一行拼 `StatusOut`（复用现有字段，`next_in_ms` 用 `plan_next` 口径从 `round_log.started_at + interval` 估算）。采集者模式行为不变。

---

## 6. 前端（ECharts 蜡烛图）

### 6.1 依赖
`web/package.json` 新增 `echarts`。按需注册（`echarts/core` + `CandlestickChart` + `BarChart` + `DataZoomComponent` + `GridComponent` + `TooltipComponent`），控制包体。

### 6.2 IPC 与 fixture
`api.ts` 加 `history(typeId, regionId?)`、`watchlist()` / `addWatch()` / `removeWatch()`，并配 `fixHistory()` fixture（浏览器 `npm run dev` 无壳也能渲染；数据形状与后端一致，数值取 A.4 实测口径）。

### 6.3 视图
现状三栏无标签页。M3 在**单物品详情（OrderBook 面板）加"历史"页签**，容纳 `HistoryChart` 组件：
- 主图：ECharts 蜡烛（open=昨 avg、close=当 avg、high/low）+ 成交量柱 + `dataZoom`（滚轮缩放、拖拽平移）。
- 区间按钮 1D / 1W / 1M / 3M / 1Y：切换用 `setOption` 增量更新，不重建实例，动画 300ms（§6）。
- **1D（分钟线）本刀置灰**并标"分钟线待后续"（分钟线已延后，无数据源，不给空图误导）。
- 诚实标注（§6）：跨站对比"仅当前快照，非历史"；图旁显示 `本地已积累 x/365 天`，按**该类型**实际积累天数（`market_history` 行数），不足处留空档不外推。

### 6.4 滚动/选中保持
沿用 M2 的 `rowKey = type_id` 锚点保持；历史页签切换不触发 T1 重拉。

---

## 7. 测试策略

- **emd-core `market_history` 幂等**：同 (region,type,date) UPSERT 两次只留一行；主键含 date 断言；迁移 v4 版本计数断言。
- **`run_history_pass` 判定**：`Db::in_memory()` + 注入假 fetch，验证"今日已补 → 0 请求""缺数据 → 只补缺的 (region,type)""≤1200/日 上限"。
- **双采集器回归**：同一 DB 路径起两个编排器，第二个 `acquire` 返回 None → 进查看者、**不产生任何网络请求**（计数断言）；释放锁后可再抢到。
- **status 查看者回落**：无 `latest` channel 时能从 `round_log`+`meta` 拼出非零 `StatusOut`。
- **emd-app**：新命令体放 `impl AppState`，`get_history` 进 `tests.rs`（脱 Tauri 测）。
- **合规不回归**：M0.5 五闸门不动；history 走同一 UA/gzip/退避栈。
- **前端**：`npm run typecheck` 必过；`fixHistory()` 下蜡烛图肉眼验收。

---

## 8. 验收标准（M3 判据）

1. 自选清单类型跑 T3 后，`market_history` 有对应日线，最新日期到"昨天"（当日次日出现）。
2. 单物品"历史"页签渲染蜡烛 + 成交量，`dataZoom` 缩放拖拽顺、区间切换增量更新不重建实例；1D 置灰不误导。
3. `emd serve` 与 Tauri 壳同开，网络请求只有一份（锁生效，采集者唯一）。
4. 冷启动：库内已有当日数据时不重复打 history（0 额外请求）。
5. 按类型显示的真实积累天数 = 该类型 `market_history` 行数，不外推。

---

## 9. 涉及文件清单

- `crates/emd-core/src/collector.rs`（新）：编排器 + `InstanceLock`。
- `crates/emd-core/src/history.rs`（新）：T3 拉取与幂等。
- `crates/emd-core/src/lib.rs`：导出 `collector`、`history`。
- `crates/emd-core/src/store/schema.rs`：迁移 v4 + 测试。
- `crates/emd-core/src/store/db.rs`：`watchlist` / `market_history` 读写与查询方法。
- `crates/emd-core/src/scheduler.rs`：暴露钩子供 collector 在轮后调 `run_history_pass`（或改由 collector 编排，二选一，实现时定，优先"collector 编排、scheduler 保持纯 T1"）。
- `crates/emd-core/src/market/*`：如需 `HistoryBar` 实体，就近放。
- `crates/emd-daemon/src/main.rs`：`run_scheduler` 改走 collector；可选 `history` 子命令 + `parse_args`/`print_usage`/测试同步。
- `crates/emd-app/src/lib.rs`：`get_history`/watchlist 命令、`impl AppState` 命令体、collector 接入 + 锁分流、status 查看者回落；`tests.rs` 补测。
- `web/package.json`：`echarts` 依赖。
- `web/src/api.ts` / `types.ts` / `store.ts` / `components/HistoryChart.tsx`（新）/ `components/OrderBook.tsx`（加页签）。

---

## 10. 风险与约束遵循（对齐方案 §11 与硬约束）

- **ESI 合规**：复用同一 `EsiClient`（UA 含邮箱、强制 gzip、Expires/304、退避、令牌桶、服务端水位降速、连接池）。4xx 不重试；history 无服务端头 → 本地估算 + 日一次自限。
- **不注册服务 / 无公网入口**：查看者与采集者靠共享库 + 文件锁协调，不新增端口/服务。
- **密钥走 keyring**：M3 不涉及新密钥（AI/飞书密钥属 M6/M7，本刀不碰）。
- **AI 仅手动、飞书仅公开数据**：本刀无 AI/推送代码路径。
- **容量红线**：`market_history` 按日归档、`watchlist` 有界；不留任何 6 分钟级历史表（与 `station_orders`/`hub_pool` 同等纪律）。
- **附录 D.4**：本刀只 L0 小样本，不触发 L2 全量；留档说明，待专门复测再放开。

---

## 11. 未决 / 实现期待定项

1. `run_history_pass` 与 `scheduler` 的耦合方式：优先"collector 拥有 T1+T3 循环、scheduler 保持纯 T1 单轮"，实现时若发现改 surface 过大，退化为"scheduler 加一个轮后回调"。二者对上层（daemon/app）接口一致。
2. "今日 date"以 UTC 还是本地日历为准：倾向 UTC（与 ESI `Expires`=次日 11:05 UTC、日报 11:15 UTC 同一时间轴），UI 再并显本地时间（§6）。实现时确认 `market_history.date` 存 ESI 原值（UTC 日历日），`last_history_pass_date` 用 UTC。
3. watchlist 默认 Top 1200 的"高流动"判据来源：可先用现有 `station_orders` 双向盘按 24h 量/档位排序近似，正式判据（历史量）待 M4/M5 数据积累后替换。M3 用近似即可，但需在 UI 标注为"建议清单"。
