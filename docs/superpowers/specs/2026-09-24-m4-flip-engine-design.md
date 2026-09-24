# M4 倒卖引擎设计规格（M4a 实现 / M4b·M4c 设计锁定）

> 日期：2026-09-24 · 上游方案：`C:\Users\Administrator\Desktop\EVE欧服市场客户端-开发方案v3.1.md` §4
> 本文档是 M4 的唯一实施依据。与 v3.1 方案冲突之处，以 §0 的纠偏与裁决为准。

---

## 0. 对方案数字的纠偏与裁决（先读这节）

| # | 方案原文 | 本规格 | 依据 |
|---|---|---|---|
| 0.1 | §4.1 默认 `sales_tax_pct=1.0`、`broker_fee_pct=2.0` | 默认 **7.5 / 3.0** | 官方 2025-03-12 补丁（Version 22.02）销售税 4%→**7.5%**；CCP 帮助页"中介费初始费率 3%"。用户指示"以游戏内默认状态为准"。可调区间保留（销售税 0–8、中介费 0–5） |
| 0.2 | 技能影响未定义 | **Accounting** 每级对销售税**相对 −11%**；**Broker Relations** 每级对中介费**绝对 −0.3pp**，地板 `min(1%, 基率)`；声望项公式预留、M4 固定 0 | CCP 官方帮助页公式 `3%−(0.3%×BR)−(0.03%×势力)−(0.02%×军团)`，下限 1%；EVE University Wiki（2026-08 快照）：`base 7.5%，−11%/级 → 满级 3.37%` |
| 0.3 | 运费 `isk_per_m3_per_jump` | M4a 改**单件 ISK**（默认 0，标"未含运费"）；m3×跳数模型挂账（跳数需要星图路由数据，M4a 不引入） | 裁决：无跳数来源时 m3 模型不可算 |
| 0.4 | 排序键含 24h 成交量 | `vol24` 来源 = `market_history` 最近一日 volume；缺失回落 `q_eff` 并标 `vol_source=Depth`（回落到深度估算） | 用户确认（history 仅覆盖 L0/L1） |
| 0.5 | 方案未定义"角色负收益"数据面 | M4c 三形态判定 + **订单级**推送契约（§4.3）；`transaction_id` 为已实现轨唯一标识 | 用户三轮确认 |
| 0.6 | 硬约束"飞书只带公开数据" | **用户显式豁免**（2026-09-24）：仅角色亏损提醒卡片可携带私有数据，卡面标"私有数据"角标；其余飞书卡片仍只带公开数据 | 用户确认（豁免记录见 §4.5） |

---

## 1. 范围与切片

- **M4a（本次实现）**：`emd-core::market::flip` 计算核心（费率模型/技能修正/扫描/排序）+ 参数与技能面板 + 扫描器视图 + **试算行**（M4c 的最小可行替代：手输买卖价即时出税后净利，负数标红）+ daemon `flip` 子命令。
- **M4b（后续）**：T1.5 每 12 分钟跨星域补拉（Amarr `10000043` / Dodixie `10000042` / Rens `10000030`）、`opportunities` 表（迁移 v5）与机会状态机（new→notified→expired/invalidated、冷却与每日上限）。
- **M4c（后续）**：SSO 角色挂链 + 亏损提醒 + 飞书私有卡片（设计锁定见 §4，实现排 M4b 后，迁移 v6）。

依赖关系：M4a 不依赖 M4b/M4c；M4b 的候选挑选复用 M4a 的 `flip::scan`；M4c 的预期轨复用 M4a 的 `FeeModel`。

---

## 2. 计算口径（M4a 核心）

### 2.1 FeeModel（技能修正层，唯一费率出口）

```rust
pub struct FeeModel {
    pub sales_tax_pct: f64,      // 默认 7.5
    pub broker_pct: f64,         // 默认 3.0
    pub accounting: u8,          // 0–5，默认 0（= 无技能影响口径）
    pub broker_relations: u8,    // 0–5，默认 0
    pub faction_standing: f64,   // 公式预留，M4 固定 0
    pub corp_standing: f64,      // 公式预留，M4 固定 0
}
```

- `effective_sales_tax() = (sales_tax_pct/100) × (1 − 0.11 × accounting)`，下界 0
- `effective_broker() = max(broker_pct/100 − 0.003×br − 0.0003×faction − 0.0002×corp, floor)`
  - **裁决**：`floor = min(0.01, broker_pct/100)` —— 官方 1% 地板只在基率 ≥1% 时成立，基率被调低时地板不反超基率
- 技能等级越界值（>5）在反序列化后由 `clamp(0,5)` 兜底
- 默认构造 `FeeModel::default()` = 游戏内无技能默认状态（7.5% / 3.0%，技能全 0）

技能验证锚点（写入单测）：
- A=0 → 7.5%；A=5 → 7.5×0.45 = **3.375%**
- B=0 → 3.0%；B=5 → 3.0−1.5 = **1.5%**
- 地板：`broker_pct=1.2, BR=5` → 1.2−1.5=−0.3 → **1.0%**（min(1%,1.2%) 地板生效）

### 2.2 FlipParams（扫描参数，持久化到 `meta` 表 KV）

```rust
pub struct FlipParams {
    pub fees: FeeModel,
    pub margin_threshold_pct: f64, // 默认 3.0
    pub capital_isk: f64,          // 默认 100_000_000
    pub capital_pct_per_trade: f64,// 默认 5.0（单笔投入上限 = 资金 × 5%）
    pub min_batch: u64,            // 默认 100（件）
    pub freight_isk_per_unit: f64, // 默认 0（UI 标注"未含运费"）
    pub include_buy_broker: bool,  // 默认 false（吃单买入不付中介费；挂买单策略可选计入）
}
```

### 2.3 扫描算法 `flip::scan`

签名：`pub fn scan(books: &[StationOrderBook], hubs: &[Hub], params: &FlipParams, vol24: &HashMap<u32, u64>) -> Vec<Opportunity>`

```
枢纽集 = hubs 的 location_id 集合（hub_pool 已保证 NPC 站 + ≥50 单 + 前 20）
按 type_id 分组单簿（仅保留枢纽集内、且该方向未被薄档剔除的单簿）
对每个 type_id，对每个有序站对 (买站 a, 卖站 b)，a ≠ b：
  1. 目标量候选：want = min( a 卖侧可执行深度, b 买侧可执行深度,
                             floor(capital × pct% ÷ a 的 best_ask 估价) )
     want < min_batch → 丢弃
  2. exec_ask = a.executable(Side::Buy, want)   // 加权吃单价 ← 买入成本
     exec_bid = b.executable(Side::Sell, want)  // 加权吃单价 ← 卖出毛额
     q_eff = min(filled_buy, filled_sell)；q_eff < min_batch → 丢弃（短填如实降级）
  3. 卖出毛额 = exec_bid.0 × q_eff
     卖出净额 = 卖出毛额 × (1 − effective_broker − effective_sales_tax)   ← 税基是全额
     成本     = exec_ask.0 × q_eff + freight_isk_per_unit × q_eff
                (+ exec_ask.0 × q_eff × effective_broker, 仅 include_buy_broker=true)
     净利     = 卖出净额 − 成本
     净利率   = 净利 ÷ 成本 × 100
  4. 过滤：净利率 < margin_threshold_pct → 丢弃
  5. vol24：history 表有覆盖 → (值, History)；否则 (q_eff, Depth)
输出排序：净利率 × ln(1 + vol24) 降序；同分按 (type_id, buy_loc, sell_loc) 稳定排序
```

**裁决**：预算 `want` 以 `best_ask` 估算、结算用加权价，实际投入可能略低于预算——不回退重算，保证确定性；UI 上"可成交量"即 `q_eff` 如实展示。

**Opportunity 输出字段**（frontend 与 daemon 共用）：

```rust
pub struct Opportunity {
    pub type_id: u32,
    pub buy_loc: u64, pub sell_loc: u64,
    pub buy_price: f64, pub sell_price: f64, // 均为加权价
    pub qty: u64,
    pub net_per_unit: f64,
    pub net_total: f64,
    pub margin_pct: f64,
    pub vol24: u64,
    pub vol_source: VolSource, // History | Depth
    pub buy_levels: u32,       // 买站卖侧档位笔数
    pub sell_levels: u32,      // 卖站买侧档位笔数
}
```

### 2.4 试算（TrialCalc，M4c 最小可行替代）

输入 `(buy_price, sell_price, qty)` + 当前 `FlipParams`，输出 `(net_per_unit, net_total, margin_pct)`：

```
净额 = sell_price × (1 − broker − tax)
成本 = buy_price (+ buy_price × broker 若 include_buy_broker) (+ freight_per_unit)
净利 = (净额 − 成本) × qty；负数即"扣税后亏损"
```

**裁决**：试算在 Rust 侧实现（`trial_calc` 命令），TS 不复制费率公式，避免双源漂移。

### 2.5 诚实性角标

- 扫描器与试算区：**"基于估算费率"** 角标常驻（费率可调、非官方接口读取）
- 技能非 0 时加挂：`技能口径：Accounting A · BR B`
- 全 0 时角标写：`技能口径：无影响（游戏默认状态）`

---

## 3. M4a IPC、daemon 与 UI

### 3.1 emd-app 命令（4 个）

| 命令 | 行为 |
|---|---|
| `scan_flip()` | 读当前 `station_orders` 快照 → `aggregate()` → `hub_pool()` → 读 history 最近一日 volume → `flip::scan()` → 附类型名/站点真名/快照年龄返回 |
| `get_flip_params()` | `meta` 表 KV 读；无记录 → `FlipParams::default()` |
| `set_flip_params(p)` | 校验（技能 0–5、数值非负、pct 范围）→ 写 `meta`；前端收到成功后立即重调 `scan_flip`（改参数/改技能 → 秒级重算，零 ESI 请求） |
| `trial_calc(buy, sell, qty)` | 用当前参数算单笔试算，负数由前端标红 |

### 3.2 daemon `flip` 子命令

`emd-daemon flip [--db <path>] [--top N]`：读真库跑一遍 `flip::scan`，打印 Top N 机会表（类型名/买站→卖站/买价/卖价/可成交量/净利率/净利/vol24 来源）；0 机会时输出丢弃原因分布（want<min_batch / 短填 / 未过阈值）。真机验收不依赖 UI。

### 3.3 web 前端

- TopBar 增顶层视图切换 **「市场 / 倒卖」**；倒卖视图 = 全宽扫描器
- 扫描器 = 参数面板（可折叠）+ 机会表：
  - 列：类型名 / 买站→卖站 / 买价 / 卖价 / 可成交量 / 净利率 / 单位净利 / 总净利 / 24h 量(含来源角标 History·Depth) / 买站档位笔数 / 卖站档位笔数
  - 排序键可切：`净利率×log量`（默认）/ `单笔绝对利润`
  - 数字列右对齐（沿用单簿对齐规范），负净利率标红
- 参数面板：费率基、阈值、资金、单笔比例、最小批量、单件运费、买入侧佣金开关 + **技能区**（Accounting / Broker Relations 步进器 0–5，旁显有效费率，如"销售税 7.5% → 3.38%"）
- 试算行：输入买价/卖价/数量 → 调 `trial_calc` → 显示单位净利/总净利/净利率，负数标红并提示"扣税后亏损：改技能等级或放弃此单"
- fixture 模式（`inTauri=false`）：`fixFlip()` 造 3 条机会（含 1 条负 margin 演示）+ `fixTrial()`；参数/技能改动在 fixture 下本地重算
- 空态区分：无快照 → "先跑一轮采集"；有快照 0 机会 → 显示原因分布

### 3.4 M4a 验收判据

1. 单测绿：费率锚点（§2.1 三组）、§4.1 算例（ask=100/bid=110/合计费率 8% → 净利 **1.2** 铁证）、技能单调性（A/B 升 → margin 不降）、四类丢弃（want<批量/短填/未过阈/同站对）、排序稳定性、参数 serde 往返、技能越界 clamp
2. `cargo test -p emd-core -p emd-daemon -p emd-app` 全绿；`npx tsc --noEmit` = 0；`vite build` 绿
3. daemon `flip` 真库跑通（有数据出 Top N；无数据出原因分布）
4. 浏览器复验：技能 0→5 切换后机会集/margin 变化且角标同步；试算行负数标红；表格列对齐 <1px；控制台 0 error

---

## 4. M4c 设计锁定（实现排 M4b 后）

### 4.1 授权与令牌

- EVE SSO **PKCE**（原生应用，无 client secret）：系统浏览器跳转 + loopback 回调收 code
- `refresh_token` 存 **keyring**（Windows Credential Manager）；DB 不落令牌
- `client_id` 设置页可配；令牌刷新仅在过期时发生

### 4.2 同步管线（跟 T1 节拍，每轮 ≤4 请求）

| 数据 | 端点 | 方式 |
|---|---|---|
| 活跃订单 | `GET /v2/characters/{id}/orders/` | 整表覆盖（尊重 Expires）；上轮快照进 `char_orders` 供状态边沿判定 |
| 交易流水 | `GET /v1/characters/{id}/wallet/transactions/` | `since` 增量；首启只拉 **90 天**建 FIFO 成本基准，覆盖不到的类型标"成本未知"不参与判定 |
| 日记账 | `GET /v1/characters/{id}/wallet/journal/` | `since` 增量；取**实际**中介费/销售税真值 |
| 技能（可选） | `GET /v4/characters/{id}/skills/` | "读取真实技能"一键填入面板，仍允许手动覆盖 |

### 4.3 负收益判定（三形态全启）与推送契约

```
① 挂卖单预期亏：单位预期净额 = 挂价 × (1 − sales_tax(A))
                 单位全成本  = FIFO 平均成本 + 挂单实付中介费/单位（journal 真值）
                 预期亏 ⟺ 净额 < 全成本
② 挂买单套牢亏：本站当前可执行卖出净额 exec_bid(q)×(1 − tax(A) − broker(B))
                 < 买单成交价 + 实付中介费/单位 ⟹ 即时浮亏
③ 已实现成交亏：卖出所得 − 实付销售税 − FIFO 成本 − 两侧实付中介费 < 0（journal 真值）
```

**判定轨道**：预期轨用 `FeeModel`（技能改动只重算预期轨）；已实现轨用 journal 真值（不受技能面板影响）。

**AlertPayload（订单级契约，一单一载荷）**：

```rust
pub struct AlertPayload {
    pub alert_key: String,        // 挂单轨 = order_id；已实现轨 = transaction_id
    pub kind: AlertKind,          // ExpectedSellLoss | RealizedLoss | BuyOrderTrap
    pub order_id: u64,            // 已实现轨 = 回填匹配到的原挂单 id，匹配不上 = 0 并标注
    pub type_id: u32,
    pub type_name: String,
    pub location_id: u64,
    pub location_name: String,
    pub is_buy: bool,             // 订单方向
    pub price: f64,
    pub volume: u64,              // 挂单轨 = volume_remain；已实现轨 = 成交数量
    pub at: DateTime<Utc>,        // 挂单轨 = issued；已实现轨 = 成交时间
    pub loss_isk: f64,            // 预计/已实现亏损额（正数）
    pub margin_pct: f64,
    pub caliber: CaliberSummary,
}
```

**ESI 事实澄清**：wallet transactions **不返回 order_id**（字段仅 transaction_id/type/price/qty/location/date/…）。已实现轨唯一标识用 `transaction_id`；同时做回填匹配（同 type + 同方向 + 同价 + 数量 ≤ 当时 remain + 在挂单存活期内）带出原 `order_id`；匹配不上卡片明示"成交流水 tx#…（原挂单未在本机观察窗内）"。

**CaliberSummary**：轨道（预期·估算费率 / 已实现·journal 真值）、有效销售税%、有效中介费%、技能口径串、单位成本 + 来源（FIFO 90 天 / 成本未知）、一行公式串、数据年龄。

### 4.4 告警状态机与限额

- **边沿触发**：首次转负才告警；预期亏加深 ≥2pp 补推
- 去重键：`order_id`（挂单轨）/ `transaction_id`（已实现轨）；冷却期内不重推
- 每日 ≤5 条 = **订单条目数**；合并卡片 = 多条目列表，**每条仍含全量字段**；超限顺延次日
- 提醒中心不受限额，全量留存；推送与中心共用 `AlertPayload` 序列化（杜绝双源漂移）

### 4.5 飞书私有卡片与豁免记录

- 版式：标题 `[亏损提醒] {type_name} · {买/卖}单 · {站点真名}`；主体字段表逐行列出 order_id（或 tx#）/type_id/location_id/方向/价格/数量/时间/亏损额/负 margin%；折叠区口径摘要；页脚角标（**私有数据** / 基于估算费率[仅预期轨]）+ 告警时刻 + "客户端提醒中心可按 order_id 检索"
- **豁免记录**：用户于 2026-09-24 显式豁免"飞书只带公开数据"约束，**仅限角色亏损提醒卡片**；其余飞书卡片仍只带公开数据；若用户收回豁免则回退本地提醒中心方案（通道抽象不变）

### 4.6 存储（迁移 v6，M4c 时落）

`char_meta`（角色 id/名/同步水位）、`char_tx`（流水缓存 + FIFO 基准）、`char_orders`（上轮挂单快照）、`alerts`（状态机：key/状态/首末见/推送计数）。M4b 占 v5（`opportunities`），互不抢占。

---

## 5. 裁决记录汇总（Rulings）

| 编号 | 裁决 | 代价（若错） |
|---|---|---|
| R1 | 费率基默认 7.5/3.0（覆盖方案 1.0/2.0） | 机会数量比旧口径大幅减少——这是真实成本，不是缺陷 |
| R2 | 中介费地板 `min(1%, 基率)` | 与官方 1% 地板在基率<1% 时的边界行为差异，影响可忽略 |
| R3 | 运费用单件 ISK；m3×跳数挂账 | M4a 无法算"按体积计费"的搬运成本，用户手填单件近似 |
| R4 | 预算按 best_ask 估算、不回退重算 | 实际投入可能略低于预算上限，确定性优先 |
| R5 | vol24 缺失回落 q_eff 并标 Depth | 排序偏好深挂单类型而非真活跃类型（有来源角标可辨） |
| R6 | 试算在 Rust 侧（trial_calc 命令） | 每次试算多一次 IPC，换来费率公式单源 |
| R7 | 已实现轨唯一标识 transaction_id + 回填 order_id | 部分成交卡片 order_id=0，需用户按 tx# 核对 |
| R8 | M4a 试算行替代 M4c 全部 SSO 能力 | 无自动监测；需用户手动输入价格试算 |

## 6. 里程碑映射

- M4a → 本文 §2、§3（本次实现）
- M4b → §1 定义（T1.5 + 状态机 + 迁移 v5）
- M4c → §4（SSO + 提醒 + 迁移 v6 + 飞书私有卡片）
