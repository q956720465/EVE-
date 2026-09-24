// 与 Rust 侧 serde 输出严格对应（emd-core::store / emd-core::scheduler）。
// 改任何一边都要同时改另一边；字段名保持 snake_case 是因为 Rust 端没做 rename。

export interface TreeGroup {
  group_id: number;
  name: string;
  type_count: number;
}

export interface TreeNode {
  category_id: number;
  name: string;
  groups: TreeGroup[];
}

export interface ListingRow {
  type_id: number;
  name: string;
  best_bid: number | null;
  bid_qty: number;
  best_ask: number | null;
  ask_qty: number;
  bid_levels: number;
  ask_levels: number;
}

export interface PriceLevel {
  price: number;
  volume: number;
  orders: number;
}

export interface TypeDetail {
  type_id: number;
  name: string;
  location_id: number;
  location_name: string;
  bid_depth: PriceLevel[];
  ask_depth: PriceLevel[];
  bid_levels: number;
  ask_levels: number;
  skipped_stale: number;
  skipped_thin: number;
  skipped_wholesale: number;
  snapshot_lm: string | null;
  updated_at: number;
}

export interface AppStatus {
  round: number;
  stage: string;
  last_seconds: number;
  orders: number;
  rows_written: number;
  hubs: number;
  next_in_ms: number;
  snapshot_lm: string | null;
  remaining_tokens: number;
  jita_rows: number;
  tree: [number, number, number];
  /** 本进程是否在采集。false = 采集锁在另一进程手里，界面是只读查看者。 */
  collecting: boolean;
}

// emd-core::store::HistoryBar —— 一条日线。字段是实测的 highest/lowest，
// 不是方案 §5 纸面上的 high/low。
export interface HistoryBar {
  date: string;
  average: number | null;
  highest: number | null;
  lowest: number | null;
  volume: number;
  order_count: number;
}

export interface Hub {
  location_id: number;
  order_count: number;
  share_pct: number;
  rank: number;
  name: string;
}

// ---- M4a 倒卖引擎（emd-core::market::flip，字段与 serde 输出一致） ----

export interface FeeModel {
  sales_tax_pct: number;
  broker_pct: number;
  accounting: number;
  broker_relations: number;
  /** 公式预留，M4 固定 0。 */
  faction_standing: number;
  corp_standing: number;
}

export interface FlipParams {
  fees: FeeModel;
  margin_threshold_pct: number;
  capital_isk: number;
  capital_pct_per_trade: number;
  min_batch: number;
  /** 单件运费（ISK），默认 0 且 UI 标注"未含运费"。 */
  freight_isk_per_unit: number;
  include_buy_broker: boolean;
}

/** "history" = 真 24h 量；"depth" = 无 history 覆盖时的深度估算。 */
export type VolSource = "history" | "depth";

export interface FlipRow {
  type_id: number;
  type_name: string;
  buy_loc: number;
  buy_loc_name: string;
  sell_loc: number;
  sell_loc_name: string;
  buy_price: number;
  sell_price: number;
  qty: number;
  net_per_unit: number;
  net_total: number;
  margin_pct: number;
  vol24: number;
  vol_source: VolSource;
  buy_levels: number;
  sell_levels: number;
  /** 跨区行的数据年龄（秒）；null/undefined = 常规枢纽行，数据来自本轮 T1。 */
  xregion_age_secs?: number | null;
}

export interface FlipScan {
  rows: FlipRow[];
  pairs_evaluated: number;
  dropped_batch: number;
  dropped_shortfall: number;
  dropped_threshold: number;
  age_secs: number | null;
  params: FlipParams;
  /** 有效费率（%）由 Rust 算好，面板只显示（spec R6：TS 不复制公式）。 */
  effective_sales_tax_pct: number;
  effective_broker_pct: number;
}

export interface TrialOut {
  net_per_unit: number;
  net_total: number;
  margin_pct: number;
}

export type FlipSortKey = "score" | "profit";

// ---- M4c 提醒中心（emd-app 的 AlertRow / AlertSettings / SsoStatus） ----
// 字段名与 Rust 侧 serde 输出逐字对应；枚举一律是 snake_case 串（Rust 的 as_str 映射是唯一源）。

/** 告警形态（emd-core::alert::AlertKind）。 */
export type AlertKind = "expected_sell_loss" | "buy_order_trap" | "realized_loss";

/** 告警状态（emd-core::alert::AlertState）。cleared = 亏损消失后的周期结束，行不删。 */
export type AlertState = "new" | "notified" | "cleared";

/**
 * 口径摘要（emd-core::alert::CaliberSummary）。
 * 这是**从 payload JSON 里读出来的**，不是后端另发的第二份结构：`AlertPayload` 是推送卡片与
 * 提醒中心共用的唯一序列化出口（spec §4.4），口径必须来自同一处，界面不重算。
 */
export interface CaliberSummary {
  /** 轨道串（预期·估算费率 / 已实现·journal 真值）。 */
  track: string;
  /** 本条判定真正用到的有效销售税（%）。 */
  sales_tax_pct: number;
  /** 本条判定真正用到的有效中介费（%）。 */
  broker_pct: number;
  skill_caliber: string;
  /** 单位成本（与 loss_isk 同口径）。 */
  unit_cost: number;
  cost_source: string;
  /** 一行公式串（带本条的实际数字，可拿计算器复算）。 */
  formula: string;
  data_age_secs: number;
}

/** `alerts.payload` 列的 JSON 文本解出来的载荷（与推送卡片同一份）。 */
export interface AlertPayload {
  alert_key: string;
  kind: AlertKind;
  /** 已实现轨 = 回填匹配到的原挂单 id；匹配不上 = 0。 */
  order_id: number;
  type_id: number;
  type_name: string;
  location_id: number;
  location_name: string;
  is_buy: boolean;
  price: number;
  /** 挂单轨 = 剩余量；已实现轨 = 成交数量。 */
  volume: number;
  at: string;
  loss_isk: number;
  margin_pct: number;
  caliber: CaliberSummary;
}

/** 提醒中心的一行（emd-app::AlertRow）。 */
export interface AlertRow {
  /** 去重键 `order:{id}` / `tx:{id}`（TEXT）：拿去库里检索那张单/那笔成交的唯一入口。 */
  alert_key: string;
  kind: AlertKind;
  char_id: number;
  type_id: number;
  type_name: string;
  location_id: number;
  location_name: string;
  is_buy: boolean;
  first_seen_at: number;
  last_seen_at: number;
  /** 本轮观测到的亏损额（正数 ISK）。 */
  last_loss_isk: number;
  /** 本轮观测到的亏损率（%，负 = 亏）。 */
  last_margin_pct: number;
  state: AlertState;
  notified_at: number | null;
  notified_day: string | null;
  /** 本条目**今天**被推送的条数（当日全局用量是各条目之和）。 */
  notified_count_day: number;
  /** 上次推送时的**亏损率**（pp）—— 不是金额。 */
  last_notified_margin_pct: number | null;
  /** `AlertPayload` 的 JSON 原文（口径摘要从这里读）。 */
  payload: string;
}

/** 通道配置的回显（emd-app::AlertSettings）：webhook 已打码，密钥只有"是否已配置"。 */
export interface AlertSettings {
  webhook: string;
  secret_set: boolean;
  enabled: boolean;
  /** 本轮真会装上的通道名（local 恒在；dingtalk 需开关 + webhook）。 */
  channels: string[];
}

/** `alert_settings_set` 的入参。secret 的三态就是 Rust 侧 `save_editing` 的三态。 */
export interface AlertSettingsIn {
  /** 打码值（含 `***`）= 保留库里那条；明文 = 新值；空串 = 摘掉 webhook。 */
  webhook: string;
  /** 缺省/undefined = 不改密钥；"" = 显式清空；非空 = 换新密钥。 */
  secret?: string | null;
  enabled: boolean;
}

/** SSO 挂链状态（emd-app::SsoStatus）。**里面没有任何令牌文本**。 */
export interface SsoStatus {
  linked: boolean;
  char_id: number | null;
  char_name: string | null;
  /** 库内 char_meta.last_sync_at（角色 id 由令牌给出）。 */
  last_sync_at: number | null;
  expires_at: number | null;
  token_expired: boolean;
  char_sync_enabled: boolean;
  client_id_set: boolean;
  /** 令牌在但解不出角色时的原因（不含令牌原文）。 */
  token_error: string | null;
}

/** 登录成功的回执（不含令牌）。 */
export interface SsoLoginOut {
  char_id: number;
  name: string;
}
