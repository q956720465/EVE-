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
