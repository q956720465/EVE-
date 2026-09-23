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
