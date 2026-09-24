mod aggregate;
mod entities;
pub mod flip;
pub mod history;
pub mod lifecycle;
mod hubs;
mod orderbook;

pub use aggregate::{aggregate, AggregateOptions, PriceLevel, Side, StationOrderBook};
pub use entities::{LocationKind, Order, OrdersResponse};
pub use flip::{
    scan, trial, FeeModel, FlipParams, Opportunity, PairVerdict, ScanOutcome, ScanStats, VolSource,
};
pub use history::{
    backfill, fetch_one, history_due, Fetched, HistoryConfig, PassReport, Estimate,
    ESI_WINDOW_DAYS, L0_DAILY_CAP, L1_DAILY_CAP, TIER_L0, TIER_L1,
};
pub use hubs::{hub_pool, Hub, DEFAULT_MIN_ORDERS, DEFAULT_TOP};
pub use orderbook::{fetch_region_orders, RoundOutcome};

/// The Forge —— 吉他所在星域。
pub const REGION_FORGE: u32 = 10000002;
/// Jita IV – Moon 4 – Caldari Navy Assembly Plant。
pub const STATION_JITA: u64 = 60003760;

/// §4.1 默认的聚合口径：45 天龄、3 笔起、留 5 档深度。
pub fn aggregate_opts() -> AggregateOptions {
    AggregateOptions::default()
}

/// 四个主枢纽星域，实测 `X-Pages` 合计 786。
pub const HUB_REGIONS: [(&str, u32); 4] = [
    ("The Forge", 10000002),
    ("Domain", 10000043),
    ("Metropolis", 10000042),
    ("Heimatar", 10000030),
];

/// 跨区单簿读取年龄闸门：45 min ≈ 3–4 个 T1.5 周期。
/// 上批数据可用，但过期数据必须消失，否则会拿 45 分钟前的价格继续配对（诚实口径）。
pub const XREGION_MAX_AGE_SECS: i64 = 45 * 60;
/// 跨区旧行剪除线（表有界的最后一道保险）。
pub const XREGION_PRUNE_SECS: i64 = 24 * 3600;
