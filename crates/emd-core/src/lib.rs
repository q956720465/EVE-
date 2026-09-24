//! # emd-core
//!
//! EVE 欧服市场客户端的核心层：ESI 访问、节流、存储与订单簿聚合。
//! 刻意不依赖 Tauri，使 M0.5 合规冒烟测试可以脱离 WebView 单独跑。
//!
//! 设计依据见《EVE 欧服市场客户端-开发方案 v3.1》§3.1、§3.4、§4.1、§5。

pub mod alert;
pub mod catalog;
pub mod char;
pub mod collector;
pub mod compliance;
pub mod config;
pub mod error;
pub mod esi;
pub mod market;
pub mod push;
pub mod scheduler;
pub mod sso;
pub mod store;
pub mod tree;

pub use alert::{
    consumed_lot_costs, detect_buy_trap, detect_expected_sell, detect_realized, order_alert_key,
    tx_alert_key, AlertKind, AlertPayload, CaliberSummary, NameLookup,
};
pub use char::{sync_character, CharSyncReport, JournalEntry, SkillLevel};
pub use char::fifo::{fifo_costs, CostSource, FifoCost};
pub use config::EsiConfig;
pub use error::{Error, Result};

/// 便捷构造：默认配置 + 指定 UA。
pub fn client(user_agent: impl Into<String>) -> Result<esi::EsiClient> {
    let cfg = EsiConfig {
        user_agent: user_agent.into(),
        ..Default::default()
    };
    esi::EsiClient::new(cfg)
}
