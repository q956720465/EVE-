//! # emd-core
//!
//! EVE 欧服市场客户端的核心层：ESI 访问、节流、存储与订单簿聚合。
//! 刻意不依赖 Tauri，使 M0.5 合规冒烟测试可以脱离 WebView 单独跑。
//!
//! 设计依据见《EVE 欧服市场客户端-开发方案 v3.1》§3.1、§3.4、§4.1、§5。

pub mod catalog;
pub mod collector;
pub mod compliance;
pub mod config;
pub mod error;
pub mod esi;
pub mod market;
pub mod scheduler;
pub mod sso;
pub mod store;
pub mod tree;

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
