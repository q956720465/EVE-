use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("HTTP 传输失败 {url}: {source}")]
    Transport {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// 非 2xx/3xx。4xx 每次消耗 5 令牌，故不可盲目重试。
    #[error("ESI 返回 {status} for {url}")]
    Status { url: String, status: u16 },

    #[error("被限流：429，需等待 {retry_after:?}")]
    RateLimited { retry_after: Option<u64> },

    #[error("错误限额水位过低（X-Esi-Error-Limit-Remain={remain}），主动暂停")]
    ErrorBudget { remain: u32 },

    /// 窗口预算不足。调用方应等待而非抢占。
    #[error("15 分钟窗口令牌不足：需 {need}，剩 {remaining}")]
    BudgetExhausted { need: u32, remaining: u32 },

    #[error("响应解析失败 {url}: {source}")]
    Parse {
        url: String,
        #[source]
        source: serde_json::Error,
    },

    /// 同一轮逐页拉取时 `Last-Modified` 发生变化 —— 快照不再自洽，
    /// 半旧半新会把跨站价差算错，必须丢弃整轮。
    #[error("分页快照漂移：期望 {expected:?}，第 {page} 页见到 {found:?}")]
    SnapshotDrift {
        page: u32,
        expected: Option<String>,
        found: Option<String>,
    },

    #[error("第 {page} 页重试 {attempts} 次仍失败")]
    PageFailed { page: u32, attempts: u32 },

    #[error("响应缺少 {header} 头（{url}）")]
    MissingHeader { header: &'static str, url: String },

    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("配置无效: {0}")]
    Config(String),
}
