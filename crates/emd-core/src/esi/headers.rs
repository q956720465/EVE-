//! 响应头读取。集中一处，便于单元测试覆盖 HTTP 日期这类易错格式。
//!
//! 头名一律用小写字符串 —— reqwest 的 `HeaderName` 常量不便作 `&'static str` 传递，
//! 而 `HeaderMap::get` 接受任何可转换类型，字符串更顺手。

use chrono::{DateTime, TimeZone, Utc};
use reqwest::header::HeaderMap;

pub fn hdr_str(h: &HeaderMap, name: &str) -> Option<String> {
    h.get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn hdr_u32(h: &HeaderMap, name: &str) -> Option<u32> {
    hdr_str(h, name).and_then(|s| s.parse().ok())
}

pub fn hdr_u64(h: &HeaderMap, name: &str) -> Option<u64> {
    hdr_str(h, name).and_then(|s| s.parse().ok())
}

/// RFC 7231 日期 → `DateTime<Utc>`。`Expires` 允许为 `-1` 或非法值，此时返回 None。
pub fn parse_http_date(v: &str) -> Option<DateTime<Utc>> {
    let st = httpdate::parse_http_date(v).ok()?;
    let secs = st.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs() as i64;
    Utc.timestamp_opt(secs, 0).single()
}

/// 响应头名，全部小写。
pub mod h {
    pub const ETAG: &str = "etag";
    pub const LAST_MODIFIED: &str = "last-modified";
    pub const EXPIRES: &str = "expires";
    pub const DATE: &str = "date";
    pub const RETRY_AFTER: &str = "retry-after";
    pub const X_PAGES: &str = "x-pages";
    pub const X_CACHE_STATUS: &str = "x-esi-cache-status";
    pub const X_RATELIMIT_GROUP: &str = "x-ratelimit-group";
    pub const X_RATELIMIT_LIMIT: &str = "x-ratelimit-limit";
    pub const X_RATELIMIT_USED: &str = "x-ratelimit-used";
    pub const X_RATELIMIT_REMAINING: &str = "x-ratelimit-remaining";
    pub const X_ERROR_REMAIN: &str = "x-esi-error-limit-remain";
    pub const X_ERROR_RESET: &str = "x-esi-error-limit-reset";
    pub const X_COMPAT_DATE: &str = "x-compatibility-date";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_exact_shape_esi_returns() {
        let d = parse_http_date("Wed, 23 Sep 2026 11:57:42 GMT").expect("should parse");
        assert_eq!(d.to_rfc3339(), "2026-09-23T11:57:42+00:00");
    }

    #[test]
    fn rejects_garbage_and_negative_expires() {
        assert!(parse_http_date("-1").is_none());
        assert!(parse_http_date("tomorrow").is_none());
        assert!(parse_http_date("").is_none());
    }

    #[test]
    fn computes_the_measured_300s_ttl() {
        // 实测：Last-Modified 11:52:42 → Expires 11:57:42
        let lm = parse_http_date("Wed, 23 Sep 2026 11:52:42 GMT").unwrap();
        let ex = parse_http_date("Wed, 23 Sep 2026 11:57:42 GMT").unwrap();
        assert_eq!((ex - lm).num_seconds(), 300);
    }

    #[test]
    fn header_lookup_is_case_insensitive_and_trims() {
        let mut m = HeaderMap::new();
        m.insert("X-Pages", " 409 ".parse().unwrap());
        assert_eq!(hdr_u32(&m, h::X_PAGES), Some(409));
        assert_eq!(hdr_u32(&m, "nope"), None);
    }
}
