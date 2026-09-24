//! ESI HTTP 客户端：连接池、gzip、`Expires` 驱动的条件请求、令牌窗口与退避。
//!
//! 三条硬约束（依据见方案 v3.1 §3.1 与附录 A）：
//! 1. 未到 `Expires` 不得对同一 URL 再发请求 —— 由本地缓存直接返回，网络请求数 0、令牌 0。
//! 2. 到点后先带 `If-None-Match`；304 只花 1 令牌且不传输 body。
//! 3. 令牌水位以服务端回报的 `X-Ratelimit-Remaining` 为准，不足时等待而非抢占。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_TYPE, IF_NONE_MATCH, USER_AGENT,
};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;

use crate::config::{cost, EsiConfig};
use crate::error::{Error, Result};
use crate::esi::bucket::{CacheStatus, Watermark, WindowBudget};
use crate::esi::cache::{CacheEntry, ConditionalCache};
use crate::esi::headers::{h, hdr_str, hdr_u32, hdr_u64, parse_http_date};

/// 内存缓存的 TTL 夹逼区间。订单簿实测 300 s；历史数据的 `Expires` 是"次日 11:05"，
/// 那种长有效期必须走 `sync_state` 持久化，不能塞进内存缓存里假装可复用。
const MAX_MEM_TTL: Duration = Duration::from_secs(360);
const MIN_MEM_TTL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct FetchMeta {
    pub url: String,
    pub status: u16,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub expires_at: Option<Instant>,
    /// 上游 `Expires` 原文（HTTP 日期）。
    ///
    /// 长 TTL 端点只能靠它做节流：history 实测 `Expires` 是次日 11:05（约 24 h 后），
    /// 而 `expires_at` 为了内存缓存的合理性被夹在 `[5 s, 360 s]`。拿夹逼值去落库
    /// 就等于把"每日一次"的端点记成"每 6 分钟一次"的闸门 —— 那是反过来的违规。
    pub expires_raw: Option<String>,
    pub x_pages: Option<u32>,
    pub watermark: Watermark,
    /// 本次是否真走了网络。`Cached` 为 false —— 合规自检就数这个。
    pub over_network: bool,
}

impl FetchMeta {
    pub fn upstream_cache_hit(&self) -> bool {
        matches!(self.watermark.cache_status, Some(CacheStatus::Hit))
    }

    /// `Expires` 原文换算成绝对 UTC 秒。缺头或非法（ESI 允许 `Expires: -1`）返回 None。
    pub fn expires_unix(&self) -> Option<i64> {
        self.expires_raw
            .as_deref()
            .and_then(parse_http_date)
            .map(|d| d.timestamp())
    }
}

#[derive(Debug, Clone)]
pub enum Fetch {
    /// 200，body 是新的。
    Fresh { body: Vec<u8>, meta: FetchMeta },
    /// 304，body 沿用上次缓存内容。
    NotModified { body: Vec<u8>, meta: FetchMeta },
    /// 未到 `Expires`，本地直接给。
    Cached { body: Vec<u8>, meta: FetchMeta },
}

impl Fetch {
    pub fn body(&self) -> &[u8] {
        match self {
            Fetch::Fresh { body, .. }
            | Fetch::NotModified { body, .. }
            | Fetch::Cached { body, .. } => body,
        }
    }

    pub fn meta(&self) -> &FetchMeta {
        match self {
            Fetch::Fresh { meta, .. }
            | Fetch::NotModified { meta, .. }
            | Fetch::Cached { meta, .. } => meta,
        }
    }

    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        let url = self.meta().url.clone();
        serde_json::from_slice(self.body()).map_err(|source| Error::Parse { url, source })
    }

    pub fn over_network(&self) -> bool {
        !matches!(self, Fetch::Cached { .. })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub requests: u64,
    pub fresh: u64,
    pub not_modified: u64,
    pub served_from_cache: u64,
    pub upstream_hit: u64,
    pub upstream_miss: u64,
    pub errors: u64,
}

pub struct EsiClient {
    cfg: EsiConfig,
    http: Client,
    cache: Mutex<ConditionalCache>,
    budget: Mutex<WindowBudget>,
    requests: AtomicU64,
    fresh: AtomicU64,
    not_modified: AtomicU64,
    from_cache: AtomicU64,
    hit: AtomicU64,
    miss: AtomicU64,
    errors: AtomicU64,
}

impl EsiClient {
    pub fn new(cfg: EsiConfig) -> Result<Self> {
        if !cfg.user_agent.contains('@') {
            return Err(Error::Config(format!(
                "User-Agent 必须含可联系的邮箱（ESI 硬性要求），当前为：{}",
                cfg.user_agent
            )));
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&cfg.user_agent)
                .map_err(|e| Error::Config(format!("User-Agent 含非法字符: {e}")))?,
        );
        headers.insert(
            h::X_COMPAT_DATE,
            HeaderValue::from_str(&cfg.compat_date)
                .map_err(|_| Error::Config(format!("compat_date 非法: {}", cfg.compat_date)))?,
        );
        // 实测未压缩 237 KB / 9.7–20.7 s，gzip 25.9 KB / 1.25–2.7 s —— 不开 gzip 整个节拍不成立。
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

        let http = Client::builder()
            .default_headers(headers)
            .gzip(true)
            .pool_max_idle_per_host(cfg.concurrency.max(8))
            .timeout(cfg.request_timeout)
            .connect_timeout(cfg.connect_timeout)
            .build()
            .map_err(|e| Error::Config(format!("构建 HTTP 客户端失败: {e}")))?;

        let budget = WindowBudget::new(cfg.window, cfg.window_budget);
        Ok(Self {
            cfg,
            http,
            cache: Mutex::new(ConditionalCache::default()),
            budget: Mutex::new(budget),
            requests: AtomicU64::new(0),
            fresh: AtomicU64::new(0),
            not_modified: AtomicU64::new(0),
            from_cache: AtomicU64::new(0),
            hit: AtomicU64::new(0),
            miss: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, ConditionalCache> {
        self.cache.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn budget(&self) -> std::sync::MutexGuard<'_, WindowBudget> {
        self.budget.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn config(&self) -> &EsiConfig {
        &self.cfg
    }

    pub fn stats(&self) -> Stats {
        Stats {
            requests: self.requests.load(Ordering::Relaxed),
            fresh: self.fresh.load(Ordering::Relaxed),
            not_modified: self.not_modified.load(Ordering::Relaxed),
            served_from_cache: self.from_cache.load(Ordering::Relaxed),
            upstream_hit: self.hit.load(Ordering::Relaxed),
            upstream_miss: self.miss.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    pub fn remaining_tokens(&self) -> u32 {
        self.budget().remaining(Instant::now())
    }

    /// 丢弃某 URL 的缓存条目（快照漂移或整轮重来时用）。
    pub fn invalidate(&self, path_and_query: &str) {
        self.cache().remove(&self.absolutize(path_and_query));
    }

    /// 按子串批量作废缓存 —— 整轮重拉前必须先清掉本轮所有页，
    /// 否则 `Expires` 未到会让后续页全部命中旧数据。
    pub fn invalidate_matching(&self, needle: &str) -> usize {
        self.cache().invalidate_matching(needle)
    }

    /// 把某 URL 的有效期立刻置为已过，但**保留 etag 与 body** ——
    /// 专用于 M0.5 检查②：真实 `Expires` 是 300 s，测试不能等 5 分钟。
    /// 生产路径绝不该调用它，因此名字里带 `expire_now` 而非 `force`。
    pub fn expire_now(&self, path_and_query: &str) -> bool {
        let url = self.absolutize(path_and_query);
        let mut c = self.cache();
        match c.entry(&url).cloned() {
            Some(mut e) => {
                e.expires_at = Instant::now() - Duration::from_millis(1);
                c.put(e);
                true
            }
            None => false,
        }
    }

    pub async fn fetch(&self, path_and_query: &str) -> Result<Fetch> {
        self.fetch_opt(path_and_query, false).await
    }

    /// `force = true` 只给用户手点刷新；调度器一律传 false，否则等于绕过缓存。
    pub async fn fetch_opt(&self, path_and_query: &str, force: bool) -> Result<Fetch> {
        self.fetch_inner(path_and_query, force, None).await
    }

    /// 角色端点专用：带 Bearer 走同一条缓存/节流/重试纪律。
    ///
    /// 独立入口（而不是给 client 挂一个全局令牌）是为了让"公开请求永不携带令牌"成为类型事实：
    /// 令牌只从这一个入参流进请求构造，公开路径根本没有拿它的地方。
    ///
    /// 缓存键仍是 URL（不含令牌）。这没有问题且是刻意的：角色端点的路径自带 character_id，
    /// 同一角色换了令牌拿到的还是同一份数据，不会串号。
    pub async fn fetch_auth(&self, path_and_query: &str, token: &str) -> Result<Fetch> {
        self.fetch_inner(path_and_query, false, Some(token)).await
    }

    /// 缓存/节流/重试的唯一主体，公开与带令牌两条入口都从这里走。
    ///
    /// 没有复制一份"带令牌版"：两份实现迟早会在退避、令牌水位或 304 处理上走偏，
    /// 而这类偏差在真实限流下才暴露。令牌只是穿过它的一件行李。
    async fn fetch_inner(
        &self,
        path_and_query: &str,
        force: bool,
        bearer: Option<&str>,
    ) -> Result<Fetch> {
        let url = self.absolutize(path_and_query);
        let mut last_err: Option<Error> = None;

        for attempt in 0..=self.cfg.page_retry_limit {
            match self.try_fetch(&url, force, bearer).await {
                Ok(f) => return Ok(f),
                Err(Error::RateLimited { retry_after }) => {
                    // 429 必须按 Retry-After 等，不能只按自己的退避曲线。
                    let wait = retry_after
                        .map(Duration::from_secs)
                        .unwrap_or_else(|| self.cfg.retry_backoff * (1 << attempt));
                    tracing::warn!("429 限流，退避 {wait:?}");
                    tokio::time::sleep(wait.min(Duration::from_secs(120))).await;
                    last_err = Some(Error::RateLimited { retry_after });
                }
                Err(e) if Self::retryable(&e) => {
                    let backoff = self.cfg.retry_backoff * (1 << attempt);
                    tracing::warn!("{url} 可重试错误，{backoff:?} 后再试（第 {} 次）：{e}", attempt + 1);
                    tokio::time::sleep(backoff).await;
                    last_err = Some(e);
                }
                // 4xx 每次消耗 5 令牌且大概率是路径错 —— 立刻上抛，不重试。
                Err(e) => {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }

        self.errors.fetch_add(1, Ordering::Relaxed);
        Err(last_err.unwrap_or_else(|| Error::Config("重试循环异常退出".into())))
    }

    /// 传输层错误（连接被重置、响应体被截断）在这条线路上是常态：实测连续
    /// 三轮各缺 1 页，全为 `error decoding response body`。不重试就等于每轮
    /// 稳定丢 1/409 页，而缺页会让某些站点的深度凭空变薄。
    fn retryable(e: &Error) -> bool {
        match e {
            Error::Transport { .. } => true,
            Error::Status { status, .. } => *status >= 500,
            _ => false,
        }
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path_and_query: &str) -> Result<T> {
        self.fetch(path_and_query).await?.json()
    }

    pub async fn get_json_auth<T: DeserializeOwned>(
        &self,
        path_and_query: &str,
        token: &str,
    ) -> Result<T> {
        self.fetch_auth(path_and_query, token).await?.json()
    }

    /// 裸 JSON POST。**不进条件请求缓存** —— 缓存键是 URL，而这类接口的语义由
    /// body 决定（`/universe/names` 换个 ID 列表就是换个答案），缓存会直接给出错结果。
    /// 令牌记账与水位照常纳入。
    pub async fn post_json(&self, path_and_query: &str, body: &[u8]) -> Result<Vec<u8>> {
        let url = self.absolutize(path_and_query);
        let mut last_err: Option<Error> = None;
        for attempt in 0..=self.cfg.page_retry_limit {
            match self.post_json_once(&url, body).await {
                Ok(b) => return Ok(b),
                Err(e) if Self::retryable(&e) => {
                    let backoff = self.cfg.retry_backoff * (1 << attempt);
                    tracing::warn!("{url} POST 可重试错误，{backoff:?} 后再试（第 {} 次）：{e}", attempt + 1);
                    tokio::time::sleep(backoff).await;
                    last_err = Some(e);
                }
                Err(e) => {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }
        self.errors.fetch_add(1, Ordering::Relaxed);
        Err(last_err.unwrap_or_else(|| Error::Config("POST 重试循环异常退出".into())))
    }

    async fn post_json_once(&self, url: &str, body: &[u8]) -> Result<Vec<u8>> {
        self.wait_for_budget(cost::CLIENT_ERROR).await;

        let resp = self
            .http
            .post(url)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(body.to_vec())
            .send()
            .await
            .map_err(|source| Error::Transport {
                url: url.to_string(),
                source,
            })?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp.bytes().await.map_err(|source| Error::Transport {
            url: url.to_string(),
            source,
        })?;

        let watermark = read_watermark(&headers);
        self.requests.fetch_add(1, Ordering::Relaxed);
        {
            let mut b = self.budget();
            b.spend(Watermark::cost_of(status.as_u16()), Instant::now());
            if let Some(r) = watermark.remaining {
                b.note_server_remaining(r, Instant::now());
            }
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::RateLimited {
                retry_after: watermark.retry_after_secs,
            });
        }
        if !status.is_success() {
            return Err(Error::Status {
                url: url.to_string(),
                status: status.as_u16(),
            });
        }
        Ok(bytes.to_vec())
    }

    fn absolutize(&self, path_and_query: &str) -> String {
        if path_and_query.starts_with("http") {
            path_and_query.to_string()
        } else {
            format!(
                "{}{}",
                self.cfg.base_url.trim_end_matches('/'),
                path_and_query
            )
        }
    }

    async fn try_fetch(&self, url: &str, force: bool, bearer: Option<&str>) -> Result<Fetch> {
        let now = Instant::now();

        // ① 未到 Expires 就不发请求 —— 防封禁的第一道闸。
        if !force {
            if let Some(e) = self.cache().fresh(url, now) {
                self.from_cache.fetch_add(1, Ordering::Relaxed);
                return Ok(Fetch::Cached {
                    meta: FetchMeta {
                        url: url.to_string(),
                        status: 200,
                        etag: e.etag.clone(),
                        last_modified: e.last_modified.clone(),
                        expires_at: Some(e.expires_at),
                        expires_raw: e.expires_raw.clone(),
                        x_pages: e.x_pages,
                        watermark: Watermark::default(),
                        over_network: false,
                    },
                    body: e.body,
                });
            }
        }

        // ② 预算预检，按最坏情况（4xx）留额度。
        self.wait_for_budget(cost::CLIENT_ERROR).await;

        // ③ 到点后优先条件请求。
        let etag = self.cache().entry(url).and_then(|e| e.etag.clone());
        let mut req = self.http.get(url);
        if let Some(t) = &etag {
            req = req.header(IF_NONE_MATCH, t.as_str());
        }
        // 令牌只在这一处注入、且只为角色端点注入：`bearer` 为 None 时连头都不构造。
        // 令牌进不了缓存键（键仍只是 URL），也进不了任何日志/错误串 —— 出错时打印的只有 URL。
        if let Some(t) = bearer {
            req = req.header(AUTHORIZATION, format!("Bearer {t}"));
        }

        let resp = req.send().await.map_err(|source| Error::Transport {
            url: url.to_string(),
            source,
        })?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp
            .bytes()
            .await
            .map_err(|source| Error::Transport {
                url: url.to_string(),
                source,
            })?;

        let watermark = read_watermark(&headers);
        self.requests.fetch_add(1, Ordering::Relaxed);
        match watermark.cache_status {
            Some(CacheStatus::Hit) => {
                self.hit.fetch_add(1, Ordering::Relaxed);
            }
            Some(CacheStatus::Miss) => {
                self.miss.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        {
            let mut b = self.budget();
            b.spend(Watermark::cost_of(status.as_u16()), Instant::now());
            if let Some(r) = watermark.remaining {
                b.note_server_remaining(r, Instant::now());
            }
        }

        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::RateLimited {
                retry_after: watermark.retry_after_secs,
            });
        }
        if let Some(er) = watermark.error_remain {
            if er < self.cfg.error_remain_floor {
                return Err(Error::ErrorBudget { remain: er });
            }
        }

        let x_pages = hdr_u32(&headers, h::X_PAGES);

        if status == StatusCode::NOT_MODIFIED {
            // 304 无 body，沿用上次。若本地 body 已被淘汰（进程重启等），必须重取。
            let prev = self.cache().entry(url).cloned();
            let Some(prev) = prev.filter(|p| !p.body.is_empty()) else {
                self.cache().remove(url);
                return Err(Error::Status {
                    url: url.to_string(),
                    status: 0,
                });
            };
            let expires = expires_at(&headers, Instant::now()).unwrap_or(prev.expires_at);
            let expires_raw = hdr_str(&headers, h::EXPIRES).or_else(|| prev.expires_raw.clone());
            let entry = CacheEntry {
                url: url.to_string(),
                body: prev.body.clone(),
                etag: hdr_str(&headers, h::ETAG).or_else(|| prev.etag.clone()),
                last_modified: hdr_str(&headers, h::LAST_MODIFIED)
                    .or_else(|| prev.last_modified.clone()),
                expires_at: expires,
                expires_raw: expires_raw.clone(),
                x_pages: x_pages.or(prev.x_pages),
            };
            let meta = FetchMeta {
                url: url.to_string(),
                status: status.as_u16(),
                etag: entry.etag.clone(),
                last_modified: entry.last_modified.clone(),
                expires_at: Some(expires),
                expires_raw: expires_raw.clone(),
                x_pages: entry.x_pages,
                watermark,
                over_network: true,
            };
            self.cache().put(entry);
            self.not_modified.fetch_add(1, Ordering::Relaxed);
            return Ok(Fetch::NotModified {
                body: prev.body,
                meta,
            });
        }

        if !status.is_success() {
            return Err(Error::Status {
                url: url.to_string(),
                status: status.as_u16(),
            });
        }

        let expires = expires_at(&headers, now).unwrap_or(now + MIN_MEM_TTL);
        let expires_raw = hdr_str(&headers, h::EXPIRES);
        let entry = CacheEntry {
            url: url.to_string(),
            body: body.to_vec(),
            etag: hdr_str(&headers, h::ETAG),
            last_modified: hdr_str(&headers, h::LAST_MODIFIED),
            expires_at: expires,
            expires_raw: expires_raw.clone(),
            x_pages,
        };
        let meta = FetchMeta {
            url: url.to_string(),
            status: status.as_u16(),
            etag: entry.etag.clone(),
            last_modified: entry.last_modified.clone(),
            expires_at: Some(expires),
            expires_raw: expires_raw.clone(),
            x_pages: entry.x_pages,
            watermark,
            over_network: true,
        };
        let cached_body = entry.body.clone();
        self.cache().put(entry);
        self.fresh.fetch_add(1, Ordering::Relaxed);
        Ok(Fetch::Fresh {
            body: cached_body,
            meta,
        })
    }

    async fn wait_for_budget(&self, need: u32) {
        loop {
            let wait = self.budget().wait_for(need, Instant::now());
            let Some(d) = wait else { return };
            let d = d.min(Duration::from_secs(60));
            tracing::warn!("窗口令牌不足，等待 {d:?}");
            tokio::time::sleep(d).await;
        }
    }
}

/// `Expires` → 本地单调时刻，夹逼到 [5 s, 360 s]。
///
/// TTL 用 `Expires - Date` 而非 `Expires - 本机时刻`：这样本机时钟偏差（§7 里要求校时的
/// 那一项）不会把缓存拉长。实测 `Expires - Date = 272 s` 而 `Expires - Last-Modified = 300 s`，
/// 前者才是"我还能安全复用多久"的真值。
fn expires_at(headers: &HeaderMap, now: Instant) -> Option<Instant> {
    let raw = hdr_str(headers, h::EXPIRES)?;
    let dt = parse_http_date(&raw)?;
    let server_now = hdr_str(headers, h::DATE).and_then(|d| parse_http_date(&d));
    let ttl = match server_now {
        Some(sn) => (dt - sn).to_std().unwrap_or(Duration::ZERO),
        None => (dt - chrono::Utc::now()).to_std().unwrap_or(Duration::ZERO),
    };
    Some(now + ttl.clamp(MIN_MEM_TTL, MAX_MEM_TTL))
}

fn read_watermark(headers: &HeaderMap) -> Watermark {    Watermark {
        group: hdr_str(headers, h::X_RATELIMIT_GROUP),
        limit: hdr_str(headers, h::X_RATELIMIT_LIMIT),
        used: hdr_u32(headers, h::X_RATELIMIT_USED),
        remaining: hdr_u32(headers, h::X_RATELIMIT_REMAINING),
        error_remain: hdr_u32(headers, h::X_ERROR_REMAIN),
        error_reset_secs: hdr_u64(headers, h::X_ERROR_RESET),
        cache_status: hdr_str(headers, h::X_CACHE_STATUS)
            .as_deref()
            .map(CacheStatus::parse),
        retry_after_secs: hdr_u64(headers, h::RETRY_AFTER),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    fn status(code: u16) -> Error {
        Error::Status {
            url: "https://esi/x".into(),
            status: code,
        }
    }

    #[test]
    fn server_side_failures_are_retried() {
        assert!(EsiClient::retryable(&status(500)));
        assert!(EsiClient::retryable(&status(502)));
        assert!(EsiClient::retryable(&status(503)));
    }

    #[test]
    fn client_side_failures_are_not_retried_because_they_cost_five_tokens() {
        assert!(!EsiClient::retryable(&status(400)));
        assert!(!EsiClient::retryable(&status(404)));
        assert!(!EsiClient::retryable(&status(420)));
    }

    #[test]
    fn logic_errors_are_not_retried() {
        assert!(!EsiClient::retryable(&Error::SnapshotDrift {
            page: 3,
            expected: None,
            found: None,
        }));
        assert!(!EsiClient::retryable(&Error::ErrorBudget { remain: 1 }));
        assert!(!EsiClient::retryable(&Error::Config("x".into())));
        assert!(!EsiClient::retryable(&Error::MissingHeader {
            header: "x-pages",
            url: "u".into()
        }));
    }

    #[test]
    fn expires_clamp_keeps_ttl_inside_five_to_six_minutes() {
        let mut h = HeaderMap::new();
        // Date 与 Expires 相差 300 s —— ESI 的真实形态。
        h.insert("date", "Wed, 23 Sep 2026 15:00:00 GMT".parse().unwrap());
        h.insert("expires", "Wed, 23 Sep 2026 15:05:00 GMT".parse().unwrap());
        let now = Instant::now();
        let ttl = expires_at(&h, now).unwrap().saturating_duration_since(now);
        assert!(ttl >= Duration::from_secs(295) && ttl <= Duration::from_secs(305), "{ttl:?}");

        // 历史接口的 Expires 是"明天 11:05"，必须被夹到内存上限，不能长期占着缓存。
        h.insert("expires", "Thu, 24 Sep 2026 11:05:00 GMT".parse().unwrap());
        let ttl = expires_at(&h, now).unwrap().saturating_duration_since(now);
        assert_eq!(ttl, MAX_MEM_TTL);
    }

    #[test]
    fn missing_or_garbage_expires_never_extends_the_cache() {
        let now = Instant::now();
        assert!(expires_at(&HeaderMap::new(), now).is_none());
        let mut h = HeaderMap::new();
        h.insert("expires", "-1".parse().unwrap());
        assert!(expires_at(&h, now).is_none());
    }

    /// 真起一个本地回声服务，记录每个请求收到的 `Authorization`（缺失记 None），回一段最小 JSON。
    ///
    /// 用真 socket 而不是断言一个手搭的 header map：本任务要证的恰恰是"报文里到底有没有这个头"，
    /// 只检查自己构造的请求对象等于复述实现，漏掉的正是发出前的那一段。
    /// 端口绑 0 让 OS 分配，测试之间不抢端口。
    fn echo_server(requests: usize) -> (String, mpsc::Receiver<Option<String>>) {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for _ in 0..requests {
                // 请求没来（实现回归了）时别把测试挂死：超时就收摊，断言侧会看到通道关闭。
                let Ok(Some(req)) = server.recv_timeout(Duration::from_secs(10)) else {
                    return;
                };
                let auth = req
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("authorization"))
                    .map(|h| h.value.as_str().to_string());
                let _ = tx.send(auth);
                let _ = req.respond(tiny_http::Response::from_string(r#"{"ok":true}"#));
            }
        });
        (format!("http://127.0.0.1:{port}"), rx)
    }

    fn client_at(base_url: &str) -> EsiClient {
        EsiClient::new(EsiConfig {
            base_url: base_url.to_string(),
            ..Default::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn auth_requests_carry_bearer_header_and_public_ones_do_not() {
        let (base_url, seen) = echo_server(2);
        let c = client_at(&base_url);

        // ① 角色端点：服务端必须看到 `Authorization: Bearer <token>`。
        let v: serde_json::Value = c
            .get_json_auth("/characters/123/orders/?page=1", "ACCESS_TOKEN")
            .await
            .unwrap();
        assert_eq!(v, serde_json::json!({ "ok": true }));
        assert_eq!(
            seen.recv_timeout(Duration::from_secs(10)).unwrap(),
            Some("Bearer ACCESS_TOKEN".to_string())
        );

        // ② 公开端点：连这个头的存在都不该有 —— 带上令牌毫无必要，且会让"缓存键只是 URL"的语义被污染。
        let _: serde_json::Value = c.get_json("/markets/prices/").await.unwrap();
        assert_eq!(seen.recv_timeout(Duration::from_secs(10)).unwrap(), None);
    }

    #[tokio::test]
    async fn fetch_auth_carries_the_bearer_and_plain_fetch_does_not() {
        let (base_url, seen) = echo_server(2);
        let c = client_at(&base_url);

        let f = c.fetch_auth("/characters/42/wallet/", "T2").await.unwrap();
        assert_eq!(f.meta().status, 200);
        assert_eq!(
            seen.recv_timeout(Duration::from_secs(10)).unwrap(),
            Some("Bearer T2".to_string())
        );

        let _ = c.fetch("/status/").await.unwrap();
        assert_eq!(seen.recv_timeout(Duration::from_secs(10)).unwrap(), None);
    }
}
