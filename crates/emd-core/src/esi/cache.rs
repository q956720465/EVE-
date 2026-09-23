//! `Expires` 驱动的条件请求缓存。
//!
//! v3.1 更正过的一条：ESI **没有** `x-server-cache-ttl` 头。有效期的事实来源只有
//! `Expires`（绝对时刻）。实测 `Expires - Last-Modified = 300s`，而
//! `Expires - Date = 272s` —— 即"收到响应后再加 300 秒"会系统性提前重复请求，
//! 那正是社区记录里可致封禁的"绕过缓存"形态。所以这里存的是 `Expires` 换算出的
//! 本地单调时刻，而不是一个倒计时。

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub url: String,
    pub body: Vec<u8>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// 到点之前不得对同一 URL 再发请求。
    pub expires_at: Instant,
    /// 上游 `Expires` 原文，用于落库与排查时钟漂移。
    pub expires_raw: Option<String>,
    /// 分页总数。必须随 body 一起缓存 —— 第二轮起首页会命中本地缓存，
    /// 届时没有响应头可读，只能靠它决定还要拉几页。
    pub x_pages: Option<u32>,
}

impl CacheEntry {
    pub fn is_fresh(&self, now: Instant) -> bool {
        now < self.expires_at
    }

    pub fn ttl_left(&self, now: Instant) -> Duration {
        self.expires_at.saturating_duration_since(now)
    }
}

/// 带字节上限的 FIFO 缓存。全星域一轮 409 页 ≈ 10 MB，不设上限会随自选清单无限增长。
pub struct ConditionalCache {
    map: HashMap<String, CacheEntry>,
    order: VecDeque<String>,
    max_bytes: usize,
    bytes: usize,
}

impl ConditionalCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            max_bytes,
            bytes: 0,
        }
    }

    /// 命中且未到 `Expires` 才返回。**过期项不删除** —— 它的 `ETag` 还要用于条件请求，
    /// 删了就退化成一次全额 200（2 令牌 + 25 KB），白花一倍配额。
    /// 淘汰只由字节上限和显式 `invalidate` 驱动。
    pub fn fresh(&mut self, url: &str, now: Instant) -> Option<CacheEntry> {
        self.map.get(url).filter(|e| e.is_fresh(now)).cloned()
    }

    pub fn entry(&self, url: &str) -> Option<&CacheEntry> {
        self.map.get(url)
    }

    pub fn put(&mut self, entry: CacheEntry) {
        let url = entry.url.clone();
        let size = entry.body.len();
        self.remove(&url);

        while self.bytes + size > self.max_bytes {
            match self.order.pop_front() {
                Some(old) => self.remove(&old),
                None => break,
            }
        }

        self.bytes += size;
        self.order.push_back(url.clone());
        self.map.insert(url, entry);
    }

    pub fn remove(&mut self, url: &str) {
        if let Some(e) = self.map.remove(url) {
            self.bytes = self.bytes.saturating_sub(e.body.len());
            self.order.retain(|u| u != url);
        }
    }

    /// 快照漂移时整批作废：按 URL 子串剔除（如 `/markets/10000002/orders`）。
    /// 逐页 `remove` 要猜页数，这里一次扫干净。
    pub fn invalidate_matching(&mut self, needle: &str) -> usize {
        let doomed: Vec<String> = self
            .map
            .keys()
            .filter(|u| u.contains(needle))
            .cloned()
            .collect();
        let n = doomed.len();
        for u in doomed {
            self.remove(&u);
        }
        n
    }

    pub fn retain_fresh(&mut self, now: Instant) {
        let stale: Vec<String> = self
            .map
            .iter()
            .filter(|(_, e)| !e.is_fresh(now))
            .map(|(u, _)| u.clone())
            .collect();
        for u in stale {
            self.remove(&u);
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Default for ConditionalCache {
    fn default() -> Self {
        // 一轮全量 10 MB + 余量。
        Self::new(48 * 1024 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(url: &str, body: usize, ttl: Duration) -> CacheEntry {
        CacheEntry {
            url: url.into(),
            body: vec![b'x'; body],
            etag: Some("\"abc\"".into()),
            last_modified: None,
            expires_at: Instant::now() + ttl,
            expires_raw: None,
            x_pages: Some(409),
        }
    }

    #[test]
    fn unexpired_entry_is_served_locally() {
        let mut c = ConditionalCache::new(1024);
        c.put(entry("u1", 10, Duration::from_secs(300)));
        assert!(c.fresh("u1", Instant::now()).is_some());
    }

    #[test]
    fn expired_entry_forces_network_but_keeps_the_etag() {
        let mut c = ConditionalCache::new(1024);
        c.put(entry("u1", 10, Duration::from_millis(1)));
        std::thread::sleep(Duration::from_millis(5));
        assert!(c.fresh("u1", Instant::now()).is_none(), "过期必须走网络");
        // 但 ETag/body 留着，好让下一次请求走 304 而不是全额 200。
        assert_eq!(c.len(), 1);
        assert!(c.entry("u1").unwrap().etag.is_some());
    }

    #[test]
    fn invalidate_drops_the_entry_entirely() {
        let mut c = ConditionalCache::new(1024);
        c.put(entry("u1", 10, Duration::from_secs(300)));
        c.remove("u1");
        assert!(c.entry("u1").is_none());
        assert_eq!(c.bytes(), 0);
    }

    #[test]
    fn invalidate_matching_clears_a_whole_region() {
        let mut c = ConditionalCache::new(4096);
        c.put(entry("https://esi/x/markets/10000002/orders?page=1", 10, Duration::from_secs(300)));
        c.put(entry("https://esi/x/markets/10000002/orders?page=2", 10, Duration::from_secs(300)));
        c.put(entry("https://esi/x/markets/10000043/orders?page=1", 10, Duration::from_secs(300)));
        let n = c.invalidate_matching("/markets/10000002/orders");
        assert_eq!(n, 2);
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn byte_cap_evicts_oldest_first() {
        let mut c = ConditionalCache::new(25);
        c.put(entry("a", 10, Duration::from_secs(300)));
        c.put(entry("b", 10, Duration::from_secs(300)));
        assert_eq!(c.bytes(), 20);
        c.put(entry("c", 10, Duration::from_secs(300)));
        assert!(c.fresh("a", Instant::now()).is_none(), "最老的 a 应被淘汰");
        assert!(c.fresh("c", Instant::now()).is_some());
        assert_eq!(c.bytes(), 20);
    }

    #[test]
    fn replacing_url_does_not_double_count_bytes() {
        let mut c = ConditionalCache::new(1024);
        c.put(entry("a", 10, Duration::from_secs(300)));
        c.put(entry("a", 4, Duration::from_secs(300)));
        assert_eq!(c.bytes(), 4);
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn retain_fresh_drops_only_stale() {
        let mut c = ConditionalCache::new(1024);
        c.put(entry("live", 1, Duration::from_secs(300)));
        c.put(entry("dead", 1, Duration::from_millis(1)));
        std::thread::sleep(Duration::from_millis(5));
        c.retain_fresh(Instant::now());
        assert!(c.entry("live").is_some());
        assert!(c.entry("dead").is_none());
    }
}
