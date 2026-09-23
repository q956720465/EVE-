//! 令牌窗口与服务端水位。
//!
//! 关键取舍：**以服务端回报的 `X-Ratelimit-Remaining` 为权威**，本地滑窗只在两次
//! 回报之间做插值。v3.0 靠纯本地估算，遇到多进程或手动刷新就会失真。

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::config::cost;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStatus {
    Hit,
    Miss,
    Refresh,
    Stale,
    Other,
}

impl CacheStatus {
    pub(crate) fn parse(v: &str) -> Self {
        match v.to_ascii_uppercase().as_str() {
            "HIT" => Self::Hit,
            "MISS" => Self::Miss,
            "REFRESH" => Self::Refresh,
            "STALE" => Self::Stale,
            _ => Self::Other,
        }
    }
}

/// 一次响应携带的限流与缓存事实。
#[derive(Debug, Clone, Default)]
pub struct Watermark {
    pub group: Option<String>,
    /// 形如 `12000/15m`。
    pub limit: Option<String>,
    pub used: Option<u32>,
    pub remaining: Option<u32>,
    pub error_remain: Option<u32>,
    pub error_reset_secs: Option<u64>,
    pub cache_status: Option<CacheStatus>,
    pub retry_after_secs: Option<u64>,
}

impl Watermark {
    /// 按状态码推断本次实际消耗的令牌 —— 用实测值，不是估的。
    pub fn cost_of(status: u16) -> u32 {
        match status {
            200..=299 => cost::OK,
            300..=399 => cost::NOT_MODIFIED,
            400..=499 => cost::CLIENT_ERROR,
            _ => cost::SERVER_ERROR,
        }
    }
}

/// 15 分钟滑动窗口预算。
#[derive(Debug)]
pub struct WindowBudget {
    window: Duration,
    capacity: u32,
    spent: VecDeque<(Instant, u32)>,
    total: u32,
    /// 服务端最后一次回报的剩余额，作为上界参考。
    server_remaining: Option<u32>,
    server_remaining_at: Option<Instant>,
}

impl WindowBudget {
    pub fn new(window: Duration, capacity: u32) -> Self {
        Self {
            window,
            capacity,
            spent: VecDeque::new(),
            total: 0,
            server_remaining: None,
            server_remaining_at: None,
        }
    }

    fn prune(&mut self, now: Instant) {
        while let Some((at, _)) = self.spent.front() {
            if now.duration_since(*at) >= self.window {
                let (_, c) = self.spent.pop_front().expect("front checked");
                self.total = self.total.saturating_sub(c);
            } else {
                break;
            }
        }
    }

    /// 本地记账：请求发出后登记实际消耗。
    pub fn spend(&mut self, amount: u32, now: Instant) {
        self.prune(now);
        if amount > 0 {
            self.spent.push_back((now, amount));
            self.total = self.total.saturating_add(amount);
        }
    }

    /// 采纳服务端回报的剩余额。窗口内它比本地记账可信。
    pub fn note_server_remaining(&mut self, remaining: u32, now: Instant) {
        self.server_remaining = Some(remaining);
        self.server_remaining_at = Some(now);
    }

    pub fn remaining(&mut self, now: Instant) -> u32 {
        self.prune(now);
        let local = self.capacity.saturating_sub(self.total);
        match (self.server_remaining, self.server_remaining_at) {
            (Some(sr), Some(at)) if now.duration_since(at) < self.window => sr.min(local),
            _ => local,
        }
    }

    pub fn spent_in_window(&mut self, now: Instant) -> u32 {
        self.prune(now);
        self.total
    }

    /// 预检：够不够发这一笔。`need` 用最坏情况（4xx=5）估，避免被拒后才发现不够。
    pub fn can_afford(&mut self, need: u32, now: Instant) -> bool {
        self.remaining(now) >= need
    }

    /// 距离窗口腾空还大概要等多久。拿不到服务端 `Retry-After` 时的兜底。
    pub fn wait_for(&mut self, need: u32, now: Instant) -> Option<Duration> {
        self.prune(now);
        let base = self.remaining(now);
        if base >= need {
            return None;
        }
        let entries: Vec<(Instant, u32)> = self.spent.iter().copied().collect();
        let mut freed = 0u32;
        for (at, c) in entries {
            freed += c;
            if base + freed >= need {
                return Some(self.window.saturating_sub(now.duration_since(at)));
            }
        }
        Some(self.window)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(ms)
    }

    #[test]
    fn status_codes_map_to_measured_token_costs() {
        assert_eq!(Watermark::cost_of(200), 2);
        assert_eq!(Watermark::cost_of(304), 1);
        assert_eq!(Watermark::cost_of(404), 5);
        assert_eq!(Watermark::cost_of(503), 0);
    }

    #[test]
    fn spending_reduces_remaining() {
        let t0 = Instant::now();
        let mut b = WindowBudget::new(Duration::from_secs(900), 12_000);
        for _ in 0..409 {
            b.spend(cost::OK, t0);
        }
        assert_eq!(b.remaining(t0), 12_000 - 818);
        assert_eq!(b.spent_in_window(t0), 818);
    }

    #[test]
    fn entries_older_than_window_are_pruned() {
        let t0 = Instant::now();
        let mut b = WindowBudget::new(Duration::from_secs(900), 12_000);
        b.spend(500, t0);
        assert_eq!(b.remaining(t0), 11_500);
        assert_eq!(b.remaining(at(900_001)), 12_000);
    }

    #[test]
    fn server_reported_remaining_wins_when_lower() {
        let t0 = Instant::now();
        let mut b = WindowBudget::new(Duration::from_secs(900), 12_000);
        b.spend(10, t0);
        assert_eq!(b.remaining(t0), 11_990);
        b.note_server_remaining(11_148, t0);
        assert_eq!(b.remaining(t0), 11_148);
    }

    #[test]
    fn wait_for_returns_none_when_affordable() {
        let t0 = Instant::now();
        let mut b = WindowBudget::new(Duration::from_secs(900), 100);
        assert_eq!(b.wait_for(50, t0), None);
        b.spend(80, t0);
        assert!(b.wait_for(50, t0).is_some());
    }

    #[test]
    fn can_afford_uses_worst_case_cost() {
        let t0 = Instant::now();
        let mut b = WindowBudget::new(Duration::from_secs(900), 10);
        assert!(b.can_afford(cost::CLIENT_ERROR, t0));
        assert!(!b.can_afford(cost::CLIENT_ERROR * 3, t0));
    }

    #[test]
    fn cache_status_parsing_is_case_insensitive() {
        assert_eq!(CacheStatus::parse("hit"), CacheStatus::Hit);
        assert_eq!(CacheStatus::parse("MISS"), CacheStatus::Miss);
        assert_eq!(CacheStatus::parse("weird"), CacheStatus::Other);
    }
}
