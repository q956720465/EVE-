//! ESI 访问策略。所有阈值来自 2026-09-23 本机实测（方案 v3.1 附录 A）。

use std::time::Duration;

/// 令牌成本：实测 `X-Ratelimit-Used` 逐条验证 —— 2xx=2、3xx=1、4xx=5、5xx=0。
pub mod cost {
    pub const OK: u32 = 2;
    pub const NOT_MODIFIED: u32 = 1;
    pub const CLIENT_ERROR: u32 = 5;
    pub const SERVER_ERROR: u32 = 0;
}

#[derive(Debug, Clone)]
pub struct EsiConfig {
    pub base_url: String,
    /// ESI 要求可据此联系到开发者。
    pub user_agent: String,
    /// 实测对 `/markets/*/orders` 不生效（响应一律回显 2020-01-01），但非法值会 400 并耗 5 令牌，
    /// 故仍发送 —— 端点纳入版本化后自动生效。取值须来自 `/meta/compatibility-dates`。
    pub compat_date: String,
    /// 实测并发 16 为吞吐饱和点（4.42 页/秒）；24 起长尾恶化（max 15 s）。
    pub concurrency: usize,
    pub page_jitter: Duration,
    /// TTFB p95 实测 5.5 s；大响应（`markets/prices` 218 KB gzip）单连接 23.7 s。
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// 页级重试上限。整轮重拉代价 82 s + 818 令牌，故优先页级重试。
    pub page_retry_limit: usize,
    pub retry_backoff: Duration,
    /// 限流窗口。`X-Ratelimit-Limit: 12000/15m`。
    pub window: Duration,
    pub window_budget: u32,
    /// 主动降速水位：窗口剩余比例低于此值即排队等待，不贴着上限跑。
    pub budget_floor: f64,
    /// `X-Esi-Error-Limit-Remain` 低于此值即全局暂停。
    pub error_remain_floor: u32,
}

impl EsiConfig {
    /// 默认配置。UA 里的联系邮箱取 `EMD_CONTACT_EMAIL`，缺省回落到项目 owner 提供的地址。
    pub fn from_env() -> Self {
        let contact = std::env::var("EMD_CONTACT_EMAIL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| String::from("956720465@qq.com"));
        Self {
            user_agent: format!("EveMarketDesk/{} ({})", env!("CARGO_PKG_VERSION"), contact),
            ..Default::default()
        }
    }
}

impl Default for EsiConfig {
    fn default() -> Self {
        Self {
            base_url: "https://esi.evetech.net".into(),
            // 版本号跟 CARGO_PKG_VERSION 走，别写死 —— 否则 bump 之后 ESI 日志里的
            // UA 版本会永远停在旧值，而那个 UA 存在的意义就是"出问题时能对上是谁"。
            user_agent: format!(
                "EveMarketDesk/{} (956720465@qq.com)",
                env!("CARGO_PKG_VERSION")
            ),
            compat_date: "2026-08-18".into(),
            concurrency: 16,
            page_jitter: Duration::from_millis(30),
            request_timeout: Duration::from_secs(45),
            connect_timeout: Duration::from_secs(10),
            page_retry_limit: 3,
            retry_backoff: Duration::from_millis(400),
            window: Duration::from_secs(15 * 60),
            window_budget: 12_000,
            budget_floor: 0.35,
            error_remain_floor: 20,
        }
    }
}

impl EsiConfig {
    /// 单轮 N 页的令牌需求，用于预检窗口余量。
    pub fn pages_cost(&self, pages: u32) -> u32 {
        pages.saturating_mul(cost::OK)
    }

    /// 窗口内允许用掉的令牌上限，给 T1/T1.5/T2 重叠留余量。
    pub fn soft_budget(&self) -> u32 {
        ((self.window_budget as f64) * (1.0 - self.budget_floor)) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_agent_carries_version_and_a_reachable_contact() {
        let cfg = EsiConfig::default();
        assert!(cfg.user_agent.contains(env!("CARGO_PKG_VERSION")), "{}", cfg.user_agent);
        assert!(cfg.user_agent.contains('@'), "UA 必须含联系邮箱：{}", cfg.user_agent);
        // 形如 "EveMarketDesk/0.1.0 (a@b)"
        assert!(cfg.user_agent.starts_with("EveMarketDesk/") && cfg.user_agent.ends_with(')'));
    }

    #[test]
    fn from_env_reads_the_contact_and_ignores_blank_values() {
        std::env::set_var("EMD_CONTACT_EMAIL", "tester@example.org");
        assert!(EsiConfig::from_env().user_agent.contains("tester@example.org"));
        // 空白必须回落到默认地址，否则 UA 里会留下一个没有邮箱的字符串，
        // 而 client 会直接拒绝构造。
        std::env::set_var("EMD_CONTACT_EMAIL", "   ");
        assert!(EsiConfig::from_env().user_agent.contains("956720465@qq.com"));
        std::env::remove_var("EMD_CONTACT_EMAIL");
    }

    #[test]
    fn token_costs_match_measured_values() {
        assert_eq!(cost::OK, 2);
        assert_eq!(cost::NOT_MODIFIED, 1);
        assert_eq!(cost::CLIENT_ERROR, 5);
        assert_eq!(cost::SERVER_ERROR, 0);
    }

    #[test]
    fn one_full_round_costs_818_tokens() {
        let cfg = EsiConfig::default();
        // 409 页 × 2；实测跑完后 X-Ratelimit-Remaining 仍见 11148。
        assert_eq!(cfg.pages_cost(409), 818);
        assert!(cfg.pages_cost(409) < (cfg.window_budget as f64 * 0.07) as u32);
    }

    #[test]
    fn worst_case_overlap_stays_inside_soft_budget() {
        let cfg = EsiConfig::default();
        assert_eq!(cfg.soft_budget(), 7_800);
        // 四枢纽档 3930 + 每 12 分钟 T1.5 的 2500 = 6430，须在软预算内。
        assert!(6_430 <= cfg.soft_budget() as usize);
    }
}
