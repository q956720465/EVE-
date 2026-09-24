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

/// 角色挂链与亏损提醒的配置（spec §4.1 / §4.2）。
///
/// 默认**关闭**：SSO 与推送都要用户自己的凭证（`client_id`、钉钉机器人），
/// 没配之前任何一轮都不该往外发请求。`redirect_uri` 与端口必须与开发者后台
/// 注册值逐字符一致（EVE 精确匹配），故由配置给出、不在这里代猜。
#[derive(Debug, Clone, PartialEq)]
pub struct CharConfig {
    pub client_id: String,
    pub redirect_uri: String,
    pub loopback_port: u16,
    /// 总开关：`EMD_CHAR_SYNC=0` 关闭。
    pub enabled: bool,
    /// 首启回填天数：只拉这个窗建 FIFO 成本基准，覆盖不到的类型标"成本未知"不参与判定。
    pub backfill_days: i64,
}

impl Default for CharConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            redirect_uri: "http://127.0.0.1:8765/callback".into(),
            loopback_port: 8765,
            enabled: false,
            backfill_days: 90,
        }
    }
}

impl CharConfig {
    /// 运行期归一：配了 `client_id` 才算"用户显式启用"，`EMD_CHAR_SYNC=0` 无条件关闭
    /// （照 `EMD_XREGION` 的先例 —— 默认值不走 env，测试不被环境左右）。
    pub fn from_env() -> Self {
        let d = Self::default();
        let client_id = std::env::var("EMD_CHAR_CLIENT_ID")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(d.client_id);
        let redirect_uri = std::env::var("EMD_CHAR_REDIRECT_URI")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(d.redirect_uri);
        let enabled = match std::env::var("EMD_CHAR_SYNC").ok().as_deref() {
            Some("0") | Some("false") => false,
            Some("1") | Some("true") => !client_id.is_empty(),
            _ => !client_id.is_empty(),
        };
        Self {
            client_id,
            redirect_uri,
            enabled,
            ..d
        }
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

    #[test]
    fn char_config_defaults_are_off_with_a_ninety_day_window() {
        let cfg = CharConfig::default();
        assert!(!cfg.enabled, "没配凭证之前不该去连 SSO");
        assert!(cfg.client_id.is_empty());
        assert_eq!(cfg.backfill_days, 90, "spec §4.2 的首启回填窗");
        assert_eq!(cfg.loopback_port, 8765);
        // 端口必须与 redirect_uri 里的那个一致（回环只绑这一个口）。
        assert!(cfg.redirect_uri.contains(&cfg.loopback_port.to_string()));
    }

    #[test]
    fn char_switch_follows_the_configured_client_id_and_the_kill_switch() {
        // env 是运行期的唯一入口：配了 client_id 才算显式启用，EMD_CHAR_SYNC=0 无条件关闭。
        for v in ["EMD_CHAR_SYNC", "EMD_CHAR_CLIENT_ID", "EMD_CHAR_REDIRECT_URI"] {
            std::env::remove_var(v);
        }
        assert!(!CharConfig::from_env().enabled, "没配 client_id = 关闭");

        std::env::set_var("EMD_CHAR_CLIENT_ID", "abc123");
        assert!(CharConfig::from_env().enabled);
        std::env::set_var("EMD_CHAR_SYNC", "0");
        assert!(!CharConfig::from_env().enabled, "kill switch 优先于 client_id");
        std::env::set_var("EMD_CHAR_SYNC", "1");
        std::env::set_var("EMD_CHAR_CLIENT_ID", "   ");
        assert!(!CharConfig::from_env().enabled, "空白 client_id 等于没配");

        std::env::set_var("EMD_CHAR_CLIENT_ID", "abc123");
        std::env::set_var("EMD_CHAR_REDIRECT_URI", "http://127.0.0.1:9999/cb");
        let cfg = CharConfig::from_env();
        assert_eq!(cfg.client_id, "abc123");
        assert_eq!(cfg.redirect_uri, "http://127.0.0.1:9999/cb");

        for v in ["EMD_CHAR_SYNC", "EMD_CHAR_CLIENT_ID", "EMD_CHAR_REDIRECT_URI"] {
            std::env::remove_var(v);
        }
    }
}
