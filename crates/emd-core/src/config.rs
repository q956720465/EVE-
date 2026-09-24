//! ESI 访问策略。所有阈值来自 2026-09-23 本机实测（方案 v3.1 附录 A）。

use std::time::Duration;

use crate::error::{Error, Result};
use crate::store::Db;

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
    /// **默认** `redirect_uri` 里烧进的那个端口（`http://127.0.0.1:8765/callback`）。
    /// 生产**不读它**：绑定端口一律由 [`CharConfig::callback_port`] 从 `redirect_uri` 现算 ——
    /// `redirect_uri` 才是权威（EVE 逐字符匹配注册值），独立端口字段只会和它漂移。
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

/// `meta` KV 里存这两个非密钥值的键（照 `push_config` 的先例：不建表、不加迁移）。
///
/// **它们不是密钥**：OAuth 原生应用的 `client_id` 本来就出现在授权页 URL 里，
/// `redirect_uri` 更是要拿去开发者后台注册的公开值。真正的秘密（令牌）只住系统凭据库，
/// "令牌不进 DTO/日志"那条纪律一个字都不动。
const META_CHAR_CLIENT_ID: &str = "char_client_id";
const META_CHAR_REDIRECT_URI: &str = "char_redirect_uri";

/// `EMD_CHAR_SYNC` 的 kill switch（`0`/`false` 关闭）。单列成函数是因为 [`CharConfig::load`]
/// 在覆盖完库里的 `client_id` 之后要重算开关 —— 归一判定只能有一份，两份早晚漂移。
fn sync_killed_by_env() -> bool {
    matches!(
        std::env::var("EMD_CHAR_SYNC").ok().as_deref(),
        Some("0") | Some("false")
    )
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
        let enabled = !sync_killed_by_env() && !client_id.trim().is_empty();
        Self {
            client_id,
            redirect_uri,
            enabled,
            ..d
        }
    }

    /// 分层配置（spec §4.1 的"设置页可配"）：`from_env()` 起底，再用 `meta` KV 里**非空**的值
    /// 覆盖 —— **库里 > env > 默认**：入库存的是用户在设置页的显式输入，优先级高于环境变量；
    /// 空串/没写过 = 不覆盖（回落 env / 默认），也就是"清掉库里的值"。
    ///
    /// 开关按**生效的** `client_id` 重算：`from_env` 只按 env 里的 client_id 判开——只用设置页
    /// 填了 client_id 的机器上，采集者仍是"关"，每个采集轮静默跳过同步与告警（T15 收尾发现
    /// 的那条 Critical 的同一形态）。`EMD_CHAR_SYNC=0` 仍是无条件 kill switch。
    ///
    /// 读库失败只 warn 并回落 env（照 `PushConfig::load`：配置面坏掉不该把整个告警回合点崩；
    /// 落回 env 只会少同步，不会误同步）。
    pub fn load(db: &Db) -> Result<Self> {
        let mut cfg = Self::from_env();
        match db.get_meta(META_CHAR_CLIENT_ID) {
            Ok(Some(v)) if !v.trim().is_empty() => cfg.client_id = v,
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "读 char_client_id 失败，回落为环境变量"),
        }
        match db.get_meta(META_CHAR_REDIRECT_URI) {
            Ok(Some(v)) if !v.trim().is_empty() => cfg.redirect_uri = v,
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "读 char_redirect_uri 失败，回落为环境变量"),
        }
        cfg.enabled = !sync_killed_by_env() && !cfg.client_id.trim().is_empty();
        Ok(cfg)
    }

    /// 把设置页的两个非密钥值写进 `meta` KV。**空串照写**：读取侧只覆盖非空值，于是
    /// "库里是空串"与"没写过"是同一件事 —— 那正是 UI 的「清空 = 回落 env/默认」。
    pub fn save_identity(db: &Db, client_id: &str, redirect_uri: &str) -> Result<()> {
        db.set_meta(META_CHAR_CLIENT_ID, client_id)?;
        db.set_meta(META_CHAR_REDIRECT_URI, redirect_uri)
    }

    /// 从 `redirect_uri` 里解出回调端口（`scheme://host:port/path` 的 authority 段那个显式端口）。
    ///
    /// **为什么以 URI 为准**：EVE 对 `redirect_uri` 与开发者后台注册值做逐字符匹配，
    /// 所以回环绑定的端口必须跟着 URI 走；另立一个端口字段早晚会与 URI 漂移，
    /// 而漂移的表现是"浏览器落到空处 → 白等满 180 s → 只换来一句不提到端口与 URI 的超时"。
    /// 解不出就返回配置错、让用户在开浏览器**之前**看到；绝不静默回落到别的端口。
    ///
    /// 手写解析而不引 `url` crate：本 workspace 没有该依赖，而这里只需要 authority 里
    /// `host:port` 的一段。冒号**从右往左**找，`[::1]:8765` 这类 IPv6 字面量的主机段冒号不会被误当分隔符。
    pub fn callback_port(&self) -> Result<u16> {
        let uri = self.redirect_uri.as_str();
        let authority = uri
            .split_once("://")
            .map(|(_, rest)| rest)
            .ok_or_else(|| bad_redirect_uri(uri, "缺少 scheme://"))?
            // authority 到第一个 `/`、`?` 或 `#` 为止。
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default();
        let (_, port) = authority
            .rsplit_once(':')
            .filter(|(host, port)| !host.is_empty() && !port.is_empty())
            .ok_or_else(|| bad_redirect_uri(uri, "URI 里没有显式端口"))?;
        port.parse::<u16>()
            .map_err(|_| bad_redirect_uri(uri, "端口不是 0-65535 的整数"))
    }
}

/// `callback_port` 的失败话术：点名要改的键与期望形状 —— 用户不必翻代码就知道怎么改。
fn bad_redirect_uri(uri: &str, why: &str) -> Error {
    Error::Config(format!(
        "EMD_CHAR_REDIRECT_URI 里解不出回调端口（{why}）：{uri:?}。请写成带显式端口的形状，如 EMD_CHAR_REDIRECT_URI=http://127.0.0.1:8765/callback（端口须与开发者后台注册的回调地址逐字符一致）"
    ))
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
    fn callback_port_is_derived_from_the_redirect_uri() {
        // 端口必须跟着 `redirect_uri` 走：EVE 逐字符匹配注册值，绑定端口没有第二个来源。
        assert_eq!(CharConfig::default().callback_port().unwrap(), 8765);

        // 回调搬到别的端口：绑定的端口随之搬（`loopback_port` 留着 8765 也不许被采用）。
        let moved = CharConfig {
            redirect_uri: "http://127.0.0.1:9999/cb".into(),
            ..Default::default()
        };
        assert_eq!(moved.callback_port().unwrap(), 9999);

        // 没有显式端口：报可照着改的配置错，绝不静默回落（回落 = 绑一个端口、浏览器落到空处、
        // 白等满 180 s 只换来一句不提到端口与 URI 的超时）。
        let no_port = CharConfig {
            redirect_uri: "http://127.0.0.1/callback".into(),
            ..Default::default()
        };
        let err = no_port.callback_port().unwrap_err().to_string();
        assert!(err.contains("EMD_CHAR_REDIRECT_URI"), "要点名去改哪个键：{err}");
        assert!(err.contains("http://127.0.0.1:8765/callback"), "要给出期望形状：{err}");

        // 其余畸形形状同样一律报错：端口越界、缺 scheme、authority 为空。
        for bad in ["http://127.0.0.1:70000/cb", "127.0.0.1:8765/callback", "http:///cb"] {
            let cfg = CharConfig {
                redirect_uri: bad.into(),
                ..Default::default()
            };
            assert!(cfg.callback_port().is_err(), "形状不对必须报错：{bad}");
        }
    }

    /// 分层加载（spec §4.1「设置页可配」的存储层）：库里的非空值赢过 env，空/缺回落 env。
    ///
    /// **本测试不碰进程环境变量**（同进程里 `char_switch_...` 那条在改 `EMD_CHAR_*`，
    /// 同一进程里的环境变量是全局的：**改 env 的测试与拿 env 当基准的测试必须互斥**，
    /// 否则并行执行时"开头取一份 `from_env()` 快照、后面再 `load()` 比较"会在别人正好
    /// 改了 env 的那一瞬间随机失败。这把锁只给本文件里碰 `EMD_CHAR_*` 的两个测试用。
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// 断言一旦依赖 env 的具体取值，就变成"成败取决于别的测试有没有正好在改环境变量"）。
    /// 于是比较对象取**本测试开头那一份** `from_env()` 快照：env 里有什么都不影响断言方向。
    #[test]
    fn layered_load_prefers_stored_values_and_falls_back_to_env() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let db = Db::in_memory().unwrap();
        let env = CharConfig::from_env();

        // 什么都没写过 = 完全等于 env 那一层。
        assert_eq!(CharConfig::load(&db).unwrap(), env);

        // 库里写了非空值：它就是生效值，与 env 里那份无关。
        CharConfig::save_identity(&db, "stored-client", "http://127.0.0.1:9999/cb").unwrap();
        let cfg = CharConfig::load(&db).unwrap();
        assert_eq!(cfg.client_id, "stored-client", "库里的值赢过 env");
        assert_eq!(cfg.redirect_uri, "http://127.0.0.1:9999/cb");
        assert_eq!(cfg.callback_port().unwrap(), 9999, "端口跟着生效的 redirect_uri 走");

        // 空串 = 清掉库里的值，回落 env；两个键各自独立（清一个不动另一个）。
        CharConfig::save_identity(&db, "", "").unwrap();
        assert_eq!(CharConfig::load(&db).unwrap(), env, "清空后回落 env/默认");
        CharConfig::save_identity(&db, "only-id", "").unwrap();
        let half = CharConfig::load(&db).unwrap();
        assert_eq!(half.client_id, "only-id");
        assert_eq!(half.redirect_uri, env.redirect_uri, "没写过的那个键回落 env");

        // 纯空白与空串同义（设置页的去空白在入口做，这里兜住手改库的形态）。
        CharConfig::save_identity(&db, "   ", "  ").unwrap();
        assert_eq!(CharConfig::load(&db).unwrap(), env);
    }

    #[test]
    fn char_switch_follows_the_configured_client_id_and_the_kill_switch() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
