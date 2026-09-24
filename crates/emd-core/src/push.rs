//! 推送层（spec §4.5 的私有卡片，通道按用户 2026-09-24 改定为钉钉）。
//!
//! 本模块装"出站"这一类事情：**通道抽象、本地提醒中心、发送器与打码**（T11）；
//! 各家的**线上契约**（端点、加签、卡片版式、错误码）落在各自的子模块里 ——
//! 钉钉的全部协议常量只在 [`dingtalk`] 写一次，本文件不持任何端点/字段名/错误码：
//! 改协议只动子模块，通道抽象一行不用碰。
//!
//! [`PushOutcome`] 由 [`dingtalk`] 定义、在这里原样再导出：它是"一条告警推出去之后到底
//! 成没成"的唯一词汇表（T11 的 [`dispatch`]、T12 的轮次报告都消费它）。定义处只有一份，
//! 本文件只做入口转发 —— 别在这里再定义一个同名的。
//!
//! # 为什么是 async
//!
//! `send` 返回 boxed future，[`dispatch`] 是 `async fn`：
//!
//! - `reqwest` 在本 workspace **只有异步形态**（`Cargo.toml` 没开 `blocking` 特性），
//!   而 `reqwest::blocking` 在 async 上下文里会直接 panic（它自建运行时）；
//!   T12 的调用点在调度器的 async 循环里（`Scheduler::run_round` 那一层），
//!   同步发送器根本没有从那里调用的合法姿势（`block_on` 在运行时线程里同样 panic）。
//! - trait 里的 `async fn` 不是 object-safe 的，而 [`dispatch`] 要收 `&[&dyn PushChannel]`
//!   （通道按配置在运行期拼装）。用 `async_trait` 宏能绕开，但那要引新依赖；一个显式的
//!   `Pin<Box<dyn Future + Send>>` 就够，代价只是每通道一次装箱（每轮几条，可忽略）。
//!
//! # 四档结果在发送路径上怎么处理（T12 照这张表接线）
//!
//! | 结果 | 含义 | 该做什么 |
//! |---|---|---|
//! | [`PushOutcome::Sent`] | 通道确认收到 | 不用做任何事 |
//! | [`PushOutcome::Retry`] | 限流 / 瞬时繁忙 / 传输失败 | 按 `retry_after_secs` 排下一次，**只有这一档值得重试** |
//! | [`PushOutcome::ChannelDisabled`] | 重试无用，配置得人工改 | 本轮起停发这条通道，把原因送进日志/UI（T14） |
//! | [`PushOutcome::Failed`] | 收到了响应但归不了类 | **不自动重试**，记进轮次报告 |
//!
//! 关键一条是 **`Failed` 一律不自动重试**：`errcode 40035`（缺 access_token）今天落在
//! `Failed` 桶里（那是 T10 的映射，改动属它的边界），而"webhook 少 token"是"必须人工修
//! 配置"——发送侧若重试 `Failed`，它就是一个永远好不了的循环。为此发送器在出发前多做一步
//! **本地预检**（[`DingTalkChannel::preflight`]）：webhook 里没有 `access_token` 时直接判
//! `ChannelDisabled`，连请求都不发，用户拿到的是一句能照着改的话。
//!
//! # 脱敏（Global Constraints，硬约束）
//!
//! [`mask`] 是**日志、错误串与 UI 回显**上唯一的打码器：webhook（内含 `access_token`）、
//! 加签密钥、`signed_url` 的产物一律先过它。三个最容易漏的地方都钉住了：
//!
//! 1. [`PushConfig`] 与 [`DingTalkChannel`] 的 `Debug` 是**手写**的 —— `{:?}` 是日志里最常见
//!    的意外泄露路径，derive 会把密钥原文交出去；
//! 2. 传输失败的原因串**不取 `reqwest::Error` 的 Display**：它会给每一条请求错误附上
//!    ` for url ({url})`（`reqwest/src/error.rs` 实测），而那个 url 就是 `signed_url`；
//! 3. 响应体原文不进任何 reason —— 成败只从 `errcode`/`errmsg` 两个字段取（钉钉不会回显
//!    我们的密钥，但中间代理的错误页会回显请求 URL）。

pub mod dingtalk;

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::alert::AlertPayload;
use crate::error::{Error, Result};
use crate::store::Db;

pub use dingtalk::PushOutcome;

/// 本地提醒中心的通道名（日志与轮次报告用；**不含任何密钥**）。
pub const LOCAL_CHANNEL: &str = "local";
/// 钉钉通道名。同上。
pub const DINGTALK_CHANNEL: &str = "dingtalk";

/// `meta` KV 里 [`PushConfig`] 的键。**不建表、不加迁移**（A4；读写照 `flip_params` 的先例）。
const META_PUSH_CONFIG: &str = "push_config";

/// 打码后的占位串。
const MASK: &str = "***";
/// 短于等于这个长度就整串打码：留头留尾会露掉大部分（8 位以内的口令根本没有"中段"可打）。
const MASK_MIN_LEN: usize = 12;

/// 头部最多留这么多字符：够亮出整段 host（`https://oapi.dingtalk.com` 是 25 字符），
/// 用户一眼认得出"这是哪条通道"；真配置都比它长，所以这个上限不会把短密钥带出来。
const MASK_HEAD_MAX: usize = 32;
/// 尾部最多留这么多字符：够认出"是哪一条"，远不足以拼回原值。
const MASK_TAIL_MAX: usize = 4;

/// 单次推送的**总**超时。钉钉正常在亚秒级；15 s 是"别把调度轮拖住"的上限，不是期望值。
/// `reqwest` 默认**没有**总超时，不设的话一个不回话的对端能把整轮挂在那里。
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
/// 连接超时：比总超时短，好让"连不上"这一档早一点让位给下一轮。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// 中段打码：**任何可能带上 webhook / token / secret 的字符串**在进日志、进错误串、
/// 回显给 UI 之前都必须过这里（Global Constraints 的脱敏硬约束）。
///
/// 留头（≤[`MASK_HEAD_MAX`] 字符）是为了"看得出这是哪条通道"，留尾（≤[`MASK_TAIL_MAX`] 字符）
/// 是为了"认得出是哪一条"，中间一律 [`MASK`]。头尾各自随长度按比例收缩（1/3 与 1/8），
/// 短串**整串**打码 —— 12 字符以内留头留尾就等于没打码，宁可让人认不出来，也不能把密钥留在明处。
///
/// **它不是万能的**：不解析 URL 结构，也不逐字段打码。它只保证一件事 —— 头尾之外全被盖住。
/// 这正好覆盖 [`dingtalk::signed_url`] 那种串（`access_token` 与 `sign` 都在中段）。
pub fn mask(s: &str) -> String {
    let n = s.chars().count();
    if n == 0 {
        return String::new();
    }
    if n <= MASK_MIN_LEN {
        return MASK.to_string();
    }
    let head = (n / 3).min(MASK_HEAD_MAX);
    let tail = (n / 8).min(MASK_TAIL_MAX);
    let mut out = String::with_capacity(head + MASK.len() + tail);
    out.extend(s.chars().take(head));
    out.push_str(MASK);
    out.extend(s.chars().skip(n - tail));
    out
}

/// 一条推送通道（spec §4.5：本地提醒中心 + 远端通道）。
///
/// 约定两条，实现者都要守：
/// - `send` **不返回 `Result`**：通道发不出去不是"调用出错"，而是"这条推送的下一步该做什么"
///   —— 正是 [`PushOutcome`] 的四档。装配层（T12）不需要为每个通道写一套错误映射。
/// - 实现**不得 panic**，也不得把 webhook/token/secret 写进 `reason`（[`mask`] 就是为它准备的）。
pub trait PushChannel: Send + Sync {
    /// 通道名（日志、轮次报告、UI 用）。**不含任何密钥**。
    fn name(&self) -> &'static str;

    /// 把一条告警发出去。返回的形状见模块头"为什么是 async"一节。
    fn send<'a>(&'a self, payload: &'a AlertPayload) -> Pin<Box<dyn Future<Output = PushOutcome> + Send + 'a>>;
}

/// 本地提醒中心（`alerts` 表）。**永不失败**（A5）—— 用户若收回私有数据豁免，它是唯一的
/// 通道（spec §4.5），所以它不能被任何网络/配置**形态**的失败碰到。
///
/// 它也不用写什么东西：告警行在它被调用之前就已由状态机落库（`Db::save_alert`），
/// "投递到本地"就是"那行已经在表里"这件事本身；提醒中心读表（`Db::load_alerts`），
/// 从不看这里的返回值。于是 `send` 是一次纯粹的确认：
/// 任何"本地失败"的返回值都只会让调用方去重试一件早已成立的事。
#[derive(Debug, Default, Clone, Copy)]
pub struct LocalChannel;

impl LocalChannel {
    pub fn new() -> Self {
        Self
    }
}

impl PushChannel for LocalChannel {
    fn name(&self) -> &'static str {
        LOCAL_CHANNEL
    }

    fn send<'a>(&'a self, payload: &'a AlertPayload) -> Pin<Box<dyn Future<Output = PushOutcome> + Send + 'a>> {
        Box::pin(async move {
            // 这条"投递"的证据是 alerts 表里的行，不是日志；只留一条 debug 免得多轮刷屏。
            tracing::debug!(channel = LOCAL_CHANNEL, alert_key = %payload.alert_key, "告警已在本地提醒中心");
            PushOutcome::Sent
        })
    }
}

/// 钉钉群自定义机器人的发送器：**这一层只有 IO**，协议全在 [`dingtalk`]。
///
/// **成败只看响应体的 `errcode`**（A2）：钉钉报错也回 HTTP 200 + JSON（T10 实测三例），
/// 拿 `resp.status()` 当判据会把"推失败"伪装成"推成功"——用户以为链路在跑，实际一条没到。
/// 状态码在本文件里只参与一件事：体里**没有** `errcode` 时区分"像是瞬时故障（5xx/408/429，
/// 可重试）"与"回话形状变了（失败）"，**任何分支都不会由状态码得出"成功"**。
///
/// 字段里躺着 webhook（含 `access_token`）与 secret，故 `Debug` 手写打码（见模块头脱敏节）。
#[derive(Clone)]
pub struct DingTalkChannel {
    /// 群机器人的完整地址，形如 `WEBHOOK_BASE?access_token=…`（[`dingtalk::WEBHOOK_BASE`]）。
    webhook: String,
    /// 加签密钥。空串是**合法**配置：机器人可以只用关键词安全设置（不验签），
    /// 缺密钥由服务器判（`310000`），本地不替它下结论。
    secret: String,
}

impl std::fmt::Debug for DingTalkChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DingTalkChannel")
            .field("webhook", &mask(&self.webhook))
            .field("secret", &mask(&self.secret))
            .finish()
    }
}

impl DingTalkChannel {
    /// `webhook` = 群机器人那条完整 URL（含 `?access_token=…`）；`secret` = 加签密钥。
    /// 两者只存活在这里与 [`PushConfig`]（库里存 `meta` KV），**不进日志、不进错误串**。
    pub fn new(webhook: impl Into<String>, secret: impl Into<String>) -> Self {
        Self { webhook: webhook.into(), secret: secret.into() }
    }

    /// 出发前的本地预检（**不发请求**）。`Some(原因)` = 这条通道现在发不了。
    ///
    /// 只查一件事：webhook 里有没有 `access_token`。实测缺/空 token 会被钉钉回
    /// `40035 缺少参数 access_token`，而 T10 的映射把它归进泛化的 `Failed` 桶 ——
    /// 那其实不是"网络抖动"，是"必须人工修配置"：在出发前就判成 `ChannelDisabled`，
    /// 用户拿到一句能照着改的话，也省掉一条注定失败、还可能被反复重试的请求。
    ///
    /// 原因串里回显 webhook 是为了指出"是哪一条坏了"，**必须先过 [`mask`]**。
    fn preflight(&self) -> Option<PushOutcome> {
        if self.webhook.trim().is_empty() {
            return Some(PushOutcome::ChannelDisabled(
                "钉钉通道没配 webhook：去群机器人设置里复制一条自定义机器人的地址".into(),
            ));
        }
        if access_token_of(&self.webhook).map_or(true, |t| t.trim().is_empty()) {
            return Some(PushOutcome::ChannelDisabled(format!(
                "webhook 里没有 access_token（当前配置 {}）：复制地址时要连查询参数一起带上，\
                 少了它钉钉一律回 40035",
                mask(&self.webhook)
            )));
        }
        None
    }
}

impl PushChannel for DingTalkChannel {
    fn name(&self) -> &'static str {
        DINGTALK_CHANNEL
    }

    fn send<'a>(&'a self, payload: &'a AlertPayload) -> Pin<Box<dyn Future<Output = PushOutcome> + Send + 'a>> {
        Box::pin(async move {
            if let Some(reason) = self.preflight() {
                return reason;
            }
            // 真实墙上钟只住这里（T10 的纯函数拿不到时钟，签名要毫秒）。
            let now_ms = chrono::Utc::now().timestamp_millis();
            let url = dingtalk::signed_url(&self.webhook, &self.secret, now_ms);
            let (title, text) = dingtalk::render_markdown(payload);
            // 消息体形状是协议的一部分（`msgtype` + `markdown.{title,text}`），常量从 T10 取。
            let body = serde_json::json!({
                "msgtype": dingtalk::MSGTYPE_MARKDOWN,
                "markdown": { "title": title, "text": text },
            });

            // `.json()` 会带上 `Content-Type: application/json` —— 这正是 `43004` 想看到的东西。
            // `url` 只交给 reqwest，**不进任何日志/错误串**（模块头脱敏节）。
            let sent = http().post(url).json(&body).send().await;
            match sent {
                Err(e) if e.is_builder() => PushOutcome::ChannelDisabled(format!(
                    "webhook 不是可用的 URL（{}；当前配置 {}）：去机器人设置里重新复制完整地址",
                    transport_kind(&e),
                    mask(&self.webhook)
                )),
                // 传输层失败（超时/连不上/报文中断）一律按**瞬时**处理：它是"再来一次"
                // 的判据，不是"去改配置"的判据 —— 判成后者会让一次 DNS 抖动永久关掉通道。
                // 真是地址写错了，重试预算耗尽后轮次报告里会留下这条原因。
                Err(e) => PushOutcome::Retry {
                    retry_after_secs: dingtalk::RETRY_AFTER_BUSY_SECS,
                    reason: format!("连不上钉钉（{}）：等下一轮再来", transport_kind(&e)),
                },
                Ok(resp) => judge(resp).await,
            }
        })
    }
}

/// 收到响应后的判据：**先读体里的 `errcode`，状态码不参与成功判定**。
///
/// - 体里有 `errcode` → 一律交给 [`dingtalk::map_errcode`]（连状态码是不是 200 都不看：
///   钉钉错误也回 200，而中间代理的 5xx 恰好带着钉钉的 JSON 时也该以体为准）。
/// - 体里没有 `errcode` → 回话的不是钉钉（代理页/网关页）。此时状态码只决定"像不像瞬时故障"，
///   且两条路都**不是成功**：把丢弃告警伪装成成功，用户会以为链路在跑。
/// - 体**原文不进 reason**：代理错误页会回显请求 URL（里面就有 access_token 与 sign）。
async fn judge(resp: reqwest::Response) -> PushOutcome {
    let status = resp.status();
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            return PushOutcome::Retry {
                retry_after_secs: dingtalk::RETRY_AFTER_BUSY_SECS,
                reason: format!("读取钉钉响应失败（{}）：等下一轮再来", transport_kind(&e)),
            }
        }
    };

    let errcode = serde_json::from_str::<serde_json::Value>(&body).ok().and_then(|v| {
        let code = v.get("errcode")?.as_i64()?;
        // errmsg 只在这里被取用：服务器不会回显我们的密钥（T10 的 `map_errcode` 同理）。
        let msg = v.get("errmsg").and_then(|m| m.as_str()).unwrap_or("");
        Some((code, msg.to_string()))
    });
    if let Some((code, msg)) = errcode {
        return dingtalk::map_errcode(code, &msg);
    }

    if status.is_server_error() || status == reqwest::StatusCode::REQUEST_TIMEOUT || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        PushOutcome::Retry {
            retry_after_secs: dingtalk::RETRY_AFTER_BUSY_SECS,
            reason: format!("钉钉端点回了 HTTP {status}（没有 errcode 可判）：等下一轮再来"),
        }
    } else {
        PushOutcome::Failed(format!(
            "响应不是钉钉的错误码 JSON（HTTP {status}）：成败无从判定，不当成功"
        ))
    }
}

/// `reqwest::Error` → 一句能读的**分类**串。
///
/// **绝不取 `Display`**：它会给请求错误附上 ` for url ({url})`，而那个 url 是 `signed_url`
/// —— 里面有 `access_token` 与 `sign`。这里只取分类谓词：够分清"是配置错了还是网络不通"，
/// 又不给日志留任何密钥。底层原因（DNS 失败、连接被拒这类文本）也刻意不带出来：
/// 它们由第三方库拼，形状不可控，而分类信息对用户已经足够指路。
fn transport_kind(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() {
        "请求超时"
    } else if e.is_connect() {
        "建立连接失败（网络不通 / 名字解析不了 / 连接被拒）"
    } else if e.is_body() {
        "收发报文中断"
    } else if e.is_decode() {
        "响应无法解码"
    } else if e.is_builder() {
        "URL 或请求头非法"
    } else {
        "请求发送失败"
    }
}

/// 取出 webhook 里 `access_token` 的值（**只看本地字符串**，不去解析 URL 结构 ——
/// 预检要判断的就是"用户配的这串里到底有没有这个参数"）。
/// 值取到 `&` 或串尾为止；空值同样算"没配"（实测空值也回 40035）。
fn access_token_of(webhook: &str) -> Option<&str> {
    let (_, rest) = webhook.split_once("access_token=")?;
    Some(&rest[..rest.find('&').unwrap_or(rest.len())])
}

/// 发送用的 HTTP 客户端：进程内共享一个（连接池与 TLS 会话复用 —— 一轮告警里可能连发几条）。
///
/// 构造失败只可能是 TLS 后端初始化不了，那是环境级故障，没有"降级成能发"的形态
/// （`reqwest::Client::new` 自己也是 panic 的），故这里直接响亮地炸。
fn http() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .expect("reqwest 客户端构造失败（TLS 后端不可用）")
    })
}

/// 多通道派发：把一条告警依次交给每条通道，**逐条独立结算**。
///
/// 返回与 `channels` **同序等长**的一一对应结果（第 i 项 = `channels[i]` 的结果；
/// 通道名在入参里，结果不重复携带）—— T12 按下标认通道。
///
/// **一个通道失败不拖垮另一个**（A6；spec 的立场是"告警宁可重复，不可全丢"）：
/// 循环里没有 `?`、没有提前 return，每条通道自己决定成/败/该不该重试，后一条照发。
/// 于是钉钉挂了（限流、密钥错、网络不通）时，本地提醒中心那条仍然落定。
///
/// 隔离的边界说明白：**panic 不在隔离范围内**。通道实现是我们自己的代码，panic 是它的 bug，
/// 把它伪造成一条 `Failed` 只会掩盖缺陷；而"告警没丢"这件事由本地提醒中心（A5）保证，
/// 与远端通道是否 panic 无关。
pub async fn dispatch(channels: &[&dyn PushChannel], payload: &AlertPayload) -> Vec<PushOutcome> {
    let mut out = Vec::with_capacity(channels.len());
    for ch in channels {
        let outcome = ch.send(payload).await;
        // 三档失败都值得看一眼；原因串按 A3 都不含密钥（`map_errcode` 的产品 + 本文件的分类串）。
        match &outcome {
            PushOutcome::Sent => {
                tracing::debug!(channel = ch.name(), alert_key = %payload.alert_key, "推送成功")
            }
            PushOutcome::Retry { retry_after_secs, reason } => tracing::warn!(
                channel = ch.name(), alert_key = %payload.alert_key,
                retry_after_secs, reason = %reason, "推送失败：可重试"
            ),
            PushOutcome::ChannelDisabled(reason) => tracing::warn!(
                channel = ch.name(), alert_key = %payload.alert_key,
                reason = %reason, "推送失败：通道需人工修配置"
            ),
            PushOutcome::Failed(reason) => tracing::warn!(
                channel = ch.name(), alert_key = %payload.alert_key,
                reason = %reason, "推送失败：未归类，不自动重试"
            ),
        }
        out.push(outcome);
    }
    out
}

/// 推送配置。**住 `meta` KV，不建表、不加迁移**（A4；键名与读写照 `flip_params` 的先例）。
///
/// 今天它就是钉钉一条通道的三个旋钮。`enabled` **默认关**：与 `CharConfig` 同一条纪律 ——
/// 没配好之前，任何一轮都不该往外发东西（告警照样进提醒中心，但不进群）。
///
/// **`Debug` 手写**：webhook 与 secret 都在字段里，derive 出的 Debug 一进日志就是明文。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushConfig {
    /// 群自定义机器人的完整地址（含 `access_token`）。**离库就带密钥**：别进日志、别进 Debug。
    pub webhook: String,
    /// 加签密钥。同上。
    pub secret: String,
    /// 总开关。
    pub enabled: bool,
}

impl Default for PushConfig {
    fn default() -> Self {
        // 空 webhook + 关闭：既是"用户还没配"的唯一形态，也是读坏时的安全回落（只少推，不误推）。
        Self { webhook: String::new(), secret: String::new(), enabled: false }
    }
}

impl std::fmt::Debug for PushConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushConfig")
            .field("webhook", &mask(&self.webhook))
            .field("secret", &mask(&self.secret))
            .field("enabled", &self.enabled)
            .finish()
    }
}

impl PushConfig {
    /// 读配置。没写过 → 默认（关闭）；读坏 → 默认 + warn（照 `get_flip_params`：配置坏掉不该把
    /// 整个告警回合点崩，而"回落到关闭"只会少推、不会误推）。
    pub fn load(db: &Db) -> Result<Self> {
        let Some(raw) = db.get_meta(META_PUSH_CONFIG)? else {
            return Ok(Self::default());
        };
        match serde_json::from_str::<Self>(&raw) {
            Ok(c) => Ok(c),
            Err(e) => {
                tracing::warn!(error = %e, "push_config 解析失败，回落为「未配置」（不外发）");
                Ok(Self::default())
            }
        }
    }

    /// 写配置。**拒绝把已打码的值存回来**：`echo()` 交给 UI 的是打码串，UI 若原样回存，
    /// 库里就躺着一条带 `***` 的假密钥 —— 之后每条推送都 310000，且从配置里看不出毛病。
    pub fn save(&self, db: &Db) -> Result<()> {
        if looks_masked(&self.webhook) || looks_masked(&self.secret) {
            return Err(Error::Config(
                "推送配置里含已打码的片段（***）：回显值不能存回来，请填新值或保留原值".into(),
            ));
        }
        let raw = serde_json::to_string(self)
            .map_err(|e| Error::Config(format!("push 配置序列化失败: {e}")))?;
        db.set_meta(META_PUSH_CONFIG, &raw)
    }

    /// 回显给 UI 的形态：webhook 与 secret **都已打码**（A4：密钥不回明文）。
    /// T14 的回显路径只许用这个，别把 `load()` 的结果直接交给前端。
    pub fn echo(&self) -> PushConfigEcho {
        PushConfigEcho {
            webhook: mask(&self.webhook),
            secret: mask(&self.secret),
            enabled: self.enabled,
        }
    }

    /// UI 回显路径的**推荐入口**：读库 + 打码一步到位 —— 让"正确的做法"同时也是最短的路
    /// （`load()` 的明文只给发送器与保存路径用）。
    pub fn load_echo(db: &Db) -> Result<PushConfigEcho> {
        Ok(Self::load(db)?.echo())
    }

    /// 本轮该发的通道（T12 的装配入口，`dispatch` 的入参就由它来）。
    ///
    /// 本地提醒中心**恒在**：它是"用户收回私有数据豁免"时的回落方案（spec §4.5），也是
    /// "宁可重复不可全丢"的执行者 —— 钉钉那条炸了，这一条仍然落在 `alerts` 表里。
    /// 钉钉只在**开关打开且 webhook 填了**时在场：装上了就意味着一轮外发，没配好不该有它。
    pub fn channels(&self) -> Vec<Box<dyn PushChannel>> {
        let mut chans: Vec<Box<dyn PushChannel>> = vec![Box::new(LocalChannel::new())];
        if self.enabled && !self.webhook.trim().is_empty() {
            chans.push(Box::new(DingTalkChannel::new(&self.webhook, &self.secret)));
        }
        chans
    }
}

/// 打码后的回显形态（T14 渲染与"配置测试"用）。**这里拿不到明文** —— 打码在构造时就做完了。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PushConfigEcho {
    /// 已中段打码的 webhook；空串 = 没配。
    pub webhook: String,
    /// 已中段打码的 secret；空串 = 没配。
    pub secret: String,
    pub enabled: bool,
}

/// 串里带打码占位就是"从 UI 回显过来的值"。钉钉的 token/secret 是 base64 字面量，
/// 不含 `*`，所以这个判据不会误伤真配置。
fn looks_masked(s: &str) -> bool {
    s.contains(MASK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{mpsc, Arc, Mutex};

    use crate::alert::{
        order_alert_key, AlertKind, AlertRecord, CaliberSummary, COST_SRC_FIFO90, TRACK_EXPECTED,
    };
    use crate::market::STATION_JITA;
    use chrono::{DateTime, Utc};

    /// 夹具里用的假密钥与假 token（真值只在用户机器上；这两个串不许出现在被打码的输出里）。
    const SECRET: &str = "SEC0000000000000000000000000000000000000000000000000000000000000fake";
    const TOKEN: &str = "tok0000000000000000000000000000000000000000000000000000000000000fake";

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn sample_payload() -> AlertPayload {
        AlertPayload {
            alert_key: order_alert_key(7001),
            kind: AlertKind::ExpectedSellLoss,
            order_id: 7001,
            type_id: 34,
            type_name: "Tritanium".to_string(),
            location_id: STATION_JITA,
            location_name: "Jita IV - Moon 4 - Caldari Navy Assembly Plant".to_string(),
            is_buy: false,
            price: 97.0,
            volume: 100,
            at: at("2026-09-20T10:00:00Z"),
            loss_isk: 127.375,
            margin_pct: -1.34,
            caliber: CaliberSummary {
                track: TRACK_EXPECTED.to_string(),
                sales_tax_pct: 3.375,
                broker_pct: 0.0,
                skill_caliber: "Accounting 5 / Broker Relations 0".to_string(),
                unit_cost: 95.0,
                cost_source: COST_SRC_FIFO90.to_string(),
                formula: "① 单位净额 93.726250 = 挂价 97.000000 × (1 − 有效税 3.3750%)".to_string(),
                data_age_secs: 60,
            },
        }
    }

    /// 把一条告警按 T9 的写法落进内存库（本地提醒中心的"记录"就是这一行）。
    fn record(db: &Db, p: &AlertPayload) {
        db.save_alert(&AlertRecord::from_payload(p, 90_000_001, 1_790_000_000)).unwrap();
    }

    /// 测试替身：把收到的载荷记下来（用来证明"另一条通道**确实收到了**"，而不是"返回的 vec 更长"），
    /// 并回一个测试自己控制的结果。
    struct Spy {
        seen: Arc<Mutex<Vec<String>>>,
        outcome: PushOutcome,
    }

    impl PushChannel for Spy {
        fn name(&self) -> &'static str {
            "spy"
        }

        fn send<'a>(&'a self, p: &'a AlertPayload) -> Pin<Box<dyn Future<Output = PushOutcome> + Send + 'a>> {
            let key = p.alert_key.clone();
            let (outcome, seen) = (self.outcome.clone(), Arc::clone(&self.seen));
            Box::pin(async move {
                seen.lock().unwrap().push(key);
                outcome
            })
        }
    }

    /// 桩服务收到的请求（真 socket 抓的报文，不是自己构造的请求对象）。
    struct Seen {
        url: String,
        content_type: Option<String>,
        body: String,
    }

    /// 起一个本地钉钉桩：按顺序回 `responses` 里的 (状态码, 响应体)，并把每个请求原样记下来。
    ///
    /// 排好的响应回完之后**再多守 5 秒**才收摊：这样"不该发请求"的测试能拿到一次明确的
    /// `recv_timeout` 超时 —— 通道提前关闭只能证明"桩没了"，证明不了"请求没来"。
    fn dingtalk_stub(responses: Vec<(u16, &'static str)>) -> (String, mpsc::Receiver<Seen>) {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut planned = responses.into_iter();
            loop {
                let idle = if planned.len() == 0 {
                    Duration::from_secs(5)
                } else {
                    Duration::from_secs(10)
                };
                let Ok(Some(mut req)) = server.recv_timeout(idle) else {
                    return; // 请求没来（实现回归了）时超时收摊，别把测试挂死
                };
                let mut body = String::new();
                let _ = req.as_reader().read_to_string(&mut body);
                let content_type = req
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("content-type"))
                    .map(|h| h.value.as_str().to_string());
                let _ = tx.send(Seen { url: req.url().to_string(), content_type, body });
                let (status, resp) = planned.next().unwrap_or((200, r#"{"errcode":0,"errmsg":"ok"}"#));
                let _ = req.respond(tiny_http::Response::from_string(resp).with_status_code(status));
            }
        });
        (format!("http://127.0.0.1:{port}"), rx)
    }

    #[tokio::test]
    async fn local_channel_always_succeeds_and_records() {
        // 本地提醒中心是"用户收回私有数据豁免"的回落方案，**必须永不失败**（spec §4.5）：
        // 它不碰网络、不碰配置，唯一的事实来源是"告警行已经在 alerts 表里"。
        let db = Db::in_memory().unwrap();
        let p = sample_payload();
        record(&db, &p);

        let chans: [&dyn PushChannel; 1] = [&LocalChannel::new()];
        for _ in 0..3 {
            assert_eq!(
                dispatch(&chans, &p).await,
                vec![PushOutcome::Sent],
                "本地通道没有失败形态：连着推几次都得是 Sent"
            );
        }

        // "记录"落在 alerts 表：提醒中心读它（load_alerts），从不读 dispatch 的返回值。
        let rows = db.load_alerts().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].alert_key, p.alert_key);
    }

    #[tokio::test]
    async fn dispatch_continues_after_one_channel_fails() {
        let p = sample_payload();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let broken = Spy { seen: Arc::clone(&seen), outcome: PushOutcome::Failed("桩：这条通道坏了".into()) };
        let good = Spy { seen: Arc::clone(&seen), outcome: PushOutcome::Sent };

        // 坏的在前：它失败不能拦住后面那条。**先断言"另一条通道真的收到了载荷"** ——
        // 这才是隔离的实质；"返回的 vec 更长"在"只结算第一条就 return"的实现上同样成立。
        let chans: [&dyn PushChannel; 2] = [&broken, &good];
        let out = dispatch(&chans, &p).await;
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[p.alert_key.clone(), p.alert_key.clone()],
            "两条通道都该收到载荷"
        );
        assert_eq!(out.len(), 2, "每条通道各结算一次");
        assert!(matches!(out[0], PushOutcome::Failed(_)), "{:?}", out[0]);
        assert_eq!(out[1], PushOutcome::Sent);

        // 反序同样成立：好通道在前时，坏通道的结果仍落在自己的下标上。
        let seen2 = Arc::new(Mutex::new(Vec::new()));
        let good2 = Spy { seen: Arc::clone(&seen2), outcome: PushOutcome::Sent };
        let broken2 = Spy {
            seen: Arc::clone(&seen2),
            outcome: PushOutcome::Retry { retry_after_secs: 60, reason: "桩：限流".into() },
        };
        let chans: [&dyn PushChannel; 2] = [&good2, &broken2];
        let out = dispatch(&chans, &p).await;
        assert_eq!(seen2.lock().unwrap().len(), 2, "坏的那条也不能让好的那条收不到");
        assert_eq!(out[0], PushOutcome::Sent);
        assert!(matches!(out[1], PushOutcome::Retry { retry_after_secs: 60, .. }), "{:?}", out[1]);
    }

    #[tokio::test]
    async fn a_broken_dingtalk_channel_still_leaves_the_alert_in_the_local_center() {
        // 真通道 + 真本地中心：钉钉这条的配置是坏的（webhook 少了 access_token），
        // 本地那条照样 Sent —— spec 的"宁可重复不可全丢"就落在这两行的组合上。
        let db = Db::in_memory().unwrap();
        let p = sample_payload();
        record(&db, &p);

        let dt = DingTalkChannel::new("https://oapi.dingtalk.com/robot/send", "");
        let local = LocalChannel::new();
        let chans: [&dyn PushChannel; 2] = [&dt, &local];
        let out = dispatch(&chans, &p).await;
        assert!(matches!(out[0], PushOutcome::ChannelDisabled(_)), "{:?}", out[0]);
        assert_eq!(out[1], PushOutcome::Sent);
        assert_eq!(db.load_alerts().unwrap().len(), 1, "钉钉挂了，本地这条仍然落定");
    }

    #[tokio::test]
    async fn sender_reads_errcode_from_the_body_not_the_http_status() {
        // 钉钉报错也回 HTTP 200（T10 实测），所以"200 = 成功"是**必然错**的判据：
        // 桩服务回 200 + 一个失败 errcode，看状态码的实现会把它判成 Sent —— 这条专抓那个。
        let (base, _seen) = dingtalk_stub(vec![
            (200, r#"{"errcode":300005,"errmsg":"token is not exist"}"#),
            (200, r#"{"errcode":0,"errmsg":"ok"}"#),
            (500, r#"{"errcode":130101,"errmsg":"send too fast, exceed 20 times per minute"}"#),
        ]);
        let ch = DingTalkChannel::new(format!("{base}/robot/send?access_token={TOKEN}"), SECRET);
        let p = sample_payload();

        let first = ch.send(&p).await;
        assert!(!matches!(first, PushOutcome::Sent), "HTTP 200 + 失败 errcode 绝不能判成功：{first:?}");
        assert!(matches!(first, PushOutcome::ChannelDisabled(_)), "token 不存在是通道级失效：{first:?}");

        assert_eq!(ch.send(&p).await, PushOutcome::Sent, "errcode == 0 才算成功");

        // 体里有 errcode 时一律以体为准，状态码不参与（这条 500 也是想说明判据不是状态码）。
        let third = ch.send(&p).await;
        assert!(matches!(third, PushOutcome::Retry { .. }), "限流码即使在非 2xx 上也按体判：{third:?}");
    }

    #[tokio::test]
    async fn sender_writes_a_signed_markdown_card() {
        let (base, seen) = dingtalk_stub(vec![(200, r#"{"errcode":0,"errmsg":"ok"}"#)]);
        let ch = DingTalkChannel::new(format!("{base}/robot/send?access_token={TOKEN}"), SECRET);
        let p = sample_payload();
        assert_eq!(ch.send(&p).await, PushOutcome::Sent);

        let seen = seen.recv_timeout(Duration::from_secs(10)).expect("请求没到桩上");
        // 加签是**查询参数**（T10 的协议常量），不是请求体字段；断言信息里回显 URL 前先打码。
        assert!(seen.url.contains(&format!("access_token={TOKEN}")), "{}", mask(&seen.url));
        assert!(seen.url.contains("timestamp="), "{}", mask(&seen.url));
        assert!(seen.url.contains("&sign="), "{}", mask(&seen.url));
        assert!(
            seen.content_type.unwrap_or_default().starts_with("application/json"),
            "`43004` 想看到的就是这个头"
        );

        let v: serde_json::Value = serde_json::from_str(&seen.body).expect("报文必须是 JSON");
        let (title, text) = dingtalk::render_markdown(&p);
        assert_eq!(v["msgtype"], dingtalk::MSGTYPE_MARKDOWN);
        assert_eq!(v["markdown"]["title"], title.as_str());
        assert_eq!(v["markdown"]["text"], text.as_str(), "卡片就是 T10 渲染的那一张");
        assert!(text.contains("私有数据"), "spec §4.5 的私有数据角标要在报文里");
        // 密钥本身不上行：加签只在 URL 上（HMAC 的产物），body 里出现密钥就是泄露。
        assert!(!seen.body.contains(SECRET), "body 里不该有签名密钥");
    }

    #[tokio::test]
    async fn a_webhook_without_access_token_is_never_sent_and_says_what_to_fix() {
        // 实测缺 token 会被回 40035，而它落进泛化的 Failed 桶 —— 发送侧若重试 Failed，
        // 那就是个永远好不了的循环。这里在出发前就判死：**连请求都不发**。
        let (base, seen) = dingtalk_stub(Vec::new());
        let webhook = format!("{base}/robot/send");
        let ch = DingTalkChannel::new(&webhook, SECRET);

        let out = ch.send(&sample_payload()).await;
        let PushOutcome::ChannelDisabled(reason) = out else {
            panic!("缺 token 必须是通道级失效（重试无用）：{out:?}")
        };
        assert!(reason.contains("access_token"), "要说清缺的是什么：{reason}");
        assert!(reason.contains("***"), "回显 webhook 也得先打码：{reason}");
        assert!(!reason.contains(&webhook), "原因串里不许回显整条 webhook：{reason}");
        assert!(
            seen.recv_timeout(Duration::from_millis(300)).is_err(),
            "预检没过就不该有请求发出去（桩还守着，收到就会进来）"
        );
    }

    #[tokio::test]
    async fn transport_failures_are_retried_and_their_reason_carries_no_url() {
        // 端口 1：本机没有服务会听它，连上去就是立刻拒绝 —— 一条真的传输层失败。
        // 这条测试是 A3 的机械保证：原因串绝不取 reqwest 的 Display（它会附 ` for url ({url})`，
        // 而那个 url 就是 signed_url，里面是 access_token 与 sign）。
        let ch = DingTalkChannel::new(
            format!("http://127.0.0.1:1/robot/send?access_token={TOKEN}"),
            SECRET,
        );
        let out = ch.send(&sample_payload()).await;
        let PushOutcome::Retry { retry_after_secs, reason } = out else {
            panic!("传输失败是瞬时故障，应可重试：{out:?}")
        };
        assert_eq!(retry_after_secs, dingtalk::RETRY_AFTER_BUSY_SECS);
        for needle in [TOKEN, SECRET, "access_token", "sign=", "timestamp=", "127.0.0.1"] {
            assert!(!reason.contains(needle), "原因串里出现了 {needle}：{reason}");
        }
    }

    /// 把日志写进内存缓冲（只有测试用；配套的 `with_default` 是**线程局部**的，不影响别的测试）。
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn webhook_and_secret_are_masked_in_display_and_logs() {
        // ---- ① 打码函数本身：中段换成 ***，头尾留住"这是哪条通道 / 哪一条" ----
        let webhook = format!("{}?access_token={TOKEN}", dingtalk::WEBHOOK_BASE);
        let m = mask(&webhook);
        assert!(!m.contains(TOKEN), "打码后不许留下 token：{m}");
        assert!(m.contains(MASK), "{m}");
        assert!(m.starts_with("https://oapi.dingtalk.com"), "前缀要够认出是哪家通道：{m}");
        assert!(m.ends_with(&webhook[webhook.len() - 4..]), "尾 4 字符留住用于辨认：{m}");
        assert!(m.len() < webhook.len() / 2 + 8, "中段必须真的被吃掉：{m}");
        // 短串整串打码：8 位的口令留头留尾等于没打码。
        assert_eq!(mask("SEC1234"), MASK);
        assert_eq!(mask(""), "");
        let sm = mask(SECRET);
        assert!(!sm.contains(SECRET) && sm.contains(MASK), "{sm}");

        // ---- ② Debug 面：`{:?}` 是日志里最常见的意外泄露路径，两个类型都是手写打码的 ----
        let cfg = PushConfig {
            webhook: webhook.clone(),
            secret: SECRET.to_string(),
            enabled: true,
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains(TOKEN) && !dbg.contains(SECRET), "{dbg}");
        assert!(dbg.contains(MASK) && dbg.contains("enabled: true"), "{dbg}");
        let chan = DingTalkChannel::new(&webhook, SECRET);
        let dbg = format!("{chan:?}");
        assert!(!dbg.contains(TOKEN) && !dbg.contains(SECRET), "{dbg}");

        // ---- ③ 回显面：交给 UI 的形态里没有明文（T14 只许用这个） ----
        let echo = cfg.echo();
        assert!(echo.enabled);
        assert!(!echo.webhook.contains(TOKEN) && !echo.secret.contains(SECRET), "{echo:?}");
        assert!(echo.webhook.contains(MASK) && echo.secret.contains(MASK), "{echo:?}");

        // ---- ④ 日志面：在本测试线程上装抓取用的 subscriber（`with_default` 是线程局部的），
        // 抓"有人随手把配置 `{:?}` 进日志"那一行的现场 —— 这条 callsite 只属于本测试，
        // 必然落在抓取范围内。（**刻意不断言 dispatch 的 warn 也落进这份日志**：那两条
        // callsite 被本模块其它测试共用，而 tracing 的 callsite 兴趣缓存让"并行测试 +
        // 线程局部 subscriber"的抓取不稳定 —— 单独跑必到、并行时有概率丢，写成断言就是
        // 一条随机挂的测试。dispatch 那条 warn 会打印的原因串另有确定性证据：
        // `a_webhook_without_access_token_…` 与 `transport_failures_…` 直接断言原因串本身。）
        let captured = Arc::new(Mutex::new(Vec::<u8>::new()));
        let writer = {
            let captured = Arc::clone(&captured);
            move || Capture(Arc::clone(&captured))
        };
        let sub = tracing_subscriber::fmt()
            .with_writer(writer)
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        tracing::subscriber::with_default(sub, || {
            tracing::info!(cfg = ?cfg, channel = ?chan, "测试：随手 Debug 一行");
            rt.block_on(async {
                let (base, _seen) = dingtalk_stub(vec![
                    // 真发出一轮（桩服务，不出网）：桩回"token 不存在"，通道写一条带原因的 warn。
                    (200, r#"{"errcode":300005,"errmsg":"token is not exist"}"#),
                ]);
                let dt = DingTalkChannel::new(format!("{base}/robot/send?access_token={TOKEN}"), SECRET);
                let local = LocalChannel::new();
                let chans: [&dyn PushChannel; 2] = [&dt, &local];
                let out = dispatch(&chans, &sample_payload()).await;
                assert!(matches!(out[0], PushOutcome::ChannelDisabled(_)), "{:?}", out[0]);
                assert_eq!(out[1], PushOutcome::Sent, "本地那条不受影响");
            });
        });
        let log = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
        assert!(log.contains("PushConfig"), "日志抓取本身要成立，否则下面的断言是空的：{log}");
        assert!(log.contains(MASK), "日志里的 webhook/secret 该是打码的：{log}");
        assert!(!log.contains(TOKEN) && !log.contains(SECRET), "日志里出现了明文密钥：{log}");
    }

    #[test]
    fn push_config_roundtrips_via_meta_and_never_gives_the_secret_back_in_clear() {
        let db = Db::in_memory().unwrap();
        // 没写过 → 默认 = 关闭 + 空（"没配好之前不往外发"的闸门）。
        assert_eq!(PushConfig::load(&db).unwrap(), PushConfig::default());

        let webhook = format!("{}?access_token={TOKEN}", dingtalk::WEBHOOK_BASE);
        let cfg = PushConfig { webhook: webhook.clone(), secret: SECRET.to_string(), enabled: true };
        cfg.save(&db).unwrap();
        assert_eq!(PushConfig::load(&db).unwrap(), cfg, "必须逐字段往返（meta KV，无迁移）");

        // 回显值原样存回来**必须拒绝**：那会把 `***` 里的假密钥写进库，之后每条都 310000，
        // 而且从配置面上看不出毛病。
        let echo = cfg.echo();
        assert_eq!(PushConfig::load_echo(&db).unwrap(), echo, "UI 回显路由（读库 + 打码）");
        assert!(!echo.webhook.contains(TOKEN) && !echo.secret.contains(SECRET), "{echo:?}");
        let bad = PushConfig { webhook: echo.webhook, secret: echo.secret, enabled: true };
        assert!(bad.save(&db).is_err(), "打码串不能回存");
        assert_eq!(PushConfig::load(&db).unwrap(), cfg, "拒绝写入不得留下半份配置");
    }

    #[test]
    fn channels_always_include_the_local_center_and_dingtalk_only_when_configured() {
        let names = |c: &PushConfig| -> Vec<&'static str> {
            c.channels().iter().map(|ch| ch.name()).collect()
        };
        assert_eq!(
            names(&PushConfig::default()),
            vec![LOCAL_CHANNEL],
            "本地提醒中心恒在（用户收回豁免时的唯一通道），钉钉没配好就不在场"
        );

        let ready = PushConfig {
            webhook: format!("{}?access_token={TOKEN}", dingtalk::WEBHOOK_BASE),
            secret: SECRET.to_string(),
            enabled: true,
        };
        assert_eq!(names(&ready), vec![LOCAL_CHANNEL, DINGTALK_CHANNEL]);

        // 开关关掉 / webhook 还空着：都不装钉钉通道 —— 装上了就意味着一轮注定失败的外发。
        for c in [
            PushConfig { enabled: false, ..ready.clone() },
            PushConfig { webhook: String::new(), ..ready.clone() },
            PushConfig { webhook: "   ".into(), ..ready.clone() },
        ] {
            assert_eq!(c.channels().len(), 1, "不该装钉钉通道：{c:?}");
        }
    }
}
