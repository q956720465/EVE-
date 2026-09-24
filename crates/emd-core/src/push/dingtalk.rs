//! 钉钉群自定义机器人：加签、markdown 私有卡片、错误码映射。
//! 通道按用户 2026-09-24 改定，替代 spec §4.5 与计划里的飞书（D1）。
//!
//! **纯函数**：无网络、无 DB、无时钟 —— `timestamp_ms` 是参数，HTTP 收发与真实墙上钟
//! 都归 T11 的发送器（Global Constraints 的零 IO 边界）。
//!
//! # 协议常量出处（核对日 2026-09-24）
//!
//! 端点、msgtype、错误码、加签配方、频控**只在本文件写一次**：T11 的发送器、卡片版式、
//! 错误处理全从这里读。每处各写一份，改协议那天必然漂移。
//!
//! ## 一、本机实测（未认证探测，2026-09-24）
//!
//! 用 `curl` 直接打真实的 `oapi.dingtalk.com/robot/send`。本机没有可用的 token，
//! 所以只走得到**错误路径**（成功路径一次都没跑过，见第四节）：
//! - `POST {WEBHOOK_BASE}?access_token=<不存在>` →
//!   `{"errcode":300005,"errmsg":"token is not exist"}`
//! - 缺 / 空 `access_token` → `{"errcode":40035,"errmsg":"缺少参数 access_token"}`
//! - `GET` 同一 URL → `{"errcode":43002,"errmsg":"需要POST请求"}`
//! - **报错也是 HTTP 200 + `Content-Type: application/json`**：成败只能看 body 里的
//!   `errcode`。T11 的发送器拿状态码当判据的话，"推失败"会伪装成"推成功"——静默丢告警。
//!
//! ## 二、三源核对一致（仍未真机验证）
//!
//! - **加签配方**：`timestamp` 毫秒；`stringToSign = "{timestamp}\n{secret}"`；
//!   `sign = urlencode(base64(hmac_sha256(key = secret, msg = stringToSign)))`；
//!   `timestamp` 与 `sign` 作为 **URL 查询参数**追加。
//!   三源一致：本计划「外部协议常量」节、钉钉官方文档「自定义机器人安全设置」页
//!   （JS 渲染，本机抓不到正文，只核到该页确为此主题）、开源实现 DingtalkChatbot 的
//!   `chatbot.py` 逐字（`quote_plus(base64.b64encode(hmac.new(secret, "{ts}\n{secret}", sha256)))`
//!   之后 `&timestamp=&sign=` 追加到 URL）。
//! - **markdown 消息体**：`{"msgtype":"markdown","markdown":{"title":"…","text":"…"}}`
//!   （另有可选的 `at`）—— Apifox 镜像的官方 API 定义「自定义机器人发送群消息」逐字。
//! - **频控 20 条/分钟**：阿里云官方错误码表里 `130101` 的原文即
//!   `send too fast, exceed 20 times per minute`。
//!
//! ## 三、错误码：官方表 + 实测对照，**计划/brief 有两条与它们冲突**（以核到的为准）
//!
//! - `0` + `ok`：成功。**未真机验证**（没有可用 token，"成功长什么样"来自文档与计划）。
//! - `310000`：安全校验失败。官方表列了**三种** errmsg —— `keywords not in content` /
//!   `sign not match` / `ip X.X.X.X not in whitelist`（阿里云错误表与 Apifox 镜像一致）。
//!   一个 code 三种含义，**只看 code 必然错**：本文件的映射同时吃 `msg`。
//! - 限流：`130101`（阿里云表原文，见上）与 `410100`（Apifox 镜像「发送速度太快而限流」）。
//! - **冲突一**：计划/brief 写 `300001` = 频率超限。三条反证都不支持它 ——
//!   ① 阿里云官方表里 `300001` 是 `token is not exist`；② Apifox 镜像的限流码是 `410100`；
//!   ③ 本机拿不存在的 token 打真实端点，回来的是 `300005` 而不是 `300001`。
//!   处理：[`map_errcode`] **先按 errmsg 特征判限流**，`300001` 带上频率特征时照样进
//!   [`PushOutcome::Retry`]（brief 的测试口径成立），不带频率特征时按 token 失效处理
//!   （不可重试）。这样真实服务器换限流码那天，不会退化成"未知失败 = 不重试 = 丢告警"。
//! - **冲突二**：计划/brief 写 `400013` = 机器人已停用。Apifox 镜像里 `400013` =
//!   `群已被解散`，而"机器人已停用"是 `400102`（阿里云表同样作 `bot is stopped`）。
//!   两者都是不可重试的通道级失效，[`map_errcode`] 的理由串把两种口径并列写出，
//!   免得 T11 在 UI 上给用户指错方向。
//! - 其余码（`40035` 缺参数 / `43004` Content-Type 不对 / `400105` 不支持的消息类型 /
//!   `400101` token 不存在 / `400106` 机器人不存在 / `-1` 系统繁忙）见下方常量区与
//!   [`map_errcode`] 的分支。
//!
//! ## 四、待核（**没有真机**）
//!
//! - 加签密钥以 `SEC` 开头：**只有社区惯例，没在官方正文核到**。代码**不校验**这个前缀
//!   —— 真值由机器人后台生成，本地校验只会误伤合法密钥。
//! - 卡片里哪些 markdown 语法钉钉真渲染：本机没有机器人，没渲染过。[`render_markdown`]
//!   因此只用最保守的 `###` 标题 + `-` 行 + `**` 加粗，**不用表格**（钉钉 markdown 的
//!   表格支持众说纷纭，赌它会渲染不如逐行写死）。
//! - 20 条/分钟的**计数口径**（按机器人 / 按群 / 按企业）没核到，只核到"每分钟 20 条"这个数。
//! - **成功路径（`errcode 0`、卡片真正渲染出来）一次都没跑过**：用户尚未配置机器人，
//!   本机没有 access_token/secret。真机首推若见 `310000 sign not match`，先查密钥与系统时钟。
//!
//! ## 五、脱敏（Global Constraints，硬约束）
//!
//! [`sign`] 的入参含 secret，[`signed_url`] 的产物含 `access_token` 与 `sign`：
//! **两者都不得进日志、错误串或 `Debug`**。本文件不打印任何东西；[`map_errcode`] 的 `msg`
//! 只接受钉钉响应体的 `errmsg`（服务器不会回显我们的密钥），调用方**不得**把 URL / sign
//! 传进来。中段打码函数（`mask`）归 T11。

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::alert::{AlertKind, AlertPayload, TRACK_EXPECTED};

/// 群自定义机器人的端点。用户配置的 webhook 是"它 + `?access_token={token}`"，
/// 也可能还带别的查询参数。**唯一持有处**：T11 的发送器从这里读，别各自拼字面量。
pub const WEBHOOK_BASE: &str = "https://oapi.dingtalk.com/robot/send";

/// 本通道用的消息类型。改成 `text`/`actionCard` 要连卡片版式一起改，故立成常量。
pub const MSGTYPE_MARKDOWN: &str = "markdown";

/// 频控上限（条/分钟）：阿里云官方错误码表 `130101` 的原文
/// `send too fast, exceed 20 times per minute`。**待核**：计数口径（按机器人/群/企业）没核到。
pub const RATE_LIMIT_PER_MINUTE: u32 = 20;

/// 限流后的建议等待：等满一个完整的分钟窗口，比试探性重试省事
/// （也省得踩 `300001` 那条歧义码）。T11 的调度据此排下一次。
pub const RETRY_AFTER_RATE_LIMIT_SECS: u64 = 60;

/// `-1 系统繁忙` 的建议等待：服务端瞬时故障，退一步就够，不必等整分钟。
pub const RETRY_AFTER_BUSY_SECS: u64 = 30;

/// 成功码（`{"errcode":0,"errmsg":"ok"}`）。
const ERR_OK: i64 = 0;
/// 安全校验失败（关键词 / 加签 / IP 白名单三种 errmsg 共用一个码）。
const ERR_SECURITY: i64 = 310000;
/// 限流（阿里云官方错误码表原文）。
const ERR_THROTTLE_ALIYUN: i64 = 130101;
/// 限流（Apifox 镜像的官方 API 定义）。
const ERR_THROTTLE_APIFOX: i64 = 410100;
/// `300001`：**一个码两种官方口径** —— 计划/brief 说"频率超限"，阿里云官方表说
/// `token is not exist`。本文件按 errmsg 消歧（见模块头"冲突一"），故它同时出现在
/// 限流判定与 token 失效两个分支里。
const ERR_LEGACY_300001: i64 = 300001;
/// webhook token 不存在（**本机实测**：拿不存在的 token 打真实端点就是这个码）。
const ERR_TOKEN_GONE_PROBED: i64 = 300005;
/// token 不存在（Apifox 镜像的 v2 口径）。
const ERR_TOKEN_GONE_V2: i64 = 400101;
/// `400013`：Apifox 镜像作「群已被解散」，计划作「机器人已停用」—— 都是不可重试的通道级失效。
const ERR_GROUP_OR_BOT_GONE: i64 = 400013;
/// 机器人已停用（阿里云表 `bot is stopped` 与 Apifox 镜像一致）。
const ERR_BOT_STOPPED: i64 = 400102;
/// 机器人不存在。
const ERR_BOT_NOT_FOUND: i64 = 400106;
/// 服务端瞬时繁忙。
const ERR_BUSY: i64 = -1;

/// 卡片上的时间形状（字段表与页脚共用一处，免得同一张卡上出现两种时间写法）。
const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

/// 一条告警推出去之后的结果（T11 的 `dispatch` 消费、T12 的轮次报告统计）。
///
/// **只分四档，按"接下来该做什么"分，不按错误码分**：能重试的 / 重试也没用的 / 成功的 /
/// 没归类到的。T11 拿到它只需回答一个问题——"这条告警还该不该再试"。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushOutcome {
    /// 钉钉确认收到（`errcode == 0`）。
    Sent,
    /// 可重试，建议 `retry_after_secs` 秒后再来。**这不是失败**：限流与瞬时繁忙都归这里，
    /// 告警本身没错，等一等就能进。
    Retry {
        retry_after_secs: u64,
        /// 给日志 / UI 的原因串（**只放 errcode 与服务器 errmsg，不放 token/sign/webhook**）。
        reason: String,
    },
    /// 通道级失效：重试无用，必须人工修配置（加签密钥不对、token 失效、机器人被停用或群散了）。
    /// 带一句人能看懂、能照着改的理由。
    ChannelDisabled(String),
    /// 非成功的兜底：收到了响应，但既不是成功、也归不到上面几档（请求畸形、平台规则拒绝等）。
    /// **未知错误码必须落这里 —— 绝不能当成成功**：把丢弃告警伪装成推送成功，用户会以为
    /// 链路在跑，实际一条都没到。
    Failed(String),
}

/// `sign = urlencode(base64(hmac_sha256(key = secret, msg = "{timestamp_ms}\n{secret}")))`。
///
/// 返回**已 URL 编码**的串。原始 base64 的 `+` 在查询串里会被解成空格、`=` 会把参数提前
/// 截断 —— 不编码就是一个"看着发出去了、钉钉一律回 310000"的隐雷。
/// 编码用 `urlencoding::encode`（标准百分号编码）：base64 字母表里没有空格，所以它与
/// 后台文档示例里的 `quote_plus` 结果逐字符相同，不存在"选错编码器"的分歧。
///
/// 入参含 secret：**不要**把这个结果或 secret 写进日志/错误串（模块头第五节）。
pub fn sign(timestamp_ms: i64, secret: &str) -> String {
    // 拼接形态本身就是协议：顺序与分隔符都不能动。改了不会报错，只会得到一个"稳定地错"的
    // 签名，而钉钉的回应永远是同一句 310000 sign not match，看不出错在哪一步。
    let string_to_sign = format!("{timestamp_ms}\n{secret}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC 接受任意长度密钥，new_from_slice 不会返回 Err");
    mac.update(string_to_sign.as_bytes());
    let digest = mac.finalize().into_bytes();
    // 先 base64 再 urlencode，顺序不能换（见函数文档：不编码的 `+` 会被解成空格）。
    urlencoding::encode(&base64::engine::general_purpose::STANDARD.encode(digest)).into_owned()
}

/// 把 `timestamp` 与 `sign` 追加成**查询参数**（不是请求体字段）后的完整 webhook。
///
/// 先探测 webhook 里有没有 `?` 再决定用 `&` 还是 `?`：用户可能配一条不带任何查询参数的
/// webhook，无脑拼 `&` 会把 sign 粘到路径上（钉钉只会回一句没法排查的错）。
///
/// **产物含 access_token 与 sign**：调用方只许拿它发请求，**不许** `Debug`/日志/错误串带上
/// 它（模块头第五节）；T11 要回显也得先过 `mask`。
pub fn signed_url(webhook: &str, secret: &str, timestamp_ms: i64) -> String {
    let sep = if webhook.contains('?') { '&' } else { '?' };
    format!("{webhook}{sep}timestamp={timestamp_ms}&sign={}", sign(timestamp_ms, secret))
}

/// 渲染钉钉 markdown 卡片，返回 `(title, text)`。
///
/// `title` 进消息体的 `markdown.title`（手机通知栏只有这一行）；`text` 是正文，
/// **首行写同一个标题** —— 钉钉卡片正文没有独立 header，标题不进正文的话，群里翻记录时
/// 就只剩一屏没有题头的数字。
///
/// 版式照 spec §4.5（标题 `[亏损提醒] {type_name} · {买/卖}单 · {站点真名}`；主体逐行字段表；
/// 口径摘要；页脚角标 + 告警时刻 + 检索提示）。两处形态差异就地记在这里：
///
/// - **D4（控制器裁决，2026-09-24）**：spec §4.5 的"折叠区口径摘要"在钉钉 markdown 里
///   **没有对应物** —— 钉钉 markdown 无折叠语法。这里落成普通小字号行（加粗小标题 + 列表），
///   信息一条不裁，只是不折叠。
/// - 页脚的"告警时刻"取载荷自己的 `at`（挂单轨 = 挂出时刻、已实现轨 = 成交时刻）：
///   本函数是纯函数、拿不到时钟（Global Constraints），真实发送的墙上钟属 T11 的发送器，
///   这里不编一个出来。口径区已单列 `data_age_secs`，读者能反推判定时刻。
pub fn render_markdown(p: &AlertPayload) -> (String, String) {
    let side = if p.is_buy { "买单" } else { "卖单" };
    let title = format!("[亏损提醒] {} · {side} · {}", p.type_name, p.location_name);
    // 三种形态的时间锚点不是同一个东西（见 `AlertPayload::at` 的文档）：标题里点明是哪个，
    // 免得读者把"挂出时刻"当成"成交时刻"，进而以为货已经卖掉了。
    let anchor = match p.kind {
        AlertKind::RealizedLoss => "成交时刻",
        _ => "挂出时刻",
    };

    // 检索号：spec §4.5 的页脚与客户端提醒中心都靠它定位。已实现轨的去重键是成交 id
    // （规范形态 `tx:{id}`，定义在 `alert::tx_alert_key`，**不在这里拼**）；原挂单 id 只是
    // 回填匹配的产物，匹配不上时是 0 —— 那张卡必须写明"没看到"，不能指一个错单号
    // （用户拿它去检索，检索到的是别人的单）。
    let id_lines = match p.kind {
        AlertKind::RealizedLoss => {
            // 只读规范串里 ':' 之后那一段；形状认不出就整串回显 —— 宁可难看，不可检索不到。
            let tx = p
                .alert_key
                .split_once(':')
                .map_or(p.alert_key.as_str(), |(_, n)| n);
            let origin = if p.order_id == 0 {
                "- **order_id**：未在本机观察窗内（卖出侧中介费按 0 计）\n".to_string()
            } else {
                format!("- **order_id**：{}\n", p.order_id)
            };
            format!("- **tx#**：{tx}\n{origin}")
        }
        _ => format!("- **order_id**：{}\n", p.order_id),
    };

    let mut text = format!("### {title}\n\n{id_lines}");
    text.push_str(&format!("- **type_id**：{}\n", p.type_id));
    text.push_str(&format!("- **location_id**：{}\n", p.location_id));
    text.push_str(&format!("- **方向**：{side}\n"));
    text.push_str(&format!("- **价格**：{:.2} ISK\n", p.price));
    text.push_str(&format!("- **数量**：{}\n", p.volume));
    text.push_str(&format!("- **时间（{anchor}）**：{} UTC\n", p.at.format(TIME_FMT)));
    text.push_str(&format!("- **亏损额**：**{:.2} ISK**\n", p.loss_isk));
    text.push_str(&format!("- **负 margin**：{:.2}%\n", p.margin_pct));

    // 口径摘要：**逐字段原样打印，一个字都不重算**。T8 把 `CaliberSummary` 填出来就是为了
    // 卡片不跟判定漂移 —— 尤其 `formula`（形态 ③ 的串里带着已接受的 C1 口径"买入侧中介费
    // 本机无法归属，未计"）与 `cost_source`（有**三个**取值，含 `COST_SRC_BUY_TRAP`，
    // 别按两值枚举匹配）。这里重算/重排一遍，就等于把那条口径悄悄删掉。
    text.push_str("\n**口径摘要**（自报轨道与数字来源）\n");
    text.push_str(&format!("- 轨道：{}\n", p.caliber.track));
    text.push_str(&format!("- 有效销售税率：{:.4}%\n", p.caliber.sales_tax_pct));
    text.push_str(&format!("- 有效中介费率：{:.4}%\n", p.caliber.broker_pct));
    text.push_str(&format!("- 技能口径：{}\n", p.caliber.skill_caliber));
    text.push_str(&format!(
        "- 单位成本：{:.4} ISK（来源：{}）\n",
        p.caliber.unit_cost, p.caliber.cost_source
    ));
    text.push_str(&format!("- 数据年龄：{} 秒\n", p.caliber.data_age_secs));
    text.push_str(&format!("- 公式：{}\n", p.caliber.formula));

    // 页脚：私有数据角标（spec §4.5 的豁免记录限定的正是这类卡片）+ 告警时刻 + 检索提示。
    // "基于估算费率"只对预期轨出现（已实现轨全是 journal 真值，标它反而误导）；判据取轨道串
    // 本身（`caliber.track` 就是 T8 自报的轨道），不重算一遍口径。
    let mut footer = String::from("---\n**私有数据**");
    if p.caliber.track == TRACK_EXPECTED {
        footer.push_str(" · 基于估算费率");
    }
    footer.push_str(&format!(
        " · 告警时刻（判定锚点）{} UTC · 客户端提醒中心可按 order_id 检索",
        p.at.format(TIME_FMT)
    ));
    text.push_str(&footer);
    text.push('\n');
    (title, text)
}

/// 钉钉响应的 `errcode`/`errmsg` → "接下来该做什么"。
///
/// **必须同时看两个输入**：钉钉对同一个 code 有多个 errmsg（`310000` 就有三种含义），
/// 只看 code 会把"IP 不在白名单"和"关键词不匹配"混成一句没法照着改的话。
///
/// `msg` **只接受钉钉响应体的 `errmsg`**：它是唯一会出现在本函数输出里的外部文本，
/// 而服务器不会回显我们的密钥。把 URL/sign 传进来，等于把它们写进日志（模块头第五节）。
pub fn map_errcode(code: i64, msg: &str) -> PushOutcome {
    if code == ERR_OK {
        return PushOutcome::Sent;
    }
    let lower = msg.to_lowercase();
    // 限流判在 code 分派**之前**：钉钉对"限流"用过不止一个码（核到的是 130101 与 410100，
    // 计划里还写过 300001），只认 code 的话，真实服务器换个码就退化成"未知失败 = 不重试 =
    // 丢告警"。errmsg 特征比 code 耐改，代价只是 errmsg 里偶然出现频率词时多退避一分钟。
    if is_throttled(code, &lower) {
        return PushOutcome::Retry {
            retry_after_secs: RETRY_AFTER_RATE_LIMIT_SECS,
            reason: format!("被钉钉限流（errcode {code} {msg}），等一个完整的分钟窗口再来"),
        };
    }
    match code {
        ERR_SECURITY => PushOutcome::ChannelDisabled(security_reason(msg)),
        // token 类失效：300005 是本机实测到的（不存在的 token），400101 是 v2 口径，
        // 300001 见模块头"冲突一"—— 不带频率特征时它是官方表里的 `token is not exist`。
        ERR_TOKEN_GONE_PROBED | ERR_TOKEN_GONE_V2 | ERR_LEGACY_300001 => {
            PushOutcome::ChannelDisabled(format!(
                "webhook token 不可用（errcode {code} {msg}）：去群机器人的设置里重新复制 webhook"
            ))
        }
        ERR_GROUP_OR_BOT_GONE => PushOutcome::ChannelDisabled(format!(
            "机器人在群里已不可用（errcode {code} {msg}）：官方两种口径分别是「群已被解散」与\
             「机器人已停用」，先去群里确认机器人还在"
        )),
        ERR_BOT_STOPPED => PushOutcome::ChannelDisabled(format!(
            "机器人已停用（errcode {code} {msg}）：去群机器人设置里重新启用"
        )),
        ERR_BOT_NOT_FOUND => PushOutcome::ChannelDisabled(format!(
            "机器人不存在（errcode {code} {msg}）：这条 webhook 可能来自另一个群，重新复制一条"
        )),
        ERR_BUSY => PushOutcome::Retry {
            retry_after_secs: RETRY_AFTER_BUSY_SECS,
            reason: format!("钉钉侧瞬时繁忙（errcode {code} {msg}）"),
        },
        // 剩下的大多是"我们自己的请求不对"（40035 缺参数 / 43004 Content-Type / 400105
        // 不支持的消息类型…）以及将来新出现的码。一律 `Failed`：**未知码绝不能落 `Sent`**
        // —— 把丢弃告警伪装成推送成功，用户会以为链路在跑，实际一条都没到；errcode 原文
        // 带出来，好对着官方错误表查。
        other => PushOutcome::Failed(format!(
            "钉钉返回非成功响应且未被归类（errcode {other} {msg}）"
        )),
    }
}

/// 限流判定：两个"只表示限流"的官方码，或者 errmsg 自己带着频率特征（钉钉换码时的兜底）。
fn is_throttled(code: i64, lower_msg: &str) -> bool {
    // `300001` **不能**无条件算限流：官方表说它是 `token is not exist`（见模块头"冲突一"）。
    // 把一个不可重试的失效码当作限流，就是拿一条永远好不了的 token 去打重试风暴；
    // 反过来漏判限流只是慢一拍。所以歧义码一律看 errmsg，不看不判。
    if matches!(code, ERR_THROTTLE_ALIYUN | ERR_THROTTLE_APIFOX) {
        return true;
    }
    lower_msg.contains("frequency")
        || lower_msg.contains("too fast")
        || lower_msg.contains("too many")
        || lower_msg.contains("exceed")
        || lower_msg.contains("限流")
        || lower_msg.contains("太快")
}

/// `310000` 的三种官方含义各给一句"照着能改"的话（这句会直接进 T11 的 UI 与日志）。
/// 认不出是哪种就照原样回显并补齐三种可能 —— 宁可难看，不可误导。
fn security_reason(msg: &str) -> String {
    let lower = msg.to_lowercase();
    let head = "机器人安全校验不通过：";
    if lower.contains("keyword") || msg.contains("关键词") {
        format!(
            "{head}关键词不匹配（errcode {ERR_SECURITY} {msg}）：机器人配了自定义关键词，\
             卡片正文里必须出现它 —— 标题那行会带着类型名，核一下关键词是不是也写在那儿"
        )
    } else if lower.contains("sign") || msg.contains("签名") {
        format!(
            "{head}加签失败（errcode {ERR_SECURITY} {msg}）：核 secret 与机器人后台是否一致、\
             timestamp 是否毫秒（且与本机时钟同窗 —— 系统时间漂了同样中这一条）"
        )
    } else if lower.contains("whitelist") || lower.contains("ip") || msg.contains("白名单") {
        format!(
            "{head}IP 不在白名单（errcode {ERR_SECURITY} {msg}）：把本机出口 IP 加进机器人的\
             IP 白名单，或改用别的安全设置"
        )
    } else {
        format!(
            "{head}errcode {ERR_SECURITY} {msg}（三种已知含义：关键词不匹配 / 加签失败 / \
             IP 不在白名单）"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    use crate::alert::{order_alert_key, CaliberSummary, COST_SRC_BUY_TRAP, COST_SRC_FIFO90, TRACK_REALIZED};
    use crate::char::JournalEntry;
    use crate::market::STATION_JITA;
    use crate::store::{CharOrder, WalletTx};

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// 形态 ① 的夹具（挂卖单预期亏）：口径摘要同时带"估算费率"与成本来源，
    /// 卡片上该有的元素它都有。
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
            margin_pct: -1.340_789_473_684_210_5,
            caliber: CaliberSummary {
                track: TRACK_EXPECTED.to_string(),
                sales_tax_pct: 3.375,
                broker_pct: 0.0,
                skill_caliber:
                    "Accounting 5 / Broker Relations 0（税率随面板；中介费取 journal 实付）"
                        .to_string(),
                unit_cost: 95.0,
                cost_source: COST_SRC_FIFO90.to_string(),
                formula: "① 单位净额 93.726250 = 挂价 97.000000 × (1 − 有效税 3.3750%)；\
                          单位全成本 95.000000 = FIFO 均价 95.000000 + 实付中介费/单位 0.000000"
                    .to_string(),
                data_age_secs: 60,
            },
        }
    }

    #[test]
    fn sign_is_urlencoded_base64_of_hmac_sha256() {
        // 用固定 timestamp + secret 断言输出稳定（防"改了拼接顺序没人发现"）
        // 关键细节：timestamp 毫秒、stringToSign = "{ts}\n{secret}"、结果 base64 后必须 urlencode
        let s = sign(1_700_000_000_000, "SECtest");
        assert!(!s.contains('+') && !s.contains('/') && !s.contains('='), "必须已 URL 编码：{s}");
        // 锚点向量：与**两个独立实现**逐字一致（Python `hmac`+`hashlib`+`base64` 与
        // `openssl dgst -sha256 -hmac` 两条路算出的同一个串）。只断言"没有 + / / ="挡不住
        // 拼接顺序或 key/msg 写反 —— 那种错签名会**稳定地错**，钉钉只回一句 310000。
        assert_eq!(s, "aZLLrriXgn05YbwaGR7knYsLeJADjr9NwLaNNKpxh4g%3D", "拼接形态或算法被改动");

        // 第二条向量**故意挑 base64 里含 `+` 与 `/` 的**：上面那条的 base64 只有 `=` 要编码，
        // 光靠它证不出"urlencode 真的做了"（漏编码的产物同样不含 + 和 /）。
        let hard = sign(1_700_000_000_000, "SECx");
        assert_eq!(
            hard, "rgcjbH%2ByFyES%2BS0llX%2BcAPCwVRpsr7sbiX5Lnyo%2FocU%3D",
            "base64 里的 + 必须编码成 %2B、/ 必须编码成 %2F"
        );
        assert!(hard.contains("%2B") && hard.contains("%2F") && hard.contains("%3D"), "{hard}");
    }

    #[test]
    fn signed_url_appends_timestamp_and_sign() {
        let u = signed_url("https://oapi.dingtalk.com/robot/send?access_token=T", "SECx", 1_700_000_000_000);
        assert!(u.contains("access_token=T"));
        assert!(u.contains("timestamp=1700000000000"));
        assert!(u.contains("&sign="));
        // 断言信息里不回显整条 URL：它含 access_token 与 sign（模块头第五节的脱敏纪律）。
        assert!(
            u.starts_with("https://oapi.dingtalk.com/robot/send?access_token=T&timestamp=1700000000000&sign="),
            "timestamp 必须是毫秒且与 sign 一起作查询参数追加"
        );
        // 不带查询参数的 webhook 也要接得上：否则 sign 会粘在路径上，报错信息还看不出原因。
        let bare = signed_url(WEBHOOK_BASE, "SECx", 1);
        assert!(bare.starts_with(&format!("{WEBHOOK_BASE}?timestamp=1&sign=")), "{bare}");
    }

    #[test]
    fn render_markdown_carries_private_badge_and_caliber() {
        let p = sample_payload();
        let (title, text) = render_markdown(&p);
        assert!(title.contains("亏损提醒"), "{title}");
        assert!(text.contains("私有数据"), "spec §4.5 要求页脚角标标私有数据");
        assert!(text.contains("order_id") || text.contains("tx#"), "必须能按 id 检索");
        assert!(text.contains("估算费率") || text.contains("journal 真值"), "轨道口径要自报");

        // 标题版式逐字照 spec §4.5；正文首行同一个标题（钉钉正文没有独立 header）。
        assert_eq!(title, "[亏损提醒] Tritanium · 卖单 · Jita IV - Moon 4 - Caldari Navy Assembly Plant");
        assert!(text.starts_with(&format!("### {title}")), "{text}");

        // 字段表要能一眼检索：order_id / type_id / location_id / 方向 / 价格 / 数量 / 时间 /
        // 亏损额 / 负 margin —— spec §4.5 的九项一项不少。
        for needle in [
            "order_id", "type_id", "location_id", "方向", "价格", "数量", "时间", "亏损额",
            "负 margin",
        ] {
            assert!(text.contains(needle), "字段表缺 {needle}：{text}");
        }

        // 口径摘要**逐字段原样打印**：整串公式必须逐字出现（不是重排、不是重算）。
        assert!(text.contains(&p.caliber.formula), "公式串必须逐字进卡片：{text}");
        assert!(text.contains(&p.caliber.skill_caliber), "技能口径必须逐字进卡片");
        assert!(text.contains(&p.caliber.track), "轨道必须自报");
        assert!(text.contains(COST_SRC_FIFO90), "成本来源必须自报");
        assert!(text.contains("60 秒"), "数据年龄要自报：{text}");

        // 页脚的其余两项。
        assert!(text.contains("告警时刻"), "{text}");
        assert!(text.contains("客户端提醒中心可按 order_id 检索"), "{text}");
    }

    #[test]
    fn render_markdown_prints_cost_source_verbatim_instead_of_matching_two_values() {
        // 成本来源有**三个**取值（T8 的备注：`COST_SRC_BUY_TRAP` 超出 spec 的两值口径）：
        // 卡片必须照印，不能按两值枚举匹配 —— 匹配就会把这个取值打进"其他"分支。
        let mut p = sample_payload();
        p.kind = AlertKind::BuyOrderTrap;
        p.is_buy = true;
        p.caliber.cost_source = COST_SRC_BUY_TRAP.to_string();
        p.caliber.track = TRACK_EXPECTED.to_string();
        let (title, text) = render_markdown(&p);
        assert!(text.contains(COST_SRC_BUY_TRAP), "{text}");
        assert!(title.contains("买单"), "方向取自 is_buy：{title}");
    }

    #[test]
    fn render_markdown_does_not_lose_the_realized_track_caveat() {
        // 形态 ③ 走到这里用的是**真判定产物**（`detect_realized`），不是手搓载荷：
        // 要证的正是"口径摘要从判定到卡片一个字没变"——重算/重排一遍，
        // T8 写进公式串的 C1 口径（"买入侧中介费本机无法归属，未计"）就会静默消失。
        let txs = vec![
            WalletTx {
                transaction_id: 1,
                date: "2026-09-10T00:00:00Z".to_string(),
                type_id: 34,
                location_id: STATION_JITA,
                is_buy: true,
                unit_price: 100.0,
                quantity: 100,
            },
            WalletTx {
                transaction_id: 2,
                date: "2026-09-20T00:00:00Z".to_string(),
                type_id: 34,
                location_id: STATION_JITA,
                is_buy: false,
                unit_price: 90.0,
                quantity: 100,
            },
        ];
        // 成交前那一轮的挂单快照：成交后这张单就不在快照里了，只有它能把这笔流水对回单号。
        let orders = vec![CharOrder {
            order_id: 555,
            type_id: 34,
            location_id: STATION_JITA,
            is_buy: false,
            price: 90.0,
            volume_remain: 100,
            issued: "2026-09-15T00:00:00Z".to_string(),
            duration: 90,
            fetched_at: at("2026-09-24T12:00:00Z").timestamp(),
        }];
        let journal = vec![
            JournalEntry {
                id: 9001,
                date: "2026-09-20T00:00:01Z".to_string(),
                ref_type: "transaction_tax".to_string(),
                amount: Some(-300.0),
                context_id: Some(2),
                description: String::new(),
            },
            JournalEntry {
                id: 9002,
                date: "2026-09-20T00:00:01Z".to_string(),
                ref_type: "brokers_fee".to_string(),
                amount: Some(-270.0),
                context_id: Some(555),
                description: String::new(),
            },
        ];
        let hits = crate::alert::detect_realized(
            &txs,
            &orders,
            &journal,
            &crate::alert::NameLookup::default(),
            at("2026-09-24T12:00:00Z").timestamp(),
        );
        assert_eq!(hits.len(), 1, "夹具应当产出一条已实现亏");
        let p = &hits[0];

        let (title, text) = render_markdown(p);
        assert!(title.contains("卖单") && title.contains("type_id 34"), "{title}");
        assert!(text.contains("tx#"), "已实现轨的检索键是 tx#：{text}");
        assert!(text.contains("tx#**：2"), "去重键里的成交 id 要原样带出来：{text}");
        assert!(text.contains("order_id**：555"), "回填匹配到的原挂单 id 要能检索：{text}");
        assert!(text.contains(TRACK_REALIZED), "已实现轨要自报 track：{text}");
        assert!(
            text.contains(&p.caliber.formula) && text.contains("买入侧中介费本机无法归属，未计"),
            "C1 口径就在公式串里，重算一遍这句话就没了：{}\n---\n{text}",
            p.caliber.formula
        );
    }

    #[test]
    fn errcode_maps_to_actionable_outcomes() {
        assert!(matches!(map_errcode(0, "ok"), PushOutcome::Sent));
        assert!(matches!(map_errcode(310000, "sign not match"), PushOutcome::ChannelDisabled(_)));
        assert!(matches!(map_errcode(300001, "frequency"), PushOutcome::Retry { .. }));
        // 未知错误码不能当成成功
        assert!(!matches!(map_errcode(999999, "?"), PushOutcome::Sent));
    }

    #[test]
    fn errcode_uses_the_message_to_disambiguate_shared_codes() {
        // 钉钉对同一个 code 有多个 errmsg（310000 三种含义），只看 code 必然误判：
        // 三种"照着能改"的建议必须各不相同，未知码必须落 Failed（非成功，不当成功）。
        let keyword = map_errcode(310000, "keywords not in content");
        let sign = map_errcode(310000, "sign not match");
        let ip = map_errcode(310000, "ip 1.2.3.4 not in whitelist");
        for o in [&keyword, &sign, &ip] {
            assert!(matches!(o, PushOutcome::ChannelDisabled(_)), "{o:?}");
        }
        assert_ne!(keyword, sign, "同一 code 的不同 errmsg 要给不同的处理建议");
        assert_ne!(sign, ip);
        let PushOutcome::ChannelDisabled(text) = keyword else { unreachable!() };
        assert!(text.contains("关键词"), "{text}");

        // 实测到的码：token 不存在 = 300005（**不是** 300001）→ 不可重试。
        assert!(matches!(map_errcode(300005, "token is not exist"), PushOutcome::ChannelDisabled(_)));
        // 同码不带频率特征时不能当限流：一个不可重试的失效码被当成限流，会变成重试风暴。
        assert!(matches!(map_errcode(300001, "token is not exist"), PushOutcome::ChannelDisabled(_)));
        // 官方表的限流码（阿里云 130101 / Apifox 410100）与"系统繁忙"都要能重试。
        assert!(matches!(
            map_errcode(130101, "send too fast, exceed 20 times per minute"),
            PushOutcome::Retry { retry_after_secs: RETRY_AFTER_RATE_LIMIT_SECS, .. }
        ));
        assert!(matches!(
            map_errcode(410100, "发送速度太快而限流"),
            PushOutcome::Retry { retry_after_secs: RETRY_AFTER_RATE_LIMIT_SECS, .. }
        ));
        assert!(matches!(map_errcode(-1, "系统繁忙"), PushOutcome::Retry { .. }));
        // 通道级失效（停用/解散/不存在）一律不可重试。
        for code in [400013, 400102, 400106, 400101] {
            assert!(matches!(map_errcode(code, "?"), PushOutcome::ChannelDisabled(_)), "{code}");
        }
        // 请求畸形（我们自己的错）与其它未知码 → Failed，且把 errcode 原文带出来便于排查。
        let PushOutcome::Failed(text) = map_errcode(400105, "不支持的消息类型") else {
            panic!("400105 应当是 Failed");
        };
        assert!(text.contains("400105"), "{text}");
    }
}
