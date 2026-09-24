//! SSO 令牌刷新**编排**：读凭据库 → 过期判定 → （仅过期时）POST 换新 → 写回凭据库。
//!
//! 为什么单开一个文件而不是塞进 [`crate::sso::flow`]：登录是"用户手点一次"的交互
//! （换浏览器、等回调、短超时），刷新是守护进程每 ~20 分钟自动跑一次的后台动作 ——
//! 触发者、失败后果、调用方三者都不同；而请求体构造与响应解析的**纯逻辑**两边共用
//! [`crate::sso::token`]，本文件不重复它。
//!
//! 协议常量核对（2026-09-25 对照 <https://developers.eveonline.com/docs/services/sso/>）：
//! - 令牌端点 `https://login.eveonline.com/v2/oauth/token`：已核对（与 [`crate::sso::token`]、
//!   [`crate::sso::flow`] 同页；授权码换令牌与刷新共用同一个端点）。本文件是第三份字面量，
//!   也是**唯一 `pub` 的一份**：生产调用方要显式传它，测试才能把同一个位置换成 `tiny_http`
//!   桩地址 —— 否则"过期 → 真刷新"那条路径只剩真机可测（见 [`refresh_if_needed`]）。
//! - 刷新请求体（`grant_type=refresh_token` + `refresh_token` + `client_id`，RFC 6749 §6）：
//!   复用 [`crate::sso::token::refresh_body`]，本文件不重写表单。
//! - spec §4.1「令牌刷新仅在过期时发生」：未过期时**一个请求都不发**，也不写凭据库。
//!
//! 脱敏纪律（Global Constraints）：access_token、refresh_token 与表单体一律不进日志、
//! 不进错误串 —— 本文件是这条链上唯一会拼错误串的地方，理由见 [`transport_kind`]。

use std::time::Duration;

use crate::error::{Error, Result};
// 分类清单与 push.rs 共用一份：两处曾各持一份，`is_builder` 的措辞已经分叉（T15 整改）。
use crate::push::transport_kind;
use crate::sso::store::TokenStore;
use crate::sso::token::{parse_token_response, refresh_body, TokenSet};

/// 刷新用的令牌端点（**唯一 `pub` 的一份字面量**，见模块头）。
///
/// 出处与日期同 [`crate::sso::token`]、[`crate::sso::flow`] 各持的那两份（2026-09-25 复核，
/// 同一页）。在这里放开可见性是为了让它当**入参**：生产调用方（[`crate::scheduler`]）显式传它，
/// 测试则把同一位置换成桩地址。
pub const DEFAULT_TOKEN_ENDPOINT: &str = "https://login.eveonline.com/v2/oauth/token";

/// 单次刷新的总超时。刷新是主循环里的一个后台步骤：它要么几秒内回来，要么根本不会回来；
/// reqwest 的默认值是**无超时**，没有这一行，一轮会永久卡在这一个请求上（而这一轮本该
/// 在几分钟后重试）。取值与登录侧同口径。
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// 过期就刷新，没过期就原样返回。返回的是**本次要用的那一份令牌**。
///
/// 四条契约（顺序就是实现顺序）：
/// - `store.load()` 没有令牌 → [`Error::Config`]。调用方的"没令牌 = 静默跳过"闸门已经跑过
///   （[`crate::scheduler::Scheduler::run_char_and_alerts`] 的 `Ok(None)` 那一支），所以走到
///   这里还是空的是**真错误**：把它也伪装成"跳过"，会让"重新登录一次就好"的故障变成永久静默。
/// - 没过期 → 原样返回，不发请求、不写凭据库（spec §4.1）。
/// - 过期 → POST [`refresh_body`] 到 `token_endpoint`（`application/x-www-form-urlencoded`），
///   用 [`parse_token_response`] 解析，写回 `store`，再返回。
/// - 响应缺 `refresh_token` 时**沿用旧值**（[`parse_token_response`] 的契约：缺席 = 空串 =
///   "本次没给"，不是"该令牌失效"）。写反了这一行就是把一条好用的刷新令牌覆盖成 `""`，
///   此后每次刷新都 `invalid_grant`，用户只能重新登录。
///
/// `now` 是参数而不是内部读钟：过期判定与 `expires_at = now + expires_in` 都钉在同一个时刻上，
/// 整个函数无隐藏时钟（与 [`crate::sso::token`] 的纯函数层同一纪律）。`token_endpoint`
/// 是参数是为了让刷新路径可测，见 [`DEFAULT_TOKEN_ENDPOINT`]。
pub async fn refresh_if_needed(
    client_id: &str,
    store: &dyn TokenStore,
    now: i64,
    token_endpoint: &str,
) -> Result<TokenSet> {
    let Some(old) = store.load()? else {
        return Err(Error::Config(
            "凭据库里没有 SSO 令牌（未登录或已登出），无法刷新".into(),
        ));
    };

    // 过期判定只走 T2 的谓词（提前 60 s 视为过期）：一轮跑起来要几十秒，踩着边界开一轮
    // 等于拿着即将作废的令牌去发请求。这里不自己比大小 —— 口径只有一份。
    if !old.is_expired(now) {
        return Ok(old);
    }

    let http = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|e| {
            Error::Config(format!(
                "构建 SSO 刷新 HTTP 客户端失败（{}）",
                transport_kind(&e)
            ))
        })?;

    let resp = http
        .post(token_endpoint)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(refresh_body(client_id, &old.refresh_token))
        .send()
        .await
        .map_err(|e| {
            Error::Config(format!(
                "刷新 SSO 令牌失败（{}，{token_endpoint}）",
                transport_kind(&e)
            ))
        })?;

    // 非 2xx 也照解：OAuth2 的错误体（error/error_description）在响应体里，交给 T2 的解析器
    // 统一成人话（与 flow.rs 的 exchange_code 同一判据）—— 这里不按状态码再分一套错法。
    let bytes = resp.bytes().await.map_err(|e| {
        Error::Config(format!(
            "读取 SSO 令牌响应失败（{}，{token_endpoint}）",
            transport_kind(&e)
        ))
    })?;
    let fresh = parse_token_response(&bytes, now)?;

    // 合并见函数文档的第四条契约：空串是"本次没给"，沿用旧的。
    let updated = TokenSet {
        access_token: fresh.access_token,
        refresh_token: if fresh.refresh_token.is_empty() {
            old.refresh_token
        } else {
            fresh.refresh_token
        },
        expires_at: fresh.expires_at,
    };
    store.save(&updated)?;
    // 只记"刷过了"与新的到期时刻（绝对秒，不是令牌内容）：守护进程跑几天时，
    // 这是判断刷新链有没有在转的唯一线索。
    tracing::info!(
        expires_at = updated.expires_at,
        "SSO 令牌已过期，刷新成功并写回凭据库"
    );
    Ok(updated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sso::store::MemoryTokenStore;
    use std::sync::mpsc;

    /// 判定基准时刻。测试里的 `expires_at` 全部相对它构造，于是"过期没过期"只由种子决定。
    const NOW: i64 = 1_790_000_000;

    /// 桩服务收到的请求（**真 socket 抓的报文**，不是自己构造的请求对象 —— 要证的恰恰是
    /// 发出去的那一段）。
    struct Seen {
        method: String,
        url: String,
        content_type: Option<String>,
        body: String,
    }

    /// 起一个本地令牌端点桩：按顺序回 `responses` 里的 (状态码, 响应体)，并把每个请求原样记下。
    ///
    /// 排好的响应回完之后**再多守一会儿**才收摊，这样"不该发请求"的测试能拿到一次明确的
    /// `recv_timeout` 超时 —— 桩提前关闭只能证明"桩没了"，证明不了"请求没来"（与 push.rs
    /// 的钉钉桩同一形态）。返回的地址带一段路径，用来证明请求走的是参数给的端点。
    fn token_stub(responses: Vec<(u16, &'static str)>) -> (String, mpsc::Receiver<Seen>) {
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
                // 请求没来（实现回归了）时别把测试挂死：超时就收摊，断言侧会看到超时。
                let Ok(Some(mut req)) = server.recv_timeout(idle) else {
                    return;
                };
                let method = req.method().as_str().to_string();
                let url = req.url().to_string();
                let content_type = req
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("content-type"))
                    .map(|h| h.value.as_str().to_string());
                let mut body = String::new();
                let _ = req.as_reader().read_to_string(&mut body);
                let _ = tx.send(Seen { method, url, content_type, body });
                let (status, resp) = planned
                    .next()
                    .unwrap_or((200, r#"{"access_token":"UNPLANNED","expires_in":1}"#));
                let _ = req.respond(tiny_http::Response::from_string(resp).with_status_code(status));
            }
        });
        (format!("http://127.0.0.1:{port}/stub/token"), rx)
    }

    fn token(access: &str, refresh: &str, expires_at: i64) -> TokenSet {
        TokenSet {
            access_token: access.into(),
            refresh_token: refresh.into(),
            expires_at,
        }
    }

    #[tokio::test]
    async fn a_live_token_is_returned_verbatim_and_nothing_is_sent() {
        // 未过期 = 一个请求都不发（spec §4.1）。桩上备了一份**内容不同**的响应：万一日后有人
        // 把"仅在过期时"改成"每轮都刷"，返回的令牌会变、这条断言先红。
        let (endpoint, seen) = token_stub(vec![(
            200,
            r#"{"access_token":"AT2","refresh_token":"RT2","expires_in":1200}"#,
        )]);
        let store = MemoryTokenStore::default();
        let live = token("AT1", "RT1", NOW + 3600);
        store.save(&live).unwrap();

        let got = refresh_if_needed("client-id", &store, NOW, &endpoint)
            .await
            .unwrap();
        assert_eq!(got, live, "未过期时返回的就是存着的那一份（逐字段相等）");
        assert!(
            seen.recv_timeout(Duration::from_millis(300)).is_err(),
            "未过期还发请求就是违反 §4.1（桩还守着，收到就会进来）"
        );
        assert_eq!(store.load().unwrap().unwrap(), live, "也不该写凭据库");
    }

    #[tokio::test]
    async fn the_60s_early_rule_of_is_expired_is_the_predicate_used_here() {
        // `is_expired` 的边界已在 token.rs 钉过；这条钉的是**本调用点真的用了那个谓词**，
        // 而不是自己写了个"到点才算过期"的比较 —— 提前 60 s 是刻意留的余量。
        let (endpoint, seen) = token_stub(vec![(
            200,
            r#"{"access_token":"AT2","refresh_token":"RT2","expires_in":1200}"#,
        )]);

        // 还剩 61 s：不算过期。
        let store = MemoryTokenStore::default();
        let live = token("AT1", "RT1", NOW + 61);
        store.save(&live).unwrap();
        assert_eq!(
            refresh_if_needed("c", &store, NOW, &endpoint).await.unwrap(),
            live
        );
        assert!(
            seen.recv_timeout(Duration::from_millis(300)).is_err(),
            "还剩 61 s 不该刷新"
        );

        // 正好剩 60 s：按契约算过期（边界含等号）。
        let store2 = MemoryTokenStore::default();
        store2.save(&token("AT1", "RT1", NOW + 60)).unwrap();
        let got = refresh_if_needed("c", &store2, NOW, &endpoint)
            .await
            .unwrap();
        assert_eq!(got.access_token, "AT2", "剩 60 s 就该刷（提前量是刻意的）");
        assert!(
            seen.recv_timeout(Duration::from_secs(10)).is_ok(),
            "这一次必须真的发出去"
        );
    }

    #[tokio::test]
    async fn an_expired_token_is_refreshed_over_the_given_endpoint_and_persisted() {
        // 刷新令牌里带上 `&`/`+`/`=`：任何"手拼表单"的写法都会在这里与 `refresh_body` 的
        // 产物分道扬镳（这三个字符不编码会把表单拆成多余字段）—— 于是下面那条相等断言
        // 就是"用了 T2 的 refresh_body"的证据，而不只是"请求体里恰好有几个关键词"。
        let rt = "RT/1+2&3=4";
        let (endpoint, seen) = token_stub(vec![(
            200,
            r#"{"access_token":"AT2","refresh_token":"RT2","expires_in":1200}"#,
        )]);
        let store = MemoryTokenStore::default();
        store.save(&token("AT1", rt, NOW - 1)).unwrap();

        let got = refresh_if_needed("my-client", &store, NOW, &endpoint)
            .await
            .unwrap();
        assert_eq!(
            (got.access_token.as_str(), got.refresh_token.as_str()),
            ("AT2", "RT2"),
            "两个令牌都来自响应"
        );
        assert_eq!(
            got.expires_at,
            NOW + 1200,
            "到期时刻 = 本次 now + expires_in（不读本机时钟）"
        );

        // 持久化：不是"返回值对"就算数 —— 下一轮从凭据库读出来的必须是刷新后那份，
        // 否则每轮都要重刷，而刷新令牌链会断在第一轮。
        assert_eq!(
            store.load().unwrap().unwrap(),
            got,
            "刷新结果必须写回凭据库（返回值之外还有落盘）"
        );

        let req = seen.recv_timeout(Duration::from_secs(10)).expect("刷新请求没到桩上");
        assert_eq!(req.method, "POST");
        assert_eq!(
            req.url, "/stub/token",
            "打的是参数给的那个端点（写死生产地址的话这个请求根本来不了桩上）"
        );
        assert_eq!(
            req.content_type.as_deref(),
            Some("application/x-www-form-urlencoded")
        );
        assert_eq!(
            req.body,
            refresh_body("my-client", rt),
            "请求体就是 T2 的 refresh_body 产物（含 urlencode）"
        );
        assert!(req.body.contains("grant_type=refresh_token"));
        assert!(req.body.contains("client_id=my-client"));
        assert!(req.body.contains("refresh_token="));
        assert!(
            seen.recv_timeout(Duration::from_millis(300)).is_err(),
            "一次刷新只发一个请求"
        );
    }

    #[tokio::test]
    async fn a_response_without_refresh_token_keeps_the_old_one() {
        // T2 契约：响应缺席 `refresh_token` → 解析器回空串 → **调用方沿用旧值**。
        // 写反了就把一条好用的刷新令牌覆盖成 `""`，此后每次刷新都 invalid_grant。
        let (endpoint, _seen) = token_stub(vec![(200, r#"{"access_token":"AT2","expires_in":1200}"#)]);
        let store = MemoryTokenStore::default();
        store.save(&token("AT1", "RT1", 0)).unwrap();

        let got = refresh_if_needed("c", &store, NOW, &endpoint)
            .await
            .unwrap();
        assert_eq!(got.access_token, "AT2", "access_token 照常换新");
        assert_eq!(
            got.refresh_token, "RT1",
            "响应没给 refresh_token → 沿用旧值，不是空串"
        );
        assert_eq!(
            store.load().unwrap().unwrap(),
            got,
            "落库的那一份：access_token 是新的、refresh_token 仍是旧的（两件事都要看）"
        );
    }

    #[tokio::test]
    async fn no_token_at_all_is_an_error_that_reads_like_not_logged_in() {
        // 调用方的"没令牌 = 跳过"闸门在前面；到这里还没令牌是真错误（如实上抛、主循环 warn
        // 一行），而不是"这一轮没有能跑的回合"。
        let (endpoint, seen) = token_stub(Vec::new());
        let store = MemoryTokenStore::default();

        let e = refresh_if_needed("c", &store, NOW, &endpoint)
            .await
            .unwrap_err();
        assert!(matches!(e, Error::Config(_)), "按契约是配置类错误：{e:?}");
        assert!(e.to_string().contains("未登录"), "文案要读得出'没登录'：{e}");
        assert!(
            seen.recv_timeout(Duration::from_millis(300)).is_err(),
            "连令牌都没有，不该发请求"
        );
    }

    #[tokio::test]
    async fn a_failed_refresh_never_puts_token_material_in_the_error() {
        // 错误串是这条链上唯一会进日志的外泄面（主循环 `warn!` 它）。用"根本不是 URL 的端点"
        // 逼出一条传输层错误：这条路径不碰网络，返回得快，且错误串里的每个字节都由我们自己拼。
        let store = MemoryTokenStore::default();
        store
            .save(&token("SECRET-AT", "SECRET-RT", 0))
            .unwrap();

        let e = refresh_if_needed("c", &store, NOW, "not-a-url")
            .await
            .unwrap_err();
        let msg = e.to_string();
        assert!(!msg.contains("SECRET-AT"), "access_token 不进错误串：{msg}");
        assert!(!msg.contains("SECRET-RT"), "refresh_token 不进错误串：{msg}");
        assert!(!msg.contains("grant_type"), "表单体也不进错误串：{msg}");
    }
}
