//! SSO 登录**编排**：起回环监听 → 开系统浏览器 → 收 code → 换令牌 → 解析角色 → 落 keyring。
//!
//! 本文件是这条链上唯一的 IO 汇集点（网络、浏览器、系统时钟都在这里），
//! 各步的纯逻辑留在 [`crate::sso`]（PKCE/URL/回调校验）与 [`crate::sso::token`]（请求体/响应解析）。
//!
//! 协议常量核对（2026-09-24 对照 <https://developers.eveonline.com/docs/services/sso/>）：
//! - 令牌端点 `https://login.eveonline.com/v2/oauth/token`：已核对（出处与 [`crate::sso::token`] 同页）。
//!   **它不在 ESI 主机上**（ESI 是 `esi.evetech.net`），所以本文件不能借 `EsiClient`
//!   ——它的 `absolutize` 会把相对路径拼到 ESI base_url 上，令牌请求会打到错的主机。
//! - 本文件与 [`crate::sso::token`] 各持一份端点字面量：两处都写明了出处，
//!   而让 `token.rs` 的私有常量升级为 `pub(crate)`（T2 明确不放开）只会多一条耦合。
//! - **待核**：`SCOPES` 的精确串（见该常量注释）——无 client_id 无法真机试授权。
//!
//! 脱敏纪律（Global Constraints）：`TokenSet`、`jwt`、`access_token`、授权 `code`、
//! `code_verifier`、`state` 一律不进日志；本文件只记主机/路径与端口。

use std::time::Duration;

use base64::Engine;

use crate::error::{Error, Result};
use crate::sso::listen::CallbackListener;
use crate::sso::store::TokenStore;
use crate::sso::token::{exchange_body, parse_token_response, TokenSet};
use crate::sso::{authorize_url, Pkce};

/// 令牌端点（授权码换令牌）。与 ESI 不同源，故不走 `EsiClient`。
const TOKEN_ENDPOINT: &str = "https://login.eveonline.com/v2/oauth/token";

/// 登录默认申请的 scope。
///
/// **待核**：精确串要注册开发者应用后逐条试授权（无 `invalid_scope` 才算对），
/// 出处同 <https://developers.eveonline.com/docs/services/sso/> 的 scope 列表（JS 渲染，抓不到正文）。
/// 三条都按"角色市场订单 / 角色钱包（transactions 与 journal 同 scope）/ 角色技能"的官方名称给出。
/// 若真机报 `invalid_scope`：只改这一处（`login` 是唯一使用者）。
const SCOPES: &[&str] = &[
    "esi-markets.read_character_orders.v1",
    "esi-wallet.read_character_wallet.v1",
    "esi-skills.read_skills.v1",
];

/// 令牌端点的单次超时。登录是用户手点的动作，慢到十几秒已经说明网络/官方侧有问题，
/// 让用户看着转圈不如直接报错重来。
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// 登录成功的结果。**不含令牌**：令牌只进 `TokenStore`，UI 只需要知道"是谁"。
pub struct LoginOutcome {
    pub char_id: u64,
    pub name: String,
}

/// 从 access_token（JWT）里解析出角色 id 与名字。纯函数：只解码、不验签、不打网络。
///
/// **不验签**是刻意的：令牌是本进程刚从官方端点直接换回来的（TLS 直连，没有中间人），
/// 这里的职责是"取出身份"，不是"信任一个外部令牌"；验签需要 JWKS 与额外依赖，无收益。
///
/// `sub` 只认 `CHARACTER:EVE:<数字>`。EVE 也会签出别的 subject（如账户级的 `USER:...`），
/// 那类令牌**取不到角色 id**——宁可直接失败，也不能猜一个出来把角色数据挂错人。
pub fn char_from_access_token(jwt: &str) -> Result<(u64, String)> {
    // JWT = header.payload.signature，payload 在第二段。段数不符说明拿到的根本不是 JWT
    // （或结构变了），此时任何"尽力解析"都只会产出错的 id。
    let segments: Vec<&str> = jwt.split('.').collect();
    if segments.len() != 3 {
        return Err(Error::Config(format!(
            "access_token 不是三段式 JWT（实际 {} 段），无法解析角色",
            segments.len()
        )));
    }

    // base64url 无 padding（RFC 7515 的 JWT 段编码）。报错信息只带解码器的位置信息，
    // 不回显令牌原文。
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(segments[1])
        .map_err(|e| Error::Config(format!("access_token 的 payload 段不是合法 base64url：{e}")))?;
    let v: serde_json::Value = serde_json::from_slice(&payload)
        .map_err(|e| Error::Config(format!("access_token 的 payload 不是合法 JSON：{e}")))?;

    let sub = v
        .get("sub")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Config("access_token 缺 sub，无法确定角色".into()))?;
    // name 同样不能缺：角色名要进 UI 与告警卡片，用空串代替会让用户看到"匿名角色"。
    let name = v
        .get("name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Config("access_token 缺 name，无法确定角色名".into()))?;

    // 逐字符校验而非直接 parse：`u64::from_str` 接受前导 `+`，那不该算合法 subject。
    let digits = sub
        .strip_prefix("CHARACTER:EVE:")
        .filter(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
        .ok_or_else(|| Error::Config("access_token 的 sub 不是 CHARACTER:EVE:<数字>，拒绝解析".into()))?;
    let char_id = digits
        .parse::<u64>()
        .map_err(|e| Error::Config(format!("access_token 的角色 id 超出 u64 范围：{e}")))?;

    Ok((char_id, name.to_string()))
}

/// 完整登录流程：返回角色身份，令牌已落 `store`。
///
/// 三个参数由调用方给定而不是从配置结构体里取：`redirect_uri` 必须与开发者后台注册值
/// **逐字符一致**（EVE 是精确匹配），`client_id` 与端口同理，都不能由本文件代猜。
/// `port` 必须就是 `redirect_uri` 里的那个端口——本函数只绑这一个端口，
/// 两者不一致时用户要等到超时才知情。
///
/// `timeout` 是等待回调的整次上限（默认建议 180 s，由调用方定）。
/// 无论成功、state 不符还是超时，回环端口都在本函数返回前释放——监听器被移进阻塞任务，
/// 任务一结束就随闭包一起析构，不留给下一次登录一个占着的端口。
pub async fn login(
    client_id: &str,
    redirect_uri: &str,
    port: u16,
    store: &dyn TokenStore,
    timeout: Duration,
) -> Result<LoginOutcome> {
    let pkce = Pkce::random();
    let state = Pkce::state();

    // 先绑端口再开浏览器：端口被占时立刻失败，而不是让用户白点一遍授权。
    let listener = CallbackListener::bind(port)?;
    let url = authorize_url(client_id, redirect_uri, &state, &pkce.challenge, SCOPES);

    // 只记主机/路径：URL 上带着 client_id 与 state（state 是本次登录的完整性凭据）。
    tracing::info!(
        "打开系统浏览器等待 EVE SSO 授权（login.eveonline.com/v2/oauth/authorize，回调端口 {port}）"
    );
    open_browser(&url)?;

    // 收回调是阻塞的（`tiny_http::recv_timeout`），挪进阻塞线程池，别占住 async 执行器。
    let code = tokio::task::spawn_blocking(move || listener.wait_for_code(timeout, &state))
        .await
        .map_err(|e| Error::Config(format!("SSO 登录等待任务异常终止：{e}")))??;

    let token = exchange_code(client_id, &code, &pkce.verifier).await?;

    // 先解析角色再落库：解析失败说明拿到一个无法归属的令牌，
    // 写进凭据库只会让下次启动拿着它做无名同步，不如让本次登录整体失败。
    let (char_id, name) = char_from_access_token(&token.access_token)?;
    store.save(&token)?;
    Ok(LoginOutcome { char_id, name })
}

/// 授权码换令牌。
///
/// 自带一个短超时的 `reqwest::Client`：**不进 ESI 的预算/节流体系**（登录一次 1–2 个请求，
/// 用户手点触发，混进 15 分钟窗口预算里只会让正常采集被它挤掉）。
async fn exchange_code(client_id: &str, code: &str, verifier: &str) -> Result<TokenSet> {
    let http = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|e| Error::Config(format!("构建 SSO HTTP 客户端失败：{e}")))?;

    let resp = http
        .post(TOKEN_ENDPOINT)
        .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(exchange_body(client_id, code, verifier))
        .send()
        .await
        .map_err(|e| Error::Transport { url: TOKEN_ENDPOINT.to_string(), source: e })?;

    // 非 2xx 也照解：OAuth2 的错误体（error/error_description）在响应体里，
    // 交给 T2 的解析器统一成人话，比在这里按状态码再分一套错法清楚。
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| Error::Transport { url: TOKEN_ENDPOINT.to_string(), source: e })?;
    // `now` 只在这里取真实时钟：纯函数层（T2）要求它由调用方传入。
    parse_token_response(&bytes, chrono::Utc::now().timestamp())
}

/// 用系统默认浏览器打开授权页。
///
/// 只做 Windows（项目其余部分同样只支持 Windows，见 `emd-app` 的 crate-type 注释），
/// 因此不引 opener 类依赖：`start` 是 cmd 内建命令，必须经 `cmd /C`；
/// 第三个参数的空串是 `start` 的窗口标题占位——不给它，URL 会被当成标题而不打开。
/// 不等待子进程：cmd 转手给 `start` 后立刻退出，等它就是白等一次进程创建。
fn open_browser(url: &str) -> Result<()> {
    std::process::Command::new("cmd")
        .args(["/C", "start", "", url])
        .spawn()
        .map_err(|e| Error::Config(format!("无法打开系统浏览器完成 SSO 授权：{e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 与 [sso.rs](../sso.rs) 里的小工具同源：base64url 无 padding（JWT 段就是这么编的）。
    fn b64url(bytes: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    /// JWT 的 sub 形如 "CHARACTER:EVE:2112625428"，name 是角色名。
    /// 用合成的 JWT（只有 payload 段是真 base64url）测纯解析，不打网络。
    #[test]
    fn char_from_access_token_reads_sub_and_name() {
        let payload = r#"{"sub":"CHARACTER:EVE:2112625428","name":"Test Pilot","exp":1}"#;
        let jwt = format!("eyJhbGciOiJub25lIn0.{}.sig", b64url(payload.as_bytes()));
        let (id, name) = char_from_access_token(&jwt).unwrap();
        assert_eq!(id, 2_112_625_428);
        assert_eq!(name, "Test Pilot");
    }

    #[test]
    fn char_from_access_token_rejects_non_character_subject() {
        // sub 不是 CHARACTER:EVE:<数字> 时必须报错，不能瞎猜一个 id 出来
        let payload = r#"{"sub":"USER:123","name":"x"}"#;
        let jwt = format!("eyJhbGciOiJub25lIn0.{}.sig", b64url(payload.as_bytes()));
        assert!(char_from_access_token(&jwt).is_err());
        assert!(char_from_access_token("not-a-jwt").is_err());
        assert!(char_from_access_token("a.b").is_err());
    }
}
