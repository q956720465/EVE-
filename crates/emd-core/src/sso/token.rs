//! SSO 令牌交换与刷新的**纯函数**层：只构造请求体、只解析响应。
//! 真正的收发（HTTP、系统时钟、keyring）在调用层（T12 装配）；本文件零 IO —— `now`
//! 是参数，不是 `Instant::now()`，这样过期判定完全可测、无隐藏依赖。
//!
//! 协议常量核对（2026-09-24 对照 <https://developers.eveonline.com/docs/services/sso/>）：
//! - 令牌端点 `https://login.eveonline.com/v2/oauth/token`：已核对（授权码换令牌与刷新共用）。
//! - 换令牌请求体是 `application/x-www-form-urlencoded`，字段为
//!   `grant_type=authorization_code` / `code` / `client_id` / `code_verifier`：已核对。
//! - native 应用无 client secret（spec §4.1）：请求体绝不带 `client_secret`，
//!   持有者身份由 PKCE 的 `code_verifier` 证明。
//! - 刷新请求体（`grant_type=refresh_token` + `refresh_token` + `client_id`）是
//!   RFC 6749 §6 的标准形态，未在官方页逐字核对——真机首刷时留意 400。
//! 若官方值变动，只改本文件。

use crate::error::{Error, Result};

/// 令牌端点。解析报错要带上它，否则"响应解析失败"不知说的是哪个请求。
const TOKEN_ENDPOINT: &str = "https://login.eveonline.com/v2/oauth/token";

/// 令牌三件套。**只在内存与 keyring 里流转，绝不落库**（Global Constraints）。
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TokenSet {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
}

impl TokenSet {
    /// 提前 60 s 视为过期：宁可多刷一次，也不要卡在边界上被 401。
    pub fn is_expired(&self, now: i64) -> bool {
        now + 60 >= self.expires_at
    }
}

/// 授权码换令牌的请求体。每个值单独 urlencode：`code` 与 `verifier` 都来自外部输入，
/// 不编码会让 `&`/`+` 把表单拆成多余字段（`+` 尤其阴险——表单里它解成空格，会静默篡改 verifier）。
pub fn exchange_body(client_id: &str, code: &str, verifier: &str) -> String {
    format!(
        "grant_type=authorization_code&code={}&client_id={}&code_verifier={}",
        urlencoding::encode(code),
        urlencoding::encode(client_id),
        urlencoding::encode(verifier)
    )
}

/// 刷新令牌的请求体。与 `exchange_body` 同样不带 `client_secret`。
pub fn refresh_body(client_id: &str, refresh_token: &str) -> String {
    format!(
        "grant_type=refresh_token&refresh_token={}&client_id={}",
        urlencoding::encode(refresh_token),
        urlencoding::encode(client_id)
    )
}

/// 远端错误文本（`error` / `error_description`）进日志前的净化。
///
/// 这两段是**对端可控**的原文，直接拼进错误串有两个问题：控制字符可以伪造日志行
/// （`error_description` 里塞换行，日志里就多出一行看起来像我们自己打的记录），
/// 超长描述会把上下文淹掉。所以滤掉控制字符（含 CR/LF/制表）并截断到 200 字符 ——
/// 令牌端点的错误体本就不含令牌，这里防的是日志被写坏，不是防泄漏。
///
/// 边界说明：`is_control()` 只覆盖 C0/C1，零宽空格、双向控制符（U+202E 这类）与
/// U+2028/2029 会原样留下。它们不会造出新的日志行，只会让这一行的显示变得古怪；
/// 真要把可见字符也限死，得改成白名单，而白名单会误伤中文描述。按威胁取舍，只挡换行类。
fn sanitize_remote_text(s: &str) -> String {
    const MAX_CHARS: usize = 200;
    let cleaned: Vec<char> = s.chars().filter(|c| !c.is_control()).collect();
    let truncated = cleaned.len() > MAX_CHARS;
    let mut out: String = cleaned.into_iter().take(MAX_CHARS).collect();
    if truncated {
        out.push('…');
    }
    out
}

/// 解析令牌端点响应（成功体与 RFC 6749 错误体都走这里）。
///
/// 三条契约：
/// - `expires_at = now + expires_in`：相对秒在这里换算成绝对时刻，调用方只需比绝对钟。
/// - **`refresh_token` 缺席时返回空串，而不是报错**。EVE 的刷新响应有时只回
///   `access_token`（旧刷新令牌仍然有效），所以空串的含义是"本次响应没给"，**不是**
///   "该令牌已失效"。**调用方负责沿用旧值**：解析层看不到旧值，无权替调用方决定。
/// - 其余缺字段（`access_token` / `expires_in`）与错误体一律报错；报错信息里**只带错误码
///   与净化后的描述**（`sanitize_remote_text`），不夹带响应体原文，令牌串不会经日志或
///   UI 外泄。
pub fn parse_token_response(bytes: &[u8], now: i64) -> Result<TokenSet> {
    let v: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| Error::Parse { url: TOKEN_ENDPOINT.to_string(), source: e })?;

    // 错误体先查：OAuth2 的错误响应本就没有 access_token，若先报"缺字段"，
    // invalid_grant / invalid_client 这类关键诊断会被淹没成一句无用的话。
    if let Some(err) = v.get("error").and_then(serde_json::Value::as_str) {
        let desc = v
            .get("error_description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("（无 error_description）");
        return Err(Error::Config(format!(
            "SSO 令牌端点返回错误 {}：{}",
            sanitize_remote_text(err),
            sanitize_remote_text(desc)
        )));
    }

    let access_token = v
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Config("SSO 令牌响应缺少 access_token".into()))?
        .to_string();

    // 过期时刻必须有出处：缺 expires_in 时宁可报错，也不默认一个值——
    // 默认 0 会让令牌永远"刚过期"（每轮都刷），默认远期则会把过期令牌当有效用。
    let expires_in = v
        .get("expires_in")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| Error::Config("SSO 令牌响应缺少 expires_in，无法算过期时刻".into()))?;

    Ok(TokenSet {
        access_token,
        // 缺席 → 空串，见本函数文档的契约。
        refresh_token: v
            .get("refresh_token")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        expires_at: now + expires_in,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_body_is_form_encoded_and_has_no_secret() {
        let b = exchange_body("abc123", "CODE", "VERIFIER");
        assert!(b.contains("grant_type=authorization_code"));
        assert!(b.contains("code=CODE"));
        assert!(b.contains("client_id=abc123"));
        assert!(b.contains("code_verifier=VERIFIER"));
        assert!(!b.contains("client_secret"), "原生应用无 client secret（spec §4.1）");
    }

    #[test]
    fn refresh_body_uses_refresh_grant() {
        let b = refresh_body("abc123", "RT");
        assert!(b.contains("grant_type=refresh_token"));
        assert!(b.contains("refresh_token=RT"));
        assert!(b.contains("client_id=abc123"));
    }

    #[test]
    fn parse_token_response_computes_expiry_from_expires_in() {
        let json = br#"{"access_token":"AT","refresh_token":"RT","expires_in":1199,"token_type":"Bearer"}"#;
        let t = parse_token_response(json, 1_700_000_000).unwrap();
        assert_eq!((t.access_token.as_str(), t.refresh_token.as_str()), ("AT", "RT"));
        assert_eq!(t.expires_at, 1_700_001_199, "expires_at = now + expires_in");
    }

    #[test]
    fn parse_token_response_rejects_missing_fields() {
        // 刷新响应里 refresh_token 可能缺席（EVE 有时只回 access_token）——
        // 这时保留旧 refresh_token，由调用方决定；解析层只负责报"缺 access_token"。
        let e = parse_token_response(br#"{"token_type":"Bearer"}"#, 1).unwrap_err();
        assert!(e.to_string().contains("access_token"), "{e}");
        // 但 http 错误体（error/error_description）要给出人话
        let e2 = parse_token_response(br#"{"error":"invalid_grant","error_description":"code expired"}"#, 1)
            .unwrap_err();
        assert!(e2.to_string().contains("invalid_grant"), "{e2}");
    }

    #[test]
    fn remote_error_text_is_sanitized_before_it_reaches_the_log() {
        // 远端可控文本防两件事：换行可以伪造日志行，超长会把上下文淹掉。
        // 用 JSON 转义写换行，确认它到不了错误串里。
        let e = parse_token_response(
            br#"{"error":"invalid_grant","error_description":"line1\nline2\r\n[INFO] fake log line"}"#,
            1,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("invalid_grant"), "{e}");
        assert!(!e.contains('\n') && !e.contains('\r'), "控制字符必须被滤掉：{e}");
        assert!(e.contains("line1line2"), "可见字符要保留：{e}");

        let long = "x".repeat(500);
        let body = format!(r#"{{"error":"invalid_client","error_description":"{long}"}}"#);
        let e = parse_token_response(body.as_bytes(), 1).unwrap_err().to_string();
        assert!(e.contains('…'), "超长描述要截断并标记：{}", &e[..e.len().min(80)]);
        assert!(e.chars().count() < 300, "截断后不该还很长：{}", e.chars().count());
    }

    #[test]
    fn parse_token_response_treats_absent_refresh_token_as_empty() {
        let t = parse_token_response(br#"{"access_token":"AT2","expires_in":1200}"#, 1_700_000_000)
            .unwrap();
        assert_eq!(t.access_token, "AT2");
        assert_eq!(t.refresh_token, "", "缺 refresh_token 时返回空串，由调用方沿用旧值");
        assert_eq!(t.expires_at, 1_700_001_200);
    }

    #[test]
    fn is_expired_flags_the_boundary_60s_early() {
        let t = TokenSet { access_token: "AT".into(), refresh_token: "RT".into(), expires_at: 1_000 };
        assert!(!t.is_expired(939), "离过期还有 61 s：仍可用");
        assert!(t.is_expired(940), "离过期还有 60 s：按契约提前视为过期（边界含等号）");
        assert!(t.is_expired(1_001));
    }
}
