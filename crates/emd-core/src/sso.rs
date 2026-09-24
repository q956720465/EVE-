//! EVE SSO 的 PKCE 纯逻辑（RFC 7636 + spec §4.1）。
//! 本模块零 IO：不发网络、不开浏览器、不读环境。编排在 `sso::flow`。
//!
//! 协议常量核对（2026-09-24 对照 <https://developers.eveonline.com/docs/services/sso/>，
//! 见计划「外部协议常量」节）：
//! - 授权端点 `https://login.eveonline.com/v2/oauth/authorize`：已核对。
//! - 令牌端点 `https://login.eveonline.com/v2/oauth/token`：已核对；请求体字段与
//!   native 无 secret 的说明见子模块 [`token`]（出处与日期在该文件顶部）。
//! - PKCE `code_challenge_method=S256` 官方支持；native 应用必须先在开发者后台注册
//!   redirect_uri（回环地址同样要注册，否则授权页直接报错）：已核对。
//! - **待核**：scope 精确串。本模块不持有 scope 常量（由调用方传入，见 T12），
//!   需注册应用后真机试授权才能确认无 `invalid_scope`。
//! 若官方值变动，只改本文件与 [`token`] 两处（各持自己的端点常量）。

pub mod token;

use base64::Engine;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// PKCE 的 verifier 与 S256 challenge。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

impl Pkce {
    /// 32 字节熵 → base64url（无 padding）= 43 字符，落在 RFC 7636 的 43–128 区间。
    pub fn from_entropy(bytes: &[u8; 32]) -> Self {
        let verifier = b64url(bytes);
        Self::from_verifier(&verifier)
    }

    pub fn random() -> Self {
        let mut buf = [0u8; 32];
        getrandom::getrandom(&mut buf).expect("系统熵源不可用");
        Self::from_entropy(&buf)
    }

    /// challenge = BASE64URL(SHA256(ASCII(verifier)))，无 padding。
    pub fn from_verifier(verifier: &str) -> Self {
        let digest = Sha256::digest(verifier.as_bytes());
        Self { verifier: verifier.to_string(), challenge: b64url(&digest) }
    }

    pub fn state() -> String {
        let mut buf = [0u8; 16];
        getrandom::getrandom(&mut buf).expect("系统熵源不可用");
        b64url(&buf)
    }
}

fn b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// 授权 URL。**scope 先排序再拼**：EVE 侧不要求顺序，但排序让 URL 可复现、便于断言与排障。
pub fn authorize_url(
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    challenge: &str,
    scopes: &[&str],
) -> String {
    let mut sorted: Vec<&str> = scopes.to_vec();
    sorted.sort_unstable();
    let enc = |s: &str| urlencoding::encode(s).into_owned();
    format!(
        "https://login.eveonline.com/v2/oauth/authorize?response_type=code&redirect_uri={}\
&client_id={}&scope={}&state={}&code_challenge={}&code_challenge_method=S256",
        enc(redirect_uri),
        enc(client_id),
        enc(&sorted.join(" ")),
        enc(state),
        enc(challenge),
    )
}

/// 校验回调并取出 code。`state` 不符 = CSRF，一律拒绝（spec §4.1 的 PKCE 完整性依赖它）。
pub fn verify_callback(query: &str, expected_state: &str) -> Result<String> {
    let mut code = None;
    let mut state = None;
    let mut err = None;
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let v = urlencoding::decode(v).map(|c| c.into_owned()).unwrap_or_else(|_| v.to_string());
        match k {
            "code" => code = Some(v),
            "state" => state = Some(v),
            "error" => err = Some(v),
            _ => {}
        }
    }
    if let Some(e) = err {
        return Err(Error::Config(format!("SSO 授权被拒：{e}")));
    }
    match state.as_deref() {
        Some(s) if s == expected_state => {}
        Some(_) => return Err(Error::Config("SSO state 不匹配（疑似 CSRF），已丢弃本次回调".into())),
        None => return Err(Error::Config("SSO 回调缺少 state".into())),
    }
    code.ok_or_else(|| Error::Config("SSO 回调缺少 code".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7636 附录 B 的官方向量：verifier → S256 challenge 必须逐字符一致。
    /// 拿官方向量当锚点，是为了让"base64url 不带 padding"这类细节有铁证。
    #[test]
    fn s256_challenge_matches_rfc7636_appendix_b() {
        let v = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let p = Pkce::from_verifier(v);
        assert_eq!(p.verifier, v);
        assert_eq!(p.challenge, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
    }

    #[test]
    fn verifier_is_43_chars_of_unreserved_alphabet() {
        let p = Pkce::random();
        assert_eq!(p.verifier.len(), 43, "32 字节 base64url 无 padding = 43 字符");
        assert!(p.verifier.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        assert_ne!(Pkce::random().verifier, p.verifier, "两次随机不能撞");
    }

    #[test]
    fn authorize_url_carries_all_required_params() {
        let url = authorize_url(
            "abc123",
            "http://127.0.0.1:8765/callback",
            "st-1",
            "CHALLENGE",
            &["esi-wallet.read_character_wallet.v1", "esi-markets.read_character_orders.v1"],
        );
        assert!(url.starts_with("https://login.eveonline.com/v2/oauth/authorize?"), "{url}");
        assert!(url.contains("response_type=code"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("client_id=abc123"));
        assert!(url.contains("state=st-1"));
        assert!(url.contains("code_challenge=CHALLENGE"));
        // scope 用空格分隔，空格必须被编码成 %20（不能是 +，EVE 侧按 %20 解）
        assert!(url.contains("scope=esi-markets.read_character_orders.v1%20esi-wallet.read_character_wallet.v1"),
            "scope 排序稳定且用 %20 分隔：{url}");
    }

    #[test]
    fn callback_parses_code_and_rejects_state_mismatch() {
        let ok = verify_callback("code=THE_CODE&state=st-1", "st-1").unwrap();
        assert_eq!(ok, "THE_CODE");
        // state 不符 = CSRF，必须拒绝
        let e = verify_callback("code=THE_CODE&state=evil", "st-1").unwrap_err();
        assert!(e.to_string().contains("state"), "{e}");
        // 用户点了拒绝授权
        let e2 = verify_callback("error=access_denied&state=st-1", "st-1").unwrap_err();
        assert!(e2.to_string().contains("access_denied"), "{e2}");
        // 缺 code
        assert!(verify_callback("state=st-1", "st-1").is_err());
    }
}
