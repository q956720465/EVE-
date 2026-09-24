# M4c 角色挂链与亏损提醒实现计划（SSO + 同步管线 + 三形态判定 + 钉钉私有卡片）

> **For agentic workers:** 本环境无编码子代理（已在 M3/M4b 验证），采用**会话内 TDD + CodeReview 子代理评审**执行。步骤用 `- [ ]` 勾选跟踪。
> 依据：`docs/superpowers/specs/2026-09-24-m4-flip-engine-design.md` §4（已设计锁定）+ 方案 v3.1 §10（推送，通道按本计划偏离说明改为钉钉）。与 spec 冲突时以 spec 为准，除本计划显式记录的偏离。

**Goal:** 落地 M4c——EVE SSO 角色挂链（PKCE + keyring）、跟 T1 节拍的角色数据同步、三形态负收益判定、告警状态机、本地提醒中心，以及钉钉私有卡片推送（最小发送能力）。

**Architecture:** 新模块 `emd-core::sso`（PKCE/令牌/存储）、`emd-core::char`（同步管线 + FIFO 成本）、`emd-core::alert`（判定纯函数 + 状态机 + 落库）、`emd-core::push`（通道抽象 + 钉钉加签）。迁移 v6 落 `char_meta`/`char_tx`/`char_orders`/`alerts` 四表。判定与卡片渲染是纯函数；IO 只在 `char::sync_character`、`alert::update_round`、`push::*` 的发送器。

**Tech Stack:** Rust（workspace 三 crate）+ rusqlite + serde + reqwest；新增 hmac/sha2/base64/urlencoding/getrandom/keyring/tiny_http；Vite/React/TS/Zustand。

**Spec:** `docs/superpowers/specs/2026-09-24-m4-flip-engine-design.md` §4

---

## 偏离 spec / 方案 v3.1 的记录（本计划显式偏离，执行时不再回改文档）

| # | 原文 | 本计划 | 理由 |
|---|---|---|---|
| D1 | §4.5 / §10 推送通道为**飞书** webhook + 卡片 schema 1.0 | 改为**钉钉**群自定义机器人 webhook + `markdown` 消息 | 用户 2026-09-24 明确改定（见记忆"推送通道决策：钉钉替代飞书"）。签名字段、卡片结构、错误码、频控全部按钉钉口径 |
| D2 | §10.2 推送基建（push_queue 幂等 / 90 s 合并窗口 / 每小时 12 / 每日 200 / 静默时段 / 多通道 / schema 探测）属 M6 | M4c **只做最小发送能力**：通道抽象 + 钉钉加签 + 告警状态机 + 本地提醒中心。完整 push 基建留 M6 | 用户选定"最小私有卡片发送"；M4c 的告警限额（§4.4 每日 ≤5 条）独立于 M6 全局限额 |
| D3 | §4.1 "系统浏览器跳转 + loopback 回调收 code" | loopback 端口**固定 8765**（可配），因 EVE 开发者应用要求 redirect_uri 精确匹配 | 用户尚未注册 client_id；固定端口让注册的 redirect_uri 稳定可复现 |
| D4 | 方案 §8 里程碑把"倒挂扫描 MISPRICE_SNIPER"列为 M4.5 | **不属 M4c**，本计划不实现 | spec §4 未含倒挂；M4.5 是独立里程碑 |

## 外部协议常量（★实现前必须对照官方文档核对，勿凭本计划直接写死）

本环境抓不到钉钉/ESI 官方文档正文（JS 渲染 / 需登录），以下常量按既有认知给出，**Task 1 与 Task 10 的第一步都是核对它们**，核对结果写进代码注释的"出处"行。

**钉钉自定义机器人（待核）**
- webhook：`https://oapi.dingtalk.com/robot/send?access_token={token}`
- 加签：`timestamp` 为**毫秒**；`stringToSign = "{timestamp}\n{secret}"`；`sign = urlencode(base64(hmac_sha256(key=secret, msg=stringToSign)))`；`timestamp` 与 `sign` 作为 **URL 查询参数**追加
- 加签密钥（secret）以 `SEC` 开头
- 成功响应：`{"errcode":0,"errmsg":"ok"}`
- markdown 消息体：`{"msgtype":"markdown","markdown":{"title":"...","text":"..."}}`
- 频控：约 20 条/分钟
- 错误码：`300001` 频率超限 / `310000` 关键词不匹配或加签失败 / `400013` 机器人已停用
- 核对方式：`curl` 打官方文档 + 用未加签/加签两种请求实测返回码；核对结论写进 `push/dingtalk.rs` 顶部注释

**EVE SSO（待核）**
- 授权端点：`https://login.eveonline.com/v2/oauth/authorize`
- 令牌端点：`https://login.eveonline.com/v2/oauth/token`
- PKCE：`code_challenge_method=S256`
- 授权请求体（URL 查询参数）：`response_type=code`、`redirect_uri`、`client_id`、`scope`（空格分隔）、`state`、`code_challenge`、`code_challenge_method`
- 换令牌请求体（form）：`grant_type=authorization_code`、`code`、`client_id`、`code_verifier`
- 刷新请求体：`grant_type=refresh_token`、`refresh_token`、`client_id`
- 响应：`access_token`/`refresh_token`/`expires_in`/`token_type`
- scope（待核精确串）：角色市场订单 `esi-markets.read_character_orders.v1`、角色钱包 `esi-wallet.read_character_wallet.v1`（transactions 与 journal 同 scope）、角色技能 `esi-skills.read_skills.v1`
- 核对方式：注册开发者应用后逐 scope 试授权，确认无 `invalid_scope`；核对结论写进 `sso.rs` 顶部注释

## Global Constraints（硬约束，每个 Task 都适用）

- **纯函数边界**：`sso::Pkce`/`authorize_url`/`verify_callback`/`parse_token_response`、`char::fifo_costs`、`alert::detect`/`tick_alert`/`can_push`、`push::dingtalk::sign`/`render_markdown`/`map_errcode` **零 IO**（无 DB、无网络、无系统时钟——`now` 一律作参数传入）。IO 只在 `sso::*` 的收发函数、`char::sync_character`、`alert::update_round`、`push` 的发送器。
- **令牌不入库**：`refresh_token`/`access_token` 只进 keyring 与内存，**任何表都不落令牌**；日志与错误串必须脱敏（webhook token、secret、access_token 一律打码）。
- **判定轨道分离**：预期轨用 `FeeModel`（技能面板改动只影响预期轨）；已实现轨用 journal 真值，**不受技能面板影响**。
- **`AlertPayload` 单一序列化出口**：推送与提醒中心共用同一结构，杜绝双源漂移。
- **`state` 落库用 snake_case 字符串**，枚举与字符串互转集中在 `AlertState`（照 `OppState` 先例）。
- **有界表**：新表主键不得含时间戳；`char_tx`/`char_orders` 按 `char_id` 裁剪保留期。
- SSO 与钉钉渠道**默认关闭**，用户显式配置后才启用；`EMD_CHAR_SYNC=0` 可关角色同步。
- `cargo test -p emd-core -p emd-daemon -p emd-app` 全绿才提交；每个 Task 一个提交。
- 注释用中文，风格对齐现有模块（写"为什么"而非"做了什么"）。
- **无 client_id / 无钉钉机器人**：所有涉及真实网络与凭证的验收一律挂账，用 fixture + 单测覆盖；不得伪造"已实测"结论。

---

## 文件结构（先定边界，任务分解据此）

**emd-core 新增**
| 文件 | 职责 |
|---|---|
| `src/sso.rs` | PKCE 纯逻辑 + 授权 URL + 回调校验（零 IO） |
| `src/sso/token.rs` | 令牌交换/刷新的**请求体构造与响应解析**（纯函数）+ 收发（IO） |
| `src/sso/store.rs` | `TokenStore` trait + `MemoryTokenStore` + `KeyringTokenStore` |
| `src/sso/flow.rs` | 端到端编排：起 loopback → 开浏览器 → 收 code → 换令牌 → 存 keyring |
| `src/char.rs` | 角色同步管线（4 端点）+ `CharSyncReport` |
| `src/char/fifo.rs` | FIFO 成本基准（90 天重放，纯函数） |
| `src/alert.rs` | 三形态判定纯函数 + `AlertPayload` + `CaliberSummary` |
| `src/alert/state.rs` | 告警状态机 + `AlertRecord` + 闸门 |
| `src/push.rs` | `PushChannel` trait + `PushOutcome` + 本地提醒中心实现 |
| `src/push/dingtalk.rs` | 钉钉加签 + markdown 渲染 + 错误码映射（纯函数）+ 发送器 |
| `src/store/char_db.rs` | 角色/告警读写层（照 `store/db.rs` 的 M4b 段先例，独立成文件避免 db.rs 继续膨胀） |

**emd-core 修改**
- `src/store/schema.rs`：迁移 v6
- `src/esi/client.rs`：Bearer 注入（`fetch_auth`/`get_json_auth`）
- `src/config.rs`：`CharConfig`（client_id / redirect_uri / loopback 端口 / 同步开关）
- `src/lib.rs`、`src/store/mod.rs`：导出

**emd-daemon 修改**
- `src/main.rs`：`alerts` / `char` 子命令

**emd-app + web**
- `crates/emd-app/src/lib.rs`：SSO 登录、通道配置、提醒列表命令
- `web/src/components/AlertCenter.tsx`（新）、`web/src/api.ts`、`web/src/types.ts`、`web/src/App.tsx`、`web/src/styles.css`

---

### Task 1: `sso.rs` — PKCE 纯逻辑与授权 URL

**Files:**
- Create: `crates/emd-core/src/sso.rs`
- Modify: `crates/emd-core/src/lib.rs`

**Interfaces:**
- Produces: `Pkce { verifier, challenge }`、`Pkce::from_entropy(&[u8;32])`、`Pkce::random()`、`authorize_url(...) -> String`、`verify_callback(query, expected_state) -> Result<String>`

- [ ] **Step 0: 核对协议常量**

用 `WebFetch` 打 EVE 官方 SSO 文档（`https://developers.eveonline.com/docs/services/sso/`）与 ESI scope 文档，逐条确认"外部协议常量"节的 EVE 部分；把核对结论写进 `sso.rs` 顶部注释（含核对日期与出处 URL）。**核对不通过就停下来报告，不要按猜测继续。**

- [ ] **Step 1: 写失败测试**（`sso.rs` 底部 `#[cfg(test)] mod tests`）

```rust
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
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p emd-core sso 2>&1 | tail -20`
Expected: 编译失败（`Pkce` 不存在）

- [ ] **Step 3: 实现**

```rust
//! EVE SSO 的 PKCE 纯逻辑（RFC 7636 + spec §4.1）。
//! 本模块零 IO：不发网络、不开浏览器、不读环境。编排在 `sso::flow`。
//!
//! 协议常量核对：<核对日期> 对照 <出处 URL>（见计划"外部协议常量"节）。
//! 若官方值变动，只改本文件与 `sso/token.rs` 的常量区。

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
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p emd-core sso 2>&1 | tail -20`
Expected: 4 例全绿

- [ ] **Step 5: 加依赖并提交**

```bash
# crates/emd-core/Cargo.toml 与 workspace Cargo.toml 的 [workspace.dependencies] 同步加：
#   base64 = "0.22" / sha2 = "0.10" / urlencoding = "2" / getrandom = "0.2"
git add Cargo.toml crates/emd-core/Cargo.toml crates/emd-core/src/sso.rs crates/emd-core/src/lib.rs
git commit -m "feat(core): SSO PKCE 纯逻辑（RFC7636 S256 + 授权 URL + state 校验，零 IO）"
```

---

### Task 2: `sso/token.rs` — 令牌交换与刷新

**Files:** Create: `crates/emd-core/src/sso/token.rs`；Modify: `crates/emd-core/src/sso.rs`（`pub mod token;`）

**Interfaces:**
- Consumes: `Error`/`Result`
- Produces: `TokenSet { access_token, refresh_token, expires_at }`、`exchange_body(client_id, code, verifier) -> String`、`refresh_body(client_id, refresh_token) -> String`、`parse_token_response(&[u8], now) -> Result<TokenSet>`

- [ ] **Step 1: 写失败测试**

```rust
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
```

- [ ] **Step 2: 跑测试确认失败** → `cargo test -p emd-core token` 编译失败

- [ ] **Step 3: 实现**（要点）

```rust
/// 令牌三件套。**只在内存与 keyring 里流转，绝不落库**（Global Constraints）。
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TokenSet {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
}

impl TokenSet {
    /// 提前 60 s 视为过期：宁可多刷一次，也不要卡在边界上被 401。
    pub fn is_expired(&self, now: i64) -> bool { now + 60 >= self.expires_at }
}

pub fn exchange_body(client_id: &str, code: &str, verifier: &str) -> String {
    format!(
        "grant_type=authorization_code&code={}&client_id={}&code_verifier={}",
        urlencoding::encode(code), urlencoding::encode(client_id), urlencoding::encode(verifier)
    )
}

pub fn refresh_body(client_id: &str, refresh_token: &str) -> String {
    format!(
        "grant_type=refresh_token&refresh_token={}&client_id={}",
        urlencoding::encode(refresh_token), urlencoding::encode(client_id)
    )
}

pub fn parse_token_response(bytes: &[u8], now: i64) -> Result<TokenSet> { /* 见测试的三条分支 */ }
```

`refresh_token` 缺席时的策略：解析层返回 `refresh_token: String::new()`，调用方沿用旧值——**在 `parse_token_response` 的文档注释里写明这条契约**，并加一例测试断言空串行为。

- [ ] **Step 4: 跑测试确认通过** → 4 例 + 空 refresh_token 例全绿

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/sso/token.rs crates/emd-core/src/sso.rs
git commit -m "feat(core): SSO 令牌交换与刷新的请求体构造/响应解析（纯函数，无 client secret）"
```

---

### Task 3: `sso/store.rs` — 令牌存储抽象与 keyring

**Files:** Create: `crates/emd-core/src/sso/store.rs`；Modify: `crates/emd-core/src/sso.rs`、`crates/emd-core/Cargo.toml`（加 `keyring`）

**Interfaces:**
- Produces: `trait TokenStore`（`load`/`save`/`clear`）、`MemoryTokenStore`、`KeyringTokenStore::new(service, account)`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn memory_store_roundtrips_and_clears() {
    let s = MemoryTokenStore::default();
    assert!(s.load().unwrap().is_none());
    let t = TokenSet { access_token: "AT".into(), refresh_token: "RT".into(), expires_at: 9 };
    s.save(&t).unwrap();
    assert_eq!(s.load().unwrap().unwrap(), t);
    s.clear().unwrap();
    assert!(s.load().unwrap().is_none(), "退出登录必须真的清掉");
}

/// keyring 的行为按"抽象契约"测，不打真实 Windows 凭据库
/// ——CI/无桌面环境没有凭据服务，打真库会让测试变脆。
#[test]
fn keyring_store_degrades_to_none_when_backend_is_unavailable() {
    // 用不存在的服务名构造，load 应回 Ok(None) 而不是 panic/Err
    // （凭据库不可用 = 没登录过，不是致命错误）
    let s = KeyringTokenStore::new("EveMarketDeskTest__nope__", "prod");
    match s.load() {
        Ok(None) => {}
        Ok(Some(_)) => panic!("不该读到令牌"),
        Err(e) => panic!("凭据库不可用应降级为 None，实际报错：{e}"),
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**

```rust
//! 令牌存储。`refresh_token` 只进这里（keyring）与内存，DB 不落（Global Constraints）。
//! trait 是为了让测试与"用户收回私有数据豁免后回退本地"的场景能替换实现。

pub trait TokenStore: Send + Sync {
    fn load(&self) -> Result<Option<TokenSet>>;
    fn save(&self, t: &TokenSet) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

#[derive(Default)]
pub struct MemoryTokenStore(std::sync::Mutex<Option<TokenSet>>);

pub struct KeyringTokenStore { service: String, account: String }
```

`KeyringTokenStore::load` 把 keyring 的"条目不存在"与"后端不可用"都映射成 `Ok(None)` 并 `tracing::debug!` —— **凭据库读不到不等于出错**，这是本轮最容易写成 Err 的地方。序列化用 `serde_json` 存整个 `TokenSet`。

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
# keyring 用 default-features = false 只开 windows-native，避免拖入 linux secret-service 依赖链
git add Cargo.toml crates/emd-core/Cargo.toml crates/emd-core/src/sso/store.rs crates/emd-core/src/sso.rs
git commit -m "feat(core): 令牌存储抽象（keyring/内存双实现，凭据库不可用降级为未登录）"
```

---

### Task 3B: `sso/flow.rs` — SSO 登录编排（loopback + 浏览器 + 令牌交换）

> **插入说明（控制器裁决）：** 原计划的「文件结构」节列出了 `src/sso/flow.rs`，但 15 个 Task 里没有任何一个拥有它——登录流程（起 loopback、开浏览器、换令牌、落 keyring）成了无主代码。这是计划缺口，不是可选项：没有它，T13/T14 的登录入口无处可调。原 Task 4-15 编号不变。

**Files:**
- Create: `crates/emd-core/src/sso/flow.rs`
- Create: `crates/emd-core/src/sso/listen.rs`
- Modify: `crates/emd-core/src/sso.rs`（`pub mod flow; pub mod listen;`）、`crates/emd-core/Cargo.toml`（加 `tiny_http`）

**Interfaces:**
- Consumes: `Pkce`/`authorize_url`/`verify_callback`（T1）、`exchange_body`/`parse_token_response`/`TokenSet`（T2）、`TokenStore`（T3）
- Produces: `LoginOutcome { char_id: u64, name: String }`、`char_from_access_token(jwt) -> Result<(u64, String)>`（纯函数）、`CallbackListener::bind(port) -> Result<Self>`、`CallbackListener::wait_for_code(timeout, expected_state) -> Result<String>`、`login(cfg, store) -> Result<LoginOutcome>`

- [ ] **Step 1: 写失败测试**

```rust
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

/// loopback 监听器：真起一个本地端口，自己发一个 GET 过去，断言能捞出 code。
/// 用 0 端口让 OS 分配，避免测试间抢端口。
#[test]
fn listener_extracts_code_from_a_real_local_request() {
    let l = CallbackListener::bind(0).unwrap();
    let addr = l.local_addr().unwrap();
    std::thread::spawn(move || {
        // 用裸 TcpStream 发一个最小 HTTP GET，避免为测试再引 HTTP 客户端
        use std::io::Write;
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        let _ = s.write_all(b"GET /callback?code=THE_CODE&state=ST HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
    });
    let code = l.wait_for_code(std::time::Duration::from_secs(5), "ST").unwrap();
    assert_eq!(code, "THE_CODE");
}

#[test]
fn listener_rejects_wrong_state() {
    let l = CallbackListener::bind(0).unwrap();
    let addr = l.local_addr().unwrap();
    std::thread::spawn(move || {
        use std::io::Write;
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        let _ = s.write_all(b"GET /callback?code=C&state=EVIL HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
    });
    assert!(l.wait_for_code(std::time::Duration::from_secs(5), "ST").is_err());
}

#[test]
fn listener_times_out_when_nobody_calls_back() {
    let l = CallbackListener::bind(0).unwrap();
    let e = l.wait_for_code(std::time::Duration::from_millis(200), "ST").unwrap_err();
    assert!(e.to_string().contains("超时"), "{e}");
}
```

- [ ] **Step 2: 跑测试确认失败** → `cargo test -p emd-core sso::flow` 编译失败

- [ ] **Step 3: 实现**

要点与必须遵守的纪律：
1. **令牌端点在 `login.eveonline.com`，不是 ESI 的 `esi.evetech.net`** —— 所以**不能**用 `EsiClient`（它的 `absolutize` 会拼到 ESI base_url）。本文件自带一个短超时的 reqwest 调用（登录是用户手点的一次性动作，1-2 个请求，不进 ESI 的预算/节流体系）。
2. **`login` 是唯一会开浏览器与阻塞等待的函数**。开浏览器用 `std::process::Command::new("cmd").args(["/C","start","",url])`（项目只做 Windows，不引 opener 依赖）。
3. **超时必设**：默认 180 s。用户不完成登录时 `login` 返回超时错误，**不留下已绑定端口**（`CallbackListener` 用 `Drop` 或显式关闭保证）。
4. **state 校验走 `verify_callback`**（T1 的纯函数），本文件不重复实现。
5. **令牌只进 `TokenStore`**，本文件不写任何 DB、不 `tracing` 打印 `TokenSet` 或 `jwt` 原文（Global Constraints 的脱敏要求）。

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git add Cargo.toml crates/emd-core/Cargo.toml crates/emd-core/src/sso.rs crates/emd-core/src/sso/flow.rs crates/emd-core/src/sso/listen.rs
git commit -m "feat(core): SSO 登录编排（loopback 监听/JWT 角色解析/浏览器跳转/令牌落 keyring，自带短超时 HTTP）"
```

---

### Task 4: `esi/client.rs` — Bearer 注入

**Files:** Modify: `crates/emd-core/src/esi/client.rs`

**Interfaces:**
- Produces: `EsiClient::fetch_auth(&self, path, token) -> Result<Fetch>`、`EsiClient::get_json_auth::<T>(&self, path, token) -> Result<T>`

- [ ] **Step 1: 写失败测试**（`client.rs` 测试模块；用 `httpmock` 或现有测试基建——**照本文件既有测试的 mock 方式写，先读一眼再动手**）

```rust
#[tokio::test]
async fn auth_requests_carry_bearer_header_and_public_ones_do_not() {
    // 断言两件事：① fetch_auth 带 Authorization: Bearer <token>
    //            ② 普通 fetch 不带 —— 公开端点带上令牌会污染缓存键语义且毫无必要
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**

把 `try_fetch(&self, url, force)` 扩为 `try_fetch(&self, url, force, bearer: Option<&str>)`，在构造 `reqwest::RequestBuilder` 处按 `bearer` 决定是否 `.header(AUTHORIZATION, format!("Bearer {t}"))`。`fetch_opt` 传 `None`，新增：

```rust
/// 角色端点专用：带 Bearer 走同一条缓存/节流/重试纪律。
/// 独立入口（而不是给 client 挂全局令牌）是为了让"公开请求永不携带令牌"成为类型事实。
pub async fn fetch_auth(&self, path_and_query: &str, token: &str) -> Result<Fetch>
```

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/esi/client.rs
git commit -m "feat(core): ESI 客户端支持 Bearer 注入（fetch_auth，公开路径仍不带令牌）"
```

---

### Task 5: `schema.rs` — 迁移 v6（char_meta / char_tx / char_orders / alerts）

**Files:** Modify: `crates/emd-core/src/store/schema.rs`

- [ ] **Step 1: 写失败测试**（并把 `versions_are_unique_and_ascending` 的 `5` 改 `6`，`bounded_tables_never_key_on_a_timestamp` 列表扩到六张）

```rust
#[test]
fn migration_v6_char_tables_are_bounded_and_token_free() {
    let sql = MIGRATIONS[5].2;
    // 令牌绝不在库里：这条断言是 Global Constraints 的机械保证
    for t in ["char_meta", "char_tx", "char_orders", "alerts"] {
        let body = sql.split(&format!("CREATE TABLE {t}")).nth(1).unwrap()
            .split("CREATE ").next().unwrap();
        assert!(!body.contains("token"), "{t} 不得有令牌列：{body}");
    }
    assert!(sql.contains("PRIMARY KEY (char_id, order_id)"), "char_orders 按角色+订单号");
    assert!(sql.contains("PRIMARY KEY (char_id, transaction_id)"), "char_tx 按角色+流水号");
}

#[test]
fn alerts_key_is_alert_key_and_has_notify_fields() {
    let sql = MIGRATIONS[5].2;
    let a = sql.split("CREATE TABLE alerts").nth(1).unwrap()
        .split("CREATE INDEX").next().unwrap();
    assert!(a.contains("alert_key"), "挂单轨 order_id / 已实现轨 transaction_id 统一叫 alert_key");
    assert!(a.contains("notified_day") && a.contains("notified_count_day"), "日限额需要自然日字段");
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**

```sql
(
    6,
    "M4c：角色挂链元数据、钱包流水/挂单快照、亏损告警状态机",
    r#"
-- 角色挂链元数据。**无令牌列**：refresh_token 只进 keyring（spec §4.1）。
CREATE TABLE char_meta (
    char_id        INTEGER PRIMARY KEY,
    name           TEXT,
    tx_cursor      TEXT,            -- wallet/transactions 的 since 增量水位（ISO8601）
    journal_cursor TEXT,            -- wallet/journal 的 since 增量水位
    orders_lm      TEXT,            -- 上轮 orders 的 Last-Modified，同源凭据
    first_sync_at  INTEGER,
    last_sync_at   INTEGER
);

-- 钱包流水缓存（transactions）：FIFO 成本基准的原料。
-- 保留期由调用层裁剪（90 天 = spec §4.2 的首启回填窗）；主键不含时间戳。
CREATE TABLE char_tx (
    char_id        INTEGER NOT NULL,
    transaction_id INTEGER NOT NULL,
    date           TEXT    NOT NULL,
    type_id        INTEGER NOT NULL,
    location_id    INTEGER NOT NULL,
    is_buy         INTEGER NOT NULL,
    unit_price     REAL    NOT NULL,
    quantity       INTEGER NOT NULL,
    PRIMARY KEY (char_id, transaction_id)
);
CREATE INDEX ix_char_tx_type ON char_tx (char_id, type_id, date);

-- 上轮挂单快照：供状态边沿判定（"首次转负才告警"要能对比上一轮）。
CREATE TABLE char_orders (
    char_id       INTEGER NOT NULL,
    order_id      INTEGER NOT NULL,
    type_id       INTEGER NOT NULL,
    location_id   INTEGER NOT NULL,
    is_buy        INTEGER NOT NULL,
    price         REAL    NOT NULL,
    volume_remain INTEGER NOT NULL,
    issued        TEXT    NOT NULL,
    duration      INTEGER NOT NULL,
    fetched_at    INTEGER NOT NULL,
    PRIMARY KEY (char_id, order_id)
);
CREATE INDEX ix_char_orders_type ON char_orders (char_id, type_id);

-- 告警状态机（spec §4.4）。alert_key = 挂单轨 order_id / 已实现轨 transaction_id。
CREATE TABLE alerts (
    alert_key          TEXT    PRIMARY KEY,
    kind               TEXT    NOT NULL,     -- expected_sell_loss/realized_loss/buy_order_trap
    char_id            INTEGER NOT NULL,
    type_id            INTEGER NOT NULL,
    location_id        INTEGER NOT NULL,
    is_buy             INTEGER NOT NULL,
    first_seen_at      INTEGER NOT NULL,
    last_seen_at       INTEGER NOT NULL,
    last_loss_isk      REAL    NOT NULL DEFAULT 0,
    last_margin_pct    REAL    NOT NULL DEFAULT 0,
    notified_at        INTEGER,
    notified_day       TEXT,
    notified_count_day INTEGER NOT NULL DEFAULT 0,
    last_notified_loss REAL,
    state              TEXT    NOT NULL,     -- new/notified/cleared
    payload            TEXT    NOT NULL      -- AlertPayload JSON：与推送共用同一序列化
);
CREATE INDEX ix_alerts_state ON alerts (state, last_seen_at DESC);
"#,
),
```

- [ ] **Step 4: 跑测试确认通过**（`cargo test -p emd-core schema`）+ 跑一次全量确认 `Db::in_memory()` 能升到 v6

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/store/schema.rs
git commit -m "feat(core): 迁移 v6——角色元数据/钱包流水/挂单快照/告警状态机（无令牌列）"
```

---

### Task 6: `store/char_db.rs` — 角色与告警读写层

**Files:** Create: `crates/emd-core/src/store/char_db.rs`；Modify: `crates/emd-core/src/store/mod.rs`

**Interfaces:**
- Produces（均为 `Db` 的 `impl` 方法）：`upsert_char_meta`、`char_meta`、`upsert_char_tx`、`load_char_tx`、`replace_char_orders`、`load_char_orders`、`prune_char_tx`、`save_alert`、`load_alerts`、`prune_alerts_cleared`

- [ ] **Step 1: 写失败测试**（独立模块 `char_persist_tests`，照 M4b 的 `lifecycle_persist_tests` 先例）

```rust
#[test]
fn char_tx_upsert_is_idempotent_and_orders_replace_is_whole_table() {
    // ① 同一 transaction_id 重放两次 → 仍 1 行（增量拉取会重叠，幂等是硬要求）
    // ② replace_char_orders 整表替换 → 旧订单消失（挂单会撤，不能留幽灵）
    // ③ prune_char_tx 按日期裁剪，90 天前的行消失
}

#[test]
fn alert_roundtrip_keeps_payload_json_verbatim() {
    // payload 列是 AlertPayload 的唯一序列化出口，读写必须逐字节稳定
    // （推送与提醒中心共用它，漂移 = 手机与界面显示不一致）
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**（照 `store/db.rs` M4b 段风格；`replace_char_orders` 用 `unchecked_transaction` 先删后插）

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/store/char_db.rs crates/emd-core/src/store/mod.rs
git commit -m "feat(core): 角色与告警读写层（流水幂等 upsert/挂单整表替换/告警 payload 单源）"
```

---

### Task 7: `char.rs` + `char/fifo.rs` — 同步管线与 FIFO 成本基准

**Files:** Create: `crates/emd-core/src/char.rs`、`crates/emd-core/src/char/fifo.rs`；Modify: `crates/emd-core/src/lib.rs`、`src/config.rs`（`CharConfig`）

**Interfaces:**
- Consumes: `EsiClient::get_json_auth`、`store::char_db` 的方法
- Produces: `CharConfig { client_id, redirect_uri, loopback_port, enabled, backfill_days }`、`CharSyncReport`、`sync_character(client, token, db, char_id, now) -> Result<CharSyncReport>`、`fifo_costs(&[WalletTx]) -> HashMap<u32, FifoCost>`、`FifoCost { avg_cost, source }`、`CostSource::{Known, Unknown}`

- [ ] **Step 1: 写失败测试**（FIFO 是纯函数，重点在这里；同步管线测装配与"成本未知"标注）

```rust
#[test]
fn fifo_averages_buys_and_consumes_on_sells() {
    // 买 100@10、买 100@20 → 持 200，均价 15
    // 卖 100 → 剩余 100，均价仍 15（FIFO 消耗最早那批）
    // 再卖 100 → 清空，成本未知
}

#[test]
fn fifo_marks_types_with_sells_but_no_buys_as_cost_unknown() {
    // 首启只回填 90 天：90 天前买的、90 天内卖的 → 有卖出无买入 → 必须标 Unknown
    // 不能拿 0 当成本（那会造出假亏损）——spec §4.2 明说"覆盖不到的类型标成本未知不参与判定"
}

#[test]
fn sync_report_counts_endpoints_and_degrades_on_optional_skills() {
    // skills 是可选端点：失败不能让整趟同步失败（其余三端点仍要落地）
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**

四端点（严格按 spec §4.2 表）：`/v2/characters/{id}/orders/`（整表覆盖、尊重 Expires）、`/v1/characters/{id}/wallet/transactions/`（`since` 增量）、`/v1/characters/{id}/wallet/journal/`（`since` 增量）、`/v4/characters/{id}/skills/`（可选）。每轮 ≤4 请求。首启只回填 `backfill_days`（默认 90）。

`fifo_costs` 纯函数：按 `date` 升序重放，买单入 FIFO 队列，卖单从队首消耗；队列清空后再卖 → 该类型标 `Unknown`。

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/char.rs crates/emd-core/src/char/ crates/emd-core/src/lib.rs crates/emd-core/src/config.rs
git commit -m "feat(core): 角色同步管线（4 端点增量/整表覆盖）+ FIFO 成本基准（未知成本显式标注）"
```

---

### Task 8: `alert.rs` — 三形态判定与 AlertPayload

**Files:** Create: `crates/emd-core/src/alert.rs`；Modify: `crates/emd-core/src/lib.rs`

**Interfaces:**
- Consumes: `FeeModel`、`flip::settle`（复用费率单源）、`char::fifo::FifoCost`
- Produces: `AlertKind`、`AlertPayload`（字段严格照 spec §4.3）、`CaliberSummary`、`detect_expected_sell(...)`、`detect_buy_trap(...)`、`detect_realized(...)`

- [ ] **Step 1: 写失败测试**（三形态各一组边界，费率复用 `FeeModel` 保证与倒卖引擎同源）

```rust
#[test]
fn expected_sell_loss_fires_when_net_below_full_cost() {
    // spec §4.3 ①：净额 = 挂价 × (1 − sales_tax(A))；全成本 = FIFO 均价 + 实付中介费/单位
    // 用 A5 技能口径：税 3.375%。挂价 100、FIFO 成本 95、中介费 0 → 96.625 > 95 不告警
    // 挂价降到 97 → 93.7 > 95 不成立 → 告警
}

#[test]
fn expected_sell_loss_never_fires_when_cost_is_unknown() {
    // 成本未知的类型必须**完全不参与**判定（spec §4.2）——
    // 拿 0 当成本会造出"每笔都在亏"的假告警，这是最坏的假阳性
}

#[test]
fn buy_trap_uses_executable_bid_not_last_price() {
    // spec §4.3 ②：用本站当前**可执行**卖出净额，不是挂单价
}

#[test]
fn realized_loss_uses_journal_truth_and_ignores_skill_panel() {
    // spec §4.3 ③ + "判定轨道分离"：改 FeeModel 不得影响已实现轨结果
}

#[test]
fn caliber_summary_declares_track_and_source() {
    // 口径摘要必须自报轨道（预期·估算费率 / 已实现·journal 真值）与成本来源
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**（`AlertPayload` 按 spec §4.3 逐字段抄；`kind` 的字符串映射集中在一个 `impl`）

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/alert.rs crates/emd-core/src/lib.rs
git commit -m "feat(core): 三形态负收益判定（预期/套牢/已实现，轨道分离，成本未知不参与）"
```

---

### Task 9: `alert/state.rs` — 告警状态机与闸门

**Files:** Create: `crates/emd-core/src/alert/state.rs`；Modify: `crates/emd-core/src/alert.rs`

**Interfaces:**
- Produces: `AlertState::{New, Notified, Cleared}`（+`as_str`/`parse`）、`AlertRecord`、`tick_alert(prev, fired, now) -> Option<AlertRecord>`、`can_push(rec, now, today) -> bool`、`mark_pushed(rec, now, today)`、`ALERT_DAILY_CAP`、`ALERT_COOLDOWN_SECS`、`ALERT_DEEPEN_PP`

- [ ] **Step 1: 写失败测试**（照 M4b `lifecycle.rs` 的纯逻辑测试风格）

```rust
#[test]
fn edge_triggered_only_on_transition_into_loss() {
    // 首次转负 → 可推；持续为负 → 不重复推（边沿触发，spec §4.4）
}

#[test]
fn deepening_by_two_pp_bypasses_cooldown_but_not_daily_cap() {
    // 亏损加深 ≥2pp → 穿透冷却；但日限 ≤5 条是硬闸，穿透不豁免
}

#[test]
fn daily_cap_counts_order_entries_not_cards() {
    // spec §4.4：每日 ≤5 条 = **订单条目数**；合并卡片里多条目各自计数
}

#[test]
fn cleared_then_refired_is_a_new_alert_but_keeps_notify_history() {
    // 订单撤销后重新挂上 = 新告警，但通知历史跨周期保留
    // （否则"撤了重挂"成为绕过冷却与日限的手段——与 M4b notified_at 同一教训）
}

#[test]
fn alert_center_never_limited_but_push_is() {
    // 提醒中心全量留存不受限额；can_push 只管推送侧
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**（结构照 `market/lifecycle.rs`：常量区 + 状态枚举 + 纯 tick + 闸门）

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/alert/state.rs crates/emd-core/src/alert.rs
git commit -m "feat(core): 告警状态机（边沿触发/深化穿透/日限按条目/跨周期保通知史）"
```

---

### Task 10: `push/dingtalk.rs` — 加签与 markdown 卡片

**Files:** Create: `crates/emd-core/src/push/dingtalk.rs`；Modify: `crates/emd-core/Cargo.toml`（hmac/sha2/base64/urlencoding 已有；无需新增）

**Interfaces:**
- Produces: `sign(timestamp_ms, secret) -> String`、`signed_url(webhook, secret, timestamp_ms) -> String`、`render_markdown(&AlertPayload) -> (String, String)`（title, text）、`map_errcode(code, msg) -> PushOutcome`

- [ ] **Step 0: 核对协议常量**

按"外部协议常量"节的核对方式确认钉钉加签与错误码；结论写进本文件顶部注释（含核对日期与出处）。**核对不通过就停下来报告。**

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn sign_is_urlencoded_base64_of_hmac_sha256() {
    // 用固定 timestamp + secret 断言输出稳定（防"改了拼接顺序没人发现"）
    // 关键细节：timestamp 毫秒、stringToSign = "{ts}\n{secret}"、结果 base64 后必须 urlencode
    let s = sign(1_700_000_000_000, "SECtest");
    assert!(!s.contains('+') && !s.contains('/') && !s.contains('='), "必须已 URL 编码：{s}");
}

#[test]
fn signed_url_appends_timestamp_and_sign() {
    let u = signed_url("https://oapi.dingtalk.com/robot/send?access_token=T", "SECx", 1_700_000_000_000);
    assert!(u.contains("access_token=T"));
    assert!(u.contains("timestamp=1700000000000"));
    assert!(u.contains("&sign="));
}

#[test]
fn render_markdown_carries_private_badge_and_caliber() {
    let (title, text) = render_markdown(&sample_payload());
    assert!(title.contains("亏损提醒"), "{title}");
    assert!(text.contains("私有数据"), "spec §4.5 要求页脚角标标私有数据");
    assert!(text.contains("order_id") || text.contains("tx#"), "必须能按 id 检索");
    assert!(text.contains("估算费率") || text.contains("journal 真值"), "轨道口径要自报");
}

#[test]
fn errcode_maps_to_actionable_outcomes() {
    assert!(matches!(map_errcode(0, "ok"), PushOutcome::Sent));
    assert!(matches!(map_errcode(310000, "sign not match"), PushOutcome::ChannelDisabled(_)));
    assert!(matches!(map_errcode(300001, "frequency"), PushOutcome::Retry { .. }));
    // 未知错误码不能当成成功
    assert!(!matches!(map_errcode(999999, "?"), PushOutcome::Sent));
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**

```rust
//! 钉钉群自定义机器人（通道按用户 2026-09-24 改定，替代 spec §4.5 的飞书）。
//! 协议常量核对：<核对日期> 对照 <出处 URL>。加签密钥以 SEC 开头，timestamp 为毫秒。
```

`render_markdown` 按 spec §4.5 版式落到钉钉 markdown：标题 `[亏损提醒] {type_name} · {买/卖}单 · {站点真名}`（钉钉 markdown 的 `title` 字段 + 正文首行标题）；正文逐行字段表（order_id 或 tx# / type_id / location_id / 方向 / 价格 / 数量 / 时间 / 亏损额 / 负 margin%）；口径摘要行；页脚 `私有数据` + 告警时刻 + "客户端提醒中心可按 order_id 检索"。**钉钉 markdown 没有折叠区**，spec §4.5 的"折叠区口径摘要"改为小字号普通行（记一条注释说明这处形态差异）。

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/push/dingtalk.rs crates/emd-core/src/push.rs
git commit -m "feat(core): 钉钉加签与 markdown 私有卡片（渲染与错误码映射为纯函数）"
```

---

### Task 11: `push.rs` — 通道抽象、本地提醒中心与发送器

**Files:** Create: `crates/emd-core/src/push.rs`；Modify: `crates/emd-core/src/lib.rs`、`src/config.rs`

**Interfaces:**
- Consumes: `push::dingtalk::*`、`store::char_db::save_alert`
- Produces: `PushOutcome`、`trait PushChannel`、`LocalChannel`、`DingTalkChannel::new(webhook, secret)`、`dispatch(channels, payload) -> Vec<PushOutcome>`、`mask(&str) -> String`（webhook token / secret / access_token 一律中段打码，日志与 UI 回显都走它）、`PushConfig`（webhook/secret/enabled，读 `meta` KV 不建表）

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn local_channel_always_succeeds_and_records() {
    // 本地提醒中心是"用户收回豁免"的回落方案，必须永不失败（spec §4.5）
}

#[tokio::test]
async fn dispatch_continues_after_one_channel_fails() {
    // 一个通道炸了不能拖垮另一个 —— 告警宁可重复也不能全丢
}

#[test]
fn webhook_and_secret_are_masked_in_display_and_logs() {
    // Global Constraints：webhook token 与 secret 不入日志。断言 mask 函数把中段替换成 ***
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**

`PushChannel` 是同步 trait（发送器内部用 blocking reqwest 或把 async 留在调用层——**照本项目既有做法选定一种并写进注释**；`esi` 用 async，`push` 若也 async 则 `dispatch` 为 async）。

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/push.rs crates/emd-core/src/lib.rs crates/emd-core/src/config.rs
git commit -m "feat(core): 推送通道抽象（本地提醒中心永不失败/钉钉通道/令牌脱敏/单通道故障隔离）"
```

---

### Task 12: scheduler 挂载角色同步与告警

**Files:** Modify: `crates/emd-core/src/scheduler.rs`

**Interfaces:**
- Consumes: `sso::store::TokenStore`、`char::sync_character`、`alert::{detect, update_round}`、`push::dispatch`
- Produces: `SchedulerConfig.char: CharConfig`、`Scheduler::run_char_and_alerts(&self) -> Result<Option<AlertRoundReport>>`、`AlertRoundReport { synced, detected, pushed, suppressed }`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn char_sync_is_skipped_when_disabled_or_no_token() {
    // EMD_CHAR_SYNC=0 或 keyring 无令牌 → 静默跳过（不是错误）
}

#[test]
fn alert_round_not_run_when_snapshot_is_stale() {
    // 与 M4b 生命周期同一教训：数据不可用 ≠ 状态变了，别拿旧盘口重新记账
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**

在 `run()` 主循环里，**生命周期钩子之后、T3 之前**挂一个钩子：`outcome.is_ok()` 且配置启用且有令牌时跑 `run_char_and_alerts`；失败只 `warn` 不打断主循环（与 T1.5 钩子同纪律）。

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-core/src/scheduler.rs
git commit -m "feat(core): 调度器挂载角色同步与告警（与 T1 节拍同频，失败不打断主循环）"
```

---

### Task 13: daemon — `alerts` / `char` 子命令

**Files:** Modify: `crates/emd-daemon/src/main.rs`

**Interfaces:**
- Consumes: `push::PushConfig`、`alert::*`、`sso::*`
- Produces: `Command::{Alerts, Char}`、`run_alerts`、`run_char`

- [ ] **Step 1: 写失败测试**（照 M4b 的 `parses_xregion_and_opps` 风格）

```rust
#[test]
fn parses_alerts_and_char() {
    // alerts 无参可跑；--kind expected_sell_loss 过滤；未知 kind 当场拒绝
    // char --status 打印挂链状态与同步水位（**绝不打印令牌**）
    // char --logout 清 keyring
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**

`alerts`：打印告警表（kind/类型/站点/亏损额/margin/状态/通知计数）+ 表尾各 kind 计数；空表提示"先跑 serve 或 alarts --update"。
`char --status`：打印 char_id/名称/上次同步/流水水位——**输出必须脱敏，断言测试里加一条"输出不含 AT/RT 字样"**。
`char --logout`：调 `TokenStore::clear()`。

- [ ] **Step 4: 跑测试 + `cargo build -p emd-daemon` 绿**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-daemon/src/main.rs
git commit -m "feat(daemon): alerts/char 子命令（告警表/挂链状态/登出，输出脱敏）"
```

---

### Task 14: app + web — 提醒中心视图与配置

**Files:** Modify: `crates/emd-app/src/lib.rs`、`crates/emd-app/src/tests.rs`、`web/src/types.ts`、`web/src/api.ts`、`web/src/App.tsx`、`web/src/styles.css`；Create: `web/src/components/AlertCenter.tsx`

**Interfaces:**
- Produces: Tauri 命令 `alerts_list` / `alert_settings_get` / `alert_settings_set` / `sso_status` / `sso_logout`；前端 `AlertRow`、`AlertSettings`

- [ ] **Step 1: 后端命令 + 测试**（`AlertRow` 含 kind/类型名/站点名/亏损额/margin/状态/通知计数；名字回填失败用 fallback 串，照 `flip_scan` 先例）

- [ ] **Step 2: 前端类型与 fixture**（`api.ts` 加 fixture：3 条告警覆盖三种 kind + 一条已推送状态；`inTauri` 判定照现有写法）

- [ ] **Step 3: UI**（`AlertCenter.tsx`：告警列表 + kind 角标 + "私有数据"提示条 + 通道配置（webhook/secret 输入，**secret 用 password 型且回显打码**）+ SSO 状态与登出按钮；`styles.css` 加样式；顶栏加「提醒」视图切换）

- [ ] **Step 4: `npx tsc --noEmit` = 0；`npm run build` 绿；浏览器走查 fixture（列表渲染、secret 打码、控制台 0 error、窄窗不破版）**

- [ ] **Step 5: 提交**

```bash
git add crates/emd-app/src/lib.rs crates/emd-app/src/tests.rs web/src/
git commit -m "feat(web+app): 提醒中心视图（告警列表/通道配置脱敏/SSO 状态）"
```

---

### Task 15: 收尾——全量回归 + CodeReview + 挂账验收

- [ ] **Step 1: 全量回归**：`cargo test -p emd-core -p emd-daemon -p emd-app`（预计 189 + 新增）全绿 + `npx tsc --noEmit` + `npm run build`

- [ ] **Step 2: CodeReview 子代理**（diff = M4c 全部提交），整改发现项

- [ ] **Step 3: 可做的真机验收**（**无 client_id、无钉钉机器人，以下为限定范围**）：
  1. `emd alerts`：空表提示正确
  2. `emd char --status`：未挂链时输出"未登录"，且**输出不含令牌字样**
  3. 迁移真机：在真库上打开一次，`schema_migration` 到 6，四新表 0 行，旧表未动
  4. 浏览器「提醒」视图：fixture 3 条告警 + 通道配置脱敏 + 控制台 0 error
- [ ] **Step 4: 挂账项写进报告**（**不得写成已验收**）：
  - SSO 真机登录（需 client_id + 注册 `http://127.0.0.1:8765/callback`）
  - 钉钉真机发送（需群机器人 webhook + 加签 SEC 密钥）
  - 角色数据真机同步（依赖 SSO）
  - FIFO 成本基准的真机对照（依赖真实流水）
  - 协议常量核对结果（Task 1/10 Step 0 的产出）
- [ ] **Step 5: 最终提交 + 报告**（含对 spec §4.1–§4.6 的逐条映射、D1–D4 四项偏离、挂账清单）

---

## 自审记录

**1. 规格覆盖**
- §4.1 授权与令牌 → Task 1/2/3/4（PKCE 纯逻辑、令牌交换刷新、keyring 存储、Bearer 注入）✓
- §4.2 同步管线（4 端点 ≤4 请求、首启 90 天、成本未知标注）→ Task 7 ✓
- §4.3 三形态判定 + AlertPayload + CaliberSummary → Task 8 ✓
- §4.4 告警状态机与限额（边沿/≥2pp/日 ≤5 按条目/提醒中心不限额）→ Task 9 ✓
- §4.5 私有卡片与豁免记录 → Task 10/11（钉钉替代飞书，见 D1）✓
- §4.6 存储（迁移 v6 四表）→ Task 5/6 ✓
- 落地形态（daemon + app + web 三层）→ Task 12/13/14 ✓
- 收尾与挂账 → Task 15 ✓

**2. 占位符扫描**
- Task 2 的 `parse_token_response` 实现体写了"见测试的三条分支"，**执行时按测试逐分支写全**（分支已在测试里列清，不是 TBD）。
- Task 4 的测试体留了空注释，因为需要先读本文件既有 mock 方式再写——**Step 1 已显式要求"先读一眼再动手"**。
- Task 6/8/9/11/12 的实现体给了要点而非全文；这些任务的测试已把行为钉死，执行时按测试补全。**若执行中发现某处需要额外决策，停下来问，不要自行发明口径。**

**3. 类型一致性**
- `TokenSet` 三字段在 Task 2/3/12 一致
- `AlertPayload`/`AlertKind`/`CaliberSummary` 在 Task 8/9/10/11/14 一致（`kind` 字符串映射集中在 Task 8 的 `impl`）
- `FifoCost`/`CostSource` 在 Task 7/8 一致
- `AlertRecord` 字段在 Task 9 定义、Task 6 的 `save_alert`/`load_alerts` 落库、Task 13 打印
- `PushOutcome` 在 Task 10 产出、Task 11 `dispatch` 消费、Task 12 统计
- `CharConfig` 在 Task 7 定义、Task 12 引用
- 存储层方法名（`save_alert`/`load_alerts`/`replace_char_orders`/`load_char_tx`）在 Task 6 定义、Task 12/13 引用一致
