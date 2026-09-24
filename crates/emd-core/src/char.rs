//! 角色同步管线（spec §4.2）：每轮四端点、≤4 请求，把角色数据落进 v6 的三张表。
//!
//! 四端点（**每条路径都自带角色 id** —— 这是 [`EsiClient::fetch_auth`] 的前置条件：
//! 缓存键只是 URL，路径里没有身份就会把 A 的缓存体喂给 B）：
//!
//! | 端点 | 方式 |
//! |---|---|
//! | `GET /v2/characters/{id}/orders/` | 整表覆盖（撤掉的挂单必须消失），尊重 `Expires` |
//! | `GET /v1/characters/{id}/wallet/transactions/` | 流水，按水位取增量 |
//! | `GET /v1/characters/{id}/wallet/journal/` | 日记账（实付费用真值，见下） |
//! | `GET /v4/characters/{id}/skills/` | 技能，**可选**：它失败不连坐另外三条 |
//!
//! 令牌纪律：`token` 只作为 `fetch_auth`/`get_json_auth` 的入参出现在本文件，
//! 不进日志、不进错误串（`EsiClient` 的错误只带 URL 与状态码）。
//!
//! **增量怎么算**（2026-09-24 核对 ESI 契约后定的落地方式）：
//! ESI 的 wallet 两端口**没有日期型 `since` 参数** —— transactions 只有游标式的 `from_id`
//! （"只给比这个 id 更早的"），journal 只有 `page`，且 journal 只回溯 30 天。
//! 因此水位不拼进 URL，而是本地生效：取回的那一段用「`date >= 水位`」过滤（**闭区间**：
//! 水位当天那批不能被漏掉，`load_char_tx` 的下界也是闭的），落库后把水位推进到本轮见过的
//! 最大 `date`（**原样文本**，一个字符都不改）。URL 恒定还让 `EsiClient` 的条件请求缓存
//! 对同一端点真正生效 —— 每轮换 URL 等于每轮全量重取，`Expires` 与 304 全部白搭。
//!
//! **契约核对**：参数表与响应字段来自 ESI 官方契约的 openapi 生成件
//! `tkhamez/eve-api-php` v14.20260519.0（version 串里的 `20260519` 就是 ESI 兼容日期），
//! 四个 path + 版本于当日无令牌探测过：`/v2/.../orders/`、`/v1/.../wallet/{transactions,journal}/`、
//! `/v4/.../skills/` 全部返回 401（路由存在、只差令牌），而编造的 `/v9/...` 返回 404。
//! **真机响应体核对挂账**：本机没有 client_id，拿不到真实报文。
//!
//! **日期不做归一化**：ESI 的 `date` 原样入库（`load_char_tx` 的闭区间下界与
//! `prune_char_tx` 的字典序裁剪都建立在"同格式 ISO8601 文本"上）。形状不对时只记日志
//! 报出来，绝不补 `Z`、不改时区、不重排 —— 篡改过的历史文本会让那两条假设当场失效，
//! 而没人会知道它被改过。

pub mod fifo;

use serde::Deserialize;

use crate::config::CharConfig;
use crate::error::{Error, Result};
use crate::esi::EsiClient;
use crate::store::{CharOrder, Db, WalletTx};

// ---------------------------------------------------------------------------
// 端点路径
// ---------------------------------------------------------------------------

/// 四端点的路径构造。**都嵌了角色 id**：这既是 ESI 的语义要求，也是 `fetch_auth` 能安全
/// 复用同一条缓存键的前提（P3）。本文件**不得**新增任何"带令牌但路径里没有角色 id"的调用。
fn orders_path(char_id: u64) -> String {
    format!("/v2/characters/{char_id}/orders/")
}

fn transactions_path(char_id: u64) -> String {
    format!("/v1/characters/{char_id}/wallet/transactions/")
}

fn journal_path(char_id: u64) -> String {
    format!("/v1/characters/{char_id}/wallet/journal/")
}

fn skills_path(char_id: u64) -> String {
    format!("/v4/characters/{char_id}/skills/")
}

// ---------------------------------------------------------------------------
// ESI 响应形状（本机无 client_id，无法真机核验；来源见各结构体的注释）
// ---------------------------------------------------------------------------

/// `/v2/characters/{id}/orders/` 一行。
///
/// **形状来源**：2026-09-24 对照 ESI 官方契约的 openapi 生成件（`tkhamez/eve-api-php`
/// v14.20260519.0，兼容日期 2026-05-19）的 `CharactersCharacterIdOrdersGetInner` 逐字段核对；
/// 只收库里有的列（`escrow`/`range`/`region_id`/`min_volume`/`volume_total`/`is_corporation`
/// 不进结构体）。**真机响应体核对挂账**（无 client_id）。
/// 不设 `deny_unknown_fields`：ESI 会加列，多出来的字段直接忽略才是长期可活的写法。
#[derive(Debug, Deserialize)]
struct ApiOrder {
    order_id: i64,
    type_id: u32,
    location_id: u64,
    /// ESI 的字段名就是 `is_buy_order`（不是 `is_buy`）；模型里标了 optional，故给默认值。
    #[serde(default)]
    is_buy_order: bool,
    price: f64,
    volume_remain: u64,
    /// 原样文本：要进 `char_orders.issued`，不解析、不改写。
    issued: String,
    duration: i64,
}

impl ApiOrder {
    fn to_row(&self) -> CharOrder {
        CharOrder {
            order_id: self.order_id,
            type_id: self.type_id,
            location_id: self.location_id,
            is_buy: self.is_buy_order,
            price: self.price,
            volume_remain: self.volume_remain,
            issued: self.issued.clone(),
            duration: self.duration,
            // 由 `replace_char_orders` 按本轮时刻统一落，这里给 0 只是占位。
            fetched_at: 0,
        }
    }
}

/// `/v1/characters/{id}/wallet/transactions/` 一行。
///
/// **形状来源**：同上的 `CharactersCharacterIdWalletTransactionsGetInner`（兼容日期 2026-05-19）
/// —— `client_id`/`is_personal`/`journal_ref_id` 不进结构体。
#[derive(Debug, Deserialize)]
struct ApiTx {
    transaction_id: i64,
    date: String,
    type_id: u32,
    location_id: u64,
    is_buy: bool,
    unit_price: f64,
    quantity: u64,
}

impl ApiTx {
    fn to_row(&self) -> WalletTx {
        WalletTx {
            transaction_id: self.transaction_id,
            date: self.date.clone(),
            type_id: self.type_id,
            location_id: self.location_id,
            is_buy: self.is_buy,
            unit_price: self.unit_price,
            quantity: self.quantity,
        }
    }
}

/// `/v1/characters/{id}/wallet/journal/` 一行。
///
/// **形状来源**：同上的 `CharactersCharacterIdWalletJournalGetInner`（兼容日期 2026-05-19）。
/// `amount`/`context_id` 在模型里都是 optional（不同 `ref_type` 填的字段不同），
/// 因此是 `Option`：**空与 0 必须分得开** —— 0 会被读成"实付 0 费"。
#[derive(Debug, Deserialize)]
struct ApiJournal {
    id: i64,
    date: String,
    ref_type: String,
    amount: Option<f64>,
    context_id: Option<i64>,
    description: String,
}

/// 一条日记账（实付费用真值）。
///
/// **v6 的四张表里没有 journal**（`char_meta`/`char_tx`/`char_orders`/`alerts`），
/// 本文件也不越界改 schema：这一轮取到的真值随 [`CharSyncReport::journal_entries`]
/// 交给调用方，由 T12 在同一个 tick 里喂给判定侧（"每轮 ≤4 请求"决定了它不可能再拉一次）。
#[derive(Debug, Clone, PartialEq)]
pub struct JournalEntry {
    pub id: i64,
    /// ESI 原样文本（同 [`WalletTx::date`] 的理由）。
    pub date: String,
    /// `broker_fee` / `transaction_tax` / `market_transaction` / ...
    pub ref_type: String,
    pub amount: Option<f64>,
    pub context_id: Option<i64>,
    pub description: String,
}

impl ApiJournal {
    fn to_entry(&self) -> JournalEntry {
        JournalEntry {
            id: self.id,
            date: self.date.clone(),
            ref_type: self.ref_type.clone(),
            amount: self.amount,
            context_id: self.context_id,
            description: self.description.clone(),
        }
    }
}

/// `/v4/characters/{id}/skills/` 的响应。
///
/// **形状来源**：同上的 `CharactersSkills`（兼容日期 2026-05-19）；
/// `skills[]` 的 `skill_id`/`active_skill_level` 两列够面板用，其余列不进结构体。
#[derive(Debug, Deserialize)]
struct ApiSkills {
    skills: Vec<ApiSkill>,
}

#[derive(Debug, Deserialize)]
struct ApiSkill {
    skill_id: u32,
    active_skill_level: u32,
}

/// 一条技能读数（面板"读取真实技能"用）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SkillLevel {
    pub skill_id: u32,
    /// 用 active 而不是 trained：面板要算的是**当下生效**的费率
    /// （alpha 克隆与专家系统会让两者不同）。
    pub active_skill_level: u32,
}

// ---------------------------------------------------------------------------
// 报告
// ---------------------------------------------------------------------------

/// 单个端点的本轮结果。
#[derive(Debug, Clone, PartialEq)]
pub struct EndpointOutcome {
    pub ok: bool,
    /// 本轮受理/落库的行数（失败固定 0）。
    pub rows: usize,
    /// 失败原因（`Error` 的 Display）。**不含令牌**。
    pub error: Option<String>,
}

impl EndpointOutcome {
    fn ok(rows: usize) -> Self {
        Self {
            ok: true,
            rows,
            error: None,
        }
    }

    fn failed(err: &Error) -> Self {
        Self {
            ok: false,
            rows: 0,
            error: Some(err.to_string()),
        }
    }
}

/// 一轮同步的结果。**每个端点各记一条**：`skills` 是可选端点，它失败不能连坐另外三条（P4）。
#[derive(Debug, Clone, PartialEq)]
pub struct CharSyncReport {
    pub char_id: u64,
    pub orders: EndpointOutcome,
    pub transactions: EndpointOutcome,
    pub journal: EndpointOutcome,
    pub skills: EndpointOutcome,
    /// 本轮窗口内的日记账（实付真值）。表里没有它，落点就是这里 —— 见 [`JournalEntry`]。
    pub journal_entries: Vec<JournalEntry>,
    /// 本轮技能读数（面板用），失败时为空。
    pub skill_levels: Vec<SkillLevel>,
}

// ---------------------------------------------------------------------------
// 同步
// ---------------------------------------------------------------------------

/// 同步一轮（brief 钉死的入口）。
///
/// 首启回填窗取 [`CharConfig::default`] 的 90 天：本函数的签名里没有配置项，
/// 需要按用户配置的窗口跑时用 [`sync_character_with`]（T12 手上才有真 `CharConfig`）。
pub async fn sync_character(
    client: &EsiClient,
    token: &str,
    db: &Db,
    char_id: u64,
    now: i64,
) -> Result<CharSyncReport> {
    sync_character_with(
        client,
        token,
        db,
        char_id,
        now,
        CharConfig::default().backfill_days,
    )
    .await
}

/// 同步一轮（显式回填窗版）。`backfill_days` 只在**首启**（库里没有 `tx_cursor`）用得上：
/// 拉一个窗，而不是把角色全部历史拉回来。
///
/// 失败面：端点级的失败（网络/状态码/解析）**不改变返回类型**，逐条记进报告；
/// `Err` 只留给 DB 与配置层的硬错误（落库失败、`now`/窗参数推不出合法日期）。
pub async fn sync_character_with(
    client: &EsiClient,
    token: &str,
    db: &Db,
    char_id: u64,
    now: i64,
    backfill_days: i64,
) -> Result<CharSyncReport> {
    let meta = db.char_meta(char_id)?;
    // 窗下界用**日期**而不是时刻：拉取侧（`date >= 下界`）与裁剪侧（`date < 裁剪线`）
    // 必须是同一条线，差一天就会"拉了又剪"白干一遍。
    let cutoff = window_start_date(now, backfill_days)?;

    // 增量水位：首启用回填窗下界（零点），此后**原样**沿用库里那份（P2）。
    let tx_since = meta
        .as_ref()
        .and_then(|m| m.tx_cursor.clone())
        .unwrap_or_else(|| format!("{cutoff}T00:00:00Z"));
    let journal_since = meta.as_ref().and_then(|m| m.journal_cursor.clone());

    // ---- 端点 1/4：挂单，整表覆盖（尊重 Expires：走 fetch_auth 的条件请求缓存）----
    // 用 fetch_auth 而不是 get_json_auth，是为了顺手拿到 Last-Modified：它是 `orders_lm`
    // 的同源凭据，本轮没有就保持库里那份（None = 不动列）。
    let mut orders_lm: Option<String> = None;
    let orders = match client.fetch_auth(&orders_path(char_id), token).await {
        Ok(f) => {
            orders_lm = f.meta().last_modified.clone();
            match f.json::<Vec<ApiOrder>>() {
                Ok(api) => {
                    let rows: Vec<CharOrder> = api.iter().map(ApiOrder::to_row).collect();
                    // 空快照也要整表替换：撤光的角色必须清掉幽灵行（T6 的语义）。
                    EndpointOutcome::ok(db.replace_char_orders(char_id, &rows, now)?)
                }
                Err(e) => EndpointOutcome::failed(&e),
            }
        }
        Err(e) => EndpointOutcome::failed(&e),
    };

    // ---- 端点 2/4：钱包流水，按水位取增量 ----
    let mut tx_cursor_next: Option<String> = None;
    let transactions = match client
        .get_json_auth::<Vec<ApiTx>>(&transactions_path(char_id), token)
        .await
    {
        Ok(api) => {
            warn_on_odd_dates(char_id, "transactions", api.iter().map(|t| t.date.as_str()));
            let fresh: Vec<WalletTx> = api
                .into_iter()
                .filter(|t| t.date.as_str() >= tx_since.as_str())
                .map(|t| t.to_row())
                .collect();
            // 水位推进到本轮见过的最大 date（原样文本）。空窗口 → None → 列不动：
            // 没数据不代表水位该动，往回退更不行。
            tx_cursor_next = fresh.iter().map(|t| t.date.as_str()).max().map(str::to_string);
            EndpointOutcome::ok(db.upsert_char_tx(char_id, &fresh)?)
        }
        Err(e) => EndpointOutcome::failed(&e),
    };

    // ---- 端点 3/4：日记账，同样按水位取增量 ----
    let mut journal_cursor_next: Option<String> = None;
    let mut journal_entries: Vec<JournalEntry> = Vec::new();
    let journal = match client
        .get_json_auth::<Vec<ApiJournal>>(&journal_path(char_id), token)
        .await
    {
        Ok(api) => {
            warn_on_odd_dates(char_id, "journal", api.iter().map(|e| e.date.as_str()));
            journal_entries = api
                .into_iter()
                .filter(|e| {
                    journal_since
                        .as_deref()
                        .map_or(true, |c| e.date.as_str() >= c)
                })
                .map(|e| e.to_entry())
                .collect();
            journal_cursor_next = journal_entries
                .iter()
                .map(|e| e.date.as_str())
                .max()
                .map(str::to_string);
            EndpointOutcome::ok(journal_entries.len())
        }
        Err(e) => EndpointOutcome::failed(&e),
    };

    // ---- 端点 4/4：技能（可选）----
    let mut skill_levels: Vec<SkillLevel> = Vec::new();
    let skills = match client
        .get_json_auth::<ApiSkills>(&skills_path(char_id), token)
        .await
    {
        Ok(api) => {
            skill_levels = api
                .skills
                .iter()
                .map(|s| SkillLevel {
                    skill_id: s.skill_id,
                    active_skill_level: s.active_skill_level,
                })
                .collect();
            EndpointOutcome::ok(skill_levels.len())
        }
        Err(e) => EndpointOutcome::failed(&e),
    };

    // 三条必需端点至少有一条落地，才算"这一轮真的同步过"。
    let landed = orders.ok || transactions.ok || journal.ok;
    if landed {
        // 水位：**None = 不动列**（P1）—— 本轮没拿到的端点，它的水位必须原样留着，
        // 否则下一轮退化成全量重拉（首启那个 90 天的回填会每轮重演）。
        db.set_char_cursors(
            char_id,
            tx_cursor_next.as_deref(),
            journal_cursor_next.as_deref(),
            orders_lm.as_deref(),
        )?;
        // 角色名是登录（T3B）的产物，本函数拿不到：沿用库里那份；行还不存在时写空串
        // （列本就可空，但 upsert 需要一个 &str）。**绝不臆造一个名字。**
        let name = meta.as_ref().and_then(|m| m.name.clone()).unwrap_or_default();
        db.upsert_char_meta(char_id, &name, now)?;
    }
    // 保留期收口：只在流水**真的刷新过**时裁剪。全灭的一轮里推进裁剪线等于让窗口
    // 在没有新数据的情况下继续吞掉旧数据 —— 断网久了会把 FIFO 基准吃空。
    if transactions.ok {
        db.prune_char_tx(&cutoff)?;
    }

    Ok(CharSyncReport {
        char_id,
        orders,
        transactions,
        journal,
        skills,
        journal_entries,
        skill_levels,
    })
}

/// 回填窗下界（`YYYY-MM-DD`，UTC）。
fn window_start_date(now: i64, backfill_days: i64) -> Result<String> {
    let days = backfill_days.max(1);
    let start = now - days * 86_400;
    chrono::DateTime::from_timestamp(start, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .ok_or_else(|| {
            Error::Config(format!(
                "now={now} 与 backfill_days={backfill_days} 推不出合法的窗下界"
            ))
        })
}

/// 日期形状核对，**只用于告警**：
/// `load_char_tx` 的闭区间下界与 `prune_char_tx` 的字典序裁剪都假定所有 `date` 是同一种
/// ISO8601 文本。ESI 哪天换了形状（带 `+08:00` 偏移、只给日期、给本地时间），
/// 那两条假设就悄悄失效了 —— 此时**必须报出来而不是顺手归一化**（P2）：改过的历史文本
/// 没人能再对上账。
fn warn_on_odd_dates<'a>(char_id: u64, endpoint: &str, dates: impl Iterator<Item = &'a str>) {
    let odd: Vec<&str> = dates.filter(|d| !date_shape_ok(d)).collect();
    if !odd.is_empty() {
        tracing::warn!(
            "char {char_id} 的 {endpoint} 有 {} 行的 date 不是 RFC3339+Z（例如 {:?}）；\
             按原文入库、不做归一化 —— 字典序即时间序的假设可能已失效",
            odd.len(),
            odd.first()
        );
    }
}

/// `YYYY-MM-DDThh:mm:ss[.fff]Z` 才认。带偏移的写法（`+08:00`）会破坏字典序假设，也算异形。
fn date_shape_ok(s: &str) -> bool {
    s.ends_with('Z') && chrono::DateTime::parse_from_rfc3339(s).is_ok()
}

#[cfg(test)]
mod sync_tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;
    use crate::config::EsiConfig;
    use crate::store::Db;

    const CHAR: u64 = 90_000_001;
    /// 故意取一个显眼的串：任何错误串/日志里出现它就说明令牌泄漏了。
    const TOKEN: &str = "SECRET-ACCESS-TOKEN";

    /// 2026-09-20T12:00:00Z —— 90 天前是 2026-06-22，测试里的"窗外/窗内"按这条线切。
    fn now() -> i64 {
        chrono::DateTime::parse_from_rfc3339("2026-09-20T12:00:00Z")
            .unwrap()
            .timestamp()
    }

    /// 一条桩路由：URL 片段、状态码、响应体、可选的 `Last-Modified`。
    struct Route {
        fragment: &'static str,
        status: u16,
        body: &'static str,
        last_modified: Option<&'static str>,
    }

    impl Route {
        fn ok(fragment: &'static str, body: &'static str) -> Self {
            Self {
                fragment,
                status: 200,
                body,
                last_modified: None,
            }
        }

        fn ok_with_lm(fragment: &'static str, body: &'static str, lm: &'static str) -> Self {
            Self {
                fragment,
                status: 200,
                body,
                last_modified: Some(lm),
            }
        }

        fn fail(fragment: &'static str, status: u16) -> Self {
            Self {
                fragment,
                status,
                body: r#"{"error":"Forbidden - token does not have the required scope"}"#,
                last_modified: None,
            }
        }
    }

    /// 起一个只认四条角色路径的本地桩服务：**首条命中的路由用掉即移除**，
    /// 于是"第二轮换一份响应"只需给同一片段再排一条。
    /// 收到的每个请求（URL + Authorization）原样回传，供测试断言报文本身
    /// （真 socket 而不是自己构造的请求对象 —— 证的就是发出去的那份报文）。
    fn char_stub(
        routes: Vec<Route>,
        requests: usize,
    ) -> (String, mpsc::Receiver<(String, Option<String>)>) {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut routes = routes;
            for _ in 0..requests {
                // 请求没来（实现回归了）时别把测试挂死：超时就收摊，断言侧会看到通道关闭。
                let Ok(Some(req)) = server.recv_timeout(Duration::from_secs(10)) else {
                    return;
                };
                let url = req.url().to_string();
                let auth = req
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("authorization"))
                    .map(|h| h.value.as_str().to_string());
                let _ = tx.send((url.clone(), auth));

                let Some(i) = routes.iter().position(|r| url.contains(r.fragment)) else {
                    let _ = req.respond(tiny_http::Response::from_string("{}").with_status_code(404));
                    continue;
                };
                let r = routes.remove(i);
                let resp = tiny_http::Response::from_string(r.body).with_status_code(r.status);
                let resp = match r.last_modified {
                    Some(lm) => resp.with_header(
                        tiny_http::Header::from_bytes(&b"Last-Modified"[..], lm.as_bytes())
                            .unwrap(),
                    ),
                    None => resp,
                };
                let _ = req.respond(resp);
            }
        });
        (format!("http://127.0.0.1:{port}"), rx)
    }

    fn client_at(base_url: &str) -> EsiClient {
        EsiClient::new(EsiConfig {
            base_url: base_url.to_string(),
            ..Default::default()
        })
        .unwrap()
    }

    /// 桩服务记录的四条路径，按到达顺序。
    fn drain_urls(rx: &mpsc::Receiver<(String, Option<String>)>, n: usize) -> Vec<(String, Option<String>)> {
        (0..n)
            .map(|_| rx.recv_timeout(Duration::from_secs(10)).expect("请求没到齐"))
            .collect()
    }

    const ORDERS_JSON: &str = r#"[{"order_id":101,"type_id":34,"location_id":60003760,
        "is_buy_order":false,"price":1000.0,"volume_remain":10,"issued":"2026-09-19T09:00:00Z",
        "duration":90,"escrow":0.0,"range":"station","region_id":10000002,"volume_total":10,
        "min_volume":1,"is_corporation":false}]"#;

    /// 两行流水：一行在 90 天窗外，一行在窗内。
    const TX_JSON: &str = r#"[{"transaction_id":5001,"date":"2026-06-01T00:00:00Z","type_id":34,
        "location_id":60003760,"is_buy":true,"unit_price":5.0,"quantity":10,"client_id":1,
        "is_personal":true,"journal_ref_id":7},
        {"transaction_id":5002,"date":"2026-09-20T11:00:00Z","type_id":35,"location_id":60003760,
        "is_buy":true,"unit_price":10.0,"quantity":100,"client_id":1,"is_personal":true,
        "journal_ref_id":8}]"#;

    const JOURNAL_JSON: &str = r#"[{"id":9001,"date":"2026-09-20T10:30:00Z","ref_type":"broker_fee",
        "amount":-1500.0,"context_id":555,"description":"Broker Fee","balance":1.0,
        "first_party_id":1,"second_party_id":2,"reason":""}]"#;

    const SKILLS_JSON: &str = r#"{"skills":[{"skill_id":16622,"active_skill_level":5,
        "trained_skill_level":5,"skillpoints_in_skill":1},
        {"skill_id":3446,"active_skill_level":4,"trained_skill_level":4,
        "skillpoints_in_skill":1}],"total_sp":1}"#;

    #[tokio::test]
    async fn sync_report_counts_endpoints_and_degrades_on_optional_skills() {
        // skills 是可选端点：失败不能让整趟同步失败（其余三端点仍要落地）。
        let (base, seen) = char_stub(
            vec![
                Route::ok_with_lm("/orders/", ORDERS_JSON, "Wed, 17 Sep 2026 00:00:00 GMT"),
                Route::ok("/wallet/transactions/", TX_JSON),
                Route::ok("/wallet/journal/", JOURNAL_JSON),
                Route::fail("/skills/", 403),
            ],
            4,
        );
        let client = client_at(&base);
        let db = Db::in_memory().unwrap();
        // 登录（T3B）先把角色名写进库；同步管线只沿用，不改写、也不臆造。
        db.upsert_char_meta(CHAR, "Pilot One", 0).unwrap();

        let report = sync_character(&client, TOKEN, &db, CHAR, now()).await.unwrap();

        // ① 三条必需端点各自落地，行数如实记。
        assert!(report.orders.ok, "{:?}", report.orders);
        assert_eq!(report.orders.rows, 1);
        assert!(report.transactions.ok, "{:?}", report.transactions);
        assert_eq!(
            report.transactions.rows, 1,
            "窗外那行（2026-06-01 早于 90 天窗）不得入库"
        );
        assert!(report.journal.ok, "{:?}", report.journal);
        assert_eq!(report.journal.rows, 1);
        assert!(report.skills.ok == false, "skills 本轮 403");
        assert!(report.skills.error.as_deref().unwrap().contains("403"));
        assert!(report.skill_levels.is_empty());
        // 令牌不进错误串（Global Constraint）。
        assert!(
            !format!("{:?}", report.skills.error).contains(TOKEN),
            "令牌泄漏进报告：{:?}",
            report.skills.error
        );

        // ② 三条必需端点的数据真的落到了库里（"skills 失败不连累它们"的实证）。
        let orders = db.load_char_orders(CHAR).unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, 101);
        assert_eq!(orders[0].issued, "2026-09-19T09:00:00Z", "日期原样入库");
        assert_eq!(orders[0].fetched_at, now());
        let txs = db.load_char_tx(CHAR, None).unwrap();
        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].transaction_id, 5002);
        assert_eq!(txs[0].date, "2026-09-20T11:00:00Z", "日期原样入库");

        // ③ 水位：本轮见过的最大 date **原样**推进；orders_lm 是本轮响应的 Last-Modified 原文。
        let meta = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(meta.tx_cursor.as_deref(), Some("2026-09-20T11:00:00Z"));
        assert_eq!(meta.journal_cursor.as_deref(), Some("2026-09-20T10:30:00Z"));
        assert_eq!(meta.orders_lm.as_deref(), Some("Wed, 17 Sep 2026 00:00:00 GMT"));
        assert_eq!(meta.name.as_deref(), Some("Pilot One"), "同步不改登录写下的名字");
        assert_eq!(meta.last_sync_at, Some(now()));

        // ④ 报告带出本轮日记账真值（表里没有 journal，落点就在这里）。
        assert_eq!(report.journal_entries.len(), 1);
        assert_eq!(report.journal_entries[0].id, 9001);
        assert_eq!(report.journal_entries[0].ref_type, "broker_fee");
        assert_eq!(report.journal_entries[0].amount, Some(-1500.0));

        // ⑤ 四条路径逐字对齐 spec §4.2，且每条都带令牌 —— 路径里都有角色 id（P3）。
        let got = drain_urls(&seen, 4);
        let urls: Vec<&str> = got.iter().map(|(u, _)| u.as_str()).collect();
        assert_eq!(
            urls,
            vec![
                format!("/v2/characters/{CHAR}/orders/"),
                format!("/v1/characters/{CHAR}/wallet/transactions/"),
                format!("/v1/characters/{CHAR}/wallet/journal/"),
                format!("/v4/characters/{CHAR}/skills/"),
            ]
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
        );
        for (url, auth) in &got {
            assert_eq!(
                auth.as_deref(),
                Some("Bearer SECRET-ACCESS-TOKEN"),
                "{url} 没带令牌"
            );
        }
    }

    #[tokio::test]
    async fn a_round_where_everything_fails_changes_nothing_in_the_db() {
        // 令牌过期/断网：四条全灭。此时**不能**刷新 last_sync_at（"刚同步过"却拿着旧数据
        // 是比不刷新更坏的谎）、不能清掉水位、更不能把上一轮的挂单快照替换成空表。
        let (base, _seen) = char_stub(
            vec![
                Route::fail("/orders/", 403),
                Route::fail("/wallet/transactions/", 403),
                Route::fail("/wallet/journal/", 403),
                Route::fail("/skills/", 403),
            ],
            4,
        );
        let client = client_at(&base);
        let db = Db::in_memory().unwrap();
        db.upsert_char_meta(CHAR, "Pilot One", 0).unwrap();
        db.set_char_cursors(
            CHAR,
            Some("2026-09-19T00:00:00Z"),
            Some("2026-09-18T00:00:00Z"),
            Some("Wed, 17 Sep 2026 00:00:00 GMT"),
        )
        .unwrap();
        db.replace_char_orders(
            CHAR,
            &[CharOrder {
                order_id: 77,
                type_id: 34,
                location_id: 60_003_760,
                is_buy: false,
                price: 9.0,
                volume_remain: 3,
                issued: "2026-09-10T00:00:00Z".into(),
                duration: 90,
                fetched_at: 1,
            }],
            1,
        )
        .unwrap();
        db.upsert_char_tx(
            CHAR,
            &[WalletTx {
                transaction_id: 4001,
                date: "2026-09-15T00:00:00Z".into(),
                type_id: 34,
                location_id: 60_003_760,
                is_buy: true,
                unit_price: 3.0,
                quantity: 7,
            }],
        )
        .unwrap();

        let report = sync_character(&client, TOKEN, &db, CHAR, now()).await.unwrap();

        assert!(!report.orders.ok && !report.transactions.ok);
        assert!(!report.journal.ok && !report.skills.ok);
        for out in [&report.orders, &report.transactions, &report.journal] {
            assert!(
                out.error.as_deref().unwrap().contains("403"),
                "失败原因要能看见：{out:?}"
            );
        }
        assert!(report.journal_entries.is_empty() && report.skill_levels.is_empty());

        assert_eq!(db.load_char_orders(CHAR).unwrap().len(), 1, "快照不得被清空");
        assert_eq!(db.load_char_tx(CHAR, None).unwrap().len(), 1, "旧流水不得被裁掉");
        let meta = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(meta.tx_cursor.as_deref(), Some("2026-09-19T00:00:00Z"));
        assert_eq!(meta.journal_cursor.as_deref(), Some("2026-09-18T00:00:00Z"));
        assert_eq!(meta.orders_lm.as_deref(), Some("Wed, 17 Sep 2026 00:00:00 GMT"));
        assert_eq!(meta.last_sync_at, Some(0), "全灭的一轮不算'同步过'");
    }

    #[tokio::test]
    async fn second_round_filters_by_the_stored_watermark_and_only_moves_it_forward() {
        let first_round = vec![
            Route::ok_with_lm("/orders/", ORDERS_JSON, "Wed, 17 Sep 2026 00:00:00 GMT"),
            Route::ok("/wallet/transactions/", TX_JSON),
            Route::ok("/wallet/journal/", JOURNAL_JSON),
            Route::ok("/skills/", SKILLS_JSON),
            // 第二轮：orders 不再回 Last-Modified（304/缺头都长这样），其余照旧换新数据。
            Route::ok(
                "/orders/",
                r#"[{"order_id":202,"type_id":35,"location_id":60003760,"is_buy_order":true,
                    "price":9.5,"volume_remain":4,"issued":"2026-09-21T07:00:00Z","duration":30}]"#,
            ),
            Route::ok(
                "/wallet/transactions/",
                r#"[{"transaction_id":6001,"date":"2026-09-19T00:00:00Z","type_id":35,
                    "location_id":60003760,"is_buy":true,"unit_price":1.0,"quantity":1},
                    {"transaction_id":6002,"date":"2026-09-21T08:00:00Z","type_id":35,
                    "location_id":60003760,"is_buy":false,"unit_price":12.0,"quantity":5}]"#,
            ),
            Route::ok(
                "/wallet/journal/",
                r#"[{"id":9002,"date":"2026-09-21T08:05:00Z","ref_type":"transaction_tax",
                    "amount":-40.0,"description":"Transaction Tax"}]"#,
            ),
            Route::ok("/skills/", SKILLS_JSON),
        ];
        let (base, seen) = char_stub(first_round, 8);
        let client = client_at(&base);
        let db = Db::in_memory().unwrap();
        db.upsert_char_meta(CHAR, "Pilot One", 0).unwrap();
        // 库里先留一行窗外的老流水：本轮必须被 `prune_char_tx` 剪掉。
        db.upsert_char_tx(
            CHAR,
            &[WalletTx {
                transaction_id: 4001,
                date: "2026-05-01T00:00:00Z".into(),
                type_id: 34,
                location_id: 60_003_760,
                is_buy: true,
                unit_price: 3.0,
                quantity: 7,
            }],
        )
        .unwrap();

        sync_character(&client, TOKEN, &db, CHAR, now()).await.unwrap();
        let meta = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(meta.tx_cursor.as_deref(), Some("2026-09-20T11:00:00Z"));
        assert_eq!(meta.orders_lm.as_deref(), Some("Wed, 17 Sep 2026 00:00:00 GMT"));
        let ids: Vec<i64> = db
            .load_char_tx(CHAR, None)
            .unwrap()
            .iter()
            .map(|t| t.transaction_id)
            .collect();
        assert_eq!(ids, vec![5002], "窗外那行既没被拉回来，也把库里原有的老行剪掉了");

        // 第二轮的请求不能命中第一轮的缓存（否则测的还是第一轮），把它们显式过期。
        for path in [
            orders_path(CHAR),
            transactions_path(CHAR),
            journal_path(CHAR),
            skills_path(CHAR),
        ] {
            assert!(client.expire_now(&path), "{path} 本该有缓存条目");
        }

        let report = sync_character(&client, TOKEN, &db, CHAR, now()).await.unwrap();

        // 水位只前进：6001（2026-09-19）早于上轮水位，被 `date >= 水位` 挡在库外。
        assert_eq!(report.transactions.rows, 1, "只有 6002 落在窗口里");
        let meta = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(meta.tx_cursor.as_deref(), Some("2026-09-21T08:00:00Z"));
        assert_eq!(meta.journal_cursor.as_deref(), Some("2026-09-21T08:05:00Z"));
        assert_eq!(
            meta.orders_lm.as_deref(),
            Some("Wed, 17 Sep 2026 00:00:00 GMT"),
            "本轮响应没带 Last-Modified → 水位保持原样（None = 不动列）"
        );
        let ids: Vec<i64> = db
            .load_char_tx(CHAR, None)
            .unwrap()
            .iter()
            .map(|t| t.transaction_id)
            .collect();
        assert_eq!(ids, vec![5002, 6002]);
        // 挂单是整表覆盖：第二轮只有一行，上一轮那行必须消失。
        let orders = db.load_char_orders(CHAR).unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, 202);
        assert_eq!(report.journal_entries.len(), 1);
        assert_eq!(report.journal_entries[0].id, 9002);
        assert_eq!(report.skill_levels.len(), 2, "第二轮读到两条技能");
        let _ = drain_urls(&seen, 8);
    }
}
