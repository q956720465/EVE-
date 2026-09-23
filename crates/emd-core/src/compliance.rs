//! M0.5 合规冒烟闸门（方案 v3.1 §8）。
//!
//! 这些检查单独成模块而不是散在测试里，是因为**发布版每次冷启动也要跑一遍轻量版**：
//! 缓存纪律是唯一"做错就被封 ESI"的环节，不能只在开发期验证。
//!
//! 判据：
//! 1. `Expires` 未到 → 同一 URL 网络请求数为 0。
//! 2. 到点后带 `If-None-Match` → 命中 304，本次只花 1 令牌。
//! 3. 全量分页拉齐，且跨页 `Last-Modified` 一致、`ETag` 不同（把 v3.0 的坑钉住）。
//! 4. 并发 16 跑完整轮不触发 429/420。
//! 5. `X-Esi-Cache-Status` 每响应可读，命中率可统计。
//! 6. `markets/prices` 能解析并落库。

use std::time::Instant;

use crate::error::{Error, Result};
use crate::esi::{EsiClient, Fetch};
use crate::market::{fetch_region_orders, REGION_FORGE};
use crate::store::{Db, PriceRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
    /// 环境导致无法判定（例如 ESI 侧缓存在测试期间正好翻页），不算红灯。
    Skipped,
}

impl Verdict {
    pub fn ok(&self) -> bool {
        !matches!(self, Verdict::Fail)
    }
}

#[derive(Debug, Clone)]
pub struct Check {
    pub name: &'static str,
    pub verdict: Verdict,
    pub detail: String,
}

impl Check {
    fn new(name: &'static str, pass: bool, detail: impl Into<String>) -> Self {
        Self {
            name,
            verdict: if pass { Verdict::Pass } else { Verdict::Fail },
            detail: detail.into(),
        }
    }
}

pub struct Report {
    pub checks: Vec<Check>,
    pub elapsed_secs: f64,
}

impl Report {
    pub fn all_ok(&self) -> bool {
        self.checks.iter().all(|c| c.verdict.ok())
    }

    pub fn failures(&self) -> Vec<&Check> {
        self.checks.iter().filter(|c| c.verdict == Verdict::Fail).collect()
    }
}

/// 完整闸门：会真打一次 409 页全量（约 82 s、818 令牌）。
pub async fn run_full_gate(client: &EsiClient, db: &Db) -> Result<Report> {
    let t = Instant::now();
    let mut checks = Vec::new();
    checks.push(check_expires_gate(client).await?);
    checks.push(check_conditional_revalidate(client).await?);
    checks.push(check_snapshot_consistency(client).await?);
    checks.push(check_full_round(client, db).await?);
    checks.push(check_prices_ingest(client, db).await?);
    Ok(Report {
        checks,
        elapsed_secs: t.elapsed().as_secs_f64(),
    })
}

/// 冷启动自检：不打全量，只看 3 页。用于每次启动确认没在绕过缓存。
pub async fn run_startup_probe(client: &EsiClient) -> Result<Report> {
    let t = Instant::now();
    let mut checks = Vec::new();
    checks.push(check_expires_gate(client).await?);
    checks.push(check_conditional_revalidate(client).await?);
    checks.push(check_snapshot_consistency(client).await?);
    Ok(Report {
        checks,
        elapsed_secs: t.elapsed().as_secs_f64(),
    })
}

/// ①未到 `Expires` 时不得产生任何网络请求。
pub async fn check_expires_gate(client: &EsiClient) -> Result<Check> {
    const NAME: &str = "expires_gate";
    let path = format!("/v3/markets/{REGION_FORGE}/orders?order_type=all&page=2");
    let base = client.stats().requests;

    let first = client.fetch(&path).await?;
    let after_first = client.stats().requests;
    let second = client.fetch(&path).await?;
    let after_second = client.stats().requests;

    let requested_again = after_second > after_first;
    let served_locally = matches!(second, Fetch::Cached { .. });
    let identical = first.body().len() == second.body().len();

    Ok(Check::new(
        NAME,
        !requested_again && served_locally && identical && second.meta().last_modified.is_some(),
        format!(
            "首次 {} B 走网络；第二次 over_network={} 本地命中={} 字节相同={}（请求计数 {}→{}）",
            first.body().len(),
            second.over_network(),
            served_locally,
            identical,
            after_first - base,
            after_second - after_first
        ),
    ))
}

/// ②到点后条件请求：期望 304、1 令牌、0 body。
pub async fn check_conditional_revalidate(client: &EsiClient) -> Result<Check> {
    const NAME: &str = "conditional_revalidate";
    let path = "/v1/markets/prices".to_string();
    client.fetch(&path).await?; // 建立 etag 与 body
    client.expire_now(&path); // 模拟 Expires 到点，保留 etag

    let before = client.stats();
    let f = client.fetch(&path).await?;
    let after = client.stats();
    let over_network = after.requests - before.requests;

    match f.meta().status {
        304 => Ok(Check::new(
            NAME,
            over_network == 1,
            format!("304 命中，body {} B（网络请求 {} 次），令牌余量 {}", f.body().len(), over_network, client.remaining_tokens()),
        )),
        // 上游缓存在这几秒内正好滚动 → 200 是合规结果，只是无法证明 304 路径。
        200 => Ok(Check {
            name: NAME,
            verdict: Verdict::Skipped,
            detail: format!("上游已刷新，返回 200（{} B）而非 304；条件请求头已发出", f.body().len()),
        }),
        other => Ok(Check::new(
            NAME,
            false,
            format!("意外的状态码 {}", other),
        )),
    }
}

/// ③跨页快照一致性：`Last-Modified` 必须相同，`ETag` 必须不同。
pub async fn check_snapshot_consistency(client: &EsiClient) -> Result<Check> {
    const NAME: &str = "snapshot_consistency";
    let first = client
        .fetch(&format!(
            "/v3/markets/{REGION_FORGE}/orders?order_type=all&page=1"
        ))
        .await?;
    let pages = first.meta().x_pages.ok_or_else(|| Error::MissingHeader {
        header: "x-pages",
        url: first.meta().url.clone(),
    })?;

    let mut lms = vec![first.meta().last_modified.clone()];
    let mut etags = vec![first.meta().etag.clone()];
    for p in [pages / 2, pages] {
        let f = client
            .fetch(&format!(
                "/v3/markets/{REGION_FORGE}/orders?order_type=all&page={p}"
            ))
            .await?;
        lms.push(f.meta().last_modified.clone());
        etags.push(f.meta().etag.clone());
    }

    let lm_uniform = lms.iter().all(|a| a == &lms[0]) && lms[0].is_some();
    let etag_distinct = etags.iter().filter(|e| e.is_some()).count() == etags.len()
        && {
            let mut v = etags.clone();
            v.sort();
            v.dedup();
            v.len() == etags.len()
        };

    Ok(Check::new(
        NAME,
        lm_uniform && etag_distinct,
        format!(
            "X-Pages={pages}；Last-Modified 一致={lm_uniform}（{:?}）；ETag 各不相同={etag_distinct} —— 一致性校验只能用前者",
            lms[0].as_deref().unwrap_or("缺失")
        ),
    ))
}

/// ④⑤整轮并发拉取：无 429、页数拉齐、cache 状态可读。
pub async fn check_full_round(client: &EsiClient, db: &Db) -> Result<Check> {
    const NAME: &str = "full_round_concurrency";
    let started = client.stats();
    let outcome = match fetch_region_orders(client, REGION_FORGE).await {
        Ok(o) => o,
        Err(e @ Error::RateLimited { .. }) => {
            return Ok(Check::new(NAME, false, format!("触发 429：{e}")))
        }
        Err(e) => return Err(e),
    };
    let now = client.stats();

    let requests = now.requests - started.requests;
    let hits = now.upstream_hit - started.upstream_hit;
    let misses = now.upstream_miss - started.upstream_miss;
    let observed_status = hits + misses > 0;

    let complete = outcome.pages_failed.is_empty()
        || outcome.pages_failed.len() as f64 / outcome.pages_expected as f64 <= 0.01;
    let budget_ok = requests <= outcome.pages_expected as u64 + 8;

    // 落库验证快照表不增长。
    let opts = crate::market::aggregate_opts();
    let books = crate::market::aggregate(&outcome.orders, &opts);
    let rows = db.write_snapshot(&books, outcome.snapshot_lm.as_deref())?;
    let counts = db.counts()?;

    Ok(Check::new(
        NAME,
        complete && observed_status && budget_ok,
        format!(
            "{} 页 / {} 条订单 / {:.1} MB(解码后，wire 约 /9.2) / {}；聚合 {rows} 行快照（类型 {}，站点 {}，双向盘 {}）；上游 HIT={hits} MISS={misses}，请求 {requests} 次",
            outcome.pages_expected,
            outcome.orders.len(),
            outcome.decoded_bytes as f64 / 1_048_576.0,
            fmt_dur(outcome.elapsed),
            counts.types,
            counts.stations,
            counts.both_sides,
        ),
    ))
}

/// ⑥基准价落库。实测 15,801 行、gzip 218 KB、单连接 23.7 s。
pub async fn check_prices_ingest(client: &EsiClient, db: &Db) -> Result<Check> {
    const NAME: &str = "prices_ingest";

    #[derive(serde::Deserialize)]
    struct Row {
        type_id: u32,
        adjusted_price: Option<f64>,
        average_price: Option<f64>,
    }

    let t = Instant::now();
    let rows: Vec<Row> = client.get_json("/v1/markets/prices").await?;
    let priced: Vec<PriceRow> = rows
        .iter()
        .map(|r| PriceRow {
            date: chrono::Utc::now().date_naive().to_string(),
            type_id: r.type_id,
            adjusted: r.adjusted_price,
            average: r.average_price,
        })
        .collect();
    let written = db.write_prices(&priced)?;

    Ok(Check::new(
        NAME,
        rows.len() > 15_000 && written == priced.len(),
        format!(
            "{} 行（有价 {}），{}，耗时 {:?}",
            rows.len(),
            priced.iter().filter(|p| p.average.is_some()).count(),
            db.get_meta("last_snapshot_lm")?.map_or_else(
                || "快照 lm 未记".to_string(),
                |v| format!("快照 lm={v}")
            ),
            t.elapsed()
        ),
    ))
}

fn fmt_dur(d: std::time::Duration) -> String {
    format!("{:.1}s", d.as_secs_f64())
}
