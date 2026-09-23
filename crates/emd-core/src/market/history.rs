//! 区域历史日线 —— 方案 v3.1 §3.3 的 L0/L1 取数层。
//!
//! 2026-09-23 本机实测的六条事实，全部写进了下面的常量与分支：
//!
//! 1. `GET /v1/markets/{region}/history?type_id=` 一次返回 **418 天**
//!    （`2025-08-01 … 2026-09-22`），gzip 7,925 B，0.79 s。所以"回填一年"和
//!    "每日增量"是同一个请求 —— 不存在独立的回填批次，第一趟就是满的。
//! 2. 响应字段是 `average/highest/lowest/order_count/volume`，日期是纯 `YYYY-MM-DD`。
//!    §5 纸面写的 `high/low` 是错的，落库列名以实测为准。
//! 3. `Expires` = 次日 11:05 UTC、`Last-Modified` = 当日 11:05:49 → 每日只有一份新数据，
//!    早于 11:05 去取拿不到昨天。
//! 4. **不带任何 `X-Ratelimit-*` 头**。用交叉实验确认它不占 `market-order` 组：
//!    40 次 history 200 与 5 次 404 之间，orders 的 `X-Ratelimit-Remaining` 只按自身
//!    每笔 2 令牌递减（11987 → 11985）。这结掉了附录 D 的第 4 条。
//! 5. 🔴 无成交的 `(region, type)` 返回 **404 `Type not found!`，不是空数组**，
//!    且 404 不带 `Expires`。每个 404 让 `X-Esi-Error-Limit-Remain` 减 1（实测 100→94）。
//!    因此绝不能拿"全部类型"去盲扫，且必须把"确认无历史"记进闸门。
//! 6. 内存缓存的 TTL 被夹在 `[5 s, 360 s]`（那是为 300 s 的订单簿设的），装不下 24 h
//!    的有效期 → 这条闸门必须落在 `sync_state` 里，进程重启也拦得住。

use std::time::{Duration, Instant};

use chrono::Timelike;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::esi::{EsiClient, FetchMeta};
use crate::market::REGION_FORGE;
use crate::store::{Db, HistoryPass, HistoryRow, HistoryTarget, SyncState};

pub const TIER_L0: &str = "L0";
pub const TIER_L1: &str = "L1";

/// 实测上游滚动窗口（天）。UI 的"已积累 x/418 天"用它，不是 §3.3 估的 365。
pub const ESI_WINDOW_DAYS: u32 = 418;
/// 本地多留几天：裁到刚好 418 会让边界日期在时钟抖动下永久缺失。
pub const KEEP_DAYS: i64 = 425;
/// 404 的复核周期。太短会持续烧全局错误限额，太长会让"刚开始有成交"的类型迟迟不出曲线。
pub const ABSENT_TTL: i64 = 7 * 86_400;
/// 200 但响应里没有 `Expires` 时的兜底闸门 —— 300 s 是 ESI 的公开缓存下限。
pub const FALLBACK_TTL: i64 = 300;

/// §3.3 的 L0 上限（也是 §3.1 给 T3 的 1 200 请求预算）。
pub const L0_DAILY_CAP: u32 = 1_200;
/// §3.3：L1 池可达 2 000 个类型，但每日只滚动补 300。
pub const L1_DAILY_CAP: u32 = 300;

/// 上游刷新点与 T3 计划时刻（§3.1：每日 11:20 UTC，让开 11:15 的 T2）。
pub const REFRESH_AT: (u32, u32) = (11, 5);
pub const T3_AT: (u32, u32) = (11, 20);

/// 连续这么多个目标全部 404 就中止这一趟。星域 ID 写错时 ESI 也是逐个 404
/// （实测 `region=10000087` → `Region not found`），不拦就会一口气把 1 200 个
/// 请求全打完，每个再扣 1 点全局错误限额。
pub const ALL_MISSING_ABORT: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct HistoryConfig {
    pub region_id: u32,
    pub l0_cap: u32,
    pub l1_daily: u32,
    /// 本次最多处理多少个目标。CLI 试跑用；None = 按上限全跑。
    pub limit: Option<u32>,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            region_id: REGION_FORGE,
            l0_cap: L0_DAILY_CAP,
            l1_daily: L1_DAILY_CAP,
            limit: None,
        }
    }
}

impl HistoryConfig {
    /// `EMD_HISTORY_CAP=0` 整体关掉每日回填（开发与回归时用，默认开）。
    pub fn from_env() -> Self {
        let cap = std::env::var("EMD_HISTORY_CAP")
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok());
        match cap {
            Some(0) => Self {
                l0_cap: 0,
                l1_daily: 0,
                ..Default::default()
            },
            Some(n) => Self {
                l0_cap: n,
                ..Default::default()
            },
            None => Self::default(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.l0_cap + self.l1_daily > 0
    }

    /// 计划请求数上界，用于回填前把成本说清楚（§3.3 要求设置页明示耗时）。
    pub fn budget(&self) -> u32 {
        self.l0_cap.saturating_add(self.l1_daily)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct HistoryPoint {
    #[serde(default)]
    average: Option<f64>,
    date: String,
    #[serde(default)]
    highest: Option<f64>,
    #[serde(default)]
    lowest: Option<f64>,
    #[serde(default)]
    order_count: u64,
    #[serde(default)]
    volume: u64,
}

/// 单个目标的结果。`Gated` 与 `Absent` 都**不**消耗请求。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Fetched {
    /// 未到 `Expires`：0 请求、0 令牌。
    Gated,
    /// 200。`rows` 是落库天数，0 表示上游给了空数组。
    /// `over_network` 区分真发出去的与内存缓存供给的（合规计数只数前者）。
    Fresh {
        rows: u32,
        bytes: u64,
        over_network: bool,
    },
    /// 404：确认该 (region, type) 无历史，已记 7 天复核周期。
    Absent,
}

/// 一趟回填的实测台账。
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PassReport {
    pub targets: u32,
    pub requested: u32,
    pub served_from_cache: u32,
    pub gated: u32,
    pub fresh: u32,
    pub rows_written: u32,
    pub absent: u32,
    pub failed: u32,
    /// 因早停而未处理的目标（`targets` 减去已判定的）。
    pub skipped: u32,
    pub seconds: f64,
    pub decoded_bytes: u64,
    /// 本地令牌桶的实际递减量。服务端不计这笔（见模块头第 4 条），
    /// 仍记账是有意保留的安全余量：多进程同时跑时本地估不到对方。
    pub tokens_local: u32,
    pub error_remain_min: Option<u32>,
    pub status: String,
}

impl PassReport {
    /// 每秒请求数。回填耗时与池子大小的换算就用它。
    pub fn rps(&self) -> f64 {
        if self.seconds <= 0.0 {
            return 0.0;
        }
        self.requested as f64 / self.seconds
    }

    pub fn per_request_ms(&self) -> f64 {
        if self.requested == 0 {
            return 0.0;
        }
        self.seconds * 1000.0 / self.requested as f64
    }

    /// 按 `rps()` 外推跑完 `n` 个目标要多久。
    pub fn est_seconds(&self, n: u32) -> f64 {
        let r = self.rps();
        if r <= 0.0 {
            return 0.0;
        }
        n as f64 / r
    }
}

pub fn path(region_id: u32, type_id: u32) -> String {
    format!("/v1/markets/{region_id}/history?type_id={type_id}")
}

/// ESI 给的是 `"2026-09-22"`。按兼容日期机制某些版本会带时间（`"… 11:05:00"`），
/// 取前 10 个字符作主键日期，避免同一天的数据落成两行。
fn norm_date(raw: &str) -> String {
    raw.chars().take(10).collect()
}

fn to_rows(region_id: u32, type_id: u32, pts: Vec<HistoryPoint>) -> Vec<HistoryRow> {
    pts.into_iter()
        .map(|p| HistoryRow {
            region_id,
            type_id,
            date: norm_date(&p.date),
            average: p.average,
            highest: p.highest,
            lowest: p.lowest,
            volume: p.volume,
            order_count: p.order_count,
        })
        .collect()
}

/// 到点才发、发完把真实 `Expires` 写回闸门。
///
/// 三条实测决定的分支：
/// - 404 是答案不是故障（模块头第 5 条），单独记一个较长的复核周期；
/// - 传输层/5xx 失败**不写闸门**，所以下一趟可以立刻重试，但也不会当天重试第二次
///   （每日趟本身由 `history_due` 拦住）；
/// - 成功后把 body 从内存缓存里丢掉：43 KB × 1 200 个会把 48 MB 的缓存整个冲掉，
///   而闸门已落库、数据已进 `market_history`，那份 body 再无用处。
pub async fn fetch_one(
    client: &EsiClient,
    db: &Db,
    region_id: u32,
    type_id: u32,
    now: i64,
) -> Result<(Fetched, Option<u32>)> {
    let key = path(region_id, type_id);
    if !db.sync_due(&key, now)? {
        return Ok((Fetched::Gated, None));
    }

    let f = match client.fetch(&key).await {
        Ok(f) => f,
        Err(Error::Status { status: 404, .. }) => {
            db.note_history_absent(&key, now + ABSENT_TTL)?;
            return Ok((Fetched::Absent, None));
        }
        Err(e) => return Err(e),
    };
    let error_remain = f.meta().watermark.error_remain;
    let over_network = f.over_network();

    let pts: Vec<HistoryPoint> = f.json()?;
    let bytes = f.body().len() as u64;
    let rows = to_rows(region_id, type_id, pts);
    let written = db.write_history(&rows)? as u32;
    persist_gate(db, &key, f.meta(), now)?;
    client.invalidate(&key);

    Ok((
        Fetched::Fresh {
            rows: written,
            bytes,
            over_network,
        },
        error_remain,
    ))
}

fn persist_gate(db: &Db, key: &str, meta: &FetchMeta, now: i64) -> Result<()> {
    let until = meta.expires_unix().unwrap_or(now + FALLBACK_TTL);
    // 上游若给了一个"过去"的 `Expires`（时钟异常或缓存穿透），至少留 60 s 冷却。
    // 不夹的话闸门等于常开，同一趟里就能把这个 URL 反复打出去。
    let until = until.max(now + 60);
    db.record_sync_ok(&SyncState {
        url_key: key.to_string(),
        endpoint: "markets-history".into(),
        etag: meta.etag.clone(),
        last_modified: meta.last_modified.clone(),
        expires_raw: meta.expires_raw.clone(),
        expires_at_unix: Some(until),
        fail_streak: 0,
    })
}

/// 跑一趟。目标顺序即 L0 自选 → L0 流动池 → L1 补齐，全部走同一并发度。
///
/// 并发沿用订单簿实测的饱和点 16（附录 A.1）。这里不重开一条采集通道：
/// 与 T1 共用 `EsiClient` 才拿得到同一个令牌桶和水位。
///
/// 用 `while let … next()` 而不是 `collect()`：连续 404 要能**中途**收手（见
/// `ALL_MISSING_ABORT`）。先 collect 再判等于 1 200 个请求全打完了才发现星域写错。
pub async fn backfill(
    client: &EsiClient,
    db: &Db,
    cfg: &HistoryConfig,
    targets: &[HistoryTarget],
) -> Result<PassReport> {
    let started_at = crate::store::now_unix();
    let t0 = Instant::now();
    let tokens0 = client.remaining_tokens();
    let mut rep = PassReport {
        targets: targets.len() as u32,
        ..Default::default()
    };
    if !cfg.enabled() {
        rep.status = "disabled".into();
        return Ok(rep);
    }
    if targets.is_empty() {
        rep.status = "no-targets".into();
        return Ok(rep);
    }

    let concurrency = client.config().concurrency.max(1);
    let jitter = client.config().page_jitter;
    let stream = stream::iter(targets.iter().cloned().enumerate())
        .map(|(i, t)| async move {
            // 抖动落在发请求之前，与订单簿同一套（A.7 缺陷 1）。
            tokio::time::sleep(jitter + Duration::from_millis((i % 7) as u64 * 10)).await;
            (
                t.type_id,
                fetch_one(client, db, cfg.region_id, t.type_id, started_at).await,
            )
        })
        .buffer_unordered(concurrency);
    futures::pin_mut!(stream);

    let mut missing_streak = 0usize;
    while let Some((type_id, r)) = stream.next().await {
        match r {
            Ok((Fetched::Gated, _)) => rep.gated += 1,
            Ok((Fetched::Absent, _)) => {
                rep.absent += 1;
                missing_streak += 1;
                if missing_streak >= ALL_MISSING_ABORT {
                    rep.status = format!(
                        "aborted: 连续 {missing_streak} 个目标 404，星域 ID {} 可能有误",
                        cfg.region_id
                    );
                    tracing::error!("{}", rep.status);
                    break;
                }
            }
            Ok((Fetched::Fresh { rows, bytes, over_network }, er)) => {
                missing_streak = 0;
                note_error_remain(&mut rep, er);
                rep.fresh += 1;
                if over_network {
                    rep.requested += 1;
                } else {
                    rep.served_from_cache += 1;
                }
                rep.rows_written += rows;
                rep.decoded_bytes += bytes;
            }
            Err(e) => {
                rep.failed += 1;
                tracing::warn!("history type {type_id} 取数失败：{e}");
            }
        }
    }
    rep.skipped = rep
        .targets
        .saturating_sub(rep.fresh + rep.absent + rep.gated + rep.failed);

    rep.seconds = t0.elapsed().as_secs_f64();
    rep.tokens_local = tokens0.saturating_sub(client.remaining_tokens());
    if rep.status.is_empty() {
        rep.status = if rep.fresh == 0 && rep.requested == 0 && rep.served_from_cache == 0 {
            // 全部被闸门挡住 ≠ 跑成功了。台账要能看出这一趟一个请求都没发。
            if rep.gated > 0 {
                "all-gated".into()
            } else {
                "no-op".into()
            }
        } else if rep.failed > 0 {
            format!("ok_with_errors: {} 个失败", rep.failed)
        } else {
            "ok".into()
        };
    }
    let pruned = db.prune_history(KEEP_DAYS)?;
    if pruned > 0 {
        tracing::info!("历史裁剪：删掉 {pruned} 行超过 {KEEP_DAYS} 天的日线");
    }
    db.record_history_pass(&HistoryPass {
        started_at,
        region_id: cfg.region_id,
        targets: rep.targets,
        requested: rep.requested,
        gated: rep.gated,
        rows_written: rep.rows_written,
        absent: rep.absent,
        failed: rep.failed,
        seconds: rep.seconds,
        decoded_bytes: rep.decoded_bytes,
        tokens_local: rep.tokens_local,
        error_remain: rep.error_remain_min,
        status: rep.status.clone(),
    })?;
    Ok(rep)
}

fn note_error_remain(rep: &mut PassReport, er: Option<u32>) {
    if let Some(v) = er {
        rep.error_remain_min = Some(rep.error_remain_min.map_or(v, |m| m.min(v)));
    }
}

/// 今天该不该跑这一趟。两个条件缺一不可：
/// - 过了 11:20 UTC（早于 11:05 上游还没出昨天的日线，跑了也是白跑）；
/// - 今天还没跑成功过（`sync_state` 是逐 URL 的，这一条管的是"整趟"）。
pub fn history_due(last_day: Option<&str>, now_utc: &chrono::DateTime<chrono::Utc>) -> bool {
    let (h, m) = T3_AT;
    let after_slot = now_utc.hour() > h || (now_utc.hour() == h && now_utc.minute() >= m);
    let today = now_utc.date_naive().to_string();
    after_slot && last_day.map(|d| d != today).unwrap_or(true)
}

/// 一趟的成本预估，给 CLI 与设置页在动手之前把话说清楚（§3.3"设置页明示耗时"）。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Estimate {
    pub targets: u32,
    pub requests: u32,
    pub tokens_local: u32,
    /// 上游窗口 418 天 × 目标数 = 本地日线行数上限。
    pub history_rows: u64,
    pub seconds_at_measured_rps: f64,
}

/// 冷估吞吐。2026-09-23 本机直连实测两档：进程内第一批 40 个请求 **26.3 请求/s**
/// （连接池未热），随后 1 196 个请求的一趟 **44.3 请求/s**（池热 + 上游全 HIT）。
/// 设置页与 CLI 的预估取保守值 25，实测过一趟之后 `Estimate` 会自动改用实测 rps。
pub const COLD_RPS: f64 = 25.0;

impl Estimate {
    pub fn of(targets: u32, measured_rps: Option<f64>) -> Self {
        let rps = measured_rps.filter(|r| *r > 0.0).unwrap_or(COLD_RPS);
        Self {
            targets,
            requests: targets,
            tokens_local: targets * crate::config::cost::OK,
            history_rows: targets as u64 * ESI_WINDOW_DAYS as u64,
            seconds_at_measured_rps: targets as f64 / rps,
        }
    }
}

/// 附录 D 第 4 条的结案实验：`history` 到底占不占 `market-order` 的令牌窗口？
///
/// 单看 history 的响应头是得不出结论的（它压根不带 `X-Ratelimit-*`），所以用交叉观测：
/// 读一次 orders 的 `Remaining` → 打 N 次 history → 再读一次 orders 的 `Remaining`。
/// 若 history 计费，两次之差会多出 `2N`；实测只差 2（即后一次 orders 自身的成本）。
///
/// 顺带把 404 的错误限额代价一起量出来（每个 404 让 `X-Esi-Error-Limit-Remain` 减 1），
/// 这是"不能盲扫全部类型"那条结论的唯一凭据。
#[derive(Debug, Clone, Default, Serialize)]
pub struct ProbeReport {
    pub history_samples: u32,
    pub history_ok: u32,
    pub history_404: u32,
    pub history_group_header: Option<String>,
    pub orders_group: Option<String>,
    pub orders_limit: Option<String>,
    pub remaining_before: Option<u32>,
    pub remaining_after: Option<u32>,
    pub error_remain_before: Option<u32>,
    pub error_remain_after: Option<u32>,
    pub rows_per_request: u32,
    pub bytes_per_request: u64,
    pub seconds: f64,
    pub verdict: String,
}

impl ProbeReport {
    /// orders 两次读数之差。2 = 只有后一次 orders 自身，history 未计费。
    pub fn orders_delta(&self) -> Option<u32> {
        match (self.remaining_before, self.remaining_after) {
            (Some(b), Some(a)) => Some(b.saturating_sub(a)),
            _ => None,
        }
    }

    pub fn history_charges_market_order(&self) -> Option<bool> {
        // 每个 2xx 的 orders 成本是 2 令牌（实测）。扣掉后一次自己的 2，剩下的就是
        // history 的账。允许 1 令牌的窗口边界抖动。
        let d = self.orders_delta()?;
        Some(d > crate::config::cost::OK + 1)
    }
}

pub async fn probe(client: &EsiClient, types: &[u32], region_id: u32) -> Result<ProbeReport> {
    let t0 = Instant::now();
    let mut rep = ProbeReport {
        history_samples: types.len() as u32,
        ..Default::default()
    };
    if types.is_empty() {
        return Err(Error::Config("probe 需要至少一个类型 ID".into()));
    }

    let leg = format!("/v3/markets/{region_id}/orders?order_type=all&page=251");
    let first = client.fetch(&leg).await?;
    rep.orders_group = first.meta().watermark.group.clone();
    rep.orders_limit = first.meta().watermark.limit.clone();
    rep.remaining_before = first.meta().watermark.remaining;
    rep.error_remain_before = first.meta().watermark.error_remain;

    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    for ty in types {
        let key = path(region_id, *ty);
        match client.fetch(&key).await {
            Ok(f) => {
                rep.history_ok += 1;
                rep.history_group_header = f.meta().watermark.group.clone();
                let pts: Vec<HistoryPoint> = f.json().unwrap_or_default();
                total_rows += pts.len() as u64;
                total_bytes += f.body().len() as u64;
                rep.error_remain_after = f.meta().watermark.error_remain;
                client.invalidate(&key);
            }
            Err(Error::Status { status: 404, .. }) => rep.history_404 += 1,
            Err(e) => return Err(e),
        }
    }
    rep.rows_per_request = if rep.history_ok > 0 {
        (total_rows / rep.history_ok as u64) as u32
    } else {
        0
    };
    rep.bytes_per_request = if rep.history_ok > 0 {
        total_bytes / rep.history_ok as u64
    } else {
        0
    };

    let leg2 = format!("/v3/markets/{region_id}/orders?order_type=all&page=252");
    let last = client.fetch(&leg2).await?;
    rep.remaining_after = last.meta().watermark.remaining;
    rep.error_remain_after = last.meta().watermark.error_remain.or(rep.error_remain_after);
    rep.seconds = t0.elapsed().as_secs_f64();

    rep.verdict = match rep.history_charges_market_order() {
        None => "无法判定：orders 两次读数缺失（多半是命中了本地缓存）".into(),
        Some(false) => format!(
            "history 不占 {} 组：{} 次请求只让 orders 的 Remaining 减 {}（= 后一次 orders 自身的成本）",
            rep.orders_group.as_deref().unwrap_or("?"),
            types.len(),
            rep.orders_delta().unwrap_or(0)
        ),
        Some(true) => format!(
            "history 占用 {} 组：两次 orders 读数差 {}，远超单笔成本",
            rep.orders_group.as_deref().unwrap_or("?"),
            rep.orders_delta().unwrap_or(0)
        ),
    };
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{aggregate, AggregateOptions, Order};
    use crate::store::Db;
    use chrono::{TimeZone, Utc};

    /// 2026-09-23 18:27 UTC 实测原样录制的响应头与 body 片段。
    const RECORDED: &str = r#"[{"average":3.85,"date":"2025-08-01","highest":3.85,"lowest":3.84,"order_count":2374,"volume":3089041705},{"average":3.94,"date":"2026-09-22","highest":4.03,"lowest":3.79,"order_count":1447,"volume":5758959099}]"#;

    #[test]
    fn parses_the_recorded_payload_verbatim() {
        let pts: Vec<HistoryPoint> = serde_json::from_str(RECORDED).unwrap();
        let rows = to_rows(REGION_FORGE, 34, pts);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].date, "2025-08-01");
        assert_eq!(rows[0].average, Some(3.85));
        assert_eq!(rows[0].highest, Some(3.85), "字段是 highest 不是 high");
        assert_eq!(rows[1].volume, 5_758_959_099);
        assert_eq!(rows[1].order_count, 1_447);
    }

    #[test]
    fn tolerates_a_datetime_date_and_missing_fields() {
        let pts: Vec<HistoryPoint> =
            serde_json::from_str(r#"[{"date":"2026-09-22 11:05:00"}]"#).unwrap();
        let rows = to_rows(1, 2, pts);
        assert_eq!(rows[0].date, "2026-09-22");
        assert_eq!(rows[0].average, None);
        assert_eq!(rows[0].volume, 0);
    }

    #[test]
    fn path_is_the_url_key() {
        assert_eq!(
            path(REGION_FORGE, 34),
            "/v1/markets/10000002/history?type_id=34"
        );
    }

    #[test]
    fn measured_window_and_caps_match_the_plan() {
        assert_eq!(ESI_WINDOW_DAYS, 418);
        assert_eq!(KEEP_DAYS, 425);
        assert_eq!(L0_DAILY_CAP, 1_200);
        assert_eq!(L1_DAILY_CAP, 300);
        // §3.1 给 T3 的预算是 1 200 请求 —— L1 的 300 是从 L0 未用满的额度里补，
        // 所以默认配置的**上界**仍是 1 200 + 300 = 1 500，由 Estimate 如实报出。
        assert_eq!(HistoryConfig::default().budget(), 1_500);
    }

    #[test]
    fn env_cap_zero_disables_the_daily_pass() {
        std::env::set_var("EMD_HISTORY_CAP", "0");
        assert!(!HistoryConfig::from_env().enabled());
        std::env::set_var("EMD_HISTORY_CAP", "40");
        assert_eq!(HistoryConfig::from_env().l0_cap, 40);
        std::env::remove_var("EMD_HISTORY_CAP");
        assert!(HistoryConfig::from_env().enabled());
    }

    #[test]
    fn due_only_after_the_t3_slot_and_once_per_day() {
        let before = Utc.with_ymd_and_hms(2026, 9, 23, 11, 4, 0).unwrap();
        assert!(!history_due(None, &before), "11:05 之前上游还没有昨天的日线");
        let slot = Utc.with_ymd_and_hms(2026, 9, 23, 11, 20, 0).unwrap();
        assert!(history_due(None, &slot));
        assert!(
            !history_due(Some("2026-09-23"), &slot),
            "同一 UTC 日不重跑"
        );
        assert!(history_due(Some("2026-09-22"), &slot));
    }

    #[test]
    fn gate_is_closed_until_the_recorded_expires_moment() {
        let db = Db::in_memory().unwrap();
        let key = path(REGION_FORGE, 34);
        assert!(db.sync_due(&key, 1_800_000_000).unwrap(), "无记录应当可发");
        db.record_sync_ok(&SyncState {
            url_key: key.clone(),
            endpoint: "markets-history".into(),
            expires_at_unix: Some(1_900_000_000),
            ..Default::default()
        })
        .unwrap();
        assert!(!db.sync_due(&key, 1_899_999_999).unwrap());
        assert!(db.sync_due(&key, 1_900_000_000).unwrap());
    }

    #[test]
    fn a_recorded_404_gates_it_for_a_week() {
        let db = Db::in_memory().unwrap();
        let key = path(REGION_FORGE, 50);
        assert!(db.sync_due(&key, 1_000).unwrap());
        db.note_history_absent(&key, 1_000 + ABSENT_TTL).unwrap();
        assert!(!db.sync_due(&key, 1_000 + ABSENT_TTL - 1).unwrap());
        assert!(db.sync_due(&key, 1_000 + ABSENT_TTL).unwrap());
    }

    #[test]
    fn upsert_is_idempotent_across_days() {
        let db = Db::in_memory().unwrap();
        let pts: Vec<HistoryPoint> = serde_json::from_str(RECORDED).unwrap();
        let rows = to_rows(REGION_FORGE, 34, pts);
        assert_eq!(db.write_history(&rows).unwrap(), 2);
        // 第二天再拿同一批（上游会重发整窗）→ 行数不涨，值可被修正。
        let mut fixed = rows.clone();
        fixed[1].average = Some(3.99);
        db.write_history(&fixed).unwrap();
        let (n, types, regions) = db.history_totals().unwrap();
        assert_eq!((n, types, regions), (2, 1, 1), "主键 upsert 不得追加");
        let got = db.history_series(REGION_FORGE, 34, None).unwrap();
        assert_eq!(got[1].average, Some(3.99));
        assert_eq!(got[0].date, "2025-08-01");
    }

    #[test]
    fn series_honours_the_range_floor_and_coverage_counts() {
        let db = Db::in_memory().unwrap();
        let pts: Vec<HistoryPoint> = serde_json::from_str(RECORDED).unwrap();
        db.write_history(&to_rows(REGION_FORGE, 34, pts)).unwrap();
        assert_eq!(db.history_series(REGION_FORGE, 34, None).unwrap().len(), 2);
        assert_eq!(
            db.history_series(REGION_FORGE, 34, Some("2026-01-01"))
                .unwrap()
                .len(),
            1
        );
        let cov = db.history_coverage(REGION_FORGE, 34).unwrap().unwrap();
        assert_eq!((cov.days, cov.first.as_deref(), cov.last.as_deref()), (2, Some("2025-08-01"), Some("2026-09-22")));
        assert!(db.history_coverage(REGION_FORGE, 999).unwrap().is_none());
    }

    #[test]
    fn targets_are_watchlist_then_liquid_then_l1() {
        let db = Db::in_memory().unwrap();
        // 造一轮快照：三个类型在同一个 NPC 站上双边挂单，成交量 34 > 35 > 36。
        // 单价刻意反过来排（36 最贵）—— 排名必须只看量，不被 ISK 带偏。
        let pairs: [(u32, f64, u64); 3] = [
            (34, 4.0, 1_000_000_000),
            (35, 5.0, 10_000_000),
            (36, 6.0, 1_000_000),
        ];
        let mut both = Vec::new();
        for (i, (ty, ask, vol)) in pairs.iter().enumerate() {
            let id = i as u64;
            both.push(order(*ty, *ask, *vol, id, false));
            both.push(order(*ty, ask - 0.5, vol / 2, 100 + id, true));
        }
        let books = aggregate(
            &both,
            &AggregateOptions {
                now: Utc.with_ymd_and_hms(2026, 9, 23, 12, 0, 0).unwrap(),
                min_levels: 1,
                ..Default::default()
            },
        );
        db.write_snapshot(&books, Some("lm")).unwrap();
        db.write_hub_pool(&crate::market::hub_pool(&both, 1, 20))
            .unwrap();

        let ids: Vec<u32> = db
            .history_targets(REGION_FORGE, 2, 0, 1_800_000_000)
            .unwrap()
            .iter()
            .map(|t| t.type_id)
            .collect();
        assert_eq!(ids, vec![34, 35], "按 ISK 规模降序、取满 l0_cap");

        // 自选排在前，且**占用** l0_cap 的名额（"补齐到 l0_cap"），但不被挤掉。
        db.watch_add(36, Some("自用".into())).unwrap();
        let t = db
            .history_targets(REGION_FORGE, 2, 0, 1_800_000_000)
            .unwrap();
        assert_eq!(t[0].type_id, 36);
        assert_eq!(t[0].reason, "watchlist");
        assert_eq!(t[0].tier, TIER_L0);
        assert_eq!(
            t.iter().map(|x| x.type_id).collect::<Vec<_>>(),
            vec![36, 34],
            "自选 1 个 + 流动池补 1 个 = l0_cap 2"
        );
        // 上限之下自选永远全进。
        let only = db.history_targets(REGION_FORGE, 0, 0, 1).unwrap();
        assert_eq!(only.iter().map(|x| x.type_id).collect::<Vec<_>>(), vec![36]);

        // L1：近 30 天活跃，且优先补覆盖最少的那个。
        let now = crate::store::now_unix();
        db.note_scope(&[777, 888], TIER_L1, "flip-candidate")
            .unwrap();
        db.write_history(&[HistoryRow {
            region_id: REGION_FORGE,
            type_id: 888,
            date: "2026-09-22".into(),
            average: Some(1.0),
            highest: Some(1.0),
            lowest: Some(1.0),
            volume: 1,
            order_count: 1,
        }])
        .unwrap();
        let t = db.history_targets(REGION_FORGE, 0, 2, now).unwrap();
        assert_eq!(
            t.iter().map(|x| x.type_id).collect::<Vec<_>>(),
            vec![36, 777, 888],
            "l0_cap=0 也不该吞掉用户点名的自选；L1 里无覆盖的 777 排在已有 1 天的 888 前"
        );
        assert_eq!(t[1].tier, TIER_L1);
        assert_eq!(t[2].tier, TIER_L1);
        assert_eq!(db.scope_types(TIER_L1).unwrap(), vec![777, 888]);
    }

    fn order(ty: u32, price: f64, vol: u64, id: u64, is_buy: bool) -> Order {
        Order {
            id,
            type_id: ty,
            location_id: 60003760,
            system_id: 30000142,
            is_buy,
            price,
            volume_remain: vol,
            volume_total: vol,
            min_volume: 1,
            duration: 90,
            issued: Utc.with_ymd_and_hms(2026, 9, 22, 1, 0, 0).unwrap(),
            range: Some("region".into()),
        }
    }

    #[test]
    fn l1_drops_out_after_thirty_days() {
        let db = Db::in_memory().unwrap();
        db.note_scope(&[34], TIER_L1, "flip-candidate").unwrap();
        // 手动改老 last_seen。
        db.conn()
            .execute_batch("UPDATE history_scope SET last_seen = last_seen - 31*86400")
            .unwrap();
        let now = crate::store::now_unix();
        assert!(db
            .history_targets(REGION_FORGE, 0, 300, now)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn probe_reads_the_measured_zero_attribution() {
        // 2026-09-23 实测：读 11987 → 5 次 history 404 → 读 11985。差 2 = 后一次 orders 自己。
        let p = ProbeReport {
            remaining_before: Some(11_987),
            remaining_after: Some(11_985),
            ..Default::default()
        };
        assert_eq!(p.orders_delta(), Some(2));
        assert_eq!(p.history_charges_market_order(), Some(false));

        // 反例：若 40 次 history 各扣 2，差就是 82 → 必须判为占用。
        let q = ProbeReport {
            remaining_before: Some(11_987),
            remaining_after: Some(11_987 - 82),
            ..Default::default()
        };
        assert_eq!(q.history_charges_market_order(), Some(true));

        // 任一读数缺失（命中缓存时就是 None）不能瞎判。
        assert_eq!(ProbeReport::default().history_charges_market_order(), None);
        // 窗口边界抖动 1 令牌不算占用。
        let r = ProbeReport {
            remaining_before: Some(11_987),
            remaining_after: Some(11_984),
            ..Default::default()
        };
        assert_eq!(r.history_charges_market_order(), Some(false));
    }

    #[tokio::test]
    async fn probe_refuses_an_empty_sample() {
        let client = client();
        let e = probe(&client, &[], REGION_FORGE).await.unwrap_err();
        assert!(e.to_string().contains("类型 ID"), "{e}");
    }

    #[test]
    fn pass_report_derives_throughput() {
        let r = PassReport {
            requested: 100,
            seconds: 20.0,
            ..Default::default()
        };
        assert_eq!(r.rps(), 5.0);
        assert_eq!(r.per_request_ms(), 200.0);
        assert_eq!(r.est_seconds(1_200), 240.0);
        assert_eq!(PassReport::default().rps(), 0.0);
        assert_eq!(PassReport::default().est_seconds(10), 0.0);
    }

    #[test]
    fn estimate_uses_measured_rps_when_available() {
        let e = Estimate::of(1_200, None);
        assert_eq!(e.tokens_local, 2_400, "本地按 2 令牌/请求记账");
        assert_eq!(e.history_rows, 1_200 * 418);
        assert_eq!(
            e.seconds_at_measured_rps,
            1_200.0 / COLD_RPS,
            "没有实测吞吐时用保守的 COLD_RPS 外推"
        );
        let fast = Estimate::of(1_200, Some(12.0));
        assert!((fast.seconds_at_measured_rps - 100.0).abs() < 0.01);
        // rps=0（上一趟全被闸门挡掉时就会出现）不能变成除零或无穷。
        assert!(Estimate::of(10, Some(0.0)).seconds_at_measured_rps.is_finite());
        assert_eq!(Estimate::of(10, Some(0.0)).seconds_at_measured_rps, 10.0 / COLD_RPS);
    }

    fn client() -> EsiClient {
        EsiClient::new(crate::config::EsiConfig::default()).unwrap()
    }

    #[tokio::test]
    async fn an_empty_database_yields_a_no_target_pass() {
        let db = Db::in_memory().unwrap();
        let client = client();
        let rep = backfill(&client, &db, &HistoryConfig::default(), &[])
            .await
            .unwrap();
        assert_eq!(rep.targets, 0);
        assert_eq!(rep.requested, 0, "没有目标就一个请求都不能发");
        assert_eq!(rep.status, "no-targets");
        assert!(
            db.last_history_pass().unwrap().is_none(),
            "没真跑过的趟不配进台账"
        );

        let off = HistoryConfig {
            l0_cap: 0,
            l1_daily: 0,
            ..Default::default()
        };
        assert_eq!(
            backfill(&client, &db, &off, &[target(34)]).await.unwrap().status,
            "disabled"
        );
    }

    #[tokio::test]
    async fn a_closed_gate_costs_zero_requests() {
        // 这就是 M0.5 的第①条在 history 上的等价物：未到 Expires 时网络请求数为 0。
        let db = Db::in_memory().unwrap();
        let client = client();
        let now = crate::store::now_unix();
        let key = path(REGION_FORGE, 34);
        db.record_sync_ok(&SyncState {
            url_key: key.clone(),
            endpoint: "markets-history".into(),
            expires_at_unix: Some(now + 3_600),
            ..Default::default()
        })
        .unwrap();
        let before = client.stats();
        let rep = backfill(&client, &db, &HistoryConfig::default(), &[target(34)])
            .await
            .unwrap();
        assert_eq!((rep.gated, rep.requested, rep.fresh), (1, 0, 0));
        assert_eq!(rep.status, "all-gated");
        assert_eq!(client.stats().requests, before.requests, "闸门关闭时不得发出请求");
        assert_eq!(db.history_totals().unwrap().0, 0);
        // 全被挡住也算"今天跑过了"：明天 11:20 才会再来一趟。
        assert_eq!(
            db.last_history_request_day().unwrap().as_deref(),
            Some(chrono::Utc::now().date_naive().to_string().as_str())
        );
    }

    fn target(type_id: u32) -> HistoryTarget {
        HistoryTarget {
            type_id,
            tier: TIER_L0.into(),
            reason: "test".into(),
        }
    }
}
