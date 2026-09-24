//! 星域订单簿的分页拉取。
//!
//! 两条实测得来的纪律：
//! - **快照一致性只能靠 `Last-Modified`**。同一轮 page 1/200/409 的 `Last-Modified` 全等
//!   （`12:07:42`），但 `ETag` 每页不同 —— 拿 ETag 判一致会永远"不一致"，进而无限重拉。
//! - **页级失败不重拉整轮**。409 页实测有 1 页返回空体；整轮重来要多花 82 s 和 818 令牌。

use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};

use crate::error::{Error, Result};
use crate::esi::{EsiClient, Fetch};
use crate::market::entities::{Order, OrdersResponse};

/// 允许的失败页比例。超过即整轮作废 —— 缺页会让某个站点的深度凭空变薄。
const MAX_FAILED_RATIO: f64 = 0.01;
const ROUND_RETRIES: u32 = 2;

/// T1.5 定向拉取的页数上限（防呆）：实测单类型单星域 ≈1 页（148 条），
/// 5 页 = 5000 条是几乎不可能的尾部；超过就截断并把 truncated 计进台账。
const TYPE_PAGE_CAP: u32 = 5;

#[derive(Debug, Clone)]
pub struct RoundOutcome {
    pub region_id: u32,
    pub orders: Vec<Order>,
    /// 本轮快照的 `Last-Modified`，落库后作为"这批数据同源"的凭据。
    pub snapshot_lm: Option<String>,
    pub pages_expected: u32,
    pub pages_ok: u32,
    pub pages_failed: Vec<u32>,
    pub elapsed: Duration,
    /// 解码后的 JSON 字节数（实测 ≈ wire 的 9.2 倍：单页 237,300 B 解压 / 25,946 B 传输）。
    /// 别拿它当带宽 —— 带宽看 `pages × 25.9 KB`。
    pub decoded_bytes: u64,
    /// 其中真正走网络（200/304）的页数。合规自检要看这个数。
    pub over_network: u32,
    pub upstream_hit: u32,
    /// 因快照漂移而整轮重来的次数。
    pub drift_retries: u32,
    /// 首页 `Expires` 换算出的本地时刻 —— 调度器据此决定下一轮最早何时开始。
    pub earliest_next: Option<Instant>,
}

impl RoundOutcome {
    pub fn completeness(&self) -> f64 {
        if self.pages_expected == 0 {
            return 0.0;
        }
        self.pages_ok as f64 / self.pages_expected as f64
    }
}

fn page_path(region_id: u32, page: u32) -> String {
    format!("/v3/markets/{region_id}/orders?order_type=all&page={page}")
}

/// T1.5 定向拉取的路径模板：`type_id` 过滤让单请求返回 1 页 ≈148 条。
pub fn type_page_path(region_id: u32, type_id: u32, page: u32) -> String {
    format!("/v3/markets/{region_id}/orders?type_id={type_id}&order_type=all&page={page}")
}

pub fn capped_pages(x_pages: u32) -> u32 {
    x_pages.clamp(1, TYPE_PAGE_CAP)
}

/// 单 (region, type) 一次定向拉取的结果。
#[derive(Debug, Clone)]
pub struct TypeFetch {
    pub region_id: u32,
    pub type_id: u32,
    pub orders: Vec<Order>,
    pub pages: u32,
    pub truncated: bool,
}

/// 拉取单个 (region, type) 的全部页（通常 1 页）。任一页失败即整型失败——
/// 缺页的半本盘口会低估深度，宁可让该类型本轮不参与配对（下批自然重试）。
pub async fn fetch_type_orders(
    client: &EsiClient,
    region_id: u32,
    type_id: u32,
) -> Result<TypeFetch> {
    let first = client
        .fetch(&type_page_path(region_id, type_id, 1))
        .await?;
    let x_pages = resolve_pages(&first)?;
    let snapshot_lm = first.meta().last_modified.clone();
    let pages = capped_pages(x_pages);
    let mut acc = decode(client, first)?.orders;
    for page in 2..=pages {
        let f = client
            .fetch(&type_page_path(region_id, type_id, page))
            .await?;
        let d = decode(client, f)?;
        // 与全量拉取同一条纪律：跨页 Last-Modified 不一致 = 快照不自洽。
        check_drift(page, &d, snapshot_lm.as_deref())?;
        acc.extend(d.orders);
    }
    Ok(TypeFetch {
        region_id,
        type_id,
        orders: acc,
        pages,
        truncated: x_pages > TYPE_PAGE_CAP,
    })
}

/// 拉取一个星域的全量订单簿。
pub async fn fetch_region_orders(client: &EsiClient, region_id: u32) -> Result<RoundOutcome> {
    let mut drift_retries = 0;
    loop {
        match round(client, region_id).await {
            Ok(mut out) => {
                out.drift_retries = drift_retries;
                return Ok(out);
            }
            Err(e @ Error::SnapshotDrift { .. }) => {
                if drift_retries >= ROUND_RETRIES {
                    return Err(e);
                }
                drift_retries += 1;
                tracing::warn!("星域 {region_id} 快照漂移，丢弃本轮重拉（第 {drift_retries} 次）");
                let needle = format!("/markets/{region_id}/orders");
                client.invalidate_matching(&needle);
            }
            Err(e) => return Err(e),
        }
    }
}

struct PageData {
    orders: Vec<Order>,
    bytes: u64,
    over_network: bool,
    upstream_hit: bool,
    last_modified: Option<String>,
}

async fn round(client: &EsiClient, region_id: u32) -> Result<RoundOutcome> {
    let started = Instant::now();
    let first = client.fetch(&page_path(region_id, 1)).await?;
    let pages_expected = resolve_pages(&first)?;
    let snapshot_lm = first.meta().last_modified.clone();
    // 下一轮最早可以发请求的时刻 —— 由首页的 Expires 决定，调度器据此对齐。
    let earliest_next = first.meta().expires_at;
    let first_data = decode(client, first)?;
    let drift = check_drift(1, &first_data, snapshot_lm.as_deref())?;
    debug_assert!(drift.is_none());

    let mut orders = first_data.orders;
    let mut decoded_bytes = first_data.bytes;
    let mut pages_ok = 1u32;
    let mut over_network = first_data.over_network as u32;
    let mut upstream_hit = first_data.upstream_hit as u32;
    let mut pages_failed: Vec<u32> = Vec::new();

    if pages_expected > 1 {
        let results: Vec<(u32, std::result::Result<PageData, Error>)> =
            stream::iter((2..=pages_expected).map(|p| page_path(region_id, p)))
                .map(|path| {
                    // 每页各拿一份 lm 的所有权：`FnMut` 闭包不能把外层变量 move 进去。
                    let lm = snapshot_lm.clone();
                    async move {
                        let page = path
                            .rsplit('=')
                            .next()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(2);
                        // 抖动必须落在发请求之前。放在结果回收循环里等于串行空等
                        // 409 × 60 ms ≈ 25 s，白涨在墙钟上且完全没有错峰作用。
                        tokio::time::sleep(jitter(client.config().page_jitter, page)).await;
                        let res = decode_by_path(client, path)
                            .await
                            .and_then(|d| check_drift(page, &d, lm.as_deref()).map(|_| d));
                        (page, res)
                    }
                })
                .buffer_unordered(client.config().concurrency)
                .collect()
                .await;

        for (page, res) in results {
            match res {
                Ok(mut d) => {
                    orders.append(&mut d.orders);
                    decoded_bytes += d.bytes;
                    pages_ok += 1;
                    over_network += d.over_network as u32;
                    upstream_hit += d.upstream_hit as u32;
                }
                Err(e @ Error::SnapshotDrift { .. }) => return Err(e),
                Err(e) => {
                    tracing::warn!("星域 {region_id} 第 {page} 页失败：{e}");
                    pages_failed.push(page);
                }
            }
        }
    }

    let ratio = pages_failed.len() as f64 / pages_expected.max(1) as f64;
    if ratio > MAX_FAILED_RATIO {
        return Err(Error::PageFailed {
            page: *pages_failed.first().unwrap_or(&0),
            attempts: pages_failed.len() as u32,
        });
    }
    if !pages_failed.is_empty() {
        tracing::warn!(
            "星域 {region_id} 缺失 {} 页（{:.2}%），本轮仍可用",
            pages_failed.len(),
            ratio * 100.0
        );
    }

    Ok(RoundOutcome {
        region_id,
        orders,
        snapshot_lm,
        pages_expected,
        pages_ok,
        pages_failed,
        elapsed: started.elapsed(),
        decoded_bytes,
        over_network,
        upstream_hit,
        drift_retries: 0,
        earliest_next,
    })
}

async fn decode_by_path(client: &EsiClient, path: String) -> std::result::Result<PageData, Error> {
    let f = client.fetch(&path).await?;
    decode(client, f)
}

fn decode(_client: &EsiClient, f: Fetch) -> std::result::Result<PageData, Error> {
    let bytes = f.body().len() as u64;
    let over_network = f.over_network();
    let upstream_hit = f.meta().upstream_cache_hit();
    let last_modified = f.meta().last_modified.clone();
    let orders = f.json::<OrdersResponse>()?.into_orders();
    Ok(PageData {
        orders,
        bytes,
        over_network,
        upstream_hit,
        last_modified,
    })
}

/// `Last-Modified` 与本轮首页不一致 => 快照不再自洽，半旧半新会把跨站价差算错。
fn check_drift(page: u32, d: &PageData, expected: Option<&str>) -> Result<Option<()>> {
    match (expected, d.last_modified.as_deref()) {
        (Some(e), Some(f)) if e != f => Err(Error::SnapshotDrift {
            page,
            expected: Some(e.to_string()),
            found: Some(f.to_string()),
        }),
        _ => Ok(None),
    }
}

/// `X-Pages` 优先；缺失时退回 body 里的 `pagination.pages`。
fn resolve_pages(first: &Fetch) -> Result<u32> {
    if let Some(p) = first.meta().x_pages {
        return Ok(p);
    }
    if let Ok(r) = first.json::<OrdersResponse>() {
        if let Some(p) = r.pages() {
            return Ok(p);
        }
    }
    Err(Error::MissingHeader {
        header: "x-pages",
        url: first.meta().url.clone(),
    })
}

/// 页间抖动：以配置值为下限、按页号错开，避免 409 个请求齐步走。
fn jitter(base: Duration, page: u32) -> Duration {
    base + Duration::from_millis((page % 7) as u64 * 10)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(orders: Vec<Order>, lm: Option<&str>) -> PageData {
        PageData {
            orders,
            bytes: 100,
            over_network: true,
            upstream_hit: false,
            last_modified: lm.map(|s| s.to_string()),
        }
    }

    #[test]
    fn matching_last_modified_is_not_drift() {
        let d = page(vec![], Some("Wed, 23 Sep 2026 12:07:42 GMT"));
        assert!(check_drift(200, &d, Some("Wed, 23 Sep 2026 12:07:42 GMT")).is_ok());
    }

    #[test]
    fn differing_last_modified_is_drift() {
        let d = page(vec![], Some("Wed, 23 Sep 2026 12:09:42 GMT"));
        let e = check_drift(409, &d, Some("Wed, 23 Sep 2026 12:07:42 GMT")).unwrap_err();
        assert!(matches!(e, Error::SnapshotDrift { page: 409, .. }));
        assert!(e.to_string().contains("快照漂移"));
    }

    #[test]
    fn missing_header_on_one_page_does_not_fake_a_drift() {
        let d = page(vec![], None);
        assert!(check_drift(3, &d, Some("x")).is_ok());
    }

    #[test]
    fn page_path_shape() {
        assert_eq!(
            page_path(10000002, 7),
            "/v3/markets/10000002/orders?order_type=all&page=7"
        );
    }

    #[test]
    fn completeness_and_failure_ratio() {
        let mut o = RoundOutcome {
            region_id: 10000002,
            orders: vec![],
            snapshot_lm: None,
            pages_expected: 409,
            pages_ok: 408,
            pages_failed: vec![17],
            elapsed: Duration::from_secs(82),
            decoded_bytes: 10_100_000,
            over_network: 408,
            upstream_hit: 0,
            drift_retries: 0,
            earliest_next: None,
        };
        // 实测那 1 页空体：408/409 = 99.75%，缺页率 0.24% < 1% → 本轮可用。
        assert!((o.completeness() - 0.99755).abs() < 1e-4);
        assert!((1.0 / 409.0) <= MAX_FAILED_RATIO);
        o.pages_failed.push(2);
        o.pages_failed.push(3);
        assert!((3.0 / 409.0) <= MAX_FAILED_RATIO);
        o.pages_failed.push(4);
        o.pages_failed.push(5);
        assert!((5.0 / 409.0) > MAX_FAILED_RATIO, "5 页缺失就该作废");
    }

    #[test]
    fn jitter_stays_bounded() {
        let base = Duration::from_millis(30);
        for p in [1u32, 8, 9, 409] {
            let j = jitter(base, p);
            assert!(j >= base && j <= base + Duration::from_millis(60));
        }
    }

    // ---- M4b：T1.5 单类型定向拉取 -----------------------------------------

    #[test]
    fn type_page_path_shape() {
        assert_eq!(
            type_page_path(10000043, 34, 2),
            "/v3/markets/10000043/orders?type_id=34&order_type=all&page=2"
        );
    }

    #[test]
    fn page_cap_is_five() {
        assert_eq!(capped_pages(1), 1);
        assert_eq!(capped_pages(3), 3);
        assert_eq!(
            capped_pages(40),
            TYPE_PAGE_CAP,
            "单类型单星域 >5 页截断并记 truncated"
        );
    }
}
