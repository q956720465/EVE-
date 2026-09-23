//! T1 调度器 —— 6 分钟节拍、枢纽池、每轮台账。
//!
//! 节拍对齐规则（方案 v3.1 §3.1）：下一轮最早开始时刻取
//! `max(本轮开始 + interval, 首页 Expires)`。
//! - 只看 `interval` 会在 ESI 快照刚刷新时提前重复请求，那正是可致封禁的形态；
//! - 只看 `Expires` 会让节拍随上游刷新点漂移，UI 的环形倒计时就没法预测。
//! 取两者较晚者，节拍稳定且不越界。实测一轮 68 s，因此每轮约睡 292 s。

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::error::{Error, Result};
use crate::esi::EsiClient;
use crate::market::{
    self,
    history::{self, HistoryConfig, PassReport},
    hub_pool, LocationKind, RoundOutcome, STATION_JITA,
};
use crate::store::{Db, HistoryTarget, RoundRecord};

/// 300 s 缓存下限 + 60 s 余量。低于 300 s 即绕过缓存，代码层面拦住。
pub const MIN_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub region_id: u32,
    pub interval: Duration,
    /// 枢纽池门槛，见 §3.4。
    pub min_orders: u64,
    pub top_hubs: usize,
    /// 跑满 N 轮后退出；None = 常驻。
    pub rounds: Option<u64>,
    /// T3 历史档（§3.1 的每日 11:20 UTC / §3.3 的 L0+L1）。
    pub history: HistoryConfig,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            region_id: market::REGION_FORGE,
            interval: Duration::from_secs(360),
            min_orders: market::DEFAULT_MIN_ORDERS,
            top_hubs: market::DEFAULT_TOP,
            rounds: None,
            // 默认值刻意不走 `from_env`：环境变量是运行期配置，测试不能被它左右。
            history: HistoryConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stage {
    Idle,
    Fetching,
    Aggregating,
    WaitingNext,
    Failed(String),
}

/// UI 的环形倒计时、顶栏"上一轮 N 秒前"都读这个。
#[derive(Debug, Clone)]
pub struct RoundState {
    pub round: u64,
    pub stage: Stage,
    pub last_seconds: f64,
    pub orders: i64,
    pub rows_written: i64,
    pub hubs: usize,
    pub next_in: Duration,
    pub snapshot_lm: Option<String>,
}

impl Default for RoundState {
    fn default() -> Self {
        Self {
            round: 0,
            stage: Stage::Idle,
            last_seconds: 0.0,
            orders: 0,
            rows_written: 0,
            hubs: 0,
            next_in: Duration::ZERO,
            snapshot_lm: None,
        }
    }
}

pub struct Scheduler {
    client: Arc<EsiClient>,
    db: Arc<Db>,
    cfg: SchedulerConfig,
    status: watch::Sender<RoundState>,
    state: watch::Receiver<RoundState>,
}

impl Scheduler {
    pub fn new(client: Arc<EsiClient>, db: Arc<Db>, mut cfg: SchedulerConfig) -> Self {
        if cfg.interval < MIN_INTERVAL {
            tracing::warn!(
                "interval {:?} 低于 ESI 缓存下限 {:?}，已抬到下限 —— 更低等于绕过缓存",
                cfg.interval,
                MIN_INTERVAL
            );
            cfg.interval = MIN_INTERVAL;
        }
        let (tx, rx) = watch::channel(RoundState::default());
        Self {
            client,
            db,
            cfg,
            status: tx,
            state: rx,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<RoundState> {
        self.state.clone()
    }

    /// 跑一轮：拉取 → 聚合 → 换快照 → 算枢纽池 → 记站点 → 写台账。
    /// 返回时 `station_orders` 已是完整的一轮新数据，中途失败则保持上一轮不变。
    pub async fn run_round(&self, round_no: u64) -> Result<RoundOutcome> {
        let started_at = chrono::Utc::now().timestamp();
        let t0 = Instant::now();
        self.publish(round_no, Stage::Fetching, Duration::ZERO, None, 0);

        let outcome = match market::fetch_region_orders(&self.client, self.cfg.region_id).await {
            Ok(o) => o,
            Err(e) => {
                self.record(&RoundRecord {
                    started_at,
                    region_id: self.cfg.region_id as i64,
                    pages: 0,
                    orders: 0,
                    rows_written: 0,
                    seconds: t0.elapsed().as_secs_f64(),
                    decoded_bytes: 0,
                    over_network: 0,
                    drift_retries: 0,
                    snapshot_lm: None,
                    status: format!("failed: {e}"),
                })?;
                self.publish(round_no, Stage::Failed(e.to_string()), Duration::ZERO, None, 0);
                return Err(e);
            }
        };

        self.publish(round_no, Stage::Aggregating, Duration::ZERO, None, 0);
        let books = market::aggregate(&outcome.orders, &market::aggregate_opts());
        let rows = self.db.write_snapshot(&books, outcome.snapshot_lm.as_deref())?;

        let hubs = hub_pool(
            &outcome.orders,
            self.cfg.min_orders,
            self.cfg.top_hubs,
        );
        self.db.write_hub_pool(&hubs)?;

        // 站点登记：本轮出现过的 location 先进字典（未解析名字），下一轮维护时批量解名。
        let mut seen: Vec<u64> = outcome.orders.iter().map(|o| o.location_id).collect();
        seen.sort_unstable();
        seen.dedup();
        for id in &seen {
            self.db
                .remember_station(*id, LocationKind::of(*id).tradable_publicly())?;
        }

        self.record(&RoundRecord {
            started_at,
            region_id: self.cfg.region_id as i64,
            pages: outcome.pages_expected as i64,
            orders: outcome.orders.len() as i64,
            rows_written: rows as i64,
            seconds: t0.elapsed().as_secs_f64(),
            decoded_bytes: outcome.decoded_bytes as i64,
            over_network: outcome.over_network as i64,
            drift_retries: outcome.drift_retries as i64,
            snapshot_lm: outcome.snapshot_lm.clone(),
            status: "ok".into(),
        })?;

        tracing::info!(
            "第 {round_no} 轮：{} 页 / {} 单 → {rows} 行快照，覆盖 {} 个站点，其中 {} 个进枢纽池，耗时 {:.1}s",
            outcome.pages_expected,
            outcome.orders.len(),
            seen.len(),
            hubs.len(),
            t0.elapsed().as_secs_f64()
        );
        // 这里不再发 WaitingNext：那条状态由 run() 的 publish_state 带齐数据后统一发出。
        // 之前在此发一条空数据的状态，会让 UI 先闪一屏"0 单 / 0 个枢纽 / 耗时 0.0s"。
        Ok(outcome)
    }

    /// 常驻循环。`shutdown` 置 true 即在当前轮结束后退出（不打断进行中的一轮 ——
    /// 半轮写库会留下比上一轮更差的数据）。
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<u64> {
        let mut round = self.last_round_no()?;
        loop {
            if *shutdown.borrow() {
                return Ok(round);
            }
            round += 1;
            let round_start = Instant::now();
            let outcome = self.run_round(round).await;

            // T3 历史增量。放在 T1 之后、算下一次时刻之前是有意的两点：
            // ① 流动池排名要用刚落地的那一轮快照；② 万一这一趟跑了几分钟，
            // `plan_next` 取的是 `max(轮开始+interval, Expires)`，晚于两个锚点，
            // 只会让下一轮推迟，绝不会提前 —— 推迟合规，提前封号。
            match self.maybe_run_history().await {
                Ok(Some(rep)) => tracing::info!(
                    "T3 历史增量：{} 个目标 / {} 次请求 → {} 行日线（{} 个无历史、{} 个失败），耗时 {:.1}s",
                    rep.targets, rep.requested, rep.rows_written, rep.absent, rep.failed, rep.seconds
                ),
                Ok(None) => {}
                Err(e) => tracing::warn!("T3 历史增量未完成：{e}"),
            }

            let earliest_next = match &outcome {
                Ok(o) => o.earliest_next,
                Err(_) => Some(round_start + self.cfg.interval),
            };
            let next = plan_next(round_start, earliest_next, self.cfg.interval);
            let wait = next.saturating_duration_since(Instant::now());
            self.publish_state(round, Stage::WaitingNext, wait, outcome.as_ref().ok());

            if let Err(e) = &outcome {
                if matches!(e, Error::RateLimited { .. }) {
                    tracing::error!("被限流，多等 60 s 再继续");
                    sleep_or_shutdown(wait + Duration::from_secs(60), &mut shutdown).await;
                    continue;
                }
                let fails = self.consecutive_failures()?;
                if fails >= 3 {
                    let backoff = self.cfg.interval * (fails.min(6) as u32);
                    tracing::error!("连续 {fails} 轮失败（{e}），退避 {backoff:?}");
                    sleep_or_shutdown(backoff, &mut shutdown).await;
                    continue;
                }
            }

            // 先判退出再睡觉，否则 `--rounds N` 会白等最后一个 6 分钟才返回。
            if let Some(limit) = self.cfg.rounds {
                if round >= limit {
                    return Ok(round);
                }
            }
            sleep_or_shutdown(wait, &mut shutdown).await;
        }
    }

    /// 已入库的轮数。用台账行数而不是自己记号，重启后节拍也能接上。
    fn last_round_no(&self) -> Result<u64> {
        self.db.round_count()
    }

    /// 到点就跑一次 T3，没到点什么都不做（返回 None）。
    pub async fn maybe_run_history(&self) -> Result<Option<PassReport>> {
        let now = chrono::Utc::now();
        if !self.cfg.history.enabled() {
            return Ok(None);
        }
        let last = self.db.last_history_request_day()?;
        if !history::history_due(last.as_deref(), &now) {
            return Ok(None);
        }
        self.run_history().await.map(Some)
    }

    /// 跑一趟历史取数（不受"每日一次"限制，供 CLI 与手动触发用）。
    pub async fn run_history(&self) -> Result<PassReport> {
        let h = &self.cfg.history;
        // 流动池排名完全来自本地快照，而快照只有**采集那个星域**的盘口。
        // 拿 A 域的盘口去决定 B 域要取哪些类型，会静默取错清单（更糟的是星域 ID
        // 写错时 ESI 逐个回 404，每个再扣 1 点全局错误限额），所以直接拒。
        let collected = self.db.collected_region()?;
        match collected {
            Some(c) if c != h.region_id => {
                return Err(Error::Config(format!(
                    "本地快照是星域 {c}，还没有 {} 的盘口，无法为它排 L0 清单（先跑一轮 round）",
                    h.region_id
                )));
            }
            None if self.db.counts()?.rows == 0 => {
                return Err(Error::Config(
                    "本地还没有任何快照，L0 高流动池无从算起".into(),
                ));
            }
            _ => {}
        }
        let now = chrono::Utc::now();
        let mut targets: Vec<HistoryTarget> =
            self.db
                .history_targets(h.region_id, h.l0_cap, h.l1_daily, now.timestamp())?;
        if let Some(n) = h.limit {
            targets.truncate(n as usize);
        }
        tracing::info!(
            "T3 历史取数：{} 个目标（L0 上限 {}，L1 每日 {}），预计 {:.0} s",
            targets.len(),
            h.l0_cap,
            h.l1_daily,
            history::Estimate::of(targets.len() as u32, None).seconds_at_measured_rps
        );
        history::backfill(&self.client, &self.db, h, &targets).await
    }

    fn consecutive_failures(&self) -> Result<usize> {
        let mut stmt = self.db.conn().prepare(
            "SELECT status FROM round_log ORDER BY id DESC LIMIT 10",
        )?;
        let rows: Vec<String> = stmt
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?;
        Ok(consecutive_failures(&rows))
    }

    fn record(&self, r: &RoundRecord) -> Result<()> {
        self.db.record_round(r)
    }

    fn publish(
        &self,
        round: u64,
        stage: Stage,
        next_in: Duration,
        lm: Option<String>,
        rows: i64,
    ) {
        let _ = self.status.send(RoundState {
            round,
            stage,
            next_in,
            snapshot_lm: lm,
            rows_written: rows,
            ..Default::default()
        });
    }

    fn publish_state(
        &self,
        round: u64,
        stage: Stage,
        next_in: Duration,
        ok: Option<&RoundOutcome>,
    ) {
        if let Some(o) = ok {
            let _ = self.status.send(RoundState {
                round,
                stage,
                last_seconds: o.elapsed.as_secs_f64(),
                orders: o.orders.len() as i64,
                rows_written: self.row_count().unwrap_or(0),
                hubs: self.hub_count().unwrap_or(0),
                next_in,
                snapshot_lm: o.snapshot_lm.clone(),
            });
        }
    }

    fn row_count(&self) -> Result<i64> {
        Ok(self.db.counts()?.rows as i64)
    }

    fn hub_count(&self) -> Result<usize> {
        Ok(self.db.hub_pool()?.len())
    }

    /// 吉他单站的类型行数 —— M1 的验收判据就是这里能查到数。
    pub fn jita_rows(&self) -> Result<usize> {
        Ok(self.db.station_book(STATION_JITA, false)?.len())
    }
}

async fn sleep_or_shutdown(d: Duration, shutdown: &mut watch::Receiver<bool>) {
    tokio::select! {
        _ = tokio::time::sleep(d) => {}
        _ = shutdown.wait_for(|v| *v) => {}
    }
}

/// 下一轮的开始时刻。见模块头的取 max 理由。
pub fn plan_next(
    round_start: Instant,
    earliest_next: Option<Instant>,
    interval: Duration,
) -> Instant {
    let cadence = round_start + interval.max(MIN_INTERVAL);
    match earliest_next {
        Some(e) if e > cadence => e,
        _ => cadence,
    }
}

/// 台账里从最新一条往前数，连续多少轮是失败的。
pub fn consecutive_failures(statuses: &[String]) -> usize {
    statuses
        .iter()
        .take_while(|s| !s.eq_ignore_ascii_case("ok"))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as D;

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn cadence_floor_is_the_esi_cache_ttl() {
        assert_eq!(MIN_INTERVAL, D::from_secs(300));
    }

    #[test]
    fn normal_round_waits_out_the_six_minute_interval() {
        let start = Instant::now();
        // 实测首页 Expires 通常在轮开始后 100~300 s 之间，早于 360 s 节拍点。
        let expires = start + D::from_secs(272);
        let next = plan_next(start, Some(expires), D::from_secs(360));
        assert_eq!(next, start + D::from_secs(360));
    }

    #[test]
    fn a_fresh_snapshot_pushes_the_next_round_later() {
        let start = Instant::now();
        let expires = start + D::from_secs(420);
        let next = plan_next(start, Some(expires), D::from_secs(360));
        assert_eq!(next, expires, "Expires 未到就开下一轮等于绕过缓存");
    }

    #[test]
    fn missing_expires_header_falls_back_to_cadence() {
        let start = Instant::now();
        let next = plan_next(start, None, D::from_secs(360));
        assert_eq!(next, start + D::from_secs(360));
    }

    #[test]
    fn a_sub_interval_request_is_raised_to_the_floor() {
        let start = Instant::now();
        // 60 s 的"实时模式"会被抬到下限，不因用户设置而绕过缓存纪律。
        let next = plan_next(start, None, D::from_secs(60));
        assert_eq!(next, start + D::from_secs(300));
    }

    #[test]
    fn failure_streaks_count_from_the_newest_row() {
        assert_eq!(consecutive_failures(&[]), 0);
        assert_eq!(consecutive_failures(&[s("ok"), s("ok")]), 0);
        assert_eq!(consecutive_failures(&[s("failed: 429"), s("ok")]), 1);
        assert_eq!(
            consecutive_failures(&[s("failed: x"), s("failed: y"), s("ok"), s("failed: z")]),
            2
        );
        assert_eq!(consecutive_failures(&[s("OK")]), 0, "大小写不算失败");
    }

    #[test]
    fn config_defaults_match_the_plan() {
        let c = SchedulerConfig::default();
        assert_eq!(c.region_id, market::REGION_FORGE);
        assert_eq!(c.interval, D::from_secs(360));
        assert_eq!(c.min_orders, 50);
        assert_eq!(c.top_hubs, 20);
        assert_eq!(c.rounds, None);
        // T3 挂在同一个配置上，默认按 §3.3 的 L0=1 200 / L1=300。
        assert_eq!((c.history.l0_cap, c.history.l1_daily), (1_200, 300));
        assert_eq!(c.history.region_id, market::REGION_FORGE);
    }

    #[tokio::test]
    async fn history_never_runs_on_a_snapshot_it_does_not_have() {
        let client = Arc::new(EsiClient::new(Default::default()).unwrap());

        // 空库：连 L0 池都算不出来，一个请求都不该发。
        let db = Arc::new(Db::in_memory().unwrap());
        let sched = Scheduler::new(client.clone(), db.clone(), SchedulerConfig::default());
        let e = sched.run_history().await.unwrap_err();
        assert!(e.to_string().contains("没有任何快照"), "{e}");
        assert_eq!(sched.client.stats().requests, 0);

        // 有快照但星域对不上：换星域取历史必须先看有没有那个域的盘口。
        db.record_round(&RoundRecord {
            started_at: 1_700_000_000,
            region_id: market::REGION_FORGE as i64,
            pages: 409,
            orders: 408_000,
            rows_written: 16_662,
            seconds: 68.0,
            decoded_bytes: 92_000_000,
            over_network: 409,
            drift_retries: 0,
            snapshot_lm: Some("lm".into()),
            status: "ok".into(),
        })
        .unwrap();
        assert_eq!(db.collected_region().unwrap(), Some(market::REGION_FORGE));
        let cfg = SchedulerConfig {
            history: HistoryConfig {
                region_id: 10000043,
                ..Default::default()
            },
            ..Default::default()
        };
        let sched = Scheduler::new(client.clone(), db.clone(), cfg);
        let e = sched.run_history().await.unwrap_err();
        assert!(e.to_string().contains("10000002"), "{e}");
        assert_eq!(sched.client.stats().requests, 0, "被拒不等于发过请求");
    }

    #[tokio::test]
    async fn the_daily_history_hook_is_quiet_before_the_slot() {
        let db = Arc::new(Db::in_memory().unwrap());
        let client = Arc::new(EsiClient::new(Default::default()).unwrap());
        // 采集与历史同域、但整档关掉：必须回 None 且一个请求都不发。
        let cfg = SchedulerConfig {
            history: HistoryConfig {
                l0_cap: 0,
                l1_daily: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let sched = Scheduler::new(client.clone(), db, cfg);
        assert!(sched.maybe_run_history().await.unwrap().is_none());
        assert_eq!(sched.client.stats().requests, 0, "关掉整档就不该有任何请求");
    }

    #[test]
    fn new_scheduler_raises_a_too_short_interval() {
        let db = Arc::new(Db::in_memory().unwrap());
        let client = Arc::new(EsiClient::new(Default::default()).unwrap());
        let cfg = SchedulerConfig {
            interval: D::from_secs(10),
            ..Default::default()
        };
        let sched = Scheduler::new(client, db, cfg);
        // interval 被抬到下限，通过 plan_next 间接可观测。
        let start = Instant::now();
        assert_eq!(
            plan_next(start, None, sched.cfg.interval),
            start + MIN_INTERVAL
        );
    }

    #[tokio::test]
    async fn subscribe_sees_the_initial_idle_state() {
        let db = Arc::new(Db::in_memory().unwrap());
        let client = Arc::new(EsiClient::new(Default::default()).unwrap());
        let sched = Scheduler::new(client, db, SchedulerConfig::default());
        let rx = sched.subscribe();
        assert_eq!(rx.borrow().stage, Stage::Idle);
        assert_eq!(rx.borrow().round, 0);
    }

    #[tokio::test]
    async fn publish_updates_every_subscriber() {
        let db = Arc::new(Db::in_memory().unwrap());
        let client = Arc::new(EsiClient::new(Default::default()).unwrap());
        let sched = Scheduler::new(client, db, SchedulerConfig::default());
        let mut rx = sched.subscribe();
        sched.publish(7, Stage::Fetching, D::from_secs(3), None, 0);
        rx.changed().await.unwrap();
        assert_eq!(rx.borrow().round, 7);
        assert_eq!(rx.borrow().stage, Stage::Fetching);
    }

    #[test]
    fn empty_database_has_no_last_round() {
        let db = Db::in_memory().unwrap();
        assert!(db.last_round().unwrap().is_none());
    }

    #[test]
    fn round_log_accumulates_across_rounds() {
        let db = Db::in_memory().unwrap();
        for i in 0..3 {
            db.record_round(&RoundRecord {
                started_at: 1_700_000_000 + i,
                region_id: 10000002,
                pages: 409,
                orders: 408_000 + i,
                rows_written: 16_700,
                seconds: 68.0,
                decoded_bytes: 96_000_000,
                over_network: 409,
                drift_retries: 0,
                snapshot_lm: Some("lm".into()),
                status: if i == 2 { "failed: x".into() } else { "ok".into() },
            })
            .unwrap();
        }
        let last = db.last_round().unwrap().unwrap();
        assert_eq!(last.status, "failed: x");
        assert_eq!(last.orders, 408_002);
        assert_eq!(last.pages, 409);
    }
}
