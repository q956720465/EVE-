//! T1 调度器 —— 6 分钟节拍、枢纽池、每轮台账。
//!
//! 节拍对齐规则（方案 v3.1 §3.1）：下一轮最早开始时刻取
//! `max(本轮开始 + interval, 首页 Expires)`。
//! - 只看 `interval` 会在 ESI 快照刚刷新时提前重复请求，那正是可致封禁的形态；
//! - 只看 `Expires` 会让节拍随上游刷新点漂移，UI 的环形倒计时就没法预测。
//! 取两者较晚者，节拍稳定且不越界。实测一轮 68 s，因此每轮约睡 292 s。

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use tokio::sync::watch;

use crate::alert::{self, AlertRoundReport};
use crate::config::CharConfig;
use crate::error::{Error, Result};
use crate::esi::EsiClient;
use crate::market::{
    self,
    history::{self, HistoryConfig, PassReport},
    hub_pool, LocationKind, RoundOutcome, STATION_JITA,
};
use crate::push::{PushChannel, PushConfig};
use crate::sso::store::{KeyringTokenStore, TokenStore};
use crate::store::{Db, HistoryTarget, RoundRecord};

/// 300 s 缓存下限 + 60 s 余量。低于 300 s 即绕过缓存，代码层面拦住。
pub const MIN_INTERVAL: Duration = Duration::from_secs(300);

/// 令牌在系统凭据库里的服务名与条目名（唯一一份字面量）。
///
/// **T13 的登录与登出必须用同一对**：登录把令牌写进 (`KEYRING_SERVICE`, `KEYRING_ACCOUNT`)，
/// 同步回合从这里读 —— 两边各指一条条目时，用户"已经登录成功"而每一轮都静默跳过（没令牌
/// 不是错误，见 [`Scheduler::run_char_and_alerts`]），从现象上完全看不出是条目没对上。
pub const KEYRING_SERVICE: &str = "EveMarketDesk";
pub const KEYRING_ACCOUNT: &str = "eve-sso";

/// T1.5 跨区补拉配置（方案 v3.1 §3.1：隔轮 ≈12 min；默认启用）。
#[derive(Debug, Clone)]
pub struct XRegionConfig {
    pub enabled: bool,
    pub candidate_top: usize,
    pub targets: [(u32, u64); 3],
    pub max_age_secs: i64,
}

impl Default for XRegionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            candidate_top: 200,
            targets: market::XREGION_TARGETS,
            max_age_secs: market::XREGION_MAX_AGE_SECS,
        }
    }
}

impl XRegionConfig {
    /// 运行期开关：`EMD_XREGION=0` 关闭（默认值刻意不走 env，测试不被环境左右）。
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(v) = std::env::var("EMD_XREGION") {
            c.enabled = v != "0";
        }
        c
    }
}

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
    /// T1.5 跨区补拉（§3.1 隔轮 / §4.1 三枢纽）。
    pub xregion: XRegionConfig,
    /// M4c 角色挂链与亏损提醒（§4.1 / §4.2）。
    pub char: CharConfig,
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
            xregion: XRegionConfig::default(),
            // M4c 同一条纪律：默认**关闭**（没配 `client_id` 之前，任何一轮都不该往外发请求）。
            // `EMD_CHAR_SYNC` 的归一在 `CharConfig::from_env()` 一处 —— 本文件不读第二个 env：
            // `EMD_XREGION` 那次归一修的是"serve 绕过了 from_env"，而 char 侧默认本就是关的，
            // 要开必须显式传 `CharConfig`（多读一次 env 会让测试的成败取决于同一进程里
            // 别的测试有没有在改环境变量）。
            char: CharConfig::default(),
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
    /// 令牌来源（M4c）。默认系统凭据库；测试与登录路径可注入
    /// （[`Scheduler::with_tokens`]）—— 这条通路上**没有**任何令牌的落库/日志面。
    tokens: Arc<dyn TokenStore>,
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
        // 运行期 kill switch：`EMD_XREGION=0` 关掉 T1.5（serve / Tauri 壳都经此归一，
        // 与 interval 归一同一先例）。默认值本身仍不读 env，测试与 from_env 的语义不变。
        if let Ok(v) = std::env::var("EMD_XREGION") {
            cfg.xregion.enabled = v != "0";
        }
        let (tx, rx) = watch::channel(RoundState::default());
        Self {
            client,
            db,
            cfg,
            tokens: Arc::new(KeyringTokenStore::new(KEYRING_SERVICE, KEYRING_ACCOUNT)),
            status: tx,
            state: rx,
        }
    }

    /// 换一个令牌来源。测试用内存实现，登录路径（T13/T14）可以传自己那份 ——
    /// 凭据库打不开的环境里不该连"读一下试试"都做不到。
    pub fn with_tokens(mut self, tokens: Arc<dyn TokenStore>) -> Self {
        self.tokens = tokens;
        self
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

            // T1.5：隔轮、错峰窗口外、且本轮快照可用时才跑；失败只告警不打断主循环
            // （跨区数据缺一批 = 少一些候选，不是错误）。
            if self.cfg.xregion.enabled
                && t1_5_due(round)
                && outcome.is_ok()
                && !xregion_blackout(chrono::Utc::now())
            {
                match self.run_t1_5().await {
                    Ok(Some(rep)) => tracing::info!(
                        "T1.5 完成：{} 本跨区单簿",
                        rep.books_written
                    ),
                    Ok(None) => {}
                    Err(e) => tracing::warn!("T1.5 未完成：{e}"),
                }
            }
            // 生命周期：每轮结算（缺席以轮为尺，v3.1「连续 2 轮」）；轮失败不结算——
            // 数据不可用 ≠ 机会消失，拿旧盘口重新记账会伪造「仍在命中」。
            if outcome.is_ok() {
                match market::lifecycle::update_round(
                    &self.db,
                    chrono::Utc::now().timestamp(),
                ) {
                    Ok(Some(s)) => tracing::info!(
                        "机会生命周期：活跃 {}｜新 {}｜复活 {}｜失效 {}｜过期 {}（写 {}）",
                        s.active,
                        s.new,
                        s.revived,
                        s.invalidated,
                        s.expired,
                        s.saved
                    ),
                    Ok(None) => {}
                    Err(e) => tracing::warn!("机会生命周期结算失败：{e}"),
                }
            }

            // M4c：角色同步与亏损告警回合，与 T1 节拍同频（每轮一次，≤4 个角色请求 + 若干推送）。
            // 位置在生命周期之后、T3 之前：判定吃的是**刚落地的这一轮盘口**（② 的可执行卖出净额），
            // 而 T3 是每日一次的大头，放它后面只会让告警的数据年龄多等几分钟。
            // 纪律与上面两个钩子相同：`outcome.is_ok()` 才跑（轮失败时不拿旧盘口判定/收尾）、
            // 失败只 warn 不打断主循环 —— 告警取不到数不是停采的理由，下一轮还会再来。
            if outcome.is_ok() {
                match self.run_char_and_alerts().await {
                    Ok(Some(rep)) => tracing::info!(
                        "角色告警回合：同步 {} 行 / 判定 {} 条 / 推送 {} 条 / 拦下 {} 条",
                        rep.synced,
                        rep.detected,
                        rep.pushed,
                        rep.suppressed
                    ),
                    Ok(None) => {}
                    Err(e) => tracing::warn!("角色告警回合未完成：{e}"),
                }
            }

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

    /// 跑一趟 T1.5：上一轮命中的 Top N 候选 × 三枢纽定向补拉 → 聚合 → 落 xregion_books。
    /// `Ok(None)` = 无候选（0 机会的快照，或没跑过采集）——不是错误。
    pub async fn run_t1_5(&self) -> Result<Option<XRegionReport>> {
        // 守 enabled 是防御性：run() 主循环的钩子已查过，但直接调用方（daemon xregion、
        // 测试）也要被同一个开关管住。
        if !self.cfg.xregion.enabled {
            return Ok(None);
        }
        let books = self.db.load_books()?;
        let hubs = self.db.flip_hubs()?;
        let vol = self.db.latest_vol24()?;
        let params = self.db.get_flip_params()?;
        let out = market::scan(&books, &hubs, &params, &vol);
        let cand = candidates_from(&out.opportunities, self.cfg.xregion.candidate_top);
        if cand.is_empty() {
            return Ok(None);
        }

        let started_at = chrono::Utc::now().timestamp();
        let t0 = Instant::now();
        // 全笛卡尔积并发（约 600 请求，实测墙钟 136–226 s @并发 16）。
        let jobs: Vec<(u32, u64, u32)> = self
            .cfg
            .xregion
            .targets
            .iter()
            .flat_map(|&(r, h)| cand.iter().map(move |&t| (r, h, t)))
            .collect();
        let results: Vec<(u32, u64, u32, Result<market::TypeFetch>)> =
            stream::iter(jobs)
                .map(|(r, h, t)| async move {
                    (
                        r,
                        h,
                        t,
                        market::fetch_type_orders(&self.client, r, t).await,
                    )
                })
                .buffer_unordered(self.client.config().concurrency)
                .collect()
                .await;

        let mut rep = XRegionReport {
            types_requested: cand.len(),
            ..Default::default()
        };
        for &(region, hub) in &self.cfg.xregion.targets {
            let mut fetched: Vec<u32> = Vec::new();
            let mut hub_books: Vec<market::StationOrderBook> = Vec::new();
            for (r, h, t, res) in &results {
                if *r != region || *h != hub {
                    continue;
                }
                match res {
                    Ok(f) => {
                        rep.types_ok += 1;
                        rep.requests += f.pages;
                        rep.orders += f.orders.len() as u64;
                        fetched.push(*t);
                        hub_books.extend(
                            market::aggregate(&f.orders, &market::aggregate_opts())
                                .into_iter()
                                .filter(|b| b.location_id == hub),
                        );
                    }
                    Err(e) => {
                        rep.types_failed += 1;
                        tracing::debug!("T1.5 {region}/{t} 失败：{e}");
                    }
                }
            }
            if fetched.is_empty() {
                // 配错枢纽 ID 的显式兜底：整站一个类型都没拉成功也要喊出来，
                // 别让 T1.5 静默空转（region id 手滑时尤其致命）。
                tracing::error!(
                    "T1.5 枢纽 {hub}（region {region}）本批 0 类型拉成功——检查 region 与站 ID 是否配对"
                );
                continue;
            }
            rep.books_written += self
                .db
                .write_xregion_books(hub, region, &fetched, &hub_books)?;
            self.db.remember_station(hub, true)?;
            // 拉到了类型但聚合后整站 0 本单簿 = 站 ID 与真实枢纽不符（订单都在别的站）。
            if hub_books.is_empty() {
                tracing::error!(
                    "T1.5 枢纽 {hub}（region {region}）本批 0 本单簿——疑似站 ID 与真实枢纽不符"
                );
            }
        }
        let pruned = self
            .db
            .prune_xregion(started_at - market::XREGION_PRUNE_SECS)?;
        rep.seconds = t0.elapsed().as_secs_f64();
        self.db.record_xregion_log(&crate::store::XRegionLog {
            started_at,
            types: rep.types_requested as i64,
            regions: self.cfg.xregion.targets.len() as i64,
            requests: rep.requests as i64,
            orders: rep.orders as i64,
            books_written: rep.books_written as i64,
            failed: rep.types_failed as i64,
            seconds: rep.seconds,
            status: if rep.types_failed == 0 {
                "ok".into()
            } else {
                format!("partial: {} 类型失败", rep.types_failed)
            },
        })?;
        tracing::info!(
            "T1.5：候选 {} 类型 × {} 枢纽 → {} 页 / {} 单 / {} 本跨区单簿（失败 {}，剪除旧行 {}），耗时 {:.1}s",
            rep.types_requested,
            self.cfg.xregion.targets.len(),
            rep.requests,
            rep.orders,
            rep.books_written,
            rep.types_failed,
            pruned,
            rep.seconds
        );
        Ok(Some(rep))
    }

    /// 一轮角色同步 + 亏损告警（M4c）。真的同步与判定在 [`alert::update_round`]（那侧的
    /// 步骤顺序是契约）；这里只做三件事：**开门条件**、令牌 → 角色身份、通道装配。
    ///
    /// `Ok(None)` 的三种形态都是"这一轮没有能跑的回合"，**都不是错误**（静默跳过）：
    /// ① 配置没启用（`EMD_CHAR_SYNC=0` 的语义由 `CharConfig.enabled` 承载，见 `SchedulerConfig`）；
    /// ② 凭据库没有令牌 —— `TokenStore::load` 的 `Ok(None)` 覆盖"没登录 / 已登出 / 凭据库
    ///    打不开 / 内容坏了"四种情形，它们都不该让主循环当成故障；
    /// ③ 同步没拿到挂单快照（见 `alert::update_round` 的第 ④ 步）。
    ///
    /// 令牌只在本函数的调用链上流转：不进日志、不进错误串、不落库（Global Constraint）。
    /// 它过期时这里**不刷新**（刷新编排不在 T12 的边界内，T2 只给了请求体构造）：四端点会
    /// 整轮 401 → ③ 那一支 warn 出"快照未刷新"并跳过这一轮，不静默、也不半推。
    pub async fn run_char_and_alerts(&self) -> Result<Option<AlertRoundReport>> {
        // ① 开关与令牌都在最前面：关着的时候连凭据库都不碰（"关"= 彻底不与外界交互）。
        if !self.cfg.char.enabled {
            return Ok(None);
        }
        let Some(tokens) = self.tokens.load()? else {
            return Ok(None);
        };
        // 角色身份从**令牌自己**里取（T3B 的纯函数，只解码不验签）：角色 id 必须与这个
        // 令牌同源 —— 让"库里恰好有哪一行 char_meta"来决定同步谁，就是拿 A 的令牌去拉 B 的
        // 订单，而 `fetch_auth` 的缓存键正是 URL 里的这个 id。
        let (char_id, _name) = crate::sso::flow::char_from_access_token(&tokens.access_token)?;
        // 通道按配置拼装（P9）：关掉的 / 没填 webhook 的钉钉通道根本不进场，于是"推送关着"
        // 就等于"只判定、只落本地提醒中心"；本地那条恒在（spec §4.5 的回落方案）。
        let channels = PushConfig::load(&self.db)?.channels();
        let refs: Vec<&dyn PushChannel> = channels.iter().map(|c| c.as_ref()).collect();
        alert::update_round(
            &self.db,
            &self.client,
            &tokens.access_token,
            char_id,
            self.cfg.char.backfill_days,
            &refs,
            chrono::Utc::now().timestamp(),
        )
        .await
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

/// 隔轮执行：轮号偶数（T1 6 min 节拍下 ≈ 每 12 分钟）。
pub fn t1_5_due(round: u64) -> bool {
    round % 2 == 0
}

/// 错峰：11:10–11:35 UTC 暂停 T1.5（v3.1 §3.1，给 T2 独占窗口留位）。
pub fn xregion_blackout(t: chrono::DateTime<chrono::Utc>) -> bool {
    use chrono::Timelike;
    let m = t.hour() * 60 + t.minute();
    (11 * 60 + 10..=11 * 60 + 35).contains(&m)
}

/// 候选类型 = 扫描结果按既有排序取前 N 个去重 type_id（保持分数序，天然截断）。
pub fn candidates_from(opps: &[market::Opportunity], top: usize) -> Vec<u32> {
    let mut seen = std::collections::HashSet::new();
    opps.iter()
        .filter(|o| seen.insert(o.type_id))
        .map(|o| o.type_id)
        .take(top)
        .collect()
}

/// T1.5 一趟的结果（日志 + daemon xregion 展示）。
#[derive(Debug, Clone, Copy, Default)]
pub struct XRegionReport {
    pub types_requested: usize,
    pub types_ok: usize,
    pub types_failed: usize,
    pub requests: u32,
    pub orders: u64,
    pub books_written: usize,
    pub seconds: f64,
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
        // T1.5 挂在同一处：默认启用、Top200、三枢纽（方案 v3.1 §4.1）。
        assert!(c.xregion.enabled);
        assert_eq!(c.xregion.candidate_top, 200);
        assert_eq!(c.xregion.targets, market::XREGION_TARGETS);
        // M4c：角色挂链与亏损提醒默认关闭、首启回填 90 天（与 `CharConfig` 的默认同源）。
        assert!(!c.char.enabled, "没配 client_id 之前，任何一轮都不该往外发请求");
        assert_eq!(c.char.backfill_days, 90);
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

    // ---- M4b：T1.5 跨区补拉的纯函数与配置 ---------------------------------

    #[test]
    fn t1_5_runs_on_even_rounds_only() {
        assert!(!t1_5_due(1));
        assert!(t1_5_due(2));
        assert!(!t1_5_due(3));
        assert!(t1_5_due(4));
    }

    #[test]
    fn blackout_window_is_11_10_to_11_35_utc() {
        use chrono::TimeZone;
        let at = |h, m| chrono::Utc.with_ymd_and_hms(2026, 9, 24, h, m, 0).unwrap();
        assert!(!xregion_blackout(at(11, 9)));
        assert!(xregion_blackout(at(11, 10)));
        assert!(xregion_blackout(at(11, 35)));
        assert!(!xregion_blackout(at(11, 36)));
        assert!(!xregion_blackout(at(3, 20)));
    }

    #[test]
    fn candidates_dedupe_types_keep_score_order() {
        let o = |t: u32| market::Opportunity {
            type_id: t,
            buy_loc: 1,
            sell_loc: 2,
            buy_price: 1.0,
            sell_price: 2.0,
            qty: 1,
            net_per_unit: 1.0,
            net_total: 1.0,
            margin_pct: 1.0,
            vol24: 1,
            vol_source: market::VolSource::Depth,
            buy_levels: 1,
            sell_levels: 1,
        };
        let list = vec![o(34), o(34), o(35), o(36), o(35)];
        assert_eq!(candidates_from(&list, 2), vec![34, 35]);
        assert_eq!(candidates_from(&list, 10), vec![34, 35, 36]);
    }

    #[test]
    fn xregion_defaults_match_the_plan() {
        let c = XRegionConfig::default();
        assert!(c.enabled, "方案 v3.1 口径：默认启用，EMD_XREGION=0 关闭");
        assert_eq!(c.candidate_top, 200);
        assert_eq!(c.targets, market::XREGION_TARGETS);
        assert_eq!(c.max_age_secs, market::XREGION_MAX_AGE_SECS);
    }

    // ---- M4c：角色同步与告警钩子（与 T1 节拍同频；数据不可用 ≠ 状态变了）----------

    use crate::alert::{detect_expected_sell, AlertRecord, AlertPayload, NameLookup};
    use crate::char::fifo::{CostSource, FifoCost};
    use crate::config::EsiConfig;
    use crate::market::FeeModel;
    use crate::sso::store::TokenStore;
    use crate::sso::token::TokenSet;
    use crate::store::CharOrder;
    use base64::Engine as _;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const CHAR_ID: u64 = 90_000_001;
    /// 判定基准：2026-09-21T14:13:20Z（自然日是 2026-09-21）。
    const T0: i64 = 1_790_000_000;

    /// 令牌存储的探针：记下 `load` 被调了几次 —— "配置关着时连凭据库都不碰"这条断言
    /// 只有它能提供证据（内存实现自己看不出这点）。
    #[derive(Default)]
    struct SpyStore {
        loads: AtomicUsize,
        token: Option<TokenSet>,
    }

    impl SpyStore {
        /// 造一个能被 `char_from_access_token` 解析的三段式 JWT（它只解码第二段，不验签）。
        fn with_token(char_id: u64) -> Self {
            Self {
                loads: AtomicUsize::new(0),
                token: Some(TokenSet {
                    access_token: jwt(char_id),
                    refresh_token: "REFRESH-TOKEN".into(),
                    expires_at: i64::MAX,
                }),
            }
        }

        fn loads(&self) -> usize {
            self.loads.load(Ordering::Relaxed)
        }
    }

    impl TokenStore for SpyStore {
        fn load(&self) -> Result<Option<TokenSet>> {
            self.loads.fetch_add(1, Ordering::Relaxed);
            Ok(self.token.clone())
        }
        fn save(&self, _t: &TokenSet) -> Result<()> {
            Ok(())
        }
        fn clear(&self) -> Result<()> {
            Ok(())
        }
    }

    fn jwt(char_id: u64) -> String {
        let claims = serde_json::json!({
            "sub": format!("CHARACTER:EVE:{char_id}"),
            "name": "Pilot One",
        });
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claims).unwrap());
        format!("hdr.{payload}.sig")
    }

    fn enabled_cfg(enabled: bool) -> SchedulerConfig {
        SchedulerConfig {
            char: CharConfig {
                client_id: "client-id".into(),
                enabled,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// 一条挂单轨的载荷（只用来种告警行；`detect_expected_sell` 自己会判"亏没亏"）。
    fn payload_for(order_id: i64) -> AlertPayload {
        let o = CharOrder {
            order_id,
            type_id: 34,
            location_id: STATION_JITA,
            is_buy: false,
            price: 97.0,
            volume_remain: 100,
            issued: "2026-09-20T10:00:00Z".to_string(),
            duration: 90,
            fetched_at: T0,
        };
        let costs = HashMap::from([(
            34u32,
            FifoCost {
                avg_cost: 95.0,
                source: CostSource::Known,
            },
        )]);
        detect_expected_sell(
            &[o],
            &costs,
            &FeeModel::default(),
            &HashMap::new(),
            &NameLookup::default(),
            T0,
        )
        .remove(0)
    }

    #[tokio::test]
    async fn char_sync_is_skipped_when_disabled_or_no_token() {
        let client = Arc::new(EsiClient::new(Default::default()).unwrap());
        let db = Arc::new(Db::in_memory().unwrap());

        // ① 开关关着（`EMD_CHAR_SYNC=0` 归一出来的就是这个状态）：即便凭据库里有令牌也一动不动，
        //    连凭据库都不读 —— "关"是彻底不与外界交互，而不是"读了再决定不用"。
        let off = Arc::new(SpyStore::with_token(CHAR_ID));
        let sched = Scheduler::new(client.clone(), db.clone(), enabled_cfg(false))
            .with_tokens(off.clone());
        assert!(
            sched.run_char_and_alerts().await.unwrap().is_none(),
            "关闭 = 静默跳过（不是错误）"
        );
        assert_eq!(off.loads(), 0, "关着的时候不该去读凭据库");
        assert_eq!(client.stats().requests, 0, "关着的时候一个请求都不发");
        assert!(db.char_meta(CHAR_ID).unwrap().is_none(), "没同步过就不该有挂链行");

        // ② 开着但凭据库没有令牌（没登录 / 已登出 / 凭据库打不开 —— `TokenStore::load` 的契约）：
        //    同样静默跳过，不报错、不发请求、不留痕。
        let none = Arc::new(SpyStore::default());
        let sched = Scheduler::new(client.clone(), db.clone(), enabled_cfg(true))
            .with_tokens(none.clone());
        assert!(sched.run_char_and_alerts().await.unwrap().is_none());
        assert_eq!(none.loads(), 1, "启用时才会去读凭据库");
        assert_eq!(client.stats().requests, 0, "没有令牌 = 一个请求都不发");
        assert!(db.char_meta(CHAR_ID).unwrap().is_none());

        // ③ 令牌在、但解析不出角色（凭据库里是别的 subject）：这是**异常**而不是"没登录" ——
        //    如实上抛，由 `run()` 的钩子 warn 掉（不打断主循环），不静默当成没事。
        let weird = Arc::new(SpyStore {
            loads: AtomicUsize::new(0),
            token: Some(TokenSet {
                access_token: "hdr.eyJzdWIiOiJVU0VSOjEifQ.sig".into(),
                refresh_token: "REFRESH-TOKEN".into(),
                expires_at: i64::MAX,
            }),
        });
        let sched = Scheduler::new(client.clone(), db.clone(), enabled_cfg(true)).with_tokens(weird);
        assert!(sched.run_char_and_alerts().await.is_err(), "认不出角色要报出来");
        assert_eq!(client.stats().requests, 0, "解析不出角色就不该发请求");
    }

    /// 只回 403 的桩服务：四个角色端点全部拿不到东西（令牌过期 / 权限不足 / 断网走的是同一条
    /// 分支）。用**真**连接而不是"没人听的端口"：实测本机对闭合端口的连接尝试要 ~2 s 才出结果，
    /// 四个端点就是 8 s，而这条测试盯的只是那一支判断。
    fn forbidden_stub(requests: usize) -> String {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        std::thread::spawn(move || {
            for _ in 0..requests {
                // 请求没来（实现回归了）时别把测试挂死：超时就收摊，断言侧会看到请求数对不上。
                let Ok(Some(req)) = server.recv_timeout(std::time::Duration::from_secs(10)) else {
                    return;
                };
                let _ = req.respond(
                    tiny_http::Response::from_string(r#"{"error":"Forbidden - token expired"}"#)
                        .with_status_code(403),
                );
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    #[tokio::test]
    async fn alert_round_not_run_when_snapshot_is_stale() {
        // 数据不可用 ≠ 状态变了（与 M4b 生命周期同一教训）：本轮挂单快照没刷新时，既不拿旧快照
        // 重新判定（会凭空造出"还在亏"），也不把已有的告警收尾成"已清"。
        // 用桩服务把四个端点全回 403（令牌过期 / 权限不足 / 断网走同一条分支）：这一轮的
        // `orders.ok == false`，旧快照仍是"最后一份好数据"，但**它不是当前盘口**。
        let client = Arc::new(
            EsiClient::new(EsiConfig {
                base_url: forbidden_stub(4),
                ..Default::default()
            })
            .unwrap(),
        );
        let db = Arc::new(Db::in_memory().unwrap());

        // 库里已有上一轮的东西：一张还在旧快照里的挂单、一条挂在它上面的告警行（若这一轮真的跑了，
        // 它会被观测刷新 —— `last_seen_at` 会变），以及一条对应"已经撤掉的单"的告警行（会被收尾）。
        db.replace_char_orders(
            CHAR_ID,
            &[CharOrder {
                order_id: 101,
                type_id: 34,
                location_id: STATION_JITA,
                is_buy: false,
                price: 97.0,
                volume_remain: 100,
                issued: "2026-09-20T10:00:00Z".to_string(),
                duration: 90,
                fetched_at: 0, // 写入时被 now 覆盖
            }],
            T0 - 360,
        )
        .unwrap();
        for order_id in [101i64, 999] {
            let rec = AlertRecord::from_payload(&payload_for(order_id), CHAR_ID, T0 - 600);
            db.save_alert(&rec).unwrap();
        }
        let before = db.load_alerts().unwrap();
        assert_eq!(before.len(), 2);

        let store = Arc::new(SpyStore::with_token(CHAR_ID));
        let sched =
            Scheduler::new(client.clone(), db.clone(), enabled_cfg(true)).with_tokens(store);
        assert!(
            sched.run_char_and_alerts().await.unwrap().is_none(),
            "快照陈旧 → 这一轮不跑（不判定、不落库、不收尾）"
        );
        assert_eq!(
            client.stats().requests,
            4,
            "确实跑了一整趟同步（四个端点各一次；403 不触发重试），不是被别的理由跳过"
        );
        assert_eq!(
            db.load_alerts().unwrap(),
            before,
            "一条告警行的任何一个字段都不该被这一轮动过（last_seen_at 变了 = 拿旧快照重新记账了）"
        );
        assert_eq!(
            db.load_char_orders(CHAR_ID).unwrap().len(),
            1,
            "旧快照也该原样留着（同步失败不得清表）"
        );
        assert!(
            db.char_meta(CHAR_ID).unwrap().is_none(),
            "全灭的一轮不算'同步过'：不刷 last_sync_at、不建行"
        );
    }
}
