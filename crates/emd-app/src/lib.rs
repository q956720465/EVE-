//! Tauri 壳：界面侧只做只读查询，采集由一条自有线程上的调度器负责。
//!
//! 为什么调度器不放进 Tauri 的 async_runtime：`rusqlite::Connection` 不是 `Sync`，
//! 于是 `Arc<Db>` 不是 `Send`，`spawn` 要求的 `Future: Send` 也就满足不了。
//! 单开一条线程配 current-thread runtime，调度器独占一条连接；界面侧再开一条连接读
//! 同一份 WAL 库 —— 采集器整天在写、界面整天在读，WAL 就是为这个形态准备的。
//! 两条连接的建表顺序是固定的：界面侧先开并跑完迁移，线程后开，避免并发迁移抢锁。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::Utc;
use emd_core::catalog;
use emd_core::collector::InstanceLock;
use emd_core::config::EsiConfig;
use emd_core::esi::EsiClient;
use emd_core::market::STATION_JITA;
use emd_core::scheduler::{RoundState, Scheduler, SchedulerConfig, MIN_INTERVAL};
use emd_core::store::{Db, HistoryBar, ListingRow, TreeNode, TypeBook};
use emd_core::tree;
use serde::Serialize;
use tauri::{RunEvent, State};
use tokio::sync::watch;

#[cfg(test)]
mod tests;

type DbRef = Arc<Mutex<Db>>;

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// 查询一律丢到 blocking 池：一轮快照 1.6 万行，跑在 WebView 的 IPC 线程上会卡界面。
async fn read<T, F>(db: DbRef, f: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(&mut Db) -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut guard = db.lock().map_err(err)?;
        f(&mut guard)
    })
    .await
    .map_err(|e| format!("查询线程异常退出：{e}"))?
}

/// 前端看到的枢纽行：`hub_pool` 并上站点名。
#[derive(Serialize)]
pub struct HubRow {
    pub location_id: u64,
    pub order_count: u64,
    pub share_pct: f64,
    pub rank: usize,
    pub name: String,
}

/// 倒卖机会行：引擎输出 + 名字解析（前端表格直接渲染）。
#[derive(Serialize)]
pub struct FlipRow {
    pub type_id: u32,
    pub type_name: String,
    pub buy_loc: u64,
    pub buy_loc_name: String,
    pub sell_loc: u64,
    pub sell_loc_name: String,
    pub buy_price: f64,
    pub sell_price: f64,
    pub qty: u64,
    pub net_per_unit: f64,
    pub net_total: f64,
    pub margin_pct: f64,
    pub vol24: u64,
    /// "history" | "depth" —— 排序键来源角标。
    pub vol_source: &'static str,
    pub buy_levels: u32,
    pub sell_levels: u32,
}

#[derive(Serialize)]
pub struct FlipScanOut {
    pub rows: Vec<FlipRow>,
    pub pairs_evaluated: usize,
    pub dropped_batch: usize,
    pub dropped_shortfall: usize,
    pub dropped_threshold: usize,
    /// 快照年龄（秒）；None = 还没跑过采集。
    pub age_secs: Option<i64>,
    /// 参数面板回显（扫完即回，面板不用再单独拉）。
    pub params: emd_core::market::FlipParams,
    /// 有效费率由 Rust 算好（%）：面板只显示，不在 TS 复制公式（spec R6）。
    pub effective_sales_tax_pct: f64,
    pub effective_broker_pct: f64,
}

#[derive(Serialize)]
pub struct TrialOut {
    pub net_per_unit: f64,
    pub net_total: f64,
    pub margin_pct: f64,
}

#[derive(Serialize)]
pub struct StatusOut {
    pub round: u64,
    pub stage: String,
    pub last_seconds: f64,
    pub orders: i64,
    pub rows_written: i64,
    pub hubs: usize,
    /// 距下一轮剩余毫秒。由"发布时刻 + 当时声明的 next_in"推算，
    /// 所以前端 5 秒轮询一次也能看到连续倒数。
    pub next_in_ms: u64,
    pub snapshot_lm: Option<String>,
    pub remaining_tokens: u32,
    pub jita_rows: i64,
    /// (组, 已命名类型, 分类)。空树时前端要给出下一步指令而不是空白。
    pub tree: (u64, u64, u64),
    /// 本进程是否真的在采集。锁被另一进程持有时为 false ——
    /// 不拿它区分的话，"查看者"与"采集者刚启动"在界面上长得一模一样。
    pub collecting: bool,
}

#[derive(Clone)]
struct AppState {
    db: DbRef,
    client: Arc<EsiClient>,
    latest: Arc<Mutex<Option<(RoundState, Instant)>>>,
    shutdown: watch::Sender<bool>,
    /// 锁归属的唯一真相：采集线程抢到锁才置 true。`collecting` 与"查看者不发
    /// 网络请求"两处判断都读它 —— 用"收过状态广播"反推会把冷启动建树的
    /// ~80 秒误标成只读，而那时本进程正在满负荷打 ESI。
    collector: Arc<AtomicBool>,
}

/// 命令体都放这里，`#[tauri::command]` 只做参数拆装。
/// 分成两层的理由是前者能被普通测试直接调用，后者要起 Tauri 运行时才测得动。
impl AppState {
    async fn tree(&self) -> Result<Vec<TreeNode>, String> {
        read(self.db.clone(), |db| db.tree().map_err(err)).await
    }

    async fn listing(&self, group_id: u32, location_id: Option<u64>) -> Result<Vec<ListingRow>, String> {
        let at = location_id.unwrap_or(STATION_JITA);
        read(self.db.clone(), move |db| {
            db.group_listing(group_id, at).map_err(err)
        })
        .await
    }

    async fn detail(&self, type_id: u32, location_id: Option<u64>) -> Result<Option<TypeBook>, String> {
        let at = location_id.unwrap_or(STATION_JITA);
        read(self.db.clone(), move |db| db.book_row(at, type_id).map_err(err)).await
    }

    async fn hubs(&self) -> Result<Vec<HubRow>, String> {
        read(self.db.clone(), |db| {
            let hubs = db.hub_pool().map_err(err)?;
            let mut out = Vec::with_capacity(hubs.len());
            for h in hubs {
                let name = db
                    .station_name(h.location_id)
                    .map_err(err)?
                    .unwrap_or_else(|| format!("站点 #{}", h.location_id));
                out.push(HubRow {
                    location_id: h.location_id,
                    order_count: h.order_count,
                    share_pct: h.share_pct,
                    rank: h.rank,
                    name,
                });
            }
            Ok(out)
        })
        .await
    }

    /// 倒卖扫描：装配输入 → 纯函数引擎 → 名字解析。
    /// 名字回填失败不报错（用 fallback 串），名字缺失不该杀掉一张利润表。
    async fn flip_scan(&self) -> Result<FlipScanOut, String> {
        read(self.db.clone(), |db| {
            let books = db.load_books().map_err(err)?;
            let hubs = db.hub_pool().map_err(err)?;
            let vol = db.latest_vol24().map_err(err)?;
            let params = db.get_flip_params().map_err(err)?;
            let age = db.last_round_age_secs().map_err(err)?;
            let out = emd_core::market::scan(&books, &hubs, &params, &vol);
            let mut rows = Vec::with_capacity(out.opportunities.len());
            for o in out.opportunities {
                let type_name = db
                    .type_name(o.type_id)
                    .map_err(err)?
                    .unwrap_or_else(|| format!("type_id {}", o.type_id));
                let buy_loc_name = db
                    .station_name(o.buy_loc)
                    .map_err(err)?
                    .unwrap_or_else(|| format!("站点 #{}", o.buy_loc));
                let sell_loc_name = db
                    .station_name(o.sell_loc)
                    .map_err(err)?
                    .unwrap_or_else(|| format!("站点 #{}", o.sell_loc));
                rows.push(FlipRow {
                    type_id: o.type_id,
                    type_name,
                    buy_loc: o.buy_loc,
                    buy_loc_name,
                    sell_loc: o.sell_loc,
                    sell_loc_name,
                    buy_price: o.buy_price,
                    sell_price: o.sell_price,
                    qty: o.qty,
                    net_per_unit: o.net_per_unit,
                    net_total: o.net_total,
                    margin_pct: o.margin_pct,
                    vol24: o.vol24,
                    vol_source: match o.vol_source {
                        emd_core::market::VolSource::History => "history",
                        emd_core::market::VolSource::Depth => "depth",
                    },
                    buy_levels: o.buy_levels,
                    sell_levels: o.sell_levels,
                });
            }
            Ok(FlipScanOut {
                rows,
                pairs_evaluated: out.stats.pairs_evaluated,
                dropped_batch: out.stats.dropped_batch,
                dropped_shortfall: out.stats.dropped_shortfall,
                dropped_threshold: out.stats.dropped_threshold,
                age_secs: age,
                effective_sales_tax_pct: params.fees.effective_sales_tax() * 100.0,
                effective_broker_pct: params.fees.effective_broker() * 100.0,
                params,
            })
        })
        .await
    }

    async fn flip_params(&self) -> Result<emd_core::market::FlipParams, String> {
        read(self.db.clone(), |db| db.get_flip_params().map_err(err)).await
    }

    async fn flip_save(&self, p: emd_core::market::FlipParams) -> Result<(), String> {
        read(self.db.clone(), move |db| db.set_flip_params(&p).map_err(err)).await
    }

    /// 试算：费率公式只在 emd-core 实现（spec R6），这里只做输入防护。
    /// 价格必须 >0：0 会被 settle 判成"成本非正"返回全零，界面按
    /// `net_total < 0` 判色会渲染成绿色的"0.00 收益"——错误信号比报错更糟。
    async fn trial(&self, buy_price: f64, sell_price: f64, qty: u64) -> Result<TrialOut, String> {
        if qty == 0 {
            return Err("数量必须大于 0".into());
        }
        if !(buy_price > 0.0) || !(sell_price > 0.0) {
            return Err("买价/卖价必须是大于 0 的数字".into());
        }
        read(self.db.clone(), move |db| {
            let params = db.get_flip_params().map_err(err)?;
            let (net_per_unit, net_total, margin_pct) =
                emd_core::market::trial(buy_price, sell_price, qty, &params);
            Ok(TrialOut { net_per_unit, net_total, margin_pct })
        })
        .await
    }

    async fn status(&self) -> Result<StatusOut, String> {
        let snapshot = self.latest.lock().map_err(err)?.clone();
        let (round, stage, last_seconds, orders, rows_written, lm, next_in_ms) = match &snapshot {
            Some((s, at)) => (
                s.round,
                format!("{:?}", s.stage),
                s.last_seconds,
                s.orders,
                s.rows_written,
                s.snapshot_lm.clone(),
                s.next_in.saturating_sub(at.elapsed()).as_millis() as u64,
            ),
            None => read(self.db.clone(), viewer_status).await?,
        };
        let tokens = self.client.remaining_tokens();
        let (hubs, jita_rows, tree, meta_lm) = read(self.db.clone(), |db| {
            let hubs = db.hub_pool().map(|v| v.len()).unwrap_or(0);
            // 只数行数：station_book() 会把 1.3 万本盘全反序列化，5 秒一次的轮询扛不住。
            let jita: i64 = db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM station_orders WHERE location_id = ?1",
                    [STATION_JITA as i64],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            let lm = db.get_meta("last_snapshot_lm").ok().flatten();
            Ok((hubs, jita, db.tree_counts().map_err(err)?, lm))
        })
        .await?;

        // 采集中的那条状态不带 Last-Modified（还没拿到响应头），但库里躺着上一轮的时刻 ——
        // 不回填的话"快照 x 分钟前"会在每次采集的 60 秒里显示成"未知"。
        let lm = lm.or(meta_lm);

        Ok(StatusOut {
            round,
            stage,
            last_seconds,
            orders,
            rows_written,
            hubs,
            next_in_ms,
            snapshot_lm: lm,
            remaining_tokens: tokens,
            jita_rows,
            tree,
            collecting: self.collector.load(Ordering::Relaxed),
        })
    }

    /// 单类型日线（蜡烛图数据源）。region 缺省 The Forge —— 历史是
    /// `(region, type)` 维度且无站点维度（方案 §3.3），跨站对比永远只有当前快照。
    async fn history(
        &self,
        type_id: u32,
        region_id: Option<u32>,
    ) -> Result<Vec<HistoryBar>, String> {
        let region = region_id.unwrap_or(emd_core::market::REGION_FORGE);
        read(self.db.clone(), move |db| {
            db.history_series(region, type_id, None).map_err(err)
        })
        .await
    }

    async fn watch_list(&self) -> Result<Vec<(u32, Option<String>)>, String> {
        read(self.db.clone(), |db| db.watch_list().map_err(err)).await
    }

    async fn watch_add(&self, type_id: u32) -> Result<(), String> {
        read(self.db.clone(), move |db| {
            db.watch_add(type_id, None).map_err(err)
        })
        .await
    }

    async fn watch_remove(&self, type_id: u32) -> Result<bool, String> {
        read(self.db.clone(), move |db| db.watch_remove(type_id).map_err(err)).await
    }

    /// 搜索：先问 ESI（精确名），拿不到再退回本地前缀匹配。
    /// 只取 `inventory_types` —— 实测 "Nyx" 会同时返回军团与角色，混进结果会误导。
    async fn search(&self, word: &str) -> Result<Vec<ListingRow>, String> {
        let word = word.trim().to_string();
        if word.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.client.clone();
        let hits = if self.collector.load(Ordering::Relaxed) {
            catalog::search(&client, &[word.as_str()]).await.unwrap_or_default()
        } else {
            // 查看者不发 ESI 请求：搜索撞的是服务端按 UA 计的共享配额，
            // 两个进程各自看不到对方的本地桶。本地别名表兜底，命中质量降一点，
            // 但不偷跑别人的配额。
            Default::default()
        };
        let ids = hits.type_ids();
        let db = self.db.clone();

        if !ids.is_empty() {
            return read(db, move |db| {
                db.listing_for_types(&ids, STATION_JITA).map_err(err)
            })
            .await;
        }
        read(db, move |db| {
            let like = db.find_types_like(&word, 200).map_err(err)?;
            let ids: Vec<u32> = like.into_iter().map(|(id, _)| id).collect();
            db.listing_for_types(&ids, STATION_JITA).map_err(err)
        })
        .await
    }

    /// 手动刷新。故意**不允许突破缓存**：绕过 `Expires` 重复拉全量正是 ESI 会封禁的行为，
    /// 所以这里只回报"还要等多久"，真正的下一次拉取由调度器按节拍执行。
    async fn refresh_hint(&self) -> Result<String, String> {
        let last = read(self.db.clone(), |db| db.last_round().map_err(err)).await?;
        let need = MIN_INTERVAL.as_secs() as i64;
        match last {
            Some(r) => {
                let since = Utc::now().timestamp() - r.started_at;
                if since < need {
                    Err(format!(
                        "上一轮开始于 {since} 秒前；ESI 订单簿缓存 300 秒，到点自动更新（还需 {} 秒）",
                        need - since
                    ))
                } else {
                    Ok("已过缓存窗口，下一轮将立即更新".into())
                }
            }
            None => Ok("还没有任何一轮，稍候会自动开始".into()),
        }
    }
}

#[tauri::command]
async fn get_tree(state: State<'_, AppState>) -> Result<Vec<TreeNode>, String> {
    state.tree().await
}

#[tauri::command]
async fn get_history(
    state: State<'_, AppState>,
    type_id: u32,
    region_id: Option<u32>,
) -> Result<Vec<HistoryBar>, String> {
    state.history(type_id, region_id).await
}

#[tauri::command]
async fn get_watchlist(state: State<'_, AppState>) -> Result<Vec<(u32, Option<String>)>, String> {
    state.watch_list().await
}

#[tauri::command]
async fn add_watch(state: State<'_, AppState>, type_id: u32) -> Result<(), String> {
    state.watch_add(type_id).await
}

#[tauri::command]
async fn remove_watch(state: State<'_, AppState>, type_id: u32) -> Result<bool, String> {
    state.watch_remove(type_id).await
}

#[tauri::command]
async fn get_listing(
    state: State<'_, AppState>,
    group_id: u32,
    location_id: Option<u64>,
) -> Result<Vec<ListingRow>, String> {
    state.listing(group_id, location_id).await
}

#[tauri::command]
async fn get_detail(
    state: State<'_, AppState>,
    type_id: u32,
    location_id: Option<u64>,
) -> Result<Option<TypeBook>, String> {
    state.detail(type_id, location_id).await
}

#[tauri::command]
async fn get_hubs(state: State<'_, AppState>) -> Result<Vec<HubRow>, String> {
    state.hubs().await
}

#[tauri::command]
async fn get_status(state: State<'_, AppState>) -> Result<StatusOut, String> {
    state.status().await
}

#[tauri::command]
async fn search_types(state: State<'_, AppState>, word: String) -> Result<Vec<ListingRow>, String> {
    state.search(&word).await
}

#[tauri::command]
async fn scan_flip(state: State<'_, AppState>) -> Result<FlipScanOut, String> {
    state.flip_scan().await
}

#[tauri::command]
async fn get_flip_params(state: State<'_, AppState>) -> Result<emd_core::market::FlipParams, String> {
    state.flip_params().await
}

#[tauri::command]
async fn set_flip_params(
    state: State<'_, AppState>,
    params: emd_core::market::FlipParams,
) -> Result<(), String> {
    state.flip_save(params).await
}

#[tauri::command]
async fn trial_calc(
    state: State<'_, AppState>,
    buy_price: f64,
    sell_price: f64,
    qty: u64,
) -> Result<TrialOut, String> {
    state.trial(buy_price, sell_price, qty).await
}

/// 只回话"还要等多久"，不代替调度器发起采集。
#[tauri::command]
async fn request_refresh(state: State<'_, AppState>) -> Result<String, String> {
    state.refresh_hint().await
}

fn db_path() -> PathBuf {
    std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .map(|p| p.join("EveMarketDesk").join("emd.sqlite3"))
        .unwrap_or_else(|| PathBuf::from("emd.sqlite3"))
}

/// 查看者模式的 status 回落：采集循环活在另一个进程里，`latest` 通道永远是空 ——
/// 不读 `round_log` 的话，界面会在"采集正常进行"的时候显示一屏零。
/// 倒计时按节拍估算（6 分钟轮减掉上一轮开始至今的秒数），不追求和采集者同步。
fn viewer_status(db: &mut Db) -> Result<(u64, String, f64, i64, i64, Option<String>, u64), String> {
    let Some(r) = db.last_round().map_err(err)? else {
        return Ok((0, "Idle".into(), 0.0, 0, 0, None, 0u64));
    };
    let round = db.round_count().unwrap_or(0);
    let lm = db.get_meta("last_snapshot_lm").ok().flatten();
    let cadence = std::time::Duration::from_secs(360);
    let since = (Utc::now().timestamp() - r.started_at).max(0);
    let next_in = cadence
        .saturating_sub(std::time::Duration::from_secs(since as u64))
        .as_millis() as u64;
    Ok((
        round,
        "WaitingNext".into(),
        r.seconds,
        r.orders,
        r.rows_written,
        lm,
        next_in,
    ))
}

/// 采集线程：一条连接、一个 current-thread runtime、一个调度器。
/// 启动时先把上一轮遗留的未命名站点解完名，再进 6 分钟节拍 ——
/// 解名走 `universe/names` 批量接口，几十秒内完成，不影响首轮时间。
///
/// 这里必须用 current-thread：`sched.run()` 的 future 里经过 `&Db` → `Connection`，
/// 不是 `Send`，多线程 runtime 的 `spawn` 收不下它。状态转发那条 future 是 `Send`，
/// 所以在同一个 runtime 上 `tokio::spawn` 没问题。
fn spawn_collector(
    client: Arc<EsiClient>,
    path: PathBuf,
    data_dir: PathBuf,
    latest: Arc<Mutex<Option<(RoundState, Instant)>>>,
    stop_rx: watch::Receiver<bool>,
    collector: Arc<AtomicBool>,
) {
    if let Err(e) = std::thread::Builder::new().name("emd-collector".into()).spawn(move || {
        let run = |db: Db| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(err)?;
            rt.block_on(collect_with_lock(
                client.clone(),
                Arc::new(db),
                data_dir.clone(),
                latest.clone(),
                stop_rx.clone(),
                collector.clone(),
            ))
        };
        // 线程体内的错误必须落日志：吞掉的 Err 表现得和"永远只读"一模一样，
        // 没有任何痕迹可查。
        if let Err(e) = Db::open(&path).map_err(err).and_then(run) {
            tracing::error!("采集线程退出：{e}");
        }
    }) {
        tracing::error!("无法启动采集线程：{e}");
    }
}

/// 抢锁 → 冷启动维护 → 常驻采集。`serve` 先开着时每 60 s 重试一次；
/// 对方退出后自动接管，不需要重启壳 —— 一次抢锁失败就永久只读会把"交接"做反。
async fn collect_with_lock(
    client: Arc<EsiClient>,
    db: Arc<Db>,
    data_dir: PathBuf,
    latest: Arc<Mutex<Option<(RoundState, Instant)>>>,
    mut stop_rx: watch::Receiver<bool>,
    collector: Arc<AtomicBool>,
) -> Result<(), String> {
    // 锁守卫在函数尾才 drop，覆盖整个采集期；提前退出（shutdown）同样释放。
    let _lock = loop {
        match InstanceLock::acquire(&data_dir) {
            Some(g) => break g,
            None => {
                tracing::info!("采集锁由另一进程持有，本窗口只读；每 60 秒重试接管");
                collector.store(false, Ordering::Relaxed);
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
                    _ = stop_rx.wait_for(|v| *v) => return Ok(()),
                }
            }
        }
    };
    collector.store(true, Ordering::Relaxed);

    let sched = Scheduler::new(client.clone(), db.clone(), SchedulerConfig::default());

    // 冷启动补树：分类字典不来自订单簿，只建一次就长期有效。
    // 放在首轮之前是有意的 —— 两条 future 共用同一条 Connection 时，
    // 调度器的 `unchecked_transaction` 会把 build_tree 的写入一起卷进事务，
    // 所以这里必须串行，不能 join。
    // 读不到计数时当作"树已存在"：宁可空着左栏，也不凭一个查询异常就花两千令牌重建。
    if db.tree_counts().map(|t| t.0).unwrap_or(1) == 0 {
        tracing::info!("本地还没有分类树，开始建一次（约 1 千个请求 / 80 秒）");
        match tree::build_tree(&client, &db).await {
            Ok(s) => tracing::info!(
                "分类树就绪：{} 组（{} 已发布）/ {} 个类型名 / {} 个分类，耗时 {:.1}s",
                s.groups,
                s.published_groups,
                s.types,
                s.categories,
                s.elapsed.as_secs_f64()
            ),
            Err(e) => tracing::warn!("分类树未建成，界面左栏会是空的：{e}"),
        }
    }

    if !db.unnamed_npc_stations(400).unwrap_or_default().is_empty() {
        match catalog::resolve_missing(&client, &db, 400).await {
            Ok(n) if n > 0 => tracing::info!("启动补解站点名：{n} 个"),
            Ok(_) => {}
            Err(e) => tracing::warn!("站点名解析未完成：{e}"),
        }
    }

    let mut rx = sched.subscribe();
    tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let s = rx.borrow().clone();
            if let Ok(mut g) = latest.lock() {
                *g = Some((s, Instant::now()));
            }
        }
    });

    if let Err(e) = sched.run(stop_rx).await {
        tracing::error!("采集循环退出：{e}");
    }
    Ok(())
}

impl AppState {
    fn ask_stop(&self) {
        // 不打断进行中的一轮：半轮写库会留下比上一轮更差的数据。
        let _ = self.shutdown.send(true);
    }
}

pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,reqwest=warn,hyper=warn".into()),
        )
        .init();

    let path = db_path();
    if let Some(dir) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(dir) {
            tracing::error!("无法创建数据目录 {dir:?}：{e}");
            return;
        }
    }

    // 界面侧连接先建：迁移在此跑完，采集线程后开就只会看到已建好的表。
    let db = match Db::open(&path) {
        Ok(db) => Arc::new(Mutex::new(db)),
        Err(e) => {
            tracing::error!("打开数据库失败 {path:?}：{e}");
            return;
        }
    };
    let client = match EsiClient::new(EsiConfig::from_env()) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!("构建 ESI 客户端失败：{e}");
            return;
        }
    };

    let (stop_tx, stop_rx) = watch::channel(false);
    let state = AppState {
        db,
        client: client.clone(),
        latest: Arc::new(Mutex::new(None)),
        shutdown: stop_tx,
        collector: Arc::new(AtomicBool::new(false)),
    };

    spawn_collector(
        client,
        path.clone(),
        path.parent().map(PathBuf::from).unwrap_or_default(),
        state.latest.clone(),
        stop_rx,
        state.collector.clone(),
    );

    let on_exit = state.clone();
    tauri::Builder::default()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            get_tree,
            get_listing,
            get_detail,
            get_hubs,
            get_status,
            get_history,
            get_watchlist,
            add_watch,
            remove_watch,
            search_types,
            scan_flip,
            get_flip_params,
            set_flip_params,
            trial_calc,
            request_refresh
        ])
        .build(tauri::generate_context!())
        .expect("构建 Tauri 应用失败")
        .run(move |_app, event| {
            if let RunEvent::Exit = event {
                on_exit.ask_stop();
            }
        });
}
