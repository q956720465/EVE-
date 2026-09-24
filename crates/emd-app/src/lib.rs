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
use std::time::{Duration, Instant};

use chrono::Utc;
use emd_core::catalog;
use emd_core::collector::InstanceLock;
use emd_core::config::{CharConfig, EsiConfig};
use emd_core::esi::EsiClient;
use emd_core::market::STATION_JITA;
use emd_core::push::{PushConfig, PushConfigEcho};
use emd_core::scheduler::{
    RoundState, Scheduler, SchedulerConfig, KEYRING_ACCOUNT, KEYRING_SERVICE, MIN_INTERVAL,
};
use emd_core::sso::flow::{char_from_access_token, login};
use emd_core::sso::store::{KeyringTokenStore, TokenStore};
use emd_core::sso::token::TokenSet;
use emd_core::store::{now_unix, Db, HistoryBar, ListingRow, TreeNode, TypeBook};
use emd_core::tree;
use serde::{Deserialize, Serialize};
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
    /// 跨区行数据年龄（秒）；None = 常规枢纽行、数据来自本轮 T1 快照。
    /// 有值时代表这行的目标站来自 T1.5 补拉，最长可滞后 12 分钟。诚实标注。
    pub xregion_age_secs: Option<i64>,
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

/// 提醒中心的一行（M4c）。列表用扁平 DTO，但**payload 原文一并带上** —— 口径摘要由前端从
/// 它里面读（`AlertPayload` 是推送卡片与提醒中心的唯一序列化出口，spec §4.4），这一层不另造
/// 第二份结构。名字（类型/站点）是**回填**的当前字典值，查不到用 fallback 串
/// （`type_id N` / `站点 #N`，与 `flip_scan` 同一套字面量）：名字缺失不该杀掉一张告警。
#[derive(Debug, Serialize)]
pub struct AlertRow {
    /// 去重键 `order:{id}` / `tx:{id}`（**TEXT**，原样透传）：用户拿它去库里检索那张单/那笔成交，
    /// 已实现轨的成交 id 只存在于这个串里（`AlertRecord` 没有单列它）。
    pub alert_key: String,
    /// 形态串（`AlertKind::as_str`）：expected_sell_loss / buy_order_trap / realized_loss。
    pub kind: &'static str,
    pub char_id: u64,
    pub type_id: u32,
    pub type_name: String,
    pub location_id: u64,
    pub location_name: String,
    /// 订单方向：true = 买单。
    pub is_buy: bool,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
    /// 本轮观测到的亏损额（正数 ISK）。**不是** `notified_*` 那一组（那组记的是推送史）。
    pub last_loss_isk: f64,
    /// 本轮观测到的亏损率（%，负 = 亏）。
    pub last_margin_pct: f64,
    /// 状态串（`AlertState::as_str`）：new / notified / cleared。
    pub state: &'static str,
    pub notified_at: Option<i64>,
    pub notified_day: Option<String>,
    /// 本条目**今天**被推送的条数（当日全局用量是各条目之和，spec §4.4）。
    pub notified_count_day: u32,
    /// 上次推送时的**亏损率**（百分点）。列名叫 `last_notified_loss`，口径不是金额 ——
    /// 穿透判定比的是"再低 2pp"（`ALERT_DEEPEN_PP`），存 ISK 就没有 pp 可比。名字里不要 isk。
    pub last_notified_margin_pct: Option<f64>,
    /// `AlertPayload` 的 JSON 文本（`alerts.payload` 列原样）。前端从它里面读口径摘要。
    pub payload: String,
}

/// 通道配置的回显（`alert_settings_get` / `alert_settings_set` 的返回值）。
///
/// **密钥只有"是否已配置"一个比特**：明文与打码残片都不给前端（A4），要改密钥只能走
/// `AlertSettingsIn.secret` 的三态。
#[derive(Debug, Serialize)]
pub struct AlertSettings {
    /// 已中段打码的 webhook；空串 = 没配。**它同时是"没改"的载体**：原样回存 =
    /// `save_editing` 保留库里那条（打码串不携带信息，存下去只会把 `***` 写进库）。
    pub webhook: String,
    pub secret_set: bool,
    pub enabled: bool,
    /// 本轮真的会装上的通道名（`local` 恒在；`dingtalk` 只在开关打开且 webhook 填了时在场）。
    /// 让"开关开着但 webhook 还空着"这种"看起来配好了其实不发"的形态一眼可见。
    pub channels: Vec<&'static str>,
}

/// `alert_settings_set` 的入参 DTO。**为什么在命令层自己定义一个**：`PushConfigEcho` 有意只实现
/// `Serialize`（A4：前端不能把打了码的值喂回来），所以入参形状归这一层。
///
/// `secret` 的三态**不是装饰**（T11 的坑）：`None` = 用户没动密钥框（保留原密钥）、
/// `Some("")` = 显式清空（改用纯关键词模式）、`Some(s)` = 换新密钥。把空串当成"没改"会让
/// "只翻个开关"顺手把已配的签名密钥清掉 → 钉钉 errcode 310000 → 通道被禁用，报错话术还会
/// 去怪机器人（那时配置面上看不出任何毛病）。
#[derive(Deserialize)]
pub struct AlertSettingsIn {
    pub webhook: String,
    pub secret: Option<String>,
    pub enabled: bool,
}

/// SSO 挂链状态（`sso_status`）。**只回派生的布尔/时刻/身份，令牌原文一个字都不在里面**
/// （Global Constraints）—— 角色 id 与名字是从令牌自己解出来的，不是从库里"猜"的。
/// `Debug` 是安全的：能被 `{:?}` 打出来的字段本身就不含令牌（这也是本类型的契约）。
#[derive(Debug, Serialize)]
pub struct SsoStatus {
    /// 令牌在**且**能解出角色 = 调度器真能同步。令牌在但解不出（异形/换过格式）时也是 false：
    /// 那种状态同步会整轮失败，界面不该显示"已挂链"。
    pub linked: bool,
    pub char_id: Option<u64>,
    pub char_name: Option<String>,
    /// 库内 `char_meta.last_sync_at`（角色 id 由令牌给出，库里没有"列出所有角色"的查询）。
    pub last_sync_at: Option<i64>,
    pub expires_at: Option<i64>,
    /// 由 `TokenSet::is_expired` 判（提前 60 s 视为过期）—— 判定只此一处，TS 不复制那条边界。
    pub token_expired: bool,
    /// 角色同步开关（`CharConfig.enabled`）：false 时**每个采集轮都静默跳过同步**，
    /// 界面必须把这条说出来，否则用户只能看到"零告警"。
    pub char_sync_enabled: bool,
    /// `client_id` 是否配了（`EMD_CHAR_CLIENT_ID`）。没配时登录按钮要给出可照着做的下一步。
    pub client_id_set: bool,
    /// 令牌在、但解不出角色时的原因（一句能照着查的话；**不含令牌原文** ——
    /// `char_from_access_token` 的报错只带 JWT 段数与字段名）。
    pub token_error: Option<String>,
}

/// 登录成功的回执。**不含令牌**：令牌只落系统凭据库（`TokenStore::save`）与进程内存。
#[derive(Debug, Serialize)]
pub struct SsoLoginOut {
    pub char_id: u64,
    pub name: String,
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
    /// 令牌来源（M4c）。默认系统凭据库；测试注入内存实现。`sso_status`/`sso_logout`/`sso_login`
    /// 三个命令都只经它读写令牌 —— 这条通路上**没有**任何令牌的落库/日志面。
    tokens: Arc<dyn TokenStore>,
    /// SSO 与角色同步配置（`CharConfig::from_env()` 归一的那一份）。`sso_login` 的四个参数
    /// 全部从这里取：`client_id` 与 `redirect_uri` 必须与开发者后台的注册值逐字符一致，
    /// 不能由代码代猜（EVE 是精确匹配）。
    cfg: CharConfig,
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
            let hubs = db.flip_hubs().map_err(err)?;
            let ages = db.xregion_ages().map_err(err)?;
            let now = emd_core::store::now_unix();
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
                // 两站任一在 xregion_ages 里 = 跨区行；取"最老"那份时间戳（=min）算年龄。
                // 按 (站, 类型) 查，避免部分类型拉失败时角标低估年龄。
                let xregion_age_secs = [o.buy_loc, o.sell_loc]
                    .iter()
                    .filter_map(|l| ages.get(&(*l, o.type_id)).copied())
                    .min()
                    .map(|ts| (now - ts).max(0));
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
                    xregion_age_secs,
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

    // ---- M4c：提醒中心（T14） ------------------------------------------------

    /// 凭据库读是阻塞的（Windows 凭据管理器是一次进程间调用），与读库同一待遇：
    /// 丢进 blocking 池，别占住 WebView 的 IPC 线程。
    async fn tokens_load(&self) -> Result<Option<TokenSet>, String> {
        let store = self.tokens.clone();
        tokio::task::spawn_blocking(move || store.load().map_err(err))
            .await
            .map_err(|e| format!("凭据库读取线程异常退出：{e}"))?
    }

    /// 提醒中心列表。全表读回（**含 `Cleared`**）：闸门只管推不推、从不删行，
    /// "这个周期亏过、后来撤单了"本身就是用户要知道的事实（spec §4.4）。
    async fn alerts_list(&self) -> Result<Vec<AlertRow>, String> {
        read(self.db.clone(), |db| {
            let recs = db.load_alerts().map_err(err)?;
            let mut out = Vec::with_capacity(recs.len());
            for r in recs {
                // 名字回填走单条查询，失败不报错（用 fallback 串）—— 名字缺失不该杀掉一行告警。
                let type_name = db
                    .type_name(r.type_id)
                    .map_err(err)?
                    .unwrap_or_else(|| format!("type_id {}", r.type_id));
                let location_name = db
                    .station_name(r.location_id)
                    .map_err(err)?
                    .unwrap_or_else(|| format!("站点 #{}", r.location_id));
                out.push(AlertRow {
                    alert_key: r.alert_key,
                    kind: r.kind.as_str(),
                    char_id: r.char_id,
                    type_id: r.type_id,
                    type_name,
                    location_id: r.location_id,
                    location_name,
                    is_buy: r.is_buy,
                    first_seen_at: r.first_seen_at,
                    last_seen_at: r.last_seen_at,
                    last_loss_isk: r.last_loss_isk,
                    last_margin_pct: r.last_margin_pct,
                    state: r.state.as_str(),
                    notified_at: r.notified_at,
                    notified_day: r.notified_day,
                    notified_count_day: r.notified_count_day,
                    last_notified_margin_pct: r.last_notified_loss,
                    payload: r.payload,
                });
            }
            Ok(out)
        })
        .await
    }

    /// 通道配置的回显（三个旋钮 + 本轮真会装上的通道名）。
    /// **只走 `echo()`**：`PushConfig::load` 的明文只给发送器与保存路径用（A4）。
    async fn alert_settings(&self) -> Result<AlertSettings, String> {
        read(self.db.clone(), |db| {
            let cfg = PushConfig::load(db).map_err(err)?;
            Ok(settings_of(&cfg))
        })
        .await
    }

    /// 改通道配置。写路径只有 `PushConfig::save_editing` 一条（它是唯一能在**不需要密钥明文**
    /// 的前提下改配置的入口），保存成功后回一份新的打码回显 —— 面板据此刷新，
    /// 而不是拿用户输入自己拼（输入里的打码 webhook 与库里的真值不是一回事）。
    async fn alert_settings_save(&self, input: AlertSettingsIn) -> Result<AlertSettings, String> {
        read(self.db.clone(), move |db| {
            let echo = PushConfigEcho {
                // 打码串由 `save_editing` 解读为"保留库里那条"；明文则是新值。
                webhook: input.webhook,
                // `save_editing` 不看这个字段（密钥只走 `new_secret`，它才带得起三态），
                // 填什么都不会进库 —— 占位而已。
                secret_set: false,
                enabled: input.enabled,
            };
            PushConfig::save_editing(db, &echo, input.secret.as_deref()).map_err(err)?;
            let saved = PushConfig::load(db).map_err(err)?;
            Ok(settings_of(&saved))
        })
        .await
    }

    /// SSO 挂链状态。**派生事实进、派生事实出**：令牌只以"链没链上 / 到期时刻 / 从它解出的
    /// 角色身份"三种形态露面，原文一个字都不回前端。
    ///
    /// 读不到令牌**不算错误**：`TokenStore::load` 的 `Ok(None)` 已经把"没登录 / 已登出 /
    /// 凭据库不可用 / 凭据内容坏了"四种情形收拢成同一件事（store 层刻意如此），
    /// 这里照它的口径如实报"未挂链"即可 —— 报错会让整块面板消失，用户连登录按钮都摸不到。
    async fn sso_status(&self) -> Result<SsoStatus, String> {
        let token = self.tokens_load().await?;

        // 角色身份从**令牌自己**里解（与调度器同一判据，`char_from_access_token` 只解码不验签）。
        // 绝不拿库里 `char_meta` 的行反推"现在挂链的是谁"：那可能是上一个角色留下的行，
        // 而同步请求的路径与缓存键正是这个角色 id —— 指错人就是拿 A 的令牌去拉 B 的数据。
        let (char_id, char_name, token_error) = match token.as_ref() {
            None => (None, None, None),
            Some(t) => match char_from_access_token(&t.access_token) {
                Ok((id, name)) => (Some(id), Some(name), None),
                // 报错只带 JWT 段数与字段名，令牌原文不参与（见 `sso::flow` 的实现）。
                Err(e) => (None, None, Some(e.to_string())),
            },
        };

        let last_sync_at = match char_id {
            Some(id) => {
                read(self.db.clone(), move |db| {
                    Ok(db.char_meta(id).map_err(err)?.and_then(|m| m.last_sync_at))
                })
                .await?
            }
            // 没有角色 id 就没有可查的行键（库里没有"列出所有角色"的查询，见函数头）。
            None => None,
        };

        let now = now_unix();
        Ok(SsoStatus {
            linked: char_id.is_some(),
            char_id,
            char_name,
            last_sync_at,
            expires_at: token.as_ref().map(|t| t.expires_at),
            token_expired: token.as_ref().map(|t| t.is_expired(now)).unwrap_or(false),
            char_sync_enabled: self.cfg.enabled,
            client_id_set: !self.cfg.client_id.trim().is_empty(),
            token_error,
        })
    }

    /// 退出登录：清系统凭据库里的令牌。**库内角色数据与告警表一行不动** ——
    /// 退出是"不用这个角色了"，不是"删掉历史"。`clear()` 本身幂等，没登录时再点一次也不报错。
    async fn sso_logout(&self) -> Result<(), String> {
        let store = self.tokens.clone();
        tokio::task::spawn_blocking(move || store.clear().map_err(err))
            .await
            .map_err(|e| format!("凭据库清除线程异常退出：{e}"))?
    }

    /// 发起 SSO 登录（T3B 的编排：起回环 → 开系统浏览器 → 收 code → 换令牌 → 落凭据库）。
    ///
    /// **没配 `client_id` 就当场拒绝**：授权页会把 `client_id` 带在查询串里，空值打开的是一个
    /// 必然报错的页面 —— 用户点一次浏览器、等满 180 秒超时，才从"没反应"里猜出是配置没填。
    /// 这里直接把可照着做的那句话给出来。
    async fn sso_login(&self) -> Result<SsoLoginOut, String> {
        if self.cfg.client_id.trim().is_empty() {
            return Err(
                "先在设置里配 EMD_CHAR_CLIENT_ID / client_id —— 没有它授权页打不开（开发者后台注册应用后取）"
                    .into(),
            );
        }
        // 四个参数全部来自配置：`redirect_uri` 与端口必须逐字符等于开发者后台的注册值，
        // 由用户配、代码不代猜。超时 180 s 是"用户手点授权"的合理上限。
        let out = login(
            &self.cfg.client_id,
            &self.cfg.redirect_uri,
            self.cfg.loopback_port,
            self.tokens.as_ref(),
            Duration::from_secs(180),
        )
        .await
        .map_err(err)?;
        Ok(SsoLoginOut { char_id: out.char_id, name: out.name })
    }
}

/// `PushConfig` → 面板回显（`echo()` 打码 + 本轮真会装上的通道名）。
/// 抽成一处是为了让"回显"只有一条路径：`load()` 的明文永远不往前端走。
fn settings_of(cfg: &PushConfig) -> AlertSettings {
    let echo = cfg.echo();
    AlertSettings {
        webhook: echo.webhook,
        secret_set: echo.secret_set,
        enabled: echo.enabled,
        channels: cfg.channels().iter().map(|c| c.name()).collect(),
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

#[tauri::command]
async fn alerts_list(state: State<'_, AppState>) -> Result<Vec<AlertRow>, String> {
    state.alerts_list().await
}

#[tauri::command]
async fn alert_settings_get(state: State<'_, AppState>) -> Result<AlertSettings, String> {
    state.alert_settings().await
}

#[tauri::command]
async fn alert_settings_set(
    state: State<'_, AppState>,
    input: AlertSettingsIn,
) -> Result<AlertSettings, String> {
    state.alert_settings_save(input).await
}

#[tauri::command]
async fn sso_status(state: State<'_, AppState>) -> Result<SsoStatus, String> {
    state.sso_status().await
}

#[tauri::command]
async fn sso_logout(state: State<'_, AppState>) -> Result<(), String> {
    state.sso_logout().await
}

/// 登录会开系统浏览器并等回调（最长 180 s）：命令是 async 的，等待期不阻塞界面。
/// 回环等待在 `login` 内部已经挪进 `spawn_blocking`，所以在 Tauri 的 async 执行器上安全。
#[tauri::command]
async fn sso_login(state: State<'_, AppState>) -> Result<SsoLoginOut, String> {
    state.sso_login().await
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

    // `SchedulerConfig::default()` 的 char 侧是**关闭**的（`CharConfig::default().enabled = false`）：
    // 用它的话，用户登录成功之后每一轮都静默跳过同步 —— 一条日志都不解释，界面上只剩"永远零告警"。
    // 这里必须走 `from_env()`（与 daemon 的 `alerts --update` 同一份归一：EMD_CHAR_CLIENT_ID /
    // EMD_CHAR_SYNC）。令牌源指向与登录/登出**同一对** keyring 条目：两个常量只在
    // `emd_core::scheduler` 定义一次 —— 条目对不上时表现为"登录成功但每轮静默跳过"，
    // 而"没令牌"不是错误，从现象上完全看不出是条目没对上。`KeyringTokenStore` 是无状态句柄
    // （不带缓存），与界面侧那份指向同一条条目，读写的是同一份令牌。
    let sched = Scheduler::new(
        client.clone(),
        db.clone(),
        SchedulerConfig { char: CharConfig::from_env(), ..Default::default() },
    )
    .with_tokens(Arc::new(KeyringTokenStore::new(KEYRING_SERVICE, KEYRING_ACCOUNT)));

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
        // 界面侧的令牌源与采集线程那份指向同一对 keyring 条目（常量共用一处定义）。
        tokens: Arc::new(KeyringTokenStore::new(KEYRING_SERVICE, KEYRING_ACCOUNT)),
        // `from_env()` 只在这里读一次：`sso_login` 的 client_id/redirect_uri/端口都取自它，
        // 运行期不随环境变量再变（与 `EsiConfig::from_env()` 同一先例）。
        cfg: CharConfig::from_env(),
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
            request_refresh,
            alerts_list,
            alert_settings_get,
            alert_settings_set,
            sso_status,
            sso_logout,
            sso_login
        ])
        .build(tauri::generate_context!())
        .expect("构建 Tauri 应用失败")
        .run(move |_app, event| {
            if let RunEvent::Exit = event {
                on_exit.ask_stop();
            }
        });
}
