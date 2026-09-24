//! SQLite 存储层。WAL + 单写者串行化（方案 §7：6 分钟任务不得并发写）。

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::{Error, Result};
use crate::esi::Watermark;
use crate::market::StationOrderBook;
use crate::store::schema::MIGRATIONS;

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
pub struct PriceRow {
    pub date: String,
    pub type_id: u32,
    pub adjusted: Option<f64>,
    pub average: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct SyncState {
    pub url_key: String,
    pub endpoint: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub expires_raw: Option<String>,
    pub expires_at_unix: Option<i64>,
    pub fail_streak: i64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Counts {
    pub rows: u64,
    pub types: u64,
    pub stations: u64,
    pub npc_rows: u64,
    pub both_sides: u64,
}

/// 详情面板一行：阶梯已解析成结构，前端不必再吃 JSON 字符串。
#[derive(Debug, Clone, serde::Serialize)]
pub struct TypeBook {
    pub type_id: u32,
    pub name: String,
    pub location_id: u64,
    pub location_name: String,
    pub bid_depth: Vec<crate::market::PriceLevel>,
    pub ask_depth: Vec<crate::market::PriceLevel>,
    pub bid_levels: u32,
    pub ask_levels: u32,
    pub skipped_stale: u32,
    pub skipped_thin: u32,
    pub skipped_wholesale: u32,
    pub snapshot_lm: Option<String>,
    pub updated_at: i64,
}

/// 分类树的一个分类。
#[derive(Debug, Clone, serde::Serialize)]
pub struct TreeNode {
    pub category_id: u32,
    pub name: String,
    pub groups: Vec<TreeGroup>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TreeGroup {
    pub group_id: u32,
    pub name: String,
    pub type_count: usize,
}

/// 物品列表的一行：类型 + 指定站点的当前最优价。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ListingRow {
    pub type_id: u32,
    pub name: String,
    pub best_bid: Option<f64>,
    pub bid_qty: u64,
    pub best_ask: Option<f64>,
    pub ask_qty: u64,
    pub bid_levels: u32,
    pub ask_levels: u32,
}

/// `round_log` 的一行。调度器每轮写一条，UI 的环形进度和"上一轮耗时"都读它。
#[derive(Debug, Clone)]
pub struct RoundRecord {
    pub started_at: i64,
    pub region_id: i64,
    pub pages: i64,
    pub orders: i64,
    pub rows_written: i64,
    pub seconds: f64,
    pub decoded_bytes: i64,
    pub over_network: i64,
    pub drift_retries: i64,
    pub snapshot_lm: Option<String>,
    pub status: String,
}

/// 吉他视图需要的行。深度仍是 JSON 文本，由前端解析 —— 避免在 Rust 侧
/// 为每个类型构造完整阶梯再跨 IPC 复制一遍。
#[derive(Debug, Clone, serde::Serialize)]
pub struct StationRow {
    pub type_id: u32,
    pub best_bid: Option<f64>,
    pub bid_qty: u64,
    pub best_ask: Option<f64>,
    pub ask_qty: u64,
    pub bid_levels: u32,
    pub ask_levels: u32,
    pub bid_depth: String,
    pub ask_depth: String,
}

/// `market_history` 的一行。字段名与 ESI 响应对齐（`highest`/`lowest`）。
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRow {
    pub region_id: u32,
    pub type_id: u32,
    pub date: String,
    pub average: Option<f64>,
    pub highest: Option<f64>,
    pub lowest: Option<f64>,
    pub volume: u64,
    pub order_count: u64,
}

/// 前端画蜡烛图用的一条日线。
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct HistoryBar {
    pub date: String,
    pub average: Option<f64>,
    pub highest: Option<f64>,
    pub lowest: Option<f64>,
    pub volume: u64,
    pub order_count: u64,
}

/// 某类型在本地的积累程度。§3.3 明确要求按**该类型**显示，不能用全局均值。
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Coverage {
    pub days: u32,
    pub first: Option<String>,
    pub last: Option<String>,
}

/// 一趟 T3 取数的计划项。
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryTarget {
    pub type_id: u32,
    pub tier: String,
    pub reason: String,
}

/// `history_log` 的一行 —— 验收数字的落盘凭据。
#[derive(Debug, Clone)]
pub struct HistoryPass {
    pub started_at: i64,
    pub region_id: u32,
    pub targets: u32,
    pub requested: u32,
    pub gated: u32,
    pub rows_written: u32,
    pub absent: u32,
    pub failed: u32,
    pub seconds: f64,
    pub decoded_bytes: u64,
    pub tokens_local: u32,
    pub error_remain: Option<u32>,
    pub status: String,
}

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        // WAL + NORMAL：采集器整天在写，读写并发是常态；FULL 会让每轮写盘慢一个量级。
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "journal_mode", "MEMORY")?;
        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn migrate(&self) -> Result<()> {
        // 版本表必须先于任何查询存在 —— 否则冷库第一条 `SELECT version FROM schema_migration`
        // 就炸在鸡生蛋上。
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migration (
                version     INTEGER PRIMARY KEY,
                applied_at  TEXT NOT NULL
            );",
        )?;

        let applied: i64 = self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migration",
                [],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);

        for (version, note, sql) in MIGRATIONS {
            if version <= &applied {
                continue;
            }
            let tx = self.conn.unchecked_transaction()?;
            tx.execute_batch(sql)
                .map_err(|e| Error::Config(format!("迁移 {version} 失败: {e}")))?;
            tx.execute(
                "INSERT INTO schema_migration (version, applied_at) VALUES (?1, datetime('now'))",
                params![version],
            )?;
            tx.commit()?;
            tracing::info!("已应用迁移 {version}：{note}");
        }
        Ok(())
    }

    pub fn schema_version(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migration",
                [],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    /// 整表替换。实测 75,648 行/轮，单事务 + prepared statement 约几十毫秒。
    /// 必须原子：否则读侧会在清空与写入之间看到一个空市场。
    pub fn write_snapshot(&self, books: &[StationOrderBook], snapshot_lm: Option<&str>) -> Result<usize> {
        let ts = now_unix();
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DELETE FROM station_orders", [])?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO station_orders (
                    location_id, type_id, is_npc, best_bid, bid_qty, best_ask, ask_qty,
                    bid_levels, ask_levels, bid_depth, ask_depth,
                    skipped_stale, skipped_thin, skipped_wholesale, snapshot_lm, updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            )?;
            for b in books {
                stmt.execute(params![
                    b.location_id as i64,
                    b.type_id,
                    b.is_npc_station as i64,
                    b.best_bid,
                    b.bid_qty as i64,
                    b.best_ask,
                    b.ask_qty as i64,
                    b.bid_levels,
                    b.ask_levels,
                    serde_json::to_string(&b.bid_depth).unwrap_or_else(|_| "[]".into()),
                    serde_json::to_string(&b.ask_depth).unwrap_or_else(|_| "[]".into()),
                    b.skipped_stale,
                    b.skipped_thin,
                    b.skipped_wholesale,
                    snapshot_lm,
                    ts,
                ])?;
            }
        }
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('last_snapshot_lm', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![snapshot_lm.unwrap_or("")],
        )?;
        tx.commit()?;
        Ok(books.len())
    }

    pub fn counts(&self) -> Result<Counts> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*),
                    COUNT(DISTINCT type_id),
                    COUNT(DISTINCT location_id),
                    SUM(is_npc),
                    SUM(CASE WHEN best_bid IS NOT NULL AND best_ask IS NOT NULL THEN 1 ELSE 0 END)
             FROM station_orders",
            [],
            |r| {
                Ok(Counts {
                    rows: r.get::<_, i64>(0)? as u64,
                    types: r.get::<_, i64>(1)? as u64,
                    stations: r.get::<_, i64>(2)? as u64,
                    npc_rows: r.get::<_, Option<i64>>(3)?.unwrap_or(0) as u64,
                    both_sides: r.get::<_, Option<i64>>(4)?.unwrap_or(0) as u64,
                })
            },
        )?)
    }

    pub fn write_prices(&self, rows: &[PriceRow]) -> Result<usize> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO prices_daily (date, type_id, adjusted, average) VALUES (?1,?2,?3,?4)
                 ON CONFLICT(date, type_id) DO UPDATE SET adjusted=?3, average=?4",
            )?;
            for p in rows {
                stmt.execute(params![p.date, p.type_id, p.adjusted, p.average])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    pub fn record_health(&self, endpoint: &str, wm: &Watermark) -> Result<()> {
        self.conn.execute(
            "INSERT INTO esi_health (ts, endpoint, grp, limit_s, used, remaining, error_remain, error_reset, cache_hit)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                now_unix(),
                endpoint,
                wm.group,
                wm.limit,
                wm.used.map(|v| v as i64),
                wm.remaining.map(|v| v as i64),
                wm.error_remain.map(|v| v as i64),
                wm.error_reset_secs.map(|v| v as i64),
                wm.cache_status.map(|s| matches!(s, crate::esi::CacheStatus::Hit) as i64),
            ],
        )?;
        Ok(())
    }

    pub fn record_sync_ok(&self, s: &SyncState) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sync_state (url_key, endpoint, etag, last_modified, expires_raw, expires_at_unix, last_ok_at, fail_streak)
             VALUES (?1,?2,?3,?4,?5,?6, datetime('now'), 0)
             ON CONFLICT(url_key) DO UPDATE SET
               etag=excluded.etag, last_modified=excluded.last_modified,
               expires_raw=excluded.expires_raw, expires_at_unix=excluded.expires_at_unix,
               last_ok_at=excluded.last_ok_at, fail_streak=0",
            params![
                s.url_key,
                s.endpoint,
                s.etag,
                s.last_modified,
                s.expires_raw,
                s.expires_at_unix
            ],
        )?;
        Ok(())
    }

    /// 崩溃恢复用：某端点上次失败后连续失败几次、下次最早何时可发。
    pub fn note_sync_fail(&self, url_key: &str) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO sync_state (url_key, endpoint, last_ok_at, fail_streak)
             VALUES (?1, ?1, NULL, 1)
             ON CONFLICT(url_key) DO UPDATE SET fail_streak = fail_streak + 1",
            params![url_key],
        )?;
        let streak: i64 = self
            .conn
            .query_row(
                "SELECT fail_streak FROM sync_state WHERE url_key = ?1",
                params![url_key],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        Ok(streak)
    }

    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT value FROM meta WHERE key = ?1", params![key], |r| {
                r.get(0)
            })
            .optional()?)
    }

    /// 维护任务：裁剪过期健康水位记录，避免诊断表无限增长。
    pub fn prune_health(&self, keep_rows: u32) -> Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM esi_health WHERE ts <
               (SELECT MIN(ts) FROM (SELECT ts FROM esi_health ORDER BY ts DESC LIMIT ?1))",
            params![keep_rows],
        )?;
        Ok(n)
    }

    // ---- M1：枢纽池 / 站点字典 / 采集台账 ------------------------------------

    /// 整表替换，与 `station_orders` 同一套纪律。
    pub fn write_hub_pool(&self, hubs: &[crate::market::Hub]) -> Result<usize> {
        let ts = now_unix();
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DELETE FROM hub_pool", [])?;
        for h in hubs {
            tx.execute(
                "INSERT INTO hub_pool (location_id, order_count, share_pct, rank, computed_at)
                 VALUES (?1,?2,?3,?4,?5)",
                params![h.location_id as i64, h.order_count as i64, h.share_pct, h.rank as i64, ts],
            )?;
        }
        tx.commit()?;
        Ok(hubs.len())
    }

    pub fn hub_pool(&self) -> Result<Vec<crate::market::Hub>> {
        let mut stmt = self
            .conn
            .prepare("SELECT location_id, order_count, share_pct, rank FROM hub_pool ORDER BY rank")?;
        let rows = stmt.query_map([], |r| {
            Ok(crate::market::Hub {
                location_id: r.get::<_, i64>(0)? as u64,
                order_count: r.get::<_, i64>(1)? as u64,
                share_pct: r.get(2)?,
                rank: r.get::<_, i64>(3)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    pub fn remember_station(&self, location_id: u64, is_npc: bool) -> Result<()> {
        self.conn.execute(
            "INSERT INTO stations (location_id, is_npc) VALUES (?1, ?2)
             ON CONFLICT(location_id) DO UPDATE SET is_npc = excluded.is_npc",
            params![location_id as i64, is_npc as i64],
        )?;
        Ok(())
    }

    /// upsert 而不是 UPDATE：未登记的站点上 UPDATE 影响 0 行，那次 ESI 解出来的名字
    /// 就静默丢了。字典表不该带"必须先登记"这种隐式前提。
    pub fn name_station(&self, location_id: u64, name: &str, system_id: Option<u32>) -> Result<()> {
        self.conn.execute(
            "INSERT INTO stations (location_id, name, system_id, is_npc, resolved_at)
             VALUES (?1, ?2, ?3, ?4, datetime('now'))
             ON CONFLICT(location_id) DO UPDATE SET
               name = excluded.name, system_id = excluded.system_id,
               resolved_at = excluded.resolved_at",
            params![
                location_id as i64,
                name,
                system_id.map(|v| v as i64),
                crate::market::LocationKind::of(location_id).tradable_publicly() as i64
            ],
        )?;
        Ok(())
    }

    /// 还没有名字的 NPC 站 —— 解析前先排除玩家结构（公开接口拿不到名字，别浪费请求）。
    pub fn unnamed_npc_stations(&self, limit: u32) -> Result<Vec<u64>> {
        let mut stmt = self.conn.prepare(
            "SELECT location_id FROM stations WHERE name IS NULL AND is_npc = 1 LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |r| r.get::<_, i64>(0).map(|v| v as u64))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    pub fn station_name(&self, location_id: u64) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row(
                "SELECT name FROM stations WHERE location_id = ?1",
                params![location_id as i64],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_round(&self, r: &RoundRecord) -> Result<()> {
        self.conn.execute(
            "INSERT INTO round_log (started_at, region_id, pages, orders, rows_written, seconds,
                decoded_bytes, over_network, drift_retries, snapshot_lm, status)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                r.started_at,
                r.region_id,
                r.pages,
                r.orders,
                r.rows_written,
                r.seconds,
                r.decoded_bytes,
                r.over_network,
                r.drift_retries,
                r.snapshot_lm,
                r.status,
            ],
        )?;
        Ok(())
    }

    /// 环形进度与"上一轮怎么样"都读这张表；状态只取 ok / 非 ok 两类由调用方写。
    pub fn last_round(&self) -> Result<Option<RoundRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT started_at, region_id, pages, orders, rows_written, seconds,
                    decoded_bytes, over_network, drift_retries, snapshot_lm, status
             FROM round_log ORDER BY id DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map([], |r| {
            Ok(RoundRecord {
                started_at: r.get(0)?,
                region_id: r.get(1)?,
                pages: r.get(2)?,
                orders: r.get(3)?,
                rows_written: r.get(4)?,
                seconds: r.get(5)?,
                decoded_bytes: r.get(6)?,
                over_network: r.get(7)?,
                drift_retries: r.get(8)?,
                snapshot_lm: r.get(9)?,
                status: r.get(10)?,
            })
        })?;
        Ok(rows.next().and_then(std::result::Result::ok))
    }

    /// 本地快照是哪个星域的（最近一次成功轮次）。历史取数用它做前置校验：
    /// L0 的流动池排名完全来自 `station_orders`，换星域取历史却拿吉他所在域的盘口
    /// 排名，会静默取错清单。
    pub fn collected_region(&self) -> Result<Option<u32>> {
        Ok(self
            .conn
            .query_row(
                "SELECT region_id FROM round_log WHERE status = 'ok' ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get::<_, i64>(0).map(|v| v as u32),
            )
            .optional()?)
    }

    /// 已记录的轮数。调度器用它接上重启前的轮号。
    pub fn round_count(&self) -> Result<u64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM round_log", [], |r| r.get::<_, i64>(0))? as u64)
    }

    // ---- M2：分类树与列表查询 ----------------------------------------------

    pub fn write_groups(&self, groups: &[crate::tree::GroupDetail]) -> Result<usize> {
        let tx = self.conn.unchecked_transaction()?;
        // 整表替换：树是"当前上游快照"，留着旧组会让已删的组继续出现在树里。
        tx.execute("DELETE FROM inv_groups", [])?;
        for g in groups {
            tx.execute(
                "INSERT INTO inv_groups (group_id, category_id, name, types, published)
                 VALUES (?1,?2,?3,?4,?5)
                 ON CONFLICT(group_id) DO UPDATE SET
                   category_id=excluded.category_id, name=excluded.name,
                   types=excluded.types, published=excluded.published",
                params![
                    g.group_id,
                    g.category_id,
                    g.name,
                    serde_json::to_string(&g.types).unwrap_or_else(|_| "[]".into()),
                    g.published as i64
                ],
            )?;
        }
        tx.commit()?;
        Ok(groups.len())
    }

    /// `found` 是 `(type_id, name)`，`group_of` 反查所属组。同时写小写别名表，
    /// 让搜索走索引而不是 `LOWER(name)` 全表扫。
    pub fn write_types(
        &self,
        found: &[(u64, String)],
        group_of: &std::collections::HashMap<u32, u32>,
    ) -> Result<usize> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO inv_types (type_id, name, group_id, resolved_at)
                 VALUES (?1,?2,?3,datetime('now'))
                 ON CONFLICT(type_id) DO UPDATE SET name=excluded.name, group_id=excluded.group_id",
            )?;
            let mut alias = tx.prepare_cached(
                "INSERT OR IGNORE INTO type_aliases (key, type_id) VALUES (?1,?2)",
            )?;
            for (id, name) in found {
                let group = group_of.get(&(*id as u32)).copied();
                stmt.execute(params![*id as i64, name, group.map(|g| g as i64)])?;
                alias.execute(params![name.to_lowercase(), *id as i64])?;
                for w in name.split_whitespace() {
                    alias.execute(params![w.to_lowercase(), *id as i64])?;
                }
            }
        }
        tx.commit()?;
        Ok(found.len())
    }

    pub fn write_categories(&self, details: &[crate::tree::CategoryDetail]) -> Result<usize> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DELETE FROM inv_categories", [])?;
        let mut n = 0;
        for c in details {
            if let Some(name) = &c.name {
                tx.execute(
                    "INSERT INTO inv_categories (category_id, name) VALUES (?1,?2)
                     ON CONFLICT(category_id) DO UPDATE SET name=excluded.name",
                    params![c.category_id, name],
                )?;
                n += 1;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// 分类 → 组（仅 published），带每组类型数。整棵树一次给出，前端不必逐层请求。
    ///
    /// 类型数在 Rust 侧算而不是用 `JSON_LENGTH()`：`rusqlite` 的 `bundled` 构建
    /// 里没开 JSON1（实测报 `no such function: JSON_LENGTH`），别依赖 SQLite 编译特性。
    pub fn tree(&self) -> Result<Vec<TreeNode>> {
        let mut stmt = self.conn.prepare(
            "SELECT g.category_id, COALESCE(c.name, CAST(g.category_id AS TEXT)),
                    g.group_id, COALESCE(g.name, CAST(g.group_id AS TEXT)), g.types
             FROM inv_groups g LEFT JOIN inv_categories c ON c.category_id = g.category_id
             WHERE g.published = 1
             ORDER BY 3, 4",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)? as u32,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)? as u32,
                r.get::<_, String>(3)?,
            ))
        })?;
        let rows: Vec<_> = rows.collect::<std::result::Result<_, _>>()?;

        // 计数只数 inv_types 里真正解出名字的类型 —— 和 group_listing 的
        // FROM inv_types 同一张表，树上写的和列表摆出来的天生一致。
        let mut counter = self
            .conn
            .prepare("SELECT COUNT(*) FROM inv_types WHERE group_id = ?1")?;

        let mut out: Vec<TreeNode> = Vec::new();
        for (cat_id, cat_name, gid, gname) in rows {
            let count: usize = counter
                .query_row(params![gid as i64], |r| r.get::<_, i64>(0))
                .unwrap_or(0) as usize;
            match out.iter_mut().find(|t| t.category_id == cat_id) {
                Some(t) => t.groups.push(TreeGroup {
                    group_id: gid,
                    name: gname,
                    type_count: count,
                }),
                None => out.push(TreeNode {
                    category_id: cat_id,
                    name: cat_name,
                    groups: vec![TreeGroup {
                        group_id: gid,
                        name: gname,
                        type_count: count,
                    }],
                }),
            }
        }
        Ok(out)
    }

    /// 某组下的类型 + 吉他的当前最优买卖价（M2 物品列表页）。
    pub fn group_listing(&self, group_id: u32, location_id: u64) -> Result<Vec<ListingRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT t.type_id, t.name, o.best_bid, o.bid_qty, o.best_ask, o.ask_qty,
                    o.bid_levels, o.ask_levels
             FROM inv_types t
             LEFT JOIN station_orders o
               ON o.type_id = t.type_id AND o.location_id = ?2
             WHERE t.group_id = ?1
             ORDER BY t.name",
        )?;
        let rows = stmt.query_map(params![group_id, location_id as i64], |r| {
            Ok(ListingRow {
                type_id: r.get::<_, i64>(0)? as u32,
                name: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                best_bid: r.get(2)?,
                // LEFT JOIN 未命中时整列是 NULL，列本身的 NOT NULL DEFAULT 不生效。
                bid_qty: r.get::<_, Option<i64>>(3)?.unwrap_or_default() as u64,
                best_ask: r.get(4)?,
                ask_qty: r.get::<_, Option<i64>>(5)?.unwrap_or_default() as u64,
                bid_levels: r.get::<_, Option<i64>>(6)?.unwrap_or_default() as u32,
                ask_levels: r.get::<_, Option<i64>>(7)?.unwrap_or_default() as u32,
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// 本地前缀/别名搜索。`universe/ids` 是精确匹配，输入一半时靠这张表补全。
    pub fn find_types_like(&self, fragment: &str, limit: u32) -> Result<Vec<(u32, String)>> {
        let needle = format!("{}%", fragment.trim().to_lowercase());
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT t.type_id, t.name FROM type_aliases a
             JOIN inv_types t ON t.type_id = a.type_id
             WHERE a.key LIKE ?1 AND t.name IS NOT NULL
             ORDER BY length(a.key), t.name LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![needle, limit], |r| {
            Ok((r.get::<_, i64>(0)? as u32, r.get::<_, String>(1)?))
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    pub fn tree_counts(&self) -> Result<(u64, u64, u64)> {
        let g = self
            .conn
            .query_row("SELECT COUNT(*) FROM inv_groups WHERE published=1", [], |r| {
                r.get::<_, i64>(0)
            })?;
        let t = self.conn.query_row(
            "SELECT COUNT(*) FROM inv_types WHERE name IS NOT NULL",
            [],
            |r| r.get::<_, i64>(0),
        )?;
        let c = self
            .conn
            .query_row("SELECT COUNT(*) FROM inv_categories", [], |r| {
                r.get::<_, i64>(0)
            })?;
        Ok((g as u64, t as u64, c as u64))
    }

    pub fn type_name(&self, type_id: u32) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row(
                "SELECT name FROM inv_types WHERE type_id = ?1",
                params![type_id],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    /// 给定一批类型，返回它们在指定站点的行情行（搜索结果的形状与列表页一致）。
    /// 名字以本地树为准；树里还没有的类型显示成 `#ID` —— ESI 认识它但我们还没建到。
    pub fn listing_for_types(&self, ids: &[u32], location_id: u64) -> Result<Vec<ListingRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows_sql = String::from("SELECT column1 AS type_id FROM (VALUES ");
        for k in 0..ids.len() {
            if k > 0 {
                rows_sql.push(',');
            }
            rows_sql.push_str(&format!("(?{})", k + 1));
        }
        rows_sql.push(')');
        let sql = format!(
            "SELECT s.type_id, COALESCE(i.name, '#' || s.type_id),
                    o.best_bid, o.bid_qty, o.best_ask, o.ask_qty, o.bid_levels, o.ask_levels
             FROM ({rows_sql}) s
             LEFT JOIN inv_types i ON i.type_id = s.type_id
             LEFT JOIN station_orders o ON o.type_id = s.type_id AND o.location_id = ?{}
             ORDER BY i.name",
            ids.len() + 1
        );

        let mut args: Vec<i64> = ids.iter().map(|i| *i as i64).collect();
        args.push(location_id as i64);
        let mut stmt = self.conn.prepare(&sql)?;
        let mapped = stmt.query_map(rusqlite::params_from_iter(args.iter()), |r| {
            Ok(ListingRow {
                type_id: r.get::<_, i64>(0)? as u32,
                name: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                best_bid: r.get(2)?,
                bid_qty: r.get::<_, Option<i64>>(3)?.unwrap_or_default() as u64,
                best_ask: r.get(4)?,
                ask_qty: r.get::<_, Option<i64>>(5)?.unwrap_or_default() as u64,
                bid_levels: r.get::<_, Option<i64>>(6)?.unwrap_or_default() as u32,
                ask_levels: r.get::<_, Option<i64>>(7)?.unwrap_or_default() as u32,
            })
        })?;
        Ok(mapped.collect::<std::result::Result<_, _>>()?)
    }

    /// 单站单类型的完整阶梯（详情面板用）。
    pub fn book_row(&self, location_id: u64, type_id: u32) -> Result<Option<TypeBook>> {
        let row = self
            .conn
            .query_row(
                "SELECT o.type_id, COALESCE(t.name, CAST(o.type_id AS TEXT)), o.location_id,
                        COALESCE(s.name, CAST(o.location_id AS TEXT)),
                        o.bid_depth, o.ask_depth, o.bid_levels, o.ask_levels,
                        o.skipped_stale, o.skipped_thin, o.skipped_wholesale,
                        o.snapshot_lm, o.updated_at
                 FROM station_orders o
                 LEFT JOIN inv_types t ON t.type_id = o.type_id
                 LEFT JOIN stations s ON s.location_id = o.location_id
                 WHERE o.location_id = ?1 AND o.type_id = ?2",
                params![location_id as i64, type_id],
                |r| {
                    Ok(TypeBook {
                        type_id: r.get::<_, i64>(0)? as u32,
                        name: r.get(1)?,
                        location_id: r.get::<_, i64>(2)? as u64,
                        location_name: r.get(3)?,
                        bid_depth: serde_json::from_str(&r.get::<_, String>(4)?)
                            .unwrap_or_default(),
                        ask_depth: serde_json::from_str(&r.get::<_, String>(5)?)
                            .unwrap_or_default(),
                        bid_levels: r.get(6)?,
                        ask_levels: r.get(7)?,
                        skipped_stale: r.get(8)?,
                        skipped_thin: r.get(9)?,
                        skipped_wholesale: r.get(10)?,
                        snapshot_lm: r.get(11)?,
                        updated_at: r.get::<_, i64>(12)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    /// 单站点完整单簿，按类型排序 —— M1 吉他视图的数据源。
    pub fn station_book(
        &self,
        location_id: u64,
        only_two_sided: bool,
    ) -> Result<Vec<StationRow>> {
        let sql = format!(
            "SELECT type_id, best_bid, bid_qty, best_ask, ask_qty, bid_levels, ask_levels,
                    bid_depth, ask_depth
             FROM station_orders WHERE location_id = ?1{}
             ORDER BY type_id",
            if only_two_sided { " AND best_bid IS NOT NULL AND best_ask IS NOT NULL" } else { "" }
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params![location_id as i64], |r| {
            Ok(StationRow {
                type_id: r.get::<_, i64>(0)? as u32,
                best_bid: r.get(1)?,
                bid_qty: r.get::<_, i64>(2)? as u64,
                best_ask: r.get(3)?,
                ask_qty: r.get::<_, i64>(4)? as u64,
                bid_levels: r.get(5)?,
                ask_levels: r.get(6)?,
                bid_depth: r.get(7)?,
                ask_depth: r.get(8)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    // ---- M3：区域历史日线与取数范围 ----------------------------------------

    /// 该 URL 的 `Expires` 是否已过期（可以发请求）。没有记录视为已过期。
    ///
    /// 这条闸门为什么必须落库而不能只靠 `EsiClient` 的内存缓存：history 的
    /// 实测 `Expires` 是**次日 11:05**（约 24 h 后），而内存缓存把 TTL 夹在
    /// `[5 s, 360 s]`（那是为 300 s 的订单簿设计的）。不持久化的话，每天重启一次
    /// 就能合法地每天多打一轮 1 200 个请求。
    pub fn sync_due(&self, url_key: &str, now: i64) -> Result<bool> {
        let expires: Option<i64> = self
            .conn
            .query_row(
                "SELECT expires_at_unix FROM sync_state WHERE url_key = ?1",
                params![url_key],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        Ok(match expires {
            Some(e) => e <= now,
            None => true,
        })
    }

    /// 实测 404 不是错误而是答案：该 (region, type) 在 ESI 侧没有任何成交记录
    /// （`{"error":"Type not found!"}`）。它没有 `Expires` 头可依据，所以本地给一个
    /// 保守的复核周期 —— 没有这个记录，每天的回填都会为同一批死类型各烧 1 点
    /// 全局错误限额（实测每个 404 让 `X-Esi-Error-Limit-Remain` 减 1）。
    pub fn note_history_absent(&self, url_key: &str, until: i64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sync_state (url_key, endpoint, expires_at_unix, last_ok_at)
             VALUES (?1, 'markets-history-absent', ?2, datetime('now'))
             ON CONFLICT(url_key) DO UPDATE SET
               endpoint = excluded.endpoint,
               expires_at_unix = excluded.expires_at_unix,
               last_ok_at = excluded.last_ok_at,
               fail_streak = 0",
            params![url_key, until],
        )?;
        Ok(())
    }

    /// upsert 而非整表替换：一次请求带 418 天，其中 417 天是已有旧行，
    /// 只有最新那天是新增。冲突时全列覆盖，让上游的事后修数能落下来。
    pub fn write_history(&self, rows: &[HistoryRow]) -> Result<usize> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO market_history (region_id, type_id, date, average, highest, lowest, volume, order_count)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
                 ON CONFLICT(region_id, type_id, date) DO UPDATE SET
                   average=excluded.average, highest=excluded.highest, lowest=excluded.lowest,
                   volume=excluded.volume, order_count=excluded.order_count",
            )?;
            for r in rows {
                stmt.execute(params![
                    r.region_id,
                    r.type_id,
                    r.date,
                    r.average,
                    r.highest,
                    r.lowest,
                    r.volume as i64,
                    r.order_count as i64
                ])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    /// ESI 的滚动窗口只有 418 天，上游不再给更老的日期，本地留着只会永远拿不到
    /// 对应的新值。裁剪是卫生措施，不是容量手段。
    pub fn prune_history(&self, keep_days: i64) -> Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM market_history WHERE date < date('now', ?1)",
            params![format!("-{} days", keep_days)],
        )?;
        Ok(n)
    }

    /// 某个类型的日线。`from_date` 给定时只取该日之后（含），用于区间切换。
    pub fn history_series(
        &self,
        region_id: u32,
        type_id: u32,
        from_date: Option<&str>,
    ) -> Result<Vec<HistoryBar>> {
        let (sql, mut args): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = (
            format!(
                "SELECT date, average, highest, lowest, volume, order_count
                 FROM market_history WHERE region_id = ?1 AND type_id = ?2{}
                 ORDER BY date",
                if from_date.is_some() { " AND date >= ?3" } else { "" }
            ),
            vec![Box::new(region_id as i64), Box::new(type_id as i64)],
        );
        if let Some(d) = from_date {
            args.push(Box::new(d.to_string()));
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(args.iter()), |r| {
            Ok(HistoryBar {
                date: r.get(0)?,
                average: r.get(1)?,
                highest: r.get(2)?,
                lowest: r.get(3)?,
                volume: r.get::<_, i64>(4)? as u64,
                order_count: r.get::<_, i64>(5)? as u64,
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// 某类型的积累情况。**零行必须报 None**：`COUNT/MIN/MAX` 是聚合，空表也会返回
    /// 一行 `(0, NULL, NULL)`，不判空的话 UI 会显示"已积累 0/418 天"而不是"尚无历史"。
    pub fn history_coverage(&self, region_id: u32, type_id: u32) -> Result<Option<Coverage>> {
        Ok(self
            .conn
            .query_row(
                "SELECT COUNT(*), MIN(date), MAX(date) FROM market_history
                 WHERE region_id = ?1 AND type_id = ?2",
                params![region_id as i64, type_id as i64],
                |r| {
                    Ok(Coverage {
                        days: r.get::<_, i64>(0)? as u32,
                        first: r.get(1)?,
                        last: r.get(2)?,
                    })
                },
            )
            .optional()?
            .filter(|c| c.days > 0))
    }

    pub fn history_totals(&self) -> Result<(u64, u64, u64)> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*), COUNT(DISTINCT type_id), COUNT(DISTINCT region_id) FROM market_history",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, i64>(1)? as u64,
                    r.get::<_, i64>(2)? as u64,
                ))
            },
        )?)
    }

    pub fn watch_add(&self, type_id: u32, note: Option<&str>) -> Result<()> {
        self.conn.execute(
            "INSERT INTO watchlist (type_id, note, added_at) VALUES (?1,?2,?3)
             ON CONFLICT(type_id) DO UPDATE SET
               note = COALESCE(excluded.note, watchlist.note),
               added_at = watchlist.added_at",
            params![type_id, note, now_unix()],
        )?;
        // 自选即刻进 L0，同时留一份范围记录，便于 UI 说明它为什么在被取数。
        self.note_scope(&[type_id], crate::market::history::TIER_L0, "watchlist")
            .map(|_| ())
    }

    pub fn watch_remove(&self, type_id: u32) -> Result<bool> {
        let n = self
            .conn
            .execute("DELETE FROM watchlist WHERE type_id = ?1", params![type_id])?;
        self.conn.execute(
            "DELETE FROM history_scope WHERE type_id = ?1 AND reason = 'watchlist'",
            params![type_id],
        )?;
        Ok(n > 0)
    }

    pub fn watch_list(&self) -> Result<Vec<(u32, Option<String>)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT type_id, note FROM watchlist ORDER BY added_at DESC, type_id")?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, i64>(0)? as u32, r.get::<_, Option<String>>(1)?))
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// 记入取数范围。`first_seen` 保留最早那次，重复登记只推后 `last_seen`。
    pub fn note_scope(&self, type_ids: &[u32], tier: &str, reason: &str) -> Result<usize> {
        let now = now_unix();
        let tx = self.conn.unchecked_transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO history_scope (type_id, tier, reason, first_seen, last_seen)
                 VALUES (?1,?2,?3,?4,?4)
                 ON CONFLICT(type_id, tier) DO UPDATE SET
                   last_seen = excluded.last_seen, reason = excluded.reason",
            )?;
            for id in type_ids {
                stmt.execute(params![*id as i64, tier, reason, now])?;
                n += 1;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn scope_types(&self, tier: &str) -> Result<Vec<u32>> {
        let mut stmt = self
            .conn
            .prepare("SELECT type_id FROM history_scope WHERE tier = ?1 ORDER BY type_id")?;
        let rows = stmt.query_map(params![tier], |r| r.get::<_, i64>(0).map(|v| v as u32))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// 高流动池：**枢纽池站点上**双向盘齐全、按**单位成交量**排序。
    ///
    /// 口径选的是"最优档挂单的数量之和"，不是 ISK 金额。实测过 ISK 口径的坑：
    /// 一顶 25 亿的旗舰单体挂单会把 3.9 ISK 的 Tritanium 挤出前列，而那类东西
    /// 一周成交两顶 —— 40 个类型只拿回 6,086 行日线（平均 152 天，满窗是 418 天），
    /// 正好是"根本没什么成交"的形状。§3.3 要的是高**流动**，不是高单价。
    ///
    /// 只在枢纽池里排名（§4.3 同一条理由）：死站上的 3 笔挂单不该进自选清单。
    /// 这个口径与本地快照一致 —— 快照没有逐笔成交，只有当前盘口。
    pub fn liquid_types(&self, limit: u32) -> Result<Vec<u32>> {
        let mut stmt = self.conn.prepare(
            "SELECT o.type_id FROM station_orders o
             JOIN hub_pool h ON h.location_id = o.location_id
             WHERE o.best_bid IS NOT NULL AND o.best_ask IS NOT NULL
             GROUP BY o.type_id
             ORDER BY SUM(o.ask_qty + o.bid_qty) DESC,
                      SUM(o.ask_levels + o.bid_levels) DESC,
                      o.type_id
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |r| r.get::<_, i64>(0).map(|v| v as u32))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// L0 = 自选 ∪ 高流动池（补齐到 `l0_cap`）；L1 = 近 30 天活跃、按覆盖天数升序
    /// 滚动补 `l1_daily` 个（§3.3）。自选不受 `l0_cap` 限制 —— 用户点名的不能挤掉。
    pub fn history_targets(
        &self,
        region_id: u32,
        l0_cap: u32,
        l1_daily: u32,
        now: i64,
    ) -> Result<Vec<HistoryTarget>> {
        let mut out: Vec<HistoryTarget> = Vec::new();
        let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::new();

        for (id, _) in self.watch_list()? {
            if seen.insert(id) {
                out.push(HistoryTarget {
                    type_id: id,
                    tier: crate::market::history::TIER_L0.into(),
                    reason: "watchlist".into(),
                });
            }
        }
        let room = l0_cap.saturating_sub(out.len() as u32);
        if room > 0 {
            for id in self.liquid_types(room)? {
                if seen.insert(id) {
                    out.push(HistoryTarget {
                        type_id: id,
                        tier: crate::market::history::TIER_L0.into(),
                        reason: "liquid-pool".into(),
                    });
                }
            }
        }

        if l1_daily > 0 {
            let mut stmt = self.conn.prepare(
                "SELECT s.type_id,
                        (SELECT COUNT(*) FROM market_history m
                          WHERE m.region_id = ?1 AND m.type_id = s.type_id) AS days
                 FROM history_scope s
                 WHERE s.tier = ?2 AND s.last_seen >= ?3
                 ORDER BY days ASC, s.last_seen DESC, s.type_id ASC
                 LIMIT ?4",
            )?;
            let rows = stmt.query_map(
                params![
                    region_id as i64,
                    crate::market::history::TIER_L1,
                    now - 30 * 86_400,
                    l1_daily
                ],
                |r| Ok(r.get::<_, i64>(0)? as u32),
            )?;
            for id in rows.flatten() {
                if seen.insert(id) {
                    out.push(HistoryTarget {
                        type_id: id,
                        tier: crate::market::history::TIER_L1.into(),
                        reason: "active-candidate".into(),
                    });
                }
            }
        }
        Ok(out)
    }

    pub fn record_history_pass(&self, p: &HistoryPass) -> Result<()> {
        self.conn.execute(
            "INSERT INTO history_log (started_at, region_id, targets, requested, gated, rows_written,
                 absent, failed, seconds, decoded_bytes, tokens_local, error_remain, status)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                p.started_at,
                p.region_id as i64,
                p.targets as i64,
                p.requested as i64,
                p.gated as i64,
                p.rows_written as i64,
                p.absent as i64,
                p.failed as i64,
                p.seconds,
                p.decoded_bytes as i64,
                p.tokens_local as i64,
                p.error_remain.map(|v| v as i64),
                p.status,
            ],
        )?;
        Ok(())
    }

    pub fn last_history_pass(&self) -> Result<Option<HistoryPass>> {
        let mut stmt = self.conn.prepare(
            "SELECT started_at, region_id, targets, requested, gated, rows_written, absent, failed,
                    seconds, decoded_bytes, tokens_local, error_remain, status
             FROM history_log ORDER BY id DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map([], |r| {
            Ok(HistoryPass {
                started_at: r.get(0)?,
                region_id: r.get::<_, i64>(1)? as u32,
                targets: r.get::<_, i64>(2)? as u32,
                requested: r.get::<_, i64>(3)? as u32,
                gated: r.get::<_, i64>(4)? as u32,
                rows_written: r.get::<_, i64>(5)? as u32,
                absent: r.get::<_, i64>(6)? as u32,
                failed: r.get::<_, i64>(7)? as u32,
                seconds: r.get(8)?,
                decoded_bytes: r.get::<_, i64>(9)? as u64,
                tokens_local: r.get::<_, i64>(10)? as u32,
                error_remain: r.get::<_, Option<i64>>(11)?.map(|v| v as u32),
                status: r.get(12)?,
            })
        })?;
        Ok(rows.next().and_then(std::result::Result::ok))
    }

    /// 最近一次回填请求是否落在"今天"（UTC 日）。调度器用它决定要不要跑 T3。
    pub fn last_history_request_day(&self) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row(
                "SELECT date(started_at, 'unixepoch') FROM history_log
                 WHERE requested > 0 OR gated > 0 ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{aggregate, AggregateOptions, Order, STATION_JITA};
    use chrono::{Duration, TimeZone, Utc};

    fn order(price: f64, is_buy: bool, vol: u64, loc: u64, ty: u32) -> Order {
        Order {
            id: (price * 100.0) as u64 + vol,
            type_id: ty,
            location_id: loc,
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

    fn sample_books() -> Vec<StationOrderBook> {
        let mut orders = Vec::new();
        for (i, p) in [4.0, 4.1, 4.2].iter().enumerate() {
            orders.push(order(*p, false, (i as u64 + 1) * 1000, 60003760, 34));
        }
        for (i, p) in [3.9, 3.8, 3.7].iter().enumerate() {
            orders.push(order(*p, true, (i as u64 + 1) * 500, 60003760, 34));
        }
        // 玩家结构：13 位，应被标为不可公开交易
        orders.push(order(5.0, false, 10, 1044752365771, 34));
        orders.push(order(5.1, false, 10, 1044752365771, 34));
        orders.push(order(5.2, false, 10, 1044752365771, 34));
        aggregate(
            &orders,
            &AggregateOptions {
                now: Utc.with_ymd_and_hms(2026, 9, 23, 12, 0, 0).unwrap(),
                min_levels: 1,
                ..Default::default()
            },
        )
    }

    #[test]
    fn migration_is_idempotent_and_reports_version() {
        let db = Db::in_memory().unwrap();
        assert_eq!(db.schema_version().unwrap(), 4);
        db.migrate().unwrap();
        db.migrate().unwrap();
        assert_eq!(db.schema_version().unwrap(), 4);
    }

    #[test]
    fn snapshot_replaces_instead_of_accumulating() {
        // 这就是防 v3.0 容量事故的回归测试。
        let db = Db::in_memory().unwrap();
        let books = sample_books();
        assert!(books.len() >= 2);

        db.write_snapshot(&books, Some("Wed, 23 Sep 2026 12:07:42 GMT"))
            .unwrap();
        let first = db.counts().unwrap().rows;
        for _ in 0..5 {
            db.write_snapshot(&books, Some("Wed, 23 Sep 2026 12:13:42 GMT"))
                .unwrap();
        }
        assert_eq!(db.counts().unwrap().rows, first, "跑 6 轮后行数必须不变");
        assert_eq!(
            db.get_meta("last_snapshot_lm").unwrap().as_deref(),
            Some("Wed, 23 Sep 2026 12:13:42 GMT")
        );
    }

    #[test]
    fn counts_split_npc_and_player_structures() {
        let db = Db::in_memory().unwrap();
        db.write_snapshot(&sample_books(), Some("lm")).unwrap();
        let c = db.counts().unwrap();
        assert_eq!(c.stations, 2);
        assert_eq!(c.types, 1);
        assert_eq!(c.npc_rows, 1, "只有吉他这一条是 NPC 站");
        assert_eq!(c.both_sides, 1, "只有吉他的双向盘完整");
    }

    #[test]
    fn depth_survives_roundtrip_as_json() {
        let db = Db::in_memory().unwrap();
        db.write_snapshot(&sample_books(), Some("lm")).unwrap();
        let json: String = db
            .conn()
            .query_row(
                "SELECT ask_depth FROM station_orders WHERE location_id = 60003760",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let levels: Vec<crate::market::PriceLevel> = serde_json::from_str(&json).unwrap();
        assert_eq!(levels[0].price, 4.0, "卖盘必须从低价开始");
        assert_eq!(levels.len(), 3);
    }

    #[test]
    fn prices_upsert_without_duplicates() {
        let db = Db::in_memory().unwrap();
        let rows = vec![PriceRow {
            date: "2026-09-23".into(),
            type_id: 34,
            adjusted: Some(3.1),
            average: Some(4.0),
        }];
        assert_eq!(db.write_prices(&rows).unwrap(), 1);
        db.write_prices(&[PriceRow {
            date: "2026-09-23".into(),
            type_id: 34,
            adjusted: Some(3.5),
            average: None,
        }])
        .unwrap();
        let n: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM prices_daily", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
        let a: f64 = db
            .conn()
            .query_row(
                "SELECT adjusted FROM prices_daily WHERE type_id=34",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(a, 3.5);
    }

    #[test]
    fn fail_streak_grows_for_crash_recovery() {
        let db = Db::in_memory().unwrap();
        assert_eq!(db.note_sync_fail("/v3/markets/10000002/orders").unwrap(), 1);
        assert_eq!(db.note_sync_fail("/v3/markets/10000002/orders").unwrap(), 2);
        db.record_sync_ok(&SyncState {
            url_key: "/v3/markets/10000002/orders".into(),
            endpoint: "orders".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(db.note_sync_fail("/v3/markets/10000002/orders").unwrap(), 1);
    }

    #[test]
    fn health_rows_prune_to_newest() {
        let db = Db::in_memory().unwrap();
        for i in 0..20 {
            let wm = Watermark {
                remaining: Some(12_000 - i),
                error_remain: Some(100),
                ..Default::default()
            };
            db.record_health("orders", &wm).unwrap();
        }
        let n: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM esi_health", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 20);
        // ts 全同（同一秒）时 prune 只保证不炸；真正断言留给集成测试。
        assert!(db.prune_health(5).is_ok());
    }

    #[test]
    fn hub_pool_and_station_dict_roundtrip() {
        let db = Db::in_memory().unwrap();
        let hubs = crate::market::hub_pool(
            &{
                let mut v = Vec::new();
                for i in 0..60u32 {
                    v.push(order(4.0, false, i as u64, 60003760, 34));
                }
                for i in 0..55u32 {
                    v.push(order(4.0, false, i as u64, 60015157, 35));
                }
                v.push(order(4.0, false, 1, 1044752365771, 36));
                v
            },
            50,
            20,
        );
        assert_eq!(db.write_hub_pool(&hubs).unwrap(), 2);
        let read = db.hub_pool().unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].location_id, 60003760);
        assert_eq!(read[1].rank, 2);

        // 整表替换，不累积 —— 与 station_orders 同一纪律。
        db.write_hub_pool(&hubs).unwrap();
        assert_eq!(db.hub_pool().unwrap().len(), 2);

        // 站点字典：先登记，再解名，未命名清单随之收缩。
        db.remember_station(60003760, true).unwrap();
        db.remember_station(60015157, true).unwrap();
        db.remember_station(1044752365771, false).unwrap();
        assert_eq!(db.unnamed_npc_stations(100).unwrap().len(), 2, "玩家结构不该进解名队列");
        db.name_station(60003760, "Jita IV - Moon 4", Some(30000142)).unwrap();
        assert_eq!(db.station_name(60003760).unwrap().as_deref(), Some("Jita IV - Moon 4"));
        assert_eq!(db.unnamed_npc_stations(100).unwrap(), vec![60015157]);
        assert_eq!(db.station_name(1044752365771).unwrap(), None);
    }

    #[test]
    fn station_book_reads_back_depth_json() {
        let db = Db::in_memory().unwrap();
        db.write_snapshot(&sample_books(), Some("lm")).unwrap();
        let rows = db.station_book(60003760, true).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].type_id, 34);
        assert_eq!(rows[0].best_ask, Some(4.0));
        let levels: Vec<crate::market::PriceLevel> =
            serde_json::from_str(&rows[0].ask_depth).unwrap();
        assert_eq!(levels.len(), 3);
        // 只留双向盘时，玩家结构那条（无买盘）必须被排除。
        assert!(db.station_book(1044752365771, true).unwrap().is_empty());
        assert_eq!(db.station_book(1044752365771, false).unwrap().len(), 1);
        assert_eq!(db.round_count().unwrap(), 0);
    }

    #[test]
    fn tree_listing_and_local_search_roundtrip() {
        let db = Db::in_memory().unwrap();
        let groups = vec![
            crate::tree::GroupDetail {
                group_id: 18,
                category_id: 10,
                name: Some("Noble Metals".into()),
                published: true,
                types: vec![34, 35, 44_707],
            },
            crate::tree::GroupDetail {
                group_id: 10,
                category_id: 2,
                name: Some("Stargate".into()),
                published: false,
                types: vec![16],
            },
        ];
        assert_eq!(db.write_groups(&groups).unwrap(), 2);

        // 实测原样录制：/v1/universe/categories/10 → {"category_id":10,"groups":[94,95],"name":"Trading","published":false}
        let cats: Vec<crate::tree::CategoryDetail> = serde_json::from_str(
            r#"[{"category_id":10,"groups":[94,95],"name":"Trading","published":false},
                {"category_id":20,"groups":[111],"name":"Ship Tools","published":true},
                {"category_id":21,"groups":[],"published":true}]"#,
        )
        .unwrap();
        assert_eq!(db.write_categories(&cats).unwrap(), 2, "无 name 的分类不计数");

        let mut group_of = std::collections::HashMap::new();
        group_of.insert(34u32, 18u32);
        group_of.insert(35u32, 18u32);
        group_of.insert(16u32, 10u32);
        let found = vec![
            (34u64, "Tritanium".to_string()),
            (35u64, "Pyerite".to_string()),
            (16u64, "Stargate".to_string()),
        ];
        assert_eq!(db.write_types(&found, &group_of).unwrap(), 3);

        // 未发布组不进树。
        let tree = db.tree().unwrap();
        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0].category_id, 10);
        assert_eq!(tree[0].name, "Trading", "分类名取自 inv_categories");
        assert_eq!(tree[0].groups.len(), 1);
        assert_eq!(tree[0].groups[0].name, "Noble Metals");
        // 树里的计数必须等于列表真正能摆出来的行数：组 JSON 里那个没解出名字的
        // 44707 不在 inv_types，列表不会有它 —— 拿 types.len() 充数会让
        // "Noble Metals 3" 和"2 个类型"同屏矛盾（走查抓到过）。
        assert_eq!(tree[0].groups[0].type_count, 2);
        assert_eq!(
            tree[0].groups[0].type_count,
            db.group_listing(18, STATION_JITA).unwrap().len(),
            "树上写的数 = 点进去能看到的行数，两处口径不能打架"
        );

        // 列表页要把吉他当前价并上来。
        db.write_snapshot(&sample_books(), Some("lm")).unwrap();
        let listing = db.group_listing(18, STATION_JITA).unwrap();
        assert_eq!(listing.len(), 2);
        let tri = listing.iter().find(|r| r.type_id == 34).unwrap();
        assert_eq!(tri.name, "Tritanium");
        assert_eq!(tri.best_ask, Some(4.0), "sample_books 里 34 号在吉他的卖一");
        let py = listing.iter().find(|r| r.type_id == 35).unwrap();
        assert_eq!(py.best_ask, None, "无行情的类型必须留空而不是 0");
        assert_eq!(py.ask_qty, 0);

        // 本地前缀/词元搜索。
        assert!(db.find_types_like("tri", 10).unwrap().iter().any(|(id, _)| *id == 34));
        assert!(db.find_types_like("PYE", 10).unwrap().iter().any(|(id, _)| *id == 35), "大小写无关");
        assert!(db.find_types_like("zzz", 10).unwrap().is_empty());
        // 词元匹配：搜中间词也要命中。
        assert_eq!(db.type_name(34).unwrap().as_deref(), Some("Tritanium"));
        let (g, t, c) = db.tree_counts().unwrap();
        assert_eq!((g, t, c), (1, 3, 2));
    }

    #[test]
    fn search_shaped_listing_and_detail_book() {
        let db = Db::in_memory().unwrap();
        let mut group_of = std::collections::HashMap::new();
        group_of.insert(34u32, 18u32);
        group_of.insert(35u32, 18u32);
        db.write_types(
            &[
                (34u64, "Tritanium".to_string()),
                (35u64, "Pyerite".to_string()),
            ],
            &group_of,
        )
        .unwrap();
        db.write_snapshot(&sample_books(), Some("lm")).unwrap();

        // 空输入必须短路 —— 否则 VALUES () 是非法 SQL。
        assert!(db.listing_for_types(&[], STATION_JITA).unwrap().is_empty());

        // 命中的带行情，没命中的仍要出现（搜索不能静默吞掉用户查的东西）。
        let rows = db.listing_for_types(&[34, 99_99], STATION_JITA).unwrap();
        assert_eq!(rows.len(), 2);
        let tri = rows.iter().find(|r| r.type_id == 34).unwrap();
        assert_eq!(tri.name, "Tritanium");
        assert_eq!(tri.best_ask, Some(4.0));
        assert_eq!(tri.best_bid, Some(3.9));
        let unknown = rows.iter().find(|r| r.type_id == 99_99).unwrap();
        assert_eq!(unknown.name, "#9999", "树里还没有的类型要显示成 #ID");
        assert_eq!(unknown.best_ask, None);
        assert_eq!(unknown.ask_qty, 0, "LEFT JOIN 未命中不能落成 0 价");

        // 详情面板：阶梯从 JSON 还原成结构，站名走字典联接。
        let book = db.book_row(STATION_JITA, 34).unwrap().expect("有快照就该有盘");
        assert_eq!(book.ask_depth[0].price, 4.0, "卖盘从低价开始");
        assert_eq!(book.bid_depth[0].price, 3.9);
        assert_eq!(book.ask_depth.len(), 3);
        assert_eq!(book.snapshot_lm.as_deref(), Some("lm"));
        db.name_station(60003760, "Jita IV - Moon 4", Some(30000142))
            .unwrap();
        let named = db.book_row(STATION_JITA, 34).unwrap().unwrap();
        assert_eq!(named.location_name, "Jita IV - Moon 4");
        assert!(db.book_row(STATION_JITA, 35).unwrap().is_none());
    }

    #[test]
    fn write_groups_replaces_the_whole_table() {
        let db = Db::in_memory().unwrap();
        let g = crate::tree::GroupDetail {
            group_id: 18,
            category_id: 10,
            name: Some("Noble Metals".into()),
            published: true,
            types: vec![34],
        };
        db.write_groups(&[g.clone()]).unwrap();
        let mut gone = g.clone();
        gone.group_id = 19;
        db.write_groups(&[gone]).unwrap();
        assert_eq!(db.tree().unwrap().iter().map(|t| t.groups.len()).sum::<usize>(), 1);
        assert_eq!(db.tree_counts().unwrap().0, 1, "上游删掉的组不得继续出现在树里");
    }

    #[test]
    fn empty_snapshot_leaves_zero_rows() {
        let db = Db::in_memory().unwrap();
        db.write_snapshot(&sample_books(), Some("lm")).unwrap();
        db.write_snapshot(&[], Some("lm2")).unwrap();
        assert_eq!(db.counts().unwrap().rows, 0);
    }

    #[test]
    fn unix_clock_is_sane() {
        assert!(now_unix() > 1_700_000_000);
        assert!(now_unix() < 4_000_000_000);
        let _ = Duration::seconds(1);
    }
}
