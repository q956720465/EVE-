//! 角色三表（`char_meta` / `char_tx` / `char_orders`）的读写层（M4c）。
//!
//! 独立成文件而不是继续往 `db.rs` 里塞：这三张表的数据形状与市场侧完全不同——
//! 市场单簿每轮整表替换、机会按轮 tick，而角色侧是"一角色一行元数据 +
//! 只增不删的增量流水 + 每轮整表替换的挂单快照"三种形状混在一起。
//!
//! **令牌不在库里的任何一处**：本文件每条 SQL 都只碰上面三张表的列，
//! access/refresh token 只进 keyring 与进程内存（Global Constraint）。
//!
//! 这里只做存储，不做协议：字段名是**列名**，不是 ESI 响应体的字段名。
//! 响应体形状的核对与映射属于同步管线（`char`），那侧自己做核对。
//!
//! `replace_char_orders` 与 `write_xregion_books`（`db.rs` M4b 段）同纪律：
//! 一个 `unchecked_transaction` 里先删后插，读侧看不到"删了还没插"的中间态。

use rusqlite::{params, OptionalExtension};

use crate::error::Result;
use crate::store::db::Db;

/// 一角色一行的挂链元数据（`char_meta`）。游标与 Last-Modified 跟着行走，
/// 让"重启后从哪继续"是库里的一个事实，而不是进程内存里的一厢情愿。
#[derive(Debug, Clone, PartialEq)]
pub struct CharMeta {
    pub char_id: u64,
    pub name: Option<String>,
    /// `wallet/transactions` 的 `since` 增量水位（ISO8601 文本）。
    pub tx_cursor: Option<String>,
    /// `wallet/journal` 的 `since` 增量水位。
    pub journal_cursor: Option<String>,
    /// 上轮 orders 的 Last-Modified，条件请求的同源凭据。
    pub orders_lm: Option<String>,
    /// 首次落行的时刻（历史事实，不随每轮同步改写）。
    pub first_sync_at: Option<i64>,
    pub last_sync_at: Option<i64>,
}

/// 钱包流水一行（`char_tx`）。FIFO 成本基准的原料，主键是
/// `(char_id, transaction_id)` —— 增量拉取的 `since` 窗口会重叠，去重靠主键。
#[derive(Debug, Clone, PartialEq)]
pub struct WalletTx {
    pub transaction_id: i64,
    /// ESI 的 ISO8601 原样文本。存文本而不是时间戳：`prune_char_tx` 按日期裁剪时
    /// 字典序即时间序（同格式下），不必解析、也不必引入时区判断。
    pub date: String,
    pub type_id: u32,
    pub location_id: u64,
    /// 同一笔流水的方向：买单入 FIFO 队列，卖单从队首消耗。
    pub is_buy: bool,
    pub unit_price: f64,
    pub quantity: u64,
}

/// 上轮挂单快照的一行（`char_orders`）。整表替换的输入/输出，
/// `fetched_at` 由 `replace_char_orders` 按本轮时刻统一落，调用方不必逐行填。
#[derive(Debug, Clone, PartialEq)]
pub struct CharOrder {
    pub order_id: i64,
    pub type_id: u32,
    pub location_id: u64,
    pub is_buy: bool,
    pub price: f64,
    pub volume_remain: u64,
    /// 挂单时刻（ESI 原样文本）与时长，状态边沿判定要用。
    pub issued: String,
    pub duration: i64,
    pub fetched_at: i64,
}

impl Db {
    /// 挂链元数据的 UPSERT。**只碰 name 与两个时间戳**：游标 / Last-Modified 的推进
    /// 属于增量拉取自己的逻辑，混进来会变成"每轮同步顺手把水位抹回 NULL"。
    /// `first_sync_at` 只在行首次落库时写 —— 它是"何时开始挂链"的历史事实，
    /// 跟着每轮改写就失去意义；`last_sync_at` 才是每轮刷新。
    pub fn upsert_char_meta(&self, char_id: u64, name: &str, now: i64) -> Result<()> {
        self.conn().execute(
            "INSERT INTO char_meta (char_id, name, first_sync_at, last_sync_at)
             VALUES (?1, ?2, ?3, ?3)
             ON CONFLICT(char_id) DO UPDATE SET
               name = excluded.name,
               last_sync_at = excluded.last_sync_at,
               first_sync_at = COALESCE(char_meta.first_sync_at, excluded.first_sync_at)",
            params![char_id as i64, name, now],
        )?;
        Ok(())
    }

    /// 没挂链的角色返回 None（而不是空行）：调用方要能区分"从没同步过"与"同步过但没数据"。
    pub fn char_meta(&self, char_id: u64) -> Result<Option<CharMeta>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT char_id, name, tx_cursor, journal_cursor, orders_lm,
                        first_sync_at, last_sync_at
                 FROM char_meta WHERE char_id = ?1",
                params![char_id as i64],
                |r| {
                    Ok(CharMeta {
                        char_id: r.get::<_, i64>(0)? as u64,
                        name: r.get(1)?,
                        tx_cursor: r.get(2)?,
                        journal_cursor: r.get(3)?,
                        orders_lm: r.get(4)?,
                        first_sync_at: r.get(5)?,
                        last_sync_at: r.get(6)?,
                    })
                },
            )
            .optional()?)
    }

    /// 增量水位的**唯一写路径**（`upsert_char_meta` 刻意不碰这三列）。
    ///
    /// **`None` = "这一列不动"，不是"清空"**：某一轮里某个端点没拉到、或响应没带
    /// `Last-Modified` 时传 `None`，库里那份水位必须原样留着。反过来实现（`None` → 置 NULL）
    /// 就是每轮把水位抹回起点：`since` 增量当场退化成"从 90 天前全量重拉"。
    ///
    /// 用 UPSERT 而不是 UPDATE：行还不存在时（同步先于登录落行）静默影响 0 行会让水位
    /// 永远推不动，而"推不动"与"没数据"在库里看不出区别。`COALESCE` 让 `None` 与
    /// "不动这一列"成为同一件事。
    pub fn set_char_cursors(
        &self,
        char_id: u64,
        tx_cursor: Option<&str>,
        journal_cursor: Option<&str>,
        orders_lm: Option<&str>,
    ) -> Result<()> {
        self.conn().execute(
            "INSERT INTO char_meta (char_id, tx_cursor, journal_cursor, orders_lm)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(char_id) DO UPDATE SET
               tx_cursor = COALESCE(excluded.tx_cursor, char_meta.tx_cursor),
               journal_cursor = COALESCE(excluded.journal_cursor, char_meta.journal_cursor),
               orders_lm = COALESCE(excluded.orders_lm, char_meta.orders_lm)",
            params![char_id as i64, tx_cursor, journal_cursor, orders_lm],
        )?;
        Ok(())
    }

    /// 钱包流水落库。**幂等是硬要求**：`since` 增量窗口会重叠，同一笔
    /// transaction_id 每轮都可能再回来 —— 冲突就地更新，既不攒行也不报错。
    /// 整批一个事务：半批落库会让 FIFO 下次重放读到不完整的历史。
    /// 返回受理行数（不是"新插入"行数：重放的同一笔也计入，这是调用方的记账口径）。
    pub fn upsert_char_tx(&self, char_id: u64, txs: &[WalletTx]) -> Result<usize> {
        let tx = self.conn().unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO char_tx (char_id, transaction_id, date, type_id, location_id,
                    is_buy, unit_price, quantity)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
                 ON CONFLICT(char_id, transaction_id) DO UPDATE SET
                   date=excluded.date, type_id=excluded.type_id, location_id=excluded.location_id,
                   is_buy=excluded.is_buy, unit_price=excluded.unit_price,
                   quantity=excluded.quantity",
            )?;
            for t in txs {
                stmt.execute(params![
                    char_id as i64,
                    t.transaction_id,
                    t.date,
                    t.type_id,
                    t.location_id as i64,
                    t.is_buy as i64,
                    t.unit_price,
                    t.quantity as i64,
                ])?;
            }
        }
        tx.commit()?;
        Ok(txs.len())
    }

    /// `since_date` 是**闭区间下界**（含当天）：调用方传的是上轮水位，用严格大于
    /// 会永久漏掉"恰好在水位那天"的流水，而增量水位恰恰就是停在那天。
    /// 按 `date` 升序、同日按 transaction_id 兜底排序：FIFO 重放要求时序确定，
    /// 否则同一天两笔流水的先后会随查询计划漂移，成本基准跟着抖。
    pub fn load_char_tx(&self, char_id: u64, since_date: Option<&str>) -> Result<Vec<WalletTx>> {
        let mut stmt = self.conn().prepare(
            "SELECT transaction_id, date, type_id, location_id, is_buy, unit_price, quantity
             FROM char_tx
             WHERE char_id = ?1 AND (?2 IS NULL OR date >= ?2)
             ORDER BY date, transaction_id",
        )?;
        let rows = stmt.query_map(params![char_id as i64, since_date], |r| {
            Ok(WalletTx {
                transaction_id: r.get(0)?,
                date: r.get(1)?,
                type_id: r.get::<_, i64>(2)? as u32,
                location_id: r.get::<_, i64>(3)? as u64,
                is_buy: r.get::<_, i64>(4)? != 0,
                unit_price: r.get(5)?,
                quantity: r.get::<_, i64>(6)? as u64,
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// 该角色挂单的**整表替换**：一个事务里先删后插（照 `write_xregion_books` 的先例）。
    /// 撤单/成交的挂单在下一轮响应里就不见了，残留的旧行会让状态边沿判定继续把
    /// "已经不存在的挂单"当成在挂 —— 幽灵行必须清掉，而不是等它自己过期。
    /// 删除范围只限该角色（多角色下不能互相清）。
    /// `fetched_at` 取 `now`：一屏挂单本来就是同一瞬间的快照，不必逐行传。
    pub fn replace_char_orders(&self, char_id: u64, orders: &[CharOrder], now: i64) -> Result<usize> {
        let tx = self.conn().unchecked_transaction()?;
        tx.execute(
            "DELETE FROM char_orders WHERE char_id = ?1",
            params![char_id as i64],
        )?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO char_orders (char_id, order_id, type_id, location_id, is_buy,
                    price, volume_remain, issued, duration, fetched_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            )?;
            for o in orders {
                stmt.execute(params![
                    char_id as i64,
                    o.order_id,
                    o.type_id,
                    o.location_id as i64,
                    o.is_buy as i64,
                    o.price,
                    o.volume_remain as i64,
                    o.issued,
                    o.duration,
                    now,
                ])?;
            }
        }
        tx.commit()?;
        Ok(orders.len())
    }

    /// 该角色上一次同步那一刻的挂单快照。
    pub fn load_char_orders(&self, char_id: u64) -> Result<Vec<CharOrder>> {
        let mut stmt = self.conn().prepare(
            "SELECT order_id, type_id, location_id, is_buy, price, volume_remain,
                    issued, duration, fetched_at
             FROM char_orders WHERE char_id = ?1 ORDER BY order_id",
        )?;
        let rows = stmt.query_map(params![char_id as i64], |r| {
            Ok(CharOrder {
                order_id: r.get(0)?,
                type_id: r.get::<_, i64>(1)? as u32,
                location_id: r.get::<_, i64>(2)? as u64,
                is_buy: r.get::<_, i64>(3)? != 0,
                price: r.get(4)?,
                volume_remain: r.get::<_, i64>(5)? as u64,
                issued: r.get(6)?,
                duration: r.get(7)?,
                fetched_at: r.get(8)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// 回填窗的收口：剪掉窗外的老流水（首启只回填 `backfill_days` 天，此后不再需要它们）。
    /// 不带 char_id：窗口是时间维度的事实，与角色无关。
    /// 文本字典序比较即可 —— 同格式的 ISO8601 字典序即时间序，不必解析成时间戳。
    pub fn prune_char_tx(&self, before_date: &str) -> Result<usize> {
        Ok(self
            .conn()
            .execute("DELETE FROM char_tx WHERE date < ?1", params![before_date])?)
    }
}

#[cfg(test)]
mod char_persist_tests {
    use super::*;

    const CHAR: u64 = 90_000_001;

    fn tx(id: i64, date: &str, is_buy: bool, quantity: u64, unit_price: f64) -> WalletTx {
        WalletTx {
            transaction_id: id,
            date: date.to_string(),
            type_id: 34,
            location_id: 60_003_760,
            is_buy,
            unit_price,
            quantity,
        }
    }

    fn ord(id: i64, type_id: u32, price: f64, volume_remain: u64) -> CharOrder {
        CharOrder {
            order_id: id,
            type_id,
            location_id: 60_003_760,
            is_buy: false,
            price,
            volume_remain,
            issued: "2026-09-20T10:00:00Z".to_string(),
            duration: 90,
            fetched_at: 0, // 写入时被 now 覆盖，调用方不必填
        }
    }

    fn tx_ids(db: &Db, since: Option<&str>) -> Vec<i64> {
        db.load_char_tx(CHAR, since)
            .unwrap()
            .iter()
            .map(|t| t.transaction_id)
            .collect()
    }

    #[test]
    fn char_tx_upsert_is_idempotent_and_orders_replace_is_whole_table() {
        let db = Db::in_memory().unwrap();

        // ① 同一 transaction_id 重放两次 → 仍 1 行（增量拉取会重叠，幂等是硬要求）
        let first = tx(900, "2026-09-15T10:00:00Z", true, 5, 10.0);
        assert_eq!(db.upsert_char_tx(CHAR, &[first.clone()]).unwrap(), 1);
        // 重叠窗口里的同一笔可能带着修订值回来：冲突即就地更新，而不是攒行。
        let revised = WalletTx {
            quantity: 7,
            unit_price: 10.5,
            ..first.clone()
        };
        assert_eq!(db.upsert_char_tx(CHAR, &[revised.clone()]).unwrap(), 1);
        let back = db.load_char_tx(CHAR, None).unwrap();
        assert_eq!(back.len(), 1, "同一 transaction_id 重放不得攒行：{back:?}");
        assert_eq!(back[0], revised, "主键冲突时按新到达的值就地更新");

        // ② replace_char_orders 整表替换 → 旧订单消失（挂单会撤，不能留幽灵）
        db.replace_char_orders(CHAR, &[ord(101, 34, 1_000.0, 10), ord(102, 35, 2_000.0, 20)], 1_700_000_000)
            .unwrap();
        assert_eq!(db.load_char_orders(CHAR).unwrap().len(), 2);
        // 下一轮：101 撤单，102 改价（挂单量随成交减少）
        db.replace_char_orders(CHAR, &[ord(102, 35, 2_500.0, 15)], 1_700_000_600)
            .unwrap();
        let kept = db.load_char_orders(CHAR).unwrap();
        assert_eq!(kept.len(), 1, "整表替换后撤掉的单不得残留：{kept:?}");
        assert_eq!(kept[0].order_id, 102, "留下的单必须还在");
        assert_eq!(kept[0].price, 2_500.0, "留下的单按新快照更新");
        assert_eq!(kept[0].volume_remain, 15);
        assert_eq!(kept[0].fetched_at, 1_700_000_600, "fetched_at 是本轮抓取时刻");
        // 空快照 = 该角色当前没有挂单，旧行同样必须清空（不是"没拉到就跳过"）
        db.replace_char_orders(CHAR, &[], 1_700_000_700).unwrap();
        assert!(db.load_char_orders(CHAR).unwrap().is_empty());

        // ③ prune_char_tx 按日期裁剪，90 天前的行消失
        let rows = vec![
            tx(11, "2026-06-01T00:00:00Z", true, 1, 1.0), // 窗外的老流水
            tx(12, "2026-07-01T00:00:00Z", true, 1, 1.0), // 正好是 cutoff 当天
            tx(13, "2026-09-20T00:00:00Z", true, 1, 1.0), // 窗内
        ];
        db.upsert_char_tx(CHAR, &rows).unwrap();
        assert_eq!(tx_ids(&db, None), vec![11, 12, 900, 13], "裁剪前四行（按日期升序）");
        assert_eq!(db.prune_char_tx("2026-07-01").unwrap(), 1, "只剪严格早于 cutoff 的行");
        assert_eq!(tx_ids(&db, None), vec![12, 900, 13], "cutoff 当天算窗内，必须留下");
    }

    #[test]
    fn set_char_cursors_treats_none_as_leave_this_column_alone() {
        let db = Db::in_memory().unwrap();

        // ① 还没挂链的角色也要能写：水位是同步管线自己的写路径，
        //    若它在"行还不存在"时静默影响 0 行，增量水位就永远推不动、每轮退化成全量。
        db.set_char_cursors(CHAR, Some("2026-09-19T00:00:00Z"), None, None)
            .unwrap();
        let m = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(m.tx_cursor.as_deref(), Some("2026-09-19T00:00:00Z"));
        assert!(m.journal_cursor.is_none() && m.orders_lm.is_none());

        // ② None = **不动这一列**，不是"清空"：某一轮没拉到 journal 时，
        //    绝不能把上一轮的水位抹掉 —— 那等于下一轮从 90 天前重拉。
        db.set_char_cursors(CHAR, None, Some("2026-09-18T00:00:00Z"), None)
            .unwrap();
        let m = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(
            m.tx_cursor.as_deref(),
            Some("2026-09-19T00:00:00Z"),
            "None 不得清掉已有水位"
        );
        assert_eq!(m.journal_cursor.as_deref(), Some("2026-09-18T00:00:00Z"));

        // ③ 三列全 None：什么都不动（也不是"全清"）。
        db.set_char_cursors(CHAR, None, None, None).unwrap();
        let m = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(m.tx_cursor.as_deref(), Some("2026-09-19T00:00:00Z"));
        assert_eq!(m.journal_cursor.as_deref(), Some("2026-09-18T00:00:00Z"));

        // ④ `orders_lm` 是 HTTP 日期原文（含逗号与空格），逐字符存。
        let lm = "Wed, 17 Sep 2026 00:00:00 GMT";
        db.set_char_cursors(CHAR, None, None, Some(lm)).unwrap();
        assert_eq!(db.char_meta(CHAR).unwrap().unwrap().orders_lm.as_deref(), Some(lm));

        // ⑤ 与元数据 UPSERT 合起来用（同步管线的真实顺序）：UPSERT 只碰自己那几列，
        //    水位由本条通路独占推进 —— 两条写路径互不擦手。
        db.upsert_char_meta(CHAR, "Pilot One", 1_700_000_000).unwrap();
        let m = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(m.name.as_deref(), Some("Pilot One"));
        assert_eq!(m.last_sync_at, Some(1_700_000_000));
        assert_eq!(m.tx_cursor.as_deref(), Some("2026-09-19T00:00:00Z"));
        assert_eq!(m.orders_lm.as_deref(), Some(lm));
    }

    #[test]
    fn char_meta_upsert_keeps_first_sync_and_does_not_clobber_cursors() {
        let db = Db::in_memory().unwrap();
        assert!(db.char_meta(CHAR).unwrap().is_none(), "未挂链的角色没有行，不是空行");

        db.upsert_char_meta(CHAR, "Pilot One", 1_700_000_000).unwrap();
        let m = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(m.char_id, CHAR);
        assert_eq!(m.name.as_deref(), Some("Pilot One"));
        assert_eq!(m.first_sync_at, Some(1_700_000_000));
        assert_eq!(m.last_sync_at, Some(1_700_000_000));
        assert!(m.tx_cursor.is_none() && m.journal_cursor.is_none() && m.orders_lm.is_none());

        // 游标列由增量水位推进独占写。本任务的接口没有它的写入通路，于是直接用 SQL
        // 种一行，钉住"元数据 UPSERT 只碰自己那几列"——否则将来补上水位推进后，
        // 每轮同步都会把游标抹回 NULL、从 90 天前重拉。
        db.conn()
            .execute(
                "UPDATE char_meta SET tx_cursor=?2, journal_cursor=?3, orders_lm=?4 WHERE char_id=?1",
                params![
                    CHAR as i64,
                    "2026-09-19T00:00:00Z",
                    "2026-09-18T00:00:00Z",
                    "Wed, 17 Sep 2026 00:00:00 GMT"
                ],
            )
            .unwrap();

        // 改名 + 再同步一轮：name/last_sync_at 刷新，first_sync_at 与游标一律不动。
        db.upsert_char_meta(CHAR, "Pilot Renamed", 1_700_000_600).unwrap();
        let m = db.char_meta(CHAR).unwrap().unwrap();
        assert_eq!(m.name.as_deref(), Some("Pilot Renamed"));
        assert_eq!(m.first_sync_at, Some(1_700_000_000), "first_sync_at 是首次挂链的历史事实");
        assert_eq!(m.last_sync_at, Some(1_700_000_600));
        assert_eq!(m.tx_cursor.as_deref(), Some("2026-09-19T00:00:00Z"), "游标不得被改写");
        assert_eq!(m.journal_cursor.as_deref(), Some("2026-09-18T00:00:00Z"));
        assert_eq!(m.orders_lm.as_deref(), Some("Wed, 17 Sep 2026 00:00:00 GMT"));

        // load_char_tx 的 since_date 是闭区间下界：水位当天那批不能被漏掉，
        // 否则增量拉取每轮都会丢"发生在水位当天"的流水。
        db.upsert_char_tx(
            CHAR,
            &[tx(21, "2026-09-01T00:00:00Z", true, 1, 1.0), tx(22, "2026-09-19T05:00:00Z", false, 1, 1.0)],
        )
        .unwrap();
        // 另一个角色的流水不串号
        db.upsert_char_tx(CHAR + 1, &[tx(23, "2026-09-19T06:00:00Z", true, 1, 1.0)])
            .unwrap();
        assert_eq!(tx_ids(&db, Some("2026-09-19")), vec![22], "since 是闭区间下界，且按角色隔离");
        assert_eq!(tx_ids(&db, None).len(), 2, "不带 since 读全量，仍按角色隔离");
    }
}
