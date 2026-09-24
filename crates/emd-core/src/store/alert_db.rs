//! 告警状态机（`alerts` 表）的读写层（M4c）。
//!
//! 从 Task 6 移来：这三个方法读写 `AlertRecord`，而它要到 Task 9 才定义 —— 放在状态机旁边
//! （而不是塞进 `alert/state.rs`）才能既编译又守住"状态机零 IO"：DB 代码一律留在 `store/`，
//! `state.rs` 只有纯逻辑。语义上也不该跟 `char_db.rs` 混：那三张表是角色侧的数据形状。
//!
//! **令牌不在库里的任何一处**（Global Constraint）：本文件每条 SQL 都只碰 `alerts` 一张表。
//!
//! **`alert_key` 是 TEXT**：写入值一律来自 `AlertRecord`（即 T8 的
//! `order_alert_key`/`tx_alert_key` 产物）；本层的读与剪入口**都不吃挂单号/成交号** ——
//! 没有"拿整数去比 TEXT 列"的地方，P3 的静默零行在这层写不出来。

use rusqlite::params;

use crate::alert::{AlertKind, AlertRecord, AlertState};
use crate::error::Result;
use crate::store::db::Db;

impl Db {
    /// 告警状态行的 UPSERT（冲突键 `alert_key`）。
    ///
    /// 就地更新而不是追加：一个 key 只留一行，日限额计数与当前状态才读得出来。时间戳一旦进主键，
    /// 同一根挂单每天会多攒一行"亏损历史"，"今天推了几条"的计数被打散成多行（v6 schema 的
    /// `bounded_tables` 测试把这条钉死）。
    pub fn save_alert(&self, rec: &AlertRecord) -> Result<()> {
        self.conn().execute(
            "INSERT INTO alerts (alert_key, kind, char_id, type_id, location_id, is_buy,
                first_seen_at, last_seen_at, last_loss_isk, last_margin_pct,
                notified_at, notified_day, notified_count_day, last_notified_loss, state, payload)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)
             ON CONFLICT(alert_key) DO UPDATE SET
               kind=excluded.kind, char_id=excluded.char_id, type_id=excluded.type_id,
               location_id=excluded.location_id, is_buy=excluded.is_buy,
               first_seen_at=excluded.first_seen_at, last_seen_at=excluded.last_seen_at,
               last_loss_isk=excluded.last_loss_isk, last_margin_pct=excluded.last_margin_pct,
               notified_at=excluded.notified_at, notified_day=excluded.notified_day,
               notified_count_day=excluded.notified_count_day,
               last_notified_loss=excluded.last_notified_loss,
               state=excluded.state, payload=excluded.payload",
            params![
                rec.alert_key,
                rec.kind.as_str(),
                rec.char_id as i64,
                rec.type_id,
                rec.location_id as i64,
                rec.is_buy as i64,
                rec.first_seen_at,
                rec.last_seen_at,
                rec.last_loss_isk,
                rec.last_margin_pct,
                rec.notified_at,
                rec.notified_day,
                rec.notified_count_day,
                rec.last_notified_loss,
                rec.state.as_str(),
                rec.payload,
            ],
        )?;
        Ok(())
    }

    /// 全表读回（提醒中心与告警回合的输入）。
    ///
    /// 未知 state/kind 的行**跳过并 warn** —— 库被外部改坏时宁可少几行，也不能让整表读失败
    /// 把告警回合打崩（同 `load_opps` 纪律）。排序：最后见到在前（提醒中心要"最近出现的先看到"），
    /// 末尾按 key 兜底，保证同刻多行的顺序不随查询计划漂移。
    pub fn load_alerts(&self) -> Result<Vec<AlertRecord>> {
        let mut stmt = self.conn().prepare(
            "SELECT alert_key, kind, char_id, type_id, location_id, is_buy,
                    first_seen_at, last_seen_at, last_loss_isk, last_margin_pct,
                    notified_at, notified_day, notified_count_day, last_notified_loss, state, payload
             FROM alerts ORDER BY last_seen_at DESC, alert_key",
        )?;
        let rows = stmt.query_map([], |r| {
            let kind_raw: String = r.get(1)?;
            let state_raw: String = r.get(14)?;
            Ok((
                kind_raw.clone(),
                state_raw.clone(),
                AlertRecord {
                    alert_key: r.get(0)?,
                    // 认不出的串先用占位值读完整行，下一轮再按原始串判是否跳过
                    //（行内就 `?` 会让整表读失败，见函数头）。
                    kind: AlertKind::parse(&kind_raw).unwrap_or(AlertKind::ExpectedSellLoss),
                    char_id: r.get::<_, i64>(2)? as u64,
                    type_id: r.get::<_, i64>(3)? as u32,
                    location_id: r.get::<_, i64>(4)? as u64,
                    is_buy: r.get::<_, i64>(5)? != 0,
                    first_seen_at: r.get(6)?,
                    last_seen_at: r.get(7)?,
                    last_loss_isk: r.get(8)?,
                    last_margin_pct: r.get(9)?,
                    notified_at: r.get(10)?,
                    notified_day: r.get(11)?,
                    notified_count_day: r.get::<_, i64>(12)? as u32,
                    last_notified_loss: r.get(13)?,
                    state: AlertState::parse(&state_raw).unwrap_or(AlertState::New),
                    payload: r.get(15)?,
                },
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (kind_raw, state_raw, rec) = row?;
            if AlertKind::parse(&kind_raw).is_none() || AlertState::parse(&state_raw).is_none() {
                tracing::warn!(
                    alert_key = %rec.alert_key, kind = %kind_raw, state = %state_raw,
                    "未知告警形态/状态，跳过该行"
                );
                continue;
            }
            out.push(rec);
        }
        Ok(out)
    }

    /// 只剪"已清（`cleared`）且 `last_seen_at` 早于 `before_ts`"的行；保留期由调用方定。
    /// 活跃行（`new`/`notified`）永远留在表里，即使它很老 —— 那是"还在亏损"的事实
    /// （同 `prune_opps_terminal` 纪律）。状态串走 [`AlertState::as_str`] 单一映射，不在 SQL 里写字面量。
    pub fn prune_alerts_cleared(&self, before_ts: i64) -> Result<usize> {
        Ok(self.conn().execute(
            "DELETE FROM alerts WHERE state = ?1 AND last_seen_at < ?2",
            params![AlertState::Cleared.as_str(), before_ts],
        )?)
    }
}

#[cfg(test)]
mod alert_persist_tests {
    use super::*;
    use crate::alert::{
        mark_pushed, order_alert_key, tx_alert_key, AlertPayload, CaliberSummary, COST_SRC_FIFO90,
        TRACK_EXPECTED,
    };
    use chrono::{DateTime, Utc};

    const CHAR: u64 = 90_000_001;
    const T0: i64 = 1_790_000_000;
    const DAY: &str = "2026-09-21";

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// 一张挂单轨的载荷（字段是 T8 契约的副本；本层只关心它能不能原样落库再原样读回）。
    /// 亏损率用 17 位长小数：serde_json 的浮点回读正是这类值会差 1 ulp。
    fn payload(key: String, margin_pct: f64) -> AlertPayload {
        AlertPayload {
            alert_key: key,
            kind: AlertKind::ExpectedSellLoss,
            order_id: 7001,
            type_id: 34,
            type_name: "Tritanium".to_string(),
            location_id: 60_003_760,
            location_name: "Jita IV - Moon 4".to_string(),
            is_buy: false,
            price: 97.0,
            volume: 100,
            at: at("2026-09-21T10:00:00Z"),
            loss_isk: 127.375,
            margin_pct,
            caliber: CaliberSummary {
                track: TRACK_EXPECTED.to_string(),
                sales_tax_pct: 3.375,
                broker_pct: 0.0,
                skill_caliber: "Accounting 5 / Broker Relations 0".to_string(),
                unit_cost: 95.0,
                cost_source: COST_SRC_FIFO90.to_string(),
                formula: "① 单位净额 93.72625 = 挂价 97 × (1 − 有效税 3.375%)".to_string(),
                data_age_secs: 60,
            },
        }
    }

    fn rec(key: String, margin_pct: f64) -> AlertRecord {
        AlertRecord::from_payload(&payload(key, margin_pct), CHAR, T0)
    }

    #[test]
    fn alert_roundtrip_keeps_payload_json_verbatim() {
        // payload 列是 TEXT：**存进去的那串文本原样读回** —— 这就是"推送与提醒中心共用同一序列化"
        // 的机械保证（spec §4.4 的"杜绝双源漂移"）。
        // 比的是文本而不是 f64 位：serde_json 默认的浮点解析是"尽力而为"（`float_roundtrip` 未开），
        // 长小数回读可能差 1 ulp（T8 实测过）—— payload 是显示/契约载体，不是逐位账本。
        let db = Db::in_memory().unwrap();
        let p = payload(order_alert_key(7001), -1.340_789_473_684_210_5);
        let written = serde_json::to_string(&p).unwrap();
        let rec = AlertRecord::from_payload(&p, CHAR, T0);
        assert_eq!(rec.payload, written, "构造处写下的就是 to_string 的产物（单一序列化源）");
        db.save_alert(&rec).unwrap();

        let back = db.load_alerts().unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].payload, written, "payload 列逐字节原样（不是'读出来再序列化一遍'）");
        assert!(back[0].payload.contains("\"alert_key\":\"order:7001\""), "{}", back[0].payload);
        assert!(back[0].payload.contains("\"kind\":\"expected_sell_loss\""), "{}", back[0].payload);
        assert!(back[0].payload.contains("2026-09-21T10:00:00"), "at 是 RFC3339 文本：{}", back[0].payload);

        // 读回来还能解回同一张卡：非浮点字段逐字相等，浮点按容差（1 ulp 级）。
        let parsed: AlertPayload = serde_json::from_str(&back[0].payload).unwrap();
        assert_eq!(parsed.alert_key, p.alert_key);
        assert_eq!(parsed.kind, p.kind);
        assert_eq!(parsed.type_name, p.type_name);
        assert_eq!(parsed.at, p.at);
        assert_eq!(parsed.volume, p.volume);
        assert_eq!(parsed.caliber.formula, p.caliber.formula, "公式串是文本，逐字还原");
        assert!((parsed.margin_pct - p.margin_pct).abs() < 1e-9);
        assert!((parsed.loss_isk - p.loss_isk).abs() < 1e-9);
    }

    #[test]
    fn alert_rows_are_keyed_by_text_form_and_upsert_in_place() {
        // P3：`alerts.alert_key` 是 TEXT，而 `order_id`/`transaction_id` 是 INTEGER。
        // 写入值取 T8 的构造器产物；本层的读/剪入口都不吃 id，所以没有"拿整数比 TEXT"的地方。
        let db = Db::in_memory().unwrap();
        let order = rec(order_alert_key(101), -3.0);
        let tx = AlertRecord::from_payload(
            &AlertPayload {
                alert_key: tx_alert_key(101),
                kind: AlertKind::RealizedLoss,
                ..payload(String::new(), -5.0)
            },
            CHAR,
            T0,
        );
        assert_eq!(order.alert_key, "order:101");
        assert_eq!(tx.alert_key, "tx:101");
        db.save_alert(&order).unwrap();
        db.save_alert(&tx).unwrap();
        assert_eq!(
            db.load_alerts().unwrap().len(),
            2,
            "同号的挂单与成交是两行（前缀挡住撞键）"
        );

        // 同一 key 再存 = 就地更新（状态机每个 key 只留一行）：日限计数跟着新值走。
        let mut revised = order.clone();
        mark_pushed(&mut revised, T0, DAY);
        revised.notified_count_day = 3;
        revised.last_seen_at = T0 + 600;
        db.save_alert(&revised).unwrap();
        let loaded = db.load_alerts().unwrap();
        assert_eq!(loaded.len(), 2, "UPSERT 不攒行：{loaded:?}");
        let one = loaded
            .iter()
            .find(|r| r.alert_key == order_alert_key(101))
            .unwrap();
        assert_eq!(one, &revised, "按新值就地更新（含日限计数）");

        // 现场证一下 P3 的前提：拿 INTEGER 去比 TEXT 列，SQLite 不报错、静默零行。
        let n: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM alerts WHERE alert_key = 101", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "裸 id 当 key 查会静默零行 —— 本层的入口不吃 id，这条坑踩不到");
    }

    #[test]
    fn load_skips_unknown_state_or_kind_and_prune_takes_only_cleared_old_ones() {
        let db = Db::in_memory().unwrap();
        let new = rec(order_alert_key(7001), -3.0);
        let mut notified = rec(order_alert_key(7002), -3.0);
        mark_pushed(&mut notified, T0, DAY);
        let mut cleared = rec(order_alert_key(7003), -3.0);
        cleared.state = AlertState::Cleared;
        for r in [&new, &notified, &cleared] {
            db.save_alert(r).unwrap();
        }

        // 库被外部改坏两行（认不出的 state / kind）→ 跳过并 warn，其余照读：
        // 让整表读失败等于把告警回合打崩（同 load_opps 纪律）。
        db.conn()
            .execute(
                "INSERT INTO alerts (alert_key, kind, char_id, type_id, location_id, is_buy,
                    first_seen_at, last_seen_at, state, payload)
                 VALUES ('order:7999','expected_sell_loss',?1,34,60003760,0,0,0,'bogus','{}'),
                        ('order:7998','nope',?1,34,60003760,0,0,0,'new','{}')",
                params![CHAR as i64],
            )
            .unwrap();
        let loaded = db.load_alerts().unwrap();
        assert_eq!(loaded.len(), 3, "坏行跳过，正常行照读：{loaded:?}");
        assert!(loaded
            .iter()
            .all(|r| r.alert_key != "order:7999" && r.alert_key != "order:7998"));

        // 剪除只碰"已清 + 太老"的行：活跃行永远留在表里（那是"还在亏损"的事实）。
        assert_eq!(db.prune_alerts_cleared(T0 + 1).unwrap(), 1, "只剪已清且早于 cutoff 的行");
        let left = db.load_alerts().unwrap();
        assert_eq!(left.len(), 2, "new/notified 一行不动：{left:?}");
        assert!(left.iter().any(|r| r.alert_key == order_alert_key(7001)));
        assert!(left.iter().any(|r| r.alert_key == order_alert_key(7002)));

        // 已清但"最后一轮还见到"（last_seen_at >= cutoff）不剪 —— 保留期按最后见到算。
        let mut recent = AlertRecord::from_payload(&payload(order_alert_key(7004), -3.0), CHAR, T0 + 100);
        recent.state = AlertState::Cleared;
        db.save_alert(&recent).unwrap();
        assert_eq!(db.prune_alerts_cleared(T0 + 100).unwrap(), 0, "cutoff 当刻算'还见到'");
        assert_eq!(db.prune_alerts_cleared(T0 + 101).unwrap(), 1);
    }
}
