//! 机会生命周期状态机（方案 v3.1 §4.2；spec §1 M4b 定义）。
//! 状态迁移与通知闸门是纯逻辑——判决由调用层装配（update_round / M4c 推送层），
//! 模块本身不读 DB、不发网络。

use crate::market::Opportunity;

/// 连续缺席该轮数即 expired（v3.1：连续 2 轮未再命中）。
pub const MISS_LIMIT: u32 = 2;
/// 同一 key 的通知冷却（v3.1 默认 4h）。
pub const NOTIFY_COOLDOWN_SECS: i64 = 4 * 3600;
/// 净利率较上次通知上升该值（pp）可穿透冷却。
pub const NOTIFY_MARGIN_BYPASS_PP: f64 = 2.0;
/// 同一 key 每自然日至多几条（v3.1 新增硬闸）。
pub const NOTIFY_DAILY_CAP: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OppState {
    New,
    Notified,
    Expired,
    Invalidated,
}

impl OppState {
    /// 落库用 snake_case 字符串——枚举与字符串的互转只在这一处。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Notified => "notified",
            Self::Expired => "expired",
            Self::Invalidated => "invalidated",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "new" => Some(Self::New),
            "notified" => Some(Self::Notified),
            "expired" => Some(Self::Expired),
            "invalidated" => Some(Self::Invalidated),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Expired | Self::Invalidated)
    }
}

/// 一行机会的生命周期台账（对应 `opportunities` 表一行）。
#[derive(Debug, Clone, PartialEq)]
pub struct OppRecord {
    pub type_id: u32,
    pub buy_loc: u64,
    pub sell_loc: u64,
    pub state: OppState,
    pub miss_streak: u32,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
    pub best_margin_pct: f64,
    pub last_margin_pct: f64,
    pub last_net_total: f64,
    pub last_qty: u64,
    pub notified_at: Option<i64>,
    pub notified_day: Option<String>,
    pub notified_count_day: u32,
    pub last_notified_margin_pct: Option<f64>,
}

/// 本轮对该 key 的判决（由调用层装配）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Verdict<'a> {
    /// 本轮通过全部过滤（scan 存活机会）。
    Hit(&'a Opportunity),
    /// 双侧有盘但被过滤否掉（evaluate_pair 的 Dropped*）。
    Dropped,
    /// 双侧至少一侧无盘。
    Absent,
}

/// 状态迁移。返回 `None` = 不动（终态行只待命中复活，缺席/被否都保持原样）。
pub fn tick(prev: Option<&OppRecord>, v: Verdict<'_>, now: i64) -> Option<OppRecord> {
    match v {
        Verdict::Hit(o) => Some(match prev {
            Some(r) if !r.state.is_terminal() => OppRecord {
                state: r.state,
                miss_streak: 0,
                last_seen_at: now,
                best_margin_pct: r.best_margin_pct.max(o.margin_pct),
                last_margin_pct: o.margin_pct,
                last_net_total: o.net_total,
                last_qty: o.qty,
                ..r.clone()
            },
            // 终态复现与首次现身同构：新生命周期（防轰炸交给通知闸门，不靠旧状态压制）。
            _ => OppRecord {
                type_id: o.type_id,
                buy_loc: o.buy_loc,
                sell_loc: o.sell_loc,
                state: OppState::New,
                miss_streak: 0,
                first_seen_at: now,
                last_seen_at: now,
                best_margin_pct: o.margin_pct,
                last_margin_pct: o.margin_pct,
                last_net_total: o.net_total,
                last_qty: o.qty,
                // 通知历史跨生命周期保留（否则"消失→复现"可绕过冷却与日上限）。
                notified_at: prev.and_then(|r| r.notified_at),
                notified_day: prev.and_then(|r| r.notified_day.clone()),
                notified_count_day: prev.map(|r| r.notified_count_day).unwrap_or(0),
                last_notified_margin_pct: prev.and_then(|r| r.last_notified_margin_pct),
            },
        }),
        Verdict::Dropped => prev.and_then(|r| {
            (!r.state.is_terminal()).then(|| OppRecord {
                state: OppState::Invalidated,
                ..r.clone()
            })
        }),
        Verdict::Absent => prev.and_then(|r| {
            if r.state.is_terminal() {
                return None;
            }
            let miss = r.miss_streak + 1;
            Some(OppRecord {
                miss_streak: miss,
                state: if miss >= MISS_LIMIT {
                    OppState::Expired
                } else {
                    r.state
                },
                ..r.clone()
            })
        }),
    }
}

/// 通知闸门（v3.1 §4.2）：首发可推；4h 冷却；净利率较上次通知 +≥2pp 穿透；
/// 每自然日至多 2 条（硬闸，穿透不豁免）。true → 调用方发送后必须 mark_notified。
pub fn can_notify(rec: &OppRecord, now: i64, today: &str) -> bool {
    let count_today = if rec.notified_day.as_deref() == Some(today) {
        rec.notified_count_day
    } else {
        0
    };
    if count_today >= NOTIFY_DAILY_CAP {
        return false;
    }
    let Some(last) = rec.notified_at else {
        return true;
    };
    let cooldown_passed = now - last >= NOTIFY_COOLDOWN_SECS;
    let margin_jump = rec
        .last_notified_margin_pct
        .map(|m| rec.last_margin_pct - m >= NOTIFY_MARGIN_BYPASS_PP)
        .unwrap_or(false);
    cooldown_passed || margin_jump
}

/// 推送成功后记账（M4c 调用；自然日滚动重置计数）。
pub fn mark_notified(rec: &mut OppRecord, now: i64, today: &str) {
    rec.state = OppState::Notified;
    rec.notified_at = Some(now);
    rec.notified_count_day = if rec.notified_day.as_deref() == Some(today) {
        rec.notified_count_day + 1
    } else {
        1
    };
    rec.notified_day = Some(today.to_string());
    rec.last_notified_margin_pct = Some(rec.last_margin_pct);
}

// ---- 装配层：每轮结算（读 DB / 落库，与 history::backfill 同先例）-----------

use std::collections::{HashMap, HashSet};

use crate::error::Result;
use crate::market::flip::{self, FlipParams};
use crate::market::{PairVerdict, StationOrderBook};
use crate::store::Db;

/// 每轮机会生命周期的结算计数（daemon/tracing 观测用）。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TickStats {
    pub active: usize,
    pub new: usize,
    pub revived: usize,
    pub invalidated: usize,
    pub expired: usize,
    pub saved: usize,
}

/// 终态行的保留期：90 天后剪除（历史使命结束，表必须有界）。
pub const OPP_KEEP_SECS: i64 = 90 * 24 * 3600;

fn key_of(r: &OppRecord) -> (u32, u64, u64) {
    (r.type_id, r.buy_loc, r.sell_loc)
}

/// 一轮生命周期结算（v3.1 §4.2）。装配：读快照（含跨区窗口）→ scan →
/// 活跃键逐键复核（有盘被否=invalidated；缺盘=缺席计数）→ 终态仅命中复活 → 落库。
/// `Ok(None)` = 本地无任何快照（还没跑过采集），调用方静默跳过。
pub fn update_round(db: &Db, now: i64) -> Result<Option<TickStats>> {
    if db.counts()?.rows == 0 {
        return Ok(None);
    }
    let books: Vec<StationOrderBook> = db.load_books()?;
    let hubs = db.flip_hubs()?;
    let vol = db.latest_vol24()?;
    let params: FlipParams = db.get_flip_params()?;
    let out = flip::scan(&books, &hubs, &params, &vol);

    let passed: HashMap<(u32, u64, u64), &Opportunity> = out
        .opportunities
        .iter()
        .map(|o| ((o.type_id, o.buy_loc, o.sell_loc), o))
        .collect();
    let book_idx: HashMap<(u64, u32), &StationOrderBook> = books
        .iter()
        .map(|b| ((b.location_id, b.type_id), b))
        .collect();
    let rows: HashMap<(u32, u64, u64), OppRecord> = db
        .load_opps()?
        .into_iter()
        .map(|r| (key_of(&r), r))
        .collect();

    // 观察面 = 活跃键 ∪ 本轮命中键（终态只有命中才复活）。
    let mut keys: HashSet<(u32, u64, u64)> = rows
        .values()
        .filter(|r| !r.state.is_terminal())
        .map(key_of)
        .collect();
    keys.extend(passed.keys().copied());

    let mut stats = TickStats::default();
    for key in keys {
        let prev = rows.get(&key);
        let verdict = match passed.get(&key) {
            Some(o) => Verdict::Hit(o),
            None => match (book_idx.get(&(key.1, key.0)), book_idx.get(&(key.2, key.0))) {
                (Some(a), Some(b)) => match flip::evaluate_pair(a, b, &params) {
                    PairVerdict::DroppedBatch
                    | PairVerdict::DroppedShortfall
                    | PairVerdict::DroppedThreshold => Verdict::Dropped,
                    _ => Verdict::Absent, // NoMarket：市场不在 = 缺席
                },
                _ => Verdict::Absent,
            },
        };
        let Some(next) = tick(prev, verdict, now) else {
            continue;
        };
        if Some(&next) == prev {
            continue;
        }
        match (&next.state, prev.map(|r| r.state)) {
            (OppState::New, None) => stats.new += 1,
            (OppState::New, Some(s)) if s.is_terminal() => stats.revived += 1,
            (OppState::Invalidated, _) => stats.invalidated += 1,
            (OppState::Expired, _) => stats.expired += 1,
            _ => {}
        }
        db.save_opp(&next)?;
        stats.saved += 1;
    }
    db.prune_opps_terminal(now - OPP_KEEP_SECS)?;
    // `active` 是本回合结束后的活跃行数——UI 与 daemon 用它作为"当前在跟踪的机会数量"，
    // 若拿回合开始前的数字，首轮登记会显示 0 而错过唯一有意义的观测点。
    stats.active = db
        .load_opps()?
        .iter()
        .filter(|r| !r.state.is_terminal())
        .count();
    Ok(Some(stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{Opportunity, VolSource};

    fn opp(margin: f64) -> Opportunity {
        Opportunity {
            type_id: 34,
            buy_loc: 60003760,
            sell_loc: 60008494,
            buy_price: 100.0,
            sell_price: 110.0,
            qty: 500,
            net_per_unit: margin,
            net_total: 600.0,
            margin_pct: margin,
            vol24: 100,
            vol_source: VolSource::Depth,
            buy_levels: 5,
            sell_levels: 5,
        }
    }

    #[test]
    fn first_hit_opens_a_new_lifecycle() {
        let o = opp(5.0);
        let r = tick(None, Verdict::Hit(&o), 1000).unwrap();
        assert_eq!(r.state, OppState::New);
        assert_eq!(r.first_seen_at, 1000);
        assert_eq!(r.miss_streak, 0);
    }

    #[test]
    fn hit_refreshes_and_keeps_state() {
        let o1 = opp(5.0);
        let mut r1 = tick(None, Verdict::Hit(&o1), 1000).unwrap();
        r1.state = OppState::Notified;
        r1.miss_streak = 1;
        let o2 = opp(7.5);
        let r2 = tick(Some(&r1), Verdict::Hit(&o2), 2000).unwrap();
        assert_eq!(r2.state, OppState::Notified, "命中不改状态");
        assert_eq!(r2.miss_streak, 0, "命中清零缺席计数");
        assert!((r2.best_margin_pct - 7.5).abs() < 1e-9);
    }

    #[test]
    fn dropped_goes_invalidated_immediately_absent_needs_two_rounds() {
        let o = opp(5.0);
        let r = tick(None, Verdict::Hit(&o), 1000).unwrap();
        let inv = tick(Some(&r), Verdict::Dropped, 2000).unwrap();
        assert_eq!(inv.state, OppState::Invalidated);
        // 缺席：第一轮只累计，第二轮才过期
        let a1 = tick(Some(&r), Verdict::Absent, 2000).unwrap();
        assert_eq!(a1.state, OppState::New);
        assert_eq!(a1.miss_streak, 1);
        let a2 = tick(Some(&a1), Verdict::Absent, 3000).unwrap();
        assert_eq!(a2.state, OppState::Expired);
    }

    #[test]
    fn terminal_rows_stay_put_until_revived_as_new() {
        let o = opp(5.0);
        let mut dead = tick(None, Verdict::Hit(&o), 1000).unwrap();
        dead.state = OppState::Expired;
        assert!(tick(Some(&dead), Verdict::Absent, 2000).is_none(), "终态 + 缺席 = 不变");
        assert!(tick(Some(&dead), Verdict::Dropped, 2000).is_none(), "终态 + 被否 = 不变");
        let o2 = opp(6.0);
        let revived = tick(Some(&dead), Verdict::Hit(&o2), 3000).unwrap();
        assert_eq!(revived.state, OppState::New, "复现即新生命周期");
        assert_eq!(revived.first_seen_at, 3000);
    }

    #[test]
    fn revive_preserves_notify_history_so_cooldown_still_bites() {
        // "消失→复现"不得成为绕过冷却的手段：通知字段跨生命周期保留。
        let o = opp(5.0);
        let mut r = tick(None, Verdict::Hit(&o), 1000).unwrap();
        mark_notified(&mut r, 1000, "2027-01-15");
        r.state = OppState::Expired;
        let revived = tick(Some(&r), Verdict::Hit(&o), 2000).unwrap();
        assert_eq!(revived.notified_at, Some(1000));
        assert_eq!(revived.notified_count_day, 1);
        assert!(!can_notify(&revived, 2000, "2027-01-15"), "4h 冷却内不可推");
    }

    #[test]
    fn notify_gate_cooldown_bypass_and_daily_cap() {
        let o = opp(5.0);
        let mut r = tick(None, Verdict::Hit(&o), 1000).unwrap();
        assert!(can_notify(&r, 1000, "2027-01-15"), "首发可推");
        mark_notified(&mut r, 1000, "2027-01-15");
        assert!(!can_notify(&r, 1000 + 3600, "2027-01-15"), "4h 内冷却");
        // ≥2pp 穿透冷却
        r.last_margin_pct = 7.5;
        assert!(can_notify(&r, 1000 + 3600, "2027-01-15"), "+2pp 穿透冷却");
        // 日上限是硬闸：即使加了 2pp 也拦住
        mark_notified(&mut r, 1000 + 3600, "2027-01-15");
        r.last_margin_pct = 12.0;
        assert!(!can_notify(&r, 1000 + 7200, "2027-01-15"), "每自然日至多 2 条");
        // 跨天重置计数
        r.last_margin_pct = 5.0;
        assert!(can_notify(&r, 1000 + 86400, "2027-01-16"));
    }

    #[test]
    fn mark_notified_writes_day_and_count_rollover() {
        let o = opp(5.0);
        let mut r = tick(None, Verdict::Hit(&o), 1000).unwrap();
        mark_notified(&mut r, 1000, "2027-01-15");
        assert_eq!((r.state, r.notified_count_day), (OppState::Notified, 1));
        mark_notified(&mut r, 2000, "2027-01-16");
        assert_eq!(r.notified_count_day, 1, "跨天归 1");
        mark_notified(&mut r, 3000, "2027-01-16");
        assert_eq!(r.notified_count_day, 2);
    }
}

/// update_round 是装配层：读 DB → scan → 逐键复核 → 落库。
/// 用 in-memory Db 走完整生命周期，与纯状态机分开测试。
#[cfg(test)]
mod update_round_tests {
    use super::*;
    use crate::market::{FlipParams, Hub, PriceLevel, StationOrderBook, STATION_JITA};
    use crate::store::Db;

    fn book(loc: u64, ty: u32, asks: &[(f64, u64, u32)], bids: &[(f64, u64, u32)]) -> StationOrderBook {
        let pl = |v: &[(f64, u64, u32)]| {
            v.iter()
                .map(|&(price, volume, orders)| PriceLevel { price, volume, orders })
                .collect::<Vec<_>>()
        };
        let asks_v = pl(asks);
        let bids_v = pl(bids);
        StationOrderBook {
            location_id: loc,
            type_id: ty,
            is_npc_station: true,
            best_bid: bids_v.first().map(|l| l.price),
            bid_qty: bids_v.first().map(|l| l.volume).unwrap_or(0),
            best_ask: asks_v.first().map(|l| l.price),
            ask_qty: asks_v.first().map(|l| l.volume).unwrap_or(0),
            bid_levels: bids_v.iter().map(|l| l.orders).sum(),
            ask_levels: asks_v.iter().map(|l| l.orders).sum(),
            bid_depth: bids_v,
            ask_depth: asks_v,
            skipped_stale: 0,
            skipped_thin: 0,
            skipped_wholesale: 0,
        }
    }

    fn hubs_of(ids: &[u64]) -> Vec<Hub> {
        ids.iter()
            .enumerate()
            .map(|(i, &location_id)| Hub {
                location_id,
                order_count: 100,
                share_pct: 50.0,
                rank: i + 1,
            })
            .collect()
    }

    fn seeds() -> (Db, Vec<StationOrderBook>) {
        // 夹具对齐既有 app 测试：默认费率下 100→130 是真实盈利对（margin ≈16.35%），
        // 不受默认 3% 阈值误杀。
        let db = Db::in_memory().unwrap();
        let books = vec![
            book(STATION_JITA, 34, &[(100.0, 2000, 5)], &[]),
            book(60008494, 34, &[], &[(130.0, 2000, 5)]),
        ];
        db.write_snapshot(&books, Some("lm")).unwrap();
        db.write_hub_pool(&hubs_of(&[STATION_JITA, 60008494])).unwrap();
        (db, books)
    }

    #[test]
    fn update_round_walks_full_lifecycle() {
        let (db, _books) = seeds();
        let t0 = 1_800_000_000;
        let s1 = update_round(&db, t0).unwrap().unwrap();
        assert_eq!((s1.new, s1.active), (1, 1), "首轮登记为 new");
        let s2 = update_round(&db, t0 + 360).unwrap().unwrap();
        assert_eq!(s2.new, 0, "同盘再来一轮不重复登记");
        // 阈值抬到 99%：有盘但被否 → invalidated（即时，不等两轮）
        let mut p = FlipParams::default();
        p.margin_threshold_pct = 99.0;
        db.set_flip_params(&p).unwrap();
        let s3 = update_round(&db, t0 + 720).unwrap().unwrap();
        assert_eq!(s3.invalidated, 1);
        // 阈值恢复 → 命中即复活为 new
        db.set_flip_params(&FlipParams::default()).unwrap();
        let s4 = update_round(&db, t0 + 1080).unwrap().unwrap();
        assert_eq!(s4.revived, 1);
    }

    #[test]
    fn two_consecutive_absences_expire() {
        let (db, books) = seeds();
        let t0 = 1_800_000_000;
        update_round(&db, t0).unwrap();
        // 卖站单簿整体消失（写一个只有 Jita 的快照）
        db.write_snapshot(&books[..1], Some("lm")).unwrap();
        let s1 = update_round(&db, t0 + 360).unwrap().unwrap();
        assert_eq!(s1.expired, 0, "第一轮缺席只累计");
        let s2 = update_round(&db, t0 + 720).unwrap().unwrap();
        assert_eq!(s2.expired, 1, "第二轮缺席 → expired");
    }

    #[test]
    fn empty_snapshot_is_a_no_op() {
        let db = Db::in_memory().unwrap();
        assert!(
            update_round(&db, 1000).unwrap().is_none(),
            "无快照 → 静默跳过"
        );
    }
}
