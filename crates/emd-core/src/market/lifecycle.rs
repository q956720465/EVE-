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
