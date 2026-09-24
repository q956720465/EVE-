//! 告警状态机与推送闸门（spec §4.4）。
//!
//! **纯逻辑（零 IO）**：不读 DB、不发网络、不看系统时钟 —— `now`/`today` 一律作参数传入，
//! `fired` 由装配层查好喂进来。落库在 `store/alert_db.rs`（`Db::save_alert`/`load_alerts`/
//! `prune_alerts_cleared`），本模块只做状态迁移与闸门判定 —— 与 `market/lifecycle.rs` 同一分工，
//! 本模块是它的兄弟：那个管"机会"，这个管"亏损告警"。
//!
//! §4.4 四条语义的落点：
//! - **边沿触发**：首次转负才告警，持续为负不重复推。首次落行由 [`AlertRecord::from_payload`]
//!   造出 `New` 行（没有通知史）→ [`can_push`] 放行；推过一次后 [`mark_pushed`] 把它记成
//!   `Notified`，冷却期内 [`can_push`] 一律拒绝。"边沿"由"有没有推过"表达，状态机不比价格
//!   （每轮的数由装配层刷进 `last_margin_pct`/`last_loss_isk`）。
//! - **亏损加深 ≥2pp 补推**：[`ALERT_DEEPEN_PP`] 比的是**亏损率**（`margin_pct`，百分点）。
//!   注意 `alerts.last_notified_loss` 这一列存的是**上次推送时的亏损率**而不是亏损额 ——
//!   阈值单位是 pp，存 ISK 就判不了"加深 2pp"（列名容易被读成金额，字段注释里再写一遍）。
//! - **每日 ≤5 条 = 订单条目数**：[`ALERT_DAILY_CAP`] 是硬闸，穿透不豁免。计量单位是**条目**
//!   （一行 `alerts` = 一张挂单或一笔成交）：合并卡片里有 N 条就吃 N 条额度，当日用量 =
//!   Σ 各条目今天的推送计数（[`day_entries_used`]）。单条目签名的 `can_push` 看不到集合，
//!   所以"当日封口"由装配层用它和 `can_push` 一起做（写法见 [`can_push`] 的注释）。
//! - **提醒中心不受限额**：[`can_push`] 只管推送侧 —— 它返回 false 不代表这条告警不存在；
//!   状态行（含 `Cleared`）、payload 与全量留存从不因限额增删（剪行只由
//!   `prune_alerts_cleared` 按保留期负责）。
//!
//! **`alert_key` 只来自 T8 的构造器**：[`AlertRecord::from_payload`] 直接取
//! `AlertPayload::alert_key`（即 `order_alert_key`/`tx_alert_key` 的产物）。已实现轨的
//! `transaction_id` 根本不在 payload 里（payload 的 `order_id` 是回填匹配到的原挂单 id），
//! 想自己重拼这个 key 在类型上就写不出来 —— 这是 P3 铁律（`alert_key` 是 TEXT，而
//! `order_id`/`transaction_id` 是 INTEGER，跨存储类比较静默返回零行）的机械保证。

use crate::alert::{AlertKind, AlertPayload};

/// 同一 key 每自然日至多几条（硬闸，穿透不豁免）。
pub const ALERT_DAILY_CAP: u32 = 5;
/// 同一 key 的通知冷却（spec 未给数，照 M4b 的 4h）。
pub const ALERT_COOLDOWN_SECS: i64 = 4 * 3600;
/// 亏损率较上次推送再低该值（pp）可穿透冷却。
pub const ALERT_DEEPEN_PP: f64 = 2.0;

/// 告警状态（`alerts.state` 列）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertState {
    /// 本轮观测到的亏损，尚未推送。
    New,
    /// 已推送过（通知史写在 notified_* 列里）。
    Notified,
    /// 亏损消失（撤单/盘口回来/判定面移出）：周期结束，但行与通知史都留着。
    Cleared,
}

impl AlertState {
    /// 落库用 snake_case 字符串 —— 枚举与字符串的互转只在这一处（v6 schema 的注释钉的就是这三个串）。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Notified => "notified",
            Self::Cleared => "cleared",
        }
    }

    /// 照 `OppState::parse` 先例：认不出就 `None`，让调用方决定是跳过还是报错。
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "new" => Some(Self::New),
            "notified" => Some(Self::Notified),
            "cleared" => Some(Self::Cleared),
            _ => None,
        }
    }
}

/// 一行告警的状态台账（字段逐一对应 `alerts` 表的列；`payload` 就是该列的 JSON 文本）。
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRecord {
    /// 去重键 = T8 的 `order_alert_key`/`tx_alert_key` 产物（`order:{id}` / `tx:{id}`）。
    pub alert_key: String,
    pub kind: AlertKind,
    pub char_id: u64,
    pub type_id: u32,
    pub location_id: u64,
    pub is_buy: bool,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
    /// 本轮观测到的亏损额（正数，ISK）。
    pub last_loss_isk: f64,
    /// 本轮观测到的亏损率（%，负 = 亏）。
    pub last_margin_pct: f64,
    pub notified_at: Option<i64>,
    pub notified_day: Option<String>,
    /// 本条目**今天**被推送的条数（当日全局用量是各条目之和，见 [`day_entries_used`]）。
    pub notified_count_day: u32,
    /// 列名叫 `loss`，口径是**上次推送时的亏损率（%）**而不是亏损额 —— 穿透判定比的是
    /// "再低 2pp"（[`ALERT_DEEPEN_PP`]），存 ISK 就没有 pp 可比。
    pub last_notified_loss: Option<f64>,
    pub state: AlertState,
    /// `AlertPayload` 的 JSON：推送卡片与提醒中心共用这一份序列化（spec §4.4，杜绝双源漂移）。
    pub payload: String,
}

impl AlertRecord {
    /// 本轮观测行的**唯一构造处**：首次落行直接用它；已有行用它做"本轮观测"，再交给
    /// [`tick_alert`] 迁移状态、[`mark_pushed`] 记账（三者各管一段，互不覆盖）。
    ///
    /// `alert_key` 取 `AlertPayload::alert_key`：已实现轨的 `transaction_id` 根本不在 payload 里，
    /// 想自己重拼这个 key 在类型上就写不出来（P3 铁律的机械保证，见模块头）。
    pub fn from_payload(p: &AlertPayload, char_id: u64, now: i64) -> Self {
        Self {
            alert_key: p.alert_key.clone(),
            kind: p.kind,
            char_id,
            type_id: p.type_id,
            location_id: p.location_id,
            is_buy: p.is_buy,
            first_seen_at: now,
            last_seen_at: now,
            last_loss_isk: p.loss_isk,
            last_margin_pct: p.margin_pct,
            notified_at: None,
            notified_day: None,
            notified_count_day: 0,
            last_notified_loss: None,
            state: AlertState::New,
            payload: payload_json(p),
        }
    }

    /// 把本轮观测并进状态行：只覆盖"本轮看到的数"（亏损额/亏损率/payload/最后见到），
    /// 身份字段、状态机字段与通知史一律不碰 —— 状态由 [`tick_alert`] 迁移，通知史由
    /// [`mark_pushed`] 记账。撤单重挂之后靠它把卡片上的数刷新成新周期的。
    pub fn observe(&mut self, p: &AlertPayload, now: i64) {
        self.last_seen_at = now;
        self.last_loss_isk = p.loss_isk;
        self.last_margin_pct = p.margin_pct;
        self.payload = payload_json(p);
    }
}

/// `payload` 列的文本（唯一序列化出口）。不可能失败：非有限的数在 T8 的 `into_alert` 里就被
/// 挡掉了（NaN/Inf 造不出卡片）。真炸了也要响亮地炸 —— 写空串会让提醒中心显示一张空白卡，
/// 那比没有卡更难查（与 `into_alert` 拒收 NaN 同一条理由）。
fn payload_json(p: &AlertPayload) -> String {
    serde_json::to_string(p).expect("AlertPayload 的字段全为有限标量，序列化不会失败")
}

/// 状态迁移：`prev` = 库里这一键的上一行，`fired` = 本轮它是否仍处于亏损态。
///
/// - 没有基线行 → `None`：状态机不会凭空造一个没有 `alert_key` 的行（首次落行由
///   [`AlertRecord::from_payload`] 承担，那是唯一构造处）。
/// - 仍在亏损且上一行是 `Cleared` → **新周期**（`state = New`、`first_seen_at = now`），
///   但通知史跨周期保留 —— 否则"转正又转负"（或撤单重挂同一键）就是绕过冷却与日限的通道
///   （与 M4b `revive_preserves_notify_history_so_cooldown_still_bites` 同一教训）。
/// - 仍在亏损且不是 `Cleared` → 状态不动，只推进 `last_seen_at`（边沿触发：不产生第二次告警）。
/// - 不再亏损 → `Cleared`（周期结束，不是删除：行与 payload 全量留在提醒中心）。
pub fn tick_alert(prev: Option<&AlertRecord>, fired: bool, now: i64) -> Option<AlertRecord> {
    // 没有基线行 → 没有可迁移的输入。这里**刻意不返回**一个"空壳行"：那会让落库把没有
    // alert_key 的行写进主键列（首次落行由 from_payload 承担，它才有身份与 payload）。
    let r = prev?;
    if !fired {
        // 周期结束，但不是删除：行、payload 与通知史全量留着（提醒中心不受限额，spec §4.4），
        // 下一轮复活还要靠通知史继续咬住冷却与日限。
        return (r.state != AlertState::Cleared).then(|| AlertRecord {
            state: AlertState::Cleared,
            ..r.clone()
        });
    }
    match r.state {
        // 周期结束后再次命中 = 新告警（新周期）：first_seen_at 记当前这段，
        // 通知史**跨周期保留** —— 否则"转正又转负"就能把 4h 冷却与当日额度洗掉。
        AlertState::Cleared => Some(AlertRecord {
            state: AlertState::New,
            first_seen_at: now,
            last_seen_at: now,
            ..r.clone()
        }),
        // 持续为负：状态不动（边沿触发 = 不产生第二次告警），只推进"最后见到"。
        _ => Some(AlertRecord {
            last_seen_at: now,
            ..r.clone()
        }),
    }
}

/// 推送闸门（spec §4.4）：true → 装配层发送成功后**必须** [`mark_pushed`]，否则冷却与计数不推进。
///
/// 逐条目判定：`Cleared` 不推；本条目当日计数已满不推（硬闸的必要条件：Σ ≥ 本条目计数）；
/// 从未推过（首次转负）可推；冷却期外可推；冷却期内只有"亏损较上次推送再低 ≥
/// [`ALERT_DEEPEN_PP`] pp"能穿透。
///
/// **单条目的局限**：签名只有一个条目，看不到集合，所以"当日 ≤5 条目"的封口必须由装配层补上：
/// `can_push(rec, now, today) && day_entries_used(&all, today) < ALERT_DAILY_CAP`。
pub fn can_push(rec: &AlertRecord, now: i64, today: &str) -> bool {
    // 已清（亏损消失）的条目不在亏损态：推送侧一律拒绝。提醒中心照样读得到它 ——
    // 闸门只管"推不推"，从不删行。
    if rec.state == AlertState::Cleared {
        return false;
    }
    // 日限硬闸：本条目当天已吃满额度 → 当日全局用量必然也满了（Σ ≥ 本条目计数）。
    // 反过来不成立（别的条目吃满时本条目自己仍是 0），所以装配层要补一行：
    //     can_push(rec, now, today) && day_entries_used(&all, today) < ALERT_DAILY_CAP
    let count_today = if rec.notified_day.as_deref() == Some(today) {
        rec.notified_count_day
    } else {
        0
    };
    if count_today >= ALERT_DAILY_CAP {
        return false;
    }
    // 从未推过 = 首次转负（边沿）：直接放行。
    let Some(last) = rec.notified_at else {
        return true;
    };
    // 冷却期外可再提醒一次；冷却期内只有"亏损较上次推送再低 ≥2pp"能穿透。
    let cooldown_passed = now - last >= ALERT_COOLDOWN_SECS;
    let deepened = rec
        .last_notified_loss
        .map(|m| rec.last_margin_pct - m <= -ALERT_DEEPEN_PP)
        .unwrap_or(false);
    cooldown_passed || deepened
}

/// 推送成功后记账（装配层调用；自然日滚动重置计数）。
pub fn mark_pushed(rec: &mut AlertRecord, now: i64, today: &str) {
    rec.state = AlertState::Notified;
    rec.notified_at = Some(now);
    rec.notified_count_day = if rec.notified_day.as_deref() == Some(today) {
        rec.notified_count_day + 1
    } else {
        1
    };
    rec.notified_day = Some(today.to_string());
    // 记的是**亏损率**（pp）而不是亏损额：穿透判定要比"再低 2pp"，存 ISK 没有 pp 可比
    // （列名叫 last_notified_loss，口径见字段注释与模块头）。
    rec.last_notified_loss = Some(rec.last_margin_pct);
}

/// 当日已推送的**条目数**（全局）：Σ 各条目今天的推送计数。
///
/// 计量单位是条目而不是卡片（spec §4.4）—— 一张合并卡片里有 N 条，当日用量就 +N，
/// 所以这里求和而不是数卡片。跨天自然归零（`notified_day` 与 `today` 不同就不计入）。
pub fn day_entries_used(recs: &[AlertRecord], today: &str) -> u32 {
    recs.iter()
        .filter(|r| r.notified_day.as_deref() == Some(today))
        .map(|r| r.notified_count_day)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alert::{order_alert_key, tx_alert_key, AlertKind, CaliberSummary, COST_SRC_FIFO90, TRACK_EXPECTED};
    use chrono::{DateTime, Utc};

    const CHAR: u64 = 90_000_001;
    /// 2026-09-21 14:13:20 UTC —— 与 `DAY` 同一个自然日。
    const T0: i64 = 1_790_000_000;
    const DAY: &str = "2026-09-21";
    const DAY2: &str = "2026-09-22";

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// 一张挂单轨的载荷（字段是 T8 契约的副本；状态机测试只关心 key 与亏损率）。
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

    /// 一张已实现轨的载荷（key 是 `tx:{id}`，与挂单轨同号也不撞）。
    fn tx_payload(id: i64) -> AlertPayload {
        AlertPayload {
            alert_key: tx_alert_key(id),
            kind: AlertKind::RealizedLoss,
            order_id: 555,
            ..payload(String::new(), -5.0)
        }
    }

    /// 首次落库的条目：`state = New`、无通知史。
    fn fresh(key: String, margin_pct: f64) -> AlertRecord {
        AlertRecord::from_payload(&payload(key, margin_pct), CHAR, T0)
    }

    #[test]
    fn edge_triggered_only_on_transition_into_loss() {
        // 边沿触发（spec §4.4）：首次转负才告警；持续为负不重复推。
        let first = fresh(order_alert_key(7001), -3.0);
        assert_eq!(first.state, AlertState::New);
        assert!(can_push(&first, T0, DAY), "首次转负 → 可推");

        // 首发之后：状态记成已推，冷却期内同一条件再判定不得再推 ——
        // "边沿"由"有没有推过"表达（状态机不去比价格，价格由装配层每轮刷进 last_margin_pct）。
        let mut rec = first.clone();
        mark_pushed(&mut rec, T0, DAY);
        let still = tick_alert(Some(&rec), true, T0 + 60).expect("有基线行就有迁移");
        assert_eq!(still.state, AlertState::Notified, "持续为负不改状态");
        assert_eq!(still.last_seen_at, T0 + 60, "最后见到要推进");
        assert!(!can_push(&still, T0 + 60, DAY), "冷却期内不重复推");

        // 冷却过后仍在亏 → 允许再提醒一次（"冷却期内不重推"的反面）。
        assert!(can_push(&still, T0 + ALERT_COOLDOWN_SECS, DAY), "冷却过后可再提醒");

        // 没有基线行就没有迁移：状态机若凭空造一个没有 alert_key 的行，落库会把垃圾写进主键列
        // （首次落行由 AlertRecord::from_payload 承担）。
        assert!(tick_alert(None, true, T0).is_none(), "无基线行 → 不迁移");
    }

    #[test]
    fn deepening_by_two_pp_bypasses_cooldown_but_not_daily_cap() {
        // "预期亏加深 ≥2pp 补推"（spec §4.4）：比的是**亏损率**（百分点），不是亏损额。
        let mut rec = fresh(order_alert_key(7001), -1.0);
        mark_pushed(&mut rec, T0, DAY);
        assert_eq!(rec.last_notified_loss, Some(-1.0), "记下推送那一刻的亏损率");

        assert!(!can_push(&rec, T0 + 3600, DAY), "冷却内、没加深 → 不推");

        // 加深 2.5pp（−1.0% → −3.5%）→ 穿透冷却。
        rec.last_margin_pct = -3.5;
        assert!(can_push(&rec, T0 + 3600, DAY), "加深 ≥2pp 穿透冷却");

        // 差一点点不穿透：阈值就是 2.0，1.9pp 不算（否则"加深"会被慢速漂移磨成噪声）。
        let mut shallow = rec.clone();
        shallow.last_margin_pct = -2.9;
        assert!(!can_push(&shallow, T0 + 3600, DAY), "1.9pp 不到阈值");

        // 日限是**硬闸**，穿透不豁免：该条目当天已把 5 条额度吃满 → 拒绝，冷却过了也一样。
        let mut capped = rec.clone();
        capped.notified_count_day = ALERT_DAILY_CAP;
        assert!(!can_push(&capped, T0 + 3600, DAY), "日限 ≤5 条是硬闸，穿透不豁免");
        assert!(!can_push(&capped, T0 + ALERT_COOLDOWN_SECS, DAY));
    }

    #[test]
    fn daily_cap_counts_order_entries_not_cards() {
        // spec §4.4：每日 ≤5 条 = **订单条目数**；一张合并卡片里有 N 条就吃 N 条额度。
        // 若按卡片计（一张卡只记 1），当日用量会小 N 倍 —— 这正是"合并卡片"最容易踩的坑。
        let mut card: Vec<AlertRecord> = (7001..=7005).map(|id| fresh(order_alert_key(id), -3.0)).collect();
        for r in card.iter_mut() {
            assert!(can_push(r, T0, DAY), "每条目各自是边沿（首次转负）");
            mark_pushed(r, T0, DAY); // 一张卡里的 5 条各自记账
        }
        assert!(card.iter().all(|r| r.notified_count_day == 1), "各条目各记 1 条：{card:?}");
        assert_eq!(
            day_entries_used(&card, DAY),
            ALERT_DAILY_CAP,
            "当日用量 = 条目数 5（按卡片计只会得到 1）"
        );

        // 配额耗尽后第 6 条要顺延次日：装配层的封口判据 = 逐条目闸门 + 当日用量。
        let sixth = fresh(order_alert_key(7006), -3.0);
        assert!(can_push(&sixth, T0, DAY), "这条自己没推过（单条目闸门放行）");
        assert!(
            day_entries_used(&card, DAY) >= ALERT_DAILY_CAP,
            "但当日 5 条已满 → 顺延次日"
        );

        // 自然日滚动：次日的用量从零起算，同一条目跨天重新计数。
        assert_eq!(day_entries_used(&card, DAY2), 0, "跨天归零");
        mark_pushed(&mut card[0], T0 + 86_400, DAY2);
        assert_eq!(day_entries_used(&card, DAY2), 1, "次日只算新推的那一条");
        assert_eq!(card[0].notified_count_day, 1, "跨天重新从 1 起算");
    }

    #[test]
    fn cleared_then_refired_is_a_new_alert_but_keeps_notify_history() {
        // 亏损消失（撤单/盘口回来）→ Cleared 是**周期结束**：行与通知史都留着。
        let mut rec = fresh(order_alert_key(7001), -3.0);
        mark_pushed(&mut rec, T0, DAY);
        let cleared = tick_alert(Some(&rec), false, T0 + 600).expect("有基线行就有迁移");
        assert_eq!(cleared.state, AlertState::Cleared);
        assert_eq!(cleared.notified_at, Some(T0), "通知史随行保留");

        // 同一 alert_key 再次转负 = **新告警**（新周期），但通知史跨周期保留 ——
        // 否则"转正又转负"就是绕过冷却与日限的通道（与 M4b 的 revive 同一教训）。
        let refired = tick_alert(Some(&cleared), true, T0 + 700).expect("有基线行就有迁移");
        assert_eq!(refired.state, AlertState::New, "复现即新周期");
        assert_eq!(refired.first_seen_at, T0 + 700, "first_seen_at 记当前这段周期");
        assert_eq!(refired.notified_at, Some(T0), "通知史跨周期保留");
        assert_eq!(refired.notified_count_day, 1);
        assert!(!can_push(&refired, T0 + 700, DAY), "冷却仍咬：同键复活不得绕开 4h");
        assert_eq!(
            day_entries_used(std::slice::from_ref(&refired), DAY),
            1,
            "跨周期保留的计数仍在当日额度里"
        );

        // 真"撤了重挂"是**新 order_id = 新 alert_key**：它确实是新告警（冷却无从继承），
        // 但当日额度是**全局**的 —— 老条目今天推掉的条数还在账上，新条目要跟它共享硬闸。
        let mut day_used: Vec<AlertRecord> = vec![refired.clone()];
        for id in 7010..7014 {
            let mut r = fresh(order_alert_key(id), -3.0);
            mark_pushed(&mut r, T0, DAY);
            day_used.push(r);
        }
        assert_eq!(
            day_entries_used(&day_used, DAY),
            ALERT_DAILY_CAP,
            "老条目 + 4 条 = 当日 5 条已满"
        );
        let replaced = fresh(order_alert_key(7999), -6.0);
        assert!(can_push(&replaced, T0 + 800, DAY), "新挂的单自己是新告警（单条目闸门放行）");
        assert!(
            day_entries_used(&day_used, DAY) >= ALERT_DAILY_CAP,
            "但当日额度已尽 → 装配层顺延次日"
        );
    }

    #[test]
    fn alert_center_never_limited_but_push_is() {
        // 提醒中心全量留存、不受限额（spec §4.4）；can_push 只管推送侧。
        let mut rec = fresh(order_alert_key(7001), -3.0);
        mark_pushed(&mut rec, T0, DAY);
        assert!(!can_push(&rec, T0 + 60, DAY), "冷却内不推");
        rec.notified_count_day = ALERT_DAILY_CAP;
        assert!(!can_push(&rec, T0 + ALERT_COOLDOWN_SECS, DAY), "当日额度耗尽不推");

        // 但台账一个字都没少：行、payload、状态、最后见到 —— 提醒中心读的就是这些。
        assert_eq!(rec.state, AlertState::Notified);
        assert!(
            rec.payload.contains("\"alert_key\":\"order:7001\""),
            "payload 是提醒中心的原料：{}",
            rec.payload
        );
        assert_eq!(rec.last_seen_at, T0);
        // 闸门拿的是 &，两次拒绝都没动过任何一个字段。
        assert_eq!(rec.notified_count_day, ALERT_DAILY_CAP);

        // 周期结束的行同样留存（Cleared 不是删除）：下一轮复活还要靠它保留通知史。
        let cleared = tick_alert(Some(&rec), false, T0 + 100).expect("有基线行就有迁移");
        assert_eq!(cleared.state, AlertState::Cleared);
        assert!(cleared.payload.contains("order:7001"), "清掉亏损态不丢 payload");
        assert!(!can_push(&cleared, T0 + 7200, DAY2), "已清的条目不在亏损态，推送侧一律拒绝");
    }

    #[test]
    fn observe_refreshes_this_rounds_numbers_and_leaves_state_alone() {
        // observe 只覆盖"本轮看到的数"：状态机字段与通知史一列不动 ——
        // 否则每轮刷新都会把冷却与计数洗掉，闸门形同虚设。
        let mut rec = fresh(order_alert_key(7001), -1.0);
        mark_pushed(&mut rec, T0, DAY);
        let before = rec.clone();
        rec.observe(&payload(order_alert_key(7001), -4.0), T0 + 900);
        assert_eq!(rec.last_margin_pct, -4.0);
        assert_eq!(rec.last_seen_at, T0 + 900);
        assert_eq!(rec.state, before.state, "状态不被观测刷新改写");
        assert_eq!(rec.notified_at, before.notified_at, "通知史不被观测刷新改写");
        assert_eq!(rec.notified_count_day, before.notified_count_day);
        assert_eq!(rec.first_seen_at, before.first_seen_at);
        assert!(rec.payload.contains("\"margin_pct\":-4.0"), "{}", rec.payload);
        assert!(can_push(&rec, T0 + 900, DAY), "深化 3pp（−1.0 → −4.0）→ 穿透冷却");
    }

    #[test]
    fn alert_state_strings_are_snake_case_and_share_one_mapping() {
        // v6 schema 的注释把线上形态钉成 new/notified/cleared：枚举与字符串互转只此一处。
        assert_eq!(AlertState::New.as_str(), "new");
        assert_eq!(AlertState::Notified.as_str(), "notified");
        assert_eq!(AlertState::Cleared.as_str(), "cleared");
        assert_eq!(AlertState::parse("New"), None, "PascalCase 不是线上的状态串");
        assert_eq!(AlertState::parse("expired"), None, "expired 是机会生命周期的态，不是告警的");
        for s in [AlertState::New, AlertState::Notified, AlertState::Cleared] {
            assert_eq!(AlertState::parse(s.as_str()), Some(s), "{s:?} 往返失败");
        }
    }

    #[test]
    fn record_key_is_the_payloads_canonical_text_key() {
        // P3：记录的去重键只来自 T8 的构造器产物（`order:{id}`/`tx:{id}`）——
        // 本模块不自己拼 key，也不把 key 与裸整数混用；已实现轨的 transaction_id 根本不在
        // payload 里（payload 的 order_id 是回填匹配到的原挂单 id），想重拼都写不出来。
        let order = fresh(order_alert_key(7001), -3.0);
        let tx = AlertRecord::from_payload(&tx_payload(101), CHAR, T0);
        assert_eq!(order.alert_key, "order:7001");
        assert_eq!(tx.alert_key, "tx:101");
        assert_eq!(tx.kind, AlertKind::RealizedLoss);
        assert_ne!(
            order_alert_key(101),
            tx_alert_key(101),
            "同号的挂单与成交不得撞成同一行"
        );
        assert!(
            order.alert_key.parse::<i64>().is_err(),
            "规范形态永远不是纯数字串"
        );
    }
}
