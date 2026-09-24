# M4b 跨区补拉与机会生命周期实现计划（T1.5 + opportunities v5 + 状态机）

> **For agentic workers:** 本环境无编码子代理（已在 M3 验证），采用**会话内 TDD + CodeReview 子代理评审**执行。步骤用 `- [ ]` 勾选跟踪。
> 依据：`docs/superpowers/specs/2026-09-24-m4-flip-engine-design.md` §1 + 方案 v3.1 §3.1/§4.2。与 spec 冲突时以 spec 为准。

**Goal:** 落地 M4b——T1.5 每 12 分钟跨区补拉（Amarr/Dodixie/Rens 线）、`opportunities` 表（迁移 v5）与机会生命周期状态机（new→notified→expired/invalidated、冷却 4h 与每日上限）。

**Architecture:** 新增 `emd-core::market::lifecycle`（纯状态机 + 装配式 `update_round`）；`orderbook.rs` 加单类型定向拉取；`scheduler.rs` 挂 T1.5 隔轮钩子与每轮生命周期结算；迁移 v5 落 `opportunities` / `xregion_books` / `xregion_log` 三表。扫描/费率公式仍单源在 `flip.rs`。

**Tech Stack:** Rust（workspace 三 crate）+ rusqlite + serde；Vite/React/TS/Zustand（仅跨区角标一处）。

**执行纪律（沿用 M4a）:** 直接提交 `main`（单开发者、无分支保护）；每 Task 一个提交；`cargo test -p emd-core -p emd-daemon -p emd-app` 全绿才提交（AGENTS.md 唯一验证入口）；注释中文、写"为什么"。

**Global Constraints（硬约束，每个 Task 都适用）:**
- 引擎纯函数化：`flip::scan`/`evaluate_pair`/`lifecycle::tick` 不读 DB/网络；DB 读取只在 `db.rs` 与调用层（`lifecycle::update_round`、`scheduler::run_t1_5` 属装配层，与 `history::backfill` 同先例）
- 费率与结算公式仍只在 `flip.rs` 一处（`settle` 唯一装配）；状态机与通知闸门是独立纯逻辑
- `state` 落库用 snake_case 字符串（`new/notified/expired/invalidated`），枚举与字符串互转集中在 `OppState`
- 目标枢纽站以「每站 0 本 → 判定为 ID 配错」的显式日志兜底（真机验收必查三站各 >0 本）
- T1.5 默认启用（方案 v3.1 产品口径），`EMD_XREGION=0` 可关；11:10–11:35 UTC 错峰窗口内不跑

---

### Task 1: `flip.rs` — 抽取 `evaluate_pair`（状态机"失效"判定与 scan 同源）

**Files:** Modify: `crates/emd-core/src/market/flip.rs`

**为什么:** v3.1 §4.2 的 `invalidated` = "本轮命中但被过滤条件否掉"——生命周期复核必须能区分"有盘但被否"与"没盘了"，规则若与 scan 各写一份必然漂移。

- [ ] **Step 1: 写失败测试**（追加到 flip.rs 测试模块）

```rust
#[test]
fn pair_verdict_passed_carries_settle_numbers() {
    let books = doc_books();
    let mut p = FlipParams::default();
    p.fees.sales_tax_pct = 5.0; p.fees.broker_pct = 3.0;
    p.margin_threshold_pct = 0.0; p.min_batch = 1; p.capital_isk = 1_000_000.0;
    let v = evaluate_pair(&books[0], &books[1], &p);
    match v {
        PairVerdict::Passed { net_per_unit, qty, .. } => {
            assert!((net_per_unit - 1.2).abs() < 1e-9, "与 scan 铁证同源");
            assert_eq!(qty, 500);
        }
        other => panic!("应为 Passed：{other:?}"),
    }
}

#[test]
fn pair_verdict_classes_map_to_scan_stats() {
    let books = doc_books();
    let base = |p: &mut FlipParams| {
        p.margin_threshold_pct = -100.0; // 不设阈，专测其它分档
        p.min_batch = 1; p.capital_isk = 1_000_000.0;
    };
    let mut p = FlipParams::default(); base(&mut p); p.min_batch = 600;
    assert_eq!(evaluate_pair(&books[0], &books[1], &p), PairVerdict::DroppedBatch);

    let mut p2 = FlipParams::default(); base(&mut p2); p2.margin_threshold_pct = 99.0;
    assert_eq!(evaluate_pair(&books[0], &books[1], &p2), PairVerdict::DroppedThreshold);

    // 买站没有卖盘（只有买单）→ NoMarket（生命周期按"缺席"计）
    let no_ask = book(STATION_JITA, 34, &[], &[(110.0, 1000, 5)]);
    let mut p3 = FlipParams::default(); base(&mut p3);
    assert_eq!(evaluate_pair(&no_ask, &books[1], &p3), PairVerdict::NoMarket);
}

#[test]
fn scan_stats_unchanged_after_extraction() {
    // 抽取重构后，既有统计口径必须逐位不变（防漂移回归）。
    let mut p = FlipParams::default();
    p.margin_threshold_pct = 3.0; p.min_batch = 1; p.capital_isk = 1_000_000.0;
    let out = scan(&doc_books(), &hubs_of(&[STATION_JITA, 60015157]), &p, &HashMap::new());
    assert_eq!(out.stats.pairs_evaluated, 1);
    assert_eq!(out.stats.dropped_threshold, 1);
}
```

- [ ] **Step 2: 跑测试确认失败**（`cargo test -p emd-core flip 2>&1 | Select-String 'error|test result'`，编译失败：`PairVerdict` 不存在）

- [ ] **Step 3: 实现 `PairVerdict` 与 `evaluate_pair`，`scan` 改为调用它**

```rust
/// 单对站的裁决策略（scan 与生命周期复核共用，防两处口径漂移）。
/// `NoMarket`＝"市场不在"（买站无卖盘 / 吃单无成交）——生命周期按缺席计；
/// `Dropped*`＝"有盘但被过滤否掉"——生命周期按失效计。
#[derive(Debug, Clone, PartialEq)]
pub enum PairVerdict {
    Passed { buy_price: f64, sell_price: f64, qty: u64,
             net_per_unit: f64, net_total: f64, margin_pct: f64 },
    DroppedBatch,
    DroppedShortfall,
    DroppedThreshold,
    NoMarket,
}

pub fn evaluate_pair(a: &StationOrderBook, b: &StationOrderBook, p: &FlipParams) -> PairVerdict {
    let broker = p.fees.effective_broker();
    let tax = p.fees.effective_sales_tax();
    let Some(best_ask) = a.best_ask.filter(|v| *v > 0.0) else { return PairVerdict::NoMarket };
    let budget_qty = ((p.capital_isk * p.capital_pct_per_trade / 100.0) / best_ask).floor();
    let want = (budget_qty.max(0.0) as u64)
        .min(a.capacity(Side::Buy))
        .min(b.capacity(Side::Sell));
    if want < p.min_batch { return PairVerdict::DroppedBatch; }
    let (Some((buy_px, filled_buy)), Some((sell_px, filled_sell))) =
        (a.executable(Side::Buy, want), b.executable(Side::Sell, want)) else { return PairVerdict::NoMarket };
    let qty = filled_buy.min(filled_sell);
    if qty < p.min_batch { return PairVerdict::DroppedShortfall; }
    let Some((net_per_unit, net_total, margin_pct)) = settle(buy_px, sell_px, qty, p, broker, tax)
        else { return PairVerdict::NoMarket };
    if margin_pct < p.margin_threshold_pct { return PairVerdict::DroppedThreshold; }
    PairVerdict::Passed { buy_price: buy_px, sell_price: sell_px, qty, net_per_unit, net_total, margin_pct }
}
```

`scan` 内层循环改为（统计口径逐位保持——`NoMarket` 不计数是既有行为）：

```rust
for a in group {
    for b in group {
        if a.location_id == b.location_id { continue; }
        out.stats.pairs_evaluated += 1;
        match evaluate_pair(a, b, p) {
            PairVerdict::Passed { buy_price, sell_price, qty, net_per_unit, net_total, margin_pct } => {
                let (v24, src) = match vol24.get(&type_id) {
                    Some(&v) if v > 0 => (v, VolSource::History),
                    _ => (qty, VolSource::Depth),
                };
                out.opportunities.push(Opportunity {
                    type_id, buy_loc: a.location_id, sell_loc: b.location_id,
                    buy_price, sell_price, qty, net_per_unit, net_total, margin_pct,
                    vol24: v24, vol_source: src,
                    buy_levels: a.ask_levels, sell_levels: b.bid_levels,
                });
            }
            PairVerdict::DroppedBatch => out.stats.dropped_batch += 1,
            PairVerdict::DroppedShortfall => out.stats.dropped_shortfall += 1,
            PairVerdict::DroppedThreshold => out.stats.dropped_threshold += 1,
            PairVerdict::NoMarket => {}
        }
    }
}
```
（`DroppedShortfall` 与既有分支同为防御性：正常数据下 want 由两侧容量裁剪、不会短填——保留变体与注释，不造伪测试。）

- [ ] **Step 4: 跑测试确认通过**（新 3 例 + 既有 15 例全绿）
- [ ] **Step 5: 提交** `git commit -m "refactor(core): flip 抽出 evaluate_pair（状态机失效判定与 scan 同源，统计口径逐位不变）"`

---

### Task 2: `market::lifecycle` — 纯状态机 + 通知闸门

**Files:**
- Create: `crates/emd-core/src/market/lifecycle.rs`
- Modify: `crates/emd-core/src/market/mod.rs`（`pub mod lifecycle;` + `pub use`）

- [ ] **Step 1: 写失败测试**（lifecycle.rs 底部 `#[cfg(test)] mod tests`，与实现同文件提交）

```rust
fn opp(margin: f64) -> Opportunity {
    Opportunity { type_id: 34, buy_loc: 60003760, sell_loc: 60008494,
        buy_price: 100.0, sell_price: 110.0, qty: 500,
        net_per_unit: margin, net_total: 600.0, margin_pct: margin,
        vol24: 100, vol_source: VolSource::Depth, buy_levels: 5, sell_levels: 5 }
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
    let o1 = opp(5.0); let r1 = tick(None, Verdict::Hit(&o1), 1000).unwrap();
    let mut r1 = r1; r1.state = OppState::Notified; r1.miss_streak = 1;
    let o2 = opp(7.5);
    let r2 = tick(Some(&r1), Verdict::Hit(&o2), 2000).unwrap();
    assert_eq!(r2.state, OppState::Notified, "命中不改状态");
    assert_eq!(r2.miss_streak, 0, "命中清零缺席计数");
    assert!((r2.best_margin_pct - 7.5).abs() < 1e-9);
}

#[test]
fn dropped_goes_invalidated_immediately_absent_needs_two_rounds() {
    let o = opp(5.0); let r = tick(None, Verdict::Hit(&o), 1000).unwrap();
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
```

- [ ] **Step 2: 跑测试确认失败**（`lifecycle` 模块不存在）

- [ ] **Step 3: 实现**（先实现纯部分；`update_round` 装配在 Task 6 补——本步只到 `mark_notified` 为止，`update_round` 先不写）

```rust
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
pub enum OppState { New, Notified, Expired, Invalidated }

impl OppState {
    pub fn as_str(self) -> &'static str {
        match self { Self::New => "new", Self::Notified => "notified",
                     Self::Expired => "expired", Self::Invalidated => "invalidated" }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s { "new" => Some(Self::New), "notified" => Some(Self::Notified),
                  "expired" => Some(Self::Expired), "invalidated" => Some(Self::Invalidated),
                  _ => None }
    }
    pub fn is_terminal(self) -> bool { matches!(self, Self::Expired | Self::Invalidated) }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OppRecord {
    pub type_id: u32, pub buy_loc: u64, pub sell_loc: u64,
    pub state: OppState,
    pub miss_streak: u32,
    pub first_seen_at: i64, pub last_seen_at: i64,
    pub best_margin_pct: f64, pub last_margin_pct: f64,
    pub last_net_total: f64, pub last_qty: u64,
    pub notified_at: Option<i64>,
    pub notified_day: Option<String>,
    pub notified_count_day: u32,
    pub last_notified_margin_pct: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Verdict<'a> {
    /// 本轮通过全部过滤（scan 存活机会）。
    Hit(&'a Opportunity),
    /// 双侧有盘但被过滤否掉（evaluate_pair 的 Dropped*）。
    Dropped,
    /// 双侧至少一侧无盘。
    Absent,
}

pub fn tick(prev: Option<&OppRecord>, v: Verdict<'_>, now: i64) -> Option<OppRecord> {
    match v {
        Verdict::Hit(o) => Some(match prev {
            Some(r) if !r.state.is_terminal() => OppRecord {
                state: r.state, miss_streak: 0, last_seen_at: now,
                best_margin_pct: r.best_margin_pct.max(o.margin_pct),
                last_margin_pct: o.margin_pct, last_net_total: o.net_total, last_qty: o.qty,
                ..r.clone()
            },
            _ => OppRecord {
                type_id: o.type_id, buy_loc: o.buy_loc, sell_loc: o.sell_loc,
                state: OppState::New, miss_streak: 0,
                first_seen_at: now, last_seen_at: now,
                best_margin_pct: o.margin_pct, last_margin_pct: o.margin_pct,
                last_net_total: o.net_total, last_qty: o.qty,
                // 通知历史跨生命周期保留（否则"消失→复现"可绕过冷却与日上限）
                notified_at: prev.and_then(|r| r.notified_at),
                notified_day: prev.and_then(|r| r.notified_day.clone()),
                notified_count_day: prev.map(|r| r.notified_count_day).unwrap_or(0),
                last_notified_margin_pct: prev.and_then(|r| r.last_notified_margin_pct),
            },
        }),
        Verdict::Dropped => prev.and_then(|r| (!r.state.is_terminal()).then(|| OppRecord {
            state: OppState::Invalidated, ..r.clone()
        })),
        Verdict::Absent => prev.and_then(|r| {
            if r.state.is_terminal() { return None; }
            let miss = r.miss_streak + 1;
            Some(OppRecord { miss_streak: miss,
                state: if miss >= MISS_LIMIT { OppState::Expired } else { r.state },
                ..r.clone() })
        }),
    }
}

/// 通知闸门（v3.1 §4.2）：首发可推；4h 冷却；净利率较上次通知 +≥2pp 穿透；
/// 每自然日至多 2 条（硬闸，穿透不豁免）。true → 调用方发送后必须 mark_notified。
pub fn can_notify(rec: &OppRecord, now: i64, today: &str) -> bool {
    let count_today = if rec.notified_day.as_deref() == Some(today) { rec.notified_count_day } else { 0 };
    if count_today >= NOTIFY_DAILY_CAP { return false; }
    let Some(last) = rec.notified_at else { return true };
    let cooldown_passed = now - last >= NOTIFY_COOLDOWN_SECS;
    let margin_jump = rec.last_notified_margin_pct
        .map(|m| rec.last_margin_pct - m >= NOTIFY_MARGIN_BYPASS_PP)
        .unwrap_or(false);
    cooldown_passed || margin_jump
}

pub fn mark_notified(rec: &mut OppRecord, now: i64, today: &str) {
    rec.state = OppState::Notified;
    rec.notified_at = Some(now);
    rec.notified_count_day = if rec.notified_day.as_deref() == Some(today) {
        rec.notified_count_day + 1
    } else { 1 };
    rec.notified_day = Some(today.to_string());
    rec.last_notified_margin_pct = Some(rec.last_margin_pct);
}
```

- [ ] **Step 4: 跑测试确认通过**（7 例全绿）
- [ ] **Step 5: 提交** `git commit -m "feat(core): 机会生命周期纯状态机（new/notified/expired/invalidated + 4h 冷却与日上限闸门）"`

---

### Task 3: `schema.rs` — 迁移 v5（opportunities / xregion_books / xregion_log）

**Files:** Modify: `crates/emd-core/src/store/schema.rs`

- [ ] **Step 1: 写失败测试**（追加；并把 `versions_are_unique_and_ascending` 的 `assert_eq!(MIGRATIONS.len(), 4)` 改为 `5`）

```rust
#[test]
fn migration_v5_keys_opportunities_by_the_three_dimensions() {
    let sql = MIGRATIONS[4].2;
    let o = sql.split("CREATE TABLE opportunities").nth(1).unwrap()
        .split("CREATE INDEX").next().unwrap();
    assert!(o.contains("PRIMARY KEY (type_id, buy_loc, sell_loc)"),
        "opportunity_key 即三维复合键，不用哈希字符串");
    assert!(o.contains("notified_day"), "日上限需要自然日字段");
}

#[test]
fn xregion_books_is_bounded_and_age_stamped() {
    let sql = MIGRATIONS[4].2;
    let x = sql.split("CREATE TABLE xregion_books").nth(1).unwrap()
        .split("CREATE INDEX").next().unwrap();
    assert!(x.contains("PRIMARY KEY (location_id, type_id)"));
    assert!(x.contains("fetched_at"), "读取端要按年龄闸门过滤");
}
```

- [ ] **Step 2: 跑测试确认失败**（MIGRATIONS 仍 4 条）

- [ ] **Step 3: 实现 v5 迁移**（追加到 `MIGRATIONS` 数组尾部；同时把 `bounded_tables_never_key_on_a_timestamp` 的列表扩为 `["station_orders", "hub_pool", "xregion_books", "opportunities"]`——新表同样不允许时间戳进主键）

```sql
(
    5,
    "M4b：跨区单簿（T1.5）、机会生命周期与 T1.5 台账",
    r#"
-- 机会生命周期（v3.1 §4.2）。主键即 opportunity_key：三个维度本身稳定、可读、
-- 可查，哈希字符串只会给排障添堵。
CREATE TABLE opportunities (
    type_id                 INTEGER NOT NULL,
    buy_loc                 INTEGER NOT NULL,
    sell_loc                INTEGER NOT NULL,
    state                   TEXT    NOT NULL,           -- new/notified/expired/invalidated
    miss_streak             INTEGER NOT NULL DEFAULT 0,
    first_seen_at           INTEGER NOT NULL,
    last_seen_at            INTEGER NOT NULL,
    best_margin_pct         REAL    NOT NULL DEFAULT 0,
    last_margin_pct         REAL    NOT NULL DEFAULT 0,
    last_net_total          REAL    NOT NULL DEFAULT 0,
    last_qty                INTEGER NOT NULL DEFAULT 0,
    notified_at             INTEGER,
    notified_day            TEXT,
    notified_count_day      INTEGER NOT NULL DEFAULT 0,
    last_notified_margin_pct REAL,
    PRIMARY KEY (type_id, buy_loc, sell_loc)
);
CREATE INDEX ix_opportunities_state ON opportunities (state, last_seen_at DESC);

-- T1.5 跨区单簿：按 (站, 类型) UPSERT，只更新本批取到的类型；未取到的旧行
-- 靠读取端 45 min 年龄闸门兜底、24h 剪除——表因此有界（3 枢纽 × ≤200 类型）。
CREATE TABLE xregion_books (
    location_id       INTEGER NOT NULL,
    type_id           INTEGER NOT NULL,
    region_id         INTEGER NOT NULL,
    is_npc            INTEGER NOT NULL,
    best_bid          REAL,
    bid_qty           INTEGER NOT NULL DEFAULT 0,
    best_ask          REAL,
    ask_qty           INTEGER NOT NULL DEFAULT 0,
    bid_levels        INTEGER NOT NULL DEFAULT 0,
    ask_levels        INTEGER NOT NULL DEFAULT 0,
    bid_depth         TEXT NOT NULL DEFAULT '[]',
    ask_depth         TEXT NOT NULL DEFAULT '[]',
    skipped_stale     INTEGER NOT NULL DEFAULT 0,
    skipped_thin      INTEGER NOT NULL DEFAULT 0,
    skipped_wholesale INTEGER NOT NULL DEFAULT 0,
    fetched_at        INTEGER NOT NULL,
    PRIMARY KEY (location_id, type_id)
);
CREATE INDEX ix_xregion_books_type ON xregion_books (type_id);

-- 每趟 T1.5 台账（与 round_log 同纪律：只追加台账，不落行级数据）。
CREATE TABLE xregion_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    INTEGER NOT NULL,
    types         INTEGER NOT NULL,
    regions       INTEGER NOT NULL,
    requests      INTEGER NOT NULL,
    orders        INTEGER NOT NULL,
    books_written INTEGER NOT NULL,
    failed        INTEGER NOT NULL,
    seconds       REAL    NOT NULL,
    status        TEXT    NOT NULL
);
CREATE INDEX ix_xregion_log_started ON xregion_log (started_at DESC);
"#,
),
```

- [ ] **Step 4: 跑测试确认通过**（`cargo test -p emd-core schema`）
- [ ] **Step 5: 提交** `git commit -m "feat(core): 迁移 v5——opportunities 生命周期表 + xregion_books 跨区单簿 + xregion_log"`

---

### Task 4: `db.rs` — 生命周期与跨区单簿读写层

**Files:** Modify: `crates/emd-core/src/store/db.rs`、`crates/emd-core/src/store/mod.rs`（补导出 `XRegionLog`、`OppRecord` 相关不需要——`OppRecord` 属 market）

- [ ] **Step 1: 写失败测试**（db.rs 测试模块追加）

```rust
#[test]
fn opps_roundtrip_and_terminal_prune() {
    use emd_core::market::lifecycle::{OppRecord, OppState};
    let db = Db::in_memory().unwrap();
    let r = OppRecord { type_id: 34, buy_loc: 60003760, sell_loc: 60008494,
        state: OppState::New, miss_streak: 0, first_seen_at: 100, last_seen_at: 200,
        best_margin_pct: 5.0, last_margin_pct: 4.0, last_net_total: 100.0, last_qty: 10,
        notified_at: None, notified_day: None, notified_count_day: 0, last_notified_margin_pct: None };
    db.save_opp(&r).unwrap();
    assert_eq!(db.load_opps().unwrap(), vec![r.clone()]);
    // 剪除只碰终态且久未见的行
    db.save_opp(&OppRecord { state: OppState::Expired, ..r.clone() }).unwrap();
    assert_eq!(db.prune_opps_terminal(150).unwrap(), 0, "last_seen=200 未过期");
    assert_eq!(db.prune_opps_terminal(250).unwrap(), 1, "终态且 last_seen<250 → 剪");
    assert_eq!(db.load_opps().unwrap().len(), 0);
}

#[test]
fn xregion_write_is_per_type_replace_and_age_filtered() {
    use emd_core::market::StationOrderBook;
    let db = Db::in_memory().unwrap();
    let mk = |ty: u32| StationOrderBook { location_id: 60008494, type_id: ty, is_npc_station: true,
        best_bid: Some(3.0), bid_qty: 10, best_ask: Some(4.0), ask_qty: 10,
        bid_levels: 5, ask_levels: 5,
        bid_depth: vec![], ask_depth: vec![], skipped_stale: 0, skipped_thin: 0, skipped_wholesale: 0 };
    // 第一批：类型 34/35 各一本
    db.write_xregion_books(60008494, 10000043, &[34, 35], &[mk(34), mk(35)]).unwrap();
    assert_eq!(db.load_xregion_books(0).unwrap().len(), 2);
    // 第二批只取 34：34 换新、35 旧行保留（跨批语义）
    db.write_xregion_books(60008494, 10000043, &[34], &[mk(34)]).unwrap();
    assert_eq!(db.load_xregion_books(0).unwrap().len(), 2);
    // 第三批取 34 但聚合为空（薄档）：34 的旧行必须被删
    db.write_xregion_books(60008494, 10000043, &[34], &[]).unwrap();
    let rest = db.load_xregion_books(0).unwrap();
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].type_id, 35);
    // 年龄闸门：cutoff 高于写入时刻 → 全部不可见
    let cutoff = emd_core::store::now_unix() + 10;
    assert_eq!(db.load_xregion_books(cutoff).unwrap().len(), 0);
}

#[test]
fn flip_hubs_unions_hub_pool_with_xregion_locations() {
    use emd_core::market::Hub;
    let db = Db::in_memory().unwrap();
    db.write_hub_pool(&[Hub { location_id: 60003760, order_count: 100, share_pct: 80.0, rank: 1 }]).unwrap();
    let mk = |ty: u32| StationOrderBook { location_id: 60008494, type_id: ty, is_npc_station: true,
        best_bid: Some(3.0), bid_qty: 10, best_ask: Some(4.0), ask_qty: 10,
        bid_levels: 5, ask_levels: 5,
        bid_depth: vec![], ask_depth: vec![], skipped_stale: 0, skipped_thin: 0, skipped_wholesale: 0 };
    db.write_xregion_books(60008494, 10000043, &[34], &[mk(34)]).unwrap();
    let hubs = db.flip_hubs().unwrap();
    assert_eq!(hubs.len(), 2, "常规枢纽 + 跨区站在有实际行时并入");
    assert!(hubs.iter().any(|h| h.location_id == 60003760));
    assert!(hubs.iter().any(|h| h.location_id == 60008494));
    // 跨区行被整批替换删光后，该站不再出现在枢纽集里
    db.write_xregion_books(60008494, 10000043, &[34], &[]).unwrap();
    assert_eq!(db.flip_hubs().unwrap().len(), 1);
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**（开始前先在 `market/mod.rs` 补两个常量——它们被本 Task 的 `flip_hubs` 与 Task 7 引用，必须先就位；`XREGION_TARGETS` 与 fetch 留 Task 5）

```rust
/// 跨区单簿读取年龄闸门：45 min ≈ 3–4 个 T1.5 周期。
pub const XREGION_MAX_AGE_SECS: i64 = 45 * 60;
/// 跨区旧行剪除线（表有界的最后一道保险）。
pub const XREGION_PRUNE_SECS: i64 = 24 * 3600;
```

（新增方法，均放 M4a 段之后新开 `// ---- M4b：生命周期与跨区单簿 ----` 段）

```rust
pub fn save_opp(&self, r: &crate::market::lifecycle::OppRecord) -> Result<()> {
    self.conn.execute(
        "INSERT INTO opportunities (
            type_id, buy_loc, sell_loc, state, miss_streak, first_seen_at, last_seen_at,
            best_margin_pct, last_margin_pct, last_net_total, last_qty,
            notified_at, notified_day, notified_count_day, last_notified_margin_pct)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)
         ON CONFLICT(type_id, buy_loc, sell_loc) DO UPDATE SET
           state=excluded.state, miss_streak=excluded.miss_streak,
           last_seen_at=excluded.last_seen_at,
           best_margin_pct=excluded.best_margin_pct, last_margin_pct=excluded.last_margin_pct,
           last_net_total=excluded.last_net_total, last_qty=excluded.last_qty,
           notified_at=excluded.notified_at, notified_day=excluded.notified_day,
           notified_count_day=excluded.notified_count_day,
           last_notified_margin_pct=excluded.last_notified_margin_pct",
        params![r.type_id, r.buy_loc as i64, r.sell_loc as i64, r.state.as_str(),
            r.miss_streak, r.first_seen_at, r.last_seen_at,
            r.best_margin_pct, r.last_margin_pct, r.last_net_total, r.last_qty as i64,
            r.notified_at, r.notified_day, r.notified_count_day, r.last_notified_margin_pct],
    )?;
    Ok(())
}

pub fn load_opps(&self) -> Result<Vec<crate::market::lifecycle::OppRecord>> { /* SELECT 全表，state 用 OppState::parse，未知状态跳过并 warn */ }

pub fn prune_opps_terminal(&self, before_ts: i64) -> Result<usize> {
    Ok(self.conn.execute(
        "DELETE FROM opportunities WHERE state IN ('expired','invalidated') AND last_seen_at < ?1",
        params![before_ts],
    )?)
}

/// 本批 (hub, fetched_types) 的整批替换：先删这些 (站,类型) 的旧行再插新书。
/// 取到了但聚合为空（薄档）= 旧行必须删——否则陈旧盘口会继续参与配对。
pub fn write_xregion_books(&self, hub: u64, region: u32, fetched_types: &[u32],
    books: &[StationOrderBook]) -> Result<usize> {
    let ts = now_unix();
    let tx = self.conn.unchecked_transaction()?;
    {
        let mut del = tx.prepare_cached(
            "DELETE FROM xregion_books WHERE location_id = ?1 AND type_id = ?2")?;
        for &t in fetched_types { del.execute(params![hub as i64, t])?; }
    }
    {
        let mut ins = tx.prepare_cached(
            "INSERT INTO xregion_books (location_id, type_id, region_id, is_npc,
                best_bid, bid_qty, best_ask, ask_qty, bid_levels, ask_levels,
                bid_depth, ask_depth, skipped_stale, skipped_thin, skipped_wholesale, fetched_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)")?;
        for b in books {
            ins.execute(params![b.location_id as i64, b.type_id, region as i64,
                b.is_npc_station as i64, b.best_bid, b.bid_qty as i64, b.best_ask, b.ask_qty as i64,
                b.bid_levels, b.ask_levels,
                serde_json::to_string(&b.bid_depth).unwrap_or_else(|_| "[]".into()),
                serde_json::to_string(&b.ask_depth).unwrap_or_else(|_| "[]".into()),
                b.skipped_stale, b.skipped_thin, b.skipped_wholesale, ts])?;
        }
    }
    tx.commit()?;
    Ok(books.len())
}

/// 年龄闸门在读取端：cutoff 之前的行视为过期数据，不参与配对（诚实优先）。
pub fn load_xregion_books(&self, cutoff_ts: i64) -> Result<Vec<StationOrderBook>> {
    // SELECT ... FROM xregion_books WHERE fetched_at >= ?1（列与 load_books 同反序列化）
}

/// 供 UI/daemon 给跨区行标注数据年龄：站 → 最近一次抓取时刻。
pub fn xregion_ages(&self) -> Result<std::collections::HashMap<u64, i64>> { /* GROUP BY location_id, MAX(fetched_at) */ }

pub fn prune_xregion(&self, before_ts: i64) -> Result<usize> {
    Ok(self.conn.execute("DELETE FROM xregion_books WHERE fetched_at < ?1", params![before_ts])?)
}

/// flip 的枢纽集 = 常规枢纽池 ∪ 跨区枢纽站（以 xregion_books 实际有行为准）。
/// scan 只取 location_id 集合；order_count 是部分和、share_pct=0 是诚实占位。
pub fn flip_hubs(&self) -> Result<Vec<crate::market::Hub>> {
    let mut hubs = self.hub_pool()?;
    let cutoff = now_unix() - crate::market::XREGION_MAX_AGE_SECS;
    let mut stmt = self.conn.prepare(
        "SELECT location_id, SUM(bid_levels + ask_levels) FROM xregion_books
         WHERE fetched_at >= ?1 GROUP BY location_id ORDER BY location_id")?;
    let fresh: Vec<(u64, u64)> = stmt.query_map(params![cutoff], |r| Ok((
        r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)))
        .collect::<std::result::Result<_, _>>()?;
    for (i, (location_id, order_count)) in fresh.into_iter().enumerate() {
        if hubs.iter().any(|h| h.location_id == location_id) { continue; }
        hubs.push(crate::market::Hub { location_id, order_count,
            share_pct: 0.0, rank: hubs.len() + 1 });
    }
    Ok(hubs)
}

pub fn record_xregion_log(&self, l: &XRegionLog) -> Result<()> { /* INSERT */ }
pub fn last_xregion_log(&self) -> Result<Option<XRegionLog>> { /* 给 daemon stats/验收用 */ }
```

并把 `load_books()` 改为并上跨区单簿（注释同步）：

```rust
/// 当前快照的全部单簿（Forge 全量 ∪ T1.5 跨区窗口内行）——flip 扫描的输入装配。
/// 跨区行按 `XREGION_MAX_AGE_SECS` 年龄闸门过滤：上批数据可用，但过期数据
/// 必须消失，否则会拿 45 分钟前的价格继续配对（诚实口径）。
pub fn load_books(&self) -> Result<Vec<StationOrderBook>> {
    let mut books = self.load_station_books()?;          // 原 load_books 内容改名
    books.extend(self.load_xregion_books(now_unix() - crate::market::XREGION_MAX_AGE_SECS)?);
    Ok(books)
}
```

`XRegionLog` 结构体（db.rs 顶部与 `RoundRecord` 同区）：`started_at/types/regions/requests/orders/books_written/failed/seconds/status`；`store/mod.rs` 补 `pub use db::{..., XRegionLog};`

- [ ] **Step 4: 跑测试确认通过**（含既有 store 测试全绿——`load_books` 改名后注意调用点在本 crate 内）
- [ ] **Step 5: 提交** `git commit -m "feat(core): 生命周期与跨区单簿读写层（年龄闸门/整批替换/flip_hubs 并集）"`

---

### Task 5: `orderbook.rs` + `market/mod.rs` — 单类型定向拉取与常量

**Files:**
- Modify: `crates/emd-core/src/market/orderbook.rs`
- Modify: `crates/emd-core/src/market/mod.rs`

- [ ] **Step 1: 写失败测试**（orderbook.rs 测试模块追加）

```rust
#[test]
fn type_page_path_shape() {
    assert_eq!(type_page_path(10000043, 34, 2),
        "/v3/markets/10000043/orders?type_id=34&order_type=all&page=2");
}

#[test]
fn page_cap_is_five() {
    assert_eq!(capped_pages(1), 1);
    assert_eq!(capped_pages(3), 3);
    assert_eq!(capped_pages(40), TYPE_PAGE_CAP, "单类型单星域 >5 页截断并记 truncated");
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**

```rust
/// T1.5 定向拉取的页数上限（防呆）：实测单类型单星域 ≈1 页（148 条），
/// 5 页 = 5000 条是几乎不可能的尾部；超过就截断并把 truncated 计进台账。
const TYPE_PAGE_CAP: u32 = 5;

pub fn type_page_path(region_id: u32, type_id: u32, page: u32) -> String {
    format!("/v3/markets/{region_id}/orders?type_id={type_id}&order_type=all&page={page}")
}

pub fn capped_pages(x_pages: u32) -> u32 { x_pages.clamp(1, TYPE_PAGE_CAP) }

#[derive(Debug, Clone)]
pub struct TypeFetch {
    pub region_id: u32,
    pub type_id: u32,
    pub orders: Vec<Order>,
    pub pages: u32,
    pub truncated: bool,
}

/// 拉取单个 (region, type) 的全部页（通常 1 页）。任一页失败即整型失败——
/// 缺页的半本盘口会低估深度，宁可让该类型本轮不参与配对（下批自然重试）。
pub async fn fetch_type_orders(client: &EsiClient, region_id: u32, type_id: u32)
    -> Result<TypeFetch> {
    let first = client.fetch(&type_page_path(region_id, type_id, 1)).await?;
    let x_pages = resolve_pages(&first)?;
    let snapshot_lm = first.meta().last_modified.clone();
    let pages = capped_pages(x_pages);
    let mut acc = decode(client, first)?.orders;
    for page in 2..=pages {
        let f = client.fetch(&type_page_path(region_id, type_id, page)).await?;
        let d = decode(client, f)?;
        // 与全量拉取同一条纪律：跨页 Last-Modified 不一致 = 快照不自洽。
        check_drift(page, &d, snapshot_lm.as_deref())?;
        acc.extend(d.orders);
    }
    Ok(TypeFetch { region_id, type_id, orders: acc, pages, truncated: x_pages > TYPE_PAGE_CAP })
}
```

`market/mod.rs` 追加常量与模块、导出：

```rust
pub mod lifecycle;

/// T1.5 三个目标枢纽（方案 v3.1 §4.1；星域即 HUB_REGIONS 后三项）。
/// 枢纽站 ID 为公开稳定值；真机验收以"三站各 >0 本"校验 ID 未手滑。
pub const XREGION_TARGETS: [(u32, u64); 3] = [
    (10000043, 60008494), // Domain — Amarr VIII (Oris) - Emperor Family Academy
    (10000042, 60005686), // Metropolis — Hek VIII - Moon 12 - Boundless Creation Factory
    (10000030, 60004588), // Heimatar — Rens VI - Moon 8 - Brutor Tribe Treasury
];
/// 跨区单簿读取年龄闸门：45 min ≈ 3–4 个 T1.5 周期。
pub const XREGION_MAX_AGE_SECS: i64 = 45 * 60;
/// 跨区旧行剪除线（表有界的最后一道保险）。
pub const XREGION_PRUNE_SECS: i64 = 24 * 3600;

pub use flip::{..., PairVerdict};   // 追加
pub use orderbook::{fetch_region_orders, fetch_type_orders, RoundOutcome, TypeFetch}; // 追加
```

- [ ] **Step 4: 跑测试确认通过**
- [ ] **Step 5: 提交** `git commit -m "feat(core): T1.5 单类型定向拉取 + 跨区枢纽常量与年龄闸门参数"`

---

### Task 6: `lifecycle::update_round` — 每轮装配与落库

**Files:** Modify: `crates/emd-core/src/market/lifecycle.rs`

- [ ] **Step 1: 写失败测试**（lifecycle.rs 测试模块追加，用 in-memory Db 的集成测试）

```rust
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
    let mut p = FlipParams::default(); p.margin_threshold_pct = 99.0;
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
    assert!(update_round(&db, 1000).unwrap().is_none(), "无快照 → 静默跳过");
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现装配层**（与纯逻辑同文件，段落注释写清"这部分读 DB，是 history::backfill 同先例的调用层"）

```rust
use std::collections::{HashMap, HashSet};
use crate::error::Result;
use crate::market::flip::{self, FlipParams};
use crate::market::{Opportunity, PairVerdict, StationOrderBook};
use crate::store::Db;

/// 每轮机会生命周期的结算计数（daemon/tracing 观测用）。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TickStats {
    pub active: usize, pub new: usize, pub revived: usize,
    pub invalidated: usize, pub expired: usize, pub saved: usize,
}

/// 终态行的保留期：90 天后剪除（历史使命结束，表必须有界）。
pub const OPP_KEEP_SECS: i64 = 90 * 24 * 3600;

fn key_of(r: &OppRecord) -> (u32, u64, u64) { (r.type_id, r.buy_loc, r.sell_loc) }

/// 一轮生命周期结算（v3.1 §4.2）。装配：读快照（含跨区窗口）→ scan →
/// 活跃键逐键复核（有盘被否=invalidated；缺盘=缺席计数）→ 终态仅命中复活 → 落库。
/// `Ok(None)` = 本地无任何快照（还没跑过采集），调用方静默跳过。
pub fn update_round(db: &Db, now: i64) -> Result<Option<TickStats>> {
    if db.counts()?.rows == 0 { return Ok(None); }
    let books = db.load_books()?;
    let hubs = db.flip_hubs()?;
    let vol = db.latest_vol24()?;
    let params = db.get_flip_params()?;
    let out = flip::scan(&books, &hubs, &params, &vol);

    let passed: HashMap<(u32, u64, u64), &Opportunity> = out
        .opportunities.iter().map(|o| ((o.type_id, o.buy_loc, o.sell_loc), o)).collect();
    let book_idx: HashMap<(u64, u32), &StationOrderBook> = books
        .iter().map(|b| ((b.location_id, b.type_id), b)).collect();
    let rows: HashMap<(u32, u64, u64), OppRecord> = db
        .load_opps()?.into_iter().map(|r| (key_of(&r), r)).collect();

    // 观察面 = 活跃键 ∪ 本轮命中键（终态只有命中才复活）。
    let mut keys: HashSet<(u32, u64, u64)> = rows
        .values().filter(|r| !r.state.is_terminal()).map(key_of).collect();
    keys.extend(passed.keys().copied());

    let mut stats = TickStats { active: rows.values().filter(|r| !r.state.is_terminal()).count(), ..Default::default() };
    for key in keys {
        let prev = rows.get(&key);
        let verdict = match passed.get(&key) {
            Some(o) => Verdict::Hit(o),
            None => match (book_idx.get(&(key.1, key.0)), book_idx.get(&(key.2, key.0))) {
                (Some(a), Some(b)) => match flip::evaluate_pair(a, b, &params) {
                    PairVerdict::DroppedBatch | PairVerdict::DroppedShortfall | PairVerdict::DroppedThreshold
                        => Verdict::Dropped,
                    _ => Verdict::Absent, // NoMarket：市场不在 = 缺席
                },
                _ => Verdict::Absent,
            },
        };
        let Some(next) = tick(prev, verdict, now) else { continue; };
        if Some(&next) == prev { continue; }
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
    Ok(Some(stats))
}
```

- [ ] **Step 4: 跑测试确认通过**（3 例 + Task 2 的 7 例）
- [ ] **Step 5: 提交** `git commit -m "feat(core): 生命周期每轮结算（活跃键复核/终态复活/90 天剪除，装配层同 history 先例）"`

---

### Task 7: `scheduler.rs` — T1.5 配置、执行与双钩子

**Files:** Modify: `crates/emd-core/src/scheduler.rs`

- [ ] **Step 1: 写失败测试**（scheduler.rs 测试模块追加）

```rust
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
    let o = |t: u32| Opportunity { type_id: t, buy_loc: 1, sell_loc: 2,
        buy_price: 1.0, sell_price: 2.0, qty: 1, net_per_unit: 1.0, net_total: 1.0,
        margin_pct: 1.0, vol24: 1, vol_source: crate::market::VolSource::Depth,
        buy_levels: 1, sell_levels: 1 };
    let list = vec![o(34), o(34), o(35), o(36), o(35)];
    assert_eq!(candidates_from(&list, 2), vec![34, 35]);
    assert_eq!(candidates_from(&list, 10), vec![34, 35, 36]);
}

#[test]
fn xregion_defaults_match_the_plan() {
    let c = XRegionConfig::default();
    assert!(c.enabled, "方案 v3.1 口径：默认启用，EMD_XREGION=0 关闭");
    assert_eq!(c.candidate_top, 200);
    assert_eq!(c.targets, crate::market::XREGION_TARGETS);
    assert_eq!(c.max_age_secs, crate::market::XREGION_MAX_AGE_SECS);
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 实现**（配置 + 纯函数 + `run_t1_5` + `run()` 钩子）

```rust
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
        Self { enabled: true, candidate_top: 200,
               targets: market::XREGION_TARGETS, max_age_secs: market::XREGION_MAX_AGE_SECS }
    }
}

impl XRegionConfig {
    /// 运行期开关：`EMD_XREGION=0` 关闭（默认值刻意不走 env，测试不被环境左右）。
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(v) = std::env::var("EMD_XREGION") { c.enabled = v != "0"; }
        c
    }
}

/// 隔轮执行：轮号偶数（T1 6 min 节拍下 ≈ 每 12 分钟）。
pub fn t1_5_due(round: u64) -> bool { round % 2 == 0 }

/// 错峰：11:10–11:35 UTC 暂停 T1.5（v3.1 §3.1，给 T2 独占窗口留位）。
pub fn xregion_blackout(t: chrono::DateTime<chrono::Utc>) -> bool {
    use chrono::Timelike;
    let m = t.hour() * 60 + t.minute();
    (11 * 60 + 10..=11 * 60 + 35).contains(&m)
}

/// 候选类型 = 扫描结果按既有排序取前 N 个去重 type_id（保持分数序，天然截断）。 
pub fn candidates_from(opps: &[market::Opportunity], top: usize) -> Vec<u32> {
    let mut seen = std::collections::HashSet::new();
    opps.iter().filter(|o| seen.insert(o.type_id)).map(|o| o.type_id).take(top).collect()
}

/// T1.5 一趟的结果（日志 + daemon xregion 展示）。
#[derive(Debug, Clone, Copy, Default)]
pub struct XRegionReport {
    pub types_requested: usize, pub types_ok: usize, pub types_failed: usize,
    pub requests: u32, pub orders: u64, pub books_written: usize, pub seconds: f64,
}

impl Scheduler {
    /// 跑一趟 T1.5：上一轮命中的 Top N 候选 × 三枢纽定向补拉 → 聚合 → 落 xregion_books。
    /// `Ok(None)` = 无候选（0 机会的快照，或没跑过采集）——不是错误。
    pub async fn run_t1_5(&self) -> Result<Option<XRegionReport>> {
        let books = self.db.load_books()?;
        let hubs = self.db.flip_hubs()?;
        let vol = self.db.latest_vol24()?;
        let params = self.db.get_flip_params()?;
        let out = market::scan(&books, &hubs, &params, &vol);
        let cand = candidates_from(&out.opportunities, self.cfg.xregion.candidate_top);
        if cand.is_empty() { return Ok(None); }

        let started_at = chrono::Utc::now().timestamp();
        let t0 = Instant::now();
        // 全笛卡尔积并发（约 600 请求，实测墙钟 136–226 s @并发16）。
        let jobs: Vec<(u32, u64, u32)> = self.cfg.xregion.targets.iter()
            .flat_map(|&(r, h)| cand.iter().map(move |&t| (r, h, t))).collect();
        let results: Vec<(u32, u64, u32, Result<market::TypeFetch>)> = futures::stream::iter(jobs)
            .map(|(r, h, t)| async move {
                (r, h, t, market::fetch_type_orders(&self.client, r, t).await)
            })
            .buffer_unordered(self.client.config().concurrency)
            .collect().await;

        let mut rep = XRegionReport { types_requested: cand.len(), ..Default::default() };
        for &(region, hub) in &self.cfg.xregion.targets {
            let mut fetched: Vec<u32> = Vec::new();
            let mut hub_books: Vec<market::StationOrderBook> = Vec::new();
            for (r, h, t, res) in &results {
                if *r != region || *h != hub { continue; }
                match res {
                    Ok(f) => {
                        rep.types_ok += 1;
                        rep.requests += f.pages;
                        rep.orders += f.orders.len() as u64;
                        fetched.push(*t);
                        hub_books.extend(market::aggregate(&f.orders, &market::aggregate_opts())
                            .into_iter().filter(|b| b.location_id == hub));
                    }
                    Err(e) => { rep.types_failed += 1; tracing::debug!("T1.5 {region}/{t} 失败：{e}"); }
                }
            }
            if fetched.is_empty() { continue; }
            rep.books_written += self.db.write_xregion_books(hub, region, &fetched, &hub_books)?;
            self.db.remember_station(hub, true)?;
            // 配错枢纽 ID 的显式兜底：整站零本就必须喊出来，别让 T1.5 静默空转。
            if hub_books.is_empty() {
                tracing::error!("T1.5 枢纽 {hub}（region {region}）本批 0 本单簿——疑似站 ID 与真实枢纽不符");
            }
        }
        let pruned = self.db.prune_xregion(started_at - market::XREGION_PRUNE_SECS)?;
        rep.seconds = t0.elapsed().as_secs_f64();
        self.db.record_xregion_log(&crate::store::XRegionLog {
            started_at, types: rep.types_requested as i64,
            regions: self.cfg.xregion.targets.len() as i64,
            requests: rep.requests as i64, orders: rep.orders as i64,
            books_written: rep.books_written as i64, failed: rep.types_failed as i64,
            seconds: rep.seconds,
            status: if rep.types_failed == 0 { "ok".into() }
                    else { format!("partial: {} 类型失败", rep.types_failed) },
        })?;
        tracing::info!(
            "T1.5：候选 {} 类型 × {} 枢纽 → {} 页 / {} 单 / {} 本跨区单簿（失败 {}，剪除旧行 {}），耗时 {:.1}s",
            rep.types_requested, self.cfg.xregion.targets.len(),
            rep.requests, rep.orders, rep.books_written, rep.types_failed, pruned, rep.seconds);
        Ok(Some(rep))
    }
}
```

`SchedulerConfig` 增字段 `pub xregion: XRegionConfig`（Default 补 `xregion: XRegionConfig::default()`；`config_defaults_match_the_plan` 测试同步补断言）。`scheduler.rs` 顶部补 `use futures::stream::{self, StreamExt};`（`buffer_unordered` 需要 trait 在作用域）。`run()` 循环在 `run_round` 成功之后、`maybe_run_history` 之前挂两个钩子：

```rust
// T1.5：隔轮、错峰窗口外、且本轮快照可用时才跑；失败只告警不打断主循环
// （跨区数据缺一批 = 少一些候选，不是错误）。
if self.cfg.xregion.enabled && t1_5_due(round) && outcome.is_ok()
    && !xregion_blackout(chrono::Utc::now()) {
    match self.run_t1_5().await {
        Ok(Some(rep)) => tracing::info!("T1.5 完成：{} 本跨区单簿", rep.books_written),
        Ok(None) => {}
        Err(e) => tracing::warn!("T1.5 未完成：{e}"),
    }
}
// 生命周期：每轮结算（缺席以轮为尺，v3.1「连续 2 轮」）；轮失败不结算——
// 数据不可用 ≠ 机会消失，拿旧盘口重新记账会伪造「仍在命中」。
if outcome.is_ok() {
    match market::lifecycle::update_round(&self.db, chrono::Utc::now().timestamp()) {
        Ok(Some(s)) => tracing::info!(
            "机会生命周期：活跃 {}｜新 {}｜复活 {}｜失效 {}｜过期 {}（写 {}）",
            s.active, s.new, s.revived, s.invalidated, s.expired, s.saved),
        Ok(None) => {}
        Err(e) => tracing::warn!("机会生命周期结算失败：{e}"),
    }
}
```

- [ ] **Step 4: 跑测试确认通过**（4 例 + 全模块）
- [ ] **Step 5: 提交** `git commit -m "feat(core): T1.5 跨区补拉（隔轮/错峰/并发矩阵/台账）+ 每轮生命周期结算钩子"`

---

### Task 8: daemon — `xregion` / `opps` 子命令 + flip 跨区角标

**Files:** Modify: `crates/emd-daemon/src/main.rs`

- [ ] **Step 1: 写失败测试**（沿用既有 parse 测试风格）

```rust
#[test]
fn parses_xregion_and_opps() {
    let a = parse_from(["xregion"].into_iter().map(String::from)).unwrap();
    assert_eq!(a.command, Command::XRegion);
    let b = parse_from(["opps", "--update"].into_iter().map(String::from)).unwrap();
    assert_eq!(b.command, Command::Opps);
    assert!(b.do_update, "--update 打开生命周期结算");
    let c = parse_from(["opps", "--state", "expired"].into_iter().map(String::from)).unwrap();
    assert_eq!(c.state.as_deref(), Some("expired"));
    assert!(parse_from(["opps", "--state", "nonsense"].into_iter().map(String::from)).is_err(),
        "未知状态当场拒绝，不静默全表");
}
```

- [ ] **Step 2: 实现**
  - `Command` 枚举加 `XRegion, Opps`；`Args` 加 `do_update: bool`、`state: Option<String>`；`parse_args` 加 `"xregion"` / `"opps"`、`--update`、`--state`（校验 ∈ new/notified/expired/invalidated）；缺子命令提示串与 `print_usage` 同步补两行。
  - 派发：`Command::XRegion => run_xregion(&client, &db).await?`；`Command::Opps => run_opps(&db, args.do_update, args.state.as_deref())?`。
  - `run_xregion`：构造 `Scheduler::new(client, db, SchedulerConfig { xregion: XRegionConfig::from_env(), ..Default::default() })` → `run_t1_5().await?` → 打印 `XRegionReport`（含"候选空 = 先跑 round；三站 0 本 = 查站 ID"提示）；顺带打印 `db.last_xregion_log()`。
  - `run_opps`：`--update` 先跑 `market::lifecycle::update_round(db, now)` 并打印 `TickStats`；然后按 `--state` 过滤打印表：`类型名 | 买站→卖站 | 状态 | 净利率 | 缺席 | 首次/最近seen | 通知(day×count, at)`；表尾汇总各状态计数。空表提示"先 serve 或 --update"。
  - `run_flip`：`hubs` 改 `db.flip_hubs()`；`ages = db.xregion_ages()?`；每个机会行的站名列后追加跨区角标 `[跨区Xm]`（该行任一站在 ages 里时取两站最大年龄，分钟取整）；表尾注补一句"带[跨区]行的目标站数据来自上一批 T1.5 采集（最长滞后 12 分钟）"。

- [ ] **Step 3: 跑测试 + `cargo build -p emd-daemon` 绿**
- [ ] **Step 4: 提交** `git commit -m "feat(daemon): xregion/opps 子命令 + flip 跨区行角标（真机验收入口）"`

---

### Task 9: app + web — 跨区数据年龄角标

**Files:**
- Modify: `crates/emd-app/src/lib.rs`（`FlipRow` / `flip_scan`）、`crates/emd-app/src/tests.rs`
- Modify: `web/src/types.ts`、`web/src/api.ts`、`web/src/components/FlipScanner.tsx`、`web/src/styles.css`

- [ ] **Step 1: 后端**：`FlipRow` 加 `xregion_age_secs: Option<i64>`；`flip_scan` 中 hubs 改 `db.flip_hubs()`、取 `db.xregion_ages()`，按"买站/卖站中命中 ages 的站的最大年龄"填行字段（两站都不是跨区站 → None）。tests.rs 补一例：快照含 xregion 行时 `scan_flip` 的对应 row `xregion_age_secs` 为 Some（用 `db.write_xregion_books` 播种）。
- [ ] **Step 2: 前端类型与 fixture**：`types.ts` 的 `FlipRow` 加 `xregion_age_secs?: number | null`；`api.ts` fixture 的 `FIX_ROWS` 追加第 4 行跨区示例（Tritanium，Jita IV - Moon 4 → Amarr VIII (Oris) - Emperor Family Academy，`xregion_age_secs: 305`），其余行显式 `xregion_age_secs: null`。
- [ ] **Step 3: UI**：`FlipScanner.tsx` 在"买站 → 卖站"单元格内追加角标：`{r.xregion_age_secs != null && <span className="xregion" title="跨区数据来自上一批 T1.5 采集，最长滞后 12 分钟">跨区 {Math.max(1, Math.round(r.xregion_age_secs / 60))} 分钟前</span>}`；表尾 note 追加一句跨区口径说明；`styles.css` 加 `.xregion`（沿用现有角标色系，暗色变量）。
- [ ] **Step 4: `npx tsc --noEmit` = 0；`npm run build` 绿**
- [ ] **Step 5: 提交** `git commit -m "feat(web+app): 跨区行数据年龄角标（fixture 示例 + 诚实标注 T1.5 滞后）"`

---

### Task 10: 收尾——全量回归 + CodeReview + 真机验收

- [ ] **Step 1: 全量回归**：`cargo test -p emd-core -p emd-daemon -p emd-app`（预计 163 + ~25 新例全绿）+ `npx tsc --noEmit` + `npm run build`
- [ ] **Step 2: CodeReview 子代理**（diff = M4b 全部提交），整改发现项
- [ ] **Step 3: 真机验收**（按序）：
  1. `emd-daemon serve --rounds 2`（真库；第 2 轮应出现 T1.5 日志与生命周期日志；此步约 12–15 分钟，含 600+ 次 T1.5 请求）
  2. `emd-daemon xregion`：三枢纽各 >0 本（**若某站 0 本 → 站 ID 与真实不符，停下来核对再继续**）；`requests` 落在 600–1000 量级
  3. `emd-daemon opps`：表内有 new/notified 行与生命周期字段
  4. `emd-daemon flip`：出现跨区行（含 `[跨区Xm]` 角标）
  5. 浏览器「倒卖」视图：跨区行 + 角标渲染、控制台 0 error、窄窗不破版
- [ ] **Step 4: 最终提交 + 报告**（含对 spec §1 M4b 三项的逐条映射与挂账项；E. 明确记录"notified 迁移的接线留 M4c，闸门函数已就绪并测试"）

---

## 自审记录

1. **规格覆盖**：T1.5（Task 5/7：定向拉取、隔轮、错峰、候选 Top200、台账）✓；opportunities v5（Task 3/4/6）✓；状态机四态 + 连续 2 轮过期 + 即时失效 + 4h 冷却 + +2pp 穿透 + 每日 2 条（Task 2）✓；`notified` 迁移属 M4c 接线（闸门函数就绪，Task 10 记录）✓；跨区数据年龄诚实标注（Task 8/9）✓。
2. **占位符扫描**：无 TBD；三处 `/* SELECT ... */` 注释为"同列模式的反序列化"占位豁免——执行时按 `load_books` 逐列抄写（步骤已注明）。
3. **类型一致性**：`OppRecord` 15 字段在 Task 2/4/6 一致；`PairVerdict` 五变体在 Task 1/6 一致；`fetch_type_orders`/`TypeFetch` 在 Task 5/7 一致；`XRegionConfig` 字段在 Task 7 定义、Task 8 引用一致。
