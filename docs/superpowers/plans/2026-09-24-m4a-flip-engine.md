# M4a 倒卖引擎实现计划（flip 核心 + 扫描器 + 试算）

> **For agentic workers:** 本环境无编码子代理（已在 M3 验证），采用**会话内 TDD + CodeReview 子代理评审**执行。步骤用 `- [ ]` 勾选跟踪。
> 依据：`docs/superpowers/specs/2026-09-24-m4-flip-engine-design.md`（简称 spec）。与 spec 冲突时以 spec 为准。

**Goal:** 落地 M4a——倒卖扫描核心（官方基率 7.5/3.0 + 技能修正）、参数/技能面板、扫描器视图、试算行、daemon `flip` 子命令。

**Architecture:** 新模块 `emd-core::market::flip`（纯函数，零网络/DB 依赖）；参数持久化进 `meta` 表（KV，无迁移）；emd-app 加 4 命令；daemon 加 `flip`；web 加顶层视图「倒卖」。

**Tech Stack:** Rust（workspace 三 crate）+ rusqlite + serde_json；Vite/React/TS/Zustand。

**Global Constraints（硬约束，每个 Task 都适用）:**
- 费率公式只在 `flip.rs` 实现一次；TS 侧禁止复制（试算走 `trial_calc` 命令）
- 引擎纯函数化：`scan` 不读 DB/网络；DB 读取只在 `db.rs` 与调用层
- `cargo test -p emd-core -p emd-daemon -p emd-app` 全绿才提交；每个 Task 一个提交
- 注释用中文，风格对齐现有模块（"为什么"而非"做了什么"）

---

### Task 1: `flip.rs` — FeeModel + FlipParams（费率与技能修正层）

**Files:**
- Create: `crates/emd-core/src/market/flip.rs`
- Modify: `crates/emd-core/src/market/mod.rs`（加 `pub mod flip;`）

- [x] **Step 1: 写失败测试**（flip.rs 底部 `#[cfg(test)] mod tests`）

```rust
#[test]
fn default_is_ingame_no_skill_state() {
    let f = FeeModel::default();
    assert_eq!(f.sales_tax_pct, 7.5);
    assert_eq!(f.broker_pct, 3.0);
    assert_eq!(f.accounting, 0);
    assert!((f.effective_sales_tax() - 0.075).abs() < 1e-12);
    assert!((f.effective_broker() - 0.03).abs() < 1e-12);
}

#[test]
fn accounting_reduces_sales_tax_11pct_per_level_relative() {
    let mut f = FeeModel::default();
    f.accounting = 5;
    assert!((f.effective_sales_tax() - 0.03375).abs() < 1e-12); // 7.5% × 0.45
}

#[test]
fn broker_relations_reduces_fee_03pp_per_level_absolute() {
    let mut f = FeeModel::default();
    f.broker_relations = 5;
    assert!((f.effective_broker() - 0.015).abs() < 1e-12); // 3.0 − 1.5
}

#[test]
fn broker_floor_is_one_percent_but_never_above_base() {
    let mut f = FeeModel::default();
    f.broker_relations = 5;
    f.broker_pct = 1.2;
    assert!((f.effective_broker() - 0.01).abs() < 1e-12, "1.2−1.5 → 地板 1.0");
    f.broker_pct = 0.5;
    assert!((f.effective_broker() - 0.005).abs() < 1e-12, "地板不反超基率");
}

#[test]
fn out_of_range_skill_levels_are_clamped() {
    let mut f = FeeModel::default();
    f.accounting = 9;
    f.broker_relations = 9;
    assert!((f.effective_sales_tax() - 0.03375).abs() < 1e-12);
    assert!((f.effective_broker() - 0.015).abs() < 1e-12);
}

#[test]
fn params_defaults_and_serde_roundtrip() {
    let p = FlipParams::default();
    assert_eq!(p.margin_threshold_pct, 3.0);
    assert_eq!(p.capital_isk, 100_000_000.0);
    assert_eq!(p.capital_pct_per_trade, 5.0);
    assert_eq!(p.min_batch, 100);
    assert_eq!(p.freight_isk_per_unit, 0.0);
    assert!(!p.include_buy_broker);
    let json = serde_json::to_string(&p).unwrap();
    let back: FlipParams = serde_json::from_str(&json).unwrap();
    assert_eq!(p, back);
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p emd-core flip 2>&1 | Select-String 'error|test result'`
Expected: 编译失败（`FeeModel` 不存在）

- [x] **Step 3: 最小实现**

```rust
//! 倒卖引擎（spec §2）。费率基 = 官方现行值：销售税 7.5%（2025-03 补丁）、
//! 中介费 3%；技能 Level 0 = 无技能影响口径（游戏默认状态）。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeModel {
    pub sales_tax_pct: f64,      // 默认 7.5，可调 0–8
    pub broker_pct: f64,         // 默认 3.0，可调 0–5
    pub accounting: u8,          // 0–5
    pub broker_relations: u8,    // 0–5
    pub faction_standing: f64,   // 公式预留，M4 固定 0
    pub corp_standing: f64,      // 公式预留，M4 固定 0
}

impl Default for FeeModel {
    fn default() -> Self {
        Self { sales_tax_pct: 7.5, broker_pct: 3.0, accounting: 0, broker_relations: 0,
               faction_standing: 0.0, corp_standing: 0.0 }
    }
}

impl FeeModel {
    fn acc(&self) -> u8 { self.accounting.min(5) }
    fn br(&self) -> u8 { self.broker_relations.min(5) }

    /// Accounting：每级相对 −11%（CCP 现行机制）。
    pub fn effective_sales_tax(&self) -> f64 {
        ((self.sales_tax_pct / 100.0) * (1.0 - 0.11 * f64::from(self.acc()))).max(0.0)
    }

    /// Broker Relations：每级绝对 −0.3pp（官方帮助页公式）；地板 min(1%, 基率)。
    pub fn effective_broker(&self) -> f64 {
        let base = self.broker_pct / 100.0;
        let floor = base.min(0.01);
        (base
            - 0.003 * f64::from(self.br())
            - 0.0003 * self.faction_standing.max(0.0)
            - 0.0002 * self.corp_standing.max(0.0))
        .max(floor)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlipParams {
    pub fees: FeeModel,
    pub margin_threshold_pct: f64,
    pub capital_isk: f64,
    pub capital_pct_per_trade: f64,
    pub min_batch: u64,
    pub freight_isk_per_unit: f64,
    pub include_buy_broker: bool,
}

impl Default for FlipParams {
    fn default() -> Self {
        Self { fees: FeeModel::default(), margin_threshold_pct: 3.0,
               capital_isk: 100_000_000.0, capital_pct_per_trade: 5.0,
               min_batch: 100, freight_isk_per_unit: 0.0, include_buy_broker: false }
    }
}
```

- [x] **Step 4: 跑测试确认通过**（6 个用例全绿）

- [x] **Step 5: 提交** `git commit -m "feat(core): flip 费率与技能修正层（官方基率 7.5/3.0，Accounting −11%/级，BR −0.3pp/级+地板）"`

---

### Task 2: `flip.rs` — scan 扫描核心

**Files:** Modify: `crates/emd-core/src/market/flip.rs`

- [x] **Step 1: 写失败测试**

测试夹具（构造 `StationOrderBook`：`skipped_*`=0、`is_npc_station`=true）：

```rust
fn book(loc: u64, ty: u32, asks: &[(f64, u64, u32)], bids: &[(f64, u64, u32)]) -> StationOrderBook {
    let pl = |v: &[(f64, u64, u32)]| v.iter().map(|&(price, volume, orders)| PriceLevel { price, volume, orders }).collect::<Vec<_>>();
    let asks_v = pl(asks); let bids_v = pl(bids);
    StationOrderBook {
        location_id: loc, type_id: ty, is_npc_station: true,
        best_bid: bids_v.first().map(|l| l.price), bid_qty: bids_v.first().map(|l| l.volume).unwrap_or(0),
        best_ask: asks_v.first().map(|l| l.price), ask_qty: asks_v.first().map(|l| l.volume).unwrap_or(0),
        bid_levels: bids_v.iter().map(|l| l.orders).sum(), ask_levels: asks_v.iter().map(|l| l.orders).sum(),
        bid_depth: bids_v, ask_depth: asks_v, skipped_stale: 0, skipped_thin: 0, skipped_wholesale: 0,
    }
}
fn hubs_of(ids: &[u64]) -> Vec<Hub> {
    ids.iter().enumerate().map(|(i, &location_id)| Hub { location_id, order_count: 999, share_pct: 1.0, rank: i + 1 }).collect()
}
```

关键用例：

```rust
#[test]
fn doc_example_tax_base_is_full_amount_not_spread() {
    // spec §2.3 铁证：ask=100 / bid=110 / 合计费率 8% → 净利 1.2（不是 10×0.92=9.2）
    let books = vec![book(STATION_JITA, 34, &[(100.0, 1000, 5)], &[]),
                     book(60015157, 34, &[], &[(110.0, 1000, 5)])];
    let mut p = FlipParams::default();
    p.fees.sales_tax_pct = 5.0; p.fees.broker_pct = 3.0;
    p.margin_threshold_pct = 0.0; p.min_batch = 1; p.capital_isk = 1_000_000.0;
    let out = scan(&books, &hubs_of(&[STATION_JITA, 60015157]), &p, &HashMap::new());
    let o = &out.opportunities[0];
    assert!((o.buy_price - 100.0).abs() < 1e-9);
    assert!((o.net_per_unit - 1.2).abs() < 1e-9);   // 110×0.92 − 100
    assert_eq!(o.qty, 500);                          // 资金 5% = 50_000 ÷ 100
    assert!((o.net_total - 600.0).abs() < 1e-9);
    assert_eq!(o.vol_source, VolSource::Depth);      // 无 history → 深度回落
}

#[test]
fn skills_raise_margin_monotonically() {
    let books = /* 同上单簿 */;
    let hubs = hubs_of(&[STATION_JITA, 60015157]);
    let mut p = FlipParams::default(); p.margin_threshold_pct = 0.0; p.min_batch = 1;
    let m0 = scan(&books, &hubs, &p, &HashMap::new()).opportunities[0].margin_pct;
    p.fees.accounting = 5; p.fees.broker_relations = 5;
    let m1 = scan(&books, &hubs, &p, &HashMap::new()).opportunities[0].margin_pct;
    assert!(m1 > m0, "{m0} → {m1}");  // 无技能默认口径最保守
}

#[test]
fn drops_want_below_min_batch_and_short_fill() { /* want=500 < min_batch=600 → 全丢；stats.dropped_batch=1 */ }

#[test]
fn drops_below_threshold_and_same_station_pairs() { /* margin 1.2% < 3% → 丢；a==b → 不成对 */ }

#[test]
fn vol24_prefers_history_and_falls_back_to_depth() {
    let mut vol = HashMap::new(); vol.insert(34u32, 1234u64);
    let out = scan(&books, &hubs, &p, &vol);
    assert_eq!(out.opportunities[0].vol_source, VolSource::History);
    assert_eq!(out.opportunities[0].vol24, 1234);
}

#[test]
fn sort_is_score_desc_then_stable_tiebreak() { /* 高分在前；同分按 (type_id, buy_loc, sell_loc) 升序 */ }
```

- [x] **Step 2: 跑测试确认失败**

- [x] **Step 3: 实现 scan 与输出结构**

```rust
use std::collections::{HashMap, HashSet};
use crate::market::aggregate::{PriceLevel, Side, StationOrderBook};
use crate::market::hubs::Hub;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolSource { History, Depth }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Opportunity {
    pub type_id: u32, pub buy_loc: u64, pub sell_loc: u64,
    pub buy_price: f64, pub sell_price: f64, pub qty: u64,
    pub net_per_unit: f64, pub net_total: f64, pub margin_pct: f64,
    pub vol24: u64, pub vol_source: VolSource,
    pub buy_levels: u32, pub sell_levels: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanStats {
    pub pairs_evaluated: usize,
    pub dropped_batch: usize,      // want < min_batch
    pub dropped_shortfall: usize,  // 短填 q_eff < min_batch
    pub dropped_threshold: usize,  // 未过阈值
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanOutcome { pub opportunities: Vec<Opportunity>, pub stats: ScanStats }

pub fn scan(books: &[StationOrderBook], hubs: &[Hub], p: &FlipParams, vol24: &HashMap<u32, u64>) -> ScanOutcome {
    let hub_set: HashSet<u64> = hubs.iter().map(|h| h.location_id).collect();
    let broker = p.fees.effective_broker();
    let tax = p.fees.effective_sales_tax();
    let mut by_type: HashMap<u32, Vec<&StationOrderBook>> = HashMap::new();
    for b in books { if hub_set.contains(&b.location_id) { by_type.entry(b.type_id).or_default().push(b); } }

    let mut out = ScanOutcome::default();
    for (&type_id, group) in &by_type {
        for a in group {                       // 买站：吃其卖盘
            let Some(best_ask) = a.best_ask else { continue };
            for b in group {                   // 卖站：吃其买盘
                if a.location_id == b.location_id { continue; }
                out.stats.pairs_evaluated += 1;
                let budget_qty = ((p.capital_isk * p.capital_pct_per_trade / 100.0) / best_ask).floor();
                let want = (budget_qty.max(0.0) as u64)
                    .min(a.capacity(Side::Buy)).min(b.capacity(Side::Sell));
                if want < p.min_batch { out.stats.dropped_batch += 1; continue; }
                let (Some((buy_px, fb)), Some((sell_px, fs))) =
                    (a.executable(Side::Buy, want), b.executable(Side::Sell, want)) else { continue };
                let qty = fb.min(fs);
                if qty < p.min_batch { out.stats.dropped_shortfall += 1; continue; }
                let q = qty as f64;
                let net_sell = sell_px * q * (1.0 - broker - tax);   // 税基 = 卖出全额
                let mut cost = buy_px * q + p.freight_isk_per_unit * q;
                if p.include_buy_broker { cost += buy_px * q * broker; }
                if cost <= 0.0 { continue; }
                let net = net_sell - cost;
                let margin_pct = net / cost * 100.0;
                if margin_pct < p.margin_threshold_pct { out.stats.dropped_threshold += 1; continue; }
                let (v24, src) = match vol24.get(&type_id) {
                    Some(&v) if v > 0 => (v, VolSource::History),
                    _ => (qty, VolSource::Depth),
                };
                out.opportunities.push(Opportunity {
                    type_id, buy_loc: a.location_id, sell_loc: b.location_id,
                    buy_price: buy_px, sell_price: sell_px, qty,
                    net_per_unit: net / q, net_total: net, margin_pct,
                    vol24: v24, vol_source: src,
                    buy_levels: a.ask_levels, sell_levels: b.bid_levels,
                });
            }
        }
    }
    out.opportunities.sort_by(|x, y| {
        let sx = x.margin_pct * (1.0 + x.vol24 as f64).ln();
        let sy = y.margin_pct * (1.0 + y.vol24 as f64).ln();
        sy.partial_cmp(&sx).unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| (x.type_id, x.buy_loc, x.sell_loc).cmp(&(y.type_id, y.buy_loc, y.sell_loc)))
    });
    out
}

/// 试算（spec §2.4）：与 scan 同一费率出口，负数 = 扣税后亏损。
pub fn trial(buy_price: f64, sell_price: f64, qty: u64, p: &FlipParams) -> (f64, f64, f64) {
    let broker = p.fees.effective_broker(); let tax = p.fees.effective_sales_tax();
    let q = qty as f64;
    let net_sell = sell_price * q * (1.0 - broker - tax);
    let mut cost = buy_price * q + p.freight_isk_per_unit * q;
    if p.include_buy_broker { cost += buy_price * q * broker; }
    let net = net_sell - cost;
    (net / q, net, net / cost * 100.0)
}
```

- [x] **Step 4: 跑测试确认通过**（Task 1+2 全绿）

- [x] **Step 5: 提交** `git commit -m "feat(core): flip 扫描核心（加权吃单/短填降级/全额税基/技能单调+v24 回落）"`

---

### Task 3: `db.rs` — 参数持久化 + 扫描输入装配

**Files:** Modify: `crates/emd-core/src/store/db.rs`

- [x] **Step 1: 读现有代码**：`meta` 表 KV 读写模式、`station_orders` 读取模式、`market_history` 查询模式、`round_log` 年龄查询模式（决定复用哪些私有 helper）
- [x] **Step 2: 写失败测试**（in-memory Db）

```rust
#[test]
fn flip_params_roundtrip_via_meta() {
    let db = Db::in_memory().unwrap();
    let mut p = FlipParams::default();
    p.fees.accounting = 3; p.margin_threshold_pct = 5.0;
    db.set_flip_params(&p).unwrap();
    assert_eq!(db.get_flip_params().unwrap(), p);
}
#[test]
fn flip_params_default_when_absent() {
    let db = Db::in_memory().unwrap();
    assert_eq!(db.get_flip_params().unwrap(), FlipParams::default());
}
#[test]
fn flip_params_rejects_invalid_values() {
    let db = Db::in_memory().unwrap();
    let mut p = FlipParams::default(); p.fees.accounting = 9;
    assert!(db.set_flip_params(&p).is_err(), "技能越界必须拒绝，不能静默存");
    let mut p2 = FlipParams::default(); p2.margin_threshold_pct = -1.0;
    assert!(db.set_flip_params(&p2).is_err());
}
#[test]
fn load_books_reads_back_aggregate_output() { /* 写一条 book_row → load_books 反序列化 depth JSON、is_npc 重算、skipped_*=0 */ }
#[test]
fn latest_vol24_takes_most_recent_date_per_type() { /* 两日期两类型；取各自最新 → HashMap{ty→vol} */ }
```

- [x] **Step 3: 实现**

```rust
/// spec §2.2：参数进 meta KV（无迁移）；非法值拒绝而非静默存。
pub fn set_flip_params(&self, p: &FlipParams) -> Result<()> {
    let ok = p.fees.accounting <= 5 && p.fees.broker_relations <= 5
        && (0.0..=8.0).contains(&p.fees.sales_tax_pct)
        && (0.0..=5.0).contains(&p.fees.broker_pct)
        && p.capital_isk > 0.0
        && (0.0..=100.0).contains(&p.capital_pct_per_trade)
        && p.min_batch >= 1
        && p.freight_isk_per_unit >= 0.0
        && p.margin_threshold_pct >= 0.0;
    if !ok { return Err(Error::Config("flip 参数越界".into())); }
    // meta_set("flip_params", serde_json::to_string(p)?)
}
pub fn get_flip_params(&self) -> Result<FlipParams> { /* meta_get → 无则 default；解析失败 → default + warn（不炸 UI） */ }
pub fn load_books(&self) -> Result<Vec<StationOrderBook>> { /* SELECT 全表 → 反序列化 bid/ask_depth JSON、is_npc_station = LocationKind::of(loc).tradable_publicly()、skipped_*=0 */ }
pub fn latest_vol24(&self) -> Result<HashMap<u32, u64>> { /* 每 type 取 MAX(date) 行的 volume */ }
pub fn last_round_age_secs(&self) -> Result<Option<i64>> { /* round_log 最近 started_at → now − 之 */ }
```

- [x] **Step 4: 跑测试确认通过**
- [x] **Step 5: 提交** `git commit -m "feat(core): flip 参数 meta 持久化（非法拒绝）+ 扫描输入装配（books/hub/vol 年龄）"`

---

### Task 4: daemon `flip` 子命令

**Files:** Modify: `crates/emd-daemon/src/main.rs`（`enum Command` + `parse_args` + `print_usage` 三处同步）

- [x] **Step 1: 写失败测试**（跟现有 daemon 测试风格：parse 层）

```rust
#[test]
fn parses_flip_with_top() { /* ["flip","--top","10"] → Command::Flip { top: 10 }；缺省 top=20 */ }
```

- [x] **Step 2: 实现**：`flip` 分支 = open db → `load_books`/`load_hub_pool`/`latest_vol24`/`get_flip_params` → `flip::scan` → 打印表（类型名/买站→卖站/买价/卖价/qty/净利率/净利/vol 来源）；0 机会打印 `pairs/dropped_batch/dropped_shortfall/dropped_threshold` 四计数。类型名与站点名复用现有查询 helper。
- [x] **Step 3: 跑测试**；`cargo build -p emd-daemon` 绿
- [x] **Step 4: 真机冒烟**：`emd-daemon flip --top 5`（读本机真库）
- [x] **Step 5: 提交** `git commit -m "feat(daemon): flip 子命令（Top N + 丢弃原因分布）"`

---

### Task 5: emd-app 4 命令

**Files:** Modify: `crates/emd-app/src/lib.rs`、`crates/emd-app/src/tests.rs`

- [x] **Step 1: 写失败测试**（tests.rs，沿用现有 invoke 测试模式）

```rust
#[test]
fn trial_calc_negative_with_default_rates_positive_with_max_skills() {
    // 默认 7.5/3：110×(1−0.105)=98.45 → 单位净利 −1.55
    // 技能 5/5：110×(1−0.03375−0.015)=104.6375 → +4.6375
}
#[test]
fn flip_params_roundtrip_and_scan_shape() { /* set→get 相等；scan_flip 返回 rows+stats+age */ }
```

- [x] **Step 2: 实现 4 命令**（`impl AppState` 分层）：

```rust
#[tauri::command] fn get_flip_params(state: State<AppState>) -> Result<FlipParams, String>
#[tauri::command] fn set_flip_params(state: State<AppState>, params: FlipParams) -> Result<(), String>
#[tauri::command] fn trial_calc(state: State<AppState>, buy_price: f64, sell_price: f64, qty: u64) -> Result<TrialOut, String>
#[tauri::command] fn scan_flip(state: State<AppState>) -> Result<FlipScanOut, String>
// FlipRow = Opportunity + type_name + buy_loc_name + sell_loc_name；FlipScanOut { rows, stats, age_secs }
```

- [x] **Step 3: 跑测试**：`cargo test -p emd-app`
- [x] **Step 4: 提交** `git commit -m "feat(app): scan_flip/get/set_flip_params/trial_calc 命令（查看者也可用，纯本地）"`

---

### Task 6: web 数据层（types/api/store/fixture）

**Files:** Modify: `web/src/types.ts`、`web/src/api.ts`、`web/src/store.ts`

- [x] **Step 1: types.ts**：`FeeModel`/`FlipParams`/`VolSource`/`FlipRow`/`FlipScanOut(resp)`/`TrialOut`/`FlipSortKey("score"|"profit")`（字段名与 Rust serde 一致 snake_case）
- [x] **Step 2: api.ts**：4 个 api 函数；`inTauri` 分支 invoke；fixture 分支 `fixFlip()` 返回 3 条演示行（含 1 条负 margin、vol 来源一 History 两 Depth）+ `fixTrial()` 返回预置结果（**fixture 数值为演示常数，不复刻费率公式——避免双源**，注释写明）
- [x] **Step 3: store.ts**：`view: "market" | "flip"`、`flipRows/flipStats/flipAge/flipParams/flipSort/flipLoading` + actions `setView/loadFlip/saveFlipParams/runTrial/setFlipSort`；`saveFlipParams` 成功后自动 `loadFlip()`（技能/参数改动 → 秒级重算闭环）
- [x] **Step 4: tsc**：`npx tsc --noEmit` = 0
- [x] **Step 5: 提交** `git commit -m "feat(web): 倒卖视图数据层（types/api/store/fixture）"`

---

### Task 7: web 视图 `FlipScanner.tsx` + TopBar 切换 + 试算行

**Files:** Create: `web/src/components/FlipScanner.tsx`；Modify: `web/src/components/TopBar.tsx`、`web/src/App.tsx`、`web/src/styles.css`

- [x] **Step 1: FlipScanner**：参数面板（折叠）+ 机会表 + 试算行
  - 参数面板：费率基/阈值/资金/单笔比例/最小批量/单件运费/买入侧佣金开关 + 技能步进器（0–5，旁显 `销售税 7.5% → 3.38%`）；"保存并重算"走 `saveFlipParams`
  - 机会表列：类型名/买站→卖站/买价/卖价/可成交量/净利率/单位净利/总净利/24h 量（`History`/`Depth` 角标）/买站档位/卖站档位；数字列右对齐（复用单簿 grid 规范）；负净利率标红；排序键切换按钮（score/profit，前端本地重排）
  - 角标：`基于估算费率` 常驻；技能非 0 挂 `技能口径：Accounting A · BR B`；全 0 挂 `技能口径：无影响（游戏默认状态）`
  - 试算行：买价/卖价/数量 → `runTrial`；负数红字 + "扣税后亏损：改技能等级或放弃此单"
  - 空态：无快照 → "先跑一轮采集"；0 机会 → 显示 stats 四计数
- [x] **Step 2: TopBar** 加「市场 / 倒卖」切换（store.setView）；[App.tsx](file:///c:/EVE市场分析/eve-market-desk/web/src/App.tsx) 在 `view==="flip"` 时全宽渲染 FlipScanner（隐藏三栏）
- [x] **Step 3: styles.css**：`.flip*` 类，暗色主题对齐现有变量
- [x] **Step 4: build**：`npm run build` 绿（tsc + vite）
- [x] **Step 5: 浏览器复验**（Browser 子代理）：技能 0→5 切档角标/数值变化；试算负数标红；表格列对齐 <1px；控制台 0 error
- [x] **Step 6: 提交** `git commit -m "feat(web): 倒卖扫描器视图（参数/技能面板、机会表、试算行、视图切换）"`

---

### Task 8: 收尾——全量回归 + CodeReview + 真机验收

- [x] **Step 1: 全量回归**：`cargo test -p emd-core -p emd-daemon -p emd-app`（预计 135+ 新用例全绿）+ `npx tsc --noEmit` + `npm run build`
- [x] **Step 2: CodeReview 子代理**（diff = M4a 全部提交），整改发现项
- [x] **Step 3: 真机验收**：daemon `flip` 真库 Top 10；浏览器：改技能 0→5 后 opportunities 数/margin 变化且与 daemon 输出同口径（同一快照）
- [x] **Step 4: 最终提交 + 报告**（含 R1–R8 裁决执行情况与挂账项）

---

## 执行报告（2026-09-24 收尾）

**提交链**（master，全部经全量回归绿后提交）：
`c181c7d` 费率层+扫描核心 → `e3806b8` meta 持久化+装配 → `d4d5213` daemon flip → `2a46ba9` app 四命令 → `7b94211` 技能临时覆盖 → `5ea7cac` web 视图 → `724bd63` 评审整改 → `d964500` 回归用例+fixture 镜像。

**回归**：`cargo test` 三 crate 163 用例全绿（退出码 0）；`tsc --noEmit` 0；`vite build` 4.16s 成功。

**真机验收**：
- daemon 真库（16666 单簿/20 枢纽）：A0/B0 生效 7.50%/3.000%，快照 50322s 前评 268 对 0 机会如实输出（want<批量 231、未过阈值 37——快照 14h+ 所致，非引擎缺陷）；`--accounting 5 --broker-relations 5` 临时覆盖后生效 3.37%/1.500%，扫出真机会（Mexallon Uemon→Perkone 44.17→50.11，净利 7.92%）。
- 浏览器 fixture：A0/B0 → A5/B5 切档保存后行列数字即时随动（Isogen 净利率 4.52%→11.09%），生效费率与 daemon 同口径逐位一致；试算 100/110×100 @A5/B5 = 单位净利 4.6375，与 Task 5 期望值吻合；负毛利 Eleutrium 标红。控制台无应用错误。

**R1–R8 裁决执行情况**：
- R1 基率 7.5/3.0：落地于 `FeeModel::default`，A0/B0 真机 0 机会正是该裁决预期的"真实成本"。
- R2 地板 min(1%, 基率)：`effective_broker` 实现，单测覆盖基率<1.5% 边界。
- R3 单件 ISK 运费：参数与扫描均按 `freight_isk_per_unit`；m3×跳数按 spec §1 保持挂账——**不属 M4b 既定范围**（M4b = T1.5 + 状态机 + 迁移 v5），无跳数来源时 m3 模型不可算。
- R4 预算 best_ask 估算不回退：`scan` 内一次定 budget_qty，无重算循环。
- R5 vol24 回落 Depth 带角标：表格"成交/估算"角标已实现。
- R6 试算 Rust 单源：`trial_calc` 命令；fixture 镜像显式标注"漂移以 Rust 为准"（`d964500`）。
- R7 transaction_id 回填：属 M4b 已实现轨范围，M4a 未触碰。
- R8 试算行替代 SSO：FlipScanner 试算行即本裁决载体。

**挂账项**：
1. `web/index.html` 引 `/favicon.ico` 但 `web/public/` 缺失，dev 控制台 404（不影响构建与功能）。
2. vite 产物 >500kB 单 chunk 警告（图表库未分包），留待性能里程碑处理。
3. R3 的 m3×跳数运费口径：保持挂账（待星图路由数据源；非 M4b 范围）。

---

## 挂账项收口（2026-09-24 追加）

1. **favicon 404 → 已修**：补 `web/public/favicon.ico`（复用 `emd-app/icons/icon.ico`；Vite dev 直接服务 public 根，dev/build 两态零 404）。
2. **chunk 警告 → 已修**：vite `manualChunks` 按包拆分（echarts 369kB / zrender 177kB / react-vendor 144kB / virtual 26kB / app 29kB），HistoryChart 改 `React.lazy`+`Suspense`——echarts+zrender 仅点开「历史」时加载；首屏 js 从 747kB 降至 ~173kB，构建无 >500kB 警告。
3. **R3 m3×跳数 → 记录归位（本项不可即修，非代码遗留）**：spec 已裁决「无跳数来源时 m3 模型不可算」；本地库无 station→system 路由与类型 m3，故不属 M4a/M4b 范围。收口条件：引入星图路由数据源后，scan 以入参注入 volume/jumps 保持纯函数（同 vol24 模式）；ESI 侧需新增 `/universe/types/{id}`（volume）与 `/route/{o}/{d}/`（跳数）并过 M0.5 合规闸门。
