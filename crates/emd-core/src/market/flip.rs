//! 倒卖引擎（spec `docs/superpowers/specs/2026-09-24-m4-flip-engine-design.md` §2）。
//!
//! 费率基取**官方现行值**：销售税 7.5%（2025-03 补丁 4%→7.5%）、NPC 站中介费 3%；
//! 技能全 0 即"游戏内无技能默认状态"——这是全系统唯一"不依赖技能影响"的口径起点。
//! 技能修正：Accounting 每级对销售税**相对 −11%**；Broker Relations 每级对中介费
//! **绝对 −0.3pp**（CCP 帮助页公式），地板 min(1%, 基率)。
//!
//! 纯函数模块：不读 DB、不发网络请求；扫描输入由调用层装配（store 读快照、
//! hub_pool 出枢纽集、history 出 24h 量）。

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::market::hubs::Hub;
use crate::market::{Side, StationOrderBook};

/// 费率与技能修正层 —— 全系统唯一的费率出口，改这里即"改口径"。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeModel {
    /// 销售税基率（%），默认 7.5，可调 0–8。
    pub sales_tax_pct: f64,
    /// 中介费基率（%），默认 3.0，可调 0–5。
    pub broker_pct: f64,
    /// Accounting 0–5。
    pub accounting: u8,
    /// Broker Relations 0–5。
    pub broker_relations: u8,
    /// 势力声望：公式预留，M4 固定 0（保守侧）。
    pub faction_standing: f64,
    /// 军团声望：公式预留，M4 固定 0。
    pub corp_standing: f64,
}

impl Default for FeeModel {
    fn default() -> Self {
        Self {
            sales_tax_pct: 7.5,
            broker_pct: 3.0,
            accounting: 0,
            broker_relations: 0,
            faction_standing: 0.0,
            corp_standing: 0.0,
        }
    }
}

impl FeeModel {
    /// 越界技能等级按上限兜底（持久化层也会拒绝，这里是双保险）。
    fn acc(&self) -> u8 {
        self.accounting.min(5)
    }
    fn br(&self) -> u8 {
        self.broker_relations.min(5)
    }

    /// 有效销售税（小数）。Accounting 每级相对 −11%，下界 0。
    pub fn effective_sales_tax(&self) -> f64 {
        ((self.sales_tax_pct / 100.0) * (1.0 - 0.11 * f64::from(self.acc()))).max(0.0)
    }

    /// 有效中介费（小数）。每级绝对 −0.3pp；地板 min(1%, 基率)——
    /// 官方 1% 地板只在基率 ≥1% 时成立，基率被调低时地板不得反超基率。
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

/// 扫描参数（持久化进 `meta` 表 KV，无迁移）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlipParams {
    pub fees: FeeModel,
    /// 净利率阈值（%），默认 3.0。
    pub margin_threshold_pct: f64,
    /// 账户资金（ISK），默认 1 亿。
    pub capital_isk: f64,
    /// 单笔投入上限 = 资金 × 该比例（%），默认 5。
    pub capital_pct_per_trade: f64,
    /// 最小批量（件），默认 100。
    pub min_batch: u64,
    /// 单件运费（ISK）。默认 0 且 UI 标注"未含运费"；
    /// m3×跳数模型需要星图路由数据，M4a 不引入（spec R3）。
    pub freight_isk_per_unit: f64,
    /// 挂买单策略的买入侧中介费（默认 false：吃单买入不付中介费）。
    pub include_buy_broker: bool,
}

impl Default for FlipParams {
    fn default() -> Self {
        Self {
            fees: FeeModel::default(),
            margin_threshold_pct: 3.0,
            capital_isk: 100_000_000.0,
            capital_pct_per_trade: 5.0,
            min_batch: 100,
            freight_isk_per_unit: 0.0,
            include_buy_broker: false,
        }
    }
}

/// 24h 成交量的来源——排序键带来源角标，用户能分辨"真活跃"与"深度估算"。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolSource {
    History,
    Depth,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Opportunity {
    pub type_id: u32,
    pub buy_loc: u64,
    pub sell_loc: u64,
    /// 买站卖盘的加权吃单价。
    pub buy_price: f64,
    /// 卖站买盘的加权吃单价。
    pub sell_price: f64,
    pub qty: u64,
    pub net_per_unit: f64,
    pub net_total: f64,
    pub margin_pct: f64,
    pub vol24: u64,
    pub vol_source: VolSource,
    /// 买站卖侧档位笔数（薄档证据）。
    pub buy_levels: u32,
    /// 卖站买侧档位笔数。
    pub sell_levels: u32,
}

/// 丢弃原因分布——daemon 与 UI 的空态要用它说明"为什么 0 机会"，
/// 否则用户会把正常过滤当成 bug。
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanStats {
    pub pairs_evaluated: usize,
    pub dropped_batch: usize,
    pub dropped_shortfall: usize,
    pub dropped_threshold: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanOutcome {
    pub opportunities: Vec<Opportunity>,
    pub stats: ScanStats,
}

/// 跨站价差扫描（spec §2.3）。
///
/// 税基是**卖出成交全额**，不是差价——v3.0 公式在此修正（高估 7.7 倍的来源）。
/// 所有价格用"吃单到目标量的加权价"，不用 best bid/ask；短填如实降级。
pub fn scan(
    books: &[StationOrderBook],
    hubs: &[Hub],
    p: &FlipParams,
    vol24: &HashMap<u32, u64>,
) -> ScanOutcome {
    let hub_set: HashSet<u64> = hubs.iter().map(|h| h.location_id).collect();
    let broker = p.fees.effective_broker();
    let tax = p.fees.effective_sales_tax();

    let mut by_type: HashMap<u32, Vec<&StationOrderBook>> = HashMap::new();
    for b in books {
        if hub_set.contains(&b.location_id) {
            by_type.entry(b.type_id).or_default().push(b);
        }
    }

    let mut out = ScanOutcome::default();
    for (&type_id, group) in &by_type {
        for a in group {
            // 买站：吃它的卖盘；没有卖盘就买不进。
            let Some(best_ask) = a.best_ask.filter(|v| *v > 0.0) else {
                continue;
            };
            for b in group {
                // 卖站：吃它的买盘；同站对不是"跨站价差"。
                if a.location_id == b.location_id {
                    continue;
                }
                out.stats.pairs_evaluated += 1;

                // 目标量 = min(两侧可执行深度, 预算 ÷ 卖一估价)。
                // 预算用 best_ask 估算、结算用加权价，实际投入可能略低于上限——
                // 不回退重算，保证确定性（spec R4）。
                let budget_qty =
                    ((p.capital_isk * p.capital_pct_per_trade / 100.0) / best_ask).floor();
                let want = (budget_qty.max(0.0) as u64)
                    .min(a.capacity(Side::Buy))
                    .min(b.capacity(Side::Sell));
                if want < p.min_batch {
                    out.stats.dropped_batch += 1;
                    continue;
                }

                let (Some((buy_px, filled_buy)), Some((sell_px, filled_sell))) =
                    (a.executable(Side::Buy, want), b.executable(Side::Sell, want))
                else {
                    continue;
                };
                let qty = filled_buy.min(filled_sell);
                // 深度表与实际不一致时（旧版本截断等）如实降级，防御性分支。
                if qty < p.min_batch {
                    out.stats.dropped_shortfall += 1;
                    continue;
                }

                let q = qty as f64;
                let net_sell = sell_px * q * (1.0 - broker - tax); // 税基 = 全额
                let mut cost = buy_px * q + p.freight_isk_per_unit * q;
                if p.include_buy_broker {
                    cost += buy_px * q * broker;
                }
                if cost <= 0.0 {
                    continue;
                }
                let net = net_sell - cost;
                let margin_pct = net / cost * 100.0;
                if margin_pct < p.margin_threshold_pct {
                    out.stats.dropped_threshold += 1;
                    continue;
                }

                // history 覆盖（自选/活跃池）优先；缺失回落可执行深度并标来源。
                let (v24, src) = match vol24.get(&type_id) {
                    Some(&v) if v > 0 => (v, VolSource::History),
                    _ => (qty, VolSource::Depth),
                };

                out.opportunities.push(Opportunity {
                    type_id,
                    buy_loc: a.location_id,
                    sell_loc: b.location_id,
                    buy_price: buy_px,
                    sell_price: sell_px,
                    qty,
                    net_per_unit: net / q,
                    net_total: net,
                    margin_pct,
                    vol24: v24,
                    vol_source: src,
                    buy_levels: a.ask_levels,
                    sell_levels: b.bid_levels,
                });
            }
        }
    }

    // 排序键：净利率 × ln(1+24h量)；同分按 (type, 买站, 卖站) 稳定排序，
    // 避免每轮名次抖动（与 hub_pool 的稳定排序同一考量）。
    out.opportunities.sort_by(|x, y| {
        let sx = x.margin_pct * (1.0 + x.vol24 as f64).ln();
        let sy = y.margin_pct * (1.0 + y.vol24 as f64).ln();
        sy.partial_cmp(&sx)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                (x.type_id, x.buy_loc, x.sell_loc).cmp(&(y.type_id, y.buy_loc, y.sell_loc))
            })
    });
    out
}

/// 单笔试算（spec §2.4）：与 scan 共用同一费率出口，负数 = 扣税后亏损。
/// 返回 `(单位净利, 总净利, 净利率%)`。
pub fn trial(buy_price: f64, sell_price: f64, qty: u64, p: &FlipParams) -> (f64, f64, f64) {
    let broker = p.fees.effective_broker();
    let tax = p.fees.effective_sales_tax();
    let q = qty as f64;
    let net_sell = sell_price * q * (1.0 - broker - tax);
    let mut cost = buy_price * q + p.freight_isk_per_unit * q;
    if p.include_buy_broker {
        cost += buy_price * q * broker;
    }
    if cost <= 0.0 {
        return (0.0, 0.0, 0.0);
    }
    let net = net_sell - cost;
    (net / q, net, net / cost * 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{PriceLevel, STATION_JITA};

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
                order_count: 999,
                share_pct: 1.0,
                rank: i + 1,
            })
            .collect()
    }

    // ---- Task 1：费率与技能修正层 ----

    #[test]
    fn default_is_ingame_no_skill_state() {
        let f = FeeModel::default();
        assert_eq!(f.sales_tax_pct, 7.5);
        assert_eq!(f.broker_pct, 3.0);
        assert_eq!(f.accounting, 0);
        assert_eq!(f.broker_relations, 0);
        assert!((f.effective_sales_tax() - 0.075).abs() < 1e-12);
        assert!((f.effective_broker() - 0.03).abs() < 1e-12);
    }

    #[test]
    fn accounting_reduces_sales_tax_11pct_per_level_relative() {
        let mut f = FeeModel::default();
        f.accounting = 5;
        assert!((f.effective_sales_tax() - 0.03375).abs() < 1e-12, "7.5% × 0.45");
    }

    #[test]
    fn broker_relations_reduces_fee_03pp_per_level_absolute() {
        let mut f = FeeModel::default();
        f.broker_relations = 5;
        assert!((f.effective_broker() - 0.015).abs() < 1e-12, "3.0 − 1.5pp");
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
    fn standing_terms_follow_official_formula() {
        let mut f = FeeModel::default();
        f.faction_standing = 10.0;
        f.corp_standing = 10.0;
        // 3% − 0.03%×10 − 0.02%×10 = 2.5%（声望项虽预留，公式须正确）
        assert!((f.effective_broker() - 0.025).abs() < 1e-12);
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

    // ---- Task 2：扫描核心 ----

    fn doc_books() -> Vec<StationOrderBook> {
        vec![
            book(STATION_JITA, 34, &[(100.0, 1000, 5)], &[]),
            book(60015157, 34, &[], &[(110.0, 1000, 5)]),
        ]
    }

    #[test]
    fn doc_example_tax_base_is_full_amount_not_spread() {
        // §4.1 铁证：ask=100 / bid=110 / 合计费率 8% → 净利 1.2（不是 10×0.92=9.2）。
        let mut p = FlipParams::default();
        p.fees.sales_tax_pct = 5.0;
        p.fees.broker_pct = 3.0;
        p.margin_threshold_pct = 0.0;
        p.min_batch = 1;
        p.capital_isk = 1_000_000.0;
        let out = scan(&doc_books(), &hubs_of(&[STATION_JITA, 60015157]), &p, &HashMap::new());
        assert_eq!(out.opportunities.len(), 1);
        let o = &out.opportunities[0];
        assert!((o.buy_price - 100.0).abs() < 1e-9);
        assert!((o.sell_price - 110.0).abs() < 1e-9);
        assert!((o.net_per_unit - 1.2).abs() < 1e-9, "110×0.92 − 100");
        assert_eq!(o.qty, 500, "资金 5% = 50_000 ÷ 100");
        assert!((o.net_total - 600.0).abs() < 1e-9);
        assert_eq!(o.vol_source, VolSource::Depth);
        assert_eq!(o.vol24, 500);
        assert_eq!(out.stats.pairs_evaluated, 1);
    }

    #[test]
    fn skills_raise_margin_monotonically() {
        let books = doc_books();
        let hubs = hubs_of(&[STATION_JITA, 60015157]);
        let mut p = FlipParams::default();
        p.margin_threshold_pct = -100.0; // 默认费率下这单是亏的（税基全额），需要负数阈值才可见
        p.min_batch = 1;
        p.capital_isk = 1_000_000.0;
        let m0 = scan(&books, &hubs, &p, &HashMap::new()).opportunities[0].margin_pct;
        assert!(m0 < 0.0, "无技能默认口径：110×0.895 − 100 = −1.55 → 负 margin");
        p.fees.accounting = 5;
        p.fees.broker_relations = 5;
        let m1 = scan(&books, &hubs, &p, &HashMap::new()).opportunities[0].margin_pct;
        assert!(m1 > m0, "{m0} → {m1}");
        assert!((m1 - 4.6375).abs() < 1e-9, "110×0.95125 − 100 = 4.6375");
    }

    #[test]
    fn drops_want_below_min_batch() {
        let mut p = FlipParams::default();
        p.margin_threshold_pct = -100.0;
        p.min_batch = 600; // want=500 < 600
        p.capital_isk = 1_000_000.0;
        let out = scan(&doc_books(), &hubs_of(&[STATION_JITA, 60015157]), &p, &HashMap::new());
        assert!(out.opportunities.is_empty());
        assert_eq!(out.stats.dropped_batch, 1);
        assert_eq!(out.stats.dropped_shortfall, 0, "want≥min_batch 时深度保证足额，不应计短填");
    }

    #[test]
    fn drops_below_threshold_and_same_station_pairs() {
        let mut p = FlipParams::default();
        p.margin_threshold_pct = 3.0; // 默认费率下该单 margin 为负 → 丢
        p.min_batch = 1;
        p.capital_isk = 1_000_000.0;
        let out = scan(&doc_books(), &hubs_of(&[STATION_JITA, 60015157]), &p, &HashMap::new());
        assert!(out.opportunities.is_empty());
        assert_eq!(out.stats.dropped_threshold, 1);

        // 同站对不出机会：把两本都放同站。
        let same = vec![
            book(STATION_JITA, 34, &[(100.0, 100, 5)], &[]),
            book(STATION_JITA, 34, &[], &[(110.0, 100, 5)]),
        ];
        let out2 = scan(&same, &hubs_of(&[STATION_JITA]), &p, &HashMap::new());
        assert!(out2.opportunities.is_empty());
        assert_eq!(out2.stats.pairs_evaluated, 0, "同站对根本不评");
    }

    #[test]
    fn non_hub_station_is_ignored() {
        let books = vec![
            book(STATION_JITA, 34, &[(100.0, 1000, 5)], &[]),
            book(69999999, 34, &[], &[(130.0, 1000, 5)]), // 不在枢纽池
        ];
        let mut p = FlipParams::default();
        p.margin_threshold_pct = -100.0;
        p.min_batch = 1;
        let out = scan(&books, &hubs_of(&[STATION_JITA]), &p, &HashMap::new());
        assert!(out.opportunities.is_empty(), "池外站点不得参与报价");
        assert_eq!(out.stats.pairs_evaluated, 0);
    }

    #[test]
    fn weighted_price_beats_best_price_for_size() {
        // 卖站买盘 110×100 + 90×100：吃 150 件的加权价 = (110×100+90×50)/150 ≈ 103.33
        let books = vec![
            book(STATION_JITA, 34, &[(100.0, 1000, 5)], &[]),
            book(60015157, 34, &[], &[(110.0, 100, 3), (90.0, 100, 3)]),
        ];
        let mut p = FlipParams::default();
        p.fees.sales_tax_pct = 0.0;
        p.fees.broker_pct = 0.0;
        p.margin_threshold_pct = -100.0;
        p.min_batch = 1;
        p.capital_isk = 100_000_000.0; // 预算远大于深度 → want=200
        let out = scan(&books, &hubs_of(&[STATION_JITA, 60015157]), &p, &HashMap::new());
        let o = &out.opportunities[0];
        assert_eq!(o.qty, 200);
        assert!((o.sell_price - 100.0).abs() < 1e-9, "(110×100+90×100)/200");
        assert!((o.buy_price - 100.0).abs() < 1e-9);
    }

    #[test]
    fn vol24_prefers_history_and_falls_back_to_depth() {
        let books = doc_books();
        let hubs = hubs_of(&[STATION_JITA, 60015157]);
        let mut p = FlipParams::default();
        p.margin_threshold_pct = -100.0;
        p.min_batch = 1;
        p.capital_isk = 1_000_000.0;
        let mut vol = HashMap::new();
        vol.insert(34u32, 1234u64);
        let o = &scan(&books, &hubs, &p, &vol).opportunities[0];
        assert_eq!(o.vol_source, VolSource::History);
        assert_eq!(o.vol24, 1234);
    }

    #[test]
    fn sort_is_score_desc_then_stable_tiebreak() {
        let books = vec![
            book(STATION_JITA, 34, &[(100.0, 1000, 5)], &[]),
            book(60015157, 34, &[], &[(120.0, 1000, 5)]),
            book(STATION_JITA, 32, &[(100.0, 1000, 5)], &[]),
            book(60015157, 32, &[], &[(130.0, 1000, 5)]),
        ];
        let mut p = FlipParams::default();
        p.margin_threshold_pct = -100.0;
        p.min_batch = 1;
        p.capital_isk = 1_000_000.0;
        let out = scan(&books, &hubs_of(&[STATION_JITA, 60015157]), &p, &HashMap::new());
        assert_eq!(out.opportunities.len(), 2);
        assert_eq!(out.opportunities[0].type_id, 32, "更高 margin 在前");

        // 同分场景：两个类型同 margin → 按 type_id 升序稳定排序
        let tie = vec![
            book(STATION_JITA, 34, &[(100.0, 1000, 5)], &[]),
            book(60015157, 34, &[], &[(110.0, 1000, 5)]),
            book(STATION_JITA, 32, &[(100.0, 1000, 5)], &[]),
            book(60015157, 32, &[], &[(110.0, 1000, 5)]),
        ];
        let out2 = scan(&tie, &hubs_of(&[STATION_JITA, 60015157]), &p, &HashMap::new());
        let ids: Vec<u32> = out2.opportunities.iter().map(|o| o.type_id).collect();
        assert_eq!(ids, vec![32, 34]);
    }

    #[test]
    fn trial_shares_the_same_fee_exit() {
        let mut p = FlipParams::default();
        p.fees.sales_tax_pct = 5.0;
        p.fees.broker_pct = 3.0;
        let (per_unit, total, margin) = trial(100.0, 110.0, 10, &p);
        assert!((per_unit - 1.2).abs() < 1e-9);
        assert!((total - 12.0).abs() < 1e-9);
        assert!((margin - 1.2).abs() < 1e-9);

        // 默认官方费率（7.5/3）：同一单是亏的——试算行的价值就在这。
        let d = FlipParams::default();
        let (per_unit2, _, _) = trial(100.0, 110.0, 10, &d);
        assert!(per_unit2 < 0.0, "110×0.895 − 100 = −1.55");
        // 满技能：+4.6375
        let mut max = FlipParams::default();
        max.fees.accounting = 5;
        max.fees.broker_relations = 5;
        let (per_unit3, _, _) = trial(100.0, 110.0, 10, &max);
        assert!((per_unit3 - 4.6375).abs() < 1e-9);
    }

    #[test]
    fn empty_input_yields_no_opportunities() {
        let out = scan(&[], &[], &FlipParams::default(), &HashMap::new());
        assert!(out.opportunities.is_empty());
        assert_eq!(out.stats.pairs_evaluated, 0);
    }
}
