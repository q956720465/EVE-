//! FIFO 成本基准（spec §4.2）：把回填窗内的钱包流水回放成每个类型的单位成本。
//!
//! **纯函数**：无 DB、无网络、无时钟 —— 窗口由调用方按水位切好，这里只重放。
//!
//! 三条口径写在这里（判定侧与卡片口径摘要都按这三条读）：
//! 1. **均价 = 手上剩余持仓的加权平均**（Σ 剩余数量×批次单价 ÷ Σ 剩余数量）：
//!    买 100@10 + 买 100@20 → 15（还没卖，剩余就是整窗买入）；卖掉 100 之后 → **20**
//!    （FIFO 吃掉的是 @10 那批，手里只剩 @20 那批）。它回答的是"我现在拿着的这批货
//!    花了我多少钱"，所以随持仓走，不随卖单成交价跳动。
//!    口径取持仓而不是整窗买入均价：判定问的就是手上的货值不值当前挂价，整窗均价会
//!    **低估**持仓成本，`净额 < 全成本` 更难成立 —— 真亏的那些反而漏报，
//!    与这个功能存在的理由正相反（spec §4.3 ① 的 `单位全成本` 正是以这个成本起算）。
//! 2. **数量按 FIFO 消耗**：卖单从最早的批次开始扣，队尾是最近买的那批。
//! 3. **覆盖不到就标未知**：有卖无买 / 卖超窗内买入 → `Unknown`（卖掉的货有一部分
//!    是窗外买的，窗内账本从那一笔起就说不清持仓与成本，一次即定）；
//!    收尾时队列为空、也就是手上没有持仓（含整窗只有 0 数量买入）同样是 `Unknown`。
//!    **卖空本身不算覆盖失败**：窗内卖光又在窗内买回，剩下的货完全由窗内买入解释，
//!    仍按剩余批次算 —— 否则一次完整回转（flip 的正常收尾）就把最活跃的那批货
//!    永久钉成未知，负收益提醒恰好对它们失效。
//!    未知**不是 0** —— 0 成本会让每一笔卖出都算成亏损，造出满屏假告警
//!    （spec §4.2 明说这类类型不参与判定）；因此 `Unknown` 的价格取 `NaN`，
//!    误用会立刻传播成 NaN，而不是留下一个看着合法的数。

use std::collections::{HashMap, VecDeque};

use crate::store::WalletTx;

/// 成本来源。**判定侧先看它，再看价格**（`source == Unknown` 的类型必须整个跳过）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostSource {
    /// 回填窗覆盖到了：价格可信。
    Known,
    /// 窗外（有卖无买 / 超卖）或收尾时手上没有持仓：不参与判定。
    Unknown,
}

/// 一个类型的成本基准。
#[derive(Debug, Clone, Copy)]
pub struct FifoCost {
    /// 单位成本。`source == Unknown` 时是 `NaN`（见模块头注释第 3 条）。
    pub avg_cost: f64,
    pub source: CostSource,
}

impl FifoCost {
    fn known(avg_cost: f64) -> Self {
        Self {
            avg_cost,
            source: CostSource::Known,
        }
    }

    fn unknown() -> Self {
        Self {
            avg_cost: f64::NAN,
            source: CostSource::Unknown,
        }
    }

    /// 判定侧唯一的取价入口：`Unknown` 拿不到价格，于是"拿 0 当成本"这件事在类型上就写不出来。
    pub fn known_price(&self) -> Option<f64> {
        (self.source == CostSource::Known).then_some(self.avg_cost)
    }
}

/// 一个类型的出货账。
#[derive(Default)]
struct Lots {
    /// 各批次**剩余数量 + 该批单价**，队首最早。单价必须逐批存：口径是"剩余持仓花多少钱
    /// 买的"（头注释第 1 条），被 FIFO 消耗掉的那批，它的价格要跟着一起消失。
    queue: VecDeque<(u64, f64)>,
    /// 窗内覆盖失败过（有卖无买 / 超卖）。一次即定：卖掉的货有一部分是窗外买的，
    /// 从这一笔起窗内账本就不知道手上到底有多少货、花的什么钱，之后再买到的数量
    /// 不等于实际持仓，拿它判定等于用假成本。
    uncovered: bool,
}

/// 重放整段流水，给出每个出现过（买入或卖出）的类型的成本基准。
pub fn fifo_costs(txs: &[WalletTx]) -> HashMap<u32, FifoCost> {
    // 升序重放。同日用 transaction_id 兜底 —— 与 `load_char_tx` 的排序键一致，
    // 否则同一天两笔的先后会随调用方传入的顺序漂移，成本基准跟着抖。
    let mut ordered: Vec<&WalletTx> = txs.iter().collect();
    ordered.sort_by_key(|t| (t.date.as_str(), t.transaction_id));

    let mut book: HashMap<u32, Lots> = HashMap::new();
    for t in ordered {
        let lots = book.entry(t.type_id).or_default();
        if t.is_buy {
            if t.quantity > 0 {
                lots.queue.push_back((t.quantity, t.unit_price));
            }
            continue;
        }

        // 卖单：从队首扣数量（先进先出）。被扣掉的那批价格随它一起消失 ——
        // 均价只看剩余持仓（头注释第 1 条），消耗掉的批次不该继续影响成本。
        let mut left = t.quantity;
        while left > 0 {
            match lots.queue.front_mut() {
                Some((rest, _)) if *rest <= left => {
                    left -= *rest;
                    lots.queue.pop_front();
                }
                Some((rest, _)) => {
                    *rest -= left;
                    left = 0;
                }
                None => {
                    // 卖得比窗内买到的还多：卖掉的一部分来自窗外，成本无从谈起。
                    lots.uncovered = true;
                    break;
                }
            }
        }
        // 这里**不**因为队列被卖空就标未知：清仓不是覆盖失败 —— 窗内卖光之后又在窗内
        // 买回，剩下的货完全由窗内买入解释（回归测试
        // `emptying_then_rebuying_within_the_window_stays_known`）；而"手上没货"该不该
        // 算未知是收尾状态，由 `finish()` 的 held_qty == 0 分支一处判定就够，
        // 在这里提前钉死会把后面那些本来解释得清的成本一起毒掉。
    }

    book.into_iter()
        .map(|(type_id, lots)| (type_id, finish(&lots)))
        .collect()
}

/// 收口：均价取**剩余持仓**的加权平均 —— FIFO 已经吃掉最早那几批，手上剩的是后面批次，
/// 它们的价格才是"我现在拿着的这批货花了我多少钱"。队列空（有卖无买 / 卖超 / 卖空 /
/// 只有 0 数量买入）就是手上没有货，同样是未知而不是 0。
fn finish(lots: &Lots) -> FifoCost {
    if lots.uncovered {
        return FifoCost::unknown();
    }
    let mut held_qty: u64 = 0;
    let mut held_notional = 0.0;
    for (rest, price) in &lots.queue {
        held_qty += *rest;
        held_notional += *rest as f64 * *price;
    }
    if held_qty == 0 {
        return FifoCost::unknown();
    }
    FifoCost::known(held_notional / held_qty as f64)
}

#[cfg(test)]
mod fifo_tests {
    use super::*;

    const TYPE: u32 = 34;

    fn tx(id: i64, date: &str, is_buy: bool, quantity: u64, unit_price: f64) -> WalletTx {
        WalletTx {
            transaction_id: id,
            date: date.to_string(),
            type_id: TYPE,
            location_id: 60_003_760,
            is_buy,
            unit_price,
            quantity,
        }
    }

    fn buy(id: i64, date: &str, quantity: u64, unit_price: f64) -> WalletTx {
        tx(id, date, true, quantity, unit_price)
    }

    fn sell(id: i64, date: &str, quantity: u64, unit_price: f64) -> WalletTx {
        tx(id, date, false, quantity, unit_price)
    }

    #[test]
    fn fifo_averages_buys_and_consumes_on_sells() {
        // 买 100@10、买 100@20 → 持 200，均价 15（还没卖，剩余就是整窗买入）
        // 卖 100 → FIFO 吃掉最早的 @10 那批，手里只剩 @20 那批 → 均价 20
        // 再卖 100 → 清空，成本未知
        let mut txs = vec![
            buy(1, "2026-09-01T00:00:00Z", 100, 10.0),
            buy(2, "2026-09-02T00:00:00Z", 100, 20.0),
        ];
        let costs = fifo_costs(&txs);
        assert_eq!(costs[&TYPE].source, CostSource::Known);
        assert_eq!(
            costs[&TYPE].known_price(),
            Some(15.0),
            "未卖之前剩余 = 整窗买入，均价 15"
        );

        txs.push(sell(3, "2026-09-03T00:00:00Z", 100, 30.0));
        let costs = fifo_costs(&txs);
        assert_eq!(costs[&TYPE].source, CostSource::Known, "还剩 100，窗内覆盖得到");
        assert_eq!(
            costs[&TYPE].known_price(),
            Some(20.0),
            "@10 那批被 FIFO 消耗掉，剩余 100 的成本是 @20 那批的价"
        );

        txs.push(sell(4, "2026-09-04T00:00:00Z", 100, 30.0));
        let costs = fifo_costs(&txs);
        assert_eq!(costs[&TYPE].source, CostSource::Unknown, "卖空 → 未知");
        assert!(
            costs[&TYPE].avg_cost.is_nan(),
            "未知不是 0：0 会被算成'每笔都在亏'"
        );
        assert!(costs[&TYPE].known_price().is_none(), "判定侧取不到价格");
    }

    #[test]
    fn fifo_marks_types_with_sells_but_no_buys_as_cost_unknown() {
        // 首启只回填 90 天：90 天前买的、90 天内卖的 → 有卖出无买入 → 必须标 Unknown。
        // 不能拿 0 当成本（那会造出假亏损）——spec §4.2 明说"覆盖不到的类型标成本未知不参与判定"。
        let only_sells = fifo_costs(&[sell(1, "2026-09-20T00:00:00Z", 500, 12.0)]);
        let got = only_sells
            .get(&TYPE)
            .expect("有卖出的类型必须出现在结果里，判定侧才查得到它是未知");
        assert_eq!(got.source, CostSource::Unknown);
        assert!(got.avg_cost.is_nan(), "未知类型的价格必须是 NaN 而不是 0");
        assert!(got.known_price().is_none());

        // 窗内买过、但卖得比买得多：卖出的一部分成本在窗外 → 同样未知。
        let oversold = fifo_costs(&[
            buy(1, "2026-09-01T00:00:00Z", 100, 10.0),
            sell(2, "2026-09-20T00:00:00Z", 150, 12.0),
        ]);
        assert_eq!(oversold[&TYPE].source, CostSource::Unknown, "超卖 → 覆盖不到");

        // 对照：只卖掉一部分（队列没空、也没超卖）→ 仍算覆盖到。
        let partial = fifo_costs(&[
            buy(1, "2026-09-01T00:00:00Z", 100, 10.0),
            sell(2, "2026-09-20T00:00:00Z", 99, 12.0),
        ]);
        assert_eq!(partial[&TYPE].source, CostSource::Known);
        assert_eq!(partial[&TYPE].known_price(), Some(10.0));
    }

    #[test]
    fn emptying_then_rebuying_within_the_window_stays_known() {
        // 窗内清仓**不算**覆盖失败：买 100@10 → 卖光 100 → 又在窗内买 100@11。
        // 队尾剩的 100 完全由窗内买入解释，成本是 11。若这里判 Unknown，一次完整回转
        // （flip 的正常收尾）就会把该类型钉成未知，最活跃的货反而永久静默 ——
        // 正是这个功能要盯的那批货。收尾时手上没货才算未知（下面第一步）。
        let flat = fifo_costs(&[
            buy(1, "2026-09-01T00:00:00Z", 100, 10.0),
            sell(2, "2026-09-02T00:00:00Z", 100, 12.0),
        ]);
        assert_eq!(
            flat[&TYPE].source,
            CostSource::Unknown,
            "卖光后到此为止 = 手上没有货，收尾仍是未知（空仓没有成本可言）"
        );

        let rebought = fifo_costs(&[
            buy(1, "2026-09-01T00:00:00Z", 100, 10.0),
            sell(2, "2026-09-02T00:00:00Z", 100, 12.0),
            buy(3, "2026-09-03T00:00:00Z", 100, 11.0),
        ]);
        assert_eq!(
            rebought[&TYPE].source,
            CostSource::Known,
            "窗内买回的持仓完全由窗内买入解释，不该被前面那次卖光连坐"
        );
        assert_eq!(
            rebought[&TYPE].known_price(),
            Some(11.0),
            "剩余 100 的成本是买回那批的价 @11"
        );
    }

    #[test]
    fn replay_is_ordered_by_date_not_by_input_order() {
        // 输入顺序里卖单在前（真实数据可能以任意顺序递进来），但重放必须按 date 升序：
        // 先买后卖才有成本可言，反过来的话这笔卖出会被判成"超卖"。
        let txs = vec![
            sell(9, "2026-09-03T00:00:00Z", 5, 20.0),
            buy(7, "2026-09-01T00:00:00Z", 10, 10.0),
        ];
        let forward = fifo_costs(&txs);
        assert_eq!(forward[&TYPE].source, CostSource::Known);

        let mut reversed = txs.clone();
        reversed.reverse();
        let backward = fifo_costs(&reversed);
        assert_eq!(backward[&TYPE].source, forward[&TYPE].source);
        assert_eq!(backward[&TYPE].known_price(), forward[&TYPE].known_price());
    }
}
