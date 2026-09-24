//! FIFO 成本基准（spec §4.2）：把回填窗内的钱包流水回放成每个类型的单位成本。
//!
//! **纯函数**：无 DB、无网络、无时钟 —— 窗口由调用方按水位切好，这里只重放。
//!
//! 三条口径写在这里（判定侧与卡片口径摘要都按这三条读）：
//! 1. **均价 = 整窗买入的加权平均**（Σ 数量×单价 ÷ Σ 数量）：买 100@10 + 买 100@20 → 15；
//!    再卖掉 100，均价**仍是 15**。它回答的是"这批货我平均花多少钱买的"，
//!    不随卖单成交价跳动。
//! 2. **数量按 FIFO 消耗**：卖单从最早的批次开始扣，队尾是最近买的那批。
//! 3. **覆盖不到就标未知**：有卖无买 / 卖超窗内买入 / 卖空（队列被清空）→ `Unknown`。
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
    /// 窗外（有卖无买 / 超卖 / 已卖空）：不参与判定。
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
    /// 各批次**剩余数量**，队首最早。只记数量：均价口径是整窗买入加权平均（头注释第 1 条），
    /// 批次价格根本不参与计算，多存一份只会多出一条会漂移的真相。
    queue: VecDeque<u64>,
    buy_qty: u64,
    buy_notional: f64,
    /// 窗内覆盖失败过（超卖/卖空）。一次即定：仓位历史已经早于回填窗，
    /// 之后"已知"的那部分不再代表实际持仓，继续用它判定等于用假成本。
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
                lots.queue.push_back(t.quantity);
                lots.buy_qty += t.quantity;
                lots.buy_notional += t.unit_price * t.quantity as f64;
            }
            continue;
        }

        // 卖单：从队首扣数量（先进先出）。
        let mut left = t.quantity;
        while left > 0 {
            match lots.queue.front_mut() {
                Some(rest) if *rest <= left => {
                    left -= *rest;
                    lots.queue.pop_front();
                }
                Some(rest) => {
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
        if lots.queue.is_empty() {
            // 卖空：窗内已经没有持仓了。空仓没有成本可言，同样是未知而不是 0。
            lots.uncovered = true;
        }
    }

    book.into_iter()
        .map(|(type_id, lots)| (type_id, finish(&lots)))
        .collect()
}

/// 收口：一次买入都没有（有卖无买）也是未知 —— 那正是 90 天窗的典型形态。
fn finish(lots: &Lots) -> FifoCost {
    if lots.uncovered || lots.buy_qty == 0 {
        return FifoCost::unknown();
    }
    FifoCost::known(lots.buy_notional / lots.buy_qty as f64)
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
        // 买 100@10、买 100@20 → 持 200，均价 15
        // 卖 100 → 剩余 100，均价仍 15（FIFO 消耗最早那批）
        // 再卖 100 → 清空，成本未知
        let mut txs = vec![
            buy(1, "2026-09-01T00:00:00Z", 100, 10.0),
            buy(2, "2026-09-02T00:00:00Z", 100, 20.0),
        ];
        let costs = fifo_costs(&txs);
        assert_eq!(costs[&TYPE].source, CostSource::Known);
        assert_eq!(costs[&TYPE].known_price(), Some(15.0), "未卖之前的整窗买入均价");

        txs.push(sell(3, "2026-09-03T00:00:00Z", 100, 30.0));
        let costs = fifo_costs(&txs);
        assert_eq!(costs[&TYPE].source, CostSource::Known, "还剩 100，窗内覆盖得到");
        assert_eq!(
            costs[&TYPE].known_price(),
            Some(15.0),
            "FIFO 消耗的是最早那批的数量，均价口径不随卖单跳动"
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
