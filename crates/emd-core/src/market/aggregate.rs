//! 订单 → 站点单簿聚合，以及方案 §4.1 的"可执行价格"计算。
//!
//! 两条来自实测的硬过滤，缺一条就会大量误报：
//! - **薄档位**：全星域 91,544 个 `(type, location, 方向)` 组合里 59.7% 只有 1 条订单。
//! - **僵尸单**：34.5% 的订单挂单超 30 天，吉他 18.1% 的买一价由它们构成。

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::market::entities::{LocationKind, Order};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: f64,
    pub volume: u64,
    /// 同价位的订单笔数 —— 薄档判定的依据。
    pub orders: u32,
}

/// 某站在某类型上的买卖单簿快照。对应 `station_orders` 表。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StationOrderBook {
    pub location_id: u64,
    pub type_id: u32,
    pub is_npc_station: bool,
    pub best_bid: Option<f64>,
    pub bid_qty: u64,
    pub best_ask: Option<f64>,
    pub ask_qty: u64,
    /// 该方向的**订单笔数**（不是价位个数）—— 薄档判定看这个。
    pub bid_levels: u32,
    pub ask_levels: u32,
    pub bid_depth: Vec<PriceLevel>,
    pub ask_depth: Vec<PriceLevel>,
    /// 参与聚合的被过滤订单数，UI 要显示，否则用户不知道数据为何"变少了"。
    pub skipped_stale: u32,
    pub skipped_thin: u32,
    pub skipped_wholesale: u32,
}

#[derive(Debug, Clone)]
pub struct AggregateOptions {
    pub now: DateTime<Utc>,
    /// 超过此年龄的订单不参与定价。实测默认 45 天。
    pub max_age: Duration,
    /// 档位笔数下限。低于此值视为薄档，整个方向作废。
    pub min_levels: u32,
    /// 深度保留前几档。存得越多 SQLite 写盘越大，5 档够用。
    pub depth_levels: usize,
}

impl Default for AggregateOptions {
    fn default() -> Self {
        Self {
            now: Utc::now(),
            max_age: Duration::days(45),
            min_levels: 3,
            depth_levels: 5,
        }
    }
}

#[derive(Default)]
struct Ladder {
    /// 键是价格的位模式 —— `f64` 不满足 `Eq + Hash`，而"同价"在这里就是逐位相等。
    at_price: HashMap<u64, PriceAggregate>,
    stale: u32,
    wholesale: u32,
}

#[derive(Debug, Clone, Copy)]
struct PriceAggregate {
    price: f64,
    volume: u64,
    orders: u32,
}

impl Ladder {
    fn push(&mut self, o: &Order) {
        let e = self
            .at_price
            .entry(o.price.to_bits())
            .or_insert(PriceAggregate {
                price: o.price,
                volume: 0,
                orders: 0,
            });
        e.volume = e.volume.saturating_add(o.volume_remain);
        e.orders += 1;
    }

    fn order_count(&self) -> u32 {
        self.at_price.values().map(|v| v.orders).sum()
    }

    /// 买盘从高到低，卖盘从低到高。
    fn levels(&self, ascending: bool) -> Vec<PriceLevel> {
        let mut v: Vec<PriceLevel> = self
            .at_price
            .values()
            .map(|a| PriceLevel {
                price: a.price,
                volume: a.volume,
                orders: a.orders,
            })
            .collect();
        if ascending {
            v.sort_by(|a, b| a.price.total_cmp(&b.price));
        } else {
            v.sort_by(|a, b| b.price.total_cmp(&a.price));
        }
        v
    }
}

/// 聚合。返回按 `(location_id, type_id)` 唯一的单簿集合，已剔除薄档方向。
pub fn aggregate(orders: &[Order], opts: &AggregateOptions) -> Vec<StationOrderBook> {
    let mut grouped: HashMap<(u64, u32), (Ladder, Ladder)> = HashMap::new();

    for o in orders {
        let entry = grouped
            .entry((o.location_id, o.type_id))
            .or_insert_with(|| (Ladder::default(), Ladder::default()));
        let ladder = if o.is_buy { &mut entry.0 } else { &mut entry.1 };

        if o.is_wholesale() {
            ladder.wholesale += 1;
            continue;
        }
        if o.is_stale(opts.now, opts.max_age) {
            ladder.stale += 1;
            continue;
        }
        ladder.push(o);
    }

    let mut out = Vec::with_capacity(grouped.len());
    for ((location_id, type_id), (mut bids, mut asks)) in grouped {
        let bid_levels = bids.order_count();
        let ask_levels = asks.order_count();

        // 薄档判定：按订单笔数而非价位个数 —— 3 个价位但只有 3 条单照样是薄档。
        let bid_ok = bid_levels >= opts.min_levels;
        let ask_ok = ask_levels >= opts.min_levels;
        if !bid_ok {
            bids.at_price.clear();
        }
        if !ask_ok {
            asks.at_price.clear();
        }
        if bids.at_price.is_empty() && asks.at_price.is_empty() {
            continue;
        }

        let bid_book = bids.levels(false);
        let ask_book = asks.levels(true);
        let skipped_stale = bids.stale + asks.stale;
        let skipped_wholesale = bids.wholesale + asks.wholesale;
        let skipped_thin = if bid_ok { 0 } else { bid_levels } + if ask_ok { 0 } else { ask_levels };

        out.push(StationOrderBook {
            location_id,
            type_id,
            is_npc_station: LocationKind::of(location_id).tradable_publicly(),
            best_bid: bid_book.first().map(|l| l.price),
            bid_qty: bid_book.first().map(|l| l.volume).unwrap_or(0),
            best_ask: ask_book.first().map(|l| l.price),
            ask_qty: ask_book.first().map(|l| l.volume).unwrap_or(0),
            bid_levels,
            ask_levels,
            bid_depth: bid_book.into_iter().take(opts.depth_levels).collect(),
            ask_depth: ask_book.into_iter().take(opts.depth_levels).collect(),
            skipped_stale,
            skipped_thin,
            skipped_wholesale,
        });
    }

    out.sort_by(|a, b| {
        (a.type_id, a.location_id).cmp(&(b.type_id, b.location_id))
    });
    out
}

/// 吃单方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// 买：吃卖盘，价格从低到高。
    Buy,
    /// 卖：吃买盘，价格从高到低。
    Sell,
}

impl StationOrderBook {
    fn ladder(&self, side: Side) -> &[PriceLevel] {
        match side {
            Side::Buy => &self.ask_depth,
            Side::Sell => &self.bid_depth,
        }
    }

    /// 加权成交均价。方案 §4.1：一律用吃单到目标量时的加权价，不用 best bid/ask。
    /// 返回 `(加权均价, 实际成交量)`；量不足时 `filled < want`，调用方必须据此降级。
    pub fn executable(&self, side: Side, want: u64) -> Option<(f64, u64)> {
        let mut cost = 0f64;
        let mut got = 0u64;
        for l in self.ladder(side) {
            if got >= want {
                break;
            }
            let take = (want - got).min(l.volume);
            cost += take as f64 * l.price;
            got += take;
        }
        if got == 0 {
            return None;
        }
        Some((cost / got as f64, got))
    }

    /// 该方向可执行的总深度。
    pub fn capacity(&self, side: Side) -> u64 {
        self.ladder(side).iter().map(|l| l.volume).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::STATION_JITA;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 23, 12, 0, 0).unwrap()
    }

    fn order(price: f64, is_buy: bool, vol: u64, age_days: i64) -> Order {
        let issued = now() - Duration::days(age_days);
        Order {
            id: (price * 1000.0) as u64 + vol + is_buy as u64,
            type_id: 34,
            location_id: STATION_JITA,
            system_id: 30000142,
            is_buy,
            price,
            volume_remain: vol,
            volume_total: vol,
            min_volume: 1,
            duration: 90,
            issued,
            range: Some("region".into()),
        }
    }

    fn opts(min: u32) -> AggregateOptions {
        AggregateOptions {
            now: now(),
            min_levels: min,
            ..Default::default()
        }
    }

    #[test]
    fn builds_best_bid_and_ask() {
        let orders = vec![
            order(4.0, false, 100, 1),
            order(4.2, false, 50, 1),
            order(3.9, true, 80, 1),
            order(3.7, true, 200, 1),
        ];
        let books = aggregate(&orders, &opts(1));
        assert_eq!(books.len(), 1);
        let b = &books[0];
        assert_eq!(b.best_ask, Some(4.0));
        assert_eq!(b.ask_qty, 100);
        assert_eq!(b.best_bid, Some(3.9));
        assert!(b.is_npc_station);
        // 卖盘升序、买盘降序
        assert_eq!(b.ask_depth[0].price, 4.0);
        assert_eq!(b.bid_depth[0].price, 3.9);
    }

    #[test]
    fn same_price_orders_merge_into_one_level() {
        let orders = vec![
            order(4.0, false, 10, 1),
            order(4.0, false, 20, 1),
            order(4.0, false, 30, 2),
        ];
        let books = aggregate(&orders, &opts(1));
        assert_eq!(books[0].ask_depth.len(), 1);
        assert_eq!(books[0].ask_depth[0].volume, 60);
        assert_eq!(books[0].ask_levels, 3, "价位 1 个，但笔数应为 3");
    }

    #[test]
    fn thin_side_is_dropped_without_killing_the_book() {
        // 买盘只有 1 条 → 整个买侧作废，卖侧保留。这就是 v3.0 §4.1 缺的那道闸。
        let orders = vec![
            order(4.0, false, 100, 1),
            order(4.1, false, 100, 1),
            order(4.2, false, 100, 1),
            order(9.9, true, 5, 1),
        ];
        let books = aggregate(&orders, &opts(3));
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].best_bid, None, "薄买盘必须被剔除");
        assert_eq!(books[0].best_ask, Some(4.0));
        assert_eq!(books[0].skipped_thin, 1);
    }

    #[test]
    fn stale_orders_do_not_set_the_price() {
        let orders = vec![
            order(9.9, true, 1000, 60), // 僵尸高价买盘
            order(4.0, true, 100, 2),
            order(4.1, true, 100, 2),
            order(4.2, true, 100, 2),
        ];
        let books = aggregate(&orders, &opts(3));
        assert_eq!(books[0].best_bid, Some(4.2), "买一必须是新单里的最高价");
        assert_eq!(books[0].skipped_stale, 1);
    }

    #[test]
    fn wholesale_orders_are_separated() {
        let mut o = order(2.0, false, 5000, 1);
        o.min_volume = 1000;
        let orders = vec![
            o,
            order(4.0, false, 10, 1),
            order(4.1, false, 10, 1),
            order(4.2, false, 10, 1),
        ];
        let books = aggregate(&orders, &opts(3));
        assert_eq!(books[0].best_ask, Some(4.0), "批发单不得成为卖一");
        assert_eq!(books[0].skipped_wholesale, 1);
    }

    #[test]
    fn executable_price_is_volume_weighted() {
        let orders = vec![
            order(4.0, false, 100, 1),
            order(4.2, false, 100, 1),
            order(5.0, false, 100, 1),
            order(3.5, true, 100, 1),
            order(3.4, true, 100, 1),
            order(3.3, true, 100, 1),
        ];
        let books = aggregate(&orders, &opts(3));
        let b = &books[0];
        // 吃 150 件卖单：100@4.0 + 50@4.2
        let (avg, filled) = b.executable(Side::Buy, 150).unwrap();
        assert_eq!(filled, 150);
        assert!((avg - 4.066_666_7).abs() < 1e-6);
        assert_eq!(b.capacity(Side::Buy), 300);
        assert_eq!(b.capacity(Side::Sell), 300);
    }

    #[test]
    fn executable_reports_short_fill_instead_of_lying() {
        let orders = vec![
            order(4.0, false, 10, 1),
            order(4.1, false, 10, 1),
            order(4.2, false, 10, 1),
            order(3.5, true, 10, 1),
            order(3.4, true, 10, 1),
            order(3.3, true, 10, 1),
        ];
        let b = &aggregate(&orders, &opts(3))[0];
        let (avg, filled) = b.executable(Side::Buy, 500).unwrap();
        assert_eq!(filled, 30, "深度不足时必须如实回报");
        assert!(avg > 4.0);
        assert_eq!(b.executable(Side::Buy, 0), None);
    }

    #[test]
    fn depth_is_truncated_to_five_levels() {
        let mut orders = Vec::new();
        for i in 0..12 {
            orders.push(order(4.0 + i as f64 * 0.1, false, 10, 1));
            orders.push(order(3.0 - i as f64 * 0.1, true, 10, 1));
        }
        let b = &aggregate(&orders, &opts(3))[0];
        assert_eq!(b.ask_depth.len(), 5);
        assert_eq!(b.bid_depth.len(), 5);
        assert_eq!(b.ask_levels, 12, "levels 记全量，depth 只存前 5");
    }

    #[test]
    fn empty_input_yields_no_books() {
        assert!(aggregate(&[], &opts(3)).is_empty());
    }
}
