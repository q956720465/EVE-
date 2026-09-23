//! 枢纽池（方案 v3.1 §3.4）。
//!
//! v3.0 只说"站点清单必须动态生成"却没给条件；实测 357 个 location 里
//! 有一大半是个位数订单的死站，拿去比价差只会造出假机会。这里的门槛全部可量化：
//! NPC 站 + 本轮订单数 ≥ 50 + 按订单数取前 20。

use std::collections::HashMap;

use crate::market::entities::{LocationKind, Order};

/// 实测：装机即用一轮里 54 个站点能过 `min_orders` 门槛，前 20 覆盖 91.7% 订单量。
pub const DEFAULT_MIN_ORDERS: u64 = 50;
pub const DEFAULT_TOP: usize = 20;

#[derive(Debug, Clone, PartialEq)]
pub struct Hub {
    pub location_id: u64,
    pub order_count: u64,
    /// 占本轮全部订单的比例。
    pub share_pct: f64,
    /// 1 起。
    pub rank: usize,
}

/// 按"每站订单数"排序取枢纽池。只有 NPC 站有资格进池 —— 实测 13 位玩家结构占
/// The Forge 7.6% 的订单量（第 2、3 名都是它俩），但名字与税率都不可公开获取，
/// 拿去报价只会造出无法执行的假机会。
///
/// 比例的分母是**全部**订单（含玩家结构），这样 UI 上"吉他 81%"与附录 A.5 的
/// 实测口径一致；若只按 NPC 站算，吉他会变成 100%，是个误导数。
pub fn hub_pool(orders: &[Order], min_orders: u64, top: usize) -> Vec<Hub> {
    let mut counts: HashMap<u64, u64> = HashMap::new();
    let total = orders.len() as u64;
    if total == 0 {
        return Vec::new();
    }
    for o in orders {
        if !LocationKind::of(o.location_id).tradable_publicly() {
            continue;
        }
        *counts.entry(o.location_id).or_default() += 1;
    }

    let mut hubs: Vec<(u64, u64)> = counts
        .into_iter()
        .filter(|(_, c)| *c >= min_orders)
        .collect();
    // 订单数相同则按 ID 稳定排序，避免每轮名次抖动导致 UI 列表乱跳。
    hubs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    hubs.into_iter()
        .take(top)
        .enumerate()
        .map(|(i, (location_id, order_count))| Hub {
            location_id,
            order_count,
            share_pct: order_count as f64 * 100.0 / total as f64,
            rank: i + 1,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{STATION_JITA, REGION_FORGE};
    use chrono::{TimeZone, Utc};

    fn order_at(loc: u64, ty: u32) -> Order {
        Order {
            id: loc * 1_000_000 + ty as u64,
            type_id: ty,
            location_id: loc,
            system_id: 30000142,
            is_buy: false,
            price: 1.0,
            volume_remain: 1,
            volume_total: 1,
            min_volume: 1,
            duration: 90,
            issued: Utc.with_ymd_and_hms(2026, 9, 23, 1, 0, 0).unwrap(),
            range: Some("region".into()),
        }
    }

    fn fill(loc: u64, n: u64) -> Vec<Order> {
        (0..n).map(|i| order_at(loc, i as u32)).collect()
    }

    #[test]
    fn ranks_by_order_count_and_reports_share() {
        let mut orders = fill(STATION_JITA, 100);
        orders.extend(fill(60015157, 60));
        orders.extend(fill(60003754, 10));
        let hubs = hub_pool(&orders, 50, 20);
        assert_eq!(hubs.len(), 2, "10 单的小站必须被门槛挡掉");
        assert_eq!(hubs[0].location_id, STATION_JITA);
        assert_eq!(hubs[0].rank, 1);
        assert!((hubs[0].share_pct - 58.82).abs() < 0.01, "{:?}", hubs[0]);
        assert_eq!(hubs[1].location_id, 60015157);
    }

    #[test]
    fn player_structures_are_excluded_even_when_huge() {
        // 实测 1044752365771 占该星域 5.9% 订单，排第二 —— 但它不是公共市场。
        let mut orders = fill(STATION_JITA, 80);
        orders.extend(fill(1044752365771, 1000));
        let hubs = hub_pool(&orders, 50, 20);
        assert_eq!(hubs.len(), 1);
        assert_eq!(hubs[0].location_id, STATION_JITA);
        // 80 NPC 单 + 1000 玩家结构单：占比按全量分母算，才不会被读成"吉他垄断"。
        assert!((hubs[0].share_pct - 7.41).abs() < 0.01, "{:?}", hubs[0]);
    }

    #[test]
    fn top_n_caps_the_pool() {
        let mut orders = Vec::new();
        for i in 0..40u64 {
            orders.extend(fill(60000000 + i, 60 + i));
        }
        let hubs = hub_pool(&orders, 50, 20);
        assert_eq!(hubs.len(), 20);
        assert_eq!(hubs[0].location_id, 60000039, "订单最多的排前");
        assert_eq!(hubs.last().unwrap().rank, 20);
    }

    #[test]
    fn ties_break_stably_by_id() {
        let mut orders = fill(60000002, 60);
        orders.extend(fill(60000001, 60));
        let hubs = hub_pool(&orders, 50, 20);
        assert_eq!(hubs[0].location_id, 60000001, "同单数按 ID 升序，避免每轮抖动");
    }

    #[test]
    fn empty_or_all_thin_input_yields_no_hubs() {
        assert!(hub_pool(&[], 50, 20).is_empty());
        assert!(hub_pool(&fill(STATION_JITA, 3), 50, 20).is_empty());
    }

    #[test]
    fn defaults_match_the_measured_round() {
        assert_eq!(DEFAULT_MIN_ORDERS, 50);
        assert_eq!(DEFAULT_TOP, 20);
        // REGION_FORGE 只用于确认常量可导入（真正的对账在 store 层测试里）。
        assert_eq!(REGION_FORGE, 10000002);
    }
}
