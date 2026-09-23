//! 订单与站点形态。字段清单取自 2026-09-23 实测响应（方案 v3.1 附录 A.5）。

use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

/// ESI 订单。`location_id` 用 `u64` —— 13 位玩家结构 ID 会溢出 `i32`
/// （`/v1/universe/names` 就是栽在这里返 400）。
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Order {
    #[serde(rename = "order_id")]
    pub id: u64,
    #[serde(rename = "type_id")]
    pub type_id: u32,
    #[serde(rename = "location_id")]
    pub location_id: u64,
    #[serde(rename = "system_id")]
    pub system_id: u32,
    #[serde(rename = "is_buy_order")]
    pub is_buy: bool,
    pub price: f64,
    #[serde(rename = "volume_remain")]
    pub volume_remain: u64,
    #[serde(rename = "volume_total")]
    pub volume_total: u64,
    #[serde(rename = "min_volume")]
    pub min_volume: u64,
    pub duration: u32,
    pub issued: DateTime<Utc>,
    #[serde(default)]
    pub range: Option<String>,
}

impl Order {
    /// 方案 §4.1：参与倒卖定价的订单，`issued` 年龄超阈值默认剔除。
    /// 实测全星域 34.5% 订单挂单 > 30 天，且吉他 **18.1% 的买一价来自这类僵尸单**。
    pub fn is_stale(&self, now: DateTime<Utc>, max_age: Duration) -> bool {
        now - self.issued > max_age
    }

    /// `min_volume > 1` 是批发单，实测仅 661/408,033 = 0.2%，不能混进普通深度。
    pub fn is_wholesale(&self) -> bool {
        self.min_volume > 1
    }

    /// 有效可成交量：批发单要按 `min_volume` 起批，一次吃不满就得整单量。
    pub fn fillable_at_least(&self, want: u64) -> u64 {
        if self.min_volume <= 1 {
            self.volume_remain
        } else {
            let lots = want.div_ceil(self.min_volume);
            lots.saturating_mul(self.min_volume).min(self.volume_remain)
        }
    }
}

/// 站点形态。规则取自方案 §3.4 的实测纠错：NPC 站是**8 位且以 6 开头**，
/// 不是 v3.0 写的 `6000xxxx` —— 那会静默丢掉排名第 4 的 Kisogo VII（`60015157`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationKind {
    NpcStation,
    PlayerStructure,
    Unknown,
}

impl LocationKind {
    pub fn of(id: u64) -> Self {
        let s = id.to_string();
        if s.len() == 8 && s.starts_with('6') {
            Self::NpcStation
        } else if s.len() == 13 {
            Self::PlayerStructure
        } else {
            Self::Unknown
        }
    }

    /// 只有 NPC 站参与公共市场报价：13 位结构的 `universe/names` 返回 400、
    /// `universe/structures/{id}` 返回 401，名字都拿不到，更谈不上公开可交易。
    pub fn tradable_publicly(self) -> bool {
        matches!(self, Self::NpcStation)
    }
}

/// `/v3/markets/{region}/orders` 的两种真实形态。
/// 实测：带 `order_type=all` 的分页响应是 `{"orders":[...],"pagination":{...}}`，
/// 而 `?type_id=` 的单页响应是**裸数组**。反序列化必须同时吃下，否则 T1.5 直接解析失败。
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OrdersResponse {
    Bare(Vec<Order>),
    Wrapped {
        orders: Vec<Order>,
        #[serde(default)]
        pagination: Option<Pagination>,
    },
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Pagination {
    pub page: Option<u32>,
    pub limit: Option<u32>,
    pub total: Option<u32>,
    pub pages: Option<u32>,
}

impl OrdersResponse {
    pub fn into_orders(self) -> Vec<Order> {
        match self {
            OrdersResponse::Bare(v) => v,
            OrdersResponse::Wrapped { orders, .. } => orders,
        }
    }

    pub fn pages(&self) -> Option<u32> {
        match self {
            OrdersResponse::Bare(_) => None,
            OrdersResponse::Wrapped { pagination, .. } => pagination.and_then(|p| p.pages),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::STATION_JITA;
    use chrono::TimeZone;

    const RAW: &str = r#"{"duration":90,"is_buy_order":false,"issued":"2026-09-16T08:41:39Z",
      "location_id":60005458,"min_volume":1,"order_id":7423712969,"price":4.0,
      "range":"region","system_id":30000128,"type_id":34,
      "volume_remain":8860466,"volume_total":8860537}"#;

    #[test]
    fn parses_a_real_order_record() {
        let o: Order = serde_json::from_str(RAW).unwrap();
        assert_eq!(o.id, 7423712969);
        assert_eq!(o.type_id, 34);
        assert_eq!(o.location_id, 60005458);
        assert!(!o.is_buy);
        assert_eq!(o.price, 4.0);
        assert_eq!(o.volume_remain, 8860466);
        assert_eq!(o.issued.to_rfc3339(), "2026-09-16T08:41:39+00:00");
    }

    #[test]
    fn accepts_both_response_shapes() {
        let bare: OrdersResponse =
            serde_json::from_str(r#"[{"duration":90,"is_buy_order":true,"issued":"2026-09-16T08:41:39Z","location_id":60003760,"min_volume":1,"order_id":1,"price":5.0,"range":"region","system_id":30000142,"type_id":34,"volume_remain":1,"volume_total":1}]"#)
                .unwrap();
        assert_eq!(bare.pages(), None);
        assert_eq!(bare.into_orders().len(), 1);

        let wrapped: OrdersResponse = serde_json::from_str(
            r#"{"orders":[],"pagination":{"page":2,"pages":409,"limit":1000,"total":408033}}"#,
        )
        .unwrap();
        assert_eq!(wrapped.pages(), Some(409));
    }

    #[test]
    fn npc_station_rule_covers_6001xxxx() {
        assert_eq!(LocationKind::of(STATION_JITA), LocationKind::NpcStation);
        // v3.0 的 `6000xxxx` 前缀会把这些误判为非 NPC 站。
        assert_eq!(LocationKind::of(60015157), LocationKind::NpcStation);
        assert_eq!(LocationKind::of(60015027), LocationKind::NpcStation);
        assert_eq!(
            LocationKind::of(1044752365771),
            LocationKind::PlayerStructure
        );
        assert!(!LocationKind::PlayerStructure.tradable_publicly());
    }

    #[test]
    fn staleness_uses_the_45d_boundary() {
        // RAW 的 issued 是 2026-09-16，距 now 恰好 7 天。
        let fresh: Order = serde_json::from_str(RAW).unwrap();
        let old: Order = serde_json::from_str(&RAW.replace("2026-09-16", "2026-06-01")).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 9, 23, 12, 0, 0).unwrap();

        assert!(!fresh.is_stale(now, Duration::days(30)));
        assert!(!fresh.is_stale(now, Duration::days(45)));
        assert!(old.is_stale(now, Duration::days(45)), "114 天必须判为僵尸单");
        assert!(!old.is_stale(now, Duration::days(120)));
    }

    #[test]
    fn wholesale_orders_round_up_to_lot_size() {
        let raw = RAW.replace("\"min_volume\":1", "\"min_volume\":1000");
        let o: Order = serde_json::from_str(&raw).unwrap();
        assert!(o.is_wholesale());
        assert_eq!(o.fillable_at_least(1500), 2000.min(o.volume_remain));
        assert_eq!(o.fillable_at_least(0), 0);
    }

    #[test]
    fn unknown_location_shape_is_not_tradable() {
        assert_eq!(LocationKind::of(123), LocationKind::Unknown);
        assert!(!LocationKind::Unknown.tradable_publicly());
    }
}
