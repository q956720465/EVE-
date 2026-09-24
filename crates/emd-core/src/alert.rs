//! 三形态负收益判定与推送契约（spec §4.3）。
//!
//! **纯函数模块**：无 DB、无网络、无时钟 —— 挂单快照 / 流水 / journal 真值 / 名字字典 /
//! `now` 全部由装配层查好传进来，这里只做算术与契约装配。
//!
//! 三形态（与 spec §4.3 ①②③ 逐条对应）：
//! - ① [`detect_expected_sell`]：挂卖单**预期**亏。单位预期净额 = 挂价 × (1 − 有效销售税)；
//!   单位全成本 = FIFO 剩余持仓均价 + 该挂单实付中介费/单位。**预期轨**：税率随技能面板
//!   （[`FeeModel`]）重算 —— 技能一改这一轨的判定跟着变，那正是它存在的意义。
//! - ② [`detect_buy_trap`]：挂买单**套牢**亏。本站当前**可执行**卖出净额（吃买盘到该单剩余量的
//!   加权价 × (1 − 税 − 中介费)）< 买单成交价 + 实付中介费/单位 ⟹ 即时浮亏。预期轨。
//! - ③ [`detect_realized`]：**已实现**成交亏。卖出所得 − 实付销售税 − 被消耗批次的 FIFO 成本
//!   − 卖出侧实付中介费 < 0。**已实现轨**：全部取 journal 真值 —— 这个函数的参数表里根本没有
//!   `FeeModel`，轨道分离不靠注释里的纪律，靠签名（技能面板想借道都进不来）。
//!
//! **成本未知不参与判定**（spec §4.2）：`FifoCost.source == Unknown` 的类型**整个跳过** ——
//! 取价只走 `known_price()`（`Unknown` 时是 `None`），`avg_cost` 里那个 `NaN` 一次都不会被
//! 喂进比较。拿 0 当成本会造出"每笔都在亏"的假告警，那是这个功能最坏的假阳性。
//!
//! **`alert_key` 的规范文本形态**：`order:{id}` / `tx:{id}`，只在 [`order_alert_key`] /
//! [`tx_alert_key`] 一处构造。`alerts.alert_key` 是 **TEXT** 而 `order_id`/`transaction_id` 是
//! **INTEGER** —— SQLite 跨存储类比较把 INTEGER 排在任何 TEXT 之前，`alert_key = <id>` 会
//! **静默返回零行**而不报错。前缀还挡住第二件事：两类轨的 id 各自从 1 开始，裸数字会让
//! "挂单 101"与"成交 101"在同一张表里撞成同一行。
//!
//! 数据可得性的三条边界（口径摘要里如实自报，不藏在代码注释里）：
//! - **实付中介费只在它发生的那一轮 journal 里看得到**（journal 不落表，见 T7 的裁决）：
//!   拿不到就按 0 计 —— 方向是**低估成本 = 宁可漏报**，不会凭空造出告警。
//! - **中介费/单位的分母取挂单剩余量**（本机只存 `volume_remain`，没有挂单原量）：
//!   没成交过的挂单与原量相等（精确），部分成交过的单会偏高（偏向保守）。
//! - **买入侧实付中介费不计入形态 ③**：本机既没有挂单原量、也没有"这几件货来自哪张买单"的
//!   归属数据，按比例摊就是编数字。卖出侧那张单的中介费是精确的，公式串里写明了这一项缺席。
//! - journal 在 ESI 侧**只回溯 30 天**：形态 ③ 的判定面天然只有近 30 天的成交真值。
//!
//! 字段核对：② 的净额系数、③ 的三项构成都逐字照 spec §4.3；`AlertPayload` 的十四个字段
//! 逐字段照抄（推送卡片与提醒中心共用这一份序列化，杜绝双源漂移）。

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::char::fifo::{CostSource, FifoCost};
use crate::char::JournalEntry;
use crate::market::{FeeModel, Side, StationOrderBook};
use crate::store::{CharOrder, WalletTx};

/// alert_key 的两个前缀：规范文本形态只在这里定义，T9 的读写两侧都复用这两个常量。
const KEY_ORDER: &str = "order";
const KEY_TX: &str = "tx";

/// 预期轨的轨道串（推送卡片与提醒中心都读它，别各自拼字面量）。
pub const TRACK_EXPECTED: &str = "预期·估算费率";
/// 已实现轨的轨道串。
pub const TRACK_REALIZED: &str = "已实现·journal 真值";
/// ③ 的技能口径：已实现轨与技能面板无关。
pub const SKILL_NOT_APPLICABLE: &str = "不适用（journal 真值，技能面板不影响已实现轨）";
/// 成本来源：FIFO 90 天回填窗（①②③ 的成本基准都出自它）。
pub const COST_SRC_FIFO90: &str = "FIFO 90 天";
/// 成本来源：② 的成本就是这张买单自己的成交价（不是 FIFO 口径）。
pub const COST_SRC_BUY_TRAP: &str = "买单成交价 + 实付中介费";
/// 成本来源：未知。**带这条来源的条目不会出现在任何 payload 里**（整个跳过判定）——
/// 留在公开词汇表里，是给提醒中心/daemon 标注"哪些类型本轮没参与判定"用的。
pub const COST_SRC_UNKNOWN: &str = "成本未知";

/// 告警形态（spec §4.3 ①②③）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertKind {
    /// ① 挂卖单预期亏（预期轨）。
    ExpectedSellLoss,
    /// ② 挂买单套牢亏（预期轨）。
    BuyOrderTrap,
    /// ③ 已实现成交亏（已实现轨）。
    RealizedLoss,
}

impl AlertKind {
    /// 三种形态，顺序 = spec ①②③。
    pub const ALL: [AlertKind; 3] = [
        Self::ExpectedSellLoss,
        Self::BuyOrderTrap,
        Self::RealizedLoss,
    ];

    /// 落库与进 JSON 的 snake_case 串 —— **枚举与字符串的互转只在这一处**：
    /// `alerts.kind` 列、`alerts.payload` 里的 `kind`、钉钉卡片、daemon 过滤全走它。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExpectedSellLoss => "expected_sell_loss",
            Self::BuyOrderTrap => "buy_order_trap",
            Self::RealizedLoss => "realized_loss",
        }
    }

    /// 照 `OppState::parse` 先例：认不出就 `None`，让调用方决定是跳过还是报错。
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "expected_sell_loss" => Some(Self::ExpectedSellLoss),
            "buy_order_trap" => Some(Self::BuyOrderTrap),
            "realized_loss" => Some(Self::RealizedLoss),
            _ => None,
        }
    }
}

impl Serialize for AlertKind {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AlertKind {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Self::parse(&raw)
            .ok_or_else(|| serde::de::Error::custom(format!("未知的告警形态: {raw}")))
    }
}

/// 口径摘要（spec §4.3）：自报**轨道**与**数字的来源**，卡片读者一眼能分清
/// "这个数是估的还是账上真发生的"。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaliberSummary {
    /// [`TRACK_EXPECTED`] 或 [`TRACK_REALIZED`]。
    pub track: String,
    /// 本条判定**真正用到的**有效销售税（%）。
    pub sales_tax_pct: f64,
    /// 本条判定**真正用到的**有效中介费（%）。
    pub broker_pct: f64,
    /// 技能口径串（已实现轨是 [`SKILL_NOT_APPLICABLE`]）。
    pub skill_caliber: String,
    /// 单位成本（① = FIFO 均价 + 实付中介费/单位；② = 挂价 + 实付中介费/单位；
    /// ③ = 被消耗批次的单位成本）。
    pub unit_cost: f64,
    /// 单位成本的来源（[`COST_SRC_FIFO90`] / [`COST_SRC_BUY_TRAP`] / [`COST_SRC_UNKNOWN`]）。
    pub cost_source: String,
    /// 一行公式串（带本条的实际数字，读者能拿计算器复算）。
    pub formula: String,
    /// 数据年龄（秒）：本条判定所用输入的时刻到 `now`。
    pub data_age_secs: i64,
}

/// 告警载荷（spec §4.3，字段逐字照抄）：**推送与提醒中心共用的唯一序列化出口**。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertPayload {
    /// 去重键：[`order_alert_key`] / [`tx_alert_key`] 的产物（不要自己拼）。
    pub alert_key: String,
    pub kind: AlertKind,
    /// 已实现轨 = 回填匹配到的原挂单 id；匹配不上 = 0（卡片据此标注"未在本机观察窗内"）。
    pub order_id: u64,
    pub type_id: u32,
    pub type_name: String,
    pub location_id: u64,
    pub location_name: String,
    /// 订单方向。
    pub is_buy: bool,
    pub price: f64,
    /// 挂单轨 = `volume_remain`；已实现轨 = 成交数量。
    pub volume: u64,
    /// 挂单轨 = 挂出时刻；已实现轨 = 成交时刻。
    pub at: DateTime<Utc>,
    /// 预计/已实现亏损额（**正数**）。
    pub loss_isk: f64,
    /// 相对全成本的收益率（%），负 = 亏。
    pub margin_pct: f64,
    pub caliber: CaliberSummary,
}

/// 名字字典（纯函数不查库）：判定要把类型名与站点名写进卡片，而名字住在 DB 的字典表里 ——
/// 由装配层查好注入，查不到用 fallback 串（与 `flip_scan` 同一套字面量：
/// 名字缺失不该杀掉一张告警卡）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NameLookup {
    pub types: HashMap<u32, String>,
    pub stations: HashMap<u64, String>,
}

impl NameLookup {
    pub fn type_name(&self, type_id: u32) -> String {
        self.types
            .get(&type_id)
            .cloned()
            .unwrap_or_else(|| format!("type_id {type_id}"))
    }

    pub fn station_name(&self, location_id: u64) -> String {
        self.stations
            .get(&location_id)
            .cloned()
            .unwrap_or_else(|| format!("站点 #{location_id}"))
    }
}

/// 挂单轨的 `alert_key`：`order:{id}`。**唯一构造处** —— T9 的写入与查询必须调它。
pub fn order_alert_key(order_id: i64) -> String {
    format!("{KEY_ORDER}:{order_id}")
}

/// 已实现轨的 `alert_key`：`tx:{id}`。**唯一构造处** —— T9 的写入与查询必须调它。
pub fn tx_alert_key(transaction_id: i64) -> String {
    format!("{KEY_TX}:{transaction_id}")
}

/// 一个类型在重放中的批次账（与 `char::fifo` 同口径：队首最早、卖单从队首扣）。
#[derive(Default)]
struct Book {
    /// `(剩余数量, 该批单价)`。单价逐批存：吃掉的那批的成本要能算到**吃掉它的那一笔**头上。
    queue: VecDeque<(u64, f64)>,
    /// 窗内账本已经说不清持仓（有卖无买 / 卖超）。一次即定 —— 从这里往后的每一笔卖出
    /// 都不再可信（同 `fifo.rs` 的纪律：拿说不清的账本去算，比不报更坏）。
    uncovered: bool,
}

/// 每笔**卖出**成交所消耗批次的单位成本，键是卖出的 `transaction_id`（形态 ③ 的原料）。
///
/// **为什么不复用 `fifo_costs`**（P1）：它返回的是**剩余持仓**的加权均价 —— 形态 ① 问的是
/// "我手上这批货花了我多少钱"，这个口径对；形态 ③ 问的是"**这一笔卖掉的货**当初花了多少钱"，
/// 被 FIFO 吃掉的那几批的价格必须留在这一笔头上，而不是随队列出队消失。同一时刻两个数可以
/// 差很多（买 100@10 + 100@20 后卖 150：剩余均价 20，被吃掉的批次 13.33），拿后者算已实现轨
/// 是**口径错**而不是精度差。T7 的 `fifo.rs` 有意不返回它（控制器禁止 T7 提前加），
/// 于是伴生函数落在判定侧 —— 边界内解决，不动 T7 的文件。
///
/// 三条与 `fifo.rs` 逐字对齐的规则：重放按 `(date, transaction_id)` 升序、不看输入顺序；
/// 卖出从队首扣；覆盖不到（有卖无买 / 卖超）标 `Unknown` 且 `avg_cost = NaN`（未知不是 0）。
/// 一条**不同**的收尾：队列空不算失败 —— 卖光那一笔的成本完全由窗内买入解释（收尾空仓
/// 没有成本可言，不等于卖掉的那批货当初多少钱也说不清）。
pub fn consumed_lot_costs(txs: &[WalletTx]) -> HashMap<i64, FifoCost> {
    // 与 `load_char_tx` 的排序键一致：同日两笔的先后不能随调用方传入的顺序漂移。
    let mut ordered: Vec<&WalletTx> = txs.iter().collect();
    ordered.sort_by_key(|t| (t.date.as_str(), t.transaction_id));

    let mut books: HashMap<u32, Book> = HashMap::new();
    let mut out: HashMap<i64, FifoCost> = HashMap::new();
    for t in ordered {
        let book = books.entry(t.type_id).or_default();
        if t.is_buy {
            if t.quantity > 0 {
                book.queue.push_back((t.quantity, t.unit_price));
            }
            continue;
        }
        // 卖出：从队首扣，扣掉的批次成本累加到这一笔头上。
        let mut left = t.quantity;
        let mut eaten_qty: u64 = 0;
        let mut eaten_notional = 0.0;
        let mut bound = true;
        while left > 0 {
            match book.queue.front_mut() {
                Some((rest, price)) if *rest <= left => {
                    eaten_qty += *rest;
                    eaten_notional += *rest as f64 * *price;
                    left -= *rest;
                    book.queue.pop_front();
                }
                Some((rest, price)) => {
                    eaten_qty += left;
                    eaten_notional += left as f64 * *price;
                    *rest -= left;
                    left = 0;
                }
                None => {
                    // 卖得比窗内买到的多：这一笔的一部分来自窗外，成本无从谈起。
                    book.uncovered = true;
                    bound = false;
                    break;
                }
            }
        }
        let cost = if bound && !book.uncovered && eaten_qty > 0 {
            FifoCost {
                avg_cost: eaten_notional / eaten_qty as f64,
                source: CostSource::Known,
            }
        } else {
            FifoCost {
                avg_cost: f64::NAN,
                source: CostSource::Unknown,
            }
        };
        out.insert(t.transaction_id, cost);
    }
    out
}

/// 一条判定的公共事实：三形态都从挂单/流水行里取这几项，只有算术与口径不同。
/// **净额与全成本一路传到底**，由 [`Facts::into_alert`] 一处判"亏没亏"并算
/// `loss_isk`/`margin_pct` —— 三条判定各写一遍比较与减法，"亏损额"迟早会有两种定义。
struct Facts {
    alert_key: String,
    kind: AlertKind,
    order_id: u64,
    type_id: u32,
    location_id: u64,
    is_buy: bool,
    price: f64,
    volume: u64,
    at: DateTime<Utc>,
    /// 净额：这一笔真正到手的钱（已扣掉卖出时被划走的税）。
    net_isk: f64,
    /// 全成本：商品成本 + 本条能归属到的实付费用。
    full_cost_isk: f64,
}

impl Facts {
    /// 唯一的"亏没亏"判决与载荷装配。
    /// 严格小于才告警：打平不是亏（brief 的算例正是靠 96.625 > 95 这一步不告警）。
    /// 全成本非正时没有收益率可言（除零）；输入本身是 NaN/Inf 时同样不产出载荷 ——
    /// 卡片上的 NaN 比没有卡更难查，而 `FifoCost` 的 NaN 根本走不到这里（见 `known_price()`）。
    fn into_alert(self, names: &NameLookup, caliber: CaliberSummary) -> Option<AlertPayload> {
        if !(self.net_isk.is_finite() && self.full_cost_isk.is_finite()) {
            return None;
        }
        if self.full_cost_isk <= 0.0 || self.net_isk >= self.full_cost_isk {
            return None;
        }
        Some(AlertPayload {
            alert_key: self.alert_key,
            kind: self.kind,
            order_id: self.order_id,
            type_id: self.type_id,
            type_name: names.type_name(self.type_id),
            location_id: self.location_id,
            location_name: names.station_name(self.location_id),
            is_buy: self.is_buy,
            price: self.price,
            volume: self.volume,
            at: self.at,
            loss_isk: self.full_cost_isk - self.net_isk,
            margin_pct: (self.net_isk / self.full_cost_isk - 1.0) * 100.0,
            caliber,
        })
    }
}

/// 从"实付费用"字典取一笔。取不到 = 本轮没看到这笔费用（journal 不落表），按 0 计 ——
/// 方向是**低估成本 = 宁可漏报**，不会凭空造出告警。值应为正数（抽取侧已取绝对值）。
fn cash(paid: &HashMap<i64, f64>, id: i64) -> f64 {
    paid.get(&id).copied().unwrap_or(0.0)
}

/// ESI 的 ISO8601 文本 → `DateTime<Utc>`。形状不对时不 panic 也不猜时区：退到 epoch，
/// 卡片上会出现一个明显不对的时间（异形日期由 T7 的 `warn_on_odd_dates` 负责报出来）。
fn parse_at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_default()
}

/// 形态 ①：挂卖单预期亏。
///
/// 逐张**卖单**：`净额 = 挂价 × (1 − 有效销售税)` 对比 `全成本 = FIFO 剩余持仓均价 +
/// 该挂单实付中介费/单位`。税率走 `FeeModel`（预期轨：技能一改就重算）。
/// `broker_paid` 是装配层从 journal 摘出的"挂单 id → 实付中介费（正数）"。
pub fn detect_expected_sell(
    orders: &[CharOrder],
    costs: &HashMap<u32, FifoCost>,
    fees: &FeeModel,
    broker_paid: &HashMap<i64, f64>,
    names: &NameLookup,
    now: i64,
) -> Vec<AlertPayload> {
    let tax = fees.effective_sales_tax();
    let mut out = Vec::new();
    // 顺序 = 调用方传入的顺序（DB 侧已定序）：推送方的日限与冷却依赖稳定的判定顺序。
    for o in orders.iter().filter(|o| !o.is_buy) {
        if o.volume_remain == 0 {
            continue; // 没剩量可卖：既谈不上"卖得亏"，摊费用也没有分母
        }
        // P2：先看 source 再看价格。`known_price()` 是唯一取价入口，Unknown 拿不到数，
        // 「拿 0 当成本造出满屏假告警」这件事在类型上就写不出来。
        let Some(fifo) = costs.get(&o.type_id).and_then(FifoCost::known_price) else {
            continue; // 成本未知（或窗内没有任何该类型的流水）：整个跳过判定
        };
        // 实付中介费/单位：分母取**剩余量**（本机只存 volume_remain，没有挂单原量）。
        // 没成交过的挂单与原量相等（精确）；部分成交过的单会偏高（保守侧）。
        let broker_per_unit = cash(broker_paid, o.order_id) / o.volume_remain as f64;
        let net = o.price * (1.0 - tax);
        let full_cost = fifo + broker_per_unit;
        let caliber = CaliberSummary {
            track: TRACK_EXPECTED.to_string(),
            sales_tax_pct: tax * 100.0,
            // 摘要报的是**本条判定真正用到的**费率：税来自面板估价，中介费来自 journal 实付。
            // 看不到那笔中介费时是 0（= 本轮没观察到），公式串里同一处也写 0，两处一致。
            broker_pct: if o.price > 0.0 {
                broker_per_unit / o.price * 100.0
            } else {
                0.0
            },
            skill_caliber: format!(
                "Accounting {} / Broker Relations {}（税率随面板；中介费取 journal 实付）",
                fees.accounting, fees.broker_relations
            ),
            unit_cost: full_cost,
            cost_source: COST_SRC_FIFO90.to_string(),
            formula: format!(
                "① 单位净额 {net:.6} = 挂价 {:.6} × (1 − 有效税 {:.4}%)；单位全成本 {full_cost:.6} \
                 = FIFO 均价 {fifo:.6} + 实付中介费/单位 {broker_per_unit:.6}",
                o.price,
                tax * 100.0
            ),
            data_age_secs: (now - o.fetched_at).max(0),
        };
        let facts = Facts {
            alert_key: order_alert_key(o.order_id),
            kind: AlertKind::ExpectedSellLoss,
            order_id: o.order_id as u64,
            type_id: o.type_id,
            location_id: o.location_id,
            is_buy: false,
            price: o.price,
            volume: o.volume_remain,
            at: parse_at(&o.issued),
            net_isk: net * o.volume_remain as f64,
            full_cost_isk: full_cost * o.volume_remain as f64,
        };
        if let Some(p) = facts.into_alert(names, caliber) {
            out.push(p);
        }
    }
    out
}

/// 形态 ②：挂买单套牢亏。
///
/// 逐张**买单**：`本站当前可执行卖出净额`（吃本站买盘到该单剩余量的**加权价** ×
/// (1 − 税 − 中介费)）对比 `买单成交价 + 实付中介费/单位`。两侧费率都走 `FeeModel`（预期轨）。
pub fn detect_buy_trap(
    orders: &[CharOrder],
    books: &[StationOrderBook],
    fees: &FeeModel,
    broker_paid: &HashMap<i64, f64>,
    names: &NameLookup,
    now: i64,
) -> Vec<AlertPayload> {
    let tax = fees.effective_sales_tax();
    let broker = fees.effective_broker();
    let index: HashMap<(u64, u32), &StationOrderBook> = books
        .iter()
        .map(|b| ((b.location_id, b.type_id), b))
        .collect();
    let mut out = Vec::new();
    for o in orders.iter().filter(|o| o.is_buy) {
        if o.volume_remain == 0 {
            continue;
        }
        // 用挂单**自己那一站**的盘：货在那儿，卖只能在哪儿卖。
        let Some(book) = index.get(&(o.location_id, o.type_id)) else {
            continue;
        };
        // 可执行卖出净额取**加权吃单价**而不是买一价（更不是上一口成交价）：
        // 深度不足时退回"能成交的那部分"的加权价 —— 那是这批货能拿到的**最好**价，
        // 拿它判亏是稳的（卖不掉的部分只会更差）。买盘空（None）= 可执行净额无从谈起，
        // 0 或 NaN 都是编数字，跳过。
        let Some((bid, _filled)) = book.executable(Side::Sell, o.volume_remain) else {
            continue;
        };
        let broker_per_unit = cash(broker_paid, o.order_id) / o.volume_remain as f64;
        let net = bid * (1.0 - tax - broker);
        let full_cost = o.price + broker_per_unit;
        let caliber = CaliberSummary {
            track: TRACK_EXPECTED.to_string(),
            sales_tax_pct: tax * 100.0,
            broker_pct: broker * 100.0,
            skill_caliber: format!(
                "Accounting {} / Broker Relations {}（两侧费率均随面板重算）",
                fees.accounting, fees.broker_relations
            ),
            unit_cost: full_cost,
            cost_source: COST_SRC_BUY_TRAP.to_string(),
            formula: format!(
                "② 可执行卖出净额 {net:.6}/件 = 加权吃单买价 {bid:.6} × (1 − 税 {:.4}% − 中介费 {:.4}%)；\
                 买入成本 {full_cost:.6}/件 = 挂价 {:.6} + 实付中介费/单位 {broker_per_unit:.6}",
                tax * 100.0,
                broker * 100.0,
                o.price
            ),
            data_age_secs: (now - o.fetched_at).max(0),
        };
        let facts = Facts {
            alert_key: order_alert_key(o.order_id),
            kind: AlertKind::BuyOrderTrap,
            order_id: o.order_id as u64,
            type_id: o.type_id,
            location_id: o.location_id,
            is_buy: true,
            price: o.price,
            volume: o.volume_remain,
            at: parse_at(&o.issued),
            net_isk: net * o.volume_remain as f64,
            full_cost_isk: full_cost * o.volume_remain as f64,
        };
        if let Some(p) = facts.into_alert(names, caliber) {
            out.push(p);
        }
    }
    out
}

/// journal 真值抽取：**`context_id` 含义的分派点只有这一处**（ESI 的 `context_id`
/// "因历史原因是完全不同的东西"，按 `ref_type` 分派）：
/// - `brokers_fee` → 挂单 id（下单时收的那笔中介费，买卖两侧都是这个 ref_type）；
/// - `transaction_tax` → 成交 id（卖出被划走的销售税）。
///
/// 两个 ref_type 串**已核对**（2026-09-24，ESI 官方契约 `meta/openapi.json` 的
/// `CharactersCharacterIdWalletJournalGet.ref_type.enum`）：是 `brokers_fee` **复数**，
/// 不是 `broker_fee`。金额取绝对值 —— 契约原文写着 `amount` 负号表示 ISK 被划走，
/// 而公式里的"实付"是正数。
///
/// **待核**：本机无 client_id，拿不到真实 journal 报文，两条 context_id 归属只到
/// "社区惯例，加上 `context_id_type` 枚举里确有 `market_transaction_id`"这一步。
/// 归属若反了，费用项会静默变 0（方向是漏报、不造假告警）；核对后改这一个函数即可。
///
/// 另注：T7 的 `JournalEntry` 没保留 ESI 的 `context_id_type` 字段，否则归属能在运行时自证 ——
/// 属 T7 文件，已记进 T8 报告。
fn journal_truth(journal: &[JournalEntry]) -> (HashMap<i64, f64>, HashMap<i64, f64>) {
    let mut tax_by_tx: HashMap<i64, f64> = HashMap::new();
    let mut broker_by_order: HashMap<i64, f64> = HashMap::new();
    for e in journal {
        // 没有 context_id / amount 的条目对判定没有用（T7 特意用 `Option` 把"空"与"0"分开）。
        let (Some(ctx), Some(amount)) = (e.context_id, e.amount) else {
            continue;
        };
        let bucket = match e.ref_type.as_str() {
            "transaction_tax" => &mut tax_by_tx,
            "brokers_fee" => &mut broker_by_order,
            // `market_transaction` / `market_escrow` 等其他 market_* 条目不是费用真值。
            _ => continue,
        };
        *bucket.entry(ctx).or_insert(0.0) += amount.abs();
    }
    (tax_by_tx, broker_by_order)
}

/// 回填匹配（spec §4.3 的 ESI 事实澄清）：流水**不返回 order_id**，用
/// 「同类型 + 同方向 + 同价 + 数量 ≤ 当时剩余 + 落在挂单存活期内」把成交对回原挂单。
///
/// `orders` 必须是**成交前那一轮**的挂单快照 —— 成交掉的挂单在这一轮已经不在快照里，
/// 拿同步后的快照来匹配永远匹配不上（T12 要在 `sync_character` 之前读库）。这一条写在
/// 这里，是因为函数看不出调用方喂的是哪一轮的快照。
///
/// 价格用逐位相等而不是容差：两张单的价格都来自同一份 ESI JSON 的同一段十进制文本，
/// 相等就是相等；容差只会把两个真要分开的价糊成一张单。同价多单时取**挂单日最早**的
/// 那张（价格—时间优先：同价先挂的先成交），再按 order_id 兜底保证确定性。
/// 匹配不上不猜：宁可让卡片写"原挂单未在本机观察窗内"，也不指一个错单号（卡片与提醒中心
/// 都靠它检索）。快照量按逐笔累减 —— "数量 ≤ 当时 remain"是**逐笔**的约束，一张单被
/// 两笔成交分别吃掉时，第二笔要不大于第一笔吃剩的量。
fn match_origin_orders(txs: &[WalletTx], orders: &[CharOrder]) -> HashMap<i64, i64> {
    let mut budget: HashMap<i64, u64> = orders.iter().map(|o| (o.order_id, o.volume_remain)).collect();
    let mut ordered: Vec<&WalletTx> = txs.iter().collect();
    ordered.sort_by_key(|t| (t.date.as_str(), t.transaction_id));

    let mut out = HashMap::new();
    for t in ordered {
        let mut best: Option<&CharOrder> = None;
        for o in orders {
            if o.is_buy != t.is_buy || o.type_id != t.type_id || o.price != t.unit_price {
                continue;
            }
            if budget.get(&o.order_id).copied().unwrap_or(0) < t.quantity {
                continue;
            }
            // 存活期：挂出时刻 ≤ 成交时刻 ≤ 挂出时刻 + duration 天。日期形状不对的挂单
            // 不参与匹配（宁可不匹配，也不拿一个说不清的单号）。
            let (Ok(issued), Ok(done)) = (
                DateTime::parse_from_rfc3339(&o.issued),
                DateTime::parse_from_rfc3339(&t.date),
            ) else {
                continue;
            };
            if done < issued || done > issued + Duration::days(o.duration) {
                continue;
            }
            // 同价多单：取挂单最早的（价格—时间优先），再按 order_id 兜底。
            let better = match best {
                Some(b) => (&o.issued, o.order_id) < (&b.issued, b.order_id),
                None => true,
            };
            if better {
                best = Some(o);
            }
        }
        let Some(o) = best else { continue };
        *budget.entry(o.order_id).or_insert(0) -= t.quantity;
        out.insert(t.transaction_id, o.order_id);
    }
    out
}

/// 形态 ③：已实现成交亏。
///
/// 逐笔**卖出成交**：`净额 = 卖出所得 − 实付销售税` 对比
/// `全成本 = 被消耗批次的 FIFO 成本 + 卖出侧实付中介费`。全部取 journal 真值 ——
/// **函数的参数表里没有 `FeeModel`**，技能面板想借道也进不来（P4 的轨道分离靠签名）。
///
/// 两条前置：被消耗批次的成本必须已知（`consumed_lot_costs` 的 `Unknown` 跳过），
/// 且这笔成交的实付税必须在 journal 里看得到 —— 原料都拿不到就不判（见 [`journal_truth`]）。
pub fn detect_realized(
    txs: &[WalletTx],
    orders: &[CharOrder],
    journal: &[JournalEntry],
    names: &NameLookup,
    now: i64,
) -> Vec<AlertPayload> {
    let sale_costs = consumed_lot_costs(txs);
    let origin = match_origin_orders(txs, orders);
    let (tax_by_tx, broker_by_order) = journal_truth(journal);

    let mut out = Vec::new();
    for t in txs.iter().filter(|t| !t.is_buy) {
        // P2：成本未知的卖出整个跳过（超卖 / 有卖无买 / 账本已经说不清）。
        let Some(unit_cost) = sale_costs
            .get(&t.transaction_id)
            .and_then(FifoCost::known_price)
        else {
            continue;
        };
        // 本形态的原料就是 journal 真值：连这笔成交的实付税都没看到，就没有"已实现"可言。
        // 宁可这一轮不报，也不拿 0 当税去报一个编出来的亏损额（顺带挡住首启 90 天旧账的轰炸：
        // journal 只回溯 30 天，旧成交的税单根本不会出现）。
        let Some(tax) = tax_by_tx.get(&t.transaction_id).copied() else {
            continue;
        };
        // 回填匹配带出原挂单 id；匹配不上 = 0，卡片据此标注（卡片渲染由 T10/T14 负责）。
        let order_id = origin.get(&t.transaction_id).copied().unwrap_or(0);
        // 卖出侧实付中介费：就是这张挂单下单时被划走的那笔。买入侧**不计** ——
        // 本机既没有挂单原量、也没有"这几件货来自哪张买单"的归属数据（见模块头第 3 条），
        // 按比例摊就是编数字；公式串里写明了这一项缺席。
        let broker = cash(&broker_by_order, order_id);
        let income = t.unit_price * t.quantity as f64;
        let consumed = unit_cost * t.quantity as f64;
        let net = income - tax;
        let full_cost = consumed + broker;
        let matched_note = if order_id == 0 {
            "；原挂单未在本机观察窗内，卖出侧中介费按 0 计"
        } else {
            ""
        };
        let caliber = CaliberSummary {
            track: TRACK_REALIZED.to_string(),
            sales_tax_pct: if income > 0.0 {
                tax / income * 100.0
            } else {
                0.0
            },
            broker_pct: if income > 0.0 {
                broker / income * 100.0
            } else {
                0.0
            },
            skill_caliber: SKILL_NOT_APPLICABLE.to_string(),
            unit_cost: if t.quantity > 0 {
                full_cost / t.quantity as f64
            } else {
                full_cost
            },
            cost_source: COST_SRC_FIFO90.to_string(),
            formula: format!(
                "③（journal 真值）净额 {net:.6} = 卖出所得 {income:.6} − 实付销售税 {tax:.6}；\
                 全成本 {full_cost:.6} = 被消耗批次 {consumed:.6} + 卖出侧实付中介费 {broker:.6}\
                 （买入侧中介费本机无法归属，未计）{matched_note}"
            ),
            data_age_secs: (now - parse_at(&t.date).timestamp()).max(0),
        };
        let facts = Facts {
            alert_key: tx_alert_key(t.transaction_id),
            kind: AlertKind::RealizedLoss,
            order_id: order_id.max(0) as u64,
            type_id: t.type_id,
            location_id: t.location_id,
            is_buy: false,
            price: t.unit_price,
            volume: t.quantity,
            at: parse_at(&t.date),
            net_isk: net,
            full_cost_isk: full_cost,
        };
        if let Some(p) = facts.into_alert(names, caliber) {
            out.push(p);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::char::fifo::{fifo_costs, CostSource};
    use crate::market::{PriceLevel, STATION_JITA};
    use crate::store::Db;

    const TYPE: u32 = 34;

    fn ts(s: &str) -> i64 {
        DateTime::parse_from_rfc3339(s).unwrap().timestamp()
    }

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// 2026-09-24T12:00:00Z。
    fn now() -> i64 {
        ts("2026-09-24T12:00:00Z")
    }

    /// A5 满会计：税 7.5% × (1 − 0.55) = 3.375%；中介费留默认基率 3%。
    fn fees_a5() -> FeeModel {
        FeeModel {
            accounting: 5,
            ..FeeModel::default()
        }
    }

    /// A5 + BR5：税 3.375%、中介费 1.5% → 卖出净额系数 0.95125。
    fn fees_a5_br5() -> FeeModel {
        FeeModel {
            accounting: 5,
            broker_relations: 5,
            ..FeeModel::default()
        }
    }

    fn cost_known(unit: f64) -> HashMap<u32, FifoCost> {
        HashMap::from([(
            TYPE,
            FifoCost {
                avg_cost: unit,
                source: CostSource::Known,
            },
        )])
    }

    fn cost_unknown() -> HashMap<u32, FifoCost> {
        HashMap::from([(
            TYPE,
            FifoCost {
                avg_cost: f64::NAN,
                source: CostSource::Unknown,
            },
        )])
    }

    /// 一张挂在吉他、2026-09-20 挂出的单。`fetched_at` = 快照落库时刻（数据年龄从它起算）。
    fn order(id: i64, is_buy: bool, price: f64, volume_remain: u64) -> CharOrder {
        CharOrder {
            order_id: id,
            type_id: TYPE,
            location_id: STATION_JITA,
            is_buy,
            price,
            volume_remain,
            issued: "2026-09-20T10:00:00Z".to_string(),
            duration: 90,
            fetched_at: ts("2026-09-24T11:59:00Z"),
        }
    }

    fn tx(id: i64, date: &str, is_buy: bool, quantity: u64, unit_price: f64) -> WalletTx {
        WalletTx {
            transaction_id: id,
            date: date.to_string(),
            type_id: TYPE,
            location_id: STATION_JITA,
            is_buy,
            unit_price,
            quantity,
        }
    }

    fn journal(id: i64, ref_type: &str, amount: f64, context_id: i64) -> JournalEntry {
        JournalEntry {
            id,
            date: "2026-09-20T00:00:01Z".to_string(),
            ref_type: ref_type.to_string(),
            amount: Some(amount),
            context_id: Some(context_id),
            description: String::new(),
        }
    }

    fn book(loc: u64, ty: u32, bids: &[(f64, u64, u32)]) -> StationOrderBook {
        let depth: Vec<PriceLevel> = bids
            .iter()
            .map(|&(price, volume, orders)| PriceLevel {
                price,
                volume,
                orders,
            })
            .collect();
        StationOrderBook {
            location_id: loc,
            type_id: ty,
            is_npc_station: true,
            best_bid: depth.first().map(|l| l.price),
            bid_qty: depth.first().map(|l| l.volume).unwrap_or(0),
            best_ask: None,
            ask_qty: 0,
            bid_levels: depth.iter().map(|l| l.orders).sum(),
            ask_levels: 0,
            bid_depth: depth,
            ask_depth: vec![],
            skipped_stale: 0,
            skipped_thin: 0,
            skipped_wholesale: 0,
        }
    }

    /// 形态 ③ 的夹具：买 100@100 → 卖 100@90；journal 里税 300（挂成交 2）、
    /// 卖出侧中介费 270（挂原挂单 555）。
    fn realized_fixture() -> (Vec<WalletTx>, Vec<CharOrder>, Vec<JournalEntry>) {
        let txs = vec![
            tx(1, "2026-09-10T00:00:00Z", true, 100, 100.0),
            tx(2, "2026-09-20T00:00:00Z", false, 100, 90.0),
        ];
        // 成交前那一轮的挂单快照：这张卖单在成交后就不在快照里了，
        // 只有它能把这笔流水对回 order_id（T12 必须在同步**之前**读快照）。
        let orders = vec![CharOrder {
            order_id: 555,
            type_id: TYPE,
            location_id: STATION_JITA,
            is_buy: false,
            price: 90.0,
            volume_remain: 100,
            issued: "2026-09-15T00:00:00Z".to_string(),
            duration: 90,
            fetched_at: now(),
        }];
        let journal = vec![
            journal(9001, "transaction_tax", -300.0, 2),
            journal(9002, "brokers_fee", -270.0, 555),
        ];
        (txs, orders, journal)
    }

    #[test]
    fn expected_sell_loss_fires_when_net_below_full_cost() {
        // spec §4.3 ①：净额 = 挂价 × (1 − sales_tax(A))；全成本 = FIFO 均价 + 实付中介费/单位。
        // A5 口径：税 7.5% × 0.45 = 3.375%。挂价 100、FIFO 成本 95、实付中介费 0
        // → 96.625 > 95 不告警；挂价降到 97 → 93.72625 < 95 告警。
        let names = NameLookup {
            types: HashMap::from([(TYPE, "Tritanium".to_string())]),
            stations: HashMap::from([(STATION_JITA, "Jita IV - Moon 4".to_string())]),
        };
        let no_fee = HashMap::new();

        let calm = detect_expected_sell(
            &[order(7001, false, 100.0, 100)],
            &cost_known(95.0),
            &fees_a5(),
            &no_fee,
            &names,
            now(),
        );
        assert!(
            calm.is_empty(),
            "96.625 > 95：净额还在成本之上就不告警（96.625 = 100 × (1 − 0.03375)）"
        );

        let hits = detect_expected_sell(
            &[order(7001, false, 97.0, 100)],
            &cost_known(95.0),
            &fees_a5(),
            &no_fee,
            &names,
            now(),
        );
        assert_eq!(hits.len(), 1);
        let p = &hits[0];
        assert_eq!(p.kind, AlertKind::ExpectedSellLoss);
        assert_eq!(p.alert_key, "order:7001");
        assert_eq!(p.order_id, 7001);
        assert_eq!(p.type_name, "Tritanium");
        assert_eq!(p.location_name, "Jita IV - Moon 4");
        assert!(!p.is_buy);
        assert!((p.price - 97.0).abs() < 1e-12);
        assert_eq!(p.volume, 100);
        assert_eq!(p.at, at("2026-09-20T10:00:00Z"), "挂单轨的 at = 挂出时刻");
        assert!(
            (p.loss_isk - 127.375).abs() < 1e-9,
            "单位亏 95 − 93.72625 = 1.27375，× 100 = 127.375，实得 {}",
            p.loss_isk
        );
        assert!(
            (p.margin_pct + 1.340_789_473_684_210_5).abs() < 1e-9,
            "(93.72625 / 95 − 1) × 100 = −1.3408%，实得 {}",
            p.margin_pct
        );
    }

    #[test]
    fn expected_sell_loss_never_fires_when_cost_is_unknown() {
        // 成本未知的类型必须**完全不参与**判定（spec §4.2）：取价只走 known_price()，
        // 拿 0 或 NaN 当成本会造出"每笔都在亏"的假告警，这是最坏的假阳性。
        // 挂价 10 是"怎么看都亏"的极端值 —— 只有"成本未知"这一条能拦住它。
        let names = NameLookup::default();
        let no_fee = HashMap::new();
        let cheap = order(7001, false, 10.0, 100);

        let unknown = detect_expected_sell(
            std::slice::from_ref(&cheap),
            &cost_unknown(),
            &fees_a5(),
            &no_fee,
            &names,
            now(),
        );
        assert!(
            unknown.is_empty(),
            "成本未知 → 不参与判定（不是'按 0 成本算成巨亏'）"
        );

        // 对照：同一张单，成本已知就必须报 —— 否则上面那个空结果说明不了是 source 拦住的。
        let known = detect_expected_sell(&[cheap], &cost_known(95.0), &fees_a5(), &no_fee, &names, now());
        assert_eq!(known.len(), 1, "成本已知时同一张单照样告警（对照组）");

        // 类型根本不在成本表里（窗内一笔都没有）→ 同样不判定。
        let absent = detect_expected_sell(
            &[order(7001, false, 10.0, 100)],
            &HashMap::new(),
            &fees_a5(),
            &no_fee,
            &names,
            now(),
        );
        assert!(absent.is_empty(), "连成本基准都没有 → 不判定");
    }

    #[test]
    fn buy_trap_uses_executable_bid_not_last_price() {
        // spec §4.3 ②：用本站当前**可执行**卖出净额（吃买盘到该单剩余量的加权价），
        // 不是买一价、更不是最近成交价 —— 挂单量会穿过档位，买一价只代表第一档那几件。
        // 费率 A5+BR5：税 3.375% + 中介费 1.5% → 净额系数 0.95125。
        let names = NameLookup::default();
        let no_fee = HashMap::new();

        // 甲：买一 110 只有 100 件，200 件吃下去均价 (110×100 + 90×100) / 200 = 100
        //     → 净额 95.125 < 成本 100 → 套牢。（拿买一价 110 判会看成 104.6375 的"盈利"，
        //     拿上一口成交价判同样会漏。）
        // 乙：同样的买一价，深度刚好 100 件 = 挂单量 → 净额 104.6375 > 100 → 不告警。
        // 丙：本站买盘空 → 可执行净额无从谈起，不判定（0 或 NaN 都是编数字）。
        let books = vec![
            book(
                STATION_JITA,
                TYPE,
                &[(110.0, 100, 5), (90.0, 100, 5)],
            ),
            book(STATION_JITA, 35, &[(110.0, 100, 5)]),
            book(STATION_JITA, 36, &[]),
        ];
        let orders = vec![
            order(7002, true, 100.0, 200),
            CharOrder {
                type_id: 35,
                ..order(7003, true, 100.0, 100)
            },
            CharOrder {
                type_id: 36,
                ..order(7004, true, 100.0, 100)
            },
        ];

        let hits = detect_buy_trap(&orders, &books, &fees_a5_br5(), &no_fee, &names, now());
        assert_eq!(hits.len(), 1, "只有甲套牢：{hits:?}");
        let p = &hits[0];
        assert_eq!(p.kind, AlertKind::BuyOrderTrap);
        assert_eq!(p.type_id, TYPE);
        assert_eq!(p.alert_key, "order:7002");
        assert!(p.is_buy, "买单轨");
        assert!(
            (p.price - 100.0).abs() < 1e-12,
            "载荷里的 price 是挂单价（成本侧），不是买盘价"
        );
        assert_eq!(p.volume, 200);
        assert!(
            (p.loss_isk - 975.0).abs() < 1e-9,
            "单位亏 100 − 95.125 = 4.875，× 200 = 975，实得 {}",
            p.loss_isk
        );
        assert!(
            (p.margin_pct + 4.875).abs() < 1e-9,
            "95.125 / 100 − 1 = −4.875%，实得 {}",
            p.margin_pct
        );
    }

    #[test]
    fn realized_loss_uses_journal_truth_and_ignores_skill_panel() {
        // spec §4.3 ③ + 判定轨道分离：改 FeeModel 不得影响已实现轨结果。
        // 这条路的参数表里**没有** FeeModel —— 面板 0 级还是 5 级都递不进来。
        let (txs, orders, journal) = realized_fixture();
        let hits = detect_realized(&txs, &orders, &journal, &NameLookup::default(), now());
        assert_eq!(hits.len(), 1);
        let p = &hits[0];
        // 卖出所得 9000 − 实付税 300 = 净额 8700；全成本 = 消耗批次 10000 + 卖出侧中介费 270 = 10270。
        assert!(
            (p.loss_isk - 1570.0).abs() < 1e-9,
            "9000 − 300 − 10000 − 270 = −1570，实得 {}",
            p.loss_isk
        );
        assert!((p.margin_pct - (8700.0 / 10270.0 - 1.0) * 100.0).abs() < 1e-9);
        assert_eq!(p.kind, AlertKind::RealizedLoss);
        assert_eq!(p.alert_key, "tx:2", "已实现轨的去重键是 transaction_id");
        assert_eq!(p.order_id, 555, "回填匹配带出原挂单 id");
        assert_eq!(p.caliber.track, TRACK_REALIZED);
        assert_eq!(p.caliber.skill_caliber, SKILL_NOT_APPLICABLE);
        assert_eq!(p.type_name, "type_id 34", "名字查不到用 fallback");
        assert_eq!(p.location_name, "站点 #60003760");
        assert_eq!(p.at, at("2026-09-20T00:00:00Z"), "已实现轨的 at = 成交时刻");
        assert!(!p.is_buy);
        assert!((p.price - 90.0).abs() < 1e-12);
        assert_eq!(p.volume, 100);

        // 反向对照：拿**面板估价**算同一笔 = 9000 − 9000×3.375% − 10000 − 270 = −1573.75，
        // 与 1570 差 3.75 ISK —— 差值正是"实付 300"与"按 A5 估 303.75"的差。
        // 面板只改得了估价那一组数，改不动这张卡（函数签名里就没有它）。
        let est_tax = 9000.0 * fees_a5().effective_sales_tax();
        assert!((est_tax - 303.75).abs() < 1e-9, "A5 口径估出的税");
        assert!(
            (9000.0 - est_tax - 10000.0 - 270.0 - p.loss_isk).abs() > 1.0,
            "journal 真值与面板估价不是同一个数（这正是轨道分离要保住的）"
        );
        assert_eq!(
            p.caliber.data_age_secs, 388_800,
            "③ 的数据年龄 = 成交时刻（2026-09-20）到 now（2026-09-24T12:00）= 4 天半"
        );
    }

    #[test]
    fn caliber_summary_declares_track_and_source() {
        // 口径摘要必须自报**轨道**与**成本来源**：卡片读者要能一眼分清
        // "这个数是估的还是账上真发生的"，以及"成本是哪来的"。
        let (txs, orders, journal) = realized_fixture();

        let expected = detect_expected_sell(
            &[order(7001, false, 97.0, 100)],
            &cost_known(95.0),
            &fees_a5(),
            &HashMap::new(),
            &NameLookup::default(),
            now(),
        );
        let c = &expected[0].caliber;
        assert_eq!(c.track, TRACK_EXPECTED);
        assert_eq!(c.cost_source, COST_SRC_FIFO90);
        assert!((c.sales_tax_pct - 3.375).abs() < 1e-12, "报的是本条判定真用的税率");
        assert!((c.unit_cost - 95.0).abs() < 1e-12, "① = FIFO 均价 + 实付中介费/单位（此处 0）");
        assert_eq!(c.data_age_secs, 60, "快照 11:59 落库、12:00 判定 → 60 秒");
        assert!(c.skill_caliber.contains("Accounting 5"), "{}", c.skill_caliber);
        assert!(
            c.formula.contains('①') && c.formula.contains("FIFO"),
            "{}",
            c.formula
        );

        let trap = detect_buy_trap(
            &[order(7002, true, 100.0, 200)],
            &[book(STATION_JITA, TYPE, &[(110.0, 100, 5), (90.0, 100, 5)])],
            &fees_a5_br5(),
            &HashMap::new(),
            &NameLookup::default(),
            now(),
        );
        let c = &trap[0].caliber;
        assert_eq!(c.track, TRACK_EXPECTED, "② 也是预期轨（两侧费率随面板重算）");
        assert_eq!(c.cost_source, COST_SRC_BUY_TRAP);
        assert!((c.unit_cost - 100.0).abs() < 1e-12, "② 的成本是这张买单自己 + 实付中介费/单位");
        assert!((c.sales_tax_pct - 3.375).abs() < 1e-12);
        assert!((c.broker_pct - 1.5).abs() < 1e-12);
        assert!(c.formula.contains("可执行"), "{}", c.formula);

        let realized = detect_realized(&txs, &orders, &journal, &NameLookup::default(), now());
        let c = &realized[0].caliber;
        assert_eq!(c.track, TRACK_REALIZED);
        assert_eq!(c.skill_caliber, SKILL_NOT_APPLICABLE, "已实现轨不吃技能面板");
        assert_eq!(c.cost_source, COST_SRC_FIFO90);
        assert!(
            (c.unit_cost - 102.7).abs() < 1e-12,
            "③ 的单位成本 = (被消耗批次 10000 + 卖出侧中介费 270) / 100；实得 {}",
            c.unit_cost
        );
        // 卡片自洽：unit_cost × 数量 − 净额 = loss_isk（读者能拿卡上这三个数复算亏损）。
        assert!(
            (c.unit_cost * realized[0].volume as f64 - (9000.0 - 300.0) - realized[0].loss_isk)
                .abs()
                < 1e-9,
            "口径摘要的单位成本必须与亏损额同口径"
        );
        assert!(
            (c.sales_tax_pct - 3.333_333_333_333_333_5).abs() < 1e-6,
            "journal 隐含税率 = 300 / 9000，实得 {}",
            c.sales_tax_pct
        );
        assert!(
            (c.broker_pct - 3.0).abs() < 1e-9,
            "journal 隐含中介费率 = 270 / (90 × 100)，实得 {}",
            c.broker_pct
        );
        assert_eq!(c.data_age_secs, 388_800, "③ 的数据年龄从成交时刻算");
        assert!(
            c.formula.contains('③') && c.formula.contains("journal"),
            "{}",
            c.formula
        );
    }

    // ---- 契约与口径的边界（不在 brief 的五条里，但评审看的就是这些） ----

    #[test]
    fn consumed_lot_costs_charges_the_lots_a_sale_ate() {
        // 形态 ③ 要的是**被这一笔吃掉的批次**的成本，不是剩余持仓均价：
        // 买 100@10 + 100@20 后卖 150 → 吃掉 100@10 + 50@20 → 单位 2000/150 ≈ 13.333；
        // 而 fifo_costs 给的剩余持仓（50@20）均价是 20。同一时刻两个数差 40%，
        // 拿后者算已实现轨会把亏损当成盈利（或反过来）。
        let txs = vec![
            tx(1, "2026-09-01T00:00:00Z", true, 100, 10.0),
            tx(2, "2026-09-02T00:00:00Z", true, 100, 20.0),
            tx(3, "2026-09-03T00:00:00Z", false, 150, 30.0),
        ];
        let sale = consumed_lot_costs(&txs);
        let got = sale[&3].known_price().expect("窗内买入完全解释得清");
        assert!((got - 2000.0 / 150.0).abs() < 1e-12, "2000/150，实得 {got}");
        let remaining = fifo_costs(&txs);
        assert_eq!(remaining[&TYPE].known_price(), Some(20.0));
        assert!(
            (got - 20.0).abs() > 1e-9,
            "伴生函数必须给出与'剩余持仓均价'不同的数，否则就是拿剩余口径冒充已实现口径"
        );

        // 卖光的那一笔：成本完全由窗内买入解释 —— 比"剩余持仓"口径**更**确定
        // （收尾空仓没有成本可言，不等于卖掉的货当初多少钱也说不清）。
        let flat = consumed_lot_costs(&[
            tx(1, "2026-09-01T00:00:00Z", true, 100, 10.0),
            tx(2, "2026-09-02T00:00:00Z", false, 100, 12.0),
        ]);
        assert_eq!(flat[&2].known_price(), Some(10.0));

        // 超卖：这一笔的一部分来自窗外 → 标未知；**之后**同类型的每一笔卖出同样不可信
        // （账本已经说不清持仓，与 fifo.rs 的"一次即定"同纪律）。
        let oversold = consumed_lot_costs(&[
            tx(1, "2026-09-01T00:00:00Z", true, 100, 10.0),
            tx(2, "2026-09-02T00:00:00Z", false, 150, 12.0),
            tx(3, "2026-09-03T00:00:00Z", true, 100, 20.0),
            tx(4, "2026-09-04T00:00:00Z", false, 50, 25.0),
        ]);
        assert_eq!(oversold[&2].source, CostSource::Unknown, "卖超 → 未知");
        assert!(oversold[&2].avg_cost.is_nan(), "未知不是 0");
        assert_eq!(oversold[&4].source, CostSource::Unknown, "账本既然说不清，之后就都不可信");
        assert!(consumed_lot_costs(&[tx(2, "2026-09-02T00:00:00Z", false, 100, 12.0)])[&2]
            .known_price()
            .is_none(), "有卖无买：成本在窗外，同样未知");

        // 重放自己排序，与输入顺序无关（与 fifo_costs 同一条纪律）。
        let mut reversed = txs.clone();
        reversed.reverse();
        assert_eq!(consumed_lot_costs(&reversed)[&3].known_price(), Some(got));
    }

    #[test]
    fn realized_loss_skips_sales_the_journal_cannot_vouch_for() {
        // 已实现轨的原料就是 journal 真值：连这笔成交的实付税都没看到，就没有"已实现"可言 ——
        // 宁可这一轮不报，也不拿 0 当税去报一个编出来的亏损额（顺带挡住首启那 90 天旧账的轰炸）。
        let (txs, orders, journal) = realized_fixture();
        let names = NameLookup::default();

        assert!(
            detect_realized(&txs, &orders, &[], &names, now()).is_empty(),
            "没有 journal 真值 → 不判定"
        );
        let only_fee: Vec<JournalEntry> = journal
            .iter()
            .filter(|e| e.ref_type == "brokers_fee")
            .cloned()
            .collect();
        assert!(
            detect_realized(&txs, &orders, &only_fee, &names, now()).is_empty(),
            "只有中介费、没有这笔成交的税 → 同样不判定"
        );

        // 成本未知（有卖无买）→ 不参与判定（spec §4.2）。
        let sells_only = vec![tx(2, "2026-09-20T00:00:00Z", false, 100, 90.0)];
        assert!(
            detect_realized(&sells_only, &orders, &journal, &names, now()).is_empty(),
            "成本未知 → 整个跳过"
        );

        // 买单流水不是'成交亏'（形态 ③ 只对卖出成交发）。
        let buys_only = vec![tx(1, "2026-09-10T00:00:00Z", true, 100, 100.0)];
        assert!(detect_realized(&buys_only, &orders, &journal, &names, now()).is_empty());
    }

    #[test]
    fn alert_kind_strings_are_snake_case_and_share_one_mapping() {
        // kind 的字符串映射只有 as_str/parse 一处：落库的 kind 列与 payload 里的 kind
        // 绝不能是两种拼法（serde 也走同一套映射，不是各写一份）。
        assert_eq!(AlertKind::ExpectedSellLoss.as_str(), "expected_sell_loss");
        assert_eq!(AlertKind::BuyOrderTrap.as_str(), "buy_order_trap");
        assert_eq!(AlertKind::RealizedLoss.as_str(), "realized_loss");
        assert_eq!(
            AlertKind::parse("ExpectedSellLoss"),
            None,
            "PascalCase 不是线上的形态串"
        );
        for k in AlertKind::ALL {
            assert_eq!(AlertKind::parse(k.as_str()), Some(k), "{k:?} 往返失败");
            let json = serde_json::to_string(&k).unwrap();
            assert_eq!(
                json,
                format!("\"{}\"", k.as_str()),
                "serde 必须与 as_str 同一套映射，否则落库列与 payload 会漂移"
            );
            assert_eq!(serde_json::from_str::<AlertKind>(&json).unwrap(), k);
        }
        assert!(serde_json::from_str::<AlertKind>("\"nope\"").is_err());
    }

    #[test]
    fn alert_key_text_form_is_prefixed_and_unique_across_tracks() {
        // P3：alerts.alert_key 是 TEXT，而 order_id/transaction_id 是 INTEGER。
        // 规范形态必须带前缀，且只在构造处定义一次（T9 写入与查询共用）。
        assert_eq!(order_alert_key(101), "order:101");
        assert_eq!(tx_alert_key(101), "tx:101");
        assert_ne!(
            order_alert_key(101),
            tx_alert_key(101),
            "同号的挂单与成交不得撞成同一行（同一张表、同一个主键）"
        );
        assert!(
            order_alert_key(101).parse::<i64>().is_err(),
            "规范形态永远不是纯数字串"
        );

        // 现场证一下 P3 的前提：往真表里写一行，再用 INTEGER 去比 —— SQLite 不报错、
        // 直接当没有这行（`alert_key = <id>` 静默零行，正好是 T9 会踩的坑）。
        let db = Db::in_memory().unwrap();
        db.conn()
            .execute(
                "INSERT INTO alerts (alert_key, kind, char_id, type_id, location_id, is_buy,
                    first_seen_at, last_seen_at, state, payload)
                 VALUES (?1, ?2, 1, 34, 60003760, 0, 0, 0, 'new', '{}')",
                rusqlite::params![
                    order_alert_key(101),
                    AlertKind::ExpectedSellLoss.as_str().to_string()
                ],
            )
            .unwrap();
        let count = |sql: &str| -> i64 {
            db.conn().query_row(sql, [], |r| r.get(0)).unwrap()
        };
        assert_eq!(
            count("SELECT COUNT(*) FROM alerts WHERE alert_key = 'order:101'"),
            1
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM alerts WHERE alert_key = 101"),
            0,
            "INTEGER 拄 TEXT 列：不报错、静默零行 —— 这就是 P3 要防的那件事"
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM alerts WHERE alert_key = CAST(101 AS INTEGER)"),
            0,
            "跨存储类比较同样静默零行（INTEGER 永远排在 TEXT 之前）"
        );
    }

    /// 逐字段回读：非浮点字段逐字相等，**浮点允许 1 ulp 级误差**。
    ///
    /// 为什么不能要求 `assert_eq!(back, p)`：`serde_json` 默认的浮点解析是"尽力而为"
    /// （`float_roundtrip` 特性未开，开了会让全应用的市场价解析慢一倍 —— 那些数据每轮
    /// 上千个浮点），17 位十进制不保证逐位还原（本任务实测过 1 ulp 差）。
    /// `alerts.payload` 是**显示/契约载体**，不是逐位账本；T9 说的"payload 逐字节稳定"
    /// 指存进去的那串文本原样保留，不是要求 f64 位相等。
    fn assert_payload_equivalent(a: &AlertPayload, b: &AlertPayload) {
        assert_eq!(a.alert_key, b.alert_key);
        assert_eq!(a.kind, b.kind);
        assert_eq!(a.order_id, b.order_id);
        assert_eq!(a.type_id, b.type_id);
        assert_eq!(a.type_name, b.type_name);
        assert_eq!(a.location_id, b.location_id);
        assert_eq!(a.location_name, b.location_name);
        assert_eq!(a.is_buy, b.is_buy);
        assert_eq!(a.volume, b.volume);
        assert_eq!(a.at, b.at);
        assert_eq!(a.caliber.track, b.caliber.track);
        assert_eq!(a.caliber.skill_caliber, b.caliber.skill_caliber);
        assert_eq!(a.caliber.cost_source, b.caliber.cost_source);
        assert_eq!(a.caliber.formula, b.caliber.formula, "公式串是文本，必须逐字还原");
        assert_eq!(a.caliber.data_age_secs, b.caliber.data_age_secs);
        let close = |x: f64, y: f64| (x - y).abs() <= 1e-9 * y.abs().max(1.0);
        for (x, y) in [
            (a.price, b.price),
            (a.loss_isk, b.loss_isk),
            (a.margin_pct, b.margin_pct),
            (a.caliber.sales_tax_pct, b.caliber.sales_tax_pct),
            (a.caliber.broker_pct, b.caliber.broker_pct),
            (a.caliber.unit_cost, b.caliber.unit_cost),
        ] {
            assert!(close(x, y), "{x} 与 {y} 不是同一个数");
        }
    }

    #[test]
    fn payload_round_trips_through_serde_as_alerts_payload_json() {
        // `alerts.payload` 存的就是它的 JSON：推送卡片与提醒中心共用这一份序列化
        // （spec §4.4 的"杜绝双源漂移"），读回来必须逐字段相等。
        let mut hits = detect_expected_sell(
            &[order(7001, false, 97.0, 100)],
            &cost_known(95.0),
            &fees_a5(),
            &HashMap::new(),
            &NameLookup::default(),
            now(),
        );
        let p = hits.remove(0);
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"alert_key\":\"order:7001\""), "{json}");
        assert!(json.contains("\"kind\":\"expected_sell_loss\""), "{json}");
        assert!(json.contains("2026-09-20T10:00:00"), "at 是 RFC3339 时刻：{json}");
        let back: AlertPayload = serde_json::from_str(&json).unwrap();
        assert_payload_equivalent(&back, &p);

        // 三种形态都过一遍序列化（③ 的 order_id / ② 的 is_buy 与 ① 不同，字段漏一个就现形）。
        let (txs, orders, journal) = realized_fixture();
        let realized = detect_realized(&txs, &orders, &journal, &NameLookup::default(), now());
        let back: AlertPayload = serde_json::from_str(&serde_json::to_string(&realized[0]).unwrap()).unwrap();
        assert_payload_equivalent(&back, &realized[0]);
        assert_eq!(back.kind, AlertKind::RealizedLoss);

        let trap = detect_buy_trap(
            &[order(7002, true, 100.0, 200)],
            &[book(STATION_JITA, TYPE, &[(110.0, 100, 5), (90.0, 100, 5)])],
            &fees_a5_br5(),
            &HashMap::new(),
            &NameLookup::default(),
            now(),
        );
        let back: AlertPayload = serde_json::from_str(&serde_json::to_string(&trap[0]).unwrap()).unwrap();
        assert_payload_equivalent(&back, &trap[0]);
        assert_eq!(back.kind, AlertKind::BuyOrderTrap);
    }
}
