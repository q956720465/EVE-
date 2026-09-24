//! 命令层的查询与装配测试。
//!
//! 不用 `tauri::test`：那条路要起 MockRuntime，会把 tao 的窗口代码链进测试 exe，
//! 而测试 exe 没有 Windows 清单（Common-Controls v6），进程在跑起来之前就
//! STATUS_ENTRYPOINT_NOT_FOUND。命令体因此拆进 `impl AppState`，这里直接调它，
//! `#[tauri::command]` 只剩一层参数拆装。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use emd_core::alert::{
    mark_pushed, AlertKind, AlertPayload, AlertRecord, AlertState, CaliberSummary,
    COST_SRC_FIFO90, TRACK_EXPECTED,
};
use emd_core::config::{CharConfig, EsiConfig};
use emd_core::esi::EsiClient;
use emd_core::market::{aggregate, AggregateOptions, Hub, Order, STATION_JITA};
use emd_core::push::PushConfig;
use emd_core::scheduler::{RoundState, Stage};
use emd_core::sso::store::{MemoryTokenStore, TokenStore};
use emd_core::sso::token::TokenSet;
use emd_core::store::{now_unix, Db, HistoryRow, RoundRecord};
use emd_core::tree::{CategoryDetail, GroupDetail};

use super::{AlertSettingsIn, AppState};

fn order(price: f64, is_buy: bool, vol: u64) -> Order {
    Order {
        id: (price * 100.0) as u64 + vol,
        type_id: 34,
        location_id: STATION_JITA,
        system_id: 30000142,
        is_buy,
        price,
        volume_remain: vol,
        volume_total: vol,
        min_volume: 1,
        duration: 90,
        issued: chrono::Utc::now(),
        range: Some("region".into()),
    }
}

/// 一份最小可用的库：一个已发布组、一个类型、吉他一轮快照、一个枢纽。
fn seeded() -> Db {
    let db = Db::in_memory().unwrap();
    db.write_groups(&[
        GroupDetail {
            group_id: 18,
            category_id: 10,
            name: Some("Noble Metals".into()),
            published: true,
            types: vec![34],
        },
        GroupDetail {
            group_id: 10,
            category_id: 2,
            name: Some("Stargate".into()),
            published: false,
            types: vec![16],
        },
    ])
    .unwrap();
    db.write_categories(&[CategoryDetail {
        category_id: 10,
        name: Some("Material Elements".into()),
        published: true,
        groups: vec![18],
    }])
    .unwrap();
    let mut group_of = std::collections::HashMap::new();
    group_of.insert(34u32, 18u32);
    db.write_types(&[(34u64, "Tritanium".to_string())], &group_of)
        .unwrap();

    let mut orders = Vec::new();
    for (i, p) in [4.0, 4.1, 4.2].iter().enumerate() {
        orders.push(order(*p, false, (i as u64 + 1) * 1000));
    }
    for (i, p) in [3.9, 3.8, 3.7].iter().enumerate() {
        orders.push(order(*p, true, (i as u64 + 1) * 500));
    }
    let books = aggregate(
        &orders,
        &AggregateOptions {
            now: chrono::Utc::now(),
            min_levels: 1,
            ..Default::default()
        },
    );
    db.write_snapshot(&books, Some("lm-在采集途中会丢"))
        .unwrap();

    db.remember_station(STATION_JITA, true).unwrap();
    db.name_station(STATION_JITA, "Jita IV - Moon 4", Some(30000142))
        .unwrap();
    db.write_hub_pool(&[Hub {
        location_id: STATION_JITA,
        order_count: 6,
        share_pct: 100.0,
        rank: 1,
    }])
    .unwrap();
    db.record_round(&RoundRecord {
        // 刚刚跑完的一轮：request_refresh 必须据此拒绝抢跑。
        started_at: chrono::Utc::now().timestamp(),
        region_id: 10000002,
        pages: 409,
        orders: 408_955,
        rows_written: 1,
        seconds: 60.4,
        decoded_bytes: 96_000_000,
        over_network: 409,
        drift_retries: 0,
        snapshot_lm: Some("lm".into()),
        status: "ok".into(),
    })
    .unwrap();
    db
}

fn state(latest: Option<RoundState>) -> AppState {
    state_with(seeded(), latest)
}

/// 令牌源与 SSO 配置可换的状态：`sso_status`/`sso_logout`/`sso_login` 的测试要在同一份状态上
/// 注入内存凭据库（真实现在系统凭据库里，测试不许碰用户的凭据）。
fn state_sso(db: Db, tokens: Arc<dyn TokenStore>, cfg: CharConfig) -> AppState {
    AppState {
        db: Arc::new(Mutex::new(db)),
        client: Arc::new(EsiClient::new(EsiConfig::default()).unwrap()),
        latest: Arc::new(Mutex::new(None)),
        shutdown: tokio::sync::watch::channel(false).0,
        collector: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tokens,
        cfg,
    }
}

fn state_with(db: Db, latest: Option<RoundState>) -> AppState {
    let mut st = state_sso(db, Arc::new(MemoryTokenStore::default()), CharConfig::default());
    st.latest = Arc::new(Mutex::new(latest.map(|s| (s, Instant::now()))));
    st
}

/// 模拟"本进程抢到采集锁"：collecting 的真相是锁归属，不是状态通道。
fn as_collector(st: &AppState) {
    st.collector
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

fn waiting_next() -> RoundState {
    RoundState {
        round: 7,
        stage: Stage::WaitingNext,
        last_seconds: 60.4,
        orders: 408_955,
        rows_written: 16_662,
        hubs: 1,
        next_in: Duration::from_secs(300),
        snapshot_lm: Some("lm".into()),
    }
}

#[tokio::test]
async fn status_assembles_scheduler_state_and_library_counts() {
    let st = state(Some(waiting_next()));
    as_collector(&st);
    let out = st.status().await.unwrap();
    assert_eq!(out.round, 7, "调度器轮号要透传到界面");
    assert_eq!(out.stage, "WaitingNext");
    assert_eq!(out.jita_rows, 1, "吉他行数来自 COUNT 查询");
    assert_eq!(out.hubs, 1);
    assert_eq!(out.tree, (1, 1, 1), "组 / 类型 / 分类计数");
    assert!(out.next_in_ms > 0 && out.next_in_ms <= 300_000);
    assert_eq!(out.snapshot_lm.as_deref(), Some("lm"));
    assert!(out.collecting, "本进程持有时才是采集者");
}

#[tokio::test]
async fn viewer_mode_reads_the_collectors_round_from_the_database() {
    // 锁被 `emd serve` 持有时的形态：latest 永远是空，但界面不能报零 ——
    // 采集在另一个进程里好好跑着，读数必须来自共享的 round_log。
    let st = state(None);
    let out = st.status().await.unwrap();
    assert!(!out.collecting, "没在采集就要如实告知前端");
    assert_eq!(out.stage, "WaitingNext");
    assert_eq!(out.round, 1, "轮数来自 round_log");
    assert_eq!(out.orders, 408_955, "单数不能闪成 0");
    assert!(out.last_seconds > 0.0);
}

#[tokio::test]
async fn history_series_is_ordered_and_watch_roundtrips() {
    let db = seeded();
    db.write_history(&[
        HistoryRow {
            region_id: 10000002,
            type_id: 34,
            date: "2026-09-22".into(),
            average: Some(3.94),
            highest: Some(4.03),
            lowest: Some(3.79),
            volume: 5_758_959_099,
            order_count: 1_447,
        },
        HistoryRow {
            region_id: 10000002,
            type_id: 34,
            date: "2026-09-21".into(),
            average: Some(3.9),
            highest: Some(3.99),
            lowest: Some(3.8),
            volume: 4_100_000_000,
            order_count: 1_300,
        },
    ])
    .unwrap();
    let st = state_with(db, Some(waiting_next()));

    let bars = st.history(34, None).await.unwrap();
    assert_eq!(bars.len(), 2);
    assert_eq!(bars[0].date, "2026-09-21", "蜡烛图要时间升序");
    assert_eq!(bars[1].highest, Some(4.03));
    assert!(st.history(99_99, None).await.unwrap().is_empty(), "无数据要给空数组而不是报错");

    st.watch_add(34).await.unwrap();
    let list = st.watch_list().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].0, 34);
    assert!(st.watch_remove(34).await.unwrap());
    assert!(!st.watch_remove(34).await.unwrap(), "二次移除要报 false");
}

#[tokio::test]
async fn snapshot_time_falls_back_to_the_database_during_a_round() {
    // Fetching 那条状态不带 Last-Modified。此时"快照 x 前"必须仍指向上一次的时刻，
    // 否则每次采集的 60 秒里界面都会闪成"未知"。
    let mut fetching = waiting_next();
    fetching.stage = Stage::Fetching;
    fetching.snapshot_lm = None;
    fetching.next_in = Duration::ZERO;
    let st = state(Some(fetching));
    let out = st.status().await.unwrap();
    assert_eq!(out.stage, "Fetching");
    assert_eq!(out.next_in_ms, 0);
    assert!(
        out.snapshot_lm.is_some(),
        "采集途中要回落到库里的 last_snapshot_lm"
    );
}

#[tokio::test]
async fn cold_database_reports_idle_instead_of_erroring() {
    let st = state_with(Db::in_memory().unwrap(), None);
    let out = st.status().await.unwrap();
    assert_eq!(out.stage, "Idle");
    assert_eq!(out.round, 0);
    assert_eq!(out.jita_rows, 0);
    assert_eq!(out.tree, (0, 0, 0));
    assert!(out.snapshot_lm.is_none());
    assert!(st.tree().await.unwrap().is_empty());
    assert!(st.hubs().await.unwrap().is_empty());
    assert_eq!(st.search("   ").await.unwrap().len(), 0, "空词不打网络");
}

#[tokio::test]
async fn listing_and_detail_read_the_seeded_round() {
    let st = state(Some(waiting_next()));
    as_collector(&st);

    let rows = st.listing(18, None).await.unwrap();
    assert_eq!(rows.len(), 1, "未发布组不该出现在列表里");
    assert_eq!(rows[0].name, "Tritanium");
    assert_eq!(rows[0].best_ask, Some(4.0), "缺省站点就是吉他");
    assert_eq!(rows[0].best_bid, Some(3.9));

    let elsewhere = st.listing(18, Some(60015157)).await.unwrap();
    assert_eq!(
        elsewhere[0].best_ask, None,
        "换站点不能沿用吉他的价（行仍要在，否则用户以为类型丢了）"
    );
    assert_eq!(elsewhere[0].ask_qty, 0);

    let book = st.detail(34, None).await.unwrap().expect("有快照就该有盘");
    assert_eq!(book.location_name, "Jita IV - Moon 4", "站点名走字典联接");
    assert_eq!(book.ask_depth.len(), 3);
    assert_eq!(book.ask_depth[0].price, 4.0, "卖盘从低价开始");
    assert_eq!(book.bid_depth[0].price, 3.9, "买盘从高价开始");
    assert!(st.detail(99_99, None).await.unwrap().is_none());
}

#[tokio::test]
async fn hubs_carry_names_not_raw_ids() {
    let st = state(Some(waiting_next()));
    as_collector(&st);
    let hubs = st.hubs().await.unwrap();
    assert_eq!(hubs.len(), 1);
    assert_eq!(hubs[0].name, "Jita IV - Moon 4");
    assert_eq!(hubs[0].rank, 1);
    assert_eq!(hubs[0].location_id, STATION_JITA);
}

#[tokio::test]
async fn manual_refresh_refuses_to_jump_the_cache() {
    let st = state(Some(waiting_next()));
    let e = st
        .refresh_hint()
        .await
        .expect_err("刚跑完一轮时必须拒绝");
    assert!(e.contains("还需"), "要说清还要等多久，实际：{e}");
}

#[tokio::test]
async fn two_connections_on_one_file_share_the_schema() {
    // 界面侧与采集线程各持一条连接：后开的那条不该重跑迁移，也不该看不到数据。
    let dir = std::env::temp_dir().join(format!("emd-app-schema-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("emd.sqlite3");
    {
        let first = Db::open(&path).unwrap();
        assert_eq!(first.schema_version().unwrap(), 6, "M4c 迁移 v6 后要升到这里");
        first.record_round(&RoundRecord {
            started_at: 1_700_000_000,
            region_id: 10000002,
            pages: 409,
            orders: 408_955,
            rows_written: 16_662,
            seconds: 60.4,
            decoded_bytes: 96_000_000,
            over_network: 409,
            drift_retries: 0,
            snapshot_lm: None,
            status: "ok".into(),
        })
        .unwrap();
    }
    let second = Db::open(&path).unwrap();
    assert_eq!(second.schema_version().unwrap(), 6);
    assert_eq!(second.round_count().unwrap(), 1, "另一条连接的写入要看得见");
    // WAL/SHM 句柄要随连接一起放掉，否则目录删不掉。
    drop(second);
    std::fs::remove_dir_all(&dir).unwrap();
}

// ---- M4a：倒卖命令 -------------------------------------------------------

#[tokio::test]
async fn flip_params_roundtrip_through_app() {
    let st = state_with(Db::in_memory().unwrap(), None);
    let mut p = emd_core::market::FlipParams::default();
    p.fees.accounting = 4;
    p.margin_threshold_pct = 5.0;
    st.flip_save(p.clone()).await.unwrap();
    assert_eq!(st.flip_params().await.unwrap(), p);

    let mut bad = emd_core::market::FlipParams::default();
    bad.fees.accounting = 9;
    assert!(st.flip_save(bad).await.is_err(), "越界技能必须拒绝");
    assert_eq!(st.flip_params().await.unwrap(), p, "失败不得污染已存参数");
}

#[tokio::test]
async fn trial_is_negative_by_default_and_positive_with_max_skills() {
    let st = state_with(Db::in_memory().unwrap(), None);
    // 默认官方费率 7.5/3：110×0.895 − 100 = −1.55 —— 试算行拦住的就是这种单。
    let t = st.trial(100.0, 110.0, 10).await.unwrap();
    assert!((t.net_per_unit + 1.55).abs() < 1e-9);
    assert!(t.net_total < 0.0);
    assert!(t.margin_pct < 0.0);

    let mut max = emd_core::market::FlipParams::default();
    max.fees.accounting = 5;
    max.fees.broker_relations = 5;
    st.flip_save(max).await.unwrap();
    let t2 = st.trial(100.0, 110.0, 10).await.unwrap();
    assert!((t2.net_per_unit - 4.6375).abs() < 1e-9, "满技能 110×0.95125");

    // 输入防护：0 数量/0 或负价直接拒绝，不进除法也不渲染成"绿色 0 收益"。
    assert!(st.trial(100.0, 110.0, 0).await.is_err());
    assert!(st.trial(0.0, 110.0, 10).await.is_err(), "0 买价必须拒绝");
    assert!(st.trial(-1.0, 110.0, 10).await.is_err());
}

/// 倒卖链路的最小库：类型 34 的两站单簿（Jita 卖 100 / 站 60015157 买 130）+ 枢纽池。
fn flip_engine_db() -> Db {
    let db = Db::in_memory().unwrap();
    let mk = |loc: u64, asks: &[(f64, u64)], bids: &[(f64, u64)]| emd_core::market::StationOrderBook {
        location_id: loc,
        type_id: 34,
        is_npc_station: true,
        best_bid: bids.first().map(|&(p, _)| p),
        bid_qty: bids.first().map(|&(_, v)| v).unwrap_or(0),
        best_ask: asks.first().map(|&(p, _)| p),
        ask_qty: asks.first().map(|&(_, v)| v).unwrap_or(0),
        bid_levels: bids.len() as u32,
        ask_levels: asks.len() as u32,
        bid_depth: bids
            .iter()
            .map(|&(price, volume)| emd_core::market::PriceLevel { price, volume, orders: 5 })
            .collect(),
        ask_depth: asks
            .iter()
            .map(|&(price, volume)| emd_core::market::PriceLevel { price, volume, orders: 5 })
            .collect(),
        skipped_stale: 0,
        skipped_thin: 0,
        skipped_wholesale: 0,
    };
    db.write_snapshot(
        &[
            mk(emd_core::market::STATION_JITA, &[(100.0, 1000)], &[]),
            mk(60015157, &[], &[(130.0, 1000)]),
        ],
        Some("lm"),
    )
    .unwrap();
    db.write_hub_pool(&[
        emd_core::market::Hub { location_id: emd_core::market::STATION_JITA, order_count: 100, share_pct: 50.0, rank: 1 },
        emd_core::market::Hub { location_id: 60015157, order_count: 80, share_pct: 40.0, rank: 2 },
    ])
    .unwrap();
    db.name_station(60015157, "Kisogo VII – AIR Laboratories", None).unwrap();
    db
}

#[tokio::test]
async fn scan_flip_assembles_engine_output_with_names() {
    let db = flip_engine_db();
    // 不绕过持久化校验：用默认 3% 阈值 + 真实盈利对
    // （130×0.895 − 100 = 16.35/件），而不是负阈值（那会被且有理由被拒绝写入）。
    let mut p = emd_core::market::FlipParams::default();
    p.min_batch = 1;
    p.capital_isk = 1_000_000.0;
    db.set_flip_params(&p).unwrap();

    let st = state_with(db, None);
    let out = st.flip_scan().await.unwrap();
    assert_eq!(out.rows.len(), 1);
    let r = &out.rows[0];
    assert_eq!(r.type_id, 34);
    assert_eq!(r.type_name, "type_id 34", "inv_types 未建名时用 fallback 串");
    assert_eq!(r.buy_loc_name, "站点 #60003760", "未解名站点用 fallback 串");
    assert_eq!(r.sell_loc_name, "Kisogo VII – AIR Laboratories");
    assert_eq!(r.qty, 500);
    assert!((r.buy_price - 100.0).abs() < 1e-9);
    assert!((r.sell_price - 130.0).abs() < 1e-9);
    assert!((r.margin_pct - 16.35).abs() < 1e-9);
    assert!((r.net_total - 8175.0).abs() < 1e-9);
    assert_eq!(r.vol_source, "depth");
    assert_eq!(r.vol24, 500);
    assert_eq!(out.pairs_evaluated, 1);
    assert_eq!(out.age_secs, None, "没有 round_log → 面板显示先跑采集");
    assert_eq!(out.params, p, "扫完回显当前参数，面板无需再拉");
}

#[tokio::test]
async fn scan_flip_reflects_saved_params_immediately() {
    // 用户报告链路的回归：改参数 → 保存(set_flip_params) → 立即重扫(scan_flip)，
    // 生效税率与净利/净利率必须随新参数走（不随动 = 保存被吞 / 重扫没读新参数）。
    let db = flip_engine_db();
    let mut p = emd_core::market::FlipParams::default();
    p.min_batch = 1;
    p.capital_isk = 1_000_000.0;
    db.set_flip_params(&p).unwrap();
    let st = state_with(db, None);

    let before = st.flip_scan().await.unwrap();
    assert!((before.effective_sales_tax_pct - 7.5).abs() < 1e-9);
    assert!((before.effective_broker_pct - 3.0).abs() < 1e-9);
    assert!((before.rows[0].margin_pct - 16.35).abs() < 1e-9, "130×0.895 − 100");

    // 保存"销售税基 5.0 + 满技能"：税 2.25% / 中介 1.5% / margin 25.125%。
    let mut p2 = p.clone();
    p2.fees.sales_tax_pct = 5.0;
    p2.fees.accounting = 5;
    p2.fees.broker_relations = 5;
    st.flip_save(p2.clone()).await.unwrap();

    let after = st.flip_scan().await.unwrap();
    assert!((after.effective_sales_tax_pct - 2.25).abs() < 1e-9, "5.0 × (1−0.11×5)");
    assert!((after.effective_broker_pct - 1.5).abs() < 1e-9, "3.0 − 0.3pp×5");
    assert!((after.rows[0].margin_pct - 25.125).abs() < 1e-9, "130×0.9625 − 100");
    assert!((after.rows[0].net_total - 12_562.5).abs() < 1e-9, "25.125 × 500");
    assert_ne!(before.rows[0].margin_pct, after.rows[0].margin_pct, "保存后重扫数字必须变");
    assert_eq!(after.params, p2, "扫完回显新参数");
}

#[tokio::test]
async fn scan_flip_marks_xregion_age_on_rows() {
    // 跨区数据年龄角标的端到端回归：只要目标站出现在 xregion_books 里，
    // flip_scan 的对应 row 必须携带 xregion_age_secs=Some(_)——诚实标注"数据来自 T1.5"。
    let db = Db::in_memory().unwrap();
    let mk = |loc: u64, asks: &[(f64, u64)], bids: &[(f64, u64)]| emd_core::market::StationOrderBook {
        location_id: loc,
        type_id: 34,
        is_npc_station: true,
        best_bid: bids.first().map(|&(p, _)| p),
        bid_qty: bids.first().map(|&(_, v)| v).unwrap_or(0),
        best_ask: asks.first().map(|&(p, _)| p),
        ask_qty: asks.first().map(|&(_, v)| v).unwrap_or(0),
        bid_levels: bids.len() as u32,
        ask_levels: asks.len() as u32,
        bid_depth: bids
            .iter()
            .map(|&(price, volume)| emd_core::market::PriceLevel { price, volume, orders: 5 })
            .collect(),
        ask_depth: asks
            .iter()
            .map(|&(price, volume)| emd_core::market::PriceLevel { price, volume, orders: 5 })
            .collect(),
        skipped_stale: 0,
        skipped_thin: 0,
        skipped_wholesale: 0,
    };
    // station_orders 只放 Jita；60008494 (Amarr) 走 xregion_books 的 T1.5 路径。
    db.write_snapshot(&[mk(emd_core::market::STATION_JITA, &[(100.0, 1000)], &[])], Some("lm"))
        .unwrap();
    db.write_hub_pool(&[emd_core::market::Hub {
        location_id: emd_core::market::STATION_JITA,
        order_count: 100,
        share_pct: 50.0,
        rank: 1,
    }])
    .unwrap();
    db.write_xregion_books(
        60008494,
        10000043,
        &[34],
        &[mk(60008494, &[], &[(130.0, 1000)])],
    )
    .unwrap();
    let mut p = emd_core::market::FlipParams::default();
    p.min_batch = 1;
    p.capital_isk = 1_000_000.0;
    db.set_flip_params(&p).unwrap();
    let st = state_with(db, None);

    let out = st.flip_scan().await.unwrap();
    assert_eq!(out.rows.len(), 1, "Jita→Amarr 唯一对");
    let row = &out.rows[0];
    assert_eq!(row.buy_loc, emd_core::market::STATION_JITA);
    assert_eq!(row.sell_loc, 60008494);
    assert!(
        row.xregion_age_secs.is_some(),
        "卖站在 xregion_books 里 → 必须携带跨区年龄"
    );
    assert!(
        row.xregion_age_secs.unwrap() < 60,
        "刚写入 → 年龄接近 0，实际值 {:?}",
        row.xregion_age_secs
    );
}

// ---- M4c：提醒中心与通道配置（T14） --------------------------------------

/// 合成 JWT（照 `emd-daemon` 的 `char_status_output_carries_no_token_material` 先例）：
/// payload 段是 `{"sub":"CHARACTER:EVE:90000001","name":"Pilot One","exp":1790000000}` 的
/// base64url（无 padding），签名段塞哨兵 —— 真令牌的签名段同样是不可读的随机串。
/// 哨兵串真切进过解析器（下面的正向断言证明角色身份是从**这个**令牌里解出来的），
/// 所以"输出里没有哨兵"不是因为函数压根没看令牌。
const AT_SIG: &str = "AT-SENTINEL-ACCESS-TOKEN-9f3a";
const RT_SENTINEL: &str = "RT-SENTINEL-REFRESH-TOKEN-7b21";
const JWT_PAYLOAD: &str = "eyJzdWIiOiJDSEFSQUNURVI6RVZFOjkwMDAwMDAxIiwibmFtZSI6IlBpbG90IE9uZSIsImV4cCI6MTc5MDAwMDAwMH0";

fn fake_token(expires_at: i64) -> TokenSet {
    TokenSet {
        access_token: format!("eyJhbGciOiJub25lIn0.{JWT_PAYLOAD}.{AT_SIG}"),
        refresh_token: RT_SENTINEL.to_string(),
        expires_at,
    }
}

/// 一张告警载荷（字段照 T8 契约）。名字用字典里的真名与真站点名，便于断言回填确实走了库；
/// 口径摘要的两条串走 `emd-core` 的公开常量，测试里不手抄字面量。
fn alert_payload(key: &str, kind: AlertKind) -> AlertPayload {
    AlertPayload {
        alert_key: key.to_string(),
        kind,
        order_id: 7001,
        type_id: 34,
        type_name: "Tritanium".to_string(),
        location_id: STATION_JITA,
        location_name: "Jita IV - Moon 4".to_string(),
        is_buy: matches!(kind, AlertKind::BuyOrderTrap),
        price: 97.0,
        volume: 100,
        at: chrono::DateTime::parse_from_rfc3339("2026-09-20T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        loss_isk: 127.375,
        margin_pct: -1.34,
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

/// 直接读库里的通道配置：断言"写进去的到底是什么"，而不是只看命令回显。
fn stored_push_config(st: &AppState) -> PushConfig {
    PushConfig::load(&st.db.lock().unwrap()).unwrap()
}

#[tokio::test]
async fn alert_rows_carry_kind_names_and_notification_history() {
    const CHAR: u64 = 90_000_001;
    let db = seeded();
    // ① 已推送的挂单轨：通知史里记的是**亏损率**（穿透判定比"再低 2pp"），不是金额。
    let mut notified = AlertRecord::from_payload(
        &alert_payload("order:7001", AlertKind::ExpectedSellLoss),
        CHAR,
        1_790_000_000,
    );
    mark_pushed(&mut notified, 1_790_000_000, "2026-09-24");
    db.save_alert(&notified).unwrap();
    // ② 周期结束的已实现轨：行留在提醒中心（闸门只管推不推，从不删行）。
    let mut cleared = AlertRecord::from_payload(
        &alert_payload("tx:2", AlertKind::RealizedLoss),
        CHAR,
        1_789_000_000,
    );
    cleared.state = AlertState::Cleared;
    db.save_alert(&cleared).unwrap();
    // ③ 名字字典里没有的类型/站点：fallback 串必须顶上，而不是整行消失。
    let mut unknown = alert_payload("order:7002", AlertKind::BuyOrderTrap);
    unknown.type_id = 88_087;
    unknown.location_id = 60_008_494;
    unknown.loss_isk = 975.0;
    unknown.margin_pct = -4.875;
    db.save_alert(&AlertRecord::from_payload(&unknown, CHAR, 1_788_000_000))
        .unwrap();

    let st = state_with(db, None);
    let rows = st.alerts_list().await.unwrap();
    assert_eq!(rows.len(), 3, "三种形态各一行：{rows:?}");

    // 顺序 = load_alerts 的口径：最后见到在前（提醒中心要"最近出现的先看到"）。
    let r = &rows[0];
    assert_eq!(r.alert_key, "order:7001", "去重键原样透传（TEXT 形态是检索入口）");
    assert_eq!(r.kind, "expected_sell_loss", "落库串与 UI 角标走同一套 snake_case 映射");
    assert_eq!(r.state, "notified");
    assert_eq!(r.char_id, CHAR);
    assert_eq!(r.type_name, "Tritanium", "名字回填走字典");
    assert_eq!(r.location_name, "Jita IV - Moon 4");
    assert!(!r.is_buy, "挂卖单轨的方向是「卖」");
    assert!((r.last_loss_isk - 127.375).abs() < 1e-9, "亏损额是 ISK");
    assert!((r.last_margin_pct + 1.34).abs() < 1e-9);
    assert_eq!(r.notified_day.as_deref(), Some("2026-09-24"));
    assert_eq!(r.notified_count_day, 1);
    assert_eq!(
        r.last_notified_margin_pct,
        Some(-1.34),
        "这一列存的是**上次推送时的亏损率**（pp），不是亏损额 —— 拿 127.375 当它就是把两个单位混了"
    );
    assert!(
        r.payload.contains("\"alert_key\":\"order:7001\""),
        "payload 是推送卡片与提醒中心共用的那一份序列化：{}",
        r.payload
    );
    assert!(
        r.payload.contains("\"track\":\"预期·估算费率\""),
        "口径摘要必须跟过来（面板从 payload 里读它，不另造结构）：{}",
        r.payload
    );

    assert_eq!(rows[1].kind, "realized_loss");
    assert_eq!(rows[1].state, "cleared", "已清的行照样在列表里");
    assert_eq!(rows[1].notified_count_day, 0, "没推过的条目通知计数为 0");

    let r = &rows[2];
    assert_eq!(r.kind, "buy_order_trap");
    assert!(r.is_buy, "买单轨的方向是「买」");
    assert_eq!(r.type_name, "type_id 88087", "未命名类型用 fallback 串（照 flip_scan 先例）");
    assert_eq!(r.location_name, "站点 #60008494", "未解名站点用 fallback 串");
    assert_eq!(r.first_seen_at, 1_788_000_000);
    assert_eq!(r.last_seen_at, 1_788_000_000);
}

#[tokio::test]
async fn alert_settings_keep_the_secret_unless_explicitly_cleared() {
    const TOKEN: &str = "tok0000000000000000000000000000000000000000000000000000000000000fake";
    const SECRET: &str = "SEC0000000000000000000000000000000000000000000000000000000000000fake";
    let db = Db::in_memory().unwrap();
    let webhook = format!("https://oapi.dingtalk.com/robot/send?access_token={TOKEN}");
    PushConfig {
        webhook: webhook.clone(),
        secret: SECRET.to_string(),
        enabled: false,
    }
    .save(&db)
    .unwrap();
    let st = state_with(db, None);

    // 回显：webhook 打码、密钥只以"已配置"这一个比特露面（明文与残片都不给前端）。
    let echo = st.alert_settings().await.unwrap();
    assert!(echo.webhook.contains("***") && !echo.webhook.contains(TOKEN), "{echo:?}");
    assert!(echo.secret_set && !echo.enabled);
    assert_eq!(
        echo.channels,
        vec!["local"],
        "开关没开 → 只有本地提醒中心在场（本地那条恒在）"
    );

    // ① 只翻开关：回显里的打码 webhook（= 保留库里那条）+ secret=None（= 保留原密钥）。
    //    这是"用户只想开推送"的最常见动作 —— 若把空值/打码值当成新值写下去，
    //    密钥就成了 `***`，之后每条推送 errcode 310000，而配置面上看不出任何毛病。
    let saved = st
        .alert_settings_save(AlertSettingsIn {
            webhook: echo.webhook.clone(),
            secret: None,
            enabled: true,
        })
        .await
        .unwrap();
    assert!(saved.enabled && saved.secret_set, "翻开关不得动密钥：{saved:?}");
    assert_eq!(saved.channels, vec!["local", "dingtalk"], "开关打开且 webhook 有值 → 钉钉进场");
    let stored = stored_push_config(&st);
    assert_eq!(stored.secret, SECRET, "库里还是原密钥");
    assert_eq!(stored.webhook, webhook, "打码回显 = 保留库里的 webhook（不是把 *** 存下去）");

    // ② 换 webhook（明文），密钥同时不动。
    let new_webhook = format!("https://oapi.dingtalk.com/robot/send?access_token={TOKEN}2");
    st.alert_settings_save(AlertSettingsIn {
        webhook: new_webhook.clone(),
        secret: None,
        enabled: true,
    })
    .await
    .unwrap();
    let stored = stored_push_config(&st);
    assert_eq!(stored.webhook, new_webhook, "明文 webhook 是新值");
    assert_eq!(stored.secret, SECRET, "密钥一个字都没经过 UI 的手");

    // ③ 显式清空（`Some("")`）：纯关键词模式的机器人是**合法配置**，与"没改"是两件事。
    let saved = st
        .alert_settings_save(AlertSettingsIn {
            webhook: new_webhook.clone(),
            secret: Some(String::new()),
            enabled: true,
        })
        .await
        .unwrap();
    assert!(!saved.secret_set, "清掉之后回显要说「未配置」");
    assert_eq!(stored_push_config(&st).secret, "", "显式清空要真的清掉");

    // ④ 换新密钥。
    let saved = st
        .alert_settings_save(AlertSettingsIn {
            webhook: new_webhook.clone(),
            secret: Some(format!("{SECRET}2")),
            enabled: true,
        })
        .await
        .unwrap();
    assert!(saved.secret_set);
    assert_eq!(stored_push_config(&st).secret, format!("{SECRET}2"));

    // ⑤ 打码值塞进 secret 一律拒绝（回显串回存就是"之后每条都 310000"的种子），
    //    且拒绝时不得留下半份配置。
    let before = stored_push_config(&st);
    let err = st
        .alert_settings_save(AlertSettingsIn {
            webhook: new_webhook.clone(),
            secret: Some("***".to_string()),
            enabled: true,
        })
        .await
        .expect_err("打码串不许进库");
    assert!(err.contains("***"), "要说清拒绝的是什么：{err}");
    assert_eq!(stored_push_config(&st), before, "拒绝写入不得留下半份配置");

    // ⑥ 清空 webhook 框 = 摘掉那条通道（明文空串不是打码值，按新值写）：开关开着也不会有
    //    钉钉通道 —— `channels()` 只在 webhook 填了时才装它。
    let saved = st
        .alert_settings_save(AlertSettingsIn {
            webhook: String::new(),
            secret: None,
            enabled: true,
        })
        .await
        .unwrap();
    assert_eq!(saved.webhook, "", "空 webhook 回显也是空串");
    assert_eq!(saved.channels, vec!["local"], "webhook 空着 → 钉钉不在场");
}

#[tokio::test]
async fn sso_status_reports_not_linked_without_a_token() {
    let st = state_sso(
        Db::in_memory().unwrap(),
        Arc::new(MemoryTokenStore::default()),
        CharConfig::default(),
    );
    let s = st.sso_status().await.unwrap();
    assert!(
        !s.linked,
        "凭据库读空 = 未挂链，不是错误（store 层刻意把四种读空形态收拢成 Ok(None)）"
    );
    assert!(s.char_id.is_none() && s.char_name.is_none());
    assert!(s.expires_at.is_none() && !s.token_expired);
    assert_eq!(s.last_sync_at, None, "没有角色 id 就没有可查的行键");
    assert!(s.token_error.is_none());
    // 没配 client_id 的默认态：这两条正是"为什么什么都不动"的答案。
    assert!(!s.char_sync_enabled && !s.client_id_set);
}

#[tokio::test]
async fn sso_status_reads_the_character_from_the_token_and_never_echoes_it() {
    const CHAR: u64 = 90_000_001;
    let db = Db::in_memory().unwrap();
    db.upsert_char_meta(CHAR, "Pilot One", 1_789_000_000).unwrap();
    let store = Arc::new(MemoryTokenStore::default());
    let expires = now_unix() + 3600;
    store.save(&fake_token(expires)).unwrap();
    let st = state_sso(
        db,
        store.clone(),
        CharConfig {
            client_id: "test-client-id".into(),
            enabled: true,
            ..Default::default()
        },
    );

    let s = st.sso_status().await.unwrap();
    // 正向证据：身份确实是从**这个**令牌里解出来的（下面的"没有哨兵"才有意义）。
    assert!(s.linked);
    assert_eq!(s.char_id, Some(CHAR), "角色 id 取自令牌的 sub");
    assert_eq!(s.char_name.as_deref(), Some("Pilot One"));
    assert_eq!(s.expires_at, Some(expires));
    assert!(!s.token_expired, "离过期还有一小时");
    assert_eq!(s.last_sync_at, Some(1_789_000_000), "上次同步来自 char_meta");
    assert!(s.char_sync_enabled && s.client_id_set);
    assert!(s.token_error.is_none());

    // 要害：整份状态序列化之后**令牌原文一个字都不在**（前端拿到的就是这个 JSON）。
    let json = serde_json::to_string(&s).unwrap();
    assert!(!json.contains(AT_SIG) && !json.contains(RT_SENTINEL), "令牌进状态了：{json}");
    assert!(!json.contains("SENTINEL"), "{json}");

    // 已过期：判定走 `TokenSet::is_expired`（提前 60 s 视为过期），TS 侧不复制那条边界。
    store.save(&fake_token(now_unix() - 10)).unwrap();
    assert!(st.sso_status().await.unwrap().token_expired);

    // 异形令牌（换过序列化格式 / 凭据被手工改过）：如实报"取不出角色"，不报错、不猜 id，
    // 也不把令牌原文带进错误串（`sso::flow` 的报错只带 JWT 段数与字段名）。
    store
        .save(&TokenSet {
            access_token: format!("not-a-jwt-{AT_SIG}"),
            refresh_token: RT_SENTINEL.to_string(),
            expires_at: expires,
        })
        .unwrap();
    let s = st.sso_status().await.unwrap();
    assert!(!s.linked && s.char_id.is_none(), "解不出角色就不算挂链（那种状态同步会整轮失败）");
    let err = s.token_error.clone().expect("要说清为什么取不出角色");
    assert!(err.contains("段"), "报错要能读懂（JWT 段数）：{err}");
    assert!(!err.contains("SENTINEL"), "报错串带了令牌原文：{err}");
    assert!(!serde_json::to_string(&s).unwrap().contains("SENTINEL"), "{s:?}");
}

#[tokio::test]
async fn sso_logout_clears_the_token_store_and_is_idempotent() {
    let store = Arc::new(MemoryTokenStore::default());
    store.save(&fake_token(now_unix() + 3600)).unwrap();
    let st = state_sso(Db::in_memory().unwrap(), store.clone(), CharConfig::default());
    assert!(st.sso_status().await.unwrap().linked);

    st.sso_logout().await.unwrap();
    assert!(store.load().unwrap().is_none(), "登出必须清掉凭据库里的令牌");
    let s = st.sso_status().await.unwrap();
    assert!(!s.linked && s.expires_at.is_none(), "登出后状态要如实变成未挂链");

    // 幂等：本来没登录时再点一次不该报错（`TokenStore::clear` 的契约）。
    st.sso_logout().await.unwrap();
}

#[tokio::test]
async fn sso_login_fails_fast_without_a_client_id() {
    // 没配 client_id 时授权页必然报错：让用户点一次浏览器、白等满 180 秒超时，
    // 再从那句"没反应"里猜是配置没填 —— 所以这里必须当场拒绝并给出可照着做的话。
    // 本测试不打网络、不开浏览器（`login` 根本不会被调用）。
    let st = state_sso(
        Db::in_memory().unwrap(),
        Arc::new(MemoryTokenStore::default()),
        CharConfig::default(),
    );
    let err = st.sso_login().await.expect_err("没配 client_id 必须当场拒绝");
    assert!(err.contains("EMD_CHAR_CLIENT_ID"), "要说清去改哪个键：{err}");
}
