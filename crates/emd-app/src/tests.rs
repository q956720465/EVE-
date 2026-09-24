//! 命令层的查询与装配测试。
//!
//! 不用 `tauri::test`：那条路要起 MockRuntime，会把 tao 的窗口代码链进测试 exe，
//! 而测试 exe 没有 Windows 清单（Common-Controls v6），进程在跑起来之前就
//! STATUS_ENTRYPOINT_NOT_FOUND。命令体因此拆进 `impl AppState`，这里直接调它，
//! `#[tauri::command]` 只剩一层参数拆装。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use emd_core::config::EsiConfig;
use emd_core::esi::EsiClient;
use emd_core::market::{aggregate, AggregateOptions, Hub, Order, STATION_JITA};
use emd_core::scheduler::{RoundState, Stage};
use emd_core::store::{Db, HistoryRow, RoundRecord};
use emd_core::tree::{CategoryDetail, GroupDetail};

use super::AppState;

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

fn state_with(db: Db, latest: Option<RoundState>) -> AppState {
    AppState {
        db: Arc::new(Mutex::new(db)),
        client: Arc::new(EsiClient::new(EsiConfig::default()).unwrap()),
        latest: Arc::new(Mutex::new(latest.map(|s| (s, Instant::now())))),
        shutdown: tokio::sync::watch::channel(false).0,
        collector: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    }
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
    let st = AppState {
        db: Arc::new(Mutex::new(Db::in_memory().unwrap())),
        client: Arc::new(EsiClient::new(EsiConfig::default()).unwrap()),
        latest: Arc::new(Mutex::new(None)),
        shutdown: tokio::sync::watch::channel(false).0,
        collector: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };
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
