//! 迁移。表结构严格对齐方案 v3.1 §5 的**修正后**容量口径。
//!
//! 最关键的一条约束：`station_orders` 是**快照表**，每轮整表替换、不留历史。
//! v3.0 让它"按 ts 滚动保留 7 天"，而实测每轮 75,648 行 × 240 轮/天 × 7 天 ≈ 1.36 亿行
//! （约 47 GB），会直接把客户端写成砖。历史一律走 `station_daily` / `market_daily` 的每日归档。

/// `(版本, 说明, SQL)`。SQL 用 `;` 分隔多条语句。
pub const MIGRATIONS: &[(i64, &str, &str)] = &[(
    1,
    "M0 基线：同步状态、ESI 健康水位、站点单簿快照、基准价",
    r#"
-- schema_migration 由 Db::migrate 以 IF NOT EXISTS 先建，此处不重复。

-- Expires 驱动的节流凭据。重启后据此等待而非立刻爆刷（方案 §7 崩溃恢复）。
CREATE TABLE sync_state (
    url_key         TEXT PRIMARY KEY,
    endpoint        TEXT NOT NULL,
    etag            TEXT,
    last_modified   TEXT,
    expires_raw     TEXT,
    expires_at_unix INTEGER,
    last_ok_at      TEXT,
    fail_streak     INTEGER NOT NULL DEFAULT 0
);

-- 服务端回报的水位历史，供顶栏健康面板与 COLLECTOR_ERROR 判据使用。
CREATE TABLE esi_health (
    ts           INTEGER NOT NULL,
    endpoint     TEXT NOT NULL,
    grp          TEXT,
    limit_s      TEXT,
    used         INTEGER,
    remaining    INTEGER,
    error_remain INTEGER,
    error_reset  INTEGER,
    cache_hit    INTEGER
);
CREATE INDEX ix_esi_health_ts ON esi_health (ts DESC);

-- 快照表：每轮整表替换。
CREATE TABLE station_orders (
    location_id       INTEGER NOT NULL,
    type_id           INTEGER NOT NULL,
    is_npc            INTEGER NOT NULL,
    best_bid          REAL,
    bid_qty           INTEGER NOT NULL DEFAULT 0,
    best_ask          REAL,
    ask_qty           INTEGER NOT NULL DEFAULT 0,
    bid_levels        INTEGER NOT NULL DEFAULT 0,
    ask_levels        INTEGER NOT NULL DEFAULT 0,
    bid_depth         TEXT NOT NULL DEFAULT '[]',
    ask_depth         TEXT NOT NULL DEFAULT '[]',
    skipped_stale     INTEGER NOT NULL DEFAULT 0,
    skipped_thin      INTEGER NOT NULL DEFAULT 0,
    skipped_wholesale INTEGER NOT NULL DEFAULT 0,
    snapshot_lm       TEXT,
    updated_at        INTEGER NOT NULL,
    PRIMARY KEY (location_id, type_id)
);
CREATE INDEX ix_station_orders_type ON station_orders (type_id);
CREATE INDEX ix_station_orders_ask  ON station_orders (best_ask);

CREATE TABLE prices_daily (
    date     TEXT NOT NULL,
    type_id  INTEGER NOT NULL,
    adjusted REAL,
    average  REAL,
    PRIMARY KEY (date, type_id)
);

CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"#,
),
(
    2,
    "M1：站点字典、枢纽池、每轮采集台账",
    r#"
-- NPC 站名。`/v1/universe/stations/{id}` 与 `/v1/universe/names` 都公开可读，
-- 一次解析永久缓存 —— 否则 UI 只能显示一串数字 ID。
CREATE TABLE stations (
    location_id  INTEGER PRIMARY KEY,
    name         TEXT,
    system_id    INTEGER,
    is_npc       INTEGER NOT NULL,
    resolved_at  TEXT
);

-- 每轮重算的枢纽池（§3.4：NPC 站、订单数 ≥ 50、取前 20）。
-- 有界表：只保留最新一轮，跟 station_orders 同样的纪律。
CREATE TABLE hub_pool (
    location_id  INTEGER PRIMARY KEY,
    order_count  INTEGER NOT NULL,
    share_pct    REAL NOT NULL,
    rank         INTEGER NOT NULL,
    computed_at  INTEGER NOT NULL
);

-- 采集台账。环形进度、失败连击、"这一轮到底花了多久"都从这里读。
CREATE TABLE round_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    INTEGER NOT NULL,
    region_id     INTEGER NOT NULL,
    pages         INTEGER NOT NULL,
    orders        INTEGER NOT NULL,
    rows_written  INTEGER NOT NULL,
    seconds       REAL NOT NULL,
    decoded_bytes INTEGER NOT NULL,
    over_network  INTEGER NOT NULL,
    drift_retries INTEGER NOT NULL,
    snapshot_lm   TEXT,
    status        TEXT NOT NULL
);
CREATE INDEX ix_round_log_started ON round_log (started_at DESC);
"#,
),
(
    3,
    "M2：分类树（组→类型）与类型名，供市场浏览器使用",
    r#"
-- 实测唯一可用的建树来源：`/v1/universe/groups/{id}` 给 category_id + types[]，
-- 而 `/v1/markets/categories` 已 404、`/v2/markets/groups/{id}` 的 types 恒为空。
CREATE TABLE inv_groups (
    group_id     INTEGER PRIMARY KEY,
    category_id  INTEGER NOT NULL,
    name         TEXT,
    types        TEXT NOT NULL DEFAULT '[]',
    published    INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX ix_inv_groups_category ON inv_groups (category_id);

CREATE TABLE inv_types (
    type_id    INTEGER PRIMARY KEY,
    name       TEXT,
    group_id   INTEGER,
    resolved_at TEXT
);
CREATE INDEX ix_inv_types_group ON inv_types (group_id);
CREATE INDEX ix_inv_types_name ON inv_types (name);

-- 分类与组的中文名不来自 ESI（官方只有英文），先原样存英文，
-- 需要汉化时改这张表即可，不动建树逻辑。
CREATE TABLE inv_categories (
    category_id  INTEGER PRIMARY KEY,
    name         TEXT
);

-- 搜索用的小写别名表，避免每次 LOWER(name) 全表扫。
CREATE TABLE type_aliases (
    key       TEXT NOT NULL,
    type_id   INTEGER NOT NULL,
    PRIMARY KEY (key, type_id)
);
"#,
),
(
    4,
    "M3：区域历史日线、自选清单与取数范围分层",
    r#"
-- (region, type) 维度的日线。列名照实测响应来：`highest`/`lowest`，不是 §5 纸面写的 high/low。
-- 实测一次请求就返回 418 天（2025-08-01 … 2026-09-22），所以"回填"和"每日增量"
-- 是同一次请求：每天 upsert 的还是那 418 行，只多出最新一天。表因主键而天然有界。
CREATE TABLE market_history (
    region_id   INTEGER NOT NULL,
    type_id     INTEGER NOT NULL,
    date        TEXT    NOT NULL,
    average     REAL,
    highest     REAL,
    lowest      REAL,
    volume      INTEGER NOT NULL DEFAULT 0,
    order_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (region_id, type_id, date)
);

-- L0 的用户那一半。手动加的永不因流动池排名变化而被挤掉。
CREATE TABLE watchlist (
    type_id  INTEGER PRIMARY KEY,
    note     TEXT,
    added_at INTEGER NOT NULL
);

-- L1 = "近 30 天活跃过"的类型。这份记忆必须独立落表：`station_orders` 每轮整表替换，
-- 靠它判活跃等于每天清空一次候选池。M4 的倒卖扫描器与推送器往这里写。
CREATE TABLE history_scope (
    type_id    INTEGER NOT NULL,
    tier       TEXT    NOT NULL,
    reason     TEXT    NOT NULL,
    first_seen INTEGER NOT NULL,
    last_seen  INTEGER NOT NULL,
    PRIMARY KEY (type_id, tier)
);
CREATE INDEX ix_history_scope_tier ON history_scope (tier, last_seen DESC);

-- 每趟回填的台账。M3 验收要的"实测墙钟 / 行数 / 404 数 / 令牌"就是这里最新一行，
-- 而不是终端上滚过去的一次性输出。
CREATE TABLE history_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    INTEGER NOT NULL,
    region_id     INTEGER NOT NULL,
    targets       INTEGER NOT NULL,
    requested     INTEGER NOT NULL,
    gated         INTEGER NOT NULL,
    rows_written  INTEGER NOT NULL,
    absent        INTEGER NOT NULL,
    failed        INTEGER NOT NULL,
    seconds       REAL    NOT NULL,
    decoded_bytes INTEGER NOT NULL,
    tokens_local  INTEGER NOT NULL,
    error_remain  INTEGER,
    status        TEXT    NOT NULL
);
CREATE INDEX ix_history_log_started ON history_log (started_at DESC);
"#,
),
(
    5,
    "M4b：跨区单簿（T1.5）、机会生命周期与 T1.5 台账",
    r#"
-- 机会生命周期（v3.1 §4.2）。主键即 opportunity_key：三个维度本身稳定、可读、
-- 可查，哈希字符串只会给排障添堵。
CREATE TABLE opportunities (
    type_id                 INTEGER NOT NULL,
    buy_loc                 INTEGER NOT NULL,
    sell_loc                INTEGER NOT NULL,
    state                   TEXT    NOT NULL,           -- new/notified/expired/invalidated
    miss_streak             INTEGER NOT NULL DEFAULT 0,
    first_seen_at           INTEGER NOT NULL,
    last_seen_at            INTEGER NOT NULL,
    best_margin_pct         REAL    NOT NULL DEFAULT 0,
    last_margin_pct         REAL    NOT NULL DEFAULT 0,
    last_net_total          REAL    NOT NULL DEFAULT 0,
    last_qty                INTEGER NOT NULL DEFAULT 0,
    notified_at             INTEGER,
    notified_day            TEXT,
    notified_count_day      INTEGER NOT NULL DEFAULT 0,
    last_notified_margin_pct REAL,
    PRIMARY KEY (type_id, buy_loc, sell_loc)
);
CREATE INDEX ix_opportunities_state ON opportunities (state, last_seen_at DESC);

-- T1.5 跨区单簿：按 (站, 类型) UPSERT，只更新本批取到的类型；未取到的旧行
-- 靠读取端 45 min 年龄闸门兜底、24h 剪除——表因此有界（3 枢纽 × ≤200 类型）。
CREATE TABLE xregion_books (
    location_id       INTEGER NOT NULL,
    type_id           INTEGER NOT NULL,
    region_id         INTEGER NOT NULL,
    is_npc            INTEGER NOT NULL,
    best_bid          REAL,
    bid_qty           INTEGER NOT NULL DEFAULT 0,
    best_ask          REAL,
    ask_qty           INTEGER NOT NULL DEFAULT 0,
    bid_levels        INTEGER NOT NULL DEFAULT 0,
    ask_levels        INTEGER NOT NULL DEFAULT 0,
    bid_depth         TEXT NOT NULL DEFAULT '[]',
    ask_depth         TEXT NOT NULL DEFAULT '[]',
    skipped_stale     INTEGER NOT NULL DEFAULT 0,
    skipped_thin      INTEGER NOT NULL DEFAULT 0,
    skipped_wholesale INTEGER NOT NULL DEFAULT 0,
    fetched_at        INTEGER NOT NULL,
    PRIMARY KEY (location_id, type_id)
);
CREATE INDEX ix_xregion_books_type ON xregion_books (type_id);

-- 每趟 T1.5 台账（与 round_log 同纪律：只追加台账，不落行级数据）。
CREATE TABLE xregion_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    INTEGER NOT NULL,
    types         INTEGER NOT NULL,
    regions       INTEGER NOT NULL,
    requests      INTEGER NOT NULL,
    orders        INTEGER NOT NULL,
    books_written INTEGER NOT NULL,
    failed        INTEGER NOT NULL,
    seconds       REAL    NOT NULL,
    status        TEXT    NOT NULL
);
CREATE INDEX ix_xregion_log_started ON xregion_log (started_at DESC);
"#,
),
(
    6,
    "M4c：角色挂链元数据、钱包流水/挂单快照、亏损告警状态机",
    r#"
-- 角色挂链元数据。**无令牌列**：refresh_token 只进 keyring（spec §4.1）。
CREATE TABLE char_meta (
    char_id        INTEGER PRIMARY KEY,
    name           TEXT,
    tx_cursor      TEXT,            -- wallet/transactions 的 since 增量水位（ISO8601）
    journal_cursor TEXT,            -- wallet/journal 的 since 增量水位
    orders_lm      TEXT,            -- 上轮 orders 的 Last-Modified，同源凭据
    first_sync_at  INTEGER,
    last_sync_at   INTEGER
);

-- 钱包流水缓存（transactions）：FIFO 成本基准的原料。
-- 保留期由调用层裁剪（90 天 = spec §4.2 的首启回填窗）；主键不含时间戳。
CREATE TABLE char_tx (
    char_id        INTEGER NOT NULL,
    transaction_id INTEGER NOT NULL,
    date           TEXT    NOT NULL,
    type_id        INTEGER NOT NULL,
    location_id    INTEGER NOT NULL,
    is_buy         INTEGER NOT NULL,
    unit_price     REAL    NOT NULL,
    quantity       INTEGER NOT NULL,
    PRIMARY KEY (char_id, transaction_id)
);
CREATE INDEX ix_char_tx_type ON char_tx (char_id, type_id, date);

-- 上轮挂单快照：供状态边沿判定（"首次转负才告警"要能对比上一轮）。
CREATE TABLE char_orders (
    char_id       INTEGER NOT NULL,
    order_id      INTEGER NOT NULL,
    type_id       INTEGER NOT NULL,
    location_id   INTEGER NOT NULL,
    is_buy        INTEGER NOT NULL,
    price         REAL    NOT NULL,
    volume_remain INTEGER NOT NULL,
    issued        TEXT    NOT NULL,
    duration      INTEGER NOT NULL,
    fetched_at    INTEGER NOT NULL,
    PRIMARY KEY (char_id, order_id)
);
CREATE INDEX ix_char_orders_type ON char_orders (char_id, type_id);

-- 告警状态机（spec §4.4）。alert_key = 挂单轨 order_id / 已实现轨 transaction_id。
CREATE TABLE alerts (
    alert_key          TEXT    PRIMARY KEY,
    kind               TEXT    NOT NULL,     -- expected_sell_loss/realized_loss/buy_order_trap
    char_id            INTEGER NOT NULL,
    type_id            INTEGER NOT NULL,
    location_id        INTEGER NOT NULL,
    is_buy             INTEGER NOT NULL,
    first_seen_at      INTEGER NOT NULL,
    last_seen_at       INTEGER NOT NULL,
    last_loss_isk      REAL    NOT NULL DEFAULT 0,
    last_margin_pct    REAL    NOT NULL DEFAULT 0,
    notified_at        INTEGER,
    notified_day       TEXT,
    notified_count_day INTEGER NOT NULL DEFAULT 0,
    last_notified_loss REAL,
    state              TEXT    NOT NULL,     -- new/notified/cleared
    payload            TEXT    NOT NULL      -- AlertPayload JSON：与推送共用同一序列化
);
CREATE INDEX ix_alerts_state ON alerts (state, last_seen_at DESC);
"#,
),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_unique_and_ascending() {
        for w in MIGRATIONS.windows(2) {
            assert!(w[0].0 < w[1].0, "迁移版本必须递增：{:?}", w);
        }
        assert_eq!(MIGRATIONS.len(), 6);
    }

    #[test]
    fn history_is_keyed_by_region_type_and_utc_date() {
        // 实测口径：history 是 (region, type) 维度且**没有站点维度**（§3.3），
        // 日期是 ESI 的 11:05 UTC 归属日。主键里多任何一个维度都会把它变成追加表。
        let sql = MIGRATIONS[3].2;
        let mh = sql
            .split("CREATE TABLE market_history")
            .nth(1)
            .unwrap()
            .split("CREATE TABLE")
            .next()
            .unwrap();
        assert!(mh.contains("PRIMARY KEY (region_id, type_id, date)"), "{mh}");
        assert!(!mh.contains("location_id"), "history 没有站点维度，别放进来");
        assert!(mh.contains("highest"), "实测字段是 highest/lowest");
    }

    #[test]
    fn bounded_tables_never_key_on_a_timestamp() {
        // station_orders / hub_pool / xregion_books / opportunities / char_orders / alerts
        // 都是"只留最新一轮/有界"的表。一旦让 ts 进入主键，它们就退化成 v3.0 那种
        // 47 GB 的追加表。
        //
        // char_orders 与 station_orders 同纪律：每轮整表替换；alerts 是状态机，
        // 挂单轨/已实现轨各**只留当前那一行**、按 alert_key 就地更新——两者若让
        // 时间戳进主键，同一根挂单每天会多攒一行"亏损历史"。
        // char_meta 按 char_id 一角色一行、char_tx 按 (char_id, transaction_id) 幂等，
        // 都是以 id 为键的有界写入，本测试对它们无意义（列进来只是噪声）。
        for (version, _, sql) in MIGRATIONS {
            for table in [
                "station_orders",
                "hub_pool",
                "xregion_books",
                "opportunities",
                "char_orders",
                "alerts",
            ] {
                if let Some(rest) = sql.split(&format!("CREATE TABLE {table}")).nth(1) {
                    let body = rest.split("CREATE ").next().unwrap_or(rest);
                    let pk = body
                        .split("PRIMARY KEY")
                        .nth(1)
                        .unwrap_or_default()
                        .split(')')
                        .next()
                        .unwrap_or_default();
                    assert!(
                        !pk.contains("ts") && !pk.contains("started_at"),
                        "迁移 {version} 的 {table} 主键含时间戳：{pk}"
                    );
                }
            }
        }
    }

    #[test]
    fn snapshot_table_has_no_timestamp_in_its_primary_key() {
        // 这是 v3.0 容量事故的根因，用断言把它钉住。
        let sql = MIGRATIONS[0].2;
        let so = sql.split("CREATE TABLE station_orders").nth(1).unwrap();
        let so = so.split("CREATE INDEX").next().unwrap();
        assert!(
            !so.contains("ts INTEGER PRIMARY KEY") && so.contains("PRIMARY KEY (location_id, type_id)"),
            "station_orders 不得按时间戳累积历史"
        );
        assert!(!so.contains("\n    ts "), "快照表不应带逐轮 ts 列");
    }

    #[test]
    fn migration_v5_keys_opportunities_by_the_three_dimensions() {
        // opportunity_key 即三维复合键：类型 + 买站 + 卖站本身稳定、可读、可查，
        // 哈希字符串只会给排障添堵（v3.1 §4.2 口径）。
        let sql = MIGRATIONS[4].2;
        let o = sql
            .split("CREATE TABLE opportunities")
            .nth(1)
            .unwrap()
            .split("CREATE INDEX")
            .next()
            .unwrap();
        assert!(
            o.contains("PRIMARY KEY (type_id, buy_loc, sell_loc)"),
            "opportunity_key 即三维复合键，不用哈希字符串"
        );
        assert!(o.contains("notified_day"), "日上限需要自然日字段");
    }

    #[test]
    fn xregion_books_is_bounded_and_age_stamped() {
        // T1.5 跨区单簿：主键只 (站, 类型) —— 时间戳进主键会退化成追加表；
        // fetched_at 是普通列，读取端按年龄闸门过滤，写入端整批替换。
        let sql = MIGRATIONS[4].2;
        let x = sql
            .split("CREATE TABLE xregion_books")
            .nth(1)
            .unwrap()
            .split("CREATE INDEX")
            .next()
            .unwrap();
        assert!(x.contains("PRIMARY KEY (location_id, type_id)"));
        assert!(x.contains("fetched_at"), "读取端要按年龄闸门过滤");
    }

    #[test]
    fn migration_v6_char_tables_are_bounded_and_token_free() {
        let sql = MIGRATIONS[5].2;
        // 令牌绝不在库里：这条断言是 Global Constraints 的机械保证
        for t in ["char_meta", "char_tx", "char_orders", "alerts"] {
            let body = sql.split(&format!("CREATE TABLE {t}")).nth(1).unwrap()
                .split("CREATE ").next().unwrap();
            assert!(!body.contains("token"), "{t} 不得有令牌列：{body}");
        }
        assert!(sql.contains("PRIMARY KEY (char_id, order_id)"), "char_orders 按角色+订单号");
        assert!(sql.contains("PRIMARY KEY (char_id, transaction_id)"), "char_tx 按角色+流水号");
    }

    #[test]
    fn alerts_key_is_alert_key_and_has_notify_fields() {
        let sql = MIGRATIONS[5].2;
        let a = sql.split("CREATE TABLE alerts").nth(1).unwrap()
            .split("CREATE INDEX").next().unwrap();
        assert!(a.contains("alert_key"), "挂单轨 order_id / 已实现轨 transaction_id 统一叫 alert_key");
        assert!(a.contains("notified_day") && a.contains("notified_count_day"), "日限额需要自然日字段");
    }
}
