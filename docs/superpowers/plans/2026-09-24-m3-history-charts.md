# M3 历史图表（剩余工作）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把已实现但未接线的 emd-core M3 历史层接通、加单一采集编排器与单实例锁（修双采集器）、并入 T3 每日回填、接进 daemon/app 与前端 ECharts 蜡烛图，达成"动态波动分析可用"。

**Architecture:** `emd_core::collector` 用文件锁分出"采集者/查看者"，采集者在 T1 节拍循环的每轮之后按 `history_due` 调 `market::history::backfill`（复用已实现的 sync_state 幂等闸门与 L0/L1 目标选取）。daemon 与 app 都改调 collector，不再各写一套 Scheduler 胶水。前端在单物品详情加"历史"页签（ECharts 蜡烛 + dataZoom，1D 分钟线置灰）。

**Tech Stack:** Rust 2021 / tokio / rusqlite / reqwest / thiserror；Tauri 2；Vite + React 18 + TS；新增 `fs2`（跨平台文件独占锁）与 `echarts`。

**依据 spec：** `docs/superpowers/specs/2026-09-24-m3-history-charts-design.md`（尤其 §0 现状校正）。

---

## 前置说明（开工前必读）

- **当前 `emd-core` 编译不过**：`store/db.rs:1111` 已引用 `crate::market::history::TIER_L0`，但 `market/mod.rs` 未声明 `mod history;`。Task 1 是第一阻塞项，必须最先做。
- **不要重做已完成的部分**：迁移 v4、`market/history.rs`、`db.rs` 的历史方法均已存在且带测试。本计划只做"接线 + 编排器 + 集成 + 前端"。
- **git**：仓库根无 `.git`，`git` 仅在 Git Bash（MSYS2）可用、不在 PowerShell PATH。每个 Task 末尾的 commit 步骤：仅当用户已同意纳入版本管理时执行（在 Git Bash 里 `git add/commit`）；否则**跳过 commit，保留改动**。不得自行 `git init`。
- **构建/运行坑（方案附录 C）**：改完 emd-core 先 `cargo build -p emd-core -p emd-daemon` 再跑 `emd`；PowerShell 复杂命令用 `.ps1` 脚本文件跑，避免 `$_` 语言模式坑。`tauri::test` 不可用 → app 命令体一律放 `impl AppState`，用普通单测覆盖。

## File Structure（本计划涉及）

- Create `crates/emd-core/src/collector.rs` — `InstanceLock`（fs2）+ `Collector`（采集者/查看者分流）+ `run_t3_pass`。
- Modify `crates/emd-core/src/lib.rs` — `pub mod collector;`。
- Modify `crates/emd-core/src/market/mod.rs` — `mod history;` + `pub use history::{...}`。
- Modify `crates/emd-core/src/scheduler.rs` — `run` 增加 `after_round` 异步钩子（默认空实现），供 T3 在轮间串行执行。
- Modify `Cargo.toml`（workspace）+ `crates/emd-core/Cargo.toml` — 加 `fs2`。
- Modify `crates/emd-daemon/src/main.rs` — `run_scheduler` 走 `Collector`；新增 `history`、`watch` 子命令。
- Modify `crates/emd-app/src/lib.rs` — 锁分流；`get_history`/watch 命令；status 查看者回落；`impl AppState` 命令体。
- Modify `crates/emd-app/src/tests.rs` — 新命令体单测。
- Modify `web/package.json` — 加 `echarts`。
- Modify `web/src/types.ts` / `api.ts` / `store.ts` — `HistoryBar` 类型、`history()`/watch API + fixtures。
- Create `web/src/components/HistoryChart.tsx` — ECharts 蜡烛图。
- Modify `web/src/components/OrderBook.tsx` — 加"历史"页签。

---

## Task 1: 接线 market::history（解编译阻塞）

**Files:**
- Modify: `crates/emd-core/src/market/mod.rs`

- [ ] **Step 1: 声明并导出 history 模块**

在 `market/mod.rs` 顶部 `mod` 列表加 `mod history;`，并在 `pub use` 区加：

```rust
pub use history::{
    backfill, fetch_one, history_due, path, Estimate, Fetched, HistoryConfig, PassReport,
    ESI_WINDOW_DAYS, KEEP_DAYS, L0_DAILY_CAP, L1_DAILY_CAP, TIER_L0, TIER_L1,
};
```

- [ ] **Step 2: 编译 emd-core**

Run: `cargo build -p emd-core`
Expected: 成功（`db.rs:1111` 的 `crate::market::history::TIER_L0` 现可解析）。若报 `SyncState`/`expires_raw` 字段缺失等，按 history.rs 的引用在 `store/mod.rs` 的 `pub use` 补齐（`SyncState` 已导出）。

- [ ] **Step 3: 跑已有历史相关测试**

Run: `cargo test -p emd-core history`
Expected: `market::history::tests`（`parses_the_recorded_payload_verbatim` 等）与 `store::schema::tests`（`history_is_keyed_by_region_type_and_utc_date`、`versions_are_unique_and_ascending`==4）全部 PASS。

- [ ] **Step 4: Commit（仅在用户已纳入版本管理时）**

```bash
git add crates/emd-core/src/market/mod.rs && git commit -m "fix(emd-core): wire market::history into module tree"
```

---

## Task 2: 单实例锁 InstanceLock（fs2）

**Files:**
- Modify: `Cargo.toml`（workspace.dependencies）
- Modify: `crates/emd-core/Cargo.toml`（dependencies）
- Create: `crates/emd-core/src/collector.rs`（先只放 `InstanceLock`）
- Test: `crates/emd-core/src/collector.rs` 内 `#[cfg(test)] mod tests`

- [ ] **Step 1: 加 fs2 依赖**

`Cargo.toml` 的 `[workspace.dependencies]` 加：

```toml
fs2 = "0.4"
```

`crates/emd-core/Cargo.toml` 的 `[dependencies]` 加：

```toml
fs2 = { workspace = true }
```

- [ ] **Step 2: 在 lib.rs 声明模块**

`crates/emd-core/src/lib.rs` 的 `pub mod` 列表加 `pub mod collector;`。

- [ ] **Step 3: 写失败测试（第二把锁拿不到同一文件）**

在 `collector.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_holder_cannot_acquire_the_same_lock() {
        let dir = std::env::temp_dir().join(format!("emd-lock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let first = InstanceLock::acquire(&dir).expect("第一把应成功");
        assert!(InstanceLock::acquire(&dir).is_none(), "同目录第二把锁必须失败");
        drop(first);
        assert!(InstanceLock::acquire(&dir).is_some(), "释放后应可重取");
    }
}
```

- [ ] **Step 4: 运行确认失败**

Run: `cargo test -p emd-core collector::tests::second_holder`
Expected: 编译失败（`InstanceLock` 未定义）。

- [ ] **Step 5: 实现 InstanceLock**

在 `collector.rs` 顶部：

```rust
//! 进程级采集编排：单实例锁分"采集者/查看者"，采集者在 T1 轮间挂 T3 历史回填。
//!
//! 为什么要锁：`emd-daemon serve` 与 Tauri 壳此前各跑一套 Scheduler，同打 ESI 会
//! 令牌翻倍、`station_orders` 整表替换互相打架（方案 §7 常驻 + 交接的"双采集器"缺口）。

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use fs2::FileExt;

/// 持有 `collector.lock` 的独占句柄；drop 即释放（进程崩溃由 OS 回收句柄）。
pub struct InstanceLock {
    // 句柄必须在锁生命周期内存活，字段本身不被读取。
    _file: File,
    path: PathBuf,
}

impl InstanceLock {
    /// 抢到返回 Some；已被别的进程持有返回 None。锁文件留在盘上无妨（内容为空）。
    pub fn acquire(data_dir: &Path) -> Option<Self> {
        let path = data_dir.join("collector.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .ok()?;
        file.try_lock_exclusive().ok().map(|_| Self { _file: file, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
```

- [ ] **Step 6: 运行确认通过**

Run: `cargo test -p emd-core collector::tests::second_holder`
Expected: PASS。

- [ ] **Step 7: Commit（可选）**

```bash
git add Cargo.toml crates/emd-core/Cargo.toml crates/emd-core/src/lib.rs crates/emd-core/src/collector.rs && git commit -m "feat(emd-core): InstanceLock via fs2 for single collector process"
```

---

## Task 3: Scheduler 轮间钩子（为 T3 让路）

**Files:**
- Modify: `crates/emd-core/src/scheduler.rs`

- [ ] **Step 1: 写失败测试（after_round 每轮调用一次）**

在 `scheduler.rs` 的 `mod tests` 加：

```rust
#[tokio::test]
async fn after_round_hook_runs_once_per_completed_round() {
    // 用一条永不真正联网的路径不好造，这里只验证"钩子被调、且能拿到轮号"。
    // 直接测新签名 run 的循环体：以 rounds=0 立即返回不触发；改用注入 1 轮的最小桩。
    // 因 run_round 依赖真实 client，本测试仅断言 API 存在与类型正确（编译期即验证）。
    let db = Arc::new(Db::in_memory().unwrap());
    let client = Arc::new(EsiClient::new(Default::default()).unwrap());
    let sched = Scheduler::new(client, db, SchedulerConfig { rounds: Some(0), ..Default::default() });
    let hits = std::sync::atomic::AtomicUsize::new(0);
    // rounds=0：循环体在跑第一轮前就因达上限返回，钩子不应被调。
    let (_tx, rx) = tokio::sync::watch::channel(false);
    sched
        .run_with_after_round(rx, |_n| async { hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed) })
        .await
        .unwrap();
    assert_eq!(hits.load(std::sync::atomic::Ordering::Relaxed), 0);
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p emd-core after_round`
Expected: 编译失败（`run_with_after_round` 未定义）。

- [ ] **Step 3: 抽出带钩子的 run**

把现有 `pub async fn run(&self, shutdown)` 改为委托到新方法，钩子默认空：

```rust
pub async fn run(&self, shutdown: watch::Receiver<bool>) -> Result<u64> {
    self.run_with_after_round(shutdown, |_| std::future::ready(())).await
}

/// 每完成一轮（`run_round` 返回后、进入 sleep 前）调用 `after_round`。
/// 与 T1 同线程串行 await：此刻没有任何对 `Db` 的借用，可安全跑 T3 回填。
pub async fn run_with_after_round<F, Fut>(
    &self,
    mut shutdown: watch::Receiver<bool>,
    mut after_round: F,
) -> Result<u64>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    // —— 以下循环体与原 run() 一致，仅在 publish_state(...) 之后、
    //    处理失败的 rate-limit/退避分支之前，插入一行：
    //        after_round(round).await;
    // 保持"先判 rounds 上限再 sleep"的既有顺序不变（A.8 缺陷 3）。
    // 原 run() 的整段循环逻辑原样搬到这里即可，不改变节拍与失败退避。
    /* ...原 run 循环体，round 完成并发布状态后调用 after_round(round).await;... */
    Ok(0) // 占位：实现时删除，替换为搬过来的循环真实返回。
}
```

> 实现要点：`after_round` 只在 `run_round` **成功或被判定可用后**调用一次；失败整轮（`Stage::Failed`）时不调，避免在缺数据时算历史。搬移时逐行对照原 `run`，只加一行钩子调用，不动 `plan_next` / 退避 / rounds 判断顺序。

- [ ] **Step 4: 运行确认通过 + 回归**

Run: `cargo test -p emd-core scheduler`
Expected: 新测试 PASS，且既有 `normal_round_waits...`、`a_sub_interval...`、`cadence_floor...` 等全绿（未回归）。

- [ ] **Step 5: Commit（可选）**

```bash
git add crates/emd-core/src/scheduler.rs && git commit -m "feat(emd-core): Scheduler::run_with_after_round hook for T3"
```

---

## Task 4: Collector 编排（采集者跑 T1+T3，查看者零请求）

**Files:**
- Modify: `crates/emd-core/src/collector.rs`（在 Task 2 基础上加 `Collector` + `run_t3_pass`）

- [ ] **Step 1: 写失败测试（T3 目标与 due 计算，脱网络）**

```rust
#[tokio::test]
async fn t3_pass_skips_before_1120_utc_and_after_done() {
    use crate::market::history::HistoryConfig;
    use chrono::{TimeZone, Utc};
    let db = std::sync::Arc::new(crate::store::Db::in_memory().unwrap());
    // 手动记一趟"今天"的成功 pass，模拟闸门已关。
    let noon = Utc.with_ymd_and_hms(2026, 9, 24, 12, 0, 0).unwrap();
    db.record_history_pass(&crate::store::HistoryPass {
        started_at: noon.timestamp(),
        region_id: crate::market::REGION_FORGE,
        targets: 1, requested: 1, gated: 0, rows_written: 418,
        absent: 0, failed: 0, seconds: 0.1, decoded_bytes: 0,
        tokens_local: 2, error_remain: None, status: "ok".into(),
    })
    .unwrap();
    // run_t3_pass 在"已记当日"时应直接返回 None（未跑），不碰网络。
    let client = std::sync::Arc::new(crate::esi::EsiClient::new(Default::default()).unwrap());
    let rep = run_t3_pass(&client, &db, &HistoryConfig::default(), &noon).await.unwrap();
    assert!(rep.is_none(), "当日已补则不再跑，返回 None");
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p emd-core collector::tests::t3_pass`
Expected: 编译失败（`run_t3_pass` 未定义）。

- [ ] **Step 3: 实现 run_t3_pass + Collector**

在 `collector.rs` 加（`use` 补齐 `crate::{esi::EsiClient, market, store::{Db, HistoryPass}, market::history::{self, HistoryConfig}}`、`std::sync::Arc`、`std::time::Duration`、`tokio::sync::watch`、`crate::scheduler::{Scheduler, SchedulerConfig}`）：

```rust
/// 计算"最近一趟成功 pass 的 UTC 日历日"，喂给 `history_due`。
fn last_pass_day(db: &Db) -> crate::Result<Option<String>> {
    Ok(db.last_history_pass()?.map(|p| {
        chrono::DateTime::from_timestamp(p.started_at, 0)
            .map(|d| d.date_naive().to_string())
            .unwrap_or_default()
    }))
}

/// 到点才跑的一趟 T3 回填。返回 None = 今天不该跑（未到 11:20 UTC 或已补）。
/// 目标集与幂等都交给已实现的 `market::history`，这里只做"该不该跑 + 取目标"。
pub async fn run_t3_pass(
    client: &EsiClient,
    db: &Db,
    cfg: &HistoryConfig,
    now_utc: &chrono::DateTime<chrono::Utc>,
) -> crate::Result<Option<market::PassReport>> {
    if !cfg.enabled() {
        return Ok(None);
    }
    if !history::history_due(last_pass_day(db)?.as_deref(), now_utc) {
        return Ok(None);
    }
    let now = now_utc.timestamp();
    let targets = db.history_targets(cfg.region_id, cfg.l0_cap, cfg.l1_daily, now)?;
    if targets.is_empty() {
        return Ok(None);
    }
    let rep = history::backfill(client, db, cfg, &targets).await?;
    Ok(Some(rep))
}

/// 采集编排器：持有锁的一侧跑 T1+T3，没抢到锁的一侧调用方应改走"查看者"分支。
pub struct Collector {
    client: Arc<EsiClient>,
    db: Arc<Db>,
    sched_cfg: SchedulerConfig,
    hist_cfg: HistoryConfig,
}

impl Collector {
    pub fn new(client: Arc<EsiClient>, db: Arc<Db>, sched_cfg: SchedulerConfig, hist_cfg: HistoryConfig) -> Self {
        Self { client, db, sched_cfg, hist_cfg }
    }

    /// 常驻：T1 每轮之后串行跑一次 T3（到点才真打）。`shutdown` 置位则在当前轮后退出。
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> crate::Result<u64> {
        let sched = Scheduler::new(self.client.clone(), self.db.clone(), self.sched_cfg.clone());
        let client = self.client.clone();
        let db = self.db.clone();
        let hist = self.hist_cfg.clone();
        sched
            .run_with_after_round(shutdown.clone(), move |_round| {
                let (client, db, hist) = (client.clone(), db.clone(), hist.clone());
                async move {
                    let now = chrono::Utc::now();
                    match run_t3_pass(&client, &db, &hist, &now).await {
                        Ok(Some(rep)) => tracing::info!(
                            "T3 历史回填：{} 目标 / {} 请求 / {} 行 / {:.1}s / {}",
                            rep.targets, rep.requested, rep.rows_written, rep.seconds, rep.status
                        ),
                        Ok(None) => {}
                        Err(e) => tracing::warn!("T3 回填本轮跳过：{e}"),
                    }
                }
            })
            .await
    }
}
```

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p emd-core collector`
Expected: Task 2 的锁测试 + 本任务 `t3_pass` + `HistoryConfig`/`SchedulerConfig` 均 Clone/字段可见；全绿。若 `SchedulerConfig`/`HistoryConfig` 缺 `Clone`，在各自定义处 `#[derive(Clone)]` 补上。

- [ ] **Step 5: Commit（可选）**

```bash
git add crates/emd-core/src/collector.rs && git commit -m "feat(emd-core): Collector orchestrates T1 + T3, viewer mode yields zero requests"
```

---

## Task 5: daemon 接 Collector + history/watch 子命令

**Files:**
- Modify: `crates/emd-daemon/src/main.rs`

- [ ] **Step 1: 用 Collector + 锁替换 run_scheduler**

`Command::Round | Command::Serve` 分支改调新函数。`run_scheduler` 体替换为：

```rust
async fn run_scheduler(client: Arc<EsiClient>, db: Arc<Db>, region: u32, rounds: Option<u64>) -> Result<()> {
    use emd_core::collector::{Collector, InstanceLock};
    use emd_core::market::history::HistoryConfig;
    let data_dir = args_db_dir(); // 取 --db 目录（见下）
    let _lock = match InstanceLock::acquire(&data_dir) {
        Some(g) => g,
        None => {
            println!("另一进程已持有采集锁，本 daemon 不启动采集（查看者模式）。");
            return Ok(());
        }
    };
    let sched_cfg = SchedulerConfig { region_id: region, rounds, ..Default::default() };
    let collector = Collector::new(client.clone(), db.clone(), sched_cfg, HistoryConfig::from_env());
    let (_tx, rx) = tokio::sync::watch::channel(false);
    // Ctrl-C 仍走原优雅退出：这里用 stop_rx 传入。
    let rounds_done = collector.run(rx).await.context("采集循环异常退出")?;
    println!("已跑 {rounds_done} 轮｜令牌余量 {}", client.remaining_tokens());
    Ok(())
}
```

> 保留原 `round`/`serve` 的 Ctrl-C 处理与状态打印：把原 `run_scheduler` 里的 `stop_tx/stop_rx`、UI watcher 一并搬来，`collector.run(stop_rx)` 取代 `sched.run(stop_rx)`；锁句柄 `_lock` 存活整个函数期。`args_db_dir()` 从已解析的 `--db` 目录取（当前 `main` 里是 `args.db`），必要时把目录传进本函数而非内部反查。

- [ ] **Step 2: 编译并跑一轮（离线不联网时只验证类型与接线）**

Run: `cargo build -p emd-daemon`
Expected: 成功。

- [ ] **Step 3: 新增 history 子命令（成本预估 + 可选真跑）**

`enum Command` 加 `History`；`parse_args` 加 `"history" => cmd = Some(Command::History)`；`print_usage` 增一行：

```
  history  跑一次 T3 日线回填（--estimate 只预估成本；--limit N 限定目标数）
```

分支实现：

```rust
Command::History => {
    use emd_core::market::history::{self, HistoryConfig};
    let cfg = HistoryConfig { limit: (args.limit != 20).then_some(args.limit), ..HistoryConfig::from_env() };
    let now = chrono::Utc::now();
    let targets = db.history_targets(cfg.region_id, cfg.l0_cap, cfg.l1_daily, now.timestamp())?;
    let est = history::Estimate::of(targets.len() as u32, db.last_history_pass().and_then(|h| history::PassReport { seconds: h.seconds, requested: h.requested.max(1), ..Default::default() }).rps().into());
    println!("计划目标 {} 个｜请求上界 {}｜本地令牌 ~{}｜日线行上界 ~{}｜预计 {:.0}s",
        est.targets, est.requests, est.tokens_local, est.history_rows, est.seconds_at_measured_rps);
    if std::env::var("EMD_HISTORY_RUN").is_ok() {
        let rep = emd_core::collector::run_t3_pass(&client, &db, &cfg, &now)?.map(|r| r);
        println!("{:?}", rep);
    } else {
        println!("（未真跑；设 EMD_HISTORY_RUN=1 执行）");
    }
}
```

> 若 `Estimate::of` 的 measured_rps 取值让上述过于绕，退化为 `Estimate::of(targets.len() as u32, None)`（用 `COLD_RPS`），并单独 `println` 实测来自 `last_history_pass()`。

- [ ] **Step 4: 编译**

Run: `cargo build -p emd-daemon`
Expected: 成功。

- [ ] **Step 5: 手动冒烟（离线，仅看锁分流与不炸）**

Run: `cargo run -p emd-daemon -- history --db "C:\EVE市场分析\_m3smoke"`
Expected: 打印一行"计划目标 …｜请求上界 …"，不联网、不 panic。（`EMD_HISTORY_CAP=0` 时打印目标数为 0 属正常。）

- [ ] **Step 6: Commit（可选）**

```bash
git add crates/emd-daemon/src/main.rs && git commit -m "feat(daemon): run via Collector + history subcommand"
```

---

## Task 6: app 接 Collector（锁分流）+ get_history/watch 命令 + status 回落

**Files:**
- Modify: `crates/emd-app/src/lib.rs`
- Modify: `crates/emd-app/src/tests.rs`

- [ ] **Step 1: 写失败测试（命令体脱 Tauri 可测）**

在 `tests.rs` 加（沿用现有构造 `AppState`/`Db::in_memory` 的方式）：

```rust
#[tokio::test]
async fn get_history_returns_series_for_a_type() {
    let state = test_state().await; // 复用 tests.rs 现有助手；无则用 Db::in_memory + AppState::for_test
    state.db_lock().watch_add(34, Some("tri")).unwrap();
    // 直接塞两行日线（write_history 同步）
    // ... 断言 state.history(34, None) 返回按 date 升序的 Vec<HistoryBar>
}
```

> 若 `tests.rs` 尚无 `AppState` 构造助手，本步骤先加一个 `fn seeded_state(db: Db) -> AppState`（仅填 `db`/`client`/`latest`/`shutdown`），使命令体测试不依赖 Tauri 运行时。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p emd-app --no-run`（编译失败即"红"）。

- [ ] **Step 3: 加命令体到 impl AppState**

```rust
impl AppState {
    async fn history(&self, type_id: u32, region_id: Option<u32>) -> Result<Vec<HistoryBar>, String> {
        let region = region_id.unwrap_or(emd_core::market::REGION_FORGE);
        read(self.db.clone(), move |db| db.history_series(region, type_id, None).map_err(err)).await
    }
    async fn watch_list(&self) -> Result<Vec<(u32, Option<String>)>, String> {
        read(self.db.clone(), |db| db.watch_list().map_err(err)).await
    }
    async fn watch_add(&self, type_id: u32) -> Result<(), String> {
        read(self.db.clone(), move |db| db.watch_add(type_id, None).map_err(err)).await
    }
    async fn watch_remove(&self, type_id: u32) -> Result<bool, String> {
        read(self.db.clone(), move |db| db.watch_remove(type_id).map_err(err)).await
    }
}
```

`use emd_core::store::HistoryBar;`。加薄 `#[tauri::command]` 包装 `get_history`/`get_watchlist`/`add_watch`/`remove_watch`，并注册进 `invoke_handler![...]`。

- [ ] **Step 4: spawn_collector 改锁分流 + 用 Collector**

```rust
fn spawn_collector(client: Arc<EsiClient>, path: PathBuf, latest: Arc<Mutex<Option<(RoundState, Instant)>>>, stop_rx: watch::Receiver<bool>) {
    std::thread::Builder::new().name("emd-collector".into()).spawn(move || {
        let data_dir = path.parent().map(|p| p.to_path_buf()).unwrap_or_default();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(err).unwrap();
        rt.block_on(async move {
            let db = Arc::new(Db::open(&path).map_err(err).unwrap());
            // 冷启动补树 / 站点解名（保留原逻辑，串行在首轮前）……
            let lock = emd_core::collector::InstanceLock::acquire(&data_dir);
            if lock.is_none() {
                tracing::info!("采集锁被占，本窗口只读（查看者）。");
                return; // 状态展示走 §Step5 的库回落
            }
            let collector = emd_core::collector::Collector::new(
                client.clone(), db.clone(),
                SchedulerConfig::default(),
                emd_core::market::history::HistoryConfig::from_env(),
            );
            let mut rx = { let s = Scheduler::new(client.clone(), db.clone(), SchedulerConfig::default()); s.subscribe() };
            // 状态转发沿用原 latest 逻辑（把上面 rx 换成 collector 暴露的订阅或在 Collector 内提供 subscribe）
            if let Err(e) = collector.run(stop_rx).await { tracing::error!("采集循环退出：{e}"); }
        });
    }).ok();
}
```

> 实现要点：`Collector` 需暴露 `subscribe()`（内部转发 `Scheduler::subscribe`）供 `latest` watcher 取 `RoundState`。在 Task 4 的 `Collector` 加 `pub fn subscribe(&self) -> watch::Receiver<RoundState>`，构造时先建 `Scheduler` 并存 `status_rx`。若嫌早建 Scheduler，可让 `run()` 接收 `latest` 转发句柄。**保持原有"补树/解名在首轮前串行"的注释与顺序不变**（A.9：共用一条 Connection 时事务卷绕问题）。

- [ ] **Step 5: status 查看者回落**

在 `AppState::status()` 里：`self.latest.lock()` 取到 `None` 时，不再填零，改从库读：

```rust
None => {
    // 采集由别的进程持有：读 round_log 最近一轮 + meta 拼非零 StatusOut。
    let (round, last_seconds, orders, rows_written, lm, next_in_ms) =
        read(self.db.clone(), |db| {
            let r = db.last_round().map_err(err)?;
            let lm = db.get_meta("last_snapshot_lm").map_err(err)?;
            Ok(match r {
                Some(r) => (db.round_count().unwrap_or(0), r.seconds, r.orders, r.rows_written, lm.clone(),
                    (MIN_INTERVAL.as_secs() as i64 - (chrono::Utc::now().timestamp() - r.started_at)).max(0) as u64 * 1000),
                None => (0, 0.0, 0, 0, lm.clone(), 0u64),
            })
        }).await?;
    // 用上面结果继续构造 StatusOut（tokens 恒报本地 client 值；hubs/jita_rows/tree 仍查库）。
}
```

- [ ] **Step 6: 编译 + 跑 app 测试**

Run: `cargo test -p emd-app`
Expected: 新命令体测试 PASS；既有 app 测试不回归。

- [ ] **Step 7: Commit（可选）**

```bash
git add crates/emd-app/src/lib.rs crates/emd-app/src/tests.rs && git commit -m "feat(app): collector lock + get_history/watch commands + viewer status fallback"
```

---

## Task 7: 前端依赖与 IPC（echarts + api + fixtures）

**Files:**
- Modify: `web/package.json`
- Modify: `web/src/types.ts`
- Modify: `web/src/api.ts`

- [ ] **Step 1: 加 echarts 依赖**

`web/package.json` 的 `dependencies` 加 `"echarts": "^5.5.1"`。
Run: `npm --prefix web install`
Expected: `echarts` 落地，无错误。

- [ ] **Step 2: 加类型**

`web/src/types.ts` 加：

```ts
export interface HistoryBar {
  date: string;
  average: number | null;
  highest: number | null;
  lowest: number | null;
  volume: number;
  order_count: number;
}
```

- [ ] **Step 3: 加 api + fixture**

`web/src/api.ts` 的 `api` 对象加：

```ts
async history(typeId: number, regionId?: number): Promise<HistoryBar[]> {
  if (!inTauri) return fixHistory(typeId);
  return call<HistoryBar[]>("get_history", { typeId, regionId });
},
async watchlist(): Promise<Array<[number, string | null]>> {
  if (!inTauri) return [[34, "Tritanium"]];
  return call("get_watchlist");
},
async addWatch(typeId: number): Promise<void> {
  if (!inTauri) return;
  await call("add_watch", { typeId });
},
async removeWatch(typeId: number): Promise<void> {
  if (!inTauri) return;
  await call("remove_watch", { typeId });
},
```

加 `fixHistory`（形状对齐后端、数值取 A.7/实测口径，近 60 天递增随机游走，`average/highest/lowest` 合理、`volume` 正）：

```ts
function fixHistory(typeId: number): HistoryBar[] {
  const base = typeId === 34 ? 3.8 : typeId === 35 ? 4.3 : 100;
  const out: HistoryBar[] = [];
  const today = new Date();
  for (let i = 59; i >= 0; i--) {
    const d = new Date(today); d.setUTCDate(d.getUTCDate() - i);
    const drift = base * (1 + Math.sin(i / 7) * 0.05);
    const avg = Number(drift.toFixed(3));
    out.push({ date: d.toISOString().slice(0, 10), average: avg,
      highest: Number((avg * 1.03).toFixed(3)), lowest: Number((avg * 0.97).toFixed(3)),
      volume: Math.round(1e9 + (i * 37 % 50) * 1e7), order_count: 1000 + (i % 9) * 120 });
  }
  return out;
}
```

- [ ] **Step 4: 类型检查**

Run: `npm --prefix web run typecheck`
Expected: 通过。

- [ ] **Step 5: Commit（可选）**

```bash
git add web/package.json web/src/types.ts web/src/api.ts && git commit -m "feat(web): echarts dep + history/watch IPC with fixtures"
```

---

## Task 8: HistoryChart 组件 + OrderBook 历史页签

**Files:**
- Create: `web/src/components/HistoryChart.tsx`
- Modify: `web/src/components/OrderBook.tsx`
- Modify: `web/src/styles.css`（页签样式，沿用现有 CSS 变量暗色主题）

- [ ] **Step 1: 写 HistoryChart（按需注册 echarts）**

```tsx
import { useEffect, useRef, useState } from "react";
import * as echarts from "echarts/core";
import { CandlestickChart, BarChart } from "echarts/charts";
import { GridComponent, TooltipComponent, DataZoomComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { api } from "../api";
import type { HistoryBar } from "../types";

echarts.use([CandlestickChart, BarChart, GridComponent, TooltipComponent, DataZoomComponent, CanvasRenderer]);

const RANGES = [{ k: "1W", d: 7 }, { k: "1M", d: 30 }, { k: "3M", d: 90 }, { k: "1Y", d: 418 }] as const;

export default function HistoryChart({ typeId }: { typeId: number }) {
  const box = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);
  const [all, setAll] = useState<HistoryBar[]>([]);
  const [range, setRange] = useState<(typeof RANGES)[number]>({ k: "1M", d: 30 });

  useEffect(() => { void api.history(typeId).then(setAll); }, [typeId]);

  useEffect(() => {
    if (!box.current) return;
    chart.current = chart.current ?? echarts.init(box.current);
    const start = new Date(); start.setUTCDate(start.getUTCDate() - range.d);
    const from = start.toISOString().slice(0, 10);
    const rows = all.filter((r) => r.date >= from);
    const cats = rows.map((r) => r.date);
    // ECharts 蜡烛数据序：[open, close, lowest, highest]；open 用前一日 average。
    const kline = rows.map((r, i) => [rows[i - 1]?.average ?? r.average ?? 0, r.average ?? 0, r.lowest ?? 0, r.highset ?? r.highest ?? 0]);
    const vols = rows.map((r) => r.volume);
    chart.current.setOption({ // 增量更新，不重建实例（§6 动画 300ms）
      animationDuration: 300, tooltip: { trigger: "axis" },
      grid: [{ left: 55, right: 20, top: 20, height: "60%" }, { left: 55, right: 20, top: "75%", height: "18%" }],
      xAxis: [{ type: "category", data: cats }, { type: "category", gridIndex: 1, data: cats, axisLabel: { show: false } }],
      yAxis: [{ scale: true }, { gridIndex: 1, axisLabel: { show: false } }],
      dataZoom: [{ type: "inside", xAxisIndex: [0, 1] }, { type: "slider", xAxisIndex: [0, 1], bottom: 0 }],
      series: [{ type: "candlestick", data: kline }, { type: "bar", xAxisIndex: 1, yAxisIndex: 1, data: vols }],
    });
  }, [all, range]);

  useEffect(() => () => { chart.current?.dispose(); chart.current = null; }, []);

  return (
    <div>
      <div className="hist-tabs">
        <button disabled title="分钟线待后续（M3 未含 ticker_intraday）">1D</button>
        {RANGES.map((r) => (
          <button key={r.k} className={range.k === r.k ? "on" : ""} onClick={() => setRange(r)}>{r.k}</button>
        ))}
        <span className="cov">本地已积累 {all.length}/418 天</span>
      </div>
      <div ref={box} style={{ width: "100%", height: 360 }} />
    </div>
  );
}
```

> 注：上面 `r.highset ?? r.highest` 是笔误防护位——实现时写 `r.highest ?? 0`。ECharts 蜡烛数据序 `[open, close, lowest, highest]`，`open` 取前一日 `average`、`close` 取当日 `average`（对齐 §6 主图定义）。

- [ ] **Step 2: OrderBook 加页签**

`OrderBook.tsx` 现有单物品详情外层包一个轻量页签切换：`买卖单簿` / `历史`（默认单簿）。选"历史"时渲染 `<HistoryChart typeId={selectedTypeId} />`。跨站对比处保留"仅当前快照，非历史"标注（spec §6/§3.3 固有限制）。

- [ ] **Step 3: 页签样式（暗色主题，沿用 CSS 变量）**

`styles.css` 加 `.hist-tabs button`（含 `:disabled` 置灰、`.on` 高亮），复用现有变量，不新增硬编码色（design-debt 规避）。

- [ ] **Step 4: 类型检查 + 构建**

Run: `npm --prefix web run typecheck; npm --prefix web run build`
Expected: 通过，产物无 TS 报错。

- [ ] **Step 5: 肉眼验收（无壳 fixtures）**

Run: `npm --prefix web run dev`
Expected: 打开单物品详情→"历史"页签，蜡烛图 + 成交量渲染、dataZoom 拖拽顺、1W/1M/3M/1Y 切换增量更新不闪重建、1D 置灰。

- [ ] **Step 6: Commit（可选）**

```bash
git add web/src/components/HistoryChart.tsx web/src/components/OrderBook.tsx web/src/styles.css && git commit -m "feat(web): ECharts candlestick history tab (1D disabled)"
```

---

## Task 9: M3 验收（真机，联网一次性）

- [ ] **Step 1: 全量测试**

Run: `cargo test -p emd-core -p emd-daemon -p emd-app`
Expected: 全绿（含历史幂等、锁、after_round、t3_pass、命令体测试）。

- [ ] **Step 2: 实跑 T3 小样本（设自选若干 + 限额）**

Run（Git Bash / PowerShell 均可，注意 PATH）：
```
cargo run -p emd-daemon -- history --db "C:\EVE市场分析\_m3smoke"
# 看预估后真跑：EMD_HISTORY_RUN=1 且 EMD_CONTACT_EMAIL 已配
```
Expected: `market_history` 落对应类型日线、最新日期到"昨天"（A.4：当日次日出现）；`history_log` 记一行台账（targets/requested/rows/404 数/秒数）。

- [ ] **Step 3: 双进程锁验证**

Run: 一个终端 `cargo run -p emd-daemon -- serve --db <同目录>`；另一终端再启一个 `serve`。
Expected: 第二个打印"另一进程已持有采集锁…（查看者模式）"并退出，不产生网络请求；第一个正常按 360s 节拍跑。

- [ ] **Step 4: 冷启动追赶不重复**

Run: 已有当日 pass 时再启 `serve` 或 `history`。
Expected: `run_t3_pass` 返回 None，history 请求数为 0（台账不新增当日重复行）。

- [ ] **Step 5: 验收对照 spec §8**

逐条勾：日线到昨天 ✓ / 蜡烛图+dataZoom+区间不重建 ✓ / 同开只有一份请求 ✓ / 冷启动不重复 ✓ / 按类型显示真实积累天数 ✓。

---

## Self-Review（计划对 spec 覆盖自查）

- **§0 现状校正**：计划不重做 emd-core 逻辑层，只做接线（Task 1）——已对应。
- **§2 单编排器 + 锁**：Task 2（锁）、Task 4（Collector）、Task 3（轮间钩子）覆盖；采集者/查看者分流在 Task 4/5/6。
- **§3 迁移 v4**：已存在，Task 1 编译验证，无需新表；`history_scope`/`history_log` 被 T3 使用。
- **§4 T3 幂等/日一次/追赶**：Task 4 `run_t3_pass`（`history_due` + `sync_due`）；daemon `history` 与 app 常驻都走它。附录 D.4 已由既有代码结掉，计划不重复引入。
- **§5 命令/接入/status 回落**：Task 5（daemon）+ Task 6（app + 回落）。
- **§6 前端/1D 置灰/积累天数/快照标注**：Task 7 + Task 8。
- **§7 测试**：锁、after_round、t3_pass、命令体、双进程零请求，均在 Task 2/3/4/6/9。
- **类型一致性**：`HistoryBar{date,average,highest,lowest,volume,order_count}`（Rust 与 TS 对齐）；`run_t3_pass`/`Collector::new`/`run_with_after_round` 签名跨任务一致；`HistoryConfig`/`SchedulerConfig` 需 `Clone`（Task 4 步骤 4 已列补 derive）。
- **已知延后**：分钟线、L2 全量——计划未引入，符合 spec §1.2。
