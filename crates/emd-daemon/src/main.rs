//! 无头采集进程。Tauri 壳还没上之前，它就是这个项目的全部可运行形态；
//! 上了壳之后它继续作为"关窗口也在攒数据"的常驻后端存在。

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use emd_core::catalog;
use emd_core::collector::InstanceLock;
use emd_core::compliance;
use emd_core::config::EsiConfig;
use emd_core::esi::EsiClient;
use emd_core::market::history::{self, HistoryConfig};
use emd_core::market::{self, STATION_JITA};
use emd_core::scheduler::{Scheduler, SchedulerConfig, Stage, XRegionConfig};
use emd_core::store::{now_unix, Db, PriceRow};
use emd_core::tree;

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Round,
    Serve,
    Prices,
    Verify,
    Probe,
    Stats,
    Hubs,
    Jita,
    Names,
    Tree,
    Search,
    List,
    History,
    Flip,
    XRegion,
    Opps,
}

#[derive(Debug)]
struct Args {
    command: Command,
    db: PathBuf,
    region: u32,
    ua: String,
    rounds: Option<u64>,
    limit: Option<u32>,
    word: String,
    group: u32,
    type_id: Option<u32>,
    /// history 专用：只列计划，不发任何请求。
    dry: bool,
    /// history 专用：跑限流组归属实验。
    do_probe: bool,
    /// flip 专用：临时覆盖技能等级做口径对比（不持久化）。
    accounting: Option<u8>,
    broker_relations: Option<u8>,
    /// opps 专用：--update 打开每轮生命周期结算后再展示。
    do_update: bool,
    /// opps 专用：按状态过滤（new/notified/expired/invalidated）。
    state: Option<String>,
}

fn parse_args() -> Result<Args> {
    parse_from(std::env::args().skip(1))
}

/// 参数解析单独成函数：这样上面那些开关的组合能在普通测试里跑，
/// 不用伪造进程环境变量。
fn parse_from(it: impl Iterator<Item = String>) -> Result<Args> {
    let mut it = it.peekable();
    let mut cmd = None;
    let mut db = dirs_data().context("找不到 %LOCALAPPDATA%，请用 --db 指定")?;
    let mut region = market::REGION_FORGE;
    // 空串 = 交给 EMD_CONTACT_EMAIL / 默认值决定，见下面构造 cfg 处。
    let mut ua = String::new();
    let mut rounds = None;
    let mut limit: Option<u32> = None;
    let mut word = String::new();
    let mut group = 0u32;
    let mut type_id = None;
    let mut dry = false;
    let mut do_probe = false;
    let mut accounting: Option<u8> = None;
    let mut broker_relations: Option<u8> = None;
    let mut do_update = false;
    let mut state: Option<String> = None;

    while let Some(a) = it.next() {
        match a.as_str() {
            "round" => cmd = Some(Command::Round),
            "serve" => cmd = Some(Command::Serve),
            "prices" => cmd = Some(Command::Prices),
            "verify" => cmd = Some(Command::Verify),
            "probe" => cmd = Some(Command::Probe),
            "stats" => cmd = Some(Command::Stats),
            "hubs" => cmd = Some(Command::Hubs),
            "jita" => cmd = Some(Command::Jita),
            "names" => cmd = Some(Command::Names),
            "tree" => cmd = Some(Command::Tree),
            "search" => cmd = Some(Command::Search),
            "list" => cmd = Some(Command::List),
            "history" => cmd = Some(Command::History),
            "flip" => cmd = Some(Command::Flip),
            "xregion" => cmd = Some(Command::XRegion),
            "opps" => cmd = Some(Command::Opps),
            "--db" => db = PathBuf::from(it.next().context("--db 缺参数")?),
            "--word" => word = it.next().context("--word 缺参数")?,
            "--type" => {
                type_id = Some(
                    it.next()
                        .and_then(|s| s.parse().ok())
                        .context("--type 需要数字")?,
                )
            }
            "--group" => {
                group = it
                    .next()
                    .and_then(|s| s.parse().ok())
                    .context("--group 需要数字")?
            }
            "--region" => {
                region = it
                    .next()
                    .and_then(|s| s.parse().ok())
                    .context("--region 需要数字")?
            }
            "--ua" => ua = it.next().context("--ua 缺参数")?,
            "--rounds" => {
                rounds = Some(
                    it.next()
                        .and_then(|s| s.parse().ok())
                        .context("--rounds 需要数字")?,
                )
            }
            "--limit" | "--top" => {
                limit = Some(
                    it.next()
                        .and_then(|s| s.parse().ok())
                        .context("--limit 需要数字")?,
                )
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "--dry" => dry = true,
            "--probe" => do_probe = true,
            "--accounting" => {
                accounting = Some(
                    it.next()
                        .and_then(|s| s.parse().ok())
                        .context("--accounting 需要 0-5 的数字")?,
                )
            }
            "--broker-relations" => {
                broker_relations = Some(
                    it.next()
                        .and_then(|s| s.parse().ok())
                        .context("--broker-relations 需要 0-5 的数字")?,
                )
            }
            "--update" => do_update = true,
            "--state" => {
                let v = it.next().context("--state 缺参数")?;
                // 未知状态当场拒绝，不静默全表——否则用户以为过滤生效其实没有。
                emd_core::market::lifecycle::OppState::parse(&v)
                    .with_context(|| format!("--state 需要 new/notified/expired/invalidated，收到 {v}"))?;
                state = Some(v);
            }
            other => anyhow::bail!("未知参数：{other}"),
        }
    }

    Ok(Args {
        command: cmd.context("缺少子命令（round|serve|prices|verify|probe|stats|hubs|jita|names|tree|search|list|history|flip|xregion|opps）")?,
        db,
        region,
        ua,
        rounds,
        limit,
        word,
        group,
        type_id,
        dry,
        do_probe,
        accounting,
        broker_relations,
        do_update,
        state,
    })
}

fn dirs_data() -> Option<PathBuf> {
    let local = std::env::var_os("LOCALAPPDATA")?;
    Some(PathBuf::from(local).join("EveMarketDesk"))
}

fn print_usage() {
    println!(
        "用法：emd <子命令> [--db DIR] [--region N] [--ua UA] [--rounds N] [--limit N]

  round    跑一轮 T1（经调度器，与常驻模式同一条代码路径）
  serve    常驻：6 分钟节拍循环，Ctrl-C 在当前轮结束后优雅退出
  prices   拉取全服基准价 markets/prices 并落库
  verify   跑 M0.5 合规冒烟闸门（含一次 409 页全量）
  probe    冷启动轻量自检（3 页，不跑全量）
  stats    本地库现状、上一轮台账与令牌余量
  hubs     打印当前枢纽池（含站点名）
  jita     打印吉他单站单簿前 --limit 行
  flip     倒卖扫描（读本地快照）：Top N 机会 + 丢弃原因分布；费率与技能取面板参数
  xregion  手工触发一趟 T1.5 跨区补拉（三枢纽 × Top 候选；serve 里隔轮自动跑）
  opps     打印机会生命周期表；--update 先跑一轮结算再展示；--state 过滤
  names    解析库里未命名的 NPC 站（POST /v1/universe/names）
  tree     一次性建分类树（约 1 000 次请求，实测 1-2 分钟）
  search   按名称搜类型（--word，走 POST /v1/universe/ids，只取 inventory_types）
  list     打印某分类组下的类型 + 吉他当前价（--group N --limit N）
  history  跑一趟 T3 历史日线（§3.3 的 L0/L1）。--dry 只看计划，--probe 量限流组归属

  --rounds N   serve 跑满 N 轮后退出；round 隐含 1
  --limit N    jita/names/tree/list/flip 输出行数上限（默认 20）；history 下表示目标数上限
  --top N      --limit 的别名（flip 的 Top N，缺省 20）
  --accounting N        flip 临时覆盖 Accounting 等级（0-5，仅本次运行，不写库）
  --broker-relations N  flip 临时覆盖 Broker Relations 等级（0-5，仅本次运行，不写库）
  --update     opps 展示前先跑一轮 update_round（把当前快照结算进 opportunities）
  --state S    opps 按状态过滤：new | notified | expired | invalidated
  --type N     history 只取这一个类型
  --dry        history 只打印取数计划与成本估算，不发请求
  --probe      history 的交叉实验：证明 history 不占 market-order 令牌组

  环境变量：EMD_CONTACT_EMAIL（UA 里的联系邮箱）、EMD_HISTORY_CAP（0 = 关掉每日 T3）"
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,reqwest=warn,hyper=warn".into()),
        )
        .init();

    let args = parse_args()?;
    // --db 给的是目录（数据目录可迁移，见方案 §7），库文件固定在其下。
    std::fs::create_dir_all(&args.db)
        .with_context(|| format!("创建数据目录失败 {:?}", args.db))?;
    let db_path = args.db.join("emd.sqlite3");
    let db = Arc::new(Db::open(&db_path).with_context(|| format!("打开数据库失败 {db_path:?}"))?);

    let base = EsiConfig::from_env();
    let cfg = EsiConfig {
        user_agent: if args.ua.is_empty() {
            base.user_agent.clone()
        } else {
            args.ua.clone()
        },
        ..base
    };
    let client = Arc::new(EsiClient::new(cfg).context("构建 ESI 客户端失败")?);

    match args.command {
        Command::Round | Command::Serve => {
            run_scheduler(client, db, &args.db, args.region, args.rounds.or(match args.command {
                Command::Round => Some(1),
                _ => None,
            }))
            .await?
        }
        Command::Prices => run_prices(&client, &db).await?,
        Command::Verify => run_gate(&client, &db, true).await?,
        Command::Probe => run_gate(&client, &db, false).await?,
        Command::Stats => run_stats(&client, &db, &db_path)?,
        Command::Hubs => run_hubs(&db)?,
        Command::Flip => run_flip(&db, args.limit.unwrap_or(20), args.accounting, args.broker_relations)?,
        Command::XRegion => run_xregion(&client, &db).await?,
        Command::Opps => run_opps(&db, args.do_update, args.state.as_deref())?,
        Command::Jita => run_jita(&db, args.limit.unwrap_or(20))?,
        Command::Names => {
            let n = catalog::resolve_missing(&client, &db, args.limit.unwrap_or(400)).await?;
            println!("解析出 {n} 个站点名｜令牌余量 {}", client.remaining_tokens());
        }
        Command::Tree => {
            let s = tree::build_tree(&client, &db).await?;
            println!(
                "分类树：{} 组（{} 已发布，{} 拉取失败）｜{} 个类型，解名 {}｜分类 {}｜耗时 {}",
                s.groups,
                s.published_groups,
                s.failed_groups,
                s.types,
                s.names_resolved,
                s.categories,
                human(s.elapsed)
            );
            let (g, t, c) = db.tree_counts()?;
            println!("库内现有：{g} 组 / {t} 个已命名类型 / {c} 个分类");
        }
        Command::Search => {
            anyhow::ensure!(!args.word.trim().is_empty(), "search 需要 --word <文本>");
            let hits = catalog::search(&client, &[args.word.as_str()]).await?;
            println!("「{}」命中 {} 个物品类型：", args.word, hits.inventory_types.len());
            for n in &hits.inventory_types {
                let name = db.type_name(n.id as u32)?;
                println!(
                    "  {:<8} {:<32}（本地树：{}）",
                    n.id,
                    n.name,
                    name.unwrap_or_else(|| "未建组".into())
                );
            }
            if !hits.systems.is_empty() {
                println!(
                    "另有 {} 个星系同名，已忽略：{}",
                    hits.systems.len(),
                    hits.systems
                        .iter()
                        .map(|s| s.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            let like = db.find_types_like(&args.word, 10)?;
            if !like.is_empty() {
                println!("本地前缀匹配 {} 个：{}", like.len(), like.iter().map(|(id, n)| format!("{id} {n}")).collect::<Vec<_>>().join(", "));
            }
        }
        Command::List => {
            anyhow::ensure!(args.group != 0, "list 需要 --group <id>");
            let rows = db.group_listing(args.group, STATION_JITA)?;
            println!(
                "组 {} 共 {} 个类型，显示前 {}：",
                args.group,
                rows.len(),
                args.limit.unwrap_or(20)
            );
            println!("{:<9} {:<38} {:>12} {:>12} {:>6}", "type_id", "名称", "吉他买一", "吉他卖一", "档数");
            for r in rows.iter().take(args.limit.unwrap_or(20) as usize) {
                println!(
                    "{:<9} {:<38} {:>12} {:>12} {:>6}",
                    r.type_id,
                    r.name.chars().take(36).collect::<String>(),
                    r.best_bid.map(fmt_price).unwrap_or_else(|| "-".into()),
                    r.best_ask.map(fmt_price).unwrap_or_else(|| "-".into()),
                    format!("{}/{}", r.bid_levels, r.ask_levels),
                );
            }
        }
        Command::History => run_history(&client, &db, &args).await?,
    }
    Ok(())
}

/// T3 历史日线。三种模式：`--probe`（限流组归属实验）、`--dry`（只看计划）、默认（跑一趟）。
///
/// 默认模式刻意复用 `Scheduler::run_history`：壳里每天 11:20 UTC 跑的就是它，
/// CLI 另写一遍选池与记账逻辑迟早会和真跑的数对不上（M1 的 `round`/`serve` 就栽过）。
async fn run_history(client: &Arc<EsiClient>, db: &Arc<Db>, args: &Args) -> Result<()> {
    let mut h = HistoryConfig::from_env();
    h.region_id = args.region;
    h.limit = args.limit;

    // `--dry` 是纯本地路径（选池 + 闸门 + 台账都是库内查询），统一提到抢锁之前：
    // 锁被占时也能看计划、看闸门状态。
    if args.dry && !args.do_probe {
        let now = chrono::Utc::now();
        if let Some(t) = args.type_id {
            let key = history::path(args.region, t);
            let due = db.sync_due(&key, now.timestamp())?;
            println!(
                "type {t} @ region {}：{}（404 会记 7 天复核，200 会记到上游 Expires）",
                args.region,
                if due { "闸门已开，将发 1 个请求" } else { "闸门未开，本次 0 请求" }
            );
            return Ok(());
        }
        let targets = db.history_targets(args.region, h.l0_cap, h.l1_daily, now.timestamp())?;
        let n = match h.limit {
            Some(k) => targets.len().min(k as usize),
            None => targets.len(),
        };
        let l0 = targets.iter().filter(|t| t.tier == history::TIER_L0).count();
        let rps: Option<f64> = db.last_history_pass()?.and_then(|p| {
            if p.seconds > 0.0 {
                Some(p.requested as f64 / p.seconds)
            } else {
                None
            }
        });
        let est = history::Estimate::of(n as u32, rps);
        println!(
            "取数计划（region {}，{}）：{} 个目标 = L0 {} + L1 {}，闸门全开时发 {} 个请求",
            args.region,
            if h.enabled() { "启用" } else { "已关掉：EMD_HISTORY_CAP=0" },
            n,
            l0,
            n - l0,
            est.requests
        );
        println!(
            "成本：本地令牌 {}（实测不占 market-order 组）｜日线行数上限 {}｜墙钟约 {:.0} s（吞吐 {}）",
            est.tokens_local,
            est.history_rows,
            est.seconds_at_measured_rps,
            rps.map(|r| format!("{r:.2} 请求/s（上次实测）"))
                .unwrap_or_else(|| format!("{:.1} 请求/s（冷估）", history::COLD_RPS))
        );
        println!(
            "今日 UTC 日已跑过：{}｜上一趟状态：{}",
            db.last_history_request_day()?.unwrap_or_else(|| "否".into()),
            db.last_history_pass()?
                .map(|p| p.status)
                .unwrap_or_else(|| "无".into())
        );
        let mut head = Vec::new();
        for t in targets.iter().take(8) {
            let name = db
                .type_name(t.type_id)?
                .unwrap_or_else(|| format!("#{}", t.type_id));
            head.push(format!("{name}({})", t.type_id));
        }
        println!("前 8 个目标：{}", head.join("、"));
        return Ok(());
    }

    // 真发请求之前的第一道闸：T3 是按类型逐个发的批量请求，若趁 `serve`/壳在采集时
    // 插进来，两个进程各自看不到对方的本地桶，撞的是 ESI 服务端的共享配额 → 429
    // 会算在采集者头上。抢不到锁就明确拒干，而不是硬打。
    let _lock = InstanceLock::acquire(&args.db)
        .context("采集锁被占用：另一进程正在采集，先等它空闲或停掉 serve 再补历史")?;

    if args.do_probe {
        let sample: Vec<u32> = args
            .type_id
            .map(|t| vec![t])
            .unwrap_or_else(|| (34u32..42).collect());
        let rep = history::probe(client, &sample, args.region)
            .await
            .context("限流组归属实验失败")?;
        print_probe(&rep);
        return Ok(());
    }

    if let Some(t) = args.type_id {
        let now = now_unix();
        let t0 = std::time::Instant::now();
        let (f, _) = history::fetch_one(client, db, args.region, t, now).await?;
        println!(
            "type {t} @ region {}：{:?}，耗时 {:.2}s｜本地令牌余量 {}",
            args.region,
            f,
            t0.elapsed().as_secs_f64(),
            client.remaining_tokens()
        );
        if let Some(c) = db.history_coverage(args.region, t)? {
            println!(
                "本地已积累 {} 天（{} → {}），最近 3 天：{}",
                c.days,
                c.first.clone().unwrap_or_default(),
                c.last.clone().unwrap_or_default(),
                db.history_series(args.region, t, None)?
                    .into_iter()
                    .rev()
                    .take(3)
                    .map(|b| format!("{} avg {}", b.date, b.average.map(|v| v.to_string()).unwrap_or("-".into())))
                    .collect::<Vec<_>>()
                    .join(" | ")
            );
        }
        return Ok(());
    }

    let now = chrono::Utc::now();
    let targets = db.history_targets(args.region, h.l0_cap, h.l1_daily, now.timestamp())?;
    if targets.is_empty() {
        println!("目标清单为空（自选未建、流动池未入或 EMD_HISTORY_CAP=0）—— 不发请求。");
        return Ok(());
    }

    let sched = Scheduler::new(
        client.clone(),
        db.clone(),
        SchedulerConfig {
            region_id: args.region,
            history: h,
            ..Default::default()
        },
    );
    let rep = sched.run_history().await.context("历史取数失败")?;
    print_pass(&rep, db, args.region)?;
    Ok(())
}

fn print_pass(rep: &history::PassReport, db: &Arc<Db>, region: u32) -> Result<()> {
    println!("\nT3 历史日线（region {region}） {:.1} s", rep.seconds);
    println!("{}", "-".repeat(78));
    println!(
        "目标 {}｜发请求 {}｜闸门拦下 {}｜内存命中 {}｜未处理 {}",
        rep.targets, rep.requested, rep.gated, rep.served_from_cache, rep.skipped
    );
    println!(
        "落库 {} 行日线｜404 无历史 {}｜失败 {}｜解码 {:.1} MB",
        rep.rows_written, rep.absent, rep.failed, rep.decoded_bytes as f64 / 1_048_576.0
    );
    println!(
        "吞吐 {:.2} 请求/s｜摊薄 {:.0} ms/请求｜本地令牌 -{}（服务端实测不计这笔）",
        rep.rps(),
        rep.per_request_ms(),
        rep.tokens_local
    );
    println!(
        "错误限额最低水位：{}｜状态 {}",
        rep.error_remain_min
            .map(|v| format!("{v}/100"))
            .unwrap_or_else(|| "未回报".into()),
        rep.status
    );
    let (rows, types, regions) = db.history_totals()?;
    println!(
        "{}\n本地库现有 {} 行日线 / {types} 个类型 / {regions} 个星域｜最近一趟台账：{}",
        "-".repeat(78),
        rows,
        db.last_history_request_day()?
            .unwrap_or_else(|| "无".into())
    );
    if let Some(c) = db.history_coverage(region, 34)? {
        println!(
            "抽样 Tritanium(34)：已积累 {} 天（{} → {}）",
            c.days,
            c.first.unwrap_or_default(),
            c.last.unwrap_or_default()
        );
    }
    Ok(())
}

fn print_probe(rep: &history::ProbeReport) {
    println!("\n限流组归属实验（附录 D 第 4 条） {:.1} s", rep.seconds);
    println!("{}", "-".repeat(78));
    println!(
        "orders 组={}｜限额={}｜Remaining {} → {}（差 {}）",
        rep.orders_group.as_deref().unwrap_or("?"),
        rep.orders_limit.as_deref().unwrap_or("?"),
        rep.remaining_before.map(|v| v.to_string()).unwrap_or("?".into()),
        rep.remaining_after.map(|v| v.to_string()).unwrap_or("?".into()),
        rep.orders_delta().map(|v| v.to_string()).unwrap_or("?".into()),
    );
    println!(
        "history {} 次：200 {}／404 {}｜X-Ratelimit-Group 头：{}｜平均 {} 行、{} B/请求",
        rep.history_samples,
        rep.history_ok,
        rep.history_404,
        rep.history_group_header
            .as_deref()
            .unwrap_or("（响应里根本没有这个头）"),
        rep.rows_per_request,
        rep.bytes_per_request
    );
    println!(
        "错误限额 X-Esi-Error-Limit-Remain：{} → {}",
        rep.error_remain_before.map(|v| v.to_string()).unwrap_or("?".into()),
        rep.error_remain_after.map(|v| v.to_string()).unwrap_or("?".into()),
    );
    println!("结论：{}", rep.verdict);
}

/// `round` 与 `serve` 共用一条路径：以前跑 `round` 建的库会和 `serve` 不一致
/// （少枢纽池与站点登记），那样 M1 的验收就测不到真东西。
///
/// 开工前先抢数据目录的采集锁：Tauri 壳可能已经把同一份库当采集目标在跑，
/// 两个采集者同打 ESI 等于令牌翻倍 + `station_orders` 整表替换互相打架。
/// 抢不到就安静退出，让持锁的那个进程继续采（方案 §7 常驻定位）。
async fn run_scheduler(
    client: Arc<EsiClient>,
    db: Arc<Db>,
    data_dir: &std::path::Path,
    region: u32,
    rounds: Option<u64>,
) -> Result<()> {
    let _lock = match InstanceLock::acquire(data_dir) {
        Some(g) => g,
        None => {
            println!("另一进程持有采集锁（{:?}），本 daemon 不启动采集。", data_dir);
            return Ok(());
        }
    };
    let cfg = SchedulerConfig {
        region_id: region,
        rounds,
        ..Default::default()
    };
    let sched = Scheduler::new(client.clone(), db.clone(), cfg);
    let mut rx = sched.subscribe();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let tx = stop_tx.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        println!("\n收到 Ctrl-C，等当前轮结束后退出…");
        let _ = tx.send(true);
    });

    let ui = tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let s = rx.borrow().clone();
            match &s.stage {
                Stage::Failed(e) => println!("第 {} 轮失败：{e}", s.round),
                Stage::WaitingNext => println!(
                    "第 {} 轮完成：{} 单 / {} 行快照 / {} 个枢纽 / 耗时 {:.1}s｜下一轮 {} 后",
                    s.round, s.orders, s.rows_written, s.hubs, s.last_seconds,
                    human(s.next_in)
                ),
                other => println!("第 {} 轮：{:?}", s.round, other),
            }
        }
    });

    let done = sched.run(stop_rx).await;
    let _ = stop_tx.send(true);
    ui.abort();

    let rounds_done = done.context("调度循环异常退出")?;
    println!(
        "已跑 {rounds_done} 轮｜吉他单簿 {} 行｜令牌余量 {}",
        sched.jita_rows()?,
        client.remaining_tokens()
    );
    Ok(())
}

async fn run_prices(client: &EsiClient, db: &Db) -> Result<()> {
    #[derive(serde::Deserialize)]
    struct Row {
        type_id: u32,
        adjusted_price: Option<f64>,
        average_price: Option<f64>,
    }
    let t = std::time::Instant::now();
    let rows: Vec<Row> = client
        .get_json("/v1/markets/prices")
        .await
        .context("markets/prices 拉取失败")?;
    let today = chrono::Utc::now().date_naive().to_string();
    let mapped: Vec<PriceRow> = rows
        .into_iter()
        .map(|r| PriceRow {
            date: today.clone(),
            type_id: r.type_id,
            adjusted: r.adjusted_price,
            average: r.average_price,
        })
        .collect();
    let n = db.write_prices(&mapped)?;
    println!(
        "基准价 {n} 行（{today}）｜耗时 {}｜令牌余量 {}",
        human(t.elapsed()),
        client.remaining_tokens()
    );
    Ok(())
}

async fn run_gate(client: &EsiClient, db: &Db, full: bool) -> Result<()> {
    let report = if full {
        compliance::run_full_gate(client, db)
            .await
            .context("闸门执行中出错")?
    } else {
        compliance::run_startup_probe(client)
            .await
            .context("自检执行中出错")?
    };

    println!("\nM0.5 合规冒烟（{:.1}s）", report.elapsed_secs);
    println!("{:<26} {:<8} 说明", "检查项", "判定");
    println!("{}", "-".repeat(100));
    for c in &report.checks {
        let mark = match c.verdict {
            compliance::Verdict::Pass => "PASS",
            compliance::Verdict::Fail => "FAIL",
            compliance::Verdict::Skipped => "SKIP",
        };
        println!("{:<26} {:<8} {}", c.name, mark, c.detail);
    }
    println!("{}", "-".repeat(100));
    let stats = client.stats();
    println!(
        "请求 {}｜本地命中 {}｜304 {}｜上游 HIT/MISS {}/{}｜令牌余量 {}",
        stats.requests,
        stats.served_from_cache,
        stats.not_modified,
        stats.upstream_hit,
        stats.upstream_miss,
        client.remaining_tokens()
    );

    if report.all_ok() {
        println!("\n✅ 闸门通过");
        Ok(())
    } else {
        anyhow::bail!(
            "❌ 闸门未通过：{}",
            report
                .failures()
                .iter()
                .map(|c| c.name)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}

fn run_stats(client: &EsiClient, db: &Db, path: &std::path::Path) -> Result<()> {
    let c = db.counts()?;
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let prices: i64 = db
        .conn()
        .query_row("SELECT COUNT(*) FROM prices_daily", [], |r| r.get(0))?;
    println!(
        "库 {}｜schema v{}｜快照 {} 行（类型 {}／站点 {}／双向盘 {}）｜基准价 {} 行",
        path.display(),
        db.schema_version()?,
        c.rows,
        c.types,
        c.stations,
        c.both_sides,
        prices,
    );
    match db.last_round()? {
        Some(r) => println!(
            "上一轮：{}｜{} 页 {} 单 → {} 行｜{:.1}s｜漂移重拉 {}｜状态 {}",
            chrono::DateTime::from_timestamp(r.started_at, 0)
                .map(|d| d.to_rfc3339())
                .unwrap_or_else(|| "?".into()),
            r.pages,
            r.orders,
            r.rows_written,
            r.seconds,
            r.drift_retries,
            r.status,
        ),
        None => println!("上一轮：无（还没跑过 round/serve）"),
    }
    println!(
        "磁盘 {:.1} MB｜轮数 {}｜令牌余量 {}",
        size as f64 / 1_048_576.0,
        db.round_count()?,
        client.remaining_tokens()
    );
    Ok(())
}

fn run_hubs(db: &Db) -> Result<()> {
    let hubs = db.hub_pool()?;
    if hubs.is_empty() {
        println!("枢纽池为空 —— 先跑一轮 round。");
        return Ok(());
    }
    println!("{:<5} {:<14} {:>8} {:>8}  站点", "名次", "location_id", "订单数", "占比");
    for h in &hubs {
        let name = db
            .station_name(h.location_id)?
            .unwrap_or_else(|| "（未解析）".into());
        println!(
            "{:<5} {:<14} {:>8} {:>7.2}%  {name}",
            h.rank, h.location_id, h.order_count, h.share_pct
        );
    }
    Ok(())
}

/// 倒卖扫描（M4a spec §3.2）：读本地快照跑 flip::scan。
/// 0 机会时输出丢弃原因分布——没它用户会把正常过滤当成 bug。
fn run_flip(db: &Db, top: u32, acct: Option<u8>, br: Option<u8>) -> Result<()> {
    let books = db.load_books()?;
    let hubs = db.flip_hubs()?;
    let ages = db.xregion_ages()?;
    let vol = db.latest_vol24()?;
    let mut params = db.get_flip_params()?;
    // 临时覆盖只作用于本次运行：同一个快照上对比技能口径，不写库。
    if let Some(v) = acct {
        params.fees.accounting = v.min(5);
    }
    if let Some(v) = br {
        params.fees.broker_relations = v.min(5);
    }
    let age = db.last_round_age_secs()?;
    let f = &params.fees;
    println!(
        "倒卖扫描：单簿 {} 个｜枢纽 {} 个｜有效销售税 {:.2}%｜有效中介费 {:.3}%｜技能 A{} B{}｜阈值 {:.1}%｜快照 {}",
        books.len(),
        hubs.len(),
        f.effective_sales_tax() * 100.0,
        f.effective_broker() * 100.0,
        f.accounting,
        f.broker_relations,
        params.margin_threshold_pct,
        age.map(|s| format!("{s}s 前"))
            .unwrap_or_else(|| "无（先跑 round）".into()),
    );
    let out = market::scan(&books, &hubs, &params, &vol);
    if out.opportunities.is_empty() {
        println!(
            "本轮 0 机会：评估 {} 对｜want<批量 {}｜短填 {}｜未过阈值 {}",
            out.stats.pairs_evaluated,
            out.stats.dropped_batch,
            out.stats.dropped_shortfall,
            out.stats.dropped_threshold
        );
        return Ok(());
    }
    println!(
        "{:<30} {:<34} {:>12} {:>12} {:>8} {:>9} {:>14} {:>9}",
        "类型", "买站→卖站", "买价", "卖价", "可成交", "净利率%", "总净利", "24h量"
    );
    let now = now_unix();
    for o in out.opportunities.iter().take(top as usize) {
        let tname = db.type_name(o.type_id)?.unwrap_or_else(|| o.type_id.to_string());
        let an = db.station_name(o.buy_loc)?.unwrap_or_else(|| o.buy_loc.to_string());
        let bn = db.station_name(o.sell_loc)?.unwrap_or_else(|| o.sell_loc.to_string());
        let vsrc = if o.vol_source == market::VolSource::History { " " } else { "*" };
        // 任一站在 xregion_ages 里 = 跨区行；两站取"最老"那份数据的时间戳算年龄。
        let xregion_badge: String = [o.buy_loc, o.sell_loc]
            .iter()
            .filter_map(|l| ages.get(l).copied())
            .min()
            .map(|ts| {
                let secs = (now - ts).max(0);
                let mins = ((secs + 30) / 60).max(1);
                format!(" [跨区{mins}m]")
            })
            .unwrap_or_default();
        println!(
            "{:<30} {:<34} {:>12} {:>12} {:>8} {:>8.2}% {:>14} {:>8}{}",
            tname,
            ellipsis(&format!("{an}→{bn}{xregion_badge}"), 34),
            fmt_price(o.buy_price),
            fmt_price(o.sell_price),
            o.qty,
            o.margin_pct,
            fmt_price(o.net_total),
            o.vol24,
            vsrc,
        );
    }
    println!(
        "注：24h 量带 * = 该类型无 history 覆盖，用可执行深度估算；带 [跨区Xm] 行的目标站数据来自上一批 T1.5 采集（最长滞后 12 分钟）；费率为估算口径。"
    );
    Ok(())
}

/// 手工触发一趟 T1.5（serve 里隔轮自动跑，这里给验收一个入口）。
async fn run_xregion(client: &Arc<EsiClient>, db: &Arc<Db>) -> Result<()> {
    let sched = Scheduler::new(
        client.clone(),
        db.clone(),
        SchedulerConfig {
            xregion: XRegionConfig::from_env(),
            ..Default::default()
        },
    );
    match sched.run_t1_5().await? {
        Some(rep) => {
            println!(
                "T1.5：候选 {} 类型 → 成功 {} / 失败 {}｜{} 页 / {} 单 / {} 本跨区单簿｜耗时 {:.1}s",
                rep.types_requested,
                rep.types_ok,
                rep.types_failed,
                rep.requests,
                rep.orders,
                rep.books_written,
                rep.seconds
            );
            if let Some(log) = db.last_xregion_log()? {
                println!(
                    "  最近台账：状态 {}｜{} 站｜{} 类型｜{} 请求｜{} 失败",
                    log.status, log.regions, log.types, log.requests, log.failed
                );
            }
            if rep.books_written == 0 {
                println!("  提示：三站 0 本单簿 = 站 ID 与真实枢纽不符，请核对 XREGION_TARGETS");
            }
        }
        None => println!("T1.5 未跑：候选空——先跑 round 攒出机会再补拉"),
    }
    Ok(())
}

/// 打印机会生命周期表。可选先结算（--update），可选按状态过滤（--state）。
fn run_opps(db: &Db, do_update: bool, state: Option<&str>) -> Result<()> {
    use emd_core::market::lifecycle::{self, OppState};
    if do_update {
        match lifecycle::update_round(db, now_unix())? {
            Some(s) => println!(
                "本轮结算：活跃 {}｜新 {}｜复活 {}｜失效 {}｜过期 {}（写 {}）",
                s.active, s.new, s.revived, s.invalidated, s.expired, s.saved
            ),
            None => println!("本轮结算跳过（本地还没有快照）"),
        }
    }
    let filter: Option<OppState> = match state {
        Some(s) => Some(
            OppState::parse(s)
                .with_context(|| format!("--state 未知值 {s}"))?,
        ),
        None => None,
    };
    let rows = db.load_opps()?;
    let shown: Vec<_> = rows
        .iter()
        .filter(|r| filter.map_or(true, |f| r.state == f))
        .collect();
    if shown.is_empty() {
        println!("机会表为空——先跑 serve 或 opps --update");
        return Ok(());
    }
    println!(
        "{:<12} {:<36} {:<12} {:>8} {:>6} {:>12} {:>12} {}",
        "类型", "买站→卖站", "状态", "净利率%", "缺席", "首次 seen", "最近 seen", "通知(day×count@at)"
    );
    for r in shown.iter() {
        let tname = db.type_name(r.type_id)?.unwrap_or_else(|| r.type_id.to_string());
        let an = db.station_name(r.buy_loc)?.unwrap_or_else(|| r.buy_loc.to_string());
        let bn = db.station_name(r.sell_loc)?.unwrap_or_else(|| r.sell_loc.to_string());
        let notify = match (r.notified_at, r.notified_day.as_deref()) {
            (Some(at), Some(day)) => format!("{day}x{}@{at}", r.notified_count_day),
            (Some(at), None) => format!("@{at}"),
            _ => "-".into(),
        };
        println!(
            "{:<12} {:<36} {:<12} {:>8.2} {:>6} {:>12} {:>12} {}",
            ellipsis(&tname, 12),
            ellipsis(&format!("{an}→{bn}"), 36),
            r.state.as_str(),
            r.last_margin_pct,
            r.miss_streak,
            r.first_seen_at,
            r.last_seen_at,
            notify,
        );
    }
    let mut counts: std::collections::BTreeMap<&'static str, u32> = Default::default();
    for r in rows.iter() {
        *counts.entry(r.state.as_str()).or_insert(0) += 1;
    }
    let summary: Vec<String> = counts.into_iter().map(|(s, c)| format!("{s} {c}")).collect();
    println!("显示 {} 行｜全表按状态：{}", shown.len(), summary.join("｜"));
    Ok(())
}

/// 长名字截断，防止中文站名把表格列顶飞。
fn ellipsis(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn run_jita(db: &Db, limit: u32) -> Result<()> {
    let rows = db.station_book(STATION_JITA, true)?;
    println!(
        "吉他单站双向盘 {} 行类型，显示前 {}：",
        rows.len(),
        limit.min(rows.len() as u32)
    );
    println!("{:<9} {:>12} {:>12} {:>7}/{:<7} ", "type_id", "买一", "卖一", "买档", "卖档");
    for r in rows.iter().take(limit as usize) {
        println!(
            "{:<9} {:>12} {:>12} {:>7}/{:<7}",
            r.type_id,
            r.best_bid.map(|v| fmt_price(v)).unwrap_or_else(|| "-".into()),
            r.best_ask.map(|v| fmt_price(v)).unwrap_or_else(|| "-".into()),
            r.bid_levels,
            r.ask_levels,
        );
    }
    Ok(())
}

/// ISK 数值要能一眼读出量级，所以 >=1000 加千分位；Rust 的 format 不支持 `,` grouping。
fn fmt_price(v: f64) -> String {
    if v.abs() < 1000.0 {
        return format!("{v:.2}");
    }
    let digits = format!("{v:.0}");
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn human(d: std::time::Duration) -> String {
    let s = d.as_secs();
    if s >= 60 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{:.1}s", d.as_secs_f64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn prices_get_three_digit_groups() {
        assert_eq!(fmt_price(4.0666667), "4.07");
        assert_eq!(fmt_price(999.4), "999.40");
        assert_eq!(fmt_price(1000.0), "1,000");
        assert_eq!(fmt_price(423712969.0), "423,712,969");
        assert_eq!(fmt_price(210_000_000.0), "210,000,000");
        assert_eq!(fmt_price(2_100_000_000.0), "2,100,000,000");
    }

    #[test]
    fn durations_read_in_minutes_past_a_minute() {
        assert_eq!(human(Duration::from_millis(1500)), "1.5s");
        assert_eq!(human(Duration::from_secs(59)), "59.0s");
        assert_eq!(human(Duration::from_secs(60)), "1m00s");
        assert_eq!(human(Duration::from_secs(292)), "4m52s");
    }

    #[test]
    fn command_words_map() {
        // 保证 help 里列的每个子命令都能被解析到，不会写出说明里没有（或说明里有但没实现）的命令。
        let words = [
            "round", "serve", "prices", "verify", "probe", "stats", "hubs", "jita", "names",
            "tree", "search", "list", "history", "flip", "xregion", "opps",
        ];
        let want = [
            Command::Round,
            Command::Serve,
            Command::Prices,
            Command::Verify,
            Command::Probe,
            Command::Stats,
            Command::Hubs,
            Command::Jita,
            Command::Names,
            Command::Tree,
            Command::Search,
            Command::List,
            Command::History,
            Command::Flip,
            Command::XRegion,
            Command::Opps,
        ];
        for (w, x) in words.iter().zip(want.iter()) {
            let a = parse_from([w.to_string()].into_iter()).unwrap_or_else(|e| panic!("{w}: {e}"));
            assert_eq!(&a.command, x, "子命令 {w} 的映射不对");
        }
        assert_eq!(words.len(), want.len());
    }

    #[test]
    fn history_flags_parse() {
        let a = parse_from(["history", "--probe", "--region", "10000043"].into_iter().map(String::from))
            .unwrap();
        assert_eq!((a.command, a.do_probe, a.dry, a.region), (Command::History, true, false, 10000043));
        assert_eq!(a.limit, None, "不传 --limit 时不能默认截断到 20，否则每日 1 200 的 L0 会被悄悄砍掉");

        let b = parse_from(["history", "--dry", "--limit", "5", "--type", "34"].into_iter().map(String::from)).unwrap();
        assert_eq!((b.dry, b.limit, b.type_id), (true, Some(5), Some(34)));

        // 未知参数与缺参数都必须报错，不能静默按默认值跑。
        assert!(parse_from(["history", "--nope"].into_iter().map(String::from)).is_err());
        assert!(parse_from(["history", "--limit"].into_iter().map(String::from)).is_err());
        assert!(parse_from(["--dry"].into_iter().map(String::from)).is_err());
    }

    #[test]
    fn flip_flags_parse() {
        let a = parse_from(["flip"].into_iter().map(String::from)).unwrap();
        assert_eq!(a.command, Command::Flip);
        assert_eq!(a.limit, None, "不传 --top/--limit 时由派发层决定默认 20");
        let b = parse_from(["flip", "--top", "10"].into_iter().map(String::from)).unwrap();
        assert_eq!(b.limit, Some(10), "--top 是 --limit 的别名");
        let c = parse_from(
            ["flip", "--accounting", "5", "--broker-relations", "3"]
                .into_iter()
                .map(String::from),
        )
        .unwrap();
        assert_eq!((c.accounting, c.broker_relations), (Some(5), Some(3)));
        assert!(parse_from(["flip", "--accounting", "x"].into_iter().map(String::from)).is_err());
    }

    #[test]
    fn parses_xregion_and_opps() {
        let a = parse_from(["xregion"].into_iter().map(String::from)).unwrap();
        assert_eq!(a.command, Command::XRegion);
        let b = parse_from(["opps", "--update"].into_iter().map(String::from)).unwrap();
        assert_eq!(b.command, Command::Opps);
        assert!(b.do_update, "--update 打开生命周期结算");
        let c = parse_from(["opps", "--state", "expired"].into_iter().map(String::from)).unwrap();
        assert_eq!(c.state.as_deref(), Some("expired"));
        assert!(
            parse_from(["opps", "--state", "nonsense"].into_iter().map(String::from)).is_err(),
            "未知状态当场拒绝，不静默全表"
        );
    }
}
