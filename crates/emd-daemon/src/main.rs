//! 无头采集进程。Tauri 壳还没上之前，它就是这个项目的全部可运行形态；
//! 上了壳之后它继续作为"关窗口也在攒数据"的常驻后端存在。

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use emd_core::alert::{AlertKind, AlertRecord};
use emd_core::catalog;
use emd_core::collector::InstanceLock;
use emd_core::compliance;
use emd_core::config::{CharConfig, EsiConfig};
use emd_core::esi::EsiClient;
use emd_core::market::history::{self, HistoryConfig};
use emd_core::market::{self, STATION_JITA};
use emd_core::scheduler::{
    Scheduler, SchedulerConfig, Stage, XRegionConfig, KEYRING_ACCOUNT, KEYRING_SERVICE,
};
use emd_core::sso::flow::char_from_access_token;
use emd_core::sso::store::{KeyringTokenStore, TokenStore};
use emd_core::sso::token::TokenSet;
use emd_core::store::{now_unix, CharMeta, Db, PriceRow};
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
    Alerts,
    Char,
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
    /// opps/alerts 专用：--update 先跑一轮再展示（opps 结算生命周期，alerts 跑告警回合）。
    do_update: bool,
    /// opps 专用：按状态过滤（new/notified/expired/invalidated）。
    state: Option<String>,
    /// alerts 专用：按形态过滤（expected_sell_loss/buy_order_trap/realized_loss）。
    kind: Option<String>,
    /// char 专用：打印挂链状态（不带参数时也是这个动作）。
    status: bool,
    /// char 专用：--logout 清系统凭据库里的令牌。
    logout: bool,
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
    let mut kind: Option<String> = None;
    let mut status = false;
    let mut logout = false;

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
            "alerts" => cmd = Some(Command::Alerts),
            "char" => cmd = Some(Command::Char),
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
            "--kind" => {
                let v = it.next().context("--kind 缺参数")?;
                // 与 --state 同一条纪律：未知形态当场拒绝，不静默全表 ——
                // `alerts --kind typo` 默默打出三类告警，用户会以为筛选后的就是这些。
                AlertKind::parse(&v).with_context(|| {
                    format!("--kind 需要 expected_sell_loss/buy_order_trap/realized_loss，收到 {v}")
                })?;
                kind = Some(v);
            }
            "--status" => status = true,
            "--logout" => logout = true,
            other => anyhow::bail!("未知参数：{other}"),
        }
    }

    Ok(Args {
        command: cmd.context("缺少子命令（round|serve|prices|verify|probe|stats|hubs|jita|names|tree|search|list|history|flip|xregion|opps|alerts|char）")?,
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
        kind,
        status,
        logout,
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
  alerts   打印告警表（形态/alert_key/char_id/类型/站点/亏损额/margin/状态/通知计数）；--update 先跑一轮告警回合；--kind 过滤
  char     查看或退出角色挂链：默认打印挂链状态与同步水位（同 --status）；--logout 清系统凭据库里的令牌
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
  --update     opps/alerts 展示前先跑一轮：opps 结算生命周期，alerts 跑角色同步+告警回合
  --state S    opps 按状态过滤：new | notified | expired | invalidated
  --kind K     alerts 按形态过滤：expected_sell_loss | buy_order_trap | realized_loss
  --status     char 打印挂链状态（不带参数时的默认动作）
  --logout     char 退出登录：清掉系统凭据库里的 SSO 令牌（库内角色数据与告警表不动）
  --type N     history 只取这一个类型
  --dry        history 只打印取数计划与成本估算，不发请求
  --probe      history 的交叉实验：证明 history 不占 market-order 令牌组

  环境变量：EMD_CONTACT_EMAIL（UA 里的联系邮箱）、EMD_HISTORY_CAP（0 = 关掉每日 T3）、
            EMD_CHAR_CLIENT_ID（角色挂链的 SSO client_id，配了才启用同步）"
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
        Command::Alerts => run_alerts(&client, &db, args.do_update, args.kind.as_deref()).await?,
        Command::Char => run_char(
            &db,
            // P2：与登录/调度器**同一对**凭据坐标（常量只有一处定义）—— 这里若自己拼一对
            // 新的 service/account，读到的就是另一条空条目，现象是"明明登录过却一直没令牌"。
            &KeyringTokenStore::new(KEYRING_SERVICE, KEYRING_ACCOUNT),
            args.status,
            args.logout,
        )?,
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

/// `round`/`serve` 的调度配置。**char 侧必须显式带进来**：`SchedulerConfig::default()` 里是
/// `CharConfig::default().enabled = false`，照抄默认值会让每个采集轮静默跳过同步与告警 ——
/// 没有一条日志解释，用户只看到"永远零告警"（`emd alerts` 自己印的话术是"先跑 serve，每轮
/// 自动跑一次告警回合"，那句只有在这里带上 char 配置时才成立）。
///
/// 拆成纯函数是为了让这条接线能被测试钉住：入参给什么就得到什么，没有第二个默认值能在中间
/// 把它换掉。**调用点**用哪份 CharConfig（现在是 [`CharConfig::load`]）仍由评审保证 —— 测试
/// 钉的是"带上之后不会被换掉"，不是"调用点真的带了"。
fn serve_scheduler_config(region: u32, rounds: Option<u64>, char: CharConfig) -> SchedulerConfig {
    SchedulerConfig {
        region_id: region,
        rounds,
        char,
        ..Default::default()
    }
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
    // char 侧走**分层配置**（`load` = 库 > env > 默认）：设置页写进库里的
    // client_id/redirect_uri 与 env 一样能开同步 —— daemon 是关掉 Tauri 窗口之后
    // 唯一仍在采的进程，只读 env 会让"在界面里配好、关窗口后照常告警"落空。
    // `SchedulerConfig::default()` 的 char 侧仍是**关闭**的（`CharConfig::default()`），
    // 所以这里必须显式传（见 [serve_scheduler_config]）。
    let cfg = serve_scheduler_config(region, rounds, CharConfig::load(&db)?);
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
        // 按 (站, 类型) 查，避免部分类型拉失败时角标低估年龄。
        let xregion_badge: String = [o.buy_loc, o.sell_loc]
            .iter()
            .filter_map(|l| ages.get(&(*l, o.type_id)).copied())
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
            // 角标拼在 ellipsis 之外：站名 45 字符时 badge 不能被截断吃掉。
            format!(
                "{}{}",
                ellipsis(&format!("{an}→{bn}"), 34),
                xregion_badge
            ),
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

// ---------------------------------------------------------------------------
// M4c：告警表与角色挂链的运维面（T13）
// ---------------------------------------------------------------------------

/// 告警表：形态 / alert_key / char_id / 类型 / 站点 / 亏损额 / margin / 状态 / 通知计数，表尾给
/// 全表按形态的计数 —— 带 `--kind` 过滤时也看得见"另外两类各有多少"，否则无从判断过滤是否符合预期。
///
/// **两列新字段是"可动手"的前提**：`alert_key`（`order:{id}` / `tx:{id}`）是这一行的去重键，
/// 已实现轨的成交 id **只**存在于它里面（`alert/state.rs` 的 `AlertRecord`），`char_id` 则是多角色
/// 挂链时"这行是谁的"唯一的答案 —— 少了它们，用户看见亏损却指不到那张单、也认不着人。
///
/// 表头写**列名原文**而不是中文：这一列的值就是拿去库里检索的键（`alerts.alert_key` /
/// `char_orders.char_id`），照列名写省一道翻译；顺带避开中文表头的老毛病 —— `{:<20}` 数的是
/// 字符数（中日韩字符显示宽度是 2），中文表头本来就会一格一格往右漂。
///
/// `--update` 先跑一轮告警回合（[`run_alert_round`]）再展示；`--kind` 的取值已在解析期认过。
async fn run_alerts(
    client: &Arc<EsiClient>,
    db: &Arc<Db>,
    do_update: bool,
    kind: Option<&str>,
) -> Result<()> {
    if do_update {
        run_alert_round(client, db).await?;
    }
    let filter = match kind {
        Some(k) => Some(
            AlertKind::parse(k).with_context(|| format!("--kind 未知值 {k}"))?,
        ),
        None => None,
    };
    let rows = db.load_alerts()?;
    if rows.is_empty() {
        // 空表不是错误：还没登录/还没跑过告警回合。指个下一步，别让用户对着空屏猜。
        println!("告警表为空 —— 先跑 serve（每轮自动跑一次告警回合），或 alerts --update 手工跑一轮。");
        return Ok(());
    }
    let shown: Vec<&AlertRecord> = rows
        .iter()
        .filter(|r| filter.map_or(true, |f| r.kind == f))
        .collect();
    if shown.is_empty() {
        println!("--kind {} 没有命中的行。", kind.unwrap_or_default());
    } else {
        println!(
            "{:<20} {:<20} {:<10} {:<26} {:<30} {:>14} {:>10} {:<9} {}",
            "形态", "alert_key", "char_id", "类型", "站点", "亏损额", "margin%", "状态", "通知(day×count@at)"
        );
        for r in &shown {
            let tname = db.type_name(r.type_id)?.unwrap_or_else(|| r.type_id.to_string());
            let sname = db
                .station_name(r.location_id)?
                .unwrap_or_else(|| r.location_id.to_string());
            // 与 opps 同一套通知史文本：没推过就是 "-"，推过就带当日的 day×count 与时刻。
            let notify = match (r.notified_at, r.notified_day.as_deref()) {
                (Some(at), Some(day)) => format!("{day}x{}@{at}", r.notified_count_day),
                (Some(at), None) => format!("@{at}"),
                _ => "-".into(),
            };
            println!(
                "{:<20} {:<20} {:<10} {:<26} {:<30} {:>14} {:>9.2}% {:<9} {}",
                r.kind.as_str(),
                // `alert_key` **原样打、不过 `ellipsis`**：截尾就把它唯一的用途作废（拿这个 key 去
                // 库里检索那张单/那笔成交）。列宽按 `order:` + 13 位实战 id 给，真超长宁可让这一行
                // 右移一格，也不给出一个查不到的残缺 id。
                r.alert_key,
                r.char_id,
                ellipsis(&tname, 26),
                ellipsis(&sname, 30),
                fmt_price(r.last_loss_isk),
                r.last_margin_pct,
                r.state.as_str(),
                notify,
            );
        }
    }
    // 逐形态数出来而不是只数命中的那一类：三类的分母都在这一行里。走 `AlertKind::ALL`
    // 于是"0 条"也会露面 —— 缺的那一类不是被过滤掉了，而是本来就没有。
    let totals: Vec<String> = AlertKind::ALL
        .iter()
        .map(|k| {
            format!(
                "{} {}",
                k.as_str(),
                rows.iter().filter(|r| r.kind == *k).count()
            )
        })
        .collect();
    println!("显示 {} 行｜全表按形态：{}", shown.len(), totals.join("｜"));
    Ok(())
}

/// `alerts --update` 的那一轮。**复用调度器的装配**（[`Scheduler::run_char_and_alerts`]）
/// 而不是在 daemon 里另拼一遍：令牌（过期才刷）、角色身份（从令牌自己里取）、通道装配
/// （`PushConfig`：本地提醒中心恒在 + 钉钉按配置）三件事都在那一条路径上 —— 在 daemon 里
/// 重写第一遍就是 M1 `round`/`serve` 分叉的重演，两边的判定与推送迟早对不上。
///
/// **没有令牌就不跑**（P4）。本地那一半（读库 → 判定 → 落库 → 派发）在 `alert::update_round`
/// 里与同步共用一段**顺序契约**（P3 的 journal 真值必须紧跟同步返回、P2 的落库先于派发），
/// 把判定侧单独拆出来重写，等于把 T12 审核过的顺序在 daemon 里再抄一遍 —— 抄错的形态是
/// **静默少报**而不是报错。于是这里明确说"没跑、为什么"，既不伪造令牌，也不把"没登录"
/// 打成一片 0：那样看起来像"跑过了，这个角色确实没有亏损"。
///
/// 通道侧不需要"没有通道"的兜底：`PushConfig::channels()` 恒含本地提醒中心（spec §4.5 的
/// 回落方案），钉钉只在开关打开且 webhook 填了时进场；本地那条恒回 `Sent`，于是"过闸即记账"
/// 的口径与 serve 完全一致 —— 也就不存在"拿空通道列表跑一轮"这种形态。
async fn run_alert_round(client: &Arc<EsiClient>, db: &Arc<Db>) -> Result<()> {
    // P1：**必须走分层配置**（`load` = 库 > env > 默认）。`CharConfig::default()` 的 char 侧
    // 是**关闭**的 —— 用它会让"配了 client_id、登录也成功了"的机器每轮静默跳过同步，一条日志
    // 都不解释，用户看到的是"零告警"；而只读 env 会让"在设置页配好 client_id"的机器落到
    // 同一种静默里（T15 收尾发现的那条 Critical）。
    let cfg = CharConfig::load(db)?;
    if !cfg.enabled {
        println!(
            "角色同步未启用（设置页与 EMD_CHAR_CLIENT_ID 都没填 client_id，或 EMD_CHAR_SYNC=0）—— 本轮不做判定与推送，只展示本地告警表。"
        );
        return Ok(());
    }
    let store = KeyringTokenStore::new(KEYRING_SERVICE, KEYRING_ACCOUNT);
    if store.load()?.is_none() {
        println!(
            "未登录（系统凭据库里没有 SSO 令牌）—— 同步需要访问令牌，本轮不做判定与推送；先登录再跑（不伪造令牌）。"
        );
        return Ok(());
    }
    // 两道门先自己看一眼只为把 `Ok(None)` 的形态说清楚；真正的回合仍走调度器 ——
    // 那里面还有同一对门（关着时不碰凭据库），语义不变。
    let sched = Scheduler::new(
        client.clone(),
        db.clone(),
        SchedulerConfig {
            char: cfg,
            ..Default::default()
        },
    );
    match sched.run_char_and_alerts().await? {
        Some(rep) => {
            // P5：字段照打。`pushed`/`suppressed` **不覆盖**"派发过但没有任何通道回 Sent"
            // 那一档，所以 detected − pushed − suppressed 不是失败数，这里也不替它编一个。
            println!(
                "告警回合：同步 {} 行｜判定 {} 条｜推送 {} 条｜拦下 {} 条",
                rep.synced, rep.detected, rep.pushed, rep.suppressed
            );
            println!(
                "（口径：判定 = 闸门之前的命中数；拦下 = 冷却中/当日额度已尽，它们照样进提醒中心；推送只记至少一条通道确认的条目）"
            );
        }
        // `Ok(None)` 有三支成因（开关 / 令牌 / 挂单快照未刷新，`scheduler.rs` 的函数头）。上面
        // 两道门与回合内那两道门是**两次读**：令牌那一支在两次读之间可能翻转（凭据库读空、
        // 读不出来都算 `TokenStore::load` 的 `Ok(None)`）。这里分辨不出落在哪一支，就不替用户
        // 挑一个说 —— 早前只写"快照未刷新"，会把"令牌在回合开始前没了"指成 ESI 故障，让人对着
        // 网络查半天（P4 要躲的正是这一类没根据的诊断）。给个自己能确认的下一步：前两支都在
        // `char --status` 里一眼可辨，剩下的回落到日志。
        None => println!(
            "回合没跑：开关没开／令牌在回合开始前没了（含凭据库这轮读不出来）／挂单快照本轮未刷新（403/断网/解析失败）三者之一 —— 判定与推送都跳过；先跑 char --status 看开关与令牌，再看日志（RUST_LOG 调高可见 warn）。"
        ),
    }
    Ok(())
}

/// 一个角色的挂链事实（[`render_char_status`] 的输入）。
struct LinkFacts {
    id: u64,
    /// `char_meta` 的那一行；None = 行还没落（同步先于登录落行时会出现）。
    meta: Option<CharMeta>,
    /// `char_orders` 的当前行数（每轮整表替换的快照）。
    orders: usize,
    /// `char_tx` 的当前行数（老行由 `prune_char_tx` 按回填窗收口）。
    txs: usize,
}

/// 库内已挂链的角色。角色 id 只能从 `char_meta` 的行上取 —— **令牌不在库里**，于是"没登录"
/// 时也必须能回答"上一次挂链的是谁"（那正是排查"为什么没同步"的起点）。
///
/// 这里只借 `conn()` 数一遍 id，列到结构体的映射仍走存储层（`char_meta`/`load_char_*`）：
/// 照 `run_stats` 与 `Scheduler::consecutive_failures` 的裸查询先例，本任务的改动面只有本文件。
fn link_facts(db: &Db) -> Result<Vec<LinkFacts>> {
    let mut stmt = db.conn().prepare("SELECT char_id FROM char_meta ORDER BY char_id")?;
    let ids: Vec<u64> = stmt
        .query_map([], |r| r.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<i64>, _>>()?
        .into_iter()
        .map(|v| v as u64)
        .collect();
    let mut out = Vec::new();
    for id in ids {
        out.push(LinkFacts {
            id,
            meta: db.char_meta(id)?,
            orders: db.load_char_orders(id)?.len(),
            txs: db.load_char_tx(id, None)?.len(),
        });
    }
    Ok(out)
}

/// `char` 的查看与登出。
///
/// `--logout` 只清系统凭据库里的令牌（[`TokenStore::clear`]）：**库内角色数据与告警表一行不动**
/// —— 退出登录是"不用这个角色了"，不是"删掉历史"。
///
/// `--status`（或不带参数）打印挂链状态与同步水位；只跑 `--logout` 时不再压一屏水位
/// （确认句给完就够），两个一起给则是"先清再看"，退出是否真的生效一眼就知道。
fn run_char(db: &Db, store: &dyn TokenStore, status: bool, logout: bool) -> Result<()> {
    if logout {
        store.clear()?;
        println!("已退出登录：系统凭据库里的 SSO 令牌已清除（库内角色数据与告警表不受影响）。");
    }
    if !status && logout {
        return Ok(());
    }
    // `--status` 是排查"为什么什么都没发生"的第一站，开关的取值必须与真正跑回合的
    // 那两条路径同源（分层配置）：只读 env 会对着刚在设置页填好的 client_id 报"未启用"。
    let cfg = CharConfig::load(db)?;
    let tokens = store.load()?;
    let links = link_facts(db)?;
    print!("{}", render_char_status(&cfg, tokens.as_ref(), &links, now_unix()));
    Ok(())
}

/// 挂链状态的渲染。**纯函数**（事实进、字符串出）有两个理由：一是 P3 的脱敏断言要有个能断言
/// 的东西（测试里截不到 stdout），二是渲染不再顺手读库/读凭据库。
///
/// **绝不打印令牌**：access/refresh 原文、含它们的任何串都不进这里。令牌只以三种派生事实露面
/// —— 已登录/未登录、到期时刻、以及**从它自己解出来的**角色身份。
fn render_char_status(
    cfg: &CharConfig,
    tokens: Option<&TokenSet>,
    links: &[LinkFacts],
    now: i64,
) -> String {
    let mut out = String::new();
    out.push_str("挂链状态（设置页 / EMD_CHAR_* 与系统凭据库）\n");
    out.push_str(&format!(
        "  同步开关：{}\n",
        if cfg.enabled {
            format!("已启用（首启回填窗 {} 天，即同步水位与 FIFO 成本基准的窗外边界）", cfg.backfill_days)
        } else {
            "未启用（设置页与 EMD_CHAR_CLIENT_ID 都没填 client_id，或 EMD_CHAR_SYNC=0）".to_string()
        }
    ));
    match tokens {
        None => out.push_str("  令牌：未登录（系统凭据库里没有令牌）\n"),
        Some(t) => {
            // 身份从**令牌自己**里解（与调度器同一判据，只解码不验签）：库里那份 `char_meta`
            // 可能是上一个角色的行，拿它当"现在挂链的是谁"会指错人 —— 而 `fetch_auth` 的
            // 缓存键正是路径里的这个角色 id。
            out.push_str("  令牌：已登录");
            match char_from_access_token(&t.access_token) {
                Ok((id, name)) => out.push_str(&format!("（角色 {id} {name}）\n")),
                // 形状认不出时只报形状：`sso::flow` 的报错只带段数与字段名，令牌原文一个字都不带。
                Err(e) => out.push_str(&format!("（角色取不出：{e}）\n")),
            }
            // 到期**只报时刻**，不报剩余寿命以外的任何东西；已过期要说清"下一轮会先刷再同步"，
            // 否则用户会以为这台机器已经停更。
            if t.is_expired(now) {
                out.push_str(&format!(
                    "  有效期：已过期（{}）—— 下一轮同步前会先刷新\n",
                    fmt_ts(t.expires_at)
                ));
            } else {
                out.push_str(&format!(
                    "  有效期：{}（尚余 {}）\n",
                    fmt_ts(t.expires_at),
                    human(std::time::Duration::from_secs((t.expires_at - now).max(0) as u64))
                ));
            }
        }
    }
    if links.is_empty() {
        out.push_str("  库内挂链：没有 —— 登录后先跑一轮 serve（或 alerts --update）才会落 char_meta\n");
        return out;
    }
    for l in links {
        match &l.meta {
            None => out.push_str(&format!("  库内挂链：角色 {} 还没落 char_meta 行\n", l.id)),
            Some(m) => {
                out.push_str(&format!(
                    "  库内挂链：角色 {} {}\n",
                    l.id,
                    m.name.as_deref().unwrap_or("（无名）")
                ));
                out.push_str(&format!(
                    "    首次同步 {}｜上次同步 {}\n",
                    fmt_opt_ts(m.first_sync_at),
                    fmt_age(m.last_sync_at, now)
                ));
                // 水位原样回显（流水/日记账是 ESI 的 ISO8601 文本，orders_lm 是 HTTP 日期原文）：
                // 它们决定下一轮拉哪一段，看不出来就没法判断"为什么没有新数据"。
                out.push_str(&format!(
                    "    同步水位：流水 {}｜日记账 {}｜挂单 Last-Modified {}\n",
                    m.tx_cursor.as_deref().unwrap_or("（无）"),
                    m.journal_cursor.as_deref().unwrap_or("（无）"),
                    m.orders_lm.as_deref().unwrap_or("（无）"),
                ));
                out.push_str(&format!(
                    "    本地数据：挂单 {} 张｜流水 {} 行（保留期即回填窗）\n",
                    l.orders, l.txs
                ));
            }
        }
    }
    out
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

/// Unix 秒 → `YYYY-MM-DDThh:mm:ssZ`（UTC）。推不出时刻时回显原始数字而不是猜一个时间。
fn fmt_ts(t: i64) -> String {
    chrono::DateTime::from_timestamp(t, 0)
        .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| format!("@{t}"))
}

/// `Option<时间戳>` → 文本：没给就是"（无）"（列可空，空与 0 要分得开）。
fn fmt_opt_ts(t: Option<i64>) -> String {
    t.map(fmt_ts).unwrap_or_else(|| "（无）".into())
}

/// 同上，但给了就带上"多久前"——水位一类的东西，年龄比时刻更说明问题。
fn fmt_age(t: Option<i64>, now: i64) -> String {
    match t {
        Some(t) => format!(
            "{}（{}前）",
            fmt_ts(t),
            human(std::time::Duration::from_secs((now - t).max(0) as u64))
        ),
        None => "（无）".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // 只在测试里出现的令牌存储：生产路径用系统凭据库（`KeyringTokenStore`），
    // 把它提到文件头会变成非测试构建下的未使用导入。
    use emd_core::sso::store::MemoryTokenStore;
    use std::time::Duration;

    /// T15 收尾的 Critical：`serve`/`round` 的调度配置必须把 char 配置带进去。
    /// `SchedulerConfig::default()` 的 char 侧是**关闭**的（`CharConfig::default().enabled = false`），
    /// 一旦被那个默认值换掉，每个采集轮都会静默跳过同步与告警 —— 所以这里两个方向都钉死：
    /// 开着的照原样穿过，关着的也不会被"顺手打开"。
    #[test]
    fn serve_scheduler_config_carries_the_character_config_through() {
        let on = CharConfig {
            client_id: "test-client-id".into(),
            enabled: true,
            ..Default::default()
        };
        let cfg = serve_scheduler_config(10000002, Some(3), on.clone());
        assert_eq!(cfg.region_id, 10000002);
        assert_eq!(cfg.rounds, Some(3));
        assert!(cfg.char.enabled, "入参 enabled=true 不许被默认值换回 false");
        assert_eq!(cfg.char, on, "char 配置逐字穿过（client_id / redirect_uri / 回填窗）");

        // 关着的那份同样逐字穿过：不能反过来替用户打开（没配凭证之前不该往外发请求）。
        let off = CharConfig::default();
        let cfg = serve_scheduler_config(10000043, None, off.clone());
        assert!(!cfg.char.enabled);
        assert_eq!(cfg.char, off);
    }

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
            "tree", "search", "list", "history", "flip", "xregion", "opps", "alerts", "char",
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
            Command::Alerts,
            Command::Char,
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

    #[test]
    fn parses_alerts_and_char() {
        // alerts 无参可跑（只看本地表）；--update 打开告警回合；--kind 过滤，未知形态当场拒绝。
        let a = parse_from(["alerts"].into_iter().map(String::from)).unwrap();
        assert_eq!(a.command, Command::Alerts);
        assert!(!a.do_update, "不带 --update 就不跑同步/判定/推送");
        assert!(a.kind.is_none());

        let b = parse_from(["alerts", "--update"].into_iter().map(String::from)).unwrap();
        assert!(b.do_update, "--update 先跑一轮告警回合再展示");

        let c = parse_from(["alerts", "--kind", "expected_sell_loss"].into_iter().map(String::from))
            .unwrap();
        assert_eq!(c.kind.as_deref(), Some("expected_sell_loss"));
        assert!(
            parse_from(["alerts", "--kind", "nonsense"].into_iter().map(String::from)).is_err(),
            "未知形态当场拒绝，不静默全表（否则用户以为过滤生效其实没有）"
        );

        // char 的两个动作各自解析；两者互相独立（可同时给：先登出，再看清没清）。
        let d = parse_from(["char", "--status"].into_iter().map(String::from)).unwrap();
        assert_eq!(d.command, Command::Char);
        assert!(d.status && !d.logout);
        let e = parse_from(["char", "--logout"].into_iter().map(String::from)).unwrap();
        assert_eq!(e.command, Command::Char);
        assert!(e.logout && !e.status);
    }

    /// P3：`char --status` 的输出必须脱敏。哨兵串**真切进过渲染器** —— 角色 id 与名字是从
    /// 这个令牌里解出来的（下面正向断言它们出现），所以"哨兵没出现"不是因为函数压根没看令牌。
    #[test]
    fn char_status_output_carries_no_token_material() {
        // 合成 JWT：payload 段是 `{"sub":"CHARACTER:EVE:90000001","name":"Pilot One","exp":1790000000}`
        // 的 base64url（无 padding），签名段塞哨兵 —— 真令牌的签名段同样是不可读的随机串。
        const AT_SIG: &str = "AT-SENTINEL-ACCESS-TOKEN-9f3a";
        const RT: &str = "RT-SENTINEL-REFRESH-TOKEN-7b21";
        const PAYLOAD: &str = "eyJzdWIiOiJDSEFSQUNURVI6RVZFOjkwMDAwMDAxIiwibmFtZSI6IlBpbG90IE9uZSIsImV4cCI6MTc5MDAwMDAwMH0";
        let at = format!("eyJhbGciOiJub25lIn0.{PAYLOAD}.{AT_SIG}");
        let tokens = TokenSet {
            access_token: at.clone(),
            refresh_token: RT.to_string(),
            expires_at: 1_790_000_000,
        };
        let cfg = CharConfig {
            client_id: "test-client-id".into(),
            enabled: true,
            ..Default::default()
        };
        let meta = CharMeta {
            char_id: 90_000_001,
            name: Some("Pilot One".into()),
            tx_cursor: Some("2026-09-25T11:58:00Z".into()),
            journal_cursor: None,
            orders_lm: Some("Wed, 17 Sep 2026 00:00:00 GMT".into()),
            first_sync_at: Some(1_789_000_000),
            last_sync_at: Some(1_790_000_000),
        };
        let links = vec![LinkFacts {
            id: 90_000_001,
            meta: Some(meta),
            orders: 12,
            txs: 340,
        }];
        // now 远在 expires_at 之后：走"已过期"那条分支。
        let out = render_char_status(&cfg, Some(&tokens), &links, 1_790_003_600);

        // 正向证据：令牌真被读过（身份与到期判定都是从它派生的）。
        assert!(out.contains("90000001"), "角色 id 取自令牌：{out}");
        assert!(out.contains("Pilot One"), "角色名取自令牌：{out}");
        assert!(out.contains("已过期"), "到期判定由令牌的 expires_at 算出：{out}");
        assert!(out.contains("90 天"), "回填窗取自配置：{out}");

        // 本测试的要害：令牌原文一个字都不在输出里。
        assert!(!out.contains(AT_SIG), "access_token 进输出了：{out}");
        assert!(!out.contains(RT), "refresh_token 进输出了：{out}");
        assert!(!out.contains(&at), "整条 access_token 进输出了：{out}");
        assert!(!out.contains("SENTINEL"), "哨兵串进输出了：{out}");

        // 形状不是 JWT 的令牌（换过序列化格式 / 凭据被手工改过）：只报"取不出角色"，
        // 错误串里同样没有令牌（`sso::flow` 的报错只带段数与字段名）。
        let weird = TokenSet {
            access_token: format!("not-a-jwt-{AT_SIG}"),
            refresh_token: RT.to_string(),
            expires_at: 1_790_003_600,
        };
        let out = render_char_status(&cfg, Some(&weird), &[], 1_790_000_000);
        assert!(out.contains("取不出"), "{out}");
        assert!(!out.contains("SENTINEL"), "异形令牌的报错里带了原文：{out}");

        // 没登录（凭据库读空）也要能回答"上一次挂链的是谁"：这是排查"为什么没同步"的起点。
        let out = render_char_status(&cfg, None, &links, 1_790_000_000);
        assert!(out.contains("未登录"), "{out}");
        assert!(out.contains("Pilot One"), "库内的挂链事实与令牌无关：{out}");
        assert!(out.contains("2026-09-25T11:58:00Z"), "水位原样回显：{out}");
    }

    #[test]
    fn char_logout_clears_the_token_store_and_leaves_the_db_alone() {
        let db = Db::in_memory().unwrap();
        let store = MemoryTokenStore::default();
        db.upsert_char_meta(90_000_001, "Pilot One", 1_790_000_000)
            .unwrap();

        // --status 是纯读：不碰凭据库，也不改库。
        store
            .save(&TokenSet {
                access_token: "AT-SENTINEL".into(),
                refresh_token: "RT-SENTINEL".into(),
                expires_at: 0,
            })
            .unwrap();
        run_char(&db, &store, true, false).unwrap();
        assert!(store.load().unwrap().is_some(), "--status 不得动凭据库");
        assert!(db.char_meta(90_000_001).unwrap().is_some());

        // --logout 走 TokenStore::clear()：令牌没了，库内角色数据一行不动
        // （"不用这个角色了"不是"删掉历史"）。
        run_char(&db, &store, false, true).unwrap();
        assert!(store.load().unwrap().is_none(), "char --logout 必须清凭据库");
        assert!(
            db.char_meta(90_000_001).unwrap().is_some(),
            "退出登录不得删库内角色数据"
        );

        // 幂等：本来就没登录时再退一次不该报错（clear 的契约）。
        run_char(&db, &store, false, true).unwrap();
    }
}
