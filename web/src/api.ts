import type { AlertPayload, AlertRow, AlertSettings, AlertSettingsIn, AppStatus, FeeModel, FlipParams, FlipRow, FlipScan, HistoryBar, Hub, ListingRow, SsoLoginOut, SsoStatus, TrialOut, TreeGroup, TreeNode, TypeDetail } from "./types";

/**
 * 与 Rust 后端的唯一边界。
 *
 * 浏览器里直接 `npm run dev`（没有 Tauri 壳）时走 `fixtures`，只为看布局与联调交互；
 * 一旦运行在 WebView2 里就一定走真实 IPC —— 两者不能混，否则会把假数据当行情看。
 * fixture 里的费率重算（spec §3.3"参数/技能改动在 fixture 下本地重算"）是 Rust
 * 公式的走查镜像：锚点对齐 emd-core::market::flip 的单测，漂移时以 Rust 为准。
 */
const inTauri = typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

async function call<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  const { invoke } = await import("@tauri-apps/api/core");
  return (await invoke<T>(cmd, args)) as T;
}

export const api = {
  live: inTauri,

  async tree(): Promise<TreeNode[]> {
    if (!inTauri) return fixTree();
    return call<TreeNode[]>("get_tree");
  },

  async listing(groupId: number, locationId: number): Promise<ListingRow[]> {
    if (!inTauri) return fixListing(groupId);
    return call<ListingRow[]>("get_listing", { groupId, locationId });
  },

  async detail(typeId: number, locationId: number): Promise<TypeDetail | null> {
    if (!inTauri) return fixDetail(typeId);
    return call<TypeDetail | null>("get_detail", { typeId, locationId });
  },

  async search(word: string): Promise<ListingRow[]> {
    if (!inTauri) return fixListing(0).filter((r) => r.name.toLowerCase().includes(word.toLowerCase()));
    return call<ListingRow[]>("search_types", { word });
  },

  async status(): Promise<AppStatus> {
    if (!inTauri) return fixStatus();
    return call<AppStatus>("get_status");
  },

  async hubs(): Promise<Hub[]> {
    if (!inTauri) return fixHubs();
    return call<Hub[]>("get_hubs");
  },

  /** 手动刷新只影响当前视图数据，绝不去动 ESI 缓存纪律（force 在 Rust 侧被拒）。 */
  async refreshNow(): Promise<void> {
    if (!inTauri) return;
    await call("request_refresh");
  },

  /** 单类型日线（蜡烛图数据源）。region 缺省 The Forge。 */
  async history(typeId: number, regionId?: number): Promise<HistoryBar[]> {
    if (!inTauri) return fixHistory(typeId);
    return call<HistoryBar[]>("get_history", { typeId, regionId });
  },

  async watchlist(): Promise<Array<[number, string | null]>> {
    if (!inTauri) return [[34, "Tritanium"]];
    return call<Array<[number, string | null]>>("get_watchlist");
  },

  async addWatch(typeId: number): Promise<void> {
    if (!inTauri) return;
    await call("add_watch", { typeId });
  },

  async removeWatch(typeId: number): Promise<boolean> {
    if (!inTauri) return false;
    return call<boolean>("remove_watch", { typeId });
  },

  /** 倒卖扫描：读本地快照跑引擎（纯本地，零 ESI 请求）。 */
  async flipScan(): Promise<FlipScan> {
    if (!inTauri) return fixFlip();
    return call<FlipScan>("scan_flip");
  },

  async flipParams(): Promise<FlipParams> {
    if (!inTauri) return fixFlipParams;
    return call<FlipParams>("get_flip_params");
  },

  async setFlipParams(params: FlipParams): Promise<void> {
    if (!inTauri) {
      // 预览没有可落的库：参数存进内存，保存后的 loadFlip 随即用下面的镜像重算
      // （不是"只回显"——spec §3.3 要求 fixture 下参数/技能改动本地重算）。
      fixFlipParams = params;
      return;
    }
    await call("set_flip_params", { params });
  },

  /** 单笔试算：生产入口的费率公式只有 Rust 一份（spec R6）；预览走 fixture 镜像。 */
  async trialCalc(buyPrice: number, sellPrice: number, qty: number): Promise<TrialOut> {
    if (!inTauri) {
      // 与 Rust 命令层同一防护口径：坏输入直接报错，不渲染成"绿色 0 收益"。
      if (!(qty > 0)) throw new Error("数量必须大于 0");
      if (!(buyPrice > 0) || !(sellPrice > 0)) throw new Error("买价/卖价必须是大于 0 的数字");
      return fixSettle(buyPrice, sellPrice, qty);
    }
    return call<TrialOut>("trial_calc", { buyPrice, sellPrice, qty });
  },

  // ---- M4c 提醒中心 ----

  /** 告警列表（含已清的行：闸门只管推不推，从不删行）。 */
  async alertsList(): Promise<AlertRow[]> {
    if (!inTauri) return FIX_ALERTS;
    return call<AlertRow[]>("alerts_list");
  },

  /** 通道配置回显（webhook 已打码，密钥只有"是否已配置"）。 */
  async alertSettingsGet(): Promise<AlertSettings> {
    if (!inTauri) return fixSettings;
    return call<AlertSettings>("alert_settings_get");
  },

  /** 改通道配置。返回**重新打过码**的回显，面板用它刷新（而不是拿用户输入自己拼）。 */
  async alertSettingsSet(input: AlertSettingsIn): Promise<AlertSettings> {
    if (!inTauri) return fixSaveSettings(input);
    return call<AlertSettings>("alert_settings_set", { input });
  },

  /** SSO 挂链状态（只含派生事实，没有任何令牌文本）。 */
  async ssoStatus(): Promise<SsoStatus> {
    if (!inTauri) return fixSsoStatus();
    return call<SsoStatus>("sso_status");
  },

  /** 退出登录：清系统凭据库里的令牌（库内角色数据与告警表不动）。 */
  async ssoLogout(): Promise<void> {
    if (!inTauri) {
      fixSsoLinked = false;
      return;
    }
    await call("sso_logout");
  },

  /** 发起 SSO 登录：开系统浏览器等回调（最长 180 s），回执里只有角色身份。 */
  async ssoLogin(): Promise<SsoLoginOut> {
    if (!inTauri) {
      // 预览不会真去开浏览器：把内存里的状态翻成"已登录"，让两种形态都能走查。
      fixSsoLinked = true;
      return { char_id: FIX_CHAR_ID, name: "Pilot One" };
    }
    return call<SsoLoginOut>("sso_login");
  },
};

// ---------------------------------------------------------------------------
// 浏览器开发用的假数据。数值取自 2026-09-23 真机 CLI 输出，形状与后端一致。
// ---------------------------------------------------------------------------

function g(group_id: number, name: string, type_count: number): TreeGroup {
  return { group_id, name, type_count };
}

function fixTree(): TreeNode[] {
  // 树里的类型数 = fixListing 真能摆出来的行数：生产已把 tree 计数改成与
  // 列表同源（inv_types），fixture 不同步就会继续演"16 vs 9"的口径矛盾。
  const n = 9;
  return [
    {
      category_id: 10,
      name: "Material Elements",
      groups: [g(18, "Noble Metals", n), g(25, "Base Minerals", n), g(489, "Ore", n)],
    },
    {
      category_id: 11,
      name: "Commodities",
      groups: [g(420, "Component", n), g(563, "Capacitor Boosters", n)],
    },
    {
      category_id: 18,
      name: "Ship",
      groups: [g(301, "Frigate", n), g(302, "Destroyer", n), g(420, "Battlecruiser", n)],
    },
  ];
}

function fixListing(groupId: number): ListingRow[] {
  const base: Array<[number, string, number | null, number, number | null, number, number, number]> = [
    [34, "Tritanium", 3.8, 1_240_000_000, 3.94, 890_000_000, 30, 24],
    [35, "Pyerite", 4.35, 210_000_000, 4.52, 150_000_000, 18, 22],
    [36, "Isogen", 17.04, 44_000_000, 17.33, 39_000_000, 37, 114],
    [37, "Noxium", 54.09, 12_000_000, 55.1, 9_400_000, 29, 59],
    [38, "Zydrine", 646.2, 1_900_000, 687.0, 1_100_000, 32, 62],
    [39, "Megacyte", 4_102, 240_000, 4_236, 180_000, 24, 41],
    [40, "Zopicom", null, 0, 12_500, 60, 0, 3],
    [88087, "Eleutrium", 5.03, 8_800_000, 9.85, 2_100_000, 9, 16],
    [27029, "Chalcopyrite", null, 0, null, 0, 0, 0],
  ];
  // 按组轮转入参行：切组时列表真的一模一样，走查时根本看不出"刷新没生效"。
  // 用 groupId 做旋转量，假数据至少能分辨"换组 → 换行"这条链路。
  const off = groupId % base.length;
  const rotated = base.map((_, i) => base[(i + off) % base.length]!);
  const rows = groupId === 0 ? base.slice(0, 4) : rotated;
  return rows.map(([type_id, name, best_bid, bid_qty, best_ask, ask_qty, bl, al]) => ({
    type_id,
    name,
    best_bid,
    bid_qty,
    best_ask,
    ask_qty,
    bid_levels: bl,
    ask_levels: al,
  }));
}

function fixDetail(typeId: number): TypeDetail {
  // fixture 也得尊重"零挂单类型没有盘"：以前任何 typeId 都回 Tritanium 的满档，
  // 把"选中 Chalcopyrite 右栏却挂着满阶梯"的渲染未刷新疑云永久掩盖了。
  const empty = typeId === 27029 || typeId === 40;
  const row = fixListing(18).find((r) => r.type_id === typeId);
  return {
    type_id: typeId,
    name: row?.name ?? "未知类型",
    location_id: 60003760,
    location_name: "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
    bid_depth: empty
      ? []
      : [
          { price: 3.8, volume: 12_400_000, orders: 4 },
          { price: 3.79, volume: 8_100_000, orders: 7 },
          { price: 3.78, volume: 5_200_000, orders: 3 },
        ],
    ask_depth: empty
      ? []
      : [
          { price: 3.94, volume: 9_800_000, orders: 6 },
          { price: 3.95, volume: 14_200_000, orders: 11 },
          { price: 3.96, volume: 3_100_000, orders: 2 },
        ],
    bid_levels: empty ? 0 : 30,
    ask_levels: empty ? 0 : 24,
    skipped_stale: empty ? 0 : 12,
    skipped_thin: empty ? 0 : 41,
    skipped_wholesale: 0,
    snapshot_lm: "Wed, 23 Sep 2026 15:37:42 GMT",
    updated_at: Math.floor(Date.now() / 1000) - 90,
  };
}

const FIX_T0 = Date.now();

function fixStatus(): AppStatus {
  // 倒计时必须是动态的：常数 243s 让"冻结的环"看起来像真 bug（走查 #13 即此）。
  // 按页龄回落到 6 分钟循环，和真采集者的节拍同形。
  const leftMs = 360_000 - ((Date.now() - FIX_T0) % 360_000);
  return {
    round: 12,
    stage: "WaitingNext",
    last_seconds: 68.4,
    orders: 408_739,
    rows_written: 16_686,
    hubs: 20,
    next_in_ms: leftMs,
    snapshot_lm: "Wed, 23 Sep 2026 15:37:42 GMT",
    remaining_tokens: 9_481,
    jita_rows: 13_463,
    tree: [614, 15_164, 48],
    collecting: true,
  };
}

// 近 60 天的递增游走：形状与 A.4 实测口径一致（average/highest/lowest/volume）。
// 浏览器里能拉到 candlestick + dataZoom 就验收了图表链路，不拿它当行情。
function fixHistory(typeId: number): HistoryBar[] {
  const base = typeId === 34 ? 3.8 : typeId === 35 ? 4.35 : typeId === 36 ? 17.0 : 500;
  const out: HistoryBar[] = [];
  const today = new Date();
  for (let i = 59; i >= 0; i--) {
    const d = new Date(today);
    d.setUTCDate(d.getUTCDate() - i);
    const avg = Number((base * (1 + Math.sin(i / 7) * 0.05 + (59 - i) * 0.0006)).toFixed(3));
    out.push({
      date: d.toISOString().slice(0, 10),
      average: avg,
      highest: Number((avg * 1.03).toFixed(3)),
      lowest: Number((avg * 0.97).toFixed(3)),
      volume: Math.round(1e9 + ((i * 37) % 50) * 1e7),
      order_count: 1000 + (i % 9) * 120,
    });
  }
  return out;
}

function fixHubs(): Hub[] {
  return [
    { location_id: 60003760, order_count: 330_210, share_pct: 80.97, rank: 1, name: "Jita IV - Moon 4" },
    { location_id: 60015157, order_count: 5_492, share_pct: 1.35, rank: 2, name: "Kisogo VII - AIR Laboratories" },
    { location_id: 60015027, order_count: 1_750, share_pct: 0.43, rank: 3, name: "Uitra VI - Moon 4" },
  ];
}

// ---- M4a 倒卖引擎的演示数据 ------------------------------------------------
// 预览按 spec §3.3 做本地重算：参数/技能改动保存后，fixFlip/试算立即用下面的
// 费率镜像重算，角标与行列数字随新参数变化。公式单源仍在 Rust（spec R6）；
// 镜像锚点对齐 emd-core::market::flip 的单测（7.5%/0 技能 → 0.075；BR 每级
// −0.3pp、地板 min(1%, 基率)），若两边漂移，以 Rust 侧为准。
let fixFlipParams: FlipParams = {
  fees: { sales_tax_pct: 7.5, broker_pct: 3.0, accounting: 0, broker_relations: 0, faction_standing: 0, corp_standing: 0 },
  margin_threshold_pct: 3.0,
  capital_isk: 100_000_000,
  capital_pct_per_trade: 5.0,
  min_batch: 100,
  freight_isk_per_unit: 0,
  include_buy_broker: false,
};

/** FeeModel::effective_sales_tax 的镜像：Accounting 每级相对 −11%，下界 0。 */
const fixEffectiveTax = (f: FeeModel): number =>
  Math.max((f.sales_tax_pct / 100) * (1 - 0.11 * Math.min(Math.max(f.accounting, 0), 5)), 0);

/** FeeModel::effective_broker 的镜像：每级绝对 −0.3pp；地板 min(1%, 基率)。 */
const fixEffectiveBroker = (f: FeeModel): number => {
  const base = f.broker_pct / 100;
  const floor = Math.min(base, 0.01);
  return Math.max(
    base -
      0.003 * Math.min(Math.max(f.broker_relations, 0), 5) -
      0.0003 * Math.max(f.faction_standing, 0) -
      0.0002 * Math.max(f.corp_standing, 0),
    floor,
  );
};

/** flip::settle 的镜像：税基是卖出全额；预览里机会行与试算共用这一份。 */
function fixSettle(buy: number, sell: number, qty: number): TrialOut {
  const broker = fixEffectiveBroker(fixFlipParams.fees);
  const tax = fixEffectiveTax(fixFlipParams.fees);
  const netSell = sell * qty * (1 - broker - tax);
  let cost = buy * qty + fixFlipParams.freight_isk_per_unit * qty;
  if (fixFlipParams.include_buy_broker) cost += buy * qty * broker;
  if (cost <= 0) return { net_per_unit: 0, net_total: 0, margin_pct: 0 };
  const net = netSell - cost;
  return { net_per_unit: net / qty, net_total: net, margin_pct: (net / cost) * 100 };
}

/**
 * 演示快照：只有价/量是冻着的常数（模拟一份采集结果），净利数字一律现算。
 * 固定 4 行、不模拟引擎的机会筛选（min_batch/资金/阈值）——预览看的是
 * "改参数 → 角标与数字即时变化"；筛选行为以真机为准。
 */
const FIX_ROWS: Array<Omit<FlipRow, "net_per_unit" | "net_total" | "margin_pct">> = [
  {
    type_id: 36, type_name: "Isogen",
    buy_loc: 60003760, buy_loc_name: "Jita IV - Moon 4",
    sell_loc: 60015027, sell_loc_name: "Uitra VI - Moon 4",
    buy_price: 17.04, sell_price: 19.9, qty: 120_000,
    vol24: 44_000_000, vol_source: "history", buy_levels: 37, sell_levels: 9,
    xregion_age_secs: null,
  },
  {
    type_id: 34, type_name: "Tritanium",
    buy_loc: 60003760, buy_loc_name: "Jita IV - Moon 4",
    sell_loc: 60015157, sell_loc_name: "Kisogo VII - AIR Laboratories",
    buy_price: 3.94, sell_price: 4.55, qty: 2_000_000,
    vol24: 1_240_000_000, vol_source: "history", buy_levels: 24, sell_levels: 11,
    xregion_age_secs: null,
  },
  {
    type_id: 88087, type_name: "Eleutrium",
    buy_loc: 60003760, buy_loc_name: "Jita IV - Moon 4",
    sell_loc: 60015157, sell_loc_name: "Kisogo VII - AIR Laboratories",
    buy_price: 9.85, sell_price: 9.2, qty: 2_100,
    vol24: 2_100, vol_source: "depth", buy_levels: 16, sell_levels: 3,
    xregion_age_secs: null,
  },
  {
    // 跨区示例：Amarr 站数据来自上一批 T1.5，前端渲染 [跨区 5 分钟前] 角标。
    // 卖价 4.60 在默认 7.5%+3% 费率下净利 ≈ +4.5%，与"机会"语义一致（不是亏损）。
    type_id: 34, type_name: "Tritanium",
    buy_loc: 60003760, buy_loc_name: "Jita IV - Moon 4",
    sell_loc: 60008494, sell_loc_name: "Amarr VIII (Oris) - Emperor Family Academy",
    buy_price: 3.94, sell_price: 4.60, qty: 380_000,
    vol24: 1_240_000_000, vol_source: "history", buy_levels: 24, sell_levels: 8,
    xregion_age_secs: 305,
  },
];

function fixFlip(): FlipScan {
  const rows: FlipRow[] = FIX_ROWS.map((r) => ({
    ...r,
    ...fixSettle(r.buy_price, r.sell_price, r.qty),
  }));
  return {
    rows,
    pairs_evaluated: 268,
    dropped_batch: 231,
    dropped_shortfall: 0,
    dropped_threshold: 34,
    age_secs: 95,
    params: fixFlipParams,
    effective_sales_tax_pct: fixEffectiveTax(fixFlipParams.fees) * 100,
    effective_broker_pct: fixEffectiveBroker(fixFlipParams.fees) * 100,
  };
}

// ---- M4c 提醒中心的演示数据 ------------------------------------------------
// 三条告警覆盖三种形态（expected_sell_loss / buy_order_trap / realized_loss），状态也各占一个
//（notified / new / cleared）—— 走查时角标、通知史、口径摘要三块都能在屏幕上被看见，
// 不用改代码去凑。数值取自 emd-core 的判定单测锚点：① 挂价 97 对 FIFO 成本 95（A5 口径）；
// ② 买 100@100、可执行净额 95.125/件；③ journal 真值 9000 − 300 − 10000 − 270 = −1570。
const FIX_CHAR_ID = 90_000_001;
const FIX_ALERT_T0 = Math.floor(Date.now() / 1000);
const ISO_DAY = (offsetDays: number) =>
  new Date((FIX_ALERT_T0 + offsetDays * 86_400) * 1000).toISOString().slice(0, 10);

function fixAlertPayload(p: AlertPayload): string {
  // payload 列存的就是这段 JSON（推送卡片与提醒中心共用同一份序列化，spec §4.4）：
  // 口径摘要在界面上是**从它里面读的**，不是后端另发的第二份结构。
  return JSON.stringify(p);
}

const FIX_ALERTS: AlertRow[] = [
  {
    // ① 已推送的挂卖单预期亏（通知史里记的是亏损率，不是金额）
    alert_key: "order:7001",
    kind: "expected_sell_loss",
    char_id: FIX_CHAR_ID,
    type_id: 34,
    type_name: "Tritanium",
    location_id: 60003760,
    location_name: "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
    is_buy: false,
    first_seen_at: FIX_ALERT_T0 - 5400,
    last_seen_at: FIX_ALERT_T0 - 120,
    last_loss_isk: 127.375,
    last_margin_pct: -1.34,
    state: "notified",
    notified_at: FIX_ALERT_T0 - 300,
    notified_day: ISO_DAY(0),
    notified_count_day: 2,
    last_notified_margin_pct: -1.34,
    payload: fixAlertPayload({
      alert_key: "order:7001",
      kind: "expected_sell_loss",
      order_id: 7001,
      type_id: 34,
      type_name: "Tritanium",
      location_id: 60003760,
      location_name: "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
      is_buy: false,
      price: 97,
      volume: 100,
      at: new Date((FIX_ALERT_T0 - 5400) * 1000).toISOString(),
      loss_isk: 127.375,
      margin_pct: -1.34,
      caliber: {
        track: "预期·估算费率",
        sales_tax_pct: 3.375,
        broker_pct: 0,
        skill_caliber: "Accounting 5 / Broker Relations 0（税率随面板；中介费取 journal 实付）",
        unit_cost: 95,
        cost_source: "FIFO 90 天",
        formula:
          "① 单位净额 93.726250 = 挂价 97.000000 × (1 − 有效税 3.3750%)；单位全成本 95.000000 = FIFO 均价 95.000000 + 实付中介费/单位 0.000000",
        data_age_secs: 60,
      },
    }),
  },
  {
    // ② 挂买单套牢亏（未推送：状态 new）
    alert_key: "order:7002",
    kind: "buy_order_trap",
    char_id: FIX_CHAR_ID,
    type_id: 34,
    type_name: "Tritanium",
    location_id: 60003760,
    location_name: "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
    is_buy: true,
    first_seen_at: FIX_ALERT_T0 - 600,
    last_seen_at: FIX_ALERT_T0 - 60,
    last_loss_isk: 975,
    last_margin_pct: -4.875,
    state: "new",
    notified_at: null,
    notified_day: null,
    notified_count_day: 0,
    last_notified_margin_pct: null,
    payload: fixAlertPayload({
      alert_key: "order:7002",
      kind: "buy_order_trap",
      order_id: 7002,
      type_id: 34,
      type_name: "Tritanium",
      location_id: 60003760,
      location_name: "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
      is_buy: true,
      price: 100,
      volume: 200,
      at: new Date((FIX_ALERT_T0 - 600) * 1000).toISOString(),
      loss_isk: 975,
      margin_pct: -4.875,
      caliber: {
        track: "预期·估算费率",
        sales_tax_pct: 3.375,
        broker_pct: 1.5,
        skill_caliber: "Accounting 5 / Broker Relations 5（两侧费率均随面板重算）",
        unit_cost: 100,
        cost_source: "买单成交价 + 实付中介费",
        formula:
          "② 可执行卖出净额 95.125000/件 = 加权吃单买价 100.000000 × (1 − 税 3.3750% − 中介费 1.5000%)；买入成本 100.000000/件 = 挂价 100.000000 + 实付中介费/单位 0.000000",
        data_age_secs: 45,
      },
    }),
  },
  {
    // ③ 已实现成交亏（周期已结束：状态 cleared，行与通知史都留着）
    alert_key: "tx:2",
    kind: "realized_loss",
    char_id: FIX_CHAR_ID,
    type_id: 34,
    type_name: "Tritanium",
    location_id: 60003760,
    location_name: "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
    is_buy: false,
    first_seen_at: FIX_ALERT_T0 - 604_800,
    last_seen_at: FIX_ALERT_T0 - 86_400,
    last_loss_isk: 1570,
    last_margin_pct: -15.28,
    state: "cleared",
    notified_at: FIX_ALERT_T0 - 86_400,
    notified_day: ISO_DAY(-1),
    notified_count_day: 1,
    last_notified_margin_pct: -12.5,
    payload: fixAlertPayload({
      alert_key: "tx:2",
      kind: "realized_loss",
      order_id: 555,
      type_id: 34,
      type_name: "Tritanium",
      location_id: 60003760,
      location_name: "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
      is_buy: false,
      price: 90,
      volume: 100,
      at: new Date((FIX_ALERT_T0 - 604_800) * 1000).toISOString(),
      loss_isk: 1570,
      margin_pct: -15.28,
      caliber: {
        track: "已实现·journal 真值",
        sales_tax_pct: 3.3333333333333335,
        broker_pct: 3,
        skill_caliber: "不适用（journal 真值，技能面板不影响已实现轨）",
        unit_cost: 102.7,
        cost_source: "FIFO 90 天",
        formula:
          "③（journal 真值）净额 8700.000000 = 卖出所得 9000.000000 − 实付销售税 300.000000；全成本 10270.000000 = 被消耗批次 10000.000000 + 卖出侧实付中介费 270.000000（买入侧中介费本机无法归属，未计）",
        data_age_secs: 388_800,
      },
    }),
  },
];

/**
 * 预览内存里的配置（真机上那份住在 meta KV 里）。
 * 回调地址用 `CharConfig::default()` 的那个字面量：它是"库里没写、env 也没配"时的生效值，
 * 界面上要显示"要注册成什么"时看到的就是它。
 */
const FIX_CHAR_DEFAULT_REDIRECT = "http://127.0.0.1:8765/callback";

let fixSettings: AlertSettings = {
  // 已中段打码的 webhook（形态照 `emd_core::push::mask`：头 ≤32 + `***` + 尾 ≤4）。
  webhook: "https://oapi.dingtalk.com/robot/s***9f3a",
  secret_set: true,
  enabled: true,
  channels: ["local", "dingtalk"],
  client_id: "emd-preview-client-id",
  redirect_uri: FIX_CHAR_DEFAULT_REDIRECT,
};

/**
 * 预览里的挂链状态。默认已挂链：未挂链那种形态能用「退出登录」当场走出来
 * （真机上的令牌在系统凭据库里，浏览器预览里没有任何令牌可言）。
 * 每次读都按"现在"重算时刻：写死在模块加载时刻的话，几分钟后有效期就成了一句假话。
 */
let fixSsoLinked = true;

function fixSsoStatus(): SsoStatus {
  const fresh = Math.floor(Date.now() / 1000);
  // client_id 与开关都从预览配置里派生：在设置页里清空 client_id 之后，面板该显示"同步关着"
  // —— 这两种形态都要能在浏览器里走查（真机上同样由 Rust 的分层配置算出来）。
  const configured = fixSettings.client_id.trim() !== "";
  return {
    linked: fixSsoLinked,
    char_id: fixSsoLinked ? FIX_CHAR_ID : null,
    char_name: fixSsoLinked ? "Pilot One" : null,
    last_sync_at: fixSsoLinked ? fresh - 95 : null,
    expires_at: fixSsoLinked ? fresh + 960 : null,
    token_expired: false,
    char_sync_enabled: configured,
    client_id_set: configured,
    token_error: null,
  };
}

/** `emd_core::push::mask` 的预览镜像：≥12 字符才留头尾，短串整串打码。 */
function fixMask(s: string): string {
  const chars = [...s];
  const n = chars.length;
  if (n === 0) return "";
  if (n <= 12) return "***";
  const head = Math.min(Math.floor(n / 3), 32);
  const tail = Math.min(Math.floor(n / 8), 4);
  return chars.slice(0, head).join("") + "***" + chars.slice(n - tail).join("");
}

/**
 * `PushConfig::save_editing` 的预览镜像：只有"看得见的字段"能被改，密钥走三态。
 * 公式与存储单源仍在 Rust；这里镜像的是**语义**——尤其是"打码值 = 保留原值"，
 * 走查时要能看见"只翻开关不会把密钥清掉"这条（清错了之后每条推送都 310000）。
 */
function fixSaveSettings(input: AlertSettingsIn): AlertSettings {
  const webhook = input.webhook.includes("***") ? fixSettings.webhook : fixMask(input.webhook.trim());
  const secret_set = input.secret === undefined || input.secret === null ? fixSettings.secret_set : input.secret !== "";
  // SSO 两值要镜像"空串 = 清掉库里的值、回落 env/默认"这条语义（预览里那层默认就是
  // `CharConfig::default()` 的 redirect_uri）：真机上这一层由 Rust 的 `CharConfig::load` 决定。
  const client_id = input.client_id.trim();
  const redirect_uri = input.redirect_uri.trim() || FIX_CHAR_DEFAULT_REDIRECT;
  fixSettings = {
    webhook,
    secret_set,
    enabled: input.enabled,
    channels: input.enabled && webhook.trim() !== "" ? ["local", "dingtalk"] : ["local"],
    client_id,
    redirect_uri,
  };
  return fixSettings;
}
