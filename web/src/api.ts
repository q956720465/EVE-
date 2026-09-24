import type { AppStatus, FeeModel, FlipParams, FlipRow, FlipScan, HistoryBar, Hub, ListingRow, TrialOut, TreeGroup, TreeNode, TypeDetail } from "./types";

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
 * 固定 3 行、不模拟引擎的机会筛选（min_batch/资金/阈值）——预览看的是
 * "改参数 → 角标与数字即时变化"；筛选行为以真机为准。
 */
const FIX_ROWS: Array<Omit<FlipRow, "net_per_unit" | "net_total" | "margin_pct">> = [
  {
    type_id: 36, type_name: "Isogen",
    buy_loc: 60003760, buy_loc_name: "Jita IV - Moon 4",
    sell_loc: 60015027, sell_loc_name: "Uitra VI - Moon 4",
    buy_price: 17.04, sell_price: 19.9, qty: 120_000,
    vol24: 44_000_000, vol_source: "history", buy_levels: 37, sell_levels: 9,
  },
  {
    type_id: 34, type_name: "Tritanium",
    buy_loc: 60003760, buy_loc_name: "Jita IV - Moon 4",
    sell_loc: 60015157, sell_loc_name: "Kisogo VII - AIR Laboratories",
    buy_price: 3.94, sell_price: 4.55, qty: 2_000_000,
    vol24: 1_240_000_000, vol_source: "history", buy_levels: 24, sell_levels: 11,
  },
  {
    type_id: 88087, type_name: "Eleutrium",
    buy_loc: 60003760, buy_loc_name: "Jita IV - Moon 4",
    sell_loc: 60015157, sell_loc_name: "Kisogo VII - AIR Laboratories",
    buy_price: 9.85, sell_price: 9.2, qty: 2_100,
    vol24: 2_100, vol_source: "depth", buy_levels: 16, sell_levels: 3,
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
