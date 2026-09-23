import type { AppStatus, HistoryBar, Hub, ListingRow, TreeGroup, TreeNode, TypeDetail } from "./types";

/**
 * 与 Rust 后端的唯一边界。
 *
 * 浏览器里直接 `npm run dev`（没有 Tauri 壳）时走 `fixtures`，只为看布局与联调交互；
 * 一旦运行在 WebView2 里就一定走真实 IPC —— 两者不能混，否则会把假数据当行情看。
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
};

// ---------------------------------------------------------------------------
// 浏览器开发用的假数据。数值取自 2026-09-23 真机 CLI 输出，形状与后端一致。
// ---------------------------------------------------------------------------

function g(group_id: number, name: string, type_count: number): TreeGroup {
  return { group_id, name, type_count };
}

function fixTree(): TreeNode[] {
  return [
    {
      category_id: 10,
      name: "Material Elements",
      groups: [g(18, "Noble Metals", 16), g(25, "Base Minerals", 38), g(489, "Ore", 24)],
    },
    {
      category_id: 11,
      name: "Commodities",
      groups: [g(420, "Component", 260), g(563, "Capacitor Boosters", 41)],
    },
    {
      category_id: 18,
      name: "Ship",
      groups: [g(301, "Frigate", 420), g(302, "Destroyer", 280), g(420, "Battlecruiser", 210)],
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
  const rows = groupId === 0 ? base.slice(0, 4) : base;
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
  return {
    type_id: typeId,
    name: fixListing(18).find((r) => r.type_id === typeId)?.name ?? "未知类型",
    location_id: 60003760,
    location_name: "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
    bid_depth: [
      { price: 3.8, volume: 12_400_000, orders: 4 },
      { price: 3.79, volume: 8_100_000, orders: 7 },
      { price: 3.78, volume: 5_200_000, orders: 3 },
    ],
    ask_depth: [
      { price: 3.94, volume: 9_800_000, orders: 6 },
      { price: 3.95, volume: 14_200_000, orders: 11 },
      { price: 3.96, volume: 3_100_000, orders: 2 },
    ],
    bid_levels: 30,
    ask_levels: 24,
    skipped_stale: 12,
    skipped_thin: 41,
    skipped_wholesale: 0,
    snapshot_lm: "Wed, 23 Sep 2026 15:37:42 GMT",
    updated_at: Math.floor(Date.now() / 1000) - 90,
  };
}

function fixStatus(): AppStatus {
  return {
    round: 12,
    stage: "WaitingNext",
    last_seconds: 68.4,
    orders: 408_739,
    rows_written: 16_686,
    hubs: 20,
    next_in_ms: 243_000,
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
