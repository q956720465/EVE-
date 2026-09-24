import { create } from "zustand";
import { api } from "./api";
import type { AppStatus, FlipParams, FlipScan, FlipSortKey, Hub, ListingRow, TrialOut, TreeNode, TypeDetail } from "./types";

/** 吉他。主视图默认站点，切换枢纽时改这里。 */
export const JITA = 60003760;

/** 顶层视图：市场浏览（三栏）与倒卖扫描（全宽）。 */
export type View = "market" | "flip";

interface State {
  tree: TreeNode[];
  hubs: Hub[];
  status: AppStatus | null;
  expanded: Record<number, boolean>;
  groupId: number | null;
  groupName: string;
  locationId: number;
  rows: ListingRow[];
  selected: number | null;
  detail: TypeDetail | null;
  /** detail 对应哪个 type。null 详情 + 匹配 id = "真没有盘"，不匹配 = 还在读。 */
  detailFor: number | null;
  query: string;
  searchResults: ListingRow[];
  busy: boolean;
  error: string | null;

  view: View;
  flip: FlipScan | null;
  flipSort: FlipSortKey;
  flipBusy: boolean;
  trial: TrialOut | null;

  boot(): Promise<void>;
  toggle(categoryId: number): void;
  selectGroup(groupId: number, name: string): Promise<void>;
  selectLocation(locationId: number): Promise<void>;
  selectType(typeId: number): Promise<void>;
  search(word: string): Promise<void>;
  tick(): Promise<void>;
  setView(view: View): void;
  loadFlip(): Promise<void>;
  /** 返回是否保存成功——失败时调用方不得清 dirty（否则静默回滚用户输入）。 */
  saveFlipParams(p: FlipParams): Promise<boolean>;
  setFlipSort(k: FlipSortKey): void;
  runTrial(buy: number, sell: number, qty: number): Promise<void>;
}

export const useStore = create<State>((set, get) => ({
  tree: [],
  hubs: [],
  status: null,
  expanded: {},
  groupId: null,
  groupName: "",
  locationId: JITA,
  rows: [],
  selected: null,
  detail: null,
  detailFor: null,
  query: "",
  searchResults: [],
  busy: false,
  error: null,

  view: "market",
  flip: null,
  flipSort: "score",
  flipBusy: false,
  trial: null,

  async boot() {
    set({ busy: true, error: null });
    try {
      const [tree, hubs, status] = await Promise.all([api.tree(), api.hubs(), api.status()]);
      set({ tree, hubs, status, busy: false });
      // 树还没建就直接进入列表是空的，这里给出明确的下一步指示而不是空白页。
      if (tree.length === 0) {
        set({ error: "分类树还没建好：壳启动时会自动建一次（约 80 秒），建完重载即可" });
        return;
      }
      if (get().groupId === null) {
        const first = tree[0]?.groups[0];
        if (first) await get().selectGroup(first.group_id, first.name);
      }
    } catch (e) {
      set({ busy: false, error: String(e) });
    }
  },

  toggle(categoryId) {
    // 默认值必须和 CatalogTree 的 `?? false` 同一口径：之前这里缺省当"已展开"，
    // selectGroup 的首次自动 toggle 反而把分类写成收起（复验 F：首屏全收起）。
    const cur = get().expanded[categoryId] ?? false;
    set({ expanded: { ...get().expanded, [categoryId]: !cur } });
  },

  async selectGroup(groupId, name) {
    set({ groupId, groupName: name, rows: [], selected: null, detail: null, detailFor: null, query: "", searchResults: [] });
    // 选中哪个组，它所属的分类就自动展开 —— 否则首屏列表已经摆出来了，
    // 左栏三棵树还全是收起的，看不出当前视图挂在哪里（走查 #7）。
    const cat = get().tree.find((t) => t.groups.some((g) => g.group_id === groupId));
    if (cat && !get().expanded[cat.category_id]) get().toggle(cat.category_id);
    try {
      const rows = await api.listing(groupId, get().locationId);
      set({ rows });
    } catch (e) {
      set({ error: String(e) });
    }
  },

  async selectLocation(locationId) {
    set({ locationId });
    const { groupId, groupName } = get();
    if (groupId !== null) await get().selectGroup(groupId, groupName);
    if (get().selected !== null) await get().selectType(get().selected as number);
  },

  async selectType(typeId) {
    set({ selected: typeId, detail: null, detailFor: typeId });
    try {
      const detail = await api.detail(typeId, get().locationId);
      set({ detail });
    } catch (e) {
      set({ error: String(e) });
    }
  },

  async search(word) {
    set({ query: word });
    if (word.trim().length < 2) {
      set({ searchResults: [] });
      return;
    }
    try {
      set({ searchResults: await api.search(word.trim()) });
    } catch (e) {
      set({ error: String(e) });
    }
  },

  async tick() {
    try {
      const status = await api.status();
      set({ status });
      // 冷启动时壳会自己建一次分类树（实测 78 秒）。建完必须自己长出来 ——
      // 用户不知道它在建，让人手动重载等于把进度条变成谜题。
      if (status.tree[0] > 0 && get().tree.length === 0) await get().boot();
    } catch {
      // 状态轮询失败不值得打断视线；下一次会再来。
    }
  },

  setView(view) {
    set({ view });
    if (view === "flip" && get().flip === null) void get().loadFlip();
  },

  async loadFlip() {
    set({ flipBusy: true });
    try {
      set({ flip: await api.flipScan(), flipBusy: false });
    } catch (e) {
      set({ flipBusy: false, error: String(e) });
    }
  },

  async saveFlipParams(p) {
    try {
      await api.setFlipParams(p);
      // 参数/技能改完立即重扫：纯本地纯函数，零 ESI 请求（spec §3.1 的重算闭环）。
      await get().loadFlip();
      return true;
    } catch (e) {
      // 不能把错误吞掉后假装成功：调用方要靠返回值决定是否清 dirty。
      set({ error: String(e) });
      return false;
    }
  },

  setFlipSort(flipSort) {
    set({ flipSort });
  },

  async runTrial(buy, sell, qty) {
    try {
      set({ trial: await api.trialCalc(buy, sell, qty) });
    } catch (e) {
      set({ trial: null, error: String(e) });
    }
  },
}));
