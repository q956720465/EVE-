import { create } from "zustand";
import { api } from "./api";
import type { AppStatus, Hub, ListingRow, TreeNode, TypeDetail } from "./types";

/** 吉他。主视图默认站点，切换枢纽时改这里。 */
export const JITA = 60003760;

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
  query: string;
  searchResults: ListingRow[];
  busy: boolean;
  error: string | null;

  boot(): Promise<void>;
  toggle(categoryId: number): void;
  selectGroup(groupId: number, name: string): Promise<void>;
  selectLocation(locationId: number): Promise<void>;
  selectType(typeId: number): Promise<void>;
  search(word: string): Promise<void>;
  tick(): Promise<void>;
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
  query: "",
  searchResults: [],
  busy: false,
  error: null,

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
    const cur = get().expanded[categoryId] ?? true;
    set({ expanded: { ...get().expanded, [categoryId]: !cur } });
  },

  async selectGroup(groupId, name) {
    set({ groupId, groupName: name, rows: [], selected: null, detail: null, query: "", searchResults: [] });
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
    set({ selected: typeId, detail: null });
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
}));
