import { useEffect, useRef, useState } from "react";
import * as echarts from "echarts/core";
import { BarChart } from "echarts/charts";
import { DataZoomComponent, GridComponent, LegendComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { api } from "../api";
import { DOWN, fmtPrice, fmtVol, UP } from "../format";
import type { HistoryBar } from "../types";

// 按需注册：整包 echarts 会把体积翻几倍，这里只挂柱状图这条链路用到的部件。
// LegendComponent 不能省：树摇构建下没注册的组件配置会被静默丢弃（复验抓到过 legend 写了不渲染）。
echarts.use([BarChart, DataZoomComponent, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer]);

// 区间就三档，中文直书。"年"取上游滚动窗口实测上限 418 天（附录 A.4），不装作更长。
const RANGES = [
  { key: "日", label: "近几天", days: 7 },
  { key: "月", label: "近一月", days: 30 },
  { key: "年", label: "近一年", days: 418 },
] as const;

/**
 * 单类型日线柱状图（用户指定：不要蜡烛、不要滑块，只要蓝/红柱子）。
 * 配色沿用全站买卖口径：蓝 = 当天买一（卖出目标价），红 = 当天卖一（买入成本）。
 * ESI 的 history 只有 `(region, type)` 维度、无站点维度（方案 §3.3），
 * 图上永远是"该星域日均价的时间序列"，不能拿来比较站点。
 */
export default function HistoryChart({ typeId }: { typeId: number }) {
  // callback ref：容器随"有无数据"分支挂载/卸载，useRef 拿不到重挂后的节点。
  const [box, setBox] = useState<HTMLDivElement | null>(null);
  const chart = useRef<echarts.EChartsType | null>(null);
  // bars 与 loading 是一个原子状态：分开 set 会出现"数据到了但容器还没挂载、
  // 绘图 effect 拿不到 DOM"的帧 —— 浏览器验收里真实炸过的坑。
  const [data, setData] = useState<{ bars: HistoryBar[]; loading: boolean }>({
    bars: [],
    loading: true,
  });
  const [range, setRange] = useState<(typeof RANGES)[number]>(RANGES[2]);
  const [name, setName] = useState("");
  const { bars, loading } = data;

  // 抬头：切类型后光看图不知道该图属于谁（单簿有抬头，历史也得有）。
  useEffect(() => {
    void api
      .detail(typeId, 60003760)
      .then((d) => setName(d?.name ?? ""))
      .catch(() => setName(""));
  }, [typeId]);

  useEffect(() => {
    let live = true;
    setData({ bars: [], loading: true });
    void (async () => {
      let b: HistoryBar[] = [];
      try {
        b = await api.history(typeId);
      } catch {
        b = [];
      }
      if (live) setData({ bars: b, loading: false });
    })();
    return () => {
      live = false;
    };
  }, [typeId]);

  useEffect(() => {
    if (!box) return;
    // 实例只建一次；切区间走 setOption 增量更新。
    chart.current = chart.current ?? echarts.init(box);
    // 换类型进入 loading 时先 clear：否则"读取历史…"会盖在上一个类型的旧图上。
    if (loading) {
      chart.current.clear();
      return;
    }

    const from = new Date();
    from.setUTCDate(from.getUTCDate() - range.days);
    const fromStr = from.toISOString().slice(0, 10);
    const rows = bars.filter((b) => b.date >= fromStr && b.average !== null);

    const cats = rows.map((b) => b.date);
    const bids = rows.map((b) => b.lowest ?? b.average ?? 0);
    const asks = rows.map((b) => b.highest ?? b.average ?? 0);

    chart.current.setOption({
      animationDuration: 300,
      legend: {
        data: ["买一", "卖一"],
        textStyle: { color: "#cfd8dd", fontSize: 11 },
        top: 0,
        right: 8,
      },
      // 默认 tooltip 是亮色主题：白底一块扣在深色三栏上 —— 统一深底中文。
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(19,26,32,0.95)",
        borderColor: "#253138",
        textStyle: { color: "#cfd8dd", fontSize: 12 },
        formatter: (ps: unknown) => {
          const list = (ps as { dataIndex: number }[]) ?? [];
          const r = list[0] ? rows[list[0].dataIndex] : undefined;
          if (!r) return "";
          return [
            r.date,
            `买一 <span style="float:right;margin-left:12px">${fmtPrice(r.lowest)}</span>`,
            `卖一 <span style="float:right;margin-left:12px">${fmtPrice(r.highest)}</span>`,
            `日均 <span style="float:right;margin-left:12px">${fmtPrice(r.average)}</span>`,
            `成交量 <span style="float:right;margin-left:12px">${fmtVol(r.volume)}</span>`,
            `挂单笔数 <span style="float:right;margin-left:12px">${r.order_count.toLocaleString()}</span>`,
          ].join("<br/>");
        },
      },
      grid: { left: 56, right: 16, top: 28, bottom: 28 },
      xAxis: { type: "category", data: cats },
      yAxis: { scale: true, splitLine: { lineStyle: { opacity: 0.25 } } },
      // 不要底部滑块（用户指定）；滚轮缩放留在 inside 里，总览/还原靠切区间按钮。
      dataZoom: [{ type: "inside", start: 0, end: 100 }],
      series: [
        {
          name: "买一",
          type: "bar",
          data: bids,
          itemStyle: { color: UP },
        },
        { name: "卖一", type: "bar", data: asks, itemStyle: { color: DOWN } },
      ],
      // 切区间行集整个换掉；不合并旧轴配置，避免残留上一个窗口的刻度。
    }, { notMerge: true });
  }, [bars, range, box, loading]);

  // 窗口/分栏宽度一变，画布不会自己重排；不监听的话图会被拉宽压扁到下次 setOption。
  useEffect(() => {
    if (!box || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(() => chart.current?.resize());
    ro.observe(box);
    return () => ro.disconnect();
  }, [box]);

  useEffect(
    () => () => {
      chart.current?.dispose();
      chart.current = null;
    },
    [],
  );

  // 索引访问在 strict 下是 |undefined，取首尾行显式落变量而不是链式下标。
  const first = bars[0];
  const last = bars[bars.length - 1];

  return (
    <div className="hist">
      <div className="hist-ttl">
        {name || `type_id ${typeId}`} · 日线
      </div>
      <div className="hist-tabs">
        {/* 区间就三档中文；旧窗口残留问题由 notMerge 全量刷新兜住。 */}
        {RANGES.map((r) => (
          <button
            key={r.key}
            className={range.key === r.key ? "on" : ""}
            title={r.label}
            onClick={() => setRange(r)}
          >
            {r.key}
          </button>
        ))}
        <span className="cov" title="按该类型自己的天数计，不是所有类型的平均值">
          {first && last
            ? `共 ${bars.length} 天：${first.date} 至 ${last.date}`
            : "共 0 天"}
        </span>
      </div>
      {loading ? (
        <div className="empty">读取历史…</div>
      ) : bars.length === 0 ? (
        <div className="empty">
          这个类型还没有本地历史：T3 每日 11:20 UTC 后自动回填（当日数据上游次日才出），
          也可用 emd history --type {typeId} 手动补一次。
        </div>
      ) : (
        <div ref={setBox} style={{ width: "100%", height: 420 }} />
      )}
      <div className="note">
        蓝柱 = 当天买一（卖出目标价）、红柱 = 当天卖一（买入成本）。历史是 (星域, 类型)
        维度、无站点维度，跨站对比只有当前快照。
      </div>
    </div>
  );
}
