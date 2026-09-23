import { useEffect, useRef, useState } from "react";
import * as echarts from "echarts/core";
import { BarChart, CandlestickChart } from "echarts/charts";
import { DataZoomComponent, GridComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { api } from "../api";
import { DOWN, UP } from "../format";
import type { HistoryBar } from "../types";

// 按需注册：整包 echarts 会把体积翻几倍，这里只挂蜡烛图这条链路用到的部件。
echarts.use([BarChart, CandlestickChart, DataZoomComponent, GridComponent, TooltipComponent, CanvasRenderer]);

// 上游滚动窗口实测只有 418 天（附录 A.4），"1Y" 就取满它，别装作有更长。
const RANGES = [
  { key: "1W", days: 7 },
  { key: "1M", days: 30 },
  { key: "3M", days: 90 },
  { key: "1Y", days: 418 },
] as const;

type RangeKey = (typeof RANGES)[number]["key"];

/**
 * 单类型日线蜡烛图。ESI 的 history 只有 `(region, type)` 维度、没有站点维度，
 * 所以这张图永远是"该星域均价的时间序列"，不能拿来比较站点（方案 §3.3）。
 */
export default function HistoryChart({ typeId }: { typeId: number }) {
  const [box, setBox] = useState<HTMLDivElement | null>(null);
  const chart = useRef<echarts.EChartsType | null>(null);
  // bars 与 loading 是一个原子状态：分两个 useState 时若"先进 bars 再进 loading"，
  // 那一帧里图容器还没挂载，绘图 effect 拿不到 DOM —— 浏览器验收里真实炸过的坑。
  const [data, setData] = useState<{ bars: HistoryBar[]; loading: boolean }>({
    bars: [],
    loading: true,
  });
  const [range, setRange] = useState<RangeKey>("1M");
  const { bars, loading } = data;

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
    // 实例只建一次；切区间走 setOption 增量更新，重建会把 dataZoom 的用户位置抹掉。
    chart.current = chart.current ?? echarts.init(box);
    // 换类型时先 clear：否则"读取历史…"会盖在上一个类型的旧图上，瞬间读起来
    // 像同一份数据。
    if (loading) {
      chart.current.clear();
      return;
    }

    const days = RANGES.find((r) => r.key === range)?.days ?? 30;
    const from = new Date();
    from.setUTCDate(from.getUTCDate() - days);
    const fromStr = from.toISOString().slice(0, 10);
    const rows = bars.filter((b) => b.date >= fromStr && b.average !== null);

    const cats = rows.map((b) => b.date);
    // ECharts 蜡烛的数据序是 [open, close, lowest, highest]；
    // 日线没有 OHLC，按方案 §6：open = 昨日 average，close = 当日 average。
    const kline = rows.map((b, i) => [
      rows[i - 1]?.average ?? b.average ?? 0,
      b.average ?? 0,
      b.lowest ?? b.average ?? 0,
      b.highest ?? b.average ?? 0,
    ]);
    const vols = rows.map((b) => b.volume);

    chart.current.setOption({
      animationDuration: 300,
      tooltip: { trigger: "axis", axisPointer: { type: "cross" } },
      grid: [
        { left: 56, right: 16, top: 10, height: "58%" },
        { left: 56, right: 16, top: "74%", height: "14%" },
      ],
      xAxis: [
        { type: "category", data: cats, boundaryGap: true },
        { type: "category", gridIndex: 1, data: cats, axisLabel: { show: false }, axisTick: { show: false } },
      ],
      yAxis: [
        { scale: true, splitLine: { lineStyle: { opacity: 0.25 } } },
        { gridIndex: 1, axisLabel: { show: false }, splitLine: { show: false } },
      ],
      dataZoom: [
        { type: "inside", xAxisIndex: [0, 1] },
        { type: "slider", xAxisIndex: [0, 1], bottom: 0, height: 16 },
      ],
      series: [
        {
          type: "candlestick",
          data: kline,
          itemStyle: { color: UP, color0: DOWN, borderColor: UP, borderColor0: DOWN },
        },
        { type: "bar", xAxisIndex: 1, yAxisIndex: 1, data: vols, itemStyle: { opacity: 0.45 } },
      ],
    });
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

  return (
    <div className="hist">
      <div className="hist-tabs">
        {/* 分钟线数据源（ticker_intraday）不在 M3 范围，按钮置灰而不是藏掉：
            藏了用户会以为 1D 是坏了，置灰配 tooltip 才说得清"这档还没有数据"。 */}
        <button disabled title="分钟线（ticker_intraday）后续里程碑再开">
          1D
        </button>
        {RANGES.map((r) => (
          <button
            key={r.key}
            className={range === r.key ? "on" : ""}
            onClick={() => setRange(r.key)}
          >
            {r.key}
          </button>
        ))}
        <span className="cov" title="按该类型实际积累天数计，不是全局均值（方案 §3.3）">
          本地已积累 {bars.length} 天
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
        <div ref={setBox} style={{ width: "100%", height: 340 }} />
      )}
      <div className="note">
        历史是 (星域, 类型) 维度、无站点维度：图上是 The Forge 的日均价，跨站对比只有当前快照。
      </div>
    </div>
  );
}
