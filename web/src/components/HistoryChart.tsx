import { useEffect, useRef, useState } from "react";
import * as echarts from "echarts/core";
import { BarChart, CandlestickChart } from "echarts/charts";
import { DataZoomComponent, GridComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { api } from "../api";
import { DOWN, fmtPrice, fmtVol, UP } from "../format";
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

/**
 * 单类型日线蜡烛图。ESI 的 history 只有 `(region, type)` 维度、没有站点维度，
 * 所以这张图永远是"该星域均价的时间序列"，不能拿来比较站点（方案 §3.3）。
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
  const [range, setRange] = useState<(typeof RANGES)[number]>(RANGES[1]);
  // 只有点区间按钮才 bump：触发 setOption 时把 dataZoom 复位。
  // 不复位的话，1M 下拖过的缩放窗口会残留到 1Y 上 —— 验收抓到过"1Y 只有 6 周"。
  const [zoomEpoch, setZoomEpoch] = useState(0);
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
    // 实例只建一次；切区间走 setOption 增量更新，重建会把交互态整个抹掉。
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
      // 默认 tooltip 是亮色主题：白底一块扣在深色三栏上，且给的是英文
      // open/close/lowest/highest 三位小数 —— 和全站口径全对不上。
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross", label: { show: false } },
        backgroundColor: "rgba(19,26,32,0.95)",
        borderColor: "#253138",
        textStyle: { color: "#cfd8dd", fontSize: 12 },
        formatter: (ps: unknown) => {
          const first = (ps as { dataIndex: number }[])[0];
          if (!first) return "";
          const r = rows[first.dataIndex];
          if (!r) return "";
          const open = Number(kline[first.dataIndex]?.[0] ?? r.average ?? 0);
          const up = (r.average ?? 0) >= open;
          return [
            `${r.date}${up ? " 📈" : " 📉"}`,
            `均价 <span style="float:right;margin-left:12px">${fmtPrice(r.average)}</span>`,
            `最高／最低 <span style="float:right;margin-left:12px">${fmtPrice(r.highest)} / ${fmtPrice(r.lowest)}</span>`,
            `成交量 <span style="float:right;margin-left:12px">${fmtVol(r.volume)}</span>`,
            `挂单笔数 <span style="float:right;margin-left:12px">${r.order_count.toLocaleString()}</span>`,
          ].join("<br/>");
        },
      },
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
        { type: "inside", xAxisIndex: [0, 1], start: 0, end: 100 },
        { type: "slider", xAxisIndex: [0, 1], bottom: 0, height: 16, start: 0, end: 100 },
      ],
      series: [
        {
          type: "candlestick",
          data: kline,
          itemStyle: { color: UP, color0: DOWN, borderColor: UP, borderColor0: DOWN },
        },
        { type: "bar", xAxisIndex: 1, yAxisIndex: 1, data: vols, itemStyle: { opacity: 0.45 } },
      ],
      // notMerge：区间切换时行集整个换掉，合并模式会把旧 series 的残留 dataZoom 区间留下。
    }, { notMerge: false });
  }, [bars, range, zoomEpoch, box, loading]);

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

  // 想要的窗口比手里的历史还长：说清楚"不是图坏了，是本地只攒了这么多天"。
  const short = !loading && bars.length > 0 && bars.length < range.days;

  return (
    <div className="hist">
      <div className="hist-ttl">
        {name || `type_id ${typeId}`} · {range.key} 日线
      </div>
      <div className="hist-tabs">
        {/* 分钟线数据源（ticker_intraday）不在 M3 范围，按钮置灰而不是藏掉：
            藏了用户会以为 1D 是坏了，置灰配 tooltip 才说得清"这档还没有数据"。 */}
        <button disabled title="分钟线（ticker_intraday）后续里程碑再开">
          1D
        </button>
        {RANGES.map((r) => (
          <button
            key={r.key}
            className={range.key === r.key ? "on" : ""}
            onClick={() => {
              setRange(r);
              setZoomEpoch((e) => e + 1);
            }}
          >
            {r.key}
          </button>
        ))}
        <span className="cov" title="按该类型实际积累天数计，不是全局均值（方案 §3.3）">
          本地已积累 {bars.length} 天
        </span>
      </div>
      {short && (
        <div className="hint" style={{ margin: "0 0 6px" }}>
          本地历史只有 {bars.length} 天，不足 {range.key} 窗口 —— 图上就是全部了，T3 每天续一天。
        </div>
      )}
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
