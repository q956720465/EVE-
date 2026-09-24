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

/**
 * 单类型日线蜡烛图。ESI 的 history 只有 `(region, type)` 维度、没有站点维度，
 * 所以这张图永远是"该星域均价的时间序列"，不能拿来比较站点（方案 §3.3）。
 *
 * 一张图画完全部本地历史（上游窗口最多 418 天）：不再做 1W/1M 区间按钮，
 * 看局部用图下方的缩放滑块拖选、图上滚轮平移即可。
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

    // 一张图装全部：只滤掉无均价的日子，不再按窗口截断。
    const rows = bars.filter((b) => b.average !== null);

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
  }, [bars, box, loading]);

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
        {name || `type_id ${typeId}`} · 日线全部历史
      </div>
      <div className="hist-tabs">
        {/* 直白计数：起止日期 + 天数，不用 "1Y/已积累" 这类术语。
            上游窗口实测最多 418 天（附录 A.4）。 */}
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
        // 没有区间按钮后图就是唯一主角，给足高度；左右拖动看局部用下方滑块。
        <div ref={setBox} style={{ width: "100%", height: 420 }} />
      )}
      <div className="note">
        历史是 (星域, 类型) 维度、无站点维度：图上是 The Forge 的日均价，跨站对比只有当前快照。
        滚轮或底部滑块可放大拖动看局部。
      </div>
    </div>
  );
}
