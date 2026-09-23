import { useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useStore } from "../store";
import { fmtPrice } from "../format";

/**
 * 一屏上千行、每 6 分钟整表换数据 —— 必须按 type_id 做 key 且后端固定按名字排序，
 * 否则排序位置会随每次刷新跳动（v3.1 §6 里点名要防的问题）。
 *
 * 行不用 `<table>`：虚拟列表要把 tbody 变成 block、把每行绝对定位，那样表头与表体
 * 各自算列宽，实测会错开一整格。改成共享同一份 grid 模板的 div，表头自然对齐。
 */
const COLS = "minmax(0,2.4fr) minmax(0,1fr) minmax(0,1fr) minmax(0,.9fr) minmax(0,.8fr)";

export default function TypeTable() {
  const rows = useStore((s) => s.rows);
  const query = useStore((s) => s.query);
  const hits = useStore((s) => s.searchResults);
  const selected = useStore((s) => s.selected);
  const selectType = useStore((s) => s.selectType);
  const groupName = useStore((s) => s.groupName);

  const shown = query.trim().length >= 2 ? hits : rows;
  const parentRef = useRef<HTMLDivElement>(null);

  const v = useVirtualizer({
    count: shown.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 25,
    overscan: 12,
  });

  return (
    <div className="pane" ref={parentRef} style={{ overflow: "auto" }}>
      <h3>
        {query.trim().length >= 2
          ? `搜索「${query}」· ${shown.length} 个结果`
          : `${groupName} · ${shown.length} 个类型`}
      </h3>
      {shown.length === 0 ? (
        <div className="empty">
          {query.trim().length >= 2
            ? "没有匹配的类型。"
            : "这个组里当前没有可显示的行情 —— 可能全部被薄档（<3 笔）或僵尸单（>45 天）过滤掉了，详情面板会显示被剔了多少条。"}
        </div>
      ) : (
        <>
          <div className="vrow head" style={{ gridTemplateColumns: COLS }}>
            <span>名称</span>
            <span className="r">买一</span>
            <span className="r">卖一</span>
            <span className="r">价差</span>
            <span className="r">档数</span>
          </div>
          <div style={{ height: v.getTotalSize(), position: "relative" }}>
            {v.getVirtualItems().map((item) => {
              const r = shown[item.index];
              if (!r) return null;
              const spread =
                r.best_bid && r.best_ask ? ((r.best_ask - r.best_bid) / r.best_bid) * 100 : null;
              return (
                <div
                  key={r.type_id}
                  className={`vrow ${selected === r.type_id ? "sel" : ""}`}
                  style={{
                    gridTemplateColumns: COLS,
                    position: "absolute",
                    top: 0,
                    left: 0,
                    right: 0,
                    transform: `translateY(${item.start}px)`,
                    height: item.size,
                  }}
                  onClick={() => void selectType(r.type_id)}
                  title={`type_id ${r.type_id}`}
                >
                  <span>{r.name || `#${r.type_id}`}</span>
                  <span className="r bid-txt">{fmtPrice(r.best_bid)}</span>
                  <span className="r ask-txt">{fmtPrice(r.best_ask)}</span>
                  <span className="r">{spread === null ? "-" : `${spread.toFixed(1)}%`}</span>
                  <span className="r o">
                    {r.bid_levels}/{r.ask_levels}
                  </span>
                </div>
              );
            })}
          </div>
        </>
      )}
    </div>
  );
}
