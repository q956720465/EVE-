import { useState } from "react";
import { useStore } from "../store";
import { ageOf, fmtPrice, fmtVol } from "../format";
import HistoryChart from "./HistoryChart";
import type { PriceLevel } from "../types";

function Ladder({ side, levels }: { side: "ask" | "bid"; levels: PriceLevel[] }) {
  // 用户指定：不要图形表达（深度条已去），只留数字；颜色仍标买/卖。
  const color = side === "ask" ? "var(--ask)" : "var(--bid)";
  return (
    <div className={`ladder ${side}`}>
      <div className="h">
        <span>价格</span>
        <span>数量</span>
        <span>累计</span>
        <span>笔</span>
      </div>
      {levels.map((l, i) => {
        const cum = levels.slice(0, i + 1).reduce((a, b) => a + b.volume, 0);
        return (
          <div key={`${l.price}-${i}`} className={`row${i === 0 ? " best" : ""}`}>
            <span className="p" style={{ color }}>
              {fmtPrice(l.price)}
            </span>
            <span className="v">{fmtVol(l.volume)}</span>
            <span className="v">{fmtVol(cum)}</span>
            <span className="o">{l.orders}</span>
          </div>
        );
      })}
      {levels.length === 0 && <div className="empty">这一侧当前没有合格挂单。</div>}
    </div>
  );
}

export default function OrderBook() {
  const detail = useStore((s) => s.detail);
  const detailFor = useStore((s) => s.detailFor);
  const hubs = useStore((s) => s.hubs);
  const locationId = useStore((s) => s.locationId);
  const selectLocation = useStore((s) => s.selectLocation);
  const selected = useStore((s) => s.selected);
  // 单簿与历史共用右栏：复刻游戏市场窗口时它们是同一个物品的两面。
  const [view, setView] = useState<"book" | "hist">("book");

  return (
    <div className="pane">
      <h3>
        {view === "book" ? "买卖单簿（前 5 档）" : "历史日线"}
        <span className="viewtabs">
          <button className={view === "book" ? "on" : ""} onClick={() => setView("book")}>
            单簿
          </button>
          <button className={view === "hist" ? "on" : ""} onClick={() => setView("hist")}>
            历史
          </button>
        </span>
      </h3>
      <div className="locbar">
        {hubs.slice(0, 6).map((h) => (
          <button
            key={h.location_id}
            className={locationId === h.location_id ? "on" : ""}
            onClick={() => void selectLocation(h.location_id)}
            title={`${h.order_count.toLocaleString()} 条订单 · 占该星域 ${h.share_pct.toFixed(2)}%`}
          >
            {h.name.split(" - ")[0]}
          </button>
        ))}
      </div>

      {!selected ? (
        <div className="empty">从中间列表选一个类型。</div>
      ) : view === "hist" ? (
        <HistoryChart typeId={selected} />
      ) : !detail ? (
        // 区分"还在读"与"真没有盘"：无快照行的类型 book_row 返回 null，
        // 以前永远显示"读取中…"，用户会盯着一个永远不会来的加载转圈（走查 #10/#16）。
        detailFor === selected ? (
          <div className="empty">
            这个类型在当前站点没有可显示的挂单 —— 可能全部被薄档（&lt;3 笔）或僵尸单（&gt;45 天）过滤，
            也可能根本没人挂。换站点或等下一轮快照再看。
          </div>
        ) : (
          <div className="empty">读取中…</div>
        )
      ) : (
        <div className="book">
          <div className="ttl">{detail.name}</div>
          <div className="sub">
            {detail.location_name} · type_id {detail.type_id} · 快照 {ageOf(detail.snapshot_lm)}
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
            <Ladder side="ask" levels={detail.ask_depth} />
            <Ladder side="bid" levels={detail.bid_depth} />
          </div>

          <div className="filters">
            ⚙ 档位笔数：买 {detail.bid_levels} / 卖 {detail.ask_levels}
            <br />
            本类型被剔除：僵尸单 {detail.skipped_stale} · 薄档 {detail.skipped_thin} · 批发单{" "}
            {detail.skipped_wholesale}
          </div>
          <div className="note">
            卖单栏是你的买入成本、买单栏是卖出目标；只列前 5 档，实际吃单会顺着阶梯往上抬价。
          </div>
        </div>
      )}
    </div>
  );
}
