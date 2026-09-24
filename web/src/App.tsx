import { useEffect } from "react";
import { api } from "./api";
import { useStore } from "./store";
import TopBar from "./components/TopBar";
import CatalogTree from "./components/CatalogTree";
import TypeTable from "./components/TypeTable";
import OrderBook from "./components/OrderBook";
import FlipScanner from "./components/FlipScanner";
import { fmtDur } from "./format";

export default function App() {
  const boot = useStore((s) => s.boot);
  const tick = useStore((s) => s.tick);
  const error = useStore((s) => s.error);
  const status = useStore((s) => s.status);
  const rows = useStore((s) => s.rows);
  const query = useStore((s) => s.query);
  const searchResults = useStore((s) => s.searchResults);
  const groupName = useStore((s) => s.groupName);
  const view = useStore((s) => s.view);
  const flip = useStore((s) => s.flip);

  useEffect(() => {
    void boot();
    const id = setInterval(() => void tick(), 5000);
    return () => clearInterval(id);
  }, [boot, tick]);

  return (
    <div className="app">
      <TopBar />
      {view === "flip" ? (
        <div className="panes one">
          <FlipScanner />
        </div>
      ) : (
        <div className="panes">
          <CatalogTree />
          <TypeTable />
          <OrderBook />
        </div>
      )}
      <div className="statusbar">
        <span>{error ? <span className="hint">⚠ {error}</span> : "就绪"}</span>
        {status && (
          <>
            <span>第 {status.round} 轮</span>
            <span>本轮 {status.orders.toLocaleString()} 条订单 → {status.rows_written.toLocaleString()} 行快照</span>
            <span>耗时 {status.last_seconds.toFixed(1)}s</span>
            {/* 倒计时与令牌水位都是采集者本进程的内存态，查看者拿不到 ——
                显示估算值会被当成真话，宁缺毋滥。 */}
            {status.collecting && <span>下次采集 {fmtDur(status.next_in_ms)} 后</span>}
            {status.collecting && (
              <span>令牌余量 {status.remaining_tokens.toLocaleString()} / 12000·15min</span>
            )}
            {/* 采集锁在别的进程时，轮询数字来自共享库的上一轮台账：
                不标出来，用户会把"整点不动的快照"当成采集卡死。 */}
            {!status.collecting && api.live && (
              <span className="hint">只读模式 · 采集由另一进程持有</span>
            )}
          </>
        )}
        <span className="grow" />
        {/* 搜索态下中栏摆的是搜索结果，状态栏却还报上一组的行数——两处口径打架（走查 #14）。 */}
        {view === "flip" ? (
          <span>
            {(flip?.rows.length ?? 0).toLocaleString()} 条机会 · 倒卖扫描
          </span>
        ) : query.trim().length >= 2 ? (
          <span>{searchResults.length} 行 · 搜索中</span>
        ) : (
          <span>{rows.length.toLocaleString()} 行 · {groupName}</span>
        )}
      </div>
    </div>
  );
}
