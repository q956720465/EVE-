import { useState } from "react";
import { api } from "../api";
import { useStore } from "../store";
import { ageOf, fmtDur } from "../format";

const CYCLE_MS = 360_000;
const R = 15;
const C = 2 * Math.PI * R;

/**
 * 环形进度按"6 分钟节拍"算，而不是按本轮耗时 —— 用户关心的是"下一次数据更新还有多久"。
 * 采集本身只要 68~126 s，绝大部分时间是在等 ESI 的 300 s 缓存过期。
 */
export default function TopBar() {
  const status = useStore((s) => s.status);
  const [word, setWord] = useState("");
  const search = useStore((s) => s.search);
  const refresh = useStore((s) => s.selectGroup);
  const groupId = useStore((s) => s.groupId);
  const groupName = useStore((s) => s.groupName);
  const view = useStore((s) => s.view);
  const setView = useStore((s) => s.setView);

  const left = status?.next_in_ms ?? 0;
  const frac = Math.min(1, Math.max(0, 1 - left / CYCLE_MS));
  // 采集那 60 秒里 next_in 是 0，倒计时会僵在"0s" —— 那不是"还有 0 秒"，是"正在跑"。
  const busy = status?.stage === "Fetching" || status?.stage === "Aggregating";
  // 查看者拿不到采集者的内存态倒计时（status 里那个是按节拍估的），
  // 拿估算当事实画环会让用户误判"马上就更新"，改说上一轮的快照年龄。
  const viewer = status != null && !status.collecting;

  return (
    <div className="topbar">
      <div
        className="ring"
        title={`弧长 = 距下次采集的剩余占比（满环=刚发布，空环=到点）。本轮采集耗时 ${(status?.last_seconds ?? 0).toFixed(1)}s，其余在等缓存过期`}
      >
        <svg width="36" height="36" viewBox="0 0 36 36">
          <circle cx="18" cy="18" r={R} fill="none" stroke="var(--line)" strokeWidth="3" />
          <circle
            cx="18"
            cy="18"
            r={R}
            fill="none"
            stroke="var(--accent)"
            strokeWidth="3"
            strokeDasharray={`${C * frac} ${C}`}
            strokeLinecap="round"
            transform="rotate(-90 18 18)"
          />
        </svg>
        <div className="txt">
          <b>{!status ? "--" : viewer ? "只读" : busy ? "采集中" : fmtDur(left)}</b>
          <span>{viewer ? "上一轮快照" : "吉他下次刷新"}</span>
        </div>
      </div>

      <span className={`badge ${api.live ? "live" : "dev"}`} title={api.live ? "数据来自 Tauri IPC" : "浏览器预览：显示的是假数据"}>
        {api.live ? "ESI 实时" : "浏览器预览（假数据）"}
      </span>

      {status && (
        <span className="badge" title="ESI 快照时间，即这批数据的共同来源时刻">
          快照 {ageOf(status.snapshot_lm)}
        </span>
      )}
      {status && status.collecting && (
        <span className="badge" title="market-order 桶 12000 令牌 / 15 分钟（本进程估算；查看者不显示，它的本地桶永远满格，不是事实）">
          令牌 {status.remaining_tokens.toLocaleString()}
        </span>
      )}

      <span className="viewswitch" title="市场 = 三栏浏览；倒卖 = 全宽扫描器；提醒 = 告警与推送配置">
        <button className={view === "market" ? "on" : ""} onClick={() => setView("market")}>
          市场
        </button>
        <button className={view === "flip" ? "on" : ""} onClick={() => setView("flip")}>
          倒卖
        </button>
        <button className={view === "alerts" ? "on" : ""} onClick={() => setView("alerts")}>
          提醒
        </button>
      </span>

      <div className="grow" />

      {view === "market" && (
        <>
          <input
            className="search"
            aria-label="搜索物品类型"
            placeholder="搜索类型名（≥2 字，走 universe/ids + 本地别名）"
            value={word}
            onChange={(e) => {
              setWord(e.target.value);
              void search(e.target.value);
            }}
          />
          <button
            onClick={() => {
              if (groupId !== null) void refresh(groupId, groupName);
            }}
            title="从本地库重新读取当前组（不会突破 ESI 缓存去抢请求）"
          >
            重载本组
          </button>
        </>
      )}
    </div>
  );
}
