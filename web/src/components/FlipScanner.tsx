import { useEffect, useMemo, useState } from "react";
import { useStore } from "../store";
import { fmtPrice, fmtVol } from "../format";
import type { FlipParams } from "../types";

/** 快照年龄：后端给的是秒数（不是 HTTP 日期），单独一个小函数。 */
function fmtAge(secs: number | null): string {
  if (secs === null) return "未采集（先跑一轮 round）";
  // 阈值取 120s：90～119s 若按分钟四舍五入会显示成"2 分钟前"，比实际老一倍。
  if (secs < 120) return `${secs} 秒前`;
  if (secs < 5400) return `${Math.round(secs / 60)} 分钟前`;
  return `${Math.round(secs / 3600)} 小时前`;
}

/**
 * 倒卖扫描器（M4a）。三个关键口径决定：
 * - 费率公式只在 Rust（emd-core::market::flip）；这里的"有效费率"是后端回传的
 *   只读展示，试算走 trial_calc 命令 —— TS 侧不复制任何金额公式（spec R6）。
 * - 排序键 "净利率×活跃度" 用后端顺序（后端按 margin×ln(1+vol) 排好）；
 *   "单笔绝对利润" 是纯前端重排（只是换个比法，不涉及计算口径）。
 * - 技能步进器改完"保存并重算"，参数落库 + 立即重扫，全程零 ESI 请求。
 */
export default function FlipScanner() {
  const flip = useStore((s) => s.flip);
  const flipBusy = useStore((s) => s.flipBusy);
  const flipSort = useStore((s) => s.flipSort);
  const setFlipSort = useStore((s) => s.setFlipSort);
  const saveFlipParams = useStore((s) => s.saveFlipParams);
  const loadFlip = useStore((s) => s.loadFlip);
  const trial = useStore((s) => s.trial);
  const runTrial = useStore((s) => s.runTrial);

  const [open, setOpen] = useState(true);
  const [p, setP] = useState<FlipParams | null>(null);
  const [dirty, setDirty] = useState(false);
  const [buy, setBuy] = useState("100");
  const [sell, setSell] = useState("110");
  const [qty, setQty] = useState("100");

  // 回显后端参数；本地有未保存改动时不覆盖用户输入。
  const echoed = flip?.params ?? null;
  useEffect(() => {
    if (echoed && !dirty) setP(echoed);
  }, [echoed, dirty]);

  const rows = flip?.rows ?? [];
  const sorted = useMemo(() => {
    const rs = [...rows];
    if (flipSort === "profit") rs.sort((a, b) => b.net_total - a.net_total);
    return rs;
  }, [rows, flipSort]);

  function edit(mut: (draft: FlipParams) => void) {
    if (!p) return;
    const next: FlipParams = { ...p, fees: { ...p.fees } };
    mut(next);
    setP(next);
    setDirty(true);
  }

  const skillsActive = p != null && (p.fees.accounting > 0 || p.fees.broker_relations > 0);
  const caliber = !p
    ? ""
    : skillsActive
      ? `技能口径：Accounting ${p.fees.accounting} · BR ${p.fees.broker_relations}`
      : "技能口径：无影响（游戏默认状态）";

  return (
    <div className="pane flip">
      <h3>
        倒卖扫描
        <span className="viewtabs">
          <button onClick={() => void loadFlip()} title="重新读本地快照并扫描（不会发任何 ESI 请求）">
            {flipBusy ? "扫描中…" : "重扫"}
          </button>
        </span>
      </h3>

      <div className="badges">
        <span className="b" title="费率是估算参数：无公开接口读取官方税率，面值可在面板调整">
          基于估算费率
        </span>
        <span className="b">{caliber}</span>
        <span className="b">快照 {fmtAge(flip?.age_secs ?? null)}</span>
        {flip && (
          <span className="b" title="有效费率由 Rust 按 技能修正公式 算好回传">
            生效销售税 {flip.effective_sales_tax_pct.toFixed(2)}% · 中介费{" "}
            {flip.effective_broker_pct.toFixed(2)}%
          </span>
        )}
      </div>

      {p && (
        <div className="flip-params">
          <button className="fold" onClick={() => setOpen(!open)}>
            {open ? "▾" : "▸"} 参数与技能（{dirty ? "有未保存改动" : "已保存"}）
          </button>
          {open && (
            <>
              <div className="grid">
                <label>
                  销售税基（%，默认 7.5）
                  <input
                    type="number"
                    step="0.1"
                    min="0"
                    max="8"
                    value={p.fees.sales_tax_pct}
                    onChange={(e) => edit((d) => (d.fees.sales_tax_pct = Number(e.target.value)))}
                  />
                </label>
                <label>
                  中介费基（%，默认 3）
                  <input
                    type="number"
                    step="0.1"
                    min="0"
                    max="5"
                    value={p.fees.broker_pct}
                    onChange={(e) => edit((d) => (d.fees.broker_pct = Number(e.target.value)))}
                  />
                </label>
                <label>
                  净利率阈值（%）
                  <input
                    type="number"
                    step="0.5"
                    min="0"
                    value={p.margin_threshold_pct}
                    onChange={(e) => edit((d) => (d.margin_threshold_pct = Number(e.target.value)))}
                  />
                </label>
                <label>
                  账户资金（ISK）
                  <input
                    type="number"
                    step="1000000"
                    min="1"
                    value={p.capital_isk}
                    onChange={(e) => edit((d) => (d.capital_isk = Number(e.target.value)))}
                  />
                </label>
                <label>
                  单笔投入上限（% 资金）
                  <input
                    type="number"
                    step="1"
                    min="0"
                    max="100"
                    value={p.capital_pct_per_trade}
                    onChange={(e) => edit((d) => (d.capital_pct_per_trade = Number(e.target.value)))}
                  />
                </label>
                <label>
                  最小批量（件）
                  <input
                    type="number"
                    step="10"
                    min="1"
                    value={p.min_batch}
                    onChange={(e) => edit((d) => (d.min_batch = Number(e.target.value)))}
                  />
                </label>
                <label title="当前为单件运费近似；按体积×跳数的模型需要星图路由数据，挂账后续">
                  单件运费（ISK，未含运费时填 0）
                  <input
                    type="number"
                    step="0.1"
                    min="0"
                    value={p.freight_isk_per_unit}
                    onChange={(e) => edit((d) => (d.freight_isk_per_unit = Number(e.target.value)))}
                  />
                </label>
                <label title="吃单买入不付中介费（默认关）；挂买单收货的策略打开它">
                  买入侧中介费
                  <input
                    type="checkbox"
                    checked={p.include_buy_broker}
                    onChange={(e) => edit((d) => (d.include_buy_broker = e.target.checked))}
                  />
                </label>
              </div>

              <div className="flip-skills">
                <label>
                  Accounting（0–5，减销售税 11%/级）
                  <input
                    type="number"
                    min="0"
                    max="5"
                    step="1"
                    value={p.fees.accounting}
                    onChange={(e) => edit((d) => (d.fees.accounting = Number(e.target.value)))}
                  />
                </label>
                <label>
                  Broker Relations（0–5，减中介费 0.3pp/级）
                  <input
                    type="number"
                    min="0"
                    max="5"
                    step="1"
                    value={p.fees.broker_relations}
                    onChange={(e) => edit((d) => (d.fees.broker_relations = Number(e.target.value)))}
                  />
                </label>
                <button
                  className={dirty ? "on" : ""}
                  onClick={() => {
                    void saveFlipParams(p).then(() => setDirty(false));
                  }}
                  title="写入参数并立即用当前快照重扫（纯本地，不发 ESI 请求）"
                >
                  保存并重算
                </button>
              </div>

              <div className="flip-trial">
                <label>
                  试算：买入价
                  <input value={buy} onChange={(e) => setBuy(e.target.value)} />
                </label>
                <label>
                  卖出价
                  <input value={sell} onChange={(e) => setSell(e.target.value)} />
                </label>
                <label>
                  数量
                  <input value={qty} onChange={(e) => setQty(e.target.value)} />
                </label>
                <button
                  onClick={() => void runTrial(Number(buy), Number(sell), Number(qty))}
                  title="按当前已保存的费率与技能等级试算：扣税后为负就别挂这单"
                >
                  试算
                </button>
                {trial && (
                  <span className={`out ${trial.net_total < 0 ? "neg" : "pos"}`}>
                    单位净利 {fmtPrice(trial.net_per_unit)} · 总净利 {fmtPrice(trial.net_total)} ·{" "}
                    净利率 {trial.margin_pct.toFixed(2)}%
                    {trial.net_total < 0 && " —— 扣税后亏损：改技能等级或放弃此单"}
                  </span>
                )}
              </div>
            </>
          )}
        </div>
      )}

      {flip && (
        <div className="flip-sort">
          排序：
          <button className={flipSort === "score" ? "on" : ""} onClick={() => setFlipSort("score")}>
            净利率×活跃度
          </button>
          <button className={flipSort === "profit" ? "on" : ""} onClick={() => setFlipSort("profit")}>
            单笔绝对利润
          </button>
          <span className="grow" />
          <span className="dim">评估 {flip.pairs_evaluated.toLocaleString()} 对站↔站</span>
        </div>
      )}

      {!flip ? (
        <div className="empty">读取中…（首次扫描要读整个快照）</div>
      ) : sorted.length === 0 ? (
        <div className="empty">
          本轮 0 机会：want&lt;批量 {flip.dropped_batch}｜短填 {flip.dropped_shortfall}｜未过阈值{" "}
          {flip.dropped_threshold}（共评估 {flip.pairs_evaluated} 对）。
          <br />
          没到阈值是常态：默认口径（无技能）合计费率达 10.5%，把技能等级填成你的真实水平、或调低阈值/最小批量再看。
        </div>
      ) : (
        <div className="flip-tablewrap">
          <table className="flip-table">
            <thead>
              <tr>
                <th className="l">类型</th>
                <th className="l">买站 → 卖站</th>
                <th>买价</th>
                <th>卖价</th>
                <th>可成交量</th>
                <th>净利率</th>
                <th>单位净利</th>
                <th>总净利</th>
                <th>24h 量</th>
                <th>买站档位</th>
                <th>卖站档位</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((r) => (
                <tr key={`${r.type_id}-${r.buy_loc}-${r.sell_loc}`}>
                  <td className="l">
                    {r.type_name} <span className="dim">#{r.type_id}</span>
                  </td>
                  <td className="l">
                    {r.buy_loc_name} → {r.sell_loc_name}
                  </td>
                  <td>{fmtPrice(r.buy_price)}</td>
                  <td>{fmtPrice(r.sell_price)}</td>
                  <td>{fmtVol(r.qty)}</td>
                  <td className={r.margin_pct < 0 ? "neg" : "pos"}>{r.margin_pct.toFixed(2)}%</td>
                  <td className={r.net_per_unit < 0 ? "neg" : ""}>{fmtPrice(r.net_per_unit)}</td>
                  <td className={r.net_total < 0 ? "neg" : ""}>{fmtPrice(r.net_total)}</td>
                  <td>
                    {fmtVol(r.vol24)}{" "}
                    <span
                      className="vsrc"
                      title={
                        r.vol_source === "history"
                          ? "market_history 最近一日的真实成交量"
                          : "该类型无 history 覆盖，用可执行深度估算"
                      }
                    >
                      {r.vol_source === "history" ? "成交" : "估算"}
                    </span>
                  </td>
                  <td>{r.buy_levels}</td>
                  <td>{r.sell_levels}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div className="note">
        净利率 = （卖出净额 − 买入成本 − 运费）÷ 总投入，税基是卖出全额（方案 §4.1 修正口径）；
        数字是估算相对值，不构成"稳赚"承诺。深度只取快照存储的前 5 档。
      </div>
    </div>
  );
}
