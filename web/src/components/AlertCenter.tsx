import { useEffect, useState } from "react";
import { api } from "../api";
import { useStore } from "../store";
import { fmtPrice } from "../format";
import type { AlertKind, AlertPayload, AlertRow, AlertState } from "../types";

/** 形态角标的文字（枚举 → 中文只在界面这一层做一次；串仍是 Rust 的 snake_case）。 */
const KIND_TEXT: Record<AlertKind, string> = {
  expected_sell_loss: "预期亏·卖单",
  buy_order_trap: "套牢亏·买单",
  realized_loss: "已实现亏",
};

const KIND_TITLE: Record<AlertKind, string> = {
  expected_sell_loss: "① 挂卖单预期亏：挂价扣有效税后仍低于 FIFO 成本 + 实付中介费",
  buy_order_trap: "② 挂买单套牢亏：本站当前可执行卖出净额低于买入成本 + 实付中介费",
  realized_loss: "③ 已实现成交亏：journal 真值（卖出所得 − 实付税 − 被消耗批次成本 − 卖出侧中介费）",
};

const STATE_TEXT: Record<AlertState, string> = {
  new: "未推送",
  notified: "已推送",
  cleared: "已清（周期结束）",
};

const STATE_TITLE: Record<AlertState, string> = {
  new: "本轮观测到亏损，尚未推送（可能是冷却中/当日额度已尽/还没有通道）",
  notified: "至少一条通道确认收到过（本地提醒中心也算一条）",
  cleared: "亏损消失（撤单 / 盘口回来 / 判定面移出）：周期结束，但行与通知史都留着",
};

/** 秒级时间戳 → 本地时刻（只用于显示，不参与任何判定）。 */
function fmtTs(ts: number | null): string {
  if (ts === null) return "-";
  return new Date(ts * 1000).toLocaleString();
}

/** 距今多久（秒级时间戳）。与 `ageOf`（HTTP 日期）分开：这里拿到的是 Unix 秒。 */
function fmtAgo(ts: number | null): string {
  if (ts === null) return "未知";
  const s = Math.max(0, Math.round(Date.now() / 1000 - ts));
  if (s < 90) return `${s} 秒前`;
  if (s < 5400) return `${Math.round(s / 60)} 分钟前`;
  if (s < 172_800) return `${Math.round(s / 3600)} 小时前`;
  return `${Math.round(s / 86_400)} 天前`;
}

/** 距某个**未来**时刻还有多久（令牌有效期用）。未来时刻走 fmtAgo 会永远显示"0 秒前"。 */
function fmtLeft(ts: number): string {
  const s = ts - Math.floor(Date.now() / 1000);
  if (s < 90) return `${s} 秒后`;
  if (s < 5400) return `${Math.round(s / 60)} 分钟后`;
  return `${Math.round(s / 3600)} 小时后`;
}

/** 一段**时长**（秒）—— 口径摘要里的"数据年龄"是区间不是时刻，别写成"60 秒前"。 */
function fmtAgeSecs(secs: number): string {
  if (secs < 90) return `${secs} 秒`;
  if (secs < 5400) return `${Math.round(secs / 60)} 分钟`;
  if (secs < 172_800) return `${Math.round(secs / 3600)} 小时`;
  return `${Math.round(secs / 86_400)} 天`;
}

/** 通知史：`day×count@时刻`，与 daemon 的 `alerts` 表同一套文本口径。 */
function fmtNotify(r: AlertRow): string {
  if (r.notified_at === null) return "未推送过";
  const day = r.notified_day ?? "（无日键）";
  return `${day} ×${r.notified_count_day} @ ${fmtTs(r.notified_at)}`;
}

/**
 * `alerts.payload` 的解析。**失败不抛**：一行 payload 坏掉不该让整块面板白屏，
 * 更不该把它静默改成"没有口径"——那正是要显示给用户看的东西（与 Rust 侧"名字缺失
 * 不该杀掉一行告警"同一纪律）。
 */
function parseCard(payload: string): { card: AlertPayload | null; error: string | null } {
  try {
    return { card: JSON.parse(payload) as AlertPayload, error: null };
  } catch (e) {
    return { card: null, error: String(e) };
  }
}

/**
 * 提醒中心（M4c）。
 *
 * 三块内容的边界（spec §4.4 / §4.5）：
 * - **列表**：全量留存，不受推送限额影响（闸门只管推不推，从不删行）。行里是角色自己的挂单与
 *   钱包数据 —— 全应用唯一的私有数据视图，所以顶上有「私有数据」提示条。
 * - **口径摘要**从 `payload` 的 JSON 里读（推送卡片与提醒中心共用同一份序列化，spec §4.4）：
 *   界面不重算任何金额/费率，只展示 Rust 判定时写下的那一份。
 * - **通道配置**：密钥只以"是否已配置"露面。留空 = 不动它，要清空必须显式勾选 ——
 *   把"没动"与"清空"混起来会让只翻开关的用户悄悄丢掉加签密钥（之后每条推送 310000）。
 */
export default function AlertCenter() {
  const alerts = useStore((s) => s.alerts);
  const alertsBusy = useStore((s) => s.alertsBusy);
  const settings = useStore((s) => s.settings);
  const sso = useStore((s) => s.sso);
  const ssoBusy = useStore((s) => s.ssoBusy);
  const loadAlerts = useStore((s) => s.loadAlerts);
  const loadSettings = useStore((s) => s.loadSettings);
  const saveSettings = useStore((s) => s.saveSettings);
  const loadSso = useStore((s) => s.loadSso);
  const loginSso = useStore((s) => s.loginSso);
  const logoutSso = useStore((s) => s.logoutSso);

  // 表单本地态：回显后端的值；有未保存改动时不被回显覆盖（照 FlipScanner 的先例）。
  const [webhook, setWebhook] = useState("");
  const [enabled, setEnabled] = useState(false);
  const [secret, setSecret] = useState("");
  const [clearSecret, setClearSecret] = useState(false);
  // SSO 的两个非密钥值（spec §4.1 的设置页可配）。回显的是**生效值**（库 > env > 默认），
  // 空串保存 = 清掉库里的值、回落 env/默认 —— 与密钥框的"留空 = 不改"是两回事，故不复用三态。
  const [clientId, setClientId] = useState("");
  const [redirectUri, setRedirectUri] = useState("");
  const [dirty, setDirty] = useState(false);

  useEffect(() => {
    if (settings && !dirty) {
      setWebhook(settings.webhook);
      setEnabled(settings.enabled);
      setClientId(settings.client_id);
      setRedirectUri(settings.redirect_uri);
      // 密钥框永远是空的：库里那份明文从不回前端，用户不填就是不填。
      setSecret("");
      setClearSecret(false);
    }
  }, [settings, dirty]);

  function save() {
    // 三态映射（**这里就是那个坑**）：留空且没勾"清除" = 不改密钥（undefined）；
    // 勾了清除 = 显式清空（空串）；填了值 = 换新密钥。
    const secretIn = clearSecret ? "" : secret.trim() === "" ? undefined : secret;
    void saveSettings({ webhook, secret: secretIn, enabled, client_id: clientId, redirect_uri: redirectUri }).then(
      (ok) => {
        // 只有真保存成功才清 dirty：失败时保持脏标记与用户输入，
        // 否则后端拒绝会被回显覆盖成"已保存"的假象。
        if (ok) {
          setDirty(false);
          setSecret("");
          setClearSecret(false);
          // 保存后立刻回读挂链状态：`client_id_set` / 开关都是后端按库里的新值算的，
          // 不回读的话"刚填完 client_id，登录按钮还是灰的"，看起来像没保存成功。
          void loadSso();
        }
      },
    );
  }

  const rows = alerts ?? [];
  const dingtalkOn = (settings?.channels ?? []).includes("dingtalk");

  return (
    <div className="pane alerts">
      <h3>
        提醒中心
        <span className="viewtabs">
          <button
            onClick={() => {
              void loadAlerts();
              void loadSettings();
              void loadSso();
            }}
            title="重新读本地告警表与通道配置（不发任何 ESI / 钉钉请求）"
          >
            {alertsBusy ? "读取中…" : "重载"}
          </button>
        </span>
      </h3>

      {/* 全应用唯一的私有数据视图：这些行是角色自己的挂单与钱包数据（spec §4.5 的豁免记录）。
          提示条必须常驻 —— 用户看别的视图时不会想到这一块是"我的账"。 */}
      <div className="private-note" title="spec §4.5：私有数据豁免只覆盖本视图与推送卡片，不改变其余视图只用公开市场数据的口径">
        <b>私有数据</b>
        这一屏是你的角色挂单与钱包流水（ESI 授权读到的），全应用只有这里与推送卡片会碰它；
        其余视图一律只用公开市场数据。数据只落在本机库里，退出登录不会删除已有告警行。
      </div>

      <div className="alert-grid">
        {/* ---- SSO 挂链 ---- */}
        <div className="panel">
          <div className="panel-ttl">角色挂链（EVE SSO）</div>
          {!sso ? (
            <div className="empty">读取中…</div>
          ) : (
            <>
              <div className="kv">
                <span>状态</span>
                <span>
                  {sso.linked ? (
                    <>
                      <span className="ok">已挂链</span> · {sso.char_name}{" "}
                      <span className="dim">#{sso.char_id}</span>
                    </>
                  ) : (
                    <span className="hint">未挂链</span>
                  )}
                </span>
              </div>
              <div className="kv">
                <span>令牌有效期</span>
                <span>
                  {sso.expires_at === null ? (
                    "无令牌"
                  ) : sso.token_expired ? (
                    <span className="hint">已过期（{fmtTs(sso.expires_at)}）—— 下一轮同步前会自动刷新</span>
                  ) : (
                    <>
                      {fmtTs(sso.expires_at)} <span className="dim">（还有 {fmtLeft(sso.expires_at)}）</span>
                    </>
                  )}
                </span>
              </div>
              <div className="kv">
                <span>上次同步</span>
                <span>
                  {sso.last_sync_at === null ? (
                    <span className="dim">未知（还没跑过同步回合）</span>
                  ) : (
                    <>
                      {fmtTs(sso.last_sync_at)} <span className="dim">（{fmtAgo(sso.last_sync_at)}）</span>
                    </>
                  )}
                </span>
              </div>
              {/* "为什么什么都不动"的两条：开关关着 / client_id 没配。不写出来，用户只能看到零告警。 */}
              {!sso.char_sync_enabled && (
                <div className="hint small">
                  角色同步<b>关着</b>（client_id 没填，或 EMD_CHAR_SYNC=0）：每个采集轮都会静默跳过
                  同步与告警判定。
                </div>
              )}
              {!sso.client_id_set && (
                <div className="hint small">
                  没有 client_id 就打不开授权页：先在 EVE 开发者后台注册应用（回调地址要注册成{" "}
                  <b>{settings?.redirect_uri || "读配置中…"}</b>），再把它填进下面这一栏。
                </div>
              )}
              {sso.token_error && (
                <div className="hint small">令牌读到了但解不出角色：{sso.token_error}（重新登录即可）</div>
              )}
              {/* 设置页可配（spec §4.1）：注册完应用把两个值粘进来即可，不必再设环境变量。
                  两句话都有"为什么"：URI 是精确匹配（差一个字符浏览器就落到空处、白等到超时）；
                  生效时机分两种（登录当场读库，采集者只读启动那一刻的快照）。 */}
              <label className="field">
                client_id（开发者后台注册应用后取）
                <input
                  value={clientId}
                  onChange={(e) => {
                    setClientId(e.target.value);
                    setDirty(true);
                  }}
                  placeholder="EVE 开发者应用的 Client ID"
                  title="与开发者后台的 Client ID 逐字符一致；它出现在授权页 URL 里，不是密钥"
                />
              </label>
              <label className="field">
                回调地址 redirect_uri
                <input
                  value={redirectUri}
                  onChange={(e) => {
                    setRedirectUri(e.target.value);
                    setDirty(true);
                  }}
                  placeholder="http://127.0.0.1:8765/callback"
                  title="必须与开发者后台注册的回调地址逐字符一致（含端口）：EVE 是精确匹配，差一个字符浏览器就落到空处"
                />
              </label>
              <div className="hint small">
                回调地址必须与开发者后台注册值<b>逐字符一致</b>（含端口）—— 差一个字符，浏览器就落到空处、
                登录白等到超时。两个值清空保存 = 清掉本机库里存的那份，回落环境变量 / 默认值。
                <br />
                生效时机：<b>登录当场生效</b>（命令在点下去那一刻读库）；<b>角色同步与首启回填窗要重启应用
                </b>才轮到采集者用上新配置（它只读启动那一刻的快照）。
              </div>
              <div className="row-actions">
                <button
                  className={dirty ? "on" : ""}
                  onClick={save}
                  title="与推送配置同一个保存入口：写库后回读；client_id / 回调地址当场（登录时）生效"
                >
                  {dirty ? "保存配置（有未保存改动）" : "保存配置"}
                </button>
                <button
                  onClick={() => void loginSso()}
                  disabled={ssoBusy || !sso.client_id_set}
                  title={
                    sso.client_id_set
                      ? "打开系统浏览器完成 EVE 授权（最长等 180 秒），令牌只落系统凭据库"
                      : "先在下面填 client_id：没有它授权页必然报错"
                  }
                >
                  {ssoBusy ? "等待授权…" : "登录 EVE SSO"}
                </button>
                <button
                  onClick={() => void logoutSso()}
                  disabled={ssoBusy || !sso.linked}
                  title="清除系统凭据库里的令牌（库内角色数据与告警表一行不动）"
                >
                  退出登录
                </button>
                {!api.live && <span className="dim small">预览：登录/登出只改内存里的示例状态</span>}
              </div>
            </>
          )}
        </div>

        {/* ---- 通道配置 ---- */}
        <div className="panel">
          <div className="panel-ttl">推送通道（钉钉群机器人）</div>
          {!settings ? (
            <div className="empty">读取中…</div>
          ) : (
            <>
              <div className="kv">
                <span>本轮通道</span>
                <span>
                  {(settings.channels ?? []).map((c) => (
                    <span key={c} className={c === "dingtalk" ? "tag on" : "tag"}>
                      {c === "dingtalk" ? "钉钉" : "本地提醒中心"}
                    </span>
                  ))}
                  {settings.enabled && !dingtalkOn && (
                    <span className="hint small">开关开着但 webhook 是空的 → 钉钉通道不在场</span>
                  )}
                </span>
              </div>
              <label className="field">
                Webhook（含 access_token）
                <input
                  value={webhook}
                  onChange={(e) => {
                    setWebhook(e.target.value);
                    setDirty(true);
                  }}
                  placeholder="https://oapi.dingtalk.com/robot/send?access_token=…"
                  title="回显的是中段打码值：不动它就保留库里那条真地址；清空此框 = 摘掉 webhook"
                />
              </label>
              <label className="field">
                加签密钥 SEC（当前：{settings.secret_set ? "已配置" : "未配置"}）
                <input
                  type="password"
                  autoComplete="new-password"
                  value={secret}
                  disabled={clearSecret}
                  onChange={(e) => {
                    setSecret(e.target.value);
                    setDirty(true);
                  }}
                  placeholder={
                    clearSecret
                      ? "将被清空（改用关键词安全设置）"
                      : settings.secret_set
                        ? "留空 = 不改动已配置的密钥"
                        : "留空 = 不使用加签（只用关键词安全设置）"
                  }
                  title="密钥明文从不回显：留空即不改变库里的值；要换就填新值"
                />
              </label>
              <label className="check">
                <input
                  type="checkbox"
                  checked={clearSecret}
                  onChange={(e) => {
                    setClearSecret(e.target.checked);
                    if (e.target.checked) setSecret("");
                    setDirty(true);
                  }}
                  title="把库里的密钥置空（钉钉只按关键词校验）——与「留空」是两件事"
                />
                清除已配置的密钥（只按关键词校验，不签名）
              </label>
              <label className="check">
                <input
                  type="checkbox"
                  checked={enabled}
                  onChange={(e) => {
                    setEnabled(e.target.checked);
                    setDirty(true);
                  }}
                  title="总开关。关掉 = 只判定、只落本地提醒中心，绝不外发"
                />
                启用推送（关掉则只判定并记进提醒中心）
              </label>
              <div className="row-actions">
                <button className={dirty ? "on" : ""} onClick={save} title="写入配置并回读打码结果">
                  {dirty ? "保存配置（有未保存改动）" : "保存配置"}
                </button>
                <span className="dim small">
                  密钥与 webhook 只落本机库（meta KV），日志与回显一律打码
                </span>
              </div>
            </>
          )}
        </div>
      </div>

      {/* ---- 告警列表 ---- */}
      {alerts === null ? (
        <div className="empty">读取中…</div>
      ) : rows.length === 0 ? (
        <div className="empty">
          还没有任何告警。前提有三条：① 在上面填好 client_id 并登录；② 跑采集（
          <span className="dim">emd serve</span> 或让本窗口持有采集锁）；③ 判定面里真的有亏损
          —— 成本未知的类型整个不参与判定（拿 0 当成本会造出满屏假告警）。
        </div>
      ) : (
        <div className="alert-tablewrap">
          <table className="alert-table">
            <thead>
              <tr>
                <th className="l">形态</th>
                <th className="l">类型</th>
                <th className="l">站点</th>
                <th>方向</th>
                <th>亏损额 (ISK)</th>
                <th>亏损率</th>
                <th>状态</th>
                <th className="l">通知史</th>
                <th className="l">首次 / 最近见到</th>
                <th className="l">口径摘要（来自 payload）</th>
                <th className="l">检索键</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const { card, error } = parseCard(r.payload);
                const c = card?.caliber ?? null;
                return (
                  <tr key={r.alert_key}>
                    <td className="l">
                      <span className={`kbadge k-${r.kind}`} title={KIND_TITLE[r.kind]}>
                        {KIND_TEXT[r.kind]}
                      </span>
                    </td>
                    <td className="l">
                      {r.type_name} <span className="dim">#{r.type_id}</span>
                    </td>
                    <td className="l">
                      {r.location_name} <span className="dim">#{r.location_id}</span>
                    </td>
                    <td>{r.is_buy ? <span className="bid-txt">买</span> : <span className="ask-txt">卖</span>}</td>
                    <td>{fmtPrice(r.last_loss_isk)}</td>
                    <td className="neg">{r.last_margin_pct.toFixed(2)}%</td>
                    <td>
                      <span className={`sbadge s-${r.state}`} title={STATE_TITLE[r.state]}>
                        {STATE_TEXT[r.state]}
                      </span>
                    </td>
                    <td className="l">
                      {fmtNotify(r)}
                      {r.last_notified_margin_pct !== null && (
                        <span className="dim">
                          {" "}
                          · 推时亏损率 {r.last_notified_margin_pct.toFixed(2)}%（再低 2pp 可穿透冷却）
                        </span>
                      )}
                    </td>
                    <td className="l dim">
                      {fmtTs(r.first_seen_at)}
                      <br />
                      {fmtTs(r.last_seen_at)}
                    </td>
                    {/* 口径摘要：字段逐个来自 payload 的 JSON（推送卡片与这里共用同一份序列化）。 */}
                    <td className="l caliber">
                      {c === null ? (
                        <span className="hint">payload 解析失败（{error}）—— 卡片原文：{r.payload}</span>
                      ) : (
                        <>
                          <div className="dim">
                            {c.track} · 有效税 {c.sales_tax_pct.toFixed(3)}% · 中介费{" "}
                            {c.broker_pct.toFixed(3)}%
                          </div>
                          <div className="dim">
                            单位成本 {fmtPrice(c.unit_cost)}（{c.cost_source}）· 数据年龄{" "}
                            {fmtAgeSecs(c.data_age_secs)}
                          </div>
                          <div className="dim">{c.skill_caliber}</div>
                          <div className="formula">{c.formula}</div>
                          <div className="dim">
                            卡上原挂单 id {card?.order_id || "—（未在本机观察窗内）"} · 挂价/成交价{" "}
                            {fmtPrice(card?.price)} · 数量 {card?.volume}
                          </div>
                        </>
                      )}
                    </td>
                    <td className="l mono">{r.alert_key}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className="note">
        亏损额与亏损率是<b>判定那一刻</b>的数（每轮命中的条目会被刷新成最新观测）；「已清」只表示这一轮
        不再命中（撤单 / 盘口回来），行与通知史都留着。推送闸门：边沿触发、同一条 4 小时内不重复、
        亏损率再低 2pp 可穿透、当日全局至多 5 条 —— 被闸门拦下的条目照样在这里。
      </div>
    </div>
  );
}
