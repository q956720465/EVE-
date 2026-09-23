const GROUPS = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

/**
 * ECharts 在 JS 里配色吃不了 CSS 变量，值单独放这里。
 * 语义与单簿一致：跌（收盘<开盘）用卖盘色，涨用买盘色 ——
 * 改 styles.css 的 --bid/--ask 时要记得同步这里。
 */
export const UP = "#4ea3ff";
export const DOWN = "#ff6b5e";

/** ISK 价格：<1000 留两位小数，大数加千分位（对齐 Rust 侧 fmt_price 的行为）。 */
export function fmtPrice(v: number | null | undefined): string {
  if (v === null || v === undefined) return "-";
  if (Math.abs(v) < 1000) return v.toFixed(2);
  return GROUPS.format(v);
}

/** 数量按 K/M/B 缩写 —— 单簿里 1,240,000,000 这种数读不动。 */
export function fmtVol(v: number): string {
  if (v <= 0) return "0";
  const abs = Math.abs(v);
  if (abs >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return String(v);
}

export function fmtDur(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s}s`;
  return `${Math.floor(s / 60)}m${String(s % 60).padStart(2, "0")}s`;
}

/** RFC 7231 的 HTTP 日期（ESI 的 Last-Modified）→ 距今多久。 */
export function ageOf(httpDate: string | null): string {
  if (!httpDate) return "未知";
  const t = Date.parse(httpDate);
  if (Number.isNaN(t)) return httpDate;
  const s = Math.max(0, Math.round((Date.now() - t) / 1000));
  if (s < 90) return `${s} 秒前`;
  if (s < 5400) return `${Math.round(s / 60)} 分钟前`;
  return `${Math.round(s / 3600)} 小时前`;
}
