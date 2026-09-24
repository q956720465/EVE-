# AGENTS.md — eve-market-desk

EVE Online 欧服市场数据采集与可视化桌面应用（Tauri 2 + Rust + React）。

## 验证入口（唯一）

任何改动后运行测试，必须使用下面这条显式三 crate 命令：

```
cargo test -p emd-core -p emd-daemon -p emd-app
```

> ⚠️ 不要在仓库根直接跑不带 `-p` 的 `cargo test` 作为验证结论：
> workspace 的 `default-members` 有意排除了 `emd-app`（见 `Cargo.toml` 注释，
> Tauri 壳带 200+ 依赖会拖慢默认构建），默认调用会静默跳过 `emd-app` 的
> 13 个 `emd_app_lib` 测试（实测：默认入口 150 个 vs 显式入口 163 个）。

前端改动另需 `npm run typecheck`（转发到 `web/`）。

## 构建命令速查

| 目的 | 命令 |
|---|---|
| 全量测试（唯一验证入口） | `cargo test -p emd-core -p emd-daemon -p emd-app` |
| 快速构建核心库+守护进程（默认成员） | `cargo build` |
| 构建 Tauri 壳（不进默认构建） | `cargo build -p emd-app` |
| 开发运行整应用 | `cargo tauri dev` |

实测冷缓存构建耗时（本机，供权衡参考）：默认成员约 60s，追加构建
`emd-app` 约 +111s（近 3 倍），因此**不要**把 `emd-app` 加回
`default-members`，改用上面的显式测试入口覆盖它。

## 其他

- 守护进程数据目录默认 `%LOCALAPPDATA%\EveMarketDesk\emd.sqlite3`，可用 `--db` 覆盖。
- 结构化日志走 `tracing`，运行时用 `RUST_LOG` 调整级别（默认 `info,reqwest=warn,hyper=warn`）。
