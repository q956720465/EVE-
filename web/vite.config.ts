import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Tauri 的 dev 走固定端口，且不能自动开浏览器（由 Rust 侧创建 WebView 窗口）
export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  server: {
    port: 5173,
    strictPort: true,
    host: "127.0.0.1",
  },
  envPrefix: ["VITE_", "TAURI_"],
  build: {
    target: "es2022",
    outDir: "dist",
    sourcemap: true,
    rollupOptions: {
      output: {
        // 首屏只需 app+react；echarts/zrender 由 HistoryChart 懒加载
        // （点开「历史」才拉）。zrender 单独成包：它自己就 ~130kB，
        // 挤在 echarts 包里会让单 chunk 超 500kB 阈值。
        // 顺序即优先级：@tanstack/react-virtual 含 "react" 子串，必须先于 react 判。
        manualChunks(id) {
          if (!id.includes("node_modules")) return;
          if (id.includes("zrender")) return "zrender";
          if (id.includes("echarts")) return "echarts";
          if (id.includes("@tanstack")) return "virtual";
          if (id.includes("@tauri-apps")) return "tauri";
          if (id.includes("react") || id.includes("scheduler") || id.includes("zustand"))
            return "react-vendor";
        },
      },
    },
  },
});
