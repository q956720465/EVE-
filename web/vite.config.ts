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
  },
});
