import { defineConfig } from "vitest/config"
import { fileURLToPath } from "node:url"

// 只测纯逻辑（node 环境，无 jsdom/RTL）。解析 tsconfig 的 @/* 别名到 src/。
export default defineConfig({
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
})
