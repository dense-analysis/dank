import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  base: "/reader/",
  plugins: [react()],
  server: {
    proxy: {
      "/api/reader": "http://127.0.0.1:8091",
      "/assets": "http://127.0.0.1:8091",
    },
  },
  test: { environment: "jsdom", include: ["src/**/*.test.{ts,tsx}"] },
});
