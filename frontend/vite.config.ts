import { defineConfig } from "vite";
import solid from "vite-plugin-solid";
import tailwindcss from "@tailwindcss/vite";
import { visualizer } from "rollup-plugin-visualizer";

export default defineConfig({
  plugins: [
    solid(),
    tailwindcss(),
    visualizer({
      filename: "stats.html",
      open: true, // ビルド完了後に自動でブラウザを開く
      gzipSize: true,
      brotliSize: true,
    }),
  ],
  resolve: {
    alias: {
      "$features": "/src/features",
      "$components": "/src/components",
      "$pages": "/src/pages",
      "$lib": "/src/lib",
      "$schemas": "/src/schemas",
    },
  },
});
