import { resolve } from "path";
import { defineConfig } from "vite";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";

import { dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));


// https://vite.dev/config/
export default defineConfig({
  build: {
    lib: {
      entry: resolve(__dirname, "src/index.ts"),
      name: "mzpeak_ts",
      fileName: "mzpeak_ts",
      formats: ["es"]
    },
  },
  plugins: [wasm(), topLevelAwait()],
});