import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The coordinator is the only backend. Proxying /v1 keeps the dashboard
// same-origin, so the coordinator needs no CORS configuration.
const coordinator = process.env.RELAY_COORDINATOR ?? "http://127.0.0.1:7070";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: { "/v1": { target: coordinator, changeOrigin: false } },
  },
  preview: {
    port: 4173,
    proxy: { "/v1": { target: coordinator, changeOrigin: false } },
  },
});
