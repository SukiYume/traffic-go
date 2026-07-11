import fs from 'node:fs';
import path from 'node:path';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

function resolveProxyTarget(): string {
  const explicitTarget = process.env.TRAFFIC_GO_DEV_PROXY?.trim();
  if (explicitTarget) {
    return explicitTarget;
  }

  try {
    const configPath = path.resolve(__dirname, '../deploy/config.example.yaml');
    const content = fs.readFileSync(configPath, 'utf8');
    const match = content.match(/^\s*listen:\s*["']?([^"'\r\n]+)["']?\s*$/m);
    if (match?.[1]) {
      return `http://${match[1].trim()}`;
    }
  } catch {
    // Fall back to the backend's built-in default if the example config is unavailable.
  }

  return 'http://127.0.0.1:8080';
}

const proxyTarget = resolveProxyTarget();

export default defineConfig({
  plugins: [react()],
  base: './',
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          const normalized = id.replace(/\\/g, '/');
          if (normalized.includes('/node_modules/react/') || normalized.includes('/node_modules/react-dom/') || normalized.includes('/node_modules/scheduler/')) {
            return 'react';
          }
          if (normalized.includes('/node_modules/react-router') || normalized.includes('/node_modules/@remix-run/')) {
            return 'router';
          }
          if (normalized.includes('/node_modules/@tanstack/')) {
            return 'query';
          }
          if (normalized.includes('/node_modules/recharts/') || normalized.includes('/node_modules/d3-')) {
            return 'charts';
          }
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': proxyTarget,
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test/setup.ts'],
    css: true,
  },
});
