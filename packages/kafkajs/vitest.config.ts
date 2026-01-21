import { defineConfig } from 'vitest/config'

// biome-ignore lint/style/noDefaultExport: vite expects default export
export default defineConfig({
  test: {
    globals: true,
    watch: false,
    mockReset: true,
    pool: 'threads',
    maxWorkers: 1,
    testTimeout: 30000, // Kafkajs operations need more time
    coverage: {
      provider: 'v8',
      include: ['lib/**/*.ts'],
      exclude: ['vitest.config.ts', 'lib/**/index.ts'],
      thresholds: {
        lines: 90,
        functions: 90,
        branches: 80,
        statements: 90,
      },
    },
  },
})
