import { defineConfig } from 'vitest/config'

// biome-ignore lint/style/noDefaultExport: vite expects default export
export default defineConfig({
  test: {
    globals: true,
    watch: false,
    mockReset: true,
    pool: 'threads',
    maxWorkers: 1,
    coverage: {
      provider: 'v8',
      include: ['lib/**/*.ts'],
      exclude: ['vitest.config.ts', 'lib/**/index.ts'],
      thresholds: {
        lines: 95,
        functions: 55,
        branches: 100,
        statements: 95,
      },
    },
  },
})
