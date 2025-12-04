import { defineConfig } from 'vitest/config'

// biome-ignore lint/style/noDefaultExport: vite expects default export
export default defineConfig({
  test: {
    globals: true,
    watch: false,
    restoreMocks: true,
    pool: 'threads',
    fileParallelism: false,
    poolOptions: {
      threads: { singleThread: true },
    },
    coverage: {
      provider: 'v8',
      include: ['lib/**/*.ts'],
      exclude: ['vitest.config.ts', 'lib/**/index.ts'],
      thresholds: {
        lines: 80,
        functions: 90,
        branches: 74,
        statements: 80,
      },
    },
  },
})
