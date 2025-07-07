import { defineConfig } from 'vitest/config'

// biome-ignore lint/style/noDefaultExport: vite expects default export
export default defineConfig({
  test: {
    globals: true,
    watch: false,
    restoreMocks: true,
    pool: 'threads',
    coverage: {
      provider: 'v8',
      include: ['lib/**/*.ts'],
      exclude: ['vitest.config.ts', 'lib/**/index.ts', 'lib/events/eventTypes.ts'],
      thresholds: {
        lines: 6,
        functions: 60,
        branches: 70,
        statements: 6,
      },
    },
  },
})
