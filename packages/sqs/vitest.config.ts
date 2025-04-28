import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    watch: false,
    restoreMocks: true,
    pool: 'threads',
    // TODO: check if needed
    //poolOptions: {
    //  threads: { singleThread: true },
    //},
    coverage: {
      provider: 'v8',
      include: ['lib/**/*.ts'],
      exclude: ['vitest.config.ts', 'lib/**/index.ts'],
      thresholds: {
        lines: 88,
        functions: 100,
        branches: 74,
        statements: 88,
      },
    },
  },
})
