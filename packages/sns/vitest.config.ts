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
        lines: 84,
        functions: 90,
        branches: 65,
        statements: 84,
      },
    },
  },
})
