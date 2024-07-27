import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    poolOptions: {
      threads: {
        singleThread: true,
      },
    },
    pool: 'threads',
    watch: false,
    environment: 'node',
    reporters: ['default'],
    coverage: {
      provider: 'v8',
      include: ['lib/**/*.ts'],
      exclude: ['lib/**/*.spec.ts', 'lib/**/*.test.ts', 'test/**/*.*', 'lib/types/**/*.*'],
      reporter: ['text'],
      all: true,
      thresholds: {
        lines: 85,
        functions: 90,
        branches: 65,
        statements: 85,
      },
    },
  },
})
