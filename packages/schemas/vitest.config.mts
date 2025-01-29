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
      exclude: [
        'lib/**/*.spec.ts',
        'lib/**/*.test.ts',
        'test/**/*.*',
        'lib/types/**/*.*',
        'lib/sns/fakes',
      ],
      reporter: ['text'],
      all: true,
      thresholds: {
        lines: 6,
        functions: 60,
        branches: 70,
        statements: 6,
      },
    },
  },
})
