import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
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
        'lib/queues/AbstractPublisherManager.ts',
      ],
      reporter: ['text'],
      all: true,
      thresholds: {
        lines: 35,
        functions: 65,
        branches: 80,
        statements: 35,
      },
    },
  },
})
