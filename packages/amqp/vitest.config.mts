import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    poolOptions: {
      threads: {
        singleThread: true,
      },
    },
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
        'lib/AmqpExchangePublisherManager.ts',
        'lib/AbstractAmqpExchangePublisher.ts',
      ],
      reporter: ['text'],
      all: true,
      thresholds: {
        lines: 90,
        functions: 95,
        branches: 79,
        statements: 90,
      },
    },
  },
})
