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
      exclude: [
        'vitest.config.ts',
        'lib/**/index.ts',
        'lib/fakes/FakeConsumerErrorResolver.ts',
        'lib/schemas/pubSubSchemas.ts',
        'lib/types/MessageTypes.ts',
      ],
      thresholds: {
        lines: 79,
        functions: 88,
        branches: 74,
        statements: 79,
      },
    },
  },
})
