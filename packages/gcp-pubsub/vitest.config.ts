import { defineConfig } from 'vitest/config'

// biome-ignore lint/style/noDefaultExport: vite expects default export
export default defineConfig({
  test: {
    globals: true,
    watch: false,
    mockReset: true,
    pool: 'threads',
    maxWorkers: 1,
    fileParallelism: false,
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
        lines: 85,
        functions: 88,
        branches: 75,
        statements: 85,
      },
    },
  },
})
