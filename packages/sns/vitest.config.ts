import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    threads: false,
    watch: false,
    environment: 'node',
    reporters: ['default'],
    coverage: {
      include: ['lib/**/*.ts'],
      exclude: ['lib/**/*.spec.ts', 'lib/**/*.test.ts', 'test/**/*.*', 'lib/types/**/*.*'],
      reporter: ['text'],
      all: true,
      lines: 90,
      functions: 100,
      branches: 78,
      statements: 90,
    },
  },
})
