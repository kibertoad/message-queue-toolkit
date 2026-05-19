import { defineConfig } from 'vitest/config'

// biome-ignore lint/style/noDefaultExport: vite expects default export
export default defineConfig({
  test: {
    globals: true,
    watch: false,
    mockReset: true,
    pool: 'threads',
    maxWorkers: 1,
    setupFiles: ['test/utils/vitest.setup.ts'],
    include: ['bench/**/*.ts'],
  },
})
