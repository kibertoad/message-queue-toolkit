/**
 * Codec micro-benchmarks — compress/decompress latency in isolation (no network).
 *
 * Run: pnpm --filter @message-queue-toolkit/sqs bench
 *
 * These tests measure the CPU cost of the codec only, free from LocalStack
 * network noise. Each case runs ITERATIONS compress+decompress round-trips and
 * asserts the total time is below a very conservative ceiling, making them safe
 * to run in CI as regression guards.
 *
 * Expected times on typical developer/CI hardware:
 *   small payload (~100 B)  → ~20–100 ms for 100 iterations
 *   large payload (~6 KB)   → ~50–300 ms for 100 iterations
 *
 * The ceilings below are set at ~10× the expected worst case so that only a
 * genuine algorithmic regression (or a severely starved CI runner) will fail.
 */
import { ZstdCodecHandler } from '@message-queue-toolkit/codec'
import { describe, expect, it } from 'vitest'

const handler = new ZstdCodecHandler()
const ITERATIONS = 100

/** Small message — representative of the "skip compression" boundary (~100 B). */
const SMALL = Buffer.from(JSON.stringify({ id: 'bench-small', messageType: 'add' }), 'utf8')

/** Large message — repetitive text that compresses very well (~6 KB). */
const LARGE = Buffer.from(
  JSON.stringify({
    id: 'bench-large',
    messageType: 'add',
    metadata: {
      description: 'The quick brown fox jumps over the lazy dog. '.repeat(60),
      items: Array.from({ length: 80 }, (_, i) => ({
        id: `item-${i}`,
        value: `value-number-${i}`,
        enabled: i % 2 === 0,
      })),
    },
  }),
  'utf8',
)

describe('ZstdCodecHandler micro-benchmark', () => {
  it(`compress+decompress ${ITERATIONS}× small payload (${SMALL.byteLength} B) in under 2 s`, async () => {
    const t0 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const compressed = await handler.compress(SMALL)
      await handler.decompress(compressed)
    }
    const elapsed = performance.now() - t0
    const perOp = (elapsed / ITERATIONS).toFixed(2)
    console.log(
      `  small: ${ITERATIONS} round-trips in ${elapsed.toFixed(0)} ms (${perOp} ms/op, ` +
        `${((SMALL.byteLength * ITERATIONS) / elapsed / 1000).toFixed(2)} MB/s input)`,
    )
    expect(
      elapsed,
      `${ITERATIONS} round-trips took ${elapsed.toFixed(0)} ms — possible regression`,
    ).toBeLessThan(2000)
  })

  it(`compress+decompress ${ITERATIONS}× large payload (${LARGE.byteLength} B) in under 5 s`, async () => {
    const t0 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const compressed = await handler.compress(LARGE)
      await handler.decompress(compressed)
    }
    const elapsed = performance.now() - t0
    const perOp = (elapsed / ITERATIONS).toFixed(2)
    console.log(
      `  large: ${ITERATIONS} round-trips in ${elapsed.toFixed(0)} ms (${perOp} ms/op, ` +
        `${((LARGE.byteLength * ITERATIONS) / elapsed / 1000).toFixed(2)} MB/s input)`,
    )
    expect(
      elapsed,
      `${ITERATIONS} round-trips took ${elapsed.toFixed(0)} ms — possible regression`,
    ).toBeLessThan(5000)
  })
})
