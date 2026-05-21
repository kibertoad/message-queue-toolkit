/**
 * Codec integration benchmarks — publish and consume throughput with vs without zstd compression.
 *
 * Run: pnpm --filter @message-queue-toolkit/sqs bench
 *
 * Each benchmark pre-fills queues (consume) or sends N messages (publish) and
 * measures wall-clock time, reporting msg/s and the overhead percentage.
 * All queues are deleted before and after each case.
 *
 * LIMITATION: N=50 against LocalStack means results are dominated by network
 * round-trips (~5–20 ms each), not compression CPU cost (~0.1–1 ms). These
 * numbers show end-to-end throughput, not codec overhead. For the codec CPU cost
 * in isolation see bench/codecMicro.bench.ts, which is assertable in CI.
 * These integration benchmarks print to console only and cannot catch regressions.
 */
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, beforeAll, describe, it } from 'vitest'

import { SqsPermissionConsumer } from '../test/consumers/SqsPermissionConsumer.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../test/consumers/userConsumerSchemas.ts'
import { SqsPermissionPublisher } from '../test/publishers/SqsPermissionPublisher.ts'
import type { TestAwsResourceAdmin } from '../test/utils/testAdmin.ts'
import type { Dependencies } from '../test/utils/testContext.ts'
import { registerDependencies } from '../test/utils/testContext.ts'

// ─── Configuration ────────────────────────────────────────────────────────────

const N = 50

/** Small message with minimal payload (~80 B serialised). */
const SMALL_META: undefined = undefined

/**
 * Large message with repetitive text (~6 KB serialised).
 * Repetitive content compresses very well, showing the realistic best case.
 */
const LARGE_META: Record<string, unknown> = {
  description: 'The quick brown fox jumps over the lazy dog. '.repeat(60),
  items: Array.from({ length: 80 }, (_, i) => ({
    id: `item-${i}`,
    value: `value-number-${i}`,
    enabled: i % 2 === 0,
  })),
}

const CASES = [
  { label: 'small payload (~80 B) ', suffix: 'sm', meta: SMALL_META },
  { label: 'large payload (~6 KB) ', suffix: 'lg', meta: LARGE_META },
] as const

// ─── Helpers ──────────────────────────────────────────────────────────────────

function makeMessages(
  prefix: string,
  count: number,
  meta: Record<string, unknown> | undefined,
): PERMISSIONS_ADD_MESSAGE_TYPE[] {
  return Array.from({ length: count }, (_, i) => ({
    id: `${prefix}-${i}`,
    messageType: 'add' as const,
    ...(meta !== undefined ? { metadata: meta } : {}),
  }))
}

function printRow(label: string, count: number, plainMs: number, codecMs: number): void {
  const tps = (ms: number) => ((count / ms) * 1000).toFixed(0).padStart(6)
  const diff = codecMs - plainMs
  const pct = ((diff / plainMs) * 100).toFixed(1)
  const sign = diff >= 0 ? '+' : ''
  console.log(
    `  ${label}` +
      `  plain: ${String(plainMs.toFixed(0)).padStart(5)} ms (${tps(plainMs)} msg/s)` +
      `  zstd: ${String(codecMs.toFixed(0)).padStart(5)} ms (${tps(codecMs)} msg/s)` +
      `  overhead: ${sign}${pct}%`,
  )
}

// ─── Suite ────────────────────────────────────────────────────────────────────

describe('SQS codec benchmarks', () => {
  let diContainer: AwilixContainer<Dependencies>
  let testAdmin: TestAwsResourceAdmin

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionPublisher: asValue(() => undefined),
      permissionConsumer: asValue(() => undefined),
    })
    testAdmin = diContainer.cradle.testAdmin
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  // ── Publish ────────────────────────────────────────────────────────────────

  it(`publish: with vs without zstd  (${N} messages)`, async () => {
    console.log(`\n${'─'.repeat(72)}`)
    console.log(`  PUBLISH BENCHMARK  —  ${N} messages per run`)
    console.log('─'.repeat(72))

    for (const { label, suffix, meta } of CASES) {
      const plainQ = `bench-pub-plain-${suffix}`
      const codecQ = `bench-pub-codec-${suffix}`
      await testAdmin.deleteQueues(plainQ, codecQ)

      const plainMsgs = makeMessages('bpp', N, meta)
      const codecMsgs = makeMessages('bcp', N, meta)

      // ── Plain publish ──
      const plainPub = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: { queue: { QueueName: plainQ } },
      })
      await plainPub.init()
      const t0 = performance.now()
      for (const msg of plainMsgs) await plainPub.publish(msg)
      const plainMs = performance.now() - t0
      await plainPub.close()

      // ── Codec publish ──
      const codecPub = new SqsPermissionPublisher(diContainer.cradle, {
        codec: 'zstd',
        creationConfig: { queue: { QueueName: codecQ } },
      })
      await codecPub.init()
      const t1 = performance.now()
      for (const msg of codecMsgs) await codecPub.publish(msg)
      const codecMs = performance.now() - t1
      await codecPub.close()

      await testAdmin.deleteQueues(plainQ, codecQ)
      printRow(label, N, plainMs, codecMs)
    }
  }, 120_000)

  // ── Consume ────────────────────────────────────────────────────────────────

  it(`consume: with vs without zstd  (${N} messages)`, async () => {
    console.log(`\n${'─'.repeat(72)}`)
    console.log(`  CONSUME BENCHMARK  —  ${N} messages per run`)
    console.log('─'.repeat(72))

    for (const { label, suffix, meta } of CASES) {
      const plainQ = `bench-con-plain-${suffix}`
      const codecQ = `bench-con-codec-${suffix}`
      await testAdmin.deleteQueues(plainQ, codecQ)

      const plainMsgs = makeMessages('bpc', N, meta)
      const codecMsgs = makeMessages('bcc', N, meta)

      // ── Pre-fill plain queue ──
      const plainPub = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: { queue: { QueueName: plainQ } },
      })
      await plainPub.init()
      for (const msg of plainMsgs) await plainPub.publish(msg)
      await plainPub.close()

      // ── Pre-fill codec queue ──
      const codecPub = new SqsPermissionPublisher(diContainer.cradle, {
        codec: 'zstd',
        creationConfig: { queue: { QueueName: codecQ } },
      })
      await codecPub.init()
      for (const msg of codecMsgs) await codecPub.publish(msg)
      await codecPub.close()

      // ── Measure plain consume ──
      // deletionConfig: { deleteIfExists: false } preserves the pre-filled queue
      const plainCon = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: plainQ } },
        deletionConfig: { deleteIfExists: false },
      })
      await plainCon.start()
      const t2 = performance.now()
      await Promise.all(
        plainMsgs.map((m) => plainCon.handlerSpy.waitForMessageWithId(m.id, 'consumed')),
      )
      const plainMs = performance.now() - t2
      await plainCon.close(true)

      // ── Measure codec consume ──
      // No codec option needed — consumers auto-detect envelopes from __mqtCodec.
      const codecCon = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: codecQ } },
        deletionConfig: { deleteIfExists: false },
      })
      await codecCon.start()
      const t3 = performance.now()
      await Promise.all(
        codecMsgs.map((m) => codecCon.handlerSpy.waitForMessageWithId(m.id, 'consumed')),
      )
      const codecMs = performance.now() - t3
      await codecCon.close(true)

      await testAdmin.deleteQueues(plainQ, codecQ)
      printRow(label, N, plainMs, codecMs)
    }
  }, 120_000)
})
