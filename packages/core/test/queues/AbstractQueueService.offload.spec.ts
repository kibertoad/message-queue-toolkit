/**
 * Regression tests for `AbstractQueueService.offloadMessagePayloadIfNeeded`.
 *
 * Identity fields (`messageIdField`, `messageTimestampField`, `messageDeduplicationIdField`,
 * `messageDeduplicationOptionsField`) all have defaulted names ('id', 'timestamp', ...) and
 * are unconditionally copied to the offloaded payload when present on the source message.
 * The `type` field — which downstream subscriptions rely on for routing/filtering, e.g. SNS
 * `FilterPolicyScope: 'MessageBody'` — must be handled the same way: present-on-source ⇒
 * present-on-offloaded, regardless of which `messageTypeResolver` mode (or absence) is
 * configured. Otherwise large messages get silently dropped by SNS subscription filters.
 */
import type { CommonLogger, ErrorReporter } from '@lokalise/node-core'
import type { Either } from '@lokalise/node-core'
import { describe, expect, it } from 'vitest'
import type { ZodSchema } from 'zod/v4'
import type { MessageInvalidFormatError, MessageValidationError } from '../../lib/errors/Errors.ts'
import type { OffloadedPayloadPointerPayload } from '../../lib/payload-store/offloadedPayloadMessageSchemas.ts'
import type { PayloadStore } from '../../lib/payload-store/payloadStoreTypes.ts'
import {
  AbstractQueueService,
  type ResolvedMessage,
} from '../../lib/queues/AbstractQueueService.ts'
import type { MessageTypeResolverConfig } from '../../lib/queues/MessageTypeResolver.ts'
import type { QueueDependencies } from '../../lib/types/queueOptionsTypes.ts'

type TestMessage = { type?: string; id: string; timestamp: string; payload: unknown }

class TestQueueService extends AbstractQueueService<
  TestMessage,
  TestMessage,
  QueueDependencies,
  Record<string, never>
> {
  protected resolveSchema(): Either<Error, ZodSchema<TestMessage>> {
    throw new Error('not used in this test')
  }
  protected resolveMessage(): Either<
    MessageInvalidFormatError | MessageValidationError,
    ResolvedMessage
  > {
    throw new Error('not used in this test')
  }
  protected resolveNextFunction(): () => void {
    throw new Error('not used in this test')
  }
  protected processPrehandlers(): Promise<undefined> {
    throw new Error('not used in this test')
  }
  protected preHandlerBarrier<BarrierOutput>(): Promise<{
    isPassing: boolean
    output?: BarrierOutput
  }> {
    throw new Error('not used in this test')
  }
  processMessage(): Promise<Either<'retryLater', 'success'>> {
    throw new Error('not used in this test')
  }
  public close(): Promise<unknown> {
    return Promise.resolve()
  }

  // Expose protected method for direct testing.
  public callOffload(message: TestMessage, sizeFn: () => number) {
    return this.offloadMessagePayloadIfNeeded(message, sizeFn)
  }
}

const noopLogger: CommonLogger = {
  level: 'silent',
  fatal: () => undefined,
  error: () => undefined,
  warn: () => undefined,
  info: () => undefined,
  debug: () => undefined,
  trace: () => undefined,
  silent: () => undefined,
  child: () => noopLogger,
} as unknown as CommonLogger

const noopReporter: ErrorReporter = { report: () => undefined }

const recordingStore = (storedKey: string): PayloadStore => ({
  storePayload: () => Promise.resolve(storedKey),
  retrievePayload: () => Promise.resolve(null),
})

const buildService = (messageTypeResolver?: MessageTypeResolverConfig) =>
  new TestQueueService(
    { errorReporter: noopReporter, logger: noopLogger },
    {
      messageTypeResolver,
      // any threshold; we'll always force size > threshold via sizeFn
      payloadStoreConfig: {
        messageSizeThreshold: 1,
        store: recordingStore('payload-id-1'),
        storeName: 's3',
      },
    },
  )

const baseMessage: TestMessage = {
  id: 'msg-1',
  timestamp: '2026-01-01T00:00:00.000Z',
  type: 'order.created',
  payload: { large: 'data' },
}

describe('AbstractQueueService.offloadMessagePayloadIfNeeded — `type` preservation', () => {
  it('preserves `type` when no messageTypeResolver is configured', async () => {
    const svc = buildService(undefined)
    const result = (await svc.callOffload(baseMessage, () => 9999)) as OffloadedPayloadPointerPayload
    expect((result as unknown as TestMessage).type).toBe('order.created')
    expect(result.payloadRef?.id).toBe('payload-id-1')
    expect(result.id).toBe('msg-1')
    expect(result.timestamp).toBe('2026-01-01T00:00:00.000Z')
  })

  it('preserves `type` when messageTypeResolver is `literal` mode', async () => {
    const svc = buildService({ literal: 'order.created' })
    const result = (await svc.callOffload(baseMessage, () => 9999)) as OffloadedPayloadPointerPayload
    expect((result as unknown as TestMessage).type).toBe('order.created')
  })

  it('preserves `type` when messageTypeResolver is custom `resolver` mode', async () => {
    const svc = buildService({ resolver: () => 'order.created' })
    const result = (await svc.callOffload(baseMessage, () => 9999)) as OffloadedPayloadPointerPayload
    expect((result as unknown as TestMessage).type).toBe('order.created')
  })

  it('preserves `type` at the configured path when messageTypeResolver is `messageTypePath`', async () => {
    const svc = buildService({ messageTypePath: 'type' })
    const result = (await svc.callOffload(baseMessage, () => 9999)) as OffloadedPayloadPointerPayload
    expect((result as unknown as TestMessage).type).toBe('order.created')
  })

  it('preserves `type` at a non-default nested path when configured via `messageTypePath`', async () => {
    const svc = buildService({ messageTypePath: 'metadata.eventName' })
    const message = {
      id: 'msg-2',
      timestamp: '2026-01-01T00:00:00.000Z',
      payload: 'p',
      metadata: { eventName: 'shipment.dispatched' },
    } as unknown as TestMessage
    const result = (await svc.callOffload(message, () => 9999)) as OffloadedPayloadPointerPayload
    expect((result as unknown as { metadata: { eventName: string } }).metadata.eventName).toBe(
      'shipment.dispatched',
    )
  })

  it('does not invent a `type` when the source message has none', async () => {
    const svc = buildService(undefined)
    const message: TestMessage = {
      id: 'msg-3',
      timestamp: '2026-01-01T00:00:00.000Z',
      payload: 'p',
    }
    const result = (await svc.callOffload(message, () => 9999)) as OffloadedPayloadPointerPayload
    expect((result as unknown as TestMessage).type).toBeUndefined()
  })
})
