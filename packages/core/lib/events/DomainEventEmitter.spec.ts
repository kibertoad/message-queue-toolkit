import { randomUUID } from 'node:crypto'
import { InternalError, stringValueSerializer } from '@lokalise/node-core'
import type { CommonEventDefinitionPublisherSchemaType } from '@message-queue-toolkit/schemas'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ErroredFakeListener } from '../../test/fakes/ErroredFakeListener.ts'
import { FlakyFakeListener } from '../../test/fakes/FlakyFakeListener.ts'
import type { Dependencies, TestEventsType } from '../../test/testContext.ts'
import { registerDependencies, TestEvents } from '../../test/testContext.ts'
import type { DomainEventEmitter } from './DomainEventEmitter.ts'
import { FakeListener } from './fakes/FakeListener.ts'

// `emit()` stamps id/timestamp onto the payload it is given, so every test needs its own copy
const buildCreatedEventPayload = (): CommonEventDefinitionPublisherSchemaType<
  typeof TestEvents.created
> => ({
  payload: {
    message: 'msg',
  },
  type: 'entity.created',
  metadata: {
    originatedFrom: 'service',
    producedBy: 'producer',
    schemaVersion: '1',
    correlationId: randomUUID(),
  },
})

const buildUpdatedEventPayload = (): CommonEventDefinitionPublisherSchemaType<
  typeof TestEvents.updated
> => ({
  ...buildCreatedEventPayload(),
  type: 'entity.updated',
})

const expectedCreatedPayload = {
  id: expect.any(String),
  timestamp: expect.any(String),
  payload: {
    message: 'msg',
  },
  type: 'entity.created',
  metadata: {
    correlationId: expect.any(String),
    originatedFrom: 'service',
    producedBy: 'producer',
    schemaVersion: '1',
  },
}

const expectedUpdatedPayload = {
  ...expectedCreatedPayload,
  type: 'entity.updated',
}

describe('DomainEventEmitter', () => {
  let diContainer: AwilixContainer<Dependencies>
  let eventEmitter: DomainEventEmitter<TestEventsType>
  let createdEventPayload: CommonEventDefinitionPublisherSchemaType<typeof TestEvents.created>
  let updatedEventPayload: CommonEventDefinitionPublisherSchemaType<typeof TestEvents.updated>

  beforeEach(async () => {
    diContainer = await registerDependencies()
    eventEmitter = diContainer.cradle.eventEmitter
    createdEventPayload = buildCreatedEventPayload()
    updatedEventPayload = buildUpdatedEventPayload()
  })

  afterEach(async () => {
    await eventEmitter.dispose()
    await diContainer.dispose()
  })

  describe('emit', () => {
    it('emits event to anyListener - foreground', async () => {
      const fakeListener = new FakeListener()
      eventEmitter.onAny(fakeListener)
      const transactionManagerStartSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'startWithGroup',
      )
      const transactionManagerStopSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'stop',
      )

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)

      expect(fakeListener.receivedEvents).toHaveLength(1) // is processed synchronously so no need to wait
      expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)

      const processedEvent = await eventEmitter.handlerSpy.waitForMessageWithId(
        emittedEvent.id,
        'consumed',
      )
      expect(processedEvent.message.type).toBe(TestEvents.created.consumerSchema.shape.type.value)

      expect(transactionManagerStartSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStartSpy).toHaveBeenCalledWith(
        'fg_event_listener:entity.created:FakeListener',
        expect.any(String),
        'entity.created',
      )

      expect(transactionManagerStopSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStopSpy).toHaveBeenCalledWith(
        transactionManagerStartSpy.mock.calls[0]![1],
        true,
      )
    })

    it('emits event to anyListener - background', async () => {
      const fakeListener = new FakeListener(100)
      eventEmitter.onAny(fakeListener, true)
      const transactionManagerStartSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'startWithGroup',
      )
      const transactionManagerStopSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'stop',
      )

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      expect(fakeListener.receivedEvents).toHaveLength(0)

      const processedEvent = await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id)
      expect(processedEvent.message.type).toBe(TestEvents.created.consumerSchema.shape.type.value)
      // even thought event is consumed, the listener is still processing
      // Wait for the event to be processed
      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)

      expect(transactionManagerStartSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStartSpy).toHaveBeenCalledWith(
        'bg_event_listener:entity.created:FakeListener',
        expect.any(String),
        'entity.created',
      )

      expect(transactionManagerStopSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStopSpy).toHaveBeenCalledWith(
        transactionManagerStartSpy.mock.calls[0]![1],
        true,
      )
    })

    it('emits event to anyListener and populates metadata', async () => {
      const fakeListener = new FakeListener()
      eventEmitter.onAny(fakeListener)

      const emittedEvent = await eventEmitter.emit(TestEvents.created, {
        payload: {
          message: 'msg',
        },
      })

      const processedEvent = await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id)

      expect(processedEvent.message.type).toBe(TestEvents.created.consumerSchema.shape.type.value)
      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(fakeListener.receivedEvents[0]).toMatchObject({
        id: expect.any(String),
        payload: {
          message: 'msg',
        },
        timestamp: expect.any(String),
        type: 'entity.created',
      })
      expect(fakeListener.receivedEvents[0]!.metadata).toMatchObject({
        correlationId: expect.any(String),
        schemaVersion: '1.0.0',
        producedBy: 'test',
        originatedFrom: 'test',
      })
    })

    it('can check spy for messages not being sent', async () => {
      const fakeListener = new FakeListener()
      eventEmitter.onAny(fakeListener)

      await eventEmitter.emit(TestEvents.created, createdEventPayload)

      const notEmittedEvent = eventEmitter.handlerSpy.checkForMessage({
        type: 'entity.updated',
      })
      const emittedEvent = eventEmitter.handlerSpy.checkForMessage({
        type: 'entity.created',
      })

      expect(notEmittedEvent).toBeUndefined()
      expect(emittedEvent).toBeDefined()
    })

    it('emits event to anyListener with metadata', async () => {
      const fakeListener = new FakeListener()
      eventEmitter.onAny(fakeListener)

      const partialCreatedEventPayload = {
        ...createdEventPayload,
        metadata: {
          ...createdEventPayload.metadata,
          producedBy: undefined,
        },
      }
      await eventEmitter.emit(TestEvents.created, partialCreatedEventPayload, {
        correlationId: 'dummy',
      })

      const emitResult = await eventEmitter.handlerSpy.waitForMessage({
        type: 'entity.created',
      })

      expect(emitResult.message).toEqual({
        id: expect.any(String),
        metadata: {
          correlationId: createdEventPayload.metadata!.correlationId!,
          originatedFrom: 'service',
          producedBy: undefined,
          schemaVersion: '1',
        },
        payload: {
          message: 'msg',
        },
        timestamp: expect.any(String),
        type: 'entity.created',
      })
      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(fakeListener.receivedEvents[0]).toMatchObject({
        id: expect.any(String),
        metadata: {
          correlationId: expect.any(String),
          originatedFrom: 'service',
          producedBy: undefined,
          schemaVersion: '1',
        },
        payload: {
          message: 'msg',
        },
        timestamp: expect.any(String),
        type: 'entity.created',
      })
    })

    it('emits event to singleListener - foreground', async () => {
      const fakeListener = new FakeListener()
      eventEmitter.on('entity.created', fakeListener)
      const transactionManagerStartSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'startWithGroup',
      )
      const transactionManagerStopSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'stop',
      )

      await eventEmitter.emit(TestEvents.created, createdEventPayload)

      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)

      expect(transactionManagerStartSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStartSpy).toHaveBeenCalledWith(
        'fg_event_listener:entity.created:FakeListener',
        expect.any(String),
        'entity.created',
      )

      expect(transactionManagerStopSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStopSpy).toHaveBeenCalledWith(
        transactionManagerStartSpy.mock.calls[0]![1],
        true,
      )
    })

    it('emits event to singleListener - background', async () => {
      const fakeListener = new FakeListener(100)
      eventEmitter.on('entity.created', fakeListener, true)

      const transactionManagerStartSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'startWithGroup',
      )
      const transactionManagerStopSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'stop',
      )

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      expect(fakeListener.receivedEvents).toHaveLength(0)

      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')
      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)

      expect(transactionManagerStartSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStartSpy).toHaveBeenCalledWith(
        'bg_event_listener:entity.created:FakeListener',
        expect.any(String),
        'entity.created',
      )

      expect(transactionManagerStopSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStopSpy).toHaveBeenCalledWith(
        transactionManagerStartSpy.mock.calls[0]![1],
        true,
      )
    })

    it('emits event to manyListener - foreground', async () => {
      const fakeListener = new FakeListener()
      eventEmitter.onMany(['entity.created', 'entity.updated'], fakeListener)
      const transactionManagerStartSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'startWithGroup',
      )
      const transactionManagerStopSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'stop',
      )

      await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.emit(TestEvents.updated, updatedEventPayload)

      expect(fakeListener.receivedEvents).toHaveLength(2)
      expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
      expect(fakeListener.receivedEvents[1]).toMatchObject(expectedUpdatedPayload)

      expect(transactionManagerStartSpy).toHaveBeenCalledTimes(2)
      expect(transactionManagerStopSpy).toHaveBeenCalledTimes(2)
    })

    it('emits event to manyListener - background', async () => {
      const fakeListener = new FakeListener(100)
      eventEmitter.onMany(['entity.created', 'entity.updated'], fakeListener, true)
      const transactionManagerStartSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'startWithGroup',
      )
      const transactionManagerStopSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'stop',
      )

      const eventEmitted1 = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      const emittedEvent2 = await eventEmitter.emit(TestEvents.updated, updatedEventPayload)
      expect(fakeListener.receivedEvents).toHaveLength(0)

      await eventEmitter.handlerSpy.waitForMessageWithId(eventEmitted1.id, 'consumed')
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent2.id, 'consumed')

      expect(fakeListener.receivedEvents).toHaveLength(2)
      expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
      expect(fakeListener.receivedEvents[1]).toMatchObject(expectedUpdatedPayload)

      expect(transactionManagerStartSpy).toHaveBeenCalledTimes(2)
      expect(transactionManagerStopSpy).toHaveBeenCalledTimes(2)
    })

    it('foreground listener error handling', async () => {
      const fakeListener = new ErroredFakeListener()
      eventEmitter.onAny(fakeListener)
      const transactionManagerStartSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'startWithGroup',
      )
      const transactionManagerStopSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'stop',
      )

      await expect(eventEmitter.emit(TestEvents.created, createdEventPayload)).rejects.toThrow(
        'ErroredFakeListener error',
      )

      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)

      expect(transactionManagerStartSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStartSpy).toHaveBeenCalledWith(
        'fg_event_listener:entity.created:ErroredFakeListener',
        expect.any(String),
        'entity.created',
      )

      expect(transactionManagerStopSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStopSpy).toHaveBeenCalledWith(
        transactionManagerStartSpy.mock.calls[0]![1],
        false,
      )
    })

    it('background listener error handling', async () => {
      const fakeListener = new ErroredFakeListener(100)
      eventEmitter.onAny(fakeListener, true)
      const reporterSpy = vi.spyOn(diContainer.cradle.errorReporter, 'report')
      const logSpy = vi.spyOn(diContainer.cradle.logger, 'error')
      const transactionManagerStartSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'startWithGroup',
      )
      const transactionManagerStopSpy = vi.spyOn(
        diContainer.cradle.transactionObservabilityManager,
        'stop',
      )

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      expect(fakeListener.receivedEvents).toHaveLength(0)

      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')
      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)

      const expectedContext = {
        eventId: emittedEvent.id,
        eventType: 'entity.created',
        eventTimestamp: emittedEvent.timestamp,
        eventMetadata: stringValueSerializer(emittedEvent.metadata),
        eventHandlerId: 'ErroredFakeListener',
        'x-request-id': emittedEvent.metadata?.correlationId,
        attempts: 1,
      }
      expect(reporterSpy).toHaveBeenCalledWith({
        error: expect.any(Error),
        context: expectedContext,
      })
      expect(logSpy).toHaveBeenCalledWith({
        error: expect.anything(),
        msg: 'ErroredFakeListener error',
        ...expectedContext,
      })

      expect(transactionManagerStartSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStartSpy).toHaveBeenCalledWith(
        'bg_event_listener:entity.created:ErroredFakeListener',
        expect.any(String),
        'entity.created',
      )

      expect(transactionManagerStopSpy).toHaveBeenCalledOnce()
      expect(transactionManagerStopSpy).toHaveBeenCalledWith(
        transactionManagerStartSpy.mock.calls[0]![1],
        false,
      )
    })
  })

  describe('non-error throwables', () => {
    it('wraps them before reporting', async () => {
      const reporterSpy = vi.spyOn(diContainer.cradle.errorReporter, 'report')
      eventEmitter.onAny(
        {
          eventHandlerId: 'ThrowingListener',
          handleEvent: () => {
            throw 'boom'
          },
        },
        true,
      )

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')

      expect(reporterSpy).toHaveBeenCalledWith({
        error: expect.objectContaining({
          message: 'Event handler ThrowingListener threw a non-error value: ""boom""',
          cause: 'boom',
        }),
        context: expect.objectContaining({ eventHandlerId: 'ThrowingListener' }),
      })
    })
  })

  describe('retries', () => {
    const retryOptions = { maxRetries: 2, baseRetryDelayMs: 1, maxRetryDelayMs: 1 }

    it('retries a failing background handler until it succeeds', async () => {
      const fakeListener = new FlakyFakeListener(1)
      const reporterSpy = vi.spyOn(diContainer.cradle.errorReporter, 'report')
      eventEmitter.onAny(fakeListener, { isBackgroundHandler: true, retry: retryOptions })

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')

      expect(fakeListener.attempts).toBe(2)
      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(reporterSpy).not.toHaveBeenCalled()
    })

    it('reports the failure once retries are exhausted', async () => {
      const fakeListener = new ErroredFakeListener()
      const reporterSpy = vi.spyOn(diContainer.cradle.errorReporter, 'report')
      eventEmitter.onAny(fakeListener, { isBackgroundHandler: true, retry: retryOptions })

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')

      expect(fakeListener.receivedEvents).toHaveLength(3)
      expect(reporterSpy).toHaveBeenCalledOnce()
      expect(reporterSpy).toHaveBeenCalledWith({
        error: expect.any(Error),
        context: expect.objectContaining({
          eventHandlerId: 'ErroredFakeListener',
          attempts: 3,
        }),
      })
    })

    it('does not retry when the error is not retryable', async () => {
      const fakeListener = new ErroredFakeListener()
      const isRetryable = vi.fn().mockReturnValue(false)
      eventEmitter.onAny(fakeListener, {
        isBackgroundHandler: true,
        retry: { ...retryOptions, isRetryable },
      })

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')

      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(isRetryable).toHaveBeenCalledWith(expect.any(Error))
    })

    it('does not retry by default', async () => {
      const fakeListener = new ErroredFakeListener()
      eventEmitter.onAny(fakeListener, true)

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')

      expect(fakeListener.receivedEvents).toHaveLength(1)
    })

    it('retries foreground handlers and rethrows once exhausted', async () => {
      const fakeListener = new ErroredFakeListener()
      eventEmitter.onAny(fakeListener, { retry: retryOptions })

      await expect(eventEmitter.emit(TestEvents.created, createdEventPayload)).rejects.toThrow(
        'ErroredFakeListener error',
      )
      expect(fakeListener.receivedEvents).toHaveLength(3)
    })

    it('rejects invalid retry options at registration time', () => {
      const fakeListener = new FakeListener()

      for (const maxRetries of [Number.POSITIVE_INFINITY, Number.NaN, 1.5, -1, 6]) {
        expect(() => eventEmitter.onAny(fakeListener, { retry: { maxRetries } })).toThrow(
          /maxRetries must be an integer between 0 and 5/,
        )
      }
      expect(() => eventEmitter.onAny(fakeListener, { retry: { maxRetries: 5 } })).not.toThrow()
      expect(() =>
        eventEmitter.onAny(fakeListener, { retry: { maxRetries: undefined } }),
      ).not.toThrow()
      for (const baseRetryDelayMs of [-1, Number.NaN, 10_001]) {
        expect(() => eventEmitter.onAny(fakeListener, { retry: { baseRetryDelayMs } })).toThrow(
          /baseRetryDelayMs must be between 0 and 10000 ms/,
        )
      }
      expect(() =>
        eventEmitter.onAny(fakeListener, {
          retry: { maxRetryDelayMs: Number.POSITIVE_INFINITY },
        }),
      ).toThrow(/maxRetryDelayMs must be between 0 and 10000 ms/)
      expect(() =>
        // @ts-expect-error covering JS callers that ignore the type
        eventEmitter.onAny(fakeListener, { retry: { isRetryable: 'yes' } }),
      ).toThrow(/isRetryable must be a function/)
      expect(() =>
        // @ts-expect-error covering JS callers that ignore the type
        eventEmitter.onAny(fakeListener, { retry: { isRetryable: async () => true } }),
      ).toThrow(/isRetryable must be synchronous/)
      expect(() =>
        eventEmitter.onAny(fakeListener, {
          retry: { baseRetryDelayMs: 5000, maxRetryDelayMs: 1000 },
        }),
      ).toThrow(/baseRetryDelayMs \(5000\) must not exceed maxRetryDelayMs \(1000\)/)
      // an explicitly passed undefined still falls back to the parameter default
      expect(() => eventEmitter.onAny(fakeListener, undefined)).not.toThrow()
      for (const invalidOptions of [5, 0, 'background', '', () => {}, [], Symbol('x'), null]) {
        expect(() =>
          // @ts-expect-error covering JS callers that ignore the type
          eventEmitter.onAny(fakeListener, invalidOptions),
        ).toThrow(/registration options must be an object or a boolean/)
      }
    })

    it('stops retrying when isRetryable itself throws', async () => {
      const fakeListener = new ErroredFakeListener()
      const reporterSpy = vi.spyOn(diContainer.cradle.errorReporter, 'report')
      eventEmitter.onAny(fakeListener, {
        isBackgroundHandler: true,
        retry: {
          ...retryOptions,
          isRetryable: () => {
            throw new Error('predicate error')
          },
        },
      })

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')

      expect(fakeListener.receivedEvents).toHaveLength(1)
      expect(reporterSpy).toHaveBeenCalledWith({
        error: expect.objectContaining({ message: 'ErroredFakeListener error' }),
        context: expect.objectContaining({ attempts: 1 }),
      })
      await expect(eventEmitter.dispose()).resolves.toBeUndefined()
    })

    it('does not retry errors the emitter itself raises as permanent', async () => {
      const isRetryable = vi.fn().mockReturnValue(true)
      let attempts = 0
      eventEmitter.onAny(
        {
          eventHandlerId: 'DisposedEmitterListener',
          handleEvent: () => {
            attempts++
            throw new InternalError({
              errorCode: 'EVENT_EMITTER_DISPOSED',
              message: 'Cannot emit event, emitter is already disposed',
            })
          },
        },
        { isBackgroundHandler: true, retry: { ...retryOptions, isRetryable } },
      )

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')

      expect(attempts).toBe(1)
      expect(isRetryable).not.toHaveBeenCalled()
    })

    it('does not retry a publisher schema validation failure', async () => {
      const isRetryable = vi.fn().mockReturnValue(true)
      let attempts = 0
      eventEmitter.onMany(
        ['entity.created'],
        {
          eventHandlerId: 'InvalidFollowUpListener',
          handleEvent: async () => {
            attempts++
            await eventEmitter.emit(TestEvents.updated, {
              ...updatedEventPayload,
              // @ts-expect-error emitting a payload the publisher schema rejects
              payload: { message: 123 },
            })
          },
        },
        { isBackgroundHandler: true, retry: { ...retryOptions, isRetryable } },
      )

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')

      expect(attempts).toBe(1)
      expect(isRetryable).not.toHaveBeenCalled()
    })

    it('abandons pending backoffs when the emitter is disposed', async () => {
      const fakeListener = new ErroredFakeListener()
      eventEmitter.onAny(fakeListener, {
        isBackgroundHandler: true,
        retry: { maxRetries: 5, baseRetryDelayMs: 10_000, maxRetryDelayMs: 10_000 },
      })

      await eventEmitter.emit(TestEvents.created, createdEventPayload)
      expect(fakeListener.receivedEvents).toHaveLength(1)

      const disposeStart = Date.now()
      await eventEmitter.dispose()

      expect(Date.now() - disposeStart).toBeLessThan(1000)
      expect(fakeListener.receivedEvents).toHaveLength(1)
    })

    it('falls back to library defaults for options that are not provided', async () => {
      const fakeListener = new FlakyFakeListener(1)
      eventEmitter.onAny(fakeListener, {
        isBackgroundHandler: true,
        retry: { maxRetries: 1 },
      })

      const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id, 'consumed')

      expect(fakeListener.attempts).toBe(2)
      expect(fakeListener.receivedEvents).toHaveLength(1)
    })
  })

  describe('dispose', () => {
    it('nothing in progress and error is thrown', async () => {
      await expect(eventEmitter.dispose()).resolves.toBeUndefined()
    })

    it('BG work in progress so dispose should wait for handlers to be done', async () => {
      const fakeListener1 = new FakeListener(100)
      const fakeListener2 = new FakeListener(200)
      eventEmitter.onMany(['entity.created'], fakeListener1, true)
      eventEmitter.onMany(['entity.updated'], fakeListener2, true)

      await eventEmitter.emit(TestEvents.created, createdEventPayload)
      await eventEmitter.emit(TestEvents.updated, updatedEventPayload)
      expect(fakeListener1.receivedEvents).toHaveLength(0)
      expect(fakeListener2.receivedEvents).toHaveLength(0)

      await eventEmitter.dispose()

      expect(fakeListener1.receivedEvents).toHaveLength(1)
      expect(fakeListener2.receivedEvents).toHaveLength(1)
      expect(fakeListener1.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
      expect(fakeListener2.receivedEvents[0]).toMatchObject(expectedUpdatedPayload)
    })

    it('rejects emitting after dispose instead of dropping the event', async () => {
      const fakeListener = new FakeListener()
      eventEmitter.onAny(fakeListener)

      await eventEmitter.dispose()

      await expect(
        eventEmitter.emit(TestEvents.created, createdEventPayload),
      ).rejects.toMatchObject({ errorCode: 'EVENT_EMITTER_DISPOSED' })
      expect(fakeListener.receivedEvents).toHaveLength(0)
    })

    it('awaits background handlers that register while draining', async () => {
      // The chaining listener emits a follow-up event from within a background handler, so the
      // follow-up's own background handler only registers once dispose() started draining.
      const chainedListener = new FakeListener(100)
      eventEmitter.onMany(['entity.updated'], chainedListener, true)
      eventEmitter.onMany(
        ['entity.created'],
        {
          eventHandlerId: 'ChainingListener',
          handleEvent: async () => {
            await eventEmitter.emit(TestEvents.updated, updatedEventPayload)
          },
        },
        true,
      )

      await eventEmitter.emit(TestEvents.created, createdEventPayload)
      expect(chainedListener.receivedEvents).toHaveLength(0)

      await eventEmitter.dispose()

      expect(chainedListener.receivedEvents).toHaveLength(1)
    })

    it('awaits an event still running its foreground handlers when draining starts', async () => {
      // An event is only tracked as an in-progress background handler once its foreground handlers
      // have completed, so disposal starting mid-foreground must not miss it.
      const backgroundListener = new FakeListener(100)
      eventEmitter.onMany(['entity.created'], backgroundListener, true)

      let releaseForegroundHandler: () => void = () => {}
      const foregroundHandlerGate = new Promise<void>((resolve) => {
        releaseForegroundHandler = resolve
      })
      eventEmitter.onMany(['entity.created'], {
        eventHandlerId: 'GatedForegroundListener',
        handleEvent: () => foregroundHandlerGate,
      })

      // Not awaited: emit() only resolves once the gated foreground handler is released
      const emitPromise = eventEmitter.emit(TestEvents.created, createdEventPayload)
      expect(backgroundListener.receivedEvents).toHaveLength(0)

      const disposePromise = eventEmitter.dispose()
      releaseForegroundHandler()
      await disposePromise

      expect(backgroundListener.receivedEvents).toHaveLength(1)
      await emitPromise
    })
  })
})
