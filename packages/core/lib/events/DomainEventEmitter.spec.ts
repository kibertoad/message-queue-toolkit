import { randomUUID } from 'node:crypto'

import type { CommonEventDefinitionPublisherSchemaType } from '@message-queue-toolkit/schemas'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { Dependencies, TestEventsType } from '../../test/testContext'
import { TestEvents, registerDependencies } from '../../test/testContext'

import { ErroredFakeListener } from '../../test/fakes/ErroredFakeListener'
import type { DomainEventEmitter } from './DomainEventEmitter'
import { FakeListener } from './fakes/FakeListener'

const createdEventPayload: CommonEventDefinitionPublisherSchemaType<typeof TestEvents.created> = {
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
}

const updatedEventPayload: CommonEventDefinitionPublisherSchemaType<typeof TestEvents.updated> = {
  ...createdEventPayload,
  type: 'entity.updated',
}

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

describe('AutopilotEventEmitter', () => {
  let diContainer: AwilixContainer<Dependencies>
  let eventEmitter: DomainEventEmitter<TestEventsType>

  beforeEach(async () => {
    diContainer = await registerDependencies()
    eventEmitter = diContainer.cradle.eventEmitter
  })

  afterEach(async () => {
    await diContainer.dispose()
  })

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
      transactionManagerStartSpy.mock.calls[0][1],
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
      transactionManagerStartSpy.mock.calls[0][1],
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
    expect(fakeListener.receivedEvents[0].metadata).toMatchObject({
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
      transactionManagerStartSpy.mock.calls[0][1],
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
      transactionManagerStartSpy.mock.calls[0][1],
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
      transactionManagerStartSpy.mock.calls[0][1],
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
      event: JSON.stringify(emittedEvent),
      eventHandlerId: 'ErroredFakeListener',
      'x-request-id': emittedEvent.metadata?.correlationId,
    }
    expect(reporterSpy).toHaveBeenCalledWith({
      error: expect.any(Error),
      context: expectedContext,
    })
    expect(logSpy).toHaveBeenCalledWith({
      error: expect.anything(),
      message: 'ErroredFakeListener error',
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
      transactionManagerStartSpy.mock.calls[0][1],
      false,
    )
  })
})
