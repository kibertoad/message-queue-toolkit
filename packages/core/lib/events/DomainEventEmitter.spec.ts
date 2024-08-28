import { randomUUID } from 'node:crypto'

import { waitAndRetry } from '@lokalise/node-core'
import type { CommonEventDefinitionPublisherSchemaType } from '@message-queue-toolkit/schemas'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { Dependencies, TestEventsType } from '../../test/testContext'
import { TestEvents, registerDependencies } from '../../test/testContext'

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

    const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)

    const processedEvent = await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id)

    expect(processedEvent.message.type).toBe(TestEvents.created.consumerSchema.shape.type.value)
    expect(fakeListener.receivedEvents).toHaveLength(1)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
  })

  it('emits event to anyListener - background', async () => {
    const fakeListener = new FakeListener(100)
    eventEmitter.onAny(fakeListener, true)

    const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)
    const processedEvent = await eventEmitter.handlerSpy.waitForMessageWithId(emittedEvent.id)

    expect(processedEvent.message.type).toBe(TestEvents.created.consumerSchema.shape.type.value)
    // even thought event is consumed, the listener is still processing
    expect(fakeListener.receivedEvents).toHaveLength(0)
    // Wait for the event to be processed
    await waitAndRetry(() => fakeListener.receivedEvents.length > 0)
    expect(fakeListener.receivedEvents).toHaveLength(1)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
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

    await eventEmitter.emit(TestEvents.created, createdEventPayload)

    expect(fakeListener.receivedEvents).toHaveLength(1)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
  })

  it('emits event to singleListener - background', async () => {
    const fakeListener = new FakeListener(100)
    eventEmitter.on('entity.created', fakeListener, true)

    await eventEmitter.emit(TestEvents.created, createdEventPayload)

    // even thought event is consumed, the listener is still processing
    expect(fakeListener.receivedEvents).toHaveLength(0)
    // Wait for the event to be processed
    await waitAndRetry(() => fakeListener.receivedEvents.length > 0)
    expect(fakeListener.receivedEvents).toHaveLength(1)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
  })

  it('emits event to manyListener - foreground', async () => {
    const { eventEmitter } = diContainer.cradle
    const fakeListener = new FakeListener()
    eventEmitter.onMany(['entity.created', 'entity.updated'], fakeListener)

    await eventEmitter.emit(TestEvents.created, createdEventPayload)
    await eventEmitter.emit(TestEvents.updated, updatedEventPayload)

    expect(fakeListener.receivedEvents).toHaveLength(2)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
    expect(fakeListener.receivedEvents[1]).toMatchObject(expectedUpdatedPayload)
  })

  it('emits event to manyListener - background', async () => {
    const { eventEmitter } = diContainer.cradle
    const fakeListener = new FakeListener(100)
    eventEmitter.onMany(['entity.created', 'entity.updated'], fakeListener, true)

    await eventEmitter.emit(TestEvents.created, createdEventPayload)
    await eventEmitter.emit(TestEvents.updated, updatedEventPayload)

    expect(fakeListener.receivedEvents).toHaveLength(0)
    await waitAndRetry(() => fakeListener.receivedEvents.length === 2)

    expect(fakeListener.receivedEvents).toHaveLength(2)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
    expect(fakeListener.receivedEvents[1]).toMatchObject(expectedUpdatedPayload)
  })
})
