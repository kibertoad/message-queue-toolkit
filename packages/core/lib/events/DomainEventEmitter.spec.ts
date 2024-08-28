import { randomUUID } from 'node:crypto'

import { waitAndRetry } from '@lokalise/node-core'
import type {
  CommonEventDefinitionPublisherSchemaType,
  ConsumerMessageSchema,
} from '@message-queue-toolkit/schemas'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { Dependencies, TestEventsType } from '../../test/testContext'
import { TestEvents, registerDependencies } from '../../test/testContext'

import type { DomainEventEmitter } from './DomainEventEmitter'
import { FakeDelayedListener } from './fakes/FakeDelayedListener'
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

    const processedEvent = await eventEmitter.handlerSpy.waitForMessageWithId<
      ConsumerMessageSchema<typeof TestEvents.created>
    >(emittedEvent.id)

    expect(processedEvent.message.type).toBe(TestEvents.created.consumerSchema.shape.type.value)

    await waitAndRetry(() => {
      return fakeListener.receivedEvents.length > 0
    })

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

    const processedEvent = await eventEmitter.handlerSpy.waitForMessageWithId<
      ConsumerMessageSchema<typeof TestEvents.created>
    >(emittedEvent.id)

    expect(processedEvent.message.type).toBe(TestEvents.created.consumerSchema.shape.type.value)

    await waitAndRetry(() => {
      return fakeListener.receivedEvents.length > 0
    })

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

    const notEmittedEvent = eventEmitter.handlerSpy.checkForMessage<
      ConsumerMessageSchema<typeof TestEvents.updated>
    >({
      type: 'entity.updated',
    })
    const emittedEvent = eventEmitter.handlerSpy.checkForMessage<
      ConsumerMessageSchema<typeof TestEvents.created>
    >({
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

  it('emits event to singleListener', async () => {
    const fakeListener = new FakeListener()
    eventEmitter.on('entity.created', fakeListener)

    await eventEmitter.emit(TestEvents.created, createdEventPayload)

    await waitAndRetry(() => {
      return fakeListener.receivedEvents.length > 0
    })

    expect(fakeListener.receivedEvents).toHaveLength(1)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
  })

  it('emits event to manyListener', async () => {
    const { eventEmitter } = diContainer.cradle
    const fakeListener = new FakeListener()
    eventEmitter.onMany(['entity.created', 'entity.updated'], fakeListener)

    await eventEmitter.emit(TestEvents.created, createdEventPayload)
    await eventEmitter.emit(TestEvents.updated, updatedEventPayload)

    await waitAndRetry(() => {
      return fakeListener.receivedEvents.length === 2
    })

    expect(fakeListener.receivedEvents).toHaveLength(2)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
    expect(fakeListener.receivedEvents[1]).toMatchObject(expectedUpdatedPayload)
  })
})
