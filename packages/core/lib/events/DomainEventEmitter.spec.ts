import { randomUUID } from 'node:crypto'

import { waitAndRetry } from '@lokalise/node-core'
import type { CommonEventDefinitionPublisherSchemaType } from '@message-queue-toolkit/schemas'
import type { AwilixContainer } from 'awilix'
import { afterAll, beforeAll, expect } from 'vitest'
import type { z } from 'zod'

import type { Dependencies } from '../../test/testContext'
import { registerDependencies, TestEvents } from '../../test/testContext'

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
  beforeAll(async () => {
    diContainer = await registerDependencies()
  })

  afterAll(async () => {
    await diContainer.dispose()
  })

  it('emits event to anyListener', async () => {
    const { eventEmitter } = diContainer.cradle
    const fakeListener = new FakeListener(diContainer.cradle.eventRegistry.supportedEvents)
    eventEmitter.onAny(fakeListener)

    const emittedEvent = await eventEmitter.emit(TestEvents.created, createdEventPayload)

    const processedEvent = await eventEmitter.handlerSpy.waitForMessageWithId<
      z.infer<typeof TestEvents.created.consumerSchema>
    >(emittedEvent.id)

    expect(processedEvent.message.type).toBe(TestEvents.created.consumerSchema.shape.type.value)

    await waitAndRetry(() => {
      return fakeListener.receivedEvents.length > 0
    })

    expect(fakeListener.receivedEvents).toHaveLength(1)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
  })

  it('emits event to anyListener with metadata', async () => {
    const { eventEmitter } = diContainer.cradle
    const fakeListener = new FakeListener(diContainer.cradle.eventRegistry.supportedEvents)
    eventEmitter.onAny(fakeListener)

    await eventEmitter.emit(TestEvents.created, createdEventPayload, {
      correlationId: 'dummy',
    })

    await waitAndRetry(() => {
      return fakeListener.receivedEvents.length > 0
    })

    expect(fakeListener.receivedEvents).toHaveLength(1)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
    expect(fakeListener.receivedMetadata).toHaveLength(1)
    expect(fakeListener.receivedMetadata[0]).toMatchObject({
      correlationId: 'dummy',
    })
  })

  it('emits event to singleListener', async () => {
    const { eventEmitter } = diContainer.cradle
    const fakeListener = new FakeListener(diContainer.cradle.eventRegistry.supportedEvents)
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
    const fakeListener = new FakeListener(diContainer.cradle.eventRegistry.supportedEvents)
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
