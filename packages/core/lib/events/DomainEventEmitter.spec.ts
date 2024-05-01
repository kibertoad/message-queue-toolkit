import { randomUUID } from 'node:crypto'

import { waitAndRetry } from '@lokalise/node-core'
import type { AwilixContainer } from 'awilix'
import { afterAll, beforeAll, expect } from 'vitest'

import type { Dependencies } from '../../test/testContext'
import { registerDependencies, TestEvents } from '../../test/testContext'

import type { CommonEventDefinitionSchemaType } from './eventTypes'
import { FakeListener } from './fakes/FakeListener'

const createdEventPayload: CommonEventDefinitionSchemaType<typeof TestEvents.created> = {
  payload: {
    message: 'msg',
  },
  type: 'entity.created',
  id: randomUUID(),
  metadata: {
    timestamp: new Date().toISOString(),
    originApp: 'de',
    producerApp: 'dede',
    schemaVersion: '1',
    correlationId: randomUUID(),
  },
}

const updatedEventPayload: CommonEventDefinitionSchemaType<typeof TestEvents.updated> = {
  ...createdEventPayload,
  type: 'entity.updated',
}

const expectedCreatedPayload = {
  id: expect.any(String),
  metadata: {
    timestamp: expect.any(String),
    correlationId: expect.any(String),
    originApp: 'de',
    producerApp: 'dede',
    schemaVersion: '1',
  },
  payload: {
    message: 'msg',
  },
  type: 'entity.created',
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

    await eventEmitter.emit(TestEvents.created, createdEventPayload)

    await waitAndRetry(() => {
      return fakeListener.receivedEvents.length > 0
    })

    expect(fakeListener.receivedEvents).toHaveLength(1)
    expect(fakeListener.receivedEvents[0]).toMatchObject(expectedCreatedPayload)
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
