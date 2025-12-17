import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { FakeQueueConsumer } from '../test/fakes/FakeQueueConsumer.ts'
import { TEST_AMQP_CONFIG } from '../test/utils/testAmqpConfig.ts'
import type { Dependencies } from '../test/utils/testContext.ts'
import { registerDependencies, TestEvents } from '../test/utils/testContext.ts'

describe('AmqpQueuePublisherManager', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>

    beforeEach(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG)
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('publishes to the correct queue', async () => {
      const { queuePublisherManager } = diContainer.cradle
      const fakeConsumer = new FakeQueueConsumer(diContainer.cradle, TestEvents.updated)
      await fakeConsumer.start()

      const publishedMessage = queuePublisherManager.publishSync(FakeQueueConsumer.QUEUE_NAME, {
        type: 'entity.updated',
        payload: {
          updatedData: 'msg',
        },
      })

      const result = await fakeConsumer.handlerSpy.waitForMessageWithId(publishedMessage.id)

      expect(result.processingResult).toEqual({ status: 'consumed' })
    })

    it('fills incomplete metadata', async () => {
      const { queuePublisherManager } = diContainer.cradle
      const fakeConsumer = new FakeQueueConsumer(diContainer.cradle, TestEvents.updated)
      await fakeConsumer.start()

      const publishedMessage = queuePublisherManager.publishSync(FakeQueueConsumer.QUEUE_NAME, {
        type: 'entity.updated',
        payload: {
          updatedData: 'msg',
        },
        metadata: {
          correlationId: 'some-id',
        },
      })

      const result = await fakeConsumer.handlerSpy.waitForMessageWithId(publishedMessage.id)

      expect(result.processingResult).toEqual({ status: 'consumed' })
      expect(result.message.metadata).toMatchInlineSnapshot(`
        {
          "correlationId": "some-id",
          "originatedFrom": "service",
          "producedBy": "service",
          "schemaVersion": "1.0.0",
        }
      `)
    })

    it('skips lazy init if not enabled', async () => {
      const { queuePublisherManagerNoLazy } = diContainer.cradle
      const fakeConsumer = new FakeQueueConsumer(diContainer.cradle, TestEvents.updated)
      await fakeConsumer.start()

      expect(() =>
        queuePublisherManagerNoLazy.publishSync(FakeQueueConsumer.QUEUE_NAME, {
          type: 'entity.updated',
          payload: {
            updatedData: 'msg',
          },
          metadata: {
            correlationId: 'some-id',
          },
        }),
      ).toThrow(/Error while publishing to AMQP Cannot read properties of undefined/)
    })

    it('publishes to the correct queue with lazy init disabled', async () => {
      await diContainer.cradle.queuePublisherManagerNoLazy.initRegisteredPublishers()

      const { queuePublisherManagerNoLazy } = diContainer.cradle
      const fakeConsumer = new FakeQueueConsumer(diContainer.cradle, TestEvents.updated)
      await fakeConsumer.start()

      const publishedMessage = queuePublisherManagerNoLazy.publishSync(
        FakeQueueConsumer.QUEUE_NAME,
        {
          type: 'entity.updated',
          payload: {
            updatedData: 'msg',
          },
        },
      )

      const result = await fakeConsumer.handlerSpy.waitForMessageWithId(publishedMessage.id)

      expect(result.processingResult).toEqual({ status: 'consumed' })
    })

    it('not publishes to the queue with lazy publisher, when it was not initialized', async () => {
      await diContainer.cradle.queuePublisherManagerNoLazy.initRegisteredPublishers([])

      const { queuePublisherManagerNoLazy } = diContainer.cradle
      const fakeConsumer = new FakeQueueConsumer(diContainer.cradle, TestEvents.updated)
      await fakeConsumer.start()

      expect(() =>
        queuePublisherManagerNoLazy.publishSync(FakeQueueConsumer.QUEUE_NAME, {
          type: 'entity.updated',
          payload: {
            updatedData: 'msg',
          },
          metadata: {
            correlationId: 'some-id',
          },
        }),
      ).toThrow(/Error while publishing to AMQP Cannot read properties of undefined/)
    })

    it('throws when deprecated publish method is called', () => {
      const { queuePublisherManager } = diContainer.cradle

      expect(() => queuePublisherManager.publish()).toThrow(
        'Please use `publishSync` method for AMQP publisher managers',
      )
    })

    it('throws when publishing to unknown queue', () => {
      const { queuePublisherManager } = diContainer.cradle

      expect(() =>
        queuePublisherManager.publishSync('unknown-queue' as any, {
          type: 'entity.updated',
          payload: {
            updatedData: 'msg',
          },
        }),
      ).toThrow('No publisher for queue unknown-queue')
    })
  })
})
