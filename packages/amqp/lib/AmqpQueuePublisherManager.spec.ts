import type { AwilixContainer } from 'awilix'
import { beforeAll, describe, expect, it } from 'vitest'

import { FakeQueueConsumer } from '../test/fakes/FakeQueueConsumer'
import { TEST_AMQP_CONFIG } from '../test/utils/testAmqpConfig'
import { TestEvents, registerDependencies } from '../test/utils/testContext'
import type { Dependencies } from '../test/utils/testContext'

describe('AmqpQueuePublisherManager', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    beforeAll(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG)
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

      expect(result.processingResult).toBe('consumed')
    })
  })
})
