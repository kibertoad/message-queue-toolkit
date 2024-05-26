import type { AwilixContainer } from 'awilix'
import { beforeAll } from 'vitest'

import { FakeTopicConsumer } from '../test/fakes/FakeTopicConsumer'
import { TEST_AMQP_CONFIG } from '../test/utils/testAmqpConfig'
import { registerDependencies, TestEvents } from '../test/utils/testContext'
import type { Dependencies } from '../test/utils/testContext'

describe('AmqpTopicPublisherManager', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    beforeAll(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG)
    })

    it('publishes to the correct queues', async () => {
      const { topicPublisherManager } = diContainer.cradle
      const fakeConsumer = new FakeTopicConsumer(diContainer.cradle, TestEvents.updatedPubSub, {
        queueName: 'queue1',
        topicPattern: 'topic1',
      })
      await fakeConsumer.start()
      const fakeConsumer2 = new FakeTopicConsumer(diContainer.cradle, TestEvents.updatedPubSub, {
        queueName: 'queue2',
        topicPattern: 'topic2',
      })
      await fakeConsumer2.start()
      const fakeConsumer3 = new FakeTopicConsumer(diContainer.cradle, TestEvents.updatedPubSub, {
        queueName: 'queue3',
        topicPattern: 'topic1',
      })
      await fakeConsumer3.start()

      const publishedMessage = topicPublisherManager.publishSync(
        TestEvents.updatedPubSub.exchange,
        {
          type: 'entity.updated',
          payload: {
            updatedData: 'msg',
          },
        },
        {
          routingKey: 'topic1',
        },
      )

      const result = await fakeConsumer.handlerSpy.waitForMessageWithId(publishedMessage.id)
      const result2 = await fakeConsumer3.handlerSpy.waitForMessageWithId(publishedMessage.id)
      expect(result.processingResult).toBe('consumed')
      expect(result2.processingResult).toBe('consumed')
      expect(fakeConsumer.messageCounter).toEqual(1)
      expect(fakeConsumer2.messageCounter).toEqual(0)
      expect(fakeConsumer3.messageCounter).toEqual(1)
    })
  })
})
