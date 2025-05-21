import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { userConsumer } from './common/Dependencies.ts'
import { publisherManager } from './common/TestPublisherManager.ts'
import { UserConsumer } from './common/UserConsumer.js'

describe('Publish message', () => {
  beforeEach(async () => {
    await publisherManager.initRegisteredPublishers([UserConsumer.SUBSCRIBED_TOPIC_NAME])
    await userConsumer.start()
  })

  afterEach(async () => {
    await userConsumer.close()
  })

  it('Publishes a message', async () => {
    await publisherManager.publish(UserConsumer.SUBSCRIBED_TOPIC_NAME, {
      type: 'user.created',
      payload: {
        id: '456',
        name: 'Jane Doe',
      },
    })

    await publisherManager.publish(UserConsumer.SUBSCRIBED_TOPIC_NAME, {
      type: 'user.created',
      payload: {
        id: '123',
        name: 'John Doe',
      },
    })

    const receivedMessage = await userConsumer.handlerSpy.waitForMessage({
      type: 'user.created',
      payload: {
        name: 'John Doe',
      },
    })

    expect(receivedMessage.message.payload).toMatchInlineSnapshot(`
          {
            "id": "123",
            "name": "John Doe",
          }
        `)
  })
})
