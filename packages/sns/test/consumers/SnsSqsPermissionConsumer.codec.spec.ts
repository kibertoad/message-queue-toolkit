import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { SnsPermissionPublisher } from '../publishers/SnsPermissionPublisher.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SnsSqsPermissionConsumer } from './SnsSqsPermissionConsumer.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas.ts'

describe('SnsSqsPermissionConsumer - zstd codec', () => {
  let diContainer: AwilixContainer<Dependencies>
  let testAdmin: TestAwsResourceAdmin
  let publisher: SnsPermissionPublisher
  let consumer: SnsSqsPermissionConsumer

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionPublisher: asValue(() => undefined),
      permissionConsumer: asValue(() => undefined),
    })
    testAdmin = diContainer.cradle.testAdmin
  })

  beforeEach(async () => {
    await testAdmin.deleteQueues(SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME)
    await testAdmin.deleteTopics(SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME)

    // No codec option needed — zstd is auto-registered on every consumer.
    consumer = new SnsSqsPermissionConsumer(diContainer.cradle)
    // skipCompressionBelow: 0 forces compression for these small test messages so the
    // suite genuinely exercises the codec path (they are well under the 512 B default).
    publisher = new SnsPermissionPublisher(diContainer.cradle, {
      codec: 'zstd',
      skipCompressionBelow: 0,
    })

    await consumer.start()
    await publisher.init()
  })

  afterEach(async () => {
    await publisher.close()
    await consumer.close()
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  it('publishes a compressed SNS message and consumer decompresses it correctly', async () => {
    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'sns-codec-test-1',
      messageType: 'add',
      metadata: { info: 'hello sns zstd' },
    }

    await publisher.publish(message)

    const result = await consumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)
  }, 15000)

  it('consumer correctly handles multiple compressed SNS messages in sequence', async () => {
    const messages: PERMISSIONS_ADD_MESSAGE_TYPE[] = [
      { id: 'sns-codec-seq-1', messageType: 'add' },
      { id: 'sns-codec-seq-2', messageType: 'add' },
      { id: 'sns-codec-seq-3', messageType: 'add' },
    ]

    for (const msg of messages) {
      await publisher.publish(msg)
    }

    for (const msg of messages) {
      const result = await consumer.handlerSpy.waitForMessageWithId(msg.id, 'consumed')
      expect(result.message).toMatchObject(msg)
    }
  }, 15000)

  it('consumer without codec option auto-detects and decompresses zstd messages from SNS', async () => {
    // Capture resource handles before close() invalidates them
    const { queueUrl, topicArn, subscriptionArn } = consumer.subscriptionProps
    // Stop the beforeEach consumer so it cannot steal messages from the shared queue
    await consumer.close()

    // Consumer without explicit codec — decompression is auto-detected from envelope __mqtCodec field
    const autoConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
      locatorConfig: {
        queueUrl,
        topicArn,
        subscriptionArn,
      },
    })
    await autoConsumer.start()
    // Reassign so afterEach closes autoConsumer instead of the already-closed consumer
    consumer = autoConsumer

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'sns-codec-auto-1',
      messageType: 'add',
    }
    await publisher.publish(message)

    const result = await autoConsumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)
  }, 15000)
})
