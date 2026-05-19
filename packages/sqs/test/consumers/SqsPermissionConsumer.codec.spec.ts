import { ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs'
import { compressMessageBody } from '@message-queue-toolkit/codec'
import { MessageCodecEnum } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SqsPermissionConsumer } from './SqsPermissionConsumer.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas.ts'

describe('SqsPermissionConsumer - zstd codec', () => {
  let diContainer: AwilixContainer<Dependencies>
  let testAdmin: TestAwsResourceAdmin
  let publisher: SqsPermissionPublisher
  let consumer: SqsPermissionConsumer

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionPublisher: asValue(() => undefined),
      permissionConsumer: asValue(() => undefined),
    })
    testAdmin = diContainer.cradle.testAdmin
  })

  beforeEach(async () => {
    await testAdmin.deleteQueues(SqsPermissionConsumer.QUEUE_NAME)

    consumer = new SqsPermissionConsumer(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      deletionConfig: { deleteIfExists: false },
    })
    publisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
    })

    await consumer.start()
    await publisher.init()
  })

  afterEach(async () => {
    await publisher.close()
    await consumer.close(true)
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  it('publishes a compressed message and consumer decompresses it correctly', async () => {
    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-test-1',
      messageType: 'add',
      metadata: { info: 'hello zstd' },
    }

    await publisher.publish(message)

    const result = await consumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)
  })

  it('published SQS message body is a codec envelope containing valid zstd bytes', async () => {
    // Use an isolated queue with no consumer so we can read the raw message without a race
    const wireQueueName = `${SqsPermissionConsumer.QUEUE_NAME}-wire-check`
    await testAdmin.deleteQueues(wireQueueName)

    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      creationConfig: { queue: { QueueName: wireQueueName } },
    })
    await wirePublisher.init()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-wire-1',
      messageType: 'add',
    }
    await wirePublisher.publish(message)

    // Read the raw message directly from SQS — no consumer is running on this queue
    const { Messages } = await diContainer.cradle.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: wirePublisher.queueProps.url,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      }),
    )
    expect(Messages, 'Expected a message to be in the queue').toBeDefined()
    expect(Messages!.length).toBe(1)

    // Body must be a self-describing codec envelope, not raw message JSON
    const envelope = JSON.parse(Messages![0]!.Body!) as Record<string, unknown>
    expect(envelope.__codec).toBe(MessageCodecEnum.ZSTD)
    expect(typeof envelope.__data).toBe('string')

    // __data must decode to a valid zstd frame: magic number 0xFD2FB528 (LE → 28 B5 2F FD)
    const compressed = Buffer.from(envelope.__data as string, 'base64')
    expect(compressed.subarray(0, 4)).toEqual(Buffer.from([0x28, 0xb5, 0x2f, 0xfd]))

    await wirePublisher.close()
  })

  it('consumer correctly handles multiple compressed messages in sequence', async () => {
    const messages: PERMISSIONS_ADD_MESSAGE_TYPE[] = [
      { id: 'codec-seq-1', messageType: 'add' },
      { id: 'codec-seq-2', messageType: 'add' },
      { id: 'codec-seq-3', messageType: 'add' },
    ]

    for (const msg of messages) {
      await publisher.publish(msg)
    }

    for (const msg of messages) {
      const result = await consumer.handlerSpy.waitForMessageWithId(msg.id, 'consumed')
      expect(result.message).toMatchObject(msg)
    }
  })

  it('consumer decompresses a message compressed externally with zstd', async () => {
    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-external-1',
      messageType: 'add',
      metadata: { source: 'external-compressor' },
    }

    // Simulate a publisher that compressed the message itself
    const compressedBody = await compressMessageBody(JSON.stringify(message), MessageCodecEnum.ZSTD)
    await diContainer.cradle.sqsClient.send(
      new SendMessageCommand({
        QueueUrl: consumer.queueProps.url,
        MessageBody: compressedBody,
      }),
    )

    const result = await consumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)
  })

  it('consumer without codec option still decompresses zstd messages (auto-detection)', async () => {
    // Use a dedicated queue so only autoConsumer polls it — avoids both the race
    // condition (shared queue) and localstack long-poll timing issues (abort + restart)
    const autoQueueName = `${SqsPermissionConsumer.QUEUE_NAME}-auto-detect`
    await testAdmin.deleteQueues(autoQueueName)

    const autoPublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      creationConfig: { queue: { QueueName: autoQueueName } },
    })
    await autoPublisher.init()

    // Consumer without codec — auto-detects from envelope __codec field
    const autoConsumer = new SqsPermissionConsumer(diContainer.cradle, {
      creationConfig: { queue: { QueueName: autoQueueName } },
      deletionConfig: { deleteIfExists: false },
    })
    await autoConsumer.start()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-auto-detect-1',
      messageType: 'add',
    }
    await autoPublisher.publish(message)

    const result = await autoConsumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)

    await autoPublisher.close()
    await autoConsumer.close(true)
  }, 15000)
})
