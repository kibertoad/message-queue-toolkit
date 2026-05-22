import type { Transform } from 'node:stream'
import { PassThrough } from 'node:stream'
import { ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import { compressMessageBody } from '@message-queue-toolkit/codec'
import type { MessageCodecHandler } from '@message-queue-toolkit/core'
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

// Padding that pushes any test message's JSON representation above the default
// skipCompressionBelow threshold (512 bytes), ensuring compression is actually applied.
const LARGE_PADDING = 'x'.repeat(450)

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
    // Message is padded to exceed the default skipCompressionBelow (512 bytes) so that
    // compression actually fires. Without the padding the payload would be sent as plain
    // JSON and the wire assertion below would fail.
    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-test-1',
      messageType: 'add',
      metadata: { info: 'hello zstd', padding: LARGE_PADDING },
    }

    // Wire assertion: verify the message is actually sent as a codec envelope.
    // Uses an isolated queue with no consumer to avoid a race on the raw SQS body.
    const wireQueueName = `${SqsPermissionConsumer.QUEUE_NAME}-roundtrip-wire`
    await testAdmin.deleteQueues(wireQueueName)
    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      creationConfig: { queue: { QueueName: wireQueueName } },
    })
    await wirePublisher.init()
    await wirePublisher.publish(message)

    const { Messages: wireMessages } = await diContainer.cradle.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: wirePublisher.queueProps.url,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      }),
    )
    const envelope = JSON.parse(wireMessages![0]!.Body!) as Record<string, unknown>
    expect(envelope.__mqtCodec).toBe(MessageCodecEnum.ZSTD)
    expect(typeof envelope.__mqtData).toBe('string')
    const compressedBytes = Buffer.from(envelope.__mqtData as string, 'base64')
    expect(compressedBytes.subarray(0, 4)).toEqual(Buffer.from([0x28, 0xb5, 0x2f, 0xfd]))
    await wirePublisher.close()

    // Round-trip assertion: consumer receives and decompresses the message correctly.
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

    // Message must exceed the default skipCompressionBelow (512 bytes) for compression to fire.
    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-wire-1',
      messageType: 'add',
      metadata: { padding: LARGE_PADDING },
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
    expect(envelope.__mqtCodec).toBe(MessageCodecEnum.ZSTD)
    expect(typeof envelope.__mqtData).toBe('string')

    // __mqtData must decode to a valid zstd frame: magic number 0xFD2FB528 (LE → 28 B5 2F FD)
    const compressed = Buffer.from(envelope.__mqtData as string, 'base64')
    expect(compressed.subarray(0, 4)).toEqual(Buffer.from([0x28, 0xb5, 0x2f, 0xfd]))

    await wirePublisher.close()
  })

  it('consumer correctly handles multiple compressed messages in sequence', async () => {
    // Messages are padded to exceed the default skipCompressionBelow (512 bytes).
    const messages: PERMISSIONS_ADD_MESSAGE_TYPE[] = [
      { id: 'codec-seq-1', messageType: 'add', metadata: { padding: LARGE_PADDING } },
      { id: 'codec-seq-2', messageType: 'add', metadata: { padding: LARGE_PADDING } },
      { id: 'codec-seq-3', messageType: 'add', metadata: { padding: LARGE_PADDING } },
    ]

    // Wire assertion: verify each message is actually compressed on the wire.
    // Uses an isolated queue with no consumer to avoid a race on the raw SQS body.
    const wireQueueName = `${SqsPermissionConsumer.QUEUE_NAME}-seq-wire`
    await testAdmin.deleteQueues(wireQueueName)
    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      creationConfig: { queue: { QueueName: wireQueueName } },
    })
    await wirePublisher.init()

    for (const msg of messages) {
      await wirePublisher.publish(msg)
    }

    const { Messages: wireMessages } = await diContainer.cradle.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: wirePublisher.queueProps.url,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 5,
      }),
    )
    expect(wireMessages).toHaveLength(messages.length)
    for (const raw of wireMessages!) {
      const envelope = JSON.parse(raw.Body!) as Record<string, unknown>
      expect(envelope.__mqtCodec).toBe(MessageCodecEnum.ZSTD)
      expect(typeof envelope.__mqtData).toBe('string')
      const compressedBytes = Buffer.from(envelope.__mqtData as string, 'base64')
      expect(compressedBytes.subarray(0, 4)).toEqual(Buffer.from([0x28, 0xb5, 0x2f, 0xfd]))
    }
    await wirePublisher.close()

    // Round-trip assertion: consumer receives and decompresses all messages correctly.
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
    // Message is padded to exceed the default skipCompressionBelow (512 bytes) so that
    // the publisher actually compresses it. Without padding the message would be sent as
    // plain JSON and the auto-detect consumer would succeed trivially via normal parsing —
    // which would not prove decompression is working.
    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-auto-detect-1',
      messageType: 'add',
      metadata: { padding: LARGE_PADDING },
    }

    // Wire assertion: verify the publisher actually sends a codec envelope.
    // The auto-detect consumer would succeed on plain JSON too, so we need this
    // to prove decompression is actually happening rather than plain JSON parsing.
    const wireQueueName = `${SqsPermissionConsumer.QUEUE_NAME}-auto-detect-wire`
    await testAdmin.deleteQueues(wireQueueName)
    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      creationConfig: { queue: { QueueName: wireQueueName } },
    })
    await wirePublisher.init()
    await wirePublisher.publish(message)

    const { Messages: wireMessages } = await diContainer.cradle.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: wirePublisher.queueProps.url,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      }),
    )
    const envelope = JSON.parse(wireMessages![0]!.Body!) as Record<string, unknown>
    expect(envelope.__mqtCodec).toBe(MessageCodecEnum.ZSTD)
    expect(typeof envelope.__mqtData).toBe('string')
    await wirePublisher.close()

    // Round-trip assertion: consumer WITHOUT codec auto-detects the envelope and decompresses.
    // Use a dedicated queue so only autoConsumer polls it — avoids both the race
    // condition (shared queue) and localstack long-poll timing issues (abort + restart).
    const autoQueueName = `${SqsPermissionConsumer.QUEUE_NAME}-auto-detect`
    await testAdmin.deleteQueues(autoQueueName)

    const autoPublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      creationConfig: { queue: { QueueName: autoQueueName } },
    })
    await autoPublisher.init()

    // Consumer without codec — auto-detects from envelope __mqtCodec field
    const autoConsumer = new SqsPermissionConsumer(diContainer.cradle, {
      creationConfig: { queue: { QueueName: autoQueueName } },
      deletionConfig: { deleteIfExists: false },
    })
    await autoConsumer.start()

    await autoPublisher.publish(message)

    const result = await autoConsumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)

    await autoPublisher.close()
    await autoConsumer.close(true)
  }, 15000)
})

describe('SqsPermissionConsumer - skipCompressionBelow', () => {
  let diContainer: AwilixContainer<Dependencies>
  let testAdmin: TestAwsResourceAdmin

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionPublisher: asValue(() => undefined),
      permissionConsumer: asValue(() => undefined),
    })
    testAdmin = diContainer.cradle.testAdmin
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  it('sends plain JSON for small messages by default (no skipCompressionBelow set)', async () => {
    const queueName = `${SqsPermissionConsumer.QUEUE_NAME}-default-skip`
    await testAdmin.deleteQueues(queueName)

    // No skipCompressionBelow — default of 512 applies.
    // The small message (well under 512 bytes) must be sent as plain JSON.
    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      creationConfig: { queue: { QueueName: queueName } },
    })
    await wirePublisher.init()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'default-skip-1',
      messageType: 'add',
    }
    await wirePublisher.publish(message)

    const { Messages } = await diContainer.cradle.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: wirePublisher.queueProps.url,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      }),
    )
    expect(Messages, 'Expected a message to be in the queue').toBeDefined()
    expect(Messages!.length).toBe(1)

    const body = JSON.parse(Messages![0]!.Body!) as Record<string, unknown>
    expect(body.__mqtCodec).toBeUndefined()
    expect(body.__mqtData).toBeUndefined()
    expect(body.id).toBe(message.id)

    await wirePublisher.close()
  })

  it('sends plain JSON when message is smaller than skipCompressionBelow', async () => {
    const queueName = `${SqsPermissionConsumer.QUEUE_NAME}-skip-below`
    await testAdmin.deleteQueues(queueName)

    // skipCompressionBelow set very high — small message is never compressed
    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      skipCompressionBelow: 99_999,
      creationConfig: { queue: { QueueName: queueName } },
    })
    await wirePublisher.init()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'skip-below-1',
      messageType: 'add',
    }
    await wirePublisher.publish(message)

    const { Messages } = await diContainer.cradle.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: wirePublisher.queueProps.url,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      }),
    )
    expect(Messages, 'Expected a message to be in the queue').toBeDefined()
    expect(Messages!.length).toBe(1)

    const body = JSON.parse(Messages![0]!.Body!) as Record<string, unknown>
    expect(body.__mqtCodec).toBeUndefined()
    expect(body.__mqtData).toBeUndefined()
    expect(body.id).toBe(message.id)

    await wirePublisher.close()
  })

  it('compresses when skipCompressionBelow is 0 (always compress)', async () => {
    const queueName = `${SqsPermissionConsumer.QUEUE_NAME}-always-compress`
    await testAdmin.deleteQueues(queueName)

    // skipCompressionBelow: 0 disables the floor — every message is compressed
    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      skipCompressionBelow: 0,
      creationConfig: { queue: { QueueName: queueName } },
    })
    await wirePublisher.init()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'always-compress-1',
      messageType: 'add',
    }
    await wirePublisher.publish(message)

    const { Messages } = await diContainer.cradle.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: wirePublisher.queueProps.url,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      }),
    )
    expect(Messages, 'Expected a message to be in the queue').toBeDefined()
    expect(Messages!.length).toBe(1)

    const envelope = JSON.parse(Messages![0]!.Body!) as Record<string, unknown>
    expect(envelope.__mqtCodec).toBe(MessageCodecEnum.ZSTD)
    expect(typeof envelope.__mqtData).toBe('string')

    await wirePublisher.close()
  })

  it('consumer receives and processes a message sent as plain JSON due to skipCompressionBelow', async () => {
    const queueName = `${SqsPermissionConsumer.QUEUE_NAME}-skip-consumer`
    await testAdmin.deleteQueues(queueName)

    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      skipCompressionBelow: 99_999,
      creationConfig: { queue: { QueueName: queueName } },
    })
    await wirePublisher.init()

    const wireConsumer = new SqsPermissionConsumer(diContainer.cradle, {
      creationConfig: { queue: { QueueName: queueName } },
      deletionConfig: { deleteIfExists: false },
    })
    await wireConsumer.start()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'skip-consumer-1',
      messageType: 'add',
      metadata: { info: 'plain json path' },
    }
    await wirePublisher.publish(message)

    const result = await wireConsumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)

    await wirePublisher.close()
    await wireConsumer.close(true)
  })

  it('consumer with disableCodecAutoDetection passes codec-shaped message through as plain JSON', async () => {
    // A publisher with skipCompressionBelow set very high sends plain JSON even though
    // codec is configured — the body will NOT be a codec envelope, so this test
    // verifies that disableCodecAutoDetection does not break the plain-JSON path.
    // More importantly: if the message body were a real envelope, the consumer would
    // normally auto-detect and decompress it; with the flag set it must not do so.
    const queueName = `${SqsPermissionConsumer.QUEUE_NAME}-disable-auto-detect`
    await testAdmin.deleteQueues(queueName)

    // Publisher sends plain JSON (skipCompressionBelow prevents compression)
    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      skipCompressionBelow: 99_999,
      creationConfig: { queue: { QueueName: queueName } },
    })
    await wirePublisher.init()

    // Consumer opts out of auto-detection
    const noAutoConsumer = new SqsPermissionConsumer(diContainer.cradle, {
      creationConfig: { queue: { QueueName: queueName } },
      deletionConfig: { deleteIfExists: false },
      disableCodecAutoDetection: true,
    })
    await noAutoConsumer.start()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'disable-auto-detect-1',
      messageType: 'add',
      metadata: { info: 'plain json, no auto-detect' },
    }
    await wirePublisher.publish(message)

    const result = await noAutoConsumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)

    await wirePublisher.close()
    await noAutoConsumer.close(true)
  })

  it('consumer with disableCodecAutoDetection:true does not decompress a real zstd envelope', async () => {
    // When disableCodecAutoDetection is true, a real codec envelope reaching the consumer
    // must NOT be decompressed — the raw envelope object is passed to schema validation,
    // which fails because { __mqtCodec, __mqtData } has no `messageType` field.
    const queueName = `${SqsPermissionConsumer.QUEUE_NAME}-disable-real-envelope`
    await testAdmin.deleteQueues(queueName)

    const noAutoConsumer = new SqsPermissionConsumer(diContainer.cradle, {
      creationConfig: { queue: { QueueName: queueName } },
      deletionConfig: { deleteIfExists: false },
      disableCodecAutoDetection: true,
    })
    await noAutoConsumer.start()

    // Build a real zstd envelope and inject it directly into the queue
    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'disable-real-envelope-1',
      messageType: 'add',
    }
    const compressedBody = await compressMessageBody(JSON.stringify(message), MessageCodecEnum.ZSTD)
    await diContainer.cradle.sqsClient.send(
      new SendMessageCommand({
        QueueUrl: noAutoConsumer.queueProps.url,
        MessageBody: compressedBody,
      }),
    )

    // The envelope fails schema validation — consumer records it as an error, not consumed.
    await waitAndRetry(() => noAutoConsumer.handlerSpy.counts.error > 0, 100, 20)
    expect(noAutoConsumer.handlerSpy.counts.error).toBeGreaterThan(0)
    expect(noAutoConsumer.addCounter).toBe(0)

    await noAutoConsumer.close(true)
  }, 15000)
})

// ---------------------------------------------------------------------------
// Custom codec — passthrough handler registered via { name, handler } form
// ---------------------------------------------------------------------------

/**
 * Identity codec: compress/decompress are no-ops, createCompressStream returns a PassThrough.
 * Lets us verify the full custom-codec path without a real compression library in tests.
 */
class NoopCodecHandler implements MessageCodecHandler {
  compress(data: Buffer): Promise<Buffer> {
    return Promise.resolve(data)
  }
  decompress(data: Buffer): Promise<Buffer> {
    return Promise.resolve(data)
  }
  createCompressStream(): Transform {
    return new PassThrough()
  }
}

describe('SqsPermissionConsumer - custom codec registration', () => {
  const CUSTOM_CODEC_NAME = 'noop'

  let diContainer: AwilixContainer<Dependencies>
  let testAdmin: TestAwsResourceAdmin

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionPublisher: asValue(() => undefined),
      permissionConsumer: asValue(() => undefined),
    })
    testAdmin = diContainer.cradle.testAdmin
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  it('publisher wraps payload in an envelope with the custom codec name', async () => {
    const queueName = `${SqsPermissionConsumer.QUEUE_NAME}-custom-codec-wire`
    await testAdmin.deleteQueues(queueName)

    const codec = { name: CUSTOM_CODEC_NAME, handler: new NoopCodecHandler() }
    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec,
      skipCompressionBelow: 0, // always compress so the envelope is always produced
      creationConfig: { queue: { QueueName: queueName } },
    })
    await wirePublisher.init()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'custom-codec-wire-1',
      messageType: 'add',
    }
    await wirePublisher.publish(message)

    const { Messages } = await diContainer.cradle.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: wirePublisher.queueProps.url,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      }),
    )
    expect(Messages).toBeDefined()
    expect(Messages!.length).toBe(1)

    const envelope = JSON.parse(Messages![0]!.Body!) as Record<string, unknown>
    // Envelope must carry the user-supplied codec name, not 'zstd'.
    expect(envelope.__mqtCodec).toBe(CUSTOM_CODEC_NAME)
    expect(typeof envelope.__mqtData).toBe('string')
    // Since the handler is a no-op, __mqtData decodes to the original JSON.
    const decoded = Buffer.from(envelope.__mqtData as string, 'base64').toString('utf8')
    expect(JSON.parse(decoded)).toMatchObject(message)

    await wirePublisher.close()
  })

  it('consumer configured with the same custom codec decompresses and processes the message', async () => {
    const queueName = `${SqsPermissionConsumer.QUEUE_NAME}-custom-codec-roundtrip`
    await testAdmin.deleteQueues(queueName)

    const codec = { name: CUSTOM_CODEC_NAME, handler: new NoopCodecHandler() }

    const publisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec,
      skipCompressionBelow: 0,
      creationConfig: { queue: { QueueName: queueName } },
    })
    await publisher.init()

    const consumer = new SqsPermissionConsumer(diContainer.cradle, {
      codecs: [codec],
      creationConfig: { queue: { QueueName: queueName } },
      deletionConfig: { deleteIfExists: false },
    })
    await consumer.start()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'custom-codec-roundtrip-1',
      messageType: 'add',
      metadata: { info: 'custom codec round-trip' },
    }
    await publisher.publish(message)

    const result = await consumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)

    await publisher.close()
    await consumer.close(true)
  })

  it('consumer without the custom codec does not auto-detect envelopes with that codec name', async () => {
    // A consumer without extra codecs has codecKnownNames = Set(['zstd']) (built-ins only).
    // isCodecEnvelope(body, Set(['zstd'])) returns false for a 'noop' envelope, so the
    // raw envelope object reaches schema validation and fails (no messageType field).
    // The message is never successfully handled — addCounter stays at 0.
    const queueName = `${SqsPermissionConsumer.QUEUE_NAME}-custom-codec-no-autodetect`
    await testAdmin.deleteQueues(queueName)

    const codec = { name: CUSTOM_CODEC_NAME, handler: new NoopCodecHandler() }

    const publisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec,
      skipCompressionBelow: 0,
      creationConfig: { queue: { QueueName: queueName } },
    })
    await publisher.init()

    // Consumer with no extra codecs — only built-in codecs (e.g. zstd) are registered, not 'noop'
    const zstdConsumer = new SqsPermissionConsumer(diContainer.cradle, {
      creationConfig: { queue: { QueueName: queueName } },
      deletionConfig: { deleteIfExists: false },
    })
    await zstdConsumer.start()

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'custom-codec-no-autodetect-1',
      messageType: 'add',
    }
    await publisher.publish(message)

    // Give the consumer time to attempt processing, then verify no message was consumed.
    // The spy can't track by ID because the raw envelope has no top-level `id` field.
    await new Promise((resolve) => setTimeout(resolve, 2000))
    expect(zstdConsumer.addCounter).toBe(0)

    await publisher.close()
    await zstdConsumer.close(true)
  }, 10000)
})
