import { randomUUID } from 'node:crypto'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import {
  Consumer,
  type Message,
  ProduceAcks,
  Producer,
  stringDeserializers,
} from '@platformatic/kafka'
import { stringSerializers } from '@platformatic/kafka'
import { type TestContext, registerDependencies } from '../test/testContext.ts'

describe('Test platformatic-kafka', () => {
  let testContext: TestContext

  beforeAll(async () => {
    testContext = await registerDependencies()
  })

  afterAll(async () => {
    await testContext.dispose()
  })

  it('should send and receive a message', async () => {
    // Given
    const clientId = randomUUID()
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const receivedMessages: Message<string, string, string, string>[] = []

    // Create producer
    const producer = new Producer({
      clientId,
      bootstrapBrokers: testContext.cradle.kafkaConfig.brokers,
      serializers: stringSerializers,
      autocreateTopics: true,
    })

    // Create consumer
    const consumer = new Consumer({
      clientId,
      groupId: randomUUID(),
      bootstrapBrokers: testContext.cradle.kafkaConfig.brokers,
      deserializers: stringDeserializers,
      autocreateTopics: true,
    })

    const stream = await consumer.consume({ topics: [topic] })
    stream.on('data', (message) => {
      receivedMessages.push(message)
      stream.close()
    })

    // When
    await producer.send({
      messages: [{ topic, value: messageValue }],
      acks: ProduceAcks.NO_RESPONSE,
    })

    // Then
    await waitAndRetry(() => receivedMessages.length > 0)

    // Cleanup
    producer.close()
    consumer.close()

    expect(receivedMessages).toHaveLength(1)
    expect(receivedMessages[0]?.value?.toString()).toBe(messageValue)
  })
})
