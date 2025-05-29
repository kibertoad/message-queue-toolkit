import { randomUUID } from 'node:crypto'
import { KafkaJS } from '@confluentinc/kafka-javascript'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import {} from '@platformatic/kafka'
import { type TestContext, createTestContext } from '../test/utils/testContext.ts'

describe('Test confluentic-kafka', () => {
  let testContext: TestContext

  beforeAll(async () => {
    testContext = await createTestContext()
  })

  afterAll(async () => {
    await testContext.dispose()
  })

  it('should send and receive a message', async () => {
    // Given
    const clientId = randomUUID()
    const groupId = randomUUID()
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const kafka = new KafkaJS.Kafka({
      'client.id': clientId,
      'bootstrap.servers': testContext.cradle.kafkaConfig.bootstrapBrokers.join(','),
    })

    // Topics can be created from producers, but as we will first connect a consumer, we need to create the topic first
    const admin = kafka.admin()
    await admin.connect()
    await admin.createTopics({
      topics: [{ topic }],
    })

    const messages: string[] = []

    const consumer = kafka.consumer({ 'group.id': groupId })
    await consumer.connect()
    await consumer.subscribe({ topic })

    await consumer.run({
      eachMessage: ({ message }) => {
        const messageString = message.value?.toString()
        if (messageString) messages.push(messageString)
        return Promise.resolve()
      },
    })
    // Wait for the consumer to be assigned partitions
    await waitAndRetry(() => consumer.assignment().length > 0, 100, 10)

    // When
    const producer = kafka.producer()
    await producer.connect()
    await producer.send({
      topic,
      messages: [{ value: messageValue }],
    })

    // Then
    await waitAndRetry(() => messages.length > 0)

    // Cleaning up before checks to avoid stale state
    await consumer.disconnect()
    await producer.disconnect()
    await admin.disconnect()

    expect(messages).toHaveLength(1)
    expect(messages[0]).toEqual(messageValue)
  })
})
