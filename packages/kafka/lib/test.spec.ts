import { randomUUID } from 'node:crypto'
import { KafkaJS } from '@confluentinc/kafka-javascript'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import { afterAll } from 'vitest'
import { type TestContext, registerDependencies } from '../test/testContext.ts'

describe('Test', () => {
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
    const groupId = randomUUID()
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const messages: string[] = []

    const kafka = new KafkaJS.Kafka({
      'client.id': clientId,
      'bootstrap.servers': testContext.cradle.kafkaConfig.brokers.join(','),
    })

    const producer = kafka.producer({
      'allow.auto.create.topics': true,
    })
    await producer.connect()

    const consumer = kafka.consumer({
      'bootstrap.servers': testContext.cradle.kafkaConfig.brokers.join(','),
      'client.id': clientId,
      'group.id': groupId,
      'allow.auto.create.topics': true,
    })
    await consumer.connect()
    await consumer.subscribe({ topics: [topic] })

    await consumer.run({
      eachMessage: ({ message }) => {
        console.log(message)
        return Promise.resolve()
      },
    })

    await producer.send({
      topic,
      messages: [{ value: messageValue, key: '1' }],
    })

    // Then
    await waitAndRetry(() => messages.length > 0)

    console.log(messages)
    await producer.disconnect()
    await consumer.disconnect()
  })
})
