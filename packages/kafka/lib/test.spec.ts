import { randomUUID } from 'node:crypto'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import {
  Consumer,
  type Message,
  Producer,
  stringDeserializers,
  stringSerializers,
} from '@platformatic/kafka'
import { ProduceAcks } from '@platformatic/kafka'

// TODO: to be removed once we have proper tests
describe('Test', () => {
  const broker = 'localhost:9092'

  it('should send and receive a message', { timeout: 10000 }, async () => {
    // Given
    const clientId = randomUUID()
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const receivedMessages: Message<string, string, string, string>[] = []

    // Create producer
    const producer = new Producer({
      clientId,
      bootstrapBrokers: [broker],
      serializers: stringSerializers,
      autocreateTopics: true,
    })

    // Create consumer
    const consumer = new Consumer({
      clientId,
      groupId: randomUUID(),
      bootstrapBrokers: [broker],
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
    await waitAndRetry(() => receivedMessages.length > 0, 10, 800)
    expect(receivedMessages).toHaveLength(1)
    expect(receivedMessages[0]?.value?.toString()).toBe(messageValue)

    // Cleanup
    producer.close()
    consumer.close()
  })
})
