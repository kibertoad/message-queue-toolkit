import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import { KafkaConsumer, type Message, Producer } from 'node-rdkafka'

// TODO: to be removed once we have proper tests
describe('Test', () => {
  it('should send and receive a message', async () => {
    // Given
    const brokers = 'localhost:9092'
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const receivedMessages: Message[] = []

    // Create a producer
    const producer = new Producer({
      'metadata.broker.list': brokers,
      'allow.auto.create.topics': true,
    })
    producer.connect()
    await once(producer, 'ready')

    const consumer = new KafkaConsumer(
      {
        'group.id': randomUUID(),
        'metadata.broker.list': brokers,
        'allow.auto.create.topics': true,
      },
      { 'auto.offset.reset': 'earliest' },
    )
    consumer.connect()

    await new Promise<void>((resolve, reject) =>
      consumer
        .on('ready', () => {
          consumer.subscribe([topic])
          consumer.consume()
          resolve()
        })
        .on('event.error', (err) => reject(err))
        .on('data', (data) => {
          receivedMessages.push(data)
        }),
    )

    // When
    producer.produce(topic, null, Buffer.from(messageValue))
    producer.flush()

    // Then
    await waitAndRetry(() => receivedMessages.length > 0, 10, 1000)
    expect(receivedMessages).toHaveLength(1)
    expect(receivedMessages[0]?.value?.toString()).toBe(messageValue)

    // Cleanup
    producer.disconnect()
    consumer.disconnect()
  })
})
