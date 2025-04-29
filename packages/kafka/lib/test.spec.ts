import { once } from 'node:events'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import { KafkaConsumer, type Message, Producer, features, librdkafkaVersion } from 'node-rdkafka'

// TODO: to be removed once we have proper tests
describe('Test', () => {
  it('should use node-rdkafka', () => {
    expect(features).toBeDefined()
    expect(librdkafkaVersion).toBeDefined()
  })

  it('should send and receive a message', { timeout: 20000 }, async () => {
    // Given
    const brokers = 'localhost:9092'
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}-${Math.random().toString(36).slice(2)}`
    const messageValue = 'My test message'

    const receivedMessages: Message[] = []

    // Create a producer
    const producer = new Producer({
      'metadata.broker.list': brokers,
      'allow.auto.create.topics': true,
    })
    producer.connect()
    await once(producer, 'ready')
    producer.setPollInterval(10)

    // Create a consumer with a unique group and disable auto-commit for fresh offsets
    const consumer = new KafkaConsumer(
      {
        'group.id': `test-group-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        'metadata.broker.list': brokers,
        'allow.auto.create.topics': true,
        'enable.auto.commit': false,
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
    // producer.flush()
    // ensure the message is sent to the broker
    await new Promise<void>((resolve, reject) => {
      producer.flush(5000, (err) => {
        if (err) reject(err)
        else resolve()
      })
    })

    // Then
    await waitAndRetry(() => receivedMessages.length > 0, 10, 1500)
    expect(receivedMessages).toHaveLength(1)
    expect(receivedMessages[0]?.value?.toString()).toBe(messageValue)

    // Cleanup
    producer.disconnect()
    consumer.disconnect()
  })
})
