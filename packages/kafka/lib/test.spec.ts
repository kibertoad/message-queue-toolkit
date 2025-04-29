import { once } from 'node:events'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import { KafkaConsumer, type Message, Producer, features, librdkafkaVersion } from 'node-rdkafka'

// TODO: to be removed once we have proper tests
describe('Test', () => {
  it('should use node-rdkafka', () => {
    expect(features).toMatchInlineSnapshot(`
      [
        "gzip",
        "snappy",
        "sasl",
        "regex",
        "lz4",
        "sasl_gssapi",
        "sasl_plain",
        "plugins",
        "http",
      ]
    `)
    expect(librdkafkaVersion).toMatchInlineSnapshot(`"2.8.0"`)
  })

  it('should send and receive a message', { timeout: 20000 }, async () => {
    // Given
    const brokers = 'localhost:9092'
    const topic = 'test-topic'
    const messageValue = 'My test message'

    const receivedMessages: Message[] = []

    // Create a producer
    const producer = new Producer({
      'metadata.broker.list': brokers,
      'allow.auto.create.topics': true,
    })
    producer.connect()
    await once(producer, 'ready')

    // Create a consumer
    const consumer = new KafkaConsumer(
      {
        'group.id': `test-group-${Date.now()}`,
        'metadata.broker.list': brokers,
        'allow.auto.create.topics': true,
      },
      {
        'auto.offset.reset': 'latest',
      },
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
    await waitAndRetry(() => receivedMessages.length > 0, 10, 1500)
    expect(receivedMessages).toHaveLength(1)
    expect(receivedMessages[0]?.value?.toString()).toBe(messageValue)

    // Cleanup
    producer.disconnect()
    consumer.disconnect()
  })
})
