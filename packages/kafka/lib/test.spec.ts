import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import {
  Consumer as PlatformaticConsumer,
  type Message as PlatformaticMessage,
  Producer as PlatformaticProducer,
  stringDeserializers,
  stringSerializers,
} from '@platformatic/kafka'
import { ProduceAcks } from '@platformatic/kafka'
import {
  KafkaConsumer,
  type Message as RdKafkaMessage,
  Producer as RdKafkaProducer,
  features,
  librdkafkaVersion,
} from 'node-rdkafka'

// TODO: to be removed once we have proper tests
describe('Test', () => {
  const broker = 'localhost:9092'

  it.skip('should use node-rdkafka', () => {
    expect(features).toBeDefined()
    expect(librdkafkaVersion).toBeDefined()
  })

  it.skip('node-rdkafka - should send and receive a message', { timeout: 10000 }, async () => {
    // Given
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const receivedMessages: RdKafkaMessage[] = []

    // Create a producer
    const producer = new RdKafkaProducer({
      'metadata.broker.list': broker,
      'allow.auto.create.topics': true,
    })
    producer.connect()
    await once(producer, 'ready')

    // Create a consumer with a unique group and disable auto-commit for fresh offsets
    const consumer = new KafkaConsumer(
      {
        'group.id': 'test-group',
        'metadata.broker.list': broker,
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
    producer.flush()

    // Then
    await waitAndRetry(() => receivedMessages.length > 0, 10, 800)
    expect(receivedMessages).toHaveLength(1)
    expect(receivedMessages[0]?.value?.toString()).toBe(messageValue)

    // Cleanup
    producer.disconnect()
    consumer.disconnect()
  })

  it('platformatic/kafka - should send and receive a message', { timeout: 10000 }, async () => {
    // Given
    const clientId = randomUUID()
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const receivedMessages: PlatformaticMessage<string, string, string, string>[] = []

    // Create producer
    const producer = new PlatformaticProducer({
      clientId,
      bootstrapBrokers: [broker],
      serializers: stringSerializers,
      autocreateTopics: true,
    })

    // Create consumer
    const consumer = new PlatformaticConsumer({
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
