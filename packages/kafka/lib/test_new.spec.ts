import { randomUUID } from 'node:crypto'
import { KafkaJS, Producer } from '@confluentinc/kafka-javascript'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import { afterAll } from 'vitest'
import { type TestContext, registerDependencies } from '../test/testContext.ts'

// TODO: to be removed once we have proper tests
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
    //const groupId = randomUUID()
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const messages: string[] = []

    const kafka = new KafkaJS.Kafka({
      'client.id': clientId,
      'bootstrap.servers': testContext.cradle.kafkaConfig.brokers.join(','),
    })

    const producer = kafka.producer({
      'bootstrap.servers': testContext.cradle.kafkaConfig.brokers.join(','),
      'allow.auto.create.topics': true,
      'client.id': clientId,
    })
    await producer.connect()

    /*
    const consumer = kafka.consumer({ 'group.id': groupId, 'allow.auto.create.topics': true })
    await consumer.connect()
    await consumer.subscribe({ topics: [topic] })

    await consumer.run({
      eachMessage: ({ message }) => {
        const receivedMessage = message.value?.toString('utf8')
        if (receivedMessage) messages.push(receivedMessage)
        return Promise.resolve()
      },
    })
    */

    await producer.send({
      topic,
      messages: [{ value: messageValue, key: '1' }],
    })

    // Then
    await waitAndRetry(() => messages.length > 0)

    console.log(messages)
    await producer.disconnect()
    //await consumer.disconnect()
  })

  it('should send and receive a message 2', async () => {
    // Given
    const clientId = randomUUID()
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const messages: string[] = []

    const kafka = new KafkaJS.Kafka({
      kafkaJS: {
        clientId,
        brokers: testContext.cradle.kafkaConfig.brokers,
      },
    })

    const producer = kafka.producer({
      kafkaJS: {
        allowAutoTopicCreation: true,
        compression: KafkaJS.CompressionTypes.GZIP,
        acks: 0,
      },
    })
    await producer.connect()

    await producer.send({
      topic,
      messages: [{ value: messageValue, key: '1' }],
    })

    // Then
    await waitAndRetry(() => messages.length > 0)

    console.log(messages)
    await producer.disconnect()
  })

  it('should send and receive a message', async () => {
    // Given
    const clientId = randomUUID()
    const groupId = randomUUID()
    // Use a fresh, unique topic per run to avoid stale state
    const topic = `test-topic-${Date.now()}`
    const messageValue = 'My test message'

    const messages: string[] = []

    const producer = new Producer({
      'bootstrap.servers': testContext.cradle.kafkaConfig.brokers.join(','),
      'allow.auto.create.topics': true,
      'client.id': clientId,
    })
    producer.connect()
    await new Promise((resolve) => {
      producer.on('ready', () => {
        console.log('Producer is ready')
        resolve(undefined)
      })
    })

    producer.produce(topic, null, Buffer.from(messageValue), null, Date.now())
    producer.flush()

    // Then
    await waitAndRetry(() => messages.length > 0, 500, 1000)

    console.log(messages)
    producer.disconnect()
    //await consumer.disconnect()
    await new Promise((resolve) => {
      producer.on('disconnected', resolve)
    })
  })
})
