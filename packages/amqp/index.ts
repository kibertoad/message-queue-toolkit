export type { CommonMessage } from './lib/types/MessageTypes'

export type { AMQPQueueConfig } from './lib/AbstractAmqpService'

export { AbstractAmqpConsumer } from './lib/AbstractAmqpConsumer'
export { AbstractAmqpConsumerMultiSchema } from './lib/AbstractAmqpConsumerMultiSchema'
export { AmqpConsumerErrorResolver } from './lib/errors/AmqpConsumerErrorResolver'

export { AbstractAmqpPublisher } from './lib/AbstractAmqpPublisher'
export { AbstractAmqpPublisherMultiSchema } from './lib/AbstractAmqpPublisherMultiSchema'

export type { AmqpConfig } from './lib/amqpConnectionResolver'

export { resolveAmqpConnection } from './lib/amqpConnectionResolver'
export { deserializeAmqpMessage } from './lib/amqpMessageDeserializer'
