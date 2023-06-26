import type { Message } from 'amqplib'

import type { PERMISSIONS_MESSAGE_TYPE } from '../test/consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../test/consumers/userConsumerSchemas'

import { deserializeMessage } from './amqpMessageDeserializer'
import { AmqpConsumerErrorResolver } from './errors/AmqpConsumerErrorResolver'

describe('messageDeserializer', () => {
  it('deserializes valid JSON', () => {
    const messagePayload: PERMISSIONS_MESSAGE_TYPE = {
      messageType: 'add',
      userIds: [1],
      permissions: ['perm'],
    }
    const message: Message = {
      content: Buffer.from(JSON.stringify(messagePayload)),
    } as Message

    const errorProcessor = new AmqpConsumerErrorResolver()

    const deserializedPayload = deserializeMessage(
      message,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.result).toMatchObject(messagePayload)
  })

  it('throws an error on invalid JSON', () => {
    const messagePayload: Partial<PERMISSIONS_MESSAGE_TYPE> = {
      userIds: [1],
    }
    const message: Message = {
      content: Buffer.from(JSON.stringify(messagePayload)),
    } as Message

    const errorProcessor = new AmqpConsumerErrorResolver()

    const deserializedPayload = deserializeMessage(
      message,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.error).toMatchObject({
      errorCode: 'AMQP_VALIDATION_ERROR',
    })
  })

  it('throws an error on non-JSON', () => {
    const message: Message = {
      content: Buffer.from('dummy'),
    } as Message

    const errorProcessor = new AmqpConsumerErrorResolver()

    const deserializedPayload = deserializeMessage(
      message,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.error).toMatchObject({
      errorCode: 'AMQP_MESSAGE_INVALID_FORMAT',
    })
  })
})
