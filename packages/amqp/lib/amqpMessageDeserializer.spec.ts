import type { Message } from 'amqplib'
import { describe, expect, it } from 'vitest'

import type { PERMISSIONS_MESSAGE_TYPE } from '../test/consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../test/consumers/userConsumerSchemas'

import { deserializeAmqpMessage } from './amqpMessageDeserializer'
import { AmqpConsumerErrorResolver } from './errors/AmqpConsumerErrorResolver'

describe('messageDeserializer', () => {
  it('deserializes valid JSON', () => {
    const messagePayload = {
      id: '1',
      messageType: 'add',
      userIds: [1],
      permissions: ['perm'],
      nonSchemaField: 'nonSchemaField',
    }
    const message: Message = {
      content: Buffer.from(JSON.stringify(messagePayload)),
    } as Message

    const errorProcessor = new AmqpConsumerErrorResolver()

    const deserializedPayload = deserializeAmqpMessage(
      message,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.result).toMatchObject({
      originalMessage: messagePayload,
      parsedMessage: {
        id: '1',
        messageType: 'add',
        userIds: [1],
        permissions: ['perm'],
      },
    })
  })

  it('throws an error on invalid JSON', () => {
    const messagePayload: Partial<PERMISSIONS_MESSAGE_TYPE> = {
      userIds: [1],
    }
    const message: Message = {
      content: Buffer.from(JSON.stringify(messagePayload)),
    } as Message

    const errorProcessor = new AmqpConsumerErrorResolver()

    const deserializedPayload = deserializeAmqpMessage(
      message,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.error).toMatchObject({
      errorCode: 'MESSAGE_VALIDATION_ERROR',
    })
  })

  it('throws an error on non-JSON', () => {
    const message: Message = {
      content: Buffer.from('dummy'),
    } as Message

    const errorProcessor = new AmqpConsumerErrorResolver()

    const deserializedPayload = deserializeAmqpMessage(
      message,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.error).toMatchObject({
      errorCode: 'MESSAGE_INVALID_FORMAT',
    })
  })
})
