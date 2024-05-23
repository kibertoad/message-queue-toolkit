import type { SQSMessage } from '@message-queue-toolkit/sqs'
import { SqsConsumerErrorResolver } from '@message-queue-toolkit/sqs'

import type { PERMISSIONS_MESSAGE_TYPE } from '../../test/consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../../test/consumers/userConsumerSchemas'
import { SnsConsumerErrorResolver } from '../errors/SnsConsumerErrorResolver'
import type { SNS_MESSAGE_BODY_TYPE } from '../types/MessageTypes'

import { deserializeSNSMessage } from './snsMessageDeserializer'

describe('messageDeserializer', () => {
  it('deserializes valid JSON', () => {
    const messagePayload = {
      id: '1',
      messageType: 'add',
      permissions: ['perm'],
      nonSchemaField: 'nonSchemaField',
    }

    const snsMessage: SNS_MESSAGE_BODY_TYPE = {
      Message: JSON.stringify(messagePayload),
      Type: 'dummy',
      MessageId: 'dummy',
      TopicArn: 'dummy',
      Timestamp: 'dummy',
      SignatureVersion: 'dummy',
      Signature: 'dummy',
      SigningCertURL: 'dummy',
      UnsubscribeURL: 'dummy',
    }

    const message: SQSMessage = {
      Body: JSON.stringify(snsMessage),
    } as SQSMessage

    const errorProcessor = new SqsConsumerErrorResolver()

    const deserializedPayload = deserializeSNSMessage(
      message,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.result).toEqual({
      originalMessage: messagePayload,
      parsedMessage: {
        id: '1',
        messageType: 'add',
        permissions: ['perm'],
      },
    })
  })

  it('throws an error on invalid JSON', () => {
    const messagePayload: Partial<PERMISSIONS_MESSAGE_TYPE> = {
      permissions: ['perm'],
    }

    const snsMessage: SNS_MESSAGE_BODY_TYPE = {
      Message: JSON.stringify(messagePayload),
      Type: 'dummy',
      MessageId: 'dummy',
      TopicArn: 'dummy',
      Timestamp: 'dummy',
      SignatureVersion: 'dummy',
      Signature: 'dummy',
      SigningCertURL: 'dummy',
      UnsubscribeURL: 'dummy',
    }

    const message: SQSMessage = {
      Body: JSON.stringify(snsMessage),
    } as SQSMessage

    const errorProcessor = new SnsConsumerErrorResolver()

    const deserializedPayload = deserializeSNSMessage(
      message,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.error).toMatchObject({
      errorCode: 'MESSAGE_VALIDATION_ERROR',
    })
  })

  it('throws an error on invalid SNS envelope', () => {
    const messagePayload: Partial<PERMISSIONS_MESSAGE_TYPE> = {
      permissions: ['perm'],
    }

    const snsMessage: Partial<SNS_MESSAGE_BODY_TYPE> = {
      Message: JSON.stringify(messagePayload),
    }

    const message: SQSMessage = {
      Body: JSON.stringify(snsMessage),
    } as SQSMessage

    const errorProcessor = new SnsConsumerErrorResolver()

    const deserializedPayload = deserializeSNSMessage(
      message,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.error).toMatchObject({
      errorCode: 'MESSAGE_VALIDATION_ERROR',
    })
  })

  it('throws an error on non-JSON', () => {
    const message = 'dummy'

    const errorProcessor = new SqsConsumerErrorResolver()

    const deserializedPayload = deserializeSNSMessage(
      message as any,
      PERMISSIONS_MESSAGE_SCHEMA,
      errorProcessor,
    )

    expect(deserializedPayload.error).toMatchObject({
      errorCode: 'MESSAGE_INVALID_FORMAT',
    })
  })
})
