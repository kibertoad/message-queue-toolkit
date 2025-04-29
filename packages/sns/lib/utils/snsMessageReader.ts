import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  MessageInvalidFormatError,
  MessageValidationError,
  ResolvedMessage,
} from '@message-queue-toolkit/core'
import type { SQSMessage } from '@message-queue-toolkit/sqs'

import type { SNS_MESSAGE_BODY_TYPE } from '../types/MessageTypes.ts'

export function readSnsMessage(
  message: SQSMessage,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, ResolvedMessage> {
  try {
    const snsMessage: SNS_MESSAGE_BODY_TYPE = JSON.parse(message.Body)
    const messagePayload = JSON.parse(snsMessage.Message)

    return {
      result: {
        body: messagePayload,
        attributes: snsMessage.MessageAttributes,
      },
    }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
