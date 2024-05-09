import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { MessageInvalidFormatError, MessageValidationError } from '@message-queue-toolkit/core'
import type { SQSMessage } from '@message-queue-toolkit/sqs'
import type { ZodType } from 'zod'

import { SNS_MESSAGE_BODY_SCHEMA } from '../types/MessageTypes'

export const deserializeSNSMessage = <T extends object>(
  message: SQSMessage,
  type: ZodType<T>,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, T> => {
  try {
    const snsMessage = SNS_MESSAGE_BODY_SCHEMA.parse(JSON.parse(message.Body))
    const result = JSON.parse(snsMessage.Message)
    type.parse(result)

    return { result }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
