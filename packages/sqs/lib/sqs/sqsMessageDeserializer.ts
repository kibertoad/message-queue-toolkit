import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { MessageInvalidFormatError, MessageValidationError } from '@message-queue-toolkit/core'
import type { ZodType } from 'zod'

import type { SQSMessage } from '../types/MessageTypes'

export const deserializeSQSMessage = <T extends object>(
  message: SQSMessage,
  type: ZodType<T>,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, T> => {
  try {
    return {
      result: type.parse(JSON.parse(message.Body)),
    }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
