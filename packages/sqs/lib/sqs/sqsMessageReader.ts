import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { MessageInvalidFormatError, MessageValidationError } from '@message-queue-toolkit/core'

import type { SQSMessage } from '../types/MessageTypes'

export function readSqsMessage(
  message: SQSMessage,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, unknown> {
  try {
    return {
      result: JSON.parse(message.Body),
    }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
