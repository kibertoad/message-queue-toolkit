import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  MessageInvalidFormatError,
  MessageValidationError,
  ResolvedMessage,
} from '@message-queue-toolkit/core'

import type { SQSMessage } from '../types/MessageTypes.ts'

export function readSqsMessage(
  message: SQSMessage,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, ResolvedMessage> {
  try {
    return {
      result: {
        body: JSON.parse(message.Body),
        attributes: message.MessageAttributes,
      },
    }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
