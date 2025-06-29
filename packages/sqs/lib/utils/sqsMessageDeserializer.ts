import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  MessageInvalidFormatError,
  MessageValidationError,
  ParseMessageResult,
} from '@message-queue-toolkit/core'
import { parseMessage } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod/v4'

import type { SQSMessage } from '../types/MessageTypes.ts'

export const deserializeSQSMessage = <T extends object>(
  message: SQSMessage,
  type: ZodSchema<T>,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, ParseMessageResult<T>> => {
  try {
    return parseMessage(JSON.parse(message.Body), type, errorProcessor)
  } catch (exception) {
    return { error: errorProcessor.processError(exception) }
  }
}
