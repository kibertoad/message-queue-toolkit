import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { MessageInvalidFormatError, MessageValidationError } from '../errors/Errors'

export type ParseMessageResult<T> = {
  originalMessage: T
  parsedMessage: T
}

export function parseMessage<T extends object>(
  messagePayload: unknown,
  schema: ZodSchema<T>,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, ParseMessageResult<T>> {
  try {
    const parsedMessage = schema.parse(messagePayload)

    return { result: { parsedMessage, originalMessage: messagePayload as T } }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
