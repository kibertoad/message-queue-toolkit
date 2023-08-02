import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { MessageInvalidFormatError, MessageValidationError } from '../errors/Errors'

export function parseMessage<T extends object>(
  messagePayload: unknown,
  schema: ZodSchema<T>,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, T> {
  try {
    return {
      result: schema.parse(messagePayload),
    }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
