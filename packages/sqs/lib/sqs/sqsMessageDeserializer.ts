import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { ZodType } from 'zod'

import type { SqsMessageInvalidFormat, SqsValidationError } from '../errors/sqsErrors'

import type { SQSMessage } from './AbstractSqsConsumer'

export const deserializeMessage = <T extends object>(
  message: SQSMessage,
  type: ZodType<T>,
  errorProcessor: ErrorResolver,
): Either<SqsMessageInvalidFormat | SqsValidationError, T> => {
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
