import type {Either, ErrorResolver} from '@lokalise/node-core'
import type { ZodType } from 'zod'

import {SQSMessage} from "./AbstractSqsConsumer";
import {SqsMessageInvalidFormat, SqsValidationError} from "../errors/sqsErrors";

export const deserializeMessage = <T extends { }>(
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
