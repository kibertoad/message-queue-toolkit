import { types } from 'node:util'

import type { ErrorResolver } from '@lokalise/node-core'
import { InternalError, isStandardizedError } from '@lokalise/node-core'
import { ZodError } from 'zod'

import { MessageInvalidFormatError, MessageValidationError } from '@message-queue-toolkit/core'

export class SnsConsumerErrorResolver implements ErrorResolver {
  public processError(error: unknown): InternalError {
    if (types.isNativeError(error) && error?.name === 'SyntaxError') {
      return new MessageInvalidFormatError({
        message: error.message,
      })
    }
    if (error instanceof ZodError) {
      return new MessageValidationError({
        message: error.message,
        details: {
          error: error.issues,
        },
      })
    }
    if (isStandardizedError(error)) {
      return new InternalError({
        message: error.message,
        errorCode: error.code,
      })
    }
    return new InternalError({
      message: 'Error processing message',
      errorCode: 'INTERNAL_ERROR',
    })
  }
}
