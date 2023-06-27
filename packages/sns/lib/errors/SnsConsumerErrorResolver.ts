import { types } from 'node:util'

import type { ErrorResolver } from '@lokalise/node-core'
import { InternalError, isStandardizedError } from '@lokalise/node-core'
import { ZodError } from 'zod'

import { SnsMessageInvalidFormat, SnsValidationError } from './snsErrors'

export class SnsConsumerErrorResolver implements ErrorResolver {
  public processError(error: unknown): InternalError {
    if (types.isNativeError(error) && error?.name === 'SyntaxError') {
      return new SnsMessageInvalidFormat({
        message: error.message,
      })
    }
    if (error instanceof ZodError) {
      return new SnsValidationError({
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
