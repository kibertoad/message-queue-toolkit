import { InternalError } from '@lokalise/node-core'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FreeformRecord = Record<string, any>

export type CommonErrorParams = {
  message: string
  details?: FreeformRecord
}

export class SqsMessageInvalidFormat extends InternalError {
  constructor(params: CommonErrorParams) {
    super({
      message: params.message,
      errorCode: 'SQS_MESSAGE_INVALID_FORMAT',
      details: params.details,
    })
  }
}

export class SqsValidationError extends InternalError {
  constructor(params: CommonErrorParams) {
    super({
      message: params.message,
      errorCode: 'SQS_VALIDATION_ERROR',
      details: params.details,
    })
  }
}
