import { InternalError } from '@lokalise/node-core'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FreeformRecord = Record<string, any>

export type CommonErrorParams = {
  message: string
  details?: FreeformRecord
}

export class SnsMessageInvalidFormat extends InternalError {
  constructor(params: CommonErrorParams) {
    super({
      message: params.message,
      errorCode: 'SNS_MESSAGE_INVALID_FORMAT',
      details: params.details,
    })
  }
}

export class SnsValidationError extends InternalError {
  constructor(params: CommonErrorParams) {
    super({
      message: params.message,
      errorCode: 'SNS_VALIDATION_ERROR',
      details: params.details,
    })
  }
}
