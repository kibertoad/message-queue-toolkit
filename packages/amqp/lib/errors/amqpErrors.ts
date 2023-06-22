import { InternalError } from '@lokalise/node-core'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FreeformRecord = Record<string, any>

export type CommonErrorParams = {
  message: string
  details?: FreeformRecord
}

export class AmqpMessageInvalidFormat extends InternalError {
  constructor(params: CommonErrorParams) {
    super({
      message: params.message,
      errorCode: 'AMQP_MESSAGE_INVALID_FORMAT',
      details: params.details,
    })
  }
}

export class AmqpValidationError extends InternalError {
  constructor(params: CommonErrorParams) {
    super({
      message: params.message,
      errorCode: 'AMQP_VALIDATION_ERROR',
      details: params.details,
    })
  }
}
