import { InternalError } from '@lokalise/node-core'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FreeformRecord = Record<string, any>

export type CommonErrorParams = {
  message: string
  details?: FreeformRecord
}

export class MessageInvalidFormatError extends InternalError {
  constructor(params: CommonErrorParams) {
    super({
      message: params.message,
      errorCode: 'MESSAGE_INVALID_FORMAT',
      details: params.details,
    })
    this.name = 'MessageInvalidFormat'
  }
}

export class MessageValidationError extends InternalError {
  constructor(params: CommonErrorParams) {
    super({
      message: params.message,
      errorCode: 'MESSAGE_VALIDATION_ERROR',
      details: params.details,
    })
    this.name = 'MessageValidationError'
  }
}

export function isMessageError(
  err: Error | undefined,
): err is MessageValidationError | MessageInvalidFormatError {
  return (
    (err && (err.name === 'MessageValidationError' || err.name === 'MessageInvalidFormat')) ?? false
  )
}
