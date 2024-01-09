import { InternalError } from '@lokalise/node-core'

export class RetryMessageLaterError extends InternalError {
  constructor(message: string, errorCode: string, details?: Record<string, unknown>) {
    super({
      message,
      errorCode,
      details,
    })
  }
}
