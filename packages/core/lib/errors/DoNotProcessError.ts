import { InternalError } from '@lokalise/node-core'

export class DoNotProcessMessageError extends InternalError {
  constructor(message: string, errorCode: string, details?: Record<string, unknown>) {
    super({
      message,
      errorCode,
      details,
    })
  }
}
