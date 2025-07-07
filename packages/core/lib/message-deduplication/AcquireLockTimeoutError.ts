import { isError } from '@lokalise/node-core'

const AcquireLockTimeoutErrorName = 'AcquireLockTimeoutError'

export class AcquireLockTimeoutError extends Error {
  constructor(message?: string) {
    super(message)
    this.name = AcquireLockTimeoutErrorName
  }
}

export const isAcquireLockTimeoutError = (error: unknown): error is AcquireLockTimeoutError =>
  isError(error) && error.name === AcquireLockTimeoutErrorName
