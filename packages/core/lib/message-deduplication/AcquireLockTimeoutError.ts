import { isError } from '@lokalise/node-core'

const AcquireLockTimeoutErrorSymbol = Symbol.for('ACQUIRE_LOCK_TIMEOUT_ERROR')
export class AcquireLockTimeoutError extends Error {
  constructor(message?: string) {
    super(message)
    Object.defineProperty(this, AcquireLockTimeoutErrorSymbol, {
      value: true,
    })
  }
}

export const isAcquireLockTimeoutError = (error: unknown): error is AcquireLockTimeoutError =>
  // biome-ignore lint/suspicious/noExplicitAny: Expected
  isError(error) && (error as any)[AcquireLockTimeoutErrorSymbol] === true
