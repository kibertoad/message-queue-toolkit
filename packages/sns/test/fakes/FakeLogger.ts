import type { CommonLogger } from '@lokalise/node-core'
import type { Bindings, ChildLoggerOptions } from 'pino'
import type pino from 'pino'

export class FakeLogger implements CommonLogger {
  public readonly loggedMessages: unknown[] = []
  public readonly loggedWarnings: unknown[] = []
  public readonly loggedErrors: unknown[] = []

  public readonly level: pino.LevelWithSilentOrString

  constructor(level: pino.LevelWithSilentOrString = 'debug') {
    this.level = level
  }

  debug(obj: unknown) {
    this.loggedMessages.push(obj)
  }
  error(obj: unknown) {
    this.loggedErrors.push(obj)
  }
  fatal(obj: unknown) {
    this.loggedErrors.push(obj)
  }
  info(obj: unknown) {
    this.loggedMessages.push(obj)
  }
  trace(obj: unknown) {
    this.loggedMessages.push(obj)
  }
  warn(obj: unknown) {
    this.loggedWarnings.push(obj)
  }
  // Noop function
  silent(_obj: unknown) {
    return
  }

  // Child has no effect for FakeLogger
  child(_bindings: Bindings, _options?: ChildLoggerOptions): CommonLogger {
    return this
  }

  isLevelEnabled(_level: string): boolean {
    // For FakeLogger we want to track all logs
    return true
  }
}
