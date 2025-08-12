import { type CommonLogger, deepClone } from '@lokalise/node-core'
import type { Bindings, ChildLoggerOptions } from 'pino'
import type pino from 'pino'

export class FakeLogger implements CommonLogger {
  public readonly loggedMessages: unknown[] = []

  public level: pino.LevelWithSilentOrString

  constructor(level: pino.LevelWithSilentOrString = 'debug') {
    this.level = level
  }

  get msgPrefix(): string | undefined {
    return undefined
  }

  debug(obj: unknown) {
    this.saveLog(obj)
  }
  error(obj: unknown) {
    this.saveLog(obj)
  }
  fatal(obj: unknown) {
    this.saveLog(obj)
  }
  info(obj: unknown) {
    this.saveLog(obj)
  }
  trace(obj: unknown) {
    this.saveLog(obj)
  }
  warn(obj: unknown) {
    this.saveLog(obj)
  }
  silent(_obj: unknown) {
    return
  }

  // Child has no effect for FakeLogger
  child(_bindings: Bindings, _options?: ChildLoggerOptions): CommonLogger {
    return this
  }

  isLevelEnabled(_: pino.LevelWithSilentOrString): boolean {
    // For FakeLogger we want to track all logs
    return true
  }

  private saveLog(obj: unknown) {
    this.loggedMessages.push(typeof obj === 'object' ? deepClone(obj) : obj)
  }
}
