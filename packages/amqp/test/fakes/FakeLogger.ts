import { deepClone } from '@lokalise/node-core'
import type { Logger } from '@message-queue-toolkit/core'

export class FakeLogger implements Logger {
  public readonly loggedMessages: unknown[] = []
  public readonly loggedWarnings: unknown[] = []
  public readonly loggedErrors: unknown[] = []

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

  private saveLog(obj: unknown) {
    this.loggedMessages.push(typeof obj === 'object' ? deepClone(obj) : obj)
  }
}
