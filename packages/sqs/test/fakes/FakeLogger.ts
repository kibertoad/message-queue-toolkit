import type { Logger } from '@message-queue-toolkit/core'

export class FakeLogger implements Logger {
  public readonly loggedMessages: unknown[] = []
  public readonly loggedWarnings: unknown[] = []
  public readonly loggedErrors: unknown[] = []

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
}
