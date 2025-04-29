import { AmqpConsumerErrorResolver } from '../../lib/errors/AmqpConsumerErrorResolver.ts'

export class FakeConsumerErrorResolver extends AmqpConsumerErrorResolver {
  private _errors: unknown[]
  constructor() {
    super()

    this._errors = []
  }

  public override processError(error: unknown) {
    this._errors.push(error)
    return super.processError(error)
  }

  public clear() {
    this._errors = []
  }

  get errors() {
    return this._errors
  }
}
