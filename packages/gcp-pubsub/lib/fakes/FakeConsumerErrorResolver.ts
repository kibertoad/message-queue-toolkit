import { PubSubConsumerErrorResolver } from '../errors/PubSubConsumerErrorResolver.ts'

export class FakeConsumerErrorResolver extends PubSubConsumerErrorResolver {
  private _errors: unknown[]

  constructor() {
    super()
    this._errors = []
  }

  public override processError(error: unknown) {
    this._errors.push(error)
    return super.processError(error)
  }

  get errors() {
    return this._errors
  }

  public clear(): void {
    this._errors = []
  }
}
