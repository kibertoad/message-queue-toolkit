import { SqsConsumerErrorResolver } from '../errors/SqsConsumerErrorResolver.ts'

export class FakeConsumerErrorResolver extends SqsConsumerErrorResolver {
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
  public clear() {
    this._errors = []
  }
}
