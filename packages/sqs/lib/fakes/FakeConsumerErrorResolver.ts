import { SqsConsumerErrorResolver } from '../errors/SqsConsumerErrorResolver'

export class FakeConsumerErrorResolver extends SqsConsumerErrorResolver {
  public errors: Error[]

  get handleErrorCallsCount() {
    return this.errors.length
  }

  constructor() {
    super()

    this.errors = []
  }

  public override processError(error: unknown) {
    this.errors.push(error as Error)
    return super.processError(error)
  }

  public clear() {
    this.errors = []
  }
}
