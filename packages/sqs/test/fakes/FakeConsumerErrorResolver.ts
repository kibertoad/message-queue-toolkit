import { SqsConsumerErrorResolver } from '../../lib/errors/SqsConsumerErrorResolver'

export class FakeConsumerErrorResolver extends SqsConsumerErrorResolver {
  public errors: Error[]
  public handleErrorCallsCount: number
  constructor() {
    super()

    this.handleErrorCallsCount = 0
    this.errors = []
  }

  public override processError(error: unknown) {
    this.handleErrorCallsCount++
    this.errors.push(error as Error)
    return super.processError(error)
  }

  public clear() {
    this.handleErrorCallsCount = 0
  }
}
