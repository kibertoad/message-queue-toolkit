import { SqsConsumerErrorResolver } from '../../lib/errors/SqsConsumerErrorResolver'

export class FakeConsumerErrorResolver extends SqsConsumerErrorResolver {
  public handleErrorCallsCount: number
  constructor() {
    super()

    this.handleErrorCallsCount = 0
  }

  public override processError(error: unknown) {
    this.handleErrorCallsCount++
    return super.processError(error)
  }

  public clear() {
    this.handleErrorCallsCount = 0
  }
}
