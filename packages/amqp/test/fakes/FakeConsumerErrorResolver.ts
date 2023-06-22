import { AmqpConsumerErrorResolver } from '../../lib/errors/AmqpConsumerErrorResolver'

export class FakeConsumerErrorResolver extends AmqpConsumerErrorResolver {
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
