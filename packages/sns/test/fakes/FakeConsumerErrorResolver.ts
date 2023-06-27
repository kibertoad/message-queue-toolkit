import { SnsConsumerErrorResolver } from '../../lib/errors/SnsConsumerErrorResolver'

export class FakeConsumerErrorResolver extends SnsConsumerErrorResolver {
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
