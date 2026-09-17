import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import { FakeListener } from '../../lib/events/fakes/FakeListener.ts'

export class FlakyFakeListener<
  SupportedEvents extends CommonEventDefinition[],
> extends FakeListener<SupportedEvents> {
  public attempts = 0
  private readonly failuresBeforeSuccess: number

  constructor(failuresBeforeSuccess: number) {
    super()
    this.failuresBeforeSuccess = failuresBeforeSuccess
  }

  override async handleEvent(
    event: SupportedEvents[number]['publisherSchema']['_output'],
  ): Promise<void> {
    this.attempts++
    if (this.attempts <= this.failuresBeforeSuccess) {
      throw new Error(`${this.constructor.name} error`)
    }
    await super.handleEvent(event)
  }
}
