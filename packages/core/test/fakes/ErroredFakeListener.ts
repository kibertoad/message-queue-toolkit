import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import { FakeListener } from '../../lib/events/fakes/FakeListener.ts'

export class ErroredFakeListener<
  SupportedEvents extends CommonEventDefinition[],
> extends FakeListener<SupportedEvents> {
  override async handleEvent(
    event: SupportedEvents[number]['publisherSchema']['_output'],
  ): Promise<void> {
    await super.handleEvent(event)
    throw new Error(`${this.constructor.name} error`)
  }
}
