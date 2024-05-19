import type { AnyEventHandler, CommonEventDefinition } from '../eventTypes'

export class FakeListener<SupportedEvents extends CommonEventDefinition[]>
  implements AnyEventHandler<SupportedEvents>
{
  public receivedEvents: SupportedEvents[number]['consumerSchema']['_output'][] = []

  constructor(_supportedEvents: SupportedEvents) {
    this.receivedEvents = []
  }

  handleEvent(event: SupportedEvents[number]['consumerSchema']['_output']): void | Promise<void> {
    this.receivedEvents.push(event)
  }
}
