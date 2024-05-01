import type { AnyEventHandler, CommonEventDefinition } from '../eventTypes'

export class FakeListener<SupportedEvents extends CommonEventDefinition[]>
  implements AnyEventHandler<SupportedEvents>
{
  public receivedEvents: SupportedEvents[number]['schema']['_output'][] = []

  constructor(_supportedEvents: SupportedEvents) {
    this.receivedEvents = []
  }

  handleEvent(event: SupportedEvents[number]['schema']['_output']): void | Promise<void> {
    this.receivedEvents.push(event)
  }
}
