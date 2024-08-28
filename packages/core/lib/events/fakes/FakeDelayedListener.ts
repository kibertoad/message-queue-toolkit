import { setTimeout } from 'node:timers/promises'
import type { AnyEventHandler, CommonEventDefinition } from '../eventTypes'

export class FakeDelayedListener<SupportedEvents extends CommonEventDefinition[]>
  implements AnyEventHandler<SupportedEvents>
{
  public receivedEvents: SupportedEvents[number]['publisherSchema']['_output'][] = []

  private readonly delay: number

  constructor(delayMs: number) {
    this.receivedEvents = []
    this.delay = delayMs
  }

  async handleEvent(event: SupportedEvents[number]['publisherSchema']['_output']): Promise<void> {
    await setTimeout(200)
    this.receivedEvents.push(event)
  }
}
