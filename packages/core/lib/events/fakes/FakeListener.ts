import { setTimeout } from 'node:timers/promises'
import type { AnyEventHandler, CommonEventDefinition } from '../eventTypes'

export class FakeListener<SupportedEvents extends CommonEventDefinition[]>
  implements AnyEventHandler<SupportedEvents>
{
  readonly eventHandlerId = this.constructor.name

  public receivedEvents: SupportedEvents[number]['publisherSchema']['_output'][] = []
  private readonly delay: number

  constructor(delayMs?: number) {
    this.receivedEvents = []
    this.delay = delayMs ?? 0
  }

  async handleEvent(event: SupportedEvents[number]['publisherSchema']['_output']): Promise<void> {
    if (this.delay > 0) await setTimeout(this.delay)
    this.receivedEvents.push(event)
  }
}
