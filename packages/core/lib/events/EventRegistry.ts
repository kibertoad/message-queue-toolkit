import type { CommonEventDefinition, EventTypeNames } from './eventTypes.ts'

export class EventRegistry<SupportedEvents extends CommonEventDefinition[]> {
  public readonly supportedEvents: SupportedEvents
  public readonly supportedEventTypes: Set<string>
  private readonly supportedEventMap: Record<string, CommonEventDefinition> = {}

  constructor(supportedEvents: SupportedEvents) {
    this.supportedEvents = supportedEvents
    this.supportedEventTypes = new Set<string>()

    for (const supportedEvent of supportedEvents) {
      this.supportedEventMap[supportedEvent.consumerSchema.shape.type.value] = supportedEvent
      this.supportedEventTypes.add(supportedEvent.consumerSchema.shape.type.value)
    }
  }

  public getEventDefinitionByTypeName = <
    EventTypeName extends EventTypeNames<SupportedEvents[number]>,
  >(
    eventTypeName: EventTypeName,
  ): CommonEventDefinition => {
    // biome-ignore lint/style/noNonNullAssertion: <explanation>
    return this.supportedEventMap[eventTypeName]!
  }

  public isSupportedEvent(eventTypeName: string) {
    return this.supportedEventTypes.has(eventTypeName)
  }
}
