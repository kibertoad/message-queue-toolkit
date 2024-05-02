import type { CommonEventDefinition, EventTypeNames } from './eventTypes'

export class EventRegistry<SupportedEvents extends CommonEventDefinition[]> {
  public readonly supportedEvents: SupportedEvents
  private readonly supportedEventsSet: Set<string>
  private readonly supportedEventMap: Record<string, CommonEventDefinition> = {}

  constructor(supportedEvents: SupportedEvents) {
    this.supportedEvents = supportedEvents
    this.supportedEventsSet = new Set<string>()

    for (const supportedEvent of supportedEvents) {
      this.supportedEventMap[supportedEvent.schema.shape.type.value] = supportedEvent
      this.supportedEventsSet.add(supportedEvent.schema.shape.type.value)
    }
  }

  public getEventDefinitionByTypeName = <
    EventTypeName extends EventTypeNames<SupportedEvents[number]>,
  >(
    eventTypeName: EventTypeName,
  ): CommonEventDefinition => {
    return this.supportedEventMap[eventTypeName]
  }

  public isSupportedEvent(eventTypeName: string) {
    return this.supportedEventsSet.has(eventTypeName)
  }
}
