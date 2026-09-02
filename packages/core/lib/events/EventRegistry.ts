import { precompileEventDefinition } from '../utils/precompileUtils.ts'
import type { CommonEventDefinition, EventTypeNames } from './eventTypes.ts'

export class EventRegistry<SupportedEvents extends CommonEventDefinition[]> {
  public readonly supportedEvents: SupportedEvents
  public readonly supportedEventTypes: Set<string>
  private readonly supportedEventMap: Record<string, CommonEventDefinition> = {}

  constructor(supportedEvents: SupportedEvents) {
    this.supportedEvents = supportedEvents
    this.supportedEventTypes = new Set<string>()

    for (const supportedEvent of supportedEvents) {
      const eventTypeName = supportedEvent.consumerSchema.shape.type.value
      // Definitions looked up here are the ones emitted events are parsed with, so their schemas
      // are compiled. Publishers built from `supportedEvents` register the same schema objects,
      // and precompilation is memoized, so they get the compiled counterparts for free.
      // `supportedEvents` keeps the definitions exactly as they were passed in.
      this.supportedEventMap[eventTypeName] = precompileEventDefinition(supportedEvent)
      this.supportedEventTypes.add(eventTypeName)
    }
  }

  public getEventDefinitionByTypeName = <
    EventTypeName extends EventTypeNames<SupportedEvents[number]>,
  >(
    eventTypeName: EventTypeName,
  ): CommonEventDefinition => {
    // biome-ignore lint/style/noNonNullAssertion: It's ok
    return this.supportedEventMap[eventTypeName]!
  }

  public isSupportedEvent(eventTypeName: string) {
    return this.supportedEventTypes.has(eventTypeName)
  }
}
