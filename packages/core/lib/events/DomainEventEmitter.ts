import { InternalError } from '@lokalise/node-core'

import type { MetadataFiller } from '../messages/MetadataFiller'
import type { MessageMetadataType } from '../messages/baseMessageSchemas'

import type { EventRegistry } from './EventRegistry'
import type {
  EventHandler,
  AnyEventHandler,
  SingleEventHandler,
  CommonEventDefinition,
  CommonEventDefinitionConsumerSchemaType,
  EventTypeNames,
  CommonEventDefinitionPublisherSchemaType,
} from './eventTypes'

export class DomainEventEmitter<SupportedEvents extends CommonEventDefinition[]> {
  private readonly eventRegistry: EventRegistry<SupportedEvents>

  private readonly eventHandlerMap: Record<
    string,
    EventHandler<CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>>[]
  > = {}
  private readonly anyHandlers: AnyEventHandler<SupportedEvents>[] = []
  private readonly metadataFiller: MetadataFiller

  constructor({
    eventRegistry,
    metadataFiller,
  }: {
    eventRegistry: EventRegistry<SupportedEvents>
    metadataFiller: MetadataFiller
  }) {
    this.eventRegistry = eventRegistry
    this.metadataFiller = metadataFiller
  }

  public async emit<SupportedEvent extends SupportedEvents[number]>(
    supportedEvent: SupportedEvent,
    data: Omit<CommonEventDefinitionPublisherSchemaType<SupportedEvent>, 'type'>,
    metadata?: Partial<MessageMetadataType>,
  ) {
    if (!data.timestamp) {
      data.timestamp = this.metadataFiller.produceTimestamp()
    }
    if (!data.id) {
      data.id = this.metadataFiller.produceId()
    }

    const eventTypeName = supportedEvent.publisherSchema.shape.type.value

    if (!this.eventRegistry.isSupportedEvent(eventTypeName)) {
      throw new InternalError({
        errorCode: 'UNKNOWN_EVENT',
        message: `Unknown event ${eventTypeName}`,
      })
    }

    const eventHandlers = this.eventHandlerMap[eventTypeName]

    // No relevant handlers are registered, we can stop processing
    if (!eventHandlers && this.anyHandlers.length === 0) {
      return
    }

    const validatedEvent = this.eventRegistry
      .getEventDefinitionByTypeName(eventTypeName)
      .consumerSchema.parse({
        type: eventTypeName,
        ...data,
      })

    if (eventHandlers) {
      for (const handler of eventHandlers) {
        await handler.handleEvent(validatedEvent, metadata)
      }
    }

    for (const handler of this.anyHandlers) {
      await handler.handleEvent(validatedEvent, metadata)
    }
  }

  /**
   * Register handler for a specific event
   */
  public on<EventTypeName extends EventTypeNames<SupportedEvents[number]>>(
    eventTypeName: EventTypeName,
    handler: SingleEventHandler<SupportedEvents, EventTypeName>,
  ) {
    this.addOnHandler(eventTypeName, handler)
  }

  /**
   * Register handler for multiple events
   */
  public onMany<EventTypeName extends EventTypeNames<SupportedEvents[number]>>(
    eventTypeNames: EventTypeName[],
    handler: SingleEventHandler<SupportedEvents, EventTypeName>,
  ) {
    for (const eventTypeName of eventTypeNames) {
      this.on(eventTypeName, handler)
    }
  }

  /**
   * Register handler for all events supported by the emitter
   */
  public onAny(handler: AnyEventHandler<SupportedEvents>) {
    this.anyHandlers.push(handler)
  }

  private addOnHandler(
    eventTypeName: EventTypeNames<SupportedEvents[number]>,
    handler: EventHandler,
  ) {
    if (!this.eventHandlerMap[eventTypeName]) {
      this.eventHandlerMap[eventTypeName] = []
    }

    this.eventHandlerMap[eventTypeName].push(handler)
  }
}
