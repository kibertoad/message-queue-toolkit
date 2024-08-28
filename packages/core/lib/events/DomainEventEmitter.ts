import { InternalError } from '@lokalise/node-core'

import type { MetadataFiller } from '../messages/MetadataFiller'
import type { HandlerSpy, HandlerSpyParams, PublicHandlerSpy } from '../queues/HandlerSpy'
import { resolveHandlerSpy } from '../queues/HandlerSpy'

import type { ConsumerMessageMetadataType } from '@message-queue-toolkit/schemas'
import type { EventRegistry } from './EventRegistry'
import type {
  AnyEventHandler,
  CommonEventDefinition,
  CommonEventDefinitionConsumerSchemaType,
  CommonEventDefinitionPublisherSchemaType,
  EventHandler,
  EventTypeNames,
  SingleEventHandler,
} from './eventTypes'

export class DomainEventEmitter<SupportedEvents extends CommonEventDefinition[]> {
  private readonly eventRegistry: EventRegistry<SupportedEvents>

  private readonly eventHandlerMap: Record<
    string,
    EventHandler<CommonEventDefinitionPublisherSchemaType<SupportedEvents[number]>>[]
  > = {}
  private readonly anyHandlers: AnyEventHandler<SupportedEvents>[] = []
  private readonly metadataFiller: MetadataFiller
  private _handlerSpy:
    | HandlerSpy<CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>>
    | undefined

  constructor(
    {
      eventRegistry,
      metadataFiller,
    }: {
      eventRegistry: EventRegistry<SupportedEvents>
      metadataFiller: MetadataFiller
    },
    options: {
      handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
    } = {},
  ) {
    this.eventRegistry = eventRegistry
    this.metadataFiller = metadataFiller

    this._handlerSpy =
      resolveHandlerSpy<CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>>(options)
  }

  get handlerSpy(): PublicHandlerSpy<
    CommonEventDefinitionPublisherSchemaType<SupportedEvents[number]>
  > {
    if (!this._handlerSpy) {
      throw new Error(
        'HandlerSpy was not instantiated, please pass `handlerSpy` parameter during queue service creation.',
      )
    }
    return this._handlerSpy
  }

  public async emit<SupportedEvent extends SupportedEvents[number]>(
    supportedEvent: SupportedEvent,
    data: Omit<CommonEventDefinitionPublisherSchemaType<SupportedEvent>, 'type'>,
    precedingMessageMetadata?: Partial<ConsumerMessageMetadataType>,
  ): Promise<Omit<CommonEventDefinitionConsumerSchemaType<SupportedEvent>, 'type'>> {
    const eventTypeName = supportedEvent.publisherSchema.shape.type.value
    if (!this.eventRegistry.isSupportedEvent(eventTypeName)) {
      throw new InternalError({
        errorCode: 'UNKNOWN_EVENT',
        message: `Unknown event ${eventTypeName}`,
      })
    }

    const event = this.buildEvent(supportedEvent, eventTypeName, data, precedingMessageMetadata)
    const eventHandlers = this.eventHandlerMap[eventTypeName]

    // No relevant handlers are registered, we can stop processing
    if (!eventHandlers && this.anyHandlers.length === 0) {
      // @ts-ignore
      return event
    }

    const validatedEvent = this.eventRegistry
      .getEventDefinitionByTypeName(eventTypeName)
      .publisherSchema.parse(event)

    if (eventHandlers) {
      for (const handler of eventHandlers) {
        await handler.handleEvent(validatedEvent)
      }
    }

    for (const handler of this.anyHandlers) {
      await handler.handleEvent(validatedEvent)
    }

    if (this._handlerSpy) {
      this._handlerSpy.addProcessedMessage(
        {
          // @ts-ignore
          message: {
            ...validatedEvent,
          },
          processingResult: 'consumed',
        },
        validatedEvent.id,
      )
    }

    // @ts-ignore
    return validatedEvent
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

  private buildEvent<SupportedEvent extends SupportedEvents[number]>(
    supportedEvent: SupportedEvent,
    eventTypeName: string,
    data: Omit<CommonEventDefinitionPublisherSchemaType<SupportedEvent>, 'type'>,
    precedingMessageMetadata?: Partial<ConsumerMessageMetadataType>,
  ): CommonEventDefinitionPublisherSchemaType<SupportedEvent> {
    if (!data.timestamp) data.timestamp = this.metadataFiller.produceTimestamp()
    if (!data.id) data.id = this.metadataFiller.produceId()
    if (!data.metadata) {
      data.metadata = this.metadataFiller.produceMetadata(
        // @ts-ignore
        data,
        supportedEvent,
        precedingMessageMetadata ?? {},
      )
    }
    if (!data.metadata.correlationId) data.metadata.correlationId = this.metadataFiller.produceId()

    return { type: eventTypeName, ...data }
  }
}
