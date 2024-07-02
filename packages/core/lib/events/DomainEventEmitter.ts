import { InternalError } from '@lokalise/node-core'

import type { MetadataFiller } from '../messages/MetadataFiller'
import type { HandlerSpy, HandlerSpyParams, PublicHandlerSpy } from '../queues/HandlerSpy'
import { resolveHandlerSpy } from '../queues/HandlerSpy'

import type { PublisherMessageMetadataType } from '@message-queue-toolkit/schemas'
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
    EventHandler<CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>>[]
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
    CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>
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
    metadata?: PublisherMessageMetadataType,
  ): Promise<Omit<CommonEventDefinitionConsumerSchemaType<SupportedEvent>, 'type'>> {
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
      // @ts-ignore
      return data
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

    if (this._handlerSpy) {
      this._handlerSpy.addProcessedMessage(
        {
          // @ts-ignore
          message: {
            ...validatedEvent,
            ...(metadata !== undefined ? { metadata } : {}),
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
}
