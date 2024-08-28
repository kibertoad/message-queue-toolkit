import { type ErrorReporter, InternalError } from '@lokalise/node-core'

import type { MetadataFiller } from '../messages/MetadataFiller'
import type { HandlerSpy, HandlerSpyParams, PublicHandlerSpy } from '../queues/HandlerSpy'
import { resolveHandlerSpy } from '../queues/HandlerSpy'

import type { ConsumerMessageMetadataType } from '@message-queue-toolkit/schemas'
import type { Logger } from '../types/MessageQueueTypes'
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

export type DomainEventEmitterDependencies<SupportedEvents extends CommonEventDefinition[]> = {
  eventRegistry: EventRegistry<SupportedEvents>
  metadataFiller: MetadataFiller
  // TODO: make them mandatory is a breaking change, decide if we are fine with that
  logger?: Logger
  errorReporter?: ErrorReporter
}

type Handlers<T> = {
  background: T[]
  foreground: T[]
}

export class DomainEventEmitter<SupportedEvents extends CommonEventDefinition[]> {
  private readonly eventRegistry: EventRegistry<SupportedEvents>
  private readonly metadataFiller: MetadataFiller
  private readonly logger?: Logger
  private readonly errorReporter?: ErrorReporter
  private readonly _handlerSpy?: HandlerSpy<
    CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>
  >

  private readonly eventHandlerMap: Record<
    string,
    Handlers<EventHandler<CommonEventDefinitionPublisherSchemaType<SupportedEvents[number]>>>
  >
  private readonly anyHandlers: Handlers<AnyEventHandler<SupportedEvents>>

  constructor(
    deps: DomainEventEmitterDependencies<SupportedEvents>,
    options: {
      handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
    } = {},
  ) {
    this.eventRegistry = deps.eventRegistry
    this.metadataFiller = deps.metadataFiller
    this.logger = deps.logger
    this.errorReporter = deps.errorReporter

    this._handlerSpy =
      resolveHandlerSpy<CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>>(options)

    this.eventHandlerMap = {}
    this.anyHandlers = { background: [], foreground: [] }
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

    const validatedEvent = this.eventRegistry
      .getEventDefinitionByTypeName(eventTypeName)
      .publisherSchema.parse({ type: eventTypeName, ...data })

    await this.handleEvent(validatedEvent)

    if (this._handlerSpy) {
      this._handlerSpy.addProcessedMessage(
        {
          // @ts-ignore
          message: validatedEvent,
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
    bgHandler = false,
  ) {
    if (!this.eventHandlerMap[eventTypeName]) {
      this.eventHandlerMap[eventTypeName] = { foreground: [], background: [] }
    }

    if (bgHandler) this.eventHandlerMap[eventTypeName].background.push(handler)
    else this.eventHandlerMap[eventTypeName].foreground.push(handler)
  }

  /**
   * Register handler for multiple events
   */
  public onMany<EventTypeName extends EventTypeNames<SupportedEvents[number]>>(
    eventTypeNames: EventTypeName[],
    handler: SingleEventHandler<SupportedEvents, EventTypeName>,
    bgHandler = false,
  ) {
    for (const eventTypeName of eventTypeNames) {
      this.on(eventTypeName, handler, bgHandler)
    }
  }

  /**
   * Register handler for all events supported by the emitter
   */
  public onAny(handler: AnyEventHandler<SupportedEvents>, bgHandler = false) {
    if (bgHandler) this.anyHandlers.background.push(handler)
    else this.anyHandlers.foreground.push(handler)
  }

  private async handleEvent<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionPublisherSchemaType<SupportedEvent>,
  ): Promise<void> {
    const eventHandlers = this.eventHandlerMap[event.type] ?? {
      foreground: [],
      background: [],
    }

    const fgHandlers = [...eventHandlers.foreground, ...this.anyHandlers.foreground]
    for (const handler of fgHandlers) {
      await handler.handleEvent(event)
    }

    // const bgHandlers = [...eventHandlers.background, ...this.anyHandlers.background]
    // TODO: implement async handling
  }
}
