import {
  type ErrorReporter,
  InternalError,
  type TransactionObservabilityManager,
  resolveGlobalErrorLogObject,
} from '@lokalise/node-core'

import type { MetadataFiller } from '../messages/MetadataFiller'
import type { HandlerSpy, HandlerSpyParams, PublicHandlerSpy } from '../queues/HandlerSpy'
import { resolveHandlerSpy } from '../queues/HandlerSpy'

import { randomUUID } from 'node:crypto'
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
  logger: Logger
  errorReporter?: ErrorReporter
  transactionObservabilityManager?: TransactionObservabilityManager
}

type Handlers<T> = {
  background: T[]
  foreground: T[]
}

export class DomainEventEmitter<SupportedEvents extends CommonEventDefinition[]> {
  private readonly eventRegistry: EventRegistry<SupportedEvents>
  private readonly metadataFiller: MetadataFiller
  private readonly logger: Logger
  private readonly errorReporter?: ErrorReporter
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  private readonly _handlerSpy?: HandlerSpy<
    CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>
  >

  private readonly eventHandlerMap: Record<
    string,
    Handlers<EventHandler<CommonEventDefinitionPublisherSchemaType<SupportedEvents[number]>>>
  >
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
    this.transactionObservabilityManager = deps.transactionObservabilityManager

    this._handlerSpy =
      resolveHandlerSpy<CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>>(options)

    this.eventHandlerMap = {}
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
    isBackgroundHandler = false,
  ) {
    if (!this.eventHandlerMap[eventTypeName]) {
      this.eventHandlerMap[eventTypeName] = { foreground: [], background: [] }
    }

    if (isBackgroundHandler) this.eventHandlerMap[eventTypeName].background.push(handler)
    else this.eventHandlerMap[eventTypeName].foreground.push(handler)
  }

  /**
   * Register handler for multiple events
   */
  public onMany<EventTypeName extends EventTypeNames<SupportedEvents[number]>>(
    eventTypeNames: EventTypeName[],
    handler: SingleEventHandler<SupportedEvents, EventTypeName>,
    isBackgroundHandler = false,
  ) {
    for (const eventTypeName of eventTypeNames) {
      this.on(eventTypeName, handler, isBackgroundHandler)
    }
  }

  /**
   * Register handler for all events supported by the emitter
   */
  public onAny(handler: AnyEventHandler<SupportedEvents>, isBackgroundHandler = false) {
    for (const supportedEvent of this.eventRegistry.supportedEvents) {
      this.on(supportedEvent.consumerSchema.shape.type.value, handler, isBackgroundHandler)
    }
  }

  private async handleEvent<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionPublisherSchemaType<SupportedEvent>,
  ): Promise<void> {
    const eventHandlers = this.eventHandlerMap[event.type] ?? {
      foreground: [],
      background: [],
    }

    for (const handler of eventHandlers.foreground) {
      const transactionId = randomUUID()
      let isSuccessfull = false
      try {
        this.transactionObservabilityManager?.startWithGroup(
          this.buildTransactionKey(event, handler, false),
          transactionId,
          event.type,
        )
        await handler.handleEvent(event)
        isSuccessfull = true
      } finally {
        this.transactionObservabilityManager?.stop(transactionId, isSuccessfull)
      }
    }

    for (const handler of eventHandlers.background) {
      const transactionId = randomUUID()
      // not sure if we should use startWithGroup or start, using group to group all handlers for the same event type
      // should it be eventId + eventType or just eventType?
      this.transactionObservabilityManager?.startWithGroup(
        this.buildTransactionKey(event, handler, true),
        transactionId,
        event.type,
      )

      Promise.resolve(handler.handleEvent(event))
        .then(() => {
          this.transactionObservabilityManager?.stop(transactionId, true)
        })
        .catch((error) => {
          this.transactionObservabilityManager?.stop(transactionId, false)
          const context = {
            event: JSON.stringify(event),
            eventHandlerId: handler.eventHandlerId,
            'x-request-id': event.metadata?.correlationId,
          }
          this.logger.error({
            ...resolveGlobalErrorLogObject(error),
            ...context,
          })
          this.errorReporter?.report({ error: error, context })
        })
    }
  }

  private buildTransactionKey<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionPublisherSchemaType<SupportedEvent>,
    handler: EventHandler<CommonEventDefinitionPublisherSchemaType<SupportedEvent>>,
    isBackgroundHandler: boolean,
  ): string {
    return `${isBackgroundHandler ? 'bg' : 'fg'}_event_listener:${event.type}:${handler.eventHandlerId}`
  }
}
