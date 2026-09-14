import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import {
  type CommonLogger,
  type ErrorReporter,
  InternalError,
  isError,
  resolveGlobalErrorLogObject,
  type TransactionObservabilityManager,
} from '@lokalise/node-core'
import type { ConsumerMessageMetadataType } from '@message-queue-toolkit/schemas'
import type { MetadataFiller } from '../messages/MetadataFiller.ts'
import type { HandlerSpy, HandlerSpyParams, PublicHandlerSpy } from '../queues/HandlerSpy.ts'
import { resolveHandlerSpy } from '../queues/HandlerSpy.ts'
import type { EventRegistry } from './EventRegistry.ts'
import type {
  AnyEventHandler,
  CommonEventDefinition,
  CommonEventDefinitionConsumerSchemaType,
  CommonEventDefinitionPublisherSchemaType,
  EventHandler,
  EventTypeNames,
  SingleEventHandler,
} from './eventTypes.ts'

/**
 * Upper bound on how many times dispose() re-checks for event dispatches that registered while it
 * was draining. Each pass only continues if the previous one found work, so this is a guard against
 * endless event chains rather than a limit on normal shutdowns.
 */
const MAX_DISPOSE_DRAIN_PASSES = 10

const DEFAULT_HANDLER_RETRY_OPTIONS = {
  maxRetries: 0,
  baseRetryDelayMs: 100,
  maxRetryDelayMs: 1000,
} as const

/**
 * Retries are performed in-memory, within the same dispatch, so they do not survive a process
 * restart and the handler must be idempotent. `dispose()` waits for them to finish, so a large
 * retry budget directly extends graceful shutdown - keep it small.
 */
export type EventHandlerRetryOptions = {
  /**
   * Number of extra attempts after the initial one.
   * @default 0
   */
  maxRetries?: number
  /**
   * Base delay for the exponential backoff.
   * Actual delay = min(baseRetryDelayMs * 2 ^ attempt, maxRetryDelayMs)
   * @default 100
   */
  baseRetryDelayMs?: number
  /**
   * Upper bound for the delay between attempts.
   * @default 1000
   */
  maxRetryDelayMs?: number
  /**
   * Decides whether an error is worth retrying. Every error is retried by default.
   */
  isRetryable?: (error: unknown) => boolean
}

export type EventHandlerRegistrationOptions = {
  isBackgroundHandler?: boolean
  retry?: EventHandlerRetryOptions
}

type ResolvedRetryOptions = Required<Omit<EventHandlerRetryOptions, 'isRetryable'>> &
  Pick<EventHandlerRetryOptions, 'isRetryable'>

const resolveRegistrationOptions = (
  options: boolean | EventHandlerRegistrationOptions,
): EventHandlerRegistrationOptions =>
  typeof options === 'boolean' ? { isBackgroundHandler: options } : options

const resolveRetryOptions = (retry?: EventHandlerRetryOptions): ResolvedRetryOptions => ({
  maxRetries: retry?.maxRetries ?? DEFAULT_HANDLER_RETRY_OPTIONS.maxRetries,
  baseRetryDelayMs: retry?.baseRetryDelayMs ?? DEFAULT_HANDLER_RETRY_OPTIONS.baseRetryDelayMs,
  maxRetryDelayMs: retry?.maxRetryDelayMs ?? DEFAULT_HANDLER_RETRY_OPTIONS.maxRetryDelayMs,
  isRetryable: retry?.isRetryable,
})

export type DomainEventEmitterDependencies<SupportedEvents extends CommonEventDefinition[]> = {
  eventRegistry: EventRegistry<SupportedEvents>
  metadataFiller: MetadataFiller
  logger: CommonLogger
  errorReporter?: ErrorReporter
  transactionObservabilityManager?: TransactionObservabilityManager
}

type HandlerRegistration<T> = {
  handler: T
  retry: ResolvedRetryOptions
}

type Handlers<T> = {
  background: HandlerRegistration<T>[]
  foreground: HandlerRegistration<T>[]
}

export class DomainEventEmitter<SupportedEvents extends CommonEventDefinition[]> {
  private readonly eventRegistry: EventRegistry<SupportedEvents>
  private readonly metadataFiller: MetadataFiller
  private readonly logger: CommonLogger
  private readonly errorReporter?: ErrorReporter
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  private readonly _handlerSpy?: HandlerSpy<
    CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>
  >

  private readonly eventHandlerMap: Map<
    string,
    Handlers<EventHandler<CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>>>
  >
  private readonly inProgressDispatchByEventId: Map<string, Promise<void>>
  private readonly inProgressBackgroundHandlerByEventId: Map<string, Promise<void>>
  private isDisposed = false

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

    this.eventHandlerMap = new Map()
    this.inProgressDispatchByEventId = new Map()
    this.inProgressBackgroundHandlerByEventId = new Map()
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

  /**
   * Waits for accepted events to finish being dispatched, then stops accepting new ones.
   *
   * Both maps are drained: an event is only in `inProgressBackgroundHandlerByEventId` once its
   * foreground handlers are done, so mid-foreground dispatches are tracked separately. Re-checked
   * until empty, since handlers can emit events of their own, and bounded so an endless chain
   * cannot hold shutdown open.
   */
  public async dispose(): Promise<void> {
    for (let pass = 0; pass < MAX_DISPOSE_DRAIN_PASSES; pass++) {
      const pending = [
        ...this.inProgressDispatchByEventId.values(),
        ...this.inProgressBackgroundHandlerByEventId.values(),
      ]
      if (pending.length === 0) break

      await Promise.all(pending)
    }

    const pendingCount =
      this.inProgressDispatchByEventId.size + this.inProgressBackgroundHandlerByEventId.size
    if (pendingCount > 0) {
      this.logger.error(
        { pendingEventDispatches: pendingCount },
        `Event dispatches still pending after ${MAX_DISPOSE_DRAIN_PASSES} drain passes, giving up on them`,
      )
    }

    // Set before clearing the handlers, so that an event emitted from this point on fails loudly
    // rather than finding an empty handler map and being silently discarded.
    this.isDisposed = true

    this.inProgressDispatchByEventId.clear()
    this.inProgressBackgroundHandlerByEventId.clear()
    this.eventHandlerMap.clear()
    this._handlerSpy?.clear()
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

    /*
			A disposed emitter has no handlers left, so the event would reach neither its listeners nor
			any listener that forwards it onwards (for example to a message broker). Failing here lets the
			caller fail and be retried, instead of completing successfully while its event silently went nowhere.
		*/
    if (this.isDisposed) {
      throw new InternalError({
        errorCode: 'EVENT_EMITTER_DISPOSED',
        message: `Cannot emit event ${eventTypeName}, emitter is already disposed`,
        details: { eventTypeName },
      })
    }

    if (!data.timestamp) data.timestamp = this.metadataFiller.produceTimestamp()
    if (!data.id) data.id = this.metadataFiller.produceId()
    if (!data.metadata) {
      data.metadata = this.metadataFiller.produceMetadata(
        // @ts-expect-error
        data,
        supportedEvent,
        precedingMessageMetadata ?? {},
      )
    }
    if (!data.metadata.correlationId) data.metadata.correlationId = this.metadataFiller.produceId()

    const validatedEvent = this.eventRegistry
      .getEventDefinitionByTypeName(eventTypeName)
      .publisherSchema.parse({ type: eventTypeName, ...data })

    // @ts-expect-error
    await this.handleEvent(validatedEvent)

    // @ts-expect-error
    return validatedEvent
  }

  /**
   * Register handler for a specific event
   */
  public on<EventTypeName extends EventTypeNames<SupportedEvents[number]>>(
    eventTypeName: EventTypeName,
    handler: SingleEventHandler<SupportedEvents, EventTypeName>,
    options: boolean | EventHandlerRegistrationOptions = false,
  ) {
    const { isBackgroundHandler, retry } = resolveRegistrationOptions(options)

    if (!this.eventHandlerMap.has(eventTypeName)) {
      this.eventHandlerMap.set(eventTypeName, { foreground: [], background: [] })
    }

    const registration = { handler, retry: resolveRetryOptions(retry) }
    if (isBackgroundHandler) this.eventHandlerMap.get(eventTypeName)?.background.push(registration)
    else this.eventHandlerMap.get(eventTypeName)?.foreground.push(registration)
  }

  /**
   * Register handler for multiple events
   */
  public onMany<EventTypeName extends EventTypeNames<SupportedEvents[number]>>(
    eventTypeNames: EventTypeName[],
    handler: SingleEventHandler<SupportedEvents, EventTypeName>,
    options: boolean | EventHandlerRegistrationOptions = false,
  ) {
    for (const eventTypeName of eventTypeNames) {
      this.on(eventTypeName, handler, options)
    }
  }

  /**
   * Register handler for all events supported by the emitter
   */
  public onAny(
    handler: AnyEventHandler<SupportedEvents>,
    options: boolean | EventHandlerRegistrationOptions = false,
  ) {
    this.onMany(Array.from(this.eventRegistry.supportedEventTypes), handler, options)
  }

  private async handleEvent<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionConsumerSchemaType<SupportedEvent>,
  ): Promise<void> {
    const eventHandlers = this.eventHandlerMap.get(event.type)
    if (!eventHandlers) return

    const dispatch = this.executeHandlers(event, eventHandlers)
    /**
     * Registering it so that when dispose is executed before we get to background handler, we still finish processing
     */
    this.inProgressDispatchByEventId.set(
      event.id,
      dispatch.catch(() => {}),
    )

    try {
      await dispatch
    } finally {
      this.inProgressDispatchByEventId.delete(event.id)
    }
  }

  private async executeHandlers<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionConsumerSchemaType<SupportedEvent>,
    eventHandlers: Handlers<
      EventHandler<CommonEventDefinitionConsumerSchemaType<SupportedEvents[number]>>
    >,
  ): Promise<void> {
    for (const registration of eventHandlers.foreground) {
      await this.executeEventHandler(event, registration, false)
    }

    const bgPromise = Promise.all(
      eventHandlers.background.map((registration) =>
        this.executeEventHandler(event, registration, true),
      ),
    ).then(() => {
      this.inProgressBackgroundHandlerByEventId.delete(event.id)
      if (!this._handlerSpy) return
      this._handlerSpy.addProcessedMessage(
        {
          message: event,
          processingResult: { status: 'consumed' },
        },
        event.id,
        event.type,
      )
    })
    this.inProgressBackgroundHandlerByEventId.set(event.id, bgPromise)
  }

  private async executeEventHandler<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionConsumerSchemaType<SupportedEvent>,
    registration: HandlerRegistration<
      EventHandler<CommonEventDefinitionConsumerSchemaType<SupportedEvent>>
    >,
    isBackgroundHandler: boolean,
  ) {
    const { handler, retry } = registration

    for (let attempt = 0; ; attempt++) {
      try {
        await this.executeHandlerAttempt(event, handler, isBackgroundHandler)
        return
      } catch (error) {
        const attempts = attempt + 1
        const shouldRetry = attempt < retry.maxRetries && (retry.isRetryable?.(error) ?? true)
        if (!shouldRetry) {
          return this.handleFailedEventHandler(event, handler, isBackgroundHandler, error, attempts)
        }

        this.logger.error({
          ...resolveGlobalErrorLogObject(error),
          ...this.buildErrorContext(event, handler, attempts),
          maxAttempts: retry.maxRetries + 1,
          msg: `Event handler ${handler.eventHandlerId} failed, retrying`,
        })

        await setTimeout(Math.min(retry.baseRetryDelayMs * 2 ** attempt, retry.maxRetryDelayMs))
      }
    }
  }

  private async executeHandlerAttempt<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionConsumerSchemaType<SupportedEvent>,
    handler: EventHandler<CommonEventDefinitionConsumerSchemaType<SupportedEvent>>,
    isBackgroundHandler: boolean,
  ) {
    const transactionId = randomUUID()
    let isSuccessful = false
    try {
      this.transactionObservabilityManager?.startWithGroup(
        this.buildTransactionKey(event, handler, isBackgroundHandler),
        transactionId,
        event.type,
      )
      await handler.handleEvent(event)
      isSuccessful = true
    } finally {
      this.transactionObservabilityManager?.stop(transactionId, isSuccessful)
    }
  }

  private handleFailedEventHandler<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionConsumerSchemaType<SupportedEvent>,
    handler: EventHandler<CommonEventDefinitionConsumerSchemaType<SupportedEvent>>,
    isBackgroundHandler: boolean,
    error: unknown,
    attempts: number,
  ) {
    if (!isBackgroundHandler) throw error

    const context = this.buildErrorContext(event, handler, attempts)
    this.logger.error({
      ...resolveGlobalErrorLogObject(error),
      ...context,
    })
    this.errorReporter?.report({
      error: isError(error)
        ? error
        : new Error(`Event handler ${handler.eventHandlerId} threw a non-error value`),
      context,
    })
  }

  private buildErrorContext<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionConsumerSchemaType<SupportedEvent>,
    handler: EventHandler<CommonEventDefinitionConsumerSchemaType<SupportedEvent>>,
    attempts: number,
  ) {
    return {
      event: JSON.stringify(event),
      eventHandlerId: handler.eventHandlerId,
      'x-request-id': event.metadata?.correlationId,
      attempts,
    }
  }

  private buildTransactionKey<SupportedEvent extends SupportedEvents[number]>(
    event: CommonEventDefinitionConsumerSchemaType<SupportedEvent>,
    handler: EventHandler<CommonEventDefinitionConsumerSchemaType<SupportedEvent>>,
    isBackgroundHandler: boolean,
  ): string {
    return `${isBackgroundHandler ? 'bg' : 'fg'}_event_listener:${event.type}:${handler.eventHandlerId}`
  }
}
