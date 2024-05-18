import type {
  AsyncPublisher,
  CommonEventDefinition,
  QueuePublisherOptions,
} from '@message-queue-toolkit/core'
import type { TypeOf, z } from 'zod'

import type { EventRegistry } from '../events/EventRegistry'
import type { BaseEventType } from '../events/baseEventSchemas'
import type { MetadataFiller } from '../messages/MetadataFiller'
import type { SyncPublisher } from '../types/MessageQueueTypes'
import type { CommonCreationConfigType } from '../types/queueOptionsTypes'

export type MessagePublishType<T extends CommonEventDefinition> = Pick<
  z.infer<T['schema']>,
  'type' | 'payload'
> & { id?: string }

export type MessageSchemaType<T extends CommonEventDefinition> = z.infer<T['schema']>

export type AbstractPublisherFactory<
  PublisherType extends AsyncPublisher<object, unknown> | SyncPublisher<object, unknown>,
  DependenciesType,
  CreationConfigType extends CommonCreationConfigType,
  QueueLocatorType extends object,
  EventType extends BaseEventType,
  OptionsType extends Omit<
    QueuePublisherOptions<CreationConfigType, QueueLocatorType, EventType>,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  >,
> = {
  buildPublisher(dependencies: DependenciesType, options: OptionsType): PublisherType
}

export abstract class AbstractPublisherManager<
  EventDefinitionType extends CommonEventDefinition,
  EventTargets extends string,
  PublisherType extends AsyncPublisher<object, unknown> | SyncPublisher<object, unknown>,
  DependenciesType,
  CreationConfigType extends CommonCreationConfigType,
  QueueLocatorType extends object,
  EventType extends BaseEventType,
  OptionsType extends Omit<
    QueuePublisherOptions<CreationConfigType, QueueLocatorType, EventType>,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  >,
  SupportedEventDefinitions extends EventDefinitionType[],
  MetadataType,
  MessageOptionsType,
> {
  private readonly publisherFactory: AbstractPublisherFactory<
    PublisherType,
    DependenciesType,
    CreationConfigType,
    QueueLocatorType,
    EventType,
    OptionsType
  >

  protected readonly newPublisherOptions: OptionsType

  protected readonly metadataFiller: MetadataFiller<
    z.infer<SupportedEventDefinitions[number]['schema']>,
    MetadataType
  >
  protected readonly metadataField: string

  // In this context "target" can be a topic or an exchange, depending on the transport
  protected readonly targetToEventMap: Record<EventTargets, EventDefinitionType[]> = {} as Record<
    EventTargets,
    EventDefinitionType[]
  >
  protected readonly isAsync: boolean
  protected targetToPublisherMap: Record<EventTargets, PublisherType> = {} as Record<
    EventTargets,
    PublisherType
  >
  private readonly publisherDependencies: DependenciesType

  protected constructor({
    publisherFactory,
    newPublisherOptions,
    publisherDependencies,
    metadataFiller,
    eventRegistry,
    metadataField,
    isAsync,
  }: {
    publisherFactory: AbstractPublisherFactory<
      PublisherType,
      DependenciesType,
      CreationConfigType,
      QueueLocatorType,
      EventType,
      OptionsType
    >
    newPublisherOptions: OptionsType
    publisherDependencies: DependenciesType
    metadataFiller: MetadataFiller<
      TypeOf<SupportedEventDefinitions[number]['schema']>,
      MetadataType
    >
    eventRegistry: EventRegistry<SupportedEventDefinitions>
    metadataField: string
    isAsync: boolean
  }) {
    this.publisherFactory = publisherFactory
    this.newPublisherOptions = newPublisherOptions
    this.metadataFiller = metadataFiller
    this.metadataField = metadataField
    this.isAsync = isAsync
    this.publisherDependencies = publisherDependencies

    this.registerEvents(eventRegistry.supportedEvents)
    this.registerPublishers()
  }

  protected abstract resolveEventTarget(event: EventDefinitionType): EventTargets | undefined
  protected abstract resolveCreationConfig(eventTarget: string): CreationConfigType
  protected resolvePublisherConfigOverrides(_eventTarget: string): Partial<OptionsType> {
    return {}
  }

  private registerEvents(events: SupportedEventDefinitions) {
    for (const supportedEvent of events) {
      const eventTarget = this.resolveEventTarget(supportedEvent)

      if (!eventTarget) {
        continue
      }

      if (!this.targetToEventMap[eventTarget]) {
        this.targetToEventMap[eventTarget] = []
      }

      this.targetToEventMap[eventTarget].push(supportedEvent)
    }
  }
  private registerPublishers() {
    for (const eventTarget in this.targetToEventMap) {
      if (this.targetToPublisherMap[eventTarget]) {
        continue
      }

      const messageSchemas = this.targetToEventMap[eventTarget].map((entry) => {
        return entry.schema
      })
      const creationConfig = this.resolveCreationConfig(eventTarget)
      const configOverrides = this.resolvePublisherConfigOverrides(eventTarget)

      this.targetToPublisherMap[eventTarget] = this.publisherFactory.buildPublisher(
        this.publisherDependencies,
        {
          ...this.newPublisherOptions,
          creationConfig,
          messageSchemas,
          ...configOverrides,
        },
      )
    }
  }

  public injectPublisher(eventTarget: EventTargets, publisher: PublisherType) {
    this.targetToPublisherMap[eventTarget] = publisher
  }

  public injectEventDefinition(eventDefinition: EventDefinitionType) {
    const eventTarget = this.resolveEventTarget(eventDefinition)
    if (!eventTarget) {
      throw new Error('eventTarget could not be resolved for the event')
    }

    if (!this.targetToEventMap[eventTarget]) {
      this.targetToEventMap[eventTarget] = []
    }

    this.targetToEventMap[eventTarget].push(eventDefinition)
  }

  public async publish(
    eventTarget: EventTargets,
    message: MessagePublishType<SupportedEventDefinitions[number]>,
    precedingEventMetadata?: MetadataType,
    messageOptions?: MessageOptionsType,
  ): Promise<MessageSchemaType<SupportedEventDefinitions[number]>> {
    const publisher = this.targetToPublisherMap[eventTarget]
    if (!publisher) {
      throw new Error(`No publisher for target ${eventTarget}`)
    }
    // ToDo optimize the lookup
    const messageDefinition = this.targetToEventMap[eventTarget].find(
      (entry) => entry.schema.shape.type.value === message.type,
    )

    const resolvedMessage = this.resolveMessage(messageDefinition, message, precedingEventMetadata)

    if (this.isAsync) {
      await (publisher as AsyncPublisher<object, unknown>).publish(resolvedMessage, messageOptions)
    } else {
      (publisher as SyncPublisher<object, unknown>).publish(resolvedMessage, messageOptions)
    }

    return resolvedMessage
  }

  protected resolveMessage(
    messageDefinition: EventDefinitionType | undefined,
    message: MessagePublishType<SupportedEventDefinitions[number]>,
    precedingEventMetadata?: MetadataType,
  ): MessageSchemaType<SupportedEventDefinitions[number]> {
    // @ts-ignore
    const resolvedMetadata = message[this.metadataField]
      ? // @ts-ignore
        message[this.metadataField]
      : // @ts-ignore
        this.metadataFiller.produceMetadata(message, messageDefinition, precedingEventMetadata)

    return {
      id: message.id ? message.id : this.metadataFiller.produceId(),
      timestamp: this.metadataFiller.produceTimestamp(),
      ...message,
      // @ts-ignore
      metadata: resolvedMetadata,
    }
  }

  /**
   * @param eventTarget - topic or exchange
   */
  public handlerSpy(eventTarget: EventTargets) {
    const publisher = this.targetToPublisherMap[eventTarget]

    if (!publisher) {
      throw new Error(`No publisher for target ${eventTarget}`)
    }

    return publisher.handlerSpy
  }
}
