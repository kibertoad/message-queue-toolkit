import type { z } from 'zod/v4'
import type { PublisherBaseEventType } from '../events/baseEventSchemas.ts'
import type { EventRegistry } from '../events/EventRegistry.ts'
import type { CommonEventDefinition } from '../events/eventTypes.ts'
import type { MetadataFiller } from '../messages/MetadataFiller.ts'
import type { AsyncPublisher, SyncPublisher } from '../types/MessageQueueTypes.ts'
import type { CommonCreationConfigType, QueuePublisherOptions } from '../types/queueOptionsTypes.ts'

import type { PublicHandlerSpy } from './HandlerSpy.ts'

export type MessagePublishType<T extends CommonEventDefinition> = z.input<T['publisherSchema']>

export type MessageSchemaType<T extends CommonEventDefinition> = z.input<T['consumerSchema']>

export type AbstractPublisherFactory<
  PublisherType extends AsyncPublisher<object, unknown> | SyncPublisher<object, unknown>,
  DependenciesType,
  CreationConfigType extends CommonCreationConfigType,
  QueueLocatorType extends object,
  EventType extends PublisherBaseEventType,
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
  EventType extends PublisherBaseEventType,
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
    z.input<SupportedEventDefinitions[number]['publisherSchema']>,
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
      z.input<SupportedEventDefinitions[number]['publisherSchema']>,
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

  // biome-ignore lint/correctness/noUnusedPrivateClassMembers: this is used in constructor
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

  // biome-ignore lint/correctness/noUnusedPrivateClassMembers: this is used in constructor
  private registerPublishers() {
    for (const eventTarget in this.targetToEventMap) {
      if (this.targetToPublisherMap[eventTarget]) {
        continue
      }

      const messageSchemas = this.targetToEventMap[eventTarget].map((entry) => {
        return entry.consumerSchema
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

  async initRegisteredPublishers(publishersToInit?: EventTargets[]): Promise<void> {
    if (publishersToInit) {
      for (const eventTarget of publishersToInit) {
        const resolvedPublisher = this.targetToPublisherMap[eventTarget]
        if (!resolvedPublisher) {
          throw new Error(`Unsupported publisher ${eventTarget}`)
        }
        await resolvedPublisher.init()
      }
      return
    }

    for (const eventTarget in this.targetToPublisherMap) {
      await this.targetToPublisherMap[eventTarget].init()
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
    precedingEventMetadata?: Partial<MetadataType>,
    messageOptions?: MessageOptionsType,
  ): Promise<MessageSchemaType<SupportedEventDefinitions[number]>> {
    const publisher = this.targetToPublisherMap[eventTarget]
    if (!publisher) {
      throw new Error(
        `No publisher for target ${eventTarget} - did you perhaps forget to update supportedEvents passed to EventRegistry?`,
      )
    }
    const messageDefinition = this.resolveMessageDefinition(eventTarget, message)
    if (!messageDefinition) {
      throw new Error(
        `MessageDefinition for target "${eventTarget}" and type "${message.type}" not found in EventRegistry`,
      )
    }
    const resolvedMessage = this.resolveMessage(messageDefinition, message, precedingEventMetadata)

    if (this.isAsync) {
      await (publisher as AsyncPublisher<object, unknown>).publish(resolvedMessage, messageOptions)
    } else {
      publisher.publish(resolvedMessage, messageOptions)
    }

    return resolvedMessage
  }

  protected resolveMessageDefinition(
    eventTarget: EventTargets,
    message: MessagePublishType<SupportedEventDefinitions[number]>,
  ) {
    // ToDo optimize the lookup
    return this.targetToEventMap[eventTarget].find(
      (entry) => entry.consumerSchema.shape.type.value === message.type,
    )
  }

  protected resolveMessage(
    messageDefinition: EventDefinitionType | undefined,
    message: MessagePublishType<SupportedEventDefinitions[number]>,
    precedingEventMetadata?: Partial<MetadataType>,
  ): MessageSchemaType<SupportedEventDefinitions[number]> {
    const producedMetadata = this.metadataFiller.produceMetadata(
      message,
      // @ts-expect-error
      messageDefinition,
      precedingEventMetadata,
    )

    // @ts-expect-error
    const resolvedMetadata = message[this.metadataField]
      ? {
          ...producedMetadata,
          // @ts-expect-error
          ...message[this.metadataField],
        }
      : // @ts-ignore
        producedMetadata

    // @ts-expect-error
    return {
      id: this.metadataFiller.produceId(),
      timestamp: this.metadataFiller.produceTimestamp(),
      ...message,
      metadata: resolvedMetadata,
    }
  }

  public resolveBaseFields() {
    return {
      id: this.metadataFiller.produceId(),
      timestamp: this.metadataFiller.produceTimestamp(),
    }
  }

  /**
   * @param eventTarget - topic or exchange
   */
  public handlerSpy(eventTarget: EventTargets): PublicHandlerSpy<object> {
    const publisher = this.targetToPublisherMap[eventTarget]

    if (!publisher) {
      throw new Error(
        `No publisher for target ${eventTarget} - did you perhaps forget to update supportedEvents passed to EventRegistry?`,
      )
    }

    return publisher.handlerSpy
  }
}
