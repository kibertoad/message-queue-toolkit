import type { SNSClient } from '@aws-sdk/client-sns'
import type { ErrorReporter } from '@lokalise/node-core'
import type {
  BaseEventType,
  CommonEventDefinition,
  EventRegistry,
  Logger,
  MetadataFiller,
} from '@message-queue-toolkit/core'
import type { MessageMetadataType } from '@message-queue-toolkit/core/lib/messages/baseMessageSchemas'
import type { SNSDependencies } from '@message-queue-toolkit/sns'
import type z from 'zod'

import type { AbstractSnsPublisher, SNSPublisherOptions } from './AbstractSnsPublisher'
import type { SNSCreationConfig } from './AbstractSnsService'
import type { SnsPublisherFactory } from './CommonSnsPublisherFactory'
import { CommonSnsPublisherFactory } from './CommonSnsPublisherFactory'

export type SnsAwareEventDefinition = {
  snsTopic?: string
} & CommonEventDefinition

export type SnsPublisherManagerDependencies<SupportedEvents extends SnsAwareEventDefinition[]> = {
  eventRegistry: EventRegistry<SupportedEvents>
} & SNSDependencies

export type SnsPublisherManagerOptions<
  T extends AbstractSnsPublisher<EventType>,
  EventType extends BaseEventType,
  MetadataType,
> = {
  metadataField?: string
  publisherFactory: SnsPublisherFactory<T, EventType>
  metadataFiller: MetadataFiller<EventType, MetadataType>
  newPublisherOptions: Omit<
    SNSPublisherOptions<EventType>,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  > & {
    creationConfig?: Omit<SNSCreationConfig, 'topic'>
  }
}

export type SnsMessageSchemaType<T extends SnsAwareEventDefinition> = z.infer<T['schema']>

export type SnsMessagePublishType<T extends SnsAwareEventDefinition> = Pick<
  z.infer<T['schema']>,
  'type' | 'payload'
> & { id?: string }

export class SnsPublisherManager<
  T extends AbstractSnsPublisher<z.infer<SupportedEventDefinitions[number]['schema']>>,
  SupportedEventDefinitions extends SnsAwareEventDefinition[],
  MetadataType = MessageMetadataType,
> {
  private readonly publisherFactory: SnsPublisherFactory<
    T,
    z.infer<SupportedEventDefinitions[number]['schema']>
  >
  private readonly snsClient: SNSClient
  private readonly logger: Logger
  private readonly errorReporter: ErrorReporter

  private readonly topicToEventMap: Record<string, CommonEventDefinition[]> = {}
  private topicToPublisherMap: Record<string, T> = {}
  private readonly newPublisherOptions: Omit<
    SNSPublisherOptions<z.infer<SupportedEventDefinitions[number]['schema']>>,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  >
  private readonly metadataFiller: MetadataFiller<
    z.infer<SupportedEventDefinitions[number]['schema']>,
    MetadataType
  >
  private readonly metadataField: string

  constructor(
    dependencies: SnsPublisherManagerDependencies<SupportedEventDefinitions>,
    options: SnsPublisherManagerOptions<
      T,
      z.infer<SupportedEventDefinitions[number]['schema']>,
      MetadataType
    >,
  ) {
    this.snsClient = dependencies.snsClient
    this.errorReporter = dependencies.errorReporter
    this.logger = dependencies.logger
    this.publisherFactory = options.publisherFactory ?? new CommonSnsPublisherFactory()
    this.newPublisherOptions = options.newPublisherOptions
    this.metadataFiller = options.metadataFiller
    this.metadataField = options.metadataField ?? 'metadata'

    this.registerEvents(dependencies.eventRegistry.supportedEvents)
    this.registerPublishers()
  }

  private registerEvents(events: SupportedEventDefinitions) {
    for (const supportedEvent of events) {
      if (!supportedEvent.snsTopic) {
        continue
      }

      if (!this.topicToEventMap[supportedEvent.snsTopic]) {
        this.topicToEventMap[supportedEvent.snsTopic] = []
      }

      this.topicToEventMap[supportedEvent.snsTopic].push(supportedEvent)
    }
  }

  private registerPublishers() {
    const dependencies: SNSDependencies = {
      snsClient: this.snsClient,
      logger: this.logger,
      errorReporter: this.errorReporter,
    }
    for (const snsTopic in this.topicToEventMap) {
      if (this.topicToPublisherMap[snsTopic]) {
        continue
      }

      const messageSchemas = this.topicToEventMap[snsTopic].map((entry) => {
        return entry.schema
      })
      const creationConfig: SNSCreationConfig = {
        ...this.newPublisherOptions,
        topic: {
          Name: snsTopic,
        },
      }

      this.topicToPublisherMap[snsTopic] = this.publisherFactory.buildPublisher(dependencies, {
        ...this.newPublisherOptions,
        creationConfig,
        messageSchemas,
      })
    }
  }

  public injectPublisher(snsTopic: string, publisher: T) {
    this.topicToPublisherMap[snsTopic] = publisher
  }

  public async publish(
    snsTopic: string,
    message: SnsMessagePublishType<SupportedEventDefinitions[number]>,
    precedingEventMetadata?: MetadataType,
  ): Promise<SnsMessageSchemaType<SupportedEventDefinitions[number]>> {
    const publisher = this.topicToPublisherMap[snsTopic]

    if (!publisher) {
      throw new Error(`No publisher for topic ${snsTopic}`)
    }

    // @ts-ignore
    const resolvedMetadata = message[this.metadataField]
      ? // @ts-ignore
        message[this.metadataField]
      : // @ts-ignore
        this.metadataFiller.produceMetadata(message, precedingEventMetadata)

    const resolvedMessage: SnsMessageSchemaType<SupportedEventDefinitions[number]> = {
      id: message.id ? message.id : this.metadataFiller.produceId(),
      timestamp: this.metadataFiller.produceTimestamp(),
      ...message,
      // @ts-ignore
      metadata: resolvedMetadata,
    }

    await publisher.publish(resolvedMessage)

    return resolvedMessage
  }

  public handlerSpy(snsTopic: string) {
    const publisher = this.topicToPublisherMap[snsTopic]

    if (!publisher) {
      throw new Error(`No publisher for topic ${snsTopic}`)
    }

    return publisher.handlerSpy
  }
}
