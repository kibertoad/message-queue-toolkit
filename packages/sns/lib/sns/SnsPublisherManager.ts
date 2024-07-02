import type {
  EventRegistry,
  MessagePublishType,
  MessageSchemaType,
  MetadataFiller,
  PublisherBaseEventType,
} from '@message-queue-toolkit/core'
import { AbstractPublisherManager } from '@message-queue-toolkit/core'
import type { PublisherMessageMetadataType } from '@message-queue-toolkit/core/lib/messages/baseMessageSchemas'
import type { SnsAwareEventDefinition } from '@message-queue-toolkit/schemas'
import type z from 'zod'

import type {
  AbstractSnsPublisher,
  SNSMessageOptions,
  SNSPublisherOptions,
} from './AbstractSnsPublisher'
import type { SNSCreationConfig, SNSDependencies, SNSQueueLocatorType } from './AbstractSnsService'
import type { SnsPublisherFactory } from './CommonSnsPublisherFactory'
import { CommonSnsPublisherFactory } from './CommonSnsPublisherFactory'

export type { SnsAwareEventDefinition }

export type SnsPublisherManagerDependencies<SupportedEvents extends SnsAwareEventDefinition[]> = {
  eventRegistry: EventRegistry<SupportedEvents>
} & SNSDependencies

export type SnsPublisherManagerOptions<
  T extends AbstractSnsPublisher<EventType>,
  EventType extends PublisherBaseEventType,
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

export type SnsMessageSchemaType<T extends SnsAwareEventDefinition> = z.infer<T['publisherSchema']>

export class SnsPublisherManager<
  T extends AbstractSnsPublisher<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
  SupportedEventDefinitions extends SnsAwareEventDefinition[],
  MetadataType = PublisherMessageMetadataType,
> extends AbstractPublisherManager<
  SnsAwareEventDefinition,
  NonNullable<SupportedEventDefinitions[number]['snsTopic']>,
  AbstractSnsPublisher<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
  SNSDependencies,
  SNSCreationConfig,
  SNSQueueLocatorType,
  SnsMessageSchemaType<SnsAwareEventDefinition>,
  Omit<
    SNSPublisherOptions<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  >,
  SupportedEventDefinitions,
  MetadataType,
  SNSMessageOptions
> {
  constructor(
    dependencies: SnsPublisherManagerDependencies<SupportedEventDefinitions>,
    options: SnsPublisherManagerOptions<
      T,
      z.infer<SupportedEventDefinitions[number]['publisherSchema']>,
      MetadataType
    >,
  ) {
    super({
      isAsync: true,
      eventRegistry: dependencies.eventRegistry,
      metadataField: options.metadataField ?? 'metadata',
      metadataFiller: options.metadataFiller,
      newPublisherOptions: options.newPublisherOptions,
      publisherDependencies: {
        snsClient: dependencies.snsClient,
        logger: dependencies.logger,
        errorReporter: dependencies.errorReporter,
      },
      publisherFactory: options.publisherFactory ?? new CommonSnsPublisherFactory(),
    })
  }

  publish(
    topic: NonNullable<SupportedEventDefinitions[number]['snsTopic']>,
    message: MessagePublishType<SupportedEventDefinitions[number]>,
    precedingEventMetadata?: Partial<MetadataType>,
    messageOptions?: SNSMessageOptions,
  ): Promise<MessageSchemaType<SupportedEventDefinitions[number]>> {
    // Purpose of this override is to provide better name for the first argument
    // For SNS it is going to be topic
    return super.publish(topic, message, precedingEventMetadata, messageOptions)
  }

  protected override resolveCreationConfig(
    eventTarget: NonNullable<SupportedEventDefinitions[number]['snsTopic']>,
  ): SNSCreationConfig {
    return {
      ...this.newPublisherOptions,
      topic: {
        Name: eventTarget,
      },
    }
  }

  protected override resolveEventTarget(
    event: SnsAwareEventDefinition,
  ): NonNullable<SupportedEventDefinitions[number]['snsTopic']> | undefined {
    return event.snsTopic
  }
}
