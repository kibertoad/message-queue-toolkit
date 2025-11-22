import type {
  EventRegistry,
  MessagePublishType,
  MessageSchemaType,
  MetadataFiller,
  PublisherBaseEventType,
  PublisherMessageMetadataType,
} from '@message-queue-toolkit/core'
import { AbstractPublisherManager } from '@message-queue-toolkit/core'
import type z from 'zod/v4'

import type { PubSubAwareEventDefinition } from '../schemas/pubSubSchemas.ts'
import type { AbstractPubSubPublisher, PubSubMessageOptions } from './AbstractPubSubPublisher.ts'
import type {
  PubSubCreationConfig,
  PubSubDependencies,
  PubSubQueueLocatorType,
} from './AbstractPubSubService.ts'
import type { PubSubPublisherFactory } from './CommonPubSubPublisherFactory.ts'
import { CommonPubSubPublisherFactory } from './CommonPubSubPublisherFactory.ts'

export type { PubSubAwareEventDefinition }

export type PubSubPublisherManagerDependencies<
  SupportedEvents extends PubSubAwareEventDefinition[],
> = {
  eventRegistry: EventRegistry<SupportedEvents>
} & PubSubDependencies

export type PubSubPublisherManagerOptions<
  T extends AbstractPubSubPublisher<EventType>,
  EventType extends PublisherBaseEventType,
  MetadataType,
> = {
  metadataField?: string
  publisherFactory?: PubSubPublisherFactory<T, EventType>
  metadataFiller: MetadataFiller<EventType, MetadataType>
  newPublisherOptions: Omit<
    import('@message-queue-toolkit/core').QueuePublisherOptions<
      PubSubCreationConfig,
      PubSubQueueLocatorType,
      EventType
    >,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  > & {
    creationConfig?: Omit<PubSubCreationConfig, 'topic'>
  }
}

export type PubSubMessageSchemaType<T extends PubSubAwareEventDefinition> = z.output<
  T['publisherSchema']
>

export class PubSubPublisherManager<
  T extends AbstractPubSubPublisher<z.input<SupportedEventDefinitions[number]['publisherSchema']>>,
  SupportedEventDefinitions extends PubSubAwareEventDefinition[],
  MetadataType = PublisherMessageMetadataType,
> extends AbstractPublisherManager<
  PubSubAwareEventDefinition,
  NonNullable<SupportedEventDefinitions[number]['pubSubTopic']>,
  AbstractPubSubPublisher<z.input<SupportedEventDefinitions[number]['publisherSchema']>>,
  PubSubDependencies,
  PubSubCreationConfig,
  PubSubQueueLocatorType,
  PubSubMessageSchemaType<PubSubAwareEventDefinition>,
  Omit<
    import('@message-queue-toolkit/core').QueuePublisherOptions<
      PubSubCreationConfig,
      PubSubQueueLocatorType,
      z.input<SupportedEventDefinitions[number]['publisherSchema']>
    >,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  >,
  SupportedEventDefinitions,
  MetadataType,
  PubSubMessageOptions
> {
  constructor(
    dependencies: PubSubPublisherManagerDependencies<SupportedEventDefinitions>,
    options: PubSubPublisherManagerOptions<
      T,
      z.input<SupportedEventDefinitions[number]['publisherSchema']>,
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
        pubSubClient: dependencies.pubSubClient,
        logger: dependencies.logger,
        errorReporter: dependencies.errorReporter,
      },
      publisherFactory: options.publisherFactory ?? new CommonPubSubPublisherFactory(),
    })
  }

  override publish(
    topic: NonNullable<SupportedEventDefinitions[number]['pubSubTopic']>,
    message: MessagePublishType<SupportedEventDefinitions[number]>,
    precedingEventMetadata?: Partial<MetadataType>,
    messageOptions?: PubSubMessageOptions,
  ): Promise<MessageSchemaType<SupportedEventDefinitions[number]>> {
    // Purpose of this override is to provide better name for the first argument
    // For PubSub it is going to be topic
    return super.publish(topic, message, precedingEventMetadata, messageOptions)
  }

  protected override resolveCreationConfig(
    eventTarget: NonNullable<SupportedEventDefinitions[number]['pubSubTopic']>,
  ): PubSubCreationConfig {
    return {
      ...this.newPublisherOptions,
      topic: {
        name: eventTarget,
      },
    }
  }

  protected override resolveEventTarget(
    event: PubSubAwareEventDefinition,
  ): NonNullable<SupportedEventDefinitions[number]['pubSubTopic']> | undefined {
    return event.pubSubTopic
  }
}
