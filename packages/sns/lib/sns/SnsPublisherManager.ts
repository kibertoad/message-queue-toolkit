import type {
  BaseEventType,
  CommonEventDefinition,
  EventRegistry,
  MetadataFiller,
} from '@message-queue-toolkit/core'
import { AbstractPublisherManager } from '@message-queue-toolkit/core'
import type { MessageMetadataType } from '@message-queue-toolkit/core/lib/messages/baseMessageSchemas'
import type z from 'zod'

import type { AbstractSnsPublisher, SNSPublisherOptions } from './AbstractSnsPublisher'
import type { SNSCreationConfig, SNSDependencies, SNSQueueLocatorType } from './AbstractSnsService'
import type { SnsPublisherFactory } from './CommonSnsPublisherFactory'
import { CommonSnsPublisherFactory } from './CommonSnsPublisherFactory'

export type SnsAwareEventDefinition = {
  schemaVersion?: string
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
> extends AbstractPublisherManager<
  SnsAwareEventDefinition,
  NonNullable<SupportedEventDefinitions[number]['snsTopic']>,
  AbstractSnsPublisher<z.infer<SupportedEventDefinitions[number]['schema']>>,
  SNSDependencies,
  SNSCreationConfig,
  SNSQueueLocatorType,
  SnsMessageSchemaType<SnsAwareEventDefinition>,
  Omit<
    SNSPublisherOptions<z.infer<SupportedEventDefinitions[number]['schema']>>,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  >,
  SupportedEventDefinitions,
  MetadataType,
  z.infer<SupportedEventDefinitions[number]['schema']>
> {
  constructor(
    dependencies: SnsPublisherManagerDependencies<SupportedEventDefinitions>,
    options: SnsPublisherManagerOptions<
      T,
      z.infer<SupportedEventDefinitions[number]['schema']>,
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
