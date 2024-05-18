import type {
  BaseEventType,
  CommonEventDefinition,
  EventRegistry,
  MetadataFiller,
} from '@message-queue-toolkit/core'
import { AbstractPublisherManager } from '@message-queue-toolkit/core'
import type { MessageMetadataType } from '@message-queue-toolkit/core/lib/messages/baseMessageSchemas'
import type z from 'zod'

import type { AbstractAmqpPublisher, AMQPPublisherOptions } from './AbstractAmqpPublisher'
import type {
  AbstractAmqpQueuePublisher,
  AmqpQueueMessageOptions,
} from './AbstractAmqpQueuePublisher'
import type { AMQPCreationConfig, AMQPDependencies, AMQPLocator } from './AbstractAmqpService'
import type { AmqpPublisherFactory } from './CommonAmqpPublisherFactory'
import { CommonAmqpQueuePublisherFactory } from './CommonAmqpPublisherFactory'

export type AmqpAwareEventDefinition = {
  schemaVersion?: string
  exchange?: string
  queueName?: string
} & CommonEventDefinition

export type AmqpPublisherManagerDependencies<SupportedEvents extends AmqpAwareEventDefinition[]> = {
  eventRegistry: EventRegistry<SupportedEvents>
} & AMQPDependencies

export type AmqpPublisherManagerOptions<
  T extends AbstractAmqpPublisher<EventType, MO>,
  MO,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  PO extends Omit<AMQPPublisherOptions<any>, 'creationConfig'>,
  EventType extends BaseEventType,
  MetadataType,
> = {
  metadataField?: string
  publisherFactory: AmqpPublisherFactory<T, EventType, MO, PO>
  metadataFiller: MetadataFiller<EventType, MetadataType>
  newPublisherOptions: Omit<
    AMQPPublisherOptions<EventType>,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  > & {
    creationConfig?: Omit<AMQPCreationConfig, 'queueName'>
  }
}

export type AmqpMessageSchemaType<T extends AmqpAwareEventDefinition> = z.infer<T['schema']>

export class AmqpQueuePublisherManager<
  T extends AbstractAmqpQueuePublisher<z.infer<SupportedEventDefinitions[number]['schema']>>,
  SupportedEventDefinitions extends AmqpAwareEventDefinition[],
  MetadataType = MessageMetadataType,
> extends AbstractPublisherManager<
  AmqpAwareEventDefinition,
  NonNullable<SupportedEventDefinitions[number]['exchange']>,
  AbstractAmqpQueuePublisher<z.infer<SupportedEventDefinitions[number]['schema']>>,
  AMQPDependencies,
  AMQPCreationConfig,
  AMQPLocator,
  AmqpMessageSchemaType<AmqpAwareEventDefinition>,
  Omit<
    AMQPPublisherOptions<z.infer<SupportedEventDefinitions[number]['schema']>>,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  >,
  SupportedEventDefinitions,
  MetadataType,
  z.infer<SupportedEventDefinitions[number]['schema']>
> {
  constructor(
    dependencies: AmqpPublisherManagerDependencies<SupportedEventDefinitions>,
    options: AmqpPublisherManagerOptions<
      T,
      AmqpQueueMessageOptions,
      AMQPPublisherOptions<z.infer<SupportedEventDefinitions[number]['schema']>>,
      z.infer<SupportedEventDefinitions[number]['schema']>,
      MetadataType
    >,
  ) {
    super({
      isAsync: false,
      eventRegistry: dependencies.eventRegistry,
      metadataField: options.metadataField ?? 'metadata',
      metadataFiller: options.metadataFiller,
      newPublisherOptions: options.newPublisherOptions,
      publisherDependencies: {
        amqpConnectionManager: dependencies.amqpConnectionManager,
        logger: dependencies.logger,
        errorReporter: dependencies.errorReporter,
      },
      publisherFactory: options.publisherFactory ?? new CommonAmqpQueuePublisherFactory(),
    })
  }

  protected override resolveCreationConfig(
    queueName: NonNullable<SupportedEventDefinitions[number]['queueName']>,
  ): AMQPCreationConfig {
    return {
      ...this.newPublisherOptions,
      queueOptions: {},
      queueName,
    }
  }

  protected override resolveEventTarget(
    event: AmqpAwareEventDefinition,
  ): NonNullable<SupportedEventDefinitions[number]['queueName']> | undefined {
    return event.queueName
  }
}
