import type {
  CommonCreationConfigType,
  EventRegistry,
  MessageMetadataType,
  MessagePublishType,
  MessageSchemaType,
  MetadataFiller,
  PublisherBaseEventType,
} from '@message-queue-toolkit/core'
import { AbstractPublisherManager } from '@message-queue-toolkit/core'
import type { AmqpAwareEventDefinition } from '@message-queue-toolkit/schemas'
import type z from 'zod'

import type { AMQPPublisherOptions, AbstractAmqpPublisher } from './AbstractAmqpPublisher'
import type {
  AbstractAmqpQueuePublisher,
  AmqpQueueMessageOptions,
} from './AbstractAmqpQueuePublisher'
import type {
  AMQPDependencies,
  AMQPQueueCreationConfig,
  AMQPQueueLocator,
} from './AbstractAmqpService'
import type { AmqpPublisherFactory } from './CommonAmqpPublisherFactory'
import { CommonAmqpQueuePublisherFactory } from './CommonAmqpPublisherFactory'

export type { AmqpAwareEventDefinition }

export type AmqpPublisherManagerDependencies<SupportedEvents extends AmqpAwareEventDefinition[]> = {
  eventRegistry: EventRegistry<SupportedEvents>
} & AMQPDependencies

export type AmqpPublisherManagerOptions<
  PublisherType extends AbstractAmqpPublisher<
    EventType,
    MessageOptionsType,
    CreationConfig,
    LocatorConfig
  >,
  MessageOptionsType,
  PublisherOptionsType extends Omit<
    AMQPPublisherOptions<EventType, CreationConfig, LocatorConfig>,
    'creationConfig'
  >,
  EventType extends PublisherBaseEventType,
  MetadataType,
  CreationConfig extends CommonCreationConfigType = AMQPQueueCreationConfig,
  LocatorConfig extends object = AMQPQueueLocator,
> = {
  metadataField?: string
  publisherFactory: AmqpPublisherFactory<
    PublisherType,
    EventType,
    MessageOptionsType,
    PublisherOptionsType
  >
  metadataFiller: MetadataFiller<EventType, MetadataType>
  newPublisherOptions: Omit<
    AMQPPublisherOptions<EventType, CreationConfig, LocatorConfig>,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  > & {
    creationConfig?: Omit<CreationConfig, 'queueName'>
  }
}

export type AmqpMessageSchemaType<T extends AmqpAwareEventDefinition> = z.infer<
  T['publisherSchema']
>

export class AmqpQueuePublisherManager<
  T extends AbstractAmqpQueuePublisher<
    z.infer<SupportedEventDefinitions[number]['publisherSchema']>
  >,
  SupportedEventDefinitions extends AmqpAwareEventDefinition[],
  MetadataType = MessageMetadataType,
> extends AbstractPublisherManager<
  AmqpAwareEventDefinition,
  NonNullable<SupportedEventDefinitions[number]['queueName']>,
  AbstractAmqpQueuePublisher<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
  AMQPDependencies,
  AMQPQueueCreationConfig,
  AMQPQueueLocator,
  AmqpMessageSchemaType<AmqpAwareEventDefinition>,
  Omit<
    AMQPPublisherOptions<
      z.infer<SupportedEventDefinitions[number]['publisherSchema']>,
      AMQPQueueCreationConfig,
      AMQPQueueLocator
    >,
    'messageSchemas' | 'creationConfig' | 'locatorConfig'
  >,
  SupportedEventDefinitions,
  MetadataType,
  AmqpQueueMessageOptions
> {
  constructor(
    dependencies: AmqpPublisherManagerDependencies<SupportedEventDefinitions>,
    options: AmqpPublisherManagerOptions<
      T,
      AmqpQueueMessageOptions,
      AMQPPublisherOptions<
        z.infer<SupportedEventDefinitions[number]['publisherSchema']>,
        AMQPQueueCreationConfig,
        AMQPQueueLocator
      >,
      z.infer<SupportedEventDefinitions[number]['publisherSchema']>,
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
  ): AMQPQueueCreationConfig {
    return {
      ...this.newPublisherOptions,
      queueOptions: {},
      queueName,
    }
  }

  /**
   * @deprecated use `publishSync` instead.
   */
  publish(): Promise<MessageSchemaType<SupportedEventDefinitions[number]>> {
    throw new Error('Please use `publishSync` method for AMQP publisher managers')
  }

  publishSync(
    queue: NonNullable<SupportedEventDefinitions[number]['queueName']>,
    message: MessagePublishType<SupportedEventDefinitions[number]>,
    precedingEventMetadata?: Partial<MetadataType>,
    messageOptions?: AmqpQueueMessageOptions,
  ): MessageSchemaType<SupportedEventDefinitions[number]> {
    const publisher = this.targetToPublisherMap[queue]
    if (!publisher) {
      throw new Error(`No publisher for queue ${queue}`)
    }

    const messageDefinition = this.resolveMessageDefinition(queue, message)
    const resolvedMessage = this.resolveMessage(messageDefinition, message, precedingEventMetadata)
    publisher.publish(resolvedMessage, messageOptions)
    return resolvedMessage
  }

  protected override resolveEventTarget(
    event: AmqpAwareEventDefinition,
  ): NonNullable<SupportedEventDefinitions[number]['queueName']> | undefined {
    return event.queueName
  }
}
