import { AbstractPublisherManager } from '@message-queue-toolkit/core'
import type {
  MessagePublishType,
  MessageSchemaType,
  MessageMetadataType,
} from '@message-queue-toolkit/core'
import type z from 'zod'

import type { AMQPDependencies, AMQPTopicPublisherConfig } from './AbstractAmqpService'
import type {
  AbstractAmqpTopicPublisher,
  AmqpTopicMessageOptions,
  AMQPTopicPublisherOptions,
} from './AbstractAmqpTopicPublisher'
import type {
  AmqpAwareEventDefinition,
  AmqpMessageSchemaType,
  AmqpPublisherManagerDependencies,
  AmqpPublisherManagerOptions,
} from './AmqpQueuePublisherManager'
import { CommonAmqpTopicPublisherFactory } from './CommonAmqpPublisherFactory'

export class AmqpTopicPublisherManager<
  PublisherType extends AbstractAmqpTopicPublisher<
    z.infer<SupportedEventDefinitions[number]['publisherSchema']>
  >,
  SupportedEventDefinitions extends AmqpAwareEventDefinition[],
  MetadataType = MessageMetadataType,
> extends AbstractPublisherManager<
  AmqpAwareEventDefinition,
  NonNullable<SupportedEventDefinitions[number]['exchange']>,
  AbstractAmqpTopicPublisher<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
  AMQPDependencies,
  AMQPTopicPublisherConfig,
  AMQPTopicPublisherConfig,
  AmqpMessageSchemaType<AmqpAwareEventDefinition>,
  Omit<
    AMQPTopicPublisherOptions<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
    'messageSchemas' | 'locatorConfig' | 'exchange'
  >,
  SupportedEventDefinitions,
  MetadataType,
  AmqpTopicMessageOptions
> {
  constructor(
    dependencies: AmqpPublisherManagerDependencies<SupportedEventDefinitions>,
    options: AmqpPublisherManagerOptions<
      PublisherType,
      AmqpTopicMessageOptions,
      AMQPTopicPublisherOptions<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
      z.infer<SupportedEventDefinitions[number]['publisherSchema']>,
      MetadataType,
      AMQPTopicPublisherConfig,
      AMQPTopicPublisherConfig
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
      publisherFactory: options.publisherFactory ?? new CommonAmqpTopicPublisherFactory(),
    })
  }

  protected resolvePublisherConfigOverrides(
    exchange: string,
  ): Partial<
    Omit<
      AMQPTopicPublisherOptions<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
      'messageSchemas' | 'locatorConfig'
    >
  > {
    return {
      exchange,
    }
  }

  protected override resolveCreationConfig(
    exchange: NonNullable<SupportedEventDefinitions[number]['exchange']>,
  ): AMQPTopicPublisherConfig {
    return {
      ...this.newPublisherOptions,
      exchange,
      updateAttributesIfExists: false,
    }
  }

  /**
   * @deprecated use `publishSync` instead.
   */
  publish(): Promise<MessageSchemaType<SupportedEventDefinitions[number]>> {
    throw new Error('Please use `publishSync` method for AMQP publisher managers')
  }

  publishSync(
    exchange: NonNullable<SupportedEventDefinitions[number]['exchange']>,
    message: MessagePublishType<SupportedEventDefinitions[number]>,
    messageOptions?: AmqpTopicMessageOptions,
    precedingEventMetadata?: Partial<MetadataType>,
  ): MessageSchemaType<SupportedEventDefinitions[number]> {
    const publisher = this.targetToPublisherMap[exchange]
    if (!publisher) {
      throw new Error(`No publisher for exchange ${exchange}`)
    }

    const messageDefinition = this.resolveMessageDefinition(exchange, message)
    const resolvedMessage = this.resolveMessage(messageDefinition, message, precedingEventMetadata)
    publisher.publish(resolvedMessage, {
      routingKey: messageOptions?.routingKey ?? messageDefinition?.topic ?? '',
      publishOptions: messageOptions?.publishOptions,
    })
    return resolvedMessage
  }

  protected override resolveEventTarget(
    event: AmqpAwareEventDefinition,
  ): NonNullable<SupportedEventDefinitions[number]['exchange']> | undefined {
    return event.exchange
  }
}
