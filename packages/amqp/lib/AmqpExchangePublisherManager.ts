import { AbstractPublisherManager } from '@message-queue-toolkit/core'
import type { MessageMetadataType } from '@message-queue-toolkit/core/lib/messages/baseMessageSchemas'
import type { TypeOf } from 'zod'
import type z from 'zod'

import type {
  AbstractAmqpExchangePublisher,
  AMQPExchangePublisherOptions,
} from './AbstractAmqpExchangePublisher'
import type { AmqpQueueMessageOptions } from './AbstractAmqpQueuePublisher'
import type { AMQPCreationConfig, AMQPDependencies, AMQPLocator } from './AbstractAmqpService'
import type {
  AmqpAwareEventDefinition,
  AmqpMessageSchemaType,
  AmqpPublisherManagerDependencies,
  AmqpPublisherManagerOptions,
} from './AmqpQueuePublisherManager'
import { CommonAmqpExchangePublisherFactory } from './CommonAmqpPublisherFactory'

export class AmqpExchangePublisherManager<
  T extends AbstractAmqpExchangePublisher<
    z.infer<SupportedEventDefinitions[number]['publisherSchema']>
  >,
  SupportedEventDefinitions extends AmqpAwareEventDefinition[],
  MetadataType = MessageMetadataType,
> extends AbstractPublisherManager<
  AmqpAwareEventDefinition,
  NonNullable<SupportedEventDefinitions[number]['exchange']>,
  AbstractAmqpExchangePublisher<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
  AMQPDependencies,
  AMQPCreationConfig,
  AMQPLocator,
  AmqpMessageSchemaType<AmqpAwareEventDefinition>,
  Omit<
    AMQPExchangePublisherOptions<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
    'messageSchemas' | 'locatorConfig' | 'exchange'
  >,
  SupportedEventDefinitions,
  MetadataType,
  z.infer<SupportedEventDefinitions[number]['publisherSchema']>
> {
  constructor(
    dependencies: AmqpPublisherManagerDependencies<SupportedEventDefinitions>,
    options: AmqpPublisherManagerOptions<
      T,
      AmqpQueueMessageOptions,
      AMQPExchangePublisherOptions<z.infer<SupportedEventDefinitions[number]['publisherSchema']>>,
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
      publisherFactory: options.publisherFactory ?? new CommonAmqpExchangePublisherFactory(),
    })
  }

  protected resolvePublisherConfigOverrides(
    exchange: string,
  ): Partial<
    Omit<
      AMQPExchangePublisherOptions<TypeOf<SupportedEventDefinitions[number]['publisherSchema']>>,
      'messageSchemas' | 'locatorConfig'
    >
  > {
    return {
      exchange,
    }
  }

  protected override resolveCreationConfig(
    queueName: NonNullable<SupportedEventDefinitions[number]['exchange']>,
  ): AMQPCreationConfig {
    return {
      ...this.newPublisherOptions,
      queueOptions: {},
      queueName,
    }
  }

  protected override resolveEventTarget(
    event: AmqpAwareEventDefinition,
  ): NonNullable<SupportedEventDefinitions[number]['exchange']> | undefined {
    return event.queueName
  }
}
