import type { Either } from '@lokalise/node-core'
import type {
  ExistingQueueOptionsMultiSchema,
  NewQueueOptionsMultiSchema,
} from '@message-queue-toolkit/core'
import { HandlerContainer, MessageSchemaContainer } from '@message-queue-toolkit/core'
import type { ConsumerOptions } from 'sqs-consumer/src/types'
import type { ZodSchema } from 'zod'

import type { SQSMessage } from '../types/MessageTypes'

import type { SQSCreationConfig } from './AbstractSqsConsumer'
import { AbstractSqsConsumer } from './AbstractSqsConsumer'
import type { SQSConsumerDependencies, SQSQueueLocatorType } from './AbstractSqsService'

export type NewSQSConsumerOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  CreationConfigType extends SQSCreationConfig,
> = NewQueueOptionsMultiSchema<MessagePayloadSchemas, CreationConfigType, ExecutionContext> & {
  consumerOverrides?: Partial<ConsumerOptions>
}

export type ExistingSQSConsumerOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = ExistingQueueOptionsMultiSchema<MessagePayloadSchemas, QueueLocatorType, ExecutionContext> & {
  consumerOverrides?: Partial<ConsumerOptions>
}

export abstract class AbstractSqsConsumerMultiSchema<
  MessagePayloadType extends object,
  ExecutionContext,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  ConsumerOptionsType extends
    | NewSQSConsumerOptionsMultiSchema<MessagePayloadType, ExecutionContext, CreationConfigType>
    | ExistingSQSConsumerOptionsMultiSchema<
        MessagePayloadType,
        ExecutionContext,
        QueueLocatorType
      > =
    | NewSQSConsumerOptionsMultiSchema<MessagePayloadType, ExecutionContext, CreationConfigType>
    | ExistingSQSConsumerOptionsMultiSchema<MessagePayloadType, ExecutionContext, QueueLocatorType>,
> extends AbstractSqsConsumer<
  MessagePayloadType,
  QueueLocatorType,
  CreationConfigType,
  ConsumerOptionsType
> {
  messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  handlerContainer: HandlerContainer<MessagePayloadType, ExecutionContext>

  constructor(dependencies: SQSConsumerDependencies, options: ConsumerOptionsType) {
    super(dependencies, options)

    const messageSchemas = options.handlers.map((entry) => entry.schema)

    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
      messageSchemas,
      messageTypeField: options.messageTypeField,
    })
    this.handlerContainer = new HandlerContainer<MessagePayloadType, ExecutionContext>({
      messageTypeField: this.messageTypeField,
      messageHandlers: options.handlers,
    })
  }

  protected override resolveSchema(message: SQSMessage): ZodSchema<MessagePayloadType> {
    return this.messageSchemaContainer.resolveSchema(JSON.parse(message.Body))
  }

  public override async processMessage(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler(messageType)
    // @ts-ignore
    return handler(message, this)
  }
}
