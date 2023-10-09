import type { Either } from '@lokalise/node-core'
import { HandlerContainer, MessageSchemaContainer } from '@message-queue-toolkit/core'
import type {
  ExistingQueueOptionsMultiSchema,
  NewQueueOptionsMultiSchema,
  BarrierResult,
} from '@message-queue-toolkit/core'
import type { ConsumerOptions } from 'sqs-consumer/src/types'

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

  protected override resolveSchema(message: MessagePayloadType) {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  public override async processMessage(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler(messageType)
    // @ts-ignore
    return handler.handler(message, this)
  }

  protected override resolveMessageLog(message: MessagePayloadType, messageType: string): unknown {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.messageLogFormatter(message)
  }

  protected override async preHandlerBarrier<BarrierOutput>(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<BarrierResult<BarrierOutput>> {
    const handler = this.handlerContainer.resolveHandler<BarrierOutput>(messageType)
    // @ts-ignore
    return handler.preHandlerBarrier
      ? // @ts-ignore
        await handler.preHandlerBarrier(message, this)
      : {
          isPassing: true,
          output: undefined,
        }
  }
}
