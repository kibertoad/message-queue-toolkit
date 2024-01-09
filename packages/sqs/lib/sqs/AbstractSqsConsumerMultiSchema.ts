import type { Either } from '@lokalise/node-core'
import { HandlerContainer, MessageSchemaContainer } from '@message-queue-toolkit/core'
import type {
  ExistingQueueOptionsMultiSchema,
  NewQueueOptionsMultiSchema,
  BarrierResult,
  Prehandler,
  PrehandlingOutputs,
} from '@message-queue-toolkit/core'
import type { PrehandlerResult } from '@message-queue-toolkit/core/dist/lib/queues/HandlerContainer'
import type { ConsumerOptions } from 'sqs-consumer/src/types'

import type { SQSCreationConfig } from './AbstractSqsConsumer'
import { AbstractSqsConsumer } from './AbstractSqsConsumer'
import type { SQSConsumerDependencies, SQSQueueLocatorType } from './AbstractSqsService'

export type NewSQSConsumerOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
  CreationConfigType extends SQSCreationConfig,
> = NewQueueOptionsMultiSchema<
  MessagePayloadSchemas,
  CreationConfigType,
  ExecutionContext,
  PrehandlerOutput
> & {
  consumerOverrides?: Partial<ConsumerOptions>
}

export type ExistingSQSConsumerOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = ExistingQueueOptionsMultiSchema<
  MessagePayloadSchemas,
  QueueLocatorType,
  ExecutionContext,
  PrehandlerOutput
> & {
  consumerOverrides?: Partial<ConsumerOptions>
}

export abstract class AbstractSqsConsumerMultiSchema<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  ConsumerOptionsType extends NewSQSConsumerOptionsMultiSchema<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput,
    CreationConfigType
  > = NewSQSConsumerOptionsMultiSchema<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput,
    CreationConfigType
  >,
> extends AbstractSqsConsumer<
  MessagePayloadType,
  QueueLocatorType,
  CreationConfigType,
  ConsumerOptionsType,
  PrehandlerOutput
> {
  messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  handlerContainer: HandlerContainer<MessagePayloadType, ExecutionContext, PrehandlerOutput>
  protected readonly executionContext: ExecutionContext

  constructor(
    dependencies: SQSConsumerDependencies,
    options: ConsumerOptionsType,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)

    const messageSchemas = options.handlers.map((entry) => entry.schema)

    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
      messageSchemas,
      messageTypeField: options.messageTypeField,
    })
    this.handlerContainer = new HandlerContainer<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput
    >({
      messageTypeField: this.messageTypeField,
      messageHandlers: options.handlers,
    })
    this.executionContext = executionContext
  }

  protected override resolveSchema(message: MessagePayloadType) {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  public override async processMessage(
    message: MessagePayloadType,
    messageType: string,
    prehandlingOutputs: PrehandlingOutputs<PrehandlerOutput, unknown>,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler(messageType)

    return handler.handler(message, this.executionContext, prehandlingOutputs)
  }

  protected override processPrehandlers(message: MessagePayloadType, messageType: string) {
    const handler = this.handlerContainer.resolveHandler(messageType)

    if (!handler.prehandlers || handler.prehandlers.length === 0) {
      return Promise.resolve({} as PrehandlerOutput)
    }

    return new Promise<PrehandlerOutput>((resolve, reject) => {
      try {
        const prehandlerOutput = {} as PrehandlerOutput
        const next = this.resolveNextFunction(
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          handler.prehandlers!,
          message,
          0,
          prehandlerOutput,
          resolve,
          reject,
        )
        next({ result: 'success' })
      } catch (err) {
        reject(err as Error)
      }
    })
  }

  // eslint-disable-next-line max-params
  private resolveNextFunction(
    prehandlers: Prehandler<MessagePayloadType, ExecutionContext, unknown>[],
    message: MessagePayloadType,
    index: number,
    prehandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ) {
    return (prehandlerResult: PrehandlerResult) => {
      if (prehandlerResult.error) {
        reject(prehandlerResult.error)
      }

      if (prehandlers.length < index + 1) {
        resolve(prehandlerOutput)
      } else {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-call
        prehandlers[index](
          message,
          this.executionContext,
          // @ts-ignore
          prehandlerOutput,
          this.resolveNextFunction(
            prehandlers,
            message,
            index + 1,
            prehandlerOutput,
            resolve,
            reject,
          ),
        )
      }
    }
  }

  protected override resolveMessageLog(message: MessagePayloadType, messageType: string): unknown {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.messageLogFormatter(message)
  }

  protected override async preHandlerBarrier<BarrierOutput>(
    message: MessagePayloadType,
    messageType: string,
    prehandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>> {
    const handler = this.handlerContainer.resolveHandler<BarrierOutput>(messageType)
    // @ts-ignore
    return handler.preHandlerBarrier
      ? // @ts-ignore
        await handler.preHandlerBarrier(message, this.executionContext, prehandlerOutput)
      : {
          isPassing: true,
          output: undefined,
        }
  }
}
