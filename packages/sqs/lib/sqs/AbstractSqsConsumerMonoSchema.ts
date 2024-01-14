import type { Either } from '@lokalise/node-core'
import type {
  MonoSchemaQueueOptions,
  QueueConsumer as QueueConsumer,
  BarrierResult,
  Prehandler,
  PrehandlingOutputs,
} from '@message-queue-toolkit/core'
import type { PrehandlerResult } from '@message-queue-toolkit/core/dist/lib/queues/HandlerContainer'
import type { ZodSchema } from 'zod'
import { undefined } from 'zod'

import type {
  ExistingSQSConsumerOptions,
  NewSQSConsumerOptions,
  SQSCreationConfig,
} from './AbstractSqsConsumer'
import { AbstractSqsConsumer } from './AbstractSqsConsumer'
import type { SQSConsumerDependencies, SQSQueueLocatorType } from './AbstractSqsService'

const DEFAULT_BARRIER_RESULT = {
  isPassing: true,
  output: undefined,
} as const

export type CommonSQSConsumerOptionsMono<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput,
> = {
  prehandlers?: Prehandler<MessagePayloadType, ExecutionContext, PrehandlerOutput>[]
}

export type NewSQSConsumerOptionsMono<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput,
  CreationConfigType extends SQSCreationConfig,
> = NewSQSConsumerOptions<CreationConfigType> &
  MonoSchemaQueueOptions<MessagePayloadType> &
  CommonSQSConsumerOptionsMono<MessagePayloadType, ExecutionContext, PrehandlerOutput>

export type ExistingSQSConsumerOptionsMono<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = ExistingSQSConsumerOptions<QueueLocatorType> &
  MonoSchemaQueueOptions<MessagePayloadType> &
  CommonSQSConsumerOptionsMono<MessagePayloadType, ExecutionContext, PrehandlerOutput>

export abstract class AbstractSqsConsumerMonoSchema<
    MessagePayloadType extends object,
    PrehandlerOutput = undefined,
    ExecutionContext = undefined,
    BarrierOutput = undefined,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    ConsumerOptionsType extends
      | NewSQSConsumerOptionsMono<
          MessagePayloadType,
          ExecutionContext,
          PrehandlerOutput,
          CreationConfigType
        >
      | ExistingSQSConsumerOptionsMono<
          MessagePayloadType,
          ExecutionContext,
          PrehandlerOutput,
          QueueLocatorType
        > =
      | NewSQSConsumerOptionsMono<
          MessagePayloadType,
          ExecutionContext,
          PrehandlerOutput,
          CreationConfigType
        >
      | ExistingSQSConsumerOptionsMono<
          MessagePayloadType,
          ExecutionContext,
          PrehandlerOutput,
          QueueLocatorType
        >,
  >
  extends AbstractSqsConsumer<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType,
    PrehandlerOutput,
    BarrierOutput
  >
  implements QueueConsumer
{
  private readonly messageSchema: ZodSchema<MessagePayloadType>
  private readonly schemaEither: Either<Error, ZodSchema<MessagePayloadType>>
  private readonly prehandlers?: Prehandler<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput
  >[]

  protected constructor(dependencies: SQSConsumerDependencies, options: ConsumerOptionsType) {
    super(dependencies, options)

    this.prehandlers = options.prehandlers
    this.messageSchema = options.messageSchema
    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  /**
   * Override to implement barrier pattern
   */
  /* c8 ignore start */
  protected override preHandlerBarrier(_message: MessagePayloadType, _messageType: string) {
    // @ts-ignore
    return Promise.resolve(DEFAULT_BARRIER_RESULT as BarrierResult<BarrierOutput>)
  }
  /* c8 ignore end */

  abstract override processMessage(
    message: MessagePayloadType,
    messageType: string,
    prehandlingOutputs: PrehandlingOutputs<PrehandlerOutput, BarrierOutput>,
  ): Promise<Either<'retryLater', 'success'>>

  protected override processPrehandlers(message: MessagePayloadType) {
    if (!this.prehandlers || this.prehandlers.length === 0) {
      return Promise.resolve({} as PrehandlerOutput)
    }

    return new Promise<PrehandlerOutput>((resolve, reject) => {
      try {
        const prehandlerOutput = {} as PrehandlerOutput
        const next = this.resolveNextFunction(
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          this.prehandlers!,
          message,
          0,
          prehandlerOutput,
          resolve,
          reject,
        )
        next({
          result: 'success',
        })
      } catch (err) {
        reject(err as Error)
      }
    })
  }

  // eslint-disable-next-line max-params
  private resolveNextFunction(
    prehandlers: Prehandler<MessagePayloadType, ExecutionContext, PrehandlerOutput>[],
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
          this as unknown as ExecutionContext,
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

  protected resolveSchema() {
    return this.schemaEither
  }
}
