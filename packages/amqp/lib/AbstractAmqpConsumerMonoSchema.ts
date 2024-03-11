import type { Either } from '@lokalise/node-core'
import type {
  QueueConsumer,
  MonoSchemaQueueOptions,
  BarrierResult,
  Prehandler,
} from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type {
  ExistingAMQPConsumerOptions,
  NewAMQPConsumerOptions,
} from './AbstractAmqpBaseConsumer'
import { AbstractAmqpBaseConsumer } from './AbstractAmqpBaseConsumer'
import type { AMQPConsumerDependencies } from './AbstractAmqpService'

const DEFAULT_BARRIER_RESULT = {
  isPassing: true,
  output: undefined,
} as const

export abstract class AbstractAmqpConsumerMonoSchema<
    MessagePayloadType extends object,
    ExecutionContext = undefined,
    PrehandlerOutput = undefined,
    BarrierOutput = undefined,
  >
  extends AbstractAmqpBaseConsumer<
    MessagePayloadType,
    ExecutionContext,
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

  constructor(
    dependencies: AMQPConsumerDependencies,
    options:
      | (NewAMQPConsumerOptions<MessagePayloadType, ExecutionContext, PrehandlerOutput> &
          MonoSchemaQueueOptions<MessagePayloadType>)
      | (ExistingAMQPConsumerOptions<MessagePayloadType, ExecutionContext, PrehandlerOutput> &
          MonoSchemaQueueOptions<MessagePayloadType>),
  ) {
    super(dependencies, options)

    this.prehandlers = options.prehandlers
    this.messageSchema = options.messageSchema
    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  protected override processPrehandlers(message: MessagePayloadType, _messageType: string) {
    return this.processPrehandlersInternal(this.prehandlers, message)
  }

  // MonoSchema support is going away in the next semver major, so we don't care about coverage strongly
  /* c8 ignore start */
  // eslint-disable-next-line max-params
  protected override resolveNextFunction(
    prehandlers: Prehandler<MessagePayloadType, ExecutionContext, PrehandlerOutput>[],
    message: MessagePayloadType,
    index: number,
    prehandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ) {
    return this.resolveNextPreHandlerFunctionInternal(
      prehandlers,
      this as unknown as ExecutionContext,
      message,
      index,
      prehandlerOutput,
      resolve,
      reject,
    )
  }
  /* c8 ignore stop */

  protected override resolveSchema(_message: MessagePayloadType) {
    return this.schemaEither
  }

  /**
   * Override to implement barrier pattern
   */
  protected preHandlerBarrier(_message: MessagePayloadType): Promise<BarrierResult<BarrierOutput>> {
    // @ts-ignore
    return Promise.resolve(DEFAULT_BARRIER_RESULT)
  }
}
