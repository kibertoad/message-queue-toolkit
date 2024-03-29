import type { SNSClient } from '@aws-sdk/client-sns'
import type { Either } from '@lokalise/node-core'
import type { MonoSchemaQueueOptions, BarrierResult, Prehandler } from '@message-queue-toolkit/core'
import type {
  SQSConsumerDependencies,
  NewSQSConsumerOptions,
  ExistingSQSConsumerOptions,
  SQSQueueLocatorType,
  SQSCreationConfig,
  SQSMessage,
  CommonSQSConsumerOptionsMono,
} from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer, deleteSqs } from '@message-queue-toolkit/sqs'
import type { ZodSchema } from 'zod'

import { deleteSnsSqs, initSnsSqs } from '../utils/snsInitter'
import { readSnsMessage } from '../utils/snsMessageReader'
import type { SNSSubscriptionOptions } from '../utils/snsSubscriber'

import type {
  ExistingSNSOptions,
  NewSNSOptions,
  SNSCreationConfig,
  SNSQueueLocatorType,
} from './AbstractSnsService'

export type NewSnsSqsConsumerOptions = NewSQSConsumerOptions<
  SQSCreationConfig & SNSCreationConfig
> &
  NewSNSOptions & {
    subscriptionConfig?: SNSSubscriptionOptions
  }

export type NewSnsSqsConsumerOptionsMono<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> = NewSnsSqsConsumerOptions &
  MonoSchemaQueueOptions<MessagePayloadType> &
  CommonSQSConsumerOptionsMono<MessagePayloadType, ExecutionContext, PrehandlerOutput>

export type ExistingSnsSqsConsumerOptions = ExistingSQSConsumerOptions<SNSSQSQueueLocatorType> &
  ExistingSNSOptions & {
    subscriptionConfig?: SNSSubscriptionOptions
  }

export type ExistingSnsSqsConsumerOptionsMono<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput,
> = ExistingSnsSqsConsumerOptions &
  MonoSchemaQueueOptions<MessagePayloadType> &
  CommonSQSConsumerOptionsMono<MessagePayloadType, ExecutionContext, PrehandlerOutput>

export type SNSSQSConsumerDependencies = SQSConsumerDependencies & {
  snsClient: SNSClient
}

export type SNSSQSQueueLocatorType = SQSQueueLocatorType &
  SNSQueueLocatorType & {
    subscriptionArn?: string
  }

const DEFAULT_BARRIER_RESULT = {
  isPassing: true,
  output: undefined,
} as const

export abstract class AbstractSnsSqsConsumerMonoSchema<
  MessagePayloadType extends object,
  ExecutionContext = undefined,
  PrehandlerOutput = undefined,
  BarrierOutput = undefined,
> extends AbstractSqsConsumer<
  MessagePayloadType,
  SNSSQSQueueLocatorType,
  SNSCreationConfig & SQSCreationConfig,
  | NewSnsSqsConsumerOptions
  | ExistingSnsSqsConsumerOptionsMono<MessagePayloadType, ExecutionContext, PrehandlerOutput>,
  ExecutionContext,
  PrehandlerOutput,
  BarrierOutput
> {
  private readonly subscriptionConfig?: SNSSubscriptionOptions
  private readonly snsClient: SNSClient
  private readonly messageSchema: ZodSchema<MessagePayloadType>
  private readonly schemaEither: Either<Error, ZodSchema<MessagePayloadType>>
  private readonly prehandlers: Prehandler<MessagePayloadType, ExecutionContext, PrehandlerOutput>[]

  // @ts-ignore
  public topicArn: string
  // @ts-ignore
  public subscriptionArn: string

  protected constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: NewSnsSqsConsumerOptionsMono<MessagePayloadType, ExecutionContext, PrehandlerOutput>,
  ) {
    super(dependencies, {
      ...options,
    })

    this.subscriptionConfig = options.subscriptionConfig
    this.snsClient = dependencies.snsClient
    this.prehandlers = options.prehandlers ?? []
    this.messageSchema = options.messageSchema
    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  protected override resolveSchema() {
    return this.schemaEither
  }

  protected override resolveMessage(message: SQSMessage) {
    return readSnsMessage(message, this.errorResolver)
  }

  /**
   * Override to implement barrier pattern
   */
  protected preHandlerBarrier(
    _message: MessagePayloadType,
    _messageType: string,
  ): Promise<BarrierResult<BarrierOutput>> {
    // @ts-ignore
    return Promise.resolve(DEFAULT_BARRIER_RESULT)
  }

  protected override processPrehandlers(message: MessagePayloadType) {
    return this.processPrehandlersInternal(this.prehandlers, message)
  }

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

  override async init(): Promise<void> {
    if (this.deletionConfig && this.creationConfig && this.subscriptionConfig) {
      await deleteSnsSqs(
        this.sqsClient,
        this.snsClient,
        this.deletionConfig,
        this.creationConfig.queue,
        this.creationConfig.topic,
        this.subscriptionConfig,
        {
          logger: this.logger,
        },
      )
    } else if (this.deletionConfig && this.creationConfig) {
      await deleteSqs(this.sqsClient, this.deletionConfig, this.creationConfig)
    }

    const initSnsSqsResult = await initSnsSqs(
      this.sqsClient,
      this.snsClient,
      this.locatorConfig,
      this.creationConfig,
      this.subscriptionConfig,
      {
        logger: this.logger,
      },
    )
    this.queueUrl = initSnsSqsResult.queueUrl
    this.topicArn = initSnsSqsResult.topicArn
    this.subscriptionArn = initSnsSqsResult.subscriptionArn
  }
}
