import type { SNSClient, CreateTopicCommandInput, Tag } from '@aws-sdk/client-sns'
import { PublishCommand } from '@aws-sdk/client-sns'
import type { PublishCommandInput } from '@aws-sdk/client-sns/dist-types/commands/PublishCommand'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  NewQueueOptions,
  ExistingQueueOptions,
  NewQueueOptionsMultiSchema,
  ExistingQueueOptionsMultiSchema,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { SNS_MESSAGE_BODY_TYPE } from '../types/MessageTypes'
import { deleteSns, initSns } from '../utils/snsInitter'

import type { SNSMessageOptions } from './AbstractSnsPublisherMonoSchema'

export type SNSDependencies = QueueDependencies & {
  snsClient: SNSClient
}

export type SNSQueueLocatorType = {
  topicArn: string
}

export type SNSConsumerDependencies = SNSDependencies & QueueConsumerDependencies

export type SNSTopicAWSConfig = CreateTopicCommandInput
export type SNSTopicConfig = {
  tags?: Tag[]
  DataProtectionPolicy?: string
  // ToDo if correct for sns
  Attributes?: {
    DeliveryPolicy?: string
    DisplayName?: string
    Policy?: string
    SignatureVersion?: number
    TracingConfig?: string
    FifoTopic?: boolean
    ContentBasedDeduplication?: boolean
  }
}

export type NewSNSOptions = NewQueueOptions<SNSCreationConfig>

export type ExistingSNSOptions = ExistingQueueOptions<SNSQueueLocatorType>

export type NewSNSOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
> = NewQueueOptionsMultiSchema<
  MessagePayloadSchemas,
  SNSCreationConfig,
  ExecutionContext,
  PrehandlerOutput
>

export type ExistingSNSOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
> = ExistingQueueOptionsMultiSchema<
  MessagePayloadSchemas,
  SNSQueueLocatorType,
  ExecutionContext,
  PrehandlerOutput
>

export type ExtraSNSCreationParams = {
  queueUrlsWithSubscribePermissionsPrefix?: string
  sourceOwner?: string
}

export type SNSCreationConfig = {
  topic: SNSTopicAWSConfig
  updateAttributesIfExists?: boolean
} & ExtraSNSCreationParams

export abstract class AbstractSnsService<
  MessagePayloadType extends object,
  MessageEnvelopeType extends object = SNS_MESSAGE_BODY_TYPE,
  SNSOptionsType extends
    | ExistingQueueOptions<SNSQueueLocatorType>
    | NewQueueOptions<SNSCreationConfig> = ExistingSNSOptions | NewQueueOptions<SNSCreationConfig>,
  DependenciesType extends SNSDependencies = SNSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
  MessageEnvelopeType,
  DependenciesType,
  SNSCreationConfig,
  SNSQueueLocatorType,
  SNSOptionsType
> {
  protected readonly snsClient: SNSClient
  // @ts-ignore
  public topicArn: string

  private isInitted: boolean
  private initPromise?: Promise<void>

  constructor(dependencies: DependenciesType, options: SNSOptionsType) {
    super(dependencies, options)

    this.isInitted = false
    this.snsClient = dependencies.snsClient
  }

  public async init() {
    if (this.deletionConfig && this.creationConfig) {
      await deleteSns(this.snsClient, this.deletionConfig, this.creationConfig)
    }

    const initResult = await initSns(this.snsClient, this.locatorConfig, this.creationConfig)
    this.topicArn = initResult.topicArn
    this.isInitted = true
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}

  protected async internalPublish(
    message: MessagePayloadType,
    messageSchema: ZodSchema<MessagePayloadType>,
    options: SNSMessageOptions = {},
  ): Promise<void> {
    // If it's not initted yet, do the lazy init
    if (!this.isInitted) {
      // avoid multiple concurrent inits
      if (!this.initPromise) {
        this.initPromise = this.init()
      }
      await this.initPromise
    }

    try {
      messageSchema.parse(message)

      if (this.logMessages) {
        // @ts-ignore
        const resolvedLogMessage = this.resolveMessageLog(message, message[this.messageTypeField])
        this.logMessage(resolvedLogMessage)
      }

      const input = {
        Message: JSON.stringify(message),
        TopicArn: this.topicArn,
        ...options,
      } satisfies PublishCommandInput
      const command = new PublishCommand(input)
      await this.snsClient.send(command)
      this.handleMessageProcessed(message, 'published')
    } catch (error) {
      this.handleError(error)
      throw error
    }
  }
}
