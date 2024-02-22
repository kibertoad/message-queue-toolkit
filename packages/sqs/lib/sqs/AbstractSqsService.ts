import type { SQSClient, CreateQueueRequest, SendMessageCommandInput } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  NewQueueOptions,
  ExistingQueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { SQSMessage } from '../types/MessageTypes'
import { deleteSqs, initSqs } from '../utils/sqsInitter'

import type { SQSCreationConfig } from './AbstractSqsConsumer'
import type { SQSMessageOptions } from './AbstractSqsPublisherMonoSchema'

export type SQSDependencies = QueueDependencies & {
  sqsClient: SQSClient
}

export type SQSConsumerDependencies = SQSDependencies & QueueConsumerDependencies

export type SQSQueueConfig = CreateQueueRequest

export type SQSQueueLocatorType = {
  queueUrl: string
}

export abstract class AbstractSqsService<
  MessagePayloadType extends object,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  SQSOptionsType extends
    | NewQueueOptions<CreationConfigType>
    | ExistingQueueOptions<QueueLocatorType> =
    | NewQueueOptions<CreationConfigType>
    | ExistingQueueOptions<QueueLocatorType>,
  DependenciesType extends SQSDependencies = SQSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
  SQSMessage,
  DependenciesType,
  CreationConfigType,
  QueueLocatorType,
  SQSOptionsType
> {
  protected readonly sqsClient: SQSClient
  // @ts-ignore
  public queueUrl: string
  // @ts-ignore
  public queueName: string
  // @ts-ignore
  public queueArn: string

  constructor(dependencies: DependenciesType, options: SQSOptionsType) {
    super(dependencies, options)

    this.sqsClient = dependencies.sqsClient
  }

  public async init() {
    if (this.deletionConfig && this.creationConfig) {
      await deleteSqs(this.sqsClient, this.deletionConfig, this.creationConfig)
    }
    const { queueUrl, queueName, queueArn } = await initSqs(
      this.sqsClient,
      this.locatorConfig,
      this.creationConfig,
    )

    this.queueArn = queueArn
    this.queueUrl = queueUrl
    this.queueName = queueName
  }

  protected async internalPublish(
    message: MessagePayloadType,
    messageSchema: ZodSchema<MessagePayloadType>,
    options: SQSMessageOptions = {},
  ): Promise<void> {
    if (!this.queueArn) {
      // Lazy loading
      await this.init()
    }

    try {
      messageSchema.parse(message)

      if (this.logMessages) {
        // @ts-ignore
        const resolvedLogMessage = this.resolveMessageLog(message, message[this.messageTypeField])
        this.logMessage(resolvedLogMessage)
      }

      const input = {
        // SendMessageRequest
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(message),
        ...options,
      } satisfies SendMessageCommandInput
      const command = new SendMessageCommand(input)
      await this.sqsClient.send(command)
      this.handleMessageProcessed(message, 'published')
    } catch (error) {
      this.handleError(error)
      throw error
    }
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
