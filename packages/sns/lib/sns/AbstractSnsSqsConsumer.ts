import type { SNSClient } from '@aws-sdk/client-sns'
import type { STSClient } from '@aws-sdk/client-sts'
import type {
  SQSConsumerDependencies,
  SQSConsumerOptions,
  SQSCreationConfig,
  SQSMessage,
  SQSQueueLocatorType,
} from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer, deleteSqs } from '@message-queue-toolkit/sqs'
import { deleteSnsSqs, initSnsSqs } from '../utils/snsInitter.ts'
import { readSnsMessage } from '../utils/snsMessageReader.ts'
import type { SNSSubscriptionOptions } from '../utils/snsSubscriber.ts'
import type { SNSCreationConfig, SNSOptions, SNSTopicLocatorType } from './AbstractSnsService.ts'

export type SNSSQSConsumerDependencies = SQSConsumerDependencies & {
  snsClient: SNSClient
  stsClient: STSClient
}
export type SNSSQSCreationConfig = Omit<SQSCreationConfig, 'policyConfig'> & SNSCreationConfig

export type SNSSQSQueueLocatorType = Partial<SQSQueueLocatorType> &
  SNSTopicLocatorType & {
    subscriptionArn?: string
  }

export type SNSSQSConsumerOptions<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput,
> = SQSConsumerOptions<
  MessagePayloadType,
  ExecutionContext,
  PrehandlerOutput,
  SNSSQSCreationConfig,
  SNSSQSQueueLocatorType
> &
  SNSOptions & {
    subscriptionConfig?: SNSSubscriptionOptions
  }

export abstract class AbstractSnsSqsConsumer<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> extends AbstractSqsConsumer<
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput,
  SNSSQSCreationConfig,
  SNSSQSQueueLocatorType,
  SNSSQSConsumerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>
> {
  private readonly subscriptionConfig?: SNSSubscriptionOptions
  private readonly snsClient: SNSClient
  private readonly stsClient: STSClient

  /**
   * Tracks whether resources (SNS topic, SQS queue, subscription) are ready.
   * In non-blocking polling mode, this may be false initially and become true
   * when the onResourcesReady callback fires.
   */
  private resourcesReady: boolean = false

  /**
   * Tracks whether start() has been called but consumers couldn't be started
   * because resources weren't ready yet. When resources become ready and this
   * is true, consumers will be started automatically.
   */
  private startRequested: boolean = false

  // @ts-expect-error
  protected topicArn: string
  // @ts-expect-error
  protected subscriptionArn: string

  protected constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: SNSSQSConsumerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>,
    executionContext: ExecutionContext,
  ) {
    super(
      dependencies,
      {
        ...options,
      },
      executionContext,
    )

    this.subscriptionConfig = options.subscriptionConfig
    this.snsClient = dependencies.snsClient
    this.stsClient = dependencies.stsClient
  }

  override async init(): Promise<void> {
    if (this.deletionConfig && this.creationConfig && this.subscriptionConfig) {
      await deleteSnsSqs(
        this.sqsClient,
        this.snsClient,
        this.stsClient,
        this.deletionConfig,
        this.creationConfig.queue,
        this.creationConfig.topic,
        this.subscriptionConfig,
        undefined,
        this.locatorConfig,
      )
    } else if (this.deletionConfig && this.creationConfig) {
      await deleteSqs(this.sqsClient, this.deletionConfig, this.creationConfig)
    }

    const initSnsSqsResult = await initSnsSqs(
      this.sqsClient,
      this.snsClient,
      this.stsClient,
      this.locatorConfig,
      this.creationConfig,
      this.subscriptionConfig,
      {
        logger: this.logger,
        // This callback is only invoked in non-blocking mode when resources were NOT
        // immediately available. It will NOT be called if resourcesReady is true.
        onResourcesReady: (result) => {
          // Update values that were empty when resourcesReady was false
          this.topicArn = result.topicArn
          this.queueUrl = result.queueUrl
          this.subscriptionArn = result.subscriptionArn
          this.queueName = result.queueName
          this.resourcesReady = true

          // Initialize DLQ now that resources are ready (this is mutually exclusive
          // with the synchronous initDeadLetterQueue call below)
          this.initDeadLetterQueue()
            .then(() => {
              // If start() was called while resources weren't ready, start consumers now
              if (this.startRequested) {
                this.logger.info({
                  message: 'Resources now ready, starting consumers',
                  queueName: this.queueName,
                  topicArn: this.topicArn,
                })
                return this.startConsumers()
              }
            })
            .catch((err) => {
              this.logger.error({
                message: 'Failed to initialize dead letter queue or start consumers after resources became ready',
                error: err,
              })
            })
        },
      },
    )

    // Always assign topicArn and queueName (always valid in both blocking and non-blocking modes)
    this.topicArn = initSnsSqsResult.topicArn
    this.queueName = initSnsSqsResult.queueName
    this.resourcesReady = initSnsSqsResult.resourcesReady

    // Only assign queueUrl and subscriptionArn if resources are ready,
    // or if they have valid values (non-blocking mode with locatorConfig provides valid values)
    if (initSnsSqsResult.resourcesReady || initSnsSqsResult.queueUrl) {
      this.queueUrl = initSnsSqsResult.queueUrl
      this.subscriptionArn = initSnsSqsResult.subscriptionArn
    }

    // Only initialize DLQ if resources are ready and queueUrl is available
    if (initSnsSqsResult.resourcesReady && initSnsSqsResult.queueUrl) {
      await this.initDeadLetterQueue()
    }
  }

  /**
   * Starts the consumer. In non-blocking polling mode, if resources aren't ready yet,
   * this method will return immediately and consumers will start automatically once
   * resources become available.
   */
  public override async start() {
    await this.init()

    if (!this.resourcesReady) {
      // Resources not ready yet (non-blocking polling mode), mark that start was requested.
      // Consumers will be started automatically when onResourcesReady callback fires.
      this.startRequested = true
      this.logger.info({
        message: 'Start requested but resources not ready yet, will start when resources become available',
        queueName: this.queueName,
        topicArn: this.topicArn,
      })
      return
    }

    // Resources are ready, start consumers immediately
    await this.startConsumers()
  }

  protected override resolveMessage(message: SQSMessage) {
    const result = readSnsMessage(message, this.errorResolver)
    if (result.result) {
      return result
    }

    // if it not an SNS message, then it is a SQS message
    return super.resolveMessage(message)
  }

  protected override resolveSchema(messagePayload: MessagePayloadSchemas) {
    return this._messageSchemaContainer.resolveSchema(messagePayload)
  }
}
