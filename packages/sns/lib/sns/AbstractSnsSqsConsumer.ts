import type { SNSClient } from '@aws-sdk/client-sns'
import { SetSubscriptionAttributesCommand } from '@aws-sdk/client-sns'
import type { STSClient } from '@aws-sdk/client-sts'
import { type Either, InternalError } from '@lokalise/node-core'
import type {
  MessageInvalidFormatError,
  MessageValidationError,
  ResolvedMessage,
} from '@message-queue-toolkit/core'
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
  // Intersected after SQSConsumerOptions (rather than wrapping it) so its discriminated
  // union (fifoQueue: true | false) is preserved and Extract<…, {fifoQueue:true}> works.
  // Codec consumer options (`codecs`, `disableCodecAutoDetection`) come from
  // SQSConsumerOptions; SNSOptions carries no codec fields.
  SNSOptions & {
    subscriptionConfig?: SNSSubscriptionOptions
    /**
     * Opt-in DLQ for the SNS subscription itself (captures messages SNS could
     * not deliver to the SQS endpoint — endpoint deleted, IAM/policy errors,
     * throttling — that would otherwise be silently lost).
     *
     * `reuseConsumerDeadLetterQueue: true` reuses the queue declared in
     * `deadLetterQueue`. The consumer's DLQ then receives both processing
     * failures (via the source queue RedrivePolicy + maxReceiveCount) and
     * SNS delivery failures (via the subscription RedrivePolicy).
     *
     * Shape is an object (not a boolean) so future versions can add an
     * alternative that creates a separate DLQ without a breaking change.
     */
    subscriptionDeadLetterQueue?: { reuseConsumerDeadLetterQueue: true }
  }

type SubscriptionResource = {
  topicArn: string
  subscriptionArn: string
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
  private readonly reuseConsumerDeadLetterQueueForSubscription: boolean
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

  /**
   * Resolved topic + subscription handle. Populated together by `initSnsSqs`
   * (either synchronously or via the non-blocking `onResourcesReady`
   * callback). Subclasses read via the {@link subscription} getter so the
   * ARNs are impossible to access in an "uninitialised" state.
   */
  private _subscription?: SubscriptionResource

  protected constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: SNSSQSConsumerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, { ...options }, executionContext)

    this.subscriptionConfig = options.subscriptionConfig
    this.reuseConsumerDeadLetterQueueForSubscription =
      !!options.subscriptionDeadLetterQueue?.reuseConsumerDeadLetterQueue

    if (this.reuseConsumerDeadLetterQueueForSubscription && !options.deadLetterQueue) {
      throw new InternalError({
        errorCode: 'invalid_subscription_dlq_configuration',
        message:
          'subscriptionDeadLetterQueue.reuseConsumerDeadLetterQueue requires deadLetterQueue to be configured',
      })
    }

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
          this.setSubscriptionResource({
            topicArn: result.topicArn,
            subscriptionArn: result.subscriptionArn,
          })
          this.setQueueResource({
            name: result.queueName,
            url: result.queueUrl,
            arn: result.queueArn,
          })
          this.resourcesReady = true

          // Initialize DLQ now that resources are ready (this is mutually exclusive
          // with the synchronous initDeadLetterQueue call below)
          this.initDeadLetterQueue()
            .catch((err) => {
              this.logger.error({
                message: 'Failed to initialize dead letter queue after resources became ready',
                error: err,
              })
            })
            .then(() => {
              // If start() was called while resources weren't ready, start consumers now
              if (this.startRequested) {
                this.logger.info({
                  message: 'Resources now ready, starting consumers',
                  queueName: this.creationConfig?.queue.QueueName ?? this.locatorConfig?.queueName,
                  topicArn: result.topicArn,
                })
                return this.startConsumers()
              }
            })
            .catch((err) => {
              this.logger.error({
                message: 'Failed to start consumers after resources became ready',
                error: err,
              })
            })
        },
      },
    )

    // `initSnsSqs` returns undefined in non-blocking polling mode when
    // resources aren't yet ready; the onResourcesReady callback above will
    // populate state once they are.
    if (!initSnsSqsResult) {
      this.resourcesReady = false
      return
    }

    this.setSubscriptionResource({
      topicArn: initSnsSqsResult.topicArn,
      subscriptionArn: initSnsSqsResult.subscriptionArn,
    })
    this.setQueueResource({
      name: initSnsSqsResult.queueName,
      url: initSnsSqsResult.queueUrl,
      arn: initSnsSqsResult.queueArn,
    })
    this.resourcesReady = true

    await this.initDeadLetterQueue()
  }

  protected get subscription(): Readonly<SubscriptionResource> {
    if (!this._subscription) throw new Error('Subscription is not started yet')
    return this._subscription
  }

  protected setSubscriptionResource(resource: SubscriptionResource): void {
    this._subscription = resource
  }

  override async initDeadLetterQueue(): Promise<void> {
    await super.initDeadLetterQueue()

    if (!this.reuseConsumerDeadLetterQueueForSubscription) return
    const dlq = this.deadLetterQueue
    if (!dlq) return

    await this.snsClient.send(
      new SetSubscriptionAttributesCommand({
        SubscriptionArn: this.subscription.subscriptionArn,
        AttributeName: 'RedrivePolicy',
        AttributeValue: JSON.stringify({ deadLetterTargetArn: dlq.arn }),
      }),
    )
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
        message:
          'Start requested but resources not ready yet, will start when resources become available',
        queueName: this.creationConfig?.queue.QueueName ?? this.locatorConfig?.queueName,
        topicArn: this.locatorConfig?.topicArn,
      })
      return
    }

    // Resources are ready, start consumers immediately
    await this.startConsumers()
  }

  protected override resolveMessage(
    message: SQSMessage,
  ): Either<MessageInvalidFormatError | MessageValidationError, ResolvedMessage> {
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
