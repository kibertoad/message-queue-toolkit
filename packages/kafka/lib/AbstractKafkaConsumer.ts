import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import {
  InternalError,
  stringValueSerializer,
  type TransactionObservabilityManager,
} from '@lokalise/node-core'
import type {
  MessageProcessingResult,
  QueueConsumerDependencies,
} from '@message-queue-toolkit/core'
import {
  type ConsumeOptions,
  Consumer,
  type ConsumerOptions,
  type Message,
  type MessagesStream,
  ProtocolError,
  ResponseError,
  stringDeserializer,
} from '@platformatic/kafka'
import {
  AbstractKafkaService,
  type BaseKafkaOptions,
  type ProcessedMessage,
} from './AbstractKafkaService.ts'
import type { KafkaHandler, KafkaHandlerConfig } from './handler-routing/index.ts'
import type { KafkaHandlerRouting } from './handler-routing/KafkaHandlerRoutingBuilder.ts'
import type {
  DeserializedMessage,
  KafkaConfig,
  KafkaDependencies,
  RequestContext,
  SupportedMessageValues,
  SupportedTopics,
  TopicConfig,
} from './types.ts'
import { ILLEGAL_GENERATION, REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID } from './utils/errorCodes.ts'
import {
  type KafkaMessageBatchOptions,
  KafkaMessageBatchStream,
} from './utils/KafkaMessageBatchStream.ts'
import { safeJsonDeserializer } from './utils/safeJsonDeserializer.ts'

export type KafkaConsumerDependencies = KafkaDependencies &
  Pick<QueueConsumerDependencies, 'transactionObservabilityManager'>

export type KafkaBatchProcessingOptions<BatchProcessingEnabled> =
  BatchProcessingEnabled extends true
    ? {
        batchProcessingEnabled: true
        batchProcessingOptions: KafkaMessageBatchOptions
      }
    : {
        batchProcessingEnabled: false
        batchProcessingOptions?: never
      }

type MessageOrBatch<MessageValue extends object> =
  | DeserializedMessage<MessageValue>
  | DeserializedMessage<MessageValue>[]

export type KafkaConsumerOptions<
  TopicsConfig extends TopicConfig[],
  ExecutionContext,
  BatchProcessingEnabled extends boolean,
> = BaseKafkaOptions &
  Omit<
    ConsumerOptions<string, object, string, string>,
    'deserializers' | 'autocommit' | keyof KafkaConfig
  > &
  Omit<ConsumeOptions<string, object, string, string>, 'topics'> &
  KafkaBatchProcessingOptions<BatchProcessingEnabled> & {
    handlers: KafkaHandlerRouting<TopicsConfig, ExecutionContext, BatchProcessingEnabled>
  }

const commitErrorCodesToIgnore = new Set([
  ILLEGAL_GENERATION,
  UNKNOWN_MEMBER_ID,
  REBALANCE_IN_PROGRESS,
])

/*
TODO: Proper retry mechanism + DLQ -> https://lokalise.atlassian.net/browse/EDEXP-498
In the meantime, we will retry in memory up to 3 times
 */
const MAX_IN_MEMORY_RETRIES = 3

export abstract class AbstractKafkaConsumer<
  TopicsConfig extends TopicConfig[],
  ExecutionContext,
  BatchProcessingEnabled extends boolean = false,
> extends AbstractKafkaService<
  TopicsConfig,
  KafkaConsumerOptions<TopicsConfig, ExecutionContext, BatchProcessingEnabled>
> {
  private readonly consumer: Consumer<string, object, string, string>
  private consumerStream?: MessagesStream<string, object, string, string>
  private messageBatchStream?: KafkaMessageBatchStream<
    DeserializedMessage<SupportedMessageValues<TopicsConfig>>
  >

  private readonly transactionObservabilityManager: TransactionObservabilityManager
  private readonly executionContext: ExecutionContext
  private readonly maxFetches: number

  private readonly syncMessagesToProcess: Message<string, object, string, string>[] = []
  private syncMessagesProcessing: boolean = false

  constructor(
    dependencies: KafkaConsumerDependencies,
    options: KafkaConsumerOptions<TopicsConfig, ExecutionContext, BatchProcessingEnabled>,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)

    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.executionContext = executionContext
    this.maxFetches = this.options.maxFetches ?? 10

    this.consumer = new Consumer({
      ...this.options.kafka,
      ...this.options,
      autocommit: false, // Handling commits manually
      deserializers: {
        key: stringDeserializer,
        value: safeJsonDeserializer,
        headerKey: stringDeserializer,
        headerValue: stringDeserializer,
      },
    })

    const logDetails = { origin: this.constructor.name, groupId: this.options.groupId }
    /* v8 ignore start */
    this.consumer.on('consumer:group:join', (_) =>
      this.logger.debug(logDetails, 'Consumer is joining a group'),
    )
    this.consumer.on('consumer:rejoin', (_) =>
      this.logger.debug(logDetails, 'Consumer is re-joining a group after a rebalance'),
    )
    this.consumer.on('consumer:group:leave', (_) =>
      this.logger.debug(logDetails, 'Consumer is leaving the group'),
    )
    this.consumer.on('consumer:group:rebalance', (_) =>
      this.logger.debug(logDetails, 'Group is rebalancing'),
    )
    /* v8 ignore stop */
  }

  /**
   * Returns true if all client's connections are currently connected and the client is connected to at least one broker.
   */
  get isConnected(): boolean {
    // Streams are created only when init method was called
    if (!this.consumerStream && !this.messageBatchStream) return false
    try {
      return this.consumer.isConnected()
    } catch (_) {
      // this should not happen, but if so it means the consumer is not healthy
      /* v8 ignore next */
      return false
    }
  }

  /**
   * Returns `true` if the consumer is not closed, and it is currently an active member of a consumer group.
   * This method will return `false` during consumer group rebalancing.
   */
  get isActive(): boolean {
    // Streams are created only when init method was called
    if (!this.consumerStream && !this.messageBatchStream) return false
    try {
      return this.consumer.isActive()
    } catch (_) {
      // this should not happen, but if so it means the consumer is not healthy
      /* v8 ignore next */
      return false
    }
  }

  async init(): Promise<void> {
    if (this.consumerStream) return Promise.resolve()
    const topics = Object.keys(this.options.handlers)
    if (topics.length === 0) throw new Error('At least one topic must be defined')

    try {
      const { handlers: _, ...consumeOptions } = this.options // Handlers cannot be passed to consume method

      // https://github.com/platformatic/kafka/blob/main/docs/consumer.md#my-consumer-is-not-receiving-any-message-when-the-application-restarts
      await this.consumer.joinGroup({
        sessionTimeout: consumeOptions.sessionTimeout,
        rebalanceTimeout: consumeOptions.rebalanceTimeout,
        heartbeatInterval: consumeOptions.heartbeatInterval,
      })

      this.consumerStream = await this.consumer.consume({ ...consumeOptions, topics })
      if (this.options.batchProcessingEnabled && this.options.batchProcessingOptions) {
        this.messageBatchStream = new KafkaMessageBatchStream<
          DeserializedMessage<SupportedMessageValues<TopicsConfig>>
        >({
          batchSize: this.options.batchProcessingOptions.batchSize,
          timeoutMilliseconds: this.options.batchProcessingOptions.timeoutMilliseconds,
        })
        this.consumerStream.pipe(this.messageBatchStream)
      }
    } catch (error) {
      throw new InternalError({
        message: 'Consumer init failed',
        errorCode: 'KAFKA_CONSUMER_INIT_ERROR',
        cause: error,
      })
    }

    if (this.options.batchProcessingEnabled && this.messageBatchStream) {
      this.messageBatchStream.on('data', async (messageBatch) =>
        this.consume(messageBatch.topic, messageBatch.messages),
      )
      this.messageBatchStream.on('error', (error) => this.handlerError(error))
    } else {
      // biome-ignore lint/style/noNonNullAssertion: consumerStream is always created
      const stream = this.consumerStream!
      stream.on('data', (message) => {
        // Pause stream when we've reached maxFetches to control backpressure
        // Only pause if we've actually reached the limit (not on every message)
        if (this.syncMessagesToProcess.length >= this.maxFetches) {
          stream.pause()
        }

        this.syncMessagesToProcess.push(message)
        this.startProcessingSyncMessages(stream)
      })
    }

    this.consumerStream.on('error', (error) => this.handlerError(error))
  }

  private async startProcessingSyncMessages(
    stream: MessagesStream<unknown, unknown, unknown, unknown>,
  ): Promise<void> {
    if (this.syncMessagesProcessing) return

    this.syncMessagesProcessing = true

    do {
      const message = this.syncMessagesToProcess.shift()

      if (!message) {
        this.syncMessagesProcessing = false
        return
      }

      if (this.syncMessagesToProcess.length >= this.maxFetches / 2 && stream.isPaused()) {
        stream.resume()
      }

      try {
        await this.consume(
          message.topic,
          message as DeserializedMessage<SupportedMessageValues<TopicsConfig>>,
        )
      } catch (error) {
        this.handlerError(error)
      }
    } while (this.syncMessagesToProcess.length > 0)
  }

  async close(): Promise<void> {
    if (!this.consumerStream && !this.messageBatchStream) {
      // Leaving the group in case consumer joined but streams were not created
      if (this.isActive) this.consumer.leaveGroup()
      return
    }

    if (this.consumerStream) {
      await this.consumerStream.close()
      this.consumerStream = undefined
    }

    if (this.messageBatchStream) {
      await new Promise((resolve) => this.messageBatchStream?.end(resolve))
      this.messageBatchStream = undefined
    }

    await this.consumer.close()
  }

  private resolveHandler(topic: SupportedTopics<TopicsConfig>) {
    return this.options.handlers[topic]
  }

  private async consume(
    topic: string,
    messageOrBatch: MessageOrBatch<SupportedMessageValues<TopicsConfig>>,
  ): Promise<void> {
    const messageProcessingStartTimestamp = Date.now()

    const handlerConfig = this.resolveHandler(topic)

    // if there is no handler for the message, we ignore it (simulating subscription)
    if (!handlerConfig) return this.commit(messageOrBatch)

    const validMessages = this.parseMessages(
      handlerConfig,
      messageOrBatch,
      messageProcessingStartTimestamp,
    )

    if (!validMessages.length) {
      return this.commit(messageOrBatch)
    }

    // biome-ignore lint/style/noNonNullAssertion: we check validMessages length above
    const firstMessage = validMessages[0]!

    const requestContext = this.getRequestContext(firstMessage)

    /* v8 ignore next */
    const transactionId = randomUUID()
    this.transactionObservabilityManager?.start(this.buildTransactionName(topic), transactionId)

    const processingResult = await this.tryToConsumeWithRetries(
      topic,
      // If batch processing is disabled, we have only single message to process
      this.options.batchProcessingEnabled ? validMessages : firstMessage,
      handlerConfig.handler,
      requestContext,
    )

    this.handleMessagesProcessed(validMessages, processingResult, messageProcessingStartTimestamp)

    this.transactionObservabilityManager?.stop(transactionId)

    // We commit all messages, even if some of them were filtered out on validation
    return this.commit(messageOrBatch)
  }

  private parseMessages(
    handlerConfig: KafkaHandlerConfig<
      SupportedMessageValues<TopicsConfig>,
      ExecutionContext,
      BatchProcessingEnabled
    >,
    messageOrBatch: MessageOrBatch<SupportedMessageValues<TopicsConfig>>,
    messageProcessingStartTimestamp: number,
  ) {
    const messagesToCheck = Array.isArray(messageOrBatch) ? messageOrBatch : [messageOrBatch]

    const validMessages: DeserializedMessage<SupportedMessageValues<TopicsConfig>>[] = []

    for (const message of messagesToCheck) {
      // message.value can be undefined if the message is not JSON-serializable
      if (!message.value) {
        continue
      }

      const parseResult = handlerConfig.schema.safeParse(message.value)

      if (!parseResult.success) {
        this.handlerError(parseResult.error, {
          topic: message.topic,
          message: stringValueSerializer(message.value),
        })
        this.handleMessageProcessed({
          message: message,
          processingResult: { status: 'error', errorReason: 'invalidMessage' },
          messageProcessingStartTimestamp,
        })
        continue
      }

      validMessages.push({ ...message, value: parseResult.data })
    }

    return validMessages
  }

  private async tryToConsumeWithRetries<MessageValue extends object>(
    topic: string,
    messageOrBatch: MessageOrBatch<MessageValue>,
    handler: KafkaHandler<MessageValue, ExecutionContext, BatchProcessingEnabled>,
    requestContext: RequestContext,
  ) {
    let retries = 0
    let processingResult: MessageProcessingResult = {
      status: 'error',
      errorReason: 'handlerError',
    }
    do {
      // exponential backoff -> 2^(retry-1)
      if (retries > 0) await setTimeout(Math.pow(2, retries - 1))

      processingResult = await this.tryToConsume(topic, messageOrBatch, handler, requestContext)
      if (processingResult.status === 'consumed') break

      retries++
    } while (retries < MAX_IN_MEMORY_RETRIES)

    return processingResult
  }

  private async tryToConsume<MessageValue extends object>(
    topic: string,
    messageOrBatch: MessageOrBatch<MessageValue>,
    handler: KafkaHandler<MessageValue, ExecutionContext, BatchProcessingEnabled>,
    requestContext: RequestContext,
  ): Promise<MessageProcessingResult> {
    try {
      const isBatch = Array.isArray(messageOrBatch)
      if (this.options.batchProcessingEnabled && !isBatch) {
        throw new Error(
          'Batch processing is enabled, but a single message was passed to the handler',
        )
      }
      if (!this.options.batchProcessingEnabled && isBatch) {
        throw new Error(
          'Batch processing is disabled, but a batch of messages was passed to the handler',
        )
      }

      await handler(
        // We need casting to match message type with handler type - it is safe as we verify the type above
        messageOrBatch as BatchProcessingEnabled extends false
          ? DeserializedMessage<MessageValue>
          : DeserializedMessage<MessageValue>[],
        this.executionContext,
        requestContext,
      )
      return { status: 'consumed' }
    } catch (error) {
      const errorContext = Array.isArray(messageOrBatch)
        ? { batchSize: messageOrBatch.length }
        : { message: stringValueSerializer(messageOrBatch.value) }
      this.handlerError(error, {
        topic,
        ...errorContext,
      })
    }

    return { status: 'error', errorReason: 'handlerError' }
  }

  private handleMessagesProcessed(
    messages: ProcessedMessage<TopicsConfig>[],
    processingResult: MessageProcessingResult,
    messageProcessingStartTimestamp: number,
  ) {
    for (const message of messages) {
      this.handleMessageProcessed({
        message,
        processingResult,
        messageProcessingStartTimestamp,
      })
    }
  }

  private commit(messageOrBatch: MessageOrBatch<SupportedMessageValues<TopicsConfig>>) {
    if (Array.isArray(messageOrBatch)) {
      if (messageOrBatch.length === 0) {
        return Promise.resolve()
      }
      // biome-ignore lint/style/noNonNullAssertion: we check the length above
      return this.commitMessage(messageOrBatch[messageOrBatch.length - 1]!)
    } else {
      return this.commitMessage(messageOrBatch)
    }
  }

  private async commitMessage(message: DeserializedMessage<SupportedMessageValues<TopicsConfig>>) {
    try {
      this.logger.debug(
        { topic: message.topic, offset: message.offset, timestamp: message.timestamp },
        'Trying to commit message',
      )
      await message.commit()
    } catch (error) {
      if (error instanceof ResponseError) return this.handleResponseErrorOnCommit(error)
      throw error
    }
  }

  private handleResponseErrorOnCommit(responseError: ResponseError) {
    // Some errors are expected during group rebalancing, so we handle them gracefully
    for (const error of responseError.errors) {
      if (
        error instanceof ProtocolError &&
        error.apiCode &&
        commitErrorCodesToIgnore.has(error.apiCode)
      ) {
        this.logger.error(
          {
            apiCode: error.apiCode,
            apiId: error.apiId,
            responseErrorMessage: responseError.message,
            protocolErrorMessage: error.message,
            error: responseError,
          },
          `Failed to commit message: ${error.message}`,
        )
      } else {
        // If error is not recognized, rethrow it
        throw responseError
      }
    }
  }

  private buildTransactionName(topic: string) {
    const baseTransactionName = `kafka:${this.constructor.name}:${topic}`
    return this.options.batchProcessingEnabled
      ? `${baseTransactionName}:batch`
      : baseTransactionName
  }

  private getRequestContext(
    message: DeserializedMessage<SupportedMessageValues<TopicsConfig>>,
  ): RequestContext {
    let reqId = message.headers.get(this.resolveHeaderRequestIdField())
    if (!reqId || reqId.trim().length === 0) reqId = randomUUID()

    return {
      reqId,
      logger: this.logger.child({
        'x-request-id': reqId,
        origin: this.constructor.name,
        topic: message.topic,
        messageKey: message.key,
      }),
    }
  }
}
