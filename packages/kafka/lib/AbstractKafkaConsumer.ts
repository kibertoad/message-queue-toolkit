import { randomUUID } from 'node:crypto'
import { pipeline } from 'node:stream/promises'
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

  constructor(
    dependencies: KafkaConsumerDependencies,
    options: KafkaConsumerOptions<TopicsConfig, ExecutionContext, BatchProcessingEnabled>,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)

    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.executionContext = executionContext

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
    this.consumer.on('consumer:group:rejoin', () =>
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
      /* v8 ignore start */
    } catch (_) {
      // this should not happen, but if so it means the consumer is not healthy
      return false
    }
    /* v8 ignore stop */
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
      /* v8 ignore start */
    } catch (_) {
      // this should not happen, but if so it means the consumer is not healthy
      return false
    }
    /* v8 ignore stop */
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
      this.consumerStream.on('error', (error) => this.handlerError(error))

      if (this.options.batchProcessingEnabled && this.options.batchProcessingOptions) {
        this.messageBatchStream = new KafkaMessageBatchStream<
          DeserializedMessage<SupportedMessageValues<TopicsConfig>>
        >({
          batchSize: this.options.batchProcessingOptions.batchSize,
          timeoutMilliseconds: this.options.batchProcessingOptions.timeoutMilliseconds,
        })

        // Use pipeline for better error handling and backpressure management
        pipeline(this.consumerStream, this.messageBatchStream).catch((error) =>
          this.handlerError(error),
        )
      }
    } catch (error) {
      throw new InternalError({
        message: 'Consumer init failed',
        errorCode: 'KAFKA_CONSUMER_INIT_ERROR',
        cause: error,
      })
    }

    if (this.messageBatchStream) {
      this.handleSyncStreamBatch(this.messageBatchStream).catch((error) => this.handlerError(error))
    } else {
      this.handleSyncStream(this.consumerStream).catch((error) => this.handlerError(error))
    }
  }

  private async handleSyncStream(
    stream: MessagesStream<string, object, string, string>,
  ): Promise<void> {
    for await (const message of stream) {
      await this.consume(
        message.topic,
        message as DeserializedMessage<SupportedMessageValues<TopicsConfig>>,
      )
    }
  }
  private async handleSyncStreamBatch(
    stream: KafkaMessageBatchStream<DeserializedMessage<SupportedMessageValues<TopicsConfig>>>,
  ): Promise<void> {
    for await (const messageBatch of stream) {
      await this.consume(messageBatch[0].topic, messageBatch)
    }
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
    this.logger.debug({ origin: this.constructor.name, topic }, 'Consuming message(s)')

    const handlerConfig = this.resolveHandler(topic)

    // if there is no handler for the message, we ignore it (simulating subscription)
    if (!handlerConfig) return this.commit(messageOrBatch)

    const validMessages = this.parseMessages(
      handlerConfig,
      messageOrBatch,
      messageProcessingStartTimestamp,
    )

    if (!validMessages.length) {
      this.logger.debug({ origin: this.constructor.name, topic }, 'Received not valid message(s)')
      return this.commit(messageOrBatch)
    } else {
      this.logger.debug(
        { origin: this.constructor.name, topic, validMessagesCount: validMessages.length },
        'Received valid message(s) to process',
      )
    }

    // biome-ignore lint/style/noNonNullAssertion: we check validMessages length above
    const firstMessage = validMessages[0]!
    const requestContext = this.getRequestContext(firstMessage)

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
      /* v8 ignore start */
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
      /* v8 ignore stop */

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
      this.handlerError(error, { topic, ...errorContext })
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
      if (messageOrBatch.length === 0) return Promise.resolve()

      // biome-ignore lint/style/noNonNullAssertion: we check the length above
      return this.commitMessage(messageOrBatch[messageOrBatch.length - 1]!)
    } else {
      return this.commitMessage(messageOrBatch)
    }
  }

  private async commitMessage(message: DeserializedMessage<SupportedMessageValues<TopicsConfig>>) {
    const logDetails = {
      topic: message.topic,
      offset: message.offset,
      timestamp: message.timestamp,
    }
    this.logger.debug(logDetails, 'Trying to commit message')

    try {
      await message.commit()
      this.logger.debug(logDetails, 'Message committed successfully')
    } catch (error) {
      this.logger.debug(logDetails, 'Message commit failed')
      if (error instanceof ResponseError) return this.handleResponseErrorOnCommit(error)
      this.handlerError(error)
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
        this.logger.warn(
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
        this.handlerError(error)
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
