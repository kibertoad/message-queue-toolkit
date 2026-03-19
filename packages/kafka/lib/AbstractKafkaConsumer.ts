import { randomUUID } from 'node:crypto'
import { pipeline } from 'node:stream/promises'
import { setTimeout } from 'node:timers/promises'
import {
  InternalError,
  resolveGlobalErrorLogObject,
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
const MAX_RECONNECT_ATTEMPTS = 5

export abstract class AbstractKafkaConsumer<
  TopicsConfig extends TopicConfig[],
  ExecutionContext,
  BatchProcessingEnabled extends boolean = false,
> extends AbstractKafkaService<
  TopicsConfig,
  KafkaConsumerOptions<TopicsConfig, ExecutionContext, BatchProcessingEnabled>
> {
  private consumer?: Consumer<string, object, string, string>
  private consumerStream?: MessagesStream<string, object, string, string>
  private messageBatchStream?: KafkaMessageBatchStream<
    DeserializedMessage<SupportedMessageValues<TopicsConfig>>
  >
  private isReconnecting: boolean

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

    this.isReconnecting = false
  }

  /**
   * Returns `true` if all client connections are currently active and the client is connected to at least one broker.
   * During a reconnect attempt, returns `true` until all reconnect attempts are exhausted.
   */
  get isConnected(): boolean {
    if (!this.consumer) return this.isReconnecting
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
   * Returns `true` if the consumer is not closed and is an active member of a consumer group.
   * Returns `false` during consumer group rebalancing.
   * During a reconnect attempt, returns `true` until all reconnect attempts are exhausted.
   */
  get isActive(): boolean {
    if (!this.consumer) return this.isReconnecting
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
    if (this.consumer) return Promise.resolve()

    const topics = Object.keys(this.options.handlers)
    if (topics.length === 0) throw new Error('At least one topic must be defined')

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
          readableHighWaterMark: this.options.batchProcessingOptions.readableHighWaterMark,
        })

        // Use pipeline for better error handling and backpressure management.
        // pipeline() internally listens for errors on all streams and rejects if any stream errors.
        // The .catch() here reports the error; reconnection is handled by handleStream's .catch() below.
        pipeline(this.consumerStream, this.messageBatchStream).catch((error) =>
          this.handleError(error),
        )
      } else {
        this.consumerStream.on('error', (error) => this.handleError(error))
      }
    } catch (error) {
      throw new InternalError({
        message: 'Consumer init failed',
        errorCode: 'KAFKA_CONSUMER_INIT_ERROR',
        cause: error,
      })
    }

    this.handleStream(
      this.messageBatchStream ? this.messageBatchStream : this.consumerStream,
    ).catch((error) => this.reconnect(error))
  }

  private async handleStream(
    stream:
      | MessagesStream<string, object, string, string>
      | KafkaMessageBatchStream<DeserializedMessage<SupportedMessageValues<TopicsConfig>>>,
  ) {
    for await (const messageOrBatch of stream) {
      await this.consume(
        Array.isArray(messageOrBatch) ? messageOrBatch[0].topic : messageOrBatch.topic,
        messageOrBatch,
      )
    }
  }

  async close(): Promise<void> {
    if (!this.consumer) return Promise.resolve()

    await this.consumerStream?.close()
    this.consumerStream = undefined

    await new Promise((resolve) =>
      this.messageBatchStream ? this.messageBatchStream?.end(resolve) : resolve(undefined),
    )
    this.messageBatchStream = undefined

    this.consumer.leaveGroup()
    await this.consumer.close()
    this.consumer = undefined
  }

  private async reconnect(error: unknown): Promise<void> {
    this.isReconnecting = true
    this.logger.info(
      { error: resolveGlobalErrorLogObject(error) },
      'Stream error detected, attempting to reconnect',
    )

    for (let attempt = 0; attempt < MAX_RECONNECT_ATTEMPTS; attempt++) {
      try {
        await this.close()
        await setTimeout(Math.pow(2, attempt) * 1000) // Backoff delay starting with 1s
        await this.init()
        this.isReconnecting = false
        return
      } catch (error) {
        this.logger.warn(
          {
            attempt,
            maxAttempts: MAX_RECONNECT_ATTEMPTS,
            error: resolveGlobalErrorLogObject(error),
          },
          'Reconnect attempt failed',
        )
      }
    }

    await this.close() // closing in case something is open after last init call
    this.isReconnecting = false
    this.handleError(new Error('Consumer failed to reconnect after max attempts'), {
      maxAttempts: MAX_RECONNECT_ATTEMPTS,
    })
  }

  private async consume(
    topic: SupportedTopics<TopicsConfig>,
    messageOrBatch: MessageOrBatch<SupportedMessageValues<TopicsConfig>>,
  ): Promise<void> {
    const messageProcessingStartTimestamp = Date.now()
    this.logger.debug({ topic }, 'Consuming message(s)')

    const handlerConfig = this.options.handlers[topic]

    // if there is no handler for the message, we ignore it (simulating subscription)
    if (!handlerConfig) return this.commit(messageOrBatch)

    const validMessages = this.parseMessages(
      handlerConfig,
      messageOrBatch,
      messageProcessingStartTimestamp,
    )

    if (!validMessages.length) {
      this.logger.debug({ topic }, 'Received not valid message(s)')
      return this.commit(messageOrBatch)
    } else {
      this.logger.debug(
        { topic, validMessagesCount: validMessages.length },
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
        this.handleError(parseResult.error, {
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
      this.handleError(error, { topic, ...errorContext })
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

  private async commit(messageOrBatch: MessageOrBatch<SupportedMessageValues<TopicsConfig>>) {
    let messageToCommit: DeserializedMessage<SupportedMessageValues<TopicsConfig>>
    if (Array.isArray(messageOrBatch)) {
      if (messageOrBatch.length === 0) return Promise.resolve()

      // biome-ignore lint/style/noNonNullAssertion: we check the length above
      messageToCommit = messageOrBatch[messageOrBatch.length - 1]!
    } else {
      messageToCommit = messageOrBatch
    }

    const logDetails = {
      topic: messageToCommit.topic,
      offset: messageToCommit.offset,
      timestamp: messageToCommit.timestamp,
    }
    this.logger.debug(logDetails, 'Trying to commit message')

    try {
      await messageToCommit.commit()
      this.logger.debug(logDetails, 'Message committed successfully')
    } catch (error) {
      this.logger.debug(logDetails, 'Message commit failed')
      return error instanceof ResponseError
        ? this.handleResponseErrorOnCommit(error)
        : this.handleError(error)
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
        this.handleError(error)
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
