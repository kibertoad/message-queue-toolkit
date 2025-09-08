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
import { AbstractKafkaService, type BaseKafkaOptions } from './AbstractKafkaService.ts'
import type {
  KafkaBatchHandler,
  KafkaBatchHandlerConfig,
  KafkaHandler,
  KafkaHandlerConfig,
} from './handler-container/index.ts'
import type { KafkaHandlerRouting } from './handler-container/KafkaHandlerRoutingBuilder.ts'
import type {
  KafkaConfig,
  KafkaDependencies,
  RequestContext,
  SupportedMessageValues,
  SupportedTopics,
  TopicConfig,
} from './types.ts'
import { ILLEGAL_GENERATION, REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID } from './utils/errorCodes.ts'
import {
  KAFKA_DEFAULT_BATCH_SIZE,
  KAFKA_DEFAULT_BATCH_TIMEOUT_MS,
  type KafkaMessageBatchOptions,
  KafkaMessageBatchStream,
} from './utils/KafkaMessageBatchStream.js'
import { safeJsonDeserializer } from './utils/safeJsonDeserializer.ts'

export type KafkaConsumerDependencies = KafkaDependencies &
  Pick<QueueConsumerDependencies, 'transactionObservabilityManager'>

export type KafkaBatchProcessingOptions<BatchProcessingEnabled> = {
  batchProcessingEnabled: BatchProcessingEnabled
  batchProcessingOptions?: BatchProcessingEnabled extends true ? KafkaMessageBatchOptions : never
}

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
    Message<string, SupportedMessageValues<TopicsConfig>, string, string>
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
    return this.consumer.isConnected()
  }

  /**
   * Returns `true` if the consumer is not closed, and it is currently an active member of a consumer group.
   * This method will return `false` during consumer group rebalancing.
   */
  get isActive(): boolean {
    return this.consumer.isActive()
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
      if (this.options.batchProcessingEnabled) {
        this.messageBatchStream = new KafkaMessageBatchStream<
          Message<string, SupportedMessageValues<TopicsConfig>, string, string>
        >({
          batchSize: this.options.batchProcessingOptions?.batchSize ?? KAFKA_DEFAULT_BATCH_SIZE,
          timeoutMilliseconds:
            this.options.batchProcessingOptions?.timeoutMilliseconds ??
            KAFKA_DEFAULT_BATCH_TIMEOUT_MS,
        })
        this.consumerStream.pipe(this.messageBatchStream)
      }
    } catch (error) {
      this.consumer.leaveGroup()
      throw new InternalError({
        message: 'Consumer init failed',
        errorCode: 'KAFKA_CONSUMER_INIT_ERROR',
        cause: error,
      })
    }

    if (this.options.batchProcessingEnabled && this.messageBatchStream) {
      this.messageBatchStream.on('data', async (messageBatch) => {
        await this.consumeBatch(messageBatch.topic, messageBatch.messages)
      })
      this.messageBatchStream.on('error', (error) => this.handlerError(error))
    } else {
      this.consumerStream.on('data', (message) =>
        this.consume(
          message as Message<string, SupportedMessageValues<TopicsConfig>, string, string>,
        ),
      )
    }

    this.consumerStream.on('error', (error) => this.handlerError(error))
  }

  async close(): Promise<void> {
    if (!this.consumerStream && !this.messageBatchStream) return Promise.resolve()

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

  // Consume single message
  private async consume(
    message: Message<string, SupportedMessageValues<TopicsConfig>, string, string>,
  ): Promise<void> {
    // message.value can be undefined if the message is not JSON-serializable
    if (!message.value) return this.commitMessage(message)

    const messageProcessingStartTimestamp = Date.now()

    // Casting is safe as at this point we already checked that batch processing is disabled
    const handlerConfig = this.resolveHandler(message.topic) as
      | KafkaHandlerConfig<SupportedMessageValues<TopicsConfig>, ExecutionContext>
      | undefined

    // if there is no handler for the message, we ignore it (simulating subscription)
    if (!handlerConfig) return this.commitMessage(message)

    /* v8 ignore next */
    const transactionId = this.resolveMessageId(message.value) ?? randomUUID()
    this.transactionObservabilityManager?.start(
      this.buildTransactionName(message.topic),
      transactionId,
    )

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

      return this.commitMessage(message)
    }

    const validatedMessage = { ...message, value: parseResult.data }

    const requestContext = this.getRequestContext(message)

    let retries = 0
    let processingResult: MessageProcessingResult = { status: 'error', errorReason: 'handlerError' }
    do {
      // exponential backoff -> 2^(retry-1)
      if (retries > 0) await setTimeout(Math.pow(2, retries - 1))

      processingResult = await this.tryToConsume(
        validatedMessage,
        handlerConfig.handler,
        requestContext,
      )
      if (processingResult.status === 'consumed') break

      retries++
    } while (retries < MAX_IN_MEMORY_RETRIES)

    this.handleMessageProcessed({
      message: validatedMessage,
      processingResult: processingResult,
      messageProcessingStartTimestamp,
    })

    this.transactionObservabilityManager?.stop(transactionId)

    return this.commitMessage(validatedMessage)
  }

  // TODO extract to separate class and add unit tests for partitioning and observability
  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: will be refactored
  private async consumeBatch(
    topic: string,
    messages: Message<string, SupportedMessageValues<TopicsConfig>, string, string>[],
  ): Promise<void> {
    const messageProcessingStartTimestamp = Date.now()

    // Grouping message by partition since each partition should be processed and committed separately
    const messagesGroupedByPartition = messages.reduce(
      (acc, message) => {
        if (!acc[message.partition]) {
          acc[message.partition] = [message]
        } else {
          acc[message.partition]?.push(message)
        }
        return acc
      },
      {} as Record<number, Message<string, SupportedMessageValues<TopicsConfig>, string, string>[]>,
    )

    // Casting is safe as at this point we already checked that batch processing is enabled
    const handlerConfig = this.resolveHandler(topic) as
      | KafkaBatchHandlerConfig<SupportedMessageValues<TopicsConfig>, ExecutionContext>
      | undefined

    for (const messagesInPartition of Object.values(messagesGroupedByPartition)) {
      if (!handlerConfig) {
        await this.commitLastMessage(messagesInPartition)
        continue
      }

      const validMessages: Message<string, SupportedMessageValues<TopicsConfig>, string, string>[] =
        []

      for (const message of messagesInPartition) {
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

      if (!validMessages.length) {
        await this.commitLastMessage(messagesInPartition)
        continue
      }

      /* v8 ignore next */
      const transactionId = randomUUID()
      this.transactionObservabilityManager?.start(
        this.buildTransactionNameForBatch(topic),
        transactionId,
      )

      // All messages in the batch should have the same requestContext
      // biome-ignore lint/style/noNonNullAssertion: we check the length above
      const requestContext = this.getRequestContext(validMessages[0]!)

      let retries = 0
      let processingResult: MessageProcessingResult = {
        status: 'error',
        errorReason: 'handlerError',
      }
      do {
        // exponential backoff -> 2^(retry-1)
        if (retries > 0) await setTimeout(Math.pow(2, retries - 1))

        processingResult = await this.tryToConsumeBatch(
          topic,
          validMessages,
          handlerConfig.handler,
          requestContext,
        )
        if (processingResult.status === 'consumed') break

        retries++
      } while (retries < MAX_IN_MEMORY_RETRIES)

      for (const message of validMessages) {
        this.handleMessageProcessed({
          message,
          processingResult,
          messageProcessingStartTimestamp,
        })
      }

      this.transactionObservabilityManager?.stop(transactionId)

      await this.commitLastMessage(messagesInPartition)
    }
  }

  private async tryToConsume<MessageValue extends object>(
    message: Message<string, MessageValue, string, string>,
    handler: KafkaHandler<MessageValue, ExecutionContext, false>,
    requestContext: RequestContext,
  ): Promise<MessageProcessingResult> {
    try {
      await handler(message, this.executionContext, requestContext)
      return { status: 'consumed' }
    } catch (error) {
      this.handlerError(error, {
        topic: message.topic,
        message: stringValueSerializer(message.value),
      })
    }

    return { status: 'error', errorReason: 'handlerError' }
  }

  private async tryToConsumeBatch<MessageValue extends object>(
    topic: string,
    messages: Message<string, MessageValue, string, string>[],
    handler: KafkaBatchHandler<MessageValue, ExecutionContext>,
    requestContext: RequestContext,
  ): Promise<MessageProcessingResult> {
    try {
      await handler(messages, this.executionContext, requestContext)
      return { status: 'consumed' }
    } catch (error) {
      this.handlerError(error, { topic, batchSize: messages.length })
    }

    return { status: 'error', errorReason: 'handlerError' }
  }

  private commitLastMessage(messages: Message<string, object, string, string>[]) {
    if (messages.length === 0) {
      return Promise.resolve()
    }
    // biome-ignore lint/style/noNonNullAssertion: we check the length above
    return this.commitMessage(messages[messages.length - 1]!)
  }

  private async commitMessage(message: Message<string, object, string, string>) {
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
    return `kafka:${this.constructor.name}:${topic}`
  }

  private buildTransactionNameForBatch(topic: string) {
    return `${this.buildTransactionName(topic)}:batch`
  }

  private getRequestContext(message: Message<string, object, string, string>): RequestContext {
    let reqId = message.headers.get(this.resolveHeaderRequestIdField())
    if (!reqId || reqId.trim().length === 0) reqId = randomUUID()

    return {
      reqId,
      logger: this.logger.child({
        'x-request-id': reqId,
        topic: message.topic,
        messageKey: message.key,
      }),
    }
  }
}
