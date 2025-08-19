import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import {
  InternalError,
  type TransactionObservabilityManager,
  stringValueSerializer,
} from '@lokalise/node-core'
import type { QueueConsumerDependencies } from '@message-queue-toolkit/core'
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
import { KafkaHandlerContainer } from './handler-container/KafkaHandlerContainer.ts'
import type { KafkaHandlerRouting } from './handler-container/KafkaHandlerRoutingBuilder.ts'
import type { KafkaHandler } from './handler-container/index.ts'
import type {
  KafkaConfig,
  KafkaDependencies,
  RequestContext,
  SupportedMessageValues,
  TopicConfig,
} from './types.ts'
import { ILLEGAL_GENERATION, REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID } from './utils/errorCodes.ts'
import { safeJsonDeserializer } from './utils/safeJsonDeserializer.ts'

export type KafkaConsumerDependencies = KafkaDependencies &
  Pick<QueueConsumerDependencies, 'transactionObservabilityManager'>

export type KafkaConsumerOptions<
  TopicsConfig extends TopicConfig[],
  ExecutionContext,
> = BaseKafkaOptions &
  Omit<
    ConsumerOptions<string, object, string, string>,
    'deserializers' | 'autocommit' | keyof KafkaConfig
  > &
  Omit<ConsumeOptions<string, object, string, string>, 'topics'> & {
    handlers: KafkaHandlerRouting<TopicsConfig, ExecutionContext>
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
> extends AbstractKafkaService<TopicsConfig, KafkaConsumerOptions<TopicsConfig, ExecutionContext>> {
  private readonly consumer: Consumer<string, object, string, string>
  private consumerStream?: MessagesStream<string, object, string, string>

  private readonly transactionObservabilityManager: TransactionObservabilityManager
  private readonly handlerContainer: KafkaHandlerContainer<TopicsConfig, ExecutionContext>
  private readonly executionContext: ExecutionContext

  constructor(
    dependencies: KafkaConsumerDependencies,
    options: KafkaConsumerOptions<TopicsConfig, ExecutionContext>,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)

    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.handlerContainer = new KafkaHandlerContainer<TopicsConfig, ExecutionContext>(
      options.handlers,
      options.messageTypeField,
    )
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
    const topics = this.handlerContainer.topics
    if (topics.length === 0) throw new Error('At least one topic must be defined')

    try {
      const { handlers, ...consumeOptions } = this.options // Handlers cannot be passed to consume method

      // https://github.com/platformatic/kafka/blob/main/docs/consumer.md#my-consumer-is-not-receiving-any-message-when-the-application-restarts
      await this.consumer.joinGroup({
        sessionTimeout: consumeOptions.sessionTimeout,
        rebalanceTimeout: consumeOptions.rebalanceTimeout,
        heartbeatInterval: consumeOptions.heartbeatInterval,
      })
      this.consumerStream = await this.consumer.consume({ ...consumeOptions, topics })
    } catch (error) {
      throw new InternalError({
        message: 'Consumer init failed',
        errorCode: 'KAFKA_CONSUMER_INIT_ERROR',
        cause: error,
      })
    }

    this.consumerStream.on('data', (message) =>
      this.consume(
        message as Message<string, SupportedMessageValues<TopicsConfig>, string, string>,
      ),
    )
    this.consumerStream.on('error', (error) => this.handlerError(error))
  }

  async close(): Promise<void> {
    if (!this.consumerStream) return Promise.resolve()

    await new Promise((done) => this.consumerStream?.close(done))
    this.consumerStream = undefined
    await this.consumer.close()
  }

  private async consume(
    message: Message<string, SupportedMessageValues<TopicsConfig>, string, string>,
  ): Promise<void> {
    // message.value can be undefined if the message is not JSON-serializable
    if (!message.value) return this.commitMessage(message)

    const messageProcessingStartTimestamp = Date.now()

    const handler = this.handlerContainer.resolveHandler(message.topic, message.value)
    // if there is no handler for the message, we ignore it (simulating subscription)
    if (!handler) return this.commitMessage(message)

    /* v8 ignore next */
    const transactionId = this.resolveMessageId(message.value) ?? randomUUID()
    this.transactionObservabilityManager?.start(this.buildTransactionName(message), transactionId)

    const parseResult = handler.schema.safeParse(message.value)
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
    let consumed = false
    do {
      // exponential backoff -> 2^(retry-1)
      if (retries > 0) await setTimeout(Math.pow(2, retries - 1))

      consumed = await this.tryToConsume(validatedMessage, handler.handler, requestContext)
      if (consumed) break

      retries++
    } while (retries < MAX_IN_MEMORY_RETRIES)

    if (consumed) {
      this.handleMessageProcessed({
        message: validatedMessage,
        processingResult: { status: 'consumed' },
        messageProcessingStartTimestamp,
      })
    } else {
      this.handleMessageProcessed({
        message: validatedMessage,
        processingResult: { status: 'error', errorReason: 'handlerError' },
        messageProcessingStartTimestamp,
      })
    }

    this.transactionObservabilityManager?.stop(transactionId)

    return this.commitMessage(validatedMessage)
  }

  private async tryToConsume<MessageValue extends object>(
    message: Message<string, MessageValue, string, string>,
    handler: KafkaHandler<MessageValue, ExecutionContext>,
    requestContext: RequestContext,
  ): Promise<boolean> {
    try {
      await handler(message, this.executionContext, requestContext)
      return true
    } catch (error) {
      this.handlerError(error, {
        topic: message.topic,
        message: stringValueSerializer(message.value),
      })
    }

    return false
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

  private buildTransactionName(
    message: Message<string, SupportedMessageValues<TopicsConfig>, string, string>,
  ) {
    const messageType = this.resolveMessageType(message.value)

    let name = `kafka:${this.constructor.name}:${message.topic}`
    if (messageType?.trim().length) name += `:${messageType.trim()}`

    return name
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
