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
  Kafka,
  type Admin,
  type Consumer,
  type ConsumerConfig,
  type EachMessagePayload,
} from 'kafkajs'
import {
  AbstractKafkaService,
  type BaseKafkaOptions,
  type ProcessedMessage,
} from './AbstractKafkaService.ts'
import type { KafkaHandler, KafkaHandlerConfig } from './handler-routing/index.ts'
import type { KafkaHandlerRouting } from './handler-routing/KafkaHandlerRoutingBuilder.ts'
import type {
  DeserializedMessage,
  KafkaDependencies,
  RequestContext,
  SupportedMessageValues,
  SupportedTopics,
  TopicConfig,
} from './types.ts'
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
  Omit<ConsumerConfig, 'groupId'> &
  KafkaBatchProcessingOptions<BatchProcessingEnabled> & {
    handlers: KafkaHandlerRouting<TopicsConfig, ExecutionContext, BatchProcessingEnabled>
    groupId: string
    autocreateTopics?: boolean
    sessionTimeout?: number
    rebalanceTimeout?: number
    heartbeatInterval?: number
    fromBeginning?: boolean
  }

/*
TODO: Proper retry mechanism + DLQ
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
  private readonly kafka: Kafka
  private readonly consumer: Consumer
  private admin?: Admin
  private isConsumerConnected: boolean = false
  private isConsumerRunning: boolean = false
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

    this.kafka = new Kafka({
      clientId: this.options.kafka.clientId,
      brokers: this.options.kafka.brokers,
      ssl: this.options.kafka.ssl,
      sasl: this.options.kafka.sasl,
      connectionTimeout: this.options.kafka.connectionTimeout,
      requestTimeout: this.options.kafka.requestTimeout,
      retry: this.options.kafka.retry,
    })

    this.consumer = this.kafka.consumer({
      groupId: this.options.groupId,
      sessionTimeout: this.options.sessionTimeout,
      rebalanceTimeout: this.options.rebalanceTimeout,
      heartbeatInterval: this.options.heartbeatInterval,
    })

    const logDetails = { origin: this.constructor.name, groupId: this.options.groupId }
    /* v8 ignore start */
    this.consumer.on('consumer.group_join', (_) =>
      this.logger.debug(logDetails, 'Consumer is joining a group'),
    )
    this.consumer.on('consumer.rebalancing', (_) =>
      this.logger.debug(logDetails, 'Consumer is re-joining a group after a rebalance'),
    )
    this.consumer.on('consumer.disconnect', (_) =>
      this.logger.debug(logDetails, 'Consumer is leaving the group'),
    )
    /* v8 ignore stop */
  }

  /**
   * Returns true if consumer is connected to the broker
   */
  get isConnected(): boolean {
    return this.isConsumerConnected
  }

  /**
   * Returns `true` if the consumer is not closed, and it is currently an active member of a consumer group.
   * This method will return `false` during consumer group rebalancing.
   */
  get isActive(): boolean {
    return this.isConsumerRunning
  }

  async init(): Promise<void> {
    if (this.isConsumerRunning) return Promise.resolve()
    const topics = Object.keys(this.options.handlers)
    if (topics.length === 0) throw new Error('At least one topic must be defined')

    try {
      // Create topics if autocreateTopics is enabled
      if (this.options.autocreateTopics) {
        this.admin = this.kafka.admin()
        await this.admin.connect()
        await this.admin.createTopics({
          waitForLeaders: true,
          topics: topics.map((topic) => ({ topic })),
        })
      }

      await this.consumer.connect()
      this.isConsumerConnected = true

      await this.consumer.subscribe({
        topics,
        fromBeginning: this.options.fromBeginning ?? false,
      })

      // Create a promise that resolves when consumer joins the group or rejects on crash
      const groupJoinPromise = new Promise<void>((resolve, reject) => {
        const removeJoinListener = this.consumer.on(this.consumer.events.GROUP_JOIN, () => {
          removeJoinListener()
          removeCrashListener()
          resolve()
        })
        const removeCrashListener = this.consumer.on(this.consumer.events.CRASH, (event) => {
          removeJoinListener()
          removeCrashListener()
          reject(event.payload.error)
        })
      })

      if (this.options.batchProcessingEnabled && this.options.batchProcessingOptions) {
        this.messageBatchStream = new KafkaMessageBatchStream<
          DeserializedMessage<SupportedMessageValues<TopicsConfig>>
        >(
          (batch) =>
            this.consume(batch.topic, batch.messages).catch((error) => this.handlerError(error)),
          this.options.batchProcessingOptions,
        )

        await this.consumer.run({
          autoCommit: false,
          eachMessage: async (payload) => {
            const message = this.transformMessage(payload)
            if (message) {
              this.messageBatchStream!.write(message)
            }
          },
        })
      } else {
        await this.consumer.run({
          autoCommit: false,
          eachMessage: async (payload) => {
            const message = this.transformMessage(payload)
            if (message) {
              await this.consume(payload.topic, message)
              await this.commitOffset(payload)
            }
          },
        })
      }

      // Wait for consumer to actually join the group before returning
      await groupJoinPromise

      this.isConsumerRunning = true
    } catch (error) {
      throw new InternalError({
        message: 'Consumer init failed',
        errorCode: 'KAFKA_CONSUMER_INIT_ERROR',
        cause: error,
      })
    }
  }

  private transformMessage(
    payload: EachMessagePayload,
  ): DeserializedMessage<SupportedMessageValues<TopicsConfig>> | null {
    const { topic, partition, message } = payload

    const value = safeJsonDeserializer(message.value)
    if (!value) return null

    const headers: Record<string, string | undefined> = {}
    if (message.headers) {
      for (const [key, val] of Object.entries(message.headers)) {
        headers[key] = val?.toString()
      }
    }

    return {
      topic,
      partition,
      key: message.key?.toString() ?? null,
      value: value as SupportedMessageValues<TopicsConfig>,
      headers,
      offset: message.offset,
      timestamp: message.timestamp,
    }
  }

  private async commitOffset(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload
    const logDetails = {
      topic,
      offset: message.offset,
      timestamp: message.timestamp,
    }
    this.logger.debug(logDetails, 'Trying to commit message')

    try {
      await this.consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (BigInt(message.offset) + 1n).toString(),
        },
      ])
      this.logger.debug(logDetails, 'Message committed successfully')
    } catch (error) {
      this.logger.debug(logDetails, 'Message commit failed')
      // Handle rebalance-related errors gracefully
      const errorMessage = (error as Error).message ?? ''
      if (
        errorMessage.includes('rebalance') ||
        errorMessage.includes('not a member') ||
        errorMessage.includes('generation')
      ) {
        this.logger.error(
          {
            error,
            errorMessage,
          },
          `Failed to commit message: ${errorMessage}`,
        )
      } else {
        throw error
      }
    }
  }

  async close(): Promise<void> {
    if (!this.isConsumerConnected && !this.isConsumerRunning) return

    if (this.messageBatchStream) {
      await new Promise((resolve) => this.messageBatchStream?.end(resolve))
      this.messageBatchStream = undefined
    }

    await this.consumer.disconnect()

    if (this.admin) {
      await this.admin.disconnect()
      this.admin = undefined
    }

    this.isConsumerConnected = false
    this.isConsumerRunning = false
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
    if (!handlerConfig) return

    const validMessages = this.parseMessages(
      handlerConfig,
      messageOrBatch,
      messageProcessingStartTimestamp,
    )

    if (!validMessages.length) {
      this.logger.debug({ origin: this.constructor.name, topic }, 'Received not valid message(s)')
      return
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

  private buildTransactionName(topic: string) {
    const baseTransactionName = `kafka:${this.constructor.name}:${topic}`
    return this.options.batchProcessingEnabled
      ? `${baseTransactionName}:batch`
      : baseTransactionName
  }

  private getRequestContext(
    message: DeserializedMessage<SupportedMessageValues<TopicsConfig>>,
  ): RequestContext {
    let reqId = message.headers[this.resolveHeaderRequestIdField()]
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
