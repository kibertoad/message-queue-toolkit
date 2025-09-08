import { randomUUID } from 'node:crypto'
import type { Message } from '@platformatic/kafka'
import {
  AbstractKafkaConsumer,
  type KafkaConsumerDependencies,
  type KafkaConsumerOptions,
} from '../../lib/AbstractKafkaConsumer.ts'
import { KafkaHandlerConfig, KafkaHandlerRoutingBuilder } from '../../lib/index.ts'
import {
  PERMISSION_ADDED_SCHEMA,
  PERMISSION_REMOVED_SCHEMA,
  type PERMISSION_TOPIC_MESSAGES_CONFIG,
  type PermissionAdded,
  type PermissionRemoved,
} from '../utils/permissionSchemas.ts'
import { getKafkaConfig } from '../utils/testContext.ts'

type ExecutionContext = {
  incrementAmount: number
}

type PermissionBatchConsumerOptions = Partial<
  Pick<
    KafkaConsumerOptions<typeof PERMISSION_TOPIC_MESSAGES_CONFIG, ExecutionContext, true>,
    | 'kafka'
    | 'handlerSpy'
    | 'autocreateTopics'
    | 'handlers'
    | 'headerRequestIdField'
    | 'messageIdField'
    | 'batchProcessingOptions'
  >
>

export class PermissionBatchConsumer extends AbstractKafkaConsumer<
  typeof PERMISSION_TOPIC_MESSAGES_CONFIG,
  ExecutionContext,
  true
> {
  private _addedMessages: Message<string, PermissionAdded, string, string>[][] = []
  private _removedMessages: Message<string, PermissionRemoved, string, string>[][] = []

  constructor(deps: KafkaConsumerDependencies, options: PermissionBatchConsumerOptions = {}) {
    super(
      deps,
      {
        batchProcessingEnabled: true,
        batchProcessingOptions: options.batchProcessingOptions ?? {
          batchSize: 3,
          timeoutMilliseconds: 100,
        },
        handlers:
          options.handlers ??
          new KafkaHandlerRoutingBuilder<
            typeof PERMISSION_TOPIC_MESSAGES_CONFIG,
            ExecutionContext,
            true
          >()
            .addConfig(
              'permission-added',
              new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, (messages, executionContext) => {
                executionContext.incrementAmount++
                this._addedMessages.push(messages)
              }),
            )
            .addConfig(
              'permission-removed',
              new KafkaHandlerConfig(PERMISSION_REMOVED_SCHEMA, (messages, executionContext) => {
                executionContext.incrementAmount++
                this._removedMessages.push(messages)
              }),
            )
            .build(),
        autocreateTopics: options.autocreateTopics ?? true,
        groupId: randomUUID(),
        kafka: options.kafka ?? getKafkaConfig(),
        logMessages: true,
        handlerSpy: options.handlerSpy ?? true,
        headerRequestIdField: options.headerRequestIdField,
        messageIdField: options.messageIdField,
      },
      {
        incrementAmount: 0,
      },
    )
  }

  get addedMessages() {
    return this._addedMessages
  }

  get removedMessages() {
    return this._removedMessages
  }

  clear(): void {
    this._addedMessages = []
    this._removedMessages = []
  }
}
