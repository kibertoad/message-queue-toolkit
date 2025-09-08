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

type PermissionConsumerOptions = Partial<
  Pick<
    KafkaConsumerOptions<typeof PERMISSION_TOPIC_MESSAGES_CONFIG, ExecutionContext, boolean>,
    | 'kafka'
    | 'handlerSpy'
    | 'autocreateTopics'
    | 'handlers'
    | 'headerRequestIdField'
    | 'messageIdField'
  >
>

export class PermissionConsumer extends AbstractKafkaConsumer<
  typeof PERMISSION_TOPIC_MESSAGES_CONFIG,
  ExecutionContext
> {
  private _addedMessages: Message<string, PermissionAdded, string, string>[] = []
  private _removedMessages: Message<string, PermissionRemoved, string, string>[] = []

  constructor(deps: KafkaConsumerDependencies, options: PermissionConsumerOptions = {}) {
    super(
      deps,
      {
        batchProcessingEnabled: false,
        handlers:
          options.handlers ??
          new KafkaHandlerRoutingBuilder<
            typeof PERMISSION_TOPIC_MESSAGES_CONFIG,
            ExecutionContext,
            false
          >()
            .addConfig(
              'permission-added',
              new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, (message, executionContext) => {
                executionContext.incrementAmount++
                this._addedMessages.push(message)
              }),
            )
            .addConfig(
              'permission-removed',
              new KafkaHandlerConfig(PERMISSION_REMOVED_SCHEMA, (message, executionContext) => {
                executionContext.incrementAmount++
                this._removedMessages.push(message)
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
