import { randomUUID } from 'node:crypto'
import type { Message } from '@platformatic/kafka'
import {
  AbstractKafkaConsumer,
  type KafkaConsumerOptions,
} from '../../lib/AbstractKafkaConsumer.js'
import {
  type KafkaDependencies,
  KafkaHandlerConfig,
  KafkaHandlerRoutingBuilder,
} from '../../lib/index.js'
import {
  PERMISSION_ADDED_SCHEMA,
  PERMISSION_REMOVED_SCHEMA,
  PERMISSION_SCHEMA,
  type PERMISSION_TOPIC_MESSAGES_CONFIG,
  type Permission,
  type PermissionAdded,
  type PermissionRemoved,
} from '../utils/permissionSchemas.js'
import { TEST_KAFKA_CONFIG } from '../utils/testKafkaConfig.js'

export type PermissionConsumerOptions = Partial<
  Pick<
    KafkaConsumerOptions<typeof PERMISSION_TOPIC_MESSAGES_CONFIG>,
    'kafka' | 'handlerSpy' | 'autocreateTopics' | 'handlers' | 'connectTimeout'
  >
>

export class PermissionConsumer extends AbstractKafkaConsumer<
  typeof PERMISSION_TOPIC_MESSAGES_CONFIG
> {
  private _addedMessages: Message<string, PermissionAdded, string, object>[] = []
  private _removedMessages: Message<string, PermissionRemoved, string, object>[] = []
  private _noTypeMessages: Message<string, Permission, string, object>[] = []

  constructor(deps: KafkaDependencies, options: PermissionConsumerOptions = {}) {
    super(deps, {
      handlers:
        options.handlers ??
        new KafkaHandlerRoutingBuilder<typeof PERMISSION_TOPIC_MESSAGES_CONFIG>()
          .addConfig(
            'permission-added',
            new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, (message) => {
              this._addedMessages.push(message)
            }),
          )
          .addConfig(
            'permission-removed',
            new KafkaHandlerConfig(PERMISSION_REMOVED_SCHEMA, (message) => {
              this._removedMessages.push(message)
            }),
          )
          .addConfig(
            'permission-general',
            new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, (message) => {
              this._addedMessages.push(message)
            }),
          )
          .addConfig(
            'permission-general',
            new KafkaHandlerConfig(PERMISSION_REMOVED_SCHEMA, (message) => {
              this._removedMessages.push(message)
            }),
          )
          .addConfig(
            'permission-general',
            new KafkaHandlerConfig(PERMISSION_SCHEMA, (message) => {
              this._noTypeMessages.push(message)
            }),
          )
          .build(),
      autocreateTopics: options.autocreateTopics ?? true,
      clientId: randomUUID(),
      groupId: randomUUID(),
      kafka: options.kafka ?? TEST_KAFKA_CONFIG,
      logMessages: true,
      handlerSpy: options.handlerSpy ?? true,
      messageIdField: 'id',
      messageTypeField: 'type',
    })
  }

  get addedMessages() {
    return this._addedMessages
  }

  get removedMessages() {
    return this._removedMessages
  }

  get noTypeMessages() {
    return this._noTypeMessages
  }

  clear(): void {
    this._addedMessages = []
    this._removedMessages = []
    this._noTypeMessages = []
  }
}
