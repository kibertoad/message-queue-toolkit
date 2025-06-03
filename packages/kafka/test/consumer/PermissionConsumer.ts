import { randomUUID } from 'node:crypto'
import {
  AbstractKafkaConsumer,
  type KafkaConsumerOptions,
} from '../../lib/AbstractKafkaConsumer.js'
import { type KafkaDependencies, KafkaHandlerRoutingBuilder } from '../../lib/index.js'
import type { PERMISSION_TOPIC_MESSAGES_CONFIG } from '../utils/permissionSchemas.js'
import { getKafkaConfig } from '../utils/testContext.js'

export type PermissionConsumerOptions = Partial<
  Pick<
    KafkaConsumerOptions<typeof PERMISSION_TOPIC_MESSAGES_CONFIG>,
    'kafka' | 'handlerSpy' | 'autocreateTopics' | 'handlers'
  >
>

export class PermissionConsumer extends AbstractKafkaConsumer<
  typeof PERMISSION_TOPIC_MESSAGES_CONFIG
> {
  constructor(deps: KafkaDependencies, options: PermissionConsumerOptions = {}) {
    super(deps, {
      handlers: new KafkaHandlerRoutingBuilder().build(),
      autocreateTopics: options.autocreateTopics ?? true,
      clientId: randomUUID(),
      groupId: randomUUID(),
      kafka: options.kafka ?? getKafkaConfig(),
      logMessages: true,
      handlerSpy: options.handlerSpy ?? true,
      messageIdField: 'id',
      messageTypeField: 'type',
    })
  }
}
