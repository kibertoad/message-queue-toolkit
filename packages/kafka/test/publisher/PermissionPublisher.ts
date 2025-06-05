import {
  AbstractKafkaPublisher,
  type KafkaDependencies,
  type KafkaPublisherOptions,
} from '../../lib/index.ts'
import { PERMISSION_TOPIC_MESSAGES_CONFIG } from '../utils/permissionSchemas.ts'
import { TEST_KAFKA_CONFIG } from '../utils/testKafkaConfig.js'

type PermissionPublisherOptions = Partial<
  Pick<
    KafkaPublisherOptions<typeof PERMISSION_TOPIC_MESSAGES_CONFIG>,
    'handlerSpy' | 'kafka' | 'autocreateTopics' | 'topicsConfig'
  >
> & {
  disableMessageTypeField?: boolean
}

export class PermissionPublisher extends AbstractKafkaPublisher<
  typeof PERMISSION_TOPIC_MESSAGES_CONFIG
> {
  constructor(deps: KafkaDependencies, options: PermissionPublisherOptions = {}) {
    super(deps, {
      topicsConfig: options.topicsConfig ?? PERMISSION_TOPIC_MESSAGES_CONFIG,
      kafka: options.kafka ?? TEST_KAFKA_CONFIG,
      autocreateTopics: options.autocreateTopics ?? true,
      handlerSpy: options?.handlerSpy ?? true,
      logMessages: true,
      messageIdField: options.disableMessageTypeField === true ? undefined : 'id',
      messageTypeField: options.disableMessageTypeField === true ? undefined : 'type',
    })
  }
}
