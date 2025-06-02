import {
  AbstractKafkaPublisher,
  type KafkaDependencies,
  type KafkaPublisherOptions,
} from '../../lib/index.js'
import { PERMISSION_TOPIC_MESSAGES_CONFIG } from '../utils/permissionSchemas.js'
import { getKafkaConfig } from '../utils/testContext.js'

type PermissionPublisherOptions = Partial<
  Pick<
    KafkaPublisherOptions<typeof PERMISSION_TOPIC_MESSAGES_CONFIG>,
    'handlerSpy' | 'kafka' | 'autocreateTopics' | 'topicsConfig'
  >
> & {
  supportMessageTypes?: boolean
}

export class PermissionPublisher extends AbstractKafkaPublisher<
  typeof PERMISSION_TOPIC_MESSAGES_CONFIG
> {
  constructor(deps: KafkaDependencies, options: PermissionPublisherOptions = {}) {
    super(deps, {
      topicsConfig: options.topicsConfig ?? PERMISSION_TOPIC_MESSAGES_CONFIG,
      kafka: options.kafka ?? getKafkaConfig(),
      autocreateTopics: options.autocreateTopics ?? true,
      handlerSpy: options?.handlerSpy ?? true,
      logMessages: true,
      messageIdField: options.supportMessageTypes === false ? undefined : 'id',
      messageTypeField: options.supportMessageTypes === false ? undefined : 'type',
    })
  }
}
