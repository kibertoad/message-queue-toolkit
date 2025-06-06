import {
  AbstractKafkaPublisher,
  type KafkaDependencies,
  type KafkaPublisherOptions,
} from '../../lib/index.ts'
import { PERMISSION_TOPIC_MESSAGES_CONFIG } from '../utils/permissionSchemas.ts'
import { getKafkaConfig } from '../utils/testContext.ts'

type PermissionPublisherOptions = Partial<
  Pick<
    KafkaPublisherOptions<typeof PERMISSION_TOPIC_MESSAGES_CONFIG>,
    'handlerSpy' | 'kafka' | 'autocreateTopics' | 'topicsConfig' | 'headerRequestIdField'
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
      kafka: options.kafka ?? getKafkaConfig(),
      autocreateTopics: options.autocreateTopics ?? true,
      handlerSpy: options?.handlerSpy ?? true,
      logMessages: true,
      messageIdField: options.disableMessageTypeField === true ? undefined : 'id',
      messageTypeField: options.disableMessageTypeField === true ? undefined : 'type',
      headerRequestIdField: options.headerRequestIdField,
    })
  }
}
