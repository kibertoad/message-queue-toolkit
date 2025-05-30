import {
  AbstractKafkaPublisher,
  type KafkaDependencies,
  type KafkaPublisherOptions,
} from '../../lib/index.js'
import { PERMISSION_TOPIC_MESSAGES_CONFIG, TOPICS } from '../utils/permissionSchemas.js'
import { getKafkaConfig } from '../utils/testContext.js'

type PermissionPublisherOptions = Partial<
  Pick<
    KafkaPublisherOptions<typeof PERMISSION_TOPIC_MESSAGES_CONFIG>,
    'creationConfig' | 'locatorConfig' | 'handlerSpy' | 'kafka'
  >
>
export class PermissionPublisher extends AbstractKafkaPublisher<
  typeof PERMISSION_TOPIC_MESSAGES_CONFIG
> {
  constructor(deps: KafkaDependencies, options: PermissionPublisherOptions = {}) {
    super(deps, {
      ...(options.locatorConfig
        ? { locatorConfig: options.locatorConfig }
        : {
            creationConfig: options.creationConfig ?? {
              topics: TOPICS as any, // adding cast to avoid having to make TOPICS readonly
            },
          }),
      topicsConfig: PERMISSION_TOPIC_MESSAGES_CONFIG,
      kafka: options.kafka ?? getKafkaConfig(),
      handlerSpy: options?.handlerSpy ?? true,
      logMessages: true,
      messageIdField: 'id',
      messageTypeField: 'type',
    })
  }
}
