import {
  AbstractKafkaPublisher,
  type KafkaDependencies,
  type KafkaPublisherOptions,
} from '../../lib/index.js'
import {
  PERMISSION_ADDED_SCHEMA,
  PERMISSION_REMOVED_SCHEMA,
  type PermissionAdded,
  type PermissionRemoved,
} from '../utils/permissionSchemas.js'
import { getKafkaConfig } from '../utils/testContext.js'

type MessagePayload = PermissionAdded | PermissionRemoved

type PermissionPublisherOptions<MessagePayload extends object> = Partial<
  Pick<
    KafkaPublisherOptions<MessagePayload>,
    'creationConfig' | 'locatorConfig' | 'handlerSpy' | 'kafka' | 'messageSchemas'
  >
>
export class PermissionPublisher extends AbstractKafkaPublisher<MessagePayload> {
  public static readonly TOPIC_NAME = 'permission'

  constructor(deps: KafkaDependencies, options: PermissionPublisherOptions<MessagePayload> = {}) {
    super(deps, {
      ...(options.locatorConfig
        ? { locatorConfig: options.locatorConfig }
        : {
            creationConfig: options.creationConfig ?? {
              topic: PermissionPublisher.TOPIC_NAME,
            },
          }),
      messageSchemas: options.messageSchemas ?? [
        PERMISSION_ADDED_SCHEMA,
        PERMISSION_REMOVED_SCHEMA,
      ],
      kafka: options.kafka ?? getKafkaConfig(),
      handlerSpy: options?.handlerSpy ?? true,
      logMessages: true,
      messageIdField: 'id',
      messageTypeField: 'type',
    })
  }
}
