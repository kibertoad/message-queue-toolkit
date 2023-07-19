import { AbstractSnsPublisher } from '../../lib/sns/AbstractSnsPublisher'
import type {
  SNSDependencies,
  NewSNSOptions,
  ExistingSNSOptions,
} from '../../lib/sns/AbstractSnsService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'

export class SnsPermissionPublisher extends AbstractSnsPublisher<PERMISSIONS_MESSAGE_TYPE> {
  public static TOPIC_NAME = 'user_permissions'

  constructor(
    dependencies: SNSDependencies,
    options:
      | Pick<NewSNSOptions<PERMISSIONS_MESSAGE_TYPE>, 'creationConfig'>
      | Pick<ExistingSNSOptions<PERMISSIONS_MESSAGE_TYPE>, 'locatorConfig'> = {
      creationConfig: {
        topic: {
          Name: SnsPermissionPublisher.TOPIC_NAME,
        },
      },
    },
  ) {
    super(dependencies, {
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
      ...options,
    })
  }
}
