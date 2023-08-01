import { AbstractSnsPublisher } from '../../lib/sns/AbstractSnsPublisher'
import type {
  SNSDependencies,
  NewSNSOptions,
  ExistingSNSOptions,
} from '../../lib/sns/AbstractSnsService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'

export class SnsPermissionPublisher extends AbstractSnsPublisher<PERMISSIONS_MESSAGE_TYPE> {
  public static TOPIC_NAME = 'user_permissions'

  constructor(
    dependencies: SNSDependencies,
    options: Pick<NewSNSOptions, 'creationConfig'> | Pick<ExistingSNSOptions, 'locatorConfig'> = {
      creationConfig: {
        topic: {
          Name: SnsPermissionPublisher.TOPIC_NAME,
        },
      },
    },
  ) {
    super(dependencies, {
      messageTypeField: 'messageType',
      ...options,
    })
  }
}
