import { AbstractSnsPublisherMonoSchema } from '../../lib/sns/AbstractSnsPublisherMonoSchema'
import type {
  SNSDependencies,
  NewSNSOptions,
  ExistingSNSOptions,
} from '../../lib/sns/AbstractSnsService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'

export class SnsPermissionPublisherMonoSchema extends AbstractSnsPublisherMonoSchema<PERMISSIONS_MESSAGE_TYPE> {
  public static TOPIC_NAME = 'user_permissions'

  constructor(
    dependencies: SNSDependencies,
    options: Pick<NewSNSOptions, 'creationConfig'> | Pick<ExistingSNSOptions, 'locatorConfig'> = {
      creationConfig: {
        topic: {
          Name: SnsPermissionPublisherMonoSchema.TOPIC_NAME,
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
