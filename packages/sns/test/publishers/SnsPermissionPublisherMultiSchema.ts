import { AbstractSnsPublisherMultiSchema } from '../../lib/sns/AbstractSnsPublisherMultiSchema'
import type {
  SNSDependencies,
  NewSNSOptions,
  ExistingSNSOptions,
} from '../../lib/sns/AbstractSnsService'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas'

type SupportedTypes = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class SnsPermissionPublisherMultiSchema extends AbstractSnsPublisherMultiSchema<SupportedTypes> {
  public static TOPIC_NAME = 'user_permissions'

  constructor(
    dependencies: SNSDependencies,
    options: Pick<NewSNSOptions, 'creationConfig'> | Pick<ExistingSNSOptions, 'locatorConfig'> = {
      creationConfig: {
        topic: {
          Name: SnsPermissionPublisherMultiSchema.TOPIC_NAME,
        },
      },
    },
  ) {
    super(dependencies, {
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA],
      messageTypeField: 'messageType',
      ...options,
    })
  }
}
