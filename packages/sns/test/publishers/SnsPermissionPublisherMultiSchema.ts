import { AbstractSnsPublisherMultiSchema } from '../../lib/sns/AbstractSnsPublisherMultiSchema'
import type {
  SNSDependencies,
  NewSNSOptionsMultiSchema,
  ExistingSNSOptionsMultiSchema,
} from '../../lib/sns/AbstractSnsService'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas'

export type PERMISSIONS_SUPPORTED_MESSAGES =
  | PERMISSIONS_ADD_MESSAGE_TYPE
  | PERMISSIONS_REMOVE_MESSAGE_TYPE
//export type PERMISSIONS_SUPPORTED_MESSAGE_TYPES = PERMISSIONS_ADD_MESSAGE_TYPE['messageType'] | PERMISSIONS_REMOVE_MESSAGE_TYPE['messageType']

export class SnsPermissionPublisherMultiSchema extends AbstractSnsPublisherMultiSchema<PERMISSIONS_SUPPORTED_MESSAGES> {
  public static TOPIC_NAME = 'user_permissions_multi'

  constructor(
    dependencies: SNSDependencies,
    options:
      | Pick<NewSNSOptionsMultiSchema<PERMISSIONS_SUPPORTED_MESSAGES>, 'creationConfig'>
      | Pick<ExistingSNSOptionsMultiSchema<PERMISSIONS_SUPPORTED_MESSAGES>, 'locatorConfig'> = {
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
