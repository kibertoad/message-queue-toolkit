import { AbstractSnsPublisher } from '../../lib/sns/AbstractSnsPublisher'
import type { SNSDependencies, SNSOptions } from '../../lib/sns/AbstractSnsService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'

export class SnsPermissionPublisher extends AbstractSnsPublisher<PERMISSIONS_MESSAGE_TYPE> {
  public static TOPIC_NAME = 'user_permissions'

  constructor(
    dependencies: SNSDependencies,
    options: Partial<Pick<SNSOptions<PERMISSIONS_MESSAGE_TYPE>, 'queueLocator'>>,
  ) {
    super(dependencies, {
      queueName: SnsPermissionPublisher.TOPIC_NAME,
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
      ...options,
    })
  }
}
