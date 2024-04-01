import { AbstractSnsPublisher } from '../../lib/sns/AbstractSnsPublisher'
import type { SNSDependencies, SNSOptions } from '../../lib/sns/AbstractSnsService'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas'

type SupportedTypes = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class SnsPermissionPublisher extends AbstractSnsPublisher<SupportedTypes> {
  public static readonly TOPIC_NAME = 'user_permissions_multi'

  constructor(
    dependencies: SNSDependencies,
    options?: Pick<SNSOptions, 'creationConfig' | 'locatorConfig'>,
  ) {
    super(dependencies, {
      ...(options?.locatorConfig
        ? { locatorConfig: options?.locatorConfig }
        : {
            creationConfig: options?.creationConfig ?? {
              topic: { Name: SnsPermissionPublisher.TOPIC_NAME },
            },
          }),
      deletionConfig: {
        deleteIfExists: false,
      },
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA],
      handlerSpy: true,
      messageTypeField: 'messageType',
    })
  }

  get topicArnProp(): string {
    return this.topicArn
  }
}
