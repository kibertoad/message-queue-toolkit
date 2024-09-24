import { AbstractSnsPublisher } from '../../lib/sns/AbstractSnsPublisher'
import type { SNSDependencies, SNSOptions } from '../../lib/sns/AbstractSnsService'
import {
  CreateLocateConfigMixConsumer,
  type SupportedMessages,
} from '../consumers/CreateLocateConfigMixConsumer'
import { TestEvents } from '../utils/testContext'

export class CreateLocateConfigMixPublisher extends AbstractSnsPublisher<SupportedMessages> {
  constructor(
    dependencies: SNSDependencies,
    options?: Pick<SNSOptions, 'creationConfig' | 'locatorConfig'>,
  ) {
    super(dependencies, {
      ...(options?.locatorConfig
        ? { locatorConfig: options?.locatorConfig }
        : {
            creationConfig: options?.creationConfig ?? {
              topic: { Name: CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME },
            },
          }),
      deletionConfig: {
        deleteIfExists: false,
      },
      messageSchemas: [TestEvents.created.consumerSchema, TestEvents.updated.consumerSchema],
      handlerSpy: true,
      messageTypeField: 'type',
    })
  }
}
