import { AbstractSnsPublisher } from '../../lib/sns/AbstractSnsPublisher.ts'
import type { SNSDependencies, SNSOptions } from '../../lib/sns/AbstractSnsService.ts'
import {
  CreateLocateConfigMixConsumer,
  type SupportedMessages,
} from '../consumers/CreateLocateConfigMixConsumer.ts'
import { TestEvents } from '../utils/testContext.ts'

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
      messageTypeResolver: { messageTypePath: 'type' },
    })
  }
}
