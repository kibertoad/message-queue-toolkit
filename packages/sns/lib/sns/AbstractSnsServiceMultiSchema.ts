import type { SNSClient } from '@aws-sdk/client-sns'
import type {
  ExistingQueueOptionsMultiSchema,
  NewQueueOptionsMultiSchema,
} from '@message-queue-toolkit/core'
import { AbstractQueueServiceMultiSchema } from '@message-queue-toolkit/core'

import type { SNSCreationConfig, SNSDependencies, SNSQueueLocatorType } from './AbstractSnsService'
import { initSns } from './SnsInitter'

export class AbstractSnsServiceMultiSchema<
  MessagePayloadSchemas extends object,
  SNSOptionsType extends
    | ExistingQueueOptionsMultiSchema<MessagePayloadSchemas, SNSQueueLocatorType>
    | NewQueueOptionsMultiSchema<MessagePayloadSchemas, SNSCreationConfig> =
    | ExistingQueueOptionsMultiSchema<MessagePayloadSchemas, SNSQueueLocatorType>
    | NewQueueOptionsMultiSchema<MessagePayloadSchemas, SNSCreationConfig>,
  DependenciesType extends SNSDependencies = SNSDependencies,
> extends AbstractQueueServiceMultiSchema<
  MessagePayloadSchemas,
  DependenciesType,
  SNSCreationConfig,
  SNSQueueLocatorType,
  SNSOptionsType
> {
  protected readonly snsClient: SNSClient
  // @ts-ignore
  public topicArn: string

  constructor(dependencies: DependenciesType, options: SNSOptionsType) {
    super(dependencies, options)

    this.snsClient = dependencies.snsClient
  }

  public async init() {
    const initResult = await initSns(this.snsClient, this.locatorConfig, this.creationConfig)
    this.topicArn = initResult.topicArn
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
