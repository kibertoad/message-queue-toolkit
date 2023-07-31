import type {
  MonoSchemaQueueOptions,
  QueueConsumer as QueueConsumer,
} from '@message-queue-toolkit/core'

import type { SQSMessage } from '../types/MessageTypes'

import type {
  SQSConsumerDependencies,
  SQSQueueLocatorType,
} from './AbstractSqsService'
import {ZodSchema} from "zod";
import {
  AbstractSqsConsumer,
  ExistingSQSConsumerOptions,
  NewSQSConsumerOptions,
  SQSCreationConfig
} from "./AbstractSqsConsumer";

export type NewSQSConsumerOptionsMono<
    MessagePayloadType extends object,
    CreationConfigType extends SQSCreationConfig>
    = NewSQSConsumerOptions<MessagePayloadType, CreationConfigType>
& MonoSchemaQueueOptions<MessagePayloadType>

export type ExistingSQSConsumerOptionsMono<
    MessagePayloadType extends object,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType>
    = ExistingSQSConsumerOptions<MessagePayloadType, QueueLocatorType>
    & MonoSchemaQueueOptions<MessagePayloadType>


export abstract class AbstractSqsConsumerMonoSchema<
    MessagePayloadType extends object,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    ConsumerOptionsType extends
      | NewSQSConsumerOptionsMono<MessagePayloadType, CreationConfigType>
      | ExistingSQSConsumerOptionsMono<MessagePayloadType, QueueLocatorType> =
      | NewSQSConsumerOptionsMono<MessagePayloadType, CreationConfigType>
      | ExistingSQSConsumerOptionsMono<MessagePayloadType, QueueLocatorType>,
  >
  extends AbstractSqsConsumer<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType
  >
  implements QueueConsumer
{

  private readonly messageSchema: ZodSchema

  protected constructor(dependencies: SQSConsumerDependencies, options: ConsumerOptionsType) {
    super(dependencies, options)

    this.messageSchema = options.messageSchema
  }

  protected resolveSchema(message: SQSMessage) {
    return this.messageSchema
  }
}
