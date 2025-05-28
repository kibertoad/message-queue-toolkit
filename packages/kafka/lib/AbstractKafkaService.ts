import { AbstractQueueService, type CommonQueueOptions } from '@message-queue-toolkit/core'
import type { BaseOptions } from '@platformatic/kafka'
import type { KafkaConfig, KafkaDependencies, KafkaTopicCreatorLocator } from './types.js'

export type BaseKafkaOptions = {
  kafka: KafkaConfig
} & CommonQueueOptions & // TODO: what should we do with this message type field and others?
  Omit<BaseOptions, keyof KafkaConfig> // Exclude properties that are already in KafkaConfig

export abstract class AbstractKafkaService<
  MessagePayloadType extends object,
  KafkaOptions extends BaseKafkaOptions = BaseKafkaOptions,
> extends AbstractQueueService<
  MessagePayloadType,
  object, // TODO: maybe needed for consumer? will check later
  KafkaDependencies,
  KafkaTopicCreatorLocator,
  KafkaTopicCreatorLocator,
  KafkaOptions
> {
  public override close(): Promise<void> {
    this.isInitted = false
    return Promise.resolve()
  }
}
