import type { PubSub, Subscription, Topic } from '@google-cloud/pubsub'
import type { QueueDependencies, QueueOptions } from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'
import type { PubSubMessage } from '../types/MessageTypes.ts'
import { deletePubSub, initPubSub } from '../utils/pubSubInitter.ts'

// 10MB message size limit for Pub/Sub
export const PUBSUB_MESSAGE_MAX_SIZE = 10 * 1024 * 1024

export type PubSubDependencies = QueueDependencies & {
  pubSubClient: PubSub
}

export type PubSubTopicConfig = {
  name: string
  options?: {
    messageRetentionDuration?: {
      seconds: number
      nanos?: number
    }
    messageStoragePolicy?: {
      allowedPersistenceRegions?: string[]
    }
    kmsKeyName?: string
    enableMessageOrdering?: boolean
  }
}

export type PubSubSubscriptionConfig = {
  name: string
  options?: {
    ackDeadlineSeconds?: number
    retainAckedMessages?: boolean
    messageRetentionDuration?: {
      seconds: number
      nanos?: number
    }
    enableMessageOrdering?: boolean
    deadLetterPolicy?: {
      deadLetterTopic: string
      maxDeliveryAttempts: number
    }
    filter?: string
    enableExactlyOnceDelivery?: boolean
  }
}

export type PubSubCreationConfig = {
  topic: PubSubTopicConfig
  subscription?: PubSubSubscriptionConfig
  updateAttributesIfExists?: boolean
}

export type PubSubQueueLocatorType = {
  topicName: string
  subscriptionName?: string
}

export abstract class AbstractPubSubService<
  MessagePayloadType extends object,
  QueueLocatorType extends PubSubQueueLocatorType = PubSubQueueLocatorType,
  CreationConfigType extends PubSubCreationConfig = PubSubCreationConfig,
  PubSubOptionsType extends QueueOptions<CreationConfigType, QueueLocatorType> = QueueOptions<
    CreationConfigType,
    QueueLocatorType
  >,
  DependenciesType extends PubSubDependencies = PubSubDependencies,
  ExecutionContext = unknown,
  PrehandlerOutput = unknown,
> extends AbstractQueueService<
  MessagePayloadType,
  PubSubMessage,
  DependenciesType,
  CreationConfigType,
  QueueLocatorType,
  PubSubOptionsType,
  ExecutionContext,
  PrehandlerOutput
> {
  protected readonly pubSubClient: PubSub

  protected topicName!: string
  protected topic!: Topic
  protected subscriptionName?: string
  protected subscription?: Subscription

  constructor(dependencies: DependenciesType, options: PubSubOptionsType) {
    super(dependencies, options)
    this.pubSubClient = dependencies.pubSubClient
  }

  public async init(): Promise<void> {
    if (this.deletionConfig && this.creationConfig) {
      await deletePubSub(this.pubSubClient, this.deletionConfig, this.creationConfig)
    }

    const initResult = await initPubSub(this.pubSubClient, this.locatorConfig, this.creationConfig)

    this.topicName = initResult.topicName
    this.topic = initResult.topic
    this.subscriptionName = initResult.subscriptionName
    this.subscription = initResult.subscription

    this.isInitted = true
  }

  public override async close(): Promise<void> {
    this.isInitted = false
    await Promise.resolve()
  }
}
