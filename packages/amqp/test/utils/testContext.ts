import {
  type CommonLogger,
  type ErrorReporter,
  type ErrorResolver,
  globalLogger,
} from '@lokalise/node-core'
import type {
  MessageMetricsManager,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import {
  CommonMetadataFiller,
  EventRegistry,
  enrichMessageSchemaWithBase,
} from '@message-queue-toolkit/core'
import type { NameAndRegistrationPair } from 'awilix'
import { asClass, asFunction, createContainer, Lifetime } from 'awilix'
import { AwilixManager } from 'awilix-manager'
import { z } from 'zod/v4'
import { AmqpConnectionManager } from '../../lib/AmqpConnectionManager.ts'
import type { AmqpAwareEventDefinition } from '../../lib/AmqpQueuePublisherManager.ts'
import { AmqpQueuePublisherManager } from '../../lib/AmqpQueuePublisherManager.ts'
import { AmqpTopicPublisherManager } from '../../lib/AmqpTopicPublisherManager.ts'
import type { AmqpConfig } from '../../lib/amqpConnectionResolver.ts'
import type {
  CommonAmqpQueuePublisher,
  CommonAmqpTopicPublisher,
} from '../../lib/CommonAmqpPublisherFactory.ts'
import {
  CommonAmqpQueuePublisherFactory,
  CommonAmqpTopicPublisherFactory,
} from '../../lib/CommonAmqpPublisherFactory.ts'
import { AmqpConsumerErrorResolver } from '../../lib/errors/AmqpConsumerErrorResolver.ts'
import { AmqpPermissionConsumer } from '../consumers/AmqpPermissionConsumer.ts'
import { FakeQueueConsumer } from '../fakes/FakeQueueConsumer.ts'
import { AmqpPermissionPublisher } from '../publishers/AmqpPermissionPublisher.ts'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

export const TestEvents = {
  created: {
    ...enrichMessageSchemaWithBase(
      'entity.created',
      z.object({
        newData: z.string(),
      }),
    ),
    schemaVersion: '1.0.1',
    queueName: FakeQueueConsumer.QUEUE_NAME,
  },

  updated: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        updatedData: z.string(),
      }),
    ),
    queueName: FakeQueueConsumer.QUEUE_NAME,
  },

  updatedPubSub: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        updatedData: z.string(),
      }),
    ),
    exchange: 'entity',
    topic: 'updated',
  },

  updatedPubSubV2: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        updatedData: z.string(),
      }),
    ),
    exchange: 'entity_v2',
    topic: 'updated',
  },
} as const satisfies Record<string, AmqpAwareEventDefinition>

export type TestEventsType = (typeof TestEvents)[keyof typeof TestEvents][]
export type TestEventPublishPayloadsType = z.output<TestEventsType[number]['publisherSchema']>

const TestLogger: CommonLogger = globalLogger

export async function registerDependencies(
  config: AmqpConfig,
  dependencyOverrides: DependencyOverrides = {},
  queuesEnabled = true,
) {
  const diContainer = createContainer({
    injectionMode: 'PROXY',
  })
  const awilixManager = new AwilixManager({
    diContainer,
    asyncDispose: true,
    asyncInit: true,
    eagerInject: true,
  })

  const diConfig: DiConfig = {
    logger: asFunction(() => {
      return TestLogger
    }, SINGLETON_CONFIG),
    awilixManager: asFunction(() => {
      return awilixManager
    }, SINGLETON_CONFIG),
    amqpConnectionManager: asFunction(
      ({ logger }: Dependencies) => {
        return new AmqpConnectionManager(config, logger)
      },
      {
        lifetime: Lifetime.SINGLETON,
        asyncInit: 'init',
        asyncDispose: 'close',
        asyncDisposePriority: 1,
      },
    ),
    consumerErrorResolver: asFunction(() => {
      return new AmqpConsumerErrorResolver()
    }),

    permissionConsumer: asClass(AmqpPermissionConsumer, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'start',
      asyncDispose: 'close',
      asyncDisposePriority: 10,
      enabled: queuesEnabled,
    }),
    permissionPublisher: asClass(AmqpPermissionPublisher, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncDisposePriority: 20,
      enabled: queuesEnabled,
    }),

    eventRegistry: asFunction(() => {
      return new EventRegistry(Object.values(TestEvents))
    }, SINGLETON_CONFIG),

    queuePublisherManager: asFunction(
      (dependencies) => {
        return new AmqpQueuePublisherManager(dependencies, {
          metadataFiller: new CommonMetadataFiller({
            serviceId: 'service',
          }),
          publisherFactory: new CommonAmqpQueuePublisherFactory(),
          newPublisherOptions: {
            isLazyInitEnabled: true,
            handlerSpy: true,
            messageIdField: 'id',
            messageTypeResolver: { messageTypePath: 'type' },
          },
        })
      },
      {
        lifetime: Lifetime.SINGLETON,
        enabled: queuesEnabled,
      },
    ),

    queuePublisherManagerNoLazy: asFunction(
      (dependencies) => {
        return new AmqpQueuePublisherManager(dependencies, {
          metadataFiller: new CommonMetadataFiller({
            serviceId: 'service',
          }),
          publisherFactory: new CommonAmqpQueuePublisherFactory(),
          newPublisherOptions: {
            isLazyInitEnabled: false,
            handlerSpy: true,
            messageIdField: 'id',
            messageTypeResolver: { messageTypePath: 'type' },
          },
        })
      },
      {
        lifetime: Lifetime.SINGLETON,
        enabled: queuesEnabled,
      },
    ),

    topicPublisherManager: asFunction(
      (dependencies) => {
        return new AmqpTopicPublisherManager(dependencies, {
          metadataFiller: new CommonMetadataFiller({
            serviceId: 'service',
          }),
          publisherFactory: new CommonAmqpTopicPublisherFactory(),
          newPublisherOptions: {
            handlerSpy: true,
            messageIdField: 'id',
            messageTypeResolver: { messageTypePath: 'type' },
          },
        })
      },
      {
        lifetime: Lifetime.SINGLETON,
        enabled: queuesEnabled,
      },
    ),

    // vendor-specific dependencies
    transactionObservabilityManager: asFunction(() => {
      return undefined as unknown as TransactionObservabilityManager
    }, SINGLETON_CONFIG),
    messageMetricsManager: asFunction(() => undefined, SINGLETON_CONFIG),
    errorReporter: asFunction(() => {
      return {
        report: () => {},
      } satisfies ErrorReporter
    }, SINGLETON_CONFIG),
  }
  diContainer.register(diConfig)

  for (const [dependencyKey, dependencyValue] of Object.entries(dependencyOverrides)) {
    // @ts-expect-error
    diContainer.register(dependencyKey, dependencyValue)
  }

  await awilixManager.executeInit()

  return diContainer
}

type DiConfig = NameAndRegistrationPair<Dependencies>

export interface Dependencies {
  logger: CommonLogger
  amqpConnectionManager: AmqpConnectionManager
  awilixManager: AwilixManager

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager
  messageMetricsManager?: MessageMetricsManager

  errorReporter: ErrorReporter
  consumerErrorResolver: ErrorResolver
  permissionConsumer: AmqpPermissionConsumer
  permissionPublisher: AmqpPermissionPublisher

  eventRegistry: EventRegistry<TestEventsType>
  queuePublisherManager: AmqpQueuePublisherManager<
    CommonAmqpQueuePublisher<TestEventPublishPayloadsType>,
    TestEventsType
  >

  queuePublisherManagerNoLazy: AmqpQueuePublisherManager<
    CommonAmqpQueuePublisher<TestEventPublishPayloadsType>,
    TestEventsType
  >

  topicPublisherManager: AmqpTopicPublisherManager<
    CommonAmqpTopicPublisher<TestEventPublishPayloadsType>,
    TestEventsType
  >
}
