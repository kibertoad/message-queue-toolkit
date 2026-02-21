import { S3 } from '@aws-sdk/client-s3'
import { SNSClient } from '@aws-sdk/client-sns'
import { SQSClient } from '@aws-sdk/client-sqs'
import { STSClient } from '@aws-sdk/client-sts'
import type { CommonLogger, ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type {
  MessageMetricsManager,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import {
  CommonMetadataFiller,
  EventRegistry,
  enrichMessageSchemaWithBase,
} from '@message-queue-toolkit/core'
import { FakeConsumerErrorResolver } from '@message-queue-toolkit/sqs'
import type { Resolver } from 'awilix'
import { asClass, asFunction, createContainer, Lifetime } from 'awilix'
import { AwilixManager } from 'awilix-manager'
import { Redis } from 'ioredis'
import { z } from 'zod/v4'
import type { CommonSnsPublisher } from '../../lib/sns/CommonSnsPublisherFactory.ts'
import { CommonSnsPublisherFactory } from '../../lib/sns/CommonSnsPublisherFactory.ts'
import type { SnsAwareEventDefinition } from '../../lib/sns/SnsPublisherManager.ts'
import { SnsPublisherManager } from '../../lib/sns/SnsPublisherManager.ts'
import { SnsSqsPermissionConsumer } from '../consumers/SnsSqsPermissionConsumer.ts'
import { CreateLocateConfigMixPublisher } from '../publishers/CreateLocateConfigMixPublisher.ts'
import { SnsPermissionPublisher } from '../publishers/SnsPermissionPublisher.ts'
import { getFauxqsServer } from './fauxqsInstance.ts'
import { TestAwsResourceAdmin } from './testAdmin.ts'
import { TEST_REDIS_CONFIG } from './testRedisConfig.ts'
import { TEST_AWS_CONFIG, TEST_S3_CONFIG } from './testSnsConfig.ts'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

// @ts-expect-error
const TestLogger: CommonLogger = console

export const TestEvents = {
  created: {
    ...enrichMessageSchemaWithBase(
      'entity.created',
      z.object({
        newData: z.string(),
      }),
    ),
    schemaVersion: '1.0.1',
    snsTopic: 'dummy',
  },

  updated: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        updatedData: z.string(),
      }),
    ),
    snsTopic: 'dummy',
  },
} as const satisfies Record<string, SnsAwareEventDefinition>

export type TestEventsType = (typeof TestEvents)[keyof typeof TestEvents][]
export type TestEventPublishPayloadsType = z.output<TestEventsType[number]['publisherSchema']>

export async function registerDependencies(
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

    // Not disposing sqs client allows consumers to terminate correctly
    sqsClient: asFunction(
      () => {
        return new SQSClient(TEST_AWS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
      },
    ),
    snsClient: asFunction(
      () => {
        return new SNSClient(TEST_AWS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
      },
    ),
    stsClient: asFunction(
      () => {
        return new STSClient(TEST_AWS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
      },
    ),
    s3: asFunction(() => {
      return new S3(TEST_S3_CONFIG)
    }),

    consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),

    testAdmin: asFunction((deps) => {
      return new TestAwsResourceAdmin({
        server: getFauxqsServer(),
        sqsClient: deps.sqsClient,
        s3: deps.s3,
        snsClient: deps.snsClient,
        stsClient: deps.stsClient,
      })
    }, SINGLETON_CONFIG),

    permissionConsumer: asClass(SnsSqsPermissionConsumer, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'start',
      asyncDispose: 'close',
      asyncInitPriority: 30,
      asyncDisposePriority: 30,
      enabled: queuesEnabled,
    }),
    permissionPublisher: asClass(SnsPermissionPublisher, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncInitPriority: 40,
      asyncDisposePriority: 40,
      enabled: queuesEnabled,
    }),
    createLocateConfigMixPublisher: asClass(CreateLocateConfigMixPublisher, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncInitPriority: 40,
      asyncDisposePriority: 40,
      enabled: queuesEnabled,
    }),
    eventRegistry: asFunction(() => {
      return new EventRegistry(Object.values(TestEvents))
    }, SINGLETON_CONFIG),
    publisherManager: asFunction(
      (dependencies) => {
        return new SnsPublisherManager(dependencies, {
          metadataFiller: new CommonMetadataFiller({
            serviceId: 'service',
          }),
          publisherFactory: new CommonSnsPublisherFactory(),
          newPublisherOptions: {
            handlerSpy: true,
            messageIdField: 'id',
            messageTypeResolver: { messageTypePath: 'type' },
            creationConfig: {
              updateAttributesIfExists: true,
            },
          },
        })
      },
      {
        lifetime: Lifetime.SINGLETON,
        enabled: queuesEnabled,
      },
    ),

    redis: asFunction(
      () => {
        const redisConfig = TEST_REDIS_CONFIG

        return new Redis({
          host: redisConfig.host,
          db: redisConfig.db,
          port: redisConfig.port,
          username: redisConfig.username,
          password: redisConfig.password,
          connectTimeout: redisConfig.connectTimeout,
          commandTimeout: redisConfig.commandTimeout,
          tls: redisConfig.useTls ? {} : undefined,
          maxRetriesPerRequest: null,
          lazyConnect: true, // connect handled by asyncInit
        })
      },
      {
        asyncInitPriority: 0, // starting at the very beginning
        asyncInit: 'connect',
        dispose: (redis) => {
          return new Promise((resolve) => {
            void redis.quit((_err, result) => {
              return resolve(result)
            })
          })
        },
        lifetime: Lifetime.SINGLETON,
      },
    ),

    // vendor-specific dependencies
    transactionObservabilityManager: asFunction(() => {
      return undefined
    }, SINGLETON_CONFIG),
    messageMetricsManager: asFunction(() => undefined, SINGLETON_CONFIG),
    errorReporter: asFunction(() => {
      return {
        report: () => {},
      } satisfies ErrorReporter
    }),
  }
  diContainer.register(diConfig)

  for (const [dependencyKey, dependencyValue] of Object.entries(dependencyOverrides)) {
    diContainer.register(dependencyKey, dependencyValue)
  }

  await awilixManager.executeInit()

  return diContainer
}

type DiConfig = Record<keyof Dependencies, Resolver<any>>

export interface Dependencies {
  logger: CommonLogger
  sqsClient: SQSClient
  snsClient: SNSClient
  stsClient: STSClient
  s3: S3
  awilixManager: AwilixManager
  redis: Redis

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager
  messageMetricsManager: MessageMetricsManager

  errorReporter: ErrorReporter
  consumerErrorResolver: ErrorResolver
  permissionConsumer: SnsSqsPermissionConsumer
  permissionPublisher: SnsPermissionPublisher
  eventRegistry: EventRegistry<TestEventsType>
  publisherManager: SnsPublisherManager<
    CommonSnsPublisher<TestEventPublishPayloadsType>,
    TestEventsType
  >
  createLocateConfigMixPublisher: CreateLocateConfigMixPublisher
  testAdmin: TestAwsResourceAdmin
}
