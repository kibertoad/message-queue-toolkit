import { PubSub } from '@google-cloud/pubsub'
import { Storage } from '@google-cloud/storage'
import type { CommonLogger, ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type {
  MessageMetricsManager,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import type { Resolver } from 'awilix'
import { asClass, asFunction, createContainer, Lifetime } from 'awilix'
import { AwilixManager } from 'awilix-manager'
import { Redis } from 'ioredis'
import { PubSubConsumerErrorResolver } from '../../lib/errors/PubSubConsumerErrorResolver.ts'
import { PubSubPermissionConsumer } from '../consumers/PubSubPermissionConsumer.ts'
import { PubSubPermissionPublisher } from '../publishers/PubSubPermissionPublisher.ts'
import { TEST_PUBSUB_CONFIG } from './testPubSubConfig.ts'
import { TEST_REDIS_CONFIG } from './testRedisConfig.ts'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

// @ts-expect-error
const TestLogger: CommonLogger = console

export async function registerDependencies(dependencyOverrides: DependencyOverrides = {}) {
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

    pubSubClient: asFunction(
      () => {
        return new PubSub({
          projectId: TEST_PUBSUB_CONFIG.projectId,
          apiEndpoint: TEST_PUBSUB_CONFIG.apiEndpoint,
        })
      },
      {
        lifetime: Lifetime.SINGLETON,
      },
    ),

    gcsStorage: asFunction(() => {
      return new Storage({
        projectId: 'test-project',
        apiEndpoint: 'http://127.0.0.1:4443',
      })
    }),

    consumerErrorResolver: asFunction(() => {
      return new PubSubConsumerErrorResolver()
    }),

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
          lazyConnect: true,
        })
      },
      {
        asyncInitPriority: 0,
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

    permissionConsumer: asClass(PubSubPermissionConsumer, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'start',
      asyncDispose: 'close',
      asyncDisposePriority: 10,
    }),
    permissionPublisher: asClass(PubSubPermissionPublisher, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncDisposePriority: 20,
    }),

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
  pubSubClient: PubSub
  gcsStorage: Storage
  awilixManager: AwilixManager
  redis: Redis

  transactionObservabilityManager: TransactionObservabilityManager
  messageMetricsManager: MessageMetricsManager

  errorReporter: ErrorReporter
  consumerErrorResolver: ErrorResolver
  permissionConsumer: PubSubPermissionConsumer
  permissionPublisher: PubSubPermissionPublisher
}
