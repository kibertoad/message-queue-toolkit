import { S3 } from '@aws-sdk/client-s3'
import { SQSClient } from '@aws-sdk/client-sqs'
import type { CommonLogger, ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type {
  MessageMetricsManager,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import type { Resolver } from 'awilix'
import { Lifetime, asClass, asFunction, createContainer } from 'awilix'
import { AwilixManager } from 'awilix-manager'

import { SqsConsumerErrorResolver } from '../../lib/errors/SqsConsumerErrorResolver'
import { SqsPermissionConsumer } from '../consumers/SqsPermissionConsumer'
import { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher'

import { Redis } from 'ioredis'
import { TEST_AWS_CONFIG } from './testAwsConfig'
import { TEST_REDIS_CONFIG } from './testRedisConfig'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

// @ts-ignore
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

    // Not disposing sqs client allows consumers to terminate correctly
    sqsClient: asFunction(
      () => {
        return new SQSClient(TEST_AWS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
      },
    ),
    s3: asFunction(() => {
      return new S3(TEST_AWS_CONFIG)
    }),
    consumerErrorResolver: asFunction(() => {
      return new SqsConsumerErrorResolver()
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

    permissionConsumer: asClass(SqsPermissionConsumer, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'start',
      asyncDispose: 'close',
      asyncDisposePriority: 10,
    }),
    permissionPublisher: asClass(SqsPermissionPublisher, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncDisposePriority: 20,
    }),

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

// biome-ignore lint/suspicious/noExplicitAny: This is expected
type DiConfig = Record<keyof Dependencies, Resolver<any>>

export interface Dependencies {
  logger: CommonLogger
  sqsClient: SQSClient
  s3: S3
  awilixManager: AwilixManager
  redis: Redis

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager
  messageMetricsManager: MessageMetricsManager

  errorReporter: ErrorReporter
  consumerErrorResolver: ErrorResolver
  permissionConsumer: SqsPermissionConsumer
  permissionPublisher: SqsPermissionPublisher
}
