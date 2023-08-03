import { SNSClient } from '@aws-sdk/client-sns'
import { SQSClient } from '@aws-sdk/client-sqs'
import type { ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type { Logger, TransactionObservabilityManager } from '@message-queue-toolkit/core'
import type { Resolver } from 'awilix'
import { asClass, asFunction, createContainer, Lifetime } from 'awilix'
import { AwilixManager } from 'awilix-manager'

import { SnsSqsPermissionConsumerMonoSchema } from '../consumers/SnsSqsPermissionConsumerMonoSchema'
import { SnsSqsPermissionConsumerMultiSchema } from '../consumers/SnsSqsPermissionConsumerMultiSchema'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { SnsPermissionPublisherMonoSchema } from '../publishers/SnsPermissionPublisherMonoSchema'
import { SnsPermissionPublisherMultiSchema } from '../publishers/SnsPermissionPublisherMultiSchema'

import { TEST_AWS_CONFIG } from './testSnsConfig'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

// @ts-ignore
const TestLogger: Logger = console

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
    sqsClient: asFunction(
      () => {
        return new SQSClient(TEST_AWS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
        asyncDispose: 'destroy',
        asyncDisposePriority: 40,
      },
    ),
    snsClient: asFunction(
      () => {
        return new SNSClient(TEST_AWS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
        asyncDispose: 'destroy',
        asyncDisposePriority: 40,
      },
    ),

    consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),

    permissionConsumer: asClass(SnsSqsPermissionConsumerMonoSchema, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'start',
      asyncDispose: 'close',
      asyncInitPriority: 30,
      asyncDisposePriority: 30,
      enabled: queuesEnabled,
    }),
    permissionConsumerMultiSchema: asClass(SnsSqsPermissionConsumerMultiSchema, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'start',
      asyncDispose: 'close',
      asyncInitPriority: 30,
      asyncDisposePriority: 30,
      enabled: queuesEnabled,
    }),
    permissionPublisher: asClass(SnsPermissionPublisherMonoSchema, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncInitPriority: 20,
      asyncDisposePriority: 20,
      enabled: queuesEnabled,
    }),
    permissionPublisherMultiSchema: asClass(SnsPermissionPublisherMultiSchema, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncInitPriority: 20,
      asyncDisposePriority: 20,
      enabled: queuesEnabled,
    }),

    // vendor-specific dependencies
    transactionObservabilityManager: asFunction(() => {
      return undefined
    }, SINGLETON_CONFIG),
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

type DiConfig = Record<keyof Dependencies, Resolver<unknown>>

export interface Dependencies {
  logger: Logger
  sqsClient: SQSClient
  snsClient: SNSClient
  awilixManager: AwilixManager

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager

  errorReporter: ErrorReporter
  consumerErrorResolver: ErrorResolver
  permissionConsumer: SnsSqsPermissionConsumerMonoSchema
  permissionConsumerMultiSchema: SnsSqsPermissionConsumerMultiSchema
  permissionPublisher: SnsPermissionPublisherMonoSchema
  permissionPublisherMultiSchema: SnsPermissionPublisherMultiSchema
}
