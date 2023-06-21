import type { ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type { Logger, TransactionObservabilityManager } from '@message-queue-toolkit/core'
import type { Connection } from 'amqplib'
import type { Resolver } from 'awilix'
import { asClass, asFunction, createContainer, Lifetime } from 'awilix'
import { AwilixManager } from 'awilix-manager'

import type { AmqpConfig } from '../../lib/amqpConnectionResolver'
import { resolveAmqpConnection } from '../../lib/amqpConnectionResolver'
import { ConsumerErrorResolver } from '../../lib/errors/ConsumerErrorResolver'
import { PermissionConsumer } from '../consumers/PermissionConsumer'
import { PermissionPublisher } from '../publishers/PermissionPublisher'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

// @ts-ignore
const TestLogger: Logger = console

export async function registerDependencies(
  config: AmqpConfig,
  dependencyOverrides: DependencyOverrides = {},
) {
  const diContainer = createContainer({
    injectionMode: 'PROXY',
  })
  const amqpConnection = await resolveAmqpConnection(config)
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
    amqpConnection: asFunction(
      () => {
        return amqpConnection
      },
      {
        lifetime: Lifetime.SINGLETON,
        dispose: (connection) => {
          return connection.close()
        },
      },
    ),
    consumerErrorResolver: asFunction(() => {
      return new ConsumerErrorResolver()
    }),

    permissionConsumer: asClass(PermissionConsumer, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'consume',
      asyncDispose: 'close',
      asyncDisposePriority: 10,
    }),
    permissionPublisher: asClass(PermissionPublisher, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncDisposePriority: 20,
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
  amqpConnection: Connection
  awilixManager: AwilixManager

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager

  errorReporter: ErrorReporter
  consumerErrorResolver: ErrorResolver
  permissionConsumer: PermissionConsumer
  permissionPublisher: PermissionPublisher
}
