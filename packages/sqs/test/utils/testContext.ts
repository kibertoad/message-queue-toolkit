import type { ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type { Logger, TransactionObservabilityManager } from '@message-queue-toolkit/core'
import { SQSClient } from '@aws-sdk/client-sqs'
import type { Resolver } from 'awilix'
import { asClass, asFunction, createContainer, Lifetime } from 'awilix'
import { AwilixManager } from 'awilix-manager'

import {SqsConsumerErrorResolver} from "../../lib/errors/SqsConsumerErrorResolver";
import {TEST_SQS_CONFIG} from "./testSqsConfig";
import {PermissionPublisher} from "../PermissionPublisher";
import {PermissionConsumer} from "../PermissionConsumer";

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

// @ts-ignore
const TestLogger: Logger = console

export async function registerDependencies(
  dependencyOverrides: DependencyOverrides = {},
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
        return new SQSClient(TEST_SQS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
        dispose: (sqs) => {
          return sqs.destroy()
        },
      },
    ),
    consumerErrorResolver: asFunction(() => {
      return new SqsConsumerErrorResolver()
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
  sqsClient: SQSClient
  awilixManager: AwilixManager

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager

  errorReporter: ErrorReporter
  consumerErrorResolver: ErrorResolver
  permissionConsumer: PermissionConsumer
  permissionPublisher: PermissionPublisher
}
