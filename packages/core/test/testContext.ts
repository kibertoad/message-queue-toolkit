import type { CommonLogger, ErrorReporter } from '@lokalise/node-core'
import { type Resolver, asClass } from 'awilix'
import { Lifetime, asFunction, createContainer } from 'awilix'
import { AwilixManager } from 'awilix-manager'
import { pino } from 'pino'
import { z } from 'zod/v3'

import type { TransactionObservabilityManager } from '@lokalise/node-core'
import { enrichMessageSchemaWithBase } from '@message-queue-toolkit/schemas'
import { DomainEventEmitter } from '../lib/events/DomainEventEmitter.ts'
import { EventRegistry } from '../lib/events/EventRegistry.ts'
import type { CommonEventDefinition } from '../lib/events/eventTypes.ts'
import type { MetadataFiller } from '../lib/messages/MetadataFiller.ts'
import { CommonMetadataFiller } from '../lib/messages/MetadataFiller.ts'
import { FakeTransactionObservabilityManager } from './fakes/FakeTransactionObservabilityManager.ts'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

const TestLogger: CommonLogger = pino()

export const TestEvents = {
  created: {
    ...enrichMessageSchemaWithBase(
      'entity.created',
      z.object({
        message: z.string(),
      }),
    ),
  },

  updated: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        message: z.string(),
      }),
    ),
  },
} as const satisfies Record<string, CommonEventDefinition>

export type TestEventsType = (typeof TestEvents)[keyof typeof TestEvents][]

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

    eventRegistry: asFunction(() => {
      return new EventRegistry(Object.values(TestEvents))
    }, SINGLETON_CONFIG),

    eventEmitter: asFunction((deps: Dependencies) => {
      return new DomainEventEmitter(deps, {
        handlerSpy: true,
      })
    }, SINGLETON_CONFIG),
    metadataFiller: asFunction(() => {
      return new CommonMetadataFiller({
        serviceId: 'test',
      })
    }, SINGLETON_CONFIG),

    // vendor-specific dependencies
    transactionObservabilityManager: asClass(FakeTransactionObservabilityManager, SINGLETON_CONFIG),
    errorReporter: asFunction(() => {
      return {
        report: () => {},
      } satisfies ErrorReporter
    }, SINGLETON_CONFIG),
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
  awilixManager: AwilixManager

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager

  errorReporter: ErrorReporter
  eventRegistry: EventRegistry<TestEventsType>
  eventEmitter: DomainEventEmitter<TestEventsType>
  metadataFiller: MetadataFiller
}
